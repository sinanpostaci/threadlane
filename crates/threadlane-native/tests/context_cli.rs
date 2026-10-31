#![cfg(unix)]
//! End-to-end behavioral tests for the packaged `threadlane-context` CLI.
//!
//! Covers outer help/version without context or runtime, context-free `help workflow` /
//! `help session` / `help flow` offline sibling help with hostile `PATH`, sanitized syntax errors,
//! existing private v1 context validation with legacy installation metadata, `check` + `workflow
//! catalog` + `session` missing-host refusal, flow forwarding with the same five bound stores as
//! session, foreign/offline bindings, protected scope/context flag override refusals with zero side
//! effects, bad schema/route/identity/duplicate fields, package-pin and store guards,
//! symlink/insecure/oversized context files, missing or unsafe sibling binaries, and child
//! stdio/exit/signal (`outcome_unknown`) handling.

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

async fn run_bin_with_path(bin: &Path, args: &[&str], path_value: &str) -> Output {
    Command::new(bin)
        .args(args)
        .env("PATH", path_value)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .await
        .expect("execute CLI binary with an explicit hostile PATH")
}

fn write_argv_recording_sibling(dir: &Path, name: &str, marker: &Path) -> PathBuf {
    let body = format!(
        "#!/bin/sh\nprintf '%s\\n' \"$#\" \"$@\" > \"{}\"\nexit 0\n",
        marker.display()
    );
    write_executable_script(dir, name, &body, 0o755)
}

fn read_argv_marker(marker: &Path) -> Vec<String> {
    fs::read_to_string(marker)
        .expect("read argv marker")
        .lines()
        .map(str::to_string)
        .collect()
}

fn five_store_scope_args(fixture: &ContextFixture) -> Vec<String> {
    vec![
        "--state-dir".to_string(),
        fixture
            .state_dir
            .to_str()
            .expect("utf8 state_dir")
            .to_string(),
        "--registry-dir".to_string(),
        fixture
            .registry_dir
            .to_str()
            .expect("utf8 registry_dir")
            .to_string(),
        "--plans-dir".to_string(),
        fixture
            .plans_dir
            .to_str()
            .expect("utf8 plans_dir")
            .to_string(),
        "--authority-dir".to_string(),
        fixture
            .authority_dir
            .to_str()
            .expect("utf8 authority_dir")
            .to_string(),
        "--sessions-dir".to_string(),
        fixture
            .sessions_dir
            .to_str()
            .expect("utf8 sessions_dir")
            .to_string(),
        "--binding".to_string(),
        fixture.binding_id.clone(),
        "--thread".to_string(),
        THREAD_UUID.to_string(),
    ]
}

fn workflow_scope_args(fixture: &ContextFixture) -> Vec<String> {
    vec![
        "--state-dir".to_string(),
        fixture
            .state_dir
            .to_str()
            .expect("utf8 state_dir")
            .to_string(),
        "--registry-dir".to_string(),
        fixture
            .registry_dir
            .to_str()
            .expect("utf8 registry_dir")
            .to_string(),
        "--plans-dir".to_string(),
        fixture
            .plans_dir
            .to_str()
            .expect("utf8 plans_dir")
            .to_string(),
        "--binding".to_string(),
        fixture.binding_id.clone(),
        "--thread".to_string(),
        THREAD_UUID.to_string(),
    ]
}

fn expected_recorded_argv(scope: &[String], inner: &[&str]) -> Vec<String> {
    let mut expected = Vec::with_capacity(1 + scope.len() + inner.len());
    expected.push((scope.len() + inner.len()).to_string());
    expected.extend(scope.iter().cloned());
    expected.extend(inner.iter().map(|arg| (*arg).to_string()));
    expected
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
async fn offline_family_help_succeeds_without_context_or_supervisor_and_ignores_hostile_path() {
    let mut fixture = ContextFixture::start().await;

    // Context-free family help must succeed after the fixture supervisor is stopped and without
    // any --context argument.
    fixture.shutdown().await;

    let hostile_dir = fixture.root.join("hostile-path-bin");
    fs::create_dir_all(&hostile_dir).expect("create hostile PATH directory");
    fs::set_permissions(&hostile_dir, fs::Permissions::from_mode(0o755))
        .expect("chmod hostile PATH directory 0755");
    let hostile_marker = fixture.root.join("hostile-path-child.marker");
    let hostile_script = format!(
        "#!/bin/sh\nprintf 'hostile-path' > \"{}\"\nexit 0\n",
        hostile_marker.display()
    );
    for name in ["threadlane-workflow", "threadlane-session"] {
        write_executable_script(&hostile_dir, name, &hostile_script, 0o755);
    }

    // Package siblings record their exact argv so the test can prove only fixed --help is passed.
    let workflow_marker = fixture.root.join("workflow-help-argv.marker");
    let workflow_script = format!(
        "#!/bin/sh\nprintf '%s\\n' \"$#\" \"$@\" > \"{}\"\nexit 0\n",
        workflow_marker.display()
    );
    write_executable_script(
        &fixture.pkg_bin_dir,
        "threadlane-workflow",
        &workflow_script,
        0o755,
    );

    let session_marker = fixture.root.join("session-help-argv.marker");
    let session_script = format!(
        "#!/bin/sh\nprintf '%s\\n' \"$#\" \"$@\" > \"{}\"\nexit 0\n",
        session_marker.display()
    );
    write_executable_script(
        &fixture.pkg_bin_dir,
        "threadlane-session",
        &session_script,
        0o755,
    );

    let hostile_path = hostile_dir.to_str().expect("utf8 hostile PATH directory");
    for (family, marker) in [("workflow", &workflow_marker), ("session", &session_marker)] {
        let out = run_bin_with_path(&fixture.context_bin, &["help", family], hostile_path).await;
        assert!(
            out.status.success(),
            "offline `help {family}` failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        assert!(
            out.stderr.is_empty(),
            "offline `help {family}` must not write stderr"
        );
        assert_eq!(
            fs::read_to_string(marker).expect("read package sibling argv marker"),
            "1\n--help\n",
            "offline `help {family}` must forward only fixed --help to the package sibling"
        );
        assert!(
            !hostile_marker.exists(),
            "offline `help {family}` must not resolve a sibling through PATH"
        );
    }
}

#[tokio::test]
async fn offline_flow_help_uses_fixed_help_without_context_supervisor_or_path_lookup() {
    let mut fixture = ContextFixture::start().await;
    fixture.shutdown().await;

    let hostile_dir = fixture.root.join("hostile-flow-help-path-bin");
    fs::create_dir_all(&hostile_dir).expect("create hostile flow help PATH directory");
    fs::set_permissions(&hostile_dir, fs::Permissions::from_mode(0o755))
        .expect("chmod hostile flow help PATH directory 0755");
    let hostile_marker = fixture.root.join("hostile-flow-help-child.marker");
    let hostile_script = format!(
        "#!/bin/sh\nprintf 'hostile-path' > \"{}\"\nexit 0\n",
        hostile_marker.display()
    );
    write_executable_script(&hostile_dir, "threadlane-flow", &hostile_script, 0o755);
    let hostile_path = hostile_dir
        .to_str()
        .expect("utf8 hostile flow help PATH directory");

    // A missing package flow sibling is refused even while PATH contains a same-named executable.
    let missing = run_bin_with_path(&fixture.context_bin, &["help", "flow"], hostile_path).await;
    assert_error_code(&missing, "not_configured");
    assert!(
        !hostile_marker.exists(),
        "missing package flow sibling must not fall back to PATH"
    );

    // With the supervisor stopped and no context argument, the offline form forwards exactly one
    // fixed --help argument to the canonical package flow sibling.
    let help_marker = fixture.root.join("flow-help-argv.marker");
    write_argv_recording_sibling(&fixture.pkg_bin_dir, "threadlane-flow", &help_marker);

    let out = run_bin_with_path(&fixture.context_bin, &["help", "flow"], hostile_path).await;
    assert!(
        out.status.success(),
        "offline `help flow` failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        out.stderr.is_empty(),
        "offline `help flow` must not write stderr"
    );
    assert_eq!(
        read_argv_marker(&help_marker),
        vec!["1".to_string(), "--help".to_string()],
        "offline `help flow` must forward exactly one fixed --help argument"
    );
    assert!(
        !hostile_marker.exists(),
        "offline `help flow` must not resolve a sibling through PATH"
    );
}

#[tokio::test]
async fn family_help_rejects_invalid_forms_before_child_and_preserves_sibling_refusals() {
    let mut fixture = ContextFixture::start().await;
    fixture.shutdown().await;

    // A fully valid context path proves these forms are rejected by argument parsing, not by a
    // context-read failure.
    let ctx_path =
        fixture.write_private_context("help-refusal-context.json", &fixture.valid_context_json());
    let ctx_str = ctx_path.to_str().expect("utf8 context path");
    let ctx_eq = format!("--context={ctx_str}");

    let marker_file = fixture.root.join("help-refusal-child.marker");
    let marker_script = format!(
        "#!/bin/sh\nprintf 'spawned' > \"{}\"\nexit 0\n",
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

    let invalid_help_forms: &[&[&str]] = &[
        &["help", "check"],
        &["help", "approve"],
        &["help", "host"],
        &["help", "operator"],
        &["help", "unknown-family"],
        &["help", "workflow=ignored"],
        &["help", "--help"],
        &["help", "-h"],
        &["help", "workflow", "extra"],
        &["help", "workflow", SECRET_SENTINEL],
        &["help", "session", "--help"],
        &["help", "workflow", "--help"],
        &["help", "workflow", "--state-dir=/tmp/other"],
        &["help", "session", "--thread", OTHER_THREAD_UUID],
        &["help", "--context", ctx_str],
        &["help", ctx_eq.as_str()],
        &["--context", ctx_str, "help", "workflow"],
        &[ctx_eq.as_str(), "help", "session"],
    ];
    for args in invalid_help_forms {
        let out = run_bin(&fixture.context_bin, args).await;
        assert_error_code(&out, "invalid_input");
        assert!(
            !marker_file.exists(),
            "invalid help form must be rejected before child invocation: {args:?}"
        );
    }

    // A missing package sibling is refused for offline help even when PATH contains a hostile
    // executable with the same name.
    let hostile_dir = fixture.root.join("hostile-help-path-bin");
    fs::create_dir_all(&hostile_dir).expect("create hostile help PATH directory");
    fs::set_permissions(&hostile_dir, fs::Permissions::from_mode(0o755))
        .expect("chmod hostile help PATH directory 0755");
    let hostile_marker = fixture.root.join("hostile-help-child.marker");
    let hostile_script = format!(
        "#!/bin/sh\nprintf 'hostile-path' > \"{}\"\nexit 0\n",
        hostile_marker.display()
    );
    write_executable_script(&hostile_dir, "threadlane-workflow", &hostile_script, 0o755);

    let workflow_sibling = fixture.pkg_bin_dir.join("threadlane-workflow");
    fs::remove_file(&workflow_sibling).expect("remove threadlane-workflow sibling");
    let missing_out = run_bin_with_path(
        &fixture.context_bin,
        &["help", "workflow"],
        hostile_dir.to_str().expect("utf8 hostile PATH directory"),
    )
    .await;
    assert_error_code(&missing_out, "not_configured");
    assert!(
        !hostile_marker.exists(),
        "missing package sibling must not fall back to PATH"
    );

    // Unsafe symlink siblings are still refused for offline help.
    symlink(
        Path::new(env!("CARGO_BIN_EXE_threadlane-workflow")),
        &workflow_sibling,
    )
    .expect("symlink threadlane-workflow sibling");
    let symlink_out = run_bin(&fixture.context_bin, &["help", "workflow"]).await;
    assert_error_code(&symlink_out, "security_violation");
    assert!(!marker_file.exists());
    fs::remove_file(&workflow_sibling).expect("remove symlink workflow sibling");

    // The session help form has the same missing-sibling refusal.
    fs::remove_file(fixture.pkg_bin_dir.join("threadlane-session"))
        .expect("remove threadlane-session sibling");
    let missing_session = run_bin(&fixture.context_bin, &["help", "session"]).await;
    assert_error_code(&missing_session, "not_configured");
    assert!(!marker_file.exists());
}

#[tokio::test]
async fn flow_forwards_the_same_five_bound_stores_as_session_and_ignores_path_and_metadata() {
    let mut fixture = ContextFixture::start().await;

    let hostile_dir = fixture.root.join("hostile-flow-path-bin");
    fs::create_dir_all(&hostile_dir).expect("create hostile flow PATH directory");
    fs::set_permissions(&hostile_dir, fs::Permissions::from_mode(0o755))
        .expect("chmod hostile flow PATH directory 0755");
    let hostile_marker = fixture.root.join("hostile-flow-child.marker");
    let hostile_script = format!(
        "#!/bin/sh\nprintf 'hostile-path' > \"{}\"\nexit 0\n",
        hostile_marker.display()
    );
    let hostile_flow =
        write_executable_script(&hostile_dir, "threadlane-flow", &hostile_script, 0o755);
    let hostile_path = hostile_dir
        .to_str()
        .expect("utf8 hostile flow PATH directory");

    // Non-operational context metadata naming a child executable must never select it.
    let mut flow_context = fixture.valid_context_json();
    flow_context["flow_executable"] = json!(hostile_flow);
    let ctx_path = fixture.write_private_context("flow-forwarding-context.json", &flow_context);
    let ctx_str = ctx_path.to_str().expect("utf8 flow context path");

    let flow_marker = fixture.root.join("flow-forward-argv.marker");
    let session_marker = fixture.root.join("session-forward-argv.marker");
    let workflow_marker = fixture.root.join("workflow-forward-argv.marker");
    write_argv_recording_sibling(&fixture.pkg_bin_dir, "threadlane-flow", &flow_marker);
    write_argv_recording_sibling(&fixture.pkg_bin_dir, "threadlane-session", &session_marker);
    write_argv_recording_sibling(
        &fixture.pkg_bin_dir,
        "threadlane-workflow",
        &workflow_marker,
    );

    let inner = ["fixture-command", "--limit", "5"];

    let mut flow_args = vec!["--context", ctx_str, "flow"];
    flow_args.extend(inner);
    let flow_out = run_bin_with_path(&fixture.context_bin, &flow_args, hostile_path).await;
    assert!(
        flow_out.status.success(),
        "flow forwarding failed: {}",
        String::from_utf8_lossy(&flow_out.stderr)
    );
    assert!(
        flow_out.stderr.is_empty(),
        "flow forwarding must not write stderr"
    );
    assert!(
        !hostile_marker.exists(),
        "flow forwarding must not resolve a sibling through PATH or context metadata"
    );
    let flow_argv = read_argv_marker(&flow_marker);
    assert_eq!(
        flow_argv,
        expected_recorded_argv(&five_store_scope_args(&fixture), &inner),
        "flow must inject the five context-bound stores plus binding/thread before inner args"
    );

    // Session must receive the identical five-store scope and binding/thread prefix.
    let mut session_args = vec!["--context", ctx_str, "session"];
    session_args.extend(inner);
    let session_out = run_bin(&fixture.context_bin, &session_args).await;
    assert!(
        session_out.status.success(),
        "session forwarding failed: {}",
        String::from_utf8_lossy(&session_out.stderr)
    );
    assert!(
        session_out.stderr.is_empty(),
        "session forwarding must not write stderr"
    );
    assert_eq!(
        read_argv_marker(&session_marker),
        flow_argv,
        "flow must forward exactly the same five stores and binding/thread as session"
    );

    // Workflow keeps its existing three-store scope after flow support is added.
    let mut workflow_args = vec!["--context", ctx_str, "workflow"];
    workflow_args.extend(inner);
    let workflow_out = run_bin(&fixture.context_bin, &workflow_args).await;
    assert!(
        workflow_out.status.success(),
        "workflow forwarding failed: {}",
        String::from_utf8_lossy(&workflow_out.stderr)
    );
    assert!(
        workflow_out.stderr.is_empty(),
        "workflow forwarding must not write stderr"
    );
    assert_eq!(
        read_argv_marker(&workflow_marker),
        expected_recorded_argv(&workflow_scope_args(&fixture), &inner),
        "workflow forwarding must keep its existing three-store scope"
    );

    fixture.shutdown().await;
}

#[tokio::test]
async fn missing_or_unsafe_flow_sibling_is_refused_before_child_invocation() {
    let mut fixture = ContextFixture::start().await;
    let ctx_path =
        fixture.write_private_context("flow-sibling-context.json", &fixture.valid_context_json());
    let ctx_str = ctx_path.to_str().unwrap();
    let flow_sibling = fixture.pkg_bin_dir.join("threadlane-flow");

    let hostile_dir = fixture.root.join("hostile-flow-sibling-path-bin");
    fs::create_dir_all(&hostile_dir).expect("create hostile flow sibling PATH directory");
    fs::set_permissions(&hostile_dir, fs::Permissions::from_mode(0o755))
        .expect("chmod hostile flow sibling PATH directory 0755");
    let hostile_marker = fixture.root.join("hostile-flow-sibling.marker");
    let hostile_script = format!(
        "#!/bin/sh\nprintf 'hostile-path' > \"{}\"\nexit 0\n",
        hostile_marker.display()
    );
    write_executable_script(&hostile_dir, "threadlane-flow", &hostile_script, 0o755);
    let hostile_path = hostile_dir
        .to_str()
        .expect("utf8 hostile flow sibling PATH directory");

    // A missing package flow sibling is refused without falling back to PATH.
    let missing = run_bin_with_path(
        &fixture.context_bin,
        &["--context", ctx_str, "flow", "fixture-command"],
        hostile_path,
    )
    .await;
    assert_error_code(&missing, "not_configured");
    assert!(
        !hostile_marker.exists(),
        "missing package flow sibling must not fall back to PATH"
    );

    // A symlink flow sibling is refused for operational forwarding and offline help.
    symlink(
        Path::new(env!("CARGO_BIN_EXE_threadlane-workflow")),
        &flow_sibling,
    )
    .expect("symlink threadlane-flow sibling");
    let symlink_out = run_bin(
        &fixture.context_bin,
        &["--context", ctx_str, "flow", "fixture-command"],
    )
    .await;
    assert_error_code(&symlink_out, "security_violation");
    let offline_symlink = run_bin(&fixture.context_bin, &["help", "flow"]).await;
    assert_error_code(&offline_symlink, "security_violation");
    fs::remove_file(&flow_sibling).expect("remove symlink flow sibling");

    // A directory in place of the flow sibling is refused.
    fs::create_dir(&flow_sibling).expect("create directory flow sibling");
    let dir_out = run_bin(
        &fixture.context_bin,
        &["--context", ctx_str, "flow", "fixture-command"],
    )
    .await;
    assert_error_code(&dir_out, "security_violation");
    fs::remove_dir(&flow_sibling).expect("remove directory flow sibling");

    // A non-executable regular file flow sibling is refused.
    write_executable_script(
        &fixture.pkg_bin_dir,
        "threadlane-flow",
        "#!/bin/sh\nexit 0\n",
        0o600,
    );
    let non_exec_out = run_bin(
        &fixture.context_bin,
        &["--context", ctx_str, "flow", "fixture-command"],
    )
    .await;
    assert_error_code(&non_exec_out, "security_violation");

    fixture.shutdown().await;
}

#[tokio::test]
async fn flow_rejects_invalid_extra_and_protected_arguments_before_child_invocation() {
    let mut fixture = ContextFixture::start().await;
    let ctx_path =
        fixture.write_private_context("flow-protected-context.json", &fixture.valid_context_json());
    let ctx_str = ctx_path.to_str().unwrap();
    let ctx_eq = format!("--context={ctx_str}");

    let marker_file = fixture.root.join("flow-protected-child.marker");
    let marker_script = format!(
        "#!/bin/sh\nprintf 'spawned' > \"{}\"\nexit 0\n",
        marker_file.display()
    );
    write_executable_script(
        &fixture.pkg_bin_dir,
        "threadlane-flow",
        &marker_script,
        0o755,
    );

    let invalid_cases: &[&[&str]] = &[
        // Missing operational inner arguments.
        &["--context", ctx_str, "flow"],
        // Offline help accepts no extra argument, context, or protected override.
        &["help", "flow", "extra"],
        &["help", "flow", SECRET_SENTINEL],
        &["help", "flow", "--help"],
        &["help", "flow", "--state-dir=/tmp/other"],
        &["help", "flow=ignored"],
        &["--context", ctx_str, "help", "flow"],
        &[ctx_eq.as_str(), "help", "flow"],
        // A protected override cannot occupy the family position.
        &[
            "--context",
            ctx_str,
            "--state-dir=/tmp/other",
            "flow",
            "fixture-command",
        ],
        // Protected store, binding, thread, context, and project-root overrides are refused in
        // separate, equals, and repeated forms.
        &[
            "--context",
            ctx_str,
            "flow",
            "fixture-command",
            "--state-dir",
            "/tmp/other",
        ],
        &[
            "--context",
            ctx_str,
            "flow",
            "fixture-command",
            "--state-dir=/tmp/other",
        ],
        &[
            "--context",
            ctx_str,
            "flow",
            "fixture-command",
            "--registry-dir",
            "/tmp/other",
        ],
        &[
            "--context",
            ctx_str,
            "flow",
            "fixture-command",
            "--registry-dir=/tmp/other",
        ],
        &[
            "--context",
            ctx_str,
            "flow",
            "fixture-command",
            "--plans-dir",
            "/tmp/other",
        ],
        &[
            "--context",
            ctx_str,
            "flow",
            "fixture-command",
            "--plans-dir=/tmp/other",
        ],
        &[
            "--context",
            ctx_str,
            "flow",
            "fixture-command",
            "--authority-dir",
            "/tmp/other",
        ],
        &[
            "--context",
            ctx_str,
            "flow",
            "fixture-command",
            "--authority-dir=/tmp/other",
        ],
        &[
            "--context",
            ctx_str,
            "flow",
            "fixture-command",
            "--sessions-dir",
            "/tmp/other",
        ],
        &[
            "--context",
            ctx_str,
            "flow",
            "fixture-command",
            "--sessions-dir=/tmp/other",
        ],
        &[
            "--context",
            ctx_str,
            "flow",
            "fixture-command",
            "--binding",
            "bnd_override",
        ],
        &[
            "--context",
            ctx_str,
            "flow",
            "fixture-command",
            "--binding=bnd_override",
        ],
        &[
            "--context",
            ctx_str,
            "flow",
            "fixture-command",
            "--binding=bnd_override",
            "--binding=bnd_override",
        ],
        &[
            "--context",
            ctx_str,
            "flow",
            "fixture-command",
            "--thread",
            OTHER_THREAD_UUID,
        ],
        &[
            "--context",
            ctx_str,
            "flow",
            "fixture-command",
            &format!("--thread={OTHER_THREAD_UUID}"),
        ],
        &[
            "--context",
            ctx_str,
            "flow",
            "fixture-command",
            "--thread",
            OTHER_THREAD_UUID,
            "--thread",
            OTHER_THREAD_UUID,
        ],
        &[
            "--context",
            ctx_str,
            "flow",
            "fixture-command",
            "--context",
            ctx_str,
        ],
        &[
            "--context",
            ctx_str,
            "flow",
            "fixture-command",
            ctx_eq.as_str(),
        ],
        &[
            "--context",
            ctx_str,
            "flow",
            "fixture-command",
            "--project-root",
            "/tmp/other",
        ],
        &[
            "--context",
            ctx_str,
            "flow",
            "fixture-command",
            "--project-root=/tmp/other",
        ],
        &[
            "--context",
            ctx_str,
            "flow",
            "fixture-command",
            "--project_root=/tmp/other",
        ],
    ];

    for case in invalid_cases {
        let out = run_bin(&fixture.context_bin, case).await;
        assert_error_code(&out, "invalid_input");
        assert!(
            !marker_file.exists(),
            "invalid flow arguments must never spawn the child executable: {case:?}"
        );
    }

    // A valid flow invocation proves the marker sibling is live and the refusals above were
    // caused by argument validation rather than a broken fixture.
    let valid_out = run_bin(
        &fixture.context_bin,
        &["--context", ctx_str, "flow", "fixture-command"],
    )
    .await;
    assert!(
        valid_out.status.success(),
        "valid flow forwarding failed: {}",
        String::from_utf8_lossy(&valid_out.stderr)
    );
    assert!(
        marker_file.exists(),
        "valid flow forwarding must invoke the package flow sibling"
    );

    fixture.shutdown().await;
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
