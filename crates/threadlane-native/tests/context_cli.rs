#![cfg(unix)]
//! End-to-end behavioral tests for the packaged `threadlane-context` CLI.
//!
//! Covers outer help/version without context or runtime, context-free `help workflow` /
//! `help session` / `help flow` / `help advice` offline sibling help with hostile `PATH`,
//! sanitized syntax errors, existing private v1 context validation with legacy installation
//! metadata, `check` + `workflow catalog` + `session` missing-host refusal, flow forwarding with
//! the same five bound stores as session, advice forwarding with the frozen four-flag scope
//! (`--registry-dir`, `--cache-dir <plans_dir>/advice-cache`, `--project-root`, `--context-id`)
//! without creating `advice-cache`, foreign/offline bindings, protected scope/context/cache/
//! context-id flag override refusals with zero side effects, bad schema/route/identity/duplicate
//! fields, package-pin and store guards, symlink/insecure/oversized context files, missing or
//! unsafe sibling binaries, and child stdio/exit/signal (`outcome_unknown`) handling.

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
use threadlane_native::coworker_workflow::CoworkerWorkflowStore;
use threadlane_native::external_resolution::{
    ExternalResolutionScope, ExternalResolutionTarget, RecordExternalResolutionRequest,
};
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
                        decision_advisor: None,
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

#[derive(Debug, PartialEq, Eq)]
struct TreeEntry {
    relative: PathBuf,
    mode: u32,
    bytes: Vec<u8>,
}

fn snapshot_tree(root: &Path) -> Vec<TreeEntry> {
    fn walk(root: &Path, dir: &Path, entries: &mut Vec<TreeEntry>) {
        let mut children: Vec<_> = fs::read_dir(dir)
            .expect("read snapshot directory")
            .map(|entry| entry.expect("snapshot directory entry"))
            .collect();
        children.sort_by_key(|entry| entry.file_name());
        for child in children {
            let path = child.path();
            let metadata = fs::symlink_metadata(&path).expect("stat snapshot path");
            let relative = path
                .strip_prefix(root)
                .expect("snapshot path under root")
                .to_path_buf();
            let mode = metadata.permissions().mode() & 0o777;
            if metadata.file_type().is_dir() {
                entries.push(TreeEntry {
                    relative,
                    mode,
                    bytes: Vec::new(),
                });
                walk(root, &path, entries);
            } else if metadata.file_type().is_file() {
                entries.push(TreeEntry {
                    relative,
                    mode,
                    bytes: fs::read(&path).expect("read snapshot file"),
                });
            } else {
                entries.push(TreeEntry {
                    relative,
                    mode,
                    bytes: Vec::new(),
                });
            }
        }
    }

    let mut entries = Vec::new();
    walk(root, root, &mut entries);
    entries
}

async fn run_context_flow(fixture: &ContextFixture, context: &str, inner: &[&str]) -> Output {
    let mut args = vec!["--context", context, "flow"];
    args.extend_from_slice(inner);
    Command::new(&fixture.context_bin)
        .args(args)
        .env("THREADLANE_OBSERVATION_PARENT_REQUEST_ID", "parent-request")
        .env("THREADLANE_OBSERVATION_PLAN_ID", "parent-plan")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .await
        .expect("run real context->flow command")
}

fn parse_flow_data(output: &Output) -> Value {
    assert!(
        output.status.success(),
        "flow command failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let envelope: Value = serde_json::from_slice(&output.stdout).expect("parse flow JSON envelope");
    assert_eq!(envelope["untrusted_data"], true);
    envelope["data"].clone()
}

fn assert_no_host_repo_or_journal_effects(
    fixture: &ContextFixture,
    expected_head: &str,
    expected_lib: &[u8],
) {
    assert!(
        !fixture.sessions_dir.join(&fixture.approval_id).exists(),
        "external-resolution CLI must not create an approved host session"
    );
    assert_eq!(
        fs::read_to_string(fixture.project_root.join(".git/HEAD")).expect("read fixture HEAD"),
        expected_head,
        "external-resolution CLI must not mutate Git HEAD"
    );
    assert_eq!(
        fs::read(fixture.project_root.join("src/lib.rs")).expect("read fixture source"),
        expected_lib,
        "external-resolution CLI must not mutate repository files"
    );
    assert!(
        !fixture.plans_dir.join("observation-journal.jsonl").exists(),
        "external-resolution CLI must not create an observation journal"
    );
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

fn flow_scope_args(fixture: &ContextFixture) -> Vec<String> {
    let mut args = five_store_scope_args(fixture);
    args.push("--project-root".to_string());
    args.push(
        fixture
            .project_root
            .to_str()
            .expect("utf8 project_root")
            .to_string(),
    );
    args
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

fn advice_scope_args(fixture: &ContextFixture) -> Vec<String> {
    vec![
        "--registry-dir".to_string(),
        fixture
            .registry_dir
            .to_str()
            .expect("utf8 registry_dir")
            .to_string(),
        "--cache-dir".to_string(),
        fixture
            .plans_dir
            .join("advice-cache")
            .to_str()
            .expect("utf8 advice-cache dir")
            .to_string(),
        "--project-root".to_string(),
        fixture
            .project_root
            .to_str()
            .expect("utf8 project_root")
            .to_string(),
        "--context-id".to_string(),
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
        assert!(stdout.contains("approve <APPROVE_ARGS>..."));
        assert!(stdout.contains("help approve"));
        assert!(stdout.contains("advice <ADVICE_ARGS>..."));
        assert!(stdout.contains("help advice"));
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

    // Unsupported command families (host, operator) must be rejected without pass-through.
    for family in ["host", "operator", "unknown-family"] {
        let out = run_bin(
            bin,
            &["--context", "/tmp/nonexistent.json", family, "status"],
        )
        .await;
        assert_error_code(&out, "invalid_input");
    }

    // Extra arguments after `check` or missing arguments after `workflow`/`session`/`flow`/`approve`/`advice`.
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

    let empty_flow = run_bin(bin, &["--context", "/tmp/nonexistent.json", "flow"]).await;
    assert_error_code(&empty_flow, "invalid_input");

    let empty_approve = run_bin(bin, &["--context", "/tmp/nonexistent.json", "approve"]).await;
    assert_error_code(&empty_approve, "invalid_input");

    let empty_advice = run_bin(bin, &["--context", "/tmp/nonexistent.json", "advice"]).await;
    assert_error_code(&empty_advice, "invalid_input");

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
        &["help", "host"],
        &["help", "operator"],
        &["help", "unknown-family"],
        &["help", "workflow=ignored"],
        &["help", "approve=ignored"],
        &["help", "--help"],
        &["help", "-h"],
        &["help", "workflow", "extra"],
        &["help", "approve", "extra"],
        &["help", "workflow", SECRET_SENTINEL],
        &["help", "session", "--help"],
        &["help", "approve", "--help"],
        &["help", "workflow", "--help"],
        &["help", "workflow", "--state-dir=/tmp/other"],
        &["help", "approve", "--state-dir=/tmp/other"],
        &["help", "session", "--thread", OTHER_THREAD_UUID],
        &["help", "--context", ctx_str],
        &["help", ctx_eq.as_str()],
        &["--context", ctx_str, "help", "workflow"],
        &["--context", ctx_str, "help", "approve"],
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
        expected_recorded_argv(&flow_scope_args(&fixture), &inner),
        "flow must inject the five context-bound stores plus binding/thread/project root before inner args"
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
        expected_recorded_argv(&five_store_scope_args(&fixture), &inner),
        "flow and session must forward the same five stores and binding/thread; flow additionally receives the canonical project root"
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
async fn offline_advice_help_uses_fixed_help_without_context_supervisor_path_lookup_or_cache_creation(
) {
    let mut fixture = ContextFixture::start().await;
    fixture.shutdown().await;

    let hostile_dir = fixture.root.join("hostile-advice-help-path-bin");
    fs::create_dir_all(&hostile_dir).expect("create hostile advice help PATH directory");
    fs::set_permissions(&hostile_dir, fs::Permissions::from_mode(0o755))
        .expect("chmod hostile advice help PATH directory 0755");
    let hostile_marker = fixture.root.join("hostile-advice-help-child.marker");
    let hostile_script = format!(
        "#!/bin/sh\nprintf 'hostile-path' > \"{}\"\nexit 0\n",
        hostile_marker.display()
    );
    write_executable_script(&hostile_dir, "threadlane-advice", &hostile_script, 0o755);
    let hostile_path = hostile_dir
        .to_str()
        .expect("utf8 hostile advice help PATH directory");

    // A missing package advice sibling is refused even while PATH contains a same-named executable.
    let missing = run_bin_with_path(&fixture.context_bin, &["help", "advice"], hostile_path).await;
    assert_error_code(&missing, "not_configured");
    assert!(
        !hostile_marker.exists(),
        "missing package advice sibling must not fall back to PATH"
    );
    assert!(
        !fixture.plans_dir.join("advice-cache").exists(),
        "offline `help advice` must never create advice-cache"
    );

    // With the supervisor stopped and no context argument, the offline form forwards exactly one
    // fixed --help argument to the canonical package advice sibling.
    let help_marker = fixture.root.join("advice-help-argv.marker");
    write_argv_recording_sibling(&fixture.pkg_bin_dir, "threadlane-advice", &help_marker);

    let out = run_bin_with_path(&fixture.context_bin, &["help", "advice"], hostile_path).await;
    assert!(
        out.status.success(),
        "offline `help advice` failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        out.stderr.is_empty(),
        "offline `help advice` must not write stderr"
    );
    assert_eq!(
        read_argv_marker(&help_marker),
        vec!["1".to_string(), "--help".to_string()],
        "offline `help advice` must forward exactly one fixed --help argument"
    );
    assert!(
        !hostile_marker.exists(),
        "offline `help advice` must not resolve a sibling through PATH"
    );
    assert!(
        !fixture.plans_dir.join("advice-cache").exists(),
        "offline `help advice` must not create advice-cache"
    );
}

#[tokio::test]
async fn advice_forwards_canonical_scope_argv_in_exact_order_without_creating_cache_and_preserves_legacy_families(
) {
    let mut fixture = ContextFixture::start().await;

    let hostile_dir = fixture.root.join("hostile-advice-path-bin");
    fs::create_dir_all(&hostile_dir).expect("create hostile advice PATH directory");
    fs::set_permissions(&hostile_dir, fs::Permissions::from_mode(0o755))
        .expect("chmod hostile advice PATH directory 0755");
    let hostile_marker = fixture.root.join("hostile-advice-child.marker");
    let hostile_script = format!(
        "#!/bin/sh\nprintf 'hostile-path' > \"{}\"\nexit 0\n",
        hostile_marker.display()
    );
    let hostile_advice =
        write_executable_script(&hostile_dir, "threadlane-advice", &hostile_script, 0o755);
    let hostile_path = hostile_dir
        .to_str()
        .expect("utf8 hostile advice PATH directory");

    // Non-operational context metadata naming an advice executable or cache path must never
    // select a child binary or override the context-derived advice scope.
    let mut advice_context = fixture.valid_context_json();
    advice_context["advice_executable"] = json!(hostile_advice);
    advice_context["advice_cache_dir"] = json!("/tmp/ignored-non-operational-cache");
    advice_context["context_id"] = json!("0196ed00-0000-7000-8000-0000000000ff");
    let ctx_path = fixture.write_private_context("advice-forwarding-context.json", &advice_context);
    let ctx_str = ctx_path.to_str().expect("utf8 advice context path");
    let ctx_eq = format!("--context={ctx_str}");

    let advice_marker = fixture.root.join("advice-forward-argv.marker");
    let flow_marker = fixture.root.join("advice-test-flow-argv.marker");
    let session_marker = fixture.root.join("advice-test-session-argv.marker");
    let workflow_marker = fixture.root.join("advice-test-workflow-argv.marker");
    write_argv_recording_sibling(&fixture.pkg_bin_dir, "threadlane-advice", &advice_marker);
    write_argv_recording_sibling(&fixture.pkg_bin_dir, "threadlane-flow", &flow_marker);
    write_argv_recording_sibling(&fixture.pkg_bin_dir, "threadlane-session", &session_marker);
    write_argv_recording_sibling(
        &fixture.pkg_bin_dir,
        "threadlane-workflow",
        &workflow_marker,
    );

    let req_path = fixture.root.join("advice-request.json");
    let req_str = req_path.to_str().expect("utf8 advice request path");
    let derived_cache_dir = fixture.plans_dir.join("advice-cache");
    let expected_scope = advice_scope_args(&fixture);

    // 1. `advice preview --request <ABS>`
    let preview_inner = ["preview", "--request", req_str];
    let mut preview_args = vec!["--context", ctx_str, "advice"];
    preview_args.extend(preview_inner);
    let preview_out = run_bin_with_path(&fixture.context_bin, &preview_args, hostile_path).await;
    assert!(
        preview_out.status.success(),
        "advice preview forwarding failed: {}",
        String::from_utf8_lossy(&preview_out.stderr)
    );
    assert!(
        preview_out.stderr.is_empty(),
        "advice preview forwarding must not write stderr"
    );
    assert!(
        !hostile_marker.exists(),
        "advice forwarding must not resolve a sibling through PATH or context metadata"
    );
    assert_eq!(
        read_argv_marker(&advice_marker),
        expected_recorded_argv(&expected_scope, &preview_inner),
        "advice preview must prepend --registry-dir, --cache-dir, --project-root, --context-id in exact order"
    );
    assert!(
        !derived_cache_dir.exists(),
        "threadlane-context must never create plans_dir/advice-cache"
    );

    // 2. `advice [--cache-ttl-seconds N] classify --request <ABS> --refresh` via `--context=<ABS>`
    let classify_inner = [
        "--cache-ttl-seconds",
        "1800",
        "classify",
        "--request",
        req_str,
        "--refresh",
    ];
    let mut classify_args = vec![ctx_eq.as_str(), "advice"];
    classify_args.extend(classify_inner);
    let classify_out = run_bin_with_path(&fixture.context_bin, &classify_args, hostile_path).await;
    assert!(
        classify_out.status.success(),
        "advice classify forwarding failed: {}",
        String::from_utf8_lossy(&classify_out.stderr)
    );
    assert!(classify_out.stderr.is_empty());
    assert_eq!(
        read_argv_marker(&advice_marker),
        expected_recorded_argv(&expected_scope, &classify_inner),
        "advice classify must prepend the exact four scope flags before inner args"
    );
    assert!(
        !derived_cache_dir.exists(),
        "threadlane-context router must not create advice-cache even on classify forwarding"
    );

    // 3. `advice models --tool-id jev-api`
    let models_inner = ["models", "--tool-id", "jev-api"];
    let mut models_args = vec!["--context", ctx_str, "advice"];
    models_args.extend(models_inner);
    let models_out = run_bin_with_path(&fixture.context_bin, &models_args, hostile_path).await;
    assert!(
        models_out.status.success(),
        "advice models forwarding failed: {}",
        String::from_utf8_lossy(&models_out.stderr)
    );
    assert!(models_out.stderr.is_empty());
    assert_eq!(
        read_argv_marker(&advice_marker),
        expected_recorded_argv(&expected_scope, &models_inner),
        "advice models must prepend the exact four scope flags before inner args"
    );
    assert!(!derived_cache_dir.exists());

    // 4. Legacy families (`workflow`, `session`, `flow`) preserve their exact scope argv.
    let legacy_inner = ["fixture-command", "--limit", "5"];

    let mut flow_args = vec!["--context", ctx_str, "flow"];
    flow_args.extend(legacy_inner);
    let flow_out = run_bin(&fixture.context_bin, &flow_args).await;
    assert!(flow_out.status.success());
    assert_eq!(
        read_argv_marker(&flow_marker),
        expected_recorded_argv(&flow_scope_args(&fixture), &legacy_inner)
    );

    let mut session_args = vec!["--context", ctx_str, "session"];
    session_args.extend(legacy_inner);
    let session_out = run_bin(&fixture.context_bin, &session_args).await;
    assert!(session_out.status.success());
    assert_eq!(
        read_argv_marker(&session_marker),
        expected_recorded_argv(&five_store_scope_args(&fixture), &legacy_inner)
    );

    let mut workflow_args = vec!["--context", ctx_str, "workflow"];
    workflow_args.extend(legacy_inner);
    let workflow_out = run_bin(&fixture.context_bin, &workflow_args).await;
    assert!(workflow_out.status.success());
    assert_eq!(
        read_argv_marker(&workflow_marker),
        expected_recorded_argv(&workflow_scope_args(&fixture), &legacy_inner)
    );

    fixture.shutdown().await;
}

#[tokio::test]
async fn missing_or_unsafe_advice_sibling_and_binding_or_child_failures_fail_closed() {
    let mut fixture = ContextFixture::start().await;
    let ctx_path =
        fixture.write_private_context("advice-sibling-context.json", &fixture.valid_context_json());
    let ctx_str = ctx_path.to_str().unwrap();
    let advice_sibling = fixture.pkg_bin_dir.join("threadlane-advice");

    let hostile_dir = fixture.root.join("hostile-advice-sibling-path-bin");
    fs::create_dir_all(&hostile_dir).expect("create hostile advice sibling PATH directory");
    fs::set_permissions(&hostile_dir, fs::Permissions::from_mode(0o755))
        .expect("chmod hostile advice sibling PATH directory 0755");
    let hostile_marker = fixture.root.join("hostile-advice-sibling.marker");
    let hostile_script = format!(
        "#!/bin/sh\nprintf 'hostile-path' > \"{}\"\nexit 0\n",
        hostile_marker.display()
    );
    write_executable_script(&hostile_dir, "threadlane-advice", &hostile_script, 0o755);
    let hostile_path = hostile_dir
        .to_str()
        .expect("utf8 hostile advice sibling PATH directory");

    // 1. A missing package advice sibling is refused without falling back to PATH.
    let missing = run_bin_with_path(
        &fixture.context_bin,
        &[
            "--context",
            ctx_str,
            "advice",
            "models",
            "--tool-id",
            "jev-api",
        ],
        hostile_path,
    )
    .await;
    assert_error_code(&missing, "not_configured");
    assert!(
        !hostile_marker.exists(),
        "missing package advice sibling must not fall back to PATH"
    );

    // 2. A symlink advice sibling is refused for operational forwarding and offline help.
    symlink(
        Path::new(env!("CARGO_BIN_EXE_threadlane-workflow")),
        &advice_sibling,
    )
    .expect("symlink threadlane-advice sibling");
    let symlink_out = run_bin(
        &fixture.context_bin,
        &[
            "--context",
            ctx_str,
            "advice",
            "models",
            "--tool-id",
            "jev-api",
        ],
    )
    .await;
    assert_error_code(&symlink_out, "security_violation");
    let offline_symlink = run_bin(&fixture.context_bin, &["help", "advice"]).await;
    assert_error_code(&offline_symlink, "security_violation");
    fs::remove_file(&advice_sibling).expect("remove symlink advice sibling");

    // 3. A directory in place of the advice sibling is refused.
    fs::create_dir(&advice_sibling).expect("create directory advice sibling");
    let dir_out = run_bin(
        &fixture.context_bin,
        &[
            "--context",
            ctx_str,
            "advice",
            "models",
            "--tool-id",
            "jev-api",
        ],
    )
    .await;
    assert_error_code(&dir_out, "security_violation");
    fs::remove_dir(&advice_sibling).expect("remove directory advice sibling");

    // 4. A non-executable regular file advice sibling is refused.
    write_executable_script(
        &fixture.pkg_bin_dir,
        "threadlane-advice",
        "#!/bin/sh\nexit 0\n",
        0o600,
    );
    let non_exec_out = run_bin(
        &fixture.context_bin,
        &[
            "--context",
            ctx_str,
            "advice",
            "models",
            "--tool-id",
            "jev-api",
        ],
    )
    .await;
    assert_error_code(&non_exec_out, "security_violation");
    fs::remove_file(&advice_sibling).expect("remove non-exec advice sibling");

    // 5. Child non-zero exit code and stderr output are preserved transparently without replay.
    let count_file = fixture.root.join("advice-child-count.txt");
    let failing_child_script = format!(
        "#!/bin/sh\nprintf 'x' >> \"{}\"\nprintf '{{\"code\":\"advice_refusal\",\"message\":\"advice child refused request\"}}\\n' >&2\nexit 23\n",
        count_file.display()
    );
    write_executable_script(
        &fixture.pkg_bin_dir,
        "threadlane-advice",
        &failing_child_script,
        0o755,
    );
    let child_fail_out = run_bin(
        &fixture.context_bin,
        &[
            "--context",
            ctx_str,
            "advice",
            "classify",
            "--request",
            "/tmp/req.json",
        ],
    )
    .await;
    assert_eq!(child_fail_out.status.code(), Some(23));
    let child_err = assert_error_code(&child_fail_out, "advice_refusal");
    assert_eq!(child_err["message"], "advice child refused request");
    assert_eq!(
        fs::read_to_string(&count_file)
            .expect("read advice count")
            .len(),
        1,
        "failing advice child must be invoked exactly once without replay"
    );
    fs::remove_file(&advice_sibling).expect("remove failing advice sibling");

    // 6. Signal termination after child spawn emits `outcome_unknown` and never replays.
    let sig_count_file = fixture.root.join("advice-sig-child-count.txt");
    let signal_child_script = format!(
        "#!/bin/sh\nprintf 'x' >> \"{}\"\nkill -TERM $$\n",
        sig_count_file.display()
    );
    write_executable_script(
        &fixture.pkg_bin_dir,
        "threadlane-advice",
        &signal_child_script,
        0o755,
    );
    let sig_out = run_bin(
        &fixture.context_bin,
        &[
            "--context",
            ctx_str,
            "advice",
            "classify",
            "--request",
            "/tmp/req.json",
        ],
    )
    .await;
    assert_error_code(&sig_out, "outcome_unknown");
    assert_eq!(
        fs::read_to_string(&sig_count_file)
            .expect("read advice sig count")
            .len(),
        1,
        "signal-terminated advice child must be invoked exactly once without replay"
    );
    fs::remove_file(&advice_sibling).expect("remove signal advice sibling");

    // 7. Unknown binding, foreign thread, and offline supervisor fail before spawning advice child.
    let spawn_marker = fixture.root.join("advice-unverified-binding.marker");
    let spawn_script = format!(
        "#!/bin/sh\nprintf 'spawned' > \"{}\"\nexit 0\n",
        spawn_marker.display()
    );
    write_executable_script(
        &fixture.pkg_bin_dir,
        "threadlane-advice",
        &spawn_script,
        0o755,
    );

    let mut unknown_binding_json = fixture.valid_context_json();
    unknown_binding_json["binding_id"] = json!("bnd_missing_advice_999");
    let unknown_ctx =
        fixture.write_private_context("advice-unknown-binding.json", &unknown_binding_json);
    let unknown_out = run_bin(
        &fixture.context_bin,
        &[
            "--context",
            unknown_ctx.to_str().unwrap(),
            "advice",
            "models",
            "--tool-id",
            "jev-api",
        ],
    )
    .await;
    assert_error_code(&unknown_out, "binding_not_found");
    assert!(!spawn_marker.exists());

    let mut foreign_thread_json = fixture.valid_context_json();
    foreign_thread_json["thread_uuid"] = json!(OTHER_THREAD_UUID);
    let foreign_ctx =
        fixture.write_private_context("advice-foreign-thread.json", &foreign_thread_json);
    let foreign_out = run_bin(
        &fixture.context_bin,
        &[
            "--context",
            foreign_ctx.to_str().unwrap(),
            "advice",
            "models",
            "--tool-id",
            "jev-api",
        ],
    )
    .await;
    assert_error_code(&foreign_out, "thread_uuid_mismatch");
    assert!(!spawn_marker.exists());

    fixture.shutdown().await;
    let offline_out = run_bin(
        &fixture.context_bin,
        &[
            "--context",
            ctx_str,
            "advice",
            "models",
            "--tool-id",
            "jev-api",
        ],
    )
    .await;
    assert_error_code(&offline_out, "transport_unavailable");
    assert!(!spawn_marker.exists());
    assert!(!fixture.plans_dir.join("advice-cache").exists());
}

#[tokio::test]
async fn advice_rejects_invalid_extra_and_protected_arguments_before_child_invocation() {
    let mut fixture = ContextFixture::start().await;
    let ctx_path = fixture.write_private_context(
        "advice-protected-context.json",
        &fixture.valid_context_json(),
    );
    let ctx_str = ctx_path.to_str().unwrap();
    let ctx_eq = format!("--context={ctx_str}");

    let marker_file = fixture.root.join("advice-protected-child.marker");
    let marker_script = format!(
        "#!/bin/sh\nprintf 'spawned' > \"{}\"\nexit 0\n",
        marker_file.display()
    );
    for sibling in [
        "threadlane-advice",
        "threadlane-workflow",
        "threadlane-session",
        "threadlane-flow",
    ] {
        write_executable_script(&fixture.pkg_bin_dir, sibling, &marker_script, 0o755);
    }

    let invalid_cases: &[&[&str]] = &[
        // Missing operational inner arguments.
        &["--context", ctx_str, "advice"],
        // Offline help accepts no extra argument, context, or protected override.
        &["help", "advice", "extra"],
        &["help", "advice", SECRET_SENTINEL],
        &["help", "advice", "--help"],
        &["help", "advice", "--cache-dir=/tmp/other"],
        &["help", "advice", "--context-id=other"],
        &["help", "advice=ignored"],
        &["--context", ctx_str, "help", "advice"],
        &[ctx_eq.as_str(), "help", "advice"],
        // A protected override cannot occupy the family position.
        &[
            "--context",
            ctx_str,
            "--cache-dir=/tmp/other",
            "advice",
            "preview",
            "--request",
            "/tmp/req.json",
        ],
        &[
            "--context",
            ctx_str,
            "--context-id=other",
            "advice",
            "preview",
            "--request",
            "/tmp/req.json",
        ],
        // Protected scope, cache-dir, context-id, project-root, store, binding, thread, and
        // context overrides are refused in separate, equals, underscore, and repeated forms.
        &[
            "--context",
            ctx_str,
            "advice",
            "preview",
            "--request",
            "/tmp/req.json",
            "--registry-dir",
            "/tmp/other",
        ],
        &[
            "--context",
            ctx_str,
            "advice",
            "preview",
            "--request",
            "/tmp/req.json",
            "--registry-dir=/tmp/other",
        ],
        &[
            "--context",
            ctx_str,
            "advice",
            "preview",
            "--request",
            "/tmp/req.json",
            "--registry_dir=/tmp/other",
        ],
        &[
            "--context",
            ctx_str,
            "advice",
            "preview",
            "--request",
            "/tmp/req.json",
            "--cache-dir",
            "/tmp/other",
        ],
        &[
            "--context",
            ctx_str,
            "advice",
            "preview",
            "--request",
            "/tmp/req.json",
            "--cache-dir=/tmp/other",
        ],
        &[
            "--context",
            ctx_str,
            "advice",
            "preview",
            "--request",
            "/tmp/req.json",
            "--cache_dir",
            "/tmp/other",
        ],
        &[
            "--context",
            ctx_str,
            "advice",
            "preview",
            "--request",
            "/tmp/req.json",
            "--cache_dir=/tmp/other",
        ],
        &[
            "--context",
            ctx_str,
            "advice",
            "preview",
            "--request",
            "/tmp/req.json",
            "--cache-dir=/tmp/other",
            "--cache-dir=/tmp/other",
        ],
        &[
            "--context",
            ctx_str,
            "advice",
            "preview",
            "--request",
            "/tmp/req.json",
            "--project-root",
            "/tmp/other",
        ],
        &[
            "--context",
            ctx_str,
            "advice",
            "preview",
            "--request",
            "/tmp/req.json",
            "--project-root=/tmp/other",
        ],
        &[
            "--context",
            ctx_str,
            "advice",
            "preview",
            "--request",
            "/tmp/req.json",
            "--project_root=/tmp/other",
        ],
        &[
            "--context",
            ctx_str,
            "advice",
            "preview",
            "--request",
            "/tmp/req.json",
            "--context-id",
            OTHER_THREAD_UUID,
        ],
        &[
            "--context",
            ctx_str,
            "advice",
            "preview",
            "--request",
            "/tmp/req.json",
            &format!("--context-id={OTHER_THREAD_UUID}"),
        ],
        &[
            "--context",
            ctx_str,
            "advice",
            "preview",
            "--request",
            "/tmp/req.json",
            "--context_id",
            OTHER_THREAD_UUID,
        ],
        &[
            "--context",
            ctx_str,
            "advice",
            "preview",
            "--request",
            "/tmp/req.json",
            &format!("--context_id={OTHER_THREAD_UUID}"),
        ],
        &[
            "--context",
            ctx_str,
            "advice",
            "preview",
            "--request",
            "/tmp/req.json",
            "--context-id",
            OTHER_THREAD_UUID,
            "--context-id",
            OTHER_THREAD_UUID,
        ],
        &[
            "--context",
            ctx_str,
            "advice",
            "preview",
            "--request",
            "/tmp/req.json",
            "--state-dir",
            "/tmp/other",
        ],
        &[
            "--context",
            ctx_str,
            "advice",
            "preview",
            "--request",
            "/tmp/req.json",
            "--state-dir=/tmp/other",
        ],
        &[
            "--context",
            ctx_str,
            "advice",
            "preview",
            "--request",
            "/tmp/req.json",
            "--plans-dir",
            "/tmp/other",
        ],
        &[
            "--context",
            ctx_str,
            "advice",
            "preview",
            "--request",
            "/tmp/req.json",
            "--plans-dir=/tmp/other",
        ],
        &[
            "--context",
            ctx_str,
            "advice",
            "preview",
            "--request",
            "/tmp/req.json",
            "--authority-dir",
            "/tmp/other",
        ],
        &[
            "--context",
            ctx_str,
            "advice",
            "preview",
            "--request",
            "/tmp/req.json",
            "--sessions-dir=/tmp/other",
        ],
        &[
            "--context",
            ctx_str,
            "advice",
            "preview",
            "--request",
            "/tmp/req.json",
            "--binding",
            "bnd_override",
        ],
        &[
            "--context",
            ctx_str,
            "advice",
            "preview",
            "--request",
            "/tmp/req.json",
            "--binding=bnd_override",
        ],
        &[
            "--context",
            ctx_str,
            "advice",
            "preview",
            "--request",
            "/tmp/req.json",
            "--thread",
            OTHER_THREAD_UUID,
        ],
        &[
            "--context",
            ctx_str,
            "advice",
            "preview",
            "--request",
            "/tmp/req.json",
            &format!("--thread={OTHER_THREAD_UUID}"),
        ],
        &[
            "--context",
            ctx_str,
            "advice",
            "preview",
            "--request",
            "/tmp/req.json",
            "--context",
            ctx_str,
        ],
        &[
            "--context",
            ctx_str,
            "advice",
            "preview",
            "--request",
            "/tmp/req.json",
            ctx_eq.as_str(),
        ],
        // Legacy families also reject the new --cache-dir and --context-id protected flags.
        &[
            "--context",
            ctx_str,
            "workflow",
            "catalog",
            "--cache-dir=/tmp/other",
        ],
        &[
            "--context",
            ctx_str,
            "session",
            "session",
            "--context-id",
            OTHER_THREAD_UUID,
        ],
        &[
            "--context",
            ctx_str,
            "flow",
            "fixture-command",
            "--context-id=other",
        ],
    ];

    for case in invalid_cases {
        let out = run_bin(&fixture.context_bin, case).await;
        assert_error_code(&out, "invalid_input");
        assert!(
            !marker_file.exists(),
            "invalid advice/protected arguments must never spawn the child executable: {case:?}"
        );
        assert!(
            !fixture.plans_dir.join("advice-cache").exists(),
            "invalid advice/protected arguments must never create advice-cache: {case:?}"
        );
    }

    // A valid advice invocation proves the marker sibling is live and the refusals above were
    // caused by argument validation rather than a broken fixture.
    let valid_out = run_bin(
        &fixture.context_bin,
        &[
            "--context",
            ctx_str,
            "advice",
            "preview",
            "--request",
            "/tmp/req.json",
        ],
    )
    .await;
    assert!(
        valid_out.status.success(),
        "valid advice forwarding failed: {}",
        String::from_utf8_lossy(&valid_out.stderr)
    );
    assert!(
        marker_file.exists(),
        "valid advice forwarding must invoke the package advice sibling"
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

#[tokio::test]
async fn offline_approve_help_and_approve_forwarding_with_scope_and_protected_flag_refusals() {
    let mut fixture = ContextFixture::start().await;
    let ctx_path = fixture.write_private_context(
        "approve-forward-context.json",
        &fixture.valid_context_json(),
    );
    let ctx_str = ctx_path.to_str().expect("utf8 context path");

    let hostile_dir = fixture.root.join("hostile-approve-path-bin");
    fs::create_dir_all(&hostile_dir).expect("create hostile approve PATH directory");
    fs::set_permissions(&hostile_dir, fs::Permissions::from_mode(0o755))
        .expect("chmod hostile approve PATH directory 0755");
    let hostile_marker = fixture.root.join("hostile-approve-child.marker");
    let hostile_script = format!(
        "#!/bin/sh\nprintf 'hostile-path' > \"{}\"\nexit 0\n",
        hostile_marker.display()
    );
    write_executable_script(&hostile_dir, "threadlane-approve", &hostile_script, 0o755);
    let hostile_path = hostile_dir
        .to_str()
        .expect("utf8 hostile approve PATH directory");

    // Missing package `threadlane-approve` sibling fails closed with `not_configured` without PATH fallback.
    let missing_help =
        run_bin_with_path(&fixture.context_bin, &["help", "approve"], hostile_path).await;
    assert_error_code(&missing_help, "not_configured");
    assert!(!hostile_marker.exists());

    let approve_marker = fixture.root.join("approve-forward-argv.marker");
    write_argv_recording_sibling(&fixture.pkg_bin_dir, "threadlane-approve", &approve_marker);

    let inner = ["prepare", "--request", "/tmp/approve-intent.json"];
    let mut approve_args = vec!["--context", ctx_str, "approve"];
    approve_args.extend(inner);
    let forward_out = run_bin_with_path(&fixture.context_bin, &approve_args, hostile_path).await;
    assert!(
        forward_out.status.success(),
        "approve forwarding failed: {}",
        String::from_utf8_lossy(&forward_out.stderr)
    );
    assert!(forward_out.stderr.is_empty());
    assert!(!hostile_marker.exists());

    let expected_approve_scope = vec![
        "--state-dir".to_string(),
        fixture.state_dir.to_string_lossy().into_owned(),
        "--registry-dir".to_string(),
        fixture.registry_dir.to_string_lossy().into_owned(),
        "--plans-dir".to_string(),
        fixture.plans_dir.to_string_lossy().into_owned(),
        "--authority-dir".to_string(),
        fixture.authority_dir.to_string_lossy().into_owned(),
        "--binding".to_string(),
        fixture.binding_id.clone(),
        "--thread".to_string(),
        THREAD_UUID.to_string(),
        "--project-root".to_string(),
        fixture.project_root.to_string_lossy().into_owned(),
    ];
    assert_eq!(
        read_argv_marker(&approve_marker),
        expected_recorded_argv(&expected_approve_scope, &inner)
    );

    // Protected overrides on `approve` are rejected before child invocation.
    let _ = fs::remove_file(&approve_marker);
    for bad_override in [
        "--state-dir=/tmp/other",
        "--authority-dir=/tmp/other",
        "--binding=bnd_other",
        &format!("--thread={OTHER_THREAD_UUID}"),
        "--project-root=/tmp/other",
    ] {
        let out = run_bin(
            &fixture.context_bin,
            &[
                "--context",
                ctx_str,
                "approve",
                "prepare",
                "--request",
                "/tmp/approve-intent.json",
                bad_override,
            ],
        )
        .await;
        assert_error_code(&out, "invalid_input");
        assert!(
            !approve_marker.exists(),
            "protected override {bad_override} must not invoke threadlane-approve"
        );
    }

    // Offline `help approve` succeeds after supervisor shutdown without reading context.
    fixture.shutdown().await;
    let help_out =
        run_bin_with_path(&fixture.context_bin, &["help", "approve"], hostile_path).await;
    assert!(help_out.status.success());
    assert!(help_out.stderr.is_empty());
    assert_eq!(
        read_argv_marker(&approve_marker),
        vec!["1".to_string(), "--help".to_string()]
    );
    assert!(!hostile_marker.exists());
}

#[tokio::test]
async fn exported_operator_context_admits_planning_grant_and_scope_approval_with_strict_scope_guards(
) {
    use threadlane_native::context_export::{
        export_verified_operator_context, OperatorContextExportRequest, OperatorContextServerConfig,
    };

    let mut fixture = ContextFixture::start().await;

    // Stage the real compiled `threadlane-flow` and `threadlane-approve` sibling binaries.
    copy_executable_atomic(
        Path::new(env!("CARGO_BIN_EXE_threadlane-flow")),
        &fixture.pkg_bin_dir,
        "threadlane-flow",
    );
    copy_executable_atomic(
        Path::new(env!("CARGO_BIN_EXE_threadlane-approve")),
        &fixture.pkg_bin_dir,
        "threadlane-approve",
    );

    // Create a second independent chat binding in the live supervisor.
    let sup_client =
        SupervisorClient::from_state_dir(&fixture.state_dir).expect("connect supervisor client");
    let binding_b = sup_client
        .create_binding(OTHER_THREAD_UUID, "second-verified-chat")
        .await
        .expect("create second chat binding");

    let other_project_root = fixture.root.join("other-project-repo");
    let _other_commit = init_disposable_repo(&other_project_root);
    fs::set_permissions(&other_project_root, fs::Permissions::from_mode(0o755))
        .expect("chmod other_project_root 0755");

    let pairing_dir = fixture.root.join("pairing-store");
    fs::create_dir_all(&pairing_dir).expect("create pairing_dir");
    fs::set_permissions(&pairing_dir, fs::Permissions::from_mode(0o700))
        .expect("chmod pairing_dir 0700");

    let server_cfg = OperatorContextServerConfig {
        pairing_dir: &pairing_dir,
        state_dir: &fixture.state_dir,
        workflow_stores: Some((&fixture.registry_dir, &fixture.plans_dir)),
        host_stores: Some((&fixture.authority_dir, &fixture.sessions_dir)),
        sibling_anchor_exe: Some(&fixture.context_bin),
    };

    let export_a1 = export_verified_operator_context(&OperatorContextExportRequest {
        binding_id: &fixture.binding_id,
        thread_uuid: THREAD_UUID,
        verified_project_root: &fixture.project_root,
        server_config: server_cfg,
    })
    .expect("export chat A context");
    assert!(!export_a1.reused_existing);

    let export_a2 = export_verified_operator_context(&OperatorContextExportRequest {
        binding_id: &fixture.binding_id,
        thread_uuid: THREAD_UUID,
        verified_project_root: &fixture.project_root,
        server_config: server_cfg,
    })
    .expect("idempotent repeat export chat A context");
    assert!(export_a2.reused_existing);
    assert_eq!(export_a1.context_path, export_a2.context_path);

    let export_b = export_verified_operator_context(&OperatorContextExportRequest {
        binding_id: &binding_b.binding_id,
        thread_uuid: OTHER_THREAD_UUID,
        verified_project_root: &other_project_root,
        server_config: server_cfg,
    })
    .expect("export chat B context");
    assert_ne!(export_a1.context_path, export_b.context_path);

    let ctx_a_str = export_a1.context_path.to_str().expect("utf8 ctx_a");

    // 1. Prepare and record a planning grant via the exported context and `flow grant-prepare` / `grant-record`.
    let base_commit = resolve_exact_commit_sha(
        &fixture.project_root,
        "HEAD",
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .expect("resolve HEAD commit");
    let registry = RegistryStore::open(&fixture.registry_dir).expect("open registry");
    let cur_snap = registry.snapshot().expect("registry snapshot");
    let planning_profile_id = "profile-planning-grant";
    let reg_snap = registry
        .apply(
            cur_snap.revision,
            RegistryChange::UpsertProfile {
                profile: WorkflowProfile {
                    decision_advisor: None,
                    id: planning_profile_id.to_string(),
                    name: "Planning Grant Profile".to_string(),
                    assignments: vec![
                        RoleAssignment {
                            role: WorkflowRole::Orchestrator,
                            difficulty: None,
                            choice: ModelChoice {
                                model_id: "model-deepseek-flash".to_string(),
                                effort: Some("max".to_string()),
                            },
                        },
                        RoleAssignment {
                            role: WorkflowRole::Coder,
                            difficulty: Some(Difficulty::Easy),
                            choice: ModelChoice {
                                model_id: "model-deepseek-flash".to_string(),
                                effort: Some("max".to_string()),
                            },
                        },
                    ],
                },
            },
        )
        .expect("upsert planning profile");

    let prepare_grant_req = threadlane_native::coworker_grants::PreparePlanningGrantRequest {
        request_key: "req_grant_chat_a_1".to_string(),
        binding_id: fixture.binding_id.clone(),
        thread_uuid: THREAD_UUID.to_string(),
        repository_path: fixture.project_root.clone(),
        base_commit: base_commit.clone(),
        expected_base_tree_sha: None,
        profile_id: planning_profile_id.to_string(),
        expected_registry_revision: Some(reg_snap.revision),
        roles: vec![threadlane_native::profile_flow::RolePreflightRequest {
            role: WorkflowRole::Orchestrator,
            difficulty: None,
        }],
        policy: threadlane_native::profile_flow::ExecutionPolicy {
            schema_version: 1,
            policy_id: "fresh-chat-policy".to_string(),
            allow_rules: vec![threadlane_native::profile_flow::PolicyAllowRule {
                roles: vec![WorkflowRole::Orchestrator],
                tool_id: "tool-opencode".to_string(),
                model_id: "model-deepseek-flash".to_string(),
                efforts: vec!["max".to_string()],
                allow_no_effort: false,
            }],
        },
        capabilities:
            threadlane_native::coworker_workflow::contracts::PlanningReadOnlyCapabilities::strict_read_only(),
        budget: threadlane_native::coworker_workflow::contracts::PlanningCallBudget {
            max_orchestrator_calls: 2,
            max_architect_calls: 0,
            max_total_calls: 2,
            max_timeout_ms_per_call: 120_000,
            max_output_bytes_per_call: 65_536,
        },
        created_at_ms: 1_760_000_000_000,
        expires_at_ms: 1_760_003_600_000,
    };

    let grant_intent_path = fixture.contexts_dir.join("grant-intent.json");
    fs::write(
        &grant_intent_path,
        serde_json::to_vec_pretty(&prepare_grant_req).unwrap(),
    )
    .unwrap();
    fs::set_permissions(&grant_intent_path, fs::Permissions::from_mode(0o600)).unwrap();

    let grant_prep_out = run_bin(
        &export_a1.context_router_executable,
        &[
            "--context",
            ctx_a_str,
            "flow",
            "grant-prepare",
            "--request",
            grant_intent_path.to_str().unwrap(),
        ],
    )
    .await;
    assert!(
        grant_prep_out.status.success(),
        "flow grant-prepare failed: {}",
        String::from_utf8_lossy(&grant_prep_out.stderr)
    );
    let grant_prep_env: Value =
        serde_json::from_slice(&grant_prep_out.stdout).expect("parse grant-prepare JSON");
    let grant_preview: threadlane_native::coworker_grants::PlanningGrantPreview =
        serde_json::from_value(grant_prep_env["data"].clone())
            .expect("decode PlanningGrantPreview");
    let grant_preview_digest = grant_preview.preview_digest.clone();

    let record_grant_req = threadlane_native::coworker_grants::RecordPlanningGrantRequest {
        // Recording confirms the prepared operation's identity as well as its digest.
        request_key: grant_preview.request_key.clone(),
        expected_ledger_revision: None,
        preview: grant_preview,
        confirmed_preview_digest: grant_preview_digest.clone(),
        attestation: threadlane_native::coworker_grants::LocalOperatorGrantAttestation::attested(),
    };
    let grant_req_path = fixture.contexts_dir.join("grant-request.json");
    fs::write(
        &grant_req_path,
        serde_json::to_vec_pretty(&record_grant_req).unwrap(),
    )
    .unwrap();
    fs::set_permissions(&grant_req_path, fs::Permissions::from_mode(0o600)).unwrap();

    // Missing `--operator-approved` on `flow grant-record` must fail with `unauthorized_operator` and record nothing.
    let grant_missing_confirm = run_bin(
        &export_a1.context_router_executable,
        &[
            "--context",
            ctx_a_str,
            "flow",
            "grant-record",
            "--request",
            grant_req_path.to_str().unwrap(),
            "--confirm-preview",
            &grant_preview_digest,
        ],
    )
    .await;
    assert_error_code(&grant_missing_confirm, "unauthorized_operator");

    // Explicit operator confirmation records the planning grant without paid inference.
    let grant_rec_out = run_bin(
        &export_a1.context_router_executable,
        &[
            "--context",
            ctx_a_str,
            "flow",
            "grant-record",
            "--request",
            grant_req_path.to_str().unwrap(),
            "--confirm-preview",
            &grant_preview_digest,
            "--operator-approved",
        ],
    )
    .await;
    assert!(
        grant_rec_out.status.success(),
        "flow grant-record failed: {}",
        String::from_utf8_lossy(&grant_rec_out.stderr)
    );
    let grant_rec_env: Value =
        serde_json::from_slice(&grant_rec_out.stdout).expect("parse grant-record JSON");
    assert!(grant_rec_env["data"]["grant_id"]
        .as_str()
        .is_some_and(|id| id.starts_with("pgr_")));

    // 2. Prepare and record an execution scope approval via `approve prepare` / `approve record`.
    let plans = PlanStore::open(&fixture.plans_dir).expect("open plans");
    let draft2 = plans
        .change(
            0, // Revisions are per plan; this plan has not been created yet.
            PlanChange::SaveDraft {
                plan_id: "plan-admission-fixture".to_string(),
                binding_id: fixture.binding_id.clone(),
                profile_id: planning_profile_id.to_string(),
                profile_revision: reg_snap.revision,
                summary: "Admission fixture plan".to_string(),
                tasks: vec![PlanTask {
                    id: "task-1".to_string(),
                    title: "Admission task".to_string(),
                    instruction: "Run bounded admission verification".to_string(),
                    role: WorkflowRole::Coder,
                    difficulty: Difficulty::Easy,
                    depends_on: Vec::new(),
                    allowed_write_paths: vec!["src/lib.rs".to_string()],
                }],
            },
        )
        .expect("save admission plan draft");
    let plan = plans
        .change(
            draft2.revision,
            PlanChange::Publish {
                plan_id: "plan-admission-fixture".to_string(),
                revision: draft2.revision,
            },
        )
        .expect("publish admission plan");

    let approve_intent_path = fixture.contexts_dir.join("approve-intent.json");
    let approve_intent = json!({
        "binding_id": fixture.binding_id,
        "thread_uuid": THREAD_UUID,
        "plan_id": plan.plan_id,
        "plan_revision": plan.revision,
        "expected_registry_revision": reg_snap.revision,
        "repository_path": fixture.project_root,
        "base_commit": base_commit,
        "approved_task_ids": ["task-1"],
        "approved_write_paths": ["src/lib.rs"],
        "max_parallel": 1,
        "budgets": ExecutionBudgets::default()
    });
    fs::write(
        &approve_intent_path,
        serde_json::to_vec_pretty(&approve_intent).unwrap(),
    )
    .unwrap();
    fs::set_permissions(&approve_intent_path, fs::Permissions::from_mode(0o600)).unwrap();

    // Request-file replacement with chat B's binding_id, chat B's thread_uuid, or another
    // repository_path must be rejected by `threadlane-approve` when routed through chat A's context.
    let mut wrong_binding_intent = approve_intent.clone();
    wrong_binding_intent["binding_id"] = json!(binding_b.binding_id);
    let wrong_binding_path = fixture.contexts_dir.join("approve-wrong-binding.json");
    fs::write(
        &wrong_binding_path,
        serde_json::to_vec_pretty(&wrong_binding_intent).unwrap(),
    )
    .unwrap();
    fs::set_permissions(&wrong_binding_path, fs::Permissions::from_mode(0o600)).unwrap();
    let wrong_binding_out = run_bin(
        &export_a1.context_router_executable,
        &[
            "--context",
            ctx_a_str,
            "approve",
            "prepare",
            "--request",
            wrong_binding_path.to_str().unwrap(),
        ],
    )
    .await;
    assert_error_code(&wrong_binding_out, "binding_mismatch");

    let mut wrong_thread_intent = approve_intent.clone();
    wrong_thread_intent["thread_uuid"] = json!(OTHER_THREAD_UUID);
    let wrong_thread_path = fixture.contexts_dir.join("approve-wrong-thread.json");
    fs::write(
        &wrong_thread_path,
        serde_json::to_vec_pretty(&wrong_thread_intent).unwrap(),
    )
    .unwrap();
    fs::set_permissions(&wrong_thread_path, fs::Permissions::from_mode(0o600)).unwrap();
    let wrong_thread_out = run_bin(
        &export_a1.context_router_executable,
        &[
            "--context",
            ctx_a_str,
            "approve",
            "prepare",
            "--request",
            wrong_thread_path.to_str().unwrap(),
        ],
    )
    .await;
    assert_error_code(&wrong_thread_out, "thread_uuid_mismatch");

    let mut wrong_repo_intent = approve_intent.clone();
    wrong_repo_intent["repository_path"] = json!(other_project_root);
    let wrong_repo_path = fixture.contexts_dir.join("approve-wrong-repo.json");
    fs::write(
        &wrong_repo_path,
        serde_json::to_vec_pretty(&wrong_repo_intent).unwrap(),
    )
    .unwrap();
    fs::set_permissions(&wrong_repo_path, fs::Permissions::from_mode(0o600)).unwrap();
    let wrong_repo_out = run_bin(
        &export_a1.context_router_executable,
        &[
            "--context",
            ctx_a_str,
            "approve",
            "prepare",
            "--request",
            wrong_repo_path.to_str().unwrap(),
        ],
    )
    .await;
    assert_error_code(&wrong_repo_out, "invalid_repository");

    // Valid `approve prepare` through chat A's context succeeds.
    let approve_prep_out = run_bin(
        &export_a1.context_router_executable,
        &[
            "--context",
            ctx_a_str,
            "approve",
            "prepare",
            "--request",
            approve_intent_path.to_str().unwrap(),
        ],
    )
    .await;
    assert!(
        approve_prep_out.status.success(),
        "approve prepare failed: {}",
        String::from_utf8_lossy(&approve_prep_out.stderr)
    );
    let approve_preview: Value =
        serde_json::from_slice(&approve_prep_out.stdout).expect("parse approve preview JSON");
    let approve_subject_digest = approve_preview["subject_digest"]
        .as_str()
        .expect("approve subject_digest")
        .to_string();
    let approve_req_obj = approve_preview["request"].clone();

    let approve_req_path = fixture.contexts_dir.join("approve-request.json");
    fs::write(
        &approve_req_path,
        serde_json::to_vec_pretty(&approve_req_obj).unwrap(),
    )
    .unwrap();
    fs::set_permissions(&approve_req_path, fs::Permissions::from_mode(0o600)).unwrap();

    // Missing `--operator-approved` on `approve record` fails and creates no new approval.
    let approve_missing_confirm = run_bin(
        &export_a1.context_router_executable,
        &[
            "--context",
            ctx_a_str,
            "approve",
            "record",
            "--request",
            approve_req_path.to_str().unwrap(),
            "--confirm-subject",
            &approve_subject_digest,
            "--confirm-thread",
            THREAD_UUID,
        ],
    )
    .await;
    assert_error_code(&approve_missing_confirm, "invalid_input");

    // Explicit operator confirmation records the scope approval.
    let approve_rec_out = run_bin(
        &export_a1.context_router_executable,
        &[
            "--context",
            ctx_a_str,
            "approve",
            "record",
            "--request",
            approve_req_path.to_str().unwrap(),
            "--confirm-subject",
            &approve_subject_digest,
            "--confirm-thread",
            THREAD_UUID,
            "--operator-approved",
        ],
    )
    .await;
    assert!(
        approve_rec_out.status.success(),
        "approve record failed: {}",
        String::from_utf8_lossy(&approve_rec_out.stderr)
    );
    let approve_summary: Value =
        serde_json::from_slice(&approve_rec_out.stdout).expect("parse approve summary JSON");
    let recorded_approval_id = approve_summary["approval_id"]
        .as_str()
        .expect("recorded approval_id");
    assert_ne!(recorded_approval_id, fixture.approval_id);

    fixture.shutdown().await;
}

#[tokio::test]
async fn external_resolution_real_flow_cli_preview_record_inspect_reconcile_and_project_pins() {
    let mut f = ContextFixture::start().await;
    copy_executable_atomic(
        Path::new(env!("CARGO_BIN_EXE_threadlane-flow")),
        &f.pkg_bin_dir,
        "threadlane-flow",
    );

    let context =
        f.write_private_context("external-resolution-context.json", &f.valid_context_json());
    let context = context.to_str().expect("utf8 context path").to_string();
    let approval = AuthorityStore::get_for_operator_readonly_at(&f.authority_dir, &f.approval_id)
        .expect("read the fixture's real approval");
    let request = RecordExternalResolutionRequest {
        binding_id: f.binding_id.clone(),
        thread_uuid: THREAD_UUID.to_string(),
        canonical_repo_root: f.project_root.clone(),
        target: ExternalResolutionTarget::LegacyPlan {
            plan_id: approval.plan_id.clone(),
            plan_revision: approval.plan_revision,
        },
        scope: ExternalResolutionScope::FullJob,
        expected_ledger_revision: None,
        expected_job_revision: None,
        expected_runtime_revision: None,
        request_key: "req_external_resolution_cli_real_01".to_string(),
        created_at_ms: 1_700_000_200_000,
        explanation: "Real context-to-flow external resolution CLI fixture.".to_string(),
        evidence_refs: Vec::new(),
    };
    let request_path = f.write_private_context(
        "external-resolution-request.json",
        &serde_json::to_value(&request).expect("serialize external resolution request"),
    );
    let request_path = request_path
        .to_str()
        .expect("utf8 request path")
        .to_string();

    let head_before = fs::read_to_string(f.project_root.join(".git/HEAD")).expect("read HEAD");
    let lib_before = fs::read(f.project_root.join("src/lib.rs")).expect("read fixture source");
    let plans_before = snapshot_tree(&f.plans_dir);
    let sessions_before = snapshot_tree(&f.sessions_dir);
    let authority_before = snapshot_tree(&f.authority_dir);
    let workflow = CoworkerWorkflowStore::new(&f.plans_dir).expect("open workflow store");
    let ledger_path = workflow
        .ledger_path_for_binding(&f.binding_id)
        .expect("workflow ledger path");
    assert!(!ledger_path.exists());

    // Real preview through the context router, a real flow process, the private stores and the
    // inherited observation environment. Preview must remain pure and produce the operator digest.
    let preview_out = run_context_flow(
        &f,
        &context,
        &[
            "external-resolution",
            "preview",
            "--request",
            request_path.as_str(),
        ],
    )
    .await;
    let preview = parse_flow_data(&preview_out);
    assert_eq!(preview["binding_id"], f.binding_id);
    assert_eq!(preview["thread_uuid"], THREAD_UUID);
    assert_eq!(preview["target"]["kind"], "legacy_plan");
    assert_eq!(preview["scope"]["kind"], "full_job");
    assert_eq!(
        preview["project"]["canonical_repo_root"],
        json!(f.project_root)
    );
    let preview_digest = preview["preview_digest"]
        .as_str()
        .expect("preview digest")
        .to_string();
    assert_eq!(preview_digest.len(), 64);
    assert_eq!(snapshot_tree(&f.plans_dir), plans_before);
    assert_eq!(snapshot_tree(&f.sessions_dir), sessions_before);
    assert_eq!(snapshot_tree(&f.authority_dir), authority_before);
    assert!(!ledger_path.exists());
    assert_no_host_repo_or_journal_effects(&f, &head_before, &lib_before);

    // Refusals that must be decided before the flow child acquires any shared or exclusive lock.
    let missing_operator = run_context_flow(
        &f,
        &context,
        &[
            "external-resolution",
            "record",
            "--request",
            request_path.as_str(),
            "--confirm-thread",
            THREAD_UUID,
            "--confirm-preview",
            preview_digest.as_str(),
        ],
    )
    .await;
    assert_error_code(&missing_operator, "unauthorized_operator");

    let missing_thread = run_context_flow(
        &f,
        &context,
        &[
            "external-resolution",
            "record",
            "--request",
            request_path.as_str(),
            "--confirm-preview",
            preview_digest.as_str(),
            "--operator-approved",
        ],
    )
    .await;
    assert_error_code(&missing_thread, "invalid_input");

    let wrong_thread = run_context_flow(
        &f,
        &context,
        &[
            "external-resolution",
            "record",
            "--request",
            request_path.as_str(),
            "--confirm-thread",
            OTHER_THREAD_UUID,
            "--confirm-preview",
            preview_digest.as_str(),
            "--operator-approved",
        ],
    )
    .await;
    assert_error_code(&wrong_thread, "scope_mismatch");

    let missing_preview = run_context_flow(
        &f,
        &context,
        &[
            "external-resolution",
            "record",
            "--request",
            request_path.as_str(),
            "--confirm-thread",
            THREAD_UUID,
            "--operator-approved",
        ],
    )
    .await;
    assert_error_code(&missing_preview, "invalid_input");

    // JSON self-attestation cannot replace the independent CLI flags.
    let mut self_attested =
        serde_json::to_value(&request).expect("serialize self-attested request");
    let object = self_attested
        .as_object_mut()
        .expect("request object for self-attestation");
    object.insert("trusted_local_attested".to_string(), json!(true));
    object.insert("operator_approved".to_string(), json!(true));
    let self_attested_path =
        f.write_private_context("external-resolution-self-attested.json", &self_attested);
    let self_attested_path = self_attested_path
        .to_str()
        .expect("utf8 self-attested request path")
        .to_string();
    let self_attested_out = run_context_flow(
        &f,
        &context,
        &[
            "external-resolution",
            "record",
            "--request",
            self_attested_path.as_str(),
            "--confirm-thread",
            THREAD_UUID,
            "--confirm-preview",
            preview_digest.as_str(),
            "--operator-approved",
        ],
    )
    .await;
    assert_error_code(&self_attested_out, "invalid_input");

    let protected_override = run_context_flow(
        &f,
        &context,
        &[
            "external-resolution",
            "preview",
            "--request",
            request_path.as_str(),
            "--project-root=/tmp/forged",
        ],
    )
    .await;
    assert_error_code(&protected_override, "invalid_input");

    assert_eq!(snapshot_tree(&f.plans_dir), plans_before);
    assert_eq!(snapshot_tree(&f.sessions_dir), sessions_before);
    assert_eq!(snapshot_tree(&f.authority_dir), authority_before);
    assert!(!ledger_path.exists());
    assert_no_host_repo_or_journal_effects(&f, &head_before, &lib_before);

    // Record exactly once with the real preview digest and explicit local consent.
    let record_inner = [
        "external-resolution",
        "record",
        "--request",
        request_path.as_str(),
        "--confirm-thread",
        THREAD_UUID,
        "--confirm-preview",
        preview_digest.as_str(),
        "--operator-approved",
    ];
    let record_out = run_context_flow(&f, &context, &record_inner).await;
    let receipt = parse_flow_data(&record_out);
    assert_eq!(receipt["binding_id"], f.binding_id);
    assert_eq!(receipt["thread_uuid"], THREAD_UUID);
    assert_eq!(receipt["request_key"], request.request_key);
    assert_eq!(receipt["preview_digest"], preview_digest);
    let receipt_digest = receipt["receipt_digest"]
        .as_str()
        .expect("receipt digest")
        .to_string();
    assert_eq!(receipt_digest.len(), 64);
    let plans_after_record = snapshot_tree(&f.plans_dir);
    let sessions_after_record = snapshot_tree(&f.sessions_dir);
    let ledger_bytes_after_record = fs::read(&ledger_path).expect("read ledger after record");
    let ledger_after_record = workflow
        .read_binding_ledger(&f.binding_id)
        .expect("read binding ledger after record");
    assert_eq!(ledger_after_record.external_resolutions.len(), 1);
    assert_eq!(
        ledger_after_record.external_resolutions[0].receipt_digest,
        receipt_digest
    );
    let ledger_revision_after_record = ledger_after_record.ledger_revision;
    assert_no_host_repo_or_journal_effects(&f, &head_before, &lib_before);

    // A fresh CLI process can see the immutable receipt bytes but cannot claim a local fsync
    // witness, so inspect must report PersistenceUncertain and must not rewrite anything.
    let inspect_inner = [
        "external-resolution",
        "inspect",
        "--request-key",
        request.request_key.as_str(),
    ];
    let inspected = parse_flow_data(&run_context_flow(&f, &context, &inspect_inner).await);
    assert_eq!(inspected["receipt"]["receipt_digest"], receipt_digest);
    assert_eq!(inspected["durability"], "persistence_uncertain");
    assert_eq!(inspected["applicability"]["status"], "applicable");
    assert_eq!(
        fs::read(&ledger_path).expect("read ledger after inspect"),
        ledger_bytes_after_record
    );
    assert_eq!(snapshot_tree(&f.plans_dir), plans_after_record);
    assert_eq!(snapshot_tree(&f.sessions_dir), sessions_after_record);
    assert_no_host_repo_or_journal_effects(&f, &head_before, &lib_before);

    // A conflicting duplicate preview digest is refused without touching the immutable ledger.
    let wrong_digest = "b".repeat(64);
    let duplicate_wrong_digest = run_context_flow(
        &f,
        &context,
        &[
            "external-resolution",
            "record",
            "--request",
            request_path.as_str(),
            "--confirm-thread",
            THREAD_UUID,
            "--confirm-preview",
            wrong_digest.as_str(),
            "--operator-approved",
        ],
    )
    .await;
    assert_error_code(&duplicate_wrong_digest, "unauthorized_operator");
    assert_eq!(
        fs::read(&ledger_path).expect("read ledger after wrong digest"),
        ledger_bytes_after_record
    );
    assert_eq!(snapshot_tree(&f.plans_dir), plans_after_record);

    // The exact duplicate must refuse uncertainty rather than pretending the append was durable.
    let duplicate_exact = run_context_flow(&f, &context, &record_inner).await;
    assert_error_code(&duplicate_exact, "persistence_uncertain");
    assert_eq!(
        fs::read(&ledger_path).expect("read ledger after exact duplicate"),
        ledger_bytes_after_record
    );
    assert_eq!(snapshot_tree(&f.plans_dir), plans_after_record);
    assert_eq!(snapshot_tree(&f.sessions_dir), sessions_after_record);
    assert_no_host_repo_or_journal_effects(&f, &head_before, &lib_before);

    // Explicit reconcile of the original key/digest is allowed to fsync the unchanged bytes only.
    // It must not append a receipt, increment the ledger revision, or alter any stored byte.
    let reconcile_out = run_context_flow(
        &f,
        &context,
        &[
            "external-resolution",
            "record",
            "--request",
            request_path.as_str(),
            "--confirm-thread",
            THREAD_UUID,
            "--confirm-preview",
            preview_digest.as_str(),
            "--reconcile-existing",
            "--operator-approved",
        ],
    )
    .await;
    let reconciled = parse_flow_data(&reconcile_out);
    assert_eq!(reconciled["receipt_digest"], receipt_digest);
    assert_eq!(
        fs::read(&ledger_path).expect("read ledger after reconcile"),
        ledger_bytes_after_record
    );
    assert_eq!(
        workflow
            .read_binding_ledger(&f.binding_id)
            .expect("read ledger revision after reconcile")
            .ledger_revision,
        ledger_revision_after_record
    );
    assert_eq!(snapshot_tree(&f.plans_dir), plans_after_record);
    assert_eq!(snapshot_tree(&f.sessions_dir), sessions_after_record);
    assert_no_host_repo_or_journal_effects(&f, &head_before, &lib_before);

    // The fsync witness remains process-local, so the next fresh process is honestly uncertain.
    let reinspected = parse_flow_data(&run_context_flow(&f, &context, &inspect_inner).await);
    assert_eq!(reinspected["receipt"]["receipt_digest"], receipt_digest);
    assert_eq!(reinspected["durability"], "persistence_uncertain");

    // A genuinely canonical second project under the same binding/thread must not surface or
    // reconcile R1's receipt. The wrong root is rejected before any durability operation.
    let other_project = f.root.join("external-resolution-other-project");
    let _other_commit = init_disposable_repo(&other_project);
    fs::set_permissions(&other_project, fs::Permissions::from_mode(0o755))
        .expect("chmod second canonical project 0755");
    let mut other_context_value = f.valid_context_json();
    other_context_value["project_root"] = json!(other_project);
    let other_context_path = f.write_private_context(
        "external-resolution-other-context.json",
        &other_context_value,
    );
    let other_context = other_context_path
        .to_str()
        .expect("utf8 other context path")
        .to_string();

    let other_inspect = run_context_flow(&f, &other_context, &inspect_inner).await;
    assert_error_code(&other_inspect, "scope_mismatch");
    let other_inspect_all =
        run_context_flow(&f, &other_context, &["external-resolution", "inspect"]).await;
    assert_error_code(&other_inspect_all, "scope_mismatch");

    let mut other_request = request.clone();
    other_request.canonical_repo_root = other_project.clone();
    let other_request_path = f.write_private_context(
        "external-resolution-other-request.json",
        &serde_json::to_value(&other_request).expect("serialize other request"),
    );
    let other_request_path = other_request_path
        .to_str()
        .expect("utf8 other request path")
        .to_string();
    let other_reconcile = run_context_flow(
        &f,
        &other_context,
        &[
            "external-resolution",
            "record",
            "--request",
            other_request_path.as_str(),
            "--confirm-thread",
            THREAD_UUID,
            "--confirm-preview",
            preview_digest.as_str(),
            "--reconcile-existing",
            "--operator-approved",
        ],
    )
    .await;
    assert_error_code(&other_reconcile, "scope_mismatch");

    // Direct flow invocations with the pinned R2 root are refused for both inspect entry routes
    // and for reconcile, proving the context router cannot be bypassed.
    let flow_bin = f.pkg_bin_dir.join("threadlane-flow");
    let other_project_str = other_project
        .to_str()
        .expect("utf8 other project")
        .to_string();

    let mut direct_inspect_args = five_store_scope_args(&f);
    direct_inspect_args.extend([
        "--project-root".to_string(),
        other_project_str.clone(),
        "external-resolution".to_string(),
        "inspect".to_string(),
        "--request-key".to_string(),
        request.request_key.clone(),
    ]);
    let direct_inspect_refs: Vec<&str> = direct_inspect_args.iter().map(String::as_str).collect();
    let direct_inspect = run_bin(&flow_bin, &direct_inspect_refs).await;
    assert_error_code(&direct_inspect, "scope_mismatch");

    let mut direct_inspect_all_args = five_store_scope_args(&f);
    direct_inspect_all_args.extend([
        "--project-root".to_string(),
        other_project_str.clone(),
        "external-resolution".to_string(),
        "inspect".to_string(),
    ]);
    let direct_inspect_all_refs: Vec<&str> =
        direct_inspect_all_args.iter().map(String::as_str).collect();
    let direct_inspect_all = run_bin(&flow_bin, &direct_inspect_all_refs).await;
    assert_error_code(&direct_inspect_all, "scope_mismatch");

    let mut direct_reconcile_args = five_store_scope_args(&f);
    direct_reconcile_args.extend([
        "--project-root".to_string(),
        other_project_str,
        "external-resolution".to_string(),
        "record".to_string(),
        "--request".to_string(),
        other_request_path,
        "--confirm-thread".to_string(),
        THREAD_UUID.to_string(),
        "--confirm-preview".to_string(),
        preview_digest.clone(),
        "--reconcile-existing".to_string(),
        "--operator-approved".to_string(),
    ]);
    let direct_reconcile_refs: Vec<&str> =
        direct_reconcile_args.iter().map(String::as_str).collect();
    let direct_reconcile = run_bin(&flow_bin, &direct_reconcile_refs).await;
    assert_error_code(&direct_reconcile, "scope_mismatch");

    assert_eq!(
        fs::read(&ledger_path).expect("read ledger after wrong-project refusals"),
        ledger_bytes_after_record
    );
    assert_eq!(
        workflow
            .read_binding_ledger(&f.binding_id)
            .expect("read ledger after wrong-project refusals")
            .ledger_revision,
        ledger_revision_after_record
    );
    assert_eq!(snapshot_tree(&f.plans_dir), plans_after_record);
    assert_eq!(snapshot_tree(&f.sessions_dir), sessions_after_record);
    assert_no_host_repo_or_journal_effects(&f, &head_before, &lib_before);

    f.shutdown().await;
}

#[tokio::test]
async fn admission_flow_forwarding_preserves_scope_and_refuses_protected_overrides() {
    let mut f = ContextFixture::start().await;
    let context = f.write_private_context("admission-context.json", &f.valid_context_json());
    let marker = f.root.join("admission-argv.marker");
    write_argv_recording_sibling(&f.pkg_bin_dir, "threadlane-flow", &marker);
    for operation in ["preview", "record", "inspect", "reconcile"] {
        let inner = ["coworker-admission", operation, "--job-id", "job_fixture"];
        let mut args = vec!["--context", context.to_str().unwrap(), "flow"];
        args.extend(inner);
        assert!(run_bin(&f.context_bin, &args).await.status.success());
        assert_eq!(
            read_argv_marker(&marker),
            expected_recorded_argv(&flow_scope_args(&f), &inner)
        );
        for protected in [
            "--binding=forged",
            "--thread=forged",
            "--plans-dir=/tmp/forged",
            "--registry-dir=/tmp/forged",
            "--authority-dir=/tmp/forged",
            "--sessions-dir=/tmp/forged",
            "--state-dir=/tmp/forged",
        ] {
            if marker.exists() {
                fs::remove_file(&marker).unwrap();
            }
            let mut hostile = args.clone();
            hostile.push(protected);
            assert!(!run_bin(&f.context_bin, &hostile).await.status.success());
            assert!(!marker.exists());
        }
    }
    f.shutdown().await;
}
