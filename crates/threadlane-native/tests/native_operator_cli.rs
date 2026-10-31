#![cfg(unix)]
//! Behavioral end-to-end tests for the trusted local operator workflow and session CLIs.
//!
//! Every test uses a real disposable `Supervisor`, private registry/plan/authority/session stores,
//! a disposable Git repository, and a fake worker executable that records any invocation. The
//! workflow CLI is exercised through the actual `threadlane-workflow` subprocess; the session CLI
//! is exercised through the actual `threadlane-session` subprocess for static, binding, store, and
//! no-auto-host refusals. No test makes a paid provider call, and no test fabricates a successful
//! host session.

use std::fs;
use std::os::unix::fs::{symlink, PermissionsExt};
use std::path::{Path, PathBuf};
use std::process::Output;
use std::sync::Arc;
use std::time::Duration;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tempfile::TempDir;
use threadlane::{Supervisor, SupervisorClient, SupervisorConfig, SupervisorHandle};
use threadlane_authority::{ApprovalSummary, AuthorityStore, OperatorApprovalRequest};
use threadlane_host::{ApprovedHost, ApprovedHostConfig, HostPhase, HostSessionSummary};
use threadlane_native::{
    CatalogPage, PlanPage, PlanStore, PlanSummary, PlanSummaryPage, PublishedPlanView,
    RegistryStore, SavePlanDraft,
};
use threadlane_workers::{
    resolve_exact_commit_sha, run_sanitized_git, ExecutionBudgets, DEFAULT_GIT_COMMAND_TIMEOUT_MS,
};
use threadlane_workflow::{
    compile_plan_revision, Difficulty, ModelChoice, ModelDefinition, PlanChange, PlanStatus,
    PlanTask, RegistryChange, RoleAssignment, ToolAdapter, ToolDefinition, WorkflowProfile,
    WorkflowRole,
};
use tokio::process::Command;

const THREAD_UUID: &str = "550e8400-e29b-41d4-a716-446655440000";
const OTHER_THREAD_UUID: &str = "6ba7b810-9dad-11d1-80b4-00c04fd430c8";
const FOREIGN_THREAD_UUID: &str = "0196ed00-0000-7000-8000-0000000000b2";
const BINDING_LABEL: &str = "native-operator-cli-chat";
const FOREIGN_BINDING_LABEL: &str = "native-operator-cli-foreign-chat";
const PROFILE_ID: &str = "profile-native-operator";
const WORKFLOW_PLAN_ID: &str = "plan-native-operator-workflow";
const SESSION_PLAN_ID: &str = "plan-native-operator-session";
const FOREIGN_PLAN_ID: &str = "plan-native-operator-foreign";
const SESSION_TASK_ID: &str = "task-session";
const WORKFLOW_TASK_ALPHA: &str = "task-alpha";
const WORKFLOW_TASK_BETA: &str = "task-beta";
const MISSING_APPROVAL_ID: &str = "apr_1234567890abcdef1234567890abcdef";
const MAX_IO_BYTES: usize = 1_048_576;
const COMMAND_TIMEOUT: Duration = Duration::from_secs(30);
const SECRET_SENTINEL: &str = "THREADLANE_NATIVE_OPERATOR_SECRET_SENTINEL_4f2a8c";

#[derive(Debug, Deserialize)]
struct OperatorEnvelope<T> {
    untrusted_data: bool,
    route: String,
    native_per_call_identity_verified: bool,
    binding_id: String,
    data: T,
}

#[derive(Debug, Deserialize)]
struct CliFailure {
    code: String,
    message: String,
}

struct OperatorFixture {
    _tmp: TempDir,
    root: PathBuf,
    repo: PathBuf,
    fake_worker: PathBuf,
    worker_marker: PathBuf,
    state_dir: PathBuf,
    registry_dir: PathBuf,
    plans_dir: PathBuf,
    authority_dir: PathBuf,
    sessions_dir: PathBuf,
    requests_dir: PathBuf,
    plans: Arc<PlanStore>,
    authority: Arc<AuthorityStore>,
    supervisor: Option<SupervisorHandle>,
    binding_id: String,
    foreign_binding_id: String,
    registry_revision: u64,
    approval: ApprovalSummary,
    foreign_approval: ApprovalSummary,
}

impl OperatorFixture {
    async fn start() -> Self {
        let tmp = TempDir::new().expect("create private operator fixture tempdir");
        let root = fs::canonicalize(tmp.path()).expect("canonicalize operator fixture tempdir");
        fs::set_permissions(&root, fs::Permissions::from_mode(0o700))
            .expect("chmod operator fixture root 0700");

        let repo = root.join("repo");
        let base_commit = init_disposable_repo(&repo);

        let bin_dir = root.join("bin");
        create_private_dir(&bin_dir);
        let fake_worker = bin_dir.join("fake-opencode");
        let worker_marker = root.join("worker-invoked");
        write_fake_worker(&fake_worker, &worker_marker);

        let state_dir = root.join("runtime");
        let registry_dir = root.join("registry-store");
        let plans_dir = root.join("plans-store");
        let authority_dir = root.join("authority-store");
        let sessions_dir = root.join("sessions-store");
        let requests_dir = root.join("requests");
        create_private_dir(&sessions_dir);
        create_private_dir(&requests_dir);

        let supervisor = Supervisor::start(SupervisorConfig {
            state_dir: state_dir.clone(),
            tick_interval: Duration::from_millis(50),
            codex: None,
            workers: None,
        })
        .await
        .expect("start disposable operator supervisor");
        let supervisor_client =
            SupervisorClient::from_state_dir(&state_dir).expect("connect operator supervisor");
        let binding = supervisor_client
            .create_binding(THREAD_UUID, BINDING_LABEL)
            .await
            .expect("create control binding for operator CLI");
        let foreign_binding = supervisor_client
            .create_binding(FOREIGN_THREAD_UUID, FOREIGN_BINDING_LABEL)
            .await
            .expect("create foreign control binding for scope isolation");

        let registry = Arc::new(RegistryStore::open(&registry_dir).expect("open registry store"));
        let s1 = registry
            .apply(
                0,
                RegistryChange::UpsertTool {
                    tool: ToolDefinition {
                        id: "tool-opencode".to_string(),
                        label: "OpenCode v2 fixture".to_string(),
                        adapter: ToolAdapter::OpencodeV2,
                        executable_path: Some(fake_worker.clone()),
                        enabled: true,
                    },
                },
            )
            .expect("upsert operator tool");
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
            .expect("upsert operator model");
        let s3 = registry
            .apply(
                s2.revision,
                RegistryChange::UpsertProfile {
                    profile: WorkflowProfile {
                        id: PROFILE_ID.to_string(),
                        name: "Native operator CLI fixture profile".to_string(),
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
            .expect("upsert operator profile");
        let registry_snapshot = registry
            .apply(
                s3.revision,
                RegistryChange::SetDefaultProfile {
                    profile_id: Some(PROFILE_ID.to_string()),
                },
            )
            .expect("pin operator default profile");
        let registry_revision = registry_snapshot.revision;

        let plans = Arc::new(PlanStore::open(&plans_dir).expect("open plan store"));
        let session_draft = plans
            .change(
                0,
                PlanChange::SaveDraft {
                    plan_id: SESSION_PLAN_ID.to_string(),
                    binding_id: binding.binding_id.clone(),
                    profile_id: PROFILE_ID.to_string(),
                    profile_revision: registry_revision,
                    summary: "Native operator session fixture plan".to_string(),
                    tasks: session_plan_tasks(),
                },
            )
            .expect("save session fixture draft");
        let session_published = plans
            .change(
                session_draft.revision,
                PlanChange::Publish {
                    plan_id: SESSION_PLAN_ID.to_string(),
                    revision: session_draft.revision,
                },
            )
            .expect("publish session fixture plan");
        let foreign_draft = plans
            .change(
                0,
                PlanChange::SaveDraft {
                    plan_id: FOREIGN_PLAN_ID.to_string(),
                    binding_id: foreign_binding.binding_id.clone(),
                    profile_id: PROFILE_ID.to_string(),
                    profile_revision: registry_revision,
                    summary: "Foreign binding fixture plan".to_string(),
                    tasks: session_plan_tasks(),
                },
            )
            .expect("save foreign fixture draft");
        let foreign_published = plans
            .change(
                foreign_draft.revision,
                PlanChange::Publish {
                    plan_id: FOREIGN_PLAN_ID.to_string(),
                    revision: foreign_draft.revision,
                },
            )
            .expect("publish foreign fixture plan");

        let session_compiled = compile_plan_revision(&session_published, &registry_snapshot)
            .expect("compile session fixture plan");
        let foreign_compiled = compile_plan_revision(&foreign_published, &registry_snapshot)
            .expect("compile foreign fixture plan");

        let authority = Arc::new(
            AuthorityStore::open(&authority_dir, Arc::clone(&registry), Arc::clone(&plans))
                .expect("open private authority store"),
        );
        let approval = authority
            .approve(OperatorApprovalRequest {
                binding_id: binding.binding_id.clone(),
                thread_uuid: THREAD_UUID.to_string(),
                plan_id: SESSION_PLAN_ID.to_string(),
                plan_revision: session_published.revision,
                expected_registry_revision: registry_revision,
                expected_plan_digest: session_compiled.plan_digest,
                repository_path: repo.clone(),
                base_commit: base_commit.clone(),
                approved_task_ids: vec![SESSION_TASK_ID.to_string()],
                approved_write_paths: vec!["src/lib.rs".to_string()],
                max_parallel: 1,
                budgets: ExecutionBudgets::default(),
            })
            .expect("persist own operator approval");
        let foreign_approval = authority
            .approve(OperatorApprovalRequest {
                binding_id: foreign_binding.binding_id.clone(),
                thread_uuid: FOREIGN_THREAD_UUID.to_string(),
                plan_id: FOREIGN_PLAN_ID.to_string(),
                plan_revision: foreign_published.revision,
                expected_registry_revision: registry_revision,
                expected_plan_digest: foreign_compiled.plan_digest,
                repository_path: repo.clone(),
                base_commit: base_commit.clone(),
                approved_task_ids: vec![SESSION_TASK_ID.to_string()],
                approved_write_paths: vec!["src/lib.rs".to_string()],
                max_parallel: 1,
                budgets: ExecutionBudgets::default(),
            })
            .expect("persist foreign operator approval");
        assert_ne!(approval.approval_id, foreign_approval.approval_id);

        Self {
            _tmp: tmp,
            root,
            repo,
            fake_worker,
            worker_marker,
            state_dir,
            registry_dir,
            plans_dir,
            authority_dir,
            sessions_dir,
            requests_dir,
            plans,
            authority,
            supervisor: Some(supervisor),
            binding_id: binding.binding_id,
            foreign_binding_id: foreign_binding.binding_id,
            registry_revision,
            approval,
            foreign_approval,
        }
    }

    fn workflow_args(&self, command: &str, extra: &[String]) -> Vec<String> {
        workflow_arguments(
            &self.state_dir,
            &self.registry_dir,
            &self.plans_dir,
            &self.binding_id,
            THREAD_UUID,
            command,
            extra,
        )
    }

    fn session_dirs(&self) -> SessionStoreDirs<'_> {
        SessionStoreDirs {
            state_dir: &self.state_dir,
            registry_dir: &self.registry_dir,
            plans_dir: &self.plans_dir,
            authority_dir: &self.authority_dir,
            sessions_dir: &self.sessions_dir,
        }
    }

    fn session_args(&self, command: &str, extra: &[String]) -> Vec<String> {
        session_arguments(
            &self.session_dirs(),
            &self.binding_id,
            THREAD_UUID,
            command,
            extra,
        )
    }

    fn workflow_draft(&self) -> SavePlanDraft {
        SavePlanDraft {
            command_key: "cmd-native-operator-workflow-1".to_string(),
            plan_id: Some(WORKFLOW_PLAN_ID.to_string()),
            expected_revision: 0,
            profile_id: PROFILE_ID.to_string(),
            profile_revision: self.registry_revision,
            summary: "Native operator workflow CLI plan".to_string(),
            tasks: workflow_plan_tasks(),
        }
    }

    fn write_request<T: Serialize>(&self, name: &str, request: &T) -> PathBuf {
        let path = self.requests_dir.join(name);
        let bytes = serde_json::to_vec(request).expect("serialize private operator request");
        write_private_bytes(&path, &bytes);
        path
    }

    fn write_request_bytes(&self, name: &str, bytes: &[u8]) -> PathBuf {
        let path = self.requests_dir.join(name);
        write_private_bytes(&path, bytes);
        path
    }

    fn approval_count(&self) -> usize {
        let path = self.authority_dir.join("approvals.json");
        if !path.exists() {
            return 0;
        }
        let bytes = fs::read(&path).expect("read approvals ledger");
        let ledger: Value = serde_json::from_slice(&bytes).expect("parse approvals ledger");
        ledger
            .get("approvals")
            .and_then(Value::as_array)
            .map_or(0, Vec::len)
    }

    fn plan_revisions(&self, plan_id: &str) -> Vec<threadlane_workflow::PlanRevision> {
        self.plans
            .plan_revisions(plan_id)
            .expect("read plan revisions")
    }

    fn assert_no_execution_effects(&self) {
        assert!(
            !self.worker_marker.exists(),
            "operator CLIs must not launch the configured worker executable"
        );
        let worktrees = self.repo.join(".git/worktrees");
        if worktrees.exists() {
            let names = fs::read_dir(&worktrees)
                .expect("read fixture worktrees directory")
                .collect::<Result<Vec<_>, _>>()
                .expect("read fixture worktree entries");
            assert!(
                names.is_empty(),
                "operator CLIs must not create worker worktrees"
            );
        }
    }

    fn assert_sessions_empty(&self) {
        let entries = fs::read_dir(&self.sessions_dir)
            .expect("read sessions root")
            .collect::<Result<Vec<_>, _>>()
            .expect("read session directory entries");
        let names: Vec<_> = entries.into_iter().map(|entry| entry.file_name()).collect();
        assert!(
            names.is_empty(),
            "no host session directory may be created automatically: {names:?}"
        );
    }

    async fn shutdown(&mut self) {
        if let Some(supervisor) = self.supervisor.take() {
            supervisor.shutdown().await;
        }
    }
}

fn workflow_arguments(
    state_dir: &Path,
    registry_dir: &Path,
    plans_dir: &Path,
    binding_id: &str,
    thread_uuid: &str,
    command: &str,
    extra: &[String],
) -> Vec<String> {
    let mut args = vec![
        "--state-dir".to_string(),
        state_dir.display().to_string(),
        "--registry-dir".to_string(),
        registry_dir.display().to_string(),
        "--plans-dir".to_string(),
        plans_dir.display().to_string(),
        "--binding".to_string(),
        binding_id.to_string(),
        "--thread".to_string(),
        thread_uuid.to_string(),
        command.to_string(),
    ];
    args.extend(extra.iter().cloned());
    args
}

#[derive(Clone, Copy)]
struct SessionStoreDirs<'a> {
    state_dir: &'a Path,
    registry_dir: &'a Path,
    plans_dir: &'a Path,
    authority_dir: &'a Path,
    sessions_dir: &'a Path,
}

fn session_arguments(
    dirs: &SessionStoreDirs<'_>,
    binding_id: &str,
    thread_uuid: &str,
    command: &str,
    extra: &[String],
) -> Vec<String> {
    let mut args = vec![
        "--state-dir".to_string(),
        dirs.state_dir.display().to_string(),
        "--registry-dir".to_string(),
        dirs.registry_dir.display().to_string(),
        "--plans-dir".to_string(),
        dirs.plans_dir.display().to_string(),
        "--authority-dir".to_string(),
        dirs.authority_dir.display().to_string(),
        "--sessions-dir".to_string(),
        dirs.sessions_dir.display().to_string(),
        "--binding".to_string(),
        binding_id.to_string(),
        "--thread".to_string(),
        thread_uuid.to_string(),
        command.to_string(),
    ];
    args.extend(extra.iter().cloned());
    args
}

fn session_plan_tasks() -> Vec<PlanTask> {
    vec![PlanTask {
        id: SESSION_TASK_ID.to_string(),
        title: "Session fixture task".to_string(),
        instruction: "Inspect the approved session fixture task.".to_string(),
        role: WorkflowRole::Coder,
        difficulty: Difficulty::Easy,
        depends_on: Vec::new(),
        allowed_write_paths: vec!["src/lib.rs".to_string()],
    }]
}

fn workflow_plan_tasks() -> Vec<PlanTask> {
    vec![
        PlanTask {
            id: WORKFLOW_TASK_ALPHA.to_string(),
            title: "First workflow CLI task".to_string(),
            instruction: "Implement the first workflow CLI fixture task.".to_string(),
            role: WorkflowRole::Coder,
            difficulty: Difficulty::Easy,
            depends_on: Vec::new(),
            allowed_write_paths: vec!["src/lib.rs".to_string()],
        },
        PlanTask {
            id: WORKFLOW_TASK_BETA.to_string(),
            title: "Second workflow CLI task".to_string(),
            instruction: "Implement the second workflow CLI fixture task.".to_string(),
            role: WorkflowRole::Coder,
            difficulty: Difficulty::Easy,
            depends_on: Vec::new(),
            allowed_write_paths: vec!["src/lib.rs".to_string()],
        },
    ]
}

fn init_disposable_repo(repo: &Path) -> String {
    fs::create_dir_all(repo.join("src")).expect("create disposable repository src");
    fs::set_permissions(repo, fs::Permissions::from_mode(0o700))
        .expect("chmod disposable repository 0700");
    fs::write(
        repo.join("src/lib.rs"),
        "pub fn native_operator_fixture() -> u32 { 1 }\n",
    )
    .expect("write disposable repository src/lib.rs");

    run_git(repo, &["init", "--quiet", "-b", "main"]);
    run_git(
        repo,
        &[
            "-c",
            "user.name=Threadlane Native Operator Fixture",
            "-c",
            "user.email=fixture@threadlane.local",
            "-c",
            "commit.gpgsign=false",
            "add",
            "--",
            "src/lib.rs",
        ],
    );
    run_git(
        repo,
        &[
            "-c",
            "user.name=Threadlane Native Operator Fixture",
            "-c",
            "user.email=fixture@threadlane.local",
            "-c",
            "commit.gpgsign=false",
            "commit",
            "--quiet",
            "-m",
            "Initial native operator fixture commit",
        ],
    );
    resolve_exact_commit_sha(repo, "HEAD", DEFAULT_GIT_COMMAND_TIMEOUT_MS)
        .expect("resolve native operator fixture HEAD")
}

fn run_git(dir: &Path, args: &[&str]) -> String {
    run_sanitized_git(dir, args, DEFAULT_GIT_COMMAND_TIMEOUT_MS)
        .unwrap_or_else(|err| panic!("git {args:?} in {} failed: {err}", dir.display()))
}

fn write_fake_worker(path: &Path, marker: &Path) {
    let script = format!(
        "#!/bin/sh\nset -eu\nprintf 'invoked\\n' > '{}'\nexit 97\n",
        marker.display()
    );
    fs::write(path, script).expect("write fake worker executable");
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))
        .expect("chmod fake worker executable 0700");
}

fn create_private_dir(path: &Path) {
    fs::create_dir_all(path).expect("create private fixture directory");
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))
        .unwrap_or_else(|err| panic!("chmod private fixture {} 0700: {err}", path.display()));
}

fn write_private_bytes(path: &Path, bytes: &[u8]) {
    let parent = path.parent().expect("private request path has a parent");
    create_private_dir(parent);
    fs::write(path, bytes).expect("write private request file");
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))
        .expect("chmod private request file 0600");
}

fn assert_mode(path: &Path, expected: u32, label: &str) {
    let mode = fs::symlink_metadata(path)
        .unwrap_or_else(|err| panic!("stat {label} {}: {err}", path.display()))
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(mode, expected, "{label} permissions must be {expected:04o}");
}

fn sanitized_command(program: PathBuf) -> Command {
    let mut command = Command::new(program);
    command
        .env("GIT_TERMINAL_PROMPT", "0")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_SYSTEM", "/dev/null")
        .env("GIT_CONFIG_GLOBAL", "/dev/null");
    command
}

async fn run_operator_command(mut command: Command, args: &[String], label: &str) -> Output {
    command.args(args);
    tokio::time::timeout(COMMAND_TIMEOUT, command.output())
        .await
        .unwrap_or_else(|_| panic!("{label} did not finish within {COMMAND_TIMEOUT:?}"))
        .unwrap_or_else(|err| panic!("execute {label}: {err}"))
}

async fn run_workflow(args: &[String]) -> Output {
    run_operator_command(
        sanitized_command(PathBuf::from(env!("CARGO_BIN_EXE_threadlane-workflow"))),
        args,
        "threadlane-workflow",
    )
    .await
}

async fn run_session(args: &[String]) -> Output {
    run_operator_command(
        sanitized_command(PathBuf::from(env!("CARGO_BIN_EXE_threadlane-session"))),
        args,
        "threadlane-session",
    )
    .await
}

fn parse_success<T: DeserializeOwned>(output: &Output, context: &str) -> T {
    assert!(
        output.status.success(),
        "{context} failed with status {:?}\nstdout: {}\nstderr: {}",
        output.status.code(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        output.stdout.len() <= MAX_IO_BYTES,
        "{context}: stdout exceeded the bounded 1 MiB result envelope"
    );
    serde_json::from_slice(&output.stdout).unwrap_or_else(|err| {
        panic!(
            "{context}: stdout must contain one JSON operator envelope: {err}\nstdout: {}",
            String::from_utf8_lossy(&output.stdout)
        )
    })
}

fn parse_envelope<T: DeserializeOwned>(output: &Output, context: &str, binding_id: &str) -> T {
    let envelope: OperatorEnvelope<T> = parse_success(output, context);
    assert!(
        envelope.untrusted_data,
        "{context}: result data is untrusted"
    );
    assert_eq!(
        envelope.route, "trusted_local_operator",
        "{context}: unexpected operator route"
    );
    assert!(
        !envelope.native_per_call_identity_verified,
        "{context}: CLI flags must not claim native per-call identity verification"
    );
    assert_eq!(
        envelope.binding_id, binding_id,
        "{context}: result envelope binding scope changed"
    );
    envelope.data
}

fn expect_failure(output: &Output, context: &str) -> CliFailure {
    assert!(
        !output.status.success(),
        "{context}: expected a nonzero exit, got success\nstdout: {}",
        String::from_utf8_lossy(&output.stdout)
    );
    assert!(
        output.stdout.is_empty(),
        "{context}: failed commands must not write result data to stdout\nstdout: {}",
        String::from_utf8_lossy(&output.stdout)
    );
    assert!(
        output.stderr.len() <= MAX_IO_BYTES,
        "{context}: stderr exceeded the bounded 1 MiB error envelope"
    );
    let failure: CliFailure = serde_json::from_slice(&output.stderr).unwrap_or_else(|err| {
        panic!(
            "{context}: stderr must contain one static JSON error envelope: {err}\nstderr: {}",
            String::from_utf8_lossy(&output.stderr)
        )
    });
    assert!(
        !failure.code.trim().is_empty(),
        "{context}: error code must not be empty"
    );
    assert!(
        !failure.message.trim().is_empty(),
        "{context}: error message must not be empty"
    );
    failure
}

fn expect_failure_code(output: &Output, context: &str, expected: &str) -> CliFailure {
    let failure = expect_failure(output, context);
    assert_eq!(
        failure.code, expected,
        "{context}: unexpected static error code"
    );
    failure
}

fn assert_confirmation_refusal(failure: &CliFailure, context: &str) {
    assert!(
        failure.code == "invalid_input"
            || failure.code.ends_with("mismatch")
            || failure.code.contains("confirm"),
        "{context}: expected a static confirmation refusal before host effects, got `{}`",
        failure.code
    );
}

fn assert_no_secret_echo(output: &Output, context: &str) {
    for (stream, bytes) in [
        ("stdout", output.stdout.as_slice()),
        ("stderr", output.stderr.as_slice()),
    ] {
        let text = String::from_utf8_lossy(bytes);
        assert!(
            !text.contains(SECRET_SENTINEL),
            "{context}: raw request sentinel was echoed on {stream}: {text}"
        );
    }
}

fn assert_sha256(value: &str, label: &str) {
    assert_eq!(
        value.len(),
        64,
        "{label} must be 64 hex characters: {value}"
    );
    assert!(
        value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
        "{label} must be lowercase hexadecimal: {value}"
    );
}

fn different_digest(actual: &str) -> String {
    if actual.starts_with('0') {
        "1".repeat(64)
    } else {
        "0".repeat(64)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn workflow_catalog_save_publish_get_and_list_round_trip_uses_private_request() {
    let mut fx = OperatorFixture::start().await;
    let approvals_before = fx.approval_count();

    let catalog_args = fx.workflow_args("catalog", &["--limit".to_string(), "5".to_string()]);
    let catalog: CatalogPage = parse_envelope(
        &run_workflow(&catalog_args).await,
        "catalog",
        &fx.binding_id,
    );
    assert_eq!(catalog.registry_revision, fx.registry_revision);
    assert_eq!(catalog.default_profile_id.as_deref(), Some(PROFILE_ID));
    assert!(!catalog.has_more);
    let profile = catalog
        .profiles
        .iter()
        .find(|profile| profile.profile_id.as_str() == PROFILE_ID)
        .expect("catalog must contain the configured fixture profile");
    assert_eq!(profile.name, "Native operator CLI fixture profile");
    assert_eq!(profile.profile_revision, fx.registry_revision);
    assert!(profile.is_default);
    assert!(profile.available);
    assert_eq!(profile.assignments.len(), 1);
    let assignment = &profile.assignments[0];
    assert_eq!(assignment.role, WorkflowRole::Coder);
    assert_eq!(assignment.difficulty, Some(Difficulty::Easy));
    assert_eq!(assignment.model_id, "model-deepseek-flash");
    assert_eq!(assignment.model_arg, "opencode-go/deepseek-v4.1-flash");
    assert_eq!(assignment.effort.as_deref(), Some("max"));
    assert_eq!(assignment.route.as_str(), "headless_worker");
    let catalog_json = serde_json::to_string(profile).expect("serialize catalog profile");
    assert!(
        !catalog_json.contains(fx.fake_worker.to_string_lossy().as_ref()),
        "catalog output must not expose worker executable paths"
    );
    assert!(
        !catalog_json.contains(SECRET_SENTINEL),
        "catalog output must not expose request secrets"
    );

    let draft = fx.workflow_draft();
    let request_path = fx.write_request("workflow-save-draft.json", &draft);
    assert_mode(&request_path, 0o600, "private save-draft request");
    let save_args = fx.workflow_args(
        "save-draft",
        &["--request".to_string(), request_path.display().to_string()],
    );
    let saved: PlanSummary = parse_envelope(
        &run_workflow(&save_args).await,
        "save-draft",
        &fx.binding_id,
    );
    assert_eq!(saved.plan_id, WORKFLOW_PLAN_ID);
    assert_eq!(saved.binding_id, fx.binding_id);
    assert_eq!(saved.revision, 1);
    assert_eq!(saved.status, PlanStatus::Draft);
    assert_eq!(saved.task_count, 2);
    assert_sha256(
        saved.plan_digest.as_deref().expect("draft plan digest"),
        "saved draft plan_digest",
    );
    assert_eq!(
        fx.plans
            .get_revision(WORKFLOW_PLAN_ID, 1)
            .expect("read persisted draft")
            .expect("draft revision exists")
            .status,
        PlanStatus::Draft
    );
    assert_mode(
        &request_path,
        0o600,
        "private save-draft request after successful use",
    );

    let publish_args = fx.workflow_args(
        "publish",
        &[
            "--plan-id".to_string(),
            WORKFLOW_PLAN_ID.to_string(),
            "--revision".to_string(),
            "1".to_string(),
        ],
    );
    let published: PublishedPlanView = parse_envelope(
        &run_workflow(&publish_args).await,
        "publish",
        &fx.binding_id,
    );
    assert_eq!(published.header.plan_id, WORKFLOW_PLAN_ID);
    assert_eq!(published.header.revision, 1);
    assert_eq!(published.header.status, PlanStatus::Published);
    assert_eq!(published.receipt.state, PlanStatus::Published);
    assert_eq!(
        published.header.plan_digest.as_deref(),
        saved.plan_digest.as_deref()
    );
    assert_eq!(published.tasks.len(), 2);
    assert_eq!(published.tasks[0].task_id, WORKFLOW_TASK_ALPHA);
    assert_eq!(published.tasks[1].task_id, WORKFLOW_TASK_BETA);
    assert_eq!(published.tasks[0].route.as_str(), "headless_worker");
    assert_eq!(
        published.tasks[0].allowed_write_paths,
        vec!["src/lib.rs".to_string()]
    );
    let published_json = serde_json::to_string(&published).expect("serialize published view");
    assert!(
        !published_json.contains(fx.fake_worker.to_string_lossy().as_ref()),
        "published view must not expose worker executable paths"
    );

    let first_page_args = fx.workflow_args(
        "get-plan",
        &[
            "--plan-id".to_string(),
            WORKFLOW_PLAN_ID.to_string(),
            "--revision".to_string(),
            "1".to_string(),
            "--limit".to_string(),
            "1".to_string(),
        ],
    );
    let first_page: PlanPage = parse_envelope(
        &run_workflow(&first_page_args).await,
        "get-plan first page",
        &fx.binding_id,
    );
    assert_eq!(first_page.header.plan_id, WORKFLOW_PLAN_ID);
    assert_eq!(first_page.header.task_count, 2);
    assert_eq!(first_page.tasks.len(), 1);
    assert_eq!(first_page.tasks[0].id, WORKFLOW_TASK_ALPHA);
    assert!(first_page.has_more);
    let next_cursor = first_page
        .next_task_cursor
        .clone()
        .expect("first task page cursor");
    let second_page_args = fx.workflow_args(
        "get-plan",
        &[
            "--plan-id".to_string(),
            WORKFLOW_PLAN_ID.to_string(),
            "--revision".to_string(),
            "1".to_string(),
            "--cursor".to_string(),
            next_cursor,
            "--limit".to_string(),
            "1".to_string(),
        ],
    );
    let second_page: PlanPage = parse_envelope(
        &run_workflow(&second_page_args).await,
        "get-plan second page",
        &fx.binding_id,
    );
    assert_eq!(second_page.tasks.len(), 1);
    assert_eq!(second_page.tasks[0].id, WORKFLOW_TASK_BETA);
    assert!(!second_page.has_more);
    assert!(second_page.next_task_cursor.is_none());

    let list_args = fx.workflow_args("list-plans", &["--limit".to_string(), "20".to_string()]);
    let plan_list: PlanSummaryPage = parse_envelope(
        &run_workflow(&list_args).await,
        "list-plans",
        &fx.binding_id,
    );
    assert_eq!(plan_list.binding_id, fx.binding_id);
    let listed = plan_list
        .plans
        .iter()
        .find(|plan| plan.plan_id.as_str() == WORKFLOW_PLAN_ID)
        .expect("list-plans must include the saved plan");
    assert_eq!(listed.status, PlanStatus::Published);
    assert_eq!(listed.revision, 1);
    assert_eq!(listed.task_count, 2);
    assert!(
        plan_list
            .plans
            .iter()
            .all(|plan| plan.binding_id == fx.binding_id),
        "list-plans must not disclose plans owned by another binding"
    );

    let repeated_publish: PublishedPlanView = parse_envelope(
        &run_workflow(&publish_args).await,
        "idempotent publish replay",
        &fx.binding_id,
    );
    assert_eq!(repeated_publish.header.status, PlanStatus::Published);
    assert_eq!(
        repeated_publish.header.plan_digest,
        published.header.plan_digest
    );
    assert_eq!(fx.plan_revisions(WORKFLOW_PLAN_ID).len(), 1);

    assert_eq!(fx.approval_count(), approvals_before);
    fx.assert_no_execution_effects();
    fx.assert_sessions_empty();
    fx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn workflow_exact_save_retry_is_idempotent_and_changed_or_stale_revisions_fail_closed() {
    let mut fx = OperatorFixture::start().await;
    let approvals_before = fx.approval_count();
    let draft = fx.workflow_draft();
    let request_path = fx.write_request("workflow-idempotent-draft.json", &draft);
    let save_args = fx.workflow_args(
        "save-draft",
        &["--request".to_string(), request_path.display().to_string()],
    );

    let first: PlanSummary = parse_envelope(
        &run_workflow(&save_args).await,
        "save-draft",
        &fx.binding_id,
    );
    let replayed: PlanSummary = parse_envelope(
        &run_workflow(&save_args).await,
        "exact save-draft replay",
        &fx.binding_id,
    );
    assert_eq!(first, replayed);
    assert_eq!(fx.plan_revisions(WORKFLOW_PLAN_ID).len(), 1);

    let publish_args = fx.workflow_args(
        "publish",
        &[
            "--plan-id".to_string(),
            WORKFLOW_PLAN_ID.to_string(),
            "--revision".to_string(),
            "1".to_string(),
        ],
    );
    let published: PublishedPlanView = parse_envelope(
        &run_workflow(&publish_args).await,
        "publish",
        &fx.binding_id,
    );
    let published_again: PublishedPlanView = parse_envelope(
        &run_workflow(&publish_args).await,
        "exact publish replay",
        &fx.binding_id,
    );
    assert_eq!(published, published_again);
    assert_eq!(fx.plan_revisions(WORKFLOW_PLAN_ID).len(), 1);

    let mut changed_payload = draft.clone();
    changed_payload.summary = "Changed payload under the same command key.".to_string();
    let changed_path = fx.write_request("workflow-changed-command.json", &changed_payload);
    let changed_output = run_workflow(&fx.workflow_args(
        "save-draft",
        &["--request".to_string(), changed_path.display().to_string()],
    ))
    .await;
    expect_failure_code(
        &changed_output,
        "changed payload under a replayed command key",
        "idempotency_conflict",
    );

    let mut stale_create = draft.clone();
    stale_create.command_key = "cmd-native-operator-workflow-stale".to_string();
    let stale_path = fx.write_request("workflow-stale-create.json", &stale_create);
    let stale_output = run_workflow(&fx.workflow_args(
        "save-draft",
        &["--request".to_string(), stale_path.display().to_string()],
    ))
    .await;
    expect_failure_code(
        &stale_output,
        "new command key with a stale expected revision",
        "stale_revision",
    );

    let missing_revision_output = run_workflow(&fx.workflow_args(
        "publish",
        &[
            "--plan-id".to_string(),
            WORKFLOW_PLAN_ID.to_string(),
            "--revision".to_string(),
            "2".to_string(),
        ],
    ))
    .await;
    expect_failure_code(
        &missing_revision_output,
        "publish of a missing revision",
        "stale_revision",
    );

    let zero_revision_output = run_workflow(&fx.workflow_args(
        "publish",
        &[
            "--plan-id".to_string(),
            WORKFLOW_PLAN_ID.to_string(),
            "--revision".to_string(),
            "0".to_string(),
        ],
    ))
    .await;
    expect_failure_code(
        &zero_revision_output,
        "publish of revision zero",
        "invalid_input",
    );

    let revisions = fx.plan_revisions(WORKFLOW_PLAN_ID);
    assert_eq!(revisions.len(), 1);
    assert_eq!(revisions[0].status, PlanStatus::Published);
    assert_eq!(fx.approval_count(), approvals_before);
    fx.assert_no_execution_effects();
    fx.assert_sessions_empty();
    fx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn workflow_rejects_invalid_insecure_symlink_and_oversized_requests_without_leaks() {
    let mut fx = OperatorFixture::start().await;
    let approvals_before = fx.approval_count();
    let draft = fx.workflow_draft();

    let malformed = format!("{{ this is not valid request JSON {SECRET_SENTINEL}");
    let malformed_path = fx.write_request_bytes("workflow-malformed.json", malformed.as_bytes());
    let malformed_output = run_workflow(&fx.workflow_args(
        "save-draft",
        &[
            "--request".to_string(),
            malformed_path.display().to_string(),
        ],
    ))
    .await;
    expect_failure_code(&malformed_output, "malformed request JSON", "invalid_input");
    assert_no_secret_echo(&malformed_output, "malformed request JSON");

    let mut unknown = serde_json::to_value(&draft).expect("serialize draft request value");
    unknown
        .as_object_mut()
        .expect("draft request is an object")
        .insert("raw_secret".to_string(), json!(SECRET_SENTINEL));
    let unknown_path = fx.write_request("workflow-unknown-field.json", &unknown);
    let unknown_output = run_workflow(&fx.workflow_args(
        "save-draft",
        &["--request".to_string(), unknown_path.display().to_string()],
    ))
    .await;
    expect_failure_code(
        &unknown_output,
        "request with an unknown field",
        "invalid_input",
    );
    assert_no_secret_echo(&unknown_output, "request with an unknown field");

    let insecure_path = fx.write_request("workflow-insecure.json", &draft);
    fs::set_permissions(&insecure_path, fs::Permissions::from_mode(0o644))
        .expect("chmod otherwise-valid request 0644");
    let insecure_output = run_workflow(&fx.workflow_args(
        "save-draft",
        &["--request".to_string(), insecure_path.display().to_string()],
    ))
    .await;
    expect_failure_code(
        &insecure_output,
        "group-readable request file",
        "security_violation",
    );
    assert_no_secret_echo(&insecure_output, "group-readable request file");
    assert_mode(&insecure_path, 0o644, "refused group-readable request file");

    let valid_path = fx.write_request("workflow-symlink-target.json", &draft);
    let leaf_link = fx.requests_dir.join("workflow-leaf-link.json");
    symlink(&valid_path, &leaf_link).expect("create symlink request leaf");
    let leaf_output = run_workflow(&fx.workflow_args(
        "save-draft",
        &["--request".to_string(), leaf_link.display().to_string()],
    ))
    .await;
    expect_failure_code(&leaf_output, "symlink request leaf", "security_violation");

    let real_ancestor = fx.requests_dir.join("real-ancestor");
    create_private_dir(&real_ancestor);
    write_private_bytes(
        &real_ancestor.join("request.json"),
        &serde_json::to_vec(&draft).expect("serialize ancestor request"),
    );
    let ancestor_link = fx.requests_dir.join("linked-ancestor");
    symlink(&real_ancestor, &ancestor_link).expect("create symlink request ancestor");
    let ancestor_output = run_workflow(&fx.workflow_args(
        "save-draft",
        &[
            "--request".to_string(),
            ancestor_link.join("request.json").display().to_string(),
        ],
    ))
    .await;
    expect_failure_code(
        &ancestor_output,
        "symlink request ancestor",
        "security_violation",
    );

    let mut oversize = serde_json::to_vec(&draft).expect("serialize oversize request");
    oversize.resize(MAX_IO_BYTES + 1, b' ');
    let oversize_path = fx.write_request_bytes("workflow-oversize.json", &oversize);
    let oversize_output = run_workflow(&fx.workflow_args(
        "save-draft",
        &["--request".to_string(), oversize_path.display().to_string()],
    ))
    .await;
    expect_failure_code(&oversize_output, "oversize request file", "invalid_input");

    let directory_path = fx.requests_dir.join("workflow-directory");
    create_private_dir(&directory_path);
    let directory_output = run_workflow(&fx.workflow_args(
        "save-draft",
        &[
            "--request".to_string(),
            directory_path.display().to_string(),
        ],
    ))
    .await;
    expect_failure_code(&directory_output, "directory request path", "invalid_input");

    let relative_output = run_workflow(&fx.workflow_args(
        "save-draft",
        &[
            "--request".to_string(),
            "requests/workflow-relative.json".to_string(),
        ],
    ))
    .await;
    expect_failure_code(&relative_output, "relative request path", "invalid_input");

    assert!(
        fx.plan_revisions(WORKFLOW_PLAN_ID).is_empty(),
        "refused request files must not create a plan revision"
    );
    assert_eq!(fx.approval_count(), approvals_before);
    fx.assert_no_execution_effects();
    fx.assert_sessions_empty();
    fx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn workflow_rejects_wrong_nil_noncanonical_and_offline_bindings() {
    let mut fx = OperatorFixture::start().await;
    let approvals_before = fx.approval_count();

    let wrong_thread = run_workflow(&workflow_arguments(
        &fx.state_dir,
        &fx.registry_dir,
        &fx.plans_dir,
        &fx.binding_id,
        OTHER_THREAD_UUID,
        "catalog",
        &[],
    ))
    .await;
    expect_failure_code(
        &wrong_thread,
        "live binding with a different explicit thread",
        "thread_uuid_mismatch",
    );

    let nil_thread = run_workflow(&workflow_arguments(
        &fx.state_dir,
        &fx.registry_dir,
        &fx.plans_dir,
        &fx.binding_id,
        "00000000-0000-0000-0000-000000000000",
        "catalog",
        &[],
    ))
    .await;
    expect_failure_code(&nil_thread, "nil thread UUID", "invalid_input");

    let uppercase_uuid = THREAD_UUID.to_ascii_uppercase();
    let uppercase_thread = run_workflow(&workflow_arguments(
        &fx.state_dir,
        &fx.registry_dir,
        &fx.plans_dir,
        &fx.binding_id,
        &uppercase_uuid,
        "catalog",
        &[],
    ))
    .await;
    expect_failure_code(
        &uppercase_thread,
        "noncanonical uppercase thread UUID",
        "invalid_input",
    );

    let unknown_binding = run_workflow(&workflow_arguments(
        &fx.state_dir,
        &fx.registry_dir,
        &fx.plans_dir,
        "bnd_native_operator_missing",
        THREAD_UUID,
        "catalog",
        &[],
    ))
    .await;
    expect_failure_code(
        &unknown_binding,
        "unknown live binding",
        "binding_not_found",
    );

    fx.shutdown().await;
    let offline = run_workflow(&fx.workflow_args("catalog", &[])).await;
    expect_failure_code(
        &offline,
        "workflow command while the supervisor is offline",
        "transport_unavailable",
    );

    assert!(fx.plan_revisions(WORKFLOW_PLAN_ID).is_empty());
    assert_eq!(fx.approval_count(), approvals_before);
    fx.assert_no_execution_effects();
    fx.assert_sessions_empty();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn workflow_binding_scope_hides_and_refuses_foreign_plans() {
    let mut fx = OperatorFixture::start().await;
    let approvals_before = fx.approval_count();
    let foreign_revisions = fx.plan_revisions(FOREIGN_PLAN_ID);
    assert_eq!(foreign_revisions.len(), 1);
    let foreign_plan = &foreign_revisions[0];
    assert_eq!(foreign_plan.binding_id, fx.foreign_binding_id);
    assert_eq!(foreign_plan.status, PlanStatus::Published);

    let list_args = fx.workflow_args("list-plans", &["--limit".to_string(), "20".to_string()]);
    let plan_list: PlanSummaryPage = parse_envelope(
        &run_workflow(&list_args).await,
        "list-plans with a foreign plan present",
        &fx.binding_id,
    );
    assert!(
        plan_list
            .plans
            .iter()
            .all(|plan| plan.binding_id == fx.binding_id),
        "list-plans must only return plans owned by the selected binding"
    );
    assert!(
        !plan_list
            .plans
            .iter()
            .any(|plan| plan.plan_id == FOREIGN_PLAN_ID),
        "foreign plan must not be disclosed"
    );

    let get_foreign = run_workflow(&fx.workflow_args(
        "get-plan",
        &[
            "--plan-id".to_string(),
            FOREIGN_PLAN_ID.to_string(),
            "--revision".to_string(),
            "1".to_string(),
        ],
    ))
    .await;
    expect_failure_code(&get_foreign, "get a foreign plan", "not_found");

    let publish_foreign = run_workflow(&fx.workflow_args(
        "publish",
        &[
            "--plan-id".to_string(),
            FOREIGN_PLAN_ID.to_string(),
            "--revision".to_string(),
            "1".to_string(),
        ],
    ))
    .await;
    expect_failure_code(
        &publish_foreign,
        "publish a foreign plan through the wrong binding",
        "not_found",
    );

    assert_eq!(
        fx.plan_revisions(FOREIGN_PLAN_ID)[0].status,
        PlanStatus::Published
    );
    assert_eq!(fx.approval_count(), approvals_before);
    fx.assert_no_execution_effects();
    fx.assert_sessions_empty();
    fx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn workflow_rejects_overlapping_symlink_and_non_private_store_directories() {
    let mut fx = OperatorFixture::start().await;
    let approvals_before = fx.approval_count();

    let duplicate = run_workflow(&workflow_arguments(
        &fx.state_dir,
        &fx.registry_dir,
        &fx.registry_dir,
        &fx.binding_id,
        THREAD_UUID,
        "catalog",
        &[],
    ))
    .await;
    expect_failure_code(
        &duplicate,
        "registry and plans alias the same store",
        "security_violation",
    );

    let nested_plans = fx.plans_dir.join("nested-plans");
    create_private_dir(&nested_plans);
    let nested = run_workflow(&workflow_arguments(
        &fx.state_dir,
        &fx.plans_dir,
        &nested_plans,
        &fx.binding_id,
        THREAD_UUID,
        "catalog",
        &[],
    ))
    .await;
    expect_failure_code(
        &nested,
        "registry and plans stores overlap",
        "security_violation",
    );

    let plans_link = fx.root.join("workflow-plans-link");
    symlink(&fx.plans_dir, &plans_link).expect("create symlink plans store directory");
    let linked = run_workflow(&workflow_arguments(
        &fx.state_dir,
        &fx.registry_dir,
        &plans_link,
        &fx.binding_id,
        THREAD_UUID,
        "catalog",
        &[],
    ))
    .await;
    expect_failure_code(&linked, "symlink plans store", "security_violation");

    let open_plans = fx.root.join("open-plans-store");
    fs::create_dir_all(&open_plans).expect("create non-private plans directory");
    fs::set_permissions(&open_plans, fs::Permissions::from_mode(0o755))
        .expect("chmod non-private plans directory 0755");
    let open = run_workflow(&workflow_arguments(
        &fx.state_dir,
        &fx.registry_dir,
        &open_plans,
        &fx.binding_id,
        THREAD_UUID,
        "catalog",
        &[],
    ))
    .await;
    expect_failure_code(
        &open,
        "non-private plans store directory",
        "security_violation",
    );
    assert_mode(&open_plans, 0o755, "refused non-private plans directory");

    assert!(fx.plan_revisions(WORKFLOW_PLAN_ID).is_empty());
    assert_eq!(fx.approval_count(), approvals_before);
    fx.assert_no_execution_effects();
    fx.assert_sessions_empty();
    fx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn workflow_rejects_missing_unknown_and_incomplete_flags_with_static_errors() {
    let mut fx = OperatorFixture::start().await;
    let approvals_before = fx.approval_count();

    let cases: Vec<(&str, Vec<String>)> = vec![
        ("no arguments", Vec::new()),
        ("missing global stores", vec!["catalog".to_string()]),
        ("unknown subcommand", fx.workflow_args("bogus", &[])),
        (
            "missing save-draft request",
            fx.workflow_args("save-draft", &[]),
        ),
        (
            "unknown flag echo",
            fx.workflow_args(
                "catalog",
                &["--unknown".to_string(), SECRET_SENTINEL.to_string()],
            ),
        ),
    ];

    for (label, args) in cases {
        let output = run_workflow(&args).await;
        expect_failure_code(&output, label, "invalid_input");
        assert_no_secret_echo(&output, label);
    }

    assert!(fx.plan_revisions(WORKFLOW_PLAN_ID).is_empty());
    assert_eq!(fx.approval_count(), approvals_before);
    fx.assert_no_execution_effects();
    fx.assert_sessions_empty();
    fx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn session_rejects_malformed_flags_and_missing_dispatch_cancel_confirmations() {
    let mut fx = OperatorFixture::start().await;
    let approvals_before = fx.approval_count();
    let approval_id = fx.approval.approval_id.clone();
    let digest = fx.approval.subject_digest.clone();

    let cases: Vec<(&str, Vec<String>)> = vec![
        ("no arguments", Vec::new()),
        (
            "missing session global stores",
            vec![
                "session".to_string(),
                "--approval-id".to_string(),
                approval_id.clone(),
            ],
        ),
        ("unknown subcommand", fx.session_args("bogus", &[])),
        (
            "dispatch without either confirmation",
            fx.session_args(
                "dispatch",
                &[
                    "--approval-id".to_string(),
                    approval_id.clone(),
                    "--task-id".to_string(),
                    SESSION_TASK_ID.to_string(),
                ],
            ),
        ),
        (
            "dispatch without operator attestation",
            fx.session_args(
                "dispatch",
                &[
                    "--approval-id".to_string(),
                    approval_id.clone(),
                    "--task-id".to_string(),
                    SESSION_TASK_ID.to_string(),
                    "--confirm-approval".to_string(),
                    digest.clone(),
                ],
            ),
        ),
        (
            "dispatch without approval digest",
            fx.session_args(
                "dispatch",
                &[
                    "--approval-id".to_string(),
                    approval_id.clone(),
                    "--task-id".to_string(),
                    SESSION_TASK_ID.to_string(),
                    "--operator-approved".to_string(),
                ],
            ),
        ),
        (
            "cancel without either confirmation",
            fx.session_args(
                "cancel",
                &[
                    "--approval-id".to_string(),
                    approval_id.clone(),
                    "--task-id".to_string(),
                    SESSION_TASK_ID.to_string(),
                    "--expected-revision".to_string(),
                    "1".to_string(),
                ],
            ),
        ),
        (
            "cancel without operator attestation",
            fx.session_args(
                "cancel",
                &[
                    "--approval-id".to_string(),
                    approval_id.clone(),
                    "--task-id".to_string(),
                    SESSION_TASK_ID.to_string(),
                    "--expected-revision".to_string(),
                    "1".to_string(),
                    "--confirm-approval".to_string(),
                    digest.clone(),
                ],
            ),
        ),
        (
            "dispatch without task id",
            fx.session_args(
                "dispatch",
                &[
                    "--approval-id".to_string(),
                    approval_id.clone(),
                    "--confirm-approval".to_string(),
                    digest.clone(),
                    "--operator-approved".to_string(),
                ],
            ),
        ),
        (
            "unknown flag echo",
            fx.session_args(
                "session",
                &[
                    "--approval-id".to_string(),
                    approval_id,
                    "--unknown".to_string(),
                    SECRET_SENTINEL.to_string(),
                ],
            ),
        ),
    ];

    for (label, args) in cases {
        let output = run_session(&args).await;
        expect_failure_code(&output, label, "invalid_input");
        assert_no_secret_echo(&output, label);
        fx.assert_sessions_empty();
    }

    assert_eq!(fx.approval_count(), approvals_before);
    fx.assert_no_execution_effects();
    fx.assert_sessions_empty();
    fx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn session_wrong_digest_and_foreign_approval_scope_fail_before_host_effects() {
    let mut fx = OperatorFixture::start().await;
    let approvals_before = fx.approval_count();
    let approval_id = fx.approval.approval_id.clone();
    let wrong_digest = different_digest(&fx.approval.subject_digest);

    let wrong_dispatch = run_session(&fx.session_args(
        "dispatch",
        &[
            "--approval-id".to_string(),
            approval_id.clone(),
            "--task-id".to_string(),
            SESSION_TASK_ID.to_string(),
            "--confirm-approval".to_string(),
            wrong_digest.clone(),
            "--operator-approved".to_string(),
        ],
    ))
    .await;
    let wrong_dispatch_failure = expect_failure(
        &wrong_dispatch,
        "dispatch with a digest that differs from the stored approval",
    );
    assert_confirmation_refusal(
        &wrong_dispatch_failure,
        "dispatch with a wrong approval digest",
    );

    let wrong_cancel = run_session(&fx.session_args(
        "cancel",
        &[
            "--approval-id".to_string(),
            approval_id.clone(),
            "--task-id".to_string(),
            SESSION_TASK_ID.to_string(),
            "--expected-revision".to_string(),
            "1".to_string(),
            "--confirm-approval".to_string(),
            wrong_digest,
            "--operator-approved".to_string(),
        ],
    ))
    .await;
    let wrong_cancel_failure = expect_failure(
        &wrong_cancel,
        "cancel with a digest that differs from the stored approval",
    );
    assert_confirmation_refusal(&wrong_cancel_failure, "cancel with a wrong approval digest");

    let foreign_dirs = fx.session_dirs();
    let foreign_dispatch = run_session(&session_arguments(
        &foreign_dirs,
        &fx.binding_id,
        THREAD_UUID,
        "dispatch",
        &[
            "--approval-id".to_string(),
            fx.foreign_approval.approval_id.clone(),
            "--task-id".to_string(),
            SESSION_TASK_ID.to_string(),
            "--confirm-approval".to_string(),
            fx.foreign_approval.subject_digest.clone(),
            "--operator-approved".to_string(),
        ],
    ))
    .await;
    let foreign_failure = expect_failure(
        &foreign_dispatch,
        "dispatch of an approval owned by another binding and thread",
    );
    assert!(
        foreign_failure.code == "approval_not_found" || foreign_failure.code.ends_with("mismatch"),
        "foreign approval must fail with a static not-found or scope-mismatch code, got `{}`",
        foreign_failure.code
    );

    let stored = fx
        .authority
        .get_for_operator(&approval_id)
        .expect("read stored own approval");
    assert_eq!(stored.subject_digest, fx.approval.subject_digest);
    assert_eq!(stored.binding_id, fx.binding_id);
    assert_eq!(stored.thread_uuid, THREAD_UUID);
    let stored_foreign = fx
        .authority
        .get_for_operator(&fx.foreign_approval.approval_id)
        .expect("read stored foreign approval");
    assert_eq!(stored_foreign.binding_id, fx.foreign_binding_id);
    assert_eq!(stored_foreign.thread_uuid, FOREIGN_THREAD_UUID);

    assert_eq!(fx.approval_count(), approvals_before);
    fx.assert_no_execution_effects();
    fx.assert_sessions_empty();
    fx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn session_reads_and_mutations_never_auto_host_a_missing_session_or_approval() {
    let mut fx = OperatorFixture::start().await;
    let approvals_before = fx.approval_count();
    let approval_id = fx.approval.approval_id.clone();
    let digest = fx.approval.subject_digest.clone();

    let missing_approval = run_session(&fx.session_args(
        "session",
        &["--approval-id".to_string(), MISSING_APPROVAL_ID.to_string()],
    ))
    .await;
    expect_failure_code(
        &missing_approval,
        "session summary for a missing approval",
        "approval_not_found",
    );
    fx.assert_sessions_empty();

    let cases: Vec<(&str, Vec<String>)> = vec![
        (
            "session summary without a host session",
            fx.session_args(
                "session",
                &["--approval-id".to_string(), approval_id.clone()],
            ),
        ),
        (
            "task read without a host session",
            fx.session_args(
                "task",
                &[
                    "--approval-id".to_string(),
                    approval_id.clone(),
                    "--task-id".to_string(),
                    SESSION_TASK_ID.to_string(),
                ],
            ),
        ),
        (
            "activity read without a host session",
            fx.session_args(
                "activity",
                &["--approval-id".to_string(), approval_id.clone()],
            ),
        ),
        (
            "pull without a host session",
            fx.session_args(
                "pull",
                &[
                    "--approval-id".to_string(),
                    approval_id.clone(),
                    "--limit".to_string(),
                    "5".to_string(),
                ],
            ),
        ),
        (
            "ack without a host session",
            fx.session_args(
                "ack",
                &[
                    "--approval-id".to_string(),
                    approval_id.clone(),
                    "--batch-id".to_string(),
                    "batch_operator_fixture_1".to_string(),
                ],
            ),
        ),
        (
            "dispatch without a host session",
            fx.session_args(
                "dispatch",
                &[
                    "--approval-id".to_string(),
                    approval_id.clone(),
                    "--task-id".to_string(),
                    SESSION_TASK_ID.to_string(),
                    "--confirm-approval".to_string(),
                    digest.clone(),
                    "--operator-approved".to_string(),
                ],
            ),
        ),
        (
            "cancel without a host session",
            fx.session_args(
                "cancel",
                &[
                    "--approval-id".to_string(),
                    approval_id.clone(),
                    "--task-id".to_string(),
                    SESSION_TASK_ID.to_string(),
                    "--expected-revision".to_string(),
                    "1".to_string(),
                    "--confirm-approval".to_string(),
                    digest.clone(),
                    "--operator-approved".to_string(),
                ],
            ),
        ),
    ];

    for (label, args) in cases {
        let output = run_session(&args).await;
        expect_failure_code(&output, label, "host_not_ready");
        fx.assert_sessions_empty();
        fx.assert_no_execution_effects();
    }

    assert!(
        !fx.sessions_dir.join(&approval_id).exists(),
        "missing host session must not be created automatically"
    );
    assert_eq!(fx.approval_count(), approvals_before);
    fx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn session_rejects_wrong_offline_binding_and_overlapping_or_insecure_stores() {
    let mut fx = OperatorFixture::start().await;
    let approvals_before = fx.approval_count();
    let approval_id = fx.approval.approval_id.clone();
    let summary_args = vec!["--approval-id".to_string(), approval_id.clone()];

    let default_dirs = fx.session_dirs();
    let wrong_thread = run_session(&session_arguments(
        &default_dirs,
        &fx.binding_id,
        OTHER_THREAD_UUID,
        "session",
        &summary_args,
    ))
    .await;
    expect_failure_code(
        &wrong_thread,
        "session against a live binding with a different thread",
        "thread_uuid_mismatch",
    );

    let nil_thread = run_session(&session_arguments(
        &default_dirs,
        &fx.binding_id,
        "00000000-0000-0000-0000-000000000000",
        "session",
        &summary_args,
    ))
    .await;
    expect_failure_code(
        &nil_thread,
        "session with a nil thread UUID",
        "invalid_input",
    );

    let uppercase_uuid = THREAD_UUID.to_ascii_uppercase();
    let uppercase_thread = run_session(&session_arguments(
        &default_dirs,
        &fx.binding_id,
        &uppercase_uuid,
        "session",
        &summary_args,
    ))
    .await;
    expect_failure_code(
        &uppercase_thread,
        "session with an uppercase thread UUID",
        "invalid_input",
    );

    let unknown_binding = run_session(&session_arguments(
        &default_dirs,
        "bnd_native_operator_missing",
        THREAD_UUID,
        "session",
        &summary_args,
    ))
    .await;
    expect_failure_code(
        &unknown_binding,
        "session with an unknown binding",
        "binding_not_found",
    );

    let duplicate_authority_dirs = SessionStoreDirs {
        authority_dir: &fx.sessions_dir,
        sessions_dir: &fx.sessions_dir,
        ..default_dirs
    };
    let duplicate_authority = run_session(&session_arguments(
        &duplicate_authority_dirs,
        &fx.binding_id,
        THREAD_UUID,
        "session",
        &summary_args,
    ))
    .await;
    expect_failure_code(
        &duplicate_authority,
        "authority and sessions alias the same store",
        "security_violation",
    );

    let duplicate_plans_dirs = SessionStoreDirs {
        plans_dir: &fx.registry_dir,
        ..default_dirs
    };
    let duplicate_plans = run_session(&session_arguments(
        &duplicate_plans_dirs,
        &fx.binding_id,
        THREAD_UUID,
        "session",
        &summary_args,
    ))
    .await;
    expect_failure_code(
        &duplicate_plans,
        "registry and plans alias the same store",
        "security_violation",
    );

    let sessions_link = fx.root.join("session-store-link");
    symlink(&fx.sessions_dir, &sessions_link).expect("create symlink sessions store");
    let linked_sessions_dirs = SessionStoreDirs {
        sessions_dir: &sessions_link,
        ..default_dirs
    };
    let linked_sessions = run_session(&session_arguments(
        &linked_sessions_dirs,
        &fx.binding_id,
        THREAD_UUID,
        "session",
        &summary_args,
    ))
    .await;
    expect_failure_code(
        &linked_sessions,
        "symlink sessions store",
        "security_violation",
    );

    let open_sessions = fx.root.join("open-sessions-store");
    fs::create_dir_all(&open_sessions).expect("create non-private sessions directory");
    fs::set_permissions(&open_sessions, fs::Permissions::from_mode(0o755))
        .expect("chmod non-private sessions directory 0755");
    let open_sessions_dirs = SessionStoreDirs {
        sessions_dir: &open_sessions,
        ..default_dirs
    };
    let insecure_sessions = run_session(&session_arguments(
        &open_sessions_dirs,
        &fx.binding_id,
        THREAD_UUID,
        "session",
        &summary_args,
    ))
    .await;
    expect_failure_code(
        &insecure_sessions,
        "non-private sessions store",
        "security_violation",
    );
    assert_mode(
        &open_sessions,
        0o755,
        "refused non-private sessions directory",
    );

    fx.shutdown().await;
    let offline = run_session(&fx.session_args("session", &summary_args)).await;
    expect_failure_code(
        &offline,
        "session command while the supervisor is offline",
        "transport_unavailable",
    );

    assert_eq!(fx.approval_count(), approvals_before);
    fx.assert_no_execution_effects();
    fx.assert_sessions_empty();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn session_cli_reports_ready_stopped_and_restarted_host_lifecycle_truthfully() {
    let mut fx = OperatorFixture::start().await;
    let summary_args = fx.session_args(
        "session",
        &["--approval-id".to_string(), fx.approval.approval_id.clone()],
    );
    let stop_args = |expected_instance: &str, confirm_approval: &str| {
        fx.session_args(
            "stop",
            &[
                "--approval-id".to_string(),
                fx.approval.approval_id.clone(),
                "--expected-instance".to_string(),
                expected_instance.to_string(),
                "--confirm-approval".to_string(),
                confirm_approval.to_string(),
                "--operator-approved".to_string(),
            ],
        )
    };

    let config = ApprovedHostConfig::new(
        &fx.registry_dir,
        &fx.plans_dir,
        &fx.authority_dir,
        &fx.sessions_dir,
        &fx.approval.approval_id,
    );
    let host = ApprovedHost::start(config.clone())
        .await
        .expect("start approved host for session CLI lifecycle test");

    let ready: HostSessionSummary = parse_envelope(
        &run_session(&summary_args).await,
        "ready approved session summary",
        &fx.binding_id,
    );
    assert_eq!(ready.phase, HostPhase::Ready);
    assert!(ready.gates.supervisor_authenticated_ready);
    assert!(!ready.notifications.enabled);
    let prior_instance_id = ready
        .instance_id
        .clone()
        .expect("Ready session must expose an instance_id");

    let wrong_instance_id = format!("hst_{}", "0".repeat(32));
    assert_ne!(wrong_instance_id, prior_instance_id);
    let wrong_digest = different_digest(&fx.approval.subject_digest);

    let wrong_digest_output = run_session(&stop_args(&prior_instance_id, &wrong_digest)).await;
    expect_failure_code(
        &wrong_digest_output,
        "stop with a digest that differs from the stored approval",
        "subject_mismatch",
    );
    assert!(
        !fx.worker_marker.exists(),
        "rejected stop confirmation must not launch a worker"
    );
    let still_ready: HostSessionSummary = parse_envelope(
        &run_session(&summary_args).await,
        "session summary after rejected wrong-digest stop",
        &fx.binding_id,
    );
    assert_eq!(still_ready.phase, HostPhase::Ready);
    assert!(still_ready.gates.supervisor_authenticated_ready);
    assert_eq!(
        still_ready.instance_id.as_deref(),
        Some(prior_instance_id.as_str())
    );

    let wrong_instance_output =
        run_session(&stop_args(&wrong_instance_id, &fx.approval.subject_digest)).await;
    expect_failure_code(
        &wrong_instance_output,
        "stop with a stale expected host instance",
        "host_instance_mismatch",
    );
    assert!(
        !fx.worker_marker.exists(),
        "rejected stop instance must not launch a worker"
    );
    let still_ready: HostSessionSummary = parse_envelope(
        &run_session(&summary_args).await,
        "session summary after rejected wrong-instance stop",
        &fx.binding_id,
    );
    assert_eq!(still_ready.phase, HostPhase::Ready);
    assert!(still_ready.gates.supervisor_authenticated_ready);
    assert_eq!(
        still_ready.instance_id.as_deref(),
        Some(prior_instance_id.as_str())
    );

    // The owner handle must drive the stop latch, mirroring host CLI `run_serve`.
    let owner = tokio::spawn(async move {
        host.wait_for_stop_request().await;
        host.shutdown().await
    });

    let stopped: HostSessionSummary = parse_envelope(
        &run_session(&stop_args(&prior_instance_id, &fx.approval.subject_digest)).await,
        "exact approved session stop",
        &fx.binding_id,
    );
    assert_eq!(stopped.phase, HostPhase::Stopped);
    assert!(!stopped.gates.supervisor_authenticated_ready);
    assert_eq!(
        stopped.instance_id.as_deref(),
        Some(prior_instance_id.as_str())
    );
    owner
        .await
        .expect("host owner task must join")
        .expect("host owner must shut down cleanly");
    assert!(
        !fx.worker_marker.exists(),
        "exact stop must not dispatch or invoke a worker"
    );

    let queried_stopped: HostSessionSummary = parse_envelope(
        &run_session(&summary_args).await,
        "stopped approved session summary",
        &fx.binding_id,
    );
    assert_eq!(queried_stopped.phase, HostPhase::Stopped);
    assert!(!queried_stopped.gates.supervisor_authenticated_ready);
    assert_eq!(
        queried_stopped.instance_id.as_deref(),
        Some(prior_instance_id.as_str())
    );
    assert_eq!(queried_stopped.approval_id, fx.approval.approval_id);

    let duplicate_stopped: HostSessionSummary = parse_envelope(
        &run_session(&stop_args(&prior_instance_id, &fx.approval.subject_digest)).await,
        "duplicate stop of the already stopped session",
        &fx.binding_id,
    );
    assert_eq!(duplicate_stopped, queried_stopped);
    assert!(
        !fx.worker_marker.exists(),
        "duplicate stop must not dispatch or invoke a worker"
    );

    let restarted = ApprovedHost::restart_stopped(config, &prior_instance_id)
        .await
        .expect("restart the explicitly stopped approved host");
    let ready_after_restart: HostSessionSummary = parse_envelope(
        &run_session(&summary_args).await,
        "restarted approved session summary",
        &fx.binding_id,
    );
    assert_eq!(ready_after_restart.phase, HostPhase::Ready);
    assert!(ready_after_restart.gates.supervisor_authenticated_ready);
    assert_ne!(
        ready_after_restart.instance_id.as_deref(),
        Some(prior_instance_id.as_str())
    );
    assert!(ready_after_restart.instance_id.is_some());
    assert!(
        !ready_after_restart.notifications.enabled,
        "restarted hosts must default to notifications disabled"
    );

    restarted
        .shutdown()
        .await
        .expect("shut down the restarted approved host");
    assert!(
        !fx.worker_marker.exists(),
        "host restart checks must not dispatch or invoke a worker"
    );
    fx.shutdown().await;
    assert!(
        !fx.worker_marker.exists(),
        "fixture control shutdown must not invoke a worker"
    );
}
