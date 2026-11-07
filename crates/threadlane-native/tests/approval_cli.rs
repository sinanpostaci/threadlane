#![cfg(unix)]
//! End-to-end behavioral tests for the operator-only `threadlane-approve` CLI and the
//! corresponding `AuthorityStore` operator helpers.
//!
//! These tests use a real local supervisor, disposable Git repository, private registry/plan/
//! authority stores, and a fake OpenCode executable. The fake executable writes an invocation
//! marker if it is ever launched; preparing or recording an approval must never invoke a worker,
//! start a provider, create a worktree, or call a paid model.

use std::fs;
use std::os::unix::fs::{symlink, PermissionsExt};
use std::path::{Path, PathBuf};
use std::process::{Output, Stdio};
use std::sync::Arc;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tempfile::TempDir;
use threadlane::{Supervisor, SupervisorClient, SupervisorConfig, SupervisorHandle};
use threadlane_authority::{
    ApprovalRecord, ApprovalSummary, AuthorityError, AuthorityStore, OperatorApprovalIntent,
    OperatorApprovalPreview,
};
use threadlane_native::{PlanStore, RegistryStore};
use threadlane_workers::{
    resolve_exact_commit_sha, run_sanitized_git, ExecutionBudgets, WorkerFilePermissions,
    WorkerProvider, DEFAULT_GIT_COMMAND_TIMEOUT_MS,
};
use threadlane_workflow::{
    compile_plan_revision, Difficulty, ModelChoice, ModelDefinition, PlanChange, PlanStatus,
    PlanTask, RegistryChange, RoleAssignment, ToolAdapter, ToolDefinition, WorkflowProfile,
    WorkflowRole,
};
use tokio::process::Command;

const THREAD_UUID: &str = "550e8400-e29b-41d4-a716-446655440000";
const OTHER_THREAD_UUID: &str = "6ba7b810-9dad-11d1-80b4-00c04fd430c8";
const BINDING_LABEL: &str = "approval-cli-chat";
const PROFILE_ID: &str = "profile-approval-cli";
const PLAN_ID: &str = "plan-approval-cli";
const MAX_REQUEST_BYTES: usize = 1_048_576;
const SECRET_SENTINEL: &str = "THREADLANE_APPROVAL_SECRET_SENTINEL_7d1f6e4a";

struct ApprovalFixture {
    _tmp: TempDir,
    root: PathBuf,
    repo: PathBuf,
    base_commit: String,
    fake_worker: PathBuf,
    worker_marker: PathBuf,
    state_dir: PathBuf,
    registry_dir: PathBuf,
    plans_dir: PathBuf,
    authority_dir: PathBuf,
    requests_dir: PathBuf,
    registry: Arc<RegistryStore>,
    plans: Arc<PlanStore>,
    supervisor: Option<SupervisorHandle>,
    binding_id: String,
    registry_revision: u64,
    plan_revision: u64,
    plan_digest: String,
}

impl ApprovalFixture {
    async fn start() -> Self {
        let tmp = TempDir::new().expect("create private approval tempdir");
        let root = fs::canonicalize(tmp.path()).expect("canonicalize approval tempdir");
        fs::set_permissions(&root, fs::Permissions::from_mode(0o700))
            .expect("chmod approval tempdir 0700");

        let repo = root.join("repo");
        let base_commit = init_disposable_repo(&repo);

        let bin_dir = root.join("bin");
        fs::create_dir_all(&bin_dir).expect("create fixture bin directory");
        fs::set_permissions(&bin_dir, fs::Permissions::from_mode(0o700))
            .expect("chmod fixture bin directory 0700");
        let fake_worker = bin_dir.join("fake-opencode");
        let worker_marker = root.join("worker-invoked");
        write_fake_worker(&fake_worker, &worker_marker);

        let state_dir = root.join("runtime");
        let registry_dir = root.join("registry-store");
        let plans_dir = root.join("plans-store");
        let authority_dir = root.join("authority-store");
        let requests_dir = root.join("requests");

        let supervisor = Supervisor::start(SupervisorConfig::new(state_dir.clone()))
            .await
            .expect("start disposable supervisor");
        let supervisor_client =
            SupervisorClient::from_state_dir(&state_dir).expect("connect supervisor client");
        let binding = supervisor_client
            .create_binding(THREAD_UUID, BINDING_LABEL)
            .await
            .expect("create native client binding before planning");

        let registry = Arc::new(RegistryStore::open(&registry_dir).expect("open registry store"));
        let plans = Arc::new(PlanStore::open(&plans_dir).expect("open plan store"));

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
            .expect("upsert fake OpenCode tool");
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
            .expect("upsert actual DeepSeek Flash model pin");
        let s3 = registry
            .apply(
                s2.revision,
                RegistryChange::UpsertProfile {
                    profile: WorkflowProfile {
                        decision_advisor: None,
                        id: PROFILE_ID.to_string(),
                        name: "Approval CLI fixture profile".to_string(),
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
            .expect("upsert fixture profile");
        let registry_snapshot = registry
            .apply(
                s3.revision,
                RegistryChange::SetDefaultProfile {
                    profile_id: Some(PROFILE_ID.to_string()),
                },
            )
            .expect("pin fixture default profile");

        let draft = plans
            .change(
                0,
                PlanChange::SaveDraft {
                    plan_id: PLAN_ID.to_string(),
                    binding_id: binding.binding_id.clone(),
                    profile_id: PROFILE_ID.to_string(),
                    profile_revision: registry_snapshot.revision,
                    summary: "Approval CLI fixture plan".to_string(),
                    tasks: plan_tasks(),
                },
            )
            .expect("save fixture plan draft");
        let published = plans
            .change(
                draft.revision,
                PlanChange::Publish {
                    plan_id: PLAN_ID.to_string(),
                    revision: draft.revision,
                },
            )
            .expect("publish fixture plan");
        let compiled = compile_plan_revision(&published, &registry_snapshot)
            .expect("compile published fixture plan");

        Self {
            _tmp: tmp,
            root,
            repo,
            base_commit,
            fake_worker,
            worker_marker,
            state_dir,
            registry_dir,
            plans_dir,
            authority_dir,
            requests_dir,
            registry,
            plans,
            supervisor: Some(supervisor),
            binding_id: binding.binding_id,
            registry_revision: registry_snapshot.revision,
            plan_revision: published.revision,
            plan_digest: compiled.plan_digest,
        }
    }

    fn intent(&self) -> OperatorApprovalIntent {
        OperatorApprovalIntent {
            binding_id: self.binding_id.clone(),
            thread_uuid: THREAD_UUID.to_string(),
            plan_id: PLAN_ID.to_string(),
            plan_revision: self.plan_revision,
            expected_registry_revision: self.registry_revision,
            repository_path: self.repo.clone(),
            base_commit: self.base_commit.clone(),
            approved_task_ids: vec!["task-child".to_string(), "task-root".to_string()],
            approved_write_paths: vec!["src/lib.rs".to_string(), "src/extra.rs".to_string()],
            max_parallel: 2,
            budgets: ExecutionBudgets::default(),
        }
    }

    fn store_args(&self) -> Vec<String> {
        vec![
            "--state-dir".to_string(),
            self.state_dir.display().to_string(),
            "--registry-dir".to_string(),
            self.registry_dir.display().to_string(),
            "--plans-dir".to_string(),
            self.plans_dir.display().to_string(),
            "--authority-dir".to_string(),
            self.authority_dir.display().to_string(),
        ]
    }

    fn prepare_args(&self, request: &Path) -> Vec<String> {
        let mut args = vec!["prepare".to_string()];
        args.extend(self.store_args());
        args.push("--request".to_string());
        args.push(request.display().to_string());
        args
    }

    fn prepare_args_with_authority(&self, request: &Path, authority_dir: &Path) -> Vec<String> {
        vec![
            "prepare".to_string(),
            "--state-dir".to_string(),
            self.state_dir.display().to_string(),
            "--registry-dir".to_string(),
            self.registry_dir.display().to_string(),
            "--plans-dir".to_string(),
            self.plans_dir.display().to_string(),
            "--authority-dir".to_string(),
            authority_dir.display().to_string(),
            "--request".to_string(),
            request.display().to_string(),
        ]
    }

    fn record_args(
        &self,
        request: &Path,
        confirm_subject: &str,
        confirm_thread: &str,
        operator_approved: bool,
    ) -> Vec<String> {
        let mut args = vec!["record".to_string()];
        args.extend(self.store_args());
        args.push("--request".to_string());
        args.push(request.display().to_string());
        args.push("--confirm-subject".to_string());
        args.push(confirm_subject.to_string());
        args.push("--confirm-thread".to_string());
        args.push(confirm_thread.to_string());
        if operator_approved {
            args.push("--operator-approved".to_string());
        }
        args
    }

    async fn run_cli(&self, args: &[String]) -> Output {
        approval_command()
            .args(args)
            .output()
            .await
            .expect("execute threadlane-approve")
    }

    async fn run_cli_in(&self, current_dir: &Path, args: &[String]) -> Output {
        approval_command()
            .current_dir(current_dir)
            .args(args)
            .output()
            .await
            .expect("execute threadlane-approve in fixture directory")
    }

    async fn run_prepare(&self, request: &Path) -> Output {
        self.run_cli(&self.prepare_args(request)).await
    }

    async fn run_record(
        &self,
        request: &Path,
        confirm_subject: &str,
        confirm_thread: &str,
        operator_approved: bool,
    ) -> Output {
        self.run_cli(&self.record_args(request, confirm_subject, confirm_thread, operator_approved))
            .await
    }

    async fn prepare_ok(&self, name: &str) -> OperatorApprovalPreview {
        let path = self.write_request(name, &self.intent());
        parse_success(&self.run_prepare(&path).await, "prepare")
    }

    fn write_request<T: Serialize>(&self, name: &str, request: &T) -> PathBuf {
        let path = self.requests_dir.join(name);
        let bytes = serde_json::to_vec(request).expect("serialize private fixture request");
        write_private_bytes(&path, &bytes);
        path
    }

    fn write_request_bytes(&self, name: &str, bytes: &[u8]) -> PathBuf {
        let path = self.requests_dir.join(name);
        write_private_bytes(&path, bytes);
        path
    }

    fn authority_store(&self) -> AuthorityStore {
        AuthorityStore::open(
            &self.authority_dir,
            Arc::clone(&self.registry),
            Arc::clone(&self.plans),
        )
        .expect("open fixture authority store")
    }

    fn advance_registry(&self) -> u64 {
        self.registry
            .apply(
                self.registry_revision,
                RegistryChange::SetDefaultProfile {
                    profile_id: Some(PROFILE_ID.to_string()),
                },
            )
            .expect("advance registry revision after preview")
            .revision
    }

    fn publish_superseding_plan(&self) -> u64 {
        let mut tasks = plan_tasks();
        tasks[0].instruction =
            "Revised instruction published after the operator preview.".to_string();
        let draft = self
            .plans
            .change(
                self.plan_revision,
                PlanChange::SaveDraft {
                    plan_id: PLAN_ID.to_string(),
                    binding_id: self.binding_id.clone(),
                    profile_id: PROFILE_ID.to_string(),
                    profile_revision: self.registry_revision,
                    summary: "Superseding approval CLI fixture plan".to_string(),
                    tasks,
                },
            )
            .expect("save superseding plan draft");
        let published = self
            .plans
            .change(
                draft.revision,
                PlanChange::Publish {
                    plan_id: PLAN_ID.to_string(),
                    revision: draft.revision,
                },
            )
            .expect("publish superseding plan");
        published.revision
    }

    fn plan_status(&self) -> PlanStatus {
        self.plans
            .get_revision(PLAN_ID, self.plan_revision)
            .expect("read fixture plan revision")
            .expect("fixture plan revision exists")
            .status
    }

    fn head(&self) -> String {
        resolve_exact_commit_sha(&self.repo, "HEAD", DEFAULT_GIT_COMMAND_TIMEOUT_MS)
            .expect("resolve fixture repository HEAD")
    }

    fn worktree_names(&self) -> Vec<String> {
        let dir = self.repo.join(".git/worktrees");
        if !dir.exists() {
            return Vec::new();
        }
        let mut names = fs::read_dir(&dir)
            .expect("read fixture git worktrees directory")
            .map(|entry| {
                entry
                    .expect("read fixture git worktree entry")
                    .file_name()
                    .into_string()
                    .expect("fixture worktree name must be UTF-8")
            })
            .collect::<Vec<_>>();
        names.sort();
        names
    }

    fn approvals_path(&self) -> PathBuf {
        self.authority_dir.join("approvals.json")
    }

    fn approval_entries(&self) -> Vec<Value> {
        let path = self.approvals_path();
        if !path.exists() {
            return Vec::new();
        }
        let bytes = fs::read(&path).expect("read approvals ledger");
        let ledger: Value = serde_json::from_slice(&bytes).expect("parse approvals ledger");
        ledger
            .get("approvals")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default()
    }

    fn approval_count(&self) -> usize {
        self.approval_entries().len()
    }

    fn persisted_record(&self) -> ApprovalRecord {
        let entries = self.approval_entries();
        assert_eq!(entries.len(), 1, "expected exactly one persisted approval");
        serde_json::from_value(entries[0].clone()).expect("deserialize persisted ApprovalRecord")
    }

    fn assert_no_worker_invocation(&self) {
        assert!(
            !self.worker_marker.exists(),
            "prepare/record must never launch the configured worker executable"
        );
    }

    fn assert_no_approval_artifacts(&self) {
        assert!(
            !self.approvals_path().exists(),
            "rejected operator request must not create approvals.json"
        );
        assert_eq!(self.approval_count(), 0);
        self.assert_no_worker_invocation();
        assert_eq!(
            self.head(),
            self.base_commit,
            "prepare/record must not move the repository HEAD"
        );
        assert!(
            self.worktree_names().is_empty(),
            "prepare/record must not create worker worktrees"
        );
    }

    async fn shutdown(&mut self) {
        if let Some(supervisor) = self.supervisor.take() {
            supervisor.shutdown().await;
        }
    }
}

#[derive(Debug, Deserialize)]
struct CliFailure {
    code: String,
    message: String,
}

fn plan_tasks() -> Vec<PlanTask> {
    vec![
        PlanTask {
            id: "task-root".to_string(),
            title: "Root approval slice".to_string(),
            instruction: "Implement the root approval slice.".to_string(),
            role: WorkflowRole::Coder,
            difficulty: Difficulty::Easy,
            depends_on: Vec::new(),
            allowed_write_paths: vec!["src/lib.rs".to_string()],
        },
        PlanTask {
            id: "task-child".to_string(),
            title: "Dependent approval slice".to_string(),
            instruction: "Implement the dependent approval slice.".to_string(),
            role: WorkflowRole::Coder,
            difficulty: Difficulty::Easy,
            depends_on: vec!["task-root".to_string()],
            allowed_write_paths: vec!["src/extra.rs".to_string()],
        },
        PlanTask {
            id: "task-unselected".to_string(),
            title: "Unselected approval slice".to_string(),
            instruction: "This task must never be auto-selected by approval.".to_string(),
            role: WorkflowRole::Coder,
            difficulty: Difficulty::Easy,
            depends_on: Vec::new(),
            allowed_write_paths: vec!["src/other.rs".to_string()],
        },
    ]
}

fn init_disposable_repo(repo: &Path) -> String {
    fs::create_dir_all(repo.join("src")).expect("create fixture repository src");
    fs::set_permissions(repo, fs::Permissions::from_mode(0o700))
        .expect("chmod fixture repository 0700");
    fs::write(
        repo.join("src/lib.rs"),
        "pub fn approved_root() -> u32 { 1 }\n",
    )
    .expect("write fixture src/lib.rs");
    fs::write(
        repo.join("src/extra.rs"),
        "pub fn approved_child() -> u32 { 2 }\n",
    )
    .expect("write fixture src/extra.rs");
    fs::write(
        repo.join("src/other.rs"),
        "pub fn unselected() -> u32 { 3 }\n",
    )
    .expect("write fixture src/other.rs");

    run_git(repo, &["init", "--quiet", "-b", "main"]);
    run_git(
        repo,
        &[
            "-c",
            "user.name=Threadlane Approval Fixture",
            "-c",
            "user.email=fixture@threadlane.local",
            "-c",
            "commit.gpgsign=false",
            "add",
            "--",
            "src/lib.rs",
            "src/extra.rs",
            "src/other.rs",
        ],
    );
    run_git(
        repo,
        &[
            "-c",
            "user.name=Threadlane Approval Fixture",
            "-c",
            "user.email=fixture@threadlane.local",
            "-c",
            "commit.gpgsign=false",
            "commit",
            "--quiet",
            "-m",
            "Initial approval CLI fixture commit",
        ],
    );
    resolve_exact_commit_sha(repo, "HEAD", DEFAULT_GIT_COMMAND_TIMEOUT_MS)
        .expect("resolve initial fixture commit")
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

fn write_private_bytes(path: &Path, bytes: &[u8]) {
    let parent = path.parent().expect("request path has a parent directory");
    fs::create_dir_all(parent).expect("create private request parent directory");
    fs::set_permissions(parent, fs::Permissions::from_mode(0o700))
        .expect("chmod private request parent directory 0700");
    fs::write(path, bytes).expect("write private request file");
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))
        .expect("chmod private request file 0600");
}

fn approval_command() -> Command {
    let mut command = Command::new(PathBuf::from(env!("CARGO_BIN_EXE_threadlane-approve")));
    command
        .env("GIT_TERMINAL_PROMPT", "0")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_SYSTEM", "/dev/null")
        .env("GIT_CONFIG_GLOBAL", "/dev/null");
    command
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
        output.stdout.len() <= MAX_REQUEST_BYTES,
        "{context}: serialized stdout exceeded the 1 MiB bound"
    );
    serde_json::from_slice(&output.stdout).unwrap_or_else(|err| {
        panic!(
            "{context}: stdout must contain one DTO JSON document: {err}\nstdout: {}",
            String::from_utf8_lossy(&output.stdout)
        )
    })
}

fn expect_failure(output: &Output, context: &str) -> CliFailure {
    assert!(
        !output.status.success(),
        "{context}: expected a nonzero exit, got success\nstdout: {}",
        String::from_utf8_lossy(&output.stdout)
    );
    assert!(
        output.stdout.is_empty(),
        "{context}: rejected requests must not write a DTO to stdout\nstdout: {}",
        String::from_utf8_lossy(&output.stdout)
    );
    assert!(
        output.stderr.len() <= MAX_REQUEST_BYTES,
        "{context}: stderr exceeded the bounded 1 MiB error envelope"
    );
    let failure: CliFailure = serde_json::from_slice(&output.stderr).unwrap_or_else(|err| {
        panic!(
            "{context}: stderr must contain one safe JSON error envelope: {err}\nstderr: {}",
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

fn assert_no_secret_echo(output: &Output, context: &str) {
    for (stream, bytes) in [
        ("stdout", output.stdout.as_slice()),
        ("stderr", output.stderr.as_slice()),
    ] {
        let text = String::from_utf8_lossy(bytes);
        assert!(
            !text.contains(SECRET_SENTINEL),
            "{context}: raw request secret was echoed on {stream}: {text}"
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

fn assert_mode(path: &Path, expected: u32, label: &str) {
    let mode = fs::symlink_metadata(path)
        .unwrap_or_else(|err| panic!("stat {label} {}: {err}", path.display()))
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(mode, expected, "{label} permissions must be {expected:04o}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn prepare_freezes_exact_scope_without_approval_execution_or_worktree_side_effects() {
    let mut fx = ApprovalFixture::start().await;

    // A child cannot be selected without its transitive predecessor; prepare must reject rather
    // than silently expanding the requested scope.
    let mut missing_predecessor = fx.intent();
    missing_predecessor.approved_task_ids = vec!["task-child".to_string()];
    let missing_path = fx.write_request("prepare-missing-predecessor.json", &missing_predecessor);
    let missing_output = fx.run_prepare(&missing_path).await;
    let missing_failure = expect_failure(
        &missing_output,
        "prepare without the transitive DAG predecessor",
    );
    assert!(!missing_failure.message.trim().is_empty());
    fx.assert_no_approval_artifacts();

    // The plan binding is unchanged, but the request thread does not match the live configured
    // binding. Prepare must fail closed rather than returning an operator preview for another
    // native thread.
    let mut wrong_live_thread = fx.intent();
    wrong_live_thread.thread_uuid = OTHER_THREAD_UUID.to_string();
    let wrong_live_thread_path =
        fx.write_request("prepare-wrong-live-thread.json", &wrong_live_thread);
    let wrong_live_thread_output = fx.run_prepare(&wrong_live_thread_path).await;
    expect_failure(
        &wrong_live_thread_output,
        "prepare against a live binding with a different configured thread",
    );
    fx.assert_no_approval_artifacts();

    let preview = fx.prepare_ok("prepare-valid.json").await;

    assert_eq!(preview.request.binding_id, fx.binding_id);
    assert_eq!(preview.request.thread_uuid, THREAD_UUID);
    assert_eq!(preview.request.plan_id, PLAN_ID);
    assert_eq!(preview.request.plan_revision, fx.plan_revision);
    assert_eq!(
        preview.request.expected_registry_revision,
        fx.registry_revision
    );
    assert_eq!(preview.request.expected_plan_digest, fx.plan_digest);
    assert_eq!(preview.request.repository_path, fx.repo);
    assert_eq!(preview.request.base_commit, fx.base_commit);
    assert_eq!(
        preview.request.approved_task_ids,
        vec!["task-root".to_string(), "task-child".to_string()],
        "prepare must freeze only the requested tasks in deterministic compiled plan order"
    );
    assert_eq!(
        preview.request.approved_write_paths,
        vec!["src/lib.rs".to_string(), "src/extra.rs".to_string()]
    );
    assert_eq!(preview.request.max_parallel, 2);
    assert_eq!(preview.request.budgets, ExecutionBudgets::default());
    assert_sha256(&preview.subject_digest, "preview subject_digest");

    assert_eq!(preview.tasks.len(), 2);
    assert_eq!(preview.tasks[0].task_id, "task-root");
    assert_eq!(preview.tasks[1].task_id, "task-child");
    assert!(
        !preview
            .tasks
            .iter()
            .any(|task| task.task_id == "task-unselected"),
        "prepare must never auto-select an unrequested plan task"
    );

    let root = &preview.tasks[0];
    assert_eq!(root.title, "Root approval slice");
    assert_eq!(root.instruction, "Implement the root approval slice.");
    assert_sha256(&root.instruction_digest, "root instruction_digest");
    assert_eq!(root.role, WorkflowRole::Coder);
    assert_eq!(root.difficulty, Difficulty::Easy);
    assert_eq!(root.tool_id, "tool-opencode");
    assert_eq!(root.adapter, ToolAdapter::OpencodeV2);
    assert_eq!(root.provider, Some(WorkerProvider::OpenCodeV2));
    assert_eq!(
        root.canonical_executable.as_deref(),
        Some(fx.fake_worker.as_path())
    );
    assert_eq!(root.model_id, "model-deepseek-flash");
    assert_eq!(root.model_arg, "opencode-go/deepseek-v4.1-flash");
    assert_eq!(root.base_model, "opencode-go/deepseek-v4.1-flash");
    assert_eq!(root.effort.as_deref(), Some("max"));
    assert_eq!(
        root.cli_model_arg.as_deref(),
        Some("opencode-go/deepseek-v4.1-flash#max")
    );
    assert_eq!(root.standalone, Some(true));
    assert_eq!(
        root.file_permissions,
        Some(WorkerFilePermissions::TaskFiles)
    );
    assert!(root.depends_on.is_empty());
    assert_eq!(root.allowed_write_paths, vec!["src/lib.rs".to_string()]);
    assert_sha256(&root.task_digest, "root task_digest");
    assert!(root.profile_id.is_some());
    assert!(root.command_key.is_some());

    let child = &preview.tasks[1];
    assert_eq!(child.depends_on, vec!["task-root".to_string()]);
    assert_eq!(child.allowed_write_paths, vec!["src/extra.rs".to_string()]);
    assert_eq!(child.model_arg, "opencode-go/deepseek-v4.1-flash");
    assert_eq!(child.effort.as_deref(), Some("max"));

    fx.assert_no_approval_artifacts();
    fx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn record_freezes_exact_tasks_and_exact_retry_is_idempotent() {
    let mut fx = ApprovalFixture::start().await;
    let preview = fx.prepare_ok("prepare-record.json").await;
    let request_path = fx.write_request("record-valid.json", &preview.request);
    assert_mode(&fx.requests_dir, 0o700, "private request directory");
    assert_mode(&request_path, 0o600, "private request file");

    let summary: ApprovalSummary = parse_success(
        &fx.run_record(&request_path, &preview.subject_digest, THREAD_UUID, true)
            .await,
        "record valid operator approval",
    );

    assert!(summary.approval_id.starts_with("apr_"));
    assert_eq!(summary.subject_digest, preview.subject_digest);
    assert_eq!(summary.binding_id, fx.binding_id);
    assert_eq!(summary.thread_uuid, THREAD_UUID);
    assert_eq!(summary.plan_id, PLAN_ID);
    assert_eq!(summary.plan_revision, fx.plan_revision);
    assert_eq!(summary.registry_revision, fx.registry_revision);
    assert_eq!(summary.plan_digest, fx.plan_digest);
    assert_eq!(summary.repository_path, fx.repo);
    assert_eq!(summary.base_commit, fx.base_commit);
    assert_eq!(
        summary.approved_task_ids,
        vec!["task-root".to_string(), "task-child".to_string()]
    );
    assert_eq!(
        summary.approved_write_paths,
        vec!["src/lib.rs".to_string(), "src/extra.rs".to_string()]
    );
    assert_eq!(summary.max_parallel, 2);
    assert_eq!(summary.worker_profile_count, 2);
    assert_eq!(summary.native_route_count, 0);
    assert!(summary.created_at_ms > 0);
    assert_eq!(fx.approval_count(), 1);

    let persisted = fx.persisted_record();
    assert_eq!(persisted.approval_id, summary.approval_id);
    assert_eq!(persisted.subject_digest, preview.subject_digest);
    assert_eq!(persisted.tasks, preview.tasks);
    assert_eq!(persisted.budgets, ExecutionBudgets::default());
    assert_eq!(persisted.max_parallel, 2);
    assert_eq!(persisted.repository_path, fx.repo);
    assert_eq!(persisted.base_commit, fx.base_commit);
    assert_eq!(persisted.created_at_ms, summary.created_at_ms);
    assert_mode(&fx.authority_dir, 0o700, "authority store directory");
    assert_mode(&fx.approvals_path(), 0o600, "approvals ledger");

    let repeated: ApprovalSummary = parse_success(
        &fx.run_record(&request_path, &preview.subject_digest, THREAD_UUID, true)
            .await,
        "idempotent exact record retry",
    );
    assert_eq!(repeated, summary);
    assert_eq!(fx.approval_count(), 1);
    assert_eq!(fx.plan_status(), PlanStatus::Published);
    fx.assert_no_worker_invocation();
    assert_eq!(fx.head(), fx.base_commit);
    assert!(fx.worktree_names().is_empty());

    fx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn changed_scope_budget_and_concurrency_require_fresh_confirmation() {
    let mut fx = ApprovalFixture::start().await;
    let preview = fx.prepare_ok("prepare-changed-confirmation.json").await;
    let old_subject_digest = preview.subject_digest.clone();
    let store = fx.authority_store();

    let mut broader_scope = preview.request.clone();
    broader_scope.approved_write_paths = vec!["src/".to_string()];
    let mut changed_budget = preview.request.clone();
    changed_budget.budgets.timeout_ms = 120_000;
    let mut changed_concurrency = preview.request.clone();
    changed_concurrency.max_parallel = 1;
    let mut changed_task_selection = preview.request.clone();
    changed_task_selection.approved_task_ids = vec!["task-root".to_string()];
    changed_task_selection.approved_write_paths = vec!["src/lib.rs".to_string()];

    for (label, changed_request) in [
        ("scope", broader_scope),
        ("budget", changed_budget),
        ("concurrency", changed_concurrency),
        ("task-selection", changed_task_selection),
    ] {
        let path = fx.write_request(&format!("record-changed-{label}.json"), &changed_request);
        let output = fx
            .run_record(&path, &old_subject_digest, THREAD_UUID, true)
            .await;
        expect_failure(
            &output,
            "record with an old confirmation after changing the approved request",
        );
        assert_eq!(
            fx.approval_count(),
            0,
            "old confirmation must not persist a changed {label} request"
        );

        // The changed request is well-formed and still valid at the authority layer; only its
        // frozen subject digest differs. This distinguishes a confirmation mismatch from merely
        // malformed JSON or an invalid DTO.
        let changed_preview = store
            .preview_for_operator(changed_request.clone())
            .expect("changed request must remain a valid operator request");
        assert_eq!(changed_preview.request, changed_request);
        assert_ne!(
            changed_preview.subject_digest, old_subject_digest,
            "changing {label} must change the frozen subject digest"
        );
        fx.assert_no_approval_artifacts();
    }
    drop(store);

    let original_path = fx.write_request("record-original-after-rejections.json", &preview.request);
    let summary: ApprovalSummary = parse_success(
        &fx.run_record(&original_path, &old_subject_digest, THREAD_UUID, true)
            .await,
        "record unchanged original request after changed-request rejections",
    );
    assert_eq!(summary.subject_digest, old_subject_digest);
    assert_eq!(fx.approval_count(), 1);
    fx.assert_no_worker_invocation();
    fx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn record_requires_operator_flag_live_binding_and_exact_digest_and_thread() {
    let mut fx = ApprovalFixture::start().await;
    let preview = fx.prepare_ok("prepare-confirmations.json").await;
    let request_path = fx.write_request("record-confirmations.json", &preview.request);

    let missing_flag = fx
        .run_record(&request_path, &preview.subject_digest, THREAD_UUID, false)
        .await;
    expect_failure(&missing_flag, "record without --operator-approved");
    fx.assert_no_approval_artifacts();

    let wrong_digest = fx
        .run_record(
            &request_path,
            &different_digest(&preview.subject_digest),
            THREAD_UUID,
            true,
        )
        .await;
    expect_failure(&wrong_digest, "record with a mismatched subject digest");
    fx.assert_no_approval_artifacts();

    for malformed_digest in ["a".repeat(63), "A".repeat(64), "z".repeat(64)] {
        let output = fx
            .run_record(&request_path, &malformed_digest, THREAD_UUID, true)
            .await;
        expect_failure(&output, "record with a malformed subject digest");
        fx.assert_no_approval_artifacts();
    }

    let wrong_confirmed_thread = fx
        .run_record(
            &request_path,
            &preview.subject_digest,
            OTHER_THREAD_UUID,
            true,
        )
        .await;
    expect_failure(
        &wrong_confirmed_thread,
        "record with a confirmation thread different from the request thread",
    );
    fx.assert_no_approval_artifacts();

    // Build a fully valid request/preview for a different thread. The plan still belongs to the
    // same binding, but the live supervisor binding is configured for THREAD_UUID. The CLI must
    // fail closed against the live binding even though the request's own subject digest matches.
    let mut wrong_live_thread = preview.request.clone();
    wrong_live_thread.thread_uuid = OTHER_THREAD_UUID.to_string();
    let wrong_live_thread_preview = fx
        .authority_store()
        .preview_for_operator(wrong_live_thread.clone())
        .expect("wrong-thread request is structurally valid at the library layer");
    assert_eq!(wrong_live_thread_preview.request, wrong_live_thread);
    let wrong_live_thread_path =
        fx.write_request("record-wrong-live-thread.json", &wrong_live_thread);
    let wrong_live_thread_output = fx
        .run_record(
            &wrong_live_thread_path,
            &wrong_live_thread_preview.subject_digest,
            OTHER_THREAD_UUID,
            true,
        )
        .await;
    expect_failure(
        &wrong_live_thread_output,
        "record against a live binding with a different configured thread",
    );
    fx.assert_no_approval_artifacts();

    let summary: ApprovalSummary = parse_success(
        &fx.run_record(&request_path, &preview.subject_digest, THREAD_UUID, true)
            .await,
        "record with exact digest, thread, and operator attestation",
    );
    assert_eq!(summary.subject_digest, preview.subject_digest);
    assert_eq!(summary.thread_uuid, THREAD_UUID);
    assert_eq!(fx.approval_count(), 1);
    fx.assert_no_worker_invocation();
    fx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn record_and_prepare_reject_a_stale_registry_without_new_approvals() {
    let mut fx = ApprovalFixture::start().await;
    let preview = fx.prepare_ok("prepare-before-stale-registry.json").await;

    let advanced_revision = fx.advance_registry();
    assert_eq!(advanced_revision, fx.registry_revision + 1);

    let record_path = fx.write_request("record-stale-registry.json", &preview.request);
    let record_output = fx
        .run_record(&record_path, &preview.subject_digest, THREAD_UUID, true)
        .await;
    expect_failure(
        &record_output,
        "record after the pinned registry revision became stale",
    );
    fx.assert_no_approval_artifacts();

    let prepare_path = fx.write_request("prepare-stale-registry.json", &fx.intent());
    let prepare_output = fx.run_prepare(&prepare_path).await;
    expect_failure(
        &prepare_output,
        "prepare after the pinned registry revision became stale",
    );
    fx.assert_no_approval_artifacts();
    assert_eq!(fx.plan_status(), PlanStatus::Published);

    fx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn superseded_plan_and_replaced_plan_digest_are_rejected_without_new_approvals() {
    let mut fx = ApprovalFixture::start().await;
    let preview = fx.prepare_ok("prepare-before-supersede.json").await;

    let superseding_revision = fx.publish_superseding_plan();
    assert_eq!(superseding_revision, fx.plan_revision + 1);
    assert_eq!(fx.plan_status(), PlanStatus::Superseded);

    let prepare_path = fx.write_request("prepare-superseded.json", &fx.intent());
    let prepare_output = fx.run_prepare(&prepare_path).await;
    expect_failure(&prepare_output, "prepare of a superseded plan revision");
    fx.assert_no_approval_artifacts();

    let record_path = fx.write_request("record-superseded.json", &preview.request);
    let record_output = fx
        .run_record(&record_path, &preview.subject_digest, THREAD_UUID, true)
        .await;
    expect_failure(&record_output, "record of a superseded plan revision");
    fx.assert_no_approval_artifacts();

    fx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn record_fails_closed_when_the_configured_binding_is_offline() {
    let mut fx = ApprovalFixture::start().await;
    let preview = fx.prepare_ok("prepare-before-offline.json").await;
    let request_path = fx.write_request("record-offline.json", &preview.request);

    fx.shutdown().await;

    let output = fx
        .run_record(&request_path, &preview.subject_digest, THREAD_UUID, true)
        .await;
    expect_failure(&output, "record while the configured supervisor is offline");
    fx.assert_no_approval_artifacts();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn loader_rejects_invalid_json_private_modes_symlinks_oversize_and_relative_paths() {
    let mut fx = ApprovalFixture::start().await;
    let preview = fx.prepare_ok("prepare-before-loader-tests.json").await;
    let valid_path = fx.write_request("loader-valid.json", &preview.request);

    let malformed = format!("{{ this is not valid request JSON {SECRET_SENTINEL}");
    let malformed_path = fx.write_request_bytes("loader-malformed.json", malformed.as_bytes());
    let malformed_output = fx
        .run_record(&malformed_path, &preview.subject_digest, THREAD_UUID, true)
        .await;
    expect_failure(&malformed_output, "malformed request JSON");
    assert_no_secret_echo(&malformed_output, "malformed request JSON");
    fx.assert_no_approval_artifacts();

    let insecure_path = fx.write_request("loader-insecure.json", &preview.request);
    fs::set_permissions(&insecure_path, fs::Permissions::from_mode(0o644))
        .expect("chmod otherwise-valid request file 0644");
    let insecure_output = fx
        .run_record(&insecure_path, &preview.subject_digest, THREAD_UUID, true)
        .await;
    expect_failure(&insecure_output, "group-readable request file");
    fx.assert_no_approval_artifacts();

    let leaf_link = fx.requests_dir.join("loader-leaf-link.json");
    symlink(&valid_path, &leaf_link).expect("create symlink request leaf");
    let leaf_output = fx
        .run_record(&leaf_link, &preview.subject_digest, THREAD_UUID, true)
        .await;
    expect_failure(&leaf_output, "symlink request leaf");
    fx.assert_no_approval_artifacts();

    let real_ancestor = fx.requests_dir.join("real-ancestor");
    fs::create_dir_all(&real_ancestor).expect("create real request ancestor");
    fs::set_permissions(&real_ancestor, fs::Permissions::from_mode(0o700))
        .expect("chmod real request ancestor 0700");
    let ancestor_request = real_ancestor.join("request.json");
    write_private_bytes(
        &ancestor_request,
        &serde_json::to_vec(&preview.request).expect("serialize ancestor request"),
    );
    let ancestor_link = fx.requests_dir.join("linked-ancestor");
    symlink(&real_ancestor, &ancestor_link).expect("create symlink request ancestor");
    let linked_request_path = ancestor_link.join("request.json");
    let ancestor_output = fx
        .run_record(
            &linked_request_path,
            &preview.subject_digest,
            THREAD_UUID,
            true,
        )
        .await;
    expect_failure(&ancestor_output, "symlink request ancestor");
    fx.assert_no_approval_artifacts();

    let mut oversize = serde_json::to_vec(&preview.request).expect("serialize oversize request");
    oversize.resize(MAX_REQUEST_BYTES + 1, b' ');
    let oversize_path = fx.write_request_bytes("loader-oversize.json", &oversize);
    let oversize_output = fx
        .run_record(&oversize_path, &preview.subject_digest, THREAD_UUID, true)
        .await;
    expect_failure(&oversize_output, "oversize request file");
    fx.assert_no_approval_artifacts();

    let directory_path = fx.requests_dir.join("loader-directory");
    fs::create_dir_all(&directory_path).expect("create directory request path");
    let directory_output = fx
        .run_record(&directory_path, &preview.subject_digest, THREAD_UUID, true)
        .await;
    expect_failure(&directory_output, "directory request path");
    fx.assert_no_approval_artifacts();

    let relative_path = Path::new("requests/loader-valid.json");
    let relative_output = fx
        .run_cli_in(
            &fx.root,
            &fx.record_args(relative_path, &preview.subject_digest, THREAD_UUID, true),
        )
        .await;
    expect_failure(&relative_output, "relative request path");
    fx.assert_no_approval_artifacts();

    let summary: ApprovalSummary = parse_success(
        &fx.run_record(&valid_path, &preview.subject_digest, THREAD_UUID, true)
            .await,
        "record after loader rejects invalid request files",
    );
    assert_eq!(summary.subject_digest, preview.subject_digest);
    assert_eq!(fx.approval_count(), 1);
    fx.assert_no_worker_invocation();
    fx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn commands_reject_overlapping_symlink_and_non_private_store_directories() {
    let mut fx = ApprovalFixture::start().await;
    let intent_path = fx.write_request("prepare-store-path-guards.json", &fx.intent());

    let overlap_output = fx
        .run_cli(&fx.prepare_args_with_authority(&intent_path, &fx.registry_dir))
        .await;
    expect_failure(
        &overlap_output,
        "authority directory overlapping the registry store",
    );
    fx.assert_no_approval_artifacts();

    fs::create_dir_all(&fx.authority_dir).expect("create real authority directory");
    fs::set_permissions(&fx.authority_dir, fs::Permissions::from_mode(0o700))
        .expect("chmod real authority directory 0700");
    let authority_link = fx.root.join("authority-link");
    symlink(&fx.authority_dir, &authority_link).expect("create authority directory symlink");
    let symlink_output = fx
        .run_cli(&fx.prepare_args_with_authority(&intent_path, &authority_link))
        .await;
    expect_failure(&symlink_output, "symlink authority store directory");
    fx.assert_no_approval_artifacts();

    let open_authority = fx.root.join("authority-open");
    fs::create_dir_all(&open_authority).expect("create non-private authority directory");
    fs::set_permissions(&open_authority, fs::Permissions::from_mode(0o755))
        .expect("chmod non-private authority directory 0755");
    let open_output = fx
        .run_cli(&fx.prepare_args_with_authority(&intent_path, &open_authority))
        .await;
    expect_failure(
        &open_output,
        "non-private authority store directory permissions",
    );
    fx.assert_no_approval_artifacts();

    fx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn record_output_failure_reports_outcome_unknown_and_exact_retry_is_idempotent() {
    let mut fx = ApprovalFixture::start().await;
    let preview = fx.prepare_ok("prepare-output-failure.json").await;
    let request_path = fx.write_request("record-output-failure.json", &preview.request);

    let dev_full = Path::new("/dev/full");
    if !dev_full.exists() {
        eprintln!("skipping stdout failure assertion: /dev/full is unavailable on this host");
        fx.shutdown().await;
        return;
    }

    let full_stdout = fs::OpenOptions::new()
        .write(true)
        .open(dev_full)
        .expect("open /dev/full for bounded stdout failure");
    let child = approval_command()
        .args(fx.record_args(&request_path, &preview.subject_digest, THREAD_UUID, true))
        .stdout(Stdio::from(full_stdout))
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn record with failing stdout");
    assert!(
        child.stdout.is_none(),
        "stdout must keep the failing file descriptor instead of a captured pipe"
    );
    let output = child
        .wait_with_output()
        .await
        .expect("wait for record with failing stdout");

    let failure = expect_failure(&output, "record whose bounded stdout write fails");
    assert_eq!(
        failure.code, "outcome_unknown",
        "a successful ledger write followed by stdout failure must report outcome_unknown"
    );
    assert_eq!(fx.approval_count(), 1);
    fx.assert_no_worker_invocation();

    let summary: ApprovalSummary = parse_success(
        &fx.run_record(&request_path, &preview.subject_digest, THREAD_UUID, true)
            .await,
        "explicit exact retry after outcome_unknown",
    );
    assert_eq!(summary.subject_digest, preview.subject_digest);
    assert_eq!(fx.approval_count(), 1);

    fx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn authority_operator_helpers_preview_read_only_and_rebuild_before_duplicate_success() {
    let mut fx = ApprovalFixture::start().await;
    let store = fx.authority_store();
    let intent = fx.intent();

    let preview = store
        .prepare_for_operator(intent.clone())
        .expect("prepare operator approval preview");
    assert_eq!(preview.request.approved_task_ids.len(), 2);
    assert_sha256(&preview.subject_digest, "API preview subject_digest");
    assert!(!fx.approvals_path().exists());
    fx.assert_no_approval_artifacts();

    let repeated_preview = store
        .preview_for_operator(preview.request.clone())
        .expect("repeat read-only operator preview");
    assert_eq!(repeated_preview, preview);
    assert!(!fx.approvals_path().exists());

    for bad_digest in [
        different_digest(&preview.subject_digest),
        "a".repeat(63),
        "A".repeat(64),
        "z".repeat(64),
    ] {
        let err = store
            .approve_confirmed(preview.request.clone(), &bad_digest)
            .expect_err("mismatched or malformed confirmation digest must fail");
        match err {
            AuthorityError::Validation { field, .. } => {
                assert_eq!(field, "expected_subject_digest");
            }
            other => panic!("expected expected_subject_digest validation error, got {other:?}"),
        }
        assert!(!fx.approvals_path().exists());
        fx.assert_no_approval_artifacts();
    }

    let summary = store
        .approve_confirmed(preview.request.clone(), &preview.subject_digest)
        .expect("approve confirmed operator request");
    assert_eq!(summary.subject_digest, preview.subject_digest);
    assert_eq!(fx.approval_count(), 1);
    assert_eq!(fx.persisted_record().tasks, preview.tasks);

    let repeated_summary = store
        .approve_confirmed(preview.request.clone(), &preview.subject_digest)
        .expect("idempotent exact confirmed retry");
    assert_eq!(repeated_summary, summary);
    assert_eq!(fx.approval_count(), 1);

    // A stale duplicate rebuilds the full candidate under the store lock before consulting an
    // existing record. It fails instead of silently returning the old approval.
    let advanced_revision = fx.advance_registry();
    assert_eq!(advanced_revision, fx.registry_revision + 1);
    let stale_duplicate = store
        .approve_confirmed(preview.request.clone(), &preview.subject_digest)
        .expect_err("stale duplicate must not return the existing approval");
    assert_eq!(stale_duplicate.code(), "stale_registry_revision");
    assert_eq!(fx.approval_count(), 1);
    fx.assert_no_worker_invocation();

    drop(store);
    fx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fixed_scope_flags_enforce_binding_thread_and_project_root_before_ledger_effects() {
    let mut fx = ApprovalFixture::start().await;
    let intent_path = fx.write_request("fixed-scope-intent.json", &fx.intent());

    // Partial fixed-scope flags (--binding without --thread, or --project-root without --binding/--thread) fail closed.
    let partial_binding_only = vec![
        "--state-dir".to_string(),
        fx.state_dir.to_string_lossy().into_owned(),
        "--registry-dir".to_string(),
        fx.registry_dir.to_string_lossy().into_owned(),
        "--plans-dir".to_string(),
        fx.plans_dir.to_string_lossy().into_owned(),
        "--authority-dir".to_string(),
        fx.authority_dir.to_string_lossy().into_owned(),
        "--binding".to_string(),
        fx.binding_id.clone(),
        "prepare".to_string(),
        "--request".to_string(),
        intent_path.to_string_lossy().into_owned(),
    ];
    let partial_err = expect_failure(
        &fx.run_cli(&partial_binding_only).await,
        "partial --binding without --thread",
    );
    assert_eq!(partial_err.code, "invalid_input");

    let partial_project_only = vec![
        "--state-dir".to_string(),
        fx.state_dir.to_string_lossy().into_owned(),
        "--registry-dir".to_string(),
        fx.registry_dir.to_string_lossy().into_owned(),
        "--plans-dir".to_string(),
        fx.plans_dir.to_string_lossy().into_owned(),
        "--authority-dir".to_string(),
        fx.authority_dir.to_string_lossy().into_owned(),
        "--project-root".to_string(),
        fx.repo.to_string_lossy().into_owned(),
        "prepare".to_string(),
        "--request".to_string(),
        intent_path.to_string_lossy().into_owned(),
    ];
    let partial_proj_err = expect_failure(
        &fx.run_cli(&partial_project_only).await,
        "partial --project-root without --binding/--thread",
    );
    assert_eq!(partial_proj_err.code, "invalid_input");

    // Fixed --binding mismatch with request binding_id is rejected before ledger effects.
    let wrong_binding_args = vec![
        "--state-dir".to_string(),
        fx.state_dir.to_string_lossy().into_owned(),
        "--registry-dir".to_string(),
        fx.registry_dir.to_string_lossy().into_owned(),
        "--plans-dir".to_string(),
        fx.plans_dir.to_string_lossy().into_owned(),
        "--authority-dir".to_string(),
        fx.authority_dir.to_string_lossy().into_owned(),
        "--binding".to_string(),
        "bnd_other_chat".to_string(),
        "--thread".to_string(),
        THREAD_UUID.to_string(),
        "--project-root".to_string(),
        fx.repo.to_string_lossy().into_owned(),
        "prepare".to_string(),
        "--request".to_string(),
        intent_path.to_string_lossy().into_owned(),
    ];
    let wrong_binding_err = expect_failure(
        &fx.run_cli(&wrong_binding_args).await,
        "fixed --binding mismatch with request",
    );
    assert_eq!(wrong_binding_err.code, "binding_mismatch");

    // Fixed --thread mismatch with request thread_uuid is rejected before ledger effects.
    let wrong_thread_args = vec![
        "--state-dir".to_string(),
        fx.state_dir.to_string_lossy().into_owned(),
        "--registry-dir".to_string(),
        fx.registry_dir.to_string_lossy().into_owned(),
        "--plans-dir".to_string(),
        fx.plans_dir.to_string_lossy().into_owned(),
        "--authority-dir".to_string(),
        fx.authority_dir.to_string_lossy().into_owned(),
        "--binding".to_string(),
        fx.binding_id.clone(),
        "--thread".to_string(),
        OTHER_THREAD_UUID.to_string(),
        "--project-root".to_string(),
        fx.repo.to_string_lossy().into_owned(),
        "prepare".to_string(),
        "--request".to_string(),
        intent_path.to_string_lossy().into_owned(),
    ];
    let wrong_thread_err = expect_failure(
        &fx.run_cli(&wrong_thread_args).await,
        "fixed --thread mismatch with request",
    );
    assert_eq!(wrong_thread_err.code, "thread_uuid_mismatch");

    // Fixed --project-root mismatch with request repository_path is rejected before ledger effects.
    let other_repo = fx.root.join("other-repo");
    let _other_commit = init_disposable_repo(&other_repo);
    let wrong_repo_args = vec![
        "--state-dir".to_string(),
        fx.state_dir.to_string_lossy().into_owned(),
        "--registry-dir".to_string(),
        fx.registry_dir.to_string_lossy().into_owned(),
        "--plans-dir".to_string(),
        fx.plans_dir.to_string_lossy().into_owned(),
        "--authority-dir".to_string(),
        fx.authority_dir.to_string_lossy().into_owned(),
        "--binding".to_string(),
        fx.binding_id.clone(),
        "--thread".to_string(),
        THREAD_UUID.to_string(),
        "--project-root".to_string(),
        other_repo.to_string_lossy().into_owned(),
        "prepare".to_string(),
        "--request".to_string(),
        intent_path.to_string_lossy().into_owned(),
    ];
    let wrong_repo_err = expect_failure(
        &fx.run_cli(&wrong_repo_args).await,
        "fixed --project-root mismatch with request repository_path",
    );
    assert_eq!(wrong_repo_err.code, "invalid_repository");
    fx.assert_no_approval_artifacts();

    // Matching fixed scope succeeds for prepare and record.
    let matching_prepare_args = vec![
        "--state-dir".to_string(),
        fx.state_dir.to_string_lossy().into_owned(),
        "--registry-dir".to_string(),
        fx.registry_dir.to_string_lossy().into_owned(),
        "--plans-dir".to_string(),
        fx.plans_dir.to_string_lossy().into_owned(),
        "--authority-dir".to_string(),
        fx.authority_dir.to_string_lossy().into_owned(),
        "--binding".to_string(),
        fx.binding_id.clone(),
        "--thread".to_string(),
        THREAD_UUID.to_string(),
        "--project-root".to_string(),
        fx.repo.to_string_lossy().into_owned(),
        "prepare".to_string(),
        "--request".to_string(),
        intent_path.to_string_lossy().into_owned(),
    ];
    let preview: OperatorApprovalPreview = parse_success(
        &fx.run_cli(&matching_prepare_args).await,
        "matching fixed-scope prepare",
    );
    let req_path = fx.write_request("fixed-scope-request.json", &preview.request);

    let matching_record_args = vec![
        "--state-dir".to_string(),
        fx.state_dir.to_string_lossy().into_owned(),
        "--registry-dir".to_string(),
        fx.registry_dir.to_string_lossy().into_owned(),
        "--plans-dir".to_string(),
        fx.plans_dir.to_string_lossy().into_owned(),
        "--authority-dir".to_string(),
        fx.authority_dir.to_string_lossy().into_owned(),
        "--binding".to_string(),
        fx.binding_id.clone(),
        "--thread".to_string(),
        THREAD_UUID.to_string(),
        "--project-root".to_string(),
        fx.repo.to_string_lossy().into_owned(),
        "record".to_string(),
        "--request".to_string(),
        req_path.to_string_lossy().into_owned(),
        "--confirm-subject".to_string(),
        preview.subject_digest.clone(),
        "--confirm-thread".to_string(),
        THREAD_UUID.to_string(),
        "--operator-approved".to_string(),
    ];
    let summary: ApprovalSummary = parse_success(
        &fx.run_cli(&matching_record_args).await,
        "matching fixed-scope record",
    );
    assert_eq!(summary.subject_digest, preview.subject_digest);
    assert_eq!(fx.approval_count(), 1);
    fx.assert_no_worker_invocation();

    fx.shutdown().await;
}
