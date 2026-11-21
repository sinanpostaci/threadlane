#![cfg(unix)]
//! End-to-end behavioral tests for the operator-only `threadlane-approve` CLI and the
//! corresponding `AuthorityStore` operator helpers.
//!
//! These tests use a real local supervisor, disposable Git repository, private registry/plan/
//! authority stores, and a fake OpenCode executable. The fake executable writes an invocation
//! marker if it is ever launched; preparing or recording an approval must never invoke a worker,
//! start a provider, create a worktree, or call a paid model.

use std::collections::BTreeMap;
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
    OperatorApprovalIntentV2, OperatorApprovalPreview, OperatorApprovalRequestV2,
    AUTHORITY_REQUEST_V2_SCHEMA_VERSION,
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
use tokio::io::AsyncWriteExt;
use tokio::process::Command;

const THREAD_UUID: &str = "550e8400-e29b-41d4-a716-446655440000";
const OTHER_THREAD_UUID: &str = "6ba7b810-9dad-11d1-80b4-00c04fd430c8";
const BINDING_LABEL: &str = "approval-cli-chat";
const PROFILE_ID: &str = "profile-approval-cli";
const PLAN_ID: &str = "plan-approval-cli";
const MAX_REQUEST_BYTES: usize = 1_048_576;
const SECRET_SENTINEL: &str = "THREADLANE_APPROVAL_SECRET_SENTINEL_7d1f6e4a";
const ORDINARY_WORKER_QUICKSTART: &str =
    include_str!("../../../docs/ORDINARY-WORKER-QUICKSTART.md");

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

    fn prepare_private_args(&self) -> Vec<String> {
        let mut args = vec!["prepare-private".to_string()];
        args.extend(self.store_args());
        args
    }

    fn private_preview_temp_dir(&self) -> PathBuf {
        let dir = self.root.join("private-previews");
        fs::create_dir_all(&dir).expect("create private preview temp dir");
        fs::set_permissions(&dir, fs::Permissions::from_mode(0o700))
            .expect("chmod private preview temp dir 0700");
        dir
    }

    async fn run_cli_with_stdin(
        &self,
        args: &[String],
        stdin: &[u8],
        temp_dir: Option<&Path>,
    ) -> Output {
        run_command_with_stdin(approval_command(), args, stdin, temp_dir).await
    }

    async fn run_prepare_private(
        &self,
        intent: &OperatorApprovalIntent,
        temp_dir: &Path,
    ) -> Output {
        let stdin = serde_json::to_vec(intent).expect("serialize strict intent for stdin");
        self.run_cli_with_stdin(&self.prepare_private_args(), &stdin, Some(temp_dir))
            .await
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

    fn record_args_for(
        &self,
        input_flag: &str,
        input: &Path,
        confirm_subject: &str,
        confirm_thread: &str,
        operator_approved: bool,
    ) -> Vec<String> {
        let mut args = vec!["record".to_string()];
        args.extend(self.store_args());
        args.push(input_flag.to_string());
        args.push(input.display().to_string());
        args.push("--confirm-subject".to_string());
        args.push(confirm_subject.to_string());
        args.push("--confirm-thread".to_string());
        args.push(confirm_thread.to_string());
        if operator_approved {
            args.push("--operator-approved".to_string());
        }
        args
    }

    fn record_args(
        &self,
        request: &Path,
        confirm_subject: &str,
        confirm_thread: &str,
        operator_approved: bool,
    ) -> Vec<String> {
        self.record_args_for(
            "--request",
            request,
            confirm_subject,
            confirm_thread,
            operator_approved,
        )
    }

    fn record_preview_args(
        &self,
        preview: &Path,
        confirm_subject: &str,
        confirm_thread: &str,
        operator_approved: bool,
    ) -> Vec<String> {
        self.record_args_for(
            "--preview",
            preview,
            confirm_subject,
            confirm_thread,
            operator_approved,
        )
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

    async fn run_record_preview(
        &self,
        preview: &Path,
        confirm_subject: &str,
        confirm_thread: &str,
        operator_approved: bool,
    ) -> Output {
        self.run_cli(&self.record_preview_args(
            preview,
            confirm_subject,
            confirm_thread,
            operator_approved,
        ))
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
    #[serde(default)]
    recovery: Option<CliRecoveryContext>,
}

#[derive(Debug, Deserialize)]
struct CliRecoveryContext {
    plan_id: String,
    revision: u64,
    phase: String,
}

#[derive(Debug, Deserialize)]
struct PrivatePreviewReceipt {
    preview_path: PathBuf,
    preview: OperatorApprovalPreview,
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

fn context_command() -> Command {
    let mut command = Command::new(PathBuf::from(env!("CARGO_BIN_EXE_threadlane-context")));
    command
        .env("GIT_TERMINAL_PROMPT", "0")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_SYSTEM", "/dev/null")
        .env("GIT_CONFIG_GLOBAL", "/dev/null");
    command
}

async fn run_command_with_stdin(
    mut command: Command,
    args: &[String],
    stdin_bytes: &[u8],
    temp_dir: Option<&Path>,
) -> Output {
    command
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    if let Some(dir) = temp_dir {
        command.env("TMPDIR", dir);
    }

    let mut child = command.spawn().expect("spawn stdin command");
    {
        let mut pipe = child.stdin.take().expect("child stdin pipe");
        // A bounded-input rejection can close the child pipe before the writer finishes; the
        // command's own exit status and stderr are the authoritative result.
        let _ = pipe.write_all(stdin_bytes).await;
        let _ = pipe.shutdown().await;
    }
    child
        .wait_with_output()
        .await
        .expect("wait for stdin command")
}

fn directory_entries(dir: &Path) -> Vec<PathBuf> {
    let mut entries = fs::read_dir(dir)
        .expect("read directory entries")
        .map(|entry| entry.expect("read directory entry").path())
        .collect::<Vec<_>>();
    entries.sort();
    entries
}

fn prepare_private_fixed_args(
    fx: &ApprovalFixture,
    binding: &str,
    thread: &str,
    project_root: &Path,
) -> Vec<String> {
    let mut args = fx.prepare_private_args();
    args.push("--binding".to_string());
    args.push(binding.to_string());
    args.push("--thread".to_string());
    args.push(thread.to_string());
    args.push("--project-root".to_string());
    args.push(project_root.display().to_string());
    args
}

fn prepare_task_args(fx: &ApprovalFixture) -> Vec<String> {
    prepare_task_args_with_scope(
        fx,
        Some(fx.binding_id.as_str()),
        Some(THREAD_UUID),
        Some(fx.repo.as_path()),
    )
}

fn prepare_task_args_with_scope(
    fx: &ApprovalFixture,
    binding: Option<&str>,
    thread: Option<&str>,
    project_root: Option<&Path>,
) -> Vec<String> {
    let mut args = vec!["prepare-task".to_string()];
    args.extend(fx.store_args());
    if let Some(binding) = binding {
        args.push("--binding".to_string());
        args.push(binding.to_string());
    }
    if let Some(thread) = thread {
        args.push("--thread".to_string());
        args.push(thread.to_string());
    }
    if let Some(project_root) = project_root {
        args.push("--project-root".to_string());
        args.push(project_root.display().to_string());
    }
    args
}

fn prepare_task_request(fx: &ApprovalFixture, command_key: &str) -> Value {
    serde_json::json!({
        "binding_id": fx.binding_id.clone(),
        "thread_uuid": THREAD_UUID,
        "repository_path": fx.repo.clone(),
        "base_commit": fx.base_commit.clone(),
        "profile_id": PROFILE_ID,
        "profile_revision": fx.registry_revision,
        "command_key": command_key,
        "summary": "Ordinary single-task fast path",
        "task": {
            "id": "task-fast",
            "title": "Ordinary fast-path task",
            "instruction": "Implement the ordinary fast-path task.",
            "role": "coder",
            "difficulty": "easy",
            "depends_on": [],
            "allowed_write_paths": ["src/lib.rs"]
        },
        "budgets": {"timeout_ms": 90_000}
    })
}

fn prepare_task_bytes(request: &Value) -> Vec<u8> {
    serde_json::to_vec(request).expect("serialize strict prepare-task request")
}

fn prepare_task_plan_revision_count(fx: &ApprovalFixture, plan_id: &str) -> usize {
    fx.plans
        .plan_revisions(plan_id)
        .expect("read fast-path plan revisions")
        .len()
}

fn total_plan_revision_count(fx: &ApprovalFixture) -> usize {
    fx.plans.snapshot().expect("read all plan revisions").len()
}

fn write_prepare_task_context(fx: &ApprovalFixture, name: &str) -> PathBuf {
    let sessions_dir = fx.root.join("sessions");
    fs::create_dir_all(&sessions_dir).expect("create prepare-task router sessions directory");
    fs::set_permissions(&sessions_dir, fs::Permissions::from_mode(0o700))
        .expect("chmod prepare-task router sessions directory 0700");
    fs::create_dir_all(&fx.authority_dir).expect("create prepare-task router authority directory");
    fs::set_permissions(&fx.authority_dir, fs::Permissions::from_mode(0o700))
        .expect("chmod prepare-task router authority directory 0700");

    let context = serde_json::json!({
        "schema_version": 1,
        "project_root": fx.repo.clone(),
        "binding_id": fx.binding_id.clone(),
        "thread_uuid": THREAD_UUID,
        "route": "trusted_local_operator",
        "native_per_call_identity_verified": false,
        "stores": {
            "state_dir": fx.state_dir.clone(),
            "registry_dir": fx.registry_dir.clone(),
            "plans_dir": fx.plans_dir.clone(),
            "authority_dir": fx.authority_dir.clone(),
            "sessions_dir": sessions_dir,
        }
    });
    fx.write_request(name, &context)
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
async fn quickstart_example_prepares_exact_task_without_worker_invocation() {
    let mut fx = ApprovalFixture::start().await;

    let marker = "const req = ";
    let start = ORDINARY_WORKER_QUICKSTART
        .find(marker)
        .expect("quickstart strict JSON example")
        + marker.len();
    let rest = &ORDINARY_WORKER_QUICKSTART[start..];
    let end = rest
        .find(";\nconst q")
        .expect("quickstart JSON example terminator");
    let mut request: Value =
        serde_json::from_str(&rest[..end]).expect("documented quickstart JSON");
    request["binding_id"] = serde_json::json!(fx.binding_id);
    request["thread_uuid"] = serde_json::json!(THREAD_UUID);
    request["repository_path"] = serde_json::json!(fx.repo);
    request["base_commit"] = serde_json::json!(fx.base_commit);
    request["profile_id"] = serde_json::json!(PROFILE_ID);
    request["profile_revision"] = serde_json::json!(fx.registry_revision);
    request["command_key"] = serde_json::json!("quickstart-1");
    request["summary"] = serde_json::json!("Prepare status text");
    request["task"]["id"] = serde_json::json!("task-status");
    request["task"]["title"] = serde_json::json!("Update status");
    let shell_marker = fx.root.join("unexpected-shell-substitution");
    let instruction = format!(
        "Write the user's status $(touch {})",
        shell_marker.display()
    );
    request["task"]["instruction"] = serde_json::json!(instruction);
    let budgets: ExecutionBudgets = serde_json::from_value(request["budgets"].clone()).unwrap();

    let context_path = write_prepare_task_context(&fx, "quickstart's-context.json");
    let temp_dir = fx.private_preview_temp_dir();
    let quote = |s: &str| format!("'{}'", s.replace('\'', "'\\''"));
    let command = format!(
        "printf '%s' {} | {} --context {} approve prepare-task-exact",
        quote(&serde_json::to_string(&request).unwrap()),
        quote(env!("CARGO_BIN_EXE_threadlane-context")),
        quote(context_path.to_str().unwrap()),
    );
    let output = Command::new("sh")
        .arg("-c")
        .arg(command)
        .env("TMPDIR", &temp_dir)
        .env("GIT_TERMINAL_PROMPT", "0")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_SYSTEM", "/dev/null")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .output()
        .await
        .expect("run documented quoted command");
    let receipt: PrivatePreviewReceipt = parse_success(
        &output,
        "prepare documented quickstart task via context router",
    );
    let preview = receipt.preview;

    assert_eq!(preview.request.binding_id, fx.binding_id);
    assert_eq!(preview.request.thread_uuid, THREAD_UUID);
    assert!(preview.request.plan_id.starts_with("plan_"));
    assert_eq!(preview.request.plan_revision, 1);
    assert_eq!(
        preview.request.expected_registry_revision,
        fx.registry_revision
    );
    assert_sha256(
        &preview.request.expected_plan_digest,
        "quickstart plan digest",
    );
    assert_eq!(preview.request.repository_path, fx.repo);
    assert_eq!(preview.request.base_commit, fx.base_commit);
    assert_eq!(
        preview.request.approved_task_ids,
        vec!["task-status".to_string()]
    );
    assert_eq!(
        preview.request.approved_write_paths,
        vec!["STATUS.txt".to_string()]
    );
    assert_eq!(preview.request.max_parallel, 1);
    assert_eq!(preview.request.budgets, budgets);
    assert_eq!(preview.tasks.len(), 1);
    assert_eq!(preview.tasks[0].task_id, "task-status");
    assert_eq!(preview.tasks[0].instruction, instruction);
    assert!(
        !shell_marker.exists(),
        "request text must never run in the shell"
    );
    assert_eq!(
        preview.tasks[0].canonical_executable.as_deref(),
        Some(fx.fake_worker.as_path())
    );
    assert_eq!(
        preview.tasks[0].model_arg,
        "opencode-go/deepseek-v4.1-flash"
    );
    assert_eq!(preview.tasks[0].effort.as_deref(), Some("max"));
    assert_sha256(&preview.subject_digest, "quickstart preview subject_digest");

    fx.assert_no_worker_invocation();
    fx.assert_no_approval_artifacts();
    fx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn prepare_private_stdin_writes_verified_preview_that_record_accepts() {
    let mut fx = ApprovalFixture::start().await;
    let temp_dir = fx.private_preview_temp_dir();
    let intent = fx.intent();
    let output = fx.run_prepare_private(&intent, &temp_dir).await;
    let receipt: PrivatePreviewReceipt =
        parse_success(&output, "prepare-private strict stdin intent");

    let raw: Value = serde_json::from_slice(&output.stdout).expect("receipt is JSON");
    let object = raw.as_object().expect("receipt is an object");
    assert_eq!(
        object.len(),
        2,
        "receipt must expose only the canonical path and typed preview"
    );
    assert!(object.get("preview_path").is_some());
    assert!(object.get("preview").is_some());

    assert!(receipt.preview_path.is_absolute());
    assert_eq!(
        receipt
            .preview_path
            .file_name()
            .and_then(|name| name.to_str()),
        Some("preview.json")
    );
    let preview_dir = receipt
        .preview_path
        .parent()
        .expect("preview path has a parent directory");
    assert_eq!(
        fs::canonicalize(preview_dir).expect("canonicalize private preview directory"),
        preview_dir,
        "receipt preview_path must be canonical"
    );
    assert!(preview_dir.starts_with(&temp_dir));
    assert_eq!(
        directory_entries(&temp_dir),
        vec![preview_dir.to_path_buf()],
        "a unique private preview directory must be kept"
    );
    assert_mode(preview_dir, 0o700, "private preview directory");
    assert_mode(&receipt.preview_path, 0o600, "private preview file");
    assert!(fs::symlink_metadata(&receipt.preview_path)
        .expect("stat private preview file")
        .file_type()
        .is_file());

    let mut expected_bytes =
        serde_json::to_vec(&receipt.preview).expect("serialize receipt preview");
    expected_bytes.push(b'\n');
    assert!(expected_bytes.len() <= MAX_REQUEST_BYTES);
    assert_eq!(
        fs::read(&receipt.preview_path).expect("read private preview file"),
        expected_bytes,
        "preview file must contain the unchanged full typed preview plus newline"
    );

    let legacy_path = fx.write_request("prepare-private-equivalence.json", &intent);
    let legacy_output = fx.run_prepare(&legacy_path).await;
    let legacy_preview: OperatorApprovalPreview =
        parse_success(&legacy_output, "legacy prepare for equivalence");
    assert_eq!(
        receipt.preview, legacy_preview,
        "prepare-private must use the same authoritative prepare logic"
    );
    assert_eq!(
        fs::read(&receipt.preview_path).expect("read private preview file"),
        legacy_output.stdout,
        "private preview bytes must match the unchanged legacy prepare stdout"
    );

    fx.assert_no_approval_artifacts();

    let summary: ApprovalSummary = parse_success(
        &fx.run_record_preview(
            &receipt.preview_path,
            &receipt.preview.subject_digest,
            THREAD_UUID,
            true,
        )
        .await,
        "record the unchanged private preview returned by prepare-private",
    );
    assert_eq!(summary.subject_digest, receipt.preview.subject_digest);
    assert_eq!(summary.binding_id, fx.binding_id);
    assert_eq!(
        summary.approved_task_ids,
        vec!["task-root".to_string(), "task-child".to_string()]
    );
    assert_eq!(fx.approval_count(), 1);
    fx.assert_no_worker_invocation();
    assert_eq!(fx.head(), fx.base_commit);
    assert!(fx.worktree_names().is_empty());

    fx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn prepare_private_exact_requires_all_budgets_and_preserves_legacy_defaults() {
    let mut fx = ApprovalFixture::start().await;
    let temp_dir = fx.private_preview_temp_dir();
    let mut args = fx.prepare_private_args();
    args[0] = "prepare-private-exact".to_string();
    let mut intent = fx.intent();
    intent.budgets = serde_json::from_value(serde_json::json!({
        "timeout_ms": 90_000, "max_line_bytes": 16_384,
        "max_stdout_bytes": 2_097_152, "max_stderr_bytes": 100_000,
        "max_events": 321,
        "progress_watchdog": {"soft_timeout_ms": 60_000, "extension_ms": 10_000,
                              "recent_advancement_ms": 20_000}
    }))
    .expect("explicit valid budgets");
    let valid = serde_json::to_value(&intent).expect("serialize intent");

    for field in [
        "timeout_ms",
        "max_line_bytes",
        "max_stdout_bytes",
        "max_stderr_bytes",
        "max_events",
    ] {
        let mut missing = valid.clone();
        missing["budgets"].as_object_mut().unwrap().remove(field);
        let out = fx
            .run_cli_with_stdin(
                &args,
                &serde_json::to_vec(&missing).unwrap(),
                Some(&temp_dir),
            )
            .await;
        assert_eq!(
            expect_failure(&out, "missing explicit private budget").code,
            "invalid_input",
            "{field}"
        );
    }
    let mut missing = valid.clone();
    missing.as_object_mut().unwrap().remove("budgets");
    let out = fx
        .run_cli_with_stdin(
            &args,
            &serde_json::to_vec(&missing).unwrap(),
            Some(&temp_dir),
        )
        .await;
    assert_eq!(
        expect_failure(&out, "missing private budgets").code,
        "invalid_input"
    );
    for field in ["max_attempts", "max_workers"] {
        for nested in [false, true] {
            let mut unsupported = valid.clone();
            let target = if nested {
                &mut unsupported["budgets"]
            } else {
                &mut unsupported
            };
            target
                .as_object_mut()
                .unwrap()
                .insert(field.to_string(), Value::from(1));
            let out = fx
                .run_cli_with_stdin(
                    &args,
                    &serde_json::to_vec(&unsupported).unwrap(),
                    Some(&temp_dir),
                )
                .await;
            assert_eq!(
                expect_failure(&out, "unsupported private field").code,
                "invalid_input",
                "{field}"
            );
        }
    }
    assert!(directory_entries(&temp_dir).is_empty());
    fx.assert_no_approval_artifacts();

    let receipt: PrivatePreviewReceipt = parse_success(
        &fx.run_cli_with_stdin(&args, &serde_json::to_vec(&valid).unwrap(), Some(&temp_dir))
            .await,
        "exact private preview",
    );
    assert_eq!(receipt.preview.request.budgets, intent.budgets);
    assert_eq!(receipt.preview.request.expected_plan_digest, fx.plan_digest);
    let legacy_path = fx.write_request("exact-equivalent-intent.json", &intent);
    let equivalent: OperatorApprovalPreview =
        parse_success(&fx.run_prepare(&legacy_path).await, "legacy equivalent");
    assert_eq!(receipt.preview, equivalent);
    assert_eq!(fs::read(&receipt.preview_path).unwrap(), {
        let mut bytes = serde_json::to_vec(&receipt.preview).unwrap();
        bytes.push(b'\n');
        bytes
    });
    fx.assert_no_approval_artifacts();
    let summary: ApprovalSummary = parse_success(
        &fx.run_record_preview(
            &receipt.preview_path,
            &receipt.preview.subject_digest,
            THREAD_UUID,
            true,
        )
        .await,
        "record exact private preview separately",
    );
    assert_eq!(summary.subject_digest, receipt.preview.subject_digest);
    let record = fx.persisted_record();
    assert_eq!(record.budgets, intent.budgets);
    assert_eq!(
        serde_json::to_value(&summary).unwrap(),
        serde_json::to_value(record.to_summary()).unwrap()
    );
    fx.assert_no_worker_invocation();

    let mut legacy_default = valid;
    legacy_default.as_object_mut().unwrap().remove("budgets");
    let legacy: PrivatePreviewReceipt = parse_success(
        &fx.run_cli_with_stdin(
            &fx.prepare_private_args(),
            &serde_json::to_vec(&legacy_default).unwrap(),
            Some(&temp_dir),
        )
        .await,
        "legacy private preview retains default budgets",
    );
    assert_eq!(legacy.preview.request.budgets, ExecutionBudgets::default());
    fx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn prepare_task_exact_rejects_omissions_before_publication_and_preserves_values() {
    let mut fx = ApprovalFixture::start().await;
    let temp_dir = fx.private_preview_temp_dir();
    let mut args = prepare_task_args(&fx);
    args[0] = "prepare-task-exact".to_string();
    let mut valid = prepare_task_request(&fx, "fast-exact-1");
    valid["budgets"] = serde_json::json!({
        "timeout_ms": 90_000, "max_line_bytes": 16_384,
        "max_stdout_bytes": 2_097_152, "max_stderr_bytes": 100_000, "max_events": 321
    });
    let expected: ExecutionBudgets = serde_json::from_value(valid["budgets"].clone()).unwrap();
    let before = total_plan_revision_count(&fx);
    for field in [
        "timeout_ms",
        "max_line_bytes",
        "max_stdout_bytes",
        "max_stderr_bytes",
        "max_events",
    ] {
        let mut missing = valid.clone();
        missing["budgets"].as_object_mut().unwrap().remove(field);
        let out = fx
            .run_cli_with_stdin(&args, &prepare_task_bytes(&missing), Some(&temp_dir))
            .await;
        assert_eq!(
            expect_failure(&out, "missing exact task budget").code,
            "invalid_input",
            "{field}"
        );
    }
    for field in ["max_attempts", "max_workers"] {
        for nested in [false, true] {
            let mut unsupported = valid.clone();
            let target = if nested {
                &mut unsupported["budgets"]
            } else {
                &mut unsupported
            };
            target
                .as_object_mut()
                .unwrap()
                .insert(field.to_string(), Value::from(1));
            let out = fx
                .run_cli_with_stdin(&args, &prepare_task_bytes(&unsupported), Some(&temp_dir))
                .await;
            assert_eq!(
                expect_failure(&out, "unsupported exact task field").code,
                "invalid_input",
                "{field}"
            );
        }
    }
    let mut invalid = valid.clone();
    invalid["budgets"]["timeout_ms"] = Value::from(0);
    let out = fx
        .run_cli_with_stdin(&args, &prepare_task_bytes(&invalid), Some(&temp_dir))
        .await;
    assert_eq!(
        expect_failure(&out, "invalid exact task budget").code,
        "validation"
    );
    assert_eq!(total_plan_revision_count(&fx), before);
    assert!(directory_entries(&temp_dir).is_empty());
    fx.assert_no_approval_artifacts();

    let context_path = write_prepare_task_context(&fx, "exact-task-context.json");
    let context_args = vec![
        "--context".to_string(),
        context_path.display().to_string(),
        "approve".to_string(),
        "prepare-task-exact".to_string(),
    ];
    let receipt: PrivatePreviewReceipt = parse_success(
        &run_command_with_stdin(
            context_command(),
            &context_args,
            &prepare_task_bytes(&valid),
            Some(&temp_dir),
        )
        .await,
        "exact task preview through context router",
    );
    assert_eq!(receipt.preview.request.budgets, expected);
    assert_eq!(receipt.preview.request.max_parallel, 1);
    assert_eq!(
        receipt.preview.tasks[0].canonical_executable.as_deref(),
        Some(fx.fake_worker.as_path())
    );
    assert_eq!(
        receipt.preview.tasks[0].model_arg,
        "opencode-go/deepseek-v4.1-flash"
    );
    assert_eq!(receipt.preview.tasks[0].effort.as_deref(), Some("max"));
    assert_eq!(total_plan_revision_count(&fx), before + 1);
    fx.assert_no_approval_artifacts();
    fx.assert_no_worker_invocation();

    let legacy = prepare_task_request(&fx, "fast-legacy-default-1");
    let legacy_receipt: PrivatePreviewReceipt = parse_success(
        &fx.run_cli_with_stdin(
            &prepare_task_args(&fx),
            &prepare_task_bytes(&legacy),
            Some(&temp_dir),
        )
        .await,
        "legacy task retains partial default budgets",
    );
    assert_eq!(legacy_receipt.preview.request.budgets.timeout_ms, 90_000);
    assert_eq!(
        legacy_receipt.preview.request.budgets.max_events,
        ExecutionBudgets::default().max_events
    );
    fx.assert_no_approval_artifacts();
    fx.assert_no_worker_invocation();
    fx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn prepare_private_rejects_invalid_stdin_and_fixed_scope_before_private_artifacts() {
    let mut fx = ApprovalFixture::start().await;
    let temp_dir = fx.private_preview_temp_dir();

    let empty = fx
        .run_cli_with_stdin(&fx.prepare_private_args(), b"", Some(&temp_dir))
        .await;
    assert_eq!(
        expect_failure(&empty, "prepare-private with empty stdin").code,
        "invalid_input"
    );
    assert!(directory_entries(&temp_dir).is_empty());

    let malformed = format!("{{ not valid intent JSON {SECRET_SENTINEL}");
    let malformed_output = fx
        .run_cli_with_stdin(
            &fx.prepare_private_args(),
            malformed.as_bytes(),
            Some(&temp_dir),
        )
        .await;
    assert_eq!(
        expect_failure(&malformed_output, "prepare-private with malformed stdin").code,
        "invalid_input"
    );
    assert_no_secret_echo(&malformed_output, "prepare-private with malformed stdin");
    assert!(directory_entries(&temp_dir).is_empty());

    let mut unknown_field = serde_json::to_value(fx.intent()).expect("serialize intent as value");
    unknown_field
        .as_object_mut()
        .expect("intent is an object")
        .insert("unexpected_field".to_string(), Value::Bool(true));
    let unknown_bytes = serde_json::to_vec(&unknown_field).expect("serialize invalid intent");
    let unknown_output = fx
        .run_cli_with_stdin(&fx.prepare_private_args(), &unknown_bytes, Some(&temp_dir))
        .await;
    assert_eq!(
        expect_failure(
            &unknown_output,
            "prepare-private with an unknown intent field"
        )
        .code,
        "invalid_input"
    );
    assert!(directory_entries(&temp_dir).is_empty());

    let mut oversize = serde_json::to_vec(&fx.intent()).expect("serialize oversize stdin intent");
    oversize.resize(MAX_REQUEST_BYTES + 1, b' ');
    let oversize_output = fx
        .run_cli_with_stdin(&fx.prepare_private_args(), &oversize, Some(&temp_dir))
        .await;
    assert_eq!(
        expect_failure(&oversize_output, "prepare-private with oversize stdin").code,
        "invalid_input"
    );
    assert!(directory_entries(&temp_dir).is_empty());

    let intent_bytes = serde_json::to_vec(&fx.intent()).expect("serialize valid stdin intent");
    let wrong_binding = fx
        .run_cli_with_stdin(
            &prepare_private_fixed_args(&fx, "bnd_other_chat", THREAD_UUID, &fx.repo),
            &intent_bytes,
            Some(&temp_dir),
        )
        .await;
    assert_eq!(
        expect_failure(
            &wrong_binding,
            "prepare-private with a fixed binding mismatch"
        )
        .code,
        "binding_mismatch"
    );
    assert!(directory_entries(&temp_dir).is_empty());

    let wrong_thread = fx
        .run_cli_with_stdin(
            &prepare_private_fixed_args(&fx, &fx.binding_id, OTHER_THREAD_UUID, &fx.repo),
            &intent_bytes,
            Some(&temp_dir),
        )
        .await;
    assert_eq!(
        expect_failure(
            &wrong_thread,
            "prepare-private with a fixed thread mismatch"
        )
        .code,
        "thread_uuid_mismatch"
    );
    assert!(directory_entries(&temp_dir).is_empty());

    let other_repo = fx.root.join("other-prepare-private-repo");
    let _other_commit = init_disposable_repo(&other_repo);
    let wrong_repo = fx
        .run_cli_with_stdin(
            &prepare_private_fixed_args(&fx, &fx.binding_id, THREAD_UUID, &other_repo),
            &intent_bytes,
            Some(&temp_dir),
        )
        .await;
    assert_eq!(
        expect_failure(
            &wrong_repo,
            "prepare-private with a fixed project-root mismatch"
        )
        .code,
        "invalid_repository"
    );
    assert!(directory_entries(&temp_dir).is_empty());

    let mut partial_scope = fx.prepare_private_args();
    partial_scope.push("--binding".to_string());
    partial_scope.push(fx.binding_id.clone());
    let partial = fx
        .run_cli_with_stdin(&partial_scope, &intent_bytes, Some(&temp_dir))
        .await;
    assert_eq!(
        expect_failure(&partial, "prepare-private with partial fixed scope").code,
        "invalid_input"
    );
    assert!(directory_entries(&temp_dir).is_empty());

    let advanced_revision = fx.advance_registry();
    assert_eq!(advanced_revision, fx.registry_revision + 1);
    let stale = fx
        .run_cli_with_stdin(&fx.prepare_private_args(), &intent_bytes, Some(&temp_dir))
        .await;
    assert_eq!(
        expect_failure(&stale, "prepare-private after a stale registry revision").code,
        "stale_registry_revision"
    );
    assert!(directory_entries(&temp_dir).is_empty());

    fx.assert_no_approval_artifacts();
    fx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn context_router_forwards_prepare_private_stdin_and_returns_the_receipt() {
    let mut fx = ApprovalFixture::start().await;
    let temp_dir = fx.private_preview_temp_dir();
    let sessions_dir = fx.root.join("sessions");
    fs::create_dir_all(&sessions_dir).expect("create router sessions directory");
    fs::set_permissions(&sessions_dir, fs::Permissions::from_mode(0o700))
        .expect("chmod router sessions directory 0700");
    fs::create_dir_all(&fx.authority_dir).expect("create router authority directory");
    fs::set_permissions(&fx.authority_dir, fs::Permissions::from_mode(0o700))
        .expect("chmod router authority directory 0700");
    assert_mode(&fx.authority_dir, 0o700, "router authority directory");
    assert!(
        directory_entries(&fx.authority_dir).is_empty(),
        "router authority directory must start empty"
    );

    let context = serde_json::json!({
        "schema_version": 1,
        "project_root": fx.repo.clone(),
        "binding_id": fx.binding_id.clone(),
        "thread_uuid": THREAD_UUID,
        "route": "trusted_local_operator",
        "native_per_call_identity_verified": false,
        "stores": {
            "state_dir": fx.state_dir.clone(),
            "registry_dir": fx.registry_dir.clone(),
            "plans_dir": fx.plans_dir.clone(),
            "authority_dir": fx.authority_dir.clone(),
            "sessions_dir": sessions_dir,
        }
    });
    let context_path = fx.write_request("prepare-private-context.json", &context);

    let args = vec![
        "--context".to_string(),
        context_path.display().to_string(),
        "approve".to_string(),
        "prepare-private".to_string(),
    ];
    let stdin = serde_json::to_vec(&fx.intent()).expect("serialize router stdin intent");
    let output = run_command_with_stdin(context_command(), &args, &stdin, Some(&temp_dir)).await;
    let receipt: PrivatePreviewReceipt = parse_success(&output, "context-routed prepare-private");

    assert_eq!(receipt.preview.request.binding_id, fx.binding_id);
    assert_eq!(receipt.preview.request.thread_uuid, THREAD_UUID);
    assert_eq!(receipt.preview.request.plan_id, PLAN_ID);
    assert_eq!(receipt.preview.request.plan_revision, fx.plan_revision);
    assert_eq!(receipt.preview.request.repository_path, fx.repo);
    assert!(receipt.preview_path.starts_with(&temp_dir));
    assert_mode(
        receipt
            .preview_path
            .parent()
            .expect("router preview path parent"),
        0o700,
        "router private preview directory",
    );
    assert_mode(&receipt.preview_path, 0o600, "router private preview file");
    assert_eq!(directory_entries(&temp_dir).len(), 1);
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
async fn record_preview_route_uses_the_same_receipt_and_remains_idempotent() {
    let mut fx = ApprovalFixture::start().await;
    let preview = fx.prepare_ok("prepare-preview-route.json").await;
    let mut preview_bytes =
        serde_json::to_vec(&preview).expect("serialize actual prepare stdout preview");
    preview_bytes.push(b'\n');
    let preview_path = fx.write_request_bytes("record-preview-valid.json", &preview_bytes);
    let request_path = fx.write_request("record-preview-fallback-request.json", &preview.request);
    assert_mode(&fx.requests_dir, 0o700, "private preview directory");
    assert_mode(&preview_path, 0o600, "private preview file");

    let from_preview: ApprovalSummary = parse_success(
        &fx.run_record_preview(&preview_path, &preview.subject_digest, THREAD_UUID, true)
            .await,
        "record unchanged typed preview",
    );
    assert_eq!(from_preview.subject_digest, preview.subject_digest);
    assert_eq!(from_preview.thread_uuid, THREAD_UUID);
    assert_eq!(fx.approval_count(), 1);

    let from_request: ApprovalSummary = parse_success(
        &fx.run_record(&request_path, &preview.subject_digest, THREAD_UUID, true)
            .await,
        "record the same scope through the fallback request route",
    );
    assert_eq!(
        from_request, from_preview,
        "the unchanged preview and unchanged normalized request must produce the same receipt"
    );
    assert_eq!(fx.approval_count(), 1);
    assert_eq!(fx.persisted_record().tasks, preview.tasks);
    fx.assert_no_worker_invocation();

    fx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn record_preview_rejects_altered_projections_subject_scope_and_insecure_file() {
    let mut fx = ApprovalFixture::start().await;
    let preview = fx.prepare_ok("prepare-preview-tampering.json").await;

    let mut altered_tasks = preview.clone();
    altered_tasks.tasks[0].instruction =
        "Altered task projection after the operator preview.".to_string();
    let altered_tasks_path = fx.write_request("preview-altered-tasks.json", &altered_tasks);
    let altered_tasks_failure = expect_failure(
        &fx.run_record_preview(
            &altered_tasks_path,
            &preview.subject_digest,
            THREAD_UUID,
            true,
        )
        .await,
        "record a preview whose frozen task projection changed",
    );
    assert_eq!(altered_tasks_failure.code, "validation");
    fx.assert_no_approval_artifacts();

    let mut altered_subject = preview.clone();
    altered_subject.subject_digest = different_digest(&preview.subject_digest);
    let altered_subject_path = fx.write_request("preview-altered-subject.json", &altered_subject);
    let altered_subject_failure = expect_failure(
        &fx.run_record_preview(
            &altered_subject_path,
            &preview.subject_digest,
            THREAD_UUID,
            true,
        )
        .await,
        "record a preview whose subject digest changed",
    );
    assert_eq!(altered_subject_failure.code, "validation");
    fx.assert_no_approval_artifacts();

    let mut altered_scope = preview.clone();
    altered_scope
        .request
        .approved_write_paths
        .push("src/other.rs".to_string());
    let altered_scope_path = fx.write_request("preview-altered-scope.json", &altered_scope);
    let altered_scope_failure = expect_failure(
        &fx.run_record_preview(
            &altered_scope_path,
            &preview.subject_digest,
            THREAD_UUID,
            true,
        )
        .await,
        "record a preview whose normalized request scope changed",
    );
    assert_eq!(altered_scope_failure.code, "validation");
    fx.assert_no_approval_artifacts();

    let unchanged_path = fx.write_request("preview-unchanged-subject-check.json", &preview);
    let wrong_subject_failure = expect_failure(
        &fx.run_record_preview(
            &unchanged_path,
            &different_digest(&preview.subject_digest),
            THREAD_UUID,
            true,
        )
        .await,
        "record an unchanged preview with a mismatched confirmation subject",
    );
    assert_eq!(wrong_subject_failure.code, "validation");
    fx.assert_no_approval_artifacts();

    let insecure_path = fx.write_request("preview-insecure.json", &preview);
    fs::set_permissions(&insecure_path, fs::Permissions::from_mode(0o644))
        .expect("chmod otherwise-valid preview file 0644");
    let insecure_failure = expect_failure(
        &fx.run_record_preview(&insecure_path, &preview.subject_digest, THREAD_UUID, true)
            .await,
        "record a group-readable preview file",
    );
    assert_eq!(insecure_failure.code, "security_violation");
    fx.assert_no_approval_artifacts();

    let summary: ApprovalSummary = parse_success(
        &fx.run_record_preview(&unchanged_path, &preview.subject_digest, THREAD_UUID, true)
            .await,
        "record unchanged preview after tampered and insecure previews were rejected",
    );
    assert_eq!(summary.subject_digest, preview.subject_digest);
    assert_eq!(fx.approval_count(), 1);
    fx.assert_no_worker_invocation();

    fx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn record_preview_requires_exactly_one_input_and_operator_attestation() {
    let mut fx = ApprovalFixture::start().await;
    let preview = fx.prepare_ok("prepare-preview-arguments.json").await;
    let preview_path = fx.write_request("preview-arguments.json", &preview);
    let request_path = fx.write_request("preview-arguments-request.json", &preview.request);

    let mut both_inputs = fx.record_args(&request_path, &preview.subject_digest, THREAD_UUID, true);
    let insert_at = both_inputs
        .iter()
        .position(|arg| arg == "--request")
        .expect("record arguments contain --request");
    both_inputs.insert(insert_at, preview_path.display().to_string());
    both_inputs.insert(insert_at, "--preview".to_string());
    let both_failure = expect_failure(
        &fx.run_cli(&both_inputs).await,
        "record with both --request and --preview",
    );
    assert_eq!(both_failure.code, "invalid_input");
    fx.assert_no_approval_artifacts();

    let mut neither_input = vec!["record".to_string()];
    neither_input.extend(fx.store_args());
    neither_input.push("--confirm-subject".to_string());
    neither_input.push(preview.subject_digest.clone());
    neither_input.push("--confirm-thread".to_string());
    neither_input.push(THREAD_UUID.to_string());
    neither_input.push("--operator-approved".to_string());
    let neither_failure = expect_failure(
        &fx.run_cli(&neither_input).await,
        "record without --request or --preview",
    );
    assert_eq!(neither_failure.code, "invalid_input");
    fx.assert_no_approval_artifacts();

    let missing_flag_failure = expect_failure(
        &fx.run_record_preview(&preview_path, &preview.subject_digest, THREAD_UUID, false)
            .await,
        "record a verified preview without --operator-approved",
    );
    assert_eq!(missing_flag_failure.code, "invalid_input");
    fx.assert_no_approval_artifacts();

    let wrong_thread_failure = expect_failure(
        &fx.run_record_preview(
            &preview_path,
            &preview.subject_digest,
            OTHER_THREAD_UUID,
            true,
        )
        .await,
        "record a verified preview with a mismatched confirmation thread",
    );
    assert_eq!(wrong_thread_failure.code, "thread_uuid_mismatch");
    fx.assert_no_approval_artifacts();

    let summary: ApprovalSummary = parse_success(
        &fx.run_record_preview(&preview_path, &preview.subject_digest, THREAD_UUID, true)
            .await,
        "record verified preview with exactly one input and explicit attestation",
    );
    assert_eq!(summary.subject_digest, preview.subject_digest);
    assert_eq!(fx.approval_count(), 1);
    fx.assert_no_worker_invocation();

    fx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn record_preview_rejects_a_stale_registry_revision_without_approval() {
    let mut fx = ApprovalFixture::start().await;
    let preview = fx
        .prepare_ok("prepare-preview-before-stale-registry.json")
        .await;
    let preview_path = fx.write_request("preview-stale-registry.json", &preview);

    let advanced_revision = fx.advance_registry();
    assert_eq!(advanced_revision, fx.registry_revision + 1);

    let failure = expect_failure(
        &fx.run_record_preview(&preview_path, &preview.subject_digest, THREAD_UUID, true)
            .await,
        "record a verified preview after the pinned registry revision became stale",
    );
    assert_eq!(failure.code, "stale_registry_revision");
    fx.assert_no_approval_artifacts();
    assert_eq!(fx.plan_status(), PlanStatus::Published);

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

    let preview_path = fx.write_request("record-preview-superseded.json", &preview);
    let preview_output = fx
        .run_record_preview(&preview_path, &preview.subject_digest, THREAD_UUID, true)
        .await;
    expect_failure(
        &preview_output,
        "record a verified preview of a superseded plan revision",
    );
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn prepare_task_context_route_publishes_one_plan_and_preview_and_record_stays_separate() {
    let mut fx = ApprovalFixture::start().await;
    let temp_dir = fx.private_preview_temp_dir();
    let context_path = write_prepare_task_context(&fx, "prepare-task-context.json");
    let request = prepare_task_request(&fx, "fast-context-1");
    let stdin = prepare_task_bytes(&request);
    let args = vec![
        "--context".to_string(),
        context_path.display().to_string(),
        "approve".to_string(),
        "prepare-task".to_string(),
    ];

    let output = run_command_with_stdin(context_command(), &args, &stdin, Some(&temp_dir)).await;
    let receipt: PrivatePreviewReceipt = parse_success(&output, "context-routed prepare-task");

    let plan_id = receipt.preview.request.plan_id.clone();
    assert!(plan_id.starts_with("plan_"));
    assert_eq!(receipt.preview.request.plan_revision, 1);
    assert_eq!(receipt.preview.request.binding_id, fx.binding_id);
    assert_eq!(receipt.preview.request.thread_uuid, THREAD_UUID);
    assert_eq!(receipt.preview.request.repository_path, fx.repo);
    assert_eq!(receipt.preview.request.base_commit, fx.base_commit);
    assert_eq!(
        receipt.preview.request.expected_registry_revision,
        fx.registry_revision
    );
    assert_eq!(
        receipt.preview.request.approved_task_ids,
        vec!["task-fast".to_string()]
    );
    assert_eq!(
        receipt.preview.request.approved_write_paths,
        vec!["src/lib.rs".to_string()]
    );
    assert_eq!(receipt.preview.request.max_parallel, 1);
    assert_eq!(receipt.preview.request.budgets.timeout_ms, 90_000);
    assert_eq!(receipt.preview.tasks.len(), 1);
    assert_eq!(receipt.preview.tasks[0].task_id, "task-fast");
    assert_eq!(
        receipt.preview.tasks[0].instruction,
        "Implement the ordinary fast-path task."
    );
    assert_sha256(
        &receipt.preview.subject_digest,
        "fast-path preview subject_digest",
    );

    let revisions = fx
        .plans
        .plan_revisions(&plan_id)
        .expect("read fast-path plan revisions");
    assert_eq!(revisions.len(), 1);
    assert_eq!(revisions[0].status, PlanStatus::Published);
    assert_eq!(revisions[0].profile_id, PROFILE_ID);
    assert_eq!(revisions[0].profile_revision, fx.registry_revision);
    assert_eq!(revisions[0].summary, "Ordinary single-task fast path");
    assert_eq!(revisions[0].tasks.len(), 1);
    assert_eq!(revisions[0].tasks[0].id, "task-fast");
    assert_eq!(revisions[0].tasks[0].role, WorkflowRole::Coder);
    assert!(revisions[0].tasks[0].depends_on.is_empty());
    assert_eq!(
        revisions[0].tasks[0].allowed_write_paths,
        vec!["src/lib.rs".to_string()]
    );

    assert!(receipt.preview_path.starts_with(&temp_dir));
    assert_mode(
        receipt
            .preview_path
            .parent()
            .expect("fast-path preview path parent"),
        0o700,
        "fast-path private preview directory",
    );
    assert_mode(
        &receipt.preview_path,
        0o600,
        "fast-path private preview file",
    );
    assert_eq!(directory_entries(&temp_dir).len(), 1);
    assert_eq!(fx.approval_count(), 0);
    fx.assert_no_approval_artifacts();

    // An exact replay after publication adopts the original plan and preview scope instead of
    // creating a second plan. A new private preview is allowed; no authority is recorded.
    let replay_output =
        run_command_with_stdin(context_command(), &args, &stdin, Some(&temp_dir)).await;
    let replay: PrivatePreviewReceipt =
        parse_success(&replay_output, "context-routed prepare-task replay");
    assert_eq!(replay.preview.request.plan_id, plan_id);
    assert_eq!(replay.preview.request.plan_revision, 1);
    assert_eq!(
        replay.preview.subject_digest, receipt.preview.subject_digest,
        "an exact replay must reproduce the same frozen subject"
    );
    assert_eq!(prepare_task_plan_revision_count(&fx, &plan_id), 1);
    assert_eq!(fx.approval_count(), 0);
    assert_eq!(directory_entries(&temp_dir).len(), 2);

    // Recording remains a separate explicit operation.
    let summary: ApprovalSummary = parse_success(
        &fx.run_record_preview(
            &receipt.preview_path,
            &receipt.preview.subject_digest,
            THREAD_UUID,
            true,
        )
        .await,
        "record the fast-path private preview",
    );
    assert_eq!(summary.subject_digest, receipt.preview.subject_digest);
    assert_eq!(summary.plan_id, plan_id);
    assert_eq!(summary.plan_revision, 1);
    assert_eq!(summary.thread_uuid, THREAD_UUID);
    assert_eq!(fx.approval_count(), 1);
    fx.assert_no_worker_invocation();
    assert_eq!(fx.head(), fx.base_commit);
    assert!(fx.worktree_names().is_empty());

    // Replaying the fast path after record must not add a second plan or approval.
    let after_record =
        run_command_with_stdin(context_command(), &args, &stdin, Some(&temp_dir)).await;
    let after_record_receipt: PrivatePreviewReceipt =
        parse_success(&after_record, "prepare-task replay after record");
    assert_eq!(after_record_receipt.preview.request.plan_id, plan_id);
    assert_eq!(prepare_task_plan_revision_count(&fx, &plan_id), 1);
    assert_eq!(fx.approval_count(), 1);
    fx.assert_no_worker_invocation();
    assert_eq!(fx.head(), fx.base_commit);
    assert!(fx.worktree_names().is_empty());

    fx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn prepare_task_context_accepts_user_facing_worker_role_as_canonical_coder() {
    let mut fx = ApprovalFixture::start().await;
    let temp_dir = fx.private_preview_temp_dir();
    let context_path = write_prepare_task_context(&fx, "prepare-task-worker-role-context.json");
    let mut request = prepare_task_request(&fx, "fast-worker-role-1");
    request["task"]["role"] = serde_json::json!("worker");
    let stdin = prepare_task_bytes(&request);
    let args = vec![
        "--context".to_string(),
        context_path.display().to_string(),
        "approve".to_string(),
        "prepare-task".to_string(),
    ];

    let output = run_command_with_stdin(context_command(), &args, &stdin, Some(&temp_dir)).await;
    let receipt: PrivatePreviewReceipt = parse_success(
        &output,
        "context-routed prepare-task with the user-facing worker role",
    );

    let plan_id = receipt.preview.request.plan_id.clone();
    assert!(plan_id.starts_with("plan_"));
    assert_eq!(receipt.preview.request.plan_revision, 1);
    assert_eq!(receipt.preview.tasks.len(), 1);
    assert_eq!(receipt.preview.tasks[0].task_id, "task-fast");
    assert_eq!(receipt.preview.tasks[0].role, WorkflowRole::Coder);

    let revisions = fx
        .plans
        .plan_revisions(&plan_id)
        .expect("read worker-role plan revisions");
    assert_eq!(revisions.len(), 1);
    assert_eq!(revisions[0].status, PlanStatus::Published);
    assert_eq!(revisions[0].revision, 1);
    assert_eq!(revisions[0].tasks.len(), 1);
    assert_eq!(revisions[0].tasks[0].id, "task-fast");
    assert_eq!(revisions[0].tasks[0].role, WorkflowRole::Coder);
    assert_eq!(prepare_task_plan_revision_count(&fx, &plan_id), 1);
    assert_eq!(fx.approval_count(), 0);
    fx.assert_no_approval_artifacts();
    fx.assert_no_worker_invocation();
    assert_eq!(fx.head(), fx.base_commit);
    assert!(fx.worktree_names().is_empty());

    fx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn prepare_task_preserves_published_original_recovery_context_after_preview_tempdir_failure()
{
    let mut fx = ApprovalFixture::start().await;
    let request = prepare_task_request(&fx, "fast-recovery-1");
    let stdin = prepare_task_bytes(&request);

    let invalid_tmpdir = fx.root.join("invalid-tmpdir-file");
    fs::write(&invalid_tmpdir, b"not a directory\n").expect("write invalid TMPDIR file");
    fs::set_permissions(&invalid_tmpdir, fs::Permissions::from_mode(0o600))
        .expect("chmod invalid TMPDIR file 0600");

    let failed = fx
        .run_cli_with_stdin(&prepare_task_args(&fx), &stdin, Some(&invalid_tmpdir))
        .await;
    let failure = expect_failure(
        &failed,
        "prepare-task with an invalid TMPDIR file after publication",
    );
    assert_eq!(failure.code, "io");
    let recovery = failure
        .recovery
        .expect("published original recovery context after the private preview failure");
    assert!(recovery.plan_id.starts_with("plan_"));
    assert_eq!(recovery.revision, 1);
    assert_eq!(recovery.phase, "published");
    assert_eq!(fx.approval_count(), 0);
    fx.assert_no_approval_artifacts();
    fx.assert_no_worker_invocation();
    assert_eq!(fx.head(), fx.base_commit);
    assert!(fx.worktree_names().is_empty());

    // The safe reference names the actual original published revision and canonical worker role.
    let revisions = fx
        .plans
        .plan_revisions(&recovery.plan_id)
        .expect("read recovered original plan revisions");
    assert_eq!(revisions.len(), 1);
    assert_eq!(revisions[0].revision, recovery.revision);
    assert_eq!(revisions[0].status, PlanStatus::Published);
    assert_eq!(revisions[0].tasks.len(), 1);
    assert_eq!(revisions[0].tasks[0].id, "task-fast");
    assert_eq!(revisions[0].tasks[0].role, WorkflowRole::Coder);

    // Recovery uses the original command key and revision without creating a new plan or authority.
    let recovered_temp_dir = fx.private_preview_temp_dir();
    let recovered = fx
        .run_cli_with_stdin(&prepare_task_args(&fx), &stdin, Some(&recovered_temp_dir))
        .await;
    let recovered_receipt: PrivatePreviewReceipt = parse_success(
        &recovered,
        "prepare-task recovery replay through the original revision",
    );
    assert_eq!(recovered_receipt.preview.request.plan_id, recovery.plan_id);
    assert_eq!(
        recovered_receipt.preview.request.plan_revision,
        recovery.revision
    );
    assert_eq!(prepare_task_plan_revision_count(&fx, &recovery.plan_id), 1);
    assert_eq!(fx.approval_count(), 0);
    fx.assert_no_worker_invocation();

    fx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn prepare_task_requires_complete_exact_scope_and_current_registry_before_mutation() {
    let mut fx = ApprovalFixture::start().await;
    let temp_dir = fx.private_preview_temp_dir();
    let request = prepare_task_request(&fx, "fast-scope-1");
    let stdin = prepare_task_bytes(&request);
    let before = total_plan_revision_count(&fx);

    let missing = fx
        .run_cli_with_stdin(
            &prepare_task_args_with_scope(&fx, None, None, None),
            &stdin,
            Some(&temp_dir),
        )
        .await;
    assert_eq!(
        expect_failure(&missing, "prepare-task without fixed scope").code,
        "invalid_input"
    );

    let partial = fx
        .run_cli_with_stdin(
            &prepare_task_args_with_scope(
                &fx,
                Some(fx.binding_id.as_str()),
                Some(THREAD_UUID),
                None,
            ),
            &stdin,
            Some(&temp_dir),
        )
        .await;
    assert_eq!(
        expect_failure(&partial, "prepare-task without --project-root").code,
        "invalid_input"
    );

    let wrong_binding = fx
        .run_cli_with_stdin(
            &prepare_task_args_with_scope(
                &fx,
                Some("bnd_other_chat"),
                Some(THREAD_UUID),
                Some(fx.repo.as_path()),
            ),
            &stdin,
            Some(&temp_dir),
        )
        .await;
    assert_eq!(
        expect_failure(&wrong_binding, "prepare-task with a fixed binding mismatch").code,
        "binding_mismatch"
    );

    let wrong_thread = fx
        .run_cli_with_stdin(
            &prepare_task_args_with_scope(
                &fx,
                Some(fx.binding_id.as_str()),
                Some(OTHER_THREAD_UUID),
                Some(fx.repo.as_path()),
            ),
            &stdin,
            Some(&temp_dir),
        )
        .await;
    assert_eq!(
        expect_failure(&wrong_thread, "prepare-task with a fixed thread mismatch").code,
        "thread_uuid_mismatch"
    );

    let other_repo = fx.root.join("prepare-task-other-repo");
    let _other_commit = init_disposable_repo(&other_repo);
    let wrong_project_root = fx
        .run_cli_with_stdin(
            &prepare_task_args_with_scope(
                &fx,
                Some(fx.binding_id.as_str()),
                Some(THREAD_UUID),
                Some(other_repo.as_path()),
            ),
            &stdin,
            Some(&temp_dir),
        )
        .await;
    assert_eq!(
        expect_failure(
            &wrong_project_root,
            "prepare-task with a fixed project-root mismatch",
        )
        .code,
        "invalid_repository"
    );

    let mut changed_repo = request.clone();
    changed_repo["repository_path"] = serde_json::json!(other_repo);
    let request_repo_mismatch = fx
        .run_cli_with_stdin(
            &prepare_task_args(&fx),
            &prepare_task_bytes(&changed_repo),
            Some(&temp_dir),
        )
        .await;
    assert_eq!(
        expect_failure(
            &request_repo_mismatch,
            "prepare-task with a stdin repository_path outside fixed scope",
        )
        .code,
        "invalid_repository"
    );

    assert_eq!(total_plan_revision_count(&fx), before);
    assert!(directory_entries(&temp_dir).is_empty());
    fx.assert_no_approval_artifacts();

    // A stale profile/registry revision must be rejected before the draft is created.
    let advanced_revision = fx.advance_registry();
    assert_eq!(advanced_revision, fx.registry_revision + 1);
    let stale_request = prepare_task_request(&fx, "fast-stale-1");
    let stale = fx
        .run_cli_with_stdin(
            &prepare_task_args(&fx),
            &prepare_task_bytes(&stale_request),
            Some(&temp_dir),
        )
        .await;
    assert_eq!(
        expect_failure(&stale, "prepare-task with a stale registry revision").code,
        "stale_registry_revision"
    );
    assert_eq!(total_plan_revision_count(&fx), before);
    assert!(directory_entries(&temp_dir).is_empty());
    fx.assert_no_approval_artifacts();

    fx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn prepare_task_rejects_malformed_requests_and_conflicting_keys_without_side_effects() {
    let mut fx = ApprovalFixture::start().await;
    let temp_dir = fx.private_preview_temp_dir();
    let args = prepare_task_args(&fx);
    let before = total_plan_revision_count(&fx);

    let valid = prepare_task_request(&fx, "fast-malformed-1");
    let empty = fx.run_cli_with_stdin(&args, b"", Some(&temp_dir)).await;
    assert_eq!(
        expect_failure(&empty, "prepare-task with empty stdin").code,
        "invalid_input"
    );

    let malformed_bytes = format!("{{ not valid prepare-task JSON {SECRET_SENTINEL}");
    let malformed = fx
        .run_cli_with_stdin(&args, malformed_bytes.as_bytes(), Some(&temp_dir))
        .await;
    assert_eq!(
        expect_failure(&malformed, "prepare-task with malformed stdin").code,
        "invalid_input"
    );
    assert_no_secret_echo(&malformed, "prepare-task with malformed stdin");

    let mut unknown = valid.clone();
    unknown
        .as_object_mut()
        .expect("prepare-task request is an object")
        .insert("unexpected_field".to_string(), Value::Bool(true));
    assert_eq!(
        expect_failure(
            &fx.run_cli_with_stdin(&args, &prepare_task_bytes(&unknown), Some(&temp_dir))
                .await,
            "prepare-task with an unknown top-level field",
        )
        .code,
        "invalid_input"
    );

    let mut missing_budgets = valid.clone();
    missing_budgets
        .as_object_mut()
        .expect("prepare-task request is an object")
        .remove("budgets");
    assert_eq!(
        expect_failure(
            &fx.run_cli_with_stdin(
                &args,
                &prepare_task_bytes(&missing_budgets),
                Some(&temp_dir)
            )
            .await,
            "prepare-task without budgets",
        )
        .code,
        "invalid_input"
    );

    let mut non_coder = valid.clone();
    non_coder["task"]["role"] = serde_json::json!("reviewer");
    assert_eq!(
        expect_failure(
            &fx.run_cli_with_stdin(&args, &prepare_task_bytes(&non_coder), Some(&temp_dir))
                .await,
            "prepare-task with a non-coder task",
        )
        .code,
        "invalid_input"
    );

    let mut dependent = valid.clone();
    dependent["task"]["depends_on"] = serde_json::json!(["task-other"]);
    assert_eq!(
        expect_failure(
            &fx.run_cli_with_stdin(&args, &prepare_task_bytes(&dependent), Some(&temp_dir))
                .await,
            "prepare-task with a dependent task",
        )
        .code,
        "invalid_input"
    );

    let mut invalid_base = valid.clone();
    invalid_base["base_commit"] = serde_json::json!("0".repeat(40));
    assert_eq!(
        expect_failure(
            &fx.run_cli_with_stdin(&args, &prepare_task_bytes(&invalid_base), Some(&temp_dir))
                .await,
            "prepare-task with a non-existent base commit",
        )
        .code,
        "invalid_base_commit"
    );

    let mut invalid_budgets = valid.clone();
    invalid_budgets["budgets"] = serde_json::json!({"timeout_ms": 0});
    assert_eq!(
        expect_failure(
            &fx.run_cli_with_stdin(
                &args,
                &prepare_task_bytes(&invalid_budgets),
                Some(&temp_dir)
            )
            .await,
            "prepare-task with out-of-bounds budgets",
        )
        .code,
        "validation"
    );

    let mut oversize = prepare_task_bytes(&valid);
    oversize.resize(MAX_REQUEST_BYTES + 1, b' ');
    assert_eq!(
        expect_failure(
            &fx.run_cli_with_stdin(&args, &oversize, Some(&temp_dir))
                .await,
            "prepare-task with oversize stdin",
        )
        .code,
        "invalid_input"
    );

    assert_eq!(total_plan_revision_count(&fx), before);
    assert!(directory_entries(&temp_dir).is_empty());
    fx.assert_no_approval_artifacts();

    // The first valid invocation creates the original plan.
    let first: PrivatePreviewReceipt = parse_success(
        &fx.run_cli_with_stdin(&args, &prepare_task_bytes(&valid), Some(&temp_dir))
            .await,
        "first valid prepare-task invocation",
    );
    let plan_id = first.preview.request.plan_id.clone();
    assert_eq!(total_plan_revision_count(&fx), before + 1);
    assert_eq!(prepare_task_plan_revision_count(&fx, &plan_id), 1);
    assert_eq!(fx.approval_count(), 0);

    // Reusing the stable key for a different task must fail without creating a second plan.
    let mut changed_task = valid.clone();
    changed_task["task"]["id"] = serde_json::json!("task-other");
    changed_task["task"]["title"] = serde_json::json!("A different fast-path task");
    let conflict = fx
        .run_cli_with_stdin(&args, &prepare_task_bytes(&changed_task), Some(&temp_dir))
        .await;
    assert_eq!(
        expect_failure(
            &conflict,
            "prepare-task with the same command_key and a different task",
        )
        .code,
        "idempotency_conflict"
    );
    assert_eq!(total_plan_revision_count(&fx), before + 1);
    assert_eq!(prepare_task_plan_revision_count(&fx, &plan_id), 1);
    assert_eq!(directory_entries(&temp_dir).len(), 1);
    assert_eq!(fx.approval_count(), 0);
    fx.assert_no_worker_invocation();

    // Reusing the key with a different summary is also a conflicting original.
    let mut changed_summary = valid.clone();
    changed_summary["summary"] = serde_json::json!("A different fast-path plan summary");
    let summary_conflict = fx
        .run_cli_with_stdin(
            &args,
            &prepare_task_bytes(&changed_summary),
            Some(&temp_dir),
        )
        .await;
    assert_eq!(
        expect_failure(
            &summary_conflict,
            "prepare-task with the same command_key and a different summary",
        )
        .code,
        "idempotency_conflict"
    );
    assert_eq!(total_plan_revision_count(&fx), before + 1);
    assert_eq!(prepare_task_plan_revision_count(&fx, &plan_id), 1);
    assert_eq!(fx.approval_count(), 0);
    fx.assert_no_worker_invocation();
    assert_eq!(fx.head(), fx.base_commit);
    assert!(fx.worktree_names().is_empty());

    // A superseded original publication must be rejected as stale rather than redone.
    let superseding_draft = fx
        .plans
        .change(
            1,
            PlanChange::SaveDraft {
                plan_id: plan_id.clone(),
                binding_id: fx.binding_id.clone(),
                profile_id: PROFILE_ID.to_string(),
                profile_revision: fx.registry_revision,
                summary: "Superseding fast-path plan".to_string(),
                tasks: vec![PlanTask {
                    id: "task-fast".to_string(),
                    title: "Superseded fast-path task".to_string(),
                    instruction: "Implement a superseding fast-path task.".to_string(),
                    role: WorkflowRole::Coder,
                    difficulty: Difficulty::Easy,
                    depends_on: Vec::new(),
                    allowed_write_paths: vec!["src/lib.rs".to_string()],
                }],
            },
        )
        .expect("save superseding fast-path draft");
    fx.plans
        .change(
            superseding_draft.revision,
            PlanChange::Publish {
                plan_id: plan_id.clone(),
                revision: superseding_draft.revision,
            },
        )
        .expect("publish superseding fast-path plan");
    assert_eq!(total_plan_revision_count(&fx), before + 2);

    let stale_plan = fx
        .run_cli_with_stdin(&args, &prepare_task_bytes(&valid), Some(&temp_dir))
        .await;
    assert_eq!(
        expect_failure(
            &stale_plan,
            "prepare-task replay after the original plan was superseded",
        )
        .code,
        "stale_revision"
    );
    assert_eq!(total_plan_revision_count(&fx), before + 2);
    assert_eq!(prepare_task_plan_revision_count(&fx, &plan_id), 2);
    assert_eq!(directory_entries(&temp_dir).len(), 1);
    assert_eq!(fx.approval_count(), 0);
    fx.assert_no_worker_invocation();

    fx.shutdown().await;
}

fn prepare_task_exact_args(fx: &ApprovalFixture) -> Vec<String> {
    let mut args = prepare_task_args(fx);
    args[0] = "prepare-task-exact".to_string();
    args
}

fn prepare_task_exact_request(
    fx: &ApprovalFixture,
    command_key: &str,
    allowed_write_paths: Vec<&str>,
    required_changed_paths: Option<Vec<&str>>,
) -> Value {
    let mut request = prepare_task_request(fx, command_key);
    request["task"]["allowed_write_paths"] = serde_json::json!(allowed_write_paths);
    if let Some(required) = required_changed_paths {
        request["task"]["required_changed_paths"] = serde_json::json!(required);
    }
    request["budgets"] = serde_json::json!({
        "timeout_ms": 90_000,
        "max_line_bytes": 16_384,
        "max_stdout_bytes": 2_097_152,
        "max_stderr_bytes": 100_000,
        "max_events": 321
    });
    request
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn prepare_task_exact_with_required_changed_paths_round_trips_preview_and_request_v2() {
    let mut fx = ApprovalFixture::start().await;
    let temp_dir = fx.private_preview_temp_dir();
    let context_path = write_prepare_task_context(&fx, "prepare-task-exact-r-context.json");
    let context_args = vec![
        "--context".to_string(),
        context_path.display().to_string(),
        "approve".to_string(),
        "prepare-task-exact".to_string(),
    ];

    // Legacy `prepare-task` (non-exact) must reject `required_changed_paths` strictly before mutation.
    let before = total_plan_revision_count(&fx);
    let mut legacy_with_r = prepare_task_request(&fx, "legacy-rejects-r-1");
    legacy_with_r["task"]["required_changed_paths"] = serde_json::json!(["src/lib.rs"]);
    let legacy_rejected = fx
        .run_cli_with_stdin(
            &prepare_task_args(&fx),
            &prepare_task_bytes(&legacy_with_r),
            Some(&temp_dir),
        )
        .await;
    assert_eq!(
        expect_failure(&legacy_rejected, "legacy prepare-task rejects R").code,
        "invalid_input"
    );
    assert_eq!(total_plan_revision_count(&fx), before);
    assert!(directory_entries(&temp_dir).is_empty());

    // 1. Omitted `required_changed_paths` on `prepare-task-exact` stays legacy-compatible (`None`, omitted in JSON, no `record_request`).
    let omitted_req = prepare_task_exact_request(
        &fx,
        "exact-same-plan-r-1",
        vec!["src/lib.rs", "src/extra.rs"],
        None,
    );
    let omitted_output = run_command_with_stdin(
        context_command(),
        &context_args,
        &prepare_task_bytes(&omitted_req),
        Some(&temp_dir),
    )
    .await;
    let omitted_receipt: PrivatePreviewReceipt = parse_success(
        &omitted_output,
        "prepare-task-exact without required_changed_paths",
    );
    let omitted_raw: Value =
        serde_json::from_slice(&omitted_output.stdout).expect("omitted receipt raw JSON");
    let omitted_obj = omitted_raw.as_object().expect("omitted receipt is object");
    assert_eq!(
        omitted_obj.len(),
        2,
        "legacy None prepare-task-exact receipt must keep exact 2-key wire shape without record_request"
    );
    assert!(omitted_obj.get("record_request").is_none());
    assert!(omitted_receipt.preview.tasks[0]
        .required_changed_paths
        .is_none());
    let omitted_preview_text =
        fs::read_to_string(&omitted_receipt.preview_path).expect("read omitted preview.json");
    assert!(
        !omitted_preview_text.contains("required_changed_paths"),
        "omitted required_changed_paths must not appear in serialized preview JSON"
    );

    // 2. Opt-in `required_changed_paths` (`R`) on the exact same plan presents R separately from A,
    // alters both subject_digest and core-derived profile_id without changing base request, and
    // exposes typed v2 `record_request` directly on stdout.
    let with_r_req = prepare_task_exact_request(
        &fx,
        "exact-same-plan-r-1",
        vec!["src/lib.rs", "src/extra.rs"],
        Some(vec!["src/lib.rs"]),
    );
    let with_r_output = run_command_with_stdin(
        context_command(),
        &context_args,
        &prepare_task_bytes(&with_r_req),
        Some(&temp_dir),
    )
    .await;
    let with_r_receipt: PrivatePreviewReceipt = parse_success(
        &with_r_output,
        "prepare-task-exact with required_changed_paths",
    );
    let with_r_raw: Value =
        serde_json::from_slice(&with_r_output.stdout).expect("with-R receipt raw JSON");
    let with_r_obj = with_r_raw.as_object().expect("with-R receipt is object");
    assert_eq!(
        with_r_obj.len(),
        3,
        "Some(R) prepare-task-exact receipt must expose preview_path, preview, and record_request"
    );
    let with_r_wire_record_request = with_r_obj
        .get("record_request")
        .expect("Some(R) prepare-task-exact must expose record_request directly to client");
    let parsed_wire_req_v2: OperatorApprovalRequestV2 =
        serde_json::from_value(with_r_wire_record_request.clone())
            .expect("wire record_request must match OperatorApprovalRequestV2");
    assert_eq!(
        parsed_wire_req_v2.schema_version,
        AUTHORITY_REQUEST_V2_SCHEMA_VERSION
    );
    assert_eq!(parsed_wire_req_v2.request, with_r_receipt.preview.request);
    assert_eq!(
        parsed_wire_req_v2.task_required_changed_paths,
        BTreeMap::from([("task-fast".to_string(), vec!["src/lib.rs".to_string()])])
    );

    assert_eq!(
        with_r_receipt.preview.request,
        omitted_receipt.preview.request
    );
    assert_eq!(
        with_r_receipt.preview.request.approved_write_paths,
        vec!["src/lib.rs".to_string(), "src/extra.rs".to_string()]
    );
    assert_eq!(
        with_r_receipt.preview.tasks[0].allowed_write_paths,
        vec!["src/lib.rs".to_string(), "src/extra.rs".to_string()]
    );
    assert_eq!(
        with_r_receipt.preview.tasks[0].required_changed_paths,
        Some(vec!["src/lib.rs".to_string()])
    );
    assert_ne!(
        with_r_receipt.preview.subject_digest, omitted_receipt.preview.subject_digest,
        "selecting R must alter subject_digest"
    );
    assert_ne!(
        with_r_receipt.preview.tasks[0].profile_id, omitted_receipt.preview.tasks[0].profile_id,
        "selecting R must alter core-derived profile_id"
    );
    let preview_file_text =
        fs::read_to_string(&with_r_receipt.preview_path).expect("read preview.json with R");
    assert!(preview_file_text.contains("\"required_changed_paths\":[\"src/lib.rs\"]"));
    let preview_file_parsed: OperatorApprovalPreview =
        serde_json::from_str(&preview_file_text).expect("preview.json remains pure preview");
    assert_eq!(preview_file_parsed, with_r_receipt.preview);
    fx.assert_no_approval_artifacts();

    // 3. Record via default `--preview` persists R while keeping allowed_write_paths = A.
    let summary_preview: ApprovalSummary = parse_success(
        &fx.run_record_preview(
            &with_r_receipt.preview_path,
            &with_r_receipt.preview.subject_digest,
            THREAD_UUID,
            true,
        )
        .await,
        "record --preview with required_changed_paths",
    );
    assert_eq!(
        summary_preview.subject_digest,
        with_r_receipt.preview.subject_digest
    );
    let persisted = fx.persisted_record();
    assert_eq!(
        persisted.tasks[0].allowed_write_paths,
        vec!["src/lib.rs".to_string(), "src/extra.rs".to_string()]
    );
    assert_eq!(
        persisted.tasks[0].required_changed_paths,
        Some(vec!["src/lib.rs".to_string()])
    );

    // 4. Write the raw `record_request` JSON received directly from CLI stdout (no Rust synthesis)
    // and verify `record --request` replays identically with the same scope and original digest.
    let req_v2_path = fx.write_request("record-v2-roundtrip.json", with_r_wire_record_request);
    let summary_req_v2: ApprovalSummary = parse_success(
        &fx.run_record(
            &req_v2_path,
            &with_r_receipt.preview.subject_digest,
            THREAD_UUID,
            true,
        )
        .await,
        "record --request with raw CLI record_request idempotent replay",
    );
    assert_eq!(summary_req_v2, summary_preview);
    assert_eq!(fx.approval_count(), 1);

    // 5. Commit a second R-constrained task via `record --request` FIRST using the raw CLI
    // `record_request` received from stdout, then replay default `--preview`.
    let dir_r_req = prepare_task_exact_request(
        &fx,
        "exact-dir-r-2",
        vec!["src/"],
        Some(vec!["src/lib.rs", "src/new_exact.rs"]),
    );
    let dir_r_output = fx
        .run_cli_with_stdin(
            &prepare_task_exact_args(&fx),
            &prepare_task_bytes(&dir_r_req),
            Some(&temp_dir),
        )
        .await;
    let dir_r_receipt: PrivatePreviewReceipt = parse_success(
        &dir_r_output,
        "prepare-task-exact with directory A and exact-file R",
    );
    let dir_r_raw: Value =
        serde_json::from_slice(&dir_r_output.stdout).expect("dir-R receipt raw JSON");
    let dir_wire_record_request = dir_r_raw
        .get("record_request")
        .expect("dir-R receipt exposes record_request");
    assert_eq!(
        dir_r_receipt.preview.tasks[0].allowed_write_paths,
        vec!["src/".to_string()]
    );
    assert_eq!(
        dir_r_receipt.preview.tasks[0].required_changed_paths,
        Some(vec![
            "src/lib.rs".to_string(),
            "src/new_exact.rs".to_string()
        ])
    );
    let dir_req_v2_path = fx.write_request("record-v2-dir.json", dir_wire_record_request);
    let dir_summary_req: ApprovalSummary = parse_success(
        &fx.run_record(
            &dir_req_v2_path,
            &dir_r_receipt.preview.subject_digest,
            THREAD_UUID,
            true,
        )
        .await,
        "record --request with raw CLI record_request commits directly",
    );
    let dir_summary_preview: ApprovalSummary = parse_success(
        &fx.run_record_preview(
            &dir_r_receipt.preview_path,
            &dir_r_receipt.preview.subject_digest,
            THREAD_UUID,
            true,
        )
        .await,
        "record --preview replays v2 commit idempotently",
    );
    assert_eq!(dir_summary_preview, dir_summary_req);
    assert_eq!(
        dir_summary_req.subject_digest,
        dir_r_receipt.preview.subject_digest
    );
    assert_eq!(fx.approval_count(), 2);
    let dir_persisted = fx
        .authority_store()
        .get_for_operator(&dir_summary_req.approval_id)
        .expect("load dir R approval");
    assert_eq!(
        dir_persisted.tasks[0].allowed_write_paths,
        vec!["src/".to_string()]
    );
    assert_eq!(
        dir_persisted.tasks[0].required_changed_paths,
        Some(vec![
            "src/lib.rs".to_string(),
            "src/new_exact.rs".to_string()
        ])
    );
    fx.assert_no_worker_invocation();

    fx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn prepare_task_exact_rejects_invalid_required_changed_paths_before_mutation() {
    let mut fx = ApprovalFixture::start().await;
    let temp_dir = fx.private_preview_temp_dir();
    let args = prepare_task_exact_args(&fx);
    let before = total_plan_revision_count(&fx);

    let overbound: Vec<String> = (0..=threadlane_authority::MAX_REQUIRED_CHANGED_PATHS)
        .map(|i| format!("src/generated_{i}.rs"))
        .collect();
    let overbound_refs: Vec<&str> = overbound.iter().map(String::as_str).collect();

    let invalid_cases: Vec<(&str, Vec<&str>, Vec<&str>, &str)> = vec![
        (
            "empty required_changed_paths",
            vec!["src/lib.rs", "src/extra.rs"],
            vec![],
            "validation",
        ),
        (
            "empty path entry in required_changed_paths",
            vec!["src/lib.rs", "src/extra.rs"],
            vec![""],
            "invalid_write_scope",
        ),
        (
            "duplicate required_changed_paths",
            vec!["src/lib.rs", "src/extra.rs"],
            vec!["src/lib.rs", "src/lib.rs"],
            "validation",
        ),
        (
            "foreign path outside allowed_write_paths",
            vec!["src/lib.rs", "src/extra.rs"],
            vec!["src/other.rs"],
            "validation",
        ),
        (
            "directory trailing slash in required_changed_paths",
            vec!["src/"],
            vec!["src/"],
            "validation",
        ),
        (
            "bare existing directory in required_changed_paths",
            vec!["src/"],
            vec!["src"],
            "invalid_write_scope",
        ),
        (
            "over-bound required_changed_paths",
            vec!["src/"],
            overbound_refs,
            "validation",
        ),
    ];

    for (idx, (label, allowed, required, expected_code)) in invalid_cases.into_iter().enumerate() {
        let req = prepare_task_exact_request(
            &fx,
            &format!("exact-invalid-r-{idx}"),
            allowed,
            Some(required),
        );
        let out = fx
            .run_cli_with_stdin(&args, &prepare_task_bytes(&req), Some(&temp_dir))
            .await;
        assert_eq!(
            expect_failure(&out, label).code,
            expected_code,
            "unexpected code for {label}"
        );
    }

    assert_eq!(
        total_plan_revision_count(&fx),
        before,
        "invalid required_changed_paths must fail before saving any plan draft"
    );
    assert!(
        directory_entries(&temp_dir).is_empty(),
        "invalid required_changed_paths must leave zero private preview directories"
    );
    fx.assert_no_approval_artifacts();

    fx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn record_rejects_tampered_required_changed_paths_in_preview_and_request_v2_before_commit() {
    let mut fx = ApprovalFixture::start().await;
    let temp_dir = fx.private_preview_temp_dir();
    let args = prepare_task_exact_args(&fx);

    let req_lib = prepare_task_exact_request(
        &fx,
        "exact-tamper-r-1",
        vec!["src/lib.rs", "src/extra.rs"],
        Some(vec!["src/lib.rs"]),
    );
    let output_lib = fx
        .run_cli_with_stdin(&args, &prepare_task_bytes(&req_lib), Some(&temp_dir))
        .await;
    let receipt_lib: PrivatePreviewReceipt =
        parse_success(&output_lib, "prepare-task-exact with R=[src/lib.rs]");
    let raw_lib: Value =
        serde_json::from_slice(&output_lib.stdout).expect("raw JSON for R=[src/lib.rs]");
    let raw_record_request = raw_lib
        .get("record_request")
        .expect("prepare-task-exact exposes record_request for R=[src/lib.rs]")
        .clone();

    // Replaying the same command_key/task with a different valid subset R=[src/extra.rs] yields a different digest.
    let req_extra = prepare_task_exact_request(
        &fx,
        "exact-tamper-r-1",
        vec!["src/lib.rs", "src/extra.rs"],
        Some(vec!["src/extra.rs"]),
    );
    let receipt_extra: PrivatePreviewReceipt = parse_success(
        &fx.run_cli_with_stdin(&args, &prepare_task_bytes(&req_extra), Some(&temp_dir))
            .await,
        "prepare-task-exact with R=[src/extra.rs]",
    );
    assert_ne!(
        receipt_lib.preview.subject_digest,
        receipt_extra.preview.subject_digest
    );

    // 1. Tamper preview.json in place (copy) changing R to another valid subset of A while keeping old digest.
    let mut tampered_preview = receipt_lib.preview.clone();
    tampered_preview.tasks[0].required_changed_paths = Some(vec!["src/extra.rs".to_string()]);
    let tampered_preview_path = fx.write_request("tampered-r-preview.json", &tampered_preview);
    let out = fx
        .run_record_preview(
            &tampered_preview_path,
            &receipt_lib.preview.subject_digest,
            THREAD_UUID,
            true,
        )
        .await;
    assert_eq!(
        expect_failure(&out, "tampered preview R with old digest").code,
        "validation"
    );

    // 2. Tamper preview.json by stripping R to None.
    let mut stripped_preview = receipt_lib.preview.clone();
    stripped_preview.tasks[0].required_changed_paths = None;
    let stripped_preview_path = fx.write_request("stripped-r-preview.json", &stripped_preview);
    let out = fx
        .run_record_preview(
            &stripped_preview_path,
            &receipt_lib.preview.subject_digest,
            THREAD_UUID,
            true,
        )
        .await;
    assert_eq!(
        expect_failure(&out, "preview with stripped R").code,
        "validation"
    );

    // 3. Submit self-consistent preview for R=[src/extra.rs] against --confirm-subject for R=[src/lib.rs].
    let out = fx
        .run_record_preview(
            &receipt_extra.preview_path,
            &receipt_lib.preview.subject_digest,
            THREAD_UUID,
            true,
        )
        .await;
    assert_eq!(
        expect_failure(&out, "different R preview with confirmed lib digest").code,
        "validation"
    );

    // 4. Tamper the raw CLI `record_request` JSON changing R to another valid subset of A,
    // dropping R, or introducing an out-of-scope path against the confirmed lib digest.
    let mut tampered_req_v2 = raw_record_request.clone();
    tampered_req_v2["task_required_changed_paths"]["task-fast"] =
        serde_json::json!(["src/extra.rs"]);
    let tampered_req_v2_path = fx.write_request("tampered-r-request-v2.json", &tampered_req_v2);
    let out = fx
        .run_record(
            &tampered_req_v2_path,
            &receipt_lib.preview.subject_digest,
            THREAD_UUID,
            true,
        )
        .await;
    assert_eq!(
        expect_failure(&out, "tampered raw record_request R with old digest").code,
        "validation"
    );

    let mut dropped_r_req = raw_record_request.clone();
    dropped_r_req["task_required_changed_paths"] = serde_json::json!({});
    let dropped_r_path = fx.write_request("dropped-r-request-v2.json", &dropped_r_req);
    let out = fx
        .run_record(
            &dropped_r_path,
            &receipt_lib.preview.subject_digest,
            THREAD_UUID,
            true,
        )
        .await;
    assert_eq!(
        expect_failure(&out, "raw record_request with dropped R against R digest").code,
        "validation"
    );

    let mut foreign_r_req = raw_record_request.clone();
    foreign_r_req["task_required_changed_paths"]["task-fast"] = serde_json::json!(["src/other.rs"]);
    let foreign_r_path = fx.write_request("foreign-r-request-v2.json", &foreign_r_req);
    let out = fx
        .run_record(
            &foreign_r_path,
            &receipt_lib.preview.subject_digest,
            THREAD_UUID,
            true,
        )
        .await;
    assert_eq!(
        expect_failure(&out, "raw record_request with out-of-scope R").code,
        "validation"
    );

    // 5. Reject unsupported schema_version on raw record_request / OperatorApprovalRequestV2.
    let mut bad_ver_raw = raw_record_request.clone();
    bad_ver_raw["schema_version"] = serde_json::json!(99);
    let bad_ver_path = fx.write_request("bad-version-request-v2.json", &bad_ver_raw);
    let out = fx
        .run_record(
            &bad_ver_path,
            &receipt_lib.preview.subject_digest,
            THREAD_UUID,
            true,
        )
        .await;
    assert_eq!(
        expect_failure(&out, "unsupported v2 schema_version").code,
        "validation"
    );

    // 6. Reject legacy OperatorApprovalRequest (`preview.request`, without R) against the R-bound digest.
    let legacy_req_path = fx.write_request("legacy-dropped-r.json", &receipt_lib.preview.request);
    let out = fx
        .run_record(
            &legacy_req_path,
            &receipt_lib.preview.subject_digest,
            THREAD_UUID,
            true,
        )
        .await;
    assert_eq!(
        expect_failure(&out, "legacy request dropping R against R digest").code,
        "validation"
    );

    // Zero approvals may exist after every tamper attempt.
    fx.assert_no_approval_artifacts();

    // 7. Genuine untampered raw `record_request` commits cleanly via `--request`, and default
    // `--preview` replays idempotently with identical scope and original digest.
    let genuine_req_path = fx.write_request("genuine-record-request-v2.json", &raw_record_request);
    let summary_req: ApprovalSummary = parse_success(
        &fx.run_record(
            &genuine_req_path,
            &receipt_lib.preview.subject_digest,
            THREAD_UUID,
            true,
        )
        .await,
        "genuine untampered raw record_request commits cleanly",
    );
    let summary_preview: ApprovalSummary = parse_success(
        &fx.run_record_preview(
            &receipt_lib.preview_path,
            &receipt_lib.preview.subject_digest,
            THREAD_UUID,
            true,
        )
        .await,
        "genuine untampered preview replays idempotently",
    );
    assert_eq!(summary_preview, summary_req);
    assert_eq!(
        summary_req.subject_digest,
        receipt_lib.preview.subject_digest
    );
    let record = fx.persisted_record();
    assert_eq!(
        record.tasks[0].required_changed_paths,
        Some(vec!["src/lib.rs".to_string()])
    );
    assert_eq!(
        record.tasks[0].allowed_write_paths,
        vec!["src/lib.rs".to_string(), "src/extra.rs".to_string()]
    );
    assert_eq!(
        AUTHORITY_REQUEST_V2_SCHEMA_VERSION, 2,
        "v2 request wrapper schema_version must remain pinned to 2"
    );

    fx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn prepare_and_prepare_private_expose_record_request_only_for_some_r_and_round_trip_wire_json(
) {
    let mut fx = ApprovalFixture::start().await;
    let temp_dir = fx.private_preview_temp_dir();

    // 1. Legacy None intent and v2 intent with empty task_required_changed_paths preserve exact
    // legacy wire bytes (no `record_request`) across `prepare`, `prepare-private`, and
    // `prepare-private-exact`.
    let mut base_intent = fx.intent();
    base_intent.budgets = serde_json::from_value(serde_json::json!({
        "timeout_ms": 90_000,
        "max_line_bytes": 16_384,
        "max_stdout_bytes": 2_097_152,
        "max_stderr_bytes": 100_000,
        "max_events": 321
    }))
    .expect("explicit valid budgets");

    let legacy_intent_path = fx.write_request("prepare-legacy-intent.json", &base_intent);
    let legacy_prepare_out = fx.run_prepare(&legacy_intent_path).await;
    let legacy_preview: OperatorApprovalPreview =
        parse_success(&legacy_prepare_out, "prepare legacy None intent");
    let legacy_raw: Value =
        serde_json::from_slice(&legacy_prepare_out.stdout).expect("legacy prepare raw JSON");
    let legacy_obj = legacy_raw.as_object().expect("legacy prepare is object");
    assert_eq!(
        legacy_obj.len(),
        3,
        "legacy None prepare must emit only request, subject_digest, and tasks"
    );
    assert!(legacy_obj.get("record_request").is_none());

    let empty_v2_intent = OperatorApprovalIntentV2 {
        schema_version: AUTHORITY_REQUEST_V2_SCHEMA_VERSION,
        intent: base_intent.clone(),
        task_required_changed_paths: BTreeMap::new(),
    };
    let empty_v2_path = fx.write_request("prepare-v2-empty-r.json", &empty_v2_intent);
    let empty_v2_out = fx.run_prepare(&empty_v2_path).await;
    assert_eq!(
        empty_v2_out.stdout, legacy_prepare_out.stdout,
        "v2 intent with empty R map must produce byte-for-byte identical legacy prepare stdout"
    );

    let mut exact_args = fx.prepare_private_args();
    exact_args[0] = "prepare-private-exact".to_string();
    let empty_v2_bytes = serde_json::to_vec(&empty_v2_intent).expect("serialize empty v2 intent");
    for (label, args) in [
        ("prepare-private empty R", fx.prepare_private_args()),
        ("prepare-private-exact empty R", exact_args.clone()),
    ] {
        let out = fx
            .run_cli_with_stdin(&args, &empty_v2_bytes, Some(&temp_dir))
            .await;
        let receipt: PrivatePreviewReceipt = parse_success(&out, label);
        let raw: Value = serde_json::from_slice(&out.stdout).expect("empty-R receipt raw JSON");
        let obj = raw.as_object().expect("empty-R receipt is object");
        assert_eq!(obj.len(), 2, "{label} must omit record_request");
        assert!(obj.get("record_request").is_none());
        assert_eq!(
            fs::read(&receipt.preview_path).expect("read empty-R preview.json"),
            legacy_prepare_out.stdout,
            "{label} preview.json bytes must match legacy prepare stdout"
        );
    }

    // 2. `prepare --request` with Some(R) on task-root (and None on task-child) exposes
    // `record_request` directly on stdout; both `--request` (from raw `record_request`) and
    // `--preview` (from raw `prepare` stdout bytes) record identical scope and original digest,
    // while tampered R or inconsistent embedded `record_request` fails closed.
    let v2_partial_intent = OperatorApprovalIntentV2 {
        schema_version: AUTHORITY_REQUEST_V2_SCHEMA_VERSION,
        intent: base_intent.clone(),
        task_required_changed_paths: BTreeMap::from([(
            "task-root".to_string(),
            vec!["src/lib.rs".to_string()],
        )]),
    };
    let v2_partial_path = fx.write_request("prepare-v2-partial-r.json", &v2_partial_intent);
    let v2_prepare_out = fx.run_prepare(&v2_partial_path).await;
    assert!(
        v2_prepare_out.status.success(),
        "prepare --request v2 intent must succeed: {}",
        String::from_utf8_lossy(&v2_prepare_out.stderr)
    );
    let v2_prepare_raw: Value =
        serde_json::from_slice(&v2_prepare_out.stdout).expect("v2 prepare raw JSON");
    let v2_prepare_obj = v2_prepare_raw
        .as_object()
        .expect("v2 prepare stdout is object");
    assert_eq!(
        v2_prepare_obj.len(),
        4,
        "Some(R) prepare stdout must expose request, subject_digest, tasks, and record_request"
    );
    let v2_subject = v2_prepare_obj["subject_digest"]
        .as_str()
        .expect("v2 prepare subject_digest string")
        .to_string();
    assert_ne!(
        v2_subject, legacy_preview.subject_digest,
        "Some(R) must change subject_digest relative to legacy None"
    );
    let v2_wire_record_request = v2_prepare_obj["record_request"].clone();
    let v2_typed_record_request: OperatorApprovalRequestV2 =
        serde_json::from_value(v2_wire_record_request.clone())
            .expect("prepare stdout record_request matches OperatorApprovalRequestV2");
    assert_eq!(
        v2_typed_record_request.schema_version,
        AUTHORITY_REQUEST_V2_SCHEMA_VERSION
    );
    assert_eq!(
        serde_json::to_value(&v2_typed_record_request.request).unwrap(),
        v2_prepare_obj["request"]
    );
    assert_eq!(
        v2_typed_record_request.task_required_changed_paths,
        BTreeMap::from([("task-root".to_string(), vec!["src/lib.rs".to_string()])])
    );

    // Tamper checks before any commit: all must fail closed with zero approvals.
    let mut changed_r = v2_wire_record_request.clone();
    changed_r["task_required_changed_paths"] = serde_json::json!({
        "task-root": ["src/lib.rs"],
        "task-child": ["src/extra.rs"]
    });
    let changed_r_path = fx.write_request("prepare-v2-changed-r.json", &changed_r);
    assert_eq!(
        expect_failure(
            &fx.run_record(&changed_r_path, &v2_subject, THREAD_UUID, true)
                .await,
            "record --request with changed valid R against original digest",
        )
        .code,
        "validation"
    );

    let legacy_req_only_path = fx.write_request(
        "prepare-v2-legacy-req-only.json",
        &v2_prepare_obj["request"],
    );
    assert_eq!(
        expect_failure(
            &fx.run_record(&legacy_req_only_path, &v2_subject, THREAD_UUID, true)
                .await,
            "record --request with v1 preview.request dropping R against R digest",
        )
        .code,
        "validation"
    );

    let mut preview_tampered_embedded = v2_prepare_raw.clone();
    preview_tampered_embedded["record_request"]["task_required_changed_paths"] =
        serde_json::json!({});
    let preview_tampered_path = fx.write_request(
        "prepare-v2-tampered-embedded-record-request.json",
        &preview_tampered_embedded,
    );
    assert_eq!(
        expect_failure(
            &fx.run_record_preview(&preview_tampered_path, &v2_subject, THREAD_UUID, true)
                .await,
            "record --preview with inconsistent embedded record_request",
        )
        .code,
        "validation"
    );

    let mut preview_bad_ver = v2_prepare_raw.clone();
    preview_bad_ver["record_request"]["schema_version"] = serde_json::json!(99);
    let preview_bad_ver_path =
        fx.write_request("prepare-v2-bad-embedded-version.json", &preview_bad_ver);
    assert_eq!(
        expect_failure(
            &fx.run_record_preview(&preview_bad_ver_path, &v2_subject, THREAD_UUID, true)
                .await,
            "record --preview with unsupported embedded record_request schema_version",
        )
        .code,
        "validation"
    );

    let mut preview_null_req = v2_prepare_raw;
    preview_null_req["record_request"] = Value::Null;
    let preview_null_req_path =
        fx.write_request("prepare-v2-null-record-request.json", &preview_null_req);
    assert_eq!(
        expect_failure(
            &fx.run_record_preview(&preview_null_req_path, &v2_subject, THREAD_UUID, true)
                .await,
            "record --preview with null record_request",
        )
        .code,
        "invalid_input"
    );

    let mut legacy_smuggled = legacy_raw.clone();
    legacy_smuggled["record_request"] = serde_json::json!({
        "schema_version": AUTHORITY_REQUEST_V2_SCHEMA_VERSION,
        "request": legacy_raw["request"],
        "task_required_changed_paths": {}
    });
    let legacy_smuggled_path = fx.write_request(
        "prepare-legacy-smuggled-record-request.json",
        &legacy_smuggled,
    );
    assert_eq!(
        expect_failure(
            &fx.run_record_preview(
                &legacy_smuggled_path,
                &legacy_preview.subject_digest,
                THREAD_UUID,
                true,
            )
            .await,
            "record --preview with record_request smuggled onto legacy None preview",
        )
        .code,
        "validation"
    );
    fx.assert_no_approval_artifacts();

    // Commit directly from raw `prepare` stdout's `record_request`, then replay verbatim `prepare`
    // stdout bytes via `--preview`.
    let raw_req_v2_path = fx.write_request(
        "prepare-v2-raw-record-request.json",
        &v2_wire_record_request,
    );
    let summary_from_request: ApprovalSummary = parse_success(
        &fx.run_record(&raw_req_v2_path, &v2_subject, THREAD_UUID, true)
            .await,
        "record --request from prepare stdout record_request",
    );
    assert_eq!(summary_from_request.subject_digest, v2_subject);
    let piped_prepare_preview_path =
        fx.write_request_bytes("prepare-v2-piped-stdout.json", &v2_prepare_out.stdout);
    let summary_from_piped_preview: ApprovalSummary = parse_success(
        &fx.run_record_preview(&piped_prepare_preview_path, &v2_subject, THREAD_UUID, true)
            .await,
        "record --preview from raw piped prepare stdout bytes",
    );
    assert_eq!(summary_from_piped_preview, summary_from_request);
    assert_eq!(fx.approval_count(), 1);
    let persisted_partial = fx.persisted_record();
    let persisted_root = persisted_partial
        .tasks
        .iter()
        .find(|t| t.task_id == "task-root")
        .expect("task-root persisted");
    let persisted_child = persisted_partial
        .tasks
        .iter()
        .find(|t| t.task_id == "task-child")
        .expect("task-child persisted");
    assert_eq!(
        persisted_root.allowed_write_paths,
        vec!["src/lib.rs".to_string()]
    );
    assert_eq!(
        persisted_root.required_changed_paths,
        Some(vec!["src/lib.rs".to_string()])
    );
    assert_eq!(
        persisted_child.allowed_write_paths,
        vec!["src/extra.rs".to_string()]
    );
    assert_eq!(persisted_child.required_changed_paths, None);

    // 3. `prepare-private` and `prepare-private-exact` with Some(R) on both tasks expose
    // `record_request` on stdout while keeping `preview.json` pure `OperatorApprovalPreview`.
    let v2_both_intent = OperatorApprovalIntentV2 {
        schema_version: AUTHORITY_REQUEST_V2_SCHEMA_VERSION,
        intent: base_intent,
        task_required_changed_paths: BTreeMap::from([
            ("task-root".to_string(), vec!["src/lib.rs".to_string()]),
            ("task-child".to_string(), vec!["src/extra.rs".to_string()]),
        ]),
    };
    let v2_both_bytes = serde_json::to_vec(&v2_both_intent).expect("serialize both-R v2 intent");
    let priv_out = fx
        .run_cli_with_stdin(&fx.prepare_private_args(), &v2_both_bytes, Some(&temp_dir))
        .await;
    let priv_exact_out = fx
        .run_cli_with_stdin(&exact_args, &v2_both_bytes, Some(&temp_dir))
        .await;
    let priv_receipt: PrivatePreviewReceipt = parse_success(&priv_out, "prepare-private Some(R)");
    let priv_exact_receipt: PrivatePreviewReceipt =
        parse_success(&priv_exact_out, "prepare-private-exact Some(R)");
    assert_eq!(priv_receipt.preview, priv_exact_receipt.preview);

    let priv_raw: Value =
        serde_json::from_slice(&priv_out.stdout).expect("prepare-private raw JSON");
    let priv_exact_raw: Value =
        serde_json::from_slice(&priv_exact_out.stdout).expect("prepare-private-exact raw JSON");
    assert_eq!(priv_raw.as_object().unwrap().len(), 3);
    assert_eq!(priv_exact_raw.as_object().unwrap().len(), 3);
    assert_eq!(priv_raw["record_request"], priv_exact_raw["record_request"]);
    let priv_subject = priv_exact_receipt.preview.subject_digest.clone();
    assert_ne!(priv_subject, v2_subject);
    let priv_preview_file_val: Value = serde_json::from_slice(
        &fs::read(&priv_exact_receipt.preview_path).expect("read prepare-private-exact preview"),
    )
    .expect("preview.json is valid JSON");
    assert!(
        priv_preview_file_val
            .as_object()
            .unwrap()
            .get("record_request")
            .is_none(),
        "preview.json written by prepare-private-exact must remain pure OperatorApprovalPreview"
    );

    // Mutating R on raw stdout record_request (dropping task-child) fails closed against original digest.
    let mut priv_mutated = priv_exact_raw["record_request"].clone();
    priv_mutated["task_required_changed_paths"]
        .as_object_mut()
        .unwrap()
        .remove("task-child");
    let priv_mutated_path = fx.write_request("prepare-private-mutated-r.json", &priv_mutated);
    assert_eq!(
        expect_failure(
            &fx.run_record(&priv_mutated_path, &priv_subject, THREAD_UUID, true)
                .await,
            "record --request with mutated prepare-private-exact record_request",
        )
        .code,
        "validation"
    );
    assert_eq!(fx.approval_count(), 1);

    // Genuine raw `record_request` commits approval #2, and default `--preview` replays idempotently.
    let priv_req_path = fx.write_request(
        "prepare-private-raw-record-request.json",
        &priv_exact_raw["record_request"],
    );
    let priv_summary_req: ApprovalSummary = parse_success(
        &fx.run_record(&priv_req_path, &priv_subject, THREAD_UUID, true)
            .await,
        "record --request from prepare-private-exact stdout record_request",
    );
    let priv_summary_preview: ApprovalSummary = parse_success(
        &fx.run_record_preview(
            &priv_exact_receipt.preview_path,
            &priv_subject,
            THREAD_UUID,
            true,
        )
        .await,
        "record --preview from prepare-private-exact replays idempotently",
    );
    assert_eq!(priv_summary_preview, priv_summary_req);
    assert_eq!(priv_summary_req.subject_digest, priv_subject);
    assert_eq!(fx.approval_count(), 2);
    let persisted_both = fx
        .authority_store()
        .get_for_operator(&priv_summary_req.approval_id)
        .expect("load persisted both-R approval");
    let both_root = persisted_both
        .tasks
        .iter()
        .find(|t| t.task_id == "task-root")
        .expect("both task-root");
    let both_child = persisted_both
        .tasks
        .iter()
        .find(|t| t.task_id == "task-child")
        .expect("both task-child");
    assert_eq!(
        both_root.required_changed_paths,
        Some(vec!["src/lib.rs".to_string()])
    );
    assert_eq!(
        both_child.required_changed_paths,
        Some(vec!["src/extra.rs".to_string()])
    );
    fx.assert_no_worker_invocation();

    fx.shutdown().await;
}
