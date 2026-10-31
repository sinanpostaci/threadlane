#![cfg(unix)]
//! End-to-end behavioral and CLI tests for conversational change routing (`conversation_change`
//! and `threadlane-session preview-change` / `record-change` / `list-changes` / `resolve-change`).

use std::fs::{self, File};
use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Output;
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::Value;
use tempfile::TempDir;
use threadlane::{Supervisor, SupervisorClient, SupervisorConfig, SupervisorHandle};
use threadlane_authority::{ApprovalRecord, AuthorityStore, OperatorApprovalRequest};
use threadlane_host::{
    ApprovedHost, ApprovedHostClient, ApprovedHostConfig, ApprovedHostHandle, TaskExecutionStatus,
};
use threadlane_native::{
    inspect_unresolved_blocking_changes, ConversationCancelAction, ConversationChangeDecisionKind,
    ConversationChangeKind, ConversationChangePage, ConversationChangePreview,
    ConversationChangeRecord, ConversationChangeRequest, ConversationChangeResolutionKind,
    ConversationChangeRoute, ConversationChangeStatus, ConversationChangeStore,
    ConversationDeliveryRequest, LiveInputSupportStatus, ObservedTaskStopState, PlanStore,
    RegistryStore, ResolveConversationChangeRequest, CONVERSATION_CHANGE_NAMESPACE_DIR,
};
use threadlane_workers::{
    resolve_exact_commit_sha, run_sanitized_git, ExecutionBudgets, DEFAULT_GIT_COMMAND_TIMEOUT_MS,
};
use threadlane_workflow::{
    compile_plan_revision, Difficulty, ModelChoice, ModelDefinition, PlanChange, PlanTask,
    RegistryChange, RoleAssignment, ToolAdapter, ToolDefinition, WorkflowProfile, WorkflowRole,
};
use tokio::process::Command;

const THREAD_A: &str = "0196ed00-0000-7000-8000-00000000000a";
const THREAD_B: &str = "0196ed00-0000-7000-8000-00000000000b";
const PROFILE_ID: &str = "profile-change-routing";
const PLAN_ID: &str = "plan-change-routing";

struct ChangeRoutingFixture {
    _tmp: TempDir,
    repo_dir: PathBuf,
    base_commit: String,
    state_dir: PathBuf,
    registry_dir: PathBuf,
    plans_dir: PathBuf,
    authority_dir: PathBuf,
    sessions_dir: PathBuf,
    requests_dir: PathBuf,
    worker_count_file: PathBuf,
    finish_gate_file: PathBuf,
    cancel_exit_gate_file: PathBuf,
    registry: Arc<RegistryStore>,
    plans: Arc<PlanStore>,
    authority: Arc<AuthorityStore>,
    supervisor: Option<SupervisorHandle>,
    binding_a: String,
    binding_b: String,
    registry_revision: u64,
    plan_revision: u64,
    approval_a: ApprovalRecord,
}

impl ChangeRoutingFixture {
    async fn start() -> Self {
        let tmp = TempDir::new().expect("create tempdir");
        let root = fs::canonicalize(tmp.path()).expect("canonicalize tempdir");
        fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).expect("chmod root 0700");

        let repo_dir = root.join("repo");
        let base_commit = init_disposable_repo(&repo_dir);

        let bin_dir = root.join("bin");
        fs::create_dir_all(&bin_dir).expect("create bin_dir");
        fs::set_permissions(&bin_dir, fs::Permissions::from_mode(0o700)).expect("chmod bin_dir");

        let fake_agy_exe = bin_dir.join("fake-agy");
        let worker_count_file = root.join("worker-count.txt");
        let finish_gate_file = root.join("allow-finish.flag");
        let cancel_exit_gate_file = root.join("allow-cancel-exit.flag");
        write_controllable_agy_script(
            &fake_agy_exe,
            &worker_count_file,
            &finish_gate_file,
            &cancel_exit_gate_file,
        );

        let state_dir = create_private_dir(&root, "runtime-state");
        let registry_dir = create_private_dir(&root, "registry-store");
        let plans_dir = create_private_dir(&root, "plans-store");
        let authority_dir = create_private_dir(&root, "authority-store");
        let sessions_dir = create_private_dir(&root, "sessions-store");
        let requests_dir = create_private_dir(&root, "requests");

        let supervisor = Supervisor::start(SupervisorConfig {
            state_dir: state_dir.clone(),
            tick_interval: Duration::from_millis(25),
            codex: None,
            workers: None,
        })
        .await
        .expect("start control-plane supervisor");

        let sup_client =
            SupervisorClient::from_state_dir(&state_dir).expect("connect supervisor client");
        let binding_a = sup_client
            .create_binding(THREAD_A, "Change Routing Chat A")
            .await
            .expect("create binding A")
            .binding_id;
        let binding_b = sup_client
            .create_binding(THREAD_B, "Change Routing Chat B")
            .await
            .expect("create binding B")
            .binding_id;

        let registry = Arc::new(RegistryStore::open(&registry_dir).expect("open registry"));
        let plans = Arc::new(PlanStore::open(&plans_dir).expect("open plans"));

        let s1 = registry
            .apply(
                0,
                RegistryChange::UpsertTool {
                    tool: ToolDefinition {
                        id: "tool-agy".to_string(),
                        label: "AGY Worker".to_string(),
                        adapter: ToolAdapter::Antigravity,
                        executable_path: Some(fake_agy_exe),
                        enabled: true,
                    },
                },
            )
            .expect("upsert tool-agy");
        let s2 = registry
            .apply(
                s1.revision,
                RegistryChange::UpsertModel {
                    model: ModelDefinition {
                        id: "model-agy-argon".to_string(),
                        tool_id: "tool-agy".to_string(),
                        label: "Gemini 4 Argon High".to_string(),
                        model_arg: "gemini-4-argon-high".to_string(),
                        efforts: vec!["high".to_string()],
                        allow_no_effort: false,
                    },
                },
            )
            .expect("upsert model-agy-argon");
        let s3 = registry
            .apply(
                s2.revision,
                RegistryChange::UpsertModel {
                    model: ModelDefinition {
                        id: "model-agy-alt".to_string(),
                        tool_id: "tool-agy".to_string(),
                        label: "Gemini 4 Argon Alt".to_string(),
                        model_arg: "gemini-4-argon-high".to_string(),
                        efforts: vec!["high".to_string()],
                        allow_no_effort: false,
                    },
                },
            )
            .expect("upsert model-agy-alt");
        let reg_snap = registry
            .apply(
                s3.revision,
                RegistryChange::UpsertProfile {
                    profile: WorkflowProfile {
                        id: PROFILE_ID.to_string(),
                        name: "Change Routing Profile".to_string(),
                        assignments: vec![RoleAssignment {
                            role: WorkflowRole::Coder,
                            difficulty: Some(Difficulty::Hard),
                            choice: ModelChoice {
                                model_id: "model-agy-argon".to_string(),
                                effort: Some("high".to_string()),
                            },
                        }],
                    },
                },
            )
            .expect("upsert profile");

        let tasks = initial_plan_tasks("WAIT_FOR_GATE");
        let draft = plans
            .change(
                0,
                PlanChange::SaveDraft {
                    plan_id: PLAN_ID.to_string(),
                    binding_id: binding_a.clone(),
                    profile_id: PROFILE_ID.to_string(),
                    profile_revision: reg_snap.revision,
                    summary: "Initial plan revision 1".to_string(),
                    tasks,
                },
            )
            .expect("save draft rev 1");
        let published = plans
            .change(
                draft.revision,
                PlanChange::Publish {
                    plan_id: PLAN_ID.to_string(),
                    revision: draft.revision,
                },
            )
            .expect("publish rev 1");
        let compiled =
            compile_plan_revision(&published, &reg_snap).expect("compile published rev 1");

        let authority = Arc::new(
            AuthorityStore::open(&authority_dir, Arc::clone(&registry), Arc::clone(&plans))
                .expect("open authority"),
        );
        let approval_summary = authority
            .approve(OperatorApprovalRequest {
                binding_id: binding_a.clone(),
                thread_uuid: THREAD_A.to_string(),
                plan_id: PLAN_ID.to_string(),
                plan_revision: published.revision,
                expected_registry_revision: reg_snap.revision,
                expected_plan_digest: compiled.plan_digest,
                repository_path: repo_dir.clone(),
                base_commit: base_commit.clone(),
                approved_task_ids: vec![
                    "task-1".to_string(),
                    "task-2".to_string(),
                    "task-3".to_string(),
                ],
                approved_write_paths: vec!["src/lib.rs".to_string(), "src/dep.rs".to_string()],
                max_parallel: 2,
                budgets: ExecutionBudgets::default(),
            })
            .expect("approve initial plan");
        let approval_a = authority
            .get_for_operator(&approval_summary.approval_id)
            .expect("load approval_a record");

        Self {
            _tmp: tmp,
            repo_dir,
            base_commit,
            state_dir,
            registry_dir,
            plans_dir,
            authority_dir,
            sessions_dir,
            requests_dir,
            worker_count_file,
            finish_gate_file,
            cancel_exit_gate_file,
            registry,
            plans,
            authority,
            supervisor: Some(supervisor),
            binding_a,
            binding_b,
            registry_revision: reg_snap.revision,
            plan_revision: published.revision,
            approval_a,
        }
    }

    async fn shutdown(&mut self) {
        if let Some(sup) = self.supervisor.take() {
            sup.shutdown().await;
        }
    }

    fn host_client_a(&self) -> ApprovedHostClient {
        ApprovedHostClient::new(
            Arc::clone(&self.authority),
            &self.sessions_dir,
            self.binding_a.clone(),
            THREAD_A.to_string(),
        )
        .expect("create ApprovedHostClient for A")
    }

    fn change_store(&self) -> ConversationChangeStore {
        ConversationChangeStore::open(&self.plans_dir).expect("open ConversationChangeStore")
    }

    async fn start_host_for_approval(&self, approval_id: &str) -> ApprovedHostHandle {
        ApprovedHost::start(ApprovedHostConfig {
            registry_dir: self.registry_dir.clone(),
            plans_dir: self.plans_dir.clone(),
            authority_dir: self.authority_dir.clone(),
            sessions_dir: self.sessions_dir.clone(),
            approval_id: approval_id.to_string(),
            tick_interval: Duration::from_millis(25),
            notifications: None,
        })
        .await
        .expect("start ApprovedHost")
    }

    fn publish_and_approve_revision_2(&self, extra_write_path: &str) -> ApprovalRecord {
        let mut tasks = initial_plan_tasks("FAST_SUCCESS");
        tasks[0]
            .allowed_write_paths
            .push(extra_write_path.to_string());

        let draft2 = self
            .plans
            .change(
                self.plan_revision,
                PlanChange::SaveDraft {
                    plan_id: PLAN_ID.to_string(),
                    binding_id: self.binding_a.clone(),
                    profile_id: PROFILE_ID.to_string(),
                    profile_revision: self.registry_revision,
                    summary: "Revised plan revision 2 after conversational scope change"
                        .to_string(),
                    tasks,
                },
            )
            .expect("save draft rev 2");
        let published2 = self
            .plans
            .change(
                draft2.revision,
                PlanChange::Publish {
                    plan_id: PLAN_ID.to_string(),
                    revision: draft2.revision,
                },
            )
            .expect("publish rev 2");
        let reg_snap = self.registry.snapshot().expect("registry snapshot");
        let compiled2 =
            compile_plan_revision(&published2, &reg_snap).expect("compile published rev 2");

        let summary2 = self
            .authority
            .approve(OperatorApprovalRequest {
                binding_id: self.binding_a.clone(),
                thread_uuid: THREAD_A.to_string(),
                plan_id: PLAN_ID.to_string(),
                plan_revision: published2.revision,
                expected_registry_revision: reg_snap.revision,
                expected_plan_digest: compiled2.plan_digest,
                repository_path: self.repo_dir.clone(),
                base_commit: self.base_commit.clone(),
                approved_task_ids: vec![
                    "task-1".to_string(),
                    "task-2".to_string(),
                    "task-3".to_string(),
                ],
                approved_write_paths: vec![
                    "src/lib.rs".to_string(),
                    "src/dep.rs".to_string(),
                    extra_write_path.to_string(),
                ],
                max_parallel: 2,
                budgets: ExecutionBudgets::default(),
            })
            .expect("approve plan rev 2");
        self.authority
            .get_for_operator(&summary2.approval_id)
            .expect("load approval rev 2 record")
    }

    fn write_request_file<T: Serialize>(&self, name: &str, value: &T) -> PathBuf {
        let path = self.requests_dir.join(name);
        let bytes = serde_json::to_vec_pretty(value).expect("serialize request");
        let mut f = File::create(&path).expect("create request file");
        f.write_all(&bytes).expect("write request file");
        f.sync_all().expect("sync request file");
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600))
            .expect("chmod request file 0600");
        path
    }

    fn base_cli_args(&self, binding: &str, thread: &str) -> Vec<String> {
        vec![
            "--state-dir".to_string(),
            self.state_dir.display().to_string(),
            "--registry-dir".to_string(),
            self.registry_dir.display().to_string(),
            "--plans-dir".to_string(),
            self.plans_dir.display().to_string(),
            "--authority-dir".to_string(),
            self.authority_dir.display().to_string(),
            "--sessions-dir".to_string(),
            self.sessions_dir.display().to_string(),
            "--binding".to_string(),
            binding.to_string(),
            "--thread".to_string(),
            thread.to_string(),
        ]
    }

    async fn run_session_cli(&self, binding: &str, thread: &str, sub_args: &[&str]) -> Output {
        let mut args = self.base_cli_args(binding, thread);
        args.extend(sub_args.iter().map(|s| (*s).to_string()));
        Command::new(env!("CARGO_BIN_EXE_threadlane-session"))
            .args(&args)
            .output()
            .await
            .expect("execute threadlane-session")
    }

    fn worker_invocation_count(&self) -> u64 {
        fs::read_to_string(&self.worker_count_file)
            .ok()
            .and_then(|s| s.trim().parse::<u64>().ok())
            .unwrap_or(0)
    }
}

fn create_private_dir(root: &Path, name: &str) -> PathBuf {
    let path = root.join(name);
    fs::create_dir_all(&path).expect("create dir");
    fs::set_permissions(&path, fs::Permissions::from_mode(0o700)).expect("chmod 0700");
    path
}

fn init_disposable_repo(repo_dir: &Path) -> String {
    fs::create_dir_all(repo_dir.join("src")).expect("create repo src");
    fs::write(repo_dir.join("src/lib.rs"), "pub fn base() -> u32 { 1 }\n")
        .expect("write src/lib.rs");
    fs::write(repo_dir.join("src/dep.rs"), "pub fn dep() -> u32 { 2 }\n")
        .expect("write src/dep.rs");
    fs::write(
        repo_dir.join("src/extra.rs"),
        "pub fn extra() -> u32 { 3 }\n",
    )
    .expect("write src/extra.rs");

    run_sanitized_git(
        repo_dir,
        &["init", "--quiet"],
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .expect("git init");
    run_sanitized_git(
        repo_dir,
        &["config", "user.email", "test@threadlane.local"],
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .expect("git config email");
    run_sanitized_git(
        repo_dir,
        &["config", "user.name", "Threadlane Test"],
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .expect("git config name");
    run_sanitized_git(repo_dir, &["add", "."], DEFAULT_GIT_COMMAND_TIMEOUT_MS).expect("git add");
    run_sanitized_git(
        repo_dir,
        &["commit", "-m", "initial commit", "--quiet"],
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .expect("git commit");
    resolve_exact_commit_sha(repo_dir, "HEAD", DEFAULT_GIT_COMMAND_TIMEOUT_MS)
        .expect("resolve HEAD")
}

fn initial_plan_tasks(mode_marker: &str) -> Vec<PlanTask> {
    vec![
        PlanTask {
            id: "task-1".to_string(),
            title: "Primary implementation task".to_string(),
            instruction: format!("Implement primary module ({mode_marker})"),
            role: WorkflowRole::Coder,
            difficulty: Difficulty::Hard,
            depends_on: Vec::new(),
            allowed_write_paths: vec!["src/lib.rs".to_string()],
        },
        PlanTask {
            id: "task-2".to_string(),
            title: "Downstream dependent task".to_string(),
            instruction: "Build on task-1 output".to_string(),
            role: WorkflowRole::Coder,
            difficulty: Difficulty::Hard,
            depends_on: vec!["task-1".to_string()],
            allowed_write_paths: vec!["src/dep.rs".to_string()],
        },
        PlanTask {
            id: "task-3".to_string(),
            title: "Independent sibling task".to_string(),
            instruction: "Independent check".to_string(),
            role: WorkflowRole::Coder,
            difficulty: Difficulty::Hard,
            depends_on: Vec::new(),
            allowed_write_paths: vec!["src/dep.rs".to_string()],
        },
    ]
}

fn write_controllable_agy_script(
    script_path: &Path,
    count_file: &Path,
    finish_gate_file: &Path,
    cancel_exit_gate_file: &Path,
) {
    let script = format!(
        r#"#!/bin/sh
set -eu
COUNT_FILE="{count_file}"
FINISH_GATE="{finish_gate}"
CANCEL_GATE="{cancel_gate}"

c=0
if [ -f "$COUNT_FILE" ]; then
    c=$(cat "$COUNT_FILE")
fi
c=$((c + 1))
printf "%s" "$c" > "$COUNT_FILE"

MODEL=""
EFFORT=""
PROMPT=""
while [ "$#" -gt 0 ]; do
    case "$1" in
        --model)
            MODEL="$2"
            shift 2
            ;;
        --effort)
            EFFORT="$2"
            shift 2
            ;;
        --print)
            PROMPT="$2"
            shift 2
            ;;
        *)
            shift 1
            ;;
    esac
done

printf '{{"type":"init","session_id":"sess-agy-chg","model":"%s","effort":"%s"}}\n' "$MODEL" "$EFFORT"

on_term() {{
    while [ ! -f "$CANCEL_GATE" ]; do
        sleep 0.02
    done
    exit 0
}}
trap 'on_term' TERM INT

case "$PROMPT" in
    *WAIT_FOR_GATE*)
        while [ ! -f "$FINISH_GATE" ]; do
            sleep 0.02
        done
        ;;
esac

if [ -d "src" ]; then
    printf "pub fn base() -> u32 {{ 42 }}\n" > "src/lib.rs"
fi

printf '{{"type":"result","subtype":"success","session_id":"sess-agy-chg","model":"%s","effort":"%s"}}\n' "$MODEL" "$EFFORT"
exit 0
"#,
        count_file = count_file.display(),
        finish_gate = finish_gate_file.display(),
        cancel_gate = cancel_exit_gate_file.display(),
    );

    let mut f = File::create(script_path).expect("create fake-agy");
    f.write_all(script.as_bytes()).expect("write fake-agy");
    f.sync_all().expect("sync fake-agy");
    fs::set_permissions(script_path, fs::Permissions::from_mode(0o755))
        .expect("chmod 0755 fake-agy");
}

fn parse_cli_data<T: DeserializeOwned>(output: &Output, expected_binding: &str) -> T {
    assert!(
        output.status.success(),
        "expected CLI success, stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        output.stderr.is_empty(),
        "successful CLI run must have empty stderr"
    );
    let envelope: Value =
        serde_json::from_slice(&output.stdout).expect("parse operator stdout JSON envelope");
    assert_eq!(envelope["untrusted_data"], Value::Bool(true));
    assert_eq!(
        envelope["route"],
        Value::String("trusted_local_operator".to_string())
    );
    assert_eq!(
        envelope["native_per_call_identity_verified"],
        Value::Bool(false)
    );
    assert_eq!(
        envelope["binding_id"],
        Value::String(expected_binding.to_string())
    );
    serde_json::from_value(envelope["data"].clone()).expect("deserialize envelope data")
}

fn parse_cli_error(output: &Output) -> (String, String) {
    assert!(
        !output.status.success(),
        "expected CLI failure, stdout: {}",
        String::from_utf8_lossy(&output.stdout)
    );
    assert!(
        output.stdout.is_empty(),
        "failed CLI run must have empty stdout"
    );
    let err: Value = serde_json::from_slice(&output.stderr).expect("parse stderr JSON error");
    (
        err["code"].as_str().expect("error code").to_string(),
        err["message"].as_str().expect("error message").to_string(),
    )
}

async fn wait_for_task_status(
    client: &ApprovedHostClient,
    approval_id: &str,
    task_id: &str,
    expected: TaskExecutionStatus,
) -> threadlane_host::TaskStateSnapshot {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let snap = client
            .get_task(approval_id, task_id)
            .await
            .expect("get_task");
        if snap.status == expected {
            return snap;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for task {task_id} to reach {expected:?}, current: {:?}",
            snap.status
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

fn sorted_dir_entries(dir: &Path) -> Vec<String> {
    let Ok(read_dir) = fs::read_dir(dir) else {
        return Vec::new();
    };
    let mut entries: Vec<String> = read_dir
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .collect();
    entries.sort();
    entries
}

#[tokio::test]
async fn questions_during_active_worker_execution_never_revise_cancel_dispatch_or_block() {
    let mut fx = ChangeRoutingFixture::start().await;
    let namespace_dir = fx.plans_dir.join(CONVERSATION_CHANGE_NAMESPACE_DIR);
    let before_entries = sorted_dir_entries(&fx.plans_dir);
    assert!(
        !namespace_dir.exists(),
        "conversation-changes namespace must be absent before any mutating operation"
    );

    // Before any change is recorded, read-only helper succeeds without creating namespace files.
    let initial_block = inspect_unresolved_blocking_changes(
        &fx.plans_dir,
        &fx.binding_a,
        Some(&fx.approval_a.approval_id),
        Some("task-1"),
    )
    .expect("inspect initial blocking changes");
    assert!(!initial_block.blocked);
    assert!(!initial_block.approval_superseded);
    assert!(initial_block.superseded_by_changes.is_empty());
    assert_eq!(initial_block.current_change_revision, 0);
    assert!(!namespace_dir.exists());
    assert_eq!(sorted_dir_entries(&fx.plans_dir), before_entries);

    let host = fx.start_host_for_approval(&fx.approval_a.approval_id).await;
    let client_a = fx.host_client_a();

    client_a
        .dispatch_task(&fx.approval_a.approval_id, "task-1")
        .await
        .expect("dispatch task-1");
    let running_snap = wait_for_task_status(
        &client_a,
        &fx.approval_a.approval_id,
        "task-1",
        TaskExecutionStatus::Running,
    )
    .await;
    assert_eq!(fx.worker_invocation_count(), 1);

    let question_req = ConversationChangeRequest {
        request_key: "req-question-1".to_string(),
        expected_change_revision: 0,
        approval_id: Some(fx.approval_a.approval_id.clone()),
        plan_id: Some(PLAN_ID.to_string()),
        expected_plan_revision: Some(1),
        task_id: Some("task-1".to_string()),
        expected_task_revision: running_snap.worker.as_ref().map(|w| w.revision),
        kind: ConversationChangeKind::QuestionOrStatus,
        route: None,
        summary: "Operator asked how much of task-1 is complete.".to_string(),
        detail: Some("Pure status inquiry during active worker execution.".to_string()),
        proposed_write_paths: Vec::new(),
        proposed_profile_id: None,
        proposed_model_id: None,
        proposed_effort: None,
        requested_delivery: ConversationDeliveryRequest::SafeCheckpointOrReplan,
        cancel_action: None,
    };
    // An approval-wide model proposal has no observed task stop proof. In particular,
    // omitting task_id while a real worker runs must not fabricate a stopped state.
    let mut approval_wide = question_req.clone();
    approval_wide.request_key = "approval-wide-model-preview".to_string();
    approval_wide.task_id = None;
    approval_wide.expected_task_revision = None;
    approval_wide.kind = ConversationChangeKind::ModelOrProfileChange;
    approval_wide.proposed_model_id = Some("opencode-go/deepseek-v4.1-flash".to_string());
    let wide_path = fx.write_request_file("approval-wide-model.json", &approval_wide);
    let wide_out = fx
        .run_session_cli(
            &fx.binding_a,
            THREAD_A,
            &["preview-change", "--request", wide_path.to_str().unwrap()],
        )
        .await;
    let wide: ConversationChangePreview = parse_cli_data(&wide_out, &fx.binding_a);
    assert!(!wide.decision.prior_task_proven_stopped);
    assert!(wide.decision.blocks_task_advancement);
    assert!(wide.decision.new_approval_required);
    assert!(!wide.decision.live_input_delivered);
    assert!(!wide.decision.process_paused);
    assert!(!namespace_dir.exists(), "preview remains read-only");
    let req_path = fx.write_request_file("question-1.json", &question_req);

    // 1. Preview via `threadlane-session preview-change` (must remain read-only and not create namespace/lock).
    let preview_out = fx
        .run_session_cli(
            &fx.binding_a,
            THREAD_A,
            &["preview-change", "--request", req_path.to_str().unwrap()],
        )
        .await;
    let preview: ConversationChangePreview = parse_cli_data(&preview_out, &fx.binding_a);
    assert!(
        !namespace_dir.exists(),
        "CLI preview-change must not create conversation-changes directory"
    );
    assert_eq!(sorted_dir_entries(&fx.plans_dir), before_entries);
    assert_eq!(preview.current_change_revision, 0);
    assert_eq!(preview.next_change_revision, 1);
    assert!(!preview.idempotent_replay);
    assert!(!preview.will_invoke_cancel_task);
    assert_eq!(
        preview.decision.route,
        ConversationChangeRoute::ReadOnlyObservation
    );
    assert_eq!(
        preview.decision.decision_kind,
        ConversationChangeDecisionKind::RecordOnly
    );
    assert_eq!(
        preview.decision.status,
        ConversationChangeStatus::RecordedOnly
    );
    assert_eq!(
        preview.decision.live_input_status,
        LiveInputSupportStatus::NotApplicableReadOnly
    );
    assert!(!preview.decision.blocks_task_advancement);
    assert!(!preview.decision.replan_required);
    assert!(!preview.decision.new_approval_required);
    assert!(!preview.decision.live_input_delivered);
    assert!(!preview.decision.process_paused);
    assert!(!preview.decision.provider_resumed);
    assert!(preview.decision.frozen_approval_immutable);

    // 2. Record via `threadlane-session record-change`.
    let record_out = fx
        .run_session_cli(
            &fx.binding_a,
            THREAD_A,
            &["record-change", "--request", req_path.to_str().unwrap()],
        )
        .await;
    let recorded: ConversationChangeRecord = parse_cli_data(&record_out, &fx.binding_a);
    assert_eq!(recorded.created_change_revision, 1);
    assert_eq!(
        recorded.decision.status,
        ConversationChangeStatus::RecordedOnly
    );
    assert!(!recorded.decision.blocks_task_advancement);

    // Worker must still be running, no extra worker invocation occurred, plan rev is still 1,
    // and `inspect_unresolved_blocking_changes` is not blocked and does not mark approval superseded.
    let live_snap = client_a
        .get_task(&fx.approval_a.approval_id, "task-1")
        .await
        .expect("get task-1 after question");
    assert_eq!(live_snap.status, TaskExecutionStatus::Running);
    assert_eq!(fx.worker_invocation_count(), 1);
    let latest_plan = fx
        .plans
        .latest_revision(PLAN_ID)
        .expect("get latest plan")
        .expect("plan exists");
    assert_eq!(latest_plan.revision, 1);

    let post_question_block = inspect_unresolved_blocking_changes(
        &fx.plans_dir,
        &fx.binding_a,
        Some(&fx.approval_a.approval_id),
        Some("task-1"),
    )
    .expect("inspect blocking after question");
    assert!(!post_question_block.blocked);
    assert!(!post_question_block.approval_superseded);
    assert_eq!(post_question_block.current_change_revision, 1);
    assert!(post_question_block.blocking_changes.is_empty());
    assert!(post_question_block.superseded_by_changes.is_empty());

    // 3. Questions attempting to cancel a task or propose write-scope changes must be rejected with `route_mismatch`.
    let mut bad_question = question_req.clone();
    bad_question.request_key = "req-question-bad-cancel".to_string();
    bad_question.expected_change_revision = 1;
    bad_question.proposed_write_paths = vec!["src/extra.rs".to_string()];
    let bad_path = fx.write_request_file("question-bad.json", &bad_question);
    let bad_out = fx
        .run_session_cli(
            &fx.binding_a,
            THREAD_A,
            &["record-change", "--request", bad_path.to_str().unwrap()],
        )
        .await;
    let (err_code, _) = parse_cli_error(&bad_out);
    assert_eq!(err_code, "route_mismatch");

    // Cleanly finish the worker and shut down.
    fs::write(&fx.finish_gate_file, b"ok").expect("release finish gate");
    wait_for_task_status(
        &client_a,
        &fx.approval_a.approval_id,
        "task-1",
        TaskExecutionStatus::ReadyForReview,
    )
    .await;
    host.shutdown().await.expect("shutdown host");
    fx.shutdown().await;
}

#[tokio::test]
async fn pending_clarification_blocks_task_and_dependents_rejects_smuggling_and_resolves_at_checkpoint(
) {
    let mut fx = ChangeRoutingFixture::start().await;
    let host = fx.start_host_for_approval(&fx.approval_a.approval_id).await;
    let client_a = fx.host_client_a();

    client_a
        .dispatch_task(&fx.approval_a.approval_id, "task-1")
        .await
        .expect("dispatch task-1");
    let running_snap = wait_for_task_status(
        &client_a,
        &fx.approval_a.approval_id,
        "task-1",
        TaskExecutionStatus::Running,
    )
    .await;
    let running_rev = running_snap.worker.as_ref().map(|w| w.revision);

    // 1. Smuggling an unapproved write path or model change onto `clarification` is rejected with `route_mismatch`.
    let smuggled_scope = ConversationChangeRequest {
        request_key: "req-clar-smuggle-scope".to_string(),
        expected_change_revision: 0,
        approval_id: Some(fx.approval_a.approval_id.clone()),
        plan_id: Some(PLAN_ID.to_string()),
        expected_plan_revision: Some(1),
        task_id: Some("task-1".to_string()),
        expected_task_revision: running_rev,
        kind: ConversationChangeKind::Clarification,
        route: None,
        summary: "Try to add src/extra.rs on clarification route".to_string(),
        detail: None,
        proposed_write_paths: vec!["src/extra.rs".to_string()],
        proposed_profile_id: None,
        proposed_model_id: None,
        proposed_effort: None,
        requested_delivery: ConversationDeliveryRequest::SafeCheckpointOrReplan,
        cancel_action: None,
    };
    let smuggle_scope_path = fx.write_request_file("smuggle-scope.json", &smuggled_scope);
    let smuggle_scope_out = fx
        .run_session_cli(
            &fx.binding_a,
            THREAD_A,
            &[
                "record-change",
                "--request",
                smuggle_scope_path.to_str().unwrap(),
            ],
        )
        .await;
    assert_eq!(parse_cli_error(&smuggle_scope_out).0, "route_mismatch");

    let mut smuggled_model = smuggled_scope.clone();
    smuggled_model.request_key = "req-clar-smuggle-model".to_string();
    smuggled_model.proposed_write_paths.clear();
    smuggled_model.proposed_model_id = Some("model-agy-alt".to_string());
    let smuggle_model_path = fx.write_request_file("smuggle-model.json", &smuggled_model);
    let smuggle_model_out = fx
        .run_session_cli(
            &fx.binding_a,
            THREAD_A,
            &[
                "record-change",
                "--request",
                smuggle_model_path.to_str().unwrap(),
            ],
        )
        .await;
    assert_eq!(parse_cli_error(&smuggle_model_out).0, "route_mismatch");

    // 2. Valid clarification within existing scope (`src/lib.rs`) records `pending_clarification_at_checkpoint`.
    let clar_req = ConversationChangeRequest {
        request_key: "req-clar-valid-1".to_string(),
        expected_change_revision: 0,
        approval_id: Some(fx.approval_a.approval_id.clone()),
        plan_id: Some(PLAN_ID.to_string()),
        expected_plan_revision: Some(1),
        task_id: Some("task-1".to_string()),
        expected_task_revision: running_rev,
        kind: ConversationChangeKind::Clarification,
        route: Some(ConversationChangeRoute::CheckpointClarification),
        summary: "Clarify error formatting in src/lib.rs at review checkpoint.".to_string(),
        detail: Some("Keep existing scope and model pin; apply at next checkpoint.".to_string()),
        proposed_write_paths: vec!["src/lib.rs".to_string()],
        proposed_profile_id: None,
        proposed_model_id: None,
        proposed_effort: None,
        requested_delivery: ConversationDeliveryRequest::LiveContinuationIfSupported,
        cancel_action: None,
    };
    let clar_path = fx.write_request_file("clar-valid.json", &clar_req);
    let clar_out = fx
        .run_session_cli(
            &fx.binding_a,
            THREAD_A,
            &["record-change", "--request", clar_path.to_str().unwrap()],
        )
        .await;
    let clar_rec: ConversationChangeRecord = parse_cli_data(&clar_out, &fx.binding_a);
    assert_eq!(
        clar_rec.decision.route,
        ConversationChangeRoute::CheckpointClarification
    );
    assert_eq!(
        clar_rec.decision.decision_kind,
        ConversationChangeDecisionKind::PendingClarificationAtCheckpoint
    );
    assert_eq!(
        clar_rec.decision.status,
        ConversationChangeStatus::PendingClarification
    );
    assert_eq!(
        clar_rec.decision.live_input_status,
        LiveInputSupportStatus::UnsupportedNoLiveInputPath
    );
    assert_eq!(
        clar_rec.decision.observed_stop_state,
        Some(ObservedTaskStopState::Running)
    );
    assert!(!clar_rec.decision.prior_task_proven_stopped);
    assert!(!clar_rec.decision.live_input_delivered);
    assert!(clar_rec.decision.blocks_task_advancement);
    assert_eq!(
        clar_rec.affected_dependent_task_ids,
        vec!["task-2".to_string()]
    );

    // 3. Blocking helper blocks `task-1` and downstream dependent `task-2`, but NOT independent `task-3`.
    let block_t1 = inspect_unresolved_blocking_changes(
        &fx.plans_dir,
        &fx.binding_a,
        Some(&fx.approval_a.approval_id),
        Some("task-1"),
    )
    .expect("block summary task-1");
    assert!(block_t1.blocked);
    assert!(!block_t1.replan_required);
    assert!(!block_t1.new_approval_required);
    assert_eq!(block_t1.blocking_changes.len(), 1);

    let block_t2 = inspect_unresolved_blocking_changes(
        &fx.plans_dir,
        &fx.binding_a,
        Some(&fx.approval_a.approval_id),
        Some("task-2"),
    )
    .expect("block summary dependent task-2");
    assert!(
        block_t2.blocked,
        "downstream dependent task-2 must also be blocked"
    );

    let block_t3 = inspect_unresolved_blocking_changes(
        &fx.plans_dir,
        &fx.binding_a,
        Some(&fx.approval_a.approval_id),
        Some("task-3"),
    )
    .expect("block summary independent task-3");
    assert!(
        !block_t3.blocked,
        "independent sibling task-3 must not be blocked"
    );

    // 4. Attempting to resolve with `checkpoint_handled` while `task-1` is still `Running` fails with `task_not_proven_stopped`.
    let resolve_req = ResolveConversationChangeRequest {
        request_key: "res-clar-1".to_string(),
        change_id: clar_rec.change_id.clone(),
        expected_change_revision: 1,
        resolution_kind: ConversationChangeResolutionKind::CheckpointHandled,
        target_plan_revision: Some(1),
        new_approval_id: None,
        resolution_note: Some("Reviewed clarification at ReadyForReview checkpoint.".to_string()),
    };
    let resolve_path = fx.write_request_file("resolve-clar.json", &resolve_req);
    let early_resolve_out = fx
        .run_session_cli(
            &fx.binding_a,
            THREAD_A,
            &[
                "resolve-change",
                "--request",
                resolve_path.to_str().unwrap(),
            ],
        )
        .await;
    assert_eq!(
        parse_cli_error(&early_resolve_out).0,
        "task_not_proven_stopped"
    );

    // 5. Let `task-1` reach its real `ReadyForReview` checkpoint, then resolve.
    fs::write(&fx.finish_gate_file, b"ok").expect("allow worker finish");
    wait_for_task_status(
        &client_a,
        &fx.approval_a.approval_id,
        "task-1",
        TaskExecutionStatus::ReadyForReview,
    )
    .await;

    let resolved_out = fx
        .run_session_cli(
            &fx.binding_a,
            THREAD_A,
            &[
                "resolve-change",
                "--request",
                resolve_path.to_str().unwrap(),
            ],
        )
        .await;
    let resolved_rec: ConversationChangeRecord = parse_cli_data(&resolved_out, &fx.binding_a);
    assert_eq!(resolved_rec.updated_change_revision, 2);
    assert_eq!(
        resolved_rec.decision.status,
        ConversationChangeStatus::Resolved
    );
    assert!(!resolved_rec.decision.blocks_task_advancement);
    let res_meta = resolved_rec.resolution.expect("resolution metadata");
    assert_eq!(
        res_meta.verified_stop_state,
        Some(ObservedTaskStopState::ProvenStopped)
    );

    let post_resolve_block = inspect_unresolved_blocking_changes(
        &fx.plans_dir,
        &fx.binding_a,
        Some(&fx.approval_a.approval_id),
        Some("task-1"),
    )
    .expect("block summary after resolution");
    assert!(!post_resolve_block.blocked);
    assert!(!post_resolve_block.approval_superseded);
    assert!(post_resolve_block.blocking_changes.is_empty());
    assert!(post_resolve_block.superseded_by_changes.is_empty());

    host.shutdown().await.expect("shutdown host");
    fx.shutdown().await;
}

#[tokio::test]
async fn scope_change_distinguishes_cancel_requested_from_cancelled_and_requires_new_plan_and_approval(
) {
    let mut fx = ChangeRoutingFixture::start().await;
    let host = fx.start_host_for_approval(&fx.approval_a.approval_id).await;
    let client_a = fx.host_client_a();

    client_a
        .dispatch_task(&fx.approval_a.approval_id, "task-1")
        .await
        .expect("dispatch task-1");
    let running_snap = wait_for_task_status(
        &client_a,
        &fx.approval_a.approval_id,
        "task-1",
        TaskExecutionStatus::Running,
    )
    .await;
    let running_rev = running_snap
        .worker
        .as_ref()
        .map(|w| w.revision)
        .expect("worker revision");

    // 1. Record a scope change with typed `cancel_action` while the worker traps SIGTERM (`cancel_exit_gate_file` not yet created).
    let scope_req = ConversationChangeRequest {
        request_key: "req-scope-cancel-1".to_string(),
        expected_change_revision: 0,
        approval_id: Some(fx.approval_a.approval_id.clone()),
        plan_id: Some(PLAN_ID.to_string()),
        expected_plan_revision: Some(1),
        task_id: Some("task-1".to_string()),
        expected_task_revision: Some(running_rev),
        kind: ConversationChangeKind::ScopeChange,
        route: None,
        summary: "Expand task-1 write scope to include src/extra.rs.".to_string(),
        detail: Some(
            "Cancel task-1, publish plan revision 2, and obtain fresh approval.".to_string(),
        ),
        proposed_write_paths: vec!["src/lib.rs".to_string(), "src/extra.rs".to_string()],
        proposed_profile_id: None,
        proposed_model_id: None,
        proposed_effort: None,
        requested_delivery: ConversationDeliveryRequest::SafeCheckpointOrReplan,
        cancel_action: Some(ConversationCancelAction {
            expected_task_revision: running_rev,
            confirm_approval_digest: fx.approval_a.subject_digest.clone(),
            operator_approved: true,
            reason: Some("Scope changed; stop task-1 before replanning.".to_string()),
        }),
    };
    let scope_path = fx.write_request_file("scope-cancel.json", &scope_req);
    let record_out = fx
        .run_session_cli(
            &fx.binding_a,
            THREAD_A,
            &[
                "record-change",
                "--request",
                scope_path.to_str().unwrap(),
                "--confirm-approval",
                &fx.approval_a.subject_digest,
                "--operator-approved",
            ],
        )
        .await;
    let recorded: ConversationChangeRecord = parse_cli_data(&record_out, &fx.binding_a);
    assert_eq!(recorded.created_change_revision, 1);
    assert_eq!(
        recorded.decision.route,
        ConversationChangeRoute::ScopeOrModelReplan
    );
    assert_eq!(
        recorded.decision.decision_kind,
        ConversationChangeDecisionKind::CancelReplanNewApprovalRequired
    );
    assert_eq!(
        recorded.decision.status,
        ConversationChangeStatus::PendingReplan
    );
    assert!(recorded.decision.cancel_requested);
    assert!(recorded.decision.replan_required);
    assert!(recorded.decision.new_approval_required);
    assert!(recorded.decision.blocks_task_advancement);
    assert_eq!(
        recorded.decision.observed_task_status,
        Some(TaskExecutionStatus::CancelRequested)
    );
    assert_eq!(
        recorded.decision.observed_stop_state,
        Some(ObservedTaskStopState::CancelRequested)
    );
    assert!(
        !recorded.decision.prior_task_proven_stopped,
        "cancel_requested must report prior_task_proven_stopped = false"
    );

    // 2. Immediately while `task-1` is still in `CancelRequested` (not yet exited), `resolve_change`
    // MUST refuse with `task_not_proven_stopped`!
    let store = fx.change_store();
    let premature_resolve_err = store
        .resolve_change(
            &fx.binding_a,
            THREAD_A,
            &ResolveConversationChangeRequest {
                request_key: "res-scope-premature".to_string(),
                change_id: recorded.change_id.clone(),
                expected_change_revision: 1,
                resolution_kind: ConversationChangeResolutionKind::LinkedPlanRevision,
                target_plan_revision: Some(2),
                new_approval_id: None,
                resolution_note: Some("Attempted while cancel_requested.".to_string()),
            },
            &fx.plans,
            &fx.authority,
            &client_a,
        )
        .await
        .expect_err("resolve_change must fail while task is cancel_requested");
    assert_eq!(
        premature_resolve_err.code(),
        "task_not_proven_stopped",
        "must not allow resolution while prior task is still cancel_requested"
    );

    // Frozen approval record in AuthorityStore remains completely immutable.
    let frozen_after = fx
        .authority
        .get_for_operator(&fx.approval_a.approval_id)
        .expect("reload frozen approval");
    assert_eq!(frozen_after, fx.approval_a);

    // 3. Now release the trapped worker so it exits and reaches `Cancelled` (`ProvenStopped`),
    // then publish plan revision 2 and record fresh approval `approval_rev2`.
    fs::write(&fx.cancel_exit_gate_file, b"ok").expect("release cancel exit gate");
    wait_for_task_status(
        &client_a,
        &fx.approval_a.approval_id,
        "task-1",
        TaskExecutionStatus::Cancelled,
    )
    .await;

    let approval_rev2 = fx.publish_and_approve_revision_2("src/extra.rs");
    assert_eq!(approval_rev2.plan_revision, 2);

    let resolve_req = ResolveConversationChangeRequest {
        request_key: "res-scope-1".to_string(),
        change_id: recorded.change_id.clone(),
        expected_change_revision: 1,
        resolution_kind: ConversationChangeResolutionKind::LinkedPlanRevision,
        target_plan_revision: Some(2),
        new_approval_id: Some(approval_rev2.approval_id.clone()),
        resolution_note: Some("Plan rev 2 published and approved.".to_string()),
    };
    let resolve_path = fx.write_request_file("resolve-scope.json", &resolve_req);

    // 4. Attempting to resolve a `PendingReplan` change with `checkpoint_handled` or without `new_approval_id` is rejected.
    let bad_checkpoint_res = ResolveConversationChangeRequest {
        request_key: "res-scope-bad-kind".to_string(),
        change_id: recorded.change_id.clone(),
        expected_change_revision: 1,
        resolution_kind: ConversationChangeResolutionKind::CheckpointHandled,
        target_plan_revision: Some(1),
        new_approval_id: None,
        resolution_note: None,
    };
    let bad_kind_path = fx.write_request_file("resolve-bad-kind.json", &bad_checkpoint_res);
    let bad_kind_out = fx
        .run_session_cli(
            &fx.binding_a,
            THREAD_A,
            &[
                "resolve-change",
                "--request",
                bad_kind_path.to_str().unwrap(),
            ],
        )
        .await;
    assert_eq!(parse_cli_error(&bad_kind_out).0, "replan_required");

    let missing_approval_res = ResolveConversationChangeRequest {
        request_key: "res-scope-no-approval".to_string(),
        change_id: recorded.change_id.clone(),
        expected_change_revision: 1,
        resolution_kind: ConversationChangeResolutionKind::LinkedPlanRevision,
        target_plan_revision: Some(2),
        new_approval_id: None,
        resolution_note: None,
    };
    let missing_apr_path = fx.write_request_file("resolve-no-apr.json", &missing_approval_res);
    let missing_apr_out = fx
        .run_session_cli(
            &fx.binding_a,
            THREAD_A,
            &[
                "resolve-change",
                "--request",
                missing_apr_path.to_str().unwrap(),
            ],
        )
        .await;
    assert_eq!(parse_cli_error(&missing_apr_out).0, "new_approval_required");

    // 5. Resolve with `linked_plan_revision`, `target_plan_revision: 2`, and `new_approval_id` now succeeds!
    let resolved_out = fx
        .run_session_cli(
            &fx.binding_a,
            THREAD_A,
            &[
                "resolve-change",
                "--request",
                resolve_path.to_str().unwrap(),
            ],
        )
        .await;
    let resolved_rec: ConversationChangeRecord = parse_cli_data(&resolved_out, &fx.binding_a);
    assert_eq!(resolved_rec.updated_change_revision, 2);
    assert_eq!(
        resolved_rec.decision.status,
        ConversationChangeStatus::Resolved
    );
    assert!(resolved_rec.decision.prior_task_proven_stopped);
    let res = resolved_rec.resolution.expect("resolution present");
    assert_eq!(res.target_plan_revision, Some(2));
    assert_eq!(
        res.new_approval_id.as_deref(),
        Some(approval_rev2.approval_id.as_str())
    );
    assert_eq!(
        res.verified_stop_state,
        Some(ObservedTaskStopState::ProvenStopped)
    );

    // 6. Verify that after resolving a replan to a newer plan revision + fresh approval:
    //    - The OLD frozen approval (`fx.approval_a.approval_id`, including `task-1`, dependent `task-2`,
    //      and independent `task-3`) stays durably superseded (`blocked: true`, `approval_superseded: true`,
    //      with typed `superseded_by_changes` evidence).
    //    - The NEW approval (`approval_rev2.approval_id`) is NOT blocked (`blocked: false`,
    //      `approval_superseded: false`, `superseded_by_changes: []`).
    let list_out = fx
        .run_session_cli(
            &fx.binding_a,
            THREAD_A,
            &[
                "list-changes",
                "--approval-id",
                &fx.approval_a.approval_id,
                "--limit",
                "10",
            ],
        )
        .await;
    let page: ConversationChangePage = parse_cli_data(&list_out, &fx.binding_a);
    assert_eq!(page.current_change_revision, 2);
    assert_eq!(page.changes.len(), 1);
    assert_eq!(
        page.changes[0].decision.status,
        ConversationChangeStatus::Resolved
    );
    assert!(
        page.unresolved_block_summary.blocked,
        "old superseded approval must remain blocked after linked_plan_revision resolution"
    );
    assert!(page.unresolved_block_summary.approval_superseded);
    assert!(page.unresolved_block_summary.blocking_changes.is_empty());
    assert_eq!(page.unresolved_block_summary.superseded_by_changes.len(), 1);
    let sup_ev = &page.unresolved_block_summary.superseded_by_changes[0];
    assert_eq!(sup_ev.change_id, recorded.change_id);
    assert_eq!(
        sup_ev.superseded_approval_id.as_deref(),
        Some(fx.approval_a.approval_id.as_str())
    );
    assert_eq!(sup_ev.superseded_plan_revision, Some(1));
    assert_eq!(sup_ev.target_plan_revision, Some(2));
    assert_eq!(
        sup_ev.new_approval_id.as_deref(),
        Some(approval_rev2.approval_id.as_str())
    );
    assert_eq!(sup_ev.task_id.as_deref(), Some("task-1"));
    assert_eq!(
        sup_ev.affected_dependent_task_ids,
        vec!["task-2".to_string()]
    );

    for old_task in [None, Some("task-1"), Some("task-2"), Some("task-3")] {
        let old_summary = inspect_unresolved_blocking_changes(
            &fx.plans_dir,
            &fx.binding_a,
            Some(&fx.approval_a.approval_id),
            old_task,
        )
        .expect("inspect old superseded approval");
        assert!(
            old_summary.blocked,
            "old approval must stay blocked for task filter {old_task:?}"
        );
        assert!(old_summary.approval_superseded);
        assert!(old_summary.blocking_changes.is_empty());
        assert_eq!(old_summary.superseded_by_changes.len(), 1);
        assert_eq!(
            old_summary.superseded_by_changes[0]
                .new_approval_id
                .as_deref(),
            Some(approval_rev2.approval_id.as_str())
        );
    }

    for new_task in [None, Some("task-1"), Some("task-2"), Some("task-3")] {
        let new_summary = inspect_unresolved_blocking_changes(
            &fx.plans_dir,
            &fx.binding_a,
            Some(&approval_rev2.approval_id),
            new_task,
        )
        .expect("inspect new active approval");
        assert!(
            !new_summary.blocked,
            "new approval must not be falsely blocked for task filter {new_task:?}"
        );
        assert!(!new_summary.approval_superseded);
        assert!(new_summary.blocking_changes.is_empty());
        assert!(new_summary.superseded_by_changes.is_empty());
    }

    let new_list_out = fx
        .run_session_cli(
            &fx.binding_a,
            THREAD_A,
            &[
                "list-changes",
                "--approval-id",
                &approval_rev2.approval_id,
                "--limit",
                "10",
            ],
        )
        .await;
    let new_page: ConversationChangePage = parse_cli_data(&new_list_out, &fx.binding_a);
    assert!(!new_page.unresolved_block_summary.blocked);
    assert!(!new_page.unresolved_block_summary.approval_superseded);
    assert!(new_page
        .unresolved_block_summary
        .superseded_by_changes
        .is_empty());

    host.shutdown().await.expect("shutdown host");
    fx.shutdown().await;
}

#[tokio::test]
async fn stale_revisions_idempotency_cross_binding_isolation_and_unsupported_live_input_are_enforced(
) {
    let mut fx = ChangeRoutingFixture::start().await;
    let client_a = fx.host_client_a();
    let store = fx.change_store();

    // 1. `require_verified_live_input` fails closed with `unsupported_capability`.
    let require_live_req = ConversationChangeRequest {
        request_key: "req-require-live".to_string(),
        expected_change_revision: 0,
        approval_id: Some(fx.approval_a.approval_id.clone()),
        plan_id: Some(PLAN_ID.to_string()),
        expected_plan_revision: Some(1),
        task_id: Some("task-1".to_string()),
        expected_task_revision: Some(0),
        kind: ConversationChangeKind::Clarification,
        route: None,
        summary: "Demand verified live worker input".to_string(),
        detail: None,
        proposed_write_paths: Vec::new(),
        proposed_profile_id: None,
        proposed_model_id: None,
        proposed_effort: None,
        requested_delivery: ConversationDeliveryRequest::RequireVerifiedLiveInput,
        cancel_action: None,
    };
    let live_path = fx.write_request_file("require-live.json", &require_live_req);
    let live_out = fx
        .run_session_cli(
            &fx.binding_a,
            THREAD_A,
            &["record-change", "--request", live_path.to_str().unwrap()],
        )
        .await;
    assert_eq!(parse_cli_error(&live_out).0, "unsupported_capability");
    assert_eq!(store.current_revision(&fx.binding_a).unwrap(), 0);

    // 2. Record a `model_or_profile_change` and verify idempotent replay vs conflicting `request_key`.
    let model_req = ConversationChangeRequest {
        request_key: "req-model-change-1".to_string(),
        expected_change_revision: 0,
        approval_id: Some(fx.approval_a.approval_id.clone()),
        plan_id: None,
        expected_plan_revision: Some(1),
        task_id: Some("task-1".to_string()),
        expected_task_revision: Some(0),
        kind: ConversationChangeKind::ModelOrProfileChange,
        route: None,
        summary: "Switch task-1 model assignment to model-agy-alt.".to_string(),
        detail: None,
        proposed_write_paths: Vec::new(),
        proposed_profile_id: Some(PROFILE_ID.to_string()),
        proposed_model_id: Some("model-agy-alt".to_string()),
        proposed_effort: Some("high".to_string()),
        requested_delivery: ConversationDeliveryRequest::LiveContinuationIfSupported,
        cancel_action: None,
    };
    let first = store
        .record_change(
            &fx.binding_a,
            THREAD_A,
            &model_req,
            &fx.plans,
            &fx.authority,
            &client_a,
        )
        .await
        .expect("record model change");
    assert_eq!(first.created_change_revision, 1);
    assert_eq!(
        first.decision.live_input_status,
        LiveInputSupportStatus::UnsupportedNoLiveInputPath
    );
    assert!(!first.decision.live_input_delivered);

    // Exact retry with the same `request_key` returns the same `change_id` and revision without duplicating.
    let replay = store
        .record_change(
            &fx.binding_a,
            THREAD_A,
            &model_req,
            &fx.plans,
            &fx.authority,
            &client_a,
        )
        .await
        .expect("idempotent replay of model change");
    assert_eq!(replay.change_id, first.change_id);
    assert_eq!(store.current_revision(&fx.binding_a).unwrap(), 1);

    // Reusing `req-model-change-1` with a different summary fails with `idempotency_conflict`.
    let mut conflicting = model_req.clone();
    conflicting.summary = "Different summary with same request_key".to_string();
    let conflict_err = store
        .record_change(
            &fx.binding_a,
            THREAD_A,
            &conflicting,
            &fx.plans,
            &fx.authority,
            &client_a,
        )
        .await
        .expect_err("conflicting request_key must fail");
    assert_eq!(conflict_err.code(), "idempotency_conflict");

    // 3. Stale `expected_change_revision`, stale `expected_plan_revision`, and stale `expected_task_revision` fail with `stale_revision`.
    let mut stale_change_rev = model_req.clone();
    stale_change_rev.request_key = "req-stale-change-rev".to_string();
    stale_change_rev.expected_change_revision = 0;
    assert_eq!(
        store
            .record_change(
                &fx.binding_a,
                THREAD_A,
                &stale_change_rev,
                &fx.plans,
                &fx.authority,
                &client_a,
            )
            .await
            .expect_err("stale expected_change_revision")
            .code(),
        "stale_revision"
    );

    let mut stale_plan_rev = model_req.clone();
    stale_plan_rev.request_key = "req-stale-plan-rev".to_string();
    stale_plan_rev.expected_change_revision = 1;
    stale_plan_rev.expected_plan_revision = Some(99);
    assert_eq!(
        store
            .record_change(
                &fx.binding_a,
                THREAD_A,
                &stale_plan_rev,
                &fx.plans,
                &fx.authority,
                &client_a,
            )
            .await
            .expect_err("stale expected_plan_revision")
            .code(),
        "stale_revision"
    );

    let mut stale_task_rev = model_req.clone();
    stale_task_rev.request_key = "req-stale-task-rev".to_string();
    stale_task_rev.expected_change_revision = 1;
    stale_task_rev.expected_task_revision = Some(5);
    assert_eq!(
        store
            .record_change(
                &fx.binding_a,
                THREAD_A,
                &stale_task_rev,
                &fx.plans,
                &fx.authority,
                &client_a,
            )
            .await
            .expect_err("stale expected_task_revision")
            .code(),
        "stale_revision"
    );

    // 4. Cross-binding and cross-thread attempts via CLI are rejected with `scope_mismatch`.
    let mut foreign_req = model_req.clone();
    foreign_req.request_key = "req-foreign-binding".to_string();
    foreign_req.expected_change_revision = 0;
    let foreign_path = fx.write_request_file("foreign.json", &foreign_req);
    let foreign_out = fx
        .run_session_cli(
            &fx.binding_b,
            THREAD_B,
            &["record-change", "--request", foreign_path.to_str().unwrap()],
        )
        .await;
    assert_eq!(parse_cli_error(&foreign_out).0, "scope_mismatch");

    let foreign_list_out = fx
        .run_session_cli(
            &fx.binding_b,
            THREAD_B,
            &["list-changes", "--approval-id", &fx.approval_a.approval_id],
        )
        .await;
    assert_eq!(parse_cli_error(&foreign_list_out).0, "scope_mismatch");

    fx.shutdown().await;
}

#[tokio::test]
async fn plan_only_clarification_preserves_literal_scopes_and_blocks_downstream_tasks() {
    let mut fx = ChangeRoutingFixture::start().await;
    let request = ConversationChangeRequest {
        request_key: "plan-only-clarification".to_string(),
        expected_change_revision: 0,
        approval_id: None,
        plan_id: Some(PLAN_ID.to_string()),
        expected_plan_revision: Some(1),
        task_id: Some("task-1".to_string()),
        expected_task_revision: None,
        kind: ConversationChangeKind::Clarification,
        route: None,
        summary: "Clarify the existing literal file at its checkpoint.".to_string(),
        detail: None,
        proposed_write_paths: vec!["src/lib.rs".to_string()],
        proposed_profile_id: None,
        proposed_model_id: None,
        proposed_effort: None,
        requested_delivery: ConversationDeliveryRequest::SafeCheckpointOrReplan,
        cancel_action: None,
    };
    let mut expansion = request.clone();
    expansion.proposed_write_paths = vec!["src/lib.rs/child.rs".to_string()];
    let path = fx.write_request_file("plan-only-expansion.json", &expansion);
    let output = fx
        .run_session_cli(
            &fx.binding_a,
            THREAD_A,
            &["record-change", "--request", path.to_str().unwrap()],
        )
        .await;
    assert_eq!(parse_cli_error(&output).0, "route_mismatch");
    let path = fx.write_request_file("plan-only-valid.json", &request);
    let output = fx
        .run_session_cli(
            &fx.binding_a,
            THREAD_A,
            &["record-change", "--request", path.to_str().unwrap()],
        )
        .await;
    let record: ConversationChangeRecord = parse_cli_data(&output, &fx.binding_a);
    assert_eq!(
        record.affected_dependent_task_ids,
        vec!["task-2".to_string()]
    );
    assert!(!record.decision.prior_task_proven_stopped);
    for task in ["task-1", "task-2"] {
        let block = inspect_unresolved_blocking_changes(
            &fx.plans_dir,
            &fx.binding_a,
            Some(&fx.approval_a.approval_id),
            Some(task),
        )
        .expect("inspect plan-only block");
        assert!(block.blocked, "root and dependent must remain blocked");
    }
    let sibling = inspect_unresolved_blocking_changes(
        &fx.plans_dir,
        &fx.binding_a,
        Some(&fx.approval_a.approval_id),
        Some("task-3"),
    )
    .expect("inspect sibling");
    assert!(!sibling.blocked);
    assert_eq!(fx.worker_invocation_count(), 0);
    fx.shutdown().await;
}

#[tokio::test]
async fn plan_only_resolution_cannot_hide_an_active_approved_execution() {
    let mut fx = ChangeRoutingFixture::start().await;
    let host = fx.start_host_for_approval(&fx.approval_a.approval_id).await;
    let client = fx.host_client_a();
    client
        .dispatch_task(&fx.approval_a.approval_id, "task-1")
        .await
        .expect("dispatch");
    wait_for_task_status(
        &client,
        &fx.approval_a.approval_id,
        "task-1",
        TaskExecutionStatus::Running,
    )
    .await;
    let request = ConversationChangeRequest {
        request_key: "plan-only-live-clarification".to_string(),
        expected_change_revision: 0,
        approval_id: None,
        plan_id: Some(PLAN_ID.to_string()),
        expected_plan_revision: Some(1),
        task_id: Some("task-1".to_string()),
        expected_task_revision: None,
        kind: ConversationChangeKind::Clarification,
        route: None,
        summary: "Clarify only at a stopped checkpoint.".to_string(),
        detail: None,
        proposed_write_paths: Vec::new(),
        proposed_profile_id: None,
        proposed_model_id: None,
        proposed_effort: None,
        requested_delivery: ConversationDeliveryRequest::SafeCheckpointOrReplan,
        cancel_action: None,
    };
    let path = fx.write_request_file("plan-only-live.json", &request);
    let output = fx
        .run_session_cli(
            &fx.binding_a,
            THREAD_A,
            &["record-change", "--request", path.to_str().unwrap()],
        )
        .await;
    let record: ConversationChangeRecord = parse_cli_data(&output, &fx.binding_a);
    let resolve = ResolveConversationChangeRequest {
        request_key: "plan-only-live-resolve".to_string(),
        change_id: record.change_id,
        expected_change_revision: 1,
        resolution_kind: ConversationChangeResolutionKind::CheckpointHandled,
        target_plan_revision: Some(1),
        new_approval_id: None,
        resolution_note: None,
    };
    let path = fx.write_request_file("plan-only-live-resolve.json", &resolve);
    let output = fx
        .run_session_cli(
            &fx.binding_a,
            THREAD_A,
            &["resolve-change", "--request", path.to_str().unwrap()],
        )
        .await;
    assert_eq!(parse_cli_error(&output).0, "task_not_proven_stopped");
    let blocked = inspect_unresolved_blocking_changes(
        &fx.plans_dir,
        &fx.binding_a,
        Some(&fx.approval_a.approval_id),
        Some("task-2"),
    )
    .expect("still blocked");
    assert!(blocked.blocked);
    assert_eq!(
        client
            .get_task(&fx.approval_a.approval_id, "task-1")
            .await
            .expect("live")
            .status,
        TaskExecutionStatus::Running
    );
    assert_eq!(fx.worker_invocation_count(), 1);
    host.shutdown().await.expect("shutdown host");
    fx.shutdown().await;
}
