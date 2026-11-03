#![cfg(unix)]
//! Behavioral CLI integration tests for `threadlane-flow coworker-execution`
//! (`preview`, `record`, and `inspect`).

use std::collections::BTreeMap;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Output;
use std::sync::Arc;
use std::time::Duration;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tempfile::TempDir;
use threadlane::{Supervisor, SupervisorClient, SupervisorConfig, SupervisorHandle};
use threadlane_authority::{AuthorityStore, OperatorApprovalIntent};
use threadlane_native::coworker_workflow::{
    BindJobApprovalRequest, CanonicalProjectBaseRef, ControllerConfigurationPreview,
    ControllerJobRuntimeState, ControllerRoleExecutionBudget, CoworkerJobRecord,
    CoworkerWorkflowStore, CreateCoworkerJobRequest, FreezeControllerExecutionEnvelopeRequest,
    FrozenCheckCommandSpec, FrozenCheckKind, FrozenControllerCapabilityEnvelope, WorkBrief,
    WorkBriefKind,
};
use threadlane_native::{
    ApprovalSummary, Difficulty, ExecutionPolicy, PlanStore, PlanTask, PolicyAllowRule,
    RegistryStore, RoleExecutionRoute, SavePlanDraft, ScopedWorkflow, SelectedRoleChoice,
    ToolAdapter, WorkflowProfile, WorkflowRole, EXECUTION_POLICY_SCHEMA_VERSION,
};
use threadlane_operations::IntegrationStrategy;
use threadlane_workers::{
    resolve_exact_commit_sha, resolve_exact_tree_sha, run_sanitized_git,
    DEFAULT_GIT_COMMAND_TIMEOUT_MS,
};
use threadlane_workflow::{
    ModelChoice, ModelDefinition, RegistryChange, RoleAssignment, ToolDefinition,
};
use tokio::process::Command;

const THREAD_UUID: &str = "0196ed00-0000-7000-8000-0000000000b1";
const OTHER_THREAD_UUID: &str = "0196ed00-0000-7000-8000-0000000000b2";
const BINDING_LABEL: &str = "execution-envelope-cli-chat";
const PROFILE_ID: &str = "profile-exec-cli";
const PLAN_ID: &str = "plan_exec_cli_1";
const TASK_ID: &str = "task-1";
const TOOL_ID: &str = "tool-agy";
const MODEL_ID: &str = "model-argon";
const ALT_MODEL_ID: &str = "model-unapproved";
const MAX_IO_BYTES: usize = 1_048_576;
const COMMAND_TIMEOUT: Duration = Duration::from_secs(30);
const SECRET_SENTINEL: &str = "THREADLANE_EXEC_CLI_SECRET_SENTINEL_7a4c2d";
const WRONG_DIGEST: &str = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";

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

struct ExecutionCliFixture {
    _tmp: TempDir,
    repo_dir: PathBuf,
    base_commit: String,
    base_tree_sha: String,
    worker_marker: PathBuf,
    check_marker: PathBuf,
    fake_check_bin: PathBuf,
    state_dir: PathBuf,
    registry_dir: PathBuf,
    plans_dir: PathBuf,
    authority_dir: PathBuf,
    sessions_dir: PathBuf,
    requests_dir: PathBuf,
    supervisor: Option<SupervisorHandle>,
    binding_id: String,
    approval: ApprovalSummary,
    job: CoworkerJobRecord,
}

impl ExecutionCliFixture {
    async fn start() -> Self {
        let tmp = TempDir::new().expect("create execution cli tempdir");
        let root = fs::canonicalize(tmp.path()).expect("canonicalize execution cli tempdir");
        fs::set_permissions(&root, fs::Permissions::from_mode(0o700))
            .expect("chmod execution cli root 0700");

        let repo_dir = root.join("repo");
        let (base_commit, base_tree_sha) = init_git_repo(&repo_dir);

        let bin_dir = root.join("bin");
        create_private_dir(&bin_dir);
        let fake_worker = bin_dir.join("fake-agy");
        let worker_marker = root.join("worker-invoked");
        write_fake_executable(&fake_worker, &worker_marker);

        let fake_check_bin = bin_dir.join("fake-cargo-check");
        let check_marker = root.join("check-invoked");
        write_fake_executable(&fake_check_bin, &check_marker);

        let state_dir = root.join("runtime");
        let registry_dir = root.join("registry-store");
        let plans_dir = root.join("plans-store");
        let authority_dir = root.join("authority-store");
        let sessions_dir = root.join("sessions-store");
        let requests_dir = root.join("requests");
        for dir in [
            &state_dir,
            &registry_dir,
            &plans_dir,
            &authority_dir,
            &sessions_dir,
            &requests_dir,
        ] {
            create_private_dir(dir);
        }

        let supervisor = Supervisor::start(SupervisorConfig::new(state_dir.clone()))
            .await
            .expect("start disposable execution supervisor");
        let supervisor_client =
            SupervisorClient::from_state_dir(&state_dir).expect("connect execution supervisor");
        let binding = supervisor_client
            .create_binding(THREAD_UUID, BINDING_LABEL)
            .await
            .expect("create execution control binding");

        let registry =
            Arc::new(RegistryStore::open(&registry_dir).expect("open execution registry store"));
        let s1 = registry
            .apply(
                0,
                RegistryChange::UpsertTool {
                    tool: ToolDefinition {
                        id: TOOL_ID.to_string(),
                        label: "Antigravity execution fixture".to_string(),
                        adapter: ToolAdapter::Antigravity,
                        executable_path: Some(fake_worker),
                        enabled: true,
                    },
                },
            )
            .expect("upsert execution tool");
        let s2 = registry
            .apply(
                s1.revision,
                RegistryChange::UpsertModels {
                    models: vec![
                        ModelDefinition {
                            id: MODEL_ID.to_string(),
                            tool_id: TOOL_ID.to_string(),
                            label: "Gemini 4 Argon High".to_string(),
                            model_arg: "gemini-4-argon-high".to_string(),
                            efforts: vec!["high".to_string(), "medium".to_string()],
                            allow_no_effort: false,
                        },
                        ModelDefinition {
                            id: ALT_MODEL_ID.to_string(),
                            tool_id: TOOL_ID.to_string(),
                            label: "Unapproved Secondary Model".to_string(),
                            model_arg: "gemini-4-secondary".to_string(),
                            efforts: vec!["high".to_string()],
                            allow_no_effort: false,
                        },
                    ],
                },
            )
            .expect("upsert execution models");
        let s3 = registry
            .apply(
                s2.revision,
                RegistryChange::UpsertProfile {
                    profile: WorkflowProfile {
                        decision_advisor: None,
                        id: PROFILE_ID.to_string(),
                        name: "Execution envelope CLI profile".to_string(),
                        assignments: vec![
                            RoleAssignment {
                                role: WorkflowRole::Coder,
                                difficulty: Some(Difficulty::Hard),
                                choice: ModelChoice {
                                    model_id: MODEL_ID.to_string(),
                                    effort: Some("high".to_string()),
                                },
                            },
                            RoleAssignment {
                                role: WorkflowRole::Reviewer,
                                difficulty: Some(Difficulty::Hard),
                                choice: ModelChoice {
                                    model_id: MODEL_ID.to_string(),
                                    effort: Some("high".to_string()),
                                },
                            },
                            RoleAssignment {
                                role: WorkflowRole::Closure,
                                difficulty: Some(Difficulty::Hard),
                                choice: ModelChoice {
                                    model_id: MODEL_ID.to_string(),
                                    effort: Some("high".to_string()),
                                },
                            },
                        ],
                    },
                },
            )
            .expect("upsert execution profile");
        assert_eq!(s3.revision, 3);

        let plans = Arc::new(PlanStore::open(&plans_dir).expect("open execution plan store"));
        let workflow = ScopedWorkflow::new(
            Arc::clone(&registry),
            Arc::clone(&plans),
            binding.binding_id.clone(),
        )
        .expect("create scoped workflow");
        let draft = workflow
            .save_draft(SavePlanDraft {
                command_key: "cmd-exec-draft-1".to_string(),
                plan_id: Some(PLAN_ID.to_string()),
                expected_revision: 0,
                profile_id: PROFILE_ID.to_string(),
                profile_revision: 3,
                summary: "Execution envelope CLI test plan".to_string(),
                tasks: vec![PlanTask {
                    id: TASK_ID.to_string(),
                    title: "Implement verified module".to_string(),
                    instruction: "Update src/lib.rs within approved write scope.".to_string(),
                    role: WorkflowRole::Coder,
                    difficulty: Difficulty::Hard,
                    depends_on: Vec::new(),
                    allowed_write_paths: vec!["src/lib.rs".to_string()],
                }],
            })
            .expect("save execution plan draft");
        workflow
            .publish(&draft.plan_id, draft.revision)
            .expect("publish execution plan");

        let authority =
            AuthorityStore::open(&authority_dir, Arc::clone(&registry), Arc::clone(&plans))
                .expect("open execution authority store");
        let approval_preview = authority
            .prepare_for_operator(OperatorApprovalIntent {
                binding_id: binding.binding_id.clone(),
                thread_uuid: THREAD_UUID.to_string(),
                plan_id: PLAN_ID.to_string(),
                plan_revision: 1,
                expected_registry_revision: 3,
                repository_path: repo_dir.clone(),
                base_commit: base_commit.clone(),
                approved_task_ids: vec![TASK_ID.to_string()],
                approved_write_paths: vec!["src/lib.rs".to_string()],
                max_parallel: 1,
                budgets: Default::default(),
            })
            .expect("prepare operator approval");
        let approval = authority
            .approve_confirmed(approval_preview.request, &approval_preview.subject_digest)
            .expect("record operator approval");

        let policy = sample_execution_policy();
        let reviewer_choice = sample_headless_choice();
        let closure_choice = sample_headless_choice();
        let workflow_store =
            CoworkerWorkflowStore::new(&plans_dir).expect("open coworker workflow store");
        let created_job = workflow_store
            .create_or_amend_job(&CreateCoworkerJobRequest {
                binding_id: binding.binding_id.clone(),
                thread_uuid: THREAD_UUID.to_string(),
                request_key: "req-exec-job-1".to_string(),
                expected_ledger_revision: Some(0),
                work_brief: WorkBrief {
                    brief_id: "brf_exec_cli_1".to_string(),
                    binding_id: binding.binding_id.clone(),
                    thread_uuid: THREAD_UUID.to_string(),
                    project_base: CanonicalProjectBaseRef {
                        repository_path: repo_dir.display().to_string(),
                        base_commit: base_commit.clone(),
                        base_tree_sha: base_tree_sha.clone(),
                    },
                    kind: WorkBriefKind::NewJob,
                    summary: "Execution envelope CLI test brief".to_string(),
                    confirmed_requirements: vec![
                        "Implement src/lib.rs and verify with frozen check commands.".to_string(),
                    ],
                    proposed_additions: Vec::new(),
                    assumptions: Vec::new(),
                    open_questions: Vec::new(),
                    accepted_architect_decisions: Vec::new(),
                },
                amended_target_task_ids: Vec::new(),
                amended_write_paths: Vec::new(),
                amended_interfaces: Vec::new(),
            })
            .expect("create coworker job");
        let job = workflow_store
            .bind_job_approval(
                &BindJobApprovalRequest {
                    binding_id: binding.binding_id.clone(),
                    thread_uuid: THREAD_UUID.to_string(),
                    job_id: created_job.job_id.clone(),
                    expected_job_revision: 1,
                    expected_ledger_revision: Some(1),
                    approval_id: approval.approval_id.clone(),
                    policy,
                    controller_envelope: Some(FrozenControllerCapabilityEnvelope {
                        allowed_reviewer_choices: vec![reviewer_choice],
                        allowed_closure_choices: vec![closure_choice],
                        max_in_scope_correction_attempts: 2,
                        allowed_check_ids: vec!["check-fmt".to_string(), "check-test".to_string()],
                        local_git_integration_allowed: true,
                    }),
                    task_interfaces: BTreeMap::new(),
                },
                &authority,
            )
            .expect("bind job approval");

        Self {
            _tmp: tmp,
            repo_dir,
            base_commit,
            base_tree_sha,
            worker_marker,
            check_marker,
            fake_check_bin,
            state_dir,
            registry_dir,
            plans_dir,
            authority_dir,
            sessions_dir,
            requests_dir,
            supervisor: Some(supervisor),
            binding_id: binding.binding_id,
            approval,
            job,
        }
    }

    fn exec_args(&self, subcommand: &str, extra: &[String]) -> Vec<String> {
        self.exec_args_for(&self.binding_id, THREAD_UUID, subcommand, extra)
    }

    fn exec_args_for(
        &self,
        binding_id: &str,
        thread_uuid: &str,
        subcommand: &str,
        extra: &[String],
    ) -> Vec<String> {
        let mut args = vec![
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
            binding_id.to_string(),
            "--thread".to_string(),
            thread_uuid.to_string(),
            "coworker-execution".to_string(),
            subcommand.to_string(),
        ];
        args.extend(extra.iter().cloned());
        args
    }

    fn write_request<T: Serialize>(&self, name: &str, request: &T) -> PathBuf {
        let path = self.requests_dir.join(name);
        let bytes = serde_json::to_vec(request).expect("serialize execution request");
        write_private_bytes(&path, &bytes);
        path
    }

    fn sample_envelope_request(&self) -> FreezeControllerExecutionEnvelopeRequest {
        let mut role_budgets = BTreeMap::new();
        role_budgets.insert(
            WorkflowRole::Coder,
            ControllerRoleExecutionBudget::default_for_role(WorkflowRole::Coder, 4),
        );
        role_budgets.insert(
            WorkflowRole::Reviewer,
            ControllerRoleExecutionBudget::default_for_role(WorkflowRole::Reviewer, 4),
        );
        role_budgets.insert(
            WorkflowRole::Closure,
            ControllerRoleExecutionBudget::default_for_role(WorkflowRole::Closure, 2),
        );

        let mut check_commands = BTreeMap::new();
        check_commands.insert(
            "check-fmt".to_string(),
            FrozenCheckCommandSpec {
                check_id: "check-fmt".to_string(),
                kind: FrozenCheckKind::Formatting,
                executable: self.fake_check_bin.clone(),
                argv: vec!["fmt".to_string(), "--check".to_string()],
                repo_relative_cwd: ".".to_string(),
                timeout_ms: 30_000,
                max_stdout_bytes: 65_536,
                max_stderr_bytes: 65_536,
                required_for_task_review: true,
                required_for_candidate_closure: true,
            },
        );
        check_commands.insert(
            "check-test".to_string(),
            FrozenCheckCommandSpec {
                check_id: "check-test".to_string(),
                kind: FrozenCheckKind::Tests,
                executable: self.fake_check_bin.clone(),
                argv: vec!["test".to_string(), "--lib".to_string()],
                repo_relative_cwd: "src".to_string(),
                timeout_ms: 60_000,
                max_stdout_bytes: 65_536,
                max_stderr_bytes: 65_536,
                required_for_task_review: true,
                required_for_candidate_closure: true,
            },
        );

        FreezeControllerExecutionEnvelopeRequest {
            binding_id: self.binding_id.clone(),
            thread_uuid: THREAD_UUID.to_string(),
            job_id: self.job.job_id.clone(),
            expected_job_revision: self.job.job_revision,
            policy: sample_execution_policy(),
            reviewer_choice: sample_headless_choice(),
            closure_choice: sample_headless_choice(),
            role_budgets,
            max_in_scope_corrections_per_task: 2,
            max_total_corrections_per_job: 4,
            max_total_task_attempts_per_job: 6,
            check_commands,
            allow_task_worktree_commits: true,
            allow_run_integration_execution: true,
            default_integration_strategy: IntegrationStrategy::FastForwardOnly,
            independence_completeness_receipt: None,
            shadow_jev_policy: None,
            created_at_ms: 1_760_000_100_000,
            operator_attested: false,
            confirm_approval_subject_digest: None,
            confirm_envelope_digest: None,
        }
    }

    async fn shutdown(&mut self) {
        if let Some(supervisor) = self.supervisor.take() {
            supervisor.shutdown().await;
        }
    }
}

fn sample_execution_policy() -> ExecutionPolicy {
    ExecutionPolicy {
        schema_version: EXECUTION_POLICY_SCHEMA_VERSION,
        policy_id: "policy-exec-cli".to_string(),
        allow_rules: vec![PolicyAllowRule {
            tool_id: TOOL_ID.to_string(),
            model_id: MODEL_ID.to_string(),
            efforts: vec!["high".to_string()],
            allow_no_effort: false,
            roles: vec![
                WorkflowRole::Coder,
                WorkflowRole::Reviewer,
                WorkflowRole::Closure,
            ],
        }],
    }
}

fn sample_headless_choice() -> SelectedRoleChoice {
    SelectedRoleChoice {
        tool_id: TOOL_ID.to_string(),
        adapter: ToolAdapter::Antigravity,
        route: RoleExecutionRoute::HeadlessWorker,
        model_id: MODEL_ID.to_string(),
        model_arg: "gemini-4-argon-high".to_string(),
        effort: Some("high".to_string()),
        matched_difficulty: Some(Difficulty::Hard),
    }
}

fn init_git_repo(repo_dir: &Path) -> (String, String) {
    fs::create_dir_all(repo_dir.join("src")).expect("create repo src");
    fs::write(
        repo_dir.join("src/lib.rs"),
        "pub fn execution_seed() -> u32 { 23 }\n",
    )
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
    let commit = resolve_exact_commit_sha(repo_dir, "HEAD", DEFAULT_GIT_COMMAND_TIMEOUT_MS)
        .expect("resolve HEAD commit");
    let tree = resolve_exact_tree_sha(repo_dir, &commit, DEFAULT_GIT_COMMAND_TIMEOUT_MS)
        .expect("resolve HEAD tree");
    (commit, tree)
}

fn create_private_dir(path: &Path) {
    fs::create_dir_all(path).expect("create private fixture directory");
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))
        .unwrap_or_else(|err| panic!("chmod private fixture {} 0700: {err}", path.display()));
}

fn write_fake_executable(path: &Path, marker: &Path) {
    let script = format!(
        "#!/bin/sh\nset -eu\nprintf 'invoked\\n' > '{}'\nexit 97\n",
        marker.display()
    );
    fs::write(path, script).expect("write fake executable");
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))
        .expect("chmod fake executable 0700");
}

fn write_private_bytes(path: &Path, bytes: &[u8]) {
    let parent = path.parent().expect("private request path has a parent");
    create_private_dir(parent);
    fs::write(path, bytes).expect("write private request file");
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))
        .expect("chmod private request file 0600");
}

fn directory_entries(path: &Path) -> Vec<String> {
    let mut names: Vec<String> = fs::read_dir(path)
        .unwrap_or_else(|err| panic!("read directory {}: {err}", path.display()))
        .map(|entry| {
            entry
                .expect("read directory entry")
                .file_name()
                .to_string_lossy()
                .into_owned()
        })
        .collect();
    names.sort();
    names
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

async fn run_flow_with_env(args: &[String], envs: &[(&str, &str)]) -> Output {
    let mut command = sanitized_command(PathBuf::from(env!("CARGO_BIN_EXE_threadlane-flow")));
    command.args(args);
    for (k, v) in envs {
        command.env(k, v);
    }
    tokio::time::timeout(COMMAND_TIMEOUT, command.output())
        .await
        .expect("threadlane-flow did not finish within timeout")
        .expect("execute threadlane-flow")
}

async fn run_flow(args: &[String]) -> Output {
    run_flow_with_env(args, &[]).await
}

async fn run_flow_offline(args: &[&str]) -> Output {
    let mut command = Command::new(PathBuf::from(env!("CARGO_BIN_EXE_threadlane-flow")));
    command.args(args).env_clear();
    tokio::time::timeout(COMMAND_TIMEOUT, command.output())
        .await
        .expect("offline command did not finish")
        .expect("execute offline threadlane-flow")
}

fn parse_envelope<T: DeserializeOwned>(output: &Output, context: &str, binding_id: &str) -> T {
    assert!(
        output.status.success(),
        "{context} failed with status {:?}\nstdout: {}\nstderr: {}",
        output.status.code(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        output.stdout.len() <= MAX_IO_BYTES,
        "{context}: stdout exceeded bounded 1 MiB result envelope"
    );
    let envelope: OperatorEnvelope<T> =
        serde_json::from_slice(&output.stdout).unwrap_or_else(|err| {
            panic!(
                "{context}: invalid operator JSON envelope: {err}\nstdout: {}",
                String::from_utf8_lossy(&output.stdout)
            )
        });
    assert!(
        envelope.untrusted_data,
        "{context}: untrusted_data must be true"
    );
    assert_eq!(envelope.route, "trusted_local_operator");
    assert!(
        !envelope.native_per_call_identity_verified,
        "{context}: local operator route must never claim per-call native identity"
    );
    assert_eq!(envelope.binding_id, binding_id);
    envelope.data
}

fn expect_failure_code(output: &Output, context: &str, expected: &str) -> CliFailure {
    assert!(
        !output.status.success(),
        "{context}: expected nonzero exit, got success\nstdout: {}",
        String::from_utf8_lossy(&output.stdout)
    );
    assert!(
        output.stdout.is_empty(),
        "{context}: failed command must not write result data to stdout"
    );
    assert!(output.stderr.len() <= MAX_IO_BYTES);
    let failure: CliFailure = serde_json::from_slice(&output.stderr).unwrap_or_else(|err| {
        panic!(
            "{context}: stderr must contain static JSON error envelope: {err}\nstderr: {}",
            String::from_utf8_lossy(&output.stderr)
        )
    });
    assert_eq!(failure.code, expected, "{context}: unexpected error code");
    assert!(!failure.message.trim().is_empty());
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
            "{context}: secret sentinel echoed on {stream}: {text}"
        );
    }
}

#[tokio::test]
async fn help_and_invalid_coworker_execution_cli_arguments_use_bounded_error_envelope() {
    let help = run_flow_offline(&["--help"]).await;
    assert!(help.status.success());
    let help_text = String::from_utf8_lossy(&help.stdout);
    assert!(help_text.contains("coworker-execution"));

    let group_help = run_flow_offline(&["coworker-execution", "--help"]).await;
    assert!(group_help.status.success());
    let group_help_text = String::from_utf8_lossy(&group_help.stdout);
    for sub in ["preview", "record", "inspect"] {
        assert!(
            group_help_text.contains(sub),
            "coworker-execution --help must list `{sub}`"
        );
    }

    let record_help = run_flow_offline(&["coworker-execution", "record", "--help"]).await;
    assert!(record_help.status.success());
    let record_help_text = String::from_utf8_lossy(&record_help.stdout);
    assert!(record_help_text.contains("--confirm-approval"));
    assert!(record_help_text.contains("--confirm-envelope"));
    assert!(record_help_text.contains("--operator-approved"));

    expect_failure_code(
        &run_flow_offline(&["coworker-execution", "preview"]).await,
        "missing coworker-execution preview --request",
        "invalid_input",
    );
    expect_failure_code(
        &run_flow_offline(&["coworker-execution", "record"]).await,
        "missing coworker-execution record --request",
        "invalid_input",
    );
    expect_failure_code(
        &run_flow_offline(&["coworker-execution", "inspect"]).await,
        "missing coworker-execution inspect --job-id",
        "invalid_input",
    );
}

#[tokio::test]
async fn coworker_execution_preview_and_inspect_are_pure_and_return_exact_identity() {
    let mut fixture = ExecutionCliFixture::start().await;
    let mut preview_req = fixture.sample_envelope_request();
    // Even if the input JSON attempts to set operator_attested or confirmation digests,
    // preview must force operator_attested = false and never persist state.
    preview_req.operator_attested = true;
    preview_req.confirm_approval_subject_digest = Some(WRONG_DIGEST.to_string());
    preview_req.confirm_envelope_digest = Some(WRONG_DIGEST.to_string());
    let preview_path = fixture.write_request("exec-preview.json", &preview_req);

    let plans_before = directory_entries(&fixture.plans_dir);
    let registry_before = directory_entries(&fixture.registry_dir);
    let authority_before = directory_entries(&fixture.authority_dir);
    let sessions_before = directory_entries(&fixture.sessions_dir);
    let approvals_bytes_before =
        fs::read(fixture.authority_dir.join("approvals.json")).expect("read approvals.json");

    let parent_envs = [
        ("THREADLANE_PARENT_REQUEST_ID", "obsreq_parent_exec_preview"),
        ("THREADLANE_PLAN_ID", PLAN_ID),
    ];

    let preview_out = run_flow_with_env(
        &fixture.exec_args(
            "preview",
            &["--request".to_string(), preview_path.display().to_string()],
        ),
        &parent_envs,
    )
    .await;
    let preview: ControllerConfigurationPreview = parse_envelope(
        &preview_out,
        "coworker-execution preview",
        &fixture.binding_id,
    );

    assert!(preview.envelope_id.starts_with("env_"));
    assert_eq!(preview.envelope_digest.len(), 64);
    assert_eq!(
        preview.approval_subject_digest,
        fixture.approval.subject_digest
    );
    assert_eq!(preview.work_brief_digest, fixture.job.work_brief_digest);
    assert_eq!(preview.plan_digest, fixture.approval.plan_digest);
    assert_eq!(preview.check_spec_digests.len(), 2);
    assert!(preview.check_spec_digests.contains_key("check-fmt"));
    assert!(preview.check_spec_digests.contains_key("check-test"));

    let env = &preview.envelope;
    assert_eq!(env.envelope_id, preview.envelope_id);
    assert_eq!(env.envelope_digest, preview.envelope_digest);
    assert_eq!(env.binding_id, fixture.binding_id);
    assert_eq!(env.thread_uuid, THREAD_UUID);
    assert_eq!(env.job_id, fixture.job.job_id);
    assert_eq!(env.approval_id, fixture.approval.approval_id);
    assert_eq!(env.approval_subject_digest, fixture.approval.subject_digest);
    assert_eq!(env.project_base.repository_path, fixture.repo_dir);
    assert_eq!(env.project_base.base_commit, fixture.base_commit);
    assert_eq!(env.project_base.base_tree_sha, fixture.base_tree_sha);
    assert!(
        !env.operator_attested,
        "preview must never set operator_attested = true"
    );
    assert!(!env.native_per_call_identity_verified);
    assert_eq!(
        env.task_coder_choices.get(TASK_ID),
        Some(&sample_headless_choice())
    );
    assert_eq!(env.reviewer_choice, sample_headless_choice());
    assert_eq!(env.closure_choice, sample_headless_choice());
    assert_eq!(env.max_in_scope_corrections_per_task, 2);
    assert_eq!(env.max_total_corrections_per_job, 4);
    assert_eq!(env.max_total_task_attempts_per_job, 6);
    assert!(env.allow_task_worktree_commits);
    assert!(env.allow_run_integration_execution);
    assert_eq!(
        env.default_integration_strategy,
        IntegrationStrategy::FastForwardOnly
    );
    assert_eq!(
        env.check_commands["check-fmt"].argv,
        vec!["fmt".to_string(), "--check".to_string()]
    );
    assert_eq!(env.check_commands["check-test"].repo_relative_cwd, "src");

    let inspect_out = run_flow_with_env(
        &fixture.exec_args(
            "inspect",
            &["--job-id".to_string(), fixture.job.job_id.clone()],
        ),
        &parent_envs,
    )
    .await;
    expect_failure_code(
        &inspect_out,
        "inspect unrecorded job envelope",
        "not_configured",
    );

    assert_eq!(
        directory_entries(&fixture.plans_dir),
        plans_before,
        "preview and inspect must not create controller ledger, lock, or journal files"
    );
    assert_eq!(directory_entries(&fixture.registry_dir), registry_before);
    assert_eq!(directory_entries(&fixture.authority_dir), authority_before);
    assert_eq!(directory_entries(&fixture.sessions_dir), sessions_before);
    assert_eq!(
        fs::read(fixture.authority_dir.join("approvals.json")).expect("read approvals.json"),
        approvals_bytes_before
    );
    assert!(!fixture.worker_marker.exists());
    assert!(!fixture.check_marker.exists());

    fixture.shutdown().await;
}

#[tokio::test]
async fn coworker_execution_record_rejects_missing_attestation_or_confirm_digests_and_wrong_context(
) {
    let mut fixture = ExecutionCliFixture::start().await;
    let base_req = fixture.sample_envelope_request();
    let preview_path = fixture.write_request("preview-guard.json", &base_req);
    let preview: ControllerConfigurationPreview = parse_envelope(
        &run_flow(&fixture.exec_args(
            "preview",
            &["--request".to_string(), preview_path.display().to_string()],
        ))
        .await,
        "preview for guard test",
        &fixture.binding_id,
    );

    // 1. Wrong CLI --binding or --thread is rejected before touching any store.
    expect_failure_code(
        &run_flow(&fixture.exec_args_for(
            "bnd_unknown_exec_scope",
            THREAD_UUID,
            "preview",
            &["--request".to_string(), preview_path.display().to_string()],
        ))
        .await,
        "wrong CLI binding",
        "binding_not_found",
    );
    expect_failure_code(
        &run_flow(&fixture.exec_args_for(
            &fixture.binding_id,
            OTHER_THREAD_UUID,
            "preview",
            &["--request".to_string(), preview_path.display().to_string()],
        ))
        .await,
        "wrong CLI thread",
        "thread_uuid_mismatch",
    );

    // 2. Foreign binding/thread inside JSON request is rejected with scope_mismatch and redacts secrets.
    let mut foreign_req = base_req.clone();
    foreign_req.binding_id = "bnd_foreign_exec_scope".to_string();
    foreign_req.job_id = SECRET_SENTINEL.to_string();
    let foreign_path = fixture.write_request("foreign-scope.json", &foreign_req);
    let foreign_out = run_flow(&fixture.exec_args(
        "record",
        &[
            "--request".to_string(),
            foreign_path.display().to_string(),
            "--operator-approved".to_string(),
            "--confirm-approval".to_string(),
            preview.approval_subject_digest.clone(),
            "--confirm-envelope".to_string(),
            preview.envelope_digest.clone(),
        ],
    ))
    .await;
    expect_failure_code(&foreign_out, "foreign binding in record", "scope_mismatch");
    assert_no_secret_echo(&foreign_out, "foreign binding in record");

    // 3. Self-attested JSON without CLI flags, or missing any one of `--operator-approved`,
    //    `--confirm-approval`, or `--confirm-envelope`, must be rejected.
    let mut self_attested_req = base_req.clone();
    self_attested_req.operator_attested = true;
    self_attested_req.confirm_approval_subject_digest =
        Some(preview.approval_subject_digest.clone());
    self_attested_req.confirm_envelope_digest = Some(preview.envelope_digest.clone());
    let self_attested_path = fixture.write_request("self-attested.json", &self_attested_req);

    expect_failure_code(
        &run_flow(&fixture.exec_args(
            "record",
            &[
                "--request".to_string(),
                self_attested_path.display().to_string(),
            ],
        ))
        .await,
        "JSON self-attestation without CLI flags",
        "unauthorized_operator",
    );
    expect_failure_code(
        &run_flow(&fixture.exec_args(
            "record",
            &[
                "--request".to_string(),
                self_attested_path.display().to_string(),
                "--confirm-approval".to_string(),
                preview.approval_subject_digest.clone(),
                "--confirm-envelope".to_string(),
                preview.envelope_digest.clone(),
            ],
        ))
        .await,
        "missing --operator-approved",
        "unauthorized_operator",
    );
    expect_failure_code(
        &run_flow(&fixture.exec_args(
            "record",
            &[
                "--request".to_string(),
                self_attested_path.display().to_string(),
                "--operator-approved".to_string(),
                "--confirm-envelope".to_string(),
                preview.envelope_digest.clone(),
            ],
        ))
        .await,
        "missing --confirm-approval",
        "unauthorized_operator",
    );
    expect_failure_code(
        &run_flow(&fixture.exec_args(
            "record",
            &[
                "--request".to_string(),
                self_attested_path.display().to_string(),
                "--operator-approved".to_string(),
                "--confirm-approval".to_string(),
                preview.approval_subject_digest.clone(),
            ],
        ))
        .await,
        "missing --confirm-envelope",
        "unauthorized_operator",
    );

    // 4. Malformed or mismatched digests are rejected.
    expect_failure_code(
        &run_flow(&fixture.exec_args(
            "record",
            &[
                "--request".to_string(),
                preview_path.display().to_string(),
                "--operator-approved".to_string(),
                "--confirm-approval".to_string(),
                "not-a-sha256".to_string(),
                "--confirm-envelope".to_string(),
                preview.envelope_digest.clone(),
            ],
        ))
        .await,
        "malformed --confirm-approval",
        "invalid_input",
    );
    expect_failure_code(
        &run_flow(&fixture.exec_args(
            "record",
            &[
                "--request".to_string(),
                preview_path.display().to_string(),
                "--operator-approved".to_string(),
                "--confirm-approval".to_string(),
                WRONG_DIGEST.to_string(),
                "--confirm-envelope".to_string(),
                preview.envelope_digest.clone(),
            ],
        ))
        .await,
        "wrong --confirm-approval digest",
        "unauthorized_operator",
    );
    expect_failure_code(
        &run_flow(&fixture.exec_args(
            "record",
            &[
                "--request".to_string(),
                preview_path.display().to_string(),
                "--operator-approved".to_string(),
                "--confirm-approval".to_string(),
                preview.approval_subject_digest.clone(),
                "--confirm-envelope".to_string(),
                WRONG_DIGEST.to_string(),
            ],
        ))
        .await,
        "wrong --confirm-envelope digest",
        "unauthorized_operator",
    );

    for entry in directory_entries(&fixture.plans_dir) {
        assert!(
            !entry.starts_with("coworker-controller-"),
            "rejected record attempts must not create controller policy ledger: found {entry}"
        );
    }
    assert!(!fixture.worker_marker.exists());
    assert!(!fixture.check_marker.exists());

    fixture.shutdown().await;
}

#[tokio::test]
async fn coworker_execution_preview_record_and_inspect_idempotent_lifecycle() {
    let mut fixture = ExecutionCliFixture::start().await;
    let req = fixture.sample_envelope_request();
    let req_path = fixture.write_request("lifecycle-req.json", &req);

    let preview: ControllerConfigurationPreview = parse_envelope(
        &run_flow(&fixture.exec_args(
            "preview",
            &["--request".to_string(), req_path.display().to_string()],
        ))
        .await,
        "preview lifecycle",
        &fixture.binding_id,
    );

    let record_args = fixture.exec_args(
        "record",
        &[
            "--request".to_string(),
            req_path.display().to_string(),
            "--confirm-approval".to_string(),
            preview.approval_subject_digest.clone(),
            "--confirm-envelope".to_string(),
            preview.envelope_digest.clone(),
            "--operator-approved".to_string(),
        ],
    );
    let recorded: ControllerJobRuntimeState = parse_envelope(
        &run_flow(&record_args).await,
        "record lifecycle",
        &fixture.binding_id,
    );
    assert_eq!(recorded.job_id, fixture.job.job_id);
    assert_eq!(recorded.runtime_revision, 1);
    assert!(recorded.envelope.operator_attested);
    assert!(!recorded.envelope.native_per_call_identity_verified);
    assert_eq!(recorded.envelope.envelope_id, preview.envelope_id);
    assert_eq!(recorded.envelope.envelope_digest, preview.envelope_digest);
    assert_eq!(
        recorded.envelope.approval_subject_digest,
        preview.approval_subject_digest
    );

    // Repeating the identical record command is idempotent and preserves runtime_revision == 1.
    let recorded_again: ControllerJobRuntimeState = parse_envelope(
        &run_flow(&record_args).await,
        "record idempotent repeat",
        &fixture.binding_id,
    );
    assert_eq!(recorded_again, recorded);

    let journal_path = fixture.plans_dir.join("observation-journal.jsonl");
    let journal_before_inspect =
        fs::read_to_string(&journal_path).expect("observation journal after record");
    assert!(journal_before_inspect.contains("flow.coworker-execution.record"));

    let parent_envs = [("THREADLANE_PARENT_REQUEST_ID", "obsreq_parent_inspect")];
    let inspected: ControllerJobRuntimeState = parse_envelope(
        &run_flow_with_env(
            &fixture.exec_args(
                "inspect",
                &["--job-id".to_string(), fixture.job.job_id.clone()],
            ),
            &parent_envs,
        )
        .await,
        "inspect recorded envelope",
        &fixture.binding_id,
    );
    assert_eq!(inspected, recorded);
    assert_eq!(
        fs::read_to_string(&journal_path).expect("journal after inspect"),
        journal_before_inspect,
        "inspect must not append observation journal entries"
    );

    // Attempting to record a different valid envelope on the same job fails closed.
    let mut different_req = fixture.sample_envelope_request();
    different_req.max_total_corrections_per_job = 2;
    let different_path = fixture.write_request("different-req.json", &different_req);
    let different_preview: ControllerConfigurationPreview = parse_envelope(
        &run_flow(&fixture.exec_args(
            "preview",
            &[
                "--request".to_string(),
                different_path.display().to_string(),
            ],
        ))
        .await,
        "preview different envelope",
        &fixture.binding_id,
    );
    expect_failure_code(
        &run_flow(&fixture.exec_args(
            "record",
            &[
                "--request".to_string(),
                different_path.display().to_string(),
                "--confirm-approval".to_string(),
                different_preview.approval_subject_digest.clone(),
                "--confirm-envelope".to_string(),
                different_preview.envelope_digest.clone(),
                "--operator-approved".to_string(),
            ],
        ))
        .await,
        "re-record different envelope on same job",
        "validation_failed",
    );

    let inspected_after: ControllerJobRuntimeState = parse_envelope(
        &run_flow(&fixture.exec_args(
            "inspect",
            &["--job-id".to_string(), fixture.job.job_id.clone()],
        ))
        .await,
        "inspect after rejected re-record",
        &fixture.binding_id,
    );
    assert_eq!(inspected_after, recorded);
    assert!(!fixture.worker_marker.exists());
    assert!(!fixture.check_marker.exists());

    fixture.shutdown().await;
}

#[tokio::test]
async fn coworker_execution_rejects_tampered_checks_caps_models_native_routes_and_stale_revisions()
{
    let mut fixture = ExecutionCliFixture::start().await;
    let base_req = fixture.sample_envelope_request();
    let base_path = fixture.write_request("base-preview.json", &base_req);
    let preview: ControllerConfigurationPreview = parse_envelope(
        &run_flow(&fixture.exec_args(
            "preview",
            &["--request".to_string(), base_path.display().to_string()],
        ))
        .await,
        "preview before tampering tests",
        &fixture.binding_id,
    );
    let approvals_bytes_before =
        fs::read(fixture.authority_dir.join("approvals.json")).expect("read approvals.json");

    // 1. Tampered check command argv after preview is rejected by full envelope_digest verification.
    let mut tampered_argv_req = base_req.clone();
    tampered_argv_req
        .check_commands
        .get_mut("check-test")
        .expect("check-test spec")
        .argv = vec!["test".to_string(), "--all-targets".to_string()];
    let tampered_argv_path = fixture.write_request("tampered-argv.json", &tampered_argv_req);
    expect_failure_code(
        &run_flow(&fixture.exec_args(
            "record",
            &[
                "--request".to_string(),
                tampered_argv_path.display().to_string(),
                "--confirm-approval".to_string(),
                preview.approval_subject_digest.clone(),
                "--confirm-envelope".to_string(),
                preview.envelope_digest.clone(),
                "--operator-approved".to_string(),
            ],
        ))
        .await,
        "tampered check argv with old envelope digest",
        "unauthorized_operator",
    );

    // 2. Shell evaluation flag `-c` in check argv is rejected as a security violation.
    let mut shell_flag_req = base_req.clone();
    shell_flag_req
        .check_commands
        .get_mut("check-fmt")
        .expect("check-fmt spec")
        .argv = vec!["-c".to_string(), "echo pwned".to_string()];
    let shell_flag_path = fixture.write_request("shell-flag.json", &shell_flag_req);
    expect_failure_code(
        &run_flow(&fixture.exec_args(
            "preview",
            &[
                "--request".to_string(),
                shell_flag_path.display().to_string(),
            ],
        ))
        .await,
        "shell evaluation flag in check argv",
        "security_violation",
    );

    // 3. Unauthorized check_id not in capability envelope's allowed_check_ids is rejected.
    let mut unapproved_check_req = base_req.clone();
    let fmt_spec = unapproved_check_req
        .check_commands
        .remove("check-fmt")
        .expect("remove check-fmt");
    unapproved_check_req.check_commands.insert(
        "check-unapproved".to_string(),
        FrozenCheckCommandSpec {
            check_id: "check-unapproved".to_string(),
            ..fmt_spec
        },
    );
    let unapproved_check_path =
        fixture.write_request("unapproved-check.json", &unapproved_check_req);
    expect_failure_code(
        &run_flow(&fixture.exec_args(
            "preview",
            &[
                "--request".to_string(),
                unapproved_check_path.display().to_string(),
            ],
        ))
        .await,
        "unapproved check_id",
        "unauthorized_operator",
    );

    // 4. Per-task correction cap exceeding capability_envelope.max_in_scope_correction_attempts is rejected.
    let mut excess_caps_req = base_req.clone();
    excess_caps_req.max_in_scope_corrections_per_task = 3;
    let excess_caps_path = fixture.write_request("excess-caps.json", &excess_caps_req);
    expect_failure_code(
        &run_flow(&fixture.exec_args(
            "preview",
            &[
                "--request".to_string(),
                excess_caps_path.display().to_string(),
            ],
        ))
        .await,
        "excess max_in_scope_corrections_per_task",
        "validation_failed",
    );

    // 5. Unsupported native route (CodexCli / NativeHandoff) or unapproved model for Reviewer/Closure is rejected.
    let mut native_role_req = base_req.clone();
    native_role_req.reviewer_choice.adapter = ToolAdapter::CodexCli;
    native_role_req.reviewer_choice.route = RoleExecutionRoute::NativeHandoff;
    let native_role_path = fixture.write_request("native-role.json", &native_role_req);
    expect_failure_code(
        &run_flow(&fixture.exec_args(
            "preview",
            &[
                "--request".to_string(),
                native_role_path.display().to_string(),
            ],
        ))
        .await,
        "native route for reviewer_choice",
        "validation_failed",
    );

    let mut unapproved_model_req = base_req.clone();
    unapproved_model_req.closure_choice.model_id = ALT_MODEL_ID.to_string();
    unapproved_model_req.closure_choice.model_arg = "gemini-4-secondary".to_string();
    let unapproved_model_path =
        fixture.write_request("unapproved-model.json", &unapproved_model_req);
    expect_failure_code(
        &run_flow(&fixture.exec_args(
            "preview",
            &[
                "--request".to_string(),
                unapproved_model_path.display().to_string(),
            ],
        ))
        .await,
        "unapproved closure_choice model",
        "validation_failed",
    );

    // 6. Stale expected_job_revision is rejected.
    let mut stale_job_req = base_req.clone();
    stale_job_req.expected_job_revision = fixture.job.job_revision + 1;
    let stale_job_path = fixture.write_request("stale-job.json", &stale_job_req);
    expect_failure_code(
        &run_flow(&fixture.exec_args(
            "record",
            &[
                "--request".to_string(),
                stale_job_path.display().to_string(),
                "--confirm-approval".to_string(),
                preview.approval_subject_digest.clone(),
                "--confirm-envelope".to_string(),
                preview.envelope_digest.clone(),
                "--operator-approved".to_string(),
            ],
        ))
        .await,
        "stale expected_job_revision",
        "validation_failed",
    );

    // 7. Drifted/disabled tool in live RegistryStore is rejected on record even with valid preview digests.
    let registry = RegistryStore::open(&fixture.registry_dir).expect("open registry store");
    registry
        .apply(
            3,
            RegistryChange::UpsertTool {
                tool: ToolDefinition {
                    id: TOOL_ID.to_string(),
                    label: "Disabled tool".to_string(),
                    adapter: ToolAdapter::Antigravity,
                    executable_path: Some(fixture.fake_check_bin.clone()),
                    enabled: false,
                },
            },
        )
        .expect("disable tool in registry");
    expect_failure_code(
        &run_flow(&fixture.exec_args(
            "record",
            &[
                "--request".to_string(),
                base_path.display().to_string(),
                "--confirm-approval".to_string(),
                preview.approval_subject_digest.clone(),
                "--confirm-envelope".to_string(),
                preview.envelope_digest.clone(),
                "--operator-approved".to_string(),
            ],
        ))
        .await,
        "drifted/disabled registry on record",
        "validation_failed",
    );

    assert_eq!(
        fs::read(fixture.authority_dir.join("approvals.json")).expect("read approvals.json"),
        approvals_bytes_before,
        "failed record attempts must preserve existing ApprovalRecord untouched"
    );
    expect_failure_code(
        &run_flow(&fixture.exec_args(
            "inspect",
            &["--job-id".to_string(), fixture.job.job_id.clone()],
        ))
        .await,
        "inspect after all rejected record attempts",
        "not_configured",
    );
    assert!(!fixture.worker_marker.exists());
    assert!(!fixture.check_marker.exists());

    fixture.shutdown().await;
}
