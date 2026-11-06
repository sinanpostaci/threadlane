#![cfg(unix)]
//! CLI Activity correlation uses verified immutable scope even when owner execution fails.

use std::collections::BTreeMap;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Output;
use std::sync::Arc;
use std::time::Duration;

use serde_json::Value;
use tempfile::TempDir;
use threadlane::{Supervisor, SupervisorClient, SupervisorConfig, SupervisorHandle};
use threadlane_authority::{AuthorityStore, OperatorApprovalIntent};
use threadlane_core::observation::{
    ObservationActor, ObservationCallState, ObservationJournalRecord, ObservationRecordPhase,
};
use threadlane_native::coworker_workflow::{
    BindJobApprovalRequest, CanonicalProjectBaseRef, ControllerPolicyStore,
    ControllerRoleExecutionBudget, CoworkerJobRecord, CoworkerWorkflowStore,
    CreateCoworkerJobRequest, FreezeControllerExecutionEnvelopeRequest, FrozenCheckCommandSpec,
    FrozenCheckKind, FrozenControllerCapabilityEnvelope, WorkBrief, WorkBriefKind,
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

const THREAD_UUID: &str = "0196ed00-0000-7000-8000-0000000000b1";
const OTHER_THREAD_UUID: &str = "0196ed00-0000-7000-8000-0000000000b2";
const BINDING_LABEL: &str = "owner-activity-fixture";
const PROFILE_ID: &str = "profile-exec-cli";
const PLAN_ID: &str = "plan_exec_cli_1";
const TASK_ID: &str = "task-1";
const TOOL_ID: &str = "tool-agy";
const MODEL_ID: &str = "model-argon";
const ALT_MODEL_ID: &str = "model-unapproved";
const WRONG_DIGEST: &str = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";

struct ActivityFixture {
    _tmp: TempDir,
    repo_dir: PathBuf,
    worker_marker: PathBuf,
    check_marker: PathBuf,
    fake_check_bin: PathBuf,
    state_dir: PathBuf,
    registry_dir: PathBuf,
    plans_dir: PathBuf,
    authority_dir: PathBuf,
    sessions_dir: PathBuf,
    supervisor: Option<SupervisorHandle>,
    binding_id: String,
    approval: ApprovalSummary,
    job: CoworkerJobRecord,
}

impl ActivityFixture {
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
        for dir in [
            &state_dir,
            &registry_dir,
            &plans_dir,
            &authority_dir,
            &sessions_dir,
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
                        allow_check_failure_corrections: false,
                    }),
                    task_interfaces: BTreeMap::new(),
                },
                &authority,
            )
            .expect("bind job approval");

        Self {
            _tmp: tmp,
            repo_dir,
            worker_marker,
            check_marker,
            fake_check_bin,
            state_dir,
            registry_dir,
            plans_dir,
            authority_dir,
            sessions_dir,
            supervisor: Some(supervisor),
            binding_id: binding.binding_id,
            approval,
            job,
        }
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

impl ActivityFixture {
    fn freeze(&self) {
        let registry = Arc::new(RegistryStore::open(&self.registry_dir).expect("registry"));
        let plans = Arc::new(PlanStore::open(&self.plans_dir).expect("plans"));
        let authority =
            AuthorityStore::open(&self.authority_dir, registry, plans).expect("authority");
        let store = ControllerPolicyStore::new(&self.plans_dir).expect("policy store");
        let mut request = self.sample_envelope_request();
        let preview = store
            .preview_execution_envelope(&request, &self.job, &authority)
            .expect("preview");
        request.operator_attested = true;
        request.confirm_approval_subject_digest = Some(preview.approval_subject_digest);
        request.confirm_envelope_digest = Some(preview.envelope_digest);
        store
            .record_execution_envelope(&request, &self.job, &authority)
            .expect("freeze fixture envelope");
    }

    async fn invoke(&self, command: &[&str]) -> Output {
        let mut process = tokio::process::Command::new(env!("CARGO_BIN_EXE_threadlane-flow"));
        for (flag, dir) in [
            ("--state-dir", &self.state_dir),
            ("--registry-dir", &self.registry_dir),
            ("--plans-dir", &self.plans_dir),
            ("--authority-dir", &self.authority_dir),
            ("--sessions-dir", &self.sessions_dir),
        ] {
            process.arg(flag).arg(dir);
        }
        process
            .arg("--binding")
            .arg(&self.binding_id)
            .arg("--thread")
            .arg(THREAD_UUID)
            .arg("code-driver")
            .args(command)
            .env_remove("THREADLANE_OBSERVATION_PARENT_REQUEST_ID")
            .env(
                "THREADLANE_OBSERVATION_PLAN_ID",
                "plan_unverified_environment",
            )
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_CONFIG_SYSTEM", "/dev/null")
            .env("GIT_CONFIG_GLOBAL", "/dev/null");
        tokio::time::timeout(Duration::from_secs(30), process.output())
            .await
            .expect("bounded CLI exit")
            .expect("run fixture CLI")
    }

    fn records(&self) -> Vec<ObservationJournalRecord> {
        fs::read_to_string(self.plans_dir.join("observation-journal.jsonl"))
            .expect("journal")
            .lines()
            .map(|line| serde_json::from_str(line).expect("journal record"))
            .collect()
    }

    fn assert_correlation(
        &self,
        records: &[ObservationJournalRecord],
        final_state: ObservationCallState,
    ) {
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].phase, ObservationRecordPhase::Entry);
        assert_eq!(records[0].state, ObservationCallState::Running);
        assert_eq!(records[1].phase, ObservationRecordPhase::Finish);
        assert_eq!(records[1].state, final_state);
        assert_eq!(records[0].request_id, records[1].request_id);
        for record in records {
            assert_eq!(record.binding_id.as_deref(), Some(self.binding_id.as_str()));
            assert_eq!(record.thread_uuid.as_deref(), Some(THREAD_UUID));
            assert_eq!(record.job_id.as_deref(), Some(self.job.job_id.as_str()));
            assert_eq!(record.plan_id.as_deref(), Some(PLAN_ID));
            assert_eq!(
                record.approval_id.as_deref(),
                Some(self.approval.approval_id.as_str())
            );
            assert_eq!(record.project_path.as_deref(), self.repo_dir.to_str());
            assert_eq!(record.project_label.as_deref(), Some("repo"));
            assert_eq!(
                record.actor,
                Some(ObservationActor::unknown(Some(
                    "trusted_local_operator".into()
                )))
            );
            assert!(
                record.executor.is_none(),
                "controller code must not masquerade as model execution"
            );
            assert!(record.usage.is_none());
            assert!(
                record.task_id.is_none(),
                "owner operates on a job, not an invented task"
            );
        }
        assert!(
            !self.worker_marker.exists(),
            "correlation must not call a provider"
        );
        assert!(
            !self.check_marker.exists(),
            "correlation must not run a check"
        );
    }
}

fn assert_unattributed(records: &[ObservationJournalRecord]) {
    assert_eq!(records.len(), 2);
    assert_eq!(records[1].state, ObservationCallState::Failed);
    for record in records {
        assert!(record.project_path.is_none());
        assert!(record.project_label.is_none());
        assert!(record.job_id.is_none());
        assert!(record.plan_id.is_none());
        assert!(record.approval_id.is_none());
        assert!(record.executor.is_none());
    }
}

#[tokio::test]
async fn owner_error_retains_verified_project_job_plan_and_approval_without_fake_executor() {
    let mut fixture = ActivityFixture::start().await;
    // A manual job has real immutable authority, but no Orchestrator provenance: execution
    // must still fail admission. Activity must retain its verified identity across this Err.
    let output = fixture
        .invoke(&[
            "run-owner",
            "--job-id",
            &fixture.job.job_id,
            "--max-steps",
            "1",
        ])
        .await;
    assert!(!output.status.success());
    let failure: Value = serde_json::from_slice(&output.stderr).expect("CLI error");
    assert_eq!(failure["code"], "validation_failed");
    assert!(output.stdout.is_empty());
    let records = fixture.records();
    fixture.assert_correlation(&records, ObservationCallState::Failed);
    assert_eq!(records[1].operation, "flow.code-driver.run-owner");
    assert!(records[1]
        .summary
        .as_deref()
        .unwrap()
        .contains("validation_failed"));
    assert_eq!(
        fs::read_dir(&fixture.sessions_dir)
            .expect("sessions")
            .count(),
        0
    );
    fixture.shutdown().await;
}

#[tokio::test]
async fn successful_cancel_keeps_verified_correlation_and_status_remains_read_only() {
    let mut fixture = ActivityFixture::start().await;
    fixture.freeze();
    let status = fixture
        .invoke(&["status", "--job-id", &fixture.job.job_id])
        .await;
    assert!(
        status.status.success(),
        "{}",
        String::from_utf8_lossy(&status.stderr)
    );
    assert!(!fixture.plans_dir.join("observation-journal.jsonl").exists());
    assert_eq!(
        fs::read_dir(&fixture.sessions_dir)
            .expect("sessions")
            .count(),
        0
    );
    let cancelled = fixture
        .invoke(&["cancel", "--job-id", &fixture.job.job_id])
        .await;
    assert!(
        cancelled.status.success(),
        "{}",
        String::from_utf8_lossy(&cancelled.stderr)
    );
    let records = fixture.records();
    fixture.assert_correlation(&records, ObservationCallState::Completed);
    assert_eq!(records[1].operation, "flow.code-driver.cancel");
    let before = fs::read(fixture.plans_dir.join("observation-journal.jsonl")).unwrap();
    assert!(fixture
        .invoke(&["status", "--job-id", &fixture.job.job_id])
        .await
        .status
        .success());
    assert_eq!(
        fs::read(fixture.plans_dir.join("observation-journal.jsonl")).unwrap(),
        before
    );
    fixture.shutdown().await;
}

#[tokio::test]
async fn unknown_job_does_not_gain_identity_from_cli_or_environment() {
    let mut fixture = ActivityFixture::start().await;
    let output = fixture
        .invoke(&[
            "run-owner",
            "--job-id",
            "job_11111111111111111111111111111111",
            "--max-steps",
            "1",
        ])
        .await;
    assert!(!output.status.success());
    assert_unattributed(&fixture.records());
    fixture.shutdown().await;
}

#[tokio::test]
async fn request_for_other_thread_is_unattributed_and_does_not_start_owner() {
    let mut fixture = ActivityFixture::start().await;
    let request = fixture.state_dir.join("request.json");
    fs::write(
        &request,
        serde_json::to_vec(&serde_json::json!({
            "schema_version": 1, "binding_id": fixture.binding_id,
            "thread_uuid": OTHER_THREAD_UUID, "job_id": fixture.job.job_id
        }))
        .unwrap(),
    )
    .unwrap();
    fs::set_permissions(&request, fs::Permissions::from_mode(0o600)).unwrap();
    let output = fixture
        .invoke(&["run-owner", "--request", request.to_str().unwrap()])
        .await;
    assert!(!output.status.success());
    let failure: Value = serde_json::from_slice(&output.stderr).expect("CLI error");
    assert_eq!(failure["code"], "scope_mismatch");
    assert_unattributed(&fixture.records());
    assert!(!fixture.plans_dir.join("coworker-code-driver").exists());
    fixture.shutdown().await;
}

#[tokio::test]
async fn altered_frozen_approval_digest_cannot_supply_attribution() {
    let mut fixture = ActivityFixture::start().await;
    let ledger_path = fs::read_dir(&fixture.plans_dir)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|path| {
            path.file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with("coworker-workflow-")
                && path.extension().is_some_and(|ext| ext == "json")
        })
        .expect("workflow ledger");
    let mut ledger: Value = serde_json::from_slice(&fs::read(&ledger_path).unwrap()).unwrap();
    ledger["jobs"][0]["approval_linkage"]["approval_subject_digest"] =
        Value::String(WRONG_DIGEST.into());
    fs::write(&ledger_path, serde_json::to_vec(&ledger).unwrap()).unwrap();
    let output = fixture
        .invoke(&[
            "run-owner",
            "--job-id",
            &fixture.job.job_id,
            "--max-steps",
            "1",
        ])
        .await;
    assert!(!output.status.success());
    assert_unattributed(&fixture.records());
    assert!(!fixture.worker_marker.exists());
    fixture.shutdown().await;
}
