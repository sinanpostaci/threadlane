#![cfg(unix)]
#![allow(unused_imports, dead_code)]
//! Disposable fake-provider acceptance of admission CLI and separate-process owner launch.
//! Shared fixture setup follows the existing pipeline harness; this is not live-provider acceptance.
//! Real behavioral integration tests for [`CoworkerPipelineService`], [`CoworkerPipelineDriver`],
//! and [`OwnedCoworkerPipelineSession`].
//!
//! Exercises the automatic Coworker workflow pipeline end-to-end over a disposable Git repository,
//! real [`CoworkerPlanningService::orchestrate_brief_to_plan`] provenance, exact human approval,
//! frozen [`ControllerExecutionEnvelope`], live [`ApprovedHost`], real frozen check subprocesses,
//! strict 9-field `Reviewer` invocations (`tests/support/fake-coworker-roles.mjs`), original
//! per-task `RunIntegration`, downstream dependency input seal preparation, dependent `Coder`
//! execution verifying predecessor files, and final job aggregation + combined checks + single
//! `Closure`.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tempfile::TempDir;
use threadlane_authority::{AuthorityStore, OperatorApprovalRequest};
use threadlane_host::{
    ApprovedHost, ApprovedHostClient, ApprovedHostConfig, HostPhase, TaskExecutionStatus,
};
use threadlane_native::coworker_workflow::{
    derive_final_job_integration_identifiers, derive_job_aggregation_receipt_id,
    read_persisted_derived_task_execution_report, sha256_hex, ApplyWorkflowHoldRequest,
    BindJobApprovalRequest, CanonicalProjectBaseRef, ControllerConfigurationPreview,
    ControllerJobRuntimeState, ControllerPolicyStore, ControllerRoleExecutionBudget,
    CoworkerControllerService, CoworkerJobCompletionStatus, CoworkerJobOwnershipStore,
    CoworkerJobPhase, CoworkerJobRecord, CoworkerPipelineInFlightStageKind,
    CoworkerPipelineService, CoworkerPipelineStatus, CoworkerPlanningService,
    CoworkerTaskCorrectionExecuteStatus, CoworkerTaskStage, CoworkerWorkflowStore,
    CreateCoworkerJobRequest, CreatePlanningGrantRequest, DeclaredIndependenceCompletenessReceipt,
    FreezeControllerExecutionEnvelopeRequest, FrozenCheckCommandSpec, FrozenCheckKind,
    FrozenControllerCapabilityEnvelope, OrchestrateWorkBriefPlanRequest, PendingControllerEffect,
    PendingControllerEffectKind, PendingControllerEffectStatus, PlannedCheckSuggestion,
    PlanningCallBudget, PlanningReadOnlyCapabilities, PlanningReservationStatus,
    ReviewVerdictOutcome, StructuredOrchestratorOutput, StructuredPlannedTask,
    TaskIndependenceCompletenessEntry, TaskInterfaceOwnershipSpec, WorkBrief, WorkBriefKind,
    WorkflowHoldReason,
};
use threadlane_native::{
    Difficulty, ExecutionPolicy, NativeErrorCode, PolicyAllowRule, RegistryStore,
    RoleExecutionRoute, SelectedRoleChoice, ToolAdapter, WorkflowProfile, WorkflowRole,
};
use threadlane_operations::{
    load_integration_receipt, ClosureVerdictKind, IntegrationReceipt, IntegrationStrategy,
};
use threadlane_workers::{
    resolve_exact_commit_sha, resolve_exact_tree_sha, run_sanitized_git, ExecutionBudgets,
    DEFAULT_GIT_COMMAND_TIMEOUT_MS,
};
use threadlane_workflow::{
    ModelChoice, ModelDefinition, PlanStore, PlanTask, RegistryChange, RoleAssignment,
    ScopedWorkflow, ToolDefinition,
};

const THREAD_UUID_A: &str = "550e8400-e29b-41d4-a716-446655440000";
const BINDING_A: &str = "bnd_coworker_pipe_alpha";

#[allow(dead_code)]
struct PipelineTestFixture {
    _tmp: TempDir,
    root: PathBuf,
    repo_dir: PathBuf,
    base_commit: String,
    base_tree_sha: String,
    registry_dir: PathBuf,
    plans_dir: PathBuf,
    authority_dir: PathBuf,
    sessions_dir: PathBuf,
    ownership_dir: PathBuf,
    planning_artifacts_dir: PathBuf,
    fake_agy_exe: PathBuf,
    fake_cargo_exe: PathBuf,
    agy_count_file: PathBuf,
    agy_planning_count_file: PathBuf,
    agy_coder_count_file: PathBuf,
    agy_reviewer_count_file: PathBuf,
    agy_closure_count_file: PathBuf,
    reviewer_invocations_file: PathBuf,
    check_count_file: PathBuf,
    agy_plan_response_file: PathBuf,
    registry: Arc<RegistryStore>,
    plans: Arc<PlanStore>,
    scoped_workflow: ScopedWorkflow,
    authority: Arc<AuthorityStore>,
    workflow_store: CoworkerWorkflowStore,
    planning_service: CoworkerPlanningService,
    policy_store: ControllerPolicyStore,
    ownership_store: CoworkerJobOwnershipStore,
    controller: CoworkerControllerService,
    pipeline: CoworkerPipelineService,
}

fn init_git_repo(repo_dir: &Path) -> (String, String) {
    fs::create_dir_all(repo_dir.join("src")).expect("create repo src");
    fs::write(repo_dir.join("src/a.rs"), "pub fn a() -> u32 { 1 }\n").expect("write src/a.rs");
    fs::write(repo_dir.join("src/b.rs"), "pub fn b() -> u32 { 2 }\n").expect("write src/b.rs");
    fs::write(repo_dir.join("src/c.rs"), "pub fn c() -> u32 { 3 }\n").expect("write src/c.rs");

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

fn write_executable_script(path: &Path, content: &str) {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).expect("create script parent");
    }
    let mut f = fs::File::create(path).expect("create script");
    f.write_all(content.as_bytes()).expect("write script");
    f.sync_all().expect("sync script");
    fs::set_permissions(path, fs::Permissions::from_mode(0o755)).expect("chmod 0755 script");
}

fn read_invocation_count(path: &Path) -> u32 {
    match fs::read_to_string(path) {
        Ok(s) => s.trim().parse::<u32>().unwrap_or(0),
        Err(_) => 0,
    }
}

fn format_fixture_counter_diagnostics(fixture: &PipelineTestFixture) -> String {
    let entered_a = fixture.root.join("reviewer_entered_task-root-a.marker");
    let exited_a = fixture.root.join("reviewer_exited_task-root-a.marker");
    let hold_gate_a = fixture.root.join("hold_reviewer_task-root-a.gate");
    let entered_a_raw = fs::read_to_string(&entered_a).unwrap_or_else(|_| "<absent>".to_string());
    let exited_a_raw = fs::read_to_string(&exited_a).unwrap_or_else(|_| "<absent>".to_string());
    format!(
        "counters={{agy_total={}, planning={}, coder={}, reviewer_total={}, reviewer_a={}, reviewer_b={}, reviewer_c={}, closure={}, checks={}}}, hold_gate_a_exists={}, entered_marker_a={entered_a_raw}, exited_marker_a={exited_a_raw}",
        read_invocation_count(&fixture.agy_count_file),
        read_invocation_count(&fixture.agy_planning_count_file),
        read_invocation_count(&fixture.agy_coder_count_file),
        read_invocation_count(&fixture.agy_reviewer_count_file),
        read_invocation_count(&fixture.root.join("reviewer_count_task-root-a.txt")),
        read_invocation_count(&fixture.root.join("reviewer_count_task-root-b.txt")),
        read_invocation_count(&fixture.root.join("reviewer_count_task-dep-c.txt")),
        read_invocation_count(&fixture.agy_closure_count_file),
        read_invocation_count(&fixture.check_count_file),
        hold_gate_a.is_file(),
    )
}

fn setup_fixture() -> PipelineTestFixture {
    let tmp = TempDir::new().expect("tempdir");
    let root = fs::canonicalize(tmp.path()).expect("canonicalize temp root");

    let repo_dir = root.join("repo");
    let (base_commit, base_tree_sha) = init_git_repo(&repo_dir);
    let repo_dir = fs::canonicalize(&repo_dir).expect("canonicalize repo_dir");

    let bin_dir = root.join("bin");
    let fake_agy_exe = bin_dir.join("fake-agy");
    let fake_cargo_exe = bin_dir.join("fake-cargo");
    let agy_count_file = root.join("agy_count.txt");
    let agy_planning_count_file = root.join("agy_planning_count.txt");
    let agy_coder_count_file = root.join("agy_coder_count.txt");
    let agy_reviewer_count_file = root.join("agy_reviewer_count.txt");
    let agy_closure_count_file = root.join("agy_closure_count.txt");
    let reviewer_invocations_file = root.join("reviewer_invocations.jsonl");
    let check_count_file = root.join("check_count.txt");
    let agy_plan_response_file = root.join("agy_plan_response.json");

    let role_script_path =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/support/fake-coworker-roles.mjs");
    assert!(
        role_script_path.is_file(),
        "fake-coworker-roles.mjs must exist at {}",
        role_script_path.display()
    );
    write_executable_script(
        &fake_agy_exe,
        &format!(
            "#!/bin/sh\nset -eu\nexec node \"{}\" \"{}\" \"$@\"\n",
            role_script_path.display(),
            root.display()
        ),
    );

    let check_lock_dir = root.join(".check_counter.lock");
    write_executable_script(
        &fake_cargo_exe,
        &format!(
            "#!/bin/sh\nset -eu\nwhile ! mkdir \"{lock}\" 2>/dev/null; do sleep 0.01; done\nc=0\nif [ -f \"{cnt}\" ]; then c=$(cat \"{cnt}\"); fi\nc=$((c + 1))\nprintf \"%s\\n\" \"$c\" > \"{cnt}\"\nrmdir \"{lock}\" 2>/dev/null || true\nprintf \"running 1 test\\ntest check ... ok\\n\"\nexit 0\n",
            lock = check_lock_dir.display(),
            cnt = check_count_file.display(),
        ),
    );

    let registry_dir = root.join("registry-store");
    let plans_dir = root.join("plans-store");
    let authority_dir = root.join("authority-store");
    let sessions_dir = root.join("sessions-store");
    let ownership_dir = root.join("ownership-store");
    let planning_artifacts_dir = root.join("planning-artifacts");
    for private_dir in [&sessions_dir, &ownership_dir, &planning_artifacts_dir] {
        fs::create_dir_all(private_dir).expect("create private fixture dir");
        fs::set_permissions(private_dir, fs::Permissions::from_mode(0o700))
            .expect("chmod 0700 private fixture dir");
    }

    let registry = Arc::new(RegistryStore::open(&registry_dir).expect("open registry"));
    let plans = Arc::new(PlanStore::open(&plans_dir).expect("open plans"));
    let scoped_workflow = ScopedWorkflow::new(
        Arc::clone(&registry),
        Arc::clone(&plans),
        BINDING_A.to_string(),
    )
    .expect("create scoped_workflow");

    registry
        .apply(
            0,
            RegistryChange::UpsertTool {
                tool: ToolDefinition {
                    id: "tool-agy".to_string(),
                    label: "Antigravity CLI".to_string(),
                    adapter: ToolAdapter::Antigravity,
                    executable_path: Some(fake_agy_exe.clone()),
                    enabled: true,
                },
            },
        )
        .expect("upsert tool-agy");

    registry
        .apply(
            1,
            RegistryChange::UpsertTool {
                tool: ToolDefinition {
                    id: "tool-codex".to_string(),
                    label: "Native Codex".to_string(),
                    adapter: ToolAdapter::CodexCli,
                    executable_path: None,
                    enabled: true,
                },
            },
        )
        .expect("upsert tool-codex");

    registry
        .apply(
            2,
            RegistryChange::UpsertModels {
                models: vec![
                    ModelDefinition {
                        id: "model-argon".to_string(),
                        tool_id: "tool-agy".to_string(),
                        label: "Gemini 4 Argon High".to_string(),
                        model_arg: "gemini-4-argon-high".to_string(),
                        efforts: vec!["high".to_string()],
                        allow_no_effort: false,
                    },
                    ModelDefinition {
                        id: "model-codex-native".to_string(),
                        tool_id: "tool-codex".to_string(),
                        label: "Native Codex".to_string(),
                        model_arg: "gpt-6-astra".to_string(),
                        efforts: vec!["high".to_string()],
                        allow_no_effort: true,
                    },
                ],
            },
        )
        .expect("upsert models");

    registry
        .apply(
            3,
            RegistryChange::UpsertProfile {
                profile: WorkflowProfile {
                    decision_advisor: None,
                    id: "profile-coworker".to_string(),
                    name: "Coworker Headless Profile".to_string(),
                    assignments: vec![
                        RoleAssignment {
                            role: WorkflowRole::Orchestrator,
                            difficulty: Some(Difficulty::Hard),
                            choice: ModelChoice {
                                model_id: "model-argon".to_string(),
                                effort: Some("high".to_string()),
                            },
                        },
                        RoleAssignment {
                            role: WorkflowRole::Architect,
                            difficulty: Some(Difficulty::Hard),
                            choice: ModelChoice {
                                model_id: "model-argon".to_string(),
                                effort: Some("high".to_string()),
                            },
                        },
                        RoleAssignment {
                            role: WorkflowRole::Coder,
                            difficulty: Some(Difficulty::Hard),
                            choice: ModelChoice {
                                model_id: "model-argon".to_string(),
                                effort: Some("high".to_string()),
                            },
                        },
                        RoleAssignment {
                            role: WorkflowRole::Reviewer,
                            difficulty: Some(Difficulty::Hard),
                            choice: ModelChoice {
                                model_id: "model-argon".to_string(),
                                effort: Some("high".to_string()),
                            },
                        },
                        RoleAssignment {
                            role: WorkflowRole::Closure,
                            difficulty: Some(Difficulty::Extreme),
                            choice: ModelChoice {
                                model_id: "model-argon".to_string(),
                                effort: Some("high".to_string()),
                            },
                        },
                        RoleAssignment {
                            role: WorkflowRole::Follower,
                            difficulty: None,
                            choice: ModelChoice {
                                model_id: "model-codex-native".to_string(),
                                effort: Some("high".to_string()),
                            },
                        },
                    ],
                },
            },
        )
        .expect("upsert profile");

    let authority = Arc::new(
        AuthorityStore::open(&authority_dir, Arc::clone(&registry), Arc::clone(&plans))
            .expect("open authority"),
    );
    let workflow_store = CoworkerWorkflowStore::new(&plans_dir).expect("open workflow_store");
    let planning_service = CoworkerPlanningService::new(
        workflow_store.clone(),
        &registry_dir,
        &planning_artifacts_dir,
    )
    .expect("create planning_service");
    let policy_store = ControllerPolicyStore::new(&plans_dir).expect("open policy_store");
    let ownership_store =
        CoworkerJobOwnershipStore::new(&ownership_dir).expect("open ownership_store");
    let controller = CoworkerControllerService::new(
        workflow_store.clone(),
        policy_store.clone(),
        ownership_store.clone(),
        Arc::clone(&authority),
        &sessions_dir,
    )
    .expect("create controller")
    .with_planning_artifacts_dir(&planning_artifacts_dir)
    .expect("configure controller planning_artifacts_dir");
    let pipeline = CoworkerPipelineService::new(controller.clone()).expect("create pipeline");

    PipelineTestFixture {
        _tmp: tmp,
        root,
        repo_dir,
        base_commit,
        base_tree_sha,
        registry_dir,
        plans_dir,
        authority_dir,
        sessions_dir,
        ownership_dir,
        planning_artifacts_dir,
        fake_agy_exe,
        fake_cargo_exe,
        agy_count_file,
        agy_planning_count_file,
        agy_coder_count_file,
        agy_reviewer_count_file,
        agy_closure_count_file,
        reviewer_invocations_file,
        check_count_file,
        agy_plan_response_file,
        registry,
        plans,
        scoped_workflow,
        authority,
        workflow_store,
        planning_service,
        policy_store,
        ownership_store,
        controller,
        pipeline,
    }
}

fn project_base(fixture: &PipelineTestFixture) -> CanonicalProjectBaseRef {
    CanonicalProjectBaseRef::resolve_and_validate(&fixture.repo_dir, &fixture.base_commit)
        .expect("resolve project_base")
}

fn headless_argon_choice(difficulty: Difficulty) -> SelectedRoleChoice {
    SelectedRoleChoice {
        tool_id: "tool-agy".to_string(),
        adapter: ToolAdapter::Antigravity,
        route: RoleExecutionRoute::HeadlessWorker,
        model_id: "model-argon".to_string(),
        model_arg: "gemini-4-argon-high".to_string(),
        effort: Some("high".to_string()),
        matched_difficulty: Some(difficulty),
    }
}

fn planning_only_policy() -> ExecutionPolicy {
    ExecutionPolicy {
        schema_version: 1,
        policy_id: "policy-planning-only".to_string(),
        allow_rules: vec![PolicyAllowRule {
            tool_id: "tool-agy".to_string(),
            model_id: "model-argon".to_string(),
            efforts: vec!["high".to_string()],
            allow_no_effort: false,
            roles: vec![WorkflowRole::Orchestrator, WorkflowRole::Architect],
        }],
    }
}

fn execution_policy() -> ExecutionPolicy {
    ExecutionPolicy {
        schema_version: 1,
        policy_id: "policy-coworker-exec".to_string(),
        allow_rules: vec![PolicyAllowRule {
            tool_id: "tool-agy".to_string(),
            model_id: "model-argon".to_string(),
            efforts: vec!["high".to_string()],
            allow_no_effort: false,
            roles: vec![
                WorkflowRole::Orchestrator,
                WorkflowRole::Architect,
                WorkflowRole::Coder,
                WorkflowRole::Reviewer,
                WorkflowRole::Closure,
            ],
        }],
    }
}

fn sample_brief(fixture: &PipelineTestFixture, brief_id: &str) -> WorkBrief {
    WorkBrief {
        brief_id: brief_id.to_string(),
        binding_id: BINDING_A.to_string(),
        thread_uuid: THREAD_UUID_A.to_string(),
        project_base: project_base(fixture),
        kind: WorkBriefKind::NewJob,
        summary: format!("Pipeline test brief {brief_id}"),
        confirmed_requirements: vec![
            "Dispatch ready roots up to max_parallel".to_string(),
            "Verify, review, integrate, seal downstream inputs, and complete job once".to_string(),
        ],
        proposed_additions: Vec::new(),
        assumptions: vec!["Linux local workspace".to_string()],
        open_questions: Vec::new(),
        accepted_architect_decisions: Vec::new(),
    }
}

fn default_check_commands(
    fixture: &PipelineTestFixture,
) -> BTreeMap<String, FrozenCheckCommandSpec> {
    let mut map = BTreeMap::new();
    map.insert(
        "cargo_test".to_string(),
        FrozenCheckCommandSpec {
            check_id: "cargo_test".to_string(),
            kind: FrozenCheckKind::Tests,
            executable: fixture.fake_cargo_exe.clone(),
            argv: vec!["test".to_string()],
            repo_relative_cwd: ".".to_string(),
            timeout_ms: 30_000,
            max_stdout_bytes: 65_536,
            max_stderr_bytes: 65_536,
            required_for_task_review: true,
            required_for_candidate_closure: true,
        },
    );
    map
}

fn build_independence_completeness_receipt(
    bound_job: &CoworkerJobRecord,
) -> DeclaredIndependenceCompletenessReceipt {
    let linkage = bound_job
        .approval_linkage
        .as_ref()
        .expect("bound approval linkage");
    let mut task_entries = BTreeMap::new();
    for task in &bound_job.tasks {
        let explicitly_no_interfaces =
            task.owned_interfaces.is_empty() && task.consumed_interfaces.is_empty();
        task_entries.insert(
            task.task_id.clone(),
            TaskIndependenceCompletenessEntry {
                task_id: task.task_id.clone(),
                task_digest: task.task_digest.clone(),
                owned_interfaces: task.owned_interfaces.clone(),
                consumed_interfaces: task.consumed_interfaces.clone(),
                explicitly_no_interfaces,
                semantic_independence_attested: true,
            },
        );
    }
    let mut receipt = DeclaredIndependenceCompletenessReceipt {
        receipt_id: format!("icr_{}", bound_job.job_id),
        job_id: bound_job.job_id.clone(),
        approval_id: linkage.approval_id.clone(),
        plan_digest: linkage.plan_digest.clone(),
        task_entries,
        hold_attestations: BTreeMap::new(),
        recorded_at_ms: 1_700_000_050_000,
        operator_attested: true,
        receipt_digest: String::new(),
    };
    receipt.receipt_digest = receipt
        .compute_digest()
        .expect("compute completeness receipt digest");
    receipt
}

use serde_json::{json, Value};
use threadlane::{Supervisor, SupervisorClient, SupervisorConfig};
use threadlane_native::coworker_admission::{
    verify_complete_admission, AdmissionConfirmation, AdmissionInspection,
    AdmissionInspectionState, AdmissionPersistenceBoundary, AdmissionPreview, AdmissionReceipt,
    AdmissionRequest, AdmissionStage, CoworkerAdmissionService,
};
use threadlane_native::{CoworkerExecutionGateway, StartExecutionParams};

fn admission_service(f: &PipelineTestFixture) -> CoworkerAdmissionService {
    CoworkerAdmissionService::new(&f.plans_dir, &f.authority_dir, &f.registry_dir).unwrap()
}

fn private_json<T: serde::Serialize>(f: &PipelineTestFixture, name: &str, value: &T) -> PathBuf {
    let path = f.root.join(name);
    fs::write(&path, serde_json::to_vec(value).unwrap()).unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
    path
}

fn snapshot(path: &Path) -> BTreeMap<PathBuf, (Vec<u8>, std::time::SystemTime)> {
    let mut map = BTreeMap::new();
    for entry in fs::read_dir(path).unwrap() {
        let p = entry.unwrap().path();
        if p.is_dir() {
            map.extend(snapshot(&p));
        } else if p.is_file() {
            map.insert(
                p.clone(),
                (
                    fs::read(&p).unwrap(),
                    fs::metadata(&p).unwrap().modified().unwrap(),
                ),
            );
        }
    }
    map
}

async fn cli_output(
    f: &PipelineTestFixture,
    state: &Path,
    binding: &str,
    args: &[&str],
) -> std::process::Output {
    let output = tokio::time::timeout(
        Duration::from_secs(30),
        tokio::process::Command::new(env!("CARGO_BIN_EXE_threadlane-flow"))
            .arg("--state-dir")
            .arg(state)
            .arg("--registry-dir")
            .arg(&f.registry_dir)
            .arg("--plans-dir")
            .arg(&f.plans_dir)
            .arg("--authority-dir")
            .arg(&f.authority_dir)
            .arg("--sessions-dir")
            .arg(&f.sessions_dir)
            .arg("--binding")
            .arg(binding)
            .arg("--thread")
            .arg(THREAD_UUID_A)
            .args(args)
            .kill_on_drop(true)
            .env(
                "THREADLANE_OBSERVATION_PARENT_REQUEST_ID",
                "obsreq_admission_parent",
            )
            .output(),
    )
    .await
    .expect("bounded CLI wait")
    .unwrap();
    assert!(output.stdout.len() <= 1_048_576);
    assert!(output.stderr.len() <= 4096);
    output
}

async fn cli(f: &PipelineTestFixture, state: &Path, binding: &str, args: &[&str]) -> Value {
    let output = cli_output(f, state, binding, args).await;
    assert!(
        output.status.success(),
        "{args:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let envelope: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(envelope["route"], "trusted_local_operator");
    assert_eq!(envelope["native_per_call_identity_verified"], false);
    assert_eq!(envelope["binding_id"], binding);
    envelope["data"].clone()
}

fn assert_no_execution(f: &PipelineTestFixture) {
    assert_eq!(read_invocation_count(&f.agy_coder_count_file), 0);
    assert_eq!(read_invocation_count(&f.agy_reviewer_count_file), 0);
    assert_eq!(read_invocation_count(&f.agy_closure_count_file), 0);
    assert_eq!(read_invocation_count(&f.check_count_file), 0);
    assert_eq!(fs::read_dir(&f.sessions_dir).unwrap().count(), 0);
}

fn three_tasks() -> Vec<PlanTask> {
    vec![
        (
            "task-root-a",
            "CTRL_TASK_A implement src/a.rs",
            "src/a.rs",
            vec![],
        ),
        (
            "task-root-b",
            "CTRL_TASK_B implement src/b.rs",
            "src/b.rs",
            vec![],
        ),
        (
            "task-dep-c",
            "CTRL_TASK_C implement src/c.rs after verifying predecessor files",
            "src/c.rs",
            vec!["task-root-a".to_string(), "task-root-b".to_string()],
        ),
    ]
    .into_iter()
    .map(|(id, instruction, path, depends_on)| PlanTask {
        id: id.into(),
        title: id.into(),
        instruction: instruction.into(),
        role: WorkflowRole::Coder,
        difficulty: Difficulty::Hard,
        depends_on,
        allowed_write_paths: vec![path.into()],
    })
    .collect()
}

#[allow(clippy::too_many_arguments)]
async fn prepare_admission(
    fixture: &PipelineTestFixture,
    binding_id: &str,
    thread_uuid: &str,
    brief_id: &str,
    plan_id: &str,
    tasks: Vec<PlanTask>,
    task_interfaces: BTreeMap<String, TaskInterfaceOwnershipSpec>,
    approved_write_paths: Vec<String>,
    max_parallel: usize,
    approval_budgets: ExecutionBudgets,
    coder_budget: ControllerRoleExecutionBudget,
    correction_cap: u32,
) -> (AdmissionRequest, String) {
    Box::pin(async move {
        let pb = project_base(fixture);
        let reg_snap = fixture.registry.snapshot().expect("registry snapshot");
        let scoped_workflow = ScopedWorkflow::new(
            Arc::clone(&fixture.registry),
            Arc::clone(&fixture.plans),
            binding_id.to_string(),
        )
        .expect("create scoped_workflow for binding");
        let driver_planning_artifacts_dir = threadlane_native::planning_driver::planning_artifacts_dir(&fixture.plans_dir);
        let planning_service = CoworkerPlanningService::new(
            fixture.workflow_store.clone(),
            &fixture.registry_dir,
            &driver_planning_artifacts_dir,
        )
        .expect("create planning_service at planning_artifacts_dir");

        let brief = WorkBrief {
            brief_id: brief_id.to_string(),
            binding_id: binding_id.to_string(),
            thread_uuid: thread_uuid.to_string(),
            project_base: pb.clone(),
            kind: WorkBriefKind::NewJob,
            summary: format!("Full code-driver pipeline test brief {brief_id}"),
            confirmed_requirements: vec![
                "Dispatch ready roots up to max_parallel".to_string(),
                "Verify, review, correct in scope, integrate, seal downstream inputs, and complete job once".to_string(),
            ],
            proposed_additions: Vec::new(),
            assumptions: vec!["Linux local workspace".to_string()],
            open_questions: Vec::new(),
            accepted_architect_decisions: Vec::new(),
        };

        let job = fixture
            .workflow_store
            .create_or_amend_job(&CreateCoworkerJobRequest {
                binding_id: binding_id.to_string(),
                thread_uuid: thread_uuid.to_string(),
                request_key: format!("req_job_{brief_id}"),
                expected_ledger_revision: None,
                work_brief: brief,
                amended_target_task_ids: Vec::new(),
                amended_write_paths: Vec::new(),
                amended_interfaces: Vec::new(),
            })
            .expect("create job");

        let grant = fixture
            .workflow_store
            .create_planning_grant(&CreatePlanningGrantRequest {
                binding_id: binding_id.to_string(),
                thread_uuid: thread_uuid.to_string(),
                request_key: format!("req_grant_{brief_id}"),
                expected_ledger_revision: None,
                project_base: pb,
                profile_id: "profile-coworker".to_string(),
                registry_revision: reg_snap.revision,
                policy: planning_only_policy(),
                capabilities: PlanningReadOnlyCapabilities::strict_read_only(),
                budget: PlanningCallBudget {
                    max_total_calls: 4,
                    max_orchestrator_calls: 2,
                    max_architect_calls: 2,
                    max_timeout_ms_per_call: 30_000,
                    max_output_bytes_per_call: 65_536,
                },
                created_at_ms: 1_000,
                expires_at_ms: 60_000,
                operator_attested: true,
            })
            .expect("create planning grant");

        let planned_tasks: Vec<StructuredPlannedTask> = tasks
            .iter()
            .map(|t| {
                let iface = task_interfaces
                    .get(&t.id)
                    .cloned()
                    .unwrap_or(TaskInterfaceOwnershipSpec {
                        owned_interfaces: Vec::new(),
                        consumed_interfaces: Vec::new(),
                    });
                StructuredPlannedTask {
                    task_id: t.id.clone(),
                    title: t.title.clone(),
                    description: t.instruction.clone(),
                    role: t.role,
                    difficulty: t.difficulty,
                    depends_on: t.depends_on.clone(),
                    allowed_write_paths: t.allowed_write_paths.clone(),
                    owned_interfaces: iface.owned_interfaces,
                    consumed_interfaces: iface.consumed_interfaces,
                    model_override: None,
                    tool_override: None,
                }
            })
            .collect();
        let structured_plan = StructuredOrchestratorOutput::Plan {
            schema_version: 1,
            summary: format!(
                "Orchestrator technical implementation plan `{plan_id}` for brief `{brief_id}` with verified read-only supervision and structured task DAG."
            ),
            tasks: planned_tasks,
            check_suggestions: vec![PlannedCheckSuggestion {
                check_id: "cargo_test".to_string(),
                command_summary: "cargo test --workspace".to_string(),
                required: true,
            }],
            unconfirmed_proposal_notes: Vec::new(),
            resolved_open_questions: Vec::new(),
            deferred_proposals: Vec::new(),
        };
        let plan_json = serde_json::to_string(&structured_plan)
            .expect("serialize StructuredOrchestratorOutput::Plan");
        fs::write(&fixture.agy_plan_response_file, &plan_json)
            .expect("write agy_plan_response_file");

        let orch_req = OrchestrateWorkBriefPlanRequest {
            binding_id: binding_id.to_string(),
            thread_uuid: thread_uuid.to_string(),
            grant_id: grant.grant_id.clone(),
            job_id: job.job_id.clone(),
            request_key: format!("req_orch_{brief_id}"),
            expected_ledger_revision: None,
            difficulty: Some(Difficulty::Hard),
            architect_difficulty: Some(Difficulty::Hard),
            plan_id: Some(plan_id.to_string()),
            expected_plan_revision: 0,
            architect_advice_ids: Vec::new(),
            max_architect_rounds: 0,
            current_time_ms: 2_000,
        };
        let plan_outcome = Box::pin(
            planning_service.orchestrate_brief_to_plan(&scoped_workflow, &orch_req),
        )
        .await
        .expect("orchestrate_brief_to_plan via real planning service");
        assert_eq!(
            plan_outcome.reservation.status,
            PlanningReservationStatus::Completed
        );
        let published_outcome = plan_outcome
            .require_published_plan()
            .expect("require published plan from Orchestrator");
        let _verified_artifact = planning_service
            .read_planning_artifact(&published_outcome.planning_result.report_artifact_id)
            .expect("verify persisted planning artifact on disk");

        let job = fixture
            .workflow_store
            .read_binding_ledger(binding_id)
            .expect("read ledger after orchestrate_brief_to_plan")
            .jobs
            .into_iter()
            .find(|j| j.job_id == job.job_id)
            .expect("find job after orchestrate_brief_to_plan");
        assert_eq!(job.phase, CoworkerJobPhase::AwaitingApproval);

        let approved_task_ids = tasks.iter().map(|t| t.id.clone()).collect();
        let approval_summary = fixture
            .authority
            .approve_at(
                OperatorApprovalRequest {
                    binding_id: binding_id.to_string(),
                    thread_uuid: thread_uuid.to_string(),
                    plan_id: published_outcome.planning_result.plan_id.clone(),
                    plan_revision: published_outcome.planning_result.plan_revision,
                    expected_registry_revision: reg_snap.revision,
                    expected_plan_digest: published_outcome.planning_result.plan_digest.clone(),
                    repository_path: fixture.repo_dir.clone(),
                    base_commit: fixture.base_commit.clone(),
                    approved_task_ids,
                    approved_write_paths,
                    max_parallel,
                    budgets: approval_budgets,
                },
                1_700_000_001_000,
            )
            .expect("approve plan");

        let cap_env = FrozenControllerCapabilityEnvelope {
            allowed_reviewer_choices: vec![headless_argon_choice(Difficulty::Hard)],
            allowed_closure_choices: vec![headless_argon_choice(Difficulty::Extreme)],
            max_in_scope_correction_attempts: correction_cap,
            allowed_check_ids: vec!["cargo_test".to_string()],
            local_git_integration_allowed: true,
            allow_check_failure_corrections: false,
        };

        let binding = BindJobApprovalRequest {
            binding_id: binding_id.into(), thread_uuid: thread_uuid.into(),
            job_id: job.job_id.clone(), expected_job_revision: job.job_revision,
            expected_ledger_revision: Some(fixture.workflow_store
                .read_binding_ledger(binding_id).unwrap().ledger_revision),
            approval_id: approval_summary.approval_id.clone(),
            policy: execution_policy(), controller_envelope: Some(cap_env), task_interfaces,
        };
        let mut role_budgets = BTreeMap::new();
        role_budgets.insert(WorkflowRole::Coder, coder_budget);
        role_budgets.insert(WorkflowRole::Reviewer,
            ControllerRoleExecutionBudget::default_for_role(WorkflowRole::Reviewer, 6));
        role_budgets.insert(WorkflowRole::Closure,
            ControllerRoleExecutionBudget::default_for_role(WorkflowRole::Closure, 2));
        let execution = FreezeControllerExecutionEnvelopeRequest {
            binding_id: binding_id.into(), thread_uuid: thread_uuid.into(),
            job_id: job.job_id.clone(), expected_job_revision: job.job_revision,
            policy: execution_policy(),
            reviewer_choice: headless_argon_choice(Difficulty::Hard),
            closure_choice: headless_argon_choice(Difficulty::Extreme),
            role_budgets,
            max_in_scope_corrections_per_task: correction_cap,
            max_total_corrections_per_job: correction_cap.saturating_mul(2),
            max_total_task_attempts_per_job: 6,
            check_commands: default_check_commands(fixture),
            allow_task_worktree_commits: true,
            allow_run_integration_execution: true,
            default_integration_strategy: IntegrationStrategy::MergeNoFf,
            independence_completeness_receipt: None,
            shadow_jev_policy: None, created_at_ms: 1_700_000_100_000,
            operator_attested: false,
            confirm_approval_subject_digest: None, confirm_envelope_digest: None,
        };
        let mut request = AdmissionRequest {
            request_key: format!("admit_{brief_id}"),
            binding, planning_result: published_outcome.planning_result.clone(),
            work_brief_digest: job.work_brief_digest, execution,
            reviewer_difficulty: Some(Difficulty::Hard),
            closure_difficulty: Some(Difficulty::Extreme),
        };
        // Pure preparation only: derive completeness from the prospective job without binding.
        let preview = admission_service(fixture).preview(&request).unwrap();
        request.execution.independence_completeness_receipt =
            Some(build_independence_completeness_receipt(&preview.proposed_job));
        (request, approval_summary.approval_id)
    }).await
}

// Cleanup is armed before starting a detached owner. It runs on assertion failure as well
// as normal return; pidfds bind Linux cleanup to the actual fixture processes, never reused PIDs.
struct FixtureOwnerCleanup<'a> {
    fixture: &'a PipelineTestFixture,
    state: &'a Path,
    binding: &'a str,
    job_id: &'a str,
    processes: Vec<fs::File>,
    armed: bool,
}

impl FixtureOwnerCleanup<'_> {
    #[cfg(target_os = "linux")]
    fn remember_process(&mut self, pid: u32) {
        use std::os::fd::FromRawFd;
        let fd = unsafe { libc::syscall(libc::SYS_pidfd_open, pid, 0) };
        assert!(fd >= 0, "open pidfd for owned fixture process");
        self.processes
            .push(unsafe { fs::File::from_raw_fd(fd as i32) });
    }

    #[cfg(not(target_os = "linux"))]
    fn remember_process(&mut self, _pid: u32) {}

    #[cfg(target_os = "linux")]
    fn kill_remembered_process(&self, index: usize) {
        use std::os::fd::AsRawFd;
        unsafe {
            libc::syscall(
                libc::SYS_pidfd_send_signal,
                self.processes[index].as_raw_fd(),
                libc::SIGKILL,
                std::ptr::null::<libc::siginfo_t>(),
                0,
            );
        }
    }

    #[cfg(not(target_os = "linux"))]
    fn kill_remembered_process(&self, _index: usize) {
        panic!("owner-loss fixture requires Linux pidfds");
    }
}

impl Drop for FixtureOwnerCleanup<'_> {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        // Fence promotion before releasing a held fake Closure during failure cleanup.
        let _ = self.fixture.policy_store.mutate_job_runtime_for_operator(
            self.binding,
            self.job_id,
            |runtime| {
                runtime.stop_requested = true;
                Ok(())
            },
        );
        let _ = fs::remove_file(self.fixture.root.join("hold_closure.gate"));
        let mut command = std::process::Command::new(env!("CARGO_BIN_EXE_threadlane-flow"));
        command
            .arg("--state-dir")
            .arg(self.state)
            .arg("--registry-dir")
            .arg(&self.fixture.registry_dir)
            .arg("--plans-dir")
            .arg(&self.fixture.plans_dir)
            .arg("--authority-dir")
            .arg(&self.fixture.authority_dir)
            .arg("--sessions-dir")
            .arg(&self.fixture.sessions_dir)
            .arg("--binding")
            .arg(self.binding)
            .arg("--thread")
            .arg(THREAD_UUID_A)
            .args(["code-driver", "cancel", "--job-id", self.job_id])
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null());
        if let Ok(mut child) = command.spawn() {
            let deadline = Instant::now() + Duration::from_secs(10);
            loop {
                if matches!(child.try_wait(), Ok(Some(_))) {
                    break;
                }
                if Instant::now() >= deadline {
                    let _ = child.kill();
                    let _ = child.wait();
                    break;
                }
                std::thread::sleep(Duration::from_millis(10));
            }
        }
        for index in 0..self.processes.len() {
            self.kill_remembered_process(index);
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cli_admission_complete_record_survives_process_exit_and_detached_owner_completes_dag_once()
{
    admission_completion_scenario(CompletionScenario::Accepted).await;
}

#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cli_admission_owner_loss_during_held_closure_stays_unknown_without_replay() {
    admission_completion_scenario(CompletionScenario::OwnerLoss).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cli_admission_live_unknown_closure_is_not_hidden_by_active_completion() {
    admission_completion_scenario(CompletionScenario::UnknownEffect).await;
}

enum CompletionScenario {
    Accepted,
    OwnerLoss,
    UnknownEffect,
}

async fn admission_completion_scenario(scenario: CompletionScenario) {
    Box::pin(async {
        let f = setup_fixture();
        let state = f.root.join("runtime");
        fs::create_dir(&state).unwrap();
        fs::set_permissions(&state, fs::Permissions::from_mode(0o700)).unwrap();
        let supervisor = Supervisor::start(SupervisorConfig::new(state.clone()))
            .await
            .unwrap();
        let binding = SupervisorClient::from_state_dir(&state)
            .unwrap()
            .create_binding(THREAD_UUID_A, "admission-fixture")
            .await
            .unwrap()
            .binding_id;
        let (request, _approval_id) = prepare_admission(
            &f,
            &binding,
            THREAD_UUID_A,
            "brief-admission-cli",
            "plan-admission-cli",
            three_tasks(),
            BTreeMap::new(),
            vec!["src/a.rs".into(), "src/b.rs".into(), "src/c.rs".into()],
            2,
            ExecutionBudgets::default(),
            ControllerRoleExecutionBudget::default_for_role(WorkflowRole::Coder, 6),
            1,
        )
        .await;
        assert!(f
            .workflow_store
            .read_binding_ledger(&binding)
            .unwrap()
            .jobs
            .iter()
            .all(|j| j.approval_linkage.is_none()));
        assert_no_execution(&f);
        let path = private_json(&f, "admission.json", &request);
        let req = path.to_str().unwrap();
        let before = snapshot(&f.plans_dir);
        let git_before = snapshot(&f.repo_dir);
        let preview = cli(
            &f,
            &state,
            &binding,
            &["coworker-admission", "preview", "--request", req],
        )
        .await;
        assert_eq!(snapshot(&f.plans_dir), before);
        assert_eq!(snapshot(&f.repo_dir), git_before);
        assert_no_execution(&f);
        let pd = preview["preview_digest"].as_str().unwrap();
        let ad = preview["approval_subject_digest"].as_str().unwrap();
        let ed = preview["envelope"]["envelope_digest"].as_str().unwrap();
        let record_args = [
            "coworker-admission",
            "record",
            "--request",
            req,
            "--operator-approved",
            "--confirm-preview",
            pd,
            "--confirm-approval",
            ad,
            "--confirm-envelope",
            ed,
        ];
        let recorded = cli(&f, &state, &binding, &record_args).await;
        assert_eq!(recorded["stage"], "complete");
        assert_eq!(
            recorded["job_revision"],
            preview["proposed_job"]["job_revision"]
        );
        assert_eq!(recorded["envelope_digest"], ed);
        assert_eq!(snapshot(&f.repo_dir), git_before);
        assert_no_execution(&f);
        let recorded_files = snapshot(&f.plans_dir);
        // Lose a duplicate response after persistence. Output failure must not be reported as
        // pre-effect refusal, and another process can inspect the unchanged completed receipt.
        let mut lost = tokio::process::Command::new(env!("CARGO_BIN_EXE_threadlane-flow"));
        lost.arg("--state-dir")
            .arg(&state)
            .arg("--registry-dir")
            .arg(&f.registry_dir)
            .arg("--plans-dir")
            .arg(&f.plans_dir)
            .arg("--authority-dir")
            .arg(&f.authority_dir)
            .arg("--sessions-dir")
            .arg(&f.sessions_dir)
            .arg("--binding")
            .arg(&binding)
            .arg("--thread")
            .arg(THREAD_UUID_A)
            .args(record_args)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true);
        let mut child = lost.spawn().unwrap();
        drop(child.stdout.take());
        let response = tokio::time::timeout(Duration::from_secs(30), child.wait_with_output())
            .await
            .expect("bounded lost-response child wait")
            .unwrap();
        assert!(!response.status.success());
        let failure: Value = serde_json::from_slice(&response.stderr).unwrap();
        assert_eq!(failure["code"], "outcome_unknown");
        assert_eq!(snapshot(&f.plans_dir), recorded_files);
        let inspection = cli(
            &f,
            &state,
            &binding,
            &[
                "coworker-admission",
                "inspect",
                "--job-id",
                &request.binding.job_id,
            ],
        )
        .await;
        assert_eq!(inspection["receipt"]["stage"], "complete");
        assert_eq!(snapshot(&f.plans_dir), recorded_files);
        // A new record/reconcile process must establish existing complete durability without
        // replacing a receipt, refreshing source revisions or resetting runtime evidence.
        cli(&f, &state, &binding, &record_args).await;
        cli(
            &f,
            &state,
            &binding,
            &[
                "coworker-admission",
                "reconcile",
                "--job-id",
                &request.binding.job_id,
                "--request-key",
                &request.request_key,
            ],
        )
        .await;
        assert_eq!(snapshot(&f.plans_dir), recorded_files);
        assert_no_execution(&f);
        // No admission service record/reconcile has run in this process. The gateway and its
        // detached run-owner must independently prove the completed CLI receipt.
        let gateway = CoworkerExecutionGateway::new(
            &state,
            &f.registry_dir,
            &f.plans_dir,
            &f.authority_dir,
            &f.sessions_dir,
            &binding,
            THREAD_UUID_A,
        )
        .unwrap()
        .with_sibling_anchor_exe(PathBuf::from(env!("CARGO_BIN_EXE_threadlane-flow")));
        let start = StartExecutionParams {
            job_id: request.binding.job_id.clone(),
            expected_envelope_digest: ed.into(),
        };
        fs::write(f.root.join("hold_closure.gate"), r#"{"timeout_ms":60000}"#).unwrap();
        let mut cleanup = FixtureOwnerCleanup {
            fixture: &f,
            state: &state,
            binding: &binding,
            job_id: &request.binding.job_id,
            processes: Vec::new(),
            armed: true,
        };
        let started = gateway.start_execution(&start).await.unwrap();
        cleanup.remember_process(started.status.verified_owner_summary.as_ref().unwrap().pid_hint);
        gateway.start_execution(&start).await.unwrap();
        let deadline = Instant::now() + Duration::from_secs(180);
        let entered_path = f.root.join("closure_entered.marker");
        while !entered_path.is_file() {
            assert!(Instant::now() < deadline, "Closure not entered: {}", format_fixture_counter_diagnostics(&f));
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        let entered: Value = serde_json::from_slice(&fs::read(&entered_path).unwrap()).unwrap();
        cleanup.remember_process(entered["pid"].as_u64().unwrap() as u32);
        let held_runtime = f.policy_store.read_job_runtime(&binding, &request.binding.job_id).unwrap().unwrap();
        let closure_effect = held_runtime.pending_effects.values().find(|effect| {
            effect.kind == PendingControllerEffectKind::ClosureInvocation
        }).unwrap();
        assert_eq!(closure_effect.status, PendingControllerEffectStatus::ReservedBeforeEffect);
        assert_eq!(closure_effect.subject_digest, entered["subject_digest"].as_str().unwrap());
        assert!(held_runtime.closure_artifacts.is_empty());
        let counts_held = (
            read_invocation_count(&f.agy_coder_count_file),
            read_invocation_count(&f.agy_reviewer_count_file),
            read_invocation_count(&f.agy_closure_count_file),
            read_invocation_count(&f.check_count_file),
        );
        assert_eq!(counts_held, (3, 3, 1, 4));
        // Separate processes inspect the real live RPC while the exact Closure future is held.
        for _ in 0..3 {
            let held = cli(&f, &state, &binding, &["code-driver", "status", "--job-id", &request.binding.job_id]).await;
            assert_eq!(held["owner_state"], "live_verified_owner");
            assert_eq!(held["blocked_on_unknown_effect"], false);
            assert_eq!(held["pipeline_status"], "running");
            assert!(held["pipeline_report"]["completion_summary"].is_null());
            assert!(held["in_flight_stages"].as_array().unwrap().iter().any(|stage| stage["kind"] == "job_completion"));
        }
        if matches!(scenario, CompletionScenario::UnknownEffect) {
            f.policy_store.mutate_job_runtime_for_operator(&binding, &request.binding.job_id, |runtime| {
                let effect = runtime.pending_effects.get_mut(&closure_effect.effect_id).unwrap();
                effect.status = PendingControllerEffectStatus::EffectUnknown;
                effect.diagnostic = Some("Fixture unknown outcome must remain fenced".into());
                Ok(())
            }).unwrap();
            let unknown = cli(&f, &state, &binding, &["code-driver", "status", "--job-id", &request.binding.job_id]).await;
            assert_eq!(unknown["owner_state"], "live_verified_owner");
            assert_eq!(unknown["blocked_on_unknown_effect"], true);
            assert_eq!(unknown["pipeline_status"], "blocked_on_unknown_effect");
            assert!(unknown["pipeline_report"]["completion_summary"].is_null());
            assert_eq!(read_invocation_count(&f.agy_closure_count_file), 1);
            drop(cleanup);
            supervisor.shutdown().await;
            return;
        }
        if matches!(scenario, CompletionScenario::OwnerLoss) {
            cleanup.kill_remembered_process(0);
            fs::remove_file(f.root.join("hold_closure.gate")).unwrap();
            let lost = loop {
                let status = cli(&f, &state, &binding, &["code-driver", "status", "--job-id", &request.binding.job_id]).await;
                if status["owner_state"] == "stale_crash_artifact_detected" {
                    break status;
                }
                assert!(Instant::now() < deadline, "owner loss not observed: {status}");
                tokio::time::sleep(Duration::from_millis(25)).await;
            };
            assert_ne!(lost["job_phase"], "completed");
            assert_eq!(lost["blocked_on_unknown_effect"], true);
            assert_eq!(lost["pipeline_status"], "blocked_on_unknown_effect");
            assert!(lost["pipeline_report"]["completion_summary"].is_null());
            assert!(gateway.start_execution(&start).await.is_err());
            let runtime = f.policy_store.read_job_runtime(&binding, &request.binding.job_id).unwrap().unwrap();
            assert!(runtime.closure_artifacts.is_empty());
            assert_eq!(runtime.total_task_attempts_reserved, held_runtime.total_task_attempts_reserved);
            assert_eq!(runtime.role_usage, held_runtime.role_usage);
            assert_eq!(counts_held, (
                read_invocation_count(&f.agy_coder_count_file),
                read_invocation_count(&f.agy_reviewer_count_file),
                read_invocation_count(&f.agy_closure_count_file),
                read_invocation_count(&f.check_count_file),
            ));
            // Cleanup remains armed: the fake helper and any owner spawned during refusal
            // are stopped without interpreting their output as permission to retry Closure.
            drop(cleanup);
            supervisor.shutdown().await;
            return;
        }
        fs::remove_file(f.root.join("hold_closure.gate")).unwrap();
        loop {
            let status = cli(
                &f,
                &state,
                &binding,
                &["code-driver", "status", "--job-id", &request.binding.job_id],
            )
            .await;
            if status["job_phase"] == "completed" && status["owner_state"] == "no_active_owner" {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "pipeline did not complete: {status}; fixture: {}; pending completion evidence: {:?}",
                format_fixture_counter_diagnostics(&f),
                f.policy_store
                    .read_job_runtime(&binding, &request.binding.job_id)
                    .map(|runtime| runtime.map(|state| (state.pending_effects, state.closure_artifacts)))
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let released: Value = serde_json::from_slice(&fs::read(f.root.join("closure_released.marker")).unwrap()).unwrap();
        assert_eq!(released["released"], true);
        assert_eq!(read_invocation_count(&f.agy_coder_count_file), 3);
        assert_eq!(read_invocation_count(&f.agy_reviewer_count_file), 3);
        assert_eq!(read_invocation_count(&f.agy_closure_count_file), 1);
        assert_eq!(read_invocation_count(&f.check_count_file), 4);
        let completed_status = cli(
            &f,
            &state,
            &binding,
            &["code-driver", "status", "--job-id", &request.binding.job_id],
        )
        .await;
        assert_eq!(
            completed_status["pipeline_report"]["completion_summary"]["closure_verdict_kind"],
            "accepted"
        );
        let counts = (
            read_invocation_count(&f.agy_count_file),
            read_invocation_count(&f.check_count_file),
        );
        // A recreated gateway plus separate status/reconcile/owner processes must reuse evidence.
        let restarted = CoworkerExecutionGateway::new(
            &state,
            &f.registry_dir,
            &f.plans_dir,
            &f.authority_dir,
            &f.sessions_dir,
            &binding,
            THREAD_UUID_A,
        )
        .unwrap()
        .with_sibling_anchor_exe(PathBuf::from(env!("CARGO_BIN_EXE_threadlane-flow")));
        restarted.start_execution(&start).await.unwrap();
        cli(
            &f,
            &state,
            &binding,
            &[
                "code-driver",
                "reconcile",
                "--job-id",
                &request.binding.job_id,
            ],
        )
        .await;
        cli(
            &f,
            &state,
            &binding,
            &[
                "coworker-admission",
                "reconcile",
                "--job-id",
                &request.binding.job_id,
                "--request-key",
                &request.request_key,
            ],
        )
        .await;
        assert_eq!(
            (
                read_invocation_count(&f.agy_count_file),
                read_invocation_count(&f.check_count_file)
            ),
            counts
        );
        let completed_runtime = f.policy_store.read_job_runtime(&binding, &request.binding.job_id).unwrap().unwrap();
        let usage = completed_runtime.role_usage.get(&WorkflowRole::Closure).unwrap();
        assert_eq!(usage.calls_reserved, 1);
        assert_eq!(usage.calls_completed, 1);
        let closure_artifact = f.policy_store.load_closure_execution_artifact(
            &binding, THREAD_UUID_A, &request.binding.job_id,
            completed_runtime.closure_attempt_id.as_deref().unwrap(),
        ).unwrap();
        let complete_receipt = admission_service(&f).inspect(&binding, &request.binding.job_id).unwrap().receipt.unwrap();
        assert_eq!(serde_json::to_value(&complete_receipt).unwrap(), inspection["receipt"]);
        assert_eq!(closure_artifact.execution_report.executed_executable.as_deref(), Some(complete_receipt.preview.executable_pins.closure.executable().unwrap()));
        assert_eq!(closure_artifact.subject_digest, entered["subject_digest"].as_str().unwrap());
        assert_eq!(closure_artifact.candidate_tree_sha, entered["candidate_tree_sha"].as_str().unwrap());
        let job = f
            .workflow_store
            .read_binding_ledger(&binding)
            .unwrap()
            .jobs
            .into_iter()
            .find(|j| j.job_id == request.binding.job_id)
            .unwrap();
        assert_eq!(job.phase, CoworkerJobPhase::Completed);
        assert!(job
            .tasks
            .iter()
            .all(|t| t.stage == CoworkerTaskStage::Integrated));
        cleanup.armed = false;
        supervisor.shutdown().await;
    })
    .await
}

thread_local! {
    static FAILURE: std::cell::Cell<Option<AdmissionPersistenceBoundary>> =
        const { std::cell::Cell::new(None) };
}

fn refuse_at_boundary(
    point: AdmissionPersistenceBoundary,
) -> Result<(), threadlane_native::NativeError> {
    if FAILURE.with(|f| f.get()) != Some(point) {
        return Ok(());
    }
    use AdmissionPersistenceBoundary::*;
    if matches!(
        point,
        BeforeIntent | BeforeLinkage | BeforeRuntime | BeforeComplete
    ) {
        Err(threadlane_native::NativeError::new(
            NativeErrorCode::IoError,
            "Injected write refusal",
        ))
    } else {
        Err(threadlane_native::NativeError::persistence_uncertain(
            "Injected lost sync acknowledgement",
        ))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cli_reconciles_every_partial_boundary_without_dispatch_or_refreshed_consent() {
    Box::pin(async {
        use AdmissionPersistenceBoundary::*;
        let f = setup_fixture();
        let state = f.root.join("runtime");
        fs::create_dir(&state).unwrap();
        fs::set_permissions(&state, fs::Permissions::from_mode(0o700)).unwrap();
        let supervisor = Supervisor::start(SupervisorConfig::new(state.clone()))
            .await
            .unwrap();
        let binding = SupervisorClient::from_state_dir(&state)
            .unwrap()
            .create_binding(THREAD_UUID_A, "admission-recovery-fixture")
            .await
            .unwrap()
            .binding_id;
        for (index, point) in [
            BeforeIntent,
            AfterIntent,
            BeforeLinkage,
            AfterLinkage,
            BeforeRuntime,
            AfterRuntime,
            BeforeComplete,
            AfterComplete,
        ]
        .into_iter()
        .enumerate()
        {
            let (request, _) = prepare_admission(
                &f,
                &binding,
                THREAD_UUID_A,
                &format!("brief-partial-{index}"),
                &format!("plan-partial-{index}"),
                three_tasks(),
                BTreeMap::new(),
                vec!["src/a.rs".into(), "src/b.rs".into(), "src/c.rs".into()],
                2,
                ExecutionBudgets::default(),
                ControllerRoleExecutionBudget::default_for_role(WorkflowRole::Coder, 6),
                1,
            )
            .await;
            let api = admission_service(&f);
            let preview = api.preview(&request).unwrap();
            let consent = AdmissionConfirmation {
                trusted_local_attested: true,
                preview_digest: preview.preview_digest.clone(),
                approval_subject_digest: preview.approval_subject_digest.clone(),
                envelope_digest: preview.envelope.envelope_digest.clone(),
            };
            let git_before = snapshot(&f.repo_dir);
            FAILURE.with(|f| f.set(Some(point)));
            assert!(api
                .clone()
                .with_failure_probe(refuse_at_boundary)
                .record(&request, &consent)
                .is_err());
            FAILURE.with(|f| f.set(None));
            let before_inspection = snapshot(&f.plans_dir);
            let inspection = cli(
                &f,
                &state,
                &binding,
                &[
                    "coworker-admission",
                    "inspect",
                    "--job-id",
                    &request.binding.job_id,
                ],
            )
            .await;
            assert_eq!(snapshot(&f.plans_dir), before_inspection);
            let gateway = CoworkerExecutionGateway::new(
                &state,
                &f.registry_dir,
                &f.plans_dir,
                &f.authority_dir,
                &f.sessions_dir,
                &binding,
                THREAD_UUID_A,
            )
            .unwrap()
            .with_sibling_anchor_exe(PathBuf::from(env!("CARGO_BIN_EXE_threadlane-flow")));
            assert!(
                gateway
                    .start_execution(&StartExecutionParams {
                        job_id: request.binding.job_id.clone(),
                        expected_envelope_digest: preview.envelope.envelope_digest.clone(),
                    })
                    .await
                    .is_err(),
                "start must not finish partial {point:?}"
            );
            assert_eq!(snapshot(&f.plans_dir), before_inspection);
            assert_no_execution(&f);
            if point != BeforeIntent {
                let job = f
                    .workflow_store
                    .read_binding_ledger(&binding)
                    .unwrap()
                    .jobs
                    .into_iter()
                    .find(|j| j.job_id == request.binding.job_id)
                    .unwrap();
                assert!(
                    verify_complete_admission(&f.workflow_store, &f.policy_store, &job).is_err(),
                    "partial {point:?} must be fenced"
                );
                let wrong_key = cli_output(
                    &f,
                    &state,
                    &binding,
                    &[
                        "coworker-admission",
                        "reconcile",
                        "--job-id",
                        &request.binding.job_id,
                        "--request-key",
                        "replacement-key",
                    ],
                )
                .await;
                assert!(!wrong_key.status.success());
                assert_eq!(snapshot(&f.plans_dir), before_inspection);
                let recovered = cli(
                    &f,
                    &state,
                    &binding,
                    &[
                        "coworker-admission",
                        "reconcile",
                        "--job-id",
                        &request.binding.job_id,
                        "--request-key",
                        &request.request_key,
                    ],
                )
                .await;
                assert_eq!(recovered["preview_digest"], preview.preview_digest);
                assert_eq!(
                    recovered["envelope_digest"],
                    preview.envelope.envelope_digest
                );
                assert_eq!(recovered["stage"], "complete");
                let runtime = f
                    .policy_store
                    .get_job_runtime_readonly(&binding, &request.binding.job_id)
                    .unwrap()
                    .unwrap();
                assert_eq!(runtime.total_task_attempts_reserved, 0);
                assert!(runtime.lease.is_none());
                assert!(runtime.pending_effects.is_empty());
                assert!(runtime.attempt_artifacts.is_empty());
            } else {
                assert_eq!(inspection["state"], "absent");
            }
            assert_eq!(snapshot(&f.repo_dir), git_before);
            assert_no_execution(&f);
        }
        supervisor.shutdown().await;
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cli_refuses_conflicting_consent_scope_and_revisions_and_deduplicates_process_races() {
    Box::pin(async {
        let f = setup_fixture();
        let state = f.root.join("runtime");
        fs::create_dir(&state).unwrap();
        fs::set_permissions(&state, fs::Permissions::from_mode(0o700)).unwrap();
        let supervisor = Supervisor::start(SupervisorConfig::new(state.clone()))
            .await
            .unwrap();
        let binding = SupervisorClient::from_state_dir(&state)
            .unwrap()
            .create_binding(THREAD_UUID_A, "admission-refusal-fixture")
            .await
            .unwrap()
            .binding_id;
        let (request, _) = prepare_admission(
            &f,
            &binding,
            THREAD_UUID_A,
            "brief-refusals",
            "plan-refusals",
            three_tasks(),
            BTreeMap::new(),
            vec!["src/a.rs".into(), "src/b.rs".into(), "src/c.rs".into()],
            2,
            ExecutionBudgets::default(),
            ControllerRoleExecutionBudget::default_for_role(WorkflowRole::Coder, 6),
            1,
        )
        .await;
        let preview = admission_service(&f).preview(&request).unwrap();
        let path = private_json(&f, "admission.json", &request);
        let req = path.to_str().unwrap();
        let pd = preview.preview_digest.as_str();
        let ad = preview.approval_subject_digest.as_str();
        let ed = preview.envelope.envelope_digest.as_str();
        let before = snapshot(&f.plans_dir);
        assert!(!cli_output(
            &f,
            &state,
            "bnd_nonexistent",
            &["coworker-admission", "preview", "--request", req]
        )
        .await
        .status
        .success());
        let insecure = private_json(&f, "insecure.json", &request);
        fs::set_permissions(&insecure, fs::Permissions::from_mode(0o644)).unwrap();
        assert!(!cli_output(
            &f,
            &state,
            &binding,
            &[
                "coworker-admission",
                "preview",
                "--request",
                insecure.to_str().unwrap()
            ]
        )
        .await
        .status
        .success());
        let linked = f.root.join("symlink-request.json");
        std::os::unix::fs::symlink(&path, &linked).unwrap();
        assert!(!cli_output(
            &f,
            &state,
            &binding,
            &[
                "coworker-admission",
                "preview",
                "--request",
                linked.to_str().unwrap()
            ]
        )
        .await
        .status
        .success());
        assert_eq!(snapshot(&f.plans_dir), before);
        for args in [
            vec!["coworker-admission", "record", "--request", req],
            vec![
                "coworker-admission",
                "record",
                "--request",
                req,
                "--operator-approved",
            ],
            vec![
                "coworker-admission",
                "record",
                "--request",
                req,
                "--operator-approved",
                "--confirm-preview",
                pd,
                "--confirm-approval",
                ad,
            ],
            vec![
                "coworker-admission",
                "record",
                "--request",
                req,
                "--operator-approved",
                "--confirm-preview",
                pd,
                "--confirm-envelope",
                ed,
            ],
            vec![
                "coworker-admission",
                "record",
                "--request",
                req,
                "--operator-approved",
                "--confirm-approval",
                ad,
                "--confirm-envelope",
                ed,
            ],
            vec![
                "coworker-admission",
                "record",
                "--request",
                req,
                "--operator-approved",
                "--confirm-preview",
                ad,
                "--confirm-approval",
                ad,
                "--confirm-envelope",
                ed,
            ],
        ] {
            assert!(!cli_output(&f, &state, &binding, &args)
                .await
                .status
                .success());
            assert_eq!(snapshot(&f.plans_dir), before);
        }
        let mut self_attested = request.clone();
        self_attested.execution.operator_attested = true;
        self_attested.execution.confirm_approval_subject_digest = Some(ad.into());
        self_attested.execution.confirm_envelope_digest = Some(ed.into());
        let p = private_json(&f, "self-attested.json", &self_attested);
        assert!(!cli_output(
            &f,
            &state,
            &binding,
            &[
                "coworker-admission",
                "record",
                "--request",
                p.to_str().unwrap()
            ]
        )
        .await
        .status
        .success());
        self_attested.execution.confirm_envelope_digest = Some("f".repeat(64));
        let p = private_json(&f, "disagree.json", &self_attested);
        assert!(!cli_output(
            &f,
            &state,
            &binding,
            &[
                "coworker-admission",
                "record",
                "--request",
                p.to_str().unwrap(),
                "--operator-approved",
                "--confirm-preview",
                pd,
                "--confirm-approval",
                ad,
                "--confirm-envelope",
                ed
            ]
        )
        .await
        .status
        .success());
        let mut variants = Vec::new();
        let mut r = request.clone();
        r.binding.binding_id = "bnd_wrong".into();
        variants.push(r);
        let mut r = request.clone();
        r.execution.thread_uuid = "6ba7b810-9dad-11d1-80b4-00c04fd430c8".into();
        variants.push(r);
        let mut r = request.clone();
        r.planning_result.project_base.repository_path = f.root.display().to_string();
        variants.push(r);
        let mut r = request.clone();
        r.binding.expected_job_revision += 1;
        r.execution.expected_job_revision += 1;
        variants.push(r);
        let mut r = request.clone();
        *r.binding.expected_ledger_revision.as_mut().unwrap() += 1;
        variants.push(r);
        let mut r = request.clone();
        r.execution.max_total_task_attempts_per_job += 1;
        variants.push(r);
        for (index, variant) in variants.iter().enumerate() {
            let p = private_json(&f, &format!("wrong-{index}.json"), variant);
            assert!(!cli_output(
                &f,
                &state,
                &binding,
                &[
                    "coworker-admission",
                    "record",
                    "--request",
                    p.to_str().unwrap(),
                    "--operator-approved",
                    "--confirm-preview",
                    pd,
                    "--confirm-approval",
                    ad,
                    "--confirm-envelope",
                    ed
                ]
            )
            .await
            .status
            .success());
            assert_eq!(snapshot(&f.plans_dir), before);
        }
        let oversized = f.root.join("oversized.json");
        fs::write(&oversized, vec![b'X'; 1_048_577]).unwrap();
        fs::set_permissions(&oversized, fs::Permissions::from_mode(0o600)).unwrap();
        assert!(!cli_output(
            &f,
            &state,
            &binding,
            &[
                "coworker-admission",
                "preview",
                "--request",
                oversized.to_str().unwrap()
            ]
        )
        .await
        .status
        .success());
        let args = [
            "coworker-admission",
            "record",
            "--request",
            req,
            "--operator-approved",
            "--confirm-preview",
            pd,
            "--confirm-approval",
            ad,
            "--confirm-envelope",
            ed,
        ];
        let (a, b) = tokio::join!(
            cli_output(&f, &state, &binding, &args),
            cli_output(&f, &state, &binding, &args)
        );
        assert!(a.status.success(), "{}", String::from_utf8_lossy(&a.stderr));
        assert!(b.status.success(), "{}", String::from_utf8_lossy(&b.stderr));
        let a: Value = serde_json::from_slice(&a.stdout).unwrap();
        let b: Value = serde_json::from_slice(&b.stdout).unwrap();
        assert_eq!(a["data"], b["data"]);
        assert_no_execution(&f);
        let receipt_path = fs::read_dir(&f.plans_dir)
            .unwrap()
            .map(|e| e.unwrap().path())
            .find(|p| {
                p.file_name()
                    .unwrap()
                    .to_string_lossy()
                    .starts_with("coworker-admission-")
                    && p.extension().is_some_and(|e| e == "json")
            })
            .unwrap();
        let receipt_bytes = fs::read(&receipt_path).unwrap();
        let gateway = CoworkerExecutionGateway::new(
            &state,
            &f.registry_dir,
            &f.plans_dir,
            &f.authority_dir,
            &f.sessions_dir,
            &binding,
            THREAD_UUID_A,
        )
        .unwrap()
        .with_sibling_anchor_exe(PathBuf::from(env!("CARGO_BIN_EXE_threadlane-flow")));
        let start = StartExecutionParams {
            job_id: request.binding.job_id.clone(),
            expected_envelope_digest: ed.into(),
        };
        fs::remove_file(&receipt_path).unwrap();
        assert!(gateway.start_execution(&start).await.is_err());
        let missing = cli(
            &f,
            &state,
            &binding,
            &[
                "coworker-admission",
                "inspect",
                "--job-id",
                &request.binding.job_id,
            ],
        )
        .await;
        assert_eq!(missing["state"], "conflicting");
        let mut receipt: AdmissionReceipt = serde_json::from_slice(&receipt_bytes).unwrap();
        receipt.stage = AdmissionStage::Uncertain;
        fs::write(&receipt_path, serde_json::to_vec(&receipt).unwrap()).unwrap();
        fs::set_permissions(&receipt_path, fs::Permissions::from_mode(0o600)).unwrap();
        let uncertain = snapshot(&f.plans_dir);
        assert!(gateway.start_execution(&start).await.is_err());
        assert_eq!(snapshot(&f.plans_dir), uncertain);
        receipt.preview.authority_budgets.timeout_ms += 1;
        fs::write(&receipt_path, serde_json::to_vec(&receipt).unwrap()).unwrap();
        let corrupt = snapshot(&f.plans_dir);
        assert!(!cli_output(
            &f,
            &state,
            &binding,
            &[
                "coworker-admission",
                "inspect",
                "--job-id",
                &request.binding.job_id
            ]
        )
        .await
        .status
        .success());
        assert!(gateway.start_execution(&start).await.is_err());
        assert_eq!(snapshot(&f.plans_dir), corrupt);
        assert_no_execution(&f);
        supervisor.shutdown().await;
    })
    .await
}

fn populate_isolated_system_path_dir(isolated_bin: &Path) {
    fs::create_dir_all(isolated_bin).expect("create isolated_bin");
    fs::set_permissions(isolated_bin, fs::Permissions::from_mode(0o700))
        .expect("chmod 0700 isolated_bin");
    let Some(parent_path) = std::env::var_os("PATH") else {
        return;
    };
    for util in [
        "git", "node", "sh", "mkdir", "cat", "rmdir", "sleep", "rm", "cp", "mv", "chmod", "printf",
    ] {
        for dir in std::env::split_paths(&parent_path) {
            if !dir.is_absolute() {
                continue;
            }
            let candidate = dir.join(util);
            if let Ok(meta) = fs::metadata(&candidate) {
                if meta.is_file() && (meta.permissions().mode() & 0o111 != 0) {
                    let link_path = isolated_bin.join(util);
                    if !link_path.exists() {
                        std::os::unix::fs::symlink(&candidate, &link_path)
                            .expect("symlink basic utility into isolated_bin");
                    }
                    break;
                }
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cli_system_mode_agy_worker_child_probe() {
    let Some(isolated_bin_os) = std::env::var_os("THREADLANE_CLI_SYSTEM_MODE_AGY_BIN") else {
        return;
    };
    let isolated_bin = PathBuf::from(isolated_bin_os);
    Box::pin(async move {
        use threadlane_native::coworker_workflow::{
            FinalizePlanningCallRequest, PlanningResultRef, RecordJobPlanningResultRequest,
            ReservePlanningCallRequest,
        };
        use threadlane_workflow::{compile_plan_revision, PlanChange};

        let f = setup_fixture();
        let state = f.root.join("runtime-sys-agy");
        fs::create_dir(&state).unwrap();
        fs::set_permissions(&state, fs::Permissions::from_mode(0o700)).unwrap();
        let supervisor = Supervisor::start(SupervisorConfig::new(state.clone()))
            .await
            .unwrap();
        let binding = SupervisorClient::from_state_dir(&state)
            .unwrap()
            .create_binding(THREAD_UUID_A, "admission-system-agy-cli")
            .await
            .unwrap()
            .binding_id;

        let probe_marker = f.root.join("unexpected_provider_probe.marker");
        let providers_dir = f.root.join("providers");
        fs::create_dir_all(&providers_dir).unwrap();
        let canonical_agy_v1 = providers_dir.join("agy-v1");
        let canonical_agy_v2 = providers_dir.join("agy-v2");
        let canonical_codex_v1 = providers_dir.join("codex-v1");
        for bin in [&canonical_agy_v1, &canonical_agy_v2, &canonical_codex_v1] {
            write_executable_script(
                bin,
                &format!(
                    "#!/bin/sh\nprintf 'probed' > \"{}\"\nexit 99\n",
                    probe_marker.display()
                ),
            );
        }
        let canonical_agy_v1 = fs::canonicalize(&canonical_agy_v1).unwrap();
        let canonical_agy_v2 = fs::canonicalize(&canonical_agy_v2).unwrap();
        let canonical_codex_v1 = fs::canonicalize(&canonical_codex_v1).unwrap();

        let agy_link = isolated_bin.join("agy");
        let codex_link = isolated_bin.join("codex");
        let _ = fs::remove_file(&agy_link);
        let _ = fs::remove_file(&codex_link);
        std::os::unix::fs::symlink(&canonical_agy_v1, &agy_link).unwrap();
        std::os::unix::fs::symlink(&canonical_codex_v1, &codex_link).unwrap();

        let rev = f.registry.snapshot().unwrap().revision;
        f.registry
            .apply(
                rev,
                RegistryChange::UpsertTool {
                    tool: ToolDefinition {
                        id: "tool-agy".to_string(),
                        label: "Antigravity CLI (System Mode)".to_string(),
                        adapter: ToolAdapter::Antigravity,
                        executable_path: None,
                        enabled: true,
                    },
                },
            )
            .unwrap();
        let rev = f.registry.snapshot().unwrap().revision;
        f.registry
            .apply(
                rev,
                RegistryChange::UpsertProfile {
                    profile: WorkflowProfile {
                        decision_advisor: None,
                        id: "profile-coworker".to_string(),
                        name: "Coworker System AGY Worker + Codex Advisory".to_string(),
                        assignments: vec![
                            RoleAssignment {
                                role: WorkflowRole::Orchestrator,
                                difficulty: Some(Difficulty::Hard),
                                choice: ModelChoice {
                                    model_id: "model-codex-native".to_string(),
                                    effort: Some("high".to_string()),
                                },
                            },
                            RoleAssignment {
                                role: WorkflowRole::Architect,
                                difficulty: Some(Difficulty::Hard),
                                choice: ModelChoice {
                                    model_id: "model-codex-native".to_string(),
                                    effort: Some("high".to_string()),
                                },
                            },
                            RoleAssignment {
                                role: WorkflowRole::Coder,
                                difficulty: Some(Difficulty::Hard),
                                choice: ModelChoice {
                                    model_id: "model-argon".to_string(),
                                    effort: Some("high".to_string()),
                                },
                            },
                            RoleAssignment {
                                role: WorkflowRole::Reviewer,
                                difficulty: Some(Difficulty::Hard),
                                choice: ModelChoice {
                                    model_id: "model-codex-native".to_string(),
                                    effort: Some("high".to_string()),
                                },
                            },
                            RoleAssignment {
                                role: WorkflowRole::Closure,
                                difficulty: Some(Difficulty::Extreme),
                                choice: ModelChoice {
                                    model_id: "model-codex-native".to_string(),
                                    effort: Some("high".to_string()),
                                },
                            },
                        ],
                    },
                },
            )
            .unwrap();

        let codex_choice = |difficulty: Difficulty| SelectedRoleChoice {
            tool_id: "tool-codex".to_string(),
            adapter: ToolAdapter::CodexCli,
            route: RoleExecutionRoute::HeadlessWorker,
            model_id: "model-codex-native".to_string(),
            model_arg: "gpt-6-astra".to_string(),
            effort: Some("high".to_string()),
            matched_difficulty: Some(difficulty),
        };
        let planning_policy = ExecutionPolicy {
            schema_version: 1,
            policy_id: "policy-sys-agy-plan".to_string(),
            allow_rules: vec![PolicyAllowRule {
                tool_id: "tool-codex".to_string(),
                model_id: "model-codex-native".to_string(),
                efforts: vec!["high".to_string()],
                allow_no_effort: false,
                roles: vec![WorkflowRole::Orchestrator, WorkflowRole::Architect],
            }],
        };
        let exec_policy = ExecutionPolicy {
            schema_version: 1,
            policy_id: "policy-sys-agy-exec".to_string(),
            allow_rules: vec![
                PolicyAllowRule {
                    tool_id: "tool-agy".to_string(),
                    model_id: "model-argon".to_string(),
                    efforts: vec!["high".to_string()],
                    allow_no_effort: false,
                    roles: vec![WorkflowRole::Coder],
                },
                PolicyAllowRule {
                    tool_id: "tool-codex".to_string(),
                    model_id: "model-codex-native".to_string(),
                    efforts: vec!["high".to_string()],
                    allow_no_effort: false,
                    roles: vec![
                        WorkflowRole::Orchestrator,
                        WorkflowRole::Architect,
                        WorkflowRole::Reviewer,
                        WorkflowRole::Closure,
                    ],
                },
            ],
        };

        let pb = project_base(&f);
        let reg_snap = f.registry.snapshot().unwrap();
        let brief = WorkBrief {
            brief_id: "brief-sys-agy".to_string(),
            binding_id: binding.clone(),
            thread_uuid: THREAD_UUID_A.to_string(),
            project_base: pb.clone(),
            kind: WorkBriefKind::NewJob,
            summary: "System-mode AGY Worker CLI admission test".to_string(),
            confirmed_requirements: vec!["Validate System-mode AGY Worker pin".to_string()],
            proposed_additions: Vec::new(),
            assumptions: vec!["Isolated PATH fixture".to_string()],
            open_questions: Vec::new(),
            accepted_architect_decisions: Vec::new(),
        };
        let job = f
            .workflow_store
            .create_or_amend_job(&CreateCoworkerJobRequest {
                binding_id: binding.clone(),
                thread_uuid: THREAD_UUID_A.to_string(),
                request_key: "req-job-sys-agy".to_string(),
                expected_ledger_revision: None,
                work_brief: brief,
                amended_target_task_ids: Vec::new(),
                amended_write_paths: Vec::new(),
                amended_interfaces: Vec::new(),
            })
            .unwrap();
        let grant = f
            .workflow_store
            .create_planning_grant(&CreatePlanningGrantRequest {
                binding_id: binding.clone(),
                thread_uuid: THREAD_UUID_A.to_string(),
                request_key: "req-grant-sys-agy".to_string(),
                expected_ledger_revision: None,
                project_base: pb.clone(),
                profile_id: "profile-coworker".to_string(),
                registry_revision: reg_snap.revision,
                policy: planning_policy,
                capabilities: PlanningReadOnlyCapabilities::strict_read_only(),
                budget: PlanningCallBudget {
                    max_total_calls: 2,
                    max_orchestrator_calls: 1,
                    max_architect_calls: 1,
                    max_timeout_ms_per_call: 30_000,
                    max_output_bytes_per_call: 65_536,
                },
                created_at_ms: 1_000,
                expires_at_ms: 60_000,
                operator_attested: true,
            })
            .unwrap();
        let reservation = f
            .workflow_store
            .reserve_planning_call(&ReservePlanningCallRequest {
                binding_id: binding.clone(),
                thread_uuid: THREAD_UUID_A.to_string(),
                grant_id: grant.grant_id.clone(),
                request_key: "req-plan-call-sys-agy".to_string(),
                expected_ledger_revision: None,
                role: WorkflowRole::Orchestrator,
                difficulty: Some(Difficulty::Hard),
                selected_choice: codex_choice(Difficulty::Hard),
                context_digest: job.work_brief_digest.clone(),
                call_project_base: Some(pb.clone()),
                current_time_ms: 2_000,
            })
            .unwrap();
        let output_digest = sha256_hex(b"sys-agy-plan-output");
        f.workflow_store
            .finalize_planning_call(&FinalizePlanningCallRequest {
                binding_id: binding.clone(),
                thread_uuid: THREAD_UUID_A.to_string(),
                grant_id: grant.grant_id.clone(),
                reservation_id: reservation.reservation_id.clone(),
                expected_ledger_revision: None,
                status: PlanningReservationStatus::Completed,
                updated_at_ms: 2_100,
                run_id: Some("run-sys-agy-plan".to_string()),
                report_artifact_id: Some("report-sys-agy-plan".to_string()),
                output_digest: Some(output_digest.clone()),
                diagnostic: None,
            })
            .unwrap();

        let tasks = three_tasks();
        let draft = f
            .plans
            .change_at(
                0,
                PlanChange::SaveDraft {
                    plan_id: "plan-sys-agy".to_string(),
                    binding_id: binding.clone(),
                    profile_id: "profile-coworker".to_string(),
                    profile_revision: reg_snap.revision,
                    summary: "System-mode AGY Worker plan".to_string(),
                    tasks: tasks.clone(),
                },
                1_700_000_000_000,
            )
            .unwrap();
        let published = f
            .plans
            .change(
                draft.revision,
                PlanChange::Publish {
                    plan_id: "plan-sys-agy".to_string(),
                    revision: draft.revision,
                },
            )
            .unwrap();
        let compiled = compile_plan_revision(&published, &reg_snap).unwrap();
        let approval_summary = f
            .authority
            .approve_at(
                OperatorApprovalRequest {
                    binding_id: binding.clone(),
                    thread_uuid: THREAD_UUID_A.to_string(),
                    plan_id: "plan-sys-agy".to_string(),
                    plan_revision: published.revision,
                    expected_registry_revision: reg_snap.revision,
                    expected_plan_digest: compiled.plan_digest.clone(),
                    repository_path: f.repo_dir.clone(),
                    base_commit: f.base_commit.clone(),
                    approved_task_ids: tasks.iter().map(|t| t.id.clone()).collect(),
                    approved_write_paths: vec![
                        "src/a.rs".to_string(),
                        "src/b.rs".to_string(),
                        "src/c.rs".to_string(),
                    ],
                    max_parallel: 2,
                    budgets: ExecutionBudgets::default(),
                },
                1_700_000_001_000,
            )
            .expect("System-mode AGY Worker authority approval must succeed");
        let approval_record = f
            .authority
            .get_for_operator_readonly(&approval_summary.approval_id)
            .unwrap();
        for task in &approval_record.tasks {
            assert_eq!(
                task.canonical_executable.as_ref(),
                Some(&canonical_agy_v1),
                "authority must freeze canonical symlink target for System-mode AGY Worker"
            );
        }
        assert!(
            !probe_marker.exists(),
            "authority approval must never execute or probe provider binary"
        );

        let planning_result = PlanningResultRef {
            grant_id: grant.grant_id,
            reservation_id: reservation.reservation_id,
            request_key: reservation.request_key,
            run_id: "run-sys-agy-plan".to_string(),
            report_artifact_id: "report-sys-agy-plan".to_string(),
            output_digest,
            work_brief_digest: job.work_brief_digest.clone(),
            project_base: pb,
            profile_id: approval_record.profile_id.clone(),
            registry_revision: approval_record.registry_revision,
            policy_digest: grant.policy_digest,
            selected_choice: codex_choice(Difficulty::Hard),
            plan_id: approval_record.plan_id.clone(),
            plan_revision: approval_record.plan_revision,
            plan_digest: approval_record.plan_digest.clone(),
            plan_status: threadlane_workflow::PlanStatus::Published,
            task_interfaces: BTreeMap::new(),
            architect_advice_ids: Vec::new(),
            resolved_open_questions: Vec::new(),
            deferred_proposals: Vec::new(),
            recorded_at_ms: 2_100,
        };
        let job = f
            .workflow_store
            .record_job_planning_result(&RecordJobPlanningResultRequest {
                binding_id: binding.clone(),
                thread_uuid: THREAD_UUID_A.to_string(),
                job_id: job.job_id,
                expected_job_revision: Some(job.job_revision),
                expected_ledger_revision: None,
                planning_result: planning_result.clone(),
            })
            .unwrap();

        let mut task_entries = BTreeMap::new();
        for approved_task in &approval_record.tasks {
            task_entries.insert(
                approved_task.task_id.clone(),
                TaskIndependenceCompletenessEntry {
                    task_id: approved_task.task_id.clone(),
                    task_digest: approved_task.task_digest.clone(),
                    owned_interfaces: Vec::new(),
                    consumed_interfaces: Vec::new(),
                    explicitly_no_interfaces: true,
                    semantic_independence_attested: true,
                },
            );
        }
        let mut completeness = DeclaredIndependenceCompletenessReceipt {
            receipt_id: format!("icr_{}", job.job_id),
            job_id: job.job_id.clone(),
            approval_id: approval_summary.approval_id.clone(),
            plan_digest: approval_record.plan_digest.clone(),
            task_entries,
            hold_attestations: BTreeMap::new(),
            recorded_at_ms: 1_700_000_050_000,
            operator_attested: true,
            receipt_digest: String::new(),
        };
        completeness.receipt_digest = completeness.compute_digest().unwrap();

        let cap_env = FrozenControllerCapabilityEnvelope {
            allowed_reviewer_choices: vec![codex_choice(Difficulty::Hard)],
            allowed_closure_choices: vec![codex_choice(Difficulty::Extreme)],
            max_in_scope_correction_attempts: 1,
            allowed_check_ids: vec!["cargo_test".to_string()],
            local_git_integration_allowed: true,
            allow_check_failure_corrections: false,
        };
        let mut role_budgets = BTreeMap::new();
        role_budgets.insert(
            WorkflowRole::Coder,
            ControllerRoleExecutionBudget::default_for_role(WorkflowRole::Coder, 6),
        );
        role_budgets.insert(
            WorkflowRole::Reviewer,
            ControllerRoleExecutionBudget::default_for_role(WorkflowRole::Reviewer, 6),
        );
        role_budgets.insert(
            WorkflowRole::Closure,
            ControllerRoleExecutionBudget::default_for_role(WorkflowRole::Closure, 2),
        );
        let request = AdmissionRequest {
            request_key: "admit-sys-agy".to_string(),
            binding: BindJobApprovalRequest {
                binding_id: binding.clone(),
                thread_uuid: THREAD_UUID_A.to_string(),
                job_id: job.job_id.clone(),
                expected_job_revision: job.job_revision,
                expected_ledger_revision: Some(
                    f.workflow_store
                        .read_binding_ledger(&binding)
                        .unwrap()
                        .ledger_revision,
                ),
                approval_id: approval_summary.approval_id.clone(),
                policy: exec_policy.clone(),
                controller_envelope: Some(cap_env),
                task_interfaces: BTreeMap::new(),
            },
            planning_result,
            work_brief_digest: job.work_brief_digest.clone(),
            execution: FreezeControllerExecutionEnvelopeRequest {
                binding_id: binding.clone(),
                thread_uuid: THREAD_UUID_A.to_string(),
                job_id: job.job_id.clone(),
                expected_job_revision: job.job_revision,
                policy: exec_policy,
                reviewer_choice: codex_choice(Difficulty::Hard),
                closure_choice: codex_choice(Difficulty::Extreme),
                role_budgets,
                max_in_scope_corrections_per_task: 1,
                max_total_corrections_per_job: 2,
                max_total_task_attempts_per_job: 6,
                check_commands: default_check_commands(&f),
                allow_task_worktree_commits: true,
                allow_run_integration_execution: true,
                default_integration_strategy: IntegrationStrategy::MergeNoFf,
                independence_completeness_receipt: Some(completeness),
                shadow_jev_policy: None,
                created_at_ms: 1_700_000_100_000,
                operator_attested: false,
                confirm_approval_subject_digest: None,
                confirm_envelope_digest: None,
            },
            reviewer_difficulty: Some(Difficulty::Hard),
            closure_difficulty: Some(Difficulty::Extreme),
        };

        let req_path = private_json(&f, "admission-sys-agy.json", &request);
        let req_str = req_path.to_str().unwrap();

        let plans_before = snapshot(&f.plans_dir);
        let authority_before = snapshot(&f.authority_dir);
        let registry_before = snapshot(&f.registry_dir);
        let repo_before = snapshot(&f.repo_dir);

        let preview = cli(
            &f,
            &state,
            &binding,
            &["coworker-admission", "preview", "--request", req_str],
        )
        .await;
        assert_eq!(snapshot(&f.plans_dir), plans_before);
        assert_eq!(snapshot(&f.authority_dir), authority_before);
        assert_eq!(snapshot(&f.registry_dir), registry_before);
        assert_eq!(snapshot(&f.repo_dir), repo_before);
        assert!(
            !probe_marker.exists(),
            "preview must never invoke or probe provider"
        );
        assert_no_execution(&f);
        for (_task_id, pin) in preview["executable_pins"]["workers"].as_object().unwrap() {
            assert_eq!(
                pin["canonical_executable"].as_str().unwrap(),
                canonical_agy_v1.to_str().unwrap()
            );
        }
        assert_eq!(
            preview["executable_pins"]["reviewer"]["canonical_executable"]
                .as_str()
                .unwrap(),
            canonical_codex_v1.to_str().unwrap()
        );
        assert_eq!(
            preview["executable_pins"]["closure"]["canonical_executable"]
                .as_str()
                .unwrap(),
            canonical_codex_v1.to_str().unwrap()
        );

        let pd = preview["preview_digest"].as_str().unwrap();
        let ad = preview["approval_subject_digest"].as_str().unwrap();
        let ed = preview["envelope"]["envelope_digest"].as_str().unwrap();

        // Changed PATH target for System-mode AGY Worker must be rejected by preview and record.
        fs::remove_file(&agy_link).unwrap();
        std::os::unix::fs::symlink(&canonical_agy_v2, &agy_link).unwrap();
        assert!(!cli_output(
            &f,
            &state,
            &binding,
            &["coworker-admission", "preview", "--request", req_str],
        )
        .await
        .status
        .success());
        assert!(!cli_output(
            &f,
            &state,
            &binding,
            &[
                "coworker-admission",
                "record",
                "--request",
                req_str,
                "--operator-approved",
                "--confirm-preview",
                pd,
                "--confirm-approval",
                ad,
                "--confirm-envelope",
                ed,
            ],
        )
        .await
        .status
        .success());
        assert_eq!(snapshot(&f.plans_dir), plans_before);

        // Missing PATH executable for System-mode AGY Worker must be rejected.
        fs::remove_file(&agy_link).unwrap();
        assert!(!cli_output(
            &f,
            &state,
            &binding,
            &["coworker-admission", "preview", "--request", req_str],
        )
        .await
        .status
        .success());
        assert_eq!(snapshot(&f.plans_dir), plans_before);

        // Restore exact frozen target; verify disabled tool, adapter mismatch, and shared
        // System-mode AGY advisory refusal before recording.
        std::os::unix::fs::symlink(&canonical_agy_v1, &agy_link).unwrap();
        {
            let reg_file = f.registry_dir.join("registry.json");
            let saved_reg_bytes = fs::read(&reg_file).unwrap();

            // Disabled Worker tool must be refused.
            let mut reg_mut = f.registry.snapshot().unwrap();
            reg_mut
                .tools
                .iter_mut()
                .find(|t| t.id == "tool-agy")
                .unwrap()
                .enabled = false;
            fs::write(&reg_file, serde_json::to_vec(&reg_mut).unwrap()).unwrap();
            let registry_for_case = snapshot(&f.registry_dir);
            assert!(!cli_output(
                &f,
                &state,
                &binding,
                &["coworker-admission", "preview", "--request", req_str],
            )
            .await
            .status
            .success());
            assert_eq!(snapshot(&f.plans_dir), plans_before);
            assert_eq!(snapshot(&f.registry_dir), registry_for_case);

            // Adapter mismatch on Worker tool must be refused.
            reg_mut
                .tools
                .iter_mut()
                .find(|t| t.id == "tool-agy")
                .unwrap()
                .enabled = true;
            reg_mut
                .tools
                .iter_mut()
                .find(|t| t.id == "tool-agy")
                .unwrap()
                .adapter = ToolAdapter::OpencodeV2;
            fs::write(&reg_file, serde_json::to_vec(&reg_mut).unwrap()).unwrap();
            let registry_for_case = snapshot(&f.registry_dir);
            assert!(!cli_output(
                &f,
                &state,
                &binding,
                &["coworker-admission", "preview", "--request", req_str],
            )
            .await
            .status
            .success());
            assert_eq!(snapshot(&f.plans_dir), plans_before);
            assert_eq!(snapshot(&f.registry_dir), registry_for_case);

            // Shared System-mode AGY tool selected for Reviewer/Closure must still be refused by
            // the advisory resolver (Worker PATH discovery never bypasses advisory rules).
            reg_mut
                .tools
                .iter_mut()
                .find(|t| t.id == "tool-agy")
                .unwrap()
                .adapter = ToolAdapter::Antigravity;
            for assignment in &mut reg_mut.profiles[0].assignments {
                if matches!(
                    assignment.role,
                    WorkflowRole::Reviewer | WorkflowRole::Closure
                ) {
                    assignment.choice = ModelChoice {
                        model_id: "model-argon".to_string(),
                        effort: Some("high".to_string()),
                    };
                }
            }
            fs::write(&reg_file, serde_json::to_vec(&reg_mut).unwrap()).unwrap();
            let registry_for_case = snapshot(&f.registry_dir);
            let mut shared_req = request.clone();
            for rule in &mut shared_req.binding.policy.allow_rules {
                if rule.tool_id == "tool-agy" {
                    rule.roles = vec![
                        WorkflowRole::Coder,
                        WorkflowRole::Reviewer,
                        WorkflowRole::Closure,
                    ];
                }
            }
            shared_req.execution.policy = shared_req.binding.policy.clone();
            let agy_advisory_choice = |difficulty: Difficulty| SelectedRoleChoice {
                tool_id: "tool-agy".to_string(),
                adapter: ToolAdapter::Antigravity,
                route: RoleExecutionRoute::HeadlessWorker,
                model_id: "model-argon".to_string(),
                model_arg: "gemini-4-argon-high".to_string(),
                effort: Some("high".to_string()),
                matched_difficulty: Some(difficulty),
            };
            shared_req.execution.reviewer_choice = agy_advisory_choice(Difficulty::Hard);
            shared_req.execution.closure_choice = agy_advisory_choice(Difficulty::Extreme);
            let cap = shared_req.binding.controller_envelope.as_mut().unwrap();
            cap.allowed_reviewer_choices = vec![agy_advisory_choice(Difficulty::Hard)];
            cap.allowed_closure_choices = vec![agy_advisory_choice(Difficulty::Extreme)];
            let shared_req_path = private_json(&f, "admission-shared-agy.json", &shared_req);
            let shared_out = cli_output(
                &f,
                &state,
                &binding,
                &[
                    "coworker-admission",
                    "preview",
                    "--request",
                    shared_req_path.to_str().unwrap(),
                ],
            )
            .await;
            assert!(!shared_out.status.success());
            assert_eq!(snapshot(&f.plans_dir), plans_before);
            assert_eq!(snapshot(&f.registry_dir), registry_for_case);

            fs::write(&reg_file, &saved_reg_bytes).unwrap();
            assert_eq!(fs::read(&reg_file).unwrap(), saved_reg_bytes);
        }
        let registry_before_record = snapshot(&f.registry_dir);

        let recorded = cli(
            &f,
            &state,
            &binding,
            &[
                "coworker-admission",
                "record",
                "--request",
                req_str,
                "--operator-approved",
                "--confirm-preview",
                pd,
                "--confirm-approval",
                ad,
                "--confirm-envelope",
                ed,
            ],
        )
        .await;
        assert_eq!(recorded["stage"], "complete");
        assert_eq!(recorded["envelope_digest"], ed);
        assert_eq!(snapshot(&f.registry_dir), registry_before_record);
        assert!(
            !probe_marker.exists(),
            "record must never invoke or probe provider"
        );
        assert_no_execution(&f);

        supervisor.shutdown().await;
    })
    .await
}

#[tokio::test]
async fn cli_system_mode_agy_worker_and_codex_advisory_preview_and_record_succeed_and_reject_path_drift(
) {
    use threadlane_workers::{
        run_bounded_command, BoundedCommandBudgets, BoundedCommandSpec, BoundedCommandStatus,
    };

    let tmp = TempDir::new().expect("tempdir for isolated PATH");
    let root = fs::canonicalize(tmp.path()).expect("canonicalize tempdir");
    let isolated_bin = root.join("isolated-system-bin");
    populate_isolated_system_path_dir(&isolated_bin);

    let exe = fs::canonicalize(std::env::current_exe().expect("current_exe"))
        .expect("canonicalize current_exe");
    let isolated_bin_str = isolated_bin
        .to_str()
        .expect("utf8 isolated_bin")
        .to_string();
    let report = run_bounded_command(&BoundedCommandSpec {
        executable: exe,
        argv: vec![
            "--exact".to_string(),
            "cli_system_mode_agy_worker_child_probe".to_string(),
            "--nocapture".to_string(),
        ],
        working_directory: root,
        env_overrides: vec![
            ("PATH".to_string(), isolated_bin_str.clone()),
            (
                "THREADLANE_CLI_SYSTEM_MODE_AGY_BIN".to_string(),
                isolated_bin_str,
            ),
        ],
        strip_git_env: true,
        budgets: BoundedCommandBudgets {
            timeout_ms: 60_000,
            max_stdout_bytes: 262_144,
            max_stderr_bytes: 262_144,
        },
    })
    .await
    .expect("run bounded child probe");
    assert_eq!(
        report.status,
        BoundedCommandStatus::Succeeded,
        "status={:?}\nstdout:\n{}\nstderr:\n{}",
        report.status,
        String::from_utf8_lossy(&report.stdout_bytes),
        String::from_utf8_lossy(&report.stderr_bytes)
    );
    assert_eq!(
        report.exit_code,
        Some(0),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&report.stdout_bytes),
        String::from_utf8_lossy(&report.stderr_bytes)
    );
}
