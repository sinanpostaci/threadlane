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

#[allow(clippy::too_many_arguments)]
async fn create_planned_approved_and_frozen_job(
    fixture: &PipelineTestFixture,
    brief_id: &str,
    plan_id: &str,
    tasks: Vec<PlanTask>,
    task_interfaces: BTreeMap<String, TaskInterfaceOwnershipSpec>,
    approved_write_paths: Vec<String>,
    max_parallel: usize,
    approval_budgets: ExecutionBudgets,
    coder_budget: ControllerRoleExecutionBudget,
    default_integration_strategy: IntegrationStrategy,
    include_completeness_receipt: bool,
) -> (CoworkerJobRecord, String) {
    create_planned_approved_and_frozen_job_with_correction_caps(
        fixture,
        brief_id,
        plan_id,
        tasks,
        task_interfaces,
        approved_write_paths,
        max_parallel,
        approval_budgets,
        coder_budget,
        default_integration_strategy,
        include_completeness_receipt,
        1,
        1,
        2,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn create_planned_approved_and_frozen_job_with_correction_caps(
    fixture: &PipelineTestFixture,
    brief_id: &str,
    plan_id: &str,
    tasks: Vec<PlanTask>,
    task_interfaces: BTreeMap<String, TaskInterfaceOwnershipSpec>,
    approved_write_paths: Vec<String>,
    max_parallel: usize,
    approval_budgets: ExecutionBudgets,
    coder_budget: ControllerRoleExecutionBudget,
    default_integration_strategy: IntegrationStrategy,
    include_completeness_receipt: bool,
    max_in_scope_correction_attempts: u32,
    max_in_scope_corrections_per_task: u32,
    max_total_corrections_per_job: u32,
) -> (CoworkerJobRecord, String) {
    Box::pin(async move {
        let pb = project_base(fixture);
        let reg_snap = fixture.registry.snapshot().expect("registry snapshot");
        let brief = sample_brief(fixture, brief_id);

        let job = fixture
            .workflow_store
            .create_or_amend_job(&CreateCoworkerJobRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
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
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
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
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
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
            fixture
                .planning_service
                .orchestrate_brief_to_plan(&fixture.scoped_workflow, &orch_req),
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
        let _verified_artifact = fixture
            .planning_service
            .read_planning_artifact(&published_outcome.planning_result.report_artifact_id)
            .expect("verify persisted planning artifact on disk");

        let job = fixture
            .workflow_store
            .read_binding_ledger(BINDING_A)
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
                    binding_id: BINDING_A.to_string(),
                    thread_uuid: THREAD_UUID_A.to_string(),
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
            max_in_scope_correction_attempts,
            allowed_check_ids: vec!["cargo_test".to_string()],
            local_git_integration_allowed: true,
            allow_check_failure_corrections: false,
        };

        let bound_job = fixture
            .workflow_store
            .bind_job_approval(
                &BindJobApprovalRequest {
                    binding_id: BINDING_A.to_string(),
                    thread_uuid: THREAD_UUID_A.to_string(),
                    job_id: job.job_id,
                    expected_job_revision: job.job_revision,
                    expected_ledger_revision: None,
                    approval_id: approval_summary.approval_id.clone(),
                    policy: execution_policy(),
                    controller_envelope: Some(cap_env),
                    task_interfaces,
                },
                &fixture.authority,
            )
            .expect("bind job approval");

        let completeness_receipt = include_completeness_receipt
            .then(|| build_independence_completeness_receipt(&bound_job));

        let mut role_budgets = BTreeMap::new();
        role_budgets.insert(WorkflowRole::Coder, coder_budget);
        role_budgets.insert(
            WorkflowRole::Reviewer,
            ControllerRoleExecutionBudget::default_for_role(WorkflowRole::Reviewer, 6),
        );
        role_budgets.insert(
            WorkflowRole::Closure,
            ControllerRoleExecutionBudget::default_for_role(WorkflowRole::Closure, 2),
        );

        let preview_req = FreezeControllerExecutionEnvelopeRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            job_id: bound_job.job_id.clone(),
            expected_job_revision: bound_job.job_revision,
            policy: execution_policy(),
            reviewer_choice: headless_argon_choice(Difficulty::Hard),
            closure_choice: headless_argon_choice(Difficulty::Extreme),
            role_budgets,
            max_in_scope_corrections_per_task,
            max_total_corrections_per_job,
            max_total_task_attempts_per_job: 6,
            check_commands: default_check_commands(fixture),
            allow_task_worktree_commits: true,
            allow_run_integration_execution: true,
            default_integration_strategy,
            independence_completeness_receipt: completeness_receipt,
            shadow_jev_policy: None,
            created_at_ms: 1_700_000_100_000,
            operator_attested: false,
            confirm_approval_subject_digest: None,
            confirm_envelope_digest: None,
        };
        let preview: ControllerConfigurationPreview = fixture
            .policy_store
            .preview_execution_envelope(&preview_req, &bound_job, &fixture.authority)
            .expect("preview execution envelope");
        let mut freeze_req = preview_req;
        freeze_req.operator_attested = true;
        freeze_req.confirm_approval_subject_digest = Some(preview.approval_subject_digest);
        freeze_req.confirm_envelope_digest = Some(preview.envelope_digest);
        fixture
            .policy_store
            .record_execution_envelope(&freeze_req, &bound_job, &fixture.authority)
            .expect("record execution envelope");

        (bound_job, approval_summary.approval_id)
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn automatic_pipeline_drives_parallel_roots_survives_held_reviewer_tick_runs_dependent_and_completes_once_idempotently(
) {
    Box::pin(async {
        let fixture = setup_fixture();

        let tasks = vec![
            PlanTask {
                id: "task-root-a".to_string(),
                title: "Root task A".to_string(),
                instruction: "CTRL_TASK_A implement src/a.rs".to_string(),
                role: WorkflowRole::Coder,
                difficulty: Difficulty::Hard,
                depends_on: Vec::new(),
                allowed_write_paths: vec!["src/a.rs".to_string()],
            },
            PlanTask {
                id: "task-root-b".to_string(),
                title: "Root task B".to_string(),
                instruction: "CTRL_TASK_B implement src/b.rs".to_string(),
                role: WorkflowRole::Coder,
                difficulty: Difficulty::Hard,
                depends_on: Vec::new(),
                allowed_write_paths: vec!["src/b.rs".to_string()],
            },
            PlanTask {
                id: "task-dep-c".to_string(),
                title: "Dependent task C".to_string(),
                instruction: "CTRL_TASK_C implement src/c.rs after verifying predecessor files"
                    .to_string(),
                role: WorkflowRole::Coder,
                difficulty: Difficulty::Hard,
                depends_on: vec!["task-root-a".to_string(), "task-root-b".to_string()],
                allowed_write_paths: vec!["src/c.rs".to_string()],
            },
        ];

        let mut ifaces = BTreeMap::new();
        ifaces.insert(
            "task-root-a".to_string(),
            TaskInterfaceOwnershipSpec {
                owned_interfaces: vec!["iface_a".to_string()],
                consumed_interfaces: Vec::new(),
            },
        );
        ifaces.insert(
            "task-root-b".to_string(),
            TaskInterfaceOwnershipSpec {
                owned_interfaces: vec!["iface_b".to_string()],
                consumed_interfaces: Vec::new(),
            },
        );
        ifaces.insert(
            "task-dep-c".to_string(),
            TaskInterfaceOwnershipSpec {
                owned_interfaces: vec!["iface_c".to_string()],
                consumed_interfaces: vec!["iface_a".to_string(), "iface_b".to_string()],
            },
        );

        let (job, approval_id) = create_planned_approved_and_frozen_job(
            &fixture,
            "brf_pipe_dag_3",
            "plan-pipe-dag-3",
            tasks,
            ifaces,
            vec![
                "src/a.rs".to_string(),
                "src/b.rs".to_string(),
                "src/c.rs".to_string(),
            ],
            2, // max_parallel = 2
            ExecutionBudgets::default(),
            ControllerRoleExecutionBudget::default_for_role(WorkflowRole::Coder, 4),
            IntegrationStrategy::MergeNoFf,
            true,
        )
        .await;

        assert_eq!(read_invocation_count(&fixture.agy_planning_count_file), 1);
        assert_eq!(read_invocation_count(&fixture.agy_coder_count_file), 0);
        assert_eq!(read_invocation_count(&fixture.agy_reviewer_count_file), 0);
        assert_eq!(read_invocation_count(&fixture.agy_closure_count_file), 0);

        let host_handle = Box::pin(ApprovedHost::start(ApprovedHostConfig::new(
            &fixture.registry_dir,
            &fixture.plans_dir,
            &fixture.authority_dir,
            &fixture.sessions_dir,
            &approval_id,
        )))
        .await
        .expect("start ApprovedHost session");
        assert_eq!(host_handle.summary().phase, HostPhase::Ready);

    // Hold Reviewer for `task-root-a` across short ticks while `task-root-b` advances
    // independently through Coder -> TaskVerification -> Reviewer -> TaskIntegration.
    // Timing hierarchy:
    // - Measured local Coder A/B + Check A/B + Reviewer A/B + Integration B: ~30-35s
    // - Parent progress watchdog (`mid_report` loop): 90s
    // - Held-Reviewer orphan cleanup fallback (`hold_reviewer_task-root-a.gate`): 180s (180_000 ms)
    // - Frozen Reviewer per-call budget (`DEFAULT_TIMEOUT_MS`): 300s (300_000 ms)
    let frozen_reviewer_timeout_ms =
        ControllerRoleExecutionBudget::default_for_role(WorkflowRole::Reviewer, 4)
            .max_timeout_ms_per_call;
    assert_eq!(frozen_reviewer_timeout_ms, 300_000);
    let held_reviewer_fallback_ms: u64 = 180_000;
    let parent_watchdog_duration = Duration::from_secs(90);
    assert!(
        parent_watchdog_duration.as_millis() < u128::from(held_reviewer_fallback_ms)
            && held_reviewer_fallback_ms < frozen_reviewer_timeout_ms
    );

    let hold_gate_a = fixture.root.join("hold_reviewer_task-root-a.gate");
    let entered_marker_a = fixture.root.join("reviewer_entered_task-root-a.marker");
    let exited_marker_a = fixture.root.join("reviewer_exited_task-root-a.marker");
    fs::write(
        &hold_gate_a,
        format!("{{\"fallback_timeout_ms\":{held_reviewer_fallback_ms}}}\n"),
    )
    .expect("create hold gate for task-root-a Reviewer");

    let session = fixture
        .pipeline
        .open_owned_session(BINDING_A, THREAD_UUID_A, &job.job_id, 20_000)
        .expect("open owned pipeline session");
    let mut driver = session
        .driver()
        .expect("create retained stateful pipeline driver")
        .with_drain_window_ms(40);

    let loop_started = Instant::now();
    let deadline = loop_started + parent_watchdog_duration;
    let mut now_ms = 20_100u64;
    let mid_report = loop {
        let report = driver.tick(now_ms).await.expect("tick retained driver");
        now_ms += 100;
        let b_integrated = report
            .tasks
            .iter()
            .find(|t| t.task_id == "task-root-b")
            .is_some_and(|t| t.stage == CoworkerTaskStage::Integrated);
        if entered_marker_a.is_file() && b_integrated {
            break report;
        }
        assert!(
            Instant::now() < deadline,
            "timed out after {:?} waiting for held task-root-a Reviewer and integrated task-root-b; {}; last report: {report:?}",
            loop_started.elapsed(),
            format_fixture_counter_diagnostics(&fixture),
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    };

    assert!(
        !exited_marker_a.is_file(),
        "held task-root-a Reviewer must still be held before gate removal; {}",
        format_fixture_counter_diagnostics(&fixture)
    );

    assert_eq!(mid_report.status, CoworkerPipelineStatus::Running);
    assert_eq!(driver.in_flight_count(), 1);
    assert_eq!(mid_report.in_flight_stages.len(), 1);
    assert_eq!(
        mid_report.in_flight_stages[0].task_id.as_deref(),
        Some("task-root-a")
    );
    assert_eq!(mid_report.in_flight_stages[0].attempt_no, Some(1));
    assert_eq!(
        mid_report.in_flight_stages[0].kind,
        CoworkerPipelineInFlightStageKind::TaskReview
    );

    let snap_a_mid = mid_report
        .tasks
        .iter()
        .find(|t| t.task_id == "task-root-a")
        .expect("task-root-a snapshot");
    assert_eq!(
        snap_a_mid.in_flight_stage,
        Some(CoworkerPipelineInFlightStageKind::TaskReview)
    );
    assert!(snap_a_mid.review_subject_promoted);
    assert!(snap_a_mid.review_receipt_id.is_none());

    let snap_b_mid = mid_report
        .tasks
        .iter()
        .find(|t| t.task_id == "task-root-b")
        .expect("task-root-b snapshot");
    assert_eq!(snap_b_mid.stage, CoworkerTaskStage::Integrated);
    assert_eq!(
        snap_b_mid.review_verdict_outcome,
        Some(ReviewVerdictOutcome::Accept)
    );
    assert!(snap_b_mid.review_receipt_id.is_some());
    assert!(snap_b_mid.integration_receipt_id.is_some());

    let snap_c_mid = mid_report
        .tasks
        .iter()
        .find(|t| t.task_id == "task-dep-c")
        .expect("task-dep-c snapshot");
    assert_eq!(snap_c_mid.stage, CoworkerTaskStage::PendingDependencies);

    // Verify that `task-root-a`'s held Reviewer outlives another short tick on the retained driver
    // without losing supervision or respawning a duplicate Reviewer process.
    let mid_report_2 = driver
        .tick(now_ms)
        .await
        .expect("second short tick with held Reviewer");
    now_ms += 100;
    assert_eq!(mid_report_2.status, CoworkerPipelineStatus::Running);
    assert_eq!(driver.in_flight_count(), 1);
    assert_eq!(
        read_invocation_count(&fixture.root.join("reviewer_count_task-root-a.txt")),
        1
    );
    assert!(
        !exited_marker_a.is_file(),
        "held task-root-a Reviewer must still be held after second short tick; {}",
        format_fixture_counter_diagnostics(&fixture)
    );

    // Release the held Reviewer for `task-root-a` and drive the SAME retained driver until settled.
    fs::remove_file(&hold_gate_a).expect("release hold_reviewer_task-root-a.gate");
    let final_report = driver
        .drive_until_settled(now_ms)
        .await
        .expect("drive retained driver until settled");

    let exited_a_val: serde_json::Value = serde_json::from_str(
        &fs::read_to_string(&exited_marker_a).expect("read reviewer_exited_task-root-a.marker"),
    )
    .expect("parse reviewer_exited_task-root-a.marker");
    assert_eq!(exited_a_val["released_by_gate_removal"].as_bool(), Some(true));

    assert_eq!(final_report.status, CoworkerPipelineStatus::Completed);
    assert_eq!(final_report.job_phase, CoworkerJobPhase::Completed);
    assert_eq!(driver.in_flight_count(), 0);
    assert!(final_report.in_flight_stages.is_empty());

    // Exact role and check execution counts:
    // - 1 Orchestrator planning call
    // - 3 Coder task executions (2 parallel roots + 1 dependent)
    // - 3 Reviewer invocations (exact 1 per task, even though task-root-a outlived short ticks)
    // - 1 Closure invocation
    // - 4 frozen check runs (3 per-task verification checks + 1 combined candidate check)
    assert_eq!(read_invocation_count(&fixture.agy_planning_count_file), 1);
    assert_eq!(read_invocation_count(&fixture.agy_coder_count_file), 3);
    assert_eq!(read_invocation_count(&fixture.agy_reviewer_count_file), 3);
    assert_eq!(
        read_invocation_count(&fixture.root.join("reviewer_count_task-root-a.txt")),
        1
    );
    assert_eq!(
        read_invocation_count(&fixture.root.join("reviewer_count_task-root-b.txt")),
        1
    );
    assert_eq!(
        read_invocation_count(&fixture.root.join("reviewer_count_task-dep-c.txt")),
        1
    );
    assert_eq!(read_invocation_count(&fixture.agy_closure_count_file), 1);
    assert_eq!(read_invocation_count(&fixture.check_count_file), 4);

    // Verify all 3 Reviewer invocations bound the exact 9-field ExpectedTaskReviewIdentity and
    // kept logical plan task_ids distinct from compact host/manifest ReviewSubject.task_id UUIDs.
    let inv_raw = fs::read_to_string(&fixture.reviewer_invocations_file)
        .expect("read reviewer_invocations.jsonl");
    let inv_lines: Vec<serde_json::Value> = inv_raw
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| serde_json::from_str(l).expect("parse reviewer invocation jsonl"))
        .collect();
    assert_eq!(inv_lines.len(), 3);
    let mut seen_reviewer_tasks = BTreeSet::new();
    for entry in &inv_lines {
        let logical_tid = entry["task_id"].as_str().expect("logical task_id");
        let subject_tid = entry["subject_task_id"]
            .as_str()
            .expect("compact ReviewSubject.task_id");
        assert_ne!(
            logical_tid, subject_tid,
            "logical plan task_id must be distinct from compact manifest ReviewSubject.task_id"
        );
        seen_reviewer_tasks.insert(logical_tid.to_string());
        let expected_id = &entry["expected_identity"];
        for field in [
            "task_id",
            "attempt_no",
            "candidate_id",
            "review_subject_digest",
            "base_commit",
            "task_head_commit",
            "task_tree_sha",
            "diff_digest",
            "policy_digest",
        ] {
            assert!(
                !expected_id[field].is_null(),
                "ExpectedTaskReviewIdentity field `{field}` must be populated"
            );
        }
    }
    assert_eq!(
        seen_reviewer_tasks,
        BTreeSet::from([
            "task-root-a".to_string(),
            "task-root-b".to_string(),
            "task-dep-c".to_string()
        ])
    );

    // Verify every task snapshot in `final_report` is Integrated with verified receipts.
    for tid in ["task-root-a", "task-root-b", "task-dep-c"] {
        let snap = final_report
            .tasks
            .iter()
            .find(|t| t.task_id == tid)
            .expect("task snapshot in final report");
        assert_eq!(snap.stage, CoworkerTaskStage::Integrated);
        assert_eq!(snap.host_status, TaskExecutionStatus::ReadyForReview);
        assert!(!snap.fenced);
        assert!(snap.in_flight_stage.is_none());
        assert!(snap.review_subject_promoted);
        assert_eq!(
            snap.review_verdict_outcome,
            Some(ReviewVerdictOutcome::Accept)
        );
        assert!(snap.review_receipt_id.is_some());
        assert!(snap.integration_receipt_id.is_some());
        assert!(!snap.awaiting_correction_execution);
        assert!(snap.failed_check_ids.is_empty());
    }

    // Verify dependent task `task-dep-c` has a prepared `DependencyInputSeal` on the host binding both roots.
    let host_client = ApprovedHostClient::new(
        Arc::clone(&fixture.authority),
        &fixture.sessions_dir,
        BINDING_A,
        THREAD_UUID_A,
    )
    .expect("create ApprovedHostClient");
    let host_snap_c = host_client
        .get_task(&approval_id, "task-dep-c")
        .await
        .expect("get_task task-dep-c");
    let dep_input_c = host_snap_c
        .dependency_input
        .expect("dependency_input for task-dep-c must exist");
    assert_eq!(
        dep_input_c.predecessor_task_ids,
        vec!["task-root-a".to_string(), "task-root-b".to_string()]
    );
    assert!(!dep_input_c.input_digest.is_empty());

    // Verify original per-task integration receipts remain distinct from final job aggregation receipts.
    let runtime_state = fixture
        .policy_store
        .get_job_runtime_readonly(BINDING_A, &job.job_id)
        .expect("read job runtime")
        .expect("job runtime exists");
    let completion = final_report
        .completion_outcome
        .as_ref()
        .expect("completion_outcome must be present on Completed report");
    assert_eq!(completion.status, CoworkerJobCompletionStatus::Completed);
    let closure_verdict = completion
        .closure_verdict
        .as_ref()
        .expect("closure_verdict must be present");
    assert_eq!(closure_verdict.kind, ClosureVerdictKind::Accepted);

    let (expected_run_id, _expected_token, expected_wt_id, _expected_branch) =
        derive_final_job_integration_identifiers(
            &job.job_id,
            &approval_id,
            &runtime_state.envelope.approval_subject_digest,
        );

    for tid in ["task-root-a", "task-root-b", "task-dep-c"] {
        let art_key = ControllerJobRuntimeState::artifact_key(tid, 1);
        let artifacts = runtime_state
            .attempt_artifacts
            .get(&art_key)
            .expect("attempt artifacts for task");
        let orig_receipt: &IntegrationReceipt = artifacts
            .integration_receipt
            .as_ref()
            .expect("original per-task IntegrationReceipt");
        assert!(
            orig_receipt.job_aggregation.is_none(),
            "original per-task IntegrationReceipt must have job_aggregation == None"
        );
        let expected_agg_rcp_id = derive_job_aggregation_receipt_id(
            &job.job_id,
            &expected_run_id,
            &expected_wt_id,
            tid,
            &orig_receipt.review_receipt_id,
        );
        let agg_receipt =
            load_integration_receipt(host_handle.work_area_dir(), &expected_agg_rcp_id)
                .expect("load aggregated IntegrationReceipt");
        assert!(
            agg_receipt.job_aggregation.is_some(),
            "aggregated IntegrationReceipt must carry job_aggregation metadata"
        );
        assert_ne!(orig_receipt.receipt_id, agg_receipt.receipt_id);
    }

    // Verify final RunIntegration worktree contains all 3 tasks' files and matches candidate head.
    let final_wt = &completion.aggregation.integration_checkout_path;
    assert_eq!(
        fs::read_to_string(final_wt.join("src/a.rs")).expect("read aggregated src/a.rs"),
        "pub fn a() -> u32 { 100 }\n"
    );
    assert_eq!(
        fs::read_to_string(final_wt.join("src/b.rs")).expect("read aggregated src/b.rs"),
        "pub fn b() -> u32 { 200 }\n"
    );
    assert_eq!(
        fs::read_to_string(final_wt.join("src/c.rs")).expect("read aggregated src/c.rs"),
        "pub fn c() -> u32 { 300 }\n"
    );
    let actual_final_head =
        resolve_exact_commit_sha(final_wt, "HEAD", DEFAULT_GIT_COMMAND_TIMEOUT_MS)
            .expect("resolve final wt HEAD");
    assert_eq!(
        actual_final_head,
        completion.aggregation.final_integrated_target_commit
    );
    let combined_cand = completion
        .combined_candidate
        .as_ref()
        .expect("combined_candidate must exist");
    assert_eq!(
        combined_cand.final_commit_sha.as_deref(),
        Some(actual_final_head.as_str())
    );

    // Verify source repository HEAD and working tree were never mutated or merged.
    assert_eq!(
        resolve_exact_commit_sha(&fixture.repo_dir, "HEAD", DEFAULT_GIT_COMMAND_TIMEOUT_MS)
            .expect("source repo HEAD"),
        fixture.base_commit
    );
    assert_eq!(
        fs::read_to_string(fixture.repo_dir.join("src/a.rs")).expect("source src/a.rs"),
        "pub fn a() -> u32 { 1 }\n"
    );

    let completed_revision = final_report.job_revision;
    drop(driver);
    session.release().expect("release owned pipeline session");

    // Reopen completed job session and verify driving is a clean idempotent no-op.
    let reopened = fixture
        .pipeline
        .open_owned_session(BINDING_A, THREAD_UUID_A, &job.job_id, now_ms + 1_000)
        .expect("reopen owned pipeline session on Completed job");
    let rerun_report = reopened
        .drive_until_settled(now_ms + 1_000)
        .await
        .expect("drive reopened Completed job");
    assert_eq!(rerun_report.status, CoworkerPipelineStatus::Completed);
    assert_eq!(rerun_report.job_phase, CoworkerJobPhase::Completed);
    assert_eq!(rerun_report.job_revision, completed_revision);
    assert_eq!(read_invocation_count(&fixture.agy_coder_count_file), 3);
    assert_eq!(read_invocation_count(&fixture.agy_reviewer_count_file), 3);
    assert_eq!(read_invocation_count(&fixture.agy_closure_count_file), 1);
    assert_eq!(read_invocation_count(&fixture.check_count_file), 4);
    reopened.release().expect("release reopened session");

    host_handle.shutdown().await.expect("shutdown host");
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn automatic_pipeline_stops_at_awaiting_correction_execution_on_correct_in_scope_without_reserving_second_attempt(
) {
    Box::pin(async {
        let fixture = setup_fixture();

        let fix_tasks = vec![PlanTask {
            id: "task-fix-a".to_string(),
            title: "Task requiring in-scope fix".to_string(),
            instruction: "CTRL_TASK_A implement src/a.rs".to_string(),
            role: WorkflowRole::Coder,
            difficulty: Difficulty::Hard,
            depends_on: Vec::new(),
            allowed_write_paths: vec!["src/a.rs".to_string()],
        }];
        let mut fix_ifaces = BTreeMap::new();
        fix_ifaces.insert(
            "task-fix-a".to_string(),
            TaskInterfaceOwnershipSpec {
                owned_interfaces: vec!["iface_fix_a".to_string()],
                consumed_interfaces: Vec::new(),
            },
        );

        let (fix_job, fix_approval_id) =
            create_planned_approved_and_frozen_job_with_correction_caps(
                &fixture,
                "brf_pipe_fix",
                "plan-pipe-fix",
                fix_tasks,
                fix_ifaces,
                vec!["src/a.rs".to_string()],
                1,
                ExecutionBudgets::default(),
                ControllerRoleExecutionBudget::default_for_role(WorkflowRole::Coder, 4),
                IntegrationStrategy::FastForwardOnly,
                true,
                0,
                0,
                0,
            )
            .await;

        let host_handle = Box::pin(ApprovedHost::start(ApprovedHostConfig::new(
            &fixture.registry_dir,
            &fixture.plans_dir,
            &fixture.authority_dir,
            &fixture.sessions_dir,
            &fix_approval_id,
        )))
        .await
        .expect("start ApprovedHost for fix_job");

        // Configure the fake Reviewer to return a valid 9-field `correct_in_scope` verdict for `task-fix-a`.
        let override_path = fixture.root.join("reviewer_override_task-fix-a.json");
        fs::write(
            &override_path,
            serde_json::to_string(&serde_json::json!({
                "outcome": "correct_in_scope",
                "findings": ["a() returns 100 instead of 101"],
                "required_in_scope_fixes": ["Update a() in src/a.rs to return 101"],
                "summary": "Requires in-scope adjustment to return value."
            }))
            .expect("serialize override"),
        )
        .expect("write reviewer override for task-fix-a");

        let fix_session = fixture
            .pipeline
            .open_owned_session(BINDING_A, THREAD_UUID_A, &fix_job.job_id, 20_000)
            .expect("open session for fix_job");
        let fix_report = fix_session
            .drive_until_settled(20_000)
            .await
            .expect("drive fix_job until settled");

        assert_eq!(
            fix_report.status,
            CoworkerPipelineStatus::AwaitingCorrectionExecution
        );
        assert_eq!(fix_report.job_phase, CoworkerJobPhase::Executing);
        assert_eq!(
            fix_report.awaiting_correction_task_ids,
            vec!["task-fix-a".to_string()]
        );
        assert!(fix_report.integrated_task_ids.is_empty());
        assert!(fix_report.completion_outcome.is_none());

        let snap_fix = fix_report
            .tasks
            .iter()
            .find(|t| t.task_id == "task-fix-a")
            .expect("task-fix-a snapshot");
        assert_eq!(snap_fix.stage, CoworkerTaskStage::CorrectInScopeRequired);
        assert_eq!(snap_fix.attempt_no, 1);
        assert!(snap_fix.awaiting_correction_execution);
        assert_eq!(
            snap_fix.review_verdict_outcome,
            Some(ReviewVerdictOutcome::CorrectInScope)
        );
        assert!(
            snap_fix.review_receipt_id.is_none(),
            "CorrectInScope must never produce an acceptance ReviewReceipt"
        );
        assert!(snap_fix.integration_receipt_id.is_none());

        // Verify no second Coder attempt was reserved or dispatched when frozen correction cap is 0.
        assert_eq!(read_invocation_count(&fixture.agy_coder_count_file), 1);
        assert_eq!(read_invocation_count(&fixture.agy_reviewer_count_file), 1);
        assert_eq!(read_invocation_count(&fixture.agy_closure_count_file), 0);

        let fix_runtime = fixture
            .policy_store
            .get_job_runtime_readonly(BINDING_A, &fix_job.job_id)
            .expect("read fix_job runtime")
            .expect("fix_job runtime exists");
        assert_eq!(fix_runtime.total_corrections_reserved, 0);
        assert_eq!(fix_runtime.total_task_attempts_reserved, 1);
        assert!(fix_runtime.derived_attempts.is_empty());
        let coder_usage = fix_runtime
            .role_usage
            .get(&WorkflowRole::Coder)
            .expect("coder usage");
        assert_eq!(coder_usage.calls_reserved, 1);
        assert_eq!(coder_usage.calls_completed, 1);

        // Driving again while awaiting correction execution is a settled no-op.
        let fix_report_2 = fix_session
            .drive_until_settled(21_000)
            .await
            .expect("drive fix_job second time");
        assert_eq!(
            fix_report_2.status,
            CoworkerPipelineStatus::AwaitingCorrectionExecution
        );
        assert_eq!(read_invocation_count(&fixture.agy_coder_count_file), 1);
        assert_eq!(read_invocation_count(&fixture.agy_reviewer_count_file), 1);

        fix_session.release().expect("release fix_session");
        host_handle.shutdown().await.expect("shutdown host_handle");
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn automatic_pipeline_executes_derived_in_scope_correction_verifies_reviews_integrates_and_runs_dependent_to_completion(
) {
    Box::pin(async {
        let fixture = setup_fixture();

        let tasks = vec![
            PlanTask {
                id: "task-root-a".to_string(),
                title: "Root task A with in-scope correction".to_string(),
                instruction: "CTRL_TASK_A implement src/a.rs".to_string(),
                role: WorkflowRole::Coder,
                difficulty: Difficulty::Hard,
                depends_on: Vec::new(),
                allowed_write_paths: vec!["src/a.rs".to_string()],
            },
            PlanTask {
                id: "task-root-b".to_string(),
                title: "Root task B independent".to_string(),
                instruction: "CTRL_TASK_B implement src/b.rs".to_string(),
                role: WorkflowRole::Coder,
                difficulty: Difficulty::Hard,
                depends_on: Vec::new(),
                allowed_write_paths: vec!["src/b.rs".to_string()],
            },
            PlanTask {
                id: "task-dep-c".to_string(),
                title: "Dependent task C requiring corrected A101 and B200".to_string(),
                instruction:
                    "CTRL_TASK_C_REQUIRES_A101 implement src/c.rs after verifying corrected 101 in src/a.rs and 200 in src/b.rs"
                        .to_string(),
                role: WorkflowRole::Coder,
                difficulty: Difficulty::Hard,
                depends_on: vec!["task-root-a".to_string(), "task-root-b".to_string()],
                allowed_write_paths: vec!["src/c.rs".to_string()],
            },
        ];

        let mut ifaces = BTreeMap::new();
        ifaces.insert(
            "task-root-a".to_string(),
            TaskInterfaceOwnershipSpec {
                owned_interfaces: vec!["iface_a".to_string()],
                consumed_interfaces: Vec::new(),
            },
        );
        ifaces.insert(
            "task-root-b".to_string(),
            TaskInterfaceOwnershipSpec {
                owned_interfaces: vec!["iface_b".to_string()],
                consumed_interfaces: Vec::new(),
            },
        );
        ifaces.insert(
            "task-dep-c".to_string(),
            TaskInterfaceOwnershipSpec {
                owned_interfaces: vec!["iface_c".to_string()],
                consumed_interfaces: vec!["iface_a".to_string(), "iface_b".to_string()],
            },
        );

        let (job, approval_id) = create_planned_approved_and_frozen_job(
            &fixture,
            "brf_pipe_corr_dag_3",
            "plan-pipe-corr-dag-3",
            tasks,
            ifaces,
            vec![
                "src/a.rs".to_string(),
                "src/b.rs".to_string(),
                "src/c.rs".to_string(),
            ],
            2, // max_parallel = 2
            ExecutionBudgets::default(),
            ControllerRoleExecutionBudget::default_for_role(WorkflowRole::Coder, 5),
            IntegrationStrategy::MergeNoFf,
            true,
        )
        .await;

        let host_handle = Box::pin(ApprovedHost::start(ApprovedHostConfig::new(
            &fixture.registry_dir,
            &fixture.plans_dir,
            &fixture.authority_dir,
            &fixture.sessions_dir,
            &approval_id,
        )))
        .await
        .expect("start ApprovedHost session");
        assert_eq!(host_handle.summary().phase, HostPhase::Ready);

        // Configure attempt 1 of `task-root-a` to receive a strict 9-field `correct_in_scope`
        // verdict, while attempt 2 of `task-root-a` receives `accept`.
        let attempt_1_override_path = fixture
            .root
            .join("reviewer_override_task-root-a_attempt_1.json");
        fs::write(
            &attempt_1_override_path,
            serde_json::to_string(&serde_json::json!({
                "outcome": "correct_in_scope",
                "findings": ["a() in src/a.rs returns 100 instead of 101"],
                "required_in_scope_fixes": ["Update pub fn a() in src/a.rs to return 101"],
                "summary": "Attempt 1 requires in-scope correction in src/a.rs from 100 to 101."
            }))
            .expect("serialize attempt 1 reviewer override"),
        )
        .expect("write reviewer_override_task-root-a_attempt_1.json");

        let session = fixture
            .pipeline
            .open_owned_session(BINDING_A, THREAD_UUID_A, &job.job_id, 20_000)
            .expect("open owned pipeline session");
        let final_report = session
            .drive_until_settled(20_000)
            .await
            .expect("drive pipeline with derived correction until settled");

        assert_eq!(final_report.status, CoworkerPipelineStatus::Completed);
        assert_eq!(final_report.job_phase, CoworkerJobPhase::Completed);
        assert!(final_report.in_flight_stages.is_empty());
        assert!(final_report.awaiting_correction_task_ids.is_empty());
        assert!(final_report.failed_check_task_ids.is_empty());
        assert!(final_report.unknown_effect_task_ids.is_empty());
        assert!(final_report.blocked_or_failed_task_ids.is_empty());

        // Genuine role and check counts:
        // - 1 Orchestrator planning call
        // - 4 Coder executions: A1, B1, derived A2, dependent C1
        // - 4 Reviewer invocations: A1 (`CorrectInScope`), B1 (`Accept`), A2 (`Accept`), C1 (`Accept`)
        // - 1 Closure invocation (`Accepted`)
        // - 5 frozen check runs: A1, B1, A2, C1, + 1 combined candidate check
        assert_eq!(read_invocation_count(&fixture.agy_planning_count_file), 1);
        assert_eq!(read_invocation_count(&fixture.agy_coder_count_file), 4);
        assert_eq!(
            read_invocation_count(&fixture.root.join("agy_derived_coder_count.txt")),
            1
        );
        assert_eq!(read_invocation_count(&fixture.agy_reviewer_count_file), 4);
        assert_eq!(
            read_invocation_count(&fixture.root.join("reviewer_count_task-root-a.txt")),
            2
        );
        assert_eq!(
            read_invocation_count(&fixture.root.join("reviewer_count_task-root-a_attempt_1.txt")),
            1
        );
        assert_eq!(
            read_invocation_count(&fixture.root.join("reviewer_count_task-root-a_attempt_2.txt")),
            1
        );
        assert_eq!(
            read_invocation_count(&fixture.root.join("reviewer_count_task-root-b.txt")),
            1
        );
        assert_eq!(
            read_invocation_count(&fixture.root.join("reviewer_count_task-dep-c.txt")),
            1
        );
        assert_eq!(read_invocation_count(&fixture.agy_closure_count_file), 1);
        assert_eq!(read_invocation_count(&fixture.check_count_file), 5);

        // Verify task snapshots in `final_report`.
        let snap_a = final_report
            .tasks
            .iter()
            .find(|t| t.task_id == "task-root-a")
            .expect("task-root-a snapshot");
        assert_eq!(snap_a.stage, CoworkerTaskStage::Integrated);
        assert_eq!(snap_a.attempt_no, 2);
        assert!(!snap_a.fenced);
        assert!(snap_a.in_flight_stage.is_none());
        assert!(snap_a.review_subject_promoted);
        assert_eq!(
            snap_a.review_verdict_outcome,
            Some(ReviewVerdictOutcome::Accept)
        );
        assert!(snap_a.review_receipt_id.is_some());
        assert!(snap_a.integration_receipt_id.is_some());
        assert!(!snap_a.awaiting_correction_execution);

        for tid in ["task-root-b", "task-dep-c"] {
            let snap = final_report
                .tasks
                .iter()
                .find(|t| t.task_id == tid)
                .expect("task snapshot");
            assert_eq!(snap.stage, CoworkerTaskStage::Integrated);
            assert_eq!(snap.attempt_no, 1);
            assert!(!snap.fenced);
            assert!(snap.in_flight_stage.is_none());
            assert!(snap.review_subject_promoted);
            assert_eq!(
                snap.review_verdict_outcome,
                Some(ReviewVerdictOutcome::Accept)
            );
            assert!(snap.review_receipt_id.is_some());
            assert!(snap.integration_receipt_id.is_some());
            assert!(!snap.awaiting_correction_execution);
        }

        // Verify persisted runtime_state derived attempt receipt, role usage, and attempt 1 vs 2 artifacts.
        let runtime_state = fixture
            .policy_store
            .get_job_runtime_readonly(BINDING_A, &job.job_id)
            .expect("read job runtime")
            .expect("job runtime exists");
        assert_eq!(runtime_state.total_corrections_reserved, 1);
        assert_eq!(runtime_state.total_task_attempts_reserved, 4);
        assert_eq!(runtime_state.derived_attempts.len(), 1);
        let derived_receipt = &runtime_state.derived_attempts[0];
        assert_eq!(derived_receipt.task_id, "task-root-a");
        assert_eq!(derived_receipt.attempt_no, 2);

        let coder_usage = runtime_state
            .role_usage
            .get(&WorkflowRole::Coder)
            .expect("Coder usage");
        assert_eq!(coder_usage.calls_reserved, 4);
        assert_eq!(coder_usage.calls_completed, 4);
        let reviewer_usage = runtime_state
            .role_usage
            .get(&WorkflowRole::Reviewer)
            .expect("Reviewer usage");
        assert_eq!(reviewer_usage.calls_reserved, 4);
        assert_eq!(reviewer_usage.calls_completed, 4);
        let closure_usage = runtime_state
            .role_usage
            .get(&WorkflowRole::Closure)
            .expect("Closure usage");
        assert_eq!(closure_usage.calls_reserved, 1);
        assert_eq!(closure_usage.calls_completed, 1);

        let art_a1 = runtime_state
            .attempt_artifacts
            .get(&ControllerJobRuntimeState::artifact_key("task-root-a", 1))
            .expect("task-root-a attempt 1 artifacts");
        let art_a2 = runtime_state
            .attempt_artifacts
            .get(&ControllerJobRuntimeState::artifact_key("task-root-a", 2))
            .expect("task-root-a attempt 2 artifacts");
        assert!(!art_a1.is_derived_attempt);
        assert!(art_a2.is_derived_attempt);
        assert_eq!(art_a1.task_worktree_id, art_a2.task_worktree_id);
        assert_ne!(art_a1.committed_head_commit, art_a2.committed_head_commit);
        assert_ne!(art_a1.committed_tree_sha, art_a2.committed_tree_sha);
        assert_ne!(art_a1.candidate_id, art_a2.candidate_id);
        assert!(art_a1.review_receipt.is_none());
        assert!(art_a1.integration_receipt.is_none());
        assert!(art_a2.review_receipt.is_some());
        assert!(art_a2.integration_receipt.is_some());

        let ledger = fixture
            .workflow_store
            .read_binding_ledger(BINDING_A)
            .expect("read binding ledger");
        let persisted_job = ledger
            .jobs
            .iter()
            .find(|j| j.job_id == job.job_id)
            .expect("persisted job");
        let persisted_task_a = persisted_job
            .tasks
            .iter()
            .find(|t| t.task_id == "task-root-a")
            .expect("persisted task-root-a");
        assert_eq!(persisted_task_a.candidates.len(), 2);
        let cand_a1 = persisted_task_a
            .candidates
            .iter()
            .find(|c| c.attempt_no == 1)
            .expect("task-root-a attempt 1 candidate");
        let cand_a2 = persisted_task_a
            .candidates
            .iter()
            .find(|c| c.attempt_no == 2)
            .expect("task-root-a attempt 2 candidate");
        assert_eq!(art_a1.candidate_id.as_deref(), Some(cand_a1.candidate_id.as_str()));
        assert_eq!(art_a2.candidate_id.as_deref(), Some(cand_a2.candidate_id.as_str()));
        let verdict_a1 = cand_a1
            .review_verdict
            .as_ref()
            .expect("attempt 1 review verdict");
        assert_eq!(verdict_a1.outcome, ReviewVerdictOutcome::CorrectInScope);
        assert!(!verdict_a1.required_in_scope_fixes.is_empty());
        assert!(cand_a1.review_receipt_id.is_none());
        assert!(cand_a1.integration_receipt_id.is_none());
        let verdict_a2 = cand_a2
            .review_verdict
            .as_ref()
            .expect("attempt 2 review verdict");
        assert_eq!(verdict_a2.outcome, ReviewVerdictOutcome::Accept);
        assert!(cand_a2.review_receipt_id.is_some());
        assert!(cand_a2.integration_receipt_id.is_some());
        assert_eq!(derived_receipt.attempt_no - 1, cand_a1.attempt_no);
        assert_eq!(
            derived_receipt.prior_rejected_candidate_id.as_deref(),
            Some(cand_a1.candidate_id.as_str())
        );
        assert_eq!(
            derived_receipt.prior_review_verdict_id.as_deref(),
            Some(verdict_a1.verdict_id.as_str())
        );

        // Verify persisted private 0600 PersistedDerivedTaskExecutionArtifact on disk for attempt 2.
        let derived_coder_eff = runtime_state
            .pending_effects
            .values()
            .find(|e| {
                e.kind == PendingControllerEffectKind::CoderDispatch
                    && e.task_id.as_deref() == Some("task-root-a")
                    && e.attempt_no == Some(2)
            })
            .expect("derived CoderDispatch effect for task-root-a attempt 2");
        assert_eq!(
            derived_coder_eff.status,
            PendingControllerEffectStatus::EffectConfirmed
        );
        let derived_report = read_persisted_derived_task_execution_report(
            &fixture.plans_dir,
            &derived_coder_eff.effect_id,
        )
        .expect("read persisted derived execution report")
        .expect("persisted derived execution report must exist");
        assert_eq!(
            derived_report.status,
            CoworkerTaskCorrectionExecuteStatus::ReadyForVerification
        );
        assert_eq!(derived_report.task_id, "task-root-a");
        assert_eq!(derived_report.prior_attempt_no, 1);
        assert_eq!(derived_report.attempt_no, 2);
        assert_ne!(derived_report.original_run_id, derived_report.manifest_run_id);
        assert_ne!("task-root-a", derived_report.manifest_task_id);
        assert!(derived_report.has_changes_from_prior_head);

        // Verify original per-task integration receipts remain distinct from final job aggregation receipts.
        let completion = final_report
            .completion_outcome
            .as_ref()
            .expect("completion_outcome must be present");
        assert_eq!(completion.status, CoworkerJobCompletionStatus::Completed);
        let (expected_run_id, _expected_token, expected_wt_id, _expected_branch) =
            derive_final_job_integration_identifiers(
                &job.job_id,
                &approval_id,
                &runtime_state.envelope.approval_subject_digest,
            );
        for (tid, accepted_att) in [("task-root-a", 2u32), ("task-root-b", 1u32), ("task-dep-c", 1u32)] {
            let art = runtime_state
                .attempt_artifacts
                .get(&ControllerJobRuntimeState::artifact_key(tid, accepted_att))
                .expect("accepted attempt artifacts");
            let orig_receipt: &IntegrationReceipt = art
                .integration_receipt
                .as_ref()
                .expect("original per-task IntegrationReceipt");
            assert!(orig_receipt.job_aggregation.is_none());
            let expected_agg_rcp_id = derive_job_aggregation_receipt_id(
                &job.job_id,
                &expected_run_id,
                &expected_wt_id,
                tid,
                &orig_receipt.review_receipt_id,
            );
            let agg_receipt =
                load_integration_receipt(host_handle.work_area_dir(), &expected_agg_rcp_id)
                    .expect("load aggregated IntegrationReceipt");
            assert!(agg_receipt.job_aggregation.is_some());
            assert_ne!(orig_receipt.receipt_id, agg_receipt.receipt_id);
        }

        // Verify final RunIntegration worktree contains corrected 101 in src/a.rs, 200 in src/b.rs, and 300 in src/c.rs.
        let final_wt = &completion.aggregation.integration_checkout_path;
        assert_eq!(
            fs::read_to_string(final_wt.join("src/a.rs")).expect("read aggregated src/a.rs"),
            "pub fn a() -> u32 { 101 }\n"
        );
        assert_eq!(
            fs::read_to_string(final_wt.join("src/b.rs")).expect("read aggregated src/b.rs"),
            "pub fn b() -> u32 { 200 }\n"
        );
        assert_eq!(
            fs::read_to_string(final_wt.join("src/c.rs")).expect("read aggregated src/c.rs"),
            "pub fn c() -> u32 { 300 }\n"
        );

        let completed_revision = final_report.job_revision;
        session.release().expect("release session");

        // Reopen completed job session and verify driving is a clean idempotent no-op with zero extra calls/charges.
        let reopened = fixture
            .pipeline
            .open_owned_session(BINDING_A, THREAD_UUID_A, &job.job_id, 50_000)
            .expect("reopen owned pipeline session on Completed job");
        let rerun_report = reopened
            .drive_until_settled(50_000)
            .await
            .expect("drive reopened Completed job");
        assert_eq!(rerun_report.status, CoworkerPipelineStatus::Completed);
        assert_eq!(rerun_report.job_phase, CoworkerJobPhase::Completed);
        assert_eq!(rerun_report.job_revision, completed_revision);
        assert_eq!(read_invocation_count(&fixture.agy_coder_count_file), 4);
        assert_eq!(read_invocation_count(&fixture.agy_reviewer_count_file), 4);
        assert_eq!(read_invocation_count(&fixture.agy_closure_count_file), 1);
        assert_eq!(read_invocation_count(&fixture.check_count_file), 5);
        reopened.release().expect("release reopened session");

        host_handle.shutdown().await.expect("shutdown host");
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn automatic_pipeline_enforces_pre_effect_hold_stop_unknown_and_conflicting_request_key_refusal(
) {
    Box::pin(async {
        let fixture = setup_fixture();

        let tasks = vec![
            PlanTask {
                id: "task-hold-a".to_string(),
                title: "Hold A".to_string(),
                instruction: "CTRL_TASK_A".to_string(),
                role: WorkflowRole::Coder,
                difficulty: Difficulty::Hard,
                depends_on: Vec::new(),
                allowed_write_paths: vec!["src/a.rs".to_string()],
            },
            PlanTask {
                id: "task-hold-b".to_string(),
                title: "Hold B".to_string(),
                instruction: "CTRL_TASK_B".to_string(),
                role: WorkflowRole::Coder,
                difficulty: Difficulty::Hard,
                depends_on: Vec::new(),
                allowed_write_paths: vec!["src/b.rs".to_string()],
            },
        ];
        let mut iface_map = BTreeMap::new();
        iface_map.insert(
            "task-hold-a".to_string(),
            TaskInterfaceOwnershipSpec {
                owned_interfaces: vec!["iface_a".to_string()],
                consumed_interfaces: Vec::new(),
            },
        );
        iface_map.insert(
            "task-hold-b".to_string(),
            TaskInterfaceOwnershipSpec {
                owned_interfaces: vec!["iface_b".to_string()],
                consumed_interfaces: Vec::new(),
            },
        );

        let (job, approval_id) = create_planned_approved_and_frozen_job(
            &fixture,
            "brf_pipe_hold_fence",
            "plan-pipe-hold-fence",
            tasks,
            iface_map,
            vec!["src/a.rs".to_string(), "src/b.rs".to_string()],
            2,
            ExecutionBudgets::default(),
            ControllerRoleExecutionBudget::default_for_role(WorkflowRole::Coder, 4),
            IntegrationStrategy::MergeNoFf,
            false, // no completeness receipt -> conservative hold-all
        )
        .await;

        let host_handle = Box::pin(ApprovedHost::start(ApprovedHostConfig::new(
            &fixture.registry_dir,
            &fixture.plans_dir,
            &fixture.authority_dir,
            &fixture.sessions_dir,
            &approval_id,
        )))
        .await
        .expect("start host");

        // 1. Apply a hold targeting `task-hold-a` before any Coder dispatch. Because
        // `independence_completeness_receipt` is None, conservative hold-all fences both tasks.
        let (held_job, _) = fixture
            .workflow_store
            .apply_workflow_hold(&ApplyWorkflowHoldRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                job_id: job.job_id.clone(),
                expected_job_revision: job.job_revision,
                expected_ledger_revision: None,
                request_key: "req_hold_a".to_string(),
                reason: WorkflowHoldReason::ConversationClarificationPending,
                detail: "Pending clarification on src/a.rs".to_string(),
                directly_affected_task_ids: vec!["task-hold-a".to_string()],
                affected_write_paths: vec!["src/a.rs".to_string()],
                affected_interfaces: vec!["iface_a".to_string()],
                holds_entire_job: false,
            })
            .expect("apply hold");

        let session = fixture
            .pipeline
            .open_owned_session(BINDING_A, THREAD_UUID_A, &held_job.job_id, 30_000)
            .expect("open pipeline session");

        let held_report = session
            .drive_until_settled(30_100)
            .await
            .expect("drive while held");
        assert_eq!(held_report.status, CoworkerPipelineStatus::Held);
        assert!(held_report.dispatched_task_ids.is_empty());
        assert_eq!(
            held_report.fenced_task_ids,
            vec!["task-hold-a".to_string(), "task-hold-b".to_string()]
        );
        assert_eq!(read_invocation_count(&fixture.agy_coder_count_file), 0);

        // 2. Request stop on the job runtime state: `Stopped` takes precedence with zero effects.
        fixture
            .policy_store
            .mutate_job_runtime_for_operator(BINDING_A, &held_job.job_id, |state| {
                state.stop_requested = true;
                state.stop_or_block_reason = Some("operator requested stop".to_string());
                Ok(())
            })
            .expect("set stop_requested");

        let stopped_report = session
            .drive_until_settled(30_200)
            .await
            .expect("drive while stopped");
        assert_eq!(stopped_report.status, CoworkerPipelineStatus::Stopped);
        assert_eq!(read_invocation_count(&fixture.agy_coder_count_file), 0);

        // 3. Clear `stop_requested` and inject an unreconcilable `EffectUnknown` pending effect on
        // `task-hold-a` (`ReviewerInvocation`): verify the pipeline settles at `BlockedOnUnknownEffect`
        // without blind retry.
        fixture
            .policy_store
            .mutate_job_runtime_for_operator(BINDING_A, &held_job.job_id, |state| {
                state.stop_requested = false;
                state.stop_or_block_reason = None;
                state.pending_effects.insert(
                    "eff_00000000000000000000000000000091".to_string(),
                    PendingControllerEffect {
                        effect_id: "eff_00000000000000000000000000000091".to_string(),
                        request_key: "req_rev_a_1".to_string(),
                        job_id: held_job.job_id.clone(),
                        task_id: Some("task-hold-a".to_string()),
                        attempt_no: Some(1),
                        kind: PendingControllerEffectKind::ReviewerInvocation,
                        status: PendingControllerEffectStatus::EffectUnknown,
                        subject_digest: sha256_hex(b"unknown-review-subject"),
                        external_correlation_id: None,
                        created_at_ms: 30_250,
                        updated_at_ms: 30_250,
                        diagnostic: Some("simulated crash during Reviewer invocation".to_string()),
                    },
                );
                Ok(())
            })
            .expect("insert EffectUnknown pending effect");

        let unknown_report = session
            .drive_until_settled(30_300)
            .await
            .expect("drive with EffectUnknown");
        assert_eq!(
            unknown_report.status,
            CoworkerPipelineStatus::BlockedOnUnknownEffect
        );
        assert_eq!(
            unknown_report.unknown_effect_task_ids,
            vec!["task-hold-a".to_string()]
        );
        assert_eq!(read_invocation_count(&fixture.agy_coder_count_file), 0);
        assert_eq!(read_invocation_count(&fixture.agy_reviewer_count_file), 0);

        // 4. Inject a second `PendingControllerEffect` for the same `(task-hold-a, 1, ReviewerInvocation)`
        // slot with a conflicting `request_key`: verify admission refuses before any effect.
        fixture
            .policy_store
            .mutate_job_runtime_for_operator(BINDING_A, &held_job.job_id, |state| {
                state.pending_effects.insert(
                    "eff_00000000000000000000000000000092".to_string(),
                    PendingControllerEffect {
                        effect_id: "eff_00000000000000000000000000000092".to_string(),
                        request_key: "req_rev_a_conflicting_key".to_string(),
                        job_id: held_job.job_id.clone(),
                        task_id: Some("task-hold-a".to_string()),
                        attempt_no: Some(1),
                        kind: PendingControllerEffectKind::ReviewerInvocation,
                        status: PendingControllerEffectStatus::ReservedBeforeEffect,
                        subject_digest: sha256_hex(b"unknown-review-subject"),
                        external_correlation_id: None,
                        created_at_ms: 30_350,
                        updated_at_ms: 30_350,
                        diagnostic: None,
                    },
                );
                Ok(())
            })
            .expect("insert conflicting request_key effect");

        let conflict_err = session
            .drive_until_settled(30_400)
            .await
            .expect_err("conflicting request_key for same effect slot must fail closed");
        assert_eq!(
            conflict_err.code(),
            NativeErrorCode::ValidationFailed.as_str()
        );
        assert!(conflict_err
            .message()
            .contains("conflicting PendingControllerEffect request_key"));
        assert_eq!(read_invocation_count(&fixture.agy_coder_count_file), 0);
        assert_eq!(read_invocation_count(&fixture.agy_reviewer_count_file), 0);

        session.release().expect("release session");
        host_handle.shutdown().await.expect("shutdown host");
    })
    .await;
}

#[path = "support/coworker_full_driver.rs"]
mod coworker_full_driver;

#[path = "support/coworker_owner_failure.rs"]
mod coworker_owner_failure;

#[path = "support/coworker_progress_attention.rs"]
mod coworker_progress_attention;

mod coworker_completion_durability {
    use super::*;
    use threadlane_native::coworker_workflow::controller_policy::TaskAttemptExecutionArtifacts;
    use threadlane_native::coworker_workflow::job_completion::{
        candidate_check_report_file_path, job_integration_report_file_path,
        read_persisted_candidate_check_report, read_persisted_job_integration_report,
        CompletionArtifactFaultStep, CompletionArtifactKind, ScopedCompletionArtifactFaultGuard,
    };
    use threadlane_native::coworker_workflow::{
        CandidateCheckEvidence, FinalizeTaskOutboxRequest, OutboxDispatchKind,
    };
    use threadlane_native::coworker_workflow::{
        OutboxDispatchStatus, RecordTaskReviewVerdictRequest, ReserveTaskOutboxRequest,
    };
    use threadlane_native::coworker_workflow::{StructuredReviewVerdict, TaskCandidateRef};
    use threadlane_operations::{
        accept_review_subject, commit_task_worktree_changes, execute_integration,
        inspect_review_subject, CommitTaskChangesRequest, ExecuteIntegrationRequest,
        InspectReviewSubjectRequest, ReviewAcceptanceRequest, ReviewPolicyBinding,
        TestEvidenceSummary,
    };
    use threadlane_workers::{WorkerFilePermissions, WorkerRunStatus, WorktreeManager};

    async fn prepare_single_task_integrated_job_for_completion(
        fixture: &PipelineTestFixture,
        tag: &str,
    ) -> (CoworkerJobRecord, threadlane_host::ApprovedHostHandle) {
        let tasks = vec![PlanTask {
            id: "task-root-a".to_string(),
            title: "Root task A".to_string(),
            instruction: "CTRL_TASK_A implement src/a.rs".to_string(),
            role: WorkflowRole::Coder,
            difficulty: Difficulty::Hard,
            depends_on: Vec::new(),
            allowed_write_paths: vec!["src/a.rs".to_string()],
        }];
        let mut iface_map = BTreeMap::new();
        iface_map.insert(
            "task-root-a".to_string(),
            TaskInterfaceOwnershipSpec {
                owned_interfaces: vec!["iface_a".to_string()],
                consumed_interfaces: Vec::new(),
            },
        );
        let (job, approval_id) = Box::pin(create_planned_approved_and_frozen_job(
            fixture,
            &format!("brf_dur_{tag}"),
            &format!("plan-dur-{tag}"),
            tasks,
            iface_map,
            vec!["src/a.rs".to_string()],
            1,
            ExecutionBudgets::default(),
            ControllerRoleExecutionBudget::default_for_role(WorkflowRole::Coder, 4),
            IntegrationStrategy::FastForwardOnly,
            true,
        ))
        .await;

        let host = Box::pin(ApprovedHost::start(ApprovedHostConfig::new(
            &fixture.registry_dir,
            &fixture.plans_dir,
            &fixture.authority_dir,
            &fixture.sessions_dir,
            &approval_id,
        )))
        .await
        .expect("start real fixture host before creating owned worktrees");

        let session_dir = fixture.sessions_dir.join(&approval_id);
        let raw_work_area = session_dir.join("work-area");
        fs::create_dir_all(&raw_work_area).expect("create session work-area");
        fs::set_permissions(&session_dir, fs::Permissions::from_mode(0o700))
            .expect("chmod 0700 session_dir");
        fs::set_permissions(&raw_work_area, fs::Permissions::from_mode(0o700))
            .expect("chmod 0700 session work-area");
        let work_area_root = fs::canonicalize(&raw_work_area).expect("canonicalize work_area_root");

        let run_id = format!("run_orig_{tag}");
        let manager =
            WorktreeManager::new(&fixture.repo_dir, &work_area_root, &fixture.base_commit)
                .expect("WorktreeManager::new");
        let orig_int_manifest = manager
            .create_run_worktree_with_token(&run_id, "int01")
            .expect("create original RunIntegration worktree");
        let allowed_paths = BTreeSet::from(["src/a.rs".to_string()]);
        let task_manifest = manager
            .create_task_worktree_with_token(&run_id, "task-root-a", "tok01", &allowed_paths)
            .expect("create task worktree");
        fs::write(
            task_manifest.checkout_path.join("src/a.rs"),
            "pub fn a() -> u32 { 101 }\n",
        )
        .expect("write task src/a.rs");

        commit_task_worktree_changes(&CommitTaskChangesRequest {
            work_area_root: work_area_root.clone(),
            task_worktree_id: task_manifest.worktree_id.clone(),
            commit_message: "Implement task-root-a".to_string(),
            worker_status: Some(WorkerRunStatus::Succeeded),
            allow_missing_effort_evidence: false,
        })
        .expect("commit task changes");

        let test_evidence = TestEvidenceSummary {
            formatting_passed: true,
            clippy_passed: true,
            tests_passed: true,
            passed_test_count: 1,
            failed_test_count: 0,
            evidence_summary: "cargo test passed".to_string(),
        };
        let subject = inspect_review_subject(&InspectReviewSubjectRequest {
            work_area_root: work_area_root.clone(),
            task_worktree_id: task_manifest.worktree_id.clone(),
            policy: ReviewPolicyBinding {
                policy_id: "policy-coworker-exec".to_string(),
                codex_launch_report: None,
                codex_source_report_sha256: None,
                requested_provider: Some("antigravity".to_string()),
                requested_model: Some("gemini-4-argon-high".to_string()),
                requested_effort: Some("high".to_string()),
                observed_model: Some("gemini-4-argon-high".to_string()),
                observed_effort: Some("high".to_string()),
                file_permissions: WorkerFilePermissions::TaskFiles,
                require_effort_evidence: true,
                require_passing_tests: true,
            },
            test_evidence: test_evidence.clone(),
            worker_status: Some(WorkerRunStatus::Succeeded),
        })
        .expect("inspect review subject");

        let review_receipt = accept_review_subject(&ReviewAcceptanceRequest {
            subject: subject.clone(),
            expected_subject_digest: subject.subject_digest.clone(),
            reviewer_id: "op_controller".to_string(),
            review_note: "Approved task-root-a".to_string(),
            approved_at_ms: 3_000,
            receipt_id: Some(format!("rev_orig_{tag}")),
        })
        .expect("accept review subject");

        let integration_receipt = execute_integration(&ExecuteIntegrationRequest {
            work_area_root: work_area_root.clone(),
            integration_worktree_id: orig_int_manifest.worktree_id.clone(),
            task_worktree_id: task_manifest.worktree_id.clone(),
            subject: subject.clone(),
            review_receipt: review_receipt.clone(),
            strategy: IntegrationStrategy::FastForwardOnly,
            expected_integration_head_commit: fixture.base_commit.clone(),
            expected_task_head_commit: review_receipt.task_head_commit.clone(),
            expected_source_head_commit: fixture.base_commit.clone(),
            operator_id: "op_controller".to_string(),
            integrated_at_ms: 3_010,
            receipt_id: Some(format!("int_orig_{tag}")),
        })
        .expect("execute original per-task integration");

        let policy_digest = job
            .approval_linkage
            .as_ref()
            .expect("approval linkage")
            .policy_digest
            .clone();
        let approved_command_key = fixture
            .authority
            .get_for_operator_readonly(&approval_id)
            .expect("read exact fixture approval")
            .tasks
            .into_iter()
            .find(|task| task.task_id == "task-root-a")
            .expect("approved root task")
            .command_key
            .expect("frozen dispatch command key");
        let (job, att_outbox) = fixture
            .workflow_store
            .reserve_task_outbox(&ReserveTaskOutboxRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                job_id: job.job_id.clone(),
                expected_job_revision: job.job_revision,
                expected_ledger_revision: None,
                request_key: approved_command_key,
                kind: OutboxDispatchKind::TaskAttemptDispatch,
                task_id: "task-root-a".to_string(),
                role: WorkflowRole::Coder,
                subject_digest: review_receipt.subject_digest.clone(),
                dependency_input_commit: None,
                dependency_input_tree_sha: None,
                accepted_predecessor_receipts: Vec::new(),
            })
            .expect("reserve attempt outbox");
        let (job, _) = fixture
            .workflow_store
            .reconcile_or_finalize_outbox(&FinalizeTaskOutboxRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                job_id: job.job_id.clone(),
                expected_job_revision: job.job_revision,
                expected_ledger_revision: None,
                outbox_id: att_outbox.outbox_id,
                status: OutboxDispatchStatus::DispatchedEffectConfirmed,
                next_task_stage: CoworkerTaskStage::ReadyForReview,
                run_id: Some(run_id.clone()),
                worker_id: Some(format!("wrk_coder_{tag}")),
                task_worktree_id: Some(task_manifest.worktree_id.clone()),
                head_commit: Some(review_receipt.task_head_commit.clone()),
                tree_sha: Some(review_receipt.task_tree_sha.clone()),
                diff_digest: Some(review_receipt.diff_digest.clone()),
                report_artifact_id: Some(format!("art_coder_{tag}")),
                observed_model: Some("gemini-4-argon-high".to_string()),
                observed_effort: Some("high".to_string()),
                receipt_id: None,
                diagnostic: None,
            })
            .expect("finalize attempt outbox");
        let (job, rev_outbox) = fixture
            .workflow_store
            .reserve_task_outbox(&ReserveTaskOutboxRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                job_id: job.job_id.clone(),
                expected_job_revision: job.job_revision,
                expected_ledger_revision: None,
                request_key: format!("req_rev_{tag}"),
                kind: OutboxDispatchKind::TaskReviewDispatch,
                task_id: "task-root-a".to_string(),
                role: WorkflowRole::Reviewer,
                subject_digest: review_receipt.subject_digest.clone(),
                dependency_input_commit: None,
                dependency_input_tree_sha: None,
                accepted_predecessor_receipts: Vec::new(),
            })
            .expect("reserve review outbox");
        let (job, _) = fixture
            .workflow_store
            .reconcile_or_finalize_outbox(&FinalizeTaskOutboxRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                job_id: job.job_id.clone(),
                expected_job_revision: job.job_revision,
                expected_ledger_revision: None,
                outbox_id: rev_outbox.outbox_id,
                status: OutboxDispatchStatus::DispatchedEffectConfirmed,
                next_task_stage: CoworkerTaskStage::UnderReview,
                run_id: Some(run_id.clone()),
                worker_id: Some(format!("wrk_rev_{tag}")),
                task_worktree_id: Some(task_manifest.worktree_id.clone()),
                head_commit: None,
                tree_sha: None,
                diff_digest: None,
                report_artifact_id: Some(format!("art_rev_{tag}")),
                observed_model: Some("gemini-4-argon-high".to_string()),
                observed_effort: Some("high".to_string()),
                receipt_id: None,
                diagnostic: None,
            })
            .expect("finalize review outbox");

        let candidate_id = format!("cnd_{tag}_01");
        let check_ev = CandidateCheckEvidence {
            check_id: "cargo_test".to_string(),
            command_summary: "cargo test".to_string(),
            passed: true,
            exit_code: 0,
            evidence_digest: "aa".repeat(32),
        };
        let (job, _) = fixture
            .workflow_store
            .record_task_candidate_and_verdict(&RecordTaskReviewVerdictRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                job_id: job.job_id.clone(),
                expected_job_revision: job.job_revision,
                expected_ledger_revision: None,
                candidate: TaskCandidateRef {
                    candidate_id: candidate_id.clone(),
                    task_id: "task-root-a".to_string(),
                    attempt_no: 1,
                    base_commit: review_receipt.base_commit.clone(),
                    head_commit: review_receipt.task_head_commit.clone(),
                    tree_sha: review_receipt.task_tree_sha.clone(),
                    diff_digest: review_receipt.diff_digest.clone(),
                    write_paths: vec!["src/a.rs".to_string()],
                    check_evidence_digests: vec![check_ev.evidence_digest.clone()],
                    review_subject_digest: Some(review_receipt.subject_digest.clone()),
                    review_verdict: Some(StructuredReviewVerdict {
                        verdict_id: format!("vrd_{tag}_01"),
                        task_id: "task-root-a".to_string(),
                        attempt_no: 1,
                        candidate_id: candidate_id.clone(),
                        review_subject_digest: review_receipt.subject_digest.clone(),
                        base_commit: review_receipt.base_commit.clone(),
                        task_head_commit: review_receipt.task_head_commit.clone(),
                        task_tree_sha: review_receipt.task_tree_sha.clone(),
                        diff_digest: review_receipt.diff_digest.clone(),
                        policy_digest,
                        outcome: ReviewVerdictOutcome::Accept,
                        findings: Vec::new(),
                        required_in_scope_fixes: Vec::new(),
                        conflicting_paths_or_interfaces: Vec::new(),
                        prose_summary: Some("Accepted".to_string()),
                        prose_may_advance_acceptance: false,
                    }),
                    review_receipt_id: Some(review_receipt.receipt_id.clone()),
                    integration_receipt_id: None,
                },
            })
            .expect("record task candidate and verdict");
        let (job, int_outbox) = fixture
            .workflow_store
            .reserve_task_outbox(&ReserveTaskOutboxRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                job_id: job.job_id.clone(),
                expected_job_revision: job.job_revision,
                expected_ledger_revision: None,
                request_key: format!("req_int_{tag}"),
                kind: OutboxDispatchKind::TaskIntegration,
                task_id: "task-root-a".to_string(),
                role: WorkflowRole::Reviewer,
                subject_digest: review_receipt.subject_digest.clone(),
                dependency_input_commit: None,
                dependency_input_tree_sha: None,
                accepted_predecessor_receipts: Vec::new(),
            })
            .expect("reserve integration outbox");
        let (job, _) = fixture
            .workflow_store
            .reconcile_or_finalize_outbox(&FinalizeTaskOutboxRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                job_id: job.job_id.clone(),
                expected_job_revision: job.job_revision,
                expected_ledger_revision: None,
                outbox_id: int_outbox.outbox_id,
                status: OutboxDispatchStatus::DispatchedEffectConfirmed,
                next_task_stage: CoworkerTaskStage::Integrated,
                run_id: Some(run_id.clone()),
                worker_id: None,
                task_worktree_id: Some(task_manifest.worktree_id.clone()),
                head_commit: None,
                tree_sha: None,
                diff_digest: None,
                report_artifact_id: None,
                observed_model: None,
                observed_effort: None,
                receipt_id: Some(integration_receipt.receipt_id.clone()),
                diagnostic: None,
            })
            .expect("finalize integration outbox");

        fixture
            .policy_store
            .mutate_job_runtime_for_operator(BINDING_A, &job.job_id, |state| {
                state.attempt_artifacts.insert(
                    ControllerJobRuntimeState::artifact_key("task-root-a", 1),
                    TaskAttemptExecutionArtifacts {
                        task_id: "task-root-a".to_string(),
                        attempt_no: 1,
                        run_id: run_id.clone(),
                        worker_id: format!("wrk_coder_{tag}"),
                        task_worktree_id: task_manifest.worktree_id.clone(),
                        integration_worktree_id: orig_int_manifest.worktree_id.clone(),
                        execution_base_commit: fixture.base_commit.clone(),
                        observed_provider: "antigravity".to_string(),
                        observed_model: Some("gemini-4-argon-high".to_string()),
                        codex_launch_report: None,
                        codex_source_report_sha256: None,
                        codex_source_effect_id: None,
                        observed_effort: Some("high".to_string()),
                        committed_head_commit: Some(review_receipt.task_head_commit.clone()),
                        committed_tree_sha: Some(review_receipt.task_tree_sha.clone()),
                        candidate_id: Some(candidate_id),
                        check_evidence: vec![check_ev],
                        test_evidence_summary: Some(test_evidence),
                        review_subject: Some(subject),
                        reviewer_response_digest: Some("cc".repeat(32)),
                        review_receipt: Some(review_receipt),
                        integration_receipt: Some(integration_receipt),
                        is_derived_attempt: false,
                    },
                );
                Ok(())
            })
            .expect("insert attempt_artifacts");

        (job, host)
    }

    fn find_effect_by_kind(
        fixture: &PipelineTestFixture,
        job_id: &str,
        kind: PendingControllerEffectKind,
    ) -> PendingControllerEffect {
        let runtime = fixture
            .policy_store
            .get_job_runtime_readonly(BINDING_A, job_id)
            .expect("read job runtime")
            .expect("job runtime exists");
        runtime
            .pending_effects
            .values()
            .find(|eff| eff.kind == kind)
            .expect("expected PendingControllerEffect for kind")
            .clone()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn job_completion_durability_fault_matrix_and_recovery_without_replay() {
        // 1a. Pre-rename temp-file sync failure on JobIntegrationReport: returns IoError, leaves
        // no visible report file, and recovers cleanly from the Completed JobIntegrationIntent.
        {
            let fixture = setup_fixture();
            let (job, host) = Box::pin(prepare_single_task_integrated_job_for_completion(
                &fixture, "pre_agg",
            ))
            .await;
            let session = fixture
                .controller
                .open_owned_session(BINDING_A, THREAD_UUID_A, &job.job_id, 10_000)
                .expect("open session");
            let guard = ScopedCompletionArtifactFaultGuard::install(
                &fixture.plans_dir,
                CompletionArtifactKind::JobIntegrationReport,
                vec![CompletionArtifactFaultStep::PreRenameTempFileSync],
            );
            let err = Box::pin(
                fixture
                    .pipeline
                    .completion_service()
                    .complete_owned_session(&session, 10_100),
            )
            .await
            .expect_err("pre-rename agg sync fault must fail");
            assert_eq!(err.code(), NativeErrorCode::IoError.as_str());
            assert_eq!(guard.remaining_steps(), 0);
            drop(guard);

            let agg_eff = find_effect_by_kind(
                &fixture,
                &job.job_id,
                PendingControllerEffectKind::JobAggregation,
            );
            assert_eq!(
                agg_eff.status,
                PendingControllerEffectStatus::ReservedBeforeEffect
            );
            assert_eq!(
                read_persisted_job_integration_report(&fixture.plans_dir, &agg_eff.effect_id)
                    .expect("read absent agg report"),
                None
            );
            assert_eq!(read_invocation_count(&fixture.check_count_file), 0);
            assert_eq!(read_invocation_count(&fixture.agy_closure_count_file), 0);

            let recovered = Box::pin(
                fixture
                    .pipeline
                    .completion_service()
                    .complete_owned_session(&session, 10_200),
            )
            .await
            .expect("recover after pre-rename agg sync fault");
            assert_eq!(recovered.status, CoworkerJobCompletionStatus::Completed);
            assert_eq!(recovered.job.phase, CoworkerJobPhase::Completed);
            assert!(recovered.aggregation.reused_existing_receipts);
            assert_eq!(read_invocation_count(&fixture.check_count_file), 1);
            assert_eq!(read_invocation_count(&fixture.agy_closure_count_file), 1);
            session.release().expect("release session");
            host.shutdown().await.expect("shutdown fixture host");
        }

        // 1b. Pre-rename temp-file sync failure on CandidateCheckExecutionReport: returns IoError
        // after running check once, leaves no visible report, and refuses blind replay on retry.
        {
            let fixture = setup_fixture();
            let (job, host) = Box::pin(prepare_single_task_integrated_job_for_completion(
                &fixture, "pre_chk",
            ))
            .await;
            let session = fixture
                .controller
                .open_owned_session(BINDING_A, THREAD_UUID_A, &job.job_id, 11_000)
                .expect("open session");
            let guard = ScopedCompletionArtifactFaultGuard::install(
                &fixture.plans_dir,
                CompletionArtifactKind::CandidateCheckExecutionReport,
                vec![CompletionArtifactFaultStep::PreRenameTempFileSync],
            );
            let err = Box::pin(
                fixture
                    .pipeline
                    .completion_service()
                    .complete_owned_session(&session, 11_100),
            )
            .await
            .expect_err("pre-rename check sync fault must fail");
            assert_eq!(err.code(), NativeErrorCode::IoError.as_str());
            assert_eq!(guard.remaining_steps(), 0);
            drop(guard);

            let chk_eff = find_effect_by_kind(
                &fixture,
                &job.job_id,
                PendingControllerEffectKind::CheckExecution,
            );
            assert_eq!(
                chk_eff.status,
                PendingControllerEffectStatus::ReservedBeforeEffect
            );
            assert_eq!(
                read_persisted_candidate_check_report(&fixture.plans_dir, &chk_eff.effect_id)
                    .expect("read absent check report"),
                None
            );
            assert_eq!(read_invocation_count(&fixture.check_count_file), 1);
            assert_eq!(read_invocation_count(&fixture.agy_closure_count_file), 0);

            let retry_err = Box::pin(
                fixture
                    .pipeline
                    .completion_service()
                    .complete_owned_session(&session, 11_200),
            )
            .await
            .expect_err("unpersisted reserved check must fail closed without replay");
            assert_eq!(
                retry_err.code(),
                NativeErrorCode::AmbiguousWakeupPendingReconciliation.as_str()
            );
            let chk_after = find_effect_by_kind(
                &fixture,
                &job.job_id,
                PendingControllerEffectKind::CheckExecution,
            );
            assert_eq!(
                chk_after.status,
                PendingControllerEffectStatus::EffectUnknown
            );
            assert_eq!(read_invocation_count(&fixture.check_count_file), 1);
            assert_eq!(read_invocation_count(&fixture.agy_closure_count_file), 0);
            session.release().expect("release session");
            host.shutdown().await.expect("shutdown fixture host");
        }

        // 2. Post-rename durability matrix: PostRenameParentOpen and PostRenameParentSync across
        // both JobIntegrationReport and CandidateCheckExecutionReport.
        let post_rename_cases = [
            (
                0usize,
                CompletionArtifactKind::JobIntegrationReport,
                CompletionArtifactFaultStep::PostRenameParentOpen,
                0u32,
            ),
            (
                1usize,
                CompletionArtifactKind::JobIntegrationReport,
                CompletionArtifactFaultStep::PostRenameParentSync,
                0u32,
            ),
            (
                2usize,
                CompletionArtifactKind::CandidateCheckExecutionReport,
                CompletionArtifactFaultStep::PostRenameParentOpen,
                1u32,
            ),
            (
                3usize,
                CompletionArtifactKind::CandidateCheckExecutionReport,
                CompletionArtifactFaultStep::PostRenameParentSync,
                1u32,
            ),
        ];
        for (idx, kind, post_fault, expected_checks_on_first_try) in post_rename_cases {
            let fixture = setup_fixture();
            let tag = format!("post_{idx}");
            let (job, host) = Box::pin(prepare_single_task_integrated_job_for_completion(
                &fixture, &tag,
            ))
            .await;
            let session = fixture
                .controller
                .open_owned_session(BINDING_A, THREAD_UUID_A, &job.job_id, 20_000)
                .expect("open session");
            let guard = ScopedCompletionArtifactFaultGuard::install(
                &fixture.plans_dir,
                kind,
                vec![post_fault],
            );
            let err = Box::pin(
                fixture
                    .pipeline
                    .completion_service()
                    .complete_owned_session(&session, 20_100),
            )
            .await
            .expect_err("post-rename fault must return PersistenceUncertain");
            assert_eq!(err.code(), NativeErrorCode::PersistenceUncertain.as_str());
            assert_eq!(guard.remaining_steps(), 0);
            drop(guard);
            assert_eq!(
                read_invocation_count(&fixture.check_count_file),
                expected_checks_on_first_try
            );
            assert_eq!(read_invocation_count(&fixture.agy_closure_count_file), 0);

            let plans_dir = &fixture.plans_dir;
            let (eff_kind, eff_id, report_path) = match kind {
                CompletionArtifactKind::JobIntegrationReport => {
                    let eff = find_effect_by_kind(
                        &fixture,
                        &job.job_id,
                        PendingControllerEffectKind::JobAggregation,
                    );
                    assert_eq!(
                        eff.status,
                        PendingControllerEffectStatus::ReservedBeforeEffect
                    );
                    let path = job_integration_report_file_path(plans_dir, &eff.effect_id);
                    let ro = read_persisted_job_integration_report(plans_dir, &eff.effect_id)
                        .expect("read-only agg report");
                    assert!(!ro.expect("agg report exists").reused_existing_receipts);
                    (
                        PendingControllerEffectKind::JobAggregation,
                        eff.effect_id,
                        path,
                    )
                }
                CompletionArtifactKind::CandidateCheckExecutionReport => {
                    let eff = find_effect_by_kind(
                        &fixture,
                        &job.job_id,
                        PendingControllerEffectKind::CheckExecution,
                    );
                    assert_eq!(
                        eff.status,
                        PendingControllerEffectStatus::ReservedBeforeEffect
                    );
                    let path = candidate_check_report_file_path(plans_dir, &eff.effect_id);
                    let ro = read_persisted_candidate_check_report(plans_dir, &eff.effect_id)
                        .expect("read-only check report");
                    assert!(ro.expect("check report exists").passed());
                    (
                        PendingControllerEffectKind::CheckExecution,
                        eff.effect_id,
                        path,
                    )
                }
            };

            // Read-only inspection must not clear uncertainty or promote the effect.
            assert_eq!(
                find_effect_by_kind(&fixture, &job.job_id, eff_kind).status,
                PendingControllerEffectStatus::ReservedBeforeEffect
            );
            let meta = fs::symlink_metadata(&report_path).expect("stat report_path");
            assert_eq!(meta.permissions().mode() & 0o777, 0o600);
            let preserved_bytes = fs::read(&report_path).expect("read preserved bytes");
            let preserved_digest = sha256_hex(&preserved_bytes);

            // Recovery must remain blocked while RecoveryFileSync, RecoveryParentOpen, or
            // RecoveryParentSync fails, preserving exact bytes without extra check/Closure calls.
            let rec_guard = ScopedCompletionArtifactFaultGuard::install(
                &fixture.plans_dir,
                kind,
                vec![
                    CompletionArtifactFaultStep::RecoveryFileSync,
                    CompletionArtifactFaultStep::RecoveryParentOpen,
                    CompletionArtifactFaultStep::RecoveryParentSync,
                ],
            );
            for step_idx in 0..3u64 {
                let rec_err = Box::pin(
                    fixture
                        .pipeline
                        .completion_service()
                        .complete_owned_session(&session, 20_200 + step_idx * 100),
                )
                .await
                .expect_err("recovery must remain blocked while re-sync fails");
                assert_eq!(
                    rec_err.code(),
                    NativeErrorCode::PersistenceUncertain.as_str()
                );
                assert_eq!(
                    find_effect_by_kind(&fixture, &job.job_id, eff_kind).status,
                    PendingControllerEffectStatus::ReservedBeforeEffect
                );
                assert_eq!(
                    fs::read(&report_path).expect("read bytes after blocked recovery"),
                    preserved_bytes
                );
                assert_eq!(
                    read_invocation_count(&fixture.check_count_file),
                    expected_checks_on_first_try
                );
                assert_eq!(read_invocation_count(&fixture.agy_closure_count_file), 0);
            }
            assert_eq!(rec_guard.remaining_steps(), 0);
            drop(rec_guard);

            // Final recovery establishes file + parent durability and promotes the exact report.
            let outcome = Box::pin(
                fixture
                    .pipeline
                    .completion_service()
                    .complete_owned_session(&session, 20_600),
            )
            .await
            .expect("recovery must succeed once durability is established");
            assert_eq!(outcome.status, CoworkerJobCompletionStatus::Completed);
            assert_eq!(outcome.job.phase, CoworkerJobPhase::Completed);
            let promoted_eff = find_effect_by_kind(&fixture, &job.job_id, eff_kind);
            assert_eq!(promoted_eff.effect_id, eff_id);
            assert_eq!(
                promoted_eff.status,
                PendingControllerEffectStatus::EffectConfirmed
            );
            assert_eq!(
                fs::read(&report_path).expect("read bytes after promotion"),
                preserved_bytes
            );
            if kind == CompletionArtifactKind::JobIntegrationReport {
                assert_eq!(outcome.aggregation.report_artifact_digest, preserved_digest);
                assert!(!outcome.aggregation.reused_existing_receipts);
            }
            assert_eq!(read_invocation_count(&fixture.check_count_file), 1);
            assert_eq!(read_invocation_count(&fixture.agy_closure_count_file), 1);
            assert_eq!(read_invocation_count(&fixture.agy_coder_count_file), 0);
            assert_eq!(read_invocation_count(&fixture.agy_reviewer_count_file), 0);
            session.release().expect("release session");
            host.shutdown().await.expect("shutdown fixture host");
        }

        // 3. Candidate check spec/subject mismatch refusal and EffectUnknown durability recovery.
        {
            let fixture = setup_fixture();
            let (job, host) = Box::pin(prepare_single_task_integrated_job_for_completion(
                &fixture,
                "chk_mismatch",
            ))
            .await;
            let session = fixture
                .controller
                .open_owned_session(BINDING_A, THREAD_UUID_A, &job.job_id, 30_000)
                .expect("open session");
            let guard = ScopedCompletionArtifactFaultGuard::install(
                &fixture.plans_dir,
                CompletionArtifactKind::CandidateCheckExecutionReport,
                vec![CompletionArtifactFaultStep::PostRenameParentSync],
            );
            let err = Box::pin(
                fixture
                    .pipeline
                    .completion_service()
                    .complete_owned_session(&session, 30_100),
            )
            .await
            .expect_err("initial check post-rename sync fault");
            assert_eq!(err.code(), NativeErrorCode::PersistenceUncertain.as_str());
            drop(guard);

            let chk_eff = find_effect_by_kind(
                &fixture,
                &job.job_id,
                PendingControllerEffectKind::CheckExecution,
            );
            let report_path =
                candidate_check_report_file_path(&fixture.plans_dir, &chk_eff.effect_id);
            let preserved_bytes = fs::read(&report_path).expect("read preserved check bytes");
            let valid_report =
                read_persisted_candidate_check_report(&fixture.plans_dir, &chk_eff.effect_id)
                    .expect("read valid check report")
                    .expect("check report exists");

            // Replace on-disk report with a spec_digest-mismatched report and verify refusal.
            let mut bad_spec_report = valid_report.clone();
            bad_spec_report.spec_digest = "ff".repeat(32);
            bad_spec_report.evidence_digest = bad_spec_report
                .compute_evidence_digest()
                .expect("compute bad_spec evidence_digest");
            fs::write(
                &report_path,
                serde_json::to_vec(&bad_spec_report).expect("serialize bad_spec_report"),
            )
            .expect("write bad_spec_report");
            fs::set_permissions(&report_path, fs::Permissions::from_mode(0o600))
                .expect("chmod 0600 bad_spec_report");

            let mismatch_err = Box::pin(
                fixture
                    .pipeline
                    .completion_service()
                    .complete_owned_session(&session, 30_200),
            )
            .await
            .expect_err("spec_digest mismatch must fail closed");
            assert_eq!(
                mismatch_err.code(),
                NativeErrorCode::SecurityViolation.as_str()
            );
            assert_eq!(
                find_effect_by_kind(
                    &fixture,
                    &job.job_id,
                    PendingControllerEffectKind::CheckExecution,
                )
                .status,
                PendingControllerEffectStatus::ReservedBeforeEffect
            );
            assert_eq!(read_invocation_count(&fixture.check_count_file), 1);
            assert_eq!(read_invocation_count(&fixture.agy_closure_count_file), 0);

            // Simulate an interrupted owner separately from the rejected, mismatched report.
            fixture
                .policy_store
                .mutate_job_runtime_for_operator(BINDING_A, &job.job_id, |state| {
                    state
                        .pending_effects
                        .get_mut(&chk_eff.effect_id)
                        .expect("check effect")
                        .status = PendingControllerEffectStatus::EffectUnknown;
                    Ok(())
                })
                .expect("record interrupted check outcome");

            // Unknown-effect recovery still verifies the complete cached proof.
            let gate_err = Box::pin(
                fixture
                    .pipeline
                    .completion_service()
                    .complete_owned_session(&session, 30_300),
            )
            .await
            .expect_err("EffectUnknown with mismatched report must be refused");
            assert_eq!(gate_err.code(), NativeErrorCode::SecurityViolation.as_str());

            // Restore exact valid preserved_bytes while effect is EffectUnknown: verify recovery
            // stays blocked on RecoveryParentSync and then promotes to EffectConfirmed once durable.
            fs::write(&report_path, &preserved_bytes).expect("restore preserved_bytes");
            fs::set_permissions(&report_path, fs::Permissions::from_mode(0o600))
                .expect("chmod 0600 restored report");

            // Automatic driving remains held even with a valid visible report. Only the
            // explicit completion service below establishes durability and reconciles it.
            let mut driver = fixture
                .pipeline
                .driver(session.guard(), BINDING_A, THREAD_UUID_A, &job.job_id)
                .expect("create unknown-effect pipeline driver");
            let held = Box::pin(driver.tick(30_350))
                .await
                .expect("inspect held pipeline");
            assert_eq!(held.status, CoworkerPipelineStatus::BlockedOnUnknownEffect);
            assert_eq!(driver.in_flight_count(), 0);
            assert_eq!(read_invocation_count(&fixture.check_count_file), 1);
            assert_eq!(read_invocation_count(&fixture.agy_closure_count_file), 0);
            drop(driver);

            let rec_guard = ScopedCompletionArtifactFaultGuard::install(
                &fixture.plans_dir,
                CompletionArtifactKind::CandidateCheckExecutionReport,
                vec![CompletionArtifactFaultStep::RecoveryParentSync],
            );
            let blocked_err = Box::pin(
                fixture
                    .pipeline
                    .completion_service()
                    .complete_owned_session(&session, 30_400),
            )
            .await
            .expect_err("EffectUnknown recovery must stay blocked when re-sync fails");
            assert_eq!(
                blocked_err.code(),
                NativeErrorCode::PersistenceUncertain.as_str()
            );
            assert_eq!(
                find_effect_by_kind(
                    &fixture,
                    &job.job_id,
                    PendingControllerEffectKind::CheckExecution,
                )
                .status,
                PendingControllerEffectStatus::EffectUnknown
            );
            drop(rec_guard);

            let recovered = Box::pin(
                fixture
                    .pipeline
                    .completion_service()
                    .complete_owned_session(&session, 30_500),
            )
            .await
            .expect("EffectUnknown check with valid report must recover once durable");
            assert_eq!(recovered.status, CoworkerJobCompletionStatus::Completed);
            assert_eq!(recovered.job.phase, CoworkerJobPhase::Completed);
            assert_eq!(
                find_effect_by_kind(
                    &fixture,
                    &job.job_id,
                    PendingControllerEffectKind::CheckExecution,
                )
                .status,
                PendingControllerEffectStatus::EffectConfirmed
            );
            assert_eq!(
                fs::read(&report_path).expect("read bytes after EffectUnknown recovery"),
                preserved_bytes
            );
            assert_eq!(read_invocation_count(&fixture.check_count_file), 1);
            assert_eq!(read_invocation_count(&fixture.agy_closure_count_file), 1);
            session.release().expect("release session");
            host.shutdown().await.expect("shutdown fixture host");
        }
    }
}
