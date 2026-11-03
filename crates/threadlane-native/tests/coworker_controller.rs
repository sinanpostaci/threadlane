use std::collections::BTreeMap;
use std::fs;
use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tempfile::TempDir;
use threadlane_authority::{AuthorityStore, OperatorApprovalRequest};
use threadlane_host::{
    ApprovedHost, ApprovedHostClient, ApprovedHostConfig, DependencyPredecessorSelection,
    HostPhase, PrepareDependencyInputsRequest, TaskExecutionStatus,
};
use threadlane_native::coworker_workflow::{
    read_persisted_check_report, sha256_hex, ApplyWorkflowHoldRequest, BindJobApprovalRequest,
    CanonicalProjectBaseRef, ControllerConfigurationPreview, ControllerJobRuntimeState,
    ControllerPolicyStore, ControllerRoleExecutionBudget, CoworkerControllerPhase1Status,
    CoworkerControllerService, CoworkerControllerStepRequest, CoworkerJobOwnershipKey,
    CoworkerJobOwnershipStore, CoworkerJobPhase, CoworkerJobRecord, CoworkerPlanningService,
    CoworkerTaskStage, CoworkerTaskVerificationRequest, CoworkerTaskVerificationService,
    CoworkerTaskVerificationStatus, CoworkerWorkflowStore, CreateCoworkerJobRequest,
    CreatePlanningGrantRequest, FinalizeTaskOutboxRequest,
    FreezeControllerExecutionEnvelopeRequest, FrozenCheckCommandSpec, FrozenCheckExecutionStatus,
    FrozenCheckKind, FrozenControllerCapabilityEnvelope, OrchestrateWorkBriefPlanRequest,
    OutboxDispatchKind, OutboxDispatchStatus, PendingControllerEffect, PendingControllerEffectKind,
    PendingControllerEffectStatus, PlannedCheckSuggestion, PlanningCallBudget,
    PlanningReadOnlyCapabilities, PlanningReservationStatus, RecordTaskReviewVerdictRequest,
    ReserveTaskOutboxRequest, ReviewVerdictOutcome, StructuredOrchestratorOutput,
    StructuredPlannedTask, StructuredReviewVerdict, TaskCandidateRef, TaskInterfaceOwnershipSpec,
    WorkBrief, WorkBriefKind, WorkflowHoldReason,
};
use threadlane_native::{
    Difficulty, ExecutionPolicy, NativeErrorCode, PolicyAllowRule, RegistryStore,
    RoleExecutionRoute, SelectedRoleChoice, ToolAdapter, WorkflowProfile, WorkflowRole,
};
use threadlane_operations::{
    accept_review_subject, commit_task_worktree_changes, execute_integration,
    inspect_review_subject, CommitTaskChangesRequest, ExecuteIntegrationRequest,
    InspectReviewSubjectRequest, IntegrationStrategy, ReviewAcceptanceRequest, ReviewPolicyBinding,
    TestEvidenceSummary,
};
use threadlane_workers::{
    resolve_exact_commit_sha, resolve_exact_tree_sha, run_sanitized_git, ExecutionBudgets,
    WorkerFilePermissions, WorkerRunStatus, DEFAULT_GIT_COMMAND_TIMEOUT_MS,
};
use threadlane_workflow::{
    compile_plan_revision, ModelChoice, ModelDefinition, PlanChange, PlanStore, PlanTask,
    RegistryChange, RoleAssignment, ScopedWorkflow, ToolDefinition,
};

const THREAD_UUID_A: &str = "550e8400-e29b-41d4-a716-446655440000";
const THREAD_UUID_B: &str = "6ba7b810-9dad-11d1-80b4-00c04fd430c8";
const BINDING_A: &str = "bnd_coworker_ctrl_alpha";
const BINDING_B: &str = "bnd_coworker_ctrl_beta";

#[allow(dead_code)]
struct ControllerTestFixture {
    _tmp: TempDir,
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
    agy_prompt_file: PathBuf,
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

fn write_fake_agy_script(
    script_path: &Path,
    coder_count_file: &Path,
    planning_count_file: &Path,
    prompt_file: &Path,
    plan_response_file: &Path,
) {
    let script = format!(
        r#"#!/bin/sh
set -eu
CODER_COUNT_FILE="{coder_count_file}"
PLANNING_COUNT_FILE="{planning_count_file}"
PROMPT_FILE="{prompt_file}"
PLAN_RESPONSE_FILE="{plan_response_file}"

MODEL=""
EFFORT=""
AGY_MODE=""
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
        --mode)
            AGY_MODE="$2"
            shift 2
            ;;
        --print|--prompt|-p)
            PROMPT="$2"
            shift 2
            ;;
        *)
            shift 1
            ;;
    esac
done

if [ "$AGY_MODE" = "plan" ]; then
    pc=0
    if [ -f "$PLANNING_COUNT_FILE" ]; then
        pc=$(cat "$PLANNING_COUNT_FILE")
    fi
    pc=$((pc + 1))
    printf "%s" "$pc" > "$PLANNING_COUNT_FILE"
    printf '{{"event":"init","init":{{"session_id":"sess-agy-plan-%s","model":"%s"}}}}\n' "$pc" "$MODEL"
    esc=$(sed 's/\\/\\\\/g; s/"/\\"/g' "$PLAN_RESPONSE_FILE" | tr '\n' ' ')
    printf '{{"event":"result","result":{{"status":"SUCCESS","response":"%s"}}}}\n' "$esc"
    exit 0
fi

c=0
if [ -f "$CODER_COUNT_FILE" ]; then
    c=$(cat "$CODER_COUNT_FILE")
fi
c=$((c + 1))
printf "%s" "$c" > "$CODER_COUNT_FILE"

printf "%s" "$PROMPT" > "$PROMPT_FILE"
printf '{{"type":"init","session_id":"sess-agy-ctrl","model":"%s","effort":"%s"}}\n' "$MODEL" "$EFFORT"

if [ -d "src" ]; then
    case "$PROMPT" in
        *CTRL_TASK_A*)
            printf "pub fn a() -> u32 {{ 100 }}\n" > "src/a.rs"
            ;;
        *CTRL_TASK_B*)
            printf "pub fn b() -> u32 {{ 200 }}\n" > "src/b.rs"
            ;;
        *CTRL_TASK_C*)
            if ! grep -q "100" "src/a.rs" || ! grep -q "200" "src/b.rs"; then
                echo "expected integrated predecessor contents in src/a.rs and src/b.rs" >&2
                exit 3
            fi
            printf "pub fn c() -> u32 {{ 300 }}\n" > "src/c.rs"
            ;;
        *)
            printf "pub fn a() -> u32 {{ 42 }}\n" > "src/a.rs"
            ;;
    esac
fi

printf '{{"type":"result","subtype":"success","session_id":"sess-agy-ctrl","model":"%s","effort":"%s"}}\n' "$MODEL" "$EFFORT"
exit 0
"#,
        coder_count_file = coder_count_file.display(),
        planning_count_file = planning_count_file.display(),
        prompt_file = prompt_file.display(),
        plan_response_file = plan_response_file.display(),
    );
    write_executable_script(script_path, &script);
}

fn read_invocation_count(path: &Path) -> u32 {
    match fs::read_to_string(path) {
        Ok(s) => s.trim().parse::<u32>().unwrap_or(0),
        Err(_) => 0,
    }
}

fn setup_fixture() -> ControllerTestFixture {
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
    let agy_prompt_file = root.join("agy_prompt.txt");
    let agy_plan_response_file = root.join("agy_plan_response.json");
    write_fake_agy_script(
        &fake_agy_exe,
        &agy_count_file,
        &agy_planning_count_file,
        &agy_prompt_file,
        &agy_plan_response_file,
    );
    write_executable_script(&fake_cargo_exe, "#!/bin/sh\nexit 0\n");

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

    ControllerTestFixture {
        _tmp: tmp,
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
        agy_prompt_file,
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
    }
}

fn project_base(fixture: &ControllerTestFixture) -> CanonicalProjectBaseRef {
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

fn sample_brief(fixture: &ControllerTestFixture, brief_id: &str) -> WorkBrief {
    WorkBrief {
        brief_id: brief_id.to_string(),
        binding_id: BINDING_A.to_string(),
        thread_uuid: THREAD_UUID_A.to_string(),
        project_base: project_base(fixture),
        kind: WorkBriefKind::NewJob,
        summary: format!("Controller test brief {brief_id}"),
        confirmed_requirements: vec![
            "Dispatch ready roots up to max_parallel".to_string(),
            "Gate dependents on accepted predecessor receipts and sealed inputs".to_string(),
        ],
        proposed_additions: Vec::new(),
        assumptions: vec!["Linux local workspace".to_string()],
        open_questions: Vec::new(),
        accepted_architect_decisions: Vec::new(),
    }
}

fn default_check_commands(
    fixture: &ControllerTestFixture,
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

#[allow(clippy::too_many_arguments)]
async fn create_planned_approved_and_frozen_job(
    fixture: &ControllerTestFixture,
    brief_id: &str,
    plan_id: &str,
    tasks: Vec<PlanTask>,
    task_interfaces: BTreeMap<String, TaskInterfaceOwnershipSpec>,
    approved_write_paths: Vec<String>,
    max_parallel: usize,
    approval_budgets: ExecutionBudgets,
    coder_budget: ControllerRoleExecutionBudget,
) -> (CoworkerJobRecord, String) {
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
    fs::write(&fixture.agy_plan_response_file, &plan_json).expect("write agy_plan_response_file");

    let orch_req_key = format!("req_orch_{brief_id}");
    let plan_outcome = fixture
        .planning_service
        .orchestrate_brief_to_plan(
            &fixture.scoped_workflow,
            &OrchestrateWorkBriefPlanRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                grant_id: grant.grant_id.clone(),
                job_id: job.job_id.clone(),
                request_key: orch_req_key,
                expected_ledger_revision: None,
                difficulty: Some(Difficulty::Hard),
                architect_difficulty: Some(Difficulty::Hard),
                plan_id: Some(plan_id.to_string()),
                expected_plan_revision: 0,
                architect_advice_ids: Vec::new(),
                max_architect_rounds: 0,
                current_time_ms: 2_000,
            },
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
        max_in_scope_correction_attempts: 1,
        allowed_check_ids: vec!["cargo_test".to_string()],
        local_git_integration_allowed: true,
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

    let mut role_budgets = BTreeMap::new();
    role_budgets.insert(WorkflowRole::Coder, coder_budget);
    role_budgets.insert(
        WorkflowRole::Reviewer,
        ControllerRoleExecutionBudget::default_for_role(WorkflowRole::Reviewer, 4),
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
        max_in_scope_corrections_per_task: 1,
        max_total_corrections_per_job: 2,
        max_total_task_attempts_per_job: 6,
        check_commands: default_check_commands(fixture),
        allow_task_worktree_commits: true,
        allow_run_integration_execution: true,
        default_integration_strategy: IntegrationStrategy::FastForwardOnly,
        independence_completeness_receipt: None,
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
}

#[tokio::test]
async fn unlinked_legacy_approval_wrong_owner_and_oversized_task_budgets_refused_before_launch() {
    let fixture = setup_fixture();
    let reg_snap = fixture.registry.snapshot().expect("registry snapshot");

    // 1. Create a manual/legacy job (`planning_result == None`) with a valid human approval.
    // `bind_job_approval` preserves manual job compatibility, but automatic controller admission
    // must refuse it before launching any worker.
    let manual_job = fixture
        .workflow_store
        .create_or_amend_job(&CreateCoworkerJobRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            request_key: "req_manual_legacy_job".to_string(),
            expected_ledger_revision: None,
            work_brief: sample_brief(&fixture, "brf_manual_legacy"),
            amended_target_task_ids: Vec::new(),
            amended_write_paths: Vec::new(),
            amended_interfaces: Vec::new(),
        })
        .expect("create manual job");
    assert!(manual_job.planning_result.is_none());

    let manual_tasks = vec![PlanTask {
        id: "task-manual-a".to_string(),
        title: "Manual Task A".to_string(),
        instruction: "CTRL_TASK_A update src/a.rs".to_string(),
        role: WorkflowRole::Coder,
        difficulty: Difficulty::Hard,
        depends_on: Vec::new(),
        allowed_write_paths: vec!["src/a.rs".to_string()],
    }];
    let draft = fixture
        .plans
        .change_at(
            0,
            PlanChange::SaveDraft {
                plan_id: "plan-manual-legacy".to_string(),
                binding_id: BINDING_A.to_string(),
                profile_id: "profile-coworker".to_string(),
                profile_revision: reg_snap.revision,
                summary: "Manual plan".to_string(),
                tasks: manual_tasks.clone(),
            },
            1_700_000_000_000,
        )
        .expect("save manual draft");
    let published = fixture
        .plans
        .change(
            draft.revision,
            PlanChange::Publish {
                plan_id: "plan-manual-legacy".to_string(),
                revision: draft.revision,
            },
        )
        .expect("publish manual plan");
    let compiled = compile_plan_revision(&published, &reg_snap).expect("compile manual plan");
    let manual_approval = fixture
        .authority
        .approve_at(
            OperatorApprovalRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                plan_id: "plan-manual-legacy".to_string(),
                plan_revision: published.revision,
                expected_registry_revision: reg_snap.revision,
                expected_plan_digest: compiled.plan_digest,
                repository_path: fixture.repo_dir.clone(),
                base_commit: fixture.base_commit.clone(),
                approved_task_ids: vec!["task-manual-a".to_string()],
                approved_write_paths: vec!["src/a.rs".to_string()],
                max_parallel: 1,
                budgets: ExecutionBudgets::default(),
            },
            1_700_000_001_000,
        )
        .expect("approve manual plan");

    let bound_manual = fixture
        .workflow_store
        .bind_job_approval(
            &BindJobApprovalRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                job_id: manual_job.job_id.clone(),
                expected_job_revision: manual_job.job_revision,
                expected_ledger_revision: None,
                approval_id: manual_approval.approval_id,
                policy: execution_policy(),
                controller_envelope: Some(FrozenControllerCapabilityEnvelope {
                    allowed_reviewer_choices: vec![headless_argon_choice(Difficulty::Hard)],
                    allowed_closure_choices: vec![headless_argon_choice(Difficulty::Extreme)],
                    max_in_scope_correction_attempts: 1,
                    allowed_check_ids: vec!["cargo_test".to_string()],
                    local_git_integration_allowed: true,
                }),
                task_interfaces: BTreeMap::new(),
            },
            &fixture.authority,
        )
        .expect("manual bind_job_approval succeeds for legacy compatibility");
    assert_eq!(bound_manual.phase, CoworkerJobPhase::Executing);
    assert!(bound_manual.planning_result.is_none());

    let manual_guard = fixture
        .ownership_store
        .try_acquire_job(
            &CoworkerJobOwnershipKey::new(BINDING_A, THREAD_UUID_A, &bound_manual.job_id)
                .expect("ownership key"),
            10_000,
        )
        .expect("try_acquire_job manual")
        .into_guard()
        .expect("acquire ownership for manual job");

    let unlinked_err = fixture
        .controller
        .step_job(
            &manual_guard,
            &CoworkerControllerStepRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                job_id: bound_manual.job_id.clone(),
                now_ms: 10_100,
                lease_ttl_ms: None,
            },
        )
        .await
        .expect_err("unlinked manual approval must be refused by automatic controller admission");
    assert_eq!(
        unlinked_err.code(),
        NativeErrorCode::ValidationFailed.as_str()
    );
    assert!(unlinked_err.message().contains("PlanningResultRef"));
    assert_eq!(read_invocation_count(&fixture.agy_count_file), 0);
    drop(manual_guard);

    // 2. Approved task budget exceeding frozen Coder role budget is refused before launch
    // (cannot silently clamp or rewrite an approval with broader worker timeout/output limits).
    let mut tight_coder_budget =
        ControllerRoleExecutionBudget::default_for_role(WorkflowRole::Coder, 4);
    tight_coder_budget.max_timeout_ms_per_call = 5_000;
    let wider_approval_budgets = ExecutionBudgets {
        timeout_ms: 30_000, // exceeds 5_000 ms frozen Coder cap
        ..ExecutionBudgets::default()
    };

    let mut iface_map = BTreeMap::new();
    iface_map.insert(
        "task-budget-a".to_string(),
        TaskInterfaceOwnershipSpec {
            owned_interfaces: vec!["iface_a".to_string()],
            consumed_interfaces: Vec::new(),
        },
    );
    let (budget_job, _) = create_planned_approved_and_frozen_job(
        &fixture,
        "brf_budget_exceed",
        "plan-budget-exceed",
        vec![PlanTask {
            id: "task-budget-a".to_string(),
            title: "Budget task".to_string(),
            instruction: "CTRL_TASK_A".to_string(),
            role: WorkflowRole::Coder,
            difficulty: Difficulty::Hard,
            depends_on: Vec::new(),
            allowed_write_paths: vec!["src/a.rs".to_string()],
        }],
        iface_map,
        vec!["src/a.rs".to_string()],
        1,
        wider_approval_budgets,
        tight_coder_budget,
    )
    .await;
    assert_eq!(read_invocation_count(&fixture.agy_planning_count_file), 1);
    assert_eq!(read_invocation_count(&fixture.agy_count_file), 0);

    let budget_guard = fixture
        .ownership_store
        .try_acquire_job(
            &CoworkerJobOwnershipKey::new(BINDING_A, THREAD_UUID_A, &budget_job.job_id)
                .expect("budget key"),
            11_000,
        )
        .expect("try_acquire_job budget")
        .into_guard()
        .expect("acquire budget guard");

    // Controller without configured `planning_artifacts_dir` must refuse admission fail-closed.
    let unconfigured_controller = CoworkerControllerService::new(
        fixture.workflow_store.clone(),
        fixture.policy_store.clone(),
        fixture.ownership_store.clone(),
        Arc::clone(&fixture.authority),
        &fixture.sessions_dir,
    )
    .expect("create controller without planning_artifacts_dir");
    let unconfigured_err = unconfigured_controller
        .verify_job_admission(&budget_guard, BINDING_A, THREAD_UUID_A, &budget_job.job_id)
        .expect_err("controller without planning_artifacts_dir must reject admission");
    assert_eq!(
        unconfigured_err.code(),
        NativeErrorCode::ValidationFailed.as_str()
    );
    assert!(unconfigured_err
        .message()
        .contains("planning_artifacts_dir is not configured"));

    let empty_artifacts_dir = fixture.planning_artifacts_dir.join("empty-subdir");
    fs::create_dir_all(&empty_artifacts_dir).expect("create empty_artifacts_dir");
    fs::set_permissions(&empty_artifacts_dir, fs::Permissions::from_mode(0o700))
        .expect("chmod 0700 empty_artifacts_dir");
    let missing_artifact_controller = CoworkerControllerService::new(
        fixture.workflow_store.clone(),
        fixture.policy_store.clone(),
        fixture.ownership_store.clone(),
        Arc::clone(&fixture.authority),
        &fixture.sessions_dir,
    )
    .expect("create missing_artifact_controller")
    .with_planning_artifacts_dir(&empty_artifacts_dir)
    .expect("configure empty_artifacts_dir");
    let missing_artifact_err = missing_artifact_controller
        .verify_job_admission(&budget_guard, BINDING_A, THREAD_UUID_A, &budget_job.job_id)
        .expect_err("controller with missing PersistedPlanningCallArtifact must reject admission");
    assert_eq!(
        missing_artifact_err.code(),
        NativeErrorCode::ValidationFailed.as_str()
    );
    assert!(missing_artifact_err
        .message()
        .contains("failed to load verified PersistedPlanningCallArtifact"));

    // Contending physical ownership acquisition on the same job must fail closed.
    let contend_outcome = fixture
        .ownership_store
        .try_acquire_job(
            &CoworkerJobOwnershipKey::new(BINDING_A, THREAD_UUID_A, &budget_job.job_id)
                .expect("budget key"),
            11_050,
        )
        .expect("try_acquire_job contention");
    assert!(
        contend_outcome.into_guard().is_none(),
        "second physical ownership acquisition while guard is held must report Busy"
    );
    let contend_err = fixture
        .controller
        .open_owned_session(BINDING_A, THREAD_UUID_A, &budget_job.job_id, 11_055)
        .expect_err("second open_owned_session while guard is held must fail");
    assert_eq!(
        contend_err.code(),
        NativeErrorCode::ValidationFailed.as_str()
    );

    // Passing a guard for a different binding/thread/job must fail with SecurityViolation.
    let wrong_scope_guard = fixture
        .ownership_store
        .try_acquire_job(
            &CoworkerJobOwnershipKey::new(BINDING_B, THREAD_UUID_B, &budget_job.job_id)
                .expect("wrong scope key"),
            11_060,
        )
        .expect("try_acquire_job wrong scope")
        .into_guard()
        .expect("acquire wrong scope guard");
    let wrong_owner_err = fixture
        .controller
        .verify_job_admission(
            &wrong_scope_guard,
            BINDING_A,
            THREAD_UUID_A,
            &budget_job.job_id,
        )
        .expect_err("wrong scope guard must be rejected");
    assert_eq!(
        wrong_owner_err.code(),
        NativeErrorCode::SecurityViolation.as_str()
    );
    drop(wrong_scope_guard);

    let budget_err = fixture
        .controller
        .step_job(
            &budget_guard,
            &CoworkerControllerStepRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                job_id: budget_job.job_id.clone(),
                now_ms: 11_100,
                lease_ttl_ms: None,
            },
        )
        .await
        .expect_err("approval budget exceeding frozen Coder role budget must fail before launch");
    assert_eq!(
        budget_err.code(),
        NativeErrorCode::ValidationFailed.as_str()
    );
    assert!(budget_err
        .message()
        .contains("exceed frozen Coder role budget caps"));
    assert_eq!(read_invocation_count(&fixture.agy_count_file), 0);
    drop(budget_guard);
}

#[allow(clippy::too_many_arguments)]
fn accept_and_integrate_completed_root_task(
    fixture: &ControllerTestFixture,
    work_area_dir: &Path,
    job: CoworkerJobRecord,
    task_id: &str,
    rel_file: &str,
    run_id: &str,
    worker_id: &str,
    task_worktree_id: &str,
    integration_worktree_id: &str,
    timestamp_ms: i64,
) -> (CoworkerJobRecord, String, String) {
    commit_task_worktree_changes(&CommitTaskChangesRequest {
        work_area_root: work_area_dir.to_path_buf(),
        task_worktree_id: task_worktree_id.to_string(),
        commit_message: format!("Commit {task_id}"),
        worker_status: Some(WorkerRunStatus::Succeeded),
        allow_missing_effort_evidence: false,
    })
    .expect("commit task worktree changes");

    let policy = ReviewPolicyBinding {
        policy_id: "policy-coworker-exec".to_string(),
        requested_provider: Some("antigravity".to_string()),
        requested_model: Some("gemini-4-argon-high".to_string()),
        requested_effort: Some("high".to_string()),
        observed_model: Some("gemini-4-argon-high".to_string()),
        observed_effort: Some("high".to_string()),
        file_permissions: WorkerFilePermissions::TaskFiles,
        require_effort_evidence: true,
        require_passing_tests: true,
    };
    let test_evidence = TestEvidenceSummary {
        formatting_passed: true,
        clippy_passed: true,
        tests_passed: true,
        passed_test_count: 4,
        failed_test_count: 0,
        evidence_summary: "checks passed".to_string(),
    };
    let subject = inspect_review_subject(&InspectReviewSubjectRequest {
        work_area_root: work_area_dir.to_path_buf(),
        task_worktree_id: task_worktree_id.to_string(),
        policy,
        test_evidence,
        worker_status: Some(WorkerRunStatus::Succeeded),
    })
    .expect("inspect review subject");

    let tag = task_id.replace('-', "_");
    let review_receipt = accept_review_subject(&ReviewAcceptanceRequest {
        subject: subject.clone(),
        expected_subject_digest: subject.subject_digest.clone(),
        reviewer_id: "op_reviewer".to_string(),
        review_note: format!("Accept {task_id}"),
        approved_at_ms: timestamp_ms,
        receipt_id: Some(format!("rev_{tag}")),
    })
    .expect("accept review subject");

    let int_manifest =
        threadlane_workers::load_and_verify_manifest(work_area_dir, integration_worktree_id)
            .expect("load integration manifest");
    let current_int_head = resolve_exact_commit_sha(
        &int_manifest.checkout_path,
        "HEAD",
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .expect("resolve integration HEAD");

    let integration_receipt = execute_integration(&ExecuteIntegrationRequest {
        work_area_root: work_area_dir.to_path_buf(),
        integration_worktree_id: integration_worktree_id.to_string(),
        task_worktree_id: task_worktree_id.to_string(),
        subject,
        review_receipt: review_receipt.clone(),
        strategy: IntegrationStrategy::FastForwardOnly,
        expected_integration_head_commit: current_int_head,
        expected_task_head_commit: review_receipt.task_head_commit.clone(),
        expected_source_head_commit: fixture.base_commit.clone(),
        operator_id: "op_integrator".to_string(),
        integrated_at_ms: timestamp_ms + 10,
        receipt_id: Some(format!("int_{tag}")),
    })
    .expect("execute integration");

    let policy_digest = job
        .approval_linkage
        .as_ref()
        .expect("linkage")
        .policy_digest
        .clone();

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
            task_id: task_id.to_string(),
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
            run_id: Some(run_id.to_string()),
            worker_id: Some(worker_id.to_string()),
            task_worktree_id: Some(task_worktree_id.to_string()),
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

    let candidate_id = format!("cnd_{tag}");
    let verdict = StructuredReviewVerdict {
        verdict_id: format!("vrd_{tag}"),
        task_id: task_id.to_string(),
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
    };
    let candidate = TaskCandidateRef {
        candidate_id,
        task_id: task_id.to_string(),
        attempt_no: 1,
        base_commit: review_receipt.base_commit.clone(),
        head_commit: review_receipt.task_head_commit.clone(),
        tree_sha: review_receipt.task_tree_sha.clone(),
        diff_digest: review_receipt.diff_digest.clone(),
        write_paths: vec![rel_file.to_string()],
        check_evidence_digests: vec!["aa".repeat(32)],
        review_subject_digest: Some(review_receipt.subject_digest.clone()),
        review_verdict: Some(verdict),
        review_receipt_id: Some(review_receipt.receipt_id.clone()),
        integration_receipt_id: None,
    };
    let (job, _) = fixture
        .workflow_store
        .record_task_candidate_and_verdict(&RecordTaskReviewVerdictRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            job_id: job.job_id.clone(),
            expected_job_revision: job.job_revision,
            expected_ledger_revision: None,
            candidate,
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
            task_id: task_id.to_string(),
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
            run_id: Some(run_id.to_string()),
            worker_id: None,
            task_worktree_id: Some(task_worktree_id.to_string()),
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

    (
        job,
        review_receipt.receipt_id,
        integration_receipt.receipt_id,
    )
}

#[tokio::test]
async fn two_roots_and_dependent_launch_bounded_parallel_block_until_accepted_seal_and_reopen_idempotently(
) {
    let fixture = setup_fixture();

    let tasks = vec![
        PlanTask {
            id: "task-root-a".to_string(),
            title: "Root A".to_string(),
            instruction: "CTRL_TASK_A implement src/a.rs".to_string(),
            role: WorkflowRole::Coder,
            difficulty: Difficulty::Hard,
            depends_on: Vec::new(),
            allowed_write_paths: vec!["src/a.rs".to_string()],
        },
        PlanTask {
            id: "task-root-b".to_string(),
            title: "Root B".to_string(),
            instruction: "CTRL_TASK_B implement src/b.rs".to_string(),
            role: WorkflowRole::Coder,
            difficulty: Difficulty::Hard,
            depends_on: Vec::new(),
            allowed_write_paths: vec!["src/b.rs".to_string()],
        },
        PlanTask {
            id: "task-dep-c".to_string(),
            title: "Dependent C".to_string(),
            instruction: "CTRL_TASK_C implement src/c.rs".to_string(),
            role: WorkflowRole::Coder,
            difficulty: Difficulty::Hard,
            depends_on: vec!["task-root-a".to_string(), "task-root-b".to_string()],
            allowed_write_paths: vec!["src/c.rs".to_string()],
        },
    ];

    let mut task_interfaces = BTreeMap::new();
    task_interfaces.insert(
        "task-root-a".to_string(),
        TaskInterfaceOwnershipSpec {
            owned_interfaces: vec!["iface_a".to_string()],
            consumed_interfaces: Vec::new(),
        },
    );
    task_interfaces.insert(
        "task-root-b".to_string(),
        TaskInterfaceOwnershipSpec {
            owned_interfaces: vec!["iface_b".to_string()],
            consumed_interfaces: Vec::new(),
        },
    );
    task_interfaces.insert(
        "task-dep-c".to_string(),
        TaskInterfaceOwnershipSpec {
            owned_interfaces: vec!["iface_c".to_string()],
            consumed_interfaces: vec!["iface_a".to_string(), "iface_b".to_string()],
        },
    );

    let (job, approval_id) = create_planned_approved_and_frozen_job(
        &fixture,
        "brf_dag_3",
        "plan-dag-3",
        tasks,
        task_interfaces,
        vec![
            "src/a.rs".to_string(),
            "src/b.rs".to_string(),
            "src/c.rs".to_string(),
        ],
        2, // max_parallel = 2
        ExecutionBudgets::default(),
        ControllerRoleExecutionBudget::default_for_role(WorkflowRole::Coder, 4),
    )
    .await;
    assert_eq!(read_invocation_count(&fixture.agy_planning_count_file), 1);
    assert_eq!(read_invocation_count(&fixture.agy_count_file), 0);

    let host_handle = ApprovedHost::start(ApprovedHostConfig::new(
        &fixture.registry_dir,
        &fixture.plans_dir,
        &fixture.authority_dir,
        &fixture.sessions_dir,
        &approval_id,
    ))
    .await
    .expect("start ApprovedHost session");
    assert_eq!(host_handle.summary().phase, HostPhase::Ready);

    let session = fixture
        .controller
        .open_owned_session(BINDING_A, THREAD_UUID_A, &job.job_id, 20_000)
        .expect("open owned controller session");

    // Step until both root tasks finish ReadyForReview on the host.
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut step_ts = 20_100u64;
    let report_roots = loop {
        let rep = session.step(step_ts).await.expect("controller step");
        step_ts += 100;
        let root_a_obs = rep
            .tasks
            .iter()
            .find(|t| t.task_id == "task-root-a")
            .expect("task-root-a obs");
        let root_b_obs = rep
            .tasks
            .iter()
            .find(|t| t.task_id == "task-root-b")
            .expect("task-root-b obs");
        if root_a_obs.stage == CoworkerTaskStage::ReadyForReview
            && root_b_obs.stage == CoworkerTaskStage::ReadyForReview
        {
            break rep;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for root tasks to reach ReadyForReview; last report: {rep:?}"
        );
        tokio::time::sleep(Duration::from_millis(30)).await;
    };

    assert_eq!(
        report_roots.status,
        CoworkerControllerPhase1Status::AwaitingChecksOrReview
    );
    assert_eq!(
        read_invocation_count(&fixture.agy_count_file),
        2,
        "exactly the two independent ready roots must have been launched"
    );

    let dep_c_obs = report_roots
        .tasks
        .iter()
        .find(|t| t.task_id == "task-dep-c")
        .expect("task-dep-c obs");
    assert_eq!(
        dep_c_obs.stage,
        CoworkerTaskStage::PendingDependencies,
        "dependent task must remain PendingDependencies when predecessors are merely ReadyForReview"
    );
    assert_eq!(
        dep_c_obs.host_status,
        TaskExecutionStatus::DependencyInputsUnverified
    );
    assert!(dep_c_obs.run_id.is_none());

    for root_id in ["task-root-a", "task-root-b"] {
        let obs = report_roots
            .tasks
            .iter()
            .find(|t| t.task_id == root_id)
            .expect("root obs");
        assert_eq!(obs.stage, CoworkerTaskStage::ReadyForReview);
        assert_eq!(obs.host_status, TaskExecutionStatus::ReadyForReview);
        assert!(obs.run_id.as_deref().is_some_and(|id| !id.is_empty()));
        assert!(obs.worker_id.as_deref().is_some_and(|id| !id.is_empty()));
        assert!(obs
            .task_worktree_id
            .as_deref()
            .is_some_and(|id| !id.is_empty()));
        assert_eq!(obs.observed_model.as_deref(), Some("gemini-4-argon-high"));
        assert_eq!(obs.observed_effort.as_deref(), Some("high"));
    }

    // Repeated ticks and store/controller reopen must NOT re-launch workers or double-charge caps.
    let runtime_before_reopen = fixture
        .policy_store
        .get_job_runtime_readonly(BINDING_A, &job.job_id)
        .expect("get runtime")
        .expect("runtime exists");
    assert_eq!(runtime_before_reopen.total_task_attempts_reserved, 2);
    let coder_usage_before = runtime_before_reopen
        .role_usage
        .get(&WorkflowRole::Coder)
        .expect("coder usage");
    assert_eq!(coder_usage_before.calls_reserved, 2);
    assert_eq!(coder_usage_before.calls_completed, 2);

    session.release().expect("release first session");
    let runtime_after_release = fixture
        .policy_store
        .get_job_runtime_readonly(BINDING_A, &job.job_id)
        .expect("get runtime after release")
        .expect("runtime exists");
    assert!(
        runtime_after_release
            .lease
            .as_ref()
            .is_some_and(|l| !l.active),
        "explicit session.release() must mark its own logical lease inactive before dropping physical guard"
    );

    let reopened_controller = CoworkerControllerService::new(
        CoworkerWorkflowStore::new(&fixture.plans_dir).expect("reopen workflow_store"),
        ControllerPolicyStore::new(&fixture.plans_dir).expect("reopen policy_store"),
        CoworkerJobOwnershipStore::new(&fixture.ownership_dir).expect("reopen ownership_store"),
        Arc::clone(&fixture.authority),
        &fixture.sessions_dir,
    )
    .expect("reopen controller")
    .with_planning_artifacts_dir(&fixture.planning_artifacts_dir)
    .expect("configure reopened controller planning_artifacts_dir");
    let first_reopened_session = reopened_controller
        .open_owned_session(BINDING_A, THREAD_UUID_A, &job.job_id, step_ts)
        .expect("open reopened session");
    step_ts += 100;
    let rep_idempotent = first_reopened_session
        .step(step_ts)
        .await
        .expect("idempotent step after reopen");
    step_ts += 100;
    assert_eq!(rep_idempotent.lease_epoch, 2);
    assert!(rep_idempotent.dispatched_task_ids.is_empty());
    assert_eq!(read_invocation_count(&fixture.agy_count_file), 2);

    // Simulate an ungraceful controller crash/drop (dropping the physical OS lock guard without
    // calling `release()`, leaving `lease.active == true` with an unexpired TTL on disk).
    drop(first_reopened_session);
    let runtime_with_stale_lease = fixture
        .policy_store
        .get_job_runtime_readonly(BINDING_A, &job.job_id)
        .expect("get runtime with stale lease")
        .expect("runtime exists");
    assert!(
        runtime_with_stale_lease
            .lease
            .as_ref()
            .is_some_and(|l| l.active && l.expires_at_ms > step_ts),
        "dropped session leaves an unexpired logical lease on disk"
    );

    // A new controller that acquires the genuine non-clonable physical OS FD lock must immediately
    // recover the stale logical lease without waiting for TTL expiry and without resetting counters.
    let reopened_session = reopened_controller
        .open_owned_session(BINDING_A, THREAD_UUID_A, &job.job_id, step_ts)
        .expect("open session after ungraceful drop");
    let active_contend_err = reopened_controller
        .open_owned_session(BINDING_A, THREAD_UUID_A, &job.job_id, step_ts + 10)
        .expect_err("active physical owner must block concurrent session acquisition");
    assert_eq!(
        active_contend_err.code(),
        NativeErrorCode::ValidationFailed.as_str()
    );
    step_ts += 100;
    let rep_crash_recovered = reopened_session
        .step(step_ts)
        .await
        .expect("idempotent step after crash-style lease recovery");
    step_ts += 100;
    assert_eq!(rep_crash_recovered.lease_epoch, 3);
    assert!(rep_crash_recovered.dispatched_task_ids.is_empty());
    assert_eq!(read_invocation_count(&fixture.agy_count_file), 2);

    let runtime_after_reopen = fixture
        .policy_store
        .get_job_runtime_readonly(BINDING_A, &job.job_id)
        .expect("get runtime after reopen")
        .expect("runtime exists");
    assert_eq!(runtime_after_reopen.total_task_attempts_reserved, 2);
    let coder_usage_after = runtime_after_reopen
        .role_usage
        .get(&WorkflowRole::Coder)
        .expect("coder usage after reopen");
    assert_eq!(coder_usage_after.calls_reserved, 2);
    assert_eq!(coder_usage_after.calls_completed, 2);

    // Accept, integrate, and seal dependency inputs for `task-root-a` and `task-root-b`, then
    // verify the controller promotes `task-dep-c` to `ReadyToDispatch`, dispatches it, and
    // reconciles it to `ReadyForReview`.
    let host_client = ApprovedHostClient::new(
        Arc::clone(&fixture.authority),
        &fixture.sessions_dir,
        BINDING_A,
        THREAD_UUID_A,
    )
    .expect("create ApprovedHostClient");

    let snap_a = host_client
        .get_task(&approval_id, "task-root-a")
        .await
        .expect("get_task A");
    let snap_b = host_client
        .get_task(&approval_id, "task-root-b")
        .await
        .expect("get_task B");
    let worker_a = snap_a.worker.as_ref().expect("worker A");
    let worker_b = snap_b.worker.as_ref().expect("worker B");

    let current_job = fixture
        .workflow_store
        .read_binding_ledger(BINDING_A)
        .expect("read ledger")
        .jobs
        .into_iter()
        .find(|j| j.job_id == job.job_id)
        .expect("find job");

    let (current_job, rev_a_id, int_a_id) = accept_and_integrate_completed_root_task(
        &fixture,
        host_handle.work_area_dir(),
        current_job,
        "task-root-a",
        "src/a.rs",
        snap_a.run_id.as_deref().expect("run_id A"),
        snap_a.worker_id.as_deref().expect("worker_id A"),
        worker_a
            .worktree
            .task_worktree_id
            .as_deref()
            .expect("task_worktree_id A"),
        worker_a
            .worktree
            .run_worktree_id
            .as_deref()
            .expect("run_worktree_id A"),
        1_700_000_200_000,
    );
    let (_, rev_b_id, int_b_id) = accept_and_integrate_completed_root_task(
        &fixture,
        host_handle.work_area_dir(),
        current_job,
        "task-root-b",
        "src/b.rs",
        snap_b.run_id.as_deref().expect("run_id B"),
        snap_b.worker_id.as_deref().expect("worker_id B"),
        worker_b
            .worktree
            .task_worktree_id
            .as_deref()
            .expect("task_worktree_id B"),
        worker_b
            .worktree
            .run_worktree_id
            .as_deref()
            .expect("run_worktree_id B"),
        1_700_000_201_000,
    );

    let approval_rec = fixture
        .authority
        .get_for_operator(&approval_id)
        .expect("get approval");
    host_client
        .prepare_dependency_inputs(PrepareDependencyInputsRequest {
            approval_id: approval_id.clone(),
            expected_approval_digest: approval_rec.subject_digest,
            task_id: "task-dep-c".to_string(),
            predecessors: vec![
                DependencyPredecessorSelection {
                    task_id: "task-root-a".to_string(),
                    review_receipt_id: rev_a_id,
                    integration_receipt_id: int_a_id,
                },
                DependencyPredecessorSelection {
                    task_id: "task-root-b".to_string(),
                    review_receipt_id: rev_b_id,
                    integration_receipt_id: int_b_id,
                },
            ],
        })
        .await
        .expect("prepare_dependency_inputs for task-dep-c");

    let mut promoted_seen = false;
    let dep_deadline = Instant::now() + Duration::from_secs(10);
    let report_dep = loop {
        let rep = reopened_session
            .step(step_ts)
            .await
            .expect("step dependent C");
        step_ts += 100;
        if rep
            .promoted_dependency_ready_task_ids
            .contains(&"task-dep-c".to_string())
        {
            promoted_seen = true;
        }
        let dep_obs = rep
            .tasks
            .iter()
            .find(|t| t.task_id == "task-dep-c")
            .expect("task-dep-c obs");
        if dep_obs.stage == CoworkerTaskStage::ReadyForReview {
            break rep;
        }
        assert!(
            Instant::now() < dep_deadline,
            "timed out waiting for task-dep-c to reach ReadyForReview; last report: {rep:?}"
        );
        tokio::time::sleep(Duration::from_millis(30)).await;
    };

    assert!(promoted_seen);
    assert_eq!(
        report_dep.status,
        CoworkerControllerPhase1Status::AwaitingChecksOrReview
    );
    assert_eq!(read_invocation_count(&fixture.agy_count_file), 3);

    reopened_session
        .release()
        .expect("release reopened session");
    host_handle.shutdown().await.expect("shutdown host");
}

#[tokio::test]
async fn hold_stop_unknown_intent_and_conflicting_request_key_fence_dispatch_and_promotion() {
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
        "brf_hold_fence",
        "plan-hold-fence",
        tasks,
        iface_map,
        vec!["src/a.rs".to_string(), "src/b.rs".to_string()],
        2,
        ExecutionBudgets::default(),
        ControllerRoleExecutionBudget::default_for_role(WorkflowRole::Coder, 4),
    )
    .await;
    assert_eq!(read_invocation_count(&fixture.agy_planning_count_file), 1);
    assert_eq!(read_invocation_count(&fixture.agy_count_file), 0);

    let host_handle = ApprovedHost::start(ApprovedHostConfig::new(
        &fixture.registry_dir,
        &fixture.plans_dir,
        &fixture.authority_dir,
        &fixture.sessions_dir,
        &approval_id,
    ))
    .await
    .expect("start host");

    // 1. Apply a hold targeting `task-hold-a`. Because `independence_completeness_receipt` is None,
    // conservative hold-all fences BOTH `task-hold-a` and `task-hold-b` before any dispatch.
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
        .controller
        .open_owned_session(BINDING_A, THREAD_UUID_A, &held_job.job_id, 30_000)
        .expect("open session");

    let held_report = session.step(30_100).await.expect("step while held");
    assert_eq!(held_report.status, CoworkerControllerPhase1Status::Held);
    assert!(held_report.dispatched_task_ids.is_empty());
    assert_eq!(read_invocation_count(&fixture.agy_count_file), 0);

    // 2. Request stop on the job runtime state: even if a worker finished on the host prior to
    // controller reconciliation, `stop_requested` fences promoting its output while preserving the
    // actual host receipt and task worktree on disk.
    fixture
        .policy_store
        .mutate_job_runtime_for_operator(BINDING_A, &held_job.job_id, |state| {
            state.stop_requested = true;
            state.stop_or_block_reason = Some("operator requested stop".to_string());
            Ok(())
        })
        .expect("set stop_requested");

    let host_client = ApprovedHostClient::new(
        Arc::clone(&fixture.authority),
        &fixture.sessions_dir,
        BINDING_A,
        THREAD_UUID_A,
    )
    .expect("create host client");
    host_client
        .dispatch_task(&approval_id, "task-hold-b")
        .await
        .expect("dispatch task-hold-b directly on host");

    let wait_deadline = Instant::now() + Duration::from_secs(8);
    let snap_b = loop {
        let s = host_client
            .get_task(&approval_id, "task-hold-b")
            .await
            .expect("get_task B");
        if s.status == TaskExecutionStatus::ReadyForReview {
            break s;
        }
        assert!(Instant::now() < wait_deadline, "timed out waiting for B");
        tokio::time::sleep(Duration::from_millis(25)).await;
    };
    let worktree_path = PathBuf::from(
        snap_b
            .worker
            .as_ref()
            .and_then(|w| w.worktree.task_worktree_path.clone())
            .expect("task_worktree_path B"),
    );
    assert!(worktree_path.exists());

    let stopped_report = session.step(30_200).await.expect("step while stopped");
    assert_eq!(
        stopped_report.status,
        CoworkerControllerPhase1Status::Stopped
    );
    assert!(
        stopped_report
            .reconciled_ready_for_review_task_ids
            .is_empty(),
        "fenced/stopped job must not promote late worker output to ReadyForReview"
    );
    assert!(
        worktree_path.exists(),
        "fenced worker task worktree must be preserved on disk"
    );

    // 3. Conflicting request_key (different from the frozen ApprovedTaskRecord.command_key) on an
    // initial Coder attempt must fail closed without resetting attempt caps or replaying.
    fixture
        .policy_store
        .mutate_job_runtime_for_operator(BINDING_A, &held_job.job_id, |state| {
            state.stop_requested = false;
            state.stop_or_block_reason = None;
            state.reserve_role_call(WorkflowRole::Coder)?;
            state.pending_effects.insert(
                "eff_00000000000000000000000000000099".to_string(),
                PendingControllerEffect {
                    effect_id: "eff_00000000000000000000000000000099".to_string(),
                    request_key: "req_forged_new_key".to_string(),
                    job_id: held_job.job_id.clone(),
                    task_id: Some("task-hold-b".to_string()),
                    attempt_no: Some(1),
                    kind: PendingControllerEffectKind::CoderDispatch,
                    status: PendingControllerEffectStatus::EffectUnknown,
                    subject_digest: sha256_hex(b"forged-subject"),
                    external_correlation_id: None,
                    created_at_ms: 30_250,
                    updated_at_ms: 30_250,
                    diagnostic: Some("simulated unknown effect".to_string()),
                },
            );
            Ok(())
        })
        .expect("record unknown effect with conflicting key");

    let conflict_err = session
        .step(30_300)
        .await
        .expect_err("conflicting initial Coder request_key != frozen command_key must fail");
    assert_eq!(
        conflict_err.code(),
        NativeErrorCode::ValidationFailed.as_str()
    );
    assert!(conflict_err.message().contains("frozen command_key"));

    session.release().expect("release session");
    host_handle.shutdown().await.expect("shutdown host");
}

#[tokio::test]
async fn task_verification_commits_coder_worktree_executes_frozen_checks_and_reconciles_idempotently(
) {
    let fixture = setup_fixture();
    let check_count_file = fixture
        .plans_dir
        .parent()
        .expect("fixture root")
        .join("cargo_check_count.txt");
    write_executable_script(
        &fixture.fake_cargo_exe,
        &format!(
            "#!/bin/sh\nset -eu\nc=0\nif [ -f \"{}\" ]; then c=$(cat \"{}\"); fi\nc=$((c + 1))\nprintf \"%s\" \"$c\" > \"{}\"\nprintf \"running 1 test\\ntest src_a ... ok\\n\"\nexit 0\n",
            check_count_file.display(),
            check_count_file.display(),
            check_count_file.display(),
        ),
    );

    let mut task_interfaces = BTreeMap::new();
    task_interfaces.insert(
        "task-verify-a".to_string(),
        TaskInterfaceOwnershipSpec {
            owned_interfaces: vec!["iface_verify_a".to_string()],
            consumed_interfaces: Vec::new(),
        },
    );
    let (job, approval_id) = create_planned_approved_and_frozen_job(
        &fixture,
        "brf_verify_pass",
        "plan-verify-pass",
        vec![PlanTask {
            id: "task-verify-a".to_string(),
            title: "Verify Task A".to_string(),
            instruction: "CTRL_TASK_A implement src/a.rs".to_string(),
            role: WorkflowRole::Coder,
            difficulty: Difficulty::Hard,
            depends_on: Vec::new(),
            allowed_write_paths: vec!["src/a.rs".to_string()],
        }],
        task_interfaces,
        vec!["src/a.rs".to_string()],
        1,
        ExecutionBudgets::default(),
        ControllerRoleExecutionBudget::default_for_role(WorkflowRole::Coder, 4),
    )
    .await;

    let host_handle = ApprovedHost::start(ApprovedHostConfig::new(
        &fixture.registry_dir,
        &fixture.plans_dir,
        &fixture.authority_dir,
        &fixture.sessions_dir,
        &approval_id,
    ))
    .await
    .expect("start ApprovedHost");

    let session = fixture
        .controller
        .open_owned_session(BINDING_A, THREAD_UUID_A, &job.job_id, 40_000)
        .expect("open controller session");

    let deadline = Instant::now() + Duration::from_secs(10);
    let mut step_ts = 40_100u64;
    loop {
        let rep = session.step(step_ts).await.expect("controller step");
        step_ts += 100;
        let obs = rep
            .tasks
            .iter()
            .find(|t| t.task_id == "task-verify-a")
            .expect("task-verify-a observation");
        if obs.stage == CoworkerTaskStage::ReadyForReview {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for task-verify-a ReadyForReview: {rep:?}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    let verifier = CoworkerTaskVerificationService::new(fixture.controller.clone());
    let outcome = verifier
        .verify_and_prepare_task_attempt(
            session.guard(),
            &CoworkerTaskVerificationRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                job_id: job.job_id.clone(),
                task_id: "task-verify-a".to_string(),
                attempt_no: 1,
                now_ms: step_ts,
            },
        )
        .await
        .expect("verify_and_prepare_task_attempt");

    assert_eq!(
        outcome.status,
        CoworkerTaskVerificationStatus::ReadyForReviewer
    );
    assert_eq!(outcome.task_stage, CoworkerTaskStage::ReadyForReview);
    assert_ne!(outcome.committed_head_commit, fixture.base_commit);
    assert_ne!(outcome.committed_tree_sha, fixture.base_tree_sha);
    assert_eq!(read_invocation_count(&check_count_file), 1);
    assert_eq!(outcome.check_reports.len(), 1);
    assert_eq!(
        outcome.check_reports[0].report.status,
        FrozenCheckExecutionStatus::Passed
    );
    let persisted_report =
        read_persisted_check_report(&fixture.plans_dir, &outcome.check_reports[0].effect_id)
            .expect("read persisted check report")
            .expect("persisted check report exists");
    assert_eq!(persisted_report, outcome.check_reports[0].report);
    assert_eq!(outcome.check_evidence.len(), 1);

    let review_subject = outcome
        .review_subject
        .as_ref()
        .expect("promoted ReviewSubject");
    assert_eq!(
        review_subject.task_head_commit,
        outcome.committed_head_commit
    );
    assert_eq!(review_subject.task_tree_sha, outcome.committed_tree_sha);
    assert_eq!(review_subject.base_commit, fixture.base_commit);

    // Primary checkout must remain untouched at base_commit.
    let primary_head =
        resolve_exact_commit_sha(&fixture.repo_dir, "HEAD", DEFAULT_GIT_COMMAND_TIMEOUT_MS)
            .expect("resolve primary HEAD");
    assert_eq!(primary_head, fixture.base_commit);
    assert_eq!(
        fs::read_to_string(fixture.repo_dir.join("src/a.rs")).expect("read primary src/a.rs"),
        "pub fn a() -> u32 { 1 }\n"
    );

    let runtime = fixture
        .policy_store
        .get_job_runtime_readonly(BINDING_A, &job.job_id)
        .expect("get job runtime")
        .expect("runtime exists");
    let art_key = ControllerJobRuntimeState::artifact_key("task-verify-a", 1);
    let stored_art = runtime
        .attempt_artifacts
        .get(&art_key)
        .expect("stored attempt artifacts");
    assert_eq!(
        stored_art.committed_head_commit.as_deref(),
        Some(outcome.committed_head_commit.as_str())
    );
    assert_eq!(
        stored_art.committed_tree_sha.as_deref(),
        Some(outcome.committed_tree_sha.as_str())
    );
    assert_eq!(
        stored_art.candidate_id.as_deref(),
        Some(outcome.candidate_id.as_str())
    );
    assert_eq!(stored_art.check_evidence, outcome.check_evidence);
    assert_eq!(stored_art.review_subject, outcome.review_subject);
    assert!(stored_art.review_receipt.is_none());
    assert!(stored_art.integration_receipt.is_none());

    // Repeat call and reopened-session call must be idempotent without re-committing or re-running checks.
    let repeat_outcome = verifier
        .verify_and_prepare_task_attempt(
            session.guard(),
            &CoworkerTaskVerificationRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                job_id: job.job_id.clone(),
                task_id: "task-verify-a".to_string(),
                attempt_no: 1,
                now_ms: step_ts + 100,
            },
        )
        .await
        .expect("repeat verify_and_prepare_task_attempt");
    assert_eq!(repeat_outcome, outcome);
    assert_eq!(read_invocation_count(&check_count_file), 1);

    session.release().expect("release session");
    let reopened_session = fixture
        .controller
        .open_owned_session(BINDING_A, THREAD_UUID_A, &job.job_id, step_ts + 200)
        .expect("reopen session");
    let reopened_outcome = verifier
        .verify_and_prepare_task_attempt(
            reopened_session.guard(),
            &CoworkerTaskVerificationRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                job_id: job.job_id.clone(),
                task_id: "task-verify-a".to_string(),
                attempt_no: 1,
                now_ms: step_ts + 300,
            },
        )
        .await
        .expect("reopened verify_and_prepare_task_attempt");
    assert_eq!(reopened_outcome, outcome);
    assert_eq!(read_invocation_count(&check_count_file), 1);

    reopened_session
        .release()
        .expect("release reopened session");
    host_handle.shutdown().await.expect("shutdown host");
}

#[tokio::test]
async fn task_verification_blocks_reviewer_on_failed_frozen_check_and_retains_artifacts() {
    let fixture = setup_fixture();
    let check_count_file = fixture
        .plans_dir
        .parent()
        .expect("fixture root")
        .join("cargo_fail_count.txt");
    write_executable_script(
        &fixture.fake_cargo_exe,
        &format!(
            "#!/bin/sh\nset -eu\nc=0\nif [ -f \"{}\" ]; then c=$(cat \"{}\"); fi\nc=$((c + 1))\nprintf \"%s\" \"$c\" > \"{}\"\necho \"test src_a ... FAILED\" >&2\nexit 1\n",
            check_count_file.display(),
            check_count_file.display(),
            check_count_file.display(),
        ),
    );

    let mut task_interfaces = BTreeMap::new();
    task_interfaces.insert(
        "task-verify-fail".to_string(),
        TaskInterfaceOwnershipSpec {
            owned_interfaces: vec!["iface_verify_fail".to_string()],
            consumed_interfaces: Vec::new(),
        },
    );
    let (job, approval_id) = create_planned_approved_and_frozen_job(
        &fixture,
        "brf_verify_fail",
        "plan-verify-fail",
        vec![PlanTask {
            id: "task-verify-fail".to_string(),
            title: "Verify Failing Check Task".to_string(),
            instruction: "CTRL_TASK_A implement src/a.rs".to_string(),
            role: WorkflowRole::Coder,
            difficulty: Difficulty::Hard,
            depends_on: Vec::new(),
            allowed_write_paths: vec!["src/a.rs".to_string()],
        }],
        task_interfaces,
        vec!["src/a.rs".to_string()],
        1,
        ExecutionBudgets::default(),
        ControllerRoleExecutionBudget::default_for_role(WorkflowRole::Coder, 4),
    )
    .await;

    let host_handle = ApprovedHost::start(ApprovedHostConfig::new(
        &fixture.registry_dir,
        &fixture.plans_dir,
        &fixture.authority_dir,
        &fixture.sessions_dir,
        &approval_id,
    ))
    .await
    .expect("start ApprovedHost");

    let session = fixture
        .controller
        .open_owned_session(BINDING_A, THREAD_UUID_A, &job.job_id, 50_000)
        .expect("open controller session");

    let deadline = Instant::now() + Duration::from_secs(10);
    let mut step_ts = 50_100u64;
    loop {
        let rep = session.step(step_ts).await.expect("controller step");
        step_ts += 100;
        let obs = rep
            .tasks
            .iter()
            .find(|t| t.task_id == "task-verify-fail")
            .expect("task-verify-fail observation");
        if obs.stage == CoworkerTaskStage::ReadyForReview {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for task-verify-fail ReadyForReview: {rep:?}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    let verifier = CoworkerTaskVerificationService::new(fixture.controller.clone());
    let outcome = verifier
        .verify_and_prepare_task_attempt(
            session.guard(),
            &CoworkerTaskVerificationRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                job_id: job.job_id.clone(),
                task_id: "task-verify-fail".to_string(),
                attempt_no: 1,
                now_ms: step_ts,
            },
        )
        .await
        .expect("verify_and_prepare_task_attempt returns BlockedByFailedChecks");

    assert_eq!(
        outcome.status,
        CoworkerTaskVerificationStatus::BlockedByFailedChecks
    );
    assert_eq!(outcome.task_stage, CoworkerTaskStage::ReadyForReview);
    assert_eq!(read_invocation_count(&check_count_file), 1);
    assert_eq!(outcome.check_reports.len(), 1);
    assert_eq!(
        outcome.check_reports[0].report.status,
        FrozenCheckExecutionStatus::NonZeroExit
    );
    assert!(outcome.check_evidence.is_empty());
    assert!(outcome.test_evidence_summary.is_none());
    assert!(outcome.review_subject.is_none());

    let persisted_report =
        read_persisted_check_report(&fixture.plans_dir, &outcome.check_reports[0].effect_id)
            .expect("read persisted failed check report")
            .expect("persisted failed check report exists");
    assert_eq!(persisted_report, outcome.check_reports[0].report);

    let runtime = fixture
        .policy_store
        .get_job_runtime_readonly(BINDING_A, &job.job_id)
        .expect("get job runtime")
        .expect("runtime exists");
    let art_key = ControllerJobRuntimeState::artifact_key("task-verify-fail", 1);
    let stored_art = runtime
        .attempt_artifacts
        .get(&art_key)
        .expect("stored attempt artifacts");
    assert_eq!(
        stored_art.committed_head_commit.as_deref(),
        Some(outcome.committed_head_commit.as_str())
    );
    assert!(stored_art.check_evidence.is_empty());
    assert!(stored_art.test_evidence_summary.is_none());
    assert!(stored_art.review_subject.is_none());

    session.release().expect("release session");
    host_handle.shutdown().await.expect("shutdown host");
}
