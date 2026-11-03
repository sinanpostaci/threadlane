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
    compute_derived_coder_effect_id, compute_derived_coder_request_key,
    compute_derived_task_attempt_receipt_id, compute_task_integration_subject_digest,
    compute_task_review_effect_id, compute_task_review_request_key, deterministic_prefixed_id,
    deterministic_task_integration_receipt_id, deterministic_task_integration_request_key,
    integrate_accepted_task, read_persisted_check_report, read_persisted_task_review_report,
    sha256_hex, ApplyWorkflowHoldRequest, BindJobApprovalRequest, CanonicalProjectBaseRef,
    ControllerConfigurationPreview, ControllerJobRuntimeState, ControllerPolicyStore,
    ControllerRoleExecutionBudget, CoworkerControllerPhase1Status, CoworkerControllerService,
    CoworkerControllerStepRequest, CoworkerJobOwnershipGuard, CoworkerJobOwnershipKey,
    CoworkerJobOwnershipStore, CoworkerJobPhase, CoworkerJobRecord, CoworkerPlanningService,
    CoworkerTaskCorrectionDisposition, CoworkerTaskCorrectionPrepareRequest,
    CoworkerTaskCorrectionPrepareStatus, CoworkerTaskCorrectionService,
    CoworkerTaskIntegrationRequest, CoworkerTaskIntegrationService, CoworkerTaskIntegrationStatus,
    CoworkerTaskReviewStageRequest, CoworkerTaskReviewStageService, CoworkerTaskReviewStageStatus,
    CoworkerTaskStage, CoworkerTaskVerificationOutcome, CoworkerTaskVerificationRequest,
    CoworkerTaskVerificationService, CoworkerTaskVerificationStatus, CoworkerWorkflowStore,
    CreateCoworkerJobRequest, CreatePlanningGrantRequest, ExpectedTaskReviewIdentity,
    FinalizeTaskOutboxRequest, FreezeControllerExecutionEnvelopeRequest, FrozenCheckCommandSpec,
    FrozenCheckExecutionStatus, FrozenCheckKind, FrozenControllerCapabilityEnvelope,
    OrchestrateWorkBriefPlanRequest, OutboxDispatchKind, OutboxDispatchStatus,
    PendingControllerEffect, PendingControllerEffectKind, PendingControllerEffectStatus,
    PlannedCheckSuggestion, PlanningCallBudget, PlanningReadOnlyCapabilities,
    PlanningReservationStatus, RecordTaskReviewVerdictRequest, ReserveTaskOutboxRequest,
    ResolveWorkflowHoldRequest, ReviewVerdictOutcome, StructuredOrchestratorOutput,
    StructuredPlannedTask, StructuredReviewVerdict, StructuredTaskReviewModelResponse,
    TaskCandidateRef, TaskInterfaceOwnershipSpec, TaskReviewExecutionStatus, WorkBrief,
    WorkBriefKind, WorkflowHoldReason, TASK_REVIEW_RESPONSE_SCHEMA_VERSION,
};
use threadlane_native::{
    Difficulty, ExecutionPolicy, NativeErrorCode, PolicyAllowRule, RegistryStore,
    RoleExecutionRoute, SelectedRoleChoice, ToolAdapter, WorkflowProfile, WorkflowRole,
};
use threadlane_operations::{
    accept_review_subject, commit_task_worktree_changes, execute_integration,
    inspect_review_subject, load_integration_receipt, load_review_receipt,
    verify_review_receipt_readonly, CommitTaskChangesRequest, ExecuteIntegrationRequest,
    InspectReviewSubjectRequest, IntegrationStrategy, ReviewAcceptanceRequest, ReviewPolicyBinding,
    ReviewReceipt, ReviewSubject, TestEvidenceSummary, RECEIPTS_SUBDIR,
};
use threadlane_workers::{
    resolve_exact_commit_sha, resolve_exact_tree_sha, run_sanitized_git, ExecutionBudgets,
    WorkerFilePermissions, WorkerRunStatus, DEFAULT_GIT_COMMAND_TIMEOUT_MS, MANIFESTS_SUBDIR,
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
        *CTRL_TASK_C_ONLY_A*)
            if ! grep -q "100" "src/a.rs"; then
                echo "expected integrated predecessor contents in src/a.rs" >&2
                exit 3
            fi
            printf "pub fn c() -> u32 {{ 300 }}\n" > "src/c.rs"
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

async fn verify_and_accept_task_without_integrating(
    fixture: &ControllerTestFixture,
    guard: &CoworkerJobOwnershipGuard,
    job_id: &str,
    task_id: &str,
    rel_file: &str,
    now_ms: u64,
    approved_at_ms: i64,
) -> (
    CoworkerJobRecord,
    CoworkerTaskVerificationOutcome,
    ReviewSubject,
    ReviewReceipt,
) {
    let verifier = CoworkerTaskVerificationService::new(fixture.controller.clone());
    let verify_outcome = verifier
        .verify_and_prepare_task_attempt(
            guard,
            &CoworkerTaskVerificationRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                job_id: job_id.to_string(),
                task_id: task_id.to_string(),
                attempt_no: 1,
                now_ms,
            },
        )
        .await
        .expect("verify_and_prepare_task_attempt");
    assert_eq!(
        verify_outcome.status,
        CoworkerTaskVerificationStatus::ReadyForReviewer
    );

    let review_subject = verify_outcome
        .review_subject
        .clone()
        .expect("promoted ReviewSubject");
    let tag = task_id.replace('-', "_");
    let review_receipt = accept_review_subject(&ReviewAcceptanceRequest {
        subject: review_subject.clone(),
        expected_subject_digest: review_subject.subject_digest.clone(),
        reviewer_id: "op_reviewer".to_string(),
        review_note: format!("Accept {task_id}"),
        approved_at_ms,
        receipt_id: Some(format!("rev_{tag}")),
    })
    .expect("accept review subject");

    let current_job = fixture
        .workflow_store
        .read_binding_ledger(BINDING_A)
        .expect("read binding ledger")
        .jobs
        .into_iter()
        .find(|j| j.job_id == job_id)
        .expect("find job");
    let policy_digest = current_job
        .approval_linkage
        .as_ref()
        .expect("approval linkage")
        .policy_digest
        .clone();

    let runtime = fixture
        .policy_store
        .get_job_runtime_readonly(BINDING_A, job_id)
        .expect("read runtime")
        .expect("runtime exists");
    let art = runtime
        .attempt_artifacts
        .get(&ControllerJobRuntimeState::artifact_key(task_id, 1))
        .expect("attempt artifacts");

    let (job_with_rev, rev_outbox) = fixture
        .workflow_store
        .reserve_task_outbox(&ReserveTaskOutboxRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            job_id: job_id.to_string(),
            expected_job_revision: current_job.job_revision,
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

    let (job_under_review, _) = fixture
        .workflow_store
        .reconcile_or_finalize_outbox(&FinalizeTaskOutboxRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            job_id: job_id.to_string(),
            expected_job_revision: job_with_rev.job_revision,
            expected_ledger_revision: None,
            outbox_id: rev_outbox.outbox_id,
            status: OutboxDispatchStatus::DispatchedEffectConfirmed,
            next_task_stage: CoworkerTaskStage::UnderReview,
            run_id: Some(art.run_id.clone()),
            worker_id: Some(art.worker_id.clone()),
            task_worktree_id: Some(art.task_worktree_id.clone()),
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

    let verdict = StructuredReviewVerdict {
        verdict_id: format!("vrd_{tag}"),
        task_id: task_id.to_string(),
        attempt_no: 1,
        candidate_id: verify_outcome.candidate_id.clone(),
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
        candidate_id: verify_outcome.candidate_id.clone(),
        task_id: task_id.to_string(),
        attempt_no: 1,
        base_commit: review_receipt.base_commit.clone(),
        head_commit: review_receipt.task_head_commit.clone(),
        tree_sha: review_receipt.task_tree_sha.clone(),
        diff_digest: review_receipt.diff_digest.clone(),
        write_paths: vec![rel_file.to_string()],
        check_evidence_digests: verify_outcome
            .check_evidence
            .iter()
            .map(|c| c.evidence_digest.clone())
            .collect(),
        review_subject_digest: Some(review_receipt.subject_digest.clone()),
        review_verdict: Some(verdict),
        review_receipt_id: Some(review_receipt.receipt_id.clone()),
        integration_receipt_id: None,
    };
    let (accepted_job, _) = fixture
        .workflow_store
        .record_task_candidate_and_verdict(&RecordTaskReviewVerdictRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            job_id: job_id.to_string(),
            expected_job_revision: job_under_review.job_revision,
            expected_ledger_revision: None,
            candidate,
        })
        .expect("record accepted task candidate and verdict");

    (accepted_job, verify_outcome, review_subject, review_receipt)
}

#[tokio::test]
async fn task_integration_executes_original_run_integration_seals_dependent_and_reopens_idempotently(
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
            id: "task-dep-c".to_string(),
            title: "Dependent C".to_string(),
            instruction: "CTRL_TASK_C_ONLY_A implement src/c.rs".to_string(),
            role: WorkflowRole::Coder,
            difficulty: Difficulty::Hard,
            depends_on: vec!["task-root-a".to_string()],
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
        "task-dep-c".to_string(),
        TaskInterfaceOwnershipSpec {
            owned_interfaces: vec!["iface_c".to_string()],
            consumed_interfaces: vec!["iface_a".to_string()],
        },
    );

    let (job, approval_id) = create_planned_approved_and_frozen_job(
        &fixture,
        "brf_task_int_ok",
        "plan-task-int-ok",
        tasks,
        task_interfaces,
        vec!["src/a.rs".to_string(), "src/c.rs".to_string()],
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
        .open_owned_session(BINDING_A, THREAD_UUID_A, &job.job_id, 60_000)
        .expect("open controller session");

    let deadline = Instant::now() + Duration::from_secs(10);
    let mut step_ts = 60_100u64;
    loop {
        let rep = session.step(step_ts).await.expect("controller step");
        step_ts += 100;
        let obs = rep
            .tasks
            .iter()
            .find(|t| t.task_id == "task-root-a")
            .expect("task-root-a observation");
        if obs.stage == CoworkerTaskStage::ReadyForReview {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for task-root-a ReadyForReview: {rep:?}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    let (accepted_job, verify_outcome, review_subject, review_receipt) =
        verify_and_accept_task_without_integrating(
            &fixture,
            session.guard(),
            &job.job_id,
            "task-root-a",
            "src/a.rs",
            step_ts,
            1_700_000_600_000,
        )
        .await;
    let accepted_task = accepted_job
        .tasks
        .iter()
        .find(|t| t.task_id == "task-root-a")
        .expect("accepted task-root-a");
    assert_eq!(accepted_task.stage, CoworkerTaskStage::Accepted);

    let int_service = CoworkerTaskIntegrationService::new(fixture.controller.clone());
    let int_req = CoworkerTaskIntegrationRequest {
        binding_id: BINDING_A.to_string(),
        thread_uuid: THREAD_UUID_A.to_string(),
        job_id: job.job_id.clone(),
        task_id: "task-root-a".to_string(),
        attempt_no: 1,
        now_ms: step_ts + 100,
    };
    let outcome = int_service
        .integrate_accepted_task(session.guard(), &int_req)
        .expect("integrate_accepted_task");

    assert_eq!(outcome.status, CoworkerTaskIntegrationStatus::Integrated);
    assert_eq!(outcome.task_stage, CoworkerTaskStage::Integrated);
    assert_eq!(outcome.candidate_id, verify_outcome.candidate_id);
    assert_eq!(outcome.review_receipt_id, review_receipt.receipt_id);
    assert_eq!(outcome.review_subject_digest, review_subject.subject_digest);
    assert_eq!(
        outcome.previous_integration_head_commit,
        fixture.base_commit
    );
    assert_eq!(
        outcome.integrated_target_commit,
        verify_outcome.committed_head_commit
    );
    assert_eq!(
        outcome.integrated_tree_sha,
        verify_outcome.committed_tree_sha
    );
    assert_eq!(outcome.strategy, IntegrationStrategy::FastForwardOnly);
    assert!(outcome.integration_receipt.job_aggregation.is_none());

    // Compact worker manifest run_id/task_id must match ReviewSubject/ReviewReceipt and remain
    // distinct from the host runtime UUID and logical task_id.
    let runtime = fixture
        .policy_store
        .get_job_runtime_readonly(BINDING_A, &job.job_id)
        .expect("get runtime")
        .expect("runtime exists");
    let stored_art = runtime
        .attempt_artifacts
        .get(&ControllerJobRuntimeState::artifact_key("task-root-a", 1))
        .expect("stored attempt artifacts");
    assert_ne!(outcome.integration_receipt.run_id, stored_art.run_id);
    assert_ne!(outcome.integration_receipt.task_id, "task-root-a");
    assert_eq!(outcome.integration_receipt.run_id, review_subject.run_id);
    assert_eq!(outcome.integration_receipt.task_id, review_subject.task_id);
    assert_eq!(
        stored_art.integration_receipt.as_ref(),
        Some(&outcome.integration_receipt)
    );
    assert_eq!(stored_art.review_receipt.as_ref(), Some(&review_receipt));

    let eff = runtime
        .pending_effects
        .get(&outcome.effect_id)
        .expect("PendingControllerEffect recorded");
    assert_eq!(eff.kind, PendingControllerEffectKind::TaskIntegration);
    assert_eq!(eff.status, PendingControllerEffectStatus::EffectConfirmed);
    assert_eq!(
        eff.external_correlation_id.as_deref(),
        Some(outcome.integration_receipt_id.as_str())
    );

    // Primary source checkout HEAD and file contents must remain untouched.
    let primary_head =
        resolve_exact_commit_sha(&fixture.repo_dir, "HEAD", DEFAULT_GIT_COMMAND_TIMEOUT_MS)
            .expect("resolve primary HEAD");
    assert_eq!(primary_head, fixture.base_commit);
    assert_eq!(
        fs::read_to_string(fixture.repo_dir.join("src/a.rs")).expect("read primary src/a.rs"),
        "pub fn a() -> u32 { 1 }\n"
    );

    // Owned RunIntegration worktree checkout must now be at integrated_target_commit.
    let int_head = resolve_exact_commit_sha(
        &outcome.integration_receipt.integration_checkout_path,
        "HEAD",
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .expect("resolve integration worktree HEAD");
    assert_eq!(int_head, outcome.integrated_target_commit);

    // Repeat call and crash/reopen call must be idempotent without re-integrating or adding outbox records.
    let repeat_outcome = integrate_accepted_task(
        &fixture.controller,
        session.guard(),
        &CoworkerTaskIntegrationRequest {
            now_ms: step_ts + 200,
            ..int_req.clone()
        },
    )
    .expect("repeat integrate_accepted_task");
    assert_eq!(
        repeat_outcome.status,
        CoworkerTaskIntegrationStatus::ReopenedIntegrated
    );
    assert_eq!(repeat_outcome.job_revision, outcome.job_revision);
    assert_eq!(
        repeat_outcome.integration_receipt,
        outcome.integration_receipt
    );

    drop(session);
    let reopened_controller = CoworkerControllerService::new(
        CoworkerWorkflowStore::new(&fixture.plans_dir).expect("reopen workflow_store"),
        ControllerPolicyStore::new(&fixture.plans_dir).expect("reopen policy_store"),
        CoworkerJobOwnershipStore::new(&fixture.ownership_dir).expect("reopen ownership_store"),
        Arc::clone(&fixture.authority),
        &fixture.sessions_dir,
    )
    .expect("reopen controller")
    .with_planning_artifacts_dir(&fixture.planning_artifacts_dir)
    .expect("configure planning_artifacts_dir");
    let reopened_session = reopened_controller
        .open_owned_session(BINDING_A, THREAD_UUID_A, &job.job_id, step_ts + 300)
        .expect("reopen session");
    let reopened_outcome = integrate_accepted_task(
        &reopened_controller,
        reopened_session.guard(),
        &CoworkerTaskIntegrationRequest {
            now_ms: step_ts + 400,
            ..int_req
        },
    )
    .expect("reopened integrate_accepted_task");
    assert_eq!(
        reopened_outcome.status,
        CoworkerTaskIntegrationStatus::ReopenedIntegrated
    );
    assert_eq!(
        reopened_outcome.integration_receipt,
        outcome.integration_receipt
    );

    let ledger_after = fixture
        .workflow_store
        .read_binding_ledger(BINDING_A)
        .expect("read ledger");
    let job_after = ledger_after
        .jobs
        .iter()
        .find(|j| j.job_id == job.job_id)
        .expect("job after");
    assert_eq!(
        job_after
            .outbox
            .iter()
            .filter(|o| o.task_id == "task-root-a" && o.kind == OutboxDispatchKind::TaskIntegration)
            .count(),
        1
    );

    // ApprovedHost prepare_dependency_inputs accepts the original per-task IntegrationReceipt
    // and allows the dependent task `task-dep-c` to seal, dispatch, and reach ReadyForReview.
    let approval_rec = fixture
        .authority
        .get_for_operator(&approval_id)
        .expect("get approval");
    let host_client = ApprovedHostClient::new(
        Arc::clone(&fixture.authority),
        &fixture.sessions_dir,
        BINDING_A,
        THREAD_UUID_A,
    )
    .expect("create ApprovedHostClient");
    host_client
        .prepare_dependency_inputs(PrepareDependencyInputsRequest {
            approval_id: approval_id.clone(),
            expected_approval_digest: approval_rec.subject_digest,
            task_id: "task-dep-c".to_string(),
            predecessors: vec![DependencyPredecessorSelection {
                task_id: "task-root-a".to_string(),
                review_receipt_id: outcome.review_receipt_id.clone(),
                integration_receipt_id: outcome.integration_receipt_id.clone(),
            }],
        })
        .await
        .expect("prepare_dependency_inputs with original per-task integration receipt");

    let dep_deadline = Instant::now() + Duration::from_secs(10);
    let mut dep_ts = step_ts + 500;
    loop {
        let rep = reopened_session
            .step(dep_ts)
            .await
            .expect("step dependent task-dep-c");
        dep_ts += 100;
        let dep_obs = rep
            .tasks
            .iter()
            .find(|t| t.task_id == "task-dep-c")
            .expect("task-dep-c obs");
        if dep_obs.stage == CoworkerTaskStage::ReadyForReview {
            break;
        }
        assert!(
            Instant::now() < dep_deadline,
            "timed out waiting for task-dep-c ReadyForReview: {rep:?}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    reopened_session
        .release()
        .expect("release reopened session");
    host_handle.shutdown().await.expect("shutdown host");
}

#[tokio::test]
async fn task_integration_refuses_tampered_proofs_dirty_target_and_blocks_missing_receipt_after_intent(
) {
    let fixture = setup_fixture();

    let tasks = vec![
        PlanTask {
            id: "task-int-guard-a".to_string(),
            title: "Guard A".to_string(),
            instruction: "CTRL_TASK_A implement src/a.rs".to_string(),
            role: WorkflowRole::Coder,
            difficulty: Difficulty::Hard,
            depends_on: Vec::new(),
            allowed_write_paths: vec!["src/a.rs".to_string()],
        },
        PlanTask {
            id: "task-int-guard-b".to_string(),
            title: "Guard B".to_string(),
            instruction: "CTRL_TASK_B implement src/b.rs".to_string(),
            role: WorkflowRole::Coder,
            difficulty: Difficulty::Hard,
            depends_on: Vec::new(),
            allowed_write_paths: vec!["src/b.rs".to_string()],
        },
    ];

    let mut task_interfaces = BTreeMap::new();
    task_interfaces.insert(
        "task-int-guard-a".to_string(),
        TaskInterfaceOwnershipSpec {
            owned_interfaces: vec!["iface_guard_a".to_string()],
            consumed_interfaces: Vec::new(),
        },
    );
    task_interfaces.insert(
        "task-int-guard-b".to_string(),
        TaskInterfaceOwnershipSpec {
            owned_interfaces: vec!["iface_guard_b".to_string()],
            consumed_interfaces: Vec::new(),
        },
    );

    let (job, approval_id) = create_planned_approved_and_frozen_job(
        &fixture,
        "brf_task_int_guards",
        "plan-task-int-guards",
        tasks,
        task_interfaces,
        vec!["src/a.rs".to_string(), "src/b.rs".to_string()],
        2,
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
        .open_owned_session(BINDING_A, THREAD_UUID_A, &job.job_id, 70_000)
        .expect("open controller session");

    let deadline = Instant::now() + Duration::from_secs(10);
    let mut step_ts = 70_100u64;
    loop {
        let rep = session.step(step_ts).await.expect("controller step");
        step_ts += 100;
        let obs_a = rep
            .tasks
            .iter()
            .find(|t| t.task_id == "task-int-guard-a")
            .expect("obs A");
        let obs_b = rep
            .tasks
            .iter()
            .find(|t| t.task_id == "task-int-guard-b")
            .expect("obs B");
        if obs_a.stage == CoworkerTaskStage::ReadyForReview
            && obs_b.stage == CoworkerTaskStage::ReadyForReview
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for guard tasks ReadyForReview: {rep:?}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    let (_, _, subject_a, receipt_a) = verify_and_accept_task_without_integrating(
        &fixture,
        session.guard(),
        &job.job_id,
        "task-int-guard-a",
        "src/a.rs",
        step_ts,
        1_700_000_700_000,
    )
    .await;
    let (_, _, subject_b, receipt_b) = verify_and_accept_task_without_integrating(
        &fixture,
        session.guard(),
        &job.job_id,
        "task-int-guard-b",
        "src/b.rs",
        step_ts + 50,
        1_700_000_701_000,
    )
    .await;

    let runtime_initial = fixture
        .policy_store
        .get_job_runtime_readonly(BINDING_A, &job.job_id)
        .expect("read runtime")
        .expect("runtime exists");
    let art_a = runtime_initial
        .attempt_artifacts
        .get(&ControllerJobRuntimeState::artifact_key(
            "task-int-guard-a",
            1,
        ))
        .expect("art A")
        .clone();
    let art_b = runtime_initial
        .attempt_artifacts
        .get(&ControllerJobRuntimeState::artifact_key(
            "task-int-guard-b",
            1,
        ))
        .expect("art B")
        .clone();
    let int_manifest_a = threadlane_workers::load_and_verify_manifest(
        host_handle.work_area_dir(),
        &art_a.integration_worktree_id,
    )
    .expect("load int manifest A");
    let task_manifest_a = threadlane_workers::load_and_verify_manifest(
        host_handle.work_area_dir(),
        &art_a.task_worktree_id,
    )
    .expect("load task manifest A");
    let int_manifest_b = threadlane_workers::load_and_verify_manifest(
        host_handle.work_area_dir(),
        &art_b.integration_worktree_id,
    )
    .expect("load int manifest B");
    let task_manifest_b = threadlane_workers::load_and_verify_manifest(
        host_handle.work_area_dir(),
        &art_b.task_worktree_id,
    )
    .expect("load task manifest B");

    let req_a = CoworkerTaskIntegrationRequest {
        binding_id: BINDING_A.to_string(),
        thread_uuid: THREAD_UUID_A.to_string(),
        job_id: job.job_id.clone(),
        task_id: "task-int-guard-a".to_string(),
        attempt_no: 1,
        now_ms: step_ts + 100,
    };

    // 1. Active hold on `task-int-guard-a` refuses integration pre-effect without mutating HEAD or outbox.
    let job_before_hold = fixture
        .workflow_store
        .read_binding_ledger(BINDING_A)
        .expect("read ledger")
        .jobs
        .into_iter()
        .find(|j| j.job_id == job.job_id)
        .expect("find job");
    let (held_job, hold_rec) = fixture
        .workflow_store
        .apply_workflow_hold(&ApplyWorkflowHoldRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            job_id: job.job_id.clone(),
            expected_job_revision: job_before_hold.job_revision,
            expected_ledger_revision: None,
            request_key: "req_hold_int_guard_a".to_string(),
            reason: WorkflowHoldReason::ConversationClarificationPending,
            detail: "Hold task-int-guard-a before integration".to_string(),
            directly_affected_task_ids: vec!["task-int-guard-a".to_string()],
            affected_write_paths: vec!["src/a.rs".to_string()],
            affected_interfaces: vec!["iface_guard_a".to_string()],
            holds_entire_job: false,
        })
        .expect("apply hold on A");
    let hold_err = integrate_accepted_task(&fixture.controller, session.guard(), &req_a)
        .expect_err("held task must refuse fresh integration");
    assert_eq!(hold_err.code(), NativeErrorCode::ValidationFailed.as_str());
    assert_eq!(
        resolve_exact_commit_sha(
            &int_manifest_a.checkout_path,
            "HEAD",
            DEFAULT_GIT_COMMAND_TIMEOUT_MS
        )
        .expect("int HEAD A"),
        fixture.base_commit
    );

    let _ = fixture
        .workflow_store
        .resolve_workflow_hold(&ResolveWorkflowHoldRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            job_id: job.job_id.clone(),
            expected_job_revision: held_job.job_revision,
            expected_ledger_revision: None,
            hold_id: hold_rec.hold_id,
            resolution_note: "Clarified".to_string(),
        })
        .expect("resolve hold on A");

    // 2. Missing/removed persisted ReviewReceipt on disk refuses integration pre-effect.
    let rev_a_path = host_handle
        .work_area_dir()
        .join("manifests")
        .join("receipts")
        .join(format!("review-{}.json", receipt_a.receipt_id));
    assert!(
        rev_a_path.is_file(),
        "expected persisted ReviewReceipt at {}",
        rev_a_path.display()
    );
    let rev_a_backup = rev_a_path.with_extension("json.bak");
    fs::rename(&rev_a_path, &rev_a_backup).expect("hide review receipt A");
    let missing_rev_err = integrate_accepted_task(&fixture.controller, session.guard(), &req_a)
        .expect_err("missing persisted review receipt must refuse integration");
    assert_eq!(missing_rev_err.code(), NativeErrorCode::IoError.as_str());
    assert_eq!(
        resolve_exact_commit_sha(
            &int_manifest_a.checkout_path,
            "HEAD",
            DEFAULT_GIT_COMMAND_TIMEOUT_MS
        )
        .expect("int HEAD A after missing review receipt"),
        fixture.base_commit
    );
    assert_eq!(
        fixture
            .workflow_store
            .read_binding_ledger(BINDING_A)
            .expect("read ledger")
            .jobs
            .iter()
            .find(|j| j.job_id == job.job_id)
            .expect("find job")
            .outbox
            .iter()
            .filter(|o| o.task_id == "task-int-guard-a"
                && o.kind == OutboxDispatchKind::TaskIntegration)
            .count(),
        0
    );
    fs::rename(&rev_a_backup, &rev_a_path).expect("restore review receipt A");

    // 3. Dirty RunIntegration target worktree refuses integration pre-effect.
    let dirty_path = int_manifest_a.checkout_path.join("src/dirty_untracked.txt");
    fs::write(&dirty_path, "dirty\n").expect("write dirty file in RunIntegration worktree");
    let dirty_err = integrate_accepted_task(&fixture.controller, session.guard(), &req_a)
        .expect_err("dirty RunIntegration worktree must refuse integration");
    assert_eq!(dirty_err.code(), NativeErrorCode::ValidationFailed.as_str());
    fs::remove_file(&dirty_path).expect("clean dirty file");

    // 4. Post-effect stop fence on `task-int-guard-b` retains the verified IntegrationReceipt in
    // runtime `attempt_artifacts` (and confirms the pending effect) while refusing to advance the
    // task stage to Integrated until the stop fence is cleared.
    let req_b = CoworkerTaskIntegrationRequest {
        binding_id: BINDING_A.to_string(),
        thread_uuid: THREAD_UUID_A.to_string(),
        job_id: job.job_id.clone(),
        task_id: "task-int-guard-b".to_string(),
        attempt_no: 1,
        now_ms: step_ts + 200,
    };
    let adm_b = fixture
        .controller
        .verify_job_admission(session.guard(), BINDING_A, THREAD_UUID_A, &job.job_id)
        .expect("verify admission for B");
    let cand_b = adm_b
        .job
        .tasks
        .iter()
        .find(|t| t.task_id == "task-int-guard-b")
        .expect("task B")
        .candidates
        .first()
        .expect("candidate B")
        .clone();
    let subj_digest_b = compute_task_integration_subject_digest(
        &adm_b,
        &req_b,
        &cand_b,
        &art_b,
        &task_manifest_b,
        &int_manifest_b,
        &subject_b,
        &receipt_b,
        IntegrationStrategy::FastForwardOnly,
    );
    let expected_int_id_b = deterministic_task_integration_receipt_id(
        &job.job_id,
        "task-int-guard-b",
        1,
        &int_manifest_b.worktree_id,
        &subj_digest_b,
    );
    let expected_req_key_b = deterministic_task_integration_request_key(
        &job.job_id,
        "task-int-guard-b",
        1,
        &int_manifest_b.worktree_id,
        &subj_digest_b,
    );
    let expected_eff_id_b = deterministic_prefixed_id(
        "threadlane-native:coworker-controller-effect:v1",
        "eff",
        &[&job.job_id, "task-int-guard-b", &expected_req_key_b],
    );

    let _ = fixture
        .workflow_store
        .reserve_task_outbox(&ReserveTaskOutboxRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            job_id: job.job_id.clone(),
            expected_job_revision: adm_b.job.job_revision,
            expected_ledger_revision: None,
            request_key: expected_req_key_b.clone(),
            kind: OutboxDispatchKind::TaskIntegration,
            task_id: "task-int-guard-b".to_string(),
            role: WorkflowRole::Coder,
            subject_digest: subj_digest_b.clone(),
            dependency_input_commit: None,
            dependency_input_tree_sha: None,
            accepted_predecessor_receipts: Vec::new(),
        })
        .expect("reserve outbox B");
    let _ = execute_integration(&ExecuteIntegrationRequest {
        work_area_root: host_handle.work_area_dir().to_path_buf(),
        integration_worktree_id: int_manifest_b.worktree_id.clone(),
        task_worktree_id: task_manifest_b.worktree_id.clone(),
        subject: subject_b.clone(),
        review_receipt: receipt_b.clone(),
        expected_integration_head_commit: fixture.base_commit.clone(),
        expected_task_head_commit: cand_b.head_commit.clone(),
        expected_source_head_commit: fixture.base_commit.clone(),
        strategy: IntegrationStrategy::FastForwardOnly,
        receipt_id: Some(expected_int_id_b.clone()),
        operator_id: "ctrl_test".to_string(),
        integrated_at_ms: 1_700_000_702_000,
    })
    .expect("execute integration B");
    fixture
        .policy_store
        .mutate_job_runtime_for_operator(BINDING_A, &job.job_id, |state| {
            state.stop_requested = true;
            state.pending_effects.insert(
                expected_eff_id_b.clone(),
                PendingControllerEffect {
                    effect_id: expected_eff_id_b.clone(),
                    request_key: expected_req_key_b.clone(),
                    job_id: job.job_id.clone(),
                    task_id: Some("task-int-guard-b".to_string()),
                    attempt_no: Some(1),
                    kind: PendingControllerEffectKind::TaskIntegration,
                    status: PendingControllerEffectStatus::ReservedBeforeEffect,
                    subject_digest: subj_digest_b.clone(),
                    external_correlation_id: Some(expected_int_id_b.clone()),
                    created_at_ms: req_b.now_ms,
                    updated_at_ms: req_b.now_ms,
                    diagnostic: None,
                },
            );
            Ok(())
        })
        .expect("record pending effect B and stop_requested");

    let fenced_b = integrate_accepted_task(&fixture.controller, session.guard(), &req_b)
        .expect("fenced post-effect integration retains receipt without advancing stage");
    assert_eq!(
        fenced_b.status,
        CoworkerTaskIntegrationStatus::FencedAfterEffect
    );
    assert_eq!(fenced_b.task_stage, CoworkerTaskStage::IntegrationReserved);
    let runtime_fenced_b = fixture
        .policy_store
        .get_job_runtime_readonly(BINDING_A, &job.job_id)
        .expect("read runtime")
        .expect("runtime exists");
    assert!(runtime_fenced_b
        .attempt_artifacts
        .get(&ControllerJobRuntimeState::artifact_key(
            "task-int-guard-b",
            1
        ))
        .and_then(|a| a.integration_receipt.as_ref())
        .is_some());

    fixture
        .policy_store
        .mutate_job_runtime_for_operator(BINDING_A, &job.job_id, |state| {
            state.stop_requested = false;
            Ok(())
        })
        .expect("clear stop_requested");
    let unfenced_b = integrate_accepted_task(
        &fixture.controller,
        session.guard(),
        &CoworkerTaskIntegrationRequest {
            now_ms: req_b.now_ms + 50,
            ..req_b
        },
    )
    .expect("reopen after clearing stop_requested advances B to Integrated");
    assert_eq!(
        unfenced_b.status,
        CoworkerTaskIntegrationStatus::ReopenedIntegrated
    );
    assert_eq!(unfenced_b.task_stage, CoworkerTaskStage::Integrated);

    // 5. Simulated submitted/reserved intent on `task-int-guard-a` with missing IntegrationReceipt
    // on disk transitions to EffectUnknown and refuses blind retry or new-key bypass.
    let adm_a = fixture
        .controller
        .verify_job_admission(session.guard(), BINDING_A, THREAD_UUID_A, &job.job_id)
        .expect("verify admission for A");
    let cand_a = adm_a
        .job
        .tasks
        .iter()
        .find(|t| t.task_id == "task-int-guard-a")
        .expect("task A")
        .candidates
        .first()
        .expect("candidate A")
        .clone();
    let subj_digest_a = compute_task_integration_subject_digest(
        &adm_a,
        &req_a,
        &cand_a,
        &art_a,
        &task_manifest_a,
        &int_manifest_a,
        &subject_a,
        &receipt_a,
        IntegrationStrategy::FastForwardOnly,
    );
    let expected_int_id_a = deterministic_task_integration_receipt_id(
        &job.job_id,
        "task-int-guard-a",
        1,
        &int_manifest_a.worktree_id,
        &subj_digest_a,
    );
    let expected_req_key_a = deterministic_task_integration_request_key(
        &job.job_id,
        "task-int-guard-a",
        1,
        &int_manifest_a.worktree_id,
        &subj_digest_a,
    );
    let expected_eff_id_a = deterministic_prefixed_id(
        "threadlane-native:coworker-controller-effect:v1",
        "eff",
        &[&job.job_id, "task-int-guard-a", &expected_req_key_a],
    );

    let _ = fixture
        .workflow_store
        .reserve_task_outbox(&ReserveTaskOutboxRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            job_id: job.job_id.clone(),
            expected_job_revision: adm_a.job.job_revision,
            expected_ledger_revision: None,
            request_key: expected_req_key_a.clone(),
            kind: OutboxDispatchKind::TaskIntegration,
            task_id: "task-int-guard-a".to_string(),
            role: WorkflowRole::Coder,
            subject_digest: subj_digest_a.clone(),
            dependency_input_commit: None,
            dependency_input_tree_sha: None,
            accepted_predecessor_receipts: Vec::new(),
        })
        .expect("reserve outbox A without executing integration");
    fixture
        .policy_store
        .mutate_job_runtime_for_operator(BINDING_A, &job.job_id, |state| {
            state.pending_effects.insert(
                expected_eff_id_a.clone(),
                PendingControllerEffect {
                    effect_id: expected_eff_id_a.clone(),
                    request_key: expected_req_key_a.clone(),
                    job_id: job.job_id.clone(),
                    task_id: Some("task-int-guard-a".to_string()),
                    attempt_no: Some(1),
                    kind: PendingControllerEffectKind::TaskIntegration,
                    status: PendingControllerEffectStatus::ReservedBeforeEffect,
                    subject_digest: subj_digest_a.clone(),
                    external_correlation_id: Some(expected_int_id_a.clone()),
                    created_at_ms: req_a.now_ms,
                    updated_at_ms: req_a.now_ms,
                    diagnostic: None,
                },
            );
            Ok(())
        })
        .expect("insert ReservedBeforeEffect pending effect A");

    let missing_int_err = integrate_accepted_task(&fixture.controller, session.guard(), &req_a)
        .expect_err(
            "missing IntegrationReceipt after reservation must fail closed as EffectUnknown",
        );
    assert_eq!(
        missing_int_err.code(),
        NativeErrorCode::AmbiguousWakeupPendingReconciliation.as_str()
    );
    assert!(
        load_integration_receipt(host_handle.work_area_dir(), &expected_int_id_a).is_err(),
        "must not blindly execute integration when prior reservation receipt is missing"
    );
    assert_eq!(
        resolve_exact_commit_sha(
            &int_manifest_a.checkout_path,
            "HEAD",
            DEFAULT_GIT_COMMAND_TIMEOUT_MS
        )
        .expect("int HEAD A after EffectUnknown"),
        fixture.base_commit
    );

    let ledger_unknown = fixture
        .workflow_store
        .read_binding_ledger(BINDING_A)
        .expect("read ledger");
    let job_unknown = ledger_unknown
        .jobs
        .iter()
        .find(|j| j.job_id == job.job_id)
        .expect("job");
    let task_a_unknown = job_unknown
        .tasks
        .iter()
        .find(|t| t.task_id == "task-int-guard-a")
        .expect("task A");
    assert_eq!(task_a_unknown.stage, CoworkerTaskStage::EffectUnknown);
    let runtime_unknown = fixture
        .policy_store
        .get_job_runtime_readonly(BINDING_A, &job.job_id)
        .expect("read runtime")
        .expect("runtime exists");
    assert_eq!(
        runtime_unknown
            .pending_effects
            .get(&expected_eff_id_a)
            .expect("effect A")
            .status,
        PendingControllerEffectStatus::EffectUnknown
    );

    let retry_unknown_err = integrate_accepted_task(&fixture.controller, session.guard(), &req_a)
        .expect_err("repeat call while EffectUnknown with missing receipt must remain blocked");
    assert_eq!(
        retry_unknown_err.code(),
        NativeErrorCode::AmbiguousWakeupPendingReconciliation.as_str()
    );

    session.release().expect("release session");
    host_handle.shutdown().await.expect("shutdown host");
}

#[tokio::test]
async fn task_review_stage_executes_reviewer_mints_bound_review_receipt_and_reconciles_idempotently(
) {
    let fixture = setup_fixture();
    write_executable_script(
        &fixture.fake_cargo_exe,
        "#!/bin/sh\nset -eu\nprintf \"running 1 test\\ntest src_a ... ok\\n\"\nexit 0\n",
    );

    let mut task_interfaces = BTreeMap::new();
    task_interfaces.insert(
        "task-review-accept".to_string(),
        TaskInterfaceOwnershipSpec {
            owned_interfaces: vec!["iface_review_accept".to_string()],
            consumed_interfaces: Vec::new(),
        },
    );
    let (job, approval_id) = create_planned_approved_and_frozen_job(
        &fixture,
        "brf_review_accept",
        "plan-review-accept",
        vec![PlanTask {
            id: "task-review-accept".to_string(),
            title: "Review Accept Task A".to_string(),
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
    assert_eq!(read_invocation_count(&fixture.agy_planning_count_file), 1);

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
        .open_owned_session(BINDING_A, THREAD_UUID_A, &job.job_id, 60_000)
        .expect("open controller session");

    let deadline = Instant::now() + Duration::from_secs(10);
    let mut step_ts = 60_100u64;
    loop {
        let rep = session.step(step_ts).await.expect("controller step");
        step_ts += 100;
        let obs = rep
            .tasks
            .iter()
            .find(|t| t.task_id == "task-review-accept")
            .expect("task-review-accept observation");
        if obs.stage == CoworkerTaskStage::ReadyForReview {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for task-review-accept ReadyForReview: {rep:?}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert_eq!(read_invocation_count(&fixture.agy_count_file), 1);

    let verifier = CoworkerTaskVerificationService::new(fixture.controller.clone());
    let verify_outcome = verifier
        .verify_and_prepare_task_attempt(
            session.guard(),
            &CoworkerTaskVerificationRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                job_id: job.job_id.clone(),
                task_id: "task-review-accept".to_string(),
                attempt_no: 1,
                now_ms: step_ts,
            },
        )
        .await
        .expect("verify_and_prepare_task_attempt");
    assert_eq!(
        verify_outcome.status,
        CoworkerTaskVerificationStatus::ReadyForReviewer
    );
    let review_subject = verify_outcome
        .review_subject
        .as_ref()
        .expect("promoted ReviewSubject");

    let policy_digest = job
        .approval_linkage
        .as_ref()
        .expect("approval linkage")
        .policy_digest
        .clone();
    let accept_model_resp = StructuredTaskReviewModelResponse {
        schema_version: TASK_REVIEW_RESPONSE_SCHEMA_VERSION,
        task_id: "task-review-accept".to_string(),
        attempt_no: 1,
        candidate_id: verify_outcome.candidate_id.clone(),
        review_subject_digest: review_subject.subject_digest.clone(),
        base_commit: review_subject.base_commit.clone(),
        task_head_commit: review_subject.task_head_commit.clone(),
        task_tree_sha: review_subject.task_tree_sha.clone(),
        diff_digest: review_subject.diff_digest.clone(),
        policy_digest,
        outcome: ReviewVerdictOutcome::Accept,
        findings: Vec::new(),
        required_in_scope_fixes: Vec::new(),
        conflicting_paths_or_interfaces: Vec::new(),
        summary: Some(
            "Candidate satisfies approved write scope, frozen policy, and passing check evidence."
                .to_string(),
        ),
    };
    let accept_json = serde_json::to_string(&accept_model_resp)
        .expect("serialize StructuredTaskReviewModelResponse");
    fs::write(&fixture.agy_plan_response_file, &accept_json).expect("write reviewer accept json");

    let review_service = CoworkerTaskReviewStageService::new(fixture.controller.clone());
    let review_outcome = review_service
        .review_task_attempt(
            session.guard(),
            &CoworkerTaskReviewStageRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                job_id: job.job_id.clone(),
                task_id: "task-review-accept".to_string(),
                attempt_no: 1,
                now_ms: step_ts + 100,
                untrusted_candidate_context: Some(
                    "Implemented pub fn a() -> u32 { 100 }".to_string(),
                ),
            },
        )
        .await
        .expect("execute_task_review_stage Accept");

    assert_eq!(
        review_outcome.status,
        CoworkerTaskReviewStageStatus::Accepted
    );
    assert_eq!(review_outcome.task_stage, CoworkerTaskStage::Accepted);
    assert!(!review_outcome.reused_existing);
    assert_eq!(read_invocation_count(&fixture.agy_count_file), 1);
    assert_eq!(read_invocation_count(&fixture.agy_planning_count_file), 2);
    assert_eq!(
        review_outcome.report.status,
        TaskReviewExecutionStatus::VerifiedVerdict
    );
    assert_eq!(
        review_outcome.report.observed_model.as_deref(),
        Some("gemini-4-argon-high")
    );
    assert_eq!(
        review_outcome.report.observed_effort.as_deref(),
        Some("high")
    );

    let persisted_report =
        read_persisted_task_review_report(&fixture.plans_dir, &review_outcome.effect_id)
            .expect("read persisted task review report")
            .expect("persisted task review report exists");
    assert_eq!(persisted_report, review_outcome.report);

    let receipt = review_outcome
        .require_accepted()
        .expect("require accepted ReviewReceipt");
    let loaded_receipt = load_review_receipt(&review_subject.work_area_root, &receipt.receipt_id)
        .expect("load persisted ReviewReceipt");
    assert_eq!(&loaded_receipt, receipt);
    verify_review_receipt_readonly(review_subject, &loaded_receipt)
        .expect("verify ReviewReceipt against live ReviewSubject");

    let runtime = fixture
        .policy_store
        .get_job_runtime_readonly(BINDING_A, &job.job_id)
        .expect("get job runtime")
        .expect("runtime exists");
    let coder_usage = runtime
        .role_usage
        .get(&WorkflowRole::Coder)
        .expect("Coder usage");
    assert_eq!(coder_usage.calls_reserved, 1);
    assert_eq!(coder_usage.calls_completed, 1);
    let reviewer_usage = runtime
        .role_usage
        .get(&WorkflowRole::Reviewer)
        .expect("Reviewer usage");
    assert_eq!(reviewer_usage.calls_reserved, 1);
    assert_eq!(reviewer_usage.calls_completed, 1);

    let art_key = ControllerJobRuntimeState::artifact_key("task-review-accept", 1);
    let stored_art = runtime
        .attempt_artifacts
        .get(&art_key)
        .expect("stored attempt artifacts");
    assert_eq!(stored_art.review_receipt.as_ref(), Some(receipt));
    assert_eq!(
        stored_art.reviewer_response_digest.as_deref(),
        Some(review_outcome.reviewer_response_digest.as_str())
    );
    assert!(stored_art.integration_receipt.is_none());

    let primary_head =
        resolve_exact_commit_sha(&fixture.repo_dir, "HEAD", DEFAULT_GIT_COMMAND_TIMEOUT_MS)
            .expect("resolve primary HEAD");
    assert_eq!(primary_head, fixture.base_commit);

    // Drop and reopen controller session: repeating review_task_attempt must verify the persisted
    // report and ReviewReceipt and return idempotently without a second Reviewer invocation.
    session.release().expect("release session");
    let reopened_session = fixture
        .controller
        .open_owned_session(BINDING_A, THREAD_UUID_A, &job.job_id, step_ts + 200)
        .expect("reopen session");
    let reopened_outcome = review_service
        .review_task_attempt(
            reopened_session.guard(),
            &CoworkerTaskReviewStageRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                job_id: job.job_id.clone(),
                task_id: "task-review-accept".to_string(),
                attempt_no: 1,
                now_ms: step_ts + 300,
                untrusted_candidate_context: None,
            },
        )
        .await
        .expect("reopened review_task_attempt");
    assert!(reopened_outcome.reused_existing);
    assert_eq!(
        reopened_outcome.review_receipt,
        review_outcome.review_receipt
    );
    assert_eq!(reopened_outcome.verdict, review_outcome.verdict);
    assert_eq!(reopened_outcome.report, review_outcome.report);
    assert_eq!(read_invocation_count(&fixture.agy_planning_count_file), 2);

    let runtime_after = fixture
        .policy_store
        .get_job_runtime_readonly(BINDING_A, &job.job_id)
        .expect("get job runtime after reopen")
        .expect("runtime exists");
    let reviewer_usage_after = runtime_after
        .role_usage
        .get(&WorkflowRole::Reviewer)
        .expect("Reviewer usage after reopen");
    assert_eq!(reviewer_usage_after.calls_reserved, 1);
    assert_eq!(reviewer_usage_after.calls_completed, 1);

    reopened_session
        .release()
        .expect("release reopened session");
    host_handle.shutdown().await.expect("shutdown host");
}

#[tokio::test]
async fn task_review_stage_rejects_malformed_or_positive_prose_output_and_finalizes_to_blocked() {
    let fixture = setup_fixture();
    write_executable_script(
        &fixture.fake_cargo_exe,
        "#!/bin/sh\nset -eu\nprintf \"running 1 test\\ntest ok\\n\"\nexit 0\n",
    );

    let mut task_interfaces = BTreeMap::new();
    task_interfaces.insert(
        "task-review-bad".to_string(),
        TaskInterfaceOwnershipSpec {
            owned_interfaces: vec!["iface_review_bad".to_string()],
            consumed_interfaces: Vec::new(),
        },
    );
    let (job, approval_id) = create_planned_approved_and_frozen_job(
        &fixture,
        "brf_review_neg",
        "plan-review-neg",
        vec![PlanTask {
            id: "task-review-bad".to_string(),
            title: "Review Bad Output Task".to_string(),
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
    assert_eq!(read_invocation_count(&fixture.agy_planning_count_file), 1);

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
        .open_owned_session(BINDING_A, THREAD_UUID_A, &job.job_id, 70_000)
        .expect("open controller session");

    let deadline = Instant::now() + Duration::from_secs(10);
    let mut step_ts = 70_100u64;
    loop {
        let rep = session.step(step_ts).await.expect("controller step");
        step_ts += 100;
        let obs = rep
            .tasks
            .iter()
            .find(|t| t.task_id == "task-review-bad")
            .expect("task-review-bad observation");
        if obs.stage == CoworkerTaskStage::ReadyForReview {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for task-review-bad ReadyForReview: {rep:?}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    let review_service = CoworkerTaskReviewStageService::new(fixture.controller.clone());

    // 1. Pre-effect refusal: invoking Reviewer before task_verification commits and passes checks
    // must fail closed before reserving any Reviewer call budget or outbox record.
    let pre_verify_err = review_service
        .review_task_attempt(
            session.guard(),
            &CoworkerTaskReviewStageRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                job_id: job.job_id.clone(),
                task_id: "task-review-bad".to_string(),
                attempt_no: 1,
                now_ms: step_ts,
                untrusted_candidate_context: None,
            },
        )
        .await
        .expect_err("review before task verification must fail closed");
    assert_eq!(
        pre_verify_err.code(),
        NativeErrorCode::ValidationFailed.as_str()
    );
    assert_eq!(read_invocation_count(&fixture.agy_planning_count_file), 1);

    let verifier = CoworkerTaskVerificationService::new(fixture.controller.clone());
    let verify_bad = verifier
        .verify_and_prepare_task_attempt(
            session.guard(),
            &CoworkerTaskVerificationRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                job_id: job.job_id.clone(),
                task_id: "task-review-bad".to_string(),
                attempt_no: 1,
                now_ms: step_ts + 50,
            },
        )
        .await
        .expect("verify task-review-bad");
    let subject_bad = verify_bad.review_subject.as_ref().expect("subject_bad");

    let policy_digest = job
        .approval_linkage
        .as_ref()
        .expect("approval linkage")
        .policy_digest
        .clone();

    // 2. Real invalid natural output: Reviewer emits positive prose claiming LGTM/approval while
    // smuggling an invalid/contradictory schema field (`prose_may_advance_acceptance: true`).
    let invalid_prose_payload = serde_json::json!({
        "schema_version": 1,
        "task_id": "task-review-bad",
        "attempt_no": 1,
        "candidate_id": verify_bad.candidate_id,
        "review_subject_digest": subject_bad.subject_digest,
        "base_commit": subject_bad.base_commit,
        "task_head_commit": subject_bad.task_head_commit,
        "task_tree_sha": subject_bad.task_tree_sha,
        "diff_digest": subject_bad.diff_digest,
        "policy_digest": policy_digest,
        "outcome": "accept",
        "summary": "LGTM! Approved and ready to integrate immediately!",
        "prose_may_advance_acceptance": true
    });
    fs::write(
        &fixture.agy_plan_response_file,
        serde_json::to_string(&invalid_prose_payload).expect("serialize invalid payload"),
    )
    .expect("write invalid reviewer response");

    let bad_err = review_service
        .review_task_attempt(
            session.guard(),
            &CoworkerTaskReviewStageRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                job_id: job.job_id.clone(),
                task_id: "task-review-bad".to_string(),
                attempt_no: 1,
                now_ms: step_ts + 100,
                untrusted_candidate_context: None,
            },
        )
        .await
        .expect_err("invalid Reviewer output must be rejected");
    assert_eq!(bad_err.code(), NativeErrorCode::ValidationFailed.as_str());
    assert_eq!(read_invocation_count(&fixture.agy_planning_count_file), 2);

    let runtime_bad = fixture
        .policy_store
        .get_job_runtime_readonly(BINDING_A, &job.job_id)
        .expect("get job runtime")
        .expect("runtime exists");
    let req_key_bad = compute_task_review_request_key(
        &job.job_id,
        "task-review-bad",
        1,
        &verify_bad.candidate_id,
        &subject_bad.subject_digest,
        &policy_digest,
        &runtime_bad.envelope.envelope_digest,
        &runtime_bad.envelope.reviewer_choice,
    );
    let eff_id_bad = compute_task_review_effect_id(&job.job_id, "task-review-bad", 1, &req_key_bad);

    let retained_report = read_persisted_task_review_report(&fixture.plans_dir, &eff_id_bad)
        .expect("read retained report")
        .expect("retained report exists on disk");
    assert_eq!(
        retained_report.status,
        TaskReviewExecutionStatus::KnownTerminalFailure
    );
    assert!(retained_report.verified_verdict.is_none());

    let eff_rec = runtime_bad
        .pending_effects
        .get(&eff_id_bad)
        .expect("Reviewer PendingControllerEffect");
    assert_eq!(
        eff_rec.status,
        PendingControllerEffectStatus::EffectFailedTerminal
    );
    let reviewer_usage = runtime_bad
        .role_usage
        .get(&WorkflowRole::Reviewer)
        .expect("Reviewer role usage");
    assert_eq!(reviewer_usage.calls_reserved, 1);
    assert_eq!(reviewer_usage.calls_completed, 1);

    let art_key_bad = ControllerJobRuntimeState::artifact_key("task-review-bad", 1);
    assert!(
        runtime_bad
            .attempt_artifacts
            .get(&art_key_bad)
            .expect("artifacts bad")
            .review_receipt
            .is_none(),
        "no ReviewReceipt may be minted on invalid Reviewer output"
    );

    let ledger_after_bad = fixture
        .workflow_store
        .read_binding_ledger(BINDING_A)
        .expect("read ledger after bad review");
    let job_after_bad = ledger_after_bad
        .jobs
        .iter()
        .find(|j| j.job_id == job.job_id)
        .expect("job after bad review");
    let task_bad = job_after_bad
        .tasks
        .iter()
        .find(|t| t.task_id == "task-review-bad")
        .expect("task-review-bad record");
    assert_eq!(
        task_bad.stage,
        CoworkerTaskStage::Blocked,
        "KnownTerminalFailure must finalize TaskReviewDispatch outbox to terminal Blocked stage"
    );
    assert!(
        task_bad.unresolved_outbox_id.is_none(),
        "KnownTerminalFailure must not leave ReviewReserved unresolved"
    );
    assert!(task_bad.accepted_candidate_id.is_none());
    assert!(task_bad.candidates.is_empty());
    let outbox_bad = job_after_bad
        .outbox
        .iter()
        .find(|o| {
            o.task_id == "task-review-bad" && o.kind == OutboxDispatchKind::TaskReviewDispatch
        })
        .expect("TaskReviewDispatch outbox");
    assert_eq!(
        outbox_bad.status,
        OutboxDispatchStatus::DispatchedEffectConfirmed
    );
    assert!(outbox_bad.run_id.is_none());
    assert!(outbox_bad.worker_id.is_none());

    // Repeating review_task_attempt on the failed task must not auto-retry or consume another call.
    let repeat_bad_err = review_service
        .review_task_attempt(
            session.guard(),
            &CoworkerTaskReviewStageRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                job_id: job.job_id.clone(),
                task_id: "task-review-bad".to_string(),
                attempt_no: 1,
                now_ms: step_ts + 200,
                untrusted_candidate_context: None,
            },
        )
        .await
        .expect_err("repeat call after terminal failure must not auto-retry");
    assert_eq!(
        repeat_bad_err.code(),
        NativeErrorCode::ValidationFailed.as_str()
    );
    assert_eq!(read_invocation_count(&fixture.agy_planning_count_file), 2);

    session.release().expect("release session");
    host_handle.shutdown().await.expect("shutdown host");
}

#[tokio::test]
async fn task_review_stage_fails_closed_on_corrupt_existing_review_receipt_without_overwriting() {
    let fixture = setup_fixture();
    write_executable_script(
        &fixture.fake_cargo_exe,
        "#!/bin/sh\nset -eu\nprintf \"running 1 test\\ntest ok\\n\"\nexit 0\n",
    );

    let mut task_interfaces = BTreeMap::new();
    task_interfaces.insert(
        "task-review-corrupt".to_string(),
        TaskInterfaceOwnershipSpec {
            owned_interfaces: vec!["iface_review_corrupt".to_string()],
            consumed_interfaces: Vec::new(),
        },
    );
    let (job, approval_id) = create_planned_approved_and_frozen_job(
        &fixture,
        "brf_review_corrupt",
        "plan-review-corrupt",
        vec![PlanTask {
            id: "task-review-corrupt".to_string(),
            title: "Review Corrupt Receipt Task".to_string(),
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
    assert_eq!(read_invocation_count(&fixture.agy_planning_count_file), 1);

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
        .open_owned_session(BINDING_A, THREAD_UUID_A, &job.job_id, 80_000)
        .expect("open controller session");

    let deadline = Instant::now() + Duration::from_secs(10);
    let mut step_ts = 80_100u64;
    loop {
        let rep = session.step(step_ts).await.expect("controller step");
        step_ts += 100;
        let obs = rep
            .tasks
            .iter()
            .find(|t| t.task_id == "task-review-corrupt")
            .expect("task-review-corrupt observation");
        if obs.stage == CoworkerTaskStage::ReadyForReview {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for task-review-corrupt ReadyForReview: {rep:?}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    let verifier = CoworkerTaskVerificationService::new(fixture.controller.clone());
    let verify_corrupt = verifier
        .verify_and_prepare_task_attempt(
            session.guard(),
            &CoworkerTaskVerificationRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                job_id: job.job_id.clone(),
                task_id: "task-review-corrupt".to_string(),
                attempt_no: 1,
                now_ms: step_ts + 50,
            },
        )
        .await
        .expect("verify task-review-corrupt");
    let subject_corrupt = verify_corrupt
        .review_subject
        .as_ref()
        .expect("subject_corrupt");

    let policy_digest = job
        .approval_linkage
        .as_ref()
        .expect("approval linkage")
        .policy_digest
        .clone();

    let valid_corrupt_resp = StructuredTaskReviewModelResponse {
        schema_version: TASK_REVIEW_RESPONSE_SCHEMA_VERSION,
        task_id: "task-review-corrupt".to_string(),
        attempt_no: 1,
        candidate_id: verify_corrupt.candidate_id.clone(),
        review_subject_digest: subject_corrupt.subject_digest.clone(),
        base_commit: subject_corrupt.base_commit.clone(),
        task_head_commit: subject_corrupt.task_head_commit.clone(),
        task_tree_sha: subject_corrupt.task_tree_sha.clone(),
        diff_digest: subject_corrupt.diff_digest.clone(),
        policy_digest: policy_digest.clone(),
        outcome: ReviewVerdictOutcome::Accept,
        findings: Vec::new(),
        required_in_scope_fixes: Vec::new(),
        conflicting_paths_or_interfaces: Vec::new(),
        summary: Some("Valid accept response for corrupt receipt check.".to_string()),
    };
    let expected_identity_corrupt = ExpectedTaskReviewIdentity::from_subject(
        subject_corrupt,
        "task-review-corrupt",
        1,
        &verify_corrupt.candidate_id,
        &policy_digest,
    );
    let expected_verdict = valid_corrupt_resp
        .clone()
        .into_verified_verdict(&expected_identity_corrupt)
        .expect("derive expected verdict");
    let expected_receipt_id = deterministic_prefixed_id(
        "threadlane-native:coworker-review-receipt:v1",
        "rev",
        &[
            &job.job_id,
            "task-review-corrupt",
            "1",
            &verify_corrupt.candidate_id,
            &expected_verdict.verdict_id,
            &subject_corrupt.subject_digest,
        ],
    );
    let receipts_dir = subject_corrupt
        .work_area_root
        .join(MANIFESTS_SUBDIR)
        .join(RECEIPTS_SUBDIR);
    fs::create_dir_all(&receipts_dir).expect("create receipts_dir");
    fs::set_permissions(&receipts_dir, fs::Permissions::from_mode(0o700))
        .expect("chmod 0700 receipts_dir");
    let corrupt_receipt_path = receipts_dir.join(format!("review-{expected_receipt_id}.json"));
    let corrupt_bytes = b"{corrupt-existing-review-receipt-json";
    fs::write(&corrupt_receipt_path, corrupt_bytes).expect("write corrupt receipt");
    fs::set_permissions(&corrupt_receipt_path, fs::Permissions::from_mode(0o600))
        .expect("chmod 0600 corrupt receipt");

    fs::write(
        &fixture.agy_plan_response_file,
        serde_json::to_string(&valid_corrupt_resp).expect("serialize valid_corrupt_resp"),
    )
    .expect("write valid_corrupt_resp");

    let review_service = CoworkerTaskReviewStageService::new(fixture.controller.clone());
    let corrupt_err = review_service
        .review_task_attempt(
            session.guard(),
            &CoworkerTaskReviewStageRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                job_id: job.job_id.clone(),
                task_id: "task-review-corrupt".to_string(),
                attempt_no: 1,
                now_ms: step_ts + 100,
                untrusted_candidate_context: None,
            },
        )
        .await
        .expect_err("corrupt existing ReviewReceipt must fail closed");
    assert_eq!(corrupt_err.code(), NativeErrorCode::ProtocolError.as_str());
    assert_eq!(read_invocation_count(&fixture.agy_planning_count_file), 2);
    assert_eq!(
        fs::read(&corrupt_receipt_path).expect("read corrupt receipt after failure"),
        corrupt_bytes,
        "corrupt ReviewReceipt file must never be overwritten"
    );

    let runtime_corrupt = fixture
        .policy_store
        .get_job_runtime_readonly(BINDING_A, &job.job_id)
        .expect("get job runtime")
        .expect("runtime exists");
    let req_key_corrupt = compute_task_review_request_key(
        &job.job_id,
        "task-review-corrupt",
        1,
        &verify_corrupt.candidate_id,
        &subject_corrupt.subject_digest,
        &policy_digest,
        &runtime_corrupt.envelope.envelope_digest,
        &runtime_corrupt.envelope.reviewer_choice,
    );
    let eff_id_corrupt =
        compute_task_review_effect_id(&job.job_id, "task-review-corrupt", 1, &req_key_corrupt);
    let persisted_report = read_persisted_task_review_report(&fixture.plans_dir, &eff_id_corrupt)
        .expect("read persisted report for corrupt receipt case")
        .expect("persisted report exists");
    assert_eq!(
        persisted_report.status,
        TaskReviewExecutionStatus::VerifiedVerdict
    );

    let reviewer_usage = runtime_corrupt
        .role_usage
        .get(&WorkflowRole::Reviewer)
        .expect("Reviewer role usage");
    assert_eq!(reviewer_usage.calls_reserved, 1);
    assert_eq!(reviewer_usage.calls_completed, 1);

    let art_key_corrupt = ControllerJobRuntimeState::artifact_key("task-review-corrupt", 1);
    assert!(
        runtime_corrupt
            .attempt_artifacts
            .get(&art_key_corrupt)
            .expect("artifacts corrupt")
            .review_receipt
            .is_none(),
        "corrupt receipt must not be promoted into TaskAttemptExecutionArtifacts"
    );

    let ledger_corrupt = fixture
        .workflow_store
        .read_binding_ledger(BINDING_A)
        .expect("read ledger after corrupt receipt failure");
    let job_corrupt = ledger_corrupt
        .jobs
        .iter()
        .find(|j| j.job_id == job.job_id)
        .expect("job after corrupt receipt failure");
    let task_corrupt = job_corrupt
        .tasks
        .iter()
        .find(|t| t.task_id == "task-review-corrupt")
        .expect("task-review-corrupt record");
    assert!(task_corrupt.accepted_candidate_id.is_none());
    assert!(task_corrupt.candidates.is_empty());

    // Repeating review_task_attempt must reuse the persisted report without invoking Reviewer again
    // and still fail closed on the corrupt receipt.
    let repeat_corrupt_err = review_service
        .review_task_attempt(
            session.guard(),
            &CoworkerTaskReviewStageRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                job_id: job.job_id.clone(),
                task_id: "task-review-corrupt".to_string(),
                attempt_no: 1,
                now_ms: step_ts + 200,
                untrusted_candidate_context: None,
            },
        )
        .await
        .expect_err("repeat call with corrupt receipt must fail closed");
    assert_eq!(
        repeat_corrupt_err.code(),
        NativeErrorCode::ProtocolError.as_str()
    );
    assert_eq!(read_invocation_count(&fixture.agy_planning_count_file), 2);
    assert_eq!(
        fs::read(&corrupt_receipt_path).expect("read corrupt receipt after repeat"),
        corrupt_bytes
    );

    session.release().expect("release session");
    host_handle.shutdown().await.expect("shutdown host");
}

#[tokio::test]
async fn task_correction_prepares_derived_receipt_reserves_once_reopens_idempotently_and_survives_step(
) {
    let fixture = setup_fixture();
    write_executable_script(
        &fixture.fake_cargo_exe,
        "#!/bin/sh\nset -eu\nprintf \"running 1 test\\ntest src_a ... ok\\n\"\nexit 0\n",
    );

    let mut task_interfaces = BTreeMap::new();
    task_interfaces.insert(
        "task-corr-prep".to_string(),
        TaskInterfaceOwnershipSpec {
            owned_interfaces: vec!["iface_corr_prep".to_string()],
            consumed_interfaces: Vec::new(),
        },
    );
    let (job, approval_id) = create_planned_approved_and_frozen_job(
        &fixture,
        "brf_corr_prep",
        "plan-corr-prep",
        vec![PlanTask {
            id: "task-corr-prep".to_string(),
            title: "Correction Prep Task A".to_string(),
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
    assert_eq!(read_invocation_count(&fixture.agy_planning_count_file), 1);

    let approval_before = fixture
        .authority
        .get_for_operator(&approval_id)
        .expect("load original ApprovalRecord");
    let approved_task_before = approval_before
        .tasks
        .iter()
        .find(|t| t.task_id == "task-corr-prep")
        .expect("approved task record")
        .clone();

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
        .open_owned_session(BINDING_A, THREAD_UUID_A, &job.job_id, 90_000)
        .expect("open controller session");

    let deadline = Instant::now() + Duration::from_secs(10);
    let mut step_ts = 90_100u64;
    loop {
        let rep = session.step(step_ts).await.expect("controller step");
        step_ts += 100;
        let obs = rep
            .tasks
            .iter()
            .find(|t| t.task_id == "task-corr-prep")
            .expect("task-corr-prep observation");
        if obs.stage == CoworkerTaskStage::ReadyForReview {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for task-corr-prep ReadyForReview: {rep:?}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert_eq!(read_invocation_count(&fixture.agy_count_file), 1);

    let verifier = CoworkerTaskVerificationService::new(fixture.controller.clone());
    let verify_outcome = verifier
        .verify_and_prepare_task_attempt(
            session.guard(),
            &CoworkerTaskVerificationRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                job_id: job.job_id.clone(),
                task_id: "task-corr-prep".to_string(),
                attempt_no: 1,
                now_ms: step_ts,
            },
        )
        .await
        .expect("verify_and_prepare_task_attempt");
    assert_eq!(
        verify_outcome.status,
        CoworkerTaskVerificationStatus::ReadyForReviewer
    );
    let review_subject = verify_outcome
        .review_subject
        .as_ref()
        .expect("promoted ReviewSubject");

    let policy_digest = job
        .approval_linkage
        .as_ref()
        .expect("approval linkage")
        .policy_digest
        .clone();
    let required_fixes = vec![
        "Handle overflow in src/a.rs::a() without changing function signature".to_string(),
        "Add boundary check in src/a.rs for zero input".to_string(),
    ];
    let correct_in_scope_resp = StructuredTaskReviewModelResponse {
        schema_version: TASK_REVIEW_RESPONSE_SCHEMA_VERSION,
        task_id: "task-corr-prep".to_string(),
        attempt_no: 1,
        candidate_id: verify_outcome.candidate_id.clone(),
        review_subject_digest: review_subject.subject_digest.clone(),
        base_commit: review_subject.base_commit.clone(),
        task_head_commit: review_subject.task_head_commit.clone(),
        task_tree_sha: review_subject.task_tree_sha.clone(),
        diff_digest: review_subject.diff_digest.clone(),
        policy_digest,
        outcome: ReviewVerdictOutcome::CorrectInScope,
        findings: vec!["Missing overflow guard in src/a.rs".to_string()],
        required_in_scope_fixes: required_fixes.clone(),
        conflicting_paths_or_interfaces: Vec::new(),
        summary: Some("Requires two bounded in-scope fixes in src/a.rs.".to_string()),
    };
    fs::write(
        &fixture.agy_plan_response_file,
        serde_json::to_string(&correct_in_scope_resp)
            .expect("serialize CorrectInScope reviewer response"),
    )
    .expect("write CorrectInScope reviewer response");

    let review_service = CoworkerTaskReviewStageService::new(fixture.controller.clone());
    let review_outcome = review_service
        .review_task_attempt(
            session.guard(),
            &CoworkerTaskReviewStageRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                job_id: job.job_id.clone(),
                task_id: "task-corr-prep".to_string(),
                attempt_no: 1,
                now_ms: step_ts + 100,
                untrusted_candidate_context: Some(
                    "Implemented initial src/a.rs candidate".to_string(),
                ),
            },
        )
        .await
        .expect("review_task_attempt CorrectInScope");
    assert_eq!(
        review_outcome.status,
        CoworkerTaskReviewStageStatus::CorrectInScopeRequired
    );
    assert_eq!(
        review_outcome.task_stage,
        CoworkerTaskStage::CorrectInScopeRequired
    );
    assert!(review_outcome.review_receipt.is_none());
    assert_eq!(read_invocation_count(&fixture.agy_count_file), 1);
    assert_eq!(read_invocation_count(&fixture.agy_planning_count_file), 2);

    let runtime_before_corr = fixture
        .policy_store
        .get_job_runtime_readonly(BINDING_A, &job.job_id)
        .expect("get job runtime before correction")
        .expect("runtime exists");
    let art_key_1 = ControllerJobRuntimeState::artifact_key("task-corr-prep", 1);
    let art_1_before = runtime_before_corr
        .attempt_artifacts
        .get(&art_key_1)
        .expect("attempt 1 artifacts")
        .clone();
    let task_manifest = threadlane_workers::load_and_verify_manifest(
        &review_subject.work_area_root,
        &art_1_before.task_worktree_id,
    )
    .expect("load task manifest");
    assert_ne!(
        art_1_before.run_id, task_manifest.run_id,
        "original host run_id UUID is distinct from compact worktree manifest run_id tag"
    );
    assert_eq!(review_subject.run_id, task_manifest.run_id);

    let correction_service = CoworkerTaskCorrectionService::new(fixture.controller.clone());
    let prep_req = CoworkerTaskCorrectionPrepareRequest {
        binding_id: BINDING_A.to_string(),
        thread_uuid: THREAD_UUID_A.to_string(),
        job_id: job.job_id.clone(),
        task_id: "task-corr-prep".to_string(),
        prior_attempt_no: 1,
        now_ms: step_ts + 200,
    };
    let prep_outcome = correction_service
        .prepare_task_correction(session.guard(), &prep_req)
        .await
        .expect("prepare_task_correction");

    assert_eq!(
        prep_outcome.status,
        CoworkerTaskCorrectionPrepareStatus::Prepared
    );
    assert!(!prep_outcome.reused_existing);
    assert_eq!(prep_outcome.prior_attempt_no, 1);
    assert_eq!(prep_outcome.next_attempt_no, 2);
    assert_eq!(prep_outcome.task_stage, CoworkerTaskStage::DispatchReserved);
    assert_eq!(prep_outcome.prior_candidate_id, verify_outcome.candidate_id);
    assert_eq!(
        prep_outcome.prior_review_verdict_id.as_deref(),
        Some(review_outcome.verdict.verdict_id.as_str())
    );
    assert_eq!(
        prep_outcome.prior_review_subject_digest,
        review_subject.subject_digest
    );
    assert_eq!(
        read_invocation_count(&fixture.agy_count_file),
        1,
        "preparation must only reserve attempt 2 without launching a second Coder process"
    );

    let receipt = prep_outcome
        .require_receipt()
        .expect("require DerivedTaskAttemptReceipt")
        .clone();
    let expected_receipt_id = compute_derived_task_attempt_receipt_id(
        &job.job_id,
        "task-corr-prep",
        2,
        &verify_outcome.candidate_id,
        &review_outcome.verdict.verdict_id,
        &review_subject.subject_digest,
        &runtime_before_corr.envelope.envelope_digest,
    );
    let expected_req_key = compute_derived_coder_request_key(
        &job.job_id,
        "task-corr-prep",
        2,
        &verify_outcome.candidate_id,
        &review_outcome.verdict.verdict_id,
        &review_subject.subject_digest,
        &runtime_before_corr.envelope.envelope_digest,
        &headless_argon_choice(Difficulty::Hard),
    );
    let expected_eff_id =
        compute_derived_coder_effect_id(&job.job_id, "task-corr-prep", 2, &expected_req_key);

    assert_eq!(receipt.receipt_id, expected_receipt_id);
    assert_eq!(
        prep_outcome.request_key.as_deref(),
        Some(expected_req_key.as_str())
    );
    assert_eq!(
        prep_outcome.effect_id.as_deref(),
        Some(expected_eff_id.as_str())
    );
    assert_eq!(receipt.attempt_no, 2);
    assert_eq!(
        receipt.prior_rejected_candidate_id.as_deref(),
        Some(verify_outcome.candidate_id.as_str())
    );
    assert_eq!(
        receipt.prior_review_verdict_id.as_deref(),
        Some(review_outcome.verdict.verdict_id.as_str())
    );
    assert_eq!(
        receipt.prior_review_subject_digest.as_deref(),
        Some(review_subject.subject_digest.as_str())
    );
    assert_eq!(receipt.frozen_instruction, approved_task_before.instruction);
    assert_eq!(receipt.task_digest, approved_task_before.task_digest);
    assert_eq!(receipt.write_paths, vec!["src/a.rs".to_string()]);
    assert_eq!(
        receipt.coder_choice,
        headless_argon_choice(Difficulty::Hard)
    );
    assert_eq!(receipt.execution_base_commit, fixture.base_commit);
    assert_eq!(receipt.run_id, art_1_before.run_id);
    assert_eq!(receipt.worker_id, art_1_before.worker_id);
    assert_eq!(receipt.task_worktree_id, art_1_before.task_worktree_id);
    assert_eq!(
        receipt.integration_worktree_id,
        art_1_before.integration_worktree_id
    );
    assert_eq!(receipt.required_in_scope_fixes, required_fixes);
    assert!(receipt
        .derived_prompt
        .contains(&approved_task_before.instruction));
    assert!(receipt
        .derived_prompt
        .contains("<untrusted_required_in_scope_fixes>"));
    assert!(receipt
        .derived_prompt
        .contains("</untrusted_required_in_scope_fixes>"));
    for fix in &required_fixes {
        let quoted_fix = serde_json::to_string(fix).expect("quote fix");
        assert!(receipt.derived_prompt.contains(&quoted_fix));
    }
    assert_eq!(
        receipt.compute_digest().expect("compute receipt digest"),
        receipt.receipt_digest
    );

    let approval_after = fixture
        .authority
        .get_for_operator(&approval_id)
        .expect("load ApprovalRecord after correction prep");
    assert_eq!(
        approval_after, approval_before,
        "original ApprovalRecord must remain strictly immutable"
    );

    let runtime_after = fixture
        .policy_store
        .get_job_runtime_readonly(BINDING_A, &job.job_id)
        .expect("get job runtime after correction prep")
        .expect("runtime exists");
    let coder_usage = runtime_after
        .role_usage
        .get(&WorkflowRole::Coder)
        .expect("Coder role usage");
    assert_eq!(coder_usage.calls_reserved, 2);
    assert_eq!(coder_usage.calls_completed, 1);
    assert_eq!(runtime_after.total_task_attempts_reserved, 2);
    assert_eq!(runtime_after.total_corrections_reserved, 1);
    assert_eq!(runtime_after.derived_attempts, vec![receipt.clone()]);
    let derived_eff = runtime_after
        .pending_effects
        .get(&expected_eff_id)
        .expect("derived CoderDispatch PendingControllerEffect");
    assert_eq!(derived_eff.kind, PendingControllerEffectKind::CoderDispatch);
    assert_eq!(
        derived_eff.status,
        PendingControllerEffectStatus::ReservedBeforeEffect
    );
    assert_eq!(derived_eff.attempt_no, Some(2));
    assert_eq!(derived_eff.request_key, expected_req_key);
    assert_eq!(derived_eff.subject_digest, receipt.receipt_digest);

    let ledger_after = fixture
        .workflow_store
        .read_binding_ledger(BINDING_A)
        .expect("read binding ledger after correction prep");
    let job_after = ledger_after
        .jobs
        .iter()
        .find(|j| j.job_id == job.job_id)
        .expect("job after correction prep");
    let task_after = job_after
        .tasks
        .iter()
        .find(|t| t.task_id == "task-corr-prep")
        .expect("task after correction prep");
    assert_eq!(task_after.stage, CoworkerTaskStage::DispatchReserved);
    assert_eq!(task_after.current_attempt_no, 2);
    assert_eq!(
        task_after.unresolved_outbox_id.as_deref(),
        prep_outcome.outbox_id.as_deref()
    );
    let outbox_2 = job_after
        .outbox
        .iter()
        .find(|o| Some(o.outbox_id.as_str()) == prep_outcome.outbox_id.as_deref())
        .expect("attempt 2 outbox record");
    assert_eq!(outbox_2.kind, OutboxDispatchKind::TaskAttemptDispatch);
    assert_eq!(outbox_2.attempt_no, 2);
    assert_eq!(outbox_2.request_key, expected_req_key);
    assert_eq!(outbox_2.status, OutboxDispatchStatus::ReservedBeforeEffect);
    assert_eq!(outbox_2.subject_digest, receipt.receipt_digest);

    // Repeating prepare_task_correction on the same session and after session reopen must return
    // Reopened with the exact same receipt and zero extra counter/effect/outbox allocations.
    let repeat_same = correction_service
        .prepare_task_correction(
            session.guard(),
            &CoworkerTaskCorrectionPrepareRequest {
                now_ms: step_ts + 300,
                ..prep_req.clone()
            },
        )
        .await
        .expect("repeat prepare_task_correction on same session");
    assert_eq!(
        repeat_same.status,
        CoworkerTaskCorrectionPrepareStatus::Reopened
    );
    assert!(repeat_same.reused_existing);
    assert_eq!(repeat_same.receipt.as_ref(), Some(&receipt));
    assert_eq!(repeat_same.request_key, prep_outcome.request_key);
    assert_eq!(repeat_same.effect_id, prep_outcome.effect_id);
    assert_eq!(repeat_same.outbox_id, prep_outcome.outbox_id);

    session.release().expect("release session");
    let reopened_session = fixture
        .controller
        .open_owned_session(BINDING_A, THREAD_UUID_A, &job.job_id, step_ts + 400)
        .expect("reopen controller session");
    let reopened_outcome = correction_service
        .prepare_task_correction(
            reopened_session.guard(),
            &CoworkerTaskCorrectionPrepareRequest {
                now_ms: step_ts + 500,
                ..prep_req.clone()
            },
        )
        .await
        .expect("reopened prepare_task_correction");
    assert_eq!(
        reopened_outcome.status,
        CoworkerTaskCorrectionPrepareStatus::Reopened
    );
    assert!(reopened_outcome.reused_existing);
    assert_eq!(reopened_outcome.receipt.as_ref(), Some(&receipt));
    assert_eq!(reopened_outcome.outbox_id, prep_outcome.outbox_id);

    // Calling the initial controller step (`reopened_session.step(...)`) afterward must not
    // overwrite the derived DispatchReserved state or attempt 1 artifacts with the old host snapshot.
    let post_step_report = reopened_session
        .step(step_ts + 600)
        .await
        .expect("controller step after correction preparation");
    assert!(post_step_report.dispatched_task_ids.is_empty());
    assert!(post_step_report
        .reconciled_ready_for_review_task_ids
        .is_empty());
    let obs_after_step = post_step_report
        .tasks
        .iter()
        .find(|t| t.task_id == "task-corr-prep")
        .expect("task observation after step");
    assert_eq!(obs_after_step.stage, CoworkerTaskStage::DispatchReserved);
    assert_eq!(read_invocation_count(&fixture.agy_count_file), 1);

    let runtime_after_step = fixture
        .policy_store
        .get_job_runtime_readonly(BINDING_A, &job.job_id)
        .expect("get runtime after step")
        .expect("runtime exists");
    assert_eq!(
        runtime_after_step
            .role_usage
            .get(&WorkflowRole::Coder)
            .expect("Coder usage")
            .calls_reserved,
        2
    );
    assert_eq!(runtime_after_step.total_task_attempts_reserved, 2);
    assert_eq!(runtime_after_step.total_corrections_reserved, 1);
    assert_eq!(runtime_after_step.derived_attempts, vec![receipt]);
    assert_eq!(
        runtime_after_step.attempt_artifacts.get(&art_key_1),
        Some(&art_1_before),
        "attempt 1 TaskAttemptExecutionArtifacts must not be overwritten by initial controller step"
    );

    reopened_session
        .release()
        .expect("release reopened session");
    host_handle.shutdown().await.expect("shutdown host");
}

#[tokio::test]
async fn task_correction_refuses_missing_or_tampered_reviewer_report_fabricated_verdict_drift_holds_and_caps(
) {
    let fixture = setup_fixture();

    // Case 1: Unreviewed check-failure task (`BlockedByFailedChecks`) must be rejected with
    // `UnsupportedCapability` without fabricating a Reviewer verdict or reserving counters.
    write_executable_script(
        &fixture.fake_cargo_exe,
        "#!/bin/sh\nset -eu\necho \"test src_a ... FAILED\" >&2\nexit 1\n",
    );
    let mut check_fail_ifaces = BTreeMap::new();
    check_fail_ifaces.insert(
        "task-corr-chkfail".to_string(),
        TaskInterfaceOwnershipSpec {
            owned_interfaces: vec!["iface_corr_chkfail".to_string()],
            consumed_interfaces: Vec::new(),
        },
    );
    let (job_chkfail, approval_chkfail) = create_planned_approved_and_frozen_job(
        &fixture,
        "brf_corr_chkfail",
        "plan-corr-chkfail",
        vec![PlanTask {
            id: "task-corr-chkfail".to_string(),
            title: "Check Fail Task".to_string(),
            instruction: "CTRL_TASK_A implement src/a.rs".to_string(),
            role: WorkflowRole::Coder,
            difficulty: Difficulty::Hard,
            depends_on: Vec::new(),
            allowed_write_paths: vec!["src/a.rs".to_string()],
        }],
        check_fail_ifaces,
        vec!["src/a.rs".to_string()],
        1,
        ExecutionBudgets::default(),
        ControllerRoleExecutionBudget::default_for_role(WorkflowRole::Coder, 4),
    )
    .await;

    let host_chkfail = ApprovedHost::start(ApprovedHostConfig::new(
        &fixture.registry_dir,
        &fixture.plans_dir,
        &fixture.authority_dir,
        &fixture.sessions_dir,
        &approval_chkfail,
    ))
    .await
    .expect("start host_chkfail");
    let sess_chkfail = fixture
        .controller
        .open_owned_session(BINDING_A, THREAD_UUID_A, &job_chkfail.job_id, 100_000)
        .expect("open sess_chkfail");
    let mut step_ts = 100_100u64;
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let rep = sess_chkfail.step(step_ts).await.expect("step chkfail");
        step_ts += 100;
        if rep.tasks.iter().any(|t| {
            t.task_id == "task-corr-chkfail" && t.stage == CoworkerTaskStage::ReadyForReview
        }) {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for task-corr-chkfail"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    let verifier = CoworkerTaskVerificationService::new(fixture.controller.clone());
    let verify_chkfail = verifier
        .verify_and_prepare_task_attempt(
            sess_chkfail.guard(),
            &CoworkerTaskVerificationRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                job_id: job_chkfail.job_id.clone(),
                task_id: "task-corr-chkfail".to_string(),
                attempt_no: 1,
                now_ms: step_ts,
            },
        )
        .await
        .expect("verify failing check attempt");
    assert_eq!(
        verify_chkfail.status,
        CoworkerTaskVerificationStatus::BlockedByFailedChecks
    );

    let correction_service = CoworkerTaskCorrectionService::new(fixture.controller.clone());
    let chkfail_err = correction_service
        .prepare_task_correction(
            sess_chkfail.guard(),
            &CoworkerTaskCorrectionPrepareRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                job_id: job_chkfail.job_id.clone(),
                task_id: "task-corr-chkfail".to_string(),
                prior_attempt_no: 1,
                now_ms: step_ts + 50,
            },
        )
        .await
        .expect_err("check-failure correction must be explicitly unsupported in this slice");
    assert_eq!(
        chkfail_err.code(),
        NativeErrorCode::UnsupportedCapability.as_str()
    );
    let rt_chkfail = fixture
        .policy_store
        .get_job_runtime_readonly(BINDING_A, &job_chkfail.job_id)
        .expect("read rt_chkfail")
        .expect("rt_chkfail exists");
    assert_eq!(rt_chkfail.total_corrections_reserved, 0);
    assert_eq!(rt_chkfail.total_task_attempts_reserved, 1);
    assert!(rt_chkfail.derived_attempts.is_empty());

    sess_chkfail.release().expect("release sess_chkfail");
    host_chkfail
        .shutdown()
        .await
        .expect("shutdown host_chkfail");

    // Case 2: Fabricated/caller-created ledger verdict without an actual persisted Reviewer report
    // must be rejected before any counters or effects are reserved.
    write_executable_script(
        &fixture.fake_cargo_exe,
        "#!/bin/sh\nset -eu\nprintf \"running 1 test\\ntest src_a ... ok\\n\"\nexit 0\n",
    );
    let mut fab_ifaces = BTreeMap::new();
    fab_ifaces.insert(
        "task-corr-fab".to_string(),
        TaskInterfaceOwnershipSpec {
            owned_interfaces: vec!["iface_corr_fab".to_string()],
            consumed_interfaces: Vec::new(),
        },
    );
    let (job_fab, approval_fab) = create_planned_approved_and_frozen_job(
        &fixture,
        "brf_corr_fab",
        "plan-corr-fab",
        vec![PlanTask {
            id: "task-corr-fab".to_string(),
            title: "Fabricated Verdict Task".to_string(),
            instruction: "CTRL_TASK_A implement src/a.rs".to_string(),
            role: WorkflowRole::Coder,
            difficulty: Difficulty::Hard,
            depends_on: Vec::new(),
            allowed_write_paths: vec!["src/a.rs".to_string()],
        }],
        fab_ifaces,
        vec!["src/a.rs".to_string()],
        1,
        ExecutionBudgets::default(),
        ControllerRoleExecutionBudget::default_for_role(WorkflowRole::Coder, 4),
    )
    .await;

    let host_fab = ApprovedHost::start(ApprovedHostConfig::new(
        &fixture.registry_dir,
        &fixture.plans_dir,
        &fixture.authority_dir,
        &fixture.sessions_dir,
        &approval_fab,
    ))
    .await
    .expect("start host_fab");
    let sess_fab = fixture
        .controller
        .open_owned_session(BINDING_A, THREAD_UUID_A, &job_fab.job_id, 110_000)
        .expect("open sess_fab");
    let mut step_ts_fab = 110_100u64;
    let deadline_fab = Instant::now() + Duration::from_secs(10);
    loop {
        let rep = sess_fab.step(step_ts_fab).await.expect("step fab");
        step_ts_fab += 100;
        if rep
            .tasks
            .iter()
            .any(|t| t.task_id == "task-corr-fab" && t.stage == CoworkerTaskStage::ReadyForReview)
        {
            break;
        }
        assert!(
            Instant::now() < deadline_fab,
            "timed out waiting for task-corr-fab"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    let verify_fab = verifier
        .verify_and_prepare_task_attempt(
            sess_fab.guard(),
            &CoworkerTaskVerificationRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                job_id: job_fab.job_id.clone(),
                task_id: "task-corr-fab".to_string(),
                attempt_no: 1,
                now_ms: step_ts_fab,
            },
        )
        .await
        .expect("verify task-corr-fab");
    let subject_fab = verify_fab.review_subject.as_ref().expect("subject_fab");
    let policy_digest_fab = job_fab
        .approval_linkage
        .as_ref()
        .expect("linkage fab")
        .policy_digest
        .clone();

    // Directly inject a caller-fabricated CorrectInScope verdict into the workflow store without
    // executing `CoworkerTaskReviewStageService` or persisting a `TaskReviewExecutionReport`.
    let ledger_fab = fixture
        .workflow_store
        .read_binding_ledger(BINDING_A)
        .expect("read ledger_fab");
    let cur_job_fab = ledger_fab
        .jobs
        .iter()
        .find(|j| j.job_id == job_fab.job_id)
        .expect("cur_job_fab")
        .clone();
    let (cur_job_fab, fake_rev_outbox) = fixture
        .workflow_store
        .reserve_task_outbox(&ReserveTaskOutboxRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            job_id: job_fab.job_id.clone(),
            expected_job_revision: cur_job_fab.job_revision,
            expected_ledger_revision: None,
            request_key: "req_forged_review_fab".to_string(),
            kind: OutboxDispatchKind::TaskReviewDispatch,
            task_id: "task-corr-fab".to_string(),
            role: WorkflowRole::Reviewer,
            subject_digest: subject_fab.subject_digest.clone(),
            dependency_input_commit: None,
            dependency_input_tree_sha: None,
            accepted_predecessor_receipts: Vec::new(),
        })
        .expect("reserve fake review outbox");
    let (cur_job_fab, _) = fixture
        .workflow_store
        .reconcile_or_finalize_outbox(&FinalizeTaskOutboxRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            job_id: job_fab.job_id.clone(),
            expected_job_revision: cur_job_fab.job_revision,
            expected_ledger_revision: None,
            outbox_id: fake_rev_outbox.outbox_id,
            status: OutboxDispatchStatus::DispatchedEffectConfirmed,
            next_task_stage: CoworkerTaskStage::UnderReview,
            run_id: None,
            worker_id: None,
            task_worktree_id: Some(subject_fab.task_worktree_id.clone()),
            head_commit: Some(subject_fab.task_head_commit.clone()),
            tree_sha: Some(subject_fab.task_tree_sha.clone()),
            diff_digest: Some(subject_fab.diff_digest.clone()),
            report_artifact_id: Some("trr_00000000000000000000000000000001".to_string()),
            observed_model: Some("gemini-4-argon-high".to_string()),
            observed_effort: Some("high".to_string()),
            receipt_id: None,
            diagnostic: None,
        })
        .expect("finalize fake review outbox");
    let fabricated_verdict = StructuredReviewVerdict {
        verdict_id: "rvd_00000000000000000000000000000001".to_string(),
        task_id: "task-corr-fab".to_string(),
        attempt_no: 1,
        candidate_id: verify_fab.candidate_id.clone(),
        review_subject_digest: subject_fab.subject_digest.clone(),
        base_commit: subject_fab.base_commit.clone(),
        task_head_commit: subject_fab.task_head_commit.clone(),
        task_tree_sha: subject_fab.task_tree_sha.clone(),
        diff_digest: subject_fab.diff_digest.clone(),
        policy_digest: policy_digest_fab,
        outcome: ReviewVerdictOutcome::CorrectInScope,
        findings: vec!["Fabricated finding".to_string()],
        required_in_scope_fixes: vec!["Fabricated fix in src/a.rs".to_string()],
        conflicting_paths_or_interfaces: Vec::new(),
        prose_summary: Some("Caller-created verdict without Reviewer report".to_string()),
        prose_may_advance_acceptance: false,
    };
    let fabricated_candidate = TaskCandidateRef {
        candidate_id: verify_fab.candidate_id.clone(),
        task_id: "task-corr-fab".to_string(),
        attempt_no: 1,
        base_commit: subject_fab.base_commit.clone(),
        head_commit: subject_fab.task_head_commit.clone(),
        tree_sha: subject_fab.task_tree_sha.clone(),
        diff_digest: subject_fab.diff_digest.clone(),
        write_paths: vec!["src/a.rs".to_string()],
        check_evidence_digests: verify_fab
            .check_evidence
            .iter()
            .map(|e| e.evidence_digest.clone())
            .collect(),
        review_subject_digest: Some(subject_fab.subject_digest.clone()),
        review_verdict: Some(fabricated_verdict),
        review_receipt_id: None,
        integration_receipt_id: None,
    };
    fixture
        .workflow_store
        .record_task_candidate_and_verdict(&RecordTaskReviewVerdictRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            job_id: job_fab.job_id.clone(),
            expected_job_revision: cur_job_fab.job_revision,
            expected_ledger_revision: None,
            candidate: fabricated_candidate,
        })
        .expect("record fabricated verdict");

    let fab_err = correction_service
        .prepare_task_correction(
            sess_fab.guard(),
            &CoworkerTaskCorrectionPrepareRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                job_id: job_fab.job_id.clone(),
                task_id: "task-corr-fab".to_string(),
                prior_attempt_no: 1,
                now_ms: step_ts_fab + 100,
            },
        )
        .await
        .expect_err(
            "caller-fabricated verdict without deterministic Reviewer stage/report must fail",
        );
    assert_eq!(fab_err.code(), NativeErrorCode::ValidationFailed.as_str());
    let rt_fab = fixture
        .policy_store
        .get_job_runtime_readonly(BINDING_A, &job_fab.job_id)
        .expect("read rt_fab")
        .expect("rt_fab exists");
    assert_eq!(rt_fab.total_corrections_reserved, 0);
    assert_eq!(rt_fab.total_task_attempts_reserved, 1);
    assert!(rt_fab.derived_attempts.is_empty());

    sess_fab.release().expect("release sess_fab");
    host_fab.shutdown().await.expect("shutdown host_fab");

    // Case 3: Real Reviewer stage -> test tampered/missing Reviewer report, worktree drift,
    // workflow hold, stop_requested, and exhausted correction/attempt/role caps.
    let mut neg_ifaces = BTreeMap::new();
    neg_ifaces.insert(
        "task-corr-neg".to_string(),
        TaskInterfaceOwnershipSpec {
            owned_interfaces: vec!["iface_corr_neg".to_string()],
            consumed_interfaces: Vec::new(),
        },
    );
    let (job_neg, approval_neg) = create_planned_approved_and_frozen_job(
        &fixture,
        "brf_corr_neg",
        "plan-corr-neg",
        vec![PlanTask {
            id: "task-corr-neg".to_string(),
            title: "Negative Guards Task".to_string(),
            instruction: "CTRL_TASK_A implement src/a.rs".to_string(),
            role: WorkflowRole::Coder,
            difficulty: Difficulty::Hard,
            depends_on: Vec::new(),
            allowed_write_paths: vec!["src/a.rs".to_string()],
        }],
        neg_ifaces,
        vec!["src/a.rs".to_string()],
        1,
        ExecutionBudgets::default(),
        ControllerRoleExecutionBudget::default_for_role(WorkflowRole::Coder, 4),
    )
    .await;

    let host_neg = ApprovedHost::start(ApprovedHostConfig::new(
        &fixture.registry_dir,
        &fixture.plans_dir,
        &fixture.authority_dir,
        &fixture.sessions_dir,
        &approval_neg,
    ))
    .await
    .expect("start host_neg");
    let sess_neg = fixture
        .controller
        .open_owned_session(BINDING_A, THREAD_UUID_A, &job_neg.job_id, 120_000)
        .expect("open sess_neg");
    let mut step_ts_neg = 120_100u64;
    let deadline_neg = Instant::now() + Duration::from_secs(10);
    loop {
        let rep = sess_neg.step(step_ts_neg).await.expect("step neg");
        step_ts_neg += 100;
        if rep
            .tasks
            .iter()
            .any(|t| t.task_id == "task-corr-neg" && t.stage == CoworkerTaskStage::ReadyForReview)
        {
            break;
        }
        assert!(
            Instant::now() < deadline_neg,
            "timed out waiting for task-corr-neg"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    let verify_neg = verifier
        .verify_and_prepare_task_attempt(
            sess_neg.guard(),
            &CoworkerTaskVerificationRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                job_id: job_neg.job_id.clone(),
                task_id: "task-corr-neg".to_string(),
                attempt_no: 1,
                now_ms: step_ts_neg,
            },
        )
        .await
        .expect("verify task-corr-neg");
    let subject_neg = verify_neg.review_subject.as_ref().expect("subject_neg");
    let policy_digest_neg = job_neg
        .approval_linkage
        .as_ref()
        .expect("linkage neg")
        .policy_digest
        .clone();

    let neg_resp = StructuredTaskReviewModelResponse {
        schema_version: TASK_REVIEW_RESPONSE_SCHEMA_VERSION,
        task_id: "task-corr-neg".to_string(),
        attempt_no: 1,
        candidate_id: verify_neg.candidate_id.clone(),
        review_subject_digest: subject_neg.subject_digest.clone(),
        base_commit: subject_neg.base_commit.clone(),
        task_head_commit: subject_neg.task_head_commit.clone(),
        task_tree_sha: subject_neg.task_tree_sha.clone(),
        diff_digest: subject_neg.diff_digest.clone(),
        policy_digest: policy_digest_neg,
        outcome: ReviewVerdictOutcome::CorrectInScope,
        findings: vec!["Fix src/a.rs constant".to_string()],
        required_in_scope_fixes: vec!["Adjust constant in src/a.rs".to_string()],
        conflicting_paths_or_interfaces: Vec::new(),
        summary: Some("CorrectInScope for negative guard verification.".to_string()),
    };
    fs::write(
        &fixture.agy_plan_response_file,
        serde_json::to_string(&neg_resp).expect("serialize neg_resp"),
    )
    .expect("write neg_resp");

    let review_service = CoworkerTaskReviewStageService::new(fixture.controller.clone());
    let rev_neg = review_service
        .review_task_attempt(
            sess_neg.guard(),
            &CoworkerTaskReviewStageRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                job_id: job_neg.job_id.clone(),
                task_id: "task-corr-neg".to_string(),
                attempt_no: 1,
                now_ms: step_ts_neg + 100,
                untrusted_candidate_context: None,
            },
        )
        .await
        .expect("review_task_attempt neg");
    assert_eq!(
        rev_neg.status,
        CoworkerTaskReviewStageStatus::CorrectInScopeRequired
    );

    let report_path = PathBuf::from(&rev_neg.report_path);
    let valid_report_bytes = fs::read(&report_path).expect("read valid report bytes");
    let art_key_neg = ControllerJobRuntimeState::artifact_key("task-corr-neg", 1);
    let art_neg_before = fixture
        .policy_store
        .get_job_runtime_readonly(BINDING_A, &job_neg.job_id)
        .expect("read rt")
        .expect("rt")
        .attempt_artifacts
        .get(&art_key_neg)
        .expect("art_neg_before")
        .clone();

    // 3a. Tampered/corrupted persisted Reviewer report bytes on disk must fail closed and preserve
    // the corrupted file bytes and prior attempt artifacts.
    let corrupt_report_bytes = b"{tampered-task-review-execution-report-json";
    fs::write(&report_path, corrupt_report_bytes).expect("write corrupt report bytes");
    fs::set_permissions(&report_path, fs::Permissions::from_mode(0o600))
        .expect("chmod 0600 corrupt report");
    let corrupt_err = correction_service
        .prepare_task_correction(
            sess_neg.guard(),
            &CoworkerTaskCorrectionPrepareRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                job_id: job_neg.job_id.clone(),
                task_id: "task-corr-neg".to_string(),
                prior_attempt_no: 1,
                now_ms: step_ts_neg + 200,
            },
        )
        .await
        .expect_err("tampered Reviewer report must be rejected");
    assert_eq!(corrupt_err.code(), NativeErrorCode::ProtocolError.as_str());
    assert_eq!(
        fs::read(&report_path).expect("read corrupt report after rejection"),
        corrupt_report_bytes,
        "corrupted Reviewer report bytes must be preserved on disk"
    );

    // 3b. Missing persisted Reviewer report file on disk must fail closed.
    fs::remove_file(&report_path).expect("remove report_path");
    let missing_err = correction_service
        .prepare_task_correction(
            sess_neg.guard(),
            &CoworkerTaskCorrectionPrepareRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                job_id: job_neg.job_id.clone(),
                task_id: "task-corr-neg".to_string(),
                prior_attempt_no: 1,
                now_ms: step_ts_neg + 250,
            },
        )
        .await
        .expect_err("missing persisted Reviewer report must be rejected");
    assert_eq!(missing_err.code(), NativeErrorCode::ProtocolError.as_str());

    // Restore the valid 0600 Reviewer report bytes.
    fs::write(&report_path, &valid_report_bytes).expect("restore valid report bytes");
    fs::set_permissions(&report_path, fs::Permissions::from_mode(0o600))
        .expect("chmod 0600 restored report");

    // 3c. Live task worktree drift (uncommitted file change in task worktree after review) must be rejected.
    let task_manifest_neg = threadlane_workers::load_and_verify_manifest(
        &subject_neg.work_area_root,
        &subject_neg.task_worktree_id,
    )
    .expect("load task_manifest_neg");
    let task_file_a = task_manifest_neg.checkout_path.join("src/a.rs");
    let clean_task_file_a = fs::read_to_string(&task_file_a).expect("read clean task src/a.rs");
    fs::write(&task_file_a, "pub fn a() -> u32 { 999 }\n").expect("dirty task worktree src/a.rs");
    let drift_err = correction_service
        .prepare_task_correction(
            sess_neg.guard(),
            &CoworkerTaskCorrectionPrepareRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                job_id: job_neg.job_id.clone(),
                task_id: "task-corr-neg".to_string(),
                prior_attempt_no: 1,
                now_ms: step_ts_neg + 300,
            },
        )
        .await
        .expect_err("drifted task worktree must be rejected");
    assert_eq!(drift_err.code(), NativeErrorCode::ValidationFailed.as_str());
    fs::write(&task_file_a, &clean_task_file_a).expect("restore clean task src/a.rs");

    // 3d. Active workflow hold and `stop_requested` fence correction preparation.
    let cur_job_neg = fixture
        .workflow_store
        .read_binding_ledger(BINDING_A)
        .expect("read ledger")
        .jobs
        .into_iter()
        .find(|j| j.job_id == job_neg.job_id)
        .expect("cur_job_neg");
    let (held_job_neg, hold_rec) = fixture
        .workflow_store
        .apply_workflow_hold(&ApplyWorkflowHoldRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            job_id: job_neg.job_id.clone(),
            expected_job_revision: cur_job_neg.job_revision,
            expected_ledger_revision: None,
            request_key: "req_hold_corr_neg".to_string(),
            reason: WorkflowHoldReason::ConversationClarificationPending,
            detail: "Hold before correction".to_string(),
            directly_affected_task_ids: vec!["task-corr-neg".to_string()],
            affected_write_paths: vec!["src/a.rs".to_string()],
            affected_interfaces: vec!["iface_corr_neg".to_string()],
            holds_entire_job: false,
        })
        .expect("apply hold on task-corr-neg");
    let hold_err = correction_service
        .prepare_task_correction(
            sess_neg.guard(),
            &CoworkerTaskCorrectionPrepareRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                job_id: job_neg.job_id.clone(),
                task_id: "task-corr-neg".to_string(),
                prior_attempt_no: 1,
                now_ms: step_ts_neg + 350,
            },
        )
        .await
        .expect_err("held task must refuse correction preparation");
    assert_eq!(hold_err.code(), NativeErrorCode::ValidationFailed.as_str());

    fixture
        .workflow_store
        .resolve_workflow_hold(&ResolveWorkflowHoldRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            job_id: job_neg.job_id.clone(),
            expected_job_revision: held_job_neg.job_revision,
            expected_ledger_revision: None,
            hold_id: hold_rec.hold_id,
            resolution_note: "Resolved hold".to_string(),
        })
        .expect("resolve hold on task-corr-neg");

    fixture
        .policy_store
        .mutate_job_runtime_for_operator(BINDING_A, &job_neg.job_id, |state| {
            state.stop_requested = true;
            Ok(())
        })
        .expect("set stop_requested");
    let stop_err = correction_service
        .prepare_task_correction(
            sess_neg.guard(),
            &CoworkerTaskCorrectionPrepareRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                job_id: job_neg.job_id.clone(),
                task_id: "task-corr-neg".to_string(),
                prior_attempt_no: 1,
                now_ms: step_ts_neg + 400,
            },
        )
        .await
        .expect_err("stop_requested must refuse correction preparation");
    assert_eq!(stop_err.code(), NativeErrorCode::ValidationFailed.as_str());
    fixture
        .policy_store
        .mutate_job_runtime_for_operator(BINDING_A, &job_neg.job_id, |state| {
            state.stop_requested = false;
            Ok(())
        })
        .expect("clear stop_requested");

    // 3e. Exhausted job correction cap (`total_corrections_reserved == max_total_corrections_per_job`)
    // and exhausted Coder role call budget must refuse preparation before any new receipt or effect.
    fixture
        .policy_store
        .mutate_job_runtime_for_operator(BINDING_A, &job_neg.job_id, |state| {
            state.total_corrections_reserved = state.envelope.max_total_corrections_per_job;
            Ok(())
        })
        .expect("exhaust total_corrections_reserved");
    let cap_err = correction_service
        .prepare_task_correction(
            sess_neg.guard(),
            &CoworkerTaskCorrectionPrepareRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                job_id: job_neg.job_id.clone(),
                task_id: "task-corr-neg".to_string(),
                prior_attempt_no: 1,
                now_ms: step_ts_neg + 450,
            },
        )
        .await
        .expect_err("exhausted max_total_corrections_per_job must refuse preparation");
    assert_eq!(cap_err.code(), NativeErrorCode::ValidationFailed.as_str());

    fixture
        .policy_store
        .mutate_job_runtime_for_operator(BINDING_A, &job_neg.job_id, |state| {
            state.total_corrections_reserved = 0;
            if let Some(usage) = state.role_usage.get_mut(&WorkflowRole::Coder) {
                usage.calls_reserved = 4;
            }
            Ok(())
        })
        .expect("exhaust Coder role calls_reserved");
    let role_cap_err = correction_service
        .prepare_task_correction(
            sess_neg.guard(),
            &CoworkerTaskCorrectionPrepareRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                job_id: job_neg.job_id.clone(),
                task_id: "task-corr-neg".to_string(),
                prior_attempt_no: 1,
                now_ms: step_ts_neg + 500,
            },
        )
        .await
        .expect_err("exhausted Coder role budget must refuse preparation");
    assert_eq!(
        role_cap_err.code(),
        NativeErrorCode::ValidationFailed.as_str()
    );

    let rt_neg_after = fixture
        .policy_store
        .get_job_runtime_readonly(BINDING_A, &job_neg.job_id)
        .expect("read rt_neg_after")
        .expect("rt_neg_after exists");
    assert_eq!(rt_neg_after.total_corrections_reserved, 0);
    assert_eq!(rt_neg_after.total_task_attempts_reserved, 1);
    assert!(rt_neg_after.derived_attempts.is_empty());
    assert_eq!(
        rt_neg_after.attempt_artifacts.get(&art_key_neg),
        Some(&art_neg_before),
        "prior attempt artifacts must be preserved across all pre-effect refusals"
    );

    sess_neg.release().expect("release sess_neg");
    host_neg.shutdown().await.expect("shutdown host_neg");
}

#[tokio::test]
async fn task_correction_waits_when_parallel_coder_capacity_is_full_and_prepares_after_slot_frees()
{
    let fixture = setup_fixture();
    write_executable_script(
        &fixture.fake_cargo_exe,
        "#!/bin/sh\nset -eu\nprintf \"running 1 test\\ntest src_a ... ok\\n\"\nexit 0\n",
    );

    let gate_release_file = fixture
        .plans_dir
        .parent()
        .expect("fixture root")
        .join("release_task_b.flag");
    let script_with_gate = format!(
        r#"#!/bin/sh
set -eu
CODER_COUNT_FILE="{coder_count_file}"
PLANNING_COUNT_FILE="{planning_count_file}"
PROMPT_FILE="{prompt_file}"
PLAN_RESPONSE_FILE="{plan_response_file}"
GATE_RELEASE_FILE="{gate_release_file}"

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

case "$PROMPT" in
    *CTRL_HOLD_B*)
        while [ ! -f "$GATE_RELEASE_FILE" ]; do
            sleep 0.02
        done
        if [ -d "src" ]; then
            printf "pub fn b() -> u32 {{ 200 }}\n" > "src/b.rs"
        fi
        ;;
    *CTRL_TASK_A*)
        if [ -d "src" ]; then
            printf "pub fn a() -> u32 {{ 100 }}\n" > "src/a.rs"
        fi
        ;;
    *)
        if [ -d "src" ]; then
            printf "pub fn a() -> u32 {{ 42 }}\n" > "src/a.rs"
        fi
        ;;
esac

printf '{{"type":"result","subtype":"success","session_id":"sess-agy-ctrl","model":"%s","effort":"%s"}}\n' "$MODEL" "$EFFORT"
exit 0
"#,
        coder_count_file = fixture.agy_count_file.display(),
        planning_count_file = fixture.agy_planning_count_file.display(),
        prompt_file = fixture.agy_prompt_file.display(),
        plan_response_file = fixture.agy_plan_response_file.display(),
        gate_release_file = gate_release_file.display(),
    );
    write_executable_script(&fixture.fake_agy_exe, &script_with_gate);

    let mut task_interfaces = BTreeMap::new();
    task_interfaces.insert(
        "task-corr-cap-a".to_string(),
        TaskInterfaceOwnershipSpec {
            owned_interfaces: vec!["iface_cap_a".to_string()],
            consumed_interfaces: Vec::new(),
        },
    );
    task_interfaces.insert(
        "task-corr-cap-b".to_string(),
        TaskInterfaceOwnershipSpec {
            owned_interfaces: vec!["iface_cap_b".to_string()],
            consumed_interfaces: Vec::new(),
        },
    );
    let (job, approval_id) = create_planned_approved_and_frozen_job(
        &fixture,
        "brf_corr_cap",
        "plan-corr-cap",
        vec![
            PlanTask {
                id: "task-corr-cap-a".to_string(),
                title: "Cap Task A".to_string(),
                instruction: "CTRL_TASK_A implement src/a.rs".to_string(),
                role: WorkflowRole::Coder,
                difficulty: Difficulty::Hard,
                depends_on: Vec::new(),
                allowed_write_paths: vec!["src/a.rs".to_string()],
            },
            PlanTask {
                id: "task-corr-cap-b".to_string(),
                title: "Cap Task B".to_string(),
                instruction: "CTRL_HOLD_B implement src/b.rs".to_string(),
                role: WorkflowRole::Coder,
                difficulty: Difficulty::Hard,
                depends_on: Vec::new(),
                allowed_write_paths: vec!["src/b.rs".to_string()],
            },
        ],
        task_interfaces,
        vec!["src/a.rs".to_string(), "src/b.rs".to_string()],
        1, // max_parallel = 1
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
        .open_owned_session(BINDING_A, THREAD_UUID_A, &job.job_id, 130_000)
        .expect("open controller session");

    // 1. Dispatch task-corr-cap-a (attempt 1) to ReadyForReview while task-corr-cap-b waits at ReadyToDispatch
    // until task-corr-cap-a finishes its initial Coder run.
    let host_client = ApprovedHostClient::new(
        Arc::clone(&fixture.authority),
        &fixture.sessions_dir,
        BINDING_A,
        THREAD_UUID_A,
    )
    .expect("create host client");
    host_client
        .dispatch_task(&approval_id, "task-corr-cap-a")
        .await
        .expect("dispatch task-corr-cap-a");
    let wait_a = Instant::now() + Duration::from_secs(10);
    loop {
        let snap_a = host_client
            .get_task(&approval_id, "task-corr-cap-a")
            .await
            .expect("get_task A");
        if snap_a.status == TaskExecutionStatus::ReadyForReview {
            break;
        }
        assert!(
            Instant::now() < wait_a,
            "timed out waiting for A ReadyForReview"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    // Reconcile A to ReadyForReview and let step_job dispatch B (which stays Running on the gate).
    let step_rep = session
        .step(130_100)
        .await
        .expect("step reconciles A and dispatches B");
    let obs_a = step_rep
        .tasks
        .iter()
        .find(|t| t.task_id == "task-corr-cap-a")
        .expect("obs A");
    let obs_b = step_rep
        .tasks
        .iter()
        .find(|t| t.task_id == "task-corr-cap-b")
        .expect("obs B");
    assert_eq!(obs_a.stage, CoworkerTaskStage::ReadyForReview);
    assert_eq!(obs_b.stage, CoworkerTaskStage::Running);
    assert_eq!(step_rep.active_running_count, 1);
    assert_eq!(read_invocation_count(&fixture.agy_count_file), 2);

    // 2. Verify and review task-corr-cap-a -> CorrectInScopeRequired while task-corr-cap-b is Running.
    let verifier = CoworkerTaskVerificationService::new(fixture.controller.clone());
    let verify_a = verifier
        .verify_and_prepare_task_attempt(
            session.guard(),
            &CoworkerTaskVerificationRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                job_id: job.job_id.clone(),
                task_id: "task-corr-cap-a".to_string(),
                attempt_no: 1,
                now_ms: 130_200,
            },
        )
        .await
        .expect("verify task-corr-cap-a");
    let subject_a = verify_a.review_subject.as_ref().expect("subject A");
    let policy_digest = job
        .approval_linkage
        .as_ref()
        .expect("linkage")
        .policy_digest
        .clone();
    let rev_resp_a = StructuredTaskReviewModelResponse {
        schema_version: TASK_REVIEW_RESPONSE_SCHEMA_VERSION,
        task_id: "task-corr-cap-a".to_string(),
        attempt_no: 1,
        candidate_id: verify_a.candidate_id.clone(),
        review_subject_digest: subject_a.subject_digest.clone(),
        base_commit: subject_a.base_commit.clone(),
        task_head_commit: subject_a.task_head_commit.clone(),
        task_tree_sha: subject_a.task_tree_sha.clone(),
        diff_digest: subject_a.diff_digest.clone(),
        policy_digest,
        outcome: ReviewVerdictOutcome::CorrectInScope,
        findings: vec!["Fix return value in src/a.rs".to_string()],
        required_in_scope_fixes: vec!["Update src/a.rs return value to 101".to_string()],
        conflicting_paths_or_interfaces: Vec::new(),
        summary: Some("CorrectInScope for task-corr-cap-a.".to_string()),
    };
    fs::write(
        &fixture.agy_plan_response_file,
        serde_json::to_string(&rev_resp_a).expect("serialize rev_resp_a"),
    )
    .expect("write rev_resp_a");

    let review_service = CoworkerTaskReviewStageService::new(fixture.controller.clone());
    let rev_out_a = review_service
        .review_task_attempt(
            session.guard(),
            &CoworkerTaskReviewStageRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                job_id: job.job_id.clone(),
                task_id: "task-corr-cap-a".to_string(),
                attempt_no: 1,
                now_ms: 130_300,
                untrusted_candidate_context: None,
            },
        )
        .await
        .expect("review task-corr-cap-a");
    assert_eq!(
        rev_out_a.status,
        CoworkerTaskReviewStageStatus::CorrectInScopeRequired
    );

    // 3. While task-corr-cap-b occupies max_parallel (1), prepare_task_correction for task-corr-cap-a
    // must return WaitingForParallelCapacity with unchanged counters, no receipt, and no effect/outbox.
    let correction_service = CoworkerTaskCorrectionService::new(fixture.controller.clone());
    let waiting_outcome = correction_service
        .prepare_task_correction(
            session.guard(),
            &CoworkerTaskCorrectionPrepareRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                job_id: job.job_id.clone(),
                task_id: "task-corr-cap-a".to_string(),
                prior_attempt_no: 1,
                now_ms: 130_400,
            },
        )
        .await
        .expect("prepare_task_correction while parallel capacity is full");
    assert_eq!(
        waiting_outcome.status,
        CoworkerTaskCorrectionPrepareStatus::WaitingForParallelCapacity
    );
    assert!(waiting_outcome.is_waiting());
    assert_eq!(
        waiting_outcome.disposition,
        CoworkerTaskCorrectionDisposition::Waiting {
            max_parallel: 1,
            active_coder_executions: 1,
        }
    );
    assert_eq!(
        waiting_outcome.task_stage,
        CoworkerTaskStage::CorrectInScopeRequired
    );
    assert!(waiting_outcome.receipt.is_none());
    assert!(waiting_outcome.request_key.is_none());
    assert!(waiting_outcome.effect_id.is_none());
    assert!(waiting_outcome.outbox_id.is_none());
    assert_eq!(read_invocation_count(&fixture.agy_count_file), 2);

    let rt_waiting = fixture
        .policy_store
        .get_job_runtime_readonly(BINDING_A, &job.job_id)
        .expect("read rt_waiting")
        .expect("rt_waiting exists");
    assert_eq!(rt_waiting.total_corrections_reserved, 0);
    assert_eq!(rt_waiting.total_task_attempts_reserved, 2);
    assert!(rt_waiting.derived_attempts.is_empty());
    assert_eq!(
        rt_waiting
            .role_usage
            .get(&WorkflowRole::Coder)
            .expect("Coder usage")
            .calls_reserved,
        2
    );

    // 4. Release the gate file so task-corr-cap-b finishes and reconciles to ReadyForReview,
    // freeing the parallel slot.
    fs::write(&gate_release_file, "release\n").expect("write gate_release_file");
    let mut step_ts = 130_500u64;
    let deadline_b = Instant::now() + Duration::from_secs(10);
    loop {
        let rep = session.step(step_ts).await.expect("step until B finishes");
        step_ts += 100;
        let b_done = rep
            .tasks
            .iter()
            .find(|t| t.task_id == "task-corr-cap-b")
            .expect("obs B");
        if b_done.stage == CoworkerTaskStage::ReadyForReview {
            assert_eq!(rep.active_running_count, 0);
            break;
        }
        assert!(
            Instant::now() < deadline_b,
            "timed out waiting for B ReadyForReview"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    // 5. Now that the parallel slot is free, prepare_task_correction succeeds and reserves attempt 2.
    let prepared_after_free = correction_service
        .prepare_task_correction(
            session.guard(),
            &CoworkerTaskCorrectionPrepareRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                job_id: job.job_id.clone(),
                task_id: "task-corr-cap-a".to_string(),
                prior_attempt_no: 1,
                now_ms: step_ts + 100,
            },
        )
        .await
        .expect("prepare_task_correction after parallel slot frees");
    assert_eq!(
        prepared_after_free.status,
        CoworkerTaskCorrectionPrepareStatus::Prepared
    );
    assert_eq!(
        prepared_after_free.task_stage,
        CoworkerTaskStage::DispatchReserved
    );
    assert_eq!(prepared_after_free.next_attempt_no, 2);
    assert!(prepared_after_free.receipt.is_some());

    let rt_prepared = fixture
        .policy_store
        .get_job_runtime_readonly(BINDING_A, &job.job_id)
        .expect("read rt_prepared")
        .expect("rt_prepared exists");
    assert_eq!(rt_prepared.total_corrections_reserved, 1);
    assert_eq!(rt_prepared.total_task_attempts_reserved, 3);
    assert_eq!(rt_prepared.derived_attempts.len(), 1);
    assert_eq!(
        rt_prepared
            .role_usage
            .get(&WorkflowRole::Coder)
            .expect("Coder usage")
            .calls_reserved,
        3
    );

    session.release().expect("release session");
    host_handle.shutdown().await.expect("shutdown host");
}
