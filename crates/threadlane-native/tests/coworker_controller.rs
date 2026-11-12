use std::collections::BTreeMap;
use std::fs;
use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::time::{Duration, Instant};

use tempfile::TempDir;
use threadlane_authority::{AuthorityStore, OperatorApprovalRequest};
use threadlane_host::{
    ApprovedHost, ApprovedHostClient, ApprovedHostConfig, DependencyPredecessorSelection,
    HostPhase, PrepareDependencyInputsRequest, TaskExecutionStatus,
};
use threadlane_native::coworker_workflow::{
    compute_derived_coder_effect_id, compute_derived_coder_report_artifact_id,
    compute_derived_coder_request_key, compute_derived_task_attempt_receipt_id,
    compute_task_integration_subject_digest, compute_task_review_effect_id,
    compute_task_review_request_key, derive_final_job_integration_identifiers,
    derive_job_aggregation_receipt_id, derived_execution_report_file_path,
    deterministic_prefixed_id, deterministic_task_integration_receipt_id,
    deterministic_task_integration_request_key, integrate_accepted_task,
    read_persisted_candidate_check_report, read_persisted_check_report,
    read_persisted_derived_task_execution_report, read_persisted_job_integration_report,
    read_persisted_task_review_report, sha256_hex, ApplyWorkflowHoldRequest,
    BindJobApprovalRequest, CanonicalProjectBaseRef, ClosureExecutionArtifactRecord,
    CombinedCandidateStatus, ControllerConfigurationPreview, ControllerJobRuntimeState,
    ControllerPolicyStore, ControllerRoleExecutionBudget, CoworkerControllerPhase1Status,
    CoworkerControllerService, CoworkerControllerStepRequest, CoworkerJobCompletionRequest,
    CoworkerJobCompletionService, CoworkerJobCompletionStatus, CoworkerJobOwnershipGuard,
    CoworkerJobOwnershipKey, CoworkerJobOwnershipStore, CoworkerJobPhase, CoworkerJobRecord,
    CoworkerPlanningService, CoworkerTaskCorrectionDisposition,
    CoworkerTaskCorrectionExecuteRequest, CoworkerTaskCorrectionExecuteStatus,
    CoworkerTaskCorrectionExecutionService, CoworkerTaskCorrectionPrepareRequest,
    CoworkerTaskCorrectionPrepareStatus, CoworkerTaskCorrectionService,
    CoworkerTaskIntegrationOutcome, CoworkerTaskIntegrationRequest, CoworkerTaskIntegrationService,
    CoworkerTaskIntegrationStatus, CoworkerTaskReviewStageRequest, CoworkerTaskReviewStageService,
    CoworkerTaskReviewStageStatus, CoworkerTaskStage, CoworkerTaskVerificationOutcome,
    CoworkerTaskVerificationRequest, CoworkerTaskVerificationService,
    CoworkerTaskVerificationStatus, CoworkerWorkflowStore, CreateCoworkerJobRequest,
    CreatePlanningGrantRequest, ExpectedTaskReviewIdentity, FinalizeTaskOutboxRequest,
    FreezeControllerExecutionEnvelopeRequest, FrozenCheckCommandSpec, FrozenCheckExecutionStatus,
    FrozenCheckKind, FrozenControllerCapabilityEnvelope, OrchestrateWorkBriefPlanRequest,
    OutboxDispatchKind, OutboxDispatchStatus, PendingControllerEffect, PendingControllerEffectKind,
    PendingControllerEffectStatus, PlannedCheckSuggestion, PlanningCallBudget,
    PlanningReadOnlyCapabilities, PlanningReservationStatus, RecordTaskReviewVerdictRequest,
    ReserveTaskOutboxRequest, ResolveWorkflowHoldRequest, ReviewVerdictOutcome,
    StructuredOrchestratorOutput, StructuredPlannedTask, StructuredReviewVerdict,
    StructuredTaskReviewModelResponse, TaskCandidateRef, TaskInterfaceOwnershipSpec,
    TaskReviewExecutionStatus, WorkBrief, WorkBriefKind, WorkflowHoldReason,
    TASK_REVIEW_RESPONSE_SCHEMA_VERSION,
};
use threadlane_native::{
    Difficulty, ExecutionPolicy, NativeErrorCode, PolicyAllowRule, RegistryStore,
    RoleExecutionRoute, SelectedRoleChoice, ToolAdapter, WorkflowProfile, WorkflowRole,
};
use threadlane_operations::{
    accept_review_subject, commit_task_worktree_changes, execute_integration,
    inspect_job_integration_intent_readonly, inspect_review_subject, load_integration_receipt,
    load_review_receipt, verify_review_receipt_readonly, ClosureVerdictKind,
    CommitTaskChangesRequest, ExecuteIntegrationRequest, InspectReviewSubjectRequest,
    IntegrationStrategy, JobIntegrationIntentState, ReviewAcceptanceRequest, ReviewPolicyBinding,
    ReviewReceipt, ReviewSubject, TestEvidenceSummary, RECEIPTS_SUBDIR,
};
use threadlane_workers::{
    resolve_exact_commit_sha, resolve_exact_tree_sha, run_sanitized_git, run_sanitized_git_bytes,
    ExecutionBudgets, WorkerFilePermissions, WorkerRunStatus, WorktreeKind,
    DEFAULT_GIT_COMMAND_TIMEOUT_MS, DEFAULT_MAX_GIT_OUTPUT_BYTES, MANIFESTS_SUBDIR,
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
    let root_dir = planning_count_file.parent().expect("fixture root dir");
    let closure_count_file = root_dir.join("agy_closure_count.txt");
    let closure_response_file = root_dir.join("agy_closure_response.json");
    let script = format!(
        r#"#!/bin/sh
set -eu
CODER_COUNT_FILE="{coder_count_file}"
PLANNING_COUNT_FILE="{planning_count_file}"
CLOSURE_COUNT_FILE="{closure_count_file}"
PROMPT_FILE="{prompt_file}"
PLAN_RESPONSE_FILE="{plan_response_file}"
CLOSURE_RESPONSE_FILE="{closure_response_file}"

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
    case "$PROMPT" in
        *StructuredClosureModelResponse*)
            cc=0
            if [ -f "$CLOSURE_COUNT_FILE" ]; then
                cc=$(cat "$CLOSURE_COUNT_FILE")
            fi
            cc=$((cc + 1))
            printf "%s" "$cc" > "$CLOSURE_COUNT_FILE"
            printf "%s" "$PROMPT" > "$PROMPT_FILE"
            SUBJ_DIGEST=$(printf "%s\n" "$PROMPT" | sed -n 's/^subject_digest: //p' | head -n 1 | tr -d '\r ')
            CAND_TREE=$(printf "%s\n" "$PROMPT" | sed -n 's/^candidate_tree_sha: //p' | head -n 1 | tr -d '\r ')
            if [ -f "$CLOSURE_RESPONSE_FILE" ]; then
                raw_resp=$(sed "s/__SUBJECT_DIGEST__/$SUBJ_DIGEST/g; s/__CANDIDATE_TREE_SHA__/$CAND_TREE/g" "$CLOSURE_RESPONSE_FILE")
            else
                raw_resp=$(printf '{{"schema_version":1,"subject_digest":"%s","candidate_tree_sha":"%s","outcome":"accepted","summary":"Final combined candidate verified and accepted.","findings":[]}}' "$SUBJ_DIGEST" "$CAND_TREE")
            fi
            printf '{{"event":"init","init":{{"session_id":"sess-agy-closure-%s","model":"%s"}}}}\n' "$cc" "$MODEL"
            esc=$(printf "%s" "$raw_resp" | sed 's/\\/\\\\/g; s/"/\\"/g' | tr '\n' ' ')
            printf '{{"event":"result","result":{{"status":"SUCCESS","response":"%s"}}}}\n' "$esc"
            exit 0
            ;;
    esac
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
fi"#,
        coder_count_file = coder_count_file.display(),
        planning_count_file = planning_count_file.display(),
        closure_count_file = closure_count_file.display(),
        prompt_file = prompt_file.display(),
        plan_response_file = plan_response_file.display(),
        closure_response_file = closure_response_file.display(),
    );
    let tail = r#"

c=0
if [ -f "$CODER_COUNT_FILE" ]; then
    c=$(cat "$CODER_COUNT_FILE")
fi
c=$((c + 1))
printf "%s" "$c" > "$CODER_COUNT_FILE"

printf "%s" "$PROMPT" > "$PROMPT_FILE"
printf '{"type":"init","session_id":"sess-agy-ctrl","model":"%s","effort":"%s"}\n' "$MODEL" "$EFFORT"

if [ -d "src" ]; then
    case "$PROMPT" in
        *"<untrusted_required_in_scope_fixes>"*)
            if ! grep -q "100" "src/a.rs"; then
                echo "expected prior attempt 1 contents in src/a.rs before correction" >&2
                exit 3
            fi
            case "$PROMPT" in
                *HOLD_CORR_PROVIDER*)
                    while true; do
                        sleep 0.1
                    done
                    ;;
            esac
            printf "pub fn a() -> u32 { 101 }\n" > "src/a.rs"
            ;;
        *CTRL_TASK_A*)
            printf "pub fn a() -> u32 { 100 }\n" > "src/a.rs"
            ;;
        *CTRL_TASK_B*)
            printf "pub fn b() -> u32 { 200 }\n" > "src/b.rs"
            ;;
        *CTRL_TASK_C_ONLY_A*)
            if ! grep -q "100" "src/a.rs"; then
                echo "expected integrated predecessor contents in src/a.rs" >&2
                exit 3
            fi
            printf "pub fn c() -> u32 { 300 }\n" > "src/c.rs"
            ;;
        *CTRL_TASK_C*)
            if ! grep -q "100" "src/a.rs" || ! grep -q "200" "src/b.rs"; then
                echo "expected integrated predecessor contents in src/a.rs and src/b.rs" >&2
                exit 3
            fi
            printf "pub fn c() -> u32 { 300 }\n" > "src/c.rs"
            ;;
        *)
            printf "pub fn a() -> u32 { 42 }\n" > "src/a.rs"
            ;;
    esac
fi

printf '{"type":"result","subtype":"success","session_id":"sess-agy-ctrl","model":"%s","effort":"%s"}\n' "$MODEL" "$EFFORT"
exit 0
"#;
    let full_script = format!("{script}{tail}");
    write_executable_script(script_path, &full_script);
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
    let planning_artifacts_dir =
        threadlane_native::planning_driver::planning_artifacts_dir(&plans_dir);
    for private_dir in [
        &plans_dir,
        &sessions_dir,
        &ownership_dir,
        &planning_artifacts_dir,
    ] {
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

fn configure_opencode_coder(fixture: &ControllerTestFixture) {
    let script = fixture.fake_agy_exe.with_file_name("fake-opencode");
    write_executable_script(
        &script,
        r#"#!/bin/sh
set -eu
STAMP="$(dirname "$0")/$(basename "$(pwd)")-created-ms"
if [ "${1:-}" = "api" ] && [ "${3:-}" = "session.create" ]; then
    date +%s%3N > "$STAMP"
    printf '{"data":{"id":"sess-oc-initial","location":{"directory":"%s"},"time":{"created":%s}}}\n' "$(pwd)" "$(cat "$STAMP")"
    exit 0
fi
if [ "${1:-}" = "api" ] && [ "${3:-}" = "session.get" ]; then
    printf '{"data":{"id":"sess-oc-initial","outcome":"succeeded","location":{"directory":"%s"},"time":{"created":%s,"idle":%s}}}\n' "$(pwd)" "$(cat "$STAMP")" "$(cat "$STAMP.done")"
    exit 0
fi
if [ "${1:-}" = "session" ] && [ "${2:-}" = "export" ]; then
    printf '{"info":{"id":"sess-oc-initial","outcome":"succeeded","location":{"directory":"%s"},"time":{"created":%s,"idle":%s}},"messages":[{"id":"msg-initial","type":"assistant","sessionID":"sess-oc-initial","finish":"stop","time":{"created":%s,"completed":%s},"model":{"providerID":"opencode-go","id":"deepseek-v4.1-flash","variant":"max"}}]}\n' "$(pwd)" "$(cat "$STAMP")" "$(cat "$STAMP.done")" "$(cat "$STAMP")" "$(cat "$STAMP.done")"
    exit 0
fi
if [ "${1:-}" = "--standalone" ]; then shift; fi
if [ "${1:-}" != "run" ]; then exit 2; fi
for arg in "$@"; do
    case "$arg" in
        *CTRL_TASK_B*) while true; do sleep 0.1; done ;;
    esac
done
printf 'pub fn a() -> u32 { 100 }\n' > src/a.rs
date +%s%3N > "$STAMP.done"
printf '%s\n' '{"type":"step_start","sessionID":"sess-oc-initial","model":"opencode-go/deepseek-v4.1-flash","variant":"max"}'
printf '%s\n' '{"type":"step_finish","sessionID":"sess-oc-initial","model":"opencode-go/deepseek-v4.1-flash","variant":"max","reason":"stop"}'
"#,
    );
    let snapshot = fixture.registry.snapshot().expect("registry snapshot");
    let mut profile = snapshot.profiles[0].clone();
    fixture
        .registry
        .apply(
            snapshot.revision,
            RegistryChange::UpsertTool {
                tool: ToolDefinition {
                    id: "tool-opencode".to_string(),
                    label: "Offline OpenCode fixture".to_string(),
                    adapter: ToolAdapter::OpencodeV2,
                    executable_path: Some(script),
                    enabled: true,
                },
            },
        )
        .expect("register fake OpenCode");
    fixture
        .registry
        .apply(
            snapshot.revision + 1,
            RegistryChange::UpsertModels {
                models: vec![ModelDefinition {
                    id: "model-deepseek".to_string(),
                    tool_id: "tool-opencode".to_string(),
                    label: "DeepSeek Flash fixture".to_string(),
                    model_arg: "opencode-go/deepseek-v4.1-flash".to_string(),
                    efforts: vec!["max".to_string()],
                    allow_no_effort: false,
                }],
            },
        )
        .expect("register pinned OpenCode model");
    let coder = profile
        .assignments
        .iter_mut()
        .find(|a| a.role == WorkflowRole::Coder)
        .expect("Coder assignment");
    coder.difficulty = Some(Difficulty::Medium);
    coder.choice = ModelChoice {
        model_id: "model-deepseek".to_string(),
        effort: Some("max".to_string()),
    };
    fixture
        .registry
        .apply(
            snapshot.revision + 2,
            RegistryChange::UpsertProfile { profile },
        )
        .expect("pin fake OpenCode Coder");
}

fn execution_policy_for_fixture(fixture: &ControllerTestFixture) -> ExecutionPolicy {
    let mut policy = execution_policy();
    if fixture
        .registry
        .snapshot()
        .expect("registry snapshot")
        .tools
        .iter()
        .any(|t| t.id == "tool-opencode")
    {
        policy.allow_rules.push(PolicyAllowRule {
            tool_id: "tool-opencode".to_string(),
            model_id: "model-deepseek".to_string(),
            efforts: vec!["max".to_string()],
            allow_no_effort: false,
            roles: vec![WorkflowRole::Coder],
        });
    }
    policy
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
    sample_brief_for_scope(fixture, BINDING_A, THREAD_UUID_A, brief_id)
}

fn sample_brief_for_scope(
    fixture: &ControllerTestFixture,
    binding_id: &str,
    thread_uuid: &str,
    brief_id: &str,
) -> WorkBrief {
    WorkBrief {
        brief_id: brief_id.to_string(),
        binding_id: binding_id.to_string(),
        thread_uuid: thread_uuid.to_string(),
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
    create_planned_approved_and_frozen_job_for_scope_with_options(
        fixture,
        BINDING_A,
        THREAD_UUID_A,
        brief_id,
        plan_id,
        tasks,
        task_interfaces,
        approved_write_paths,
        max_parallel,
        approval_budgets,
        coder_budget,
        false,
        1,
        2,
        6,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn create_planned_approved_and_frozen_job_for_scope(
    fixture: &ControllerTestFixture,
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
) -> (CoworkerJobRecord, String) {
    create_planned_approved_and_frozen_job_for_scope_with_options(
        fixture,
        binding_id,
        thread_uuid,
        brief_id,
        plan_id,
        tasks,
        task_interfaces,
        approved_write_paths,
        max_parallel,
        approval_budgets,
        coder_budget,
        false,
        1,
        2,
        6,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn create_planned_approved_and_frozen_job_with_options(
    fixture: &ControllerTestFixture,
    brief_id: &str,
    plan_id: &str,
    tasks: Vec<PlanTask>,
    task_interfaces: BTreeMap<String, TaskInterfaceOwnershipSpec>,
    approved_write_paths: Vec<String>,
    max_parallel: usize,
    approval_budgets: ExecutionBudgets,
    coder_budget: ControllerRoleExecutionBudget,
    allow_check_failure_corrections: bool,
    max_in_scope_corrections_per_task: u32,
    max_total_corrections_per_job: u32,
    max_total_task_attempts_per_job: u32,
) -> (CoworkerJobRecord, String) {
    create_planned_approved_and_frozen_job_for_scope_with_options(
        fixture,
        BINDING_A,
        THREAD_UUID_A,
        brief_id,
        plan_id,
        tasks,
        task_interfaces,
        approved_write_paths,
        max_parallel,
        approval_budgets,
        coder_budget,
        allow_check_failure_corrections,
        max_in_scope_corrections_per_task,
        max_total_corrections_per_job,
        max_total_task_attempts_per_job,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn create_planned_approved_and_frozen_job_for_scope_with_options(
    fixture: &ControllerTestFixture,
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
    allow_check_failure_corrections: bool,
    max_in_scope_corrections_per_task: u32,
    max_total_corrections_per_job: u32,
    max_total_task_attempts_per_job: u32,
) -> (CoworkerJobRecord, String) {
    let pb = project_base(fixture);
    let reg_snap = fixture.registry.snapshot().expect("registry snapshot");
    let brief = sample_brief_for_scope(fixture, binding_id, thread_uuid, brief_id);
    let scoped_workflow = ScopedWorkflow::new(
        Arc::clone(&fixture.registry),
        Arc::clone(&fixture.plans),
        binding_id.to_string(),
    )
    .expect("create scoped_workflow");

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
    fs::write(&fixture.agy_plan_response_file, &plan_json).expect("write agy_plan_response_file");

    let orch_req_key = format!("req_orch_{brief_id}");
    let plan_outcome = fixture
        .planning_service
        .orchestrate_brief_to_plan(
            &scoped_workflow,
            &OrchestrateWorkBriefPlanRequest {
                binding_id: binding_id.to_string(),
                thread_uuid: thread_uuid.to_string(),
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
        max_in_scope_correction_attempts: max_in_scope_corrections_per_task,
        allowed_check_ids: vec!["cargo_test".to_string()],
        local_git_integration_allowed: true,
        allow_check_failure_corrections,
    };

    let bound_job = fixture
        .workflow_store
        .bind_job_approval(
            &BindJobApprovalRequest {
                binding_id: binding_id.to_string(),
                thread_uuid: thread_uuid.to_string(),
                job_id: job.job_id,
                expected_job_revision: job.job_revision,
                expected_ledger_revision: None,
                approval_id: approval_summary.approval_id.clone(),
                policy: execution_policy_for_fixture(fixture),
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
        binding_id: binding_id.to_string(),
        thread_uuid: thread_uuid.to_string(),
        job_id: bound_job.job_id.clone(),
        expected_job_revision: bound_job.job_revision,
        policy: execution_policy_for_fixture(fixture),
        reviewer_choice: headless_argon_choice(Difficulty::Hard),
        closure_choice: headless_argon_choice(Difficulty::Extreme),
        role_budgets,
        max_in_scope_corrections_per_task,
        max_total_corrections_per_job,
        max_total_task_attempts_per_job,
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
    assert_eq!(
        preview.allow_check_failure_corrections,
        allow_check_failure_corrections && max_in_scope_corrections_per_task > 0
    );
    assert_eq!(
        preview.envelope.allow_check_failure_corrections,
        allow_check_failure_corrections && max_in_scope_corrections_per_task > 0
    );
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
                    allow_check_failure_corrections: false,
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
    assert_initial_coder_verification(false).await;
}

#[tokio::test]
async fn initial_opencode_root_task_verification_while_parallel_root_runs_and_dependent_waits() {
    assert_initial_coder_verification(true).await;
}

async fn assert_initial_coder_verification(opencode: bool) {
    let fixture = setup_fixture();
    if opencode {
        configure_opencode_coder(&fixture);
    }
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
    let mut tasks = vec![PlanTask {
        id: "task-verify-a".to_string(),
        title: "Verify Task A".to_string(),
        instruction: "CTRL_TASK_A implement src/a.rs".to_string(),
        role: WorkflowRole::Coder,
        difficulty: if opencode {
            Difficulty::Medium
        } else {
            Difficulty::Hard
        },
        depends_on: Vec::new(),
        allowed_write_paths: vec!["src/a.rs".to_string()],
    }];
    if opencode {
        task_interfaces.insert(
            "task-running-b".to_string(),
            TaskInterfaceOwnershipSpec {
                owned_interfaces: vec!["iface_running_b".to_string()],
                consumed_interfaces: Vec::new(),
            },
        );
        task_interfaces.insert(
            "task-dependent-c".to_string(),
            TaskInterfaceOwnershipSpec {
                owned_interfaces: Vec::new(),
                consumed_interfaces: vec![
                    "iface_verify_a".to_string(),
                    "iface_running_b".to_string(),
                ],
            },
        );
        tasks.extend([
            PlanTask {
                id: "task-running-b".to_string(),
                title: "Keep second root running".to_string(),
                instruction: "CTRL_TASK_B implement src/b.rs".to_string(),
                role: WorkflowRole::Coder,
                difficulty: Difficulty::Medium,
                depends_on: Vec::new(),
                allowed_write_paths: vec!["src/b.rs".to_string()],
            },
            PlanTask {
                id: "task-dependent-c".to_string(),
                title: "Wait for both roots".to_string(),
                instruction: "CTRL_TASK_C implement src/c.rs".to_string(),
                role: WorkflowRole::Coder,
                difficulty: Difficulty::Medium,
                depends_on: vec!["task-verify-a".to_string(), "task-running-b".to_string()],
                allowed_write_paths: vec!["src/c.rs".to_string()],
            },
        ]);
    }
    let (job, approval_id) = create_planned_approved_and_frozen_job(
        &fixture,
        "brf_verify_pass",
        "plan-verify-pass",
        tasks,
        task_interfaces,
        if opencode {
            vec![
                "src/a.rs".to_string(),
                "src/b.rs".to_string(),
                "src/c.rs".to_string(),
            ]
        } else {
            vec!["src/a.rs".to_string()]
        },
        if opencode { 2 } else { 1 },
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
            if opencode {
                assert_eq!(rep.active_running_count, 1, "{rep:?}");
                assert_eq!(
                    rep.tasks
                        .iter()
                        .find(|t| t.task_id == "task-running-b")
                        .unwrap()
                        .stage,
                    CoworkerTaskStage::Running
                );
                assert_eq!(
                    rep.tasks
                        .iter()
                        .find(|t| t.task_id == "task-dependent-c")
                        .unwrap()
                        .stage,
                    CoworkerTaskStage::PendingDependencies
                );
                let runtime = fixture
                    .policy_store
                    .get_job_runtime_readonly(BINDING_A, &job.job_id)
                    .unwrap()
                    .unwrap();
                let artifact = runtime
                    .attempt_artifacts
                    .get(&ControllerJobRuntimeState::artifact_key("task-verify-a", 1))
                    .unwrap();
                assert_eq!(artifact.observed_provider, "opencode_v2");
                assert_eq!(
                    artifact.observed_model.as_deref(),
                    Some("opencode-go/deepseek-v4.1-flash")
                );
                assert_eq!(artifact.observed_effort.as_deref(), Some("max"));
                assert_eq!(
                    artifact.committed_head_commit.as_deref(),
                    Some(fixture.base_commit.as_str())
                );
                assert_eq!(
                    artifact.committed_tree_sha.as_deref(),
                    Some(fixture.base_tree_sha.as_str())
                );
                assert!(runtime
                    .pending_effects
                    .values()
                    .all(|e| e.kind == PendingControllerEffectKind::CoderDispatch));
                assert_eq!(read_invocation_count(&check_count_file), 0);
            }
            break;
        }
        if opencode && obs.stage == CoworkerTaskStage::Failed {
            let client = ApprovedHostClient::new(
                Arc::clone(&fixture.authority),
                &fixture.sessions_dir,
                BINDING_A,
                THREAD_UUID_A,
            )
            .unwrap();
            panic!(
                "OpenCode fixture failed: {:?}",
                client
                    .get_task(&approval_id, "task-verify-a")
                    .await
                    .unwrap()
            );
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

#[allow(clippy::too_many_arguments)]
async fn create_planned_approved_and_frozen_job_with_strategy(
    fixture: &ControllerTestFixture,
    brief_id: &str,
    plan_id: &str,
    tasks: Vec<PlanTask>,
    task_interfaces: BTreeMap<String, TaskInterfaceOwnershipSpec>,
    approved_write_paths: Vec<String>,
    max_parallel: usize,
    approval_budgets: ExecutionBudgets,
    coder_budget: ControllerRoleExecutionBudget,
    default_integration_strategy: IntegrationStrategy,
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
        default_integration_strategy,
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

async fn run_verify_accept_and_integrate_two_roots(
    fixture: &ControllerTestFixture,
    integration_service: &CoworkerTaskIntegrationService,
    session: &threadlane_native::coworker_workflow::OwnedCoworkerControllerSession,
    job_id: &str,
    task_a_id: &str,
    task_b_id: &str,
    base_time_ms: u64,
) -> (
    CoworkerTaskIntegrationOutcome,
    CoworkerTaskIntegrationOutcome,
) {
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut step_ts = base_time_ms;
    loop {
        let rep = session.step(step_ts).await.expect("controller step");
        step_ts += 100;
        let obs_a = rep
            .tasks
            .iter()
            .find(|t| t.task_id == task_a_id)
            .expect("task A observation");
        let obs_b = rep
            .tasks
            .iter()
            .find(|t| t.task_id == task_b_id)
            .expect("task B observation");
        if obs_a.stage == CoworkerTaskStage::ReadyForReview
            && obs_b.stage == CoworkerTaskStage::ReadyForReview
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for two root tasks ReadyForReview: {rep:?}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    let _ = verify_and_accept_task_without_integrating(
        fixture,
        session.guard(),
        job_id,
        task_a_id,
        "src/a.rs",
        step_ts,
        i64::try_from(step_ts + 1_000).expect("fixture approved_at_ms fits in i64"),
    )
    .await;

    let int_out_a = integration_service
        .integrate_accepted_task(
            session.guard(),
            &CoworkerTaskIntegrationRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                job_id: job_id.to_string(),
                task_id: task_a_id.to_string(),
                attempt_no: 1,
                now_ms: step_ts + 2_000,
            },
        )
        .expect("integrate_accepted_task for task A");
    assert_eq!(int_out_a.status, CoworkerTaskIntegrationStatus::Integrated);

    let _ = verify_and_accept_task_without_integrating(
        fixture,
        session.guard(),
        job_id,
        task_b_id,
        "src/b.rs",
        step_ts + 3_000,
        i64::try_from(step_ts + 4_000).expect("fixture approved_at_ms fits in i64"),
    )
    .await;

    let int_out_b = integration_service
        .integrate_accepted_task(
            session.guard(),
            &CoworkerTaskIntegrationRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                job_id: job_id.to_string(),
                task_id: task_b_id.to_string(),
                attempt_no: 1,
                now_ms: step_ts + 5_000,
            },
        )
        .expect("integrate_accepted_task for task B");
    assert_eq!(int_out_b.status, CoworkerTaskIntegrationStatus::Integrated);

    (int_out_a, int_out_b)
}

#[tokio::test]
async fn job_completion_aggregates_two_integrated_root_tasks_preserves_original_receipts_and_reopens_idempotently(
) {
    let fixture = setup_fixture();
    let tmp_root = fs::canonicalize(fixture._tmp.path()).expect("canonicalize tmp root");
    let integration_service = CoworkerTaskIntegrationService::new(fixture.controller.clone());
    let completion_service =
        CoworkerJobCompletionService::from_controller(fixture.controller.clone())
            .expect("construct CoworkerJobCompletionService from controller");

    // 1. First verify that two divergent root tasks frozen with `FastForwardOnly` are refused
    // pre-effect without silent strategy fallback or second-task Git mutation.
    let ff_tasks = vec![
        PlanTask {
            id: "task-ff-a".to_string(),
            title: "Task FF A".to_string(),
            instruction: "CTRL_TASK_A update src/a.rs".to_string(),
            role: WorkflowRole::Coder,
            difficulty: Difficulty::Hard,
            depends_on: Vec::new(),
            allowed_write_paths: vec!["src/a.rs".to_string()],
        },
        PlanTask {
            id: "task-ff-b".to_string(),
            title: "Task FF B".to_string(),
            instruction: "CTRL_TASK_B update src/b.rs".to_string(),
            role: WorkflowRole::Coder,
            difficulty: Difficulty::Hard,
            depends_on: Vec::new(),
            allowed_write_paths: vec!["src/b.rs".to_string()],
        },
    ];
    let mut ff_ifaces = BTreeMap::new();
    ff_ifaces.insert(
        "task-ff-a".to_string(),
        TaskInterfaceOwnershipSpec {
            owned_interfaces: vec!["mod_a::api".to_string()],
            consumed_interfaces: Vec::new(),
        },
    );
    ff_ifaces.insert(
        "task-ff-b".to_string(),
        TaskInterfaceOwnershipSpec {
            owned_interfaces: vec!["mod_b::api".to_string()],
            consumed_interfaces: Vec::new(),
        },
    );
    let (ff_job, ff_approval_id) = create_planned_approved_and_frozen_job_with_strategy(
        &fixture,
        "brf_job_agg_ff_refusal",
        "plan-job-agg-ff-refusal",
        ff_tasks,
        ff_ifaces,
        vec!["src/a.rs".to_string(), "src/b.rs".to_string()],
        2,
        ExecutionBudgets::default(),
        ControllerRoleExecutionBudget::default_for_role(WorkflowRole::Coder, 4),
        IntegrationStrategy::FastForwardOnly,
    )
    .await;

    let ff_host = ApprovedHost::start(ApprovedHostConfig::new(
        &fixture.registry_dir,
        &fixture.plans_dir,
        &fixture.authority_dir,
        &fixture.sessions_dir,
        &ff_approval_id,
    ))
    .await
    .expect("start ApprovedHost for ff_job");

    let ff_session = fixture
        .controller
        .open_owned_session(BINDING_A, THREAD_UUID_A, &ff_job.job_id, 1_700_005_000_000)
        .expect("open controller session for ff_job");
    let _ = run_verify_accept_and_integrate_two_roots(
        &fixture,
        &integration_service,
        &ff_session,
        &ff_job.job_id,
        "task-ff-a",
        "task-ff-b",
        1_700_005_001_000,
    )
    .await;

    let ff_err = completion_service
        .aggregate_integrated_job(
            ff_session.guard(),
            &CoworkerJobCompletionRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                job_id: ff_job.job_id.clone(),
                now_ms: 1_700_005_020_000,
            },
        )
        .await
        .expect_err("FastForwardOnly on two divergent root commits must fail closed without strategy fallback");
    assert_eq!(ff_err.code(), NativeErrorCode::ValidationFailed.as_str());
    drop(ff_session);
    ff_host.shutdown().await.expect("shutdown ff_host");

    // 2. Now create the two-root job frozen with `IntegrationStrategy::MergeNoFf`.
    let tasks = vec![
        PlanTask {
            id: "task-agg-a".to_string(),
            title: "Task Agg A".to_string(),
            instruction: "CTRL_TASK_A update src/a.rs".to_string(),
            role: WorkflowRole::Coder,
            difficulty: Difficulty::Hard,
            depends_on: Vec::new(),
            allowed_write_paths: vec!["src/a.rs".to_string()],
        },
        PlanTask {
            id: "task-agg-b".to_string(),
            title: "Task Agg B".to_string(),
            instruction: "CTRL_TASK_B update src/b.rs".to_string(),
            role: WorkflowRole::Coder,
            difficulty: Difficulty::Hard,
            depends_on: Vec::new(),
            allowed_write_paths: vec!["src/b.rs".to_string()],
        },
    ];
    let mut ifaces = BTreeMap::new();
    ifaces.insert(
        "task-agg-a".to_string(),
        TaskInterfaceOwnershipSpec {
            owned_interfaces: vec!["mod_a::api".to_string()],
            consumed_interfaces: Vec::new(),
        },
    );
    ifaces.insert(
        "task-agg-b".to_string(),
        TaskInterfaceOwnershipSpec {
            owned_interfaces: vec!["mod_b::api".to_string()],
            consumed_interfaces: Vec::new(),
        },
    );
    let (job, approval_id) = create_planned_approved_and_frozen_job_with_strategy(
        &fixture,
        "brf_job_agg_two_roots",
        "plan-job-agg-two-roots",
        tasks,
        ifaces,
        vec!["src/a.rs".to_string(), "src/b.rs".to_string()],
        2,
        ExecutionBudgets::default(),
        ControllerRoleExecutionBudget::default_for_role(WorkflowRole::Coder, 4),
        IntegrationStrategy::MergeNoFf,
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
    .expect("start ApprovedHost for job");

    let session = fixture
        .controller
        .open_owned_session(BINDING_A, THREAD_UUID_A, &job.job_id, 1_700_005_100_000)
        .expect("open owner session");

    let (int_out_a, int_out_b) = run_verify_accept_and_integrate_two_roots(
        &fixture,
        &integration_service,
        &session,
        &job.job_id,
        "task-agg-a",
        "task-agg-b",
        1_700_005_101_000,
    )
    .await;
    assert_ne!(
        int_out_a.integration_worktree_id, int_out_b.integration_worktree_id,
        "two root tasks must have distinct per-worker integration worktrees"
    );

    let orig_receipt_id_a = int_out_a.integration_receipt.receipt_id.clone();
    let orig_receipt_id_b = int_out_b.integration_receipt.receipt_id.clone();
    let orig_receipt_a_disk =
        load_integration_receipt(host_handle.work_area_dir(), &orig_receipt_id_a)
            .expect("load orig receipt A");
    let orig_receipt_b_disk =
        load_integration_receipt(host_handle.work_area_dir(), &orig_receipt_id_b)
            .expect("load orig receipt B");
    assert!(orig_receipt_a_disk.job_aggregation.is_none());
    assert!(orig_receipt_b_disk.job_aggregation.is_none());

    let closure_count_file = tmp_root.join("agy_closure_count.txt");
    let closure_calls_before = read_invocation_count(&closure_count_file);

    // 3. Run real multi-worker Git aggregation (`aggregate_integrated_job`).
    let agg_req = CoworkerJobCompletionRequest {
        binding_id: BINDING_A.to_string(),
        thread_uuid: THREAD_UUID_A.to_string(),
        job_id: job.job_id.clone(),
        now_ms: 1_700_005_120_000,
    };
    let agg_outcome = completion_service
        .aggregate_integrated_job(session.guard(), &agg_req)
        .await
        .expect("aggregate_integrated_job on two integrated root tasks");

    assert!(!agg_outcome.reused_existing_receipts);
    assert_eq!(agg_outcome.approved_base_commit, fixture.base_commit);
    assert_ne!(
        agg_outcome.final_integrated_target_commit,
        fixture.base_commit
    );
    assert_ne!(
        agg_outcome.final_integrated_target_commit,
        int_out_a.integrated_target_commit
    );
    assert_ne!(
        agg_outcome.final_integrated_target_commit,
        int_out_b.integrated_target_commit
    );
    assert_eq!(
        read_invocation_count(&closure_count_file),
        closure_calls_before,
        "aggregate_integrated_job must not invoke Closure"
    );

    // Verify deterministic target identifiers match `derive_final_job_integration_identifiers`.
    let runtime_state = fixture
        .policy_store
        .get_job_runtime_readonly(BINDING_A, &job.job_id)
        .expect("read runtime")
        .expect("runtime exists");
    let (expected_run_id, expected_token, expected_wt_id, expected_branch) =
        derive_final_job_integration_identifiers(
            &job.job_id,
            &approval_id,
            &runtime_state.envelope.approval_subject_digest,
        );
    assert_eq!(agg_outcome.target_integration_run_id, expected_run_id);
    assert_eq!(agg_outcome.target_integration_token, expected_token);
    assert_eq!(agg_outcome.integration_worktree_id, expected_wt_id);
    assert_eq!(agg_outcome.integration_branch_name, expected_branch);

    // Verify the final owned `RunIntegration` checkout contains both worker changes and unmodified `src/c.rs`.
    let final_wt_path = &agg_outcome.integration_checkout_path;
    assert_eq!(
        fs::read_to_string(final_wt_path.join("src/a.rs")).expect("read final src/a.rs"),
        "pub fn a() -> u32 { 100 }\n"
    );
    assert_eq!(
        fs::read_to_string(final_wt_path.join("src/b.rs")).expect("read final src/b.rs"),
        "pub fn b() -> u32 { 200 }\n"
    );
    assert_eq!(
        fs::read_to_string(final_wt_path.join("src/c.rs")).expect("read final src/c.rs"),
        "pub fn c() -> u32 { 3 }\n"
    );
    let actual_final_head =
        resolve_exact_commit_sha(final_wt_path, "HEAD", DEFAULT_GIT_COMMAND_TIMEOUT_MS)
            .expect("resolve final wt HEAD");
    let actual_final_tree =
        resolve_exact_tree_sha(final_wt_path, "HEAD", DEFAULT_GIT_COMMAND_TIMEOUT_MS)
            .expect("resolve final wt tree");
    assert_eq!(
        actual_final_head,
        agg_outcome.final_integrated_target_commit
    );
    assert_eq!(actual_final_tree, agg_outcome.final_integrated_tree_sha);

    // Primary user source checkout remains untouched at `fixture.base_commit`.
    assert_eq!(
        resolve_exact_commit_sha(&fixture.repo_dir, "HEAD", DEFAULT_GIT_COMMAND_TIMEOUT_MS)
            .expect("primary repo HEAD"),
        fixture.base_commit
    );
    assert_eq!(
        fs::read_to_string(fixture.repo_dir.join("src/a.rs")).expect("primary src/a.rs"),
        "pub fn a() -> u32 { 1 }\n"
    );
    assert_eq!(
        fs::read_to_string(fixture.repo_dir.join("src/b.rs")).expect("primary src/b.rs"),
        "pub fn b() -> u32 { 2 }\n"
    );

    // Verify `WorktreeManifest` ownership and `JobIntegrationIntent` state on disk.
    let manifest_path = host_handle
        .work_area_dir()
        .join("manifests")
        .join(format!("{expected_wt_id}.json"));
    let manifest_raw = fs::read_to_string(&manifest_path).expect("read final worktree manifest");
    let manifest: threadlane_workers::WorktreeManifest =
        serde_json::from_str(&manifest_raw).expect("parse final worktree manifest");
    assert_eq!(manifest.kind, WorktreeKind::RunIntegration);
    assert_eq!(manifest.worktree_id, expected_wt_id);
    assert_eq!(manifest.run_id, expected_run_id);
    assert_eq!(manifest.base_commit, fixture.base_commit);

    let intent =
        inspect_job_integration_intent_readonly(host_handle.work_area_dir(), &expected_wt_id)
            .expect("inspect job integration intent")
            .expect("job integration intent exists");
    assert_eq!(intent.state, JobIntegrationIntentState::Completed);
    assert_eq!(
        intent.final_integrated_target_commit.as_deref(),
        Some(agg_outcome.final_integrated_target_commit.as_str())
    );
    assert_eq!(intent.tasks.len(), 2);

    // Verify two additional aggregate `IntegrationReceipt` records exist with `job_aggregation.is_some()`
    // and that the original per-worker receipts on task candidates and disk are completely unchanged.
    let expected_agg_rcp_a = derive_job_aggregation_receipt_id(
        &job.job_id,
        &expected_run_id,
        &expected_wt_id,
        "task-agg-a",
        &int_out_a.review_receipt_id,
    );
    let expected_agg_rcp_b = derive_job_aggregation_receipt_id(
        &job.job_id,
        &expected_run_id,
        &expected_wt_id,
        "task-agg-b",
        &int_out_b.review_receipt_id,
    );
    assert_eq!(intent.tasks[0].planned_receipt_id, expected_agg_rcp_a);
    assert_eq!(intent.tasks[1].planned_receipt_id, expected_agg_rcp_b);

    let agg_rcp_a = load_integration_receipt(host_handle.work_area_dir(), &expected_agg_rcp_a)
        .expect("load agg rcp A");
    let agg_rcp_b = load_integration_receipt(host_handle.work_area_dir(), &expected_agg_rcp_b)
        .expect("load agg rcp B");
    assert!(agg_rcp_a.job_aggregation.is_some());
    assert!(agg_rcp_b.job_aggregation.is_some());
    let agg_meta_a = agg_rcp_a
        .job_aggregation
        .as_ref()
        .expect("job_aggregation A");
    let agg_meta_b = agg_rcp_b
        .job_aggregation
        .as_ref()
        .expect("job_aggregation B");
    assert_eq!(agg_meta_a.logical_task_id, "task-agg-a");
    assert_eq!(agg_meta_a.target_integration_run_id, expected_run_id);
    assert_eq!(agg_meta_a.batch_step_index, 0);
    assert_eq!(agg_meta_a.batch_total_tasks, 2);
    assert_eq!(agg_meta_a.approved_base_commit, fixture.base_commit);
    assert_eq!(agg_meta_b.logical_task_id, "task-agg-b");
    assert_eq!(agg_meta_b.target_integration_run_id, expected_run_id);
    assert_eq!(agg_meta_b.batch_step_index, 1);
    assert_eq!(agg_meta_b.batch_total_tasks, 2);
    assert_eq!(agg_meta_b.approved_base_commit, fixture.base_commit);

    assert_eq!(
        agg_rcp_a.previous_integration_head_commit, fixture.base_commit,
        "first step integrates onto approved base_commit"
    );
    assert_eq!(
        agg_rcp_b.previous_integration_head_commit, agg_rcp_a.integrated_target_commit,
        "second step integrates onto first step's target commit"
    );
    assert_eq!(
        agg_rcp_b.integrated_target_commit,
        agg_outcome.final_integrated_target_commit
    );

    // Original per-worker receipts on disk and in the workflow ledger are untouched.
    assert_eq!(
        load_integration_receipt(host_handle.work_area_dir(), &orig_receipt_id_a)
            .expect("reload orig receipt A"),
        orig_receipt_a_disk
    );
    assert_eq!(
        load_integration_receipt(host_handle.work_area_dir(), &orig_receipt_id_b)
            .expect("reload orig receipt B"),
        orig_receipt_b_disk
    );
    let ledger_after = fixture
        .workflow_store
        .read_binding_ledger(BINDING_A)
        .expect("read ledger after aggregation");
    let job_after = ledger_after
        .jobs
        .iter()
        .find(|j| j.job_id == job.job_id)
        .expect("job after aggregation");
    for (task_id, expected_orig_rcp_id) in [
        ("task-agg-a", &orig_receipt_id_a),
        ("task-agg-b", &orig_receipt_id_b),
    ] {
        let task_rec = job_after
            .tasks
            .iter()
            .find(|t| t.task_id == task_id)
            .expect("find task");
        assert_eq!(task_rec.stage, CoworkerTaskStage::Integrated);
        let cand_id = task_rec
            .active_candidate_id
            .as_deref()
            .expect("active_candidate_id");
        let cand = task_rec
            .candidates
            .iter()
            .find(|c| c.candidate_id == cand_id)
            .expect("find candidate");
        assert_eq!(
            cand.integration_receipt_id.as_deref(),
            Some(expected_orig_rcp_id.as_str()),
            "task candidate must preserve original per-worker integration_receipt_id"
        );
    }

    // Verify private (`0600`) persisted `JobIntegrationReport` artifact on disk.
    let report_meta =
        fs::symlink_metadata(&agg_outcome.report_artifact_path).expect("stat report_artifact_path");
    assert_eq!(report_meta.permissions().mode() & 0o777, 0o600);
    let persisted_report = read_persisted_job_integration_report(
        &fixture.plans_dir,
        &agg_outcome.aggregation_effect_id,
    )
    .expect("read_persisted_job_integration_report")
    .expect("persisted job integration report exists");
    assert_eq!(persisted_report, agg_outcome.job_integration_report);

    // 4. Drop session, reopen controller session, and re-run `aggregate_integrated_job`:
    // must reuse the existing verified receipt chain and `RunIntegration` worktree idempotently
    // without creating a new Git commit.
    drop(session);

    let reopened_session = fixture
        .controller
        .open_owned_session(BINDING_A, THREAD_UUID_A, &job.job_id, 1_700_005_129_000)
        .expect("reopen owner session");
    let agg_reopen = completion_service
        .aggregate_integrated_job(
            reopened_session.guard(),
            &CoworkerJobCompletionRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                job_id: job.job_id.clone(),
                now_ms: 1_700_005_130_000,
            },
        )
        .await
        .expect("re-run aggregate_integrated_job after reopen");
    assert!(agg_reopen.reused_existing_receipts);
    assert_eq!(
        agg_reopen.final_integrated_target_commit,
        agg_outcome.final_integrated_target_commit
    );
    assert_eq!(
        agg_reopen.final_integrated_tree_sha,
        agg_outcome.final_integrated_tree_sha
    );
    assert_eq!(
        resolve_exact_commit_sha(final_wt_path, "HEAD", DEFAULT_GIT_COMMAND_TIMEOUT_MS)
            .expect("final wt HEAD after reopen"),
        agg_outcome.final_integrated_target_commit
    );

    drop(reopened_session);
    host_handle.shutdown().await.expect("shutdown host_handle");
}

#[tokio::test]
async fn job_completion_executes_frozen_candidate_checks_assembles_candidate_and_runs_supervised_closure_once(
) {
    let fixture = setup_fixture();
    let tmp_root = fs::canonicalize(fixture._tmp.path()).expect("canonicalize tmp root");
    let integration_service = CoworkerTaskIntegrationService::new(fixture.controller.clone());
    let completion_service =
        CoworkerJobCompletionService::from_controller(fixture.controller.clone())
            .expect("construct CoworkerJobCompletionService from controller");

    let closure_count_file = tmp_root.join("agy_closure_count.txt");
    let closure_resp_file = tmp_root.join("agy_closure_response.json");
    let run_cargo_count_file = tmp_root.join("run_integration_cargo_count.txt");
    let cargo_invocation_log = tmp_root.join("cargo_invocation.log");

    // Configure `fake_cargo_exe` so that when it runs inside the final `RunIntegration` worktree
    // (`wt-run-*`), it verifies both `src/a.rs` and `src/b.rs` are present with their integrated
    // contents and increments `run_integration_cargo_count.txt`.
    let cargo_script = format!(
        r#"#!/usr/bin/env bash
set -euo pipefail
echo "cargo $*" >> "{log}"
if [[ "$PWD" == *"wt-run-"* ]]; then
  grep -q "100" src/a.rs
  grep -q "200" src/b.rs
  grep -q "3" src/c.rs
  COUNT_FILE="{run_count}"
  COUNT=0
  if [ -f "$COUNT_FILE" ]; then
    COUNT=$(cat "$COUNT_FILE")
  fi
  echo $((COUNT + 1)) > "$COUNT_FILE"
fi
echo "test result: ok. 3 passed; 0 failed"
exit 0
"#,
        log = cargo_invocation_log.display(),
        run_count = run_cargo_count_file.display(),
    );
    write_executable_script(&fixture.fake_cargo_exe, &cargo_script);

    // 1. First verify on a separate two-root job that a structured non-accepting Closure response
    // (even with positive-sounding prose in `summary`) returns `ClosureNotAccepted` and does not
    // transition the job to `Completed`.
    let na_tasks = vec![
        PlanTask {
            id: "task-na-a".to_string(),
            title: "Task NA A".to_string(),
            instruction: "CTRL_TASK_A update src/a.rs".to_string(),
            role: WorkflowRole::Coder,
            difficulty: Difficulty::Hard,
            depends_on: Vec::new(),
            allowed_write_paths: vec!["src/a.rs".to_string()],
        },
        PlanTask {
            id: "task-na-b".to_string(),
            title: "Task NA B".to_string(),
            instruction: "CTRL_TASK_B update src/b.rs".to_string(),
            role: WorkflowRole::Coder,
            difficulty: Difficulty::Hard,
            depends_on: Vec::new(),
            allowed_write_paths: vec!["src/b.rs".to_string()],
        },
    ];
    let mut na_ifaces = BTreeMap::new();
    na_ifaces.insert(
        "task-na-a".to_string(),
        TaskInterfaceOwnershipSpec {
            owned_interfaces: vec!["mod_a::api".to_string()],
            consumed_interfaces: Vec::new(),
        },
    );
    na_ifaces.insert(
        "task-na-b".to_string(),
        TaskInterfaceOwnershipSpec {
            owned_interfaces: vec!["mod_b::api".to_string()],
            consumed_interfaces: Vec::new(),
        },
    );
    let (na_job, na_approval_id) = create_planned_approved_and_frozen_job_with_strategy(
        &fixture,
        "brf_job_comp_non_accept",
        "plan-job-comp-non-accept",
        na_tasks,
        na_ifaces,
        vec!["src/a.rs".to_string(), "src/b.rs".to_string()],
        2,
        ExecutionBudgets::default(),
        ControllerRoleExecutionBudget::default_for_role(WorkflowRole::Coder, 4),
        IntegrationStrategy::MergeNoFf,
    )
    .await;

    let na_host = ApprovedHost::start(ApprovedHostConfig::new(
        &fixture.registry_dir,
        &fixture.plans_dir,
        &fixture.authority_dir,
        &fixture.sessions_dir,
        &na_approval_id,
    ))
    .await
    .expect("start ApprovedHost for na_job");

    let na_session = fixture
        .controller
        .open_owned_session(BINDING_A, THREAD_UUID_A, &na_job.job_id, 1_700_006_000_000)
        .expect("open na_session");
    let _ = run_verify_accept_and_integrate_two_roots(
        &fixture,
        &integration_service,
        &na_session,
        &na_job.job_id,
        "task-na-a",
        "task-na-b",
        1_700_006_001_000,
    )
    .await;

    fs::write(
        &closure_resp_file,
        r#"{"schema_version":1,"subject_digest":"__SUBJECT_DIGEST__","candidate_tree_sha":"__CANDIDATE_TREE_SHA__","outcome":"rejected","summary":"Everything looks accepted in prose, but structured outcome is rejected.","findings":["Cross-module boundary mismatch between src/a.rs and src/b.rs."]}"#,
    )
    .expect("write rejected agy_closure_response.json");

    // Heap-allocate nested futures to bound the poll frame of this stack-pinned test.
    let na_outcome = Box::pin(completion_service.complete_job(
        na_session.guard(),
        &CoworkerJobCompletionRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            job_id: na_job.job_id.clone(),
            now_ms: 1_700_006_020_000,
        },
    ))
    .await
    .expect("complete_job with rejected Closure verdict");
    assert_eq!(
        na_outcome.status,
        CoworkerJobCompletionStatus::ClosureNotAccepted
    );
    assert!(!na_outcome.is_completed());
    assert_ne!(na_outcome.job.phase, CoworkerJobPhase::Completed);
    assert_eq!(
        na_outcome
            .closure_verdict
            .as_ref()
            .expect("closure_verdict present")
            .kind,
        ClosureVerdictKind::Rejected
    );
    drop(na_session);
    na_host.shutdown().await.expect("shutdown na_host");
    fs::remove_file(&closure_resp_file).expect("remove rejected agy_closure_response.json");

    // 2. Now run the full happy-path completion flow on a two-root job.
    let tasks = vec![
        PlanTask {
            id: "task-comp-a".to_string(),
            title: "Task Comp A".to_string(),
            instruction: "CTRL_TASK_A update src/a.rs".to_string(),
            role: WorkflowRole::Coder,
            difficulty: Difficulty::Hard,
            depends_on: Vec::new(),
            allowed_write_paths: vec!["src/a.rs".to_string()],
        },
        PlanTask {
            id: "task-comp-b".to_string(),
            title: "Task Comp B".to_string(),
            instruction: "CTRL_TASK_B update src/b.rs".to_string(),
            role: WorkflowRole::Coder,
            difficulty: Difficulty::Hard,
            depends_on: Vec::new(),
            allowed_write_paths: vec!["src/b.rs".to_string()],
        },
    ];
    let mut ifaces = BTreeMap::new();
    ifaces.insert(
        "task-comp-a".to_string(),
        TaskInterfaceOwnershipSpec {
            owned_interfaces: vec!["mod_a::api".to_string()],
            consumed_interfaces: Vec::new(),
        },
    );
    ifaces.insert(
        "task-comp-b".to_string(),
        TaskInterfaceOwnershipSpec {
            owned_interfaces: vec!["mod_b::api".to_string()],
            consumed_interfaces: Vec::new(),
        },
    );
    let (job, approval_id) = create_planned_approved_and_frozen_job_with_strategy(
        &fixture,
        "brf_job_comp_accept",
        "plan-job-comp-accept",
        tasks,
        ifaces,
        vec!["src/a.rs".to_string(), "src/b.rs".to_string()],
        2,
        ExecutionBudgets::default(),
        ControllerRoleExecutionBudget::default_for_role(WorkflowRole::Coder, 4),
        IntegrationStrategy::MergeNoFf,
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
    .expect("start ApprovedHost for job");

    let session = fixture
        .controller
        .open_owned_session(BINDING_A, THREAD_UUID_A, &job.job_id, 1_700_006_100_000)
        .expect("open owner session");

    let _ = run_verify_accept_and_integrate_two_roots(
        &fixture,
        &integration_service,
        &session,
        &job.job_id,
        "task-comp-a",
        "task-comp-b",
        1_700_006_101_000,
    )
    .await;

    let closure_before = read_invocation_count(&closure_count_file);
    let run_cargo_before = read_invocation_count(&run_cargo_count_file);

    let comp_req = CoworkerJobCompletionRequest {
        binding_id: BINDING_A.to_string(),
        thread_uuid: THREAD_UUID_A.to_string(),
        job_id: job.job_id.clone(),
        now_ms: 1_700_006_120_000,
    };
    let outcome = Box::pin(completion_service.complete_job(session.guard(), &comp_req))
        .await
        .expect("complete_job happy path");

    assert_eq!(outcome.status, CoworkerJobCompletionStatus::Completed);
    assert!(outcome.is_completed());
    assert_eq!(outcome.job.phase, CoworkerJobPhase::Completed);
    assert!(outcome.failed_check_id.is_none());
    assert_eq!(
        read_invocation_count(&closure_count_file),
        closure_before + 1,
        "Closure must be invoked exactly once for the combined candidate"
    );
    assert_eq!(
        read_invocation_count(&run_cargo_count_file),
        run_cargo_before + 1,
        "frozen candidate check must execute once on the final RunIntegration worktree"
    );

    // Verify `CombinedCandidateIdentity`, `FinalCandidateSubject`, and `ClosureVerdict`.
    let combined_candidate = outcome
        .combined_candidate
        .as_ref()
        .expect("combined_candidate present");
    assert_eq!(
        combined_candidate.status,
        CombinedCandidateStatus::ClosureAccepted
    );
    assert_eq!(
        combined_candidate.final_tree_sha,
        outcome.aggregation.final_integrated_tree_sha
    );
    let final_subject = outcome
        .final_candidate_subject
        .as_ref()
        .expect("final_candidate_subject present");
    let closure_verdict = outcome
        .closure_verdict
        .as_ref()
        .expect("closure_verdict present");
    assert_eq!(closure_verdict.kind, ClosureVerdictKind::Accepted);
    closure_verdict
        .verify_against_subject(final_subject)
        .expect("closure_verdict must verify against FinalCandidateSubject");

    // Verify a tampered subject digest is rejected by `ClosureVerdict::verify_against_subject`.
    let mut tampered_subject = final_subject.clone();
    tampered_subject.subject_digest = "f".repeat(64);
    assert!(
        closure_verdict
            .verify_against_subject(&tampered_subject)
            .is_err(),
        "closure_verdict bound to wrong subject_digest must fail verification"
    );

    // Verify Closure role budget reservation and completion accounting.
    let runtime_after = fixture
        .policy_store
        .get_job_runtime_readonly(BINDING_A, &job.job_id)
        .expect("read runtime")
        .expect("runtime exists");
    let closure_usage = runtime_after
        .role_usage
        .get(&WorkflowRole::Closure)
        .expect("Closure role usage");
    assert_eq!(closure_usage.calls_reserved, 1);
    assert_eq!(closure_usage.calls_completed, 1);

    // Verify private (`0600`) persisted candidate check report on disk and context binding.
    assert_eq!(outcome.check_reports.len(), 1);
    let check_ref = &outcome.check_reports[0];
    assert_eq!(check_ref.check_id, "cargo_test");
    let check_meta = fs::symlink_metadata(&check_ref.report_path).expect("stat check report path");
    assert_eq!(check_meta.permissions().mode() & 0o777, 0o600);
    let persisted_check_report =
        read_persisted_candidate_check_report(&fixture.plans_dir, &check_ref.effect_id)
            .expect("read_persisted_candidate_check_report")
            .expect("persisted candidate check report exists");
    assert_eq!(persisted_check_report, check_ref.report);
    let frozen_check_spec = runtime_after
        .envelope
        .check_commands
        .get(&check_ref.check_id)
        .expect("frozen check command spec for cargo_test");
    persisted_check_report
        .verify_against_context(&outcome.check_subject, frozen_check_spec)
        .expect("candidate check report must verify against frozen spec and check subject");

    // Verify bounded advisory `closure_artifacts` on the completed runtime state and private full sidecar.
    let closure_outcome = outcome
        .closure_outcome
        .as_ref()
        .expect("closure_outcome present");
    assert_eq!(runtime_after.closure_artifacts.len(), 1);
    let closure_artifact = runtime_after
        .closure_artifacts
        .get(&closure_outcome.attempt.attempt_id)
        .expect("closure artifact keyed by attempt_id");
    assert_eq!(
        closure_artifact.attempt_id,
        closure_outcome.attempt.attempt_id
    );
    assert_eq!(
        closure_artifact.command_key,
        closure_outcome.attempt.command_key
    );
    assert_eq!(
        closure_artifact.subject_digest,
        final_subject.subject_digest
    );
    assert_eq!(
        closure_artifact.candidate_tree_sha,
        final_subject.candidate_tree_sha
    );
    assert!(!closure_artifact.fenced_from_promotion);
    assert!(closure_artifact.diagnostic.is_none());
    assert!(
        closure_artifact.artifact_ref.is_some(),
        "new Closure artifact record must store private full sidecar reference"
    );
    assert!(
        closure_artifact.execution_report.is_none(),
        "inline execution_report must be None when private full sidecar is used"
    );
    assert!(
        closure_artifact.advisory_response.is_none(),
        "inline advisory_response must be None when private full sidecar is used"
    );
    assert!(
        closure_artifact.response_error.is_none(),
        "inline response_error must be None when private full sidecar is used"
    );

    let loaded_closure_artifact = fixture
        .policy_store
        .load_closure_execution_artifact(
            BINDING_A,
            THREAD_UUID_A,
            &job.job_id,
            &closure_outcome.attempt.attempt_id,
        )
        .expect("load closure execution artifact sidecar");
    let loaded_closure_for_record = fixture
        .policy_store
        .load_closure_execution_artifact_for_record(
            BINDING_A,
            THREAD_UUID_A,
            &job.job_id,
            closure_artifact,
        )
        .expect("load closure execution artifact for record");
    assert_eq!(loaded_closure_artifact, loaded_closure_for_record);
    assert_eq!(
        loaded_closure_artifact.execution_report.status,
        WorkerRunStatus::Succeeded
    );
    assert_eq!(loaded_closure_artifact.execution_report.exit_code, Some(0));
    assert!(!loaded_closure_artifact.execution_report.cleanup_incomplete);
    assert!(loaded_closure_artifact.response_error.is_none());
    let closure_role_budget = runtime_after
        .envelope
        .role_budgets
        .get(&WorkflowRole::Closure)
        .expect("Closure frozen role budget");
    loaded_closure_artifact
        .advisory_response
        .as_ref()
        .expect("closure advisory_response present in sidecar")
        .validate_integrity(closure_role_budget.max_structured_response_bytes)
        .expect("closure advisory_response integrity");

    // 3. Re-invoke `complete_job` on the same session and after releasing/reopening the session:
    // must reuse the cached `Accepted` verdict and persisted candidate check report without a
    // second `Closure` model invocation, second check execution, or redundant ledger revision bump.
    let repeat_same_session = Box::pin(completion_service.complete_job(
        session.guard(),
        &CoworkerJobCompletionRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            job_id: job.job_id.clone(),
            now_ms: 1_700_006_130_000,
        },
    ))
    .await
    .expect("repeat complete_job on same session");
    assert_eq!(
        repeat_same_session.status,
        CoworkerJobCompletionStatus::Completed
    );
    assert!(
        repeat_same_session
            .closure_outcome
            .as_ref()
            .expect("closure_outcome")
            .reused_cached_verdict
    );
    assert_eq!(
        repeat_same_session.combined_candidate,
        outcome.combined_candidate
    );
    assert_eq!(
        repeat_same_session
            .combined_candidate
            .as_ref()
            .expect("repeat_same_session combined_candidate")
            .closure_reservation_id
            .as_deref(),
        Some(closure_outcome.attempt.attempt_id.as_str())
    );
    assert_eq!(
        repeat_same_session.job.job_revision, outcome.job.job_revision,
        "idempotent completed reopen must not bump job_revision"
    );
    assert_eq!(
        read_invocation_count(&closure_count_file),
        closure_before + 1,
        "repeat complete_job must not invoke Closure a second time"
    );
    assert_eq!(
        read_invocation_count(&run_cargo_count_file),
        run_cargo_before + 1,
        "repeat complete_job must not re-run candidate checks"
    );

    drop(session);

    let reopened_session = fixture
        .controller
        .open_owned_session(BINDING_A, THREAD_UUID_A, &job.job_id, 1_700_006_139_000)
        .expect("reopen session");
    let repeat_reopen = Box::pin(completion_service.complete_job(
        reopened_session.guard(),
        &CoworkerJobCompletionRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            job_id: job.job_id.clone(),
            now_ms: 1_700_006_140_000,
        },
    ))
    .await
    .expect("repeat complete_job after reopen");
    assert_eq!(repeat_reopen.status, CoworkerJobCompletionStatus::Completed);
    assert!(
        repeat_reopen
            .closure_outcome
            .as_ref()
            .expect("closure_outcome")
            .reused_cached_verdict
    );
    assert_eq!(repeat_reopen.combined_candidate, outcome.combined_candidate);
    assert_eq!(repeat_reopen.job.job_revision, outcome.job.job_revision);
    assert_eq!(
        read_invocation_count(&closure_count_file),
        closure_before + 1
    );
    assert_eq!(
        read_invocation_count(&run_cargo_count_file),
        run_cargo_before + 1
    );

    drop(reopened_session);

    // 4. Convert the accepted compact sidecar record to a valid historical inline
    // `ClosureExecutionArtifactRecord` (removing `artifact_ref` and the sidecar file in this
    // disposable fixture), then reopen and call `complete_job`: must succeed as `Completed`
    // with the exact same subject, receipt chain, and combined candidate, zero new model/check
    // budget or invocations, and without rewriting the historical inline record to sidecar format.
    let attempt_id = closure_outcome.attempt.attempt_id.clone();
    let sidecar_path = fixture
        .policy_store
        .closure_artifact_path_for_attempt(BINDING_A, &job.job_id, &attempt_id)
        .expect("closure sidecar path");
    fs::remove_file(&sidecar_path).expect("remove sidecar for legacy inline conversion");
    fixture
        .policy_store
        .mutate_job_runtime_for_operator(BINDING_A, &job.job_id, |state| {
            state.closure_artifacts.insert(
                attempt_id.clone(),
                ClosureExecutionArtifactRecord {
                    attempt_id: attempt_id.clone(),
                    command_key: loaded_closure_artifact.command_key.clone(),
                    subject_digest: loaded_closure_artifact.subject_digest.clone(),
                    candidate_tree_sha: loaded_closure_artifact.candidate_tree_sha.clone(),
                    artifact_ref: None,
                    execution_report: Some(loaded_closure_artifact.execution_report.clone()),
                    advisory_response: loaded_closure_artifact.advisory_response.clone(),
                    response_error: None,
                    observed_model: loaded_closure_artifact.observed_model.clone(),
                    observed_effort: loaded_closure_artifact.observed_effort.clone(),
                    fenced_from_promotion: false,
                    diagnostic: None,
                    recorded_at_ms: loaded_closure_artifact.recorded_at_ms,
                },
            );
            Ok(())
        })
        .expect("write valid legacy inline record");

    let legacy_session = fixture
        .controller
        .open_owned_session(BINDING_A, THREAD_UUID_A, &job.job_id, 1_700_006_149_000)
        .expect("reopen session for legacy inline closure artifact");
    let legacy_reopen = Box::pin(completion_service.complete_job(
        legacy_session.guard(),
        &CoworkerJobCompletionRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            job_id: job.job_id.clone(),
            now_ms: 1_700_006_150_000,
        },
    ))
    .await
    .expect("complete_job after converting accepted record to valid legacy inline shape");
    assert_eq!(legacy_reopen.status, CoworkerJobCompletionStatus::Completed);
    assert!(
        legacy_reopen
            .closure_outcome
            .as_ref()
            .expect("closure_outcome")
            .reused_cached_verdict
    );
    assert_eq!(
        legacy_reopen.final_candidate_subject,
        outcome.final_candidate_subject
    );
    assert!(
        !outcome
            .aggregation
            .job_integration_report
            .reused_existing_receipts
    );
    assert!(
        legacy_reopen
            .aggregation
            .job_integration_report
            .reused_existing_receipts
    );
    let mut expected_cached_report = outcome.aggregation.job_integration_report.clone();
    expected_cached_report.reused_existing_receipts = true;
    assert_eq!(
        legacy_reopen.aggregation.job_integration_report,
        expected_cached_report
    );
    assert_eq!(legacy_reopen.combined_candidate, outcome.combined_candidate);
    assert_eq!(legacy_reopen.job.job_revision, outcome.job.job_revision);
    assert_eq!(
        read_invocation_count(&closure_count_file),
        closure_before + 1,
        "reopening valid legacy inline record must not invoke Closure"
    );
    assert_eq!(
        read_invocation_count(&run_cargo_count_file),
        run_cargo_before + 1,
        "reopening valid legacy inline record must not re-run candidate checks"
    );
    assert!(
        !sidecar_path.exists(),
        "reopening valid legacy inline record must not rewrite it to a sidecar file"
    );
    let runtime_after_legacy = fixture
        .policy_store
        .get_job_runtime_readonly(BINDING_A, &job.job_id)
        .expect("read runtime after legacy reopen")
        .expect("runtime exists");
    let closure_usage_after_legacy = runtime_after_legacy
        .role_usage
        .get(&WorkflowRole::Closure)
        .expect("Closure role usage");
    assert_eq!(closure_usage_after_legacy.calls_reserved, 1);
    assert_eq!(closure_usage_after_legacy.calls_completed, 1);
    let legacy_record_after = runtime_after_legacy
        .closure_artifacts
        .get(&attempt_id)
        .expect("legacy inline closure artifact record");
    assert!(legacy_record_after.artifact_ref.is_none());
    assert!(legacy_record_after.execution_report.is_some());

    drop(legacy_session);
    host_handle.shutdown().await.expect("shutdown host_handle");
}

#[tokio::test]
async fn job_completion_blocks_closure_on_failed_candidate_check_and_fences_holds_stops_and_corrupt_post_intent_artifacts(
) {
    let fixture = setup_fixture();
    let tmp_root = fs::canonicalize(fixture._tmp.path()).expect("canonicalize tmp root");
    let integration_service = CoworkerTaskIntegrationService::new(fixture.controller.clone());
    let completion_service =
        CoworkerJobCompletionService::from_controller(fixture.controller.clone())
            .expect("construct CoworkerJobCompletionService from controller");
    let closure_count_file = tmp_root.join("agy_closure_count.txt");
    let cargo_invocation_log = tmp_root.join("cargo_invocation.log");

    // 1. Job 1: Per-task checks pass on `wt-task-*`, but the combined candidate check fails on
    // the final `RunIntegration` worktree (`wt-run-*`). Verify `complete_job` returns
    // `BlockedByFailedChecks`, persists the failed check report, does NOT assemble a combined
    // candidate, and never invokes `Closure`.
    let fail_cargo_script = format!(
        r#"#!/usr/bin/env bash
set -euo pipefail
echo "cargo $*" >> "{log}"
if [[ "$PWD" == *"wt-run-"* ]]; then
  echo "combined integration test failure: src/a.rs and src/b.rs incompatible" >&2
  exit 1
fi
echo "test result: ok. 1 passed; 0 failed"
exit 0
"#,
        log = cargo_invocation_log.display(),
    );
    write_executable_script(&fixture.fake_cargo_exe, &fail_cargo_script);

    let fail_tasks = vec![
        PlanTask {
            id: "task-chkfail-a".to_string(),
            title: "Task ChkFail A".to_string(),
            instruction: "CTRL_TASK_A update src/a.rs".to_string(),
            role: WorkflowRole::Coder,
            difficulty: Difficulty::Hard,
            depends_on: Vec::new(),
            allowed_write_paths: vec!["src/a.rs".to_string()],
        },
        PlanTask {
            id: "task-chkfail-b".to_string(),
            title: "Task ChkFail B".to_string(),
            instruction: "CTRL_TASK_B update src/b.rs".to_string(),
            role: WorkflowRole::Coder,
            difficulty: Difficulty::Hard,
            depends_on: Vec::new(),
            allowed_write_paths: vec!["src/b.rs".to_string()],
        },
    ];
    let mut fail_ifaces = BTreeMap::new();
    fail_ifaces.insert(
        "task-chkfail-a".to_string(),
        TaskInterfaceOwnershipSpec {
            owned_interfaces: vec!["mod_a::api".to_string()],
            consumed_interfaces: Vec::new(),
        },
    );
    fail_ifaces.insert(
        "task-chkfail-b".to_string(),
        TaskInterfaceOwnershipSpec {
            owned_interfaces: vec!["mod_b::api".to_string()],
            consumed_interfaces: Vec::new(),
        },
    );
    let (fail_job, fail_approval_id) = create_planned_approved_and_frozen_job_with_strategy(
        &fixture,
        "brf_job_comp_chkfail",
        "plan-job-comp-chkfail",
        fail_tasks,
        fail_ifaces,
        vec!["src/a.rs".to_string(), "src/b.rs".to_string()],
        2,
        ExecutionBudgets::default(),
        ControllerRoleExecutionBudget::default_for_role(WorkflowRole::Coder, 4),
        IntegrationStrategy::MergeNoFf,
    )
    .await;

    let fail_host = ApprovedHost::start(ApprovedHostConfig::new(
        &fixture.registry_dir,
        &fixture.plans_dir,
        &fixture.authority_dir,
        &fixture.sessions_dir,
        &fail_approval_id,
    ))
    .await
    .expect("start ApprovedHost for fail_job");

    let fail_session = fixture
        .controller
        .open_owned_session(
            BINDING_A,
            THREAD_UUID_A,
            &fail_job.job_id,
            1_700_007_000_000,
        )
        .expect("open fail_session");

    let _ = run_verify_accept_and_integrate_two_roots(
        &fixture,
        &integration_service,
        &fail_session,
        &fail_job.job_id,
        "task-chkfail-a",
        "task-chkfail-b",
        1_700_007_001_000,
    )
    .await;

    let closure_before_fail = read_invocation_count(&closure_count_file);
    // Heap-allocate nested futures to bound the poll frame of this stack-pinned test.
    let fail_outcome = Box::pin(completion_service.complete_job(
        fail_session.guard(),
        &CoworkerJobCompletionRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            job_id: fail_job.job_id.clone(),
            now_ms: 1_700_007_020_000,
        },
    ))
    .await
    .expect("complete_job when combined candidate check fails");

    assert_eq!(
        fail_outcome.status,
        CoworkerJobCompletionStatus::BlockedByFailedChecks
    );
    assert!(!fail_outcome.is_completed());
    assert_eq!(fail_outcome.failed_check_id.as_deref(), Some("cargo_test"));
    assert!(fail_outcome.combined_candidate.is_none());
    assert!(fail_outcome.final_candidate_subject.is_none());
    assert!(fail_outcome.closure_verdict.is_none());
    assert_ne!(fail_outcome.job.phase, CoworkerJobPhase::Completed);
    assert_eq!(
        read_invocation_count(&closure_count_file),
        closure_before_fail,
        "failed candidate check must block Closure invocation"
    );
    assert_eq!(fail_outcome.check_reports.len(), 1);
    assert_eq!(
        fail_outcome.check_reports[0].report.status,
        FrozenCheckExecutionStatus::NonZeroExit
    );
    let persisted_failed_report = read_persisted_candidate_check_report(
        &fixture.plans_dir,
        &fail_outcome.check_reports[0].effect_id,
    )
    .expect("read persisted failed candidate check report")
    .expect("persisted failed candidate check report exists");
    assert_eq!(
        persisted_failed_report.status,
        FrozenCheckExecutionStatus::NonZeroExit
    );
    drop(fail_session);
    fail_host.shutdown().await.expect("shutdown fail_host");

    // 2. Job 2: Restore passing `fake_cargo_exe`. Verify pre-effect fencing on active workflow
    // hold and `stop_requested`, then verify post-intent artifact corruption transitions
    // `JobAggregation` to `EffectUnknown` and fails closed with
    // `AmbiguousWakeupPendingReconciliation` while preserving the existing `RunIntegration`
    // worktree on disk without calling `Closure`.
    write_executable_script(&fixture.fake_cargo_exe, "#!/bin/sh\nexit 0\n");

    let fence_tasks = vec![
        PlanTask {
            id: "task-fence-a".to_string(),
            title: "Task Fence A".to_string(),
            instruction: "CTRL_TASK_A update src/a.rs".to_string(),
            role: WorkflowRole::Coder,
            difficulty: Difficulty::Hard,
            depends_on: Vec::new(),
            allowed_write_paths: vec!["src/a.rs".to_string()],
        },
        PlanTask {
            id: "task-fence-b".to_string(),
            title: "Task Fence B".to_string(),
            instruction: "CTRL_TASK_B update src/b.rs".to_string(),
            role: WorkflowRole::Coder,
            difficulty: Difficulty::Hard,
            depends_on: Vec::new(),
            allowed_write_paths: vec!["src/b.rs".to_string()],
        },
    ];
    let mut fence_ifaces = BTreeMap::new();
    fence_ifaces.insert(
        "task-fence-a".to_string(),
        TaskInterfaceOwnershipSpec {
            owned_interfaces: vec!["mod_a::api".to_string()],
            consumed_interfaces: Vec::new(),
        },
    );
    fence_ifaces.insert(
        "task-fence-b".to_string(),
        TaskInterfaceOwnershipSpec {
            owned_interfaces: vec!["mod_b::api".to_string()],
            consumed_interfaces: Vec::new(),
        },
    );
    let (fence_job, fence_approval_id) = create_planned_approved_and_frozen_job_with_strategy(
        &fixture,
        "brf_job_comp_fence",
        "plan-job-comp-fence",
        fence_tasks,
        fence_ifaces,
        vec!["src/a.rs".to_string(), "src/b.rs".to_string()],
        2,
        ExecutionBudgets::default(),
        ControllerRoleExecutionBudget::default_for_role(WorkflowRole::Coder, 4),
        IntegrationStrategy::MergeNoFf,
    )
    .await;

    let fence_host = ApprovedHost::start(ApprovedHostConfig::new(
        &fixture.registry_dir,
        &fixture.plans_dir,
        &fixture.authority_dir,
        &fixture.sessions_dir,
        &fence_approval_id,
    ))
    .await
    .expect("start ApprovedHost for fence_job");

    let fence_session = fixture
        .controller
        .open_owned_session(
            BINDING_A,
            THREAD_UUID_A,
            &fence_job.job_id,
            1_700_007_100_000,
        )
        .expect("open fence_session");

    let (fence_out_a, fence_out_b) = run_verify_accept_and_integrate_two_roots(
        &fixture,
        &integration_service,
        &fence_session,
        &fence_job.job_id,
        "task-fence-a",
        "task-fence-b",
        1_700_007_101_000,
    )
    .await;

    let fence_runtime = fixture
        .policy_store
        .get_job_runtime_readonly(BINDING_A, &fence_job.job_id)
        .expect("read fence runtime")
        .expect("fence runtime exists");
    let (_, _, expected_fence_wt_id, _) = derive_final_job_integration_identifiers(
        &fence_job.job_id,
        &fence_approval_id,
        &fence_runtime.envelope.approval_subject_digest,
    );
    let expected_fence_wt_path = fence_host
        .work_area_dir()
        .join("checkouts")
        .join("runs")
        .join(&expected_fence_wt_id);

    // 2a. Apply an active workflow hold before any job completion effect.
    let job_before_hold = fixture
        .workflow_store
        .read_binding_ledger(BINDING_A)
        .expect("read ledger")
        .jobs
        .into_iter()
        .find(|j| j.job_id == fence_job.job_id)
        .expect("find fence_job");
    let (held_job, hold_rec) = fixture
        .workflow_store
        .apply_workflow_hold(&ApplyWorkflowHoldRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            job_id: fence_job.job_id.clone(),
            expected_job_revision: job_before_hold.job_revision,
            expected_ledger_revision: None,
            request_key: "req_hold_before_job_completion".to_string(),
            reason: WorkflowHoldReason::ConversationClarificationPending,
            detail: "Hold before final job completion.".to_string(),
            directly_affected_task_ids: vec!["task-fence-a".to_string()],
            affected_write_paths: vec!["src/a.rs".to_string()],
            affected_interfaces: vec!["mod_a::api".to_string()],
            holds_entire_job: true,
        })
        .expect("apply workflow hold");
    let held_task_a = held_job
        .tasks
        .iter()
        .find(|t| t.task_id == "task-fence-a")
        .expect("find held task-fence-a");
    assert_eq!(held_task_a.stage, CoworkerTaskStage::Held);
    assert_eq!(
        held_task_a.stage_before_hold,
        Some(CoworkerTaskStage::Integrated)
    );

    let hold_err = Box::pin(completion_service.complete_job(
        fence_session.guard(),
        &CoworkerJobCompletionRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            job_id: fence_job.job_id.clone(),
            now_ms: 1_700_007_120_000,
        },
    ))
    .await
    .expect_err("active workflow hold must refuse complete_job pre-effect");
    assert_eq!(hold_err.code(), NativeErrorCode::ValidationFailed.as_str());
    assert!(
        !expected_fence_wt_path.exists(),
        "pre-effect hold refusal must not create final RunIntegration checkout"
    );

    // Resolve hold, then verify that previously Integrated tasks (`task-fence-a` and `task-fence-b`)
    // are restored to `CoworkerTaskStage::Integrated` with their retained accepted candidate and
    // integration receipt linkage intact and without any new review or Git effects.
    let (resumed_job, _) = fixture
        .workflow_store
        .resolve_workflow_hold(&ResolveWorkflowHoldRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            job_id: fence_job.job_id.clone(),
            expected_job_revision: held_job.job_revision,
            expected_ledger_revision: None,
            hold_id: hold_rec.hold_id,
            resolution_note: "Release hold for stop_requested test.".to_string(),
        })
        .expect("resolve workflow hold");
    assert_eq!(resumed_job.phase, CoworkerJobPhase::Executing);
    for (tid, orig_int_out) in [
        ("task-fence-a", &fence_out_a),
        ("task-fence-b", &fence_out_b),
    ] {
        let resumed_task = resumed_job
            .tasks
            .iter()
            .find(|t| t.task_id == tid)
            .expect("find resumed task");
        assert_eq!(resumed_task.stage, CoworkerTaskStage::Integrated);
        assert_eq!(resumed_task.stage_before_hold, None);
        assert!(resumed_task.active_hold_ids.is_empty());
        let acc_cid = resumed_task
            .accepted_candidate_id
            .as_deref()
            .expect("retained accepted_candidate_id");
        let acc_cand = resumed_task
            .candidates
            .iter()
            .find(|c| c.candidate_id == acc_cid)
            .expect("retained accepted candidate");
        assert_eq!(
            acc_cand.integration_receipt_id.as_deref(),
            Some(orig_int_out.integration_receipt.receipt_id.as_str())
        );
    }

    fixture
        .policy_store
        .mutate_job_runtime_for_operator(BINDING_A, &fence_job.job_id, |rt| {
            rt.stop_requested = true;
            Ok(())
        })
        .expect("set stop_requested = true");

    let stop_err = Box::pin(completion_service.complete_job(
        fence_session.guard(),
        &CoworkerJobCompletionRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            job_id: fence_job.job_id.clone(),
            now_ms: 1_700_007_122_000,
        },
    ))
    .await
    .expect_err("stop_requested must refuse complete_job pre-effect");
    assert_eq!(stop_err.code(), NativeErrorCode::ValidationFailed.as_str());
    assert!(!expected_fence_wt_path.exists());

    // Clear `stop_requested` and run `aggregate_integrated_job` so the real `RunIntegration`
    // worktree, `JobIntegrationIntent`, and `JobIntegrationReport` exist on disk.
    fixture
        .policy_store
        .mutate_job_runtime_for_operator(BINDING_A, &fence_job.job_id, |rt| {
            rt.stop_requested = false;
            Ok(())
        })
        .expect("clear stop_requested");

    let agg_fence = Box::pin(completion_service.aggregate_integrated_job(
        fence_session.guard(),
        &CoworkerJobCompletionRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            job_id: fence_job.job_id.clone(),
            now_ms: 1_700_007_125_000,
        },
    ))
    .await
    .expect("aggregate_integrated_job on fence_job");
    assert!(expected_fence_wt_path.exists());

    // 2b. Corrupt the persisted `JobIntegrationReport` artifact on disk after the durable intent
    // and `EffectConfirmed` record exist. Verify `complete_job` fails closed as `EffectUnknown`
    // (`AmbiguousWakeupPendingReconciliation`), preserves the existing `RunIntegration` worktree
    // and HEAD commit without recreating or overwriting it, does NOT invoke `Closure`, and blocks
    // subsequent retries.
    fs::write(&agg_fence.report_artifact_path, b"{corrupted_report_json")
        .expect("corrupt persisted JobIntegrationReport on disk");

    let closure_before_corrupt = read_invocation_count(&closure_count_file);
    let corrupt_err = Box::pin(completion_service
        .complete_job(
            fence_session.guard(),
            &CoworkerJobCompletionRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                job_id: fence_job.job_id.clone(),
                now_ms: 1_700_007_130_000,
            },
        ))
        .await
        .expect_err("corrupted persisted JobIntegrationReport must fail closed with AmbiguousWakeupPendingReconciliation");
    assert_eq!(
        corrupt_err.code(),
        NativeErrorCode::AmbiguousWakeupPendingReconciliation.as_str()
    );
    assert_eq!(
        read_invocation_count(&closure_count_file),
        closure_before_corrupt,
        "corrupted post-intent artifact must never invoke Closure"
    );
    assert_eq!(
        resolve_exact_commit_sha(
            &agg_fence.integration_checkout_path,
            "HEAD",
            DEFAULT_GIT_COMMAND_TIMEOUT_MS
        )
        .expect("final wt HEAD preserved after EffectUnknown"),
        agg_fence.final_integrated_target_commit
    );

    let runtime_unknown = fixture
        .policy_store
        .get_job_runtime_readonly(BINDING_A, &fence_job.job_id)
        .expect("read runtime after corrupt report")
        .expect("runtime exists");
    assert_eq!(
        runtime_unknown
            .pending_effects
            .get(&agg_fence.aggregation_effect_id)
            .expect("JobAggregation effect")
            .status,
        PendingControllerEffectStatus::EffectUnknown
    );

    let retry_unknown_err = Box::pin(completion_service.complete_job(
        fence_session.guard(),
        &CoworkerJobCompletionRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            job_id: fence_job.job_id.clone(),
            now_ms: 1_700_007_135_000,
        },
    ))
    .await
    .expect_err("repeat complete_job while JobAggregation is EffectUnknown must remain blocked");
    assert_eq!(
        retry_unknown_err.code(),
        NativeErrorCode::AmbiguousWakeupPendingReconciliation.as_str()
    );

    drop(fence_session);
    fence_host.shutdown().await.expect("shutdown fence_host");
}

fn build_large_rust_source(line_count: u32, fn_name: &str, return_value: u32) -> String {
    let mut source = String::with_capacity((line_count as usize) * 68 + 64);
    for idx in 0..line_count {
        source.push_str(&format!(
            "pub const ITEM_{idx:06}: &str = \"0123456789abcdef0123456789ab\";\n"
        ));
    }
    source.push_str(&format!("pub fn {fn_name}() -> u32 {{ {return_value} }}\n"));
    source
}

fn single_coder_task_interfaces(
    task_id: &str,
    iface: &str,
) -> BTreeMap<String, TaskInterfaceOwnershipSpec> {
    let mut map = BTreeMap::new();
    map.insert(
        task_id.to_string(),
        TaskInterfaceOwnershipSpec {
            owned_interfaces: vec![iface.to_string()],
            consumed_interfaces: Vec::new(),
        },
    );
    map
}

fn install_large_diff_agy_cases(
    fixture: &ControllerTestFixture,
    large_a_path: &Path,
    large_b_path: &Path,
    fenced_gate_path: &Path,
) {
    let orig_script = fs::read_to_string(&fixture.fake_agy_exe).expect("read fake-agy");
    let extra_cases = [
        "        *CTRL_LARGE_UNFENCED_A*)".to_string(),
        format!("            cp \"{}\" \"src/a.rs\"", large_a_path.display()),
        "            ;;".to_string(),
        "        *CTRL_LARGE_FENCED_B*)".to_string(),
        format!("            cp \"{}\" \"src/b.rs\"", large_b_path.display()),
        format!(
            "            while [ -f \"{}\" ]; do sleep 0.02; done",
            fenced_gate_path.display(),
        ),
        "            ;;".to_string(),
        "        *CTRL_TASK_A*)".to_string(),
    ]
    .join("\n");
    let updated_script = orig_script.replacen("        *CTRL_TASK_A*)", &extra_cases, 1);
    assert_ne!(updated_script, orig_script);
    write_executable_script(&fixture.fake_agy_exe, &updated_script);
}

#[tokio::test]
async fn controller_reconciles_over_64kib_tracked_diffs_unfenced_fenced_and_rejects_overflow() {
    let fixture = setup_fixture();
    let root_dir = fixture.agy_count_file.parent().expect("fixture root dir");
    let large_a_path = root_dir.join("large_src_a.rs");
    let large_b_path = root_dir.join("large_src_b.rs");
    let fenced_gate_path = root_dir.join("fenced_task_b.gate");

    fs::write(&large_a_path, build_large_rust_source(3_000, "a", 100))
        .expect("write >64KiB src/a.rs payload");
    fs::write(&large_b_path, build_large_rust_source(3_000, "b", 200))
        .expect("write >64KiB src/b.rs payload");
    install_large_diff_agy_cases(&fixture, &large_a_path, &large_b_path, &fenced_gate_path);

    let host_client = ApprovedHostClient::new(
        Arc::clone(&fixture.authority),
        &fixture.sessions_dir,
        BINDING_A,
        THREAD_UUID_A,
    )
    .expect("create ApprovedHostClient");

    // 1. Unfenced >64 KiB (~189 KiB) tracked diff reconciles to ReadyForReview with exact raw-byte digest.
    let (unfenced_job, unfenced_approval_id) = Box::pin(create_planned_approved_and_frozen_job(
        &fixture,
        "brf_large_unfenced",
        "plan-large-unfenced",
        vec![PlanTask {
            id: "task-large-a".to_string(),
            title: "Large Unfenced A".to_string(),
            instruction: "CTRL_LARGE_UNFENCED_A".to_string(),
            role: WorkflowRole::Coder,
            difficulty: Difficulty::Hard,
            depends_on: Vec::new(),
            allowed_write_paths: vec!["src/a.rs".to_string()],
        }],
        single_coder_task_interfaces("task-large-a", "iface_large_a"),
        vec!["src/a.rs".to_string()],
        1,
        ExecutionBudgets::default(),
        ControllerRoleExecutionBudget::default_for_role(WorkflowRole::Coder, 4),
    ))
    .await;

    let unfenced_host = ApprovedHost::start(ApprovedHostConfig::new(
        &fixture.registry_dir,
        &fixture.plans_dir,
        &fixture.authority_dir,
        &fixture.sessions_dir,
        &unfenced_approval_id,
    ))
    .await
    .expect("start unfenced ApprovedHost");
    let unfenced_session = fixture
        .controller
        .open_owned_session(BINDING_A, THREAD_UUID_A, &unfenced_job.job_id, 600_000)
        .expect("open unfenced session");

    let deadline = Instant::now() + Duration::from_secs(10);
    let mut step_ts = 600_100u64;
    let unfenced_rep = loop {
        let rep = Box::pin(unfenced_session.step(step_ts))
            .await
            .expect("unfenced >64KiB controller step");
        step_ts += 100;
        let obs = rep
            .tasks
            .iter()
            .find(|t| t.task_id == "task-large-a")
            .expect("task-large-a observation");
        if obs.stage == CoworkerTaskStage::ReadyForReview {
            break rep;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for ReadyForReview on task-large-a"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    };
    assert_eq!(
        unfenced_rep.status,
        CoworkerControllerPhase1Status::AwaitingChecksOrReview
    );
    let obs_a = unfenced_rep
        .tasks
        .iter()
        .find(|t| t.task_id == "task-large-a")
        .expect("task-large-a observation");

    let snap_a = host_client
        .get_task(&unfenced_approval_id, "task-large-a")
        .await
        .expect("get task-large-a snapshot");
    let wt_a = PathBuf::from(
        snap_a
            .worker
            .as_ref()
            .expect("worker snapshot")
            .worktree
            .task_worktree_path
            .as_ref()
            .expect("task_worktree_path"),
    );
    let legacy_diff_a = run_sanitized_git(
        &wt_a,
        &["diff", "--binary", &fixture.base_commit],
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    );
    assert!(legacy_diff_a.is_err());

    let raw_diff_a = run_sanitized_git_bytes(
        &wt_a,
        &["diff", "--binary", &fixture.base_commit],
        DEFAULT_MAX_GIT_OUTPUT_BYTES,
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .expect("compute 4 MiB raw-byte diff for task-large-a");
    assert!(raw_diff_a.stdout_bytes.len() > 180_000);
    let expected_digest_a = sha256_hex(&raw_diff_a.stdout_bytes);
    let expected_art_id_a = deterministic_prefixed_id(
        "threadlane-native:coworker-coder-attempt-report:v1",
        "art",
        &[
            &unfenced_approval_id,
            "task-large-a",
            obs_a.run_id.as_deref().expect("run_id"),
            obs_a.worker_id.as_deref().expect("worker_id"),
            obs_a.task_worktree_id.as_deref().expect("task_worktree_id"),
            &expected_digest_a,
        ],
    );
    let ledger_a = fixture
        .workflow_store
        .read_binding_ledger(BINDING_A)
        .expect("read binding ledger after unfenced step");
    let task_rec_a = ledger_a
        .jobs
        .iter()
        .find(|j| j.job_id == unfenced_job.job_id)
        .expect("unfenced job")
        .tasks
        .iter()
        .find(|t| t.task_id == "task-large-a")
        .expect("task-large-a record");
    assert_eq!(
        task_rec_a.attempts[0].diff_digest.as_deref(),
        Some(expected_digest_a.as_str())
    );
    assert_eq!(
        task_rec_a.attempts[0].report_artifact_id.as_deref(),
        Some(expected_art_id_a.as_str())
    );
    assert_eq!(read_invocation_count(&fixture.agy_count_file), 1);
    drop(unfenced_session);
    unfenced_host
        .shutdown()
        .await
        .expect("shutdown unfenced_host");

    // 2. Fenced >64 KiB + raw non-UTF-8 tracked diff on an already-confirmed Running outbox
    // reconciles via reconcile_fenced_ready_for_review_snapshot, retaining verified runtime
    // artifacts against actual host proof while preserving the confirmed Running ledger without
    // promoting to ReadyForReview or double-charging budgets.
    fs::write(&fenced_gate_path, b"hold").expect("write fenced gate");
    let (fenced_job, fenced_approval_id) = Box::pin(create_planned_approved_and_frozen_job(
        &fixture,
        "brf_large_fenced",
        "plan-large-fenced",
        vec![PlanTask {
            id: "task-large-b".to_string(),
            title: "Large Fenced B".to_string(),
            instruction: "CTRL_LARGE_FENCED_B".to_string(),
            role: WorkflowRole::Coder,
            difficulty: Difficulty::Hard,
            depends_on: Vec::new(),
            allowed_write_paths: vec!["src/b.rs".to_string()],
        }],
        single_coder_task_interfaces("task-large-b", "iface_large_b"),
        vec!["src/b.rs".to_string()],
        1,
        ExecutionBudgets::default(),
        ControllerRoleExecutionBudget::default_for_role(WorkflowRole::Coder, 4),
    ))
    .await;

    let fenced_host = ApprovedHost::start(ApprovedHostConfig::new(
        &fixture.registry_dir,
        &fixture.plans_dir,
        &fixture.authority_dir,
        &fixture.sessions_dir,
        &fenced_approval_id,
    ))
    .await
    .expect("start fenced ApprovedHost");
    let fenced_session = fixture
        .controller
        .open_owned_session(BINDING_A, THREAD_UUID_A, &fenced_job.job_id, 610_000)
        .expect("open fenced session");

    let dispatch_rep = Box::pin(fenced_session.step(610_100))
        .await
        .expect("dispatch task-large-b");
    assert_eq!(
        dispatch_rep.dispatched_task_ids,
        vec!["task-large-b".to_string()]
    );
    let dispatched_job_rev_b = dispatch_rep.job_revision;

    fixture
        .policy_store
        .mutate_job_runtime_for_operator(BINDING_A, &fenced_job.job_id, |rt| {
            rt.stop_requested = true;
            Ok(())
        })
        .expect("set stop_requested while worker is gated");
    fs::remove_file(&fenced_gate_path).expect("remove fenced gate");

    let fenced_deadline = Instant::now() + Duration::from_secs(10);
    let snap_b = loop {
        let snap = host_client
            .get_task(&fenced_approval_id, "task-large-b")
            .await
            .expect("poll task-large-b host snapshot");
        if snap.status == TaskExecutionStatus::ReadyForReview {
            break snap;
        }
        assert!(
            Instant::now() < fenced_deadline,
            "timed out waiting for host ReadyForReview on task-large-b"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    };
    assert_eq!(read_invocation_count(&fixture.agy_count_file), 2);

    let wb = snap_b.worker.as_ref().expect("worker snapshot");
    let run_id_b = snap_b.run_id.clone().expect("host run_id");
    let worker_id_b = snap_b.worker_id.clone().expect("host worker_id");
    let task_wt_id_b = wb
        .worktree
        .task_worktree_id
        .clone()
        .expect("host task_worktree_id");
    let run_wt_id_b = wb
        .worktree
        .run_worktree_id
        .clone()
        .expect("host run_worktree_id");
    let wt_b = PathBuf::from(
        wb.worktree
            .task_worktree_path
            .as_ref()
            .expect("task_worktree_path"),
    );
    assert_eq!(wb.worktree.base_commit, fixture.base_commit);
    assert_eq!(wb.observed_model.as_deref(), Some("gemini-4-argon-high"));
    assert_eq!(wb.observed_effort.as_deref(), Some("high"));

    let mut raw_b_bytes = fs::read(wt_b.join("src/b.rs")).expect("read worker src/b.rs");
    raw_b_bytes.extend_from_slice(b"// raw non-utf8 bytes: \xff\xfe\x80\n");
    fs::write(wt_b.join("src/b.rs"), &raw_b_bytes).expect("append non-UTF-8 bytes to src/b.rs");

    let legacy_diff_b = run_sanitized_git(
        &wt_b,
        &["diff", "--binary", &fixture.base_commit],
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    );
    assert!(legacy_diff_b.is_err());

    let raw_diff_b = run_sanitized_git_bytes(
        &wt_b,
        &["diff", "--binary", &fixture.base_commit],
        DEFAULT_MAX_GIT_OUTPUT_BYTES,
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .expect("compute raw diff for task-large-b");
    assert!(raw_diff_b.stdout_bytes.len() > 180_000);
    assert!(std::str::from_utf8(&raw_diff_b.stdout_bytes).is_err());

    let head_b = resolve_exact_commit_sha(&wt_b, "HEAD", DEFAULT_GIT_COMMAND_TIMEOUT_MS)
        .expect("resolve wt_b HEAD commit");
    let tree_b = resolve_exact_tree_sha(&wt_b, &head_b, DEFAULT_GIT_COMMAND_TIMEOUT_MS)
        .expect("resolve wt_b HEAD tree");

    let fenced_rep = Box::pin(fenced_session.step(610_500))
        .await
        .expect("reconcile fenced >64KiB ReadyForReview snapshot");
    assert_eq!(fenced_rep.status, CoworkerControllerPhase1Status::Stopped);
    assert_eq!(fenced_rep.job_revision, dispatched_job_rev_b);
    assert_eq!(fenced_rep.active_running_count, 0);
    assert!(fenced_rep.dispatched_task_ids.is_empty());
    assert!(fenced_rep
        .fenced_task_ids
        .iter()
        .any(|id| id == "task-large-b"));
    assert!(fenced_rep.reconciled_ready_for_review_task_ids.is_empty());

    let obs_b = fenced_rep
        .tasks
        .iter()
        .find(|t| t.task_id == "task-large-b")
        .expect("task-large-b observation");
    assert_eq!(obs_b.stage, CoworkerTaskStage::Running);
    assert_eq!(obs_b.host_status, TaskExecutionStatus::ReadyForReview);
    assert!(obs_b.fenced);
    assert_eq!(obs_b.run_id.as_deref(), Some(run_id_b.as_str()));
    assert_eq!(obs_b.worker_id.as_deref(), Some(worker_id_b.as_str()));
    assert_eq!(
        obs_b.task_worktree_id.as_deref(),
        Some(task_wt_id_b.as_str())
    );
    assert_eq!(obs_b.observed_model, wb.observed_model);
    assert_eq!(obs_b.observed_effort, wb.observed_effort);

    let repo_head_b =
        resolve_exact_commit_sha(&fixture.repo_dir, "HEAD", DEFAULT_GIT_COMMAND_TIMEOUT_MS)
            .expect("resolve primary repo HEAD");
    assert_eq!(repo_head_b, fixture.base_commit);
    let repo_b_path = fixture.repo_dir.join("src/b.rs");
    let repo_b_text = fs::read_to_string(&repo_b_path).expect("read repo b.rs");
    assert_eq!(repo_b_text, "pub fn b() -> u32 { 2 }\n");
    assert!(wt_b.exists());
    let kept_b_bytes = fs::read(wt_b.join("src/b.rs")).expect("read wt_b b.rs");
    assert_eq!(kept_b_bytes, raw_b_bytes);

    let ledger_b = fixture
        .workflow_store
        .read_binding_ledger(BINDING_A)
        .expect("read binding ledger after fenced step");
    let job_rec_b = ledger_b
        .jobs
        .iter()
        .find(|j| j.job_id == fenced_job.job_id)
        .expect("fenced job");
    assert_eq!(job_rec_b.job_revision, dispatched_job_rev_b);
    assert_eq!(job_rec_b.outbox.len(), 1);
    let outbox_b = &job_rec_b.outbox[0];
    assert_eq!(
        outbox_b.status,
        OutboxDispatchStatus::DispatchedEffectConfirmed
    );
    assert_eq!(outbox_b.run_id.as_deref(), Some(run_id_b.as_str()));
    assert_eq!(outbox_b.worker_id.as_deref(), Some(worker_id_b.as_str()));

    let task_rec_b = job_rec_b
        .tasks
        .iter()
        .find(|t| t.task_id == "task-large-b")
        .expect("task-large-b record");
    assert_eq!(task_rec_b.stage, CoworkerTaskStage::Running);
    assert!(task_rec_b.unresolved_outbox_id.is_none());
    assert!(task_rec_b.candidates.is_empty());
    assert!(task_rec_b.active_candidate_id.is_none());
    assert!(task_rec_b.accepted_candidate_id.is_none());
    assert_eq!(task_rec_b.attempts.len(), 1);
    let attempt_b = &task_rec_b.attempts[0];
    assert_eq!(attempt_b.run_id.as_deref(), Some(run_id_b.as_str()));
    assert_eq!(attempt_b.worker_id.as_deref(), Some(worker_id_b.as_str()));
    assert_eq!(attempt_b.base_commit, fixture.base_commit);
    assert!(attempt_b.report_artifact_id.is_none());
    assert!(attempt_b.diff_digest.is_none());
    assert!(attempt_b.head_commit.is_none());
    assert!(attempt_b.tree_sha.is_none());

    let rep2_b = Box::pin(fenced_session.step(610_600))
        .await
        .expect("repeat fenced >64KiB step idempotently");
    assert_eq!(rep2_b.status, CoworkerControllerPhase1Status::Stopped);
    assert_eq!(rep2_b.job_revision, dispatched_job_rev_b);
    assert!(rep2_b.dispatched_task_ids.is_empty());
    assert!(rep2_b.reconciled_ready_for_review_task_ids.is_empty());
    assert_eq!(read_invocation_count(&fixture.agy_count_file), 2);

    let fenced_rt = fixture
        .policy_store
        .get_job_runtime_readonly(BINDING_A, &fenced_job.job_id)
        .expect("read fenced runtime")
        .expect("fenced runtime exists");
    assert_eq!(fenced_rt.total_task_attempts_reserved, 1);
    let fenced_coder_usage = fenced_rt
        .role_usage
        .get(&WorkflowRole::Coder)
        .expect("fenced Coder usage");
    assert_eq!(fenced_coder_usage.calls_reserved, 1);
    assert_eq!(fenced_coder_usage.calls_completed, 1);

    let fenced_art_key = ControllerJobRuntimeState::artifact_key("task-large-b", 1);
    let art_b = fenced_rt
        .attempt_artifacts
        .get(&fenced_art_key)
        .expect("retained runtime artifacts for task-large-b attempt 1");
    assert_eq!(art_b.task_id, "task-large-b");
    assert_eq!(art_b.attempt_no, 1);
    assert_eq!(art_b.run_id, run_id_b);
    assert_eq!(art_b.worker_id, worker_id_b);
    assert_eq!(art_b.task_worktree_id, task_wt_id_b);
    assert_eq!(art_b.integration_worktree_id, run_wt_id_b);
    assert_eq!(art_b.execution_base_commit, fixture.base_commit);
    assert_eq!(art_b.observed_provider, wb.provider);
    assert_eq!(art_b.observed_model, wb.observed_model);
    assert_eq!(art_b.observed_effort, wb.observed_effort);
    assert_eq!(
        art_b.committed_head_commit.as_deref(),
        Some(head_b.as_str())
    );
    assert_eq!(art_b.committed_tree_sha.as_deref(), Some(tree_b.as_str()));
    assert!(art_b.candidate_id.is_none());
    assert!(art_b.check_evidence.is_empty());
    assert!(art_b.test_evidence_summary.is_none());
    assert!(art_b.review_subject.is_none());
    assert!(art_b.reviewer_response_digest.is_none());
    assert!(art_b.review_receipt.is_none());
    assert!(art_b.integration_receipt.is_none());
    assert!(!art_b.is_derived_attempt);

    let approval_b = fixture
        .authority
        .get_for_operator(&fenced_approval_id)
        .expect("read fenced approval");
    let approved_task_b = approval_b
        .tasks
        .iter()
        .find(|t| t.task_id == "task-large-b")
        .expect("approved task-large-b");
    let choice_b = &task_rec_b.selected_choice;
    let coder_ok_b = art_b.has_matching_coder_evidence(approved_task_b, choice_b);
    assert!(coder_ok_b);

    let ef_b = fenced_rt
        .pending_effects
        .values()
        .find(|e| e.request_key == obs_b.command_key)
        .expect("fenced CoderDispatch effect");
    assert_eq!(ef_b.kind, PendingControllerEffectKind::CoderDispatch);
    assert_eq!(ef_b.task_id.as_deref(), Some("task-large-b"));
    assert_eq!(ef_b.attempt_no, Some(1));
    assert_eq!(ef_b.status, PendingControllerEffectStatus::EffectConfirmed);
    assert_eq!(
        ef_b.external_correlation_id.as_deref(),
        Some(run_id_b.as_str())
    );

    drop(fenced_session);
    fenced_host.shutdown().await.expect("shutdown fenced_host");

    // 3. Bounded >4 MiB (DEFAULT_MAX_GIT_OUTPUT_BYTES) tracked diff overflow is rejected closed.
    fs::write(&fenced_gate_path, b"hold").expect("write overflow gate");
    let (overflow_job, overflow_approval_id) = Box::pin(create_planned_approved_and_frozen_job(
        &fixture,
        "brf_large_overflow",
        "plan-large-overflow",
        vec![PlanTask {
            id: "task-overflow-b".to_string(),
            title: "Large Overflow B".to_string(),
            instruction: "CTRL_LARGE_FENCED_B".to_string(),
            role: WorkflowRole::Coder,
            difficulty: Difficulty::Hard,
            depends_on: Vec::new(),
            allowed_write_paths: vec!["src/b.rs".to_string()],
        }],
        single_coder_task_interfaces("task-overflow-b", "iface_overflow_b"),
        vec!["src/b.rs".to_string()],
        1,
        ExecutionBudgets::default(),
        ControllerRoleExecutionBudget::default_for_role(WorkflowRole::Coder, 4),
    ))
    .await;

    let overflow_host = ApprovedHost::start(ApprovedHostConfig::new(
        &fixture.registry_dir,
        &fixture.plans_dir,
        &fixture.authority_dir,
        &fixture.sessions_dir,
        &overflow_approval_id,
    ))
    .await
    .expect("start overflow ApprovedHost");
    let overflow_session = fixture
        .controller
        .open_owned_session(BINDING_A, THREAD_UUID_A, &overflow_job.job_id, 620_000)
        .expect("open overflow session");

    Box::pin(overflow_session.step(620_100))
        .await
        .expect("dispatch task-overflow-b");
    fs::remove_file(&fenced_gate_path).expect("remove overflow gate");

    let ov_deadline = Instant::now() + Duration::from_secs(10);
    let snap_ov = loop {
        let snap = host_client
            .get_task(&overflow_approval_id, "task-overflow-b")
            .await
            .expect("poll task-overflow-b host snapshot");
        if snap.status == TaskExecutionStatus::ReadyForReview {
            break snap;
        }
        assert!(
            Instant::now() < ov_deadline,
            "timed out waiting for host ReadyForReview on task-overflow-b"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    };
    let wt_ov = PathBuf::from(
        snap_ov
            .worker
            .as_ref()
            .expect("worker snapshot")
            .worktree
            .task_worktree_path
            .as_ref()
            .expect("task_worktree_path"),
    );
    let overflow_source = build_large_rust_source(70_000, "b", 201);
    assert!(overflow_source.len() > DEFAULT_MAX_GIT_OUTPUT_BYTES);
    fs::write(wt_ov.join("src/b.rs"), overflow_source.as_bytes())
        .expect("write >4 MiB tracked diff to src/b.rs");

    let ov_err = Box::pin(overflow_session.step(620_500))
        .await
        .expect_err(">4 MiB git diff must be rejected by controller reconciliation");
    assert_eq!(ov_err.code(), NativeErrorCode::ValidationFailed.as_str());
    let ov_msg = ov_err.message();
    assert!(ov_msg.contains("failed to compute git diff in task worktree:"));
    assert_eq!(read_invocation_count(&fixture.agy_count_file), 3);

    let ledger_ov = fixture
        .workflow_store
        .read_binding_ledger(BINDING_A)
        .expect("read binding ledger after overflow error");
    let task_rec_ov = ledger_ov
        .jobs
        .iter()
        .find(|j| j.job_id == overflow_job.job_id)
        .expect("overflow job")
        .tasks
        .iter()
        .find(|t| t.task_id == "task-overflow-b")
        .expect("task-overflow-b record");
    assert_ne!(task_rec_ov.stage, CoworkerTaskStage::ReadyForReview);

    let overflow_rt = fixture
        .policy_store
        .get_job_runtime_readonly(BINDING_A, &overflow_job.job_id)
        .expect("read overflow runtime")
        .expect("overflow runtime exists");
    let ov_art_key = ControllerJobRuntimeState::artifact_key("task-overflow-b", 1);
    assert!(!overflow_rt.attempt_artifacts.contains_key(&ov_art_key));

    drop(overflow_session);
    overflow_host
        .shutdown()
        .await
        .expect("shutdown overflow_host");
}

#[tokio::test]
async fn controller_fenced_uncertain_outbox_reconciles_over_64kib_raw_bytes_report_id() {
    let fixture = setup_fixture();
    let root_dir = fixture.agy_count_file.parent().expect("fixture root dir");
    let large_a_path = root_dir.join("large_src_a_u.rs");
    let large_b_path = root_dir.join("large_src_b_u.rs");
    let fenced_gate_path = root_dir.join("fenced_task_u.gate");

    fs::write(&large_a_path, build_large_rust_source(3_000, "a", 100))
        .expect("write >64KiB src/a.rs payload");
    fs::write(&large_b_path, build_large_rust_source(3_000, "b", 200))
        .expect("write >64KiB src/b.rs payload");
    install_large_diff_agy_cases(&fixture, &large_a_path, &large_b_path, &fenced_gate_path);

    let (job_u, approval_id_u) = Box::pin(create_planned_approved_and_frozen_job(
        &fixture,
        "brf_large_fenced_uncertain",
        "plan-large-fenced-uncertain",
        vec![PlanTask {
            id: "task-uncertain-b".to_string(),
            title: "Large Fenced Uncertain B".to_string(),
            instruction: "CTRL_LARGE_FENCED_B".to_string(),
            role: WorkflowRole::Coder,
            difficulty: Difficulty::Hard,
            depends_on: Vec::new(),
            allowed_write_paths: vec!["src/b.rs".to_string()],
        }],
        single_coder_task_interfaces("task-uncertain-b", "iface_uncertain_b"),
        vec!["src/b.rs".to_string()],
        1,
        ExecutionBudgets::default(),
        ControllerRoleExecutionBudget::default_for_role(WorkflowRole::Coder, 4),
    ))
    .await;

    let host_u = ApprovedHost::start(ApprovedHostConfig::new(
        &fixture.registry_dir,
        &fixture.plans_dir,
        &fixture.authority_dir,
        &fixture.sessions_dir,
        &approval_id_u,
    ))
    .await
    .expect("start uncertain ApprovedHost");
    let host_client = ApprovedHostClient::new(
        Arc::clone(&fixture.authority),
        &fixture.sessions_dir,
        BINDING_A,
        THREAD_UUID_A,
    )
    .expect("create ApprovedHostClient");

    let approval_u = fixture
        .authority
        .get_for_operator(&approval_id_u)
        .expect("read approval_u");
    let approved_task_u = approval_u
        .tasks
        .iter()
        .find(|t| t.task_id == "task-uncertain-b")
        .expect("approved task-uncertain-b");
    let cmd_key_u = approved_task_u
        .command_key
        .clone()
        .expect("frozen command_key");

    // Durable intent reserved before host dispatch, followed by host execution and operator stop
    // before controller post-dispatch outbox confirmation.
    let (reserved_job_u, reserved_outbox_u) = fixture
        .workflow_store
        .reserve_task_outbox(&ReserveTaskOutboxRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            job_id: job_u.job_id.clone(),
            expected_job_revision: job_u.job_revision,
            expected_ledger_revision: None,
            request_key: cmd_key_u.clone(),
            kind: OutboxDispatchKind::TaskAttemptDispatch,
            task_id: "task-uncertain-b".to_string(),
            role: WorkflowRole::Coder,
            subject_digest: approved_task_u.task_digest.clone(),
            dependency_input_commit: None,
            dependency_input_tree_sha: None,
            accepted_predecessor_receipts: Vec::new(),
        })
        .expect("reserve initial coder outbox before host dispatch");
    assert_eq!(
        reserved_outbox_u.status,
        OutboxDispatchStatus::ReservedBeforeEffect
    );
    assert_eq!(
        reserved_job_u.tasks[0].stage,
        CoworkerTaskStage::DispatchReserved
    );
    assert_eq!(
        reserved_job_u.tasks[0].unresolved_outbox_id.as_deref(),
        Some(reserved_outbox_u.outbox_id.as_str())
    );
    let pre_attempt_u = &reserved_job_u.tasks[0].attempts[0];
    assert!(pre_attempt_u.report_artifact_id.is_none());

    host_client
        .dispatch_task(&approval_id_u, "task-uncertain-b")
        .await
        .expect("dispatch task-uncertain-b on ApprovedHost");

    fixture
        .policy_store
        .mutate_job_runtime_for_operator(BINDING_A, &job_u.job_id, |rt| {
            rt.stop_requested = true;
            Ok(())
        })
        .expect("set stop_requested before controller reconciles uncertain outbox");

    let deadline_u = Instant::now() + Duration::from_secs(10);
    let snap_u = loop {
        let snap = host_client
            .get_task(&approval_id_u, "task-uncertain-b")
            .await
            .expect("poll task-uncertain-b host snapshot");
        if snap.status == TaskExecutionStatus::ReadyForReview {
            break snap;
        }
        assert!(
            Instant::now() < deadline_u,
            "timed out waiting for host ReadyForReview on task-uncertain-b"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    };
    assert_eq!(read_invocation_count(&fixture.agy_count_file), 1);

    let wu = snap_u.worker.as_ref().expect("worker snapshot");
    let run_id_u = snap_u.run_id.clone().expect("host run_id");
    let worker_id_u = snap_u.worker_id.clone().expect("host worker_id");
    let task_wt_id_u = wu
        .worktree
        .task_worktree_id
        .clone()
        .expect("host task_worktree_id");
    let run_wt_id_u = wu
        .worktree
        .run_worktree_id
        .clone()
        .expect("host run_worktree_id");
    let wt_u = PathBuf::from(
        wu.worktree
            .task_worktree_path
            .as_ref()
            .expect("task_worktree_path"),
    );
    assert_eq!(wu.worktree.base_commit, fixture.base_commit);
    assert_eq!(wu.observed_model.as_deref(), Some("gemini-4-argon-high"));
    assert_eq!(wu.observed_effort.as_deref(), Some("high"));

    let mut raw_u_bytes = fs::read(wt_u.join("src/b.rs")).expect("read worker src/b.rs");
    raw_u_bytes.extend_from_slice(b"// raw non-utf8 bytes: \xff\xfe\x80\n");
    fs::write(wt_u.join("src/b.rs"), &raw_u_bytes).expect("append non-UTF-8 bytes to src/b.rs");

    let legacy_diff_u = run_sanitized_git(
        &wt_u,
        &["diff", "--binary", &fixture.base_commit],
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    );
    assert!(legacy_diff_u.is_err());

    let raw_diff_u = run_sanitized_git_bytes(
        &wt_u,
        &["diff", "--binary", &fixture.base_commit],
        DEFAULT_MAX_GIT_OUTPUT_BYTES,
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .expect("compute raw diff for task-uncertain-b");
    assert!(raw_diff_u.stdout_bytes.len() > 180_000);
    assert!(std::str::from_utf8(&raw_diff_u.stdout_bytes).is_err());

    let head_u = resolve_exact_commit_sha(&wt_u, "HEAD", DEFAULT_GIT_COMMAND_TIMEOUT_MS)
        .expect("resolve wt_u HEAD commit");
    let tree_u = resolve_exact_tree_sha(&wt_u, &head_u, DEFAULT_GIT_COMMAND_TIMEOUT_MS)
        .expect("resolve wt_u HEAD tree");
    let expected_digest_u = sha256_hex(&raw_diff_u.stdout_bytes);
    let expected_art_id_u = deterministic_prefixed_id(
        "threadlane-native:coworker-coder-attempt-report:v1",
        "art",
        &[
            &approval_id_u,
            "task-uncertain-b",
            &run_id_u,
            &worker_id_u,
            &task_wt_id_u,
            &expected_digest_u,
        ],
    );

    let session_u = fixture
        .controller
        .open_owned_session(BINDING_A, THREAD_UUID_A, &job_u.job_id, 615_000)
        .expect("open uncertain fenced session");
    let rep_u = Box::pin(session_u.step(615_100))
        .await
        .expect("reconcile fenced uncertain outbox >64KiB ReadyForReview snapshot");
    assert_eq!(rep_u.status, CoworkerControllerPhase1Status::Stopped);
    assert_eq!(rep_u.active_running_count, 0);
    assert!(rep_u.dispatched_task_ids.is_empty());
    assert!(rep_u.reconciled_ready_for_review_task_ids.is_empty());
    assert!(rep_u
        .fenced_task_ids
        .iter()
        .any(|id| id == "task-uncertain-b"));

    let obs_u = rep_u
        .tasks
        .iter()
        .find(|t| t.task_id == "task-uncertain-b")
        .expect("task-uncertain-b observation");
    assert_eq!(obs_u.stage, CoworkerTaskStage::Running);
    assert_eq!(obs_u.host_status, TaskExecutionStatus::ReadyForReview);
    assert!(obs_u.fenced);

    let ledger_u = fixture
        .workflow_store
        .read_binding_ledger(BINDING_A)
        .expect("read binding ledger after uncertain fenced step");
    let job_rec_u = ledger_u
        .jobs
        .iter()
        .find(|j| j.job_id == job_u.job_id)
        .expect("uncertain fenced job");
    assert_eq!(job_rec_u.outbox.len(), 1);
    let outbox_u = &job_rec_u.outbox[0];
    assert_eq!(
        outbox_u.status,
        OutboxDispatchStatus::DispatchedEffectConfirmed
    );
    assert_eq!(outbox_u.run_id.as_deref(), Some(run_id_u.as_str()));
    assert_eq!(outbox_u.worker_id.as_deref(), Some(worker_id_u.as_str()));

    let task_rec_u = job_rec_u
        .tasks
        .iter()
        .find(|t| t.task_id == "task-uncertain-b")
        .expect("task-uncertain-b record");
    assert_eq!(task_rec_u.stage, CoworkerTaskStage::Running);
    assert!(task_rec_u.unresolved_outbox_id.is_none());
    assert!(task_rec_u.candidates.is_empty());
    let attempt_u = &task_rec_u.attempts[0];
    assert_eq!(attempt_u.run_id.as_deref(), Some(run_id_u.as_str()));
    assert_eq!(attempt_u.worker_id.as_deref(), Some(worker_id_u.as_str()));
    assert_eq!(
        attempt_u.task_worktree_id.as_deref(),
        Some(task_wt_id_u.as_str())
    );
    assert_eq!(attempt_u.base_commit, fixture.base_commit);
    assert_eq!(attempt_u.head_commit.as_deref(), Some(head_u.as_str()));
    assert_eq!(attempt_u.tree_sha.as_deref(), Some(tree_u.as_str()));
    assert_eq!(
        attempt_u.diff_digest.as_deref(),
        Some(expected_digest_u.as_str())
    );
    assert_eq!(
        attempt_u.report_artifact_id.as_deref(),
        Some(expected_art_id_u.as_str())
    );
    assert_eq!(
        attempt_u.observed_model.as_deref(),
        Some("gemini-4-argon-high")
    );
    assert_eq!(attempt_u.observed_effort.as_deref(), Some("high"));

    let rep2_u = Box::pin(session_u.step(615_200))
        .await
        .expect("repeat uncertain fenced >64KiB step idempotently");
    assert_eq!(rep2_u.status, CoworkerControllerPhase1Status::Stopped);
    assert_eq!(rep2_u.job_revision, rep_u.job_revision);
    assert!(rep2_u.dispatched_task_ids.is_empty());
    assert!(rep2_u.reconciled_ready_for_review_task_ids.is_empty());
    assert_eq!(read_invocation_count(&fixture.agy_count_file), 1);

    let rt_u = fixture
        .policy_store
        .get_job_runtime_readonly(BINDING_A, &job_u.job_id)
        .expect("read uncertain fenced runtime")
        .expect("uncertain fenced runtime exists");
    assert_eq!(rt_u.total_task_attempts_reserved, 1);
    let coder_usage_u = rt_u
        .role_usage
        .get(&WorkflowRole::Coder)
        .expect("uncertain fenced Coder usage");
    assert_eq!(coder_usage_u.calls_reserved, 1);
    assert_eq!(coder_usage_u.calls_completed, 1);

    let art_key_u = ControllerJobRuntimeState::artifact_key("task-uncertain-b", 1);
    let art_u = rt_u
        .attempt_artifacts
        .get(&art_key_u)
        .expect("retained runtime artifacts for task-uncertain-b attempt 1");
    assert_eq!(art_u.run_id, run_id_u);
    assert_eq!(art_u.worker_id, worker_id_u);
    assert_eq!(art_u.task_worktree_id, task_wt_id_u);
    assert_eq!(art_u.integration_worktree_id, run_wt_id_u);
    assert_eq!(art_u.execution_base_commit, fixture.base_commit);
    assert_eq!(
        art_u.committed_head_commit.as_deref(),
        Some(head_u.as_str())
    );
    assert_eq!(art_u.committed_tree_sha.as_deref(), Some(tree_u.as_str()));

    drop(session_u);
    host_u.shutdown().await.expect("shutdown host_u");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn controller_preflight_enforces_4096_utf8_bytes_across_task_set_before_first_root_and_preserves_recovery(
) {
    async fn orchestrate_and_freeze_job(
        fixture: &ControllerTestFixture,
        brief_id: &str,
        plan_id: &str,
        tasks: Vec<PlanTask>,
        approved_write_paths: Vec<String>,
        max_parallel: usize,
        seed_legacy_approval: bool,
    ) -> (CoworkerJobRecord, String) {
        let pb = project_base(fixture);
        let reg_snap = fixture.registry.snapshot().expect("registry snapshot");
        let brief = sample_brief_for_scope(fixture, BINDING_A, THREAD_UUID_A, brief_id);
        let scoped_workflow = ScopedWorkflow::new(
            Arc::clone(&fixture.registry),
            Arc::clone(&fixture.plans),
            BINDING_A.to_string(),
        )
        .expect("create scoped_workflow");

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
            .map(|t| StructuredPlannedTask {
                task_id: t.id.clone(),
                title: t.title.clone(),
                description: t.instruction.clone(),
                role: t.role,
                difficulty: t.difficulty,
                depends_on: t.depends_on.clone(),
                allowed_write_paths: t.allowed_write_paths.clone(),
                owned_interfaces: Vec::new(),
                consumed_interfaces: Vec::new(),
                model_override: None,
                tool_override: None,
            })
            .collect();
        let structured_plan = StructuredOrchestratorOutput::Plan {
            schema_version: 1,
            summary: format!(
                "Orchestrator plan `{plan_id}` for `{brief_id}` exercising executable instruction preflight."
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
        fs::write(
            &fixture.agy_plan_response_file,
            serde_json::to_string(&structured_plan).expect("serialize structured plan"),
        )
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

        let (planned_plan_id, planned_plan_revision, planned_plan_digest) = if !seed_legacy_approval
        {
            let plan_outcome = fixture
                .planning_service
                .orchestrate_brief_to_plan(&scoped_workflow, &orch_req)
                .await
                .expect("orchestrate_brief_to_plan");
            let published_outcome = plan_outcome
                .require_published_plan()
                .expect("published plan outcome");
            (
                published_outcome.planning_result.plan_id.clone(),
                published_outcome.planning_result.plan_revision,
                published_outcome.planning_result.plan_digest.clone(),
            )
        } else {
            // Fresh Orchestrator planning now refuses oversized Coder task descriptions before
            // PlanStore draft/publication/linkage while preserving the completed call artifact.
            let fresh_plan_err = fixture
                .planning_service
                .orchestrate_brief_to_plan(&scoped_workflow, &orch_req)
                .await
                .expect_err("fresh Orchestrator planning must reject oversized description");
            assert_eq!(fresh_plan_err.code, NativeErrorCode::ValidationFailed);
            assert!(fresh_plan_err.message.contains("4096 bytes"));
            assert!(!fixture
                .plans
                .snapshot()
                .expect("snapshot plans after fresh refusal")
                .iter()
                .any(|rev| rev.binding_id == BINDING_A));

            let ledger_after = fixture
                .workflow_store
                .read_binding_ledger(BINDING_A)
                .expect("read binding ledger after fresh refusal");
            let grant_after = ledger_after
                .planning_grants
                .iter()
                .find(|g| g.grant_id == grant.grant_id)
                .expect("grant after fresh refusal");
            let completed_res = &grant_after.reservations[0];
            assert_eq!(completed_res.status, PlanningReservationStatus::Completed);

            let draft = fixture
                .plans
                .change_at(
                    0,
                    PlanChange::SaveDraft {
                        plan_id: plan_id.to_string(),
                        binding_id: BINDING_A.to_string(),
                        profile_id: "profile-coworker".to_string(),
                        profile_revision: reg_snap.revision,
                        summary: format!("Historical Orchestrator plan `{plan_id}`"),
                        tasks: tasks.clone(),
                    },
                    1_700_000_000_000,
                )
                .expect("save historical oversized draft via generic 65K PlanStore");
            let published = fixture
                .plans
                .change(
                    draft.revision,
                    PlanChange::Publish {
                        plan_id: plan_id.to_string(),
                        revision: draft.revision,
                    },
                )
                .expect("publish historical oversized plan");
            let compiled = compile_plan_revision(&published, &reg_snap)
                .expect("compile historical oversized plan");

            let seeded_pr = threadlane_native::coworker_workflow::contracts::PlanningResultRef {
                grant_id: grant_after.grant_id.clone(),
                reservation_id: completed_res.reservation_id.clone(),
                request_key: completed_res.request_key.clone(),
                run_id: completed_res.run_id.clone().expect("run_id"),
                report_artifact_id: completed_res
                    .report_artifact_id
                    .clone()
                    .expect("report_artifact_id"),
                output_digest: completed_res.output_digest.clone().expect("output_digest"),
                work_brief_digest: job.work_brief_digest.clone(),
                project_base: project_base(fixture),
                profile_id: grant_after.profile_id.clone(),
                registry_revision: grant_after.registry_revision,
                policy_digest: grant_after.policy_digest.clone(),
                selected_choice: completed_res.selected_choice.clone(),
                plan_id: published.plan_id.clone(),
                plan_revision: published.revision,
                plan_digest: compiled.plan_digest.clone(),
                plan_status: threadlane_workflow::PlanStatus::Published,
                task_interfaces: BTreeMap::new(),
                architect_advice_ids: Vec::new(),
                resolved_open_questions: Vec::new(),
                deferred_proposals: Vec::new(),
                recorded_at_ms: 2_500,
            };
            fixture
                .workflow_store
                .record_job_planning_result(
                    &threadlane_native::coworker_workflow::store::RecordJobPlanningResultRequest {
                        binding_id: BINDING_A.to_string(),
                        thread_uuid: THREAD_UUID_A.to_string(),
                        job_id: job.job_id.clone(),
                        expected_job_revision: Some(job.job_revision),
                        expected_ledger_revision: None,
                        planning_result: seeded_pr,
                    },
                )
                .expect("record historical PlanningResultRef");

            (published.plan_id, published.revision, compiled.plan_digest)
        };

        let job = fixture
            .workflow_store
            .read_binding_ledger(BINDING_A)
            .expect("read binding ledger")
            .jobs
            .into_iter()
            .find(|j| j.job_id == job.job_id)
            .expect("find orchestrated job");
        assert_eq!(job.phase, CoworkerJobPhase::AwaitingApproval);

        let approved_task_ids: Vec<String> = tasks.iter().map(|t| t.id.clone()).collect();
        let approval_req = OperatorApprovalRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            plan_id: planned_plan_id.clone(),
            plan_revision: planned_plan_revision,
            expected_registry_revision: reg_snap.revision,
            expected_plan_digest: planned_plan_digest,
            repository_path: fixture.repo_dir.clone(),
            base_commit: fixture.base_commit.clone(),
            approved_task_ids: approved_task_ids.clone(),
            approved_write_paths: approved_write_paths.clone(),
            max_parallel,
            budgets: ExecutionBudgets::default(),
        };

        let approval_id = if !seed_legacy_approval {
            fixture
                .authority
                .approve_at(approval_req, 1_700_000_001_000)
                .expect("approve valid 4096-byte plan")
                .approval_id
        } else {
            let new_approval_err = fixture
                .authority
                .approve_at(approval_req, 1_700_000_001_000)
                .expect_err("new approval must reject oversized later worker task");
            let err_text = new_approval_err.to_string();
            assert!(
                err_text.contains("task-b") && err_text.contains("4096 bytes"),
                "unexpected authority error: {err_text}"
            );

            let published_plan = fixture
                .plans
                .get_revision(&planned_plan_id, planned_plan_revision)
                .expect("read published plan")
                .expect("published plan exists");
            let compiled =
                compile_plan_revision(&published_plan, &reg_snap).expect("compile published plan");
            let canonical_repo = fs::canonicalize(&fixture.repo_dir).expect("canonical repo");
            let canonical_agy = fs::canonicalize(&fixture.fake_agy_exe).expect("canonical agy");

            let mut record = threadlane_authority::ApprovalRecord {
                approval_id: String::new(),
                subject_digest: String::new(),
                policy_version: threadlane_authority::AUTHORITY_POLICY_VERSION,
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                plan_id: published_plan.plan_id.clone(),
                plan_revision: published_plan.revision,
                profile_id: "profile-coworker".to_string(),
                registry_revision: reg_snap.revision,
                plan_digest: compiled.plan_digest,
                repository_path: canonical_repo,
                base_commit: fixture.base_commit.clone(),
                approved_task_ids,
                approved_write_paths,
                max_parallel,
                budgets: ExecutionBudgets::default(),
                tasks: compiled
                    .tasks
                    .iter()
                    .map(|ct| threadlane_authority::ApprovedTaskRecord {
                        task_id: ct.id.clone(),
                        title: ct.title.clone(),
                        instruction: ct.instruction.clone(),
                        instruction_digest: ct.instruction_digest.clone(),
                        role: ct.role,
                        difficulty: ct.difficulty,
                        tool_id: ct.tool_id.clone(),
                        adapter: ct.adapter,
                        provider: Some(threadlane_workers::WorkerProvider::Antigravity),
                        canonical_executable: Some(canonical_agy.clone()),
                        model_id: ct.choice.model_id.clone(),
                        model_arg: ct.model_arg.clone(),
                        base_model: ct.model_arg.clone(),
                        effort: ct.choice.effort.clone(),
                        cli_model_arg: Some(ct.model_arg.clone()),
                        standalone: Some(false),
                        file_permissions: Some(WorkerFilePermissions::TaskFiles),
                        depends_on: ct.depends_on.clone(),
                        allowed_write_paths: ct.allowed_write_paths.clone(),
                        task_digest: ct.task_digest.clone(),
                        profile_id: None,
                        command_key: None,
                    })
                    .collect(),
                created_at_ms: 1_700_000_001_000,
            };
            let core_digest =
                threadlane_authority::compute_authority_core_digest(&record).expect("core digest");
            for task in &mut record.tasks {
                task.profile_id = Some(threadlane_authority::derive_profile_id(
                    &core_digest,
                    &task.task_id,
                    &task.task_digest,
                ));
                task.command_key = Some(threadlane_authority::derive_command_key(
                    &core_digest,
                    &task.task_id,
                    &task.task_digest,
                ));
            }
            record.subject_digest = threadlane_authority::compute_record_subject_digest(&record)
                .expect("subject digest");
            record.approval_id = threadlane_authority::derive_approval_id(&record.subject_digest);
            threadlane_authority::verify_approval_record_integrity(
                &record,
                fixture.authority.approvals_path(),
            )
            .expect("historical sealed approval record integrity");

            let ledger_json = serde_json::json!({
                "schema_version": threadlane_authority::AUTHORITY_SCHEMA_VERSION,
                "approvals": [&record],
            });
            fs::write(
                fixture.authority.approvals_path(),
                serde_json::to_vec(&ledger_json).expect("serialize approvals ledger"),
            )
            .expect("write legacy approvals.json");
            fs::set_permissions(
                fixture.authority.approvals_path(),
                fs::Permissions::from_mode(0o600),
            )
            .expect("chmod 0600 approvals.json");
            record.approval_id
        };

        let cap_env = FrozenControllerCapabilityEnvelope {
            allowed_reviewer_choices: vec![headless_argon_choice(Difficulty::Hard)],
            allowed_closure_choices: vec![headless_argon_choice(Difficulty::Extreme)],
            max_in_scope_correction_attempts: 1,
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
                    approval_id: approval_id.clone(),
                    policy: execution_policy_for_fixture(fixture),
                    controller_envelope: Some(cap_env),
                    task_interfaces: BTreeMap::new(),
                },
                &fixture.authority,
            )
            .expect("bind job approval");

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
        let preview_req = FreezeControllerExecutionEnvelopeRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            job_id: bound_job.job_id.clone(),
            expected_job_revision: bound_job.job_revision,
            policy: execution_policy_for_fixture(fixture),
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
        let preview = fixture
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

        (bound_job, approval_id)
    }

    // 1. Exact 4,096 UTF-8 byte boundary (`12 + 2042 * 2 = 4,096` bytes, `2,054` chars)
    // dispatches through controller + host verbatim without truncation or normalization.
    let fixture_ok = setup_fixture();
    let instruction_4096_utf8 = format!("CTRL_TASK_A {}", "é".repeat(2042));
    assert_eq!(instruction_4096_utf8.len(), 4_096);
    assert_eq!(instruction_4096_utf8.chars().count(), 2_054);
    let (job_ok, approval_id_ok) = Box::pin(orchestrate_and_freeze_job(
        &fixture_ok,
        "brf_ctrl_utf8_4096",
        "plan-ctrl-utf8-4096",
        vec![PlanTask {
            id: "task-utf8-4096".to_string(),
            title: "UTF-8 4096B Boundary".to_string(),
            instruction: instruction_4096_utf8.clone(),
            role: WorkflowRole::Coder,
            difficulty: Difficulty::Hard,
            depends_on: Vec::new(),
            allowed_write_paths: vec!["src/a.rs".to_string()],
        }],
        vec!["src/a.rs".to_string()],
        1,
        false,
    ))
    .await;
    let host_ok = Box::pin(ApprovedHost::start(ApprovedHostConfig::new(
        &fixture_ok.registry_dir,
        &fixture_ok.plans_dir,
        &fixture_ok.authority_dir,
        &fixture_ok.sessions_dir,
        &approval_id_ok,
    )))
    .await
    .expect("start host_ok");
    let session_ok = fixture_ok
        .controller
        .open_owned_session(BINDING_A, THREAD_UUID_A, &job_ok.job_id, 5_000)
        .expect("open session_ok");
    let deadline_ok = Instant::now() + Duration::from_secs(15);
    loop {
        let rep = Box::pin(session_ok.step(5_100))
            .await
            .expect("step 4096B UTF-8 job");
        if rep.status == CoworkerControllerPhase1Status::AwaitingChecksOrReview {
            break;
        }
        assert!(
            Instant::now() < deadline_ok,
            "timed out waiting for 4096B UTF-8 task ReadyForReview"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert_eq!(read_invocation_count(&fixture_ok.agy_count_file), 1);
    const AGY_CONTRACT_PREFIX: &str =
        "[THREADLANE_AGY_TASK_FILES_CONTRACT] Session-local Antigravity task_files execution contract: ";
    const AGY_INSTRUCTION_MARKER: &str = "[THREADLANE_TASK_INSTRUCTION] ";
    assert!(!instruction_4096_utf8.contains(AGY_INSTRUCTION_MARKER));
    let captured_prompt =
        fs::read_to_string(&fixture_ok.agy_prompt_file).expect("read agy_prompt_file");
    assert!(
        captured_prompt.starts_with(AGY_CONTRACT_PREFIX),
        "expected software-owned task_files contract header prefix"
    );
    assert_eq!(
        captured_prompt.matches(AGY_INSTRUCTION_MARKER).count(),
        1,
        "expected exactly one task instruction boundary marker"
    );
    let (header_part, payload_part) = captured_prompt
        .split_once(AGY_INSTRUCTION_MARKER)
        .expect("split at AGY_TASK_FILES_PROMPT_BOUNDARY");
    assert!(header_part.starts_with(AGY_CONTRACT_PREFIX));
    assert!(header_part.contains("Persist changes only within declared task write scopes: ["));
    assert_eq!(payload_part.as_bytes(), instruction_4096_utf8.as_bytes());
    assert_eq!(payload_part.len(), threadlane_core::MAX_INSTRUCTION_LEN);
    assert_eq!(payload_part, instruction_4096_utf8);
    session_ok.release().expect("release session_ok");
    host_ok.shutdown().await.expect("shutdown host_ok");

    // 2. Legacy-approved frozen job with valid first root A (`3,965` bytes) + oversized second
    // parallel root B (`4,365` bytes) refuses BEFORE lease, outbox, host intent, or first root!
    let fixture_par = setup_fixture();
    let valid_root_a_instr = format!("CTRL_TASK_A {}", "a".repeat(3_953));
    let oversized_task_b_instr = format!("CTRL_TASK_B {}", "b".repeat(4_353));
    assert_eq!(valid_root_a_instr.len(), 3_965);
    assert_eq!(oversized_task_b_instr.len(), 4_365);

    let (job_par, approval_id_par) = Box::pin(orchestrate_and_freeze_job(
        &fixture_par,
        "brf_ctrl_legacy_parallel",
        "plan-ctrl-legacy-parallel",
        vec![
            PlanTask {
                id: "task-root-a".to_string(),
                title: "Valid First Root A".to_string(),
                instruction: valid_root_a_instr.clone(),
                role: WorkflowRole::Coder,
                difficulty: Difficulty::Hard,
                depends_on: Vec::new(),
                allowed_write_paths: vec!["src/a.rs".to_string()],
            },
            PlanTask {
                id: "task-b".to_string(),
                title: "Oversized Second Parallel Root B".to_string(),
                instruction: oversized_task_b_instr.clone(),
                role: WorkflowRole::Coder,
                difficulty: Difficulty::Hard,
                depends_on: Vec::new(),
                allowed_write_paths: vec!["src/b.rs".to_string()],
            },
        ],
        vec!["src/a.rs".to_string(), "src/b.rs".to_string()],
        2,
        true,
    ))
    .await;

    let host_par = Box::pin(ApprovedHost::start(ApprovedHostConfig::new(
        &fixture_par.registry_dir,
        &fixture_par.plans_dir,
        &fixture_par.authority_dir,
        &fixture_par.sessions_dir,
        &approval_id_par,
    )))
    .await
    .expect("start host_par for legacy sealed approval");
    let session_par = fixture_par
        .controller
        .open_owned_session(BINDING_A, THREAD_UUID_A, &job_par.job_id, 10_000)
        .expect("open session_par");
    let adm_par = session_par
        .verify_admission()
        .expect("verify_admission remains readable on legacy oversized job");
    let par_rev_before = adm_par.job.job_revision;
    let rt_par_before = fixture_par
        .policy_store
        .get_job_runtime_readonly(BINDING_A, &job_par.job_id)
        .expect("read rt_par_before")
        .expect("rt_par_before exists");

    let par_err = Box::pin(session_par.step(10_100))
        .await
        .expect_err("controller step must refuse oversized later root before first root starts");
    assert_eq!(par_err.code, NativeErrorCode::ValidationFailed);
    assert!(par_err.message.contains("task-b"));
    assert!(par_err.message.contains("4096 bytes"));
    assert_eq!(
        read_invocation_count(&fixture_par.agy_count_file),
        0,
        "valid first root A must NEVER start when later task B is oversized"
    );

    let rt_par = fixture_par
        .policy_store
        .get_job_runtime_readonly(BINDING_A, &job_par.job_id)
        .expect("read rt_par")
        .expect("rt_par exists");
    assert_eq!(
        rt_par, rt_par_before,
        "controller runtime state must remain unchanged on pre-effect refusal"
    );
    assert!(
        rt_par.lease.is_none(),
        "preflight refusal must occur before logical lease acquisition"
    );
    assert_eq!(rt_par.total_task_attempts_reserved, 0);
    assert_eq!(rt_par.total_corrections_reserved, 0);
    assert!(rt_par.pending_effects.is_empty());
    assert!(rt_par.role_usage.values().all(|usage| {
        usage.calls_reserved == 0 && usage.calls_completed == 0 && usage.total_runtime_ms == 0
    }));

    let job_par_after = fixture_par
        .workflow_store
        .read_binding_ledger(BINDING_A)
        .expect("read ledger after refusal")
        .jobs
        .into_iter()
        .find(|j| j.job_id == job_par.job_id)
        .expect("job_par exists");
    assert_eq!(job_par_after.job_revision, par_rev_before);
    assert!(job_par_after.outbox.is_empty());
    assert!(job_par_after
        .tasks
        .iter()
        .all(|t| t.stage == CoworkerTaskStage::ReadyToDispatch && t.attempts.is_empty()));

    let session_dir_par = fixture_par.sessions_dir.join(&approval_id_par);
    let manifest_par =
        threadlane_host::read_host_manifest_optional(&session_dir_par, &adm_par.approval)
            .expect("read manifest_par")
            .expect("manifest_par must exist");
    let binding_par = manifest_par
        .session_binding_id
        .clone()
        .expect("session_binding_id");
    let (intents_par, receipts_par) = threadlane_host::read_session_task_ledger_readonly(
        &session_dir_par,
        &adm_par.approval,
        &binding_par,
    )
    .expect("read host ledger")
    .unwrap_or_default();
    assert!(intents_par.is_empty(), "zero host TaskDispatchIntents");
    assert!(receipts_par.is_empty(), "zero host TaskRunReceipts");

    // 3. Receipt-only recovery on `fixture_par`: when BOTH tasks already have historical
    // completed `TaskRunReceipt`s on the host, `session_par.step` reconciles them to
    // `ReadyForReview` without redispatching (`agy_count` stays at `2`).
    let host_client_par = ApprovedHostClient::new(
        Arc::clone(&fixture_par.authority),
        &fixture_par.sessions_dir,
        BINDING_A,
        THREAD_UUID_A,
    )
    .expect("create host_client_par");
    host_client_par
        .dispatch_task(&approval_id_par, "task-root-a")
        .await
        .expect("dispatch valid task-root-a for historical receipt seed");
    let hist_a_deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if host_client_par
            .get_task(&approval_id_par, "task-root-a")
            .await
            .expect("poll task-root-a")
            .status
            == TaskExecutionStatus::ReadyForReview
        {
            break;
        }
        assert!(
            Instant::now() < hist_a_deadline,
            "timed out waiting for task-root-a ReadyForReview"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    let approved_task_b = adm_par
        .approval
        .tasks
        .iter()
        .find(|t| t.task_id == "task-b")
        .expect("approved task-b");
    let profile_b = approved_task_b.profile_id.clone().expect("profile_b");
    let cmd_key_b = approved_task_b.command_key.clone().expect("cmd_key_b");
    let scoped_par = threadlane::ScopedClient::from_state_dir(
        session_dir_par.join(threadlane_host::RUNTIME_STATE_SUBDIR),
        &binding_par,
    )
    .expect("open scoped runtime client");
    let hist_worker_b = scoped_par
        .start_worker(
            cmd_key_b.clone(),
            profile_b.clone(),
            "CTRL_TASK_B historical pre-limit worker run".to_string(),
        )
        .await
        .expect("start historical worker for task-b");
    let hist_b_deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if scoped_par
            .get_worker(hist_worker_b.run_id.clone())
            .await
            .expect("poll hist_worker_b")
            .status
            == threadlane_core::WorkerStatus::Completed
        {
            break;
        }
        assert!(
            Instant::now() < hist_b_deadline,
            "timed out waiting for hist_worker_b Completed"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let _ = threadlane_host::record_task_dispatch_intent(
        &session_dir_par,
        &adm_par.approval,
        &binding_par,
        threadlane_host::TaskDispatchIntent {
            task_id: "task-b".to_string(),
            task_digest: approved_task_b.task_digest.clone(),
            profile_id: profile_b.clone(),
            command_key: cmd_key_b.clone(),
            execution_binding: None,
            recorded_at_ms: 1_700_000_200_000,
        },
    )
    .expect("record historical intent for task-b");
    let _ = threadlane_host::record_task_receipt(
        &session_dir_par,
        &adm_par.approval,
        &binding_par,
        threadlane_host::TaskRunReceipt {
            task_id: "task-b".to_string(),
            task_digest: approved_task_b.task_digest.clone(),
            profile_id: profile_b,
            command_key: cmd_key_b,
            run_id: hist_worker_b.run_id,
            worker_id: hist_worker_b.worker_id,
            execution_binding: None,
            recorded_at_ms: 1_700_000_200_100,
        },
    )
    .expect("record historical receipt for task-b");
    assert_eq!(read_invocation_count(&fixture_par.agy_count_file), 2);

    let recovered_rep = Box::pin(session_par.step(10_500))
        .await
        .expect("receipt-only recovery must succeed without redispatching oversized task");
    assert_eq!(
        recovered_rep.status,
        CoworkerControllerPhase1Status::AwaitingChecksOrReview
    );
    assert!(recovered_rep.dispatched_task_ids.is_empty());
    assert_eq!(
        recovered_rep.reconciled_ready_for_review_task_ids,
        vec!["task-root-a".to_string(), "task-b".to_string()]
    );
    assert_eq!(
        read_invocation_count(&fixture_par.agy_count_file),
        2,
        "receipt-only recovery must observe existing receipts without redispatch"
    );
    session_par.release().expect("release session_par");
    host_par.shutdown().await.expect("shutdown host_par");

    // 4. Legacy-approved frozen job where oversized `task-b` is a DOWNSTREAM child
    // (`PendingDependencies` on `task-root-a`) ALSO fails preflight before `task-root-a` starts,
    // while `stop_requested` recovery remains usable for any pre-stop root receipt!
    let fixture_dep = setup_fixture();
    let (job_dep, approval_id_dep) = Box::pin(orchestrate_and_freeze_job(
        &fixture_dep,
        "brf_ctrl_legacy_dep",
        "plan-ctrl-legacy-dep",
        vec![
            PlanTask {
                id: "task-root-a".to_string(),
                title: "Valid First Root A".to_string(),
                instruction: valid_root_a_instr,
                role: WorkflowRole::Coder,
                difficulty: Difficulty::Hard,
                depends_on: Vec::new(),
                allowed_write_paths: vec!["src/a.rs".to_string()],
            },
            PlanTask {
                id: "task-b".to_string(),
                title: "Oversized Downstream Child B".to_string(),
                instruction: oversized_task_b_instr,
                role: WorkflowRole::Coder,
                difficulty: Difficulty::Hard,
                depends_on: vec!["task-root-a".to_string()],
                allowed_write_paths: vec!["src/b.rs".to_string()],
            },
        ],
        vec!["src/a.rs".to_string(), "src/b.rs".to_string()],
        1,
        true,
    ))
    .await;

    let host_dep = Box::pin(ApprovedHost::start(ApprovedHostConfig::new(
        &fixture_dep.registry_dir,
        &fixture_dep.plans_dir,
        &fixture_dep.authority_dir,
        &fixture_dep.sessions_dir,
        &approval_id_dep,
    )))
    .await
    .expect("start host_dep");
    let session_dep = fixture_dep
        .controller
        .open_owned_session(BINDING_A, THREAD_UUID_A, &job_dep.job_id, 20_000)
        .expect("open session_dep");

    let rt_dep_before = fixture_dep
        .policy_store
        .get_job_runtime_readonly(BINDING_A, &job_dep.job_id)
        .expect("read rt_dep_before")
        .expect("rt_dep_before exists");

    let dep_err = Box::pin(session_dep.step(20_100))
        .await
        .expect_err("oversized downstream child must block before valid root A starts");
    assert_eq!(dep_err.code, NativeErrorCode::ValidationFailed);
    assert!(dep_err.message.contains("task-b"));
    assert!(dep_err.message.contains("4096 bytes"));
    assert_eq!(read_invocation_count(&fixture_dep.agy_count_file), 0);
    let rt_dep = fixture_dep
        .policy_store
        .get_job_runtime_readonly(BINDING_A, &job_dep.job_id)
        .expect("read rt_dep")
        .expect("rt_dep exists");
    assert_eq!(
        rt_dep, rt_dep_before,
        "controller runtime state must remain unchanged on downstream preflight refusal"
    );
    assert!(rt_dep.lease.is_none());
    assert_eq!(rt_dep.total_task_attempts_reserved, 0);
    assert_eq!(rt_dep.total_corrections_reserved, 0);
    assert!(rt_dep.pending_effects.is_empty());
    assert!(rt_dep.role_usage.values().all(|usage| {
        usage.calls_reserved == 0 && usage.calls_completed == 0 && usage.total_runtime_ms == 0
    }));

    // Seed pre-stop root A receipt + durable stop_requested, proving Stopped reconciliation works
    // even when unstarted downstream `task-b` has an oversized legacy instruction.
    let host_client_dep = ApprovedHostClient::new(
        Arc::clone(&fixture_dep.authority),
        &fixture_dep.sessions_dir,
        BINDING_A,
        THREAD_UUID_A,
    )
    .expect("create host_client_dep");
    host_client_dep
        .dispatch_task(&approval_id_dep, "task-root-a")
        .await
        .expect("dispatch pre-stop task-root-a");
    let stop_a_deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if host_client_dep
            .get_task(&approval_id_dep, "task-root-a")
            .await
            .expect("poll pre-stop task-root-a")
            .status
            == TaskExecutionStatus::ReadyForReview
        {
            break;
        }
        assert!(
            Instant::now() < stop_a_deadline,
            "timed out waiting for pre-stop task-root-a ReadyForReview"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    fixture_dep
        .policy_store
        .mutate_job_runtime_for_operator(BINDING_A, &job_dep.job_id, |rt| {
            rt.stop_requested = true;
            Ok(())
        })
        .expect("set stop_requested on job_dep");

    let stopped_rep = Box::pin(session_dep.step(20_500))
        .await
        .expect("stopped legacy oversized job must reconcile without error");
    assert_eq!(stopped_rep.status, CoworkerControllerPhase1Status::Stopped);
    assert!(stopped_rep.dispatched_task_ids.is_empty());
    assert_eq!(read_invocation_count(&fixture_dep.agy_count_file), 1);

    session_dep.release().expect("release session_dep");
    host_dep.shutdown().await.expect("shutdown host_dep");

    // 5. Prior pending host `TaskDispatchIntent` WITHOUT `TaskRunReceipt` on an oversized legacy
    //    task (`4,097`B): `dispatch_task` returns `HostError::MutationOutcomeUncertain` (never
    //    `Validation`), host `task-receipts.json` bytes stay unchanged, zero provider runs occur,
    //    and controller `step` retains `EffectUnknown` (`BlockedOnUnknownEffect`) rather than
    //    promoting the prior uncertain attempt to `ReconciledNotExecuted` or `RefusedBeforeEffect`.
    let fixture_unk = setup_fixture();
    let oversized_4097_instr = format!(
        "CTRL_TASK_B {}",
        "u".repeat(threadlane_core::MAX_INSTRUCTION_LEN + 1 - 12)
    );
    assert_eq!(
        oversized_4097_instr.len(),
        threadlane_core::MAX_INSTRUCTION_LEN + 1
    );
    let (job_unk, approval_id_unk) = Box::pin(orchestrate_and_freeze_job(
        &fixture_unk,
        "brf_ctrl_pending_unknown",
        "plan-ctrl-pending-unknown",
        vec![PlanTask {
            id: "task-b".to_string(),
            title: "Oversized Pending Unknown Root".to_string(),
            instruction: oversized_4097_instr,
            role: WorkflowRole::Coder,
            difficulty: Difficulty::Hard,
            depends_on: Vec::new(),
            allowed_write_paths: vec!["src/b.rs".to_string()],
        }],
        vec!["src/b.rs".to_string()],
        1,
        true,
    ))
    .await;

    let host_unk = Box::pin(ApprovedHost::start(ApprovedHostConfig::new(
        &fixture_unk.registry_dir,
        &fixture_unk.plans_dir,
        &fixture_unk.authority_dir,
        &fixture_unk.sessions_dir,
        &approval_id_unk,
    )))
    .await
    .expect("start host_unk");
    let session_unk = fixture_unk
        .controller
        .open_owned_session(BINDING_A, THREAD_UUID_A, &job_unk.job_id, 30_000)
        .expect("open session_unk");
    let adm_unk = session_unk
        .verify_admission()
        .expect("verify_admission on job_unk");

    let approved_unk_b = adm_unk
        .approval
        .tasks
        .iter()
        .find(|t| t.task_id == "task-b")
        .expect("approved task-b");
    let profile_unk_b = approved_unk_b.profile_id.clone().expect("profile_id");
    let cmd_key_unk_b = approved_unk_b.command_key.clone().expect("command_key");

    let session_dir_unk = fixture_unk.sessions_dir.join(&approval_id_unk);
    let manifest_unk =
        threadlane_host::read_host_manifest_optional(&session_dir_unk, &adm_unk.approval)
            .expect("read manifest_unk")
            .expect("manifest_unk must exist");
    let binding_unk = manifest_unk.session_binding_id.expect("session_binding_id");

    // Seed prior controller reservation (`ReservedBeforeEffect`) + host `TaskDispatchIntent` without receipt.
    let _ = fixture_unk
        .workflow_store
        .reserve_task_outbox(&ReserveTaskOutboxRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            job_id: job_unk.job_id.clone(),
            expected_job_revision: adm_unk.job.job_revision,
            expected_ledger_revision: None,
            request_key: cmd_key_unk_b.clone(),
            kind: OutboxDispatchKind::TaskAttemptDispatch,
            task_id: "task-b".to_string(),
            role: WorkflowRole::Coder,
            subject_digest: approved_unk_b.task_digest.clone(),
            dependency_input_commit: None,
            dependency_input_tree_sha: None,
            accepted_predecessor_receipts: Vec::new(),
        })
        .expect("seed prior outbox reservation");
    let eff_id_unk = deterministic_prefixed_id(
        "threadlane-native:coworker-controller-effect:v1",
        "eff",
        &[&job_unk.job_id, "task-b", &cmd_key_unk_b],
    );
    fixture_unk
        .policy_store
        .mutate_job_runtime_for_operator(BINDING_A, &job_unk.job_id, |rt| {
            rt.reserve_role_call(WorkflowRole::Coder)?;
            rt.total_task_attempts_reserved = 1;
            rt.pending_effects.insert(
                eff_id_unk.clone(),
                PendingControllerEffect {
                    effect_id: eff_id_unk.clone(),
                    request_key: cmd_key_unk_b.clone(),
                    job_id: job_unk.job_id.clone(),
                    task_id: Some("task-b".to_string()),
                    attempt_no: Some(1),
                    kind: PendingControllerEffectKind::CoderDispatch,
                    status: PendingControllerEffectStatus::ReservedBeforeEffect,
                    subject_digest: approved_unk_b.task_digest.clone(),
                    external_correlation_id: None,
                    created_at_ms: 29_900,
                    updated_at_ms: 29_900,
                    diagnostic: None,
                },
            );
            Ok(())
        })
        .expect("seed prior PendingControllerEffect");

    let _ = threadlane_host::record_task_dispatch_intent(
        &session_dir_unk,
        &adm_unk.approval,
        &binding_unk,
        threadlane_host::TaskDispatchIntent {
            task_id: "task-b".to_string(),
            task_digest: approved_unk_b.task_digest.clone(),
            profile_id: profile_unk_b,
            command_key: cmd_key_unk_b.clone(),
            execution_binding: None,
            recorded_at_ms: 1_700_000_300_000,
        },
    )
    .expect("record prior host TaskDispatchIntent without receipt");

    let host_ledger_path = session_dir_unk.join(threadlane_host::TASK_RECEIPTS_FILENAME);
    let host_ledger_before = fs::read(&host_ledger_path).expect("read host task-receipts.json");

    let host_client_unk = ApprovedHostClient::new(
        Arc::clone(&fixture_unk.authority),
        &fixture_unk.sessions_dir,
        BINDING_A,
        THREAD_UUID_A,
    )
    .expect("open host_client_unk");
    let direct_err = host_client_unk
        .dispatch_task(&approval_id_unk, "task-b")
        .await
        .expect_err("dispatch_task with existing intent must remain outcome-uncertain");
    assert!(matches!(
        direct_err,
        threadlane_host::HostError::MutationOutcomeUncertain { .. }
    ));
    assert_eq!(
        fs::read(&host_ledger_path).expect("read host task-receipts.json after direct_err"),
        host_ledger_before
    );
    assert_eq!(read_invocation_count(&fixture_unk.agy_count_file), 0);

    for step_ts in [30_100u64, 30_200u64] {
        let unk_rep = Box::pin(session_unk.step(step_ts)).await.expect(
            "controller step must reconcile prior pending intent to BlockedOnUnknownEffect",
        );
        assert_eq!(
            unk_rep.status,
            CoworkerControllerPhase1Status::BlockedOnUnknownEffect
        );
        assert!(unk_rep.dispatched_task_ids.is_empty());
        assert_eq!(read_invocation_count(&fixture_unk.agy_count_file), 0);
        assert_eq!(
            fs::read(&host_ledger_path)
                .expect("read host task-receipts.json after controller step"),
            host_ledger_before,
            "host task-receipts.json must remain byte-for-byte unchanged"
        );

        let job_unk_after = fixture_unk
            .workflow_store
            .read_binding_ledger(BINDING_A)
            .expect("read ledger after unk_rep")
            .jobs
            .into_iter()
            .find(|j| j.job_id == job_unk.job_id)
            .expect("job_unk exists");
        assert_eq!(
            job_unk_after.tasks[0].stage,
            CoworkerTaskStage::EffectUnknown
        );
        assert_eq!(
            job_unk_after.outbox[0].status,
            OutboxDispatchStatus::EffectUnknown,
            "prior uncertain attempt must never promote to ReconciledNotExecuted"
        );

        let rt_unk_after = fixture_unk
            .policy_store
            .get_job_runtime_readonly(BINDING_A, &job_unk.job_id)
            .expect("read rt_unk_after")
            .expect("rt_unk_after exists");
        assert_eq!(
            rt_unk_after.pending_effects[&eff_id_unk].status,
            PendingControllerEffectStatus::EffectUnknown,
            "prior uncertain effect must never promote to RefusedBeforeEffect"
        );
    }

    session_unk.release().expect("release session_unk");
    host_unk.shutdown().await.expect("shutdown host_unk");
}

fn external_resolution_record_request(
    fixture: &ControllerTestFixture,
    job: &CoworkerJobRecord,
    scope: threadlane_native::external_resolution::ExternalResolutionScope,
    request_key: &str,
) -> threadlane_native::external_resolution::RecordExternalResolutionRequest {
    threadlane_native::external_resolution::RecordExternalResolutionRequest {
        binding_id: BINDING_A.to_string(),
        thread_uuid: THREAD_UUID_A.to_string(),
        canonical_repo_root: fixture.repo_dir.clone(),
        target: threadlane_native::external_resolution::ExternalResolutionTarget::Job {
            job_id: job.job_id.clone(),
        },
        scope,
        expected_ledger_revision: None,
        expected_job_revision: None,
        expected_runtime_revision: None,
        request_key: request_key.to_string(),
        created_at_ms: 1,
        explanation: "controller boundary external resolution test".to_string(),
        evidence_refs: Vec::new(),
    }
}

fn external_resolution_service(
    fixture: &ControllerTestFixture,
) -> threadlane_native::external_resolution::ExternalResolutionService {
    threadlane_native::external_resolution::ExternalResolutionService::new(
        &fixture.plans_dir,
        &fixture.authority_dir,
        &fixture.registry_dir,
        Some(&fixture.sessions_dir),
    )
    .expect("open external resolution service")
}

fn record_external_resolution(
    fixture: &ControllerTestFixture,
    request: &threadlane_native::external_resolution::RecordExternalResolutionRequest,
) -> threadlane_native::external_resolution::ExternalResolutionReceipt {
    let service = external_resolution_service(fixture);
    let preview = service.preview(request).expect("external resolution preview");
    service
        .record(
            request,
            &threadlane_native::external_resolution::ExternalResolutionConfirmation {
                trusted_local_attested: true,
                thread_uuid: THREAD_UUID_A.to_string(),
                preview_digest: preview.preview_digest,
            },
        )
        .expect("record external resolution receipt")
}

fn external_resolution_outbox_request(
    job: &CoworkerJobRecord,
    task_id: &str,
    request_key: &str,
) -> ReserveTaskOutboxRequest {
    ReserveTaskOutboxRequest {
        binding_id: BINDING_A.to_string(),
        thread_uuid: THREAD_UUID_A.to_string(),
        job_id: job.job_id.clone(),
        expected_job_revision: job.job_revision,
        expected_ledger_revision: None,
        request_key: request_key.to_string(),
        kind: OutboxDispatchKind::TaskAttemptDispatch,
        task_id: task_id.to_string(),
        role: WorkflowRole::Coder,
        subject_digest: "a".repeat(64),
        dependency_input_commit: None,
        dependency_input_tree_sha: None,
        accepted_predecessor_receipts: Vec::new(),
    }
}

#[tokio::test]
async fn external_resolution_full_job_blocks_admission_and_every_outbox_reservation() {
    let fixture = setup_fixture();
    let tasks = vec![
        PlanTask {
            id: "task-ext-root-a".to_string(),
            title: "External Root A".to_string(),
            instruction: "CTRL_TASK_A implement src/a.rs".to_string(),
            role: WorkflowRole::Coder,
            difficulty: Difficulty::Hard,
            depends_on: Vec::new(),
            allowed_write_paths: vec!["src/a.rs".to_string()],
        },
        PlanTask {
            id: "task-ext-root-b".to_string(),
            title: "External Root B".to_string(),
            instruction: "CTRL_TASK_B implement src/b.rs".to_string(),
            role: WorkflowRole::Coder,
            difficulty: Difficulty::Hard,
            depends_on: Vec::new(),
            allowed_write_paths: vec!["src/b.rs".to_string()],
        },
    ];
    let (job, _) = create_planned_approved_and_frozen_job(
        &fixture,
        "brf_external_full",
        "plan-external-full",
        tasks,
        BTreeMap::new(),
        vec!["src/a.rs".to_string(), "src/b.rs".to_string()],
        2,
        ExecutionBudgets::default(),
        ControllerRoleExecutionBudget::default_for_role(WorkflowRole::Coder, 4),
    )
    .await;

    let request = external_resolution_record_request(
        &fixture,
        &job,
        threadlane_native::external_resolution::ExternalResolutionScope::FullJob,
        "req_external_full",
    );
    let receipt = record_external_resolution(&fixture, &request);
    assert_eq!(receipt.binding_id, BINDING_A);
    assert_eq!(receipt.thread_uuid, THREAD_UUID_A);

    let err = fixture
        .controller
        .open_owned_session(BINDING_A, THREAD_UUID_A, &job.job_id, 20_000)
        .expect_err("full-job receipt must close controller admission");
    assert_eq!(err.code, NativeErrorCode::UnsupportedCapability);
    assert!(err.message().contains(receipt.receipt_id.as_str()));

    let reserve_err = fixture
        .workflow_store
        .reserve_task_outbox(&external_resolution_outbox_request(
            &job,
            "task-ext-root-a",
            "req_external_full_dispatch",
        ))
        .expect_err("full-job receipt must refuse every covered outbox reservation");
    assert_eq!(reserve_err.code, NativeErrorCode::UnsupportedCapability);

    let ledger = fixture
        .workflow_store
        .read_binding_ledger(BINDING_A)
        .expect("read ledger after refusals");
    assert_eq!(ledger.external_resolutions, vec![receipt]);
    assert_eq!(
        ledger.jobs[0].job_revision, job.job_revision,
        "refused dispatch must not advance the job revision"
    );
    assert!(ledger.jobs[0].outbox.is_empty());
}

#[tokio::test]
async fn external_resolution_partial_scope_fences_only_the_covered_task() {
    let fixture = setup_fixture();
    let tasks = vec![
        PlanTask {
            id: "task-ext-partial-a".to_string(),
            title: "External Partial A".to_string(),
            instruction: "CTRL_TASK_A implement src/a.rs".to_string(),
            role: WorkflowRole::Coder,
            difficulty: Difficulty::Hard,
            depends_on: Vec::new(),
            allowed_write_paths: vec!["src/a.rs".to_string()],
        },
        PlanTask {
            id: "task-ext-partial-b".to_string(),
            title: "External Partial B".to_string(),
            instruction: "CTRL_TASK_B implement src/b.rs".to_string(),
            role: WorkflowRole::Coder,
            difficulty: Difficulty::Hard,
            depends_on: Vec::new(),
            allowed_write_paths: vec!["src/b.rs".to_string()],
        },
    ];
    let (job, approval_id) = create_planned_approved_and_frozen_job(
        &fixture,
        "brf_external_partial",
        "plan-external-partial",
        tasks,
        BTreeMap::new(),
        vec!["src/a.rs".to_string(), "src/b.rs".to_string()],
        2,
        ExecutionBudgets::default(),
        ControllerRoleExecutionBudget::default_for_role(WorkflowRole::Coder, 4),
    )
    .await;

    let request = external_resolution_record_request(
        &fixture,
        &job,
        threadlane_native::external_resolution::ExternalResolutionScope::Tasks {
            task_ids: vec!["task-ext-partial-a".to_string()],
        },
        "req_external_partial",
    );
    let receipt = record_external_resolution(&fixture, &request);

    let covered_err = fixture
        .workflow_store
        .reserve_task_outbox(&external_resolution_outbox_request(
            &job,
            "task-ext-partial-a",
            "req_external_partial_dispatch",
        ))
        .expect_err("covered task must be refused");
    assert_eq!(covered_err.code, NativeErrorCode::UnsupportedCapability);

    // Advance unrelated root B through the genuine bounded controller/host path. The controller
    // must skip covered A, dispatch only B, and leave the immutable receipt untouched.
    let host = ApprovedHost::start(ApprovedHostConfig::new(
        &fixture.registry_dir,
        &fixture.plans_dir,
        &fixture.authority_dir,
        &fixture.sessions_dir,
        &approval_id,
    ))
    .await
    .expect("start ApprovedHost for unrelated task advancement");
    let session = fixture
        .controller
        .open_owned_session(BINDING_A, THREAD_UUID_A, &job.job_id, 20_000)
        .expect("partial receipt must not block unrelated controller admission");

    let deadline = Instant::now() + Duration::from_secs(10);
    let mut step_ts = 20_100u64;
    loop {
        let report = session.step(step_ts).await.expect("controller step");
        step_ts += 100;
        let observed_b = report
            .tasks
            .iter()
            .find(|task| task.task_id == "task-ext-partial-b")
            .expect("task B observation");
        let observed_a = report
            .tasks
            .iter()
            .find(|task| task.task_id == "task-ext-partial-a")
            .expect("task A observation");
        assert_ne!(
            observed_a.stage,
            CoworkerTaskStage::ReadyForReview,
            "covered task A must never be dispatched or advanced"
        );
        if observed_b.stage == CoworkerTaskStage::ReadyForReview {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for unrelated task B to advance: {report:?}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    session.release().expect("release controller session");
    host.shutdown().await.expect("shutdown ApprovedHost");

    // B's advancement changes the job revision and therefore stales the original receipt snapshot,
    // but historical coverage of A remains blocking without rebuilding a preview or re-recording.
    let ledger = fixture
        .workflow_store
        .read_binding_ledger(BINDING_A)
        .expect("read ledger after unrelated advancement");
    assert_eq!(ledger.external_resolutions, vec![receipt.clone()]);
    let job_after = ledger
        .jobs
        .iter()
        .find(|candidate| candidate.job_id == job.job_id)
        .expect("job after unrelated advancement")
        .clone();
    assert_ne!(job_after.job_revision, job.job_revision);
    let task_b_after = job_after
        .tasks
        .iter()
        .find(|task| task.task_id == "task-ext-partial-b")
        .expect("task B after advancement");
    assert_eq!(task_b_after.stage, CoworkerTaskStage::ReadyForReview);
    let task_a_after = job_after
        .tasks
        .iter()
        .find(|task| task.task_id == "task-ext-partial-a")
        .expect("task A after advancement");
    assert_ne!(task_a_after.stage, CoworkerTaskStage::ReadyForReview);
    assert!(
        job_after
            .outbox
            .iter()
            .all(|outbox| outbox.task_id != "task-ext-partial-a"),
        "covered task must not receive an outbox reservation after unrelated advancement"
    );
    assert!(
        job_after
            .outbox
            .iter()
            .any(|outbox| outbox.task_id == "task-ext-partial-b"),
        "unrelated task progress must be preserved"
    );

    let covered_after_advance = fixture
        .workflow_store
        .reserve_task_outbox(&external_resolution_outbox_request(
            &job_after,
            "task-ext-partial-a",
            "req_external_partial_dispatch_after_advance",
        ))
        .expect_err("covered task must stay refused after an unrelated job revision advance");
    assert_eq!(
        covered_after_advance.code,
        NativeErrorCode::UnsupportedCapability
    );

    let reopened = fixture
        .controller
        .open_owned_session(BINDING_A, THREAD_UUID_A, &job.job_id, step_ts + 100)
        .expect("unrelated job and read-only status must remain accessible");
    reopened
        .release()
        .expect("release reopened controller session");

    let final_ledger = fixture
        .workflow_store
        .read_binding_ledger(BINDING_A)
        .expect("read final ledger");
    assert_eq!(final_ledger.external_resolutions, vec![receipt]);
    let final_job = final_ledger
        .jobs
        .iter()
        .find(|candidate| candidate.job_id == job_after.job_id)
        .expect("final job");
    assert!(
        final_job
            .outbox
            .iter()
            .all(|outbox| outbox.task_id != "task-ext-partial-a"),
        "stale-after-advance refusal must not create a covered outbox entry"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn external_resolution_record_blocks_direct_dispatch_that_starts_after_the_receipt() {
    let fixture = setup_fixture();
    let tasks = vec![PlanTask {
        id: "task-ext-race-a".to_string(),
        title: "External race A".to_string(),
        instruction: "CTRL_TASK_A implement src/a.rs".to_string(),
        role: WorkflowRole::Coder,
        difficulty: Difficulty::Hard,
        depends_on: Vec::new(),
        allowed_write_paths: vec!["src/a.rs".to_string()],
    }];
    let (job, approval_id) = create_planned_approved_and_frozen_job(
        &fixture,
        "brf_external_race_record_first",
        "plan-external-race-record-first",
        tasks,
        BTreeMap::new(),
        vec!["src/a.rs".to_string()],
        1,
        ExecutionBudgets::default(),
        ControllerRoleExecutionBudget::default_for_role(WorkflowRole::Coder, 4),
    )
    .await;
    let request = external_resolution_record_request(
        &fixture,
        &job,
        threadlane_native::external_resolution::ExternalResolutionScope::FullJob,
        "req_external_race_record_first",
    );
    let service = external_resolution_service(&fixture);
    let preview = service.preview(&request).expect("preview race receipt");
    let approval = fixture
        .authority
        .get_for_operator(&approval_id)
        .expect("read race approval");
    let subject_digest = approval.subject_digest.clone();
    let ledger_path = fixture
        .workflow_store
        .ledger_path_for_binding(BINDING_A)
        .expect("race ledger path");

    let (entered_tx, entered_rx) = mpsc::channel::<()>();
    let (release_tx, release_rx) = mpsc::channel::<()>();
    let release_rx = Arc::new(Mutex::new(release_rx));
    let probe_service = external_resolution_service(&fixture).with_post_persist_probe(
        move |_ledger_path: &Path| -> Result<(), threadlane_native::NativeError> {
            entered_tx.send(()).map_err(|_| {
                threadlane_native::NativeError::persistence_uncertain(
                    "race probe entry channel closed",
                )
            })?;
            let release = release_rx.lock().map_err(|_| {
                threadlane_native::NativeError::persistence_uncertain("race probe lock poisoned")
            })?;
            release.recv_timeout(Duration::from_secs(5)).map_err(|_| {
                threadlane_native::NativeError::persistence_uncertain(
                    "race probe release timed out",
                )
            })?;
            Ok(())
        },
    );
    let request_for_record = request.clone();
    let preview_digest = preview.preview_digest.clone();
    let record_handle = tokio::task::spawn_blocking(move || {
        probe_service.record(
            &request_for_record,
            &threadlane_native::external_resolution::ExternalResolutionConfirmation {
                trusted_local_attested: true,
                thread_uuid: THREAD_UUID_A.to_string(),
                preview_digest,
            },
        )
    });

    tokio::time::timeout(
        Duration::from_secs(5),
        tokio::task::spawn_blocking(move || entered_rx.recv_timeout(Duration::from_secs(5))),
    )
    .await
    .expect("post-persist probe entry timed out")
    .expect("join post-persist probe waiter")
    .expect("post-persist probe entered");

    let effect_counter = Arc::new(AtomicUsize::new(0));
    let dispatch_counter = Arc::clone(&effect_counter);
    let dispatch_authority = Arc::clone(&fixture.authority);
    let dispatch_approval_id = approval_id.clone();
    let mut dispatch_handle = tokio::spawn(async move {
        threadlane_native::with_guarded_native_dispatch(
            &dispatch_authority,
            BINDING_A,
            THREAD_UUID_A,
            &dispatch_approval_id,
            "task-ext-race-a",
            Some(&subject_digest),
            move || async move {
                dispatch_counter.fetch_add(1, Ordering::SeqCst);
                Ok::<(), threadlane_native::GuardedDispatchError>(())
            },
        )
        .await
    });

    assert!(
        tokio::time::timeout(Duration::from_millis(75), &mut dispatch_handle)
            .await
            .is_err(),
        "guarded dispatch must wait while the winning record still holds its advancement guard"
    );

    release_tx
        .send(())
        .expect("release the post-persist probe");
    let receipt = tokio::time::timeout(Duration::from_secs(5), record_handle)
        .await
        .expect("record should finish after probe release")
        .expect("join record task")
        .expect("record external resolution");
    let dispatch_result = tokio::time::timeout(Duration::from_secs(5), dispatch_handle)
        .await
        .expect("dispatch should finish after receipt commit")
        .expect("join dispatch task");
    let dispatch_error = dispatch_result.expect_err("winning receipt must refuse later dispatch");
    assert!(matches!(
        dispatch_error,
        threadlane_native::GuardedDispatchError::PreEffect { code, .. }
            if code == threadlane_native::dispatch_guard::EXTERNAL_RESOLUTION_DISPATCH_BLOCKED
    ));
    assert_eq!(
        effect_counter.load(Ordering::SeqCst),
        0,
        "no fake effect may start after the winning receipt"
    );

    let ledger_bytes = fs::read(&ledger_path).expect("read ledger after winning record");
    let ledger = fixture
        .workflow_store
        .read_binding_ledger(BINDING_A)
        .expect("read ledger after winning record");
    assert_eq!(ledger.external_resolutions, vec![receipt]);
    assert_eq!(
        fs::read(&ledger_path).expect("reread ledger after refused dispatch"),
        ledger_bytes,
        "refused later dispatch must leave the immutable receipt bytes unchanged"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn external_resolution_record_waits_for_an_already_admitted_direct_dispatch_guard() {
    let fixture = setup_fixture();
    let tasks = vec![PlanTask {
        id: "task-ext-race-b".to_string(),
        title: "External race B".to_string(),
        instruction: "CTRL_TASK_A implement src/a.rs".to_string(),
        role: WorkflowRole::Coder,
        difficulty: Difficulty::Hard,
        depends_on: Vec::new(),
        allowed_write_paths: vec!["src/a.rs".to_string()],
    }];
    let (job, approval_id) = create_planned_approved_and_frozen_job(
        &fixture,
        "brf_external_race_dispatch_first",
        "plan-external-race-dispatch-first",
        tasks,
        BTreeMap::new(),
        vec!["src/a.rs".to_string()],
        1,
        ExecutionBudgets::default(),
        ControllerRoleExecutionBudget::default_for_role(WorkflowRole::Coder, 4),
    )
    .await;
    let request = external_resolution_record_request(
        &fixture,
        &job,
        threadlane_native::external_resolution::ExternalResolutionScope::FullJob,
        "req_external_race_dispatch_first",
    );
    let service = external_resolution_service(&fixture);
    let preview = service.preview(&request).expect("preview race receipt");
    let approval = fixture
        .authority
        .get_for_operator(&approval_id)
        .expect("read race approval");
    let subject_digest = approval.subject_digest.clone();
    let ledger_path = fixture
        .workflow_store
        .ledger_path_for_binding(BINDING_A)
        .expect("race ledger path");

    let (op_entered_tx, op_entered_rx) = tokio::sync::oneshot::channel::<()>();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
    let effect_counter = Arc::new(AtomicUsize::new(0));
    let dispatch_counter = Arc::clone(&effect_counter);
    let dispatch_authority = Arc::clone(&fixture.authority);
    let dispatch_approval_id = approval_id.clone();
    let dispatch_handle = tokio::spawn(async move {
        threadlane_native::with_guarded_native_dispatch(
            &dispatch_authority,
            BINDING_A,
            THREAD_UUID_A,
            &dispatch_approval_id,
            "task-ext-race-b",
            Some(&subject_digest),
            move || async move {
                op_entered_tx.send(()).map_err(|_| {
                    threadlane_native::GuardedDispatchError::pre_effect(
                        "io_error",
                        "race dispatch entry signal failed",
                    )
                })?;
                release_rx.await.map_err(|_| {
                    threadlane_native::GuardedDispatchError::pre_effect(
                        "io_error",
                        "race dispatch release signal failed",
                    )
                })?;
                dispatch_counter.fetch_add(1, Ordering::SeqCst);
                Ok::<(), threadlane_native::GuardedDispatchError>(())
            },
        )
        .await
    });

    tokio::time::timeout(Duration::from_secs(5), op_entered_rx)
        .await
        .expect("guarded dispatch op did not start")
        .expect("guarded dispatch op entry signal");

    let request_for_record = request.clone();
    let preview_digest = preview.preview_digest.clone();
    let service_for_record = external_resolution_service(&fixture);
    let mut record_handle = tokio::task::spawn_blocking(move || {
        service_for_record.record(
            &request_for_record,
            &threadlane_native::external_resolution::ExternalResolutionConfirmation {
                trusted_local_attested: true,
                thread_uuid: THREAD_UUID_A.to_string(),
                preview_digest,
            },
        )
    });

    assert!(
        tokio::time::timeout(Duration::from_millis(75), &mut record_handle)
            .await
            .is_err(),
        "record must not commit while the already-admitted dispatch holds the advancement guard"
    );

    release_tx
        .send(())
        .expect("release the admitted guarded dispatch");
    tokio::time::timeout(Duration::from_secs(5), dispatch_handle)
        .await
        .expect("admitted dispatch should finish")
        .expect("join admitted dispatch")
        .expect("admitted fake effect may win before the receipt");
    assert_eq!(
        effect_counter.load(Ordering::SeqCst),
        1,
        "the already-admitted fake effect must have run exactly once"
    );

    let receipt = tokio::time::timeout(Duration::from_secs(5), record_handle)
        .await
        .expect("record should finish after the dispatch guard is released")
        .expect("join record task")
        .expect("record external resolution after admitted dispatch");
    let ledger_bytes = fs::read(&ledger_path).expect("read ledger after record");
    let ledger = fixture
        .workflow_store
        .read_binding_ledger(BINDING_A)
        .expect("read ledger after record");
    assert_eq!(ledger.external_resolutions, vec![receipt]);

    let later_counter = Arc::new(AtomicUsize::new(0));
    let later_effect_counter = Arc::clone(&later_counter);
    let later_dispatch = threadlane_native::with_guarded_native_dispatch(
        &fixture.authority,
        BINDING_A,
        THREAD_UUID_A,
        &approval_id,
        "task-ext-race-b",
        Some(&approval.subject_digest),
        move || async move {
            later_effect_counter.fetch_add(1, Ordering::SeqCst);
            Ok::<(), threadlane_native::GuardedDispatchError>(())
        },
    )
    .await;
    let later_error = later_dispatch.expect_err("winning receipt must block the next dispatch");
    assert!(matches!(
        later_error,
        threadlane_native::GuardedDispatchError::PreEffect { code, .. }
            if code == threadlane_native::dispatch_guard::EXTERNAL_RESOLUTION_DISPATCH_BLOCKED
    ));
    assert_eq!(
        later_counter.load(Ordering::SeqCst),
        0,
        "a winning receipt must prevent every later fake effect"
    );
    assert_eq!(effect_counter.load(Ordering::SeqCst), 1);
    assert_eq!(
        fs::read(&ledger_path).expect("reread ledger after later refusal"),
        ledger_bytes,
        "later refused dispatch must not rewrite the winning receipt"
    );
}

#[tokio::test]
async fn external_resolution_full_job_blocks_execution_and_driver_but_preserves_readonly_status() {
    let fixture = setup_fixture();
    let tasks = vec![PlanTask {
        id: "task-ext-effect-a".to_string(),
        title: "External effect A".to_string(),
        instruction: "CTRL_TASK_A implement src/a.rs".to_string(),
        role: WorkflowRole::Coder,
        difficulty: Difficulty::Hard,
        depends_on: Vec::new(),
        allowed_write_paths: vec!["src/a.rs".to_string()],
    }];
    let (job, approval_id) = create_planned_approved_and_frozen_job(
        &fixture,
        "brf_external_effect_gate",
        "plan-external-effect-gate",
        tasks,
        BTreeMap::new(),
        vec!["src/a.rs".to_string()],
        1,
        ExecutionBudgets::default(),
        ControllerRoleExecutionBudget::default_for_role(WorkflowRole::Coder, 4),
    )
    .await;
    let request = external_resolution_record_request(
        &fixture,
        &job,
        threadlane_native::external_resolution::ExternalResolutionScope::FullJob,
        "req_external_effect_gate",
    );
    let receipt = record_external_resolution(&fixture, &request);

    let runtime = fixture
        .policy_store
        .get_job_runtime_readonly(BINDING_A, &job.job_id)
        .expect("read frozen runtime")
        .expect("frozen runtime exists");
    let envelope_digest = runtime.envelope.envelope_digest.clone();

    let state_dir = fixture.root.join("execution-gateway-state");
    fs::create_dir_all(&state_dir).expect("create execution gateway state dir");
    fs::set_permissions(&state_dir, fs::Permissions::from_mode(0o700))
        .expect("chmod execution gateway state dir 0700");

    let gateway = threadlane_native::CoworkerExecutionGateway::new(
        &state_dir,
        &fixture.registry_dir,
        &fixture.plans_dir,
        &fixture.authority_dir,
        &fixture.sessions_dir,
        BINDING_A,
        THREAD_UUID_A,
    )
    .expect("create execution gateway");

    let preview_error = gateway
        .preview_execution(&threadlane_native::PreviewExecutionParams {
            job_id: job.job_id.clone(),
        })
        .expect_err("full-job receipt must refuse execution preview");
    assert_eq!(preview_error.code(), NativeErrorCode::UnsupportedCapability);
    assert!(preview_error.message().contains(receipt.receipt_id.as_str()));

    let start_error = gateway
        .start_execution(&threadlane_native::StartExecutionParams {
            job_id: job.job_id.clone(),
            expected_envelope_digest: envelope_digest,
        })
        .await
        .expect_err("full-job receipt must refuse execution start before owner spawn");
    assert_eq!(start_error.code(), NativeErrorCode::UnsupportedCapability);
    assert!(start_error.message().contains(receipt.receipt_id.as_str()));

    let agy_count_before = read_invocation_count(&fixture.agy_count_file);
    let mut sessions_before: Vec<_> = fs::read_dir(&fixture.sessions_dir)
        .expect("read sessions before driver refusals")
        .map(|entry| {
            entry
                .expect("session entry")
                .file_name()
                .to_string_lossy()
                .into_owned()
        })
        .collect();
    sessions_before.sort();
    let head_before = fs::read_to_string(fixture.repo_dir.join(".git/HEAD")).expect("read HEAD");
    let driver = threadlane_native::coworker_driver::CoworkerCodeDriver::open_from_dirs(
        &state_dir,
        &fixture.registry_dir,
        &fixture.plans_dir,
        &fixture.authority_dir,
        &fixture.sessions_dir,
    )
    .expect("create code driver");

    let run_error = tokio::time::timeout(
        Duration::from_secs(5),
        driver.run_owner(
            BINDING_A,
            THREAD_UUID_A,
            &job.job_id,
            threadlane_native::coworker_driver::CoworkerDriverRunOptions::default(),
        ),
    )
    .await
    .expect("driver run-owner should refuse promptly")
    .expect_err("full-job receipt must refuse driver run-owner");
    assert_eq!(run_error.code(), NativeErrorCode::UnsupportedCapability);
    assert!(run_error.message().contains(receipt.receipt_id.as_str()));

    let ensure_error = tokio::time::timeout(
        Duration::from_secs(5),
        driver.ensure_owner(
            BINDING_A,
            THREAD_UUID_A,
            &job.job_id,
            Some(threadlane_native::coworker_driver::CoworkerDriverRunOptions::default()),
        ),
    )
    .await
    .expect("driver ensure should refuse promptly")
    .expect_err("full-job receipt must refuse driver ensure");
    assert_eq!(ensure_error.code(), NativeErrorCode::UnsupportedCapability);
    assert!(ensure_error.message().contains(receipt.receipt_id.as_str()));

    let status = tokio::time::timeout(
        Duration::from_secs(5),
        driver.status(BINDING_A, THREAD_UUID_A, &job.job_id),
    )
    .await
    .expect("read-only driver status should remain accessible")
    .expect("read-only driver status");
    assert_eq!(status.job_id, job.job_id);
    assert_eq!(status.approval_id, approval_id);
    assert_eq!(
        status.owner_state,
        threadlane_native::coworker_driver::CoworkerDriverOwnerState::NoActiveOwner
    );
    assert!(!status.blocked_on_unknown_effect);

    assert_eq!(
        read_invocation_count(&fixture.agy_count_file),
        agy_count_before,
        "execution/driver refusals must not invoke a provider"
    );
    let mut sessions_after: Vec<_> = fs::read_dir(&fixture.sessions_dir)
        .expect("read sessions after driver refusals")
        .map(|entry| {
            entry
                .expect("session entry")
                .file_name()
                .to_string_lossy()
                .into_owned()
        })
        .collect();
    sessions_after.sort();
    assert_eq!(
        sessions_after, sessions_before,
        "execution/driver refusals must not create host session state"
    );
    assert_eq!(
        fs::read_to_string(fixture.repo_dir.join(".git/HEAD")).expect("read HEAD after refusals"),
        head_before,
        "execution/driver refusals must not mutate Git"
    );

    let ledger = fixture
        .workflow_store
        .read_binding_ledger(BINDING_A)
        .expect("read ledger after execution/driver refusals");
    assert_eq!(ledger.external_resolutions, vec![receipt]);
}

#[path = "support/coworker_correction_execution.rs"]
mod coworker_correction_execution;

#[path = "support/coworker_code_driver.rs"]
mod coworker_code_driver;

#[path = "support/coworker_check_failure.rs"]
mod coworker_check_failure;

#[path = "support/coworker_execution_gateway.rs"]
mod coworker_execution_gateway;

#[path = "support/coworker_observation.rs"]
mod coworker_observation;
