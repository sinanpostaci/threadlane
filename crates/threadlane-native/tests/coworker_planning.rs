//! Focused behavioral tests for [`CoworkerPlanningService::orchestrate_brief_to_plan`],
//! real `PlanStore` / [`ScopedWorkflow`] DAG creation and publication from model output,
//! frozen [`WorkBrief`] / `job_revision` linkage and concurrent drift refusal, multi-round
//! Architect consultation, deterministic command-journal recovery, and fail-closed admission
//! guards.
//!
//! Note: These tests use disposable local Git repositories and a local fake Antigravity script
//! supervised by `threadlane_workers::run_advisory_worker_with_structured_result` (never a paid
//! provider).

use std::fs::{self, File};
use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tempfile::TempDir;
use threadlane_authority::AuthorityStore;
use threadlane_native::coworker_workflow::{
    CanonicalProjectBaseRef, CoworkerJobPhase, CoworkerJobRecord, CoworkerPlanningService,
    CoworkerWorkflowStore, CreateCoworkerJobRequest, CreatePlanningGrantRequest,
    OrchestrateWorkBriefPlanRequest, OrchestratorPlanningDecision, PlanningAdviceGrant,
    PlanningArtifactOutcomeKind, PlanningCallBudget, PlanningReadOnlyCapabilities,
    PlanningReservationStatus, WorkBrief, WorkBriefKind, WorkflowHoldReason,
};
use threadlane_native::profile_flow::EXECUTION_POLICY_SCHEMA_VERSION;
use threadlane_native::{
    Difficulty, ExecutionPolicy, NativeErrorCode, PolicyAllowRule, RegistryStore, ToolAdapter,
    WorkflowProfile, WorkflowRole,
};
use threadlane_workers::{
    resolve_exact_commit_sha, resolve_exact_tree_sha, run_sanitized_git,
    DEFAULT_GIT_COMMAND_TIMEOUT_MS,
};
use threadlane_workflow::{
    ModelChoice, ModelDefinition, PlanStatus, PlanStore, RegistryChange, RoleAssignment,
    ScopedWorkflow, ToolDefinition,
};

const THREAD_UUID: &str = "018f5a30-2222-7000-8000-000000000002";
const BINDING_ID: &str = "bnd_coworker_orchestrator_planning";

#[allow(dead_code)]
struct OrchestratorTestFixture {
    _tmp: TempDir,
    root_dir: PathBuf,
    repo_dir: PathBuf,
    project_base: CanonicalProjectBaseRef,
    registry_dir: PathBuf,
    plans_dir: PathBuf,
    authority_dir: PathBuf,
    artifacts_dir: PathBuf,
    agy_count_file: PathBuf,
    agy_mode_file: PathBuf,
    agy_prompts_dir: PathBuf,
    registry: Arc<RegistryStore>,
    plans: Arc<PlanStore>,
    workflow: ScopedWorkflow,
    authority: AuthorityStore,
    store: CoworkerWorkflowStore,
    service: CoworkerPlanningService,
}

fn init_sample_git_repo(repo_dir: &Path) -> CanonicalProjectBaseRef {
    fs::create_dir_all(repo_dir.join("src")).expect("create repo src");
    fs::write(
        repo_dir.join("src/lib.rs"),
        "pub mod exporter;\npub mod metrics;\npub mod parser;\n",
    )
    .expect("write src/lib.rs");
    fs::write(
        repo_dir.join("src/parser.rs"),
        "pub struct ParsedFrame {\n    pub version: u8,\n    pub payload_len: u16,\n}\n",
    )
    .expect("write src/parser.rs");
    fs::write(
        repo_dir.join("src/metrics.rs"),
        "pub fn record_frame_count(count: u64) -> u64 {\n    count\n}\n",
    )
    .expect("write src/metrics.rs");
    fs::write(
        repo_dir.join("src/exporter.rs"),
        "pub fn export_batch() -> bool {\n    true\n}\n",
    )
    .expect("write src/exporter.rs");

    for args in [
        &["init", "--quiet"][..],
        &["config", "user.email", "test@acme-telemetry.local"][..],
        &["config", "user.name", "Acme Telemetry Test"][..],
        &["add", "."][..],
        &["commit", "-m", "initial sample telemetry repo", "--quiet"][..],
    ] {
        run_sanitized_git(repo_dir, args, DEFAULT_GIT_COMMAND_TIMEOUT_MS).expect("git init step");
    }
    let commit = resolve_exact_commit_sha(repo_dir, "HEAD", DEFAULT_GIT_COMMAND_TIMEOUT_MS)
        .expect("resolve HEAD commit");
    let _tree = resolve_exact_tree_sha(repo_dir, &commit, DEFAULT_GIT_COMMAND_TIMEOUT_MS)
        .expect("resolve HEAD tree");
    CanonicalProjectBaseRef::resolve_and_validate(repo_dir, &commit)
        .expect("resolve canonical project base")
}

fn write_fake_agy_planning_script(
    script_path: &Path,
    count_file: &Path,
    mode_file: &Path,
    prompts_dir: &Path,
) {
    if let Some(parent) = script_path.parent() {
        fs::create_dir_all(parent).expect("create script parent dir");
    }
    fs::create_dir_all(prompts_dir).expect("create prompts dir");

    let script = format!(
        r#"#!/bin/sh
set -eu
COUNT_FILE="{count_file}"
MODE_FILE="{mode_file}"
PROMPTS_DIR="{prompts_dir}"

c=0
if [ -f "$COUNT_FILE" ]; then
    c=$(cat "$COUNT_FILE")
fi
c=$((c + 1))
printf "%s" "$c" > "$COUNT_FILE"

MODE="valid_plan"
if [ -f "$MODE_FILE" ]; then
    MODE=$(cat "$MODE_FILE")
fi

MODEL="gemini-4-argon-high"
PROMPT=""
while [ "$#" -gt 0 ]; do
    case "$1" in
        --model)
            MODEL="$2"
            shift 2
            ;;
        --prompt|-p)
            PROMPT="$2"
            shift 2
            ;;
        *)
            PROMPT="$1"
            shift 1
            ;;
    esac
done

printf "%s" "$PROMPT" > "$PROMPTS_DIR/prompt_$c.txt"
printf '{{"event":"init","init":{{"session_id":"agy-plan-sess-%s","model":"%s"}}}}\n' "$c" "$MODEL"

emit_raw_response() {{
    resp_file="$PROMPTS_DIR/resp_$c.json"
    cat > "$resp_file"
    esc=$(sed 's/\\/\\\\/g; s/"/\\"/g' "$resp_file" | tr '\n' ' ')
    printf '{{"event":"result","result":{{"status":"SUCCESS","response":"%s"}}}}\n' "$esc"
    exit 0
}}

emit_fenced_response() {{
    resp_file="$PROMPTS_DIR/resp_$c.json"
    cat > "$resp_file"
    esc=$(sed 's/\\/\\\\/g; s/"/\\"/g' "$resp_file" | tr '\n' ' ')
    printf '{{"event":"result","result":{{"status":"SUCCESS","response":"```json\\n%s\\n```"}}}}\n' "$esc"
    exit 0
}}

if [ "$MODE" = "custom_file" ]; then
    if [ -f "$PROMPTS_DIR/hook_$c.sh" ]; then
        /bin/sh "$PROMPTS_DIR/hook_$c.sh"
    elif [ -f "$PROMPTS_DIR/hook.sh" ]; then
        /bin/sh "$PROMPTS_DIR/hook.sh"
    fi
    if [ -f "$PROMPTS_DIR/custom_fenced_$c.json" ]; then
        emit_fenced_response < "$PROMPTS_DIR/custom_fenced_$c.json"
    fi
    if [ -f "$PROMPTS_DIR/custom_$c.json" ]; then
        emit_raw_response < "$PROMPTS_DIR/custom_$c.json"
    fi
    emit_raw_response < "$PROMPTS_DIR/custom.json"
fi

if [ "$MODE" = "malformed_json" ]; then
    printf '{{"event":"result","result":{{"status":"SUCCESS","response":"{{\\"outcome\\":\\"plan\\",\\"schema_version\\":1,\\"unknown_field\\":true}}"}}}}\n'
    exit 0
fi

emit_raw_response <<'EOF'
{{"outcome":"plan","schema_version":1,"summary":"Compile two independent Coder tasks and one dependent exporter Coder task for sample-telemetry-service with explicit interface contracts and >256-byte structured payload.","tasks":[{{"task_id":"task_parse","title":"Implement binary frame header parser","description":"Parse binary telemetry frames in src/parser.rs and expose the stable ParsedFrame struct for downstream batch export.","role":"coder","difficulty":"medium","depends_on":[],"allowed_write_paths":["src/parser.rs"],"owned_interfaces":["iface_parsed_frame"],"consumed_interfaces":[]}},{{"task_id":"task_metrics","title":"Add frame counter helper","description":"Update src/metrics.rs to track total parsed telemetry frames without overlapping parser or exporter files.","role":"coder","difficulty":"easy","depends_on":[],"allowed_write_paths":["src/metrics.rs"],"owned_interfaces":[],"consumed_interfaces":[]}},{{"task_id":"task_export","title":"Wire batch exporter to ParsedFrame","description":"Consume iface_parsed_frame in src/exporter.rs and emit framed telemetry batches to the configured sink.","role":"coder","difficulty":"hard","depends_on":["task_parse"],"allowed_write_paths":["src/exporter.rs"],"owned_interfaces":["iface_export_sink"],"consumed_interfaces":["iface_parsed_frame"]}}],"check_suggestions":[{{"check_id":"chk_telemetry_unit","command_summary":"cargo test --workspace","required":true}}],"unconfirmed_proposal_notes":["Keep optional pushgateway exporter out of scope until human approval."],"resolved_open_questions":[],"deferred_proposals":[]}}
EOF
"#,
        count_file = count_file.display(),
        mode_file = mode_file.display(),
        prompts_dir = prompts_dir.display(),
    );

    let mut f = File::create(script_path).expect("create fake-agy");
    f.write_all(script.as_bytes()).expect("write fake-agy");
    f.sync_all().expect("sync fake-agy");
    fs::set_permissions(script_path, fs::Permissions::from_mode(0o755))
        .expect("chmod 0755 fake-agy");
}

#[allow(dead_code)]
impl OrchestratorTestFixture {
    fn new() -> Self {
        let tmp = TempDir::new().expect("tempdir");
        let root_dir = fs::canonicalize(tmp.path()).expect("canonicalize temp root");

        let repo_dir = root_dir.join("sample-telemetry-service");
        let project_base = init_sample_git_repo(&repo_dir);

        let bin_dir = root_dir.join("bin");
        let fake_agy_exe = bin_dir.join("fake-agy");
        let agy_count_file = root_dir.join("agy-invocations.txt");
        let agy_mode_file = root_dir.join("agy-mode.txt");
        let agy_prompts_dir = root_dir.join("agy-prompts");
        write_fake_agy_planning_script(
            &fake_agy_exe,
            &agy_count_file,
            &agy_mode_file,
            &agy_prompts_dir,
        );

        let registry_dir = root_dir.join("registry-store");
        let plans_dir = root_dir.join("plans-store");
        let authority_dir = root_dir.join("authority-store");
        let artifacts_dir = root_dir.join("planning-artifacts");

        let registry = Arc::new(RegistryStore::open(&registry_dir).expect("open registry"));
        let plans = Arc::new(PlanStore::open(&plans_dir).expect("open plans"));
        let workflow = ScopedWorkflow::new(
            Arc::clone(&registry),
            Arc::clone(&plans),
            BINDING_ID.to_string(),
        )
        .expect("create scoped workflow");
        let authority =
            AuthorityStore::open(&authority_dir, Arc::clone(&registry), Arc::clone(&plans))
                .expect("open authority store");

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
                RegistryChange::UpsertModels {
                    models: vec![ModelDefinition {
                        id: "model-argon".to_string(),
                        tool_id: "tool-agy".to_string(),
                        label: "Gemini 4 Argon High".to_string(),
                        model_arg: "gemini-4-argon-high".to_string(),
                        efforts: vec!["high".to_string()],
                        allow_no_effort: false,
                    }],
                },
            )
            .expect("upsert models");

        registry
            .apply(
                2,
                RegistryChange::UpsertProfile {
                    profile: WorkflowProfile {
                        decision_advisor: None,
                        id: "profile-main".to_string(),
                        name: "Main Planning Profile".to_string(),
                        assignments: vec![
                            RoleAssignment {
                                role: WorkflowRole::Architect,
                                difficulty: None,
                                choice: ModelChoice {
                                    model_id: "model-argon".to_string(),
                                    effort: Some("high".to_string()),
                                },
                            },
                            RoleAssignment {
                                role: WorkflowRole::Orchestrator,
                                difficulty: None,
                                choice: ModelChoice {
                                    model_id: "model-argon".to_string(),
                                    effort: Some("high".to_string()),
                                },
                            },
                            RoleAssignment {
                                role: WorkflowRole::Coder,
                                difficulty: None,
                                choice: ModelChoice {
                                    model_id: "model-argon".to_string(),
                                    effort: Some("high".to_string()),
                                },
                            },
                        ],
                    },
                },
            )
            .expect("upsert profile");

        let store = CoworkerWorkflowStore::new(&plans_dir).expect("open coworker store");
        let service = CoworkerPlanningService::new(store.clone(), &registry_dir, &artifacts_dir)
            .expect("create planning service");

        Self {
            _tmp: tmp,
            root_dir,
            repo_dir,
            project_base,
            registry_dir,
            plans_dir,
            authority_dir,
            artifacts_dir,
            agy_count_file,
            agy_mode_file,
            agy_prompts_dir,
            registry,
            plans,
            workflow,
            authority,
            store,
            service,
        }
    }

    fn invocation_count(&self) -> u32 {
        match fs::read_to_string(&self.agy_count_file) {
            Ok(s) => s.trim().parse::<u32>().unwrap_or(0),
            Err(_) => 0,
        }
    }

    fn set_mode(&self, mode: &str) {
        fs::write(&self.agy_mode_file, mode).expect("write mode file");
    }

    fn write_custom_response(&self, json: &str) {
        self.set_mode("custom_file");
        fs::write(self.agy_prompts_dir.join("custom.json"), json).expect("write custom.json");
    }

    fn write_custom_call_response(&self, call_index: u32, json: &str, fenced: bool) {
        self.set_mode("custom_file");
        let name = if fenced {
            format!("custom_fenced_{call_index}.json")
        } else {
            format!("custom_{call_index}.json")
        };
        fs::write(self.agy_prompts_dir.join(name), json).expect("write numbered custom json");
    }

    fn read_captured_prompt(&self, call_index: u32) -> String {
        fs::read_to_string(
            self.agy_prompts_dir
                .join(format!("prompt_{call_index}.txt")),
        )
        .expect("read captured prompt")
    }

    fn reopen_all(
        &self,
    ) -> (
        CoworkerWorkflowStore,
        CoworkerPlanningService,
        ScopedWorkflow,
    ) {
        let registry = Arc::new(RegistryStore::open(&self.registry_dir).expect("reopen registry"));
        let plans = Arc::new(PlanStore::open(&self.plans_dir).expect("reopen plans"));
        let workflow = ScopedWorkflow::new(registry, plans, BINDING_ID.to_string())
            .expect("reopen scoped workflow");
        let store = CoworkerWorkflowStore::new(&self.plans_dir).expect("reopen store");
        let service =
            CoworkerPlanningService::new(store.clone(), &self.registry_dir, &self.artifacts_dir)
                .expect("reopen service");
        (store, service, workflow)
    }

    fn default_policy() -> ExecutionPolicy {
        ExecutionPolicy {
            schema_version: EXECUTION_POLICY_SCHEMA_VERSION,
            policy_id: "pol-orchestrator-planning".to_string(),
            allow_rules: vec![PolicyAllowRule {
                tool_id: "tool-agy".to_string(),
                model_id: "model-argon".to_string(),
                efforts: vec!["high".to_string()],
                allow_no_effort: false,
                roles: vec![WorkflowRole::Architect, WorkflowRole::Orchestrator],
            }],
        }
    }

    fn create_grant_with_budget(
        &self,
        request_key: &str,
        budget: PlanningCallBudget,
        expires_at_ms: u64,
    ) -> PlanningAdviceGrant {
        let reg_rev = self
            .registry
            .snapshot()
            .expect("registry snapshot")
            .revision;
        self.store
            .create_planning_grant(&CreatePlanningGrantRequest {
                binding_id: BINDING_ID.to_string(),
                thread_uuid: THREAD_UUID.to_string(),
                request_key: request_key.to_string(),
                expected_ledger_revision: None,
                project_base: self.project_base.clone(),
                profile_id: "profile-main".to_string(),
                registry_revision: reg_rev,
                policy: Self::default_policy(),
                capabilities: PlanningReadOnlyCapabilities::strict_read_only(),
                budget,
                created_at_ms: 1_700_000_000_000,
                expires_at_ms,
                operator_attested: true,
            })
            .expect("create planning grant")
    }

    fn create_default_grant(&self, request_key: &str) -> PlanningAdviceGrant {
        self.create_grant_with_budget(
            request_key,
            PlanningCallBudget {
                max_total_calls: 8,
                max_orchestrator_calls: 4,
                max_architect_calls: 4,
                max_timeout_ms_per_call: 30_000,
                max_output_bytes_per_call: 65_536,
            },
            1_800_000_000_000,
        )
    }

    fn create_initial_job(
        &self,
        request_key: &str,
        confirmed_requirements: Vec<String>,
        open_questions: Vec<String>,
        proposed_additions: Vec<String>,
    ) -> CoworkerJobRecord {
        let brief = WorkBrief {
            brief_id: format!("brf_{request_key}"),
            binding_id: BINDING_ID.to_string(),
            thread_uuid: THREAD_UUID.to_string(),
            project_base: self.project_base.clone(),
            kind: WorkBriefKind::NewJob,
            summary: "Build binary frame parser, metrics counter, and batch exporter".to_string(),
            confirmed_requirements,
            proposed_additions,
            assumptions: vec![
                "Preserve deterministic zero-copy header layout on sample-telemetry-service"
                    .to_string(),
            ],
            open_questions,
            accepted_architect_decisions: Vec::new(),
        };
        self.store
            .create_or_amend_job(&CreateCoworkerJobRequest {
                binding_id: BINDING_ID.to_string(),
                thread_uuid: THREAD_UUID.to_string(),
                request_key: request_key.to_string(),
                expected_ledger_revision: None,
                work_brief: brief,
                amended_target_task_ids: Vec::new(),
                amended_write_paths: Vec::new(),
                amended_interfaces: Vec::new(),
            })
            .expect("create initial job")
    }

    fn plan_req(
        grant_id: &str,
        job_id: &str,
        request_key: &str,
        max_architect_rounds: u32,
    ) -> OrchestrateWorkBriefPlanRequest {
        OrchestrateWorkBriefPlanRequest {
            binding_id: BINDING_ID.to_string(),
            thread_uuid: THREAD_UUID.to_string(),
            grant_id: grant_id.to_string(),
            job_id: job_id.to_string(),
            request_key: request_key.to_string(),
            expected_ledger_revision: None,
            difficulty: None,
            architect_difficulty: None,
            plan_id: None,
            expected_plan_revision: 0,
            architect_advice_ids: Vec::new(),
            max_architect_rounds,
            current_time_ms: 1_700_000_020_000,
        }
    }
}

#[tokio::test]
async fn orchestrator_plan_json_above_256_bytes_creates_and_publishes_dag_and_links_exact_provenance(
) {
    let fx = OrchestratorTestFixture::new();
    let grant = fx.create_default_grant("grant_plan_dag_1");
    let job = fx.create_initial_job(
        "job_plan_dag_1",
        vec![
            "Implement binary frame header parser in src/parser.rs".to_string(),
            "Add frame counter helper in src/metrics.rs".to_string(),
            "Wire batch exporter in src/exporter.rs to consume parsed frames".to_string(),
        ],
        Vec::new(),
        Vec::new(),
    );
    assert_eq!(job.phase, CoworkerJobPhase::Planning);
    assert_eq!(job.job_revision, 1);

    let req = OrchestrateWorkBriefPlanRequest {
        binding_id: BINDING_ID.to_string(),
        thread_uuid: THREAD_UUID.to_string(),
        grant_id: grant.grant_id.clone(),
        job_id: job.job_id.clone(),
        request_key: "orch_plan_dag_1".to_string(),
        expected_ledger_revision: None,
        difficulty: None,
        architect_difficulty: None,
        plan_id: None,
        expected_plan_revision: 0,
        architect_advice_ids: Vec::new(),
        max_architect_rounds: 0,
        current_time_ms: 1_700_000_010_000,
    };

    let outcome = fx
        .service
        .orchestrate_brief_to_plan(&fx.workflow, &req)
        .await
        .expect("orchestrate brief to plan");
    assert_eq!(fx.invocation_count(), 1);
    assert_eq!(
        outcome.reservation.status,
        PlanningReservationStatus::Completed
    );

    let OrchestratorPlanningDecision::Plan(ref published_outcome) = outcome.decision else {
        panic!("expected Plan decision, got {:?}", outcome.decision);
    };

    let artifact_id = outcome
        .reservation
        .report_artifact_id
        .as_deref()
        .expect("reservation report_artifact_id");
    let artifact = fx
        .service
        .read_planning_artifact(artifact_id)
        .expect("read persisted Orchestrator call artifact");
    assert!(
        artifact.output_byte_count.expect("output_byte_count") > 256,
        "expected >256-byte Orchestrator JSON payload"
    );
    let PlanningArtifactOutcomeKind::OrchestratorOutput {
        job_id: ref art_job_id,
        job_revision: art_job_rev,
        work_brief_digest: ref art_brief_digest,
        work_brief: ref art_brief,
        ..
    } = artifact.outcome
    else {
        panic!("expected OrchestratorOutput artifact outcome");
    };
    assert_eq!(art_job_id, &job.job_id);
    assert_eq!(art_job_rev, Some(1));
    assert_eq!(art_brief_digest, &job.work_brief_digest);
    assert_eq!(art_brief.as_ref(), Some(&job.work_brief));

    let fetched_plan = fx
        .workflow
        .get_plan(
            &published_outcome.published_plan.header.plan_id,
            None,
            None,
            None,
        )
        .expect("fetch published plan from ScopedWorkflow");
    assert_eq!(fetched_plan.header.revision, 1);
    assert_eq!(fetched_plan.header.status, PlanStatus::Published);
    assert_eq!(fetched_plan.header.profile_id, "profile-main");
    assert_eq!(
        fetched_plan.header.profile_revision,
        grant.registry_revision
    );
    assert_eq!(fetched_plan.tasks.len(), 3);
    assert_eq!(fetched_plan.tasks[0].id, "task_parse");
    assert_eq!(fetched_plan.tasks[0].role, WorkflowRole::Coder);
    assert_eq!(fetched_plan.tasks[0].difficulty, Difficulty::Medium);
    assert!(fetched_plan.tasks[0].depends_on.is_empty());
    assert_eq!(
        fetched_plan.tasks[0].allowed_write_paths,
        vec!["src/parser.rs"]
    );
    assert_eq!(fetched_plan.tasks[1].id, "task_metrics");
    assert_eq!(fetched_plan.tasks[1].difficulty, Difficulty::Easy);
    assert!(fetched_plan.tasks[1].depends_on.is_empty());
    assert_eq!(fetched_plan.tasks[2].id, "task_export");
    assert_eq!(fetched_plan.tasks[2].difficulty, Difficulty::Hard);
    assert_eq!(fetched_plan.tasks[2].depends_on, vec!["task_parse"]);
    assert_eq!(
        fetched_plan.tasks[2].allowed_write_paths,
        vec!["src/exporter.rs"]
    );

    let ledger = fx
        .store
        .read_binding_ledger(BINDING_ID)
        .expect("read ledger");
    let updated_job = ledger
        .jobs
        .iter()
        .find(|j| j.job_id == job.job_id)
        .expect("find updated job");
    assert_eq!(updated_job.phase, CoworkerJobPhase::AwaitingApproval);
    assert_eq!(updated_job.job_revision, 2);
    assert!(updated_job.approval_linkage.is_none());
    assert!(updated_job.tasks.is_empty());

    let pr = updated_job
        .planning_result
        .as_ref()
        .expect("linked PlanningResultRef");
    assert_eq!(pr, &published_outcome.planning_result);
    assert_eq!(pr.grant_id, grant.grant_id);
    assert_eq!(pr.reservation_id, outcome.reservation.reservation_id);
    assert_eq!(pr.request_key, "orch_plan_dag_1");
    assert_eq!(pr.run_id, artifact.run_id);
    assert_eq!(pr.report_artifact_id, artifact.artifact_id);
    assert_eq!(
        Some(pr.output_digest.as_str()),
        artifact.output_digest.as_deref()
    );
    assert_eq!(pr.work_brief_digest, job.work_brief_digest);
    assert_eq!(pr.project_base, fx.project_base);
    assert_eq!(pr.profile_id, "profile-main");
    assert_eq!(pr.registry_revision, grant.registry_revision);
    assert_eq!(pr.policy_digest, grant.policy_digest);
    assert_eq!(pr.plan_id, fetched_plan.header.plan_id);
    assert_eq!(pr.plan_revision, 1);
    assert_eq!(
        Some(pr.plan_digest.as_str()),
        fetched_plan.header.plan_digest.as_deref()
    );
    assert_eq!(
        pr.task_interfaces["task_parse"].owned_interfaces,
        vec!["iface_parsed_frame"]
    );
    assert_eq!(
        pr.task_interfaces["task_export"].consumed_interfaces,
        vec!["iface_parsed_frame"]
    );
}

#[tokio::test]
async fn orchestrator_reopen_and_repeat_request_returns_identical_published_plan_and_result_ref_with_single_invocation(
) {
    let fx = OrchestratorTestFixture::new();
    let grant = fx.create_default_grant("grant_plan_reopen_1");
    let job = fx.create_initial_job(
        "job_plan_reopen_1",
        vec![
            "Implement binary frame header parser in src/parser.rs".to_string(),
            "Add frame counter helper in src/metrics.rs".to_string(),
            "Wire batch exporter in src/exporter.rs to consume parsed frames".to_string(),
        ],
        Vec::new(),
        Vec::new(),
    );
    let mut req =
        OrchestratorTestFixture::plan_req(&grant.grant_id, &job.job_id, "orch_plan_reopen_1", 0);

    let first = fx
        .service
        .orchestrate_brief_to_plan(&fx.workflow, &req)
        .await
        .expect("first orchestrate call");
    assert_eq!(fx.invocation_count(), 1);
    let first_plan = first.require_published_plan().expect("first plan").clone();

    let (store2, service2, workflow2) = fx.reopen_all();
    req.current_time_ms = 1_700_000_030_000;
    let replayed = service2
        .orchestrate_brief_to_plan(&workflow2, &req)
        .await
        .expect("replayed orchestrate call after reopen");
    assert_eq!(fx.invocation_count(), 1);
    let replayed_plan = replayed.require_published_plan().expect("replayed plan");

    assert_eq!(replayed.reservation, first.reservation);
    assert_eq!(replayed_plan.published_plan, first_plan.published_plan);
    assert_eq!(replayed_plan.planning_result, first_plan.planning_result);
    assert_eq!(replayed_plan.published_plan.header.revision, 1);

    let list = workflow2.list_plans(None, None).expect("list plans");
    assert_eq!(list.plans.len(), 1);
    assert_eq!(list.plans[0].revision, 1);

    let ledger = store2.read_binding_ledger(BINDING_ID).expect("read ledger");
    let updated_job = ledger
        .jobs
        .iter()
        .find(|j| j.job_id == job.job_id)
        .expect("find job");
    assert_eq!(updated_job.job_revision, 2);
    assert_eq!(
        updated_job.planning_result.as_ref(),
        Some(&first_plan.planning_result)
    );
}

#[tokio::test]
async fn orchestrator_clarification_and_invalid_role_overlap_or_malformed_outputs_never_publish_or_link(
) {
    let fx = OrchestratorTestFixture::new();
    let grant = fx.create_default_grant("grant_clarify_and_guard_1");
    let job = fx.create_initial_job(
        "job_clarify_and_guard_1",
        vec!["Implement binary frame header parser in src/parser.rs".to_string()],
        vec!["Should unknown frame versions be dropped or quarantined?".to_string()],
        vec!["Add optional UDP multicast mirror".to_string()],
    );

    fx.write_custom_call_response(
        1,
        r#"{"outcome":"requires_human_clarification","schema_version":1,"summary":"Frame version error-handling policy must be clarified before compiling tasks.","questions":["Should unknown frame versions return a typed parse error or increment a drop counter?"],"referenced_files":["src/parser.rs"]}"#,
        false,
    );
    let clarify_req =
        OrchestratorTestFixture::plan_req(&grant.grant_id, &job.job_id, "orch_clarify_1", 0);
    let clarify_out = fx
        .service
        .orchestrate_brief_to_plan(&fx.workflow, &clarify_req)
        .await
        .expect("clarification outcome");
    assert!(clarify_out.require_published_plan().is_err());
    assert!(clarify_out.published_plan.is_none());
    assert!(clarify_out.planning_result.is_none());
    let OrchestratorPlanningDecision::RequiresHumanClarification(ref c) = clarify_out.decision
    else {
        panic!("expected RequiresHumanClarification");
    };
    assert_eq!(c.referenced_files, vec!["src/parser.rs"]);

    fx.write_custom_call_response(
        2,
        r#"{"outcome":"plan","schema_version":1,"summary":"Invalid reviewer task node.","tasks":[{"task_id":"task_rev","title":"Review parser","description":"Review src/parser.rs.","role":"reviewer","difficulty":"medium","depends_on":[],"allowed_write_paths":["src/parser.rs"]}]}"#,
        false,
    );
    fx.write_custom_call_response(
        3,
        r#"{"outcome":"plan","schema_version":1,"summary":"Overlapping parallel scopes.","tasks":[{"task_id":"task_a","title":"Edit parser","description":"Update src/parser.rs.","role":"coder","difficulty":"easy","depends_on":[],"allowed_write_paths":["src/parser.rs"]},{"task_id":"task_b","title":"Edit src tree","description":"Update src directory concurrently.","role":"coder","difficulty":"easy","depends_on":[],"allowed_write_paths":["src"]}]}"#,
        false,
    );
    for (key, call_idx) in [("orch_bad_role_2", 2u32), ("orch_overlap_3", 3u32)] {
        let bad_req = OrchestratorTestFixture::plan_req(&grant.grant_id, &job.job_id, key, 0);
        let err = fx
            .service
            .orchestrate_brief_to_plan(&fx.workflow, &bad_req)
            .await
            .expect_err("invalid plan output must fail closed");
        assert_eq!(err.code, NativeErrorCode::ValidationFailed);
        assert_eq!(fx.invocation_count(), call_idx);
    }

    fx.set_mode("malformed_json");
    let malformed_req =
        OrchestratorTestFixture::plan_req(&grant.grant_id, &job.job_id, "orch_malformed_4", 0);
    let err = fx
        .service
        .orchestrate_brief_to_plan(&fx.workflow, &malformed_req)
        .await
        .expect_err("malformed JSON must fail closed");
    assert_eq!(err.code, NativeErrorCode::ValidationFailed);
    assert_eq!(fx.invocation_count(), 4);
    assert!(fx
        .workflow
        .list_plans(None, None)
        .expect("list plans")
        .plans
        .is_empty());

    let ledger = fx.store.read_binding_ledger(BINDING_ID).expect("ledger");
    let unchanged_job = ledger
        .jobs
        .iter()
        .find(|j| j.job_id == job.job_id)
        .expect("find job");
    assert_eq!(unchanged_job.phase, CoworkerJobPhase::Planning);
    assert_eq!(unchanged_job.job_revision, 1);
    assert!(unchanged_job.planning_result.is_none());
    assert!(unchanged_job.approval_linkage.is_none());
    assert_eq!(
        unchanged_job.work_brief.proposed_additions,
        vec!["Add optional UDP multicast mirror"]
    );
}

#[tokio::test]
async fn orchestrator_mid_flight_brief_amendment_preserves_artifact_and_refuses_stale_plan_linkage()
{
    let fx = OrchestratorTestFixture::new();
    let grant = fx.create_default_grant("grant_mid_flight_amend_1");
    let job = fx.create_initial_job(
        "job_mid_flight_amend_1",
        vec!["Implement binary frame header parser in src/parser.rs".to_string()],
        Vec::new(),
        Vec::new(),
    );

    let in_flight_marker = fx.agy_prompts_dir.join("in_flight_1.marker");
    let resume_marker = fx.agy_prompts_dir.join("resume_1.marker");
    let hook_script = format!(
        "#!/bin/sh\nset -eu\n: > \"{}\"\ni=0\nwhile [ ! -f \"{}\" ] && [ \"$i\" -lt 250 ]; do\n  sleep 0.02\n  i=$((i + 1))\ndone\n",
        in_flight_marker.display(),
        resume_marker.display(),
    );
    fs::write(fx.agy_prompts_dir.join("hook_1.sh"), hook_script).expect("write hook_1.sh");
    fx.write_custom_call_response(
        1,
        r#"{"outcome":"plan","schema_version":1,"summary":"Plan compiled against initial pre-amendment brief.","tasks":[{"task_id":"task_parse","title":"Implement binary frame header parser","description":"Parse binary frames in src/parser.rs.","role":"coder","difficulty":"medium","depends_on":[],"allowed_write_paths":["src/parser.rs"]}]}"#,
        false,
    );
    let req = OrchestratorTestFixture::plan_req(
        &grant.grant_id,
        &job.job_id,
        "orch_mid_flight_amend_1",
        0,
    );

    let amend_fut = async {
        let deadline = Instant::now() + Duration::from_secs(5);
        while !in_flight_marker.exists() {
            assert!(Instant::now() < deadline, "timed out waiting for marker");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let mut amended_brief = job.work_brief.clone();
        amended_brief.brief_id = "brf_job_mid_flight_amend_2".to_string();
        amended_brief.kind = WorkBriefKind::Amendment {
            target_job_id: job.job_id.clone(),
            target_job_revision: 1,
            amendment_reason: "Switch to length-prefixed v2 frame header".to_string(),
        };
        amended_brief.confirmed_requirements =
            vec!["Implement length-prefixed v2 frame parser in src/parser.rs".to_string()];
        let amended_job = fx
            .store
            .create_or_amend_job(&CreateCoworkerJobRequest {
                binding_id: BINDING_ID.to_string(),
                thread_uuid: THREAD_UUID.to_string(),
                request_key: "job_mid_flight_amend_2".to_string(),
                expected_ledger_revision: None,
                work_brief: amended_brief,
                amended_target_task_ids: Vec::new(),
                amended_write_paths: vec!["src/parser.rs".to_string()],
                amended_interfaces: Vec::new(),
            })
            .expect("amend job while Orchestrator call is in flight");
        fs::write(&resume_marker, "go").expect("write resume marker");
        amended_job
    };

    let (orch_res, amended_job) = tokio::join!(
        fx.service.orchestrate_brief_to_plan(&fx.workflow, &req),
        amend_fut
    );
    let err = orch_res.expect_err("mid-flight amendment must refuse plan publication and linkage");
    assert_eq!(err.code, NativeErrorCode::ValidationFailed);
    assert_eq!(fx.invocation_count(), 1);
    assert!(fx
        .workflow
        .list_plans(None, None)
        .expect("list plans")
        .plans
        .is_empty());

    let ledger = fx.store.read_binding_ledger(BINDING_ID).expect("ledger");
    let stored_grant = ledger
        .planning_grants
        .iter()
        .find(|g| g.grant_id == grant.grant_id)
        .expect("find grant");
    assert_eq!(
        stored_grant.reservations[0].status,
        PlanningReservationStatus::Completed
    );
    let art_id = stored_grant.reservations[0]
        .report_artifact_id
        .as_deref()
        .expect("persisted artifact id");
    let artifact = fx.service.read_planning_artifact(art_id).expect("artifact");
    let PlanningArtifactOutcomeKind::OrchestratorOutput {
        job_id: ref art_job_id,
        job_revision: art_job_rev,
        work_brief_digest: ref art_digest,
        ..
    } = artifact.outcome
    else {
        panic!("expected OrchestratorOutput artifact");
    };
    assert_eq!(art_job_id, &job.job_id);
    assert_eq!(art_job_rev, Some(1));
    assert_eq!(art_digest, &job.work_brief_digest);

    let old_job = ledger
        .jobs
        .iter()
        .find(|j| j.job_id == job.job_id)
        .expect("old job");
    assert!(old_job.planning_result.is_none());
    assert!(old_job.holds[0].active);
    assert_eq!(
        old_job.holds[0].reason,
        WorkflowHoldReason::BriefAmendmentPending
    );
    let new_job = ledger
        .jobs
        .iter()
        .find(|j| j.job_id == amended_job.job_id)
        .expect("new amended job");
    assert!(new_job.planning_result.is_none());
}

#[tokio::test]
async fn orchestrator_multi_round_architect_consultation_and_round_cap_enforcement() {
    let fx = OrchestratorTestFixture::new();
    let grant = fx.create_default_grant("grant_multi_round_arch_1");
    let job = fx.create_initial_job(
        "job_multi_round_arch_1",
        vec![
            "Implement binary frame header parser in src/parser.rs".to_string(),
            "Wire batch exporter in src/exporter.rs to consume parsed frames".to_string(),
        ],
        vec!["Should ParsedFrame use a value header struct?".to_string()],
        vec!["Add optional Prometheus pushgateway sink".to_string()],
    );

    fx.write_custom_call_response(
        1,
        r#"{"outcome":"requires_architect_advice","schema_version":1,"summary":"Consult Architect on ParsedFrame ownership boundary.","questions":[{"question_id":"q_parsed_frame","question_summary":"Should ParsedFrame in src/parser.rs be a copyable header struct?","dependency_files":["src/parser.rs","src/exporter.rs"],"interfaces":["iface_parsed_frame"]}]}"#,
        false,
    );
    fx.write_custom_call_response(
        2,
        r#"{"schema_version":1,"decision_summary":"Keep ParsedFrame as a fixed-size header struct in src/parser.rs.","rationale":"Decouples frame header validation from exporter buffer lifetimes.","recommended_write_paths":["src/parser.rs","src/exporter.rs"],"interface_contracts":{"iface_parsed_frame":"pub struct ParsedFrame { pub version: u8, pub payload_len: u16 }"},"open_questions":[]}"#,
        false,
    );
    fx.write_custom_call_response(
        3,
        r#"{"outcome":"plan","schema_version":1,"summary":"Compile parser and exporter DAG incorporating Architect advice.","tasks":[{"task_id":"task_parse","title":"Implement ParsedFrame header parser","description":"Define copyable ParsedFrame in src/parser.rs per Architect contract.","role":"coder","difficulty":"medium","depends_on":[],"allowed_write_paths":["src/parser.rs"],"owned_interfaces":["iface_parsed_frame"],"consumed_interfaces":[]},{"task_id":"task_export","title":"Consume ParsedFrame in batch exporter","description":"Wire src/exporter.rs to iface_parsed_frame.","role":"coder","difficulty":"hard","depends_on":["task_parse"],"allowed_write_paths":["src/exporter.rs"],"owned_interfaces":[],"consumed_interfaces":["iface_parsed_frame"]}],"check_suggestions":[{"check_id":"chk_unit","command_summary":"cargo test","required":true}],"unconfirmed_proposal_notes":["Deferred pushgateway sink until human approval."],"resolved_open_questions":["Should ParsedFrame use a value header struct?"],"deferred_proposals":["Add optional Prometheus pushgateway sink"]}"#,
        false,
    );

    let req =
        OrchestratorTestFixture::plan_req(&grant.grant_id, &job.job_id, "orch_multi_round_1", 1);
    let outcome = fx
        .service
        .orchestrate_brief_to_plan(&fx.workflow, &req)
        .await
        .expect("multi-round orchestration succeeds");
    assert_eq!(fx.invocation_count(), 3);
    assert_eq!(outcome.consulted_architect_advice.len(), 1);
    let adv_id = &outcome.consulted_architect_advice[0].record.advice_id;
    let prompt_3 = fx.read_captured_prompt(3);
    assert!(prompt_3.contains(adv_id));
    assert!(prompt_3.contains("Keep ParsedFrame as a fixed-size header struct"));

    let pub_plan = outcome.require_published_plan().expect("published plan");
    assert_eq!(
        pub_plan.planning_result.architect_advice_ids,
        vec![adv_id.clone()]
    );
    assert_eq!(
        pub_plan.planning_result.resolved_open_questions,
        vec!["Should ParsedFrame use a value header struct?"]
    );
    assert_eq!(
        pub_plan.planning_result.deferred_proposals,
        vec!["Add optional Prometheus pushgateway sink"]
    );

    let job_cap = fx.create_initial_job(
        "job_multi_round_cap_2",
        vec!["Implement binary frame header parser in src/parser.rs".to_string()],
        Vec::new(),
        Vec::new(),
    );
    fx.write_custom_call_response(
        4,
        r#"{"outcome":"requires_architect_advice","schema_version":1,"summary":"Need Architect round 1.","questions":[{"question_id":"q_cap_1","question_summary":"Question 1 on src/parser.rs?","dependency_files":["src/parser.rs"],"interfaces":[]}]}"#,
        false,
    );
    fx.write_custom_call_response(
        5,
        r#"{"schema_version":1,"decision_summary":"Architect answer 1.","rationale":"Rationale 1.","recommended_write_paths":["src/parser.rs"],"interface_contracts":{},"open_questions":[]}"#,
        false,
    );
    fx.write_custom_call_response(
        6,
        r#"{"outcome":"requires_architect_advice","schema_version":1,"summary":"Still requesting Architect after round 1.","questions":[{"question_id":"q_cap_2","question_summary":"Follow-up question exceeding round cap?","dependency_files":["src/parser.rs"],"interfaces":[]}]}"#,
        false,
    );
    let cap_req = OrchestratorTestFixture::plan_req(
        &grant.grant_id,
        &job_cap.job_id,
        "orch_multi_round_cap_2",
        1,
    );
    let cap_err = fx
        .service
        .orchestrate_brief_to_plan(&fx.workflow, &cap_req)
        .await
        .expect_err("exceeding max_architect_rounds must fail closed");
    assert_eq!(cap_err.code, NativeErrorCode::ValidationFailed);
    assert_eq!(fx.invocation_count(), 6);
}

// Historical fixtures are built only inside disposable stores. Temporarily hiding
// the owner models the old missing admission check; real worker supervision still
// produces and validates the bounded receipt, reservation, and published revision.
struct LegacyCollision {
    fx: OrchestratorTestFixture,
    commands: threadlane_native::PlanningCommandStore,
    command: threadlane_native::PlanningCommandRecord,
    owner: CoworkerJobRecord,
    job: CoworkerJobRecord,
    artifact_path: PathBuf,
    artifact_bytes: Vec<u8>,
}

fn receipt_command_store(fx: &OrchestratorTestFixture) -> threadlane_native::PlanningCommandStore {
    let commands_dir = fx.root_dir.join("receipt-commands");
    fs::create_dir_all(&commands_dir).expect("create commands directory");
    fs::set_permissions(&commands_dir, fs::Permissions::from_mode(0o700))
        .expect("private commands directory");
    let ownership =
        threadlane_native::coworker_workflow::CoworkerJobOwnershipStore::new(&commands_dir)
            .expect("ownership store");
    threadlane_native::PlanningCommandStore::new(
        &commands_dir,
        fx.store.clone(),
        &fx.registry_dir,
        ownership,
    )
    .expect("command store")
    .with_artifacts_dir(&fx.artifacts_dir)
    .expect("artifact directory")
}

fn replace_fixture_job(fx: &OrchestratorTestFixture, replacement: &CoworkerJobRecord) {
    fx.store
        .transact_binding_ledger(BINDING_ID, None, |ledger| {
            *ledger
                .jobs
                .iter_mut()
                .find(|j| j.job_id == replacement.job_id)
                .expect("fixture job") = replacement.clone();
            Ok(threadlane_native::coworker_workflow::CoworkerLedgerMutationOutcome::Mutated(()))
        })
        .expect("replace historical fixture job");
}

async fn legacy_collision_fixture() -> LegacyCollision {
    legacy_collision_fixture_with_budget(false).await
}

async fn legacy_collision_fixture_with_budget(exhaust_after_receipt: bool) -> LegacyCollision {
    use threadlane_native::{EnqueuePlanningCommandRequest, PlanningCommandPayload};
    let fx = OrchestratorTestFixture::new();
    let grant = if exhaust_after_receipt {
        fx.create_grant_with_budget(
            "receipt-grant",
            PlanningCallBudget {
                max_total_calls: 2,
                max_orchestrator_calls: 2,
                max_architect_calls: 0,
                max_timeout_ms_per_call: 30_000,
                max_output_bytes_per_call: 65_536,
            },
            1_800_000_000_000,
        )
    } else {
        fx.create_default_grant("receipt-grant")
    };
    let original = fx.create_initial_job(
        "receipt-owner",
        vec!["Implement telemetry parser".into()],
        vec![],
        vec![],
    );
    let output = fx
        .service
        .orchestrate_brief_to_plan(
            &fx.workflow,
            &OrchestratorTestFixture::plan_req(
                &grant.grant_id,
                &original.job_id,
                "receipt-owner-plan",
                0,
            ),
        )
        .await
        .expect("owner plan");
    let original_result = output.planning_result.expect("owner result");
    let mut brief = original.work_brief.clone();
    brief.brief_id = "brf_receipt_amendment".into();
    brief.kind = WorkBriefKind::Amendment {
        target_job_id: original.job_id.clone(),
        target_job_revision: 2,
        amendment_reason: "Refine telemetry parser behavior".into(),
    };
    let job = fx
        .store
        .create_or_amend_job(&CreateCoworkerJobRequest {
            binding_id: BINDING_ID.into(),
            thread_uuid: THREAD_UUID.into(),
            request_key: "receipt-amendment".into(),
            expected_ledger_revision: None,
            work_brief: brief,
            amended_target_task_ids: vec![],
            amended_write_paths: vec!["src/parser.rs".into()],
            amended_interfaces: vec![],
        })
        .expect("amendment job");
    let owner = fx
        .store
        .read_binding_ledger(BINDING_ID)
        .expect("ledger")
        .jobs
        .into_iter()
        .find(|j| j.job_id == original.job_id)
        .expect("held owner");
    let mut hidden_owner = owner.clone();
    hidden_owner.planning_result = None;
    replace_fixture_job(&fx, &hidden_owner);
    let commands = receipt_command_store(&fx);
    let mut request =
        OrchestratorTestFixture::plan_req(&grant.grant_id, &job.job_id, "legacy-collision", 0);
    request.plan_id = Some(original_result.plan_id.clone());
    request.expected_plan_revision = 1;
    let command = commands
        .enqueue_command(&EnqueuePlanningCommandRequest::from_orchestrator(request))
        .expect("historical enqueue with missing owner visibility");
    let guard = commands
        .ownership_store()
        .try_acquire_job(
            &command.ownership_key().expect("key"),
            threadlane_native::current_wall_clock_ms().expect("clock"),
        )
        .expect("acquire")
        .into_guard()
        .expect("guard");
    commands
        .claim_running(&guard, BINDING_ID, THREAD_UUID, &command.command_id, None)
        .expect("claim");
    let PlanningCommandPayload::Orchestrator(request) = &command.payload else {
        panic!("orchestrator");
    };
    let historical_output = fx
        .service
        .orchestrate_brief_to_plan(&fx.workflow, request)
        .await
        .expect("historical output");
    let result = historical_output
        .planning_result
        .expect("historical result");
    assert_eq!(result.plan_id, original_result.plan_id);
    assert_eq!(result.plan_revision, 2);
    // Simulate the legacy crash window after publish but before linkage. Preserve
    // both original plan revisions, the immutable command, and the genuine receipt.
    replace_fixture_job(&fx, &job);
    replace_fixture_job(&fx, &owner);
    let command = commands
        .record_unknown(
            &guard,
            BINDING_ID,
            THREAD_UUID,
            &command.command_id,
            "legacy cross-job publication/link collision",
            None,
        )
        .expect("unknown");
    drop(guard);
    let artifact_path = fx
        .artifacts_dir
        .join(format!("{}.json", result.report_artifact_id));
    let artifact_bytes = fs::read(&artifact_path).expect("artifact bytes");
    assert_eq!(fx.invocation_count(), 2);
    LegacyCollision {
        fx,
        commands,
        command,
        owner,
        job,
        artifact_path,
        artifact_bytes,
    }
}

#[tokio::test]
async fn cross_job_plan_id_is_refused_before_enqueue_and_direct_service_call() {
    use threadlane_native::EnqueuePlanningCommandRequest;
    let legacy = legacy_collision_fixture().await;
    let mut request = match &legacy.command.payload {
        threadlane_native::PlanningCommandPayload::Orchestrator(r) => r.clone(),
        _ => unreachable!(),
    };
    request.request_key = "new-cross-job-refused".into();
    let before = legacy
        .fx
        .store
        .read_binding_ledger(BINDING_ID)
        .expect("before");
    let enqueue_error = legacy
        .commands
        .enqueue_command(&EnqueuePlanningCommandRequest::from_orchestrator(
            request.clone(),
        ))
        .expect_err("cross-job enqueue must fail");
    // Unknown commands block unrelated enqueue; verify admission in an empty command store.
    let fresh_commands_dir = legacy.fx.root_dir.join("fresh-refusal-commands");
    fs::create_dir_all(&fresh_commands_dir).expect("fresh commands directory");
    fs::set_permissions(&fresh_commands_dir, fs::Permissions::from_mode(0o700))
        .expect("private fresh commands directory");
    let fresh_ownership =
        threadlane_native::coworker_workflow::CoworkerJobOwnershipStore::new(&fresh_commands_dir)
            .expect("fresh ownership");
    let fresh_commands = threadlane_native::PlanningCommandStore::new(
        &fresh_commands_dir,
        legacy.fx.store.clone(),
        &legacy.fx.registry_dir,
        fresh_ownership,
    )
    .expect("fresh commands");
    let error = fresh_commands
        .enqueue_command(&EnqueuePlanningCommandRequest::from_orchestrator(
            request.clone(),
        ))
        .expect_err("owner admission must fail");
    assert!(error.message().contains("owned by another job"));
    assert!(!enqueue_error.message().is_empty());
    let error = legacy
        .fx
        .service
        .orchestrate_brief_to_plan(&legacy.fx.workflow, &request)
        .await
        .expect_err("direct owner admission must fail");
    assert!(error.message().contains("owned by another job"));
    assert_eq!(
        legacy
            .fx
            .store
            .read_binding_ledger(BINDING_ID)
            .expect("after"),
        before
    );
    assert_eq!(legacy.fx.invocation_count(), 2);
}

#[tokio::test]
async fn receipt_only_collision_recovery_preserves_history_and_reopens_without_provider_replay() {
    use threadlane_native::{
        PlanningCommandDriver, PlanningCommandResultRef, PlanningCommandStatus,
    };
    let legacy = legacy_collision_fixture().await;
    let old_plan_id = legacy
        .owner
        .planning_result
        .as_ref()
        .expect("owner result")
        .plan_id
        .clone();
    let old_r1 = legacy
        .fx
        .workflow
        .get_plan(&old_plan_id, Some(1), None, None)
        .expect("r1");
    let old_r2 = legacy
        .fx
        .workflow
        .get_plan(&old_plan_id, Some(2), None, None)
        .expect("r2");
    let before = legacy
        .fx
        .store
        .read_binding_ledger(BINDING_ID)
        .expect("ledger");
    // A read only command query cannot publish or link a plan.
    assert_eq!(
        legacy
            .commands
            .get_command(BINDING_ID, THREAD_UUID, &legacy.command.command_id)
            .expect("query")
            .expect("command")
            .status,
        PlanningCommandStatus::Unknown
    );
    assert_eq!(
        legacy
            .fx
            .store
            .read_binding_ledger(BINDING_ID)
            .expect("ledger after read"),
        before
    );
    let driver = PlanningCommandDriver::new(legacy.commands.clone(), legacy.fx.service.clone())
        .expect("driver");
    let outcome = driver
        .execute_command(BINDING_ID, THREAD_UUID, &legacy.command.command_id)
        .await
        .expect("recover exact receipt");
    assert!(outcome.reconciled_from_existing);
    let PlanningCommandResultRef::OrchestratorPlan {
        planning_result,
        artifact_verified,
        ..
    } = outcome.command.result_ref.as_ref().expect("result")
    else {
        panic!("plan");
    };
    assert!(*artifact_verified);
    assert_ne!(planning_result.plan_id, old_plan_id);
    assert_eq!(planning_result.plan_revision, 1);
    assert_eq!(
        planning_result.request_key,
        legacy.command.service_request_key
    );
    let artifact: threadlane_native::coworker_workflow::PersistedPlanningCallArtifact =
        serde_json::from_slice(&legacy.artifact_bytes).expect("artifact");
    assert_eq!(planning_result.report_artifact_id, artifact.artifact_id);
    assert_eq!(
        Some(&planning_result.output_digest),
        artifact.output_digest.as_ref()
    );
    assert_eq!(outcome.command.payload, legacy.command.payload);
    assert_eq!(
        outcome.command.command_digest,
        legacy.command.command_digest
    );
    let after = legacy
        .fx
        .store
        .read_binding_ledger(BINDING_ID)
        .expect("after");
    assert_eq!(after.planning_grants, before.planning_grants);
    assert_eq!(
        after
            .jobs
            .iter()
            .find(|j| j.job_id == legacy.owner.job_id)
            .expect("owner"),
        &legacy.owner
    );
    let job = after
        .jobs
        .iter()
        .find(|j| j.job_id == legacy.job.job_id)
        .expect("recovered job");
    assert_eq!(job.phase, CoworkerJobPhase::AwaitingApproval);
    assert!(job.approval_linkage.is_none());
    assert!(job.tasks.is_empty());
    assert_eq!(
        legacy
            .fx
            .workflow
            .get_plan(&old_plan_id, Some(1), None, None)
            .expect("r1 after"),
        old_r1
    );
    assert_eq!(
        legacy
            .fx
            .workflow
            .get_plan(&old_plan_id, Some(2), None, None)
            .expect("r2 after"),
        old_r2
    );
    assert_eq!(
        fs::read(&legacy.artifact_path).expect("artifact after"),
        legacy.artifact_bytes
    );
    let reopened = threadlane_native::PlanningCommandStore::open_existing(
        legacy.commands.commands_dir(),
        legacy.fx.store.clone(),
        &legacy.fx.registry_dir,
    )
    .expect("reopen commands")
    .with_artifacts_dir(&legacy.fx.artifacts_dir)
    .expect("artifacts");
    let (_, service, _) = legacy.fx.reopen_all();
    let reopened_driver = PlanningCommandDriver::new(reopened, service).expect("reopened driver");
    let duplicate = reopened_driver
        .execute_command(BINDING_ID, THREAD_UUID, &legacy.command.command_id)
        .await
        .expect("duplicate");
    assert!(duplicate.was_already_completed);
    assert_eq!(duplicate.command.result_ref, outcome.command.result_ref);
    assert_eq!(legacy.fx.invocation_count(), 2);
}

#[tokio::test]
async fn same_job_plan_updates_remain_supported() {
    use threadlane_native::EnqueuePlanningCommandRequest;
    let fx = OrchestratorTestFixture::new();
    let grant = fx.create_default_grant("same-job-grant");
    let job = fx.create_initial_job(
        "same-job",
        vec!["Implement telemetry parser".into()],
        vec![],
        vec![],
    );
    let first = fx
        .service
        .orchestrate_brief_to_plan(
            &fx.workflow,
            &OrchestratorTestFixture::plan_req(&grant.grant_id, &job.job_id, "same-job-first", 0),
        )
        .await
        .expect("first plan")
        .planning_result
        .expect("first result");
    let commands = receipt_command_store(&fx);
    let mut request =
        OrchestratorTestFixture::plan_req(&grant.grant_id, &job.job_id, "same-job-update", 0);
    request.plan_id = Some(first.plan_id.clone());
    request.expected_plan_revision = 1;
    let command = commands
        .enqueue_command(&EnqueuePlanningCommandRequest::from_orchestrator(request))
        .expect("same-job update enqueue");
    let driver = threadlane_native::PlanningCommandDriver::new(commands, fx.service.clone())
        .expect("driver");
    let outcome = driver
        .execute_command(BINDING_ID, THREAD_UUID, &command.command_id)
        .await
        .expect("same-job update");
    let threadlane_native::PlanningCommandResultRef::OrchestratorPlan {
        planning_result, ..
    } = outcome.command.result_ref.expect("result")
    else {
        panic!("plan");
    };
    assert_eq!(planning_result.plan_id, first.plan_id);
    assert_eq!(planning_result.plan_revision, 2);
    assert_eq!(fx.invocation_count(), 2);
}

#[tokio::test]
async fn receipt_recovery_rejects_missing_tampered_and_drifted_evidence_without_mutations() {
    use threadlane_native::coworker_workflow::{
        ApplyWorkflowHoldRequest, FrozenApprovalLinkage, PersistedPlanningCallArtifact,
    };
    use threadlane_native::{PlanningCommandDriver, PlanningCommandStatus};
    let legacy = legacy_collision_fixture().await;
    let driver = PlanningCommandDriver::new(legacy.commands.clone(), legacy.fx.service.clone())
        .expect("driver");
    for mode in [
        "missing",
        "tampered-output",
        "wrong-job",
        "wrong-scope",
        "missing-brief",
        "missing-revision",
        "changed-brief",
        "coherent-brief-tamper",
        "revision-drift",
        "held",
        "approved",
        "nonterminal",
        "superseded",
        "dirty-source",
    ] {
        let mut artifact: PersistedPlanningCallArtifact =
            serde_json::from_slice(&legacy.artifact_bytes).expect("artifact");
        match mode {
            "missing" => fs::remove_file(&legacy.artifact_path).expect("remove synthetic artifact"),
            "tampered-output" => {
                artifact
                    .verified_response
                    .as_mut()
                    .expect("response")
                    .text
                    .push(' ');
                fs::write(
                    &legacy.artifact_path,
                    serde_json::to_vec(&artifact).expect("serialize artifact"),
                )
                .expect("tamper synthetic artifact");
            }
            "wrong-scope" => {
                artifact.thread_uuid = "018f5a30-2222-7000-8000-000000000003".into();
                fs::write(
                    &legacy.artifact_path,
                    serde_json::to_vec(&artifact).expect("serialize artifact"),
                )
                .expect("wrong scope");
            }
            "wrong-job" | "missing-brief" | "missing-revision" => {
                let PlanningArtifactOutcomeKind::OrchestratorOutput {
                    job_id,
                    work_brief,
                    job_revision,
                    ..
                } = &mut artifact.outcome
                else {
                    panic!("output");
                };
                match mode {
                    "wrong-job" => *job_id = legacy.owner.job_id.clone(),
                    "missing-brief" => *work_brief = None,
                    _ => *job_revision = None,
                }
                artifact
                    .validate(&artifact.artifact_id)
                    .expect("structurally valid but insufficient receipt");
                fs::write(
                    &legacy.artifact_path,
                    serde_json::to_vec(&artifact).expect("serialize artifact"),
                )
                .expect("insufficient artifact");
            }
            "changed-brief" | "coherent-brief-tamper" | "revision-drift" => {
                let mut job = legacy.job.clone();
                job.job_revision += 1;
                if mode != "revision-drift" {
                    job.work_brief
                        .confirmed_requirements
                        .push("New unplanned requirement".into());
                    job.work_brief_digest = job.work_brief.digest().expect("changed brief digest");
                }
                if mode == "coherent-brief-tamper" {
                    let PlanningArtifactOutcomeKind::OrchestratorOutput {
                        work_brief,
                        work_brief_digest,
                        job_revision,
                        ..
                    } = &mut artifact.outcome
                    else {
                        panic!("output");
                    };
                    *work_brief = Some(job.work_brief.clone());
                    *work_brief_digest = job.work_brief_digest.clone();
                    *job_revision = Some(job.job_revision);
                    artifact
                        .validate(&artifact.artifact_id)
                        .expect("coherent metadata still cannot change command context");
                    fs::write(
                        &legacy.artifact_path,
                        serde_json::to_vec(&artifact).expect("serialize artifact"),
                    )
                    .expect("coherently changed artifact");
                }
                replace_fixture_job(&legacy.fx, &job);
            }
            "held" => {
                legacy
                    .fx
                    .store
                    .apply_workflow_hold(&ApplyWorkflowHoldRequest {
                        binding_id: BINDING_ID.into(),
                        thread_uuid: THREAD_UUID.into(),
                        job_id: legacy.job.job_id.clone(),
                        expected_job_revision: legacy.job.job_revision,
                        expected_ledger_revision: None,
                        request_key: "receipt-hold".into(),
                        reason: WorkflowHoldReason::ConversationReplanRequired,
                        detail: "Hold pending human replan".into(),
                        directly_affected_task_ids: vec![],
                        affected_write_paths: vec![],
                        affected_interfaces: vec![],
                        holds_entire_job: true,
                    })
                    .expect("hold");
            }
            "approved" => {
                // Schema-valid synthetic historical linkage is local test data only.
                let pr = legacy.owner.planning_result.as_ref().expect("owner plan");
                let mut job = legacy.job.clone();
                job.approval_linkage = Some(FrozenApprovalLinkage {
                    approval_id: "apr_0123456789abcdef0123456789abcdef".into(),
                    approval_subject_digest: "a".repeat(64),
                    plan_id: "plan_receipt_independent_approval".into(),
                    plan_revision: pr.plan_revision,
                    plan_digest: pr.plan_digest.clone(),
                    project_base: pr.project_base.clone(),
                    profile_id: pr.profile_id.clone(),
                    registry_revision: pr.registry_revision,
                    policy_id: "receipt-synthetic-policy".into(),
                    policy_digest: "b".repeat(64),
                    approved_task_ids: vec!["task_parse".into()],
                    approved_write_paths: vec!["src/parser.rs".into()],
                    controller_envelope: None,
                });
                replace_fixture_job(&legacy.fx, &job);
            }
            "nonterminal" => {
                legacy.fx.store.transact_binding_ledger(BINDING_ID, None, |ledger| {
                    let grant = ledger.planning_grants.iter_mut().find(|g| g.grant_id == legacy.command.grant_id).expect("grant");
                    let reservation = grant.reservations.iter_mut().find(|r| r.request_key == legacy.command.service_request_key).expect("reservation");
                    reservation.status = PlanningReservationStatus::EffectUnknown;
                    grant.usage.completed_calls -= 1;
                    grant.usage.unknown_effect_calls += 1;
                    Ok(threadlane_native::coworker_workflow::CoworkerLedgerMutationOutcome::Mutated(()))
                }).expect("nonterminal fixture");
            }
            "superseded" => {
                let mut brief = legacy.job.work_brief.clone();
                brief.brief_id = "brf_receipt_superseding".into();
                brief.kind = WorkBriefKind::Amendment {
                    target_job_id: legacy.job.job_id.clone(),
                    target_job_revision: legacy.job.job_revision,
                    amendment_reason: "Newer scope".into(),
                };
                legacy
                    .fx
                    .store
                    .create_or_amend_job(&CreateCoworkerJobRequest {
                        binding_id: BINDING_ID.into(),
                        thread_uuid: THREAD_UUID.into(),
                        request_key: "receipt-superseding".into(),
                        expected_ledger_revision: None,
                        work_brief: brief,
                        amended_target_task_ids: vec![],
                        amended_write_paths: vec!["src/parser.rs".into()],
                        amended_interfaces: vec![],
                    })
                    .expect("superseding job");
            }
            "dirty-source" => {
                fs::write(legacy.fx.repo_dir.join("untracked.txt"), "dirty").expect("dirty source")
            }
            _ => unreachable!(),
        }
        let before = legacy
            .fx
            .store
            .read_binding_ledger(BINDING_ID)
            .expect("before refusal");
        let command_before = legacy
            .commands
            .get_command(BINDING_ID, THREAD_UUID, &legacy.command.command_id)
            .expect("command before");
        let plans_before = legacy
            .fx
            .workflow
            .list_plans(None, None)
            .expect("plans before");
        driver
            .execute_command(BINDING_ID, THREAD_UUID, &legacy.command.command_id)
            .await
            .expect_err(mode);
        assert_eq!(
            legacy
                .fx
                .store
                .read_binding_ledger(BINDING_ID)
                .expect("after refusal"),
            before,
            "{mode}"
        );
        assert_eq!(
            legacy
                .commands
                .get_command(BINDING_ID, THREAD_UUID, &legacy.command.command_id)
                .expect("command after"),
            command_before,
            "{mode}"
        );
        assert_eq!(
            legacy
                .fx
                .workflow
                .list_plans(None, None)
                .expect("plans after"),
            plans_before,
            "{mode}"
        );
        assert_eq!(
            command_before.expect("command").status,
            PlanningCommandStatus::Unknown
        );
        assert_eq!(legacy.fx.invocation_count(), 2, "{mode}");
        fs::write(&legacy.artifact_path, &legacy.artifact_bytes)
            .expect("restore synthetic artifact");
        replace_fixture_job(&legacy.fx, &legacy.job);
        if mode == "nonterminal" || mode == "superseded" {
            legacy.fx.store.transact_binding_ledger(BINDING_ID, None, |ledger| {
                ledger.jobs.retain(|j| j.job_id == legacy.job.job_id || j.job_id == legacy.owner.job_id);
                for grant in &mut ledger.planning_grants {
                    for reservation in &mut grant.reservations {
                        if reservation.request_key == legacy.command.service_request_key && reservation.status == PlanningReservationStatus::EffectUnknown {
                            reservation.status = PlanningReservationStatus::Completed;
                            grant.usage.completed_calls += 1;
                            grant.usage.unknown_effect_calls -= 1;
                        }
                    }
                }
                Ok(threadlane_native::coworker_workflow::CoworkerLedgerMutationOutcome::Mutated(()))
            }).expect("restore fixture history");
        }
        if mode == "dirty-source" {
            fs::remove_file(legacy.fx.repo_dir.join("untracked.txt"))
                .expect("restore clean source");
        }
    }
}

#[tokio::test]
async fn receipt_recovery_resumes_publication_and_link_crash_windows_without_new_revision_or_call()
{
    use threadlane_native::{
        PlanningCommandBindingLedger, PlanningCommandDriver, PlanningCommandStatus,
    };
    let legacy = legacy_collision_fixture().await;
    let driver = PlanningCommandDriver::new(legacy.commands.clone(), legacy.fx.service.clone())
        .expect("driver");
    let first = driver
        .execute_command(BINDING_ID, THREAD_UUID, &legacy.command.command_id)
        .await
        .expect("initial recovery");
    let completed_job = legacy
        .fx
        .store
        .read_binding_ledger(BINDING_ID)
        .expect("ledger")
        .jobs
        .into_iter()
        .find(|j| j.job_id == legacy.job.job_id)
        .expect("linked job");
    let command_path = legacy
        .commands
        .ledger_path_for_binding(BINDING_ID)
        .expect("command ledger path");
    let completed_ledger: PlanningCommandBindingLedger =
        serde_json::from_slice(&fs::read(&command_path).expect("command ledger"))
            .expect("parse ledger");
    let plans = legacy.fx.workflow.list_plans(None, None).expect("plans");
    for linked in [false, true] {
        replace_fixture_job(
            &legacy.fx,
            if linked { &completed_job } else { &legacy.job },
        );
        // This represents a process dying after publish (or after link) but before
        // command completion. No production recovery interface edits ledger JSON.
        let mut interrupted = completed_ledger.clone();
        let command = interrupted
            .commands
            .iter_mut()
            .find(|c| c.command_id == legacy.command.command_id)
            .expect("command");
        command.status = PlanningCommandStatus::Unknown;
        command.pending_reconciliation = true;
        command.result_ref = None;
        command.completed_at_ms = None;
        command.diagnostic = Some("synthetic crash before command completion".into());
        command.validate().expect("valid interrupted command");
        fs::write(
            &command_path,
            serde_json::to_vec(&interrupted).expect("serialize interrupted ledger"),
        )
        .expect("persist synthetic crash");
        let reopened = threadlane_native::PlanningCommandStore::open_existing(
            legacy.commands.commands_dir(),
            legacy.fx.store.clone(),
            &legacy.fx.registry_dir,
        )
        .expect("reopen")
        .with_artifacts_dir(&legacy.fx.artifacts_dir)
        .expect("artifacts");
        let (_, service, _) = legacy.fx.reopen_all();
        let reopened_driver = PlanningCommandDriver::new(reopened, service).expect("driver");
        let recovered = reopened_driver
            .execute_command(BINDING_ID, THREAD_UUID, &legacy.command.command_id)
            .await
            .expect("recover partial persistence");
        assert!(recovered.reconciled_from_existing);
        assert_eq!(recovered.command.result_ref, first.command.result_ref);
        assert_eq!(
            legacy
                .fx
                .workflow
                .list_plans(None, None)
                .expect("plans after recovery"),
            plans
        );
        assert_eq!(legacy.fx.invocation_count(), 2);
        assert_eq!(
            fs::read(&legacy.artifact_path).expect("artifact"),
            legacy.artifact_bytes
        );
    }
}

#[tokio::test]
async fn completed_receipt_proof_survives_expired_or_exhausted_grant_without_new_authority() {
    use threadlane_native::coworker_workflow::{
        CoworkerLedgerMutationOutcome, PlanningGrantStatus,
    };
    for expired in [false, true] {
        let legacy = legacy_collision_fixture_with_budget(true).await;
        let grant_before = legacy
            .fx
            .store
            .read_binding_ledger(BINDING_ID)
            .expect("ledger")
            .planning_grants
            .into_iter()
            .find(|g| g.grant_id == legacy.command.grant_id)
            .expect("grant");
        assert_eq!(grant_before.status, PlanningGrantStatus::Exhausted);
        if expired {
            legacy
                .fx
                .store
                .transact_binding_ledger(BINDING_ID, None, |ledger| {
                    let grant = ledger
                        .planning_grants
                        .iter_mut()
                        .find(|g| g.grant_id == legacy.command.grant_id)
                        .expect("grant");
                    grant.expires_at_ms = threadlane_native::current_wall_clock_ms()
                        .expect("clock")
                        .saturating_sub(1);
                    grant.status = PlanningGrantStatus::Expired;
                    Ok(CoworkerLedgerMutationOutcome::Mutated(()))
                })
                .expect("historical expiration");
        }
        let before = legacy
            .fx
            .store
            .read_binding_ledger(BINDING_ID)
            .expect("before")
            .planning_grants;
        let driver = threadlane_native::PlanningCommandDriver::new(
            legacy.commands.clone(),
            legacy.fx.service.clone(),
        )
        .expect("driver");
        assert!(
            driver
                .execute_command(BINDING_ID, THREAD_UUID, &legacy.command.command_id)
                .await
                .expect("receipt recovery")
                .reconciled_from_existing
        );
        assert_eq!(
            legacy
                .fx
                .store
                .read_binding_ledger(BINDING_ID)
                .expect("after")
                .planning_grants,
            before
        );
        assert_eq!(legacy.fx.invocation_count(), 2);
    }
}

#[tokio::test]
async fn recovered_result_verifier_refuses_arbitrary_plan_revision_or_receipt_substitution() {
    use threadlane_native::{PlanningCommandDriver, PlanningCommandResultRef};
    let legacy = legacy_collision_fixture().await;
    let driver = PlanningCommandDriver::new(legacy.commands.clone(), legacy.fx.service.clone())
        .expect("driver");
    let completed = driver
        .execute_command(BINDING_ID, THREAD_UUID, &legacy.command.command_id)
        .await
        .expect("recovered command")
        .command;
    let linked_job = legacy
        .fx
        .store
        .read_binding_ledger(BINDING_ID)
        .expect("ledger")
        .jobs
        .into_iter()
        .find(|j| j.job_id == legacy.job.job_id)
        .expect("job");
    for mutation in ["plan-id", "revision", "artifact", "request-key"] {
        let mut substituted = completed.clone();
        let PlanningCommandResultRef::OrchestratorPlan {
            planning_result, ..
        } = substituted.result_ref.as_mut().expect("result")
        else {
            panic!("plan");
        };
        match mutation {
            "plan-id" => planning_result.plan_id = "plan_arbitrary_substitution".into(),
            "revision" => planning_result.plan_revision += 1,
            "artifact" => {
                planning_result.report_artifact_id = legacy
                    .owner
                    .planning_result
                    .as_ref()
                    .expect("owner receipt")
                    .report_artifact_id
                    .clone()
            }
            "request-key" => planning_result.request_key = "unrelated-service-request".into(),
            _ => unreachable!(),
        }
        // Keep job/result references mutually consistent to exercise the stronger
        // command/receipt boundary, rather than just unequal result references.
        let mut job = linked_job.clone();
        job.planning_result = Some(planning_result.as_ref().clone());
        if matches!(mutation, "artifact" | "request-key") {
            // The shared ledger rejects receipt substitution before it can even
            // become a candidate result. Preserve that stronger store invariant.
            legacy.fx.store.transact_binding_ledger(BINDING_ID, None, |ledger| {
                *ledger.jobs.iter_mut().find(|j| j.job_id == job.job_id).expect("job") = job.clone();
                Ok(threadlane_native::coworker_workflow::CoworkerLedgerMutationOutcome::Mutated(()))
            }).expect_err("store must reject substituted receipt provenance");
        } else {
            replace_fixture_job(&legacy.fx, &job);
        }
        legacy
            .commands
            .verify_completed_command(&substituted)
            .expect_err(mutation);
        replace_fixture_job(&legacy.fx, &linked_job);
    }
    legacy
        .commands
        .verify_completed_command(&completed)
        .expect("exact result still verifies");
    assert_eq!(legacy.fx.invocation_count(), 2);
}

fn write_fake_codex_planning_script(script_path: &Path, count_file: &Path, prompts_dir: &Path) {
    use std::os::unix::fs::OpenOptionsExt;

    if let Some(parent) = script_path.parent() {
        fs::create_dir_all(parent).expect("create codex script parent dir");
        fs::set_permissions(parent, fs::Permissions::from_mode(0o700))
            .expect("chmod 0700 codex script parent dir");
    }
    fs::create_dir_all(prompts_dir).expect("create codex prompts dir");
    fs::set_permissions(prompts_dir, fs::Permissions::from_mode(0o700))
        .expect("chmod 0700 codex prompts dir");

    let script = format!(
        r#"#!/bin/sh
set -eu
COUNT_FILE="{count_file}"
PROMPTS_DIR="{prompts_dir}"

c=0
if [ -f "$COUNT_FILE" ]; then
    c=$(cat "$COUNT_FILE")
fi
c=$((c + 1))
printf "%s" "$c" > "$COUNT_FILE"
printf "%s\n" "$@" > "$PROMPTS_DIR/args_$c.txt"

resp_file="$PROMPTS_DIR/resp_$c.json"
cat <<'EOF' > "$resp_file"
{{"outcome":"plan","schema_version":1,"summary":"Compile two independent Coder tasks and one dependent exporter Coder task for sample-telemetry-service using read-only Codex Orchestrator advisory execution.","tasks":[{{"task_id":"task_parse","title":"Implement binary frame header parser","description":"Parse binary telemetry frames in src/parser.rs and expose the stable ParsedFrame struct for downstream batch export.","role":"coder","difficulty":"medium","depends_on":[],"allowed_write_paths":["src/parser.rs"],"owned_interfaces":["iface_parsed_frame"],"consumed_interfaces":[]}},{{"task_id":"task_metrics","title":"Add frame counter helper","description":"Update src/metrics.rs to track total parsed telemetry frames without overlapping parser or exporter files.","role":"coder","difficulty":"easy","depends_on":[],"allowed_write_paths":["src/metrics.rs"],"owned_interfaces":[],"consumed_interfaces":[]}},{{"task_id":"task_export","title":"Wire batch exporter to ParsedFrame","description":"Consume iface_parsed_frame in src/exporter.rs and emit framed telemetry batches to the configured sink.","role":"coder","difficulty":"hard","depends_on":["task_parse"],"allowed_write_paths":["src/exporter.rs"],"owned_interfaces":["iface_export_sink"],"consumed_interfaces":["iface_parsed_frame"]}}],"check_suggestions":[{{"check_id":"chk_telemetry_unit","command_summary":"cargo test --workspace","required":true}}],"unconfirmed_proposal_notes":["Keep optional pushgateway exporter out of scope until human approval."],"resolved_open_questions":[],"deferred_proposals":[]}}
EOF

esc=$(sed 's/\\/\\\\/g; s/"/\\"/g' "$resp_file" | tr '\n' ' ')
printf '{{"type":"thread.started","thread_id":"01972d12-3456-7000-8000-00000000000%s"}}\n' "$c"
printf '{{"type":"turn.started"}}\n'
printf '{{"type":"item.completed","item":{{"id":"item_%s","type":"agent_message","text":"%s"}}}}\n' "$c" "$esc"
printf '{{"type":"turn.completed","usage":{{"input_tokens":1420,"cached_input_tokens":320,"output_tokens":410}}}}\n'
"#,
        count_file = count_file.display(),
        prompts_dir = prompts_dir.display(),
    );

    let staging_path = script_path.with_extension("tmp");
    let mut f = fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o700)
        .open(&staging_path)
        .expect("create staged fake-codex");
    f.write_all(script.as_bytes()).expect("write fake-codex");
    f.sync_all().expect("sync fake-codex");
    drop(f);
    fs::rename(&staging_path, script_path).expect("publish closed fake-codex");
}

#[tokio::test]
async fn codex_sol_xhigh_orchestrator_planning_command_executes_replays_without_reinvo_and_records_unfabricated_observation(
) {
    use threadlane_core::observation::{read_journal_records_readonly, ObservationRecordPhase};
    use threadlane_native::coworker_grants::{
        prepare_planning_grant_preview_with_store, record_planning_grant,
        LocalOperatorGrantAttestation, PreparePlanningGrantRequest, RecordPlanningGrantRequest,
    };
    use threadlane_native::profile_flow::{RoleExecutionRoute, RolePreflightRequest};
    use threadlane_native::{
        EnqueuePlanningCommandRequest, PlanningCommandBindingLedger, PlanningCommandDriver,
        PlanningCommandResultRef, PlanningCommandStatus, PlanningCommandStore,
    };

    let fx = OrchestratorTestFixture::new();
    let fake_codex_exe = fx.root_dir.join("bin/fake-codex");
    let codex_count_file = fx.root_dir.join("codex-invocations.txt");
    let codex_prompts_dir = fx.root_dir.join("codex-prompts");
    write_fake_codex_planning_script(&fake_codex_exe, &codex_count_file, &codex_prompts_dir);

    let rev0 = fx.registry.snapshot().expect("snapshot").revision;
    fx.registry
        .apply(
            rev0,
            RegistryChange::UpsertTool {
                tool: ToolDefinition {
                    id: "tool-codex".to_string(),
                    label: "Codex CLI".to_string(),
                    adapter: ToolAdapter::CodexCli,
                    executable_path: Some(fake_codex_exe.clone()),
                    enabled: true,
                },
            },
        )
        .expect("upsert tool-codex");
    fx.registry
        .apply(
            rev0 + 1,
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
                        id: "model-codex-sol".to_string(),
                        tool_id: "tool-codex".to_string(),
                        label: "GPT-6.1 Sol".to_string(),
                        model_arg: "gpt-6.1-sol".to_string(),
                        efforts: vec!["xhigh".to_string()],
                        allow_no_effort: false,
                    },
                ],
            },
        )
        .expect("upsert models with codex sol");
    fx.registry
        .apply(
            rev0 + 2,
            RegistryChange::UpsertProfile {
                profile: WorkflowProfile {
                    decision_advisor: None,
                    id: "profile-main".to_string(),
                    name: "Main Planning Profile".to_string(),
                    assignments: vec![
                        RoleAssignment {
                            role: WorkflowRole::Architect,
                            difficulty: None,
                            choice: ModelChoice {
                                model_id: "model-codex-sol".to_string(),
                                effort: Some("xhigh".to_string()),
                            },
                        },
                        RoleAssignment {
                            role: WorkflowRole::Orchestrator,
                            difficulty: None,
                            choice: ModelChoice {
                                model_id: "model-codex-sol".to_string(),
                                effort: Some("xhigh".to_string()),
                            },
                        },
                        RoleAssignment {
                            role: WorkflowRole::Coder,
                            difficulty: None,
                            choice: ModelChoice {
                                model_id: "model-argon".to_string(),
                                effort: Some("high".to_string()),
                            },
                        },
                    ],
                },
            },
        )
        .expect("upsert profile with codex sol orchestrator/architect");

    let codex_policy = ExecutionPolicy {
        schema_version: EXECUTION_POLICY_SCHEMA_VERSION,
        policy_id: "pol-codex-sol-planning".to_string(),
        allow_rules: vec![PolicyAllowRule {
            tool_id: "tool-codex".to_string(),
            model_id: "model-codex-sol".to_string(),
            efforts: vec!["xhigh".to_string()],
            allow_no_effort: false,
            roles: vec![WorkflowRole::Orchestrator, WorkflowRole::Architect],
        }],
    };

    let snapshot = fx.registry.snapshot().expect("snapshot with codex sol");
    let preview = prepare_planning_grant_preview_with_store(
        &fx.store,
        &snapshot,
        &PreparePlanningGrantRequest {
            request_key: "req_grant_codex_sol_1".to_string(),
            binding_id: BINDING_ID.to_string(),
            thread_uuid: THREAD_UUID.to_string(),
            repository_path: fx.repo_dir.clone(),
            base_commit: fx.project_base.base_commit.clone(),
            expected_base_tree_sha: Some(fx.project_base.base_tree_sha.clone()),
            profile_id: "profile-main".to_string(),
            expected_registry_revision: Some(snapshot.revision),
            roles: vec![
                RolePreflightRequest {
                    role: WorkflowRole::Orchestrator,
                    difficulty: None,
                },
                RolePreflightRequest {
                    role: WorkflowRole::Architect,
                    difficulty: None,
                },
            ],
            policy: codex_policy.clone(),
            capabilities: PlanningReadOnlyCapabilities::strict_read_only(),
            budget: PlanningCallBudget {
                max_total_calls: 4,
                max_orchestrator_calls: 2,
                max_architect_calls: 2,
                max_timeout_ms_per_call: 30_000,
                max_output_bytes_per_call: 65_536,
            },
            created_at_ms: 1_700_000_000_000,
            expires_at_ms: 1_800_000_000_000,
        },
    )
    .expect("prepare codex planning grant preview");

    let recorded_grant = record_planning_grant(
        &fx.store,
        &snapshot,
        &RecordPlanningGrantRequest {
            request_key: "req_grant_codex_sol_1".to_string(),
            expected_ledger_revision: None,
            preview: preview.clone(),
            confirmed_preview_digest: preview.preview_digest.clone(),
            attestation: LocalOperatorGrantAttestation::attested(),
        },
    )
    .expect("record codex planning grant");

    let job = fx.create_initial_job(
        "job_codex_sol_1",
        vec![
            "Implement binary frame header parser in src/parser.rs".to_string(),
            "Add frame counter helper in src/metrics.rs".to_string(),
            "Wire batch exporter in src/exporter.rs to consume parsed frames".to_string(),
        ],
        Vec::new(),
        Vec::new(),
    );
    let cmd_store = receipt_command_store(&fx);
    let plan_req = OrchestratorTestFixture::plan_req(
        &recorded_grant.grant_id,
        &job.job_id,
        "cmd_orch_codex_sol_1",
        0,
    );
    let enqueue_req = EnqueuePlanningCommandRequest::from_orchestrator(plan_req);
    let enqueued = cmd_store
        .enqueue_command(&enqueue_req)
        .expect("enqueue codex orchestrator command");
    assert_eq!(enqueued.selected_choice.adapter, ToolAdapter::CodexCli);
    assert_eq!(
        enqueued.selected_choice.route,
        RoleExecutionRoute::HeadlessWorker
    );
    assert_eq!(enqueued.selected_choice.model_arg, "gpt-6.1-sol");
    assert_eq!(enqueued.selected_choice.effort.as_deref(), Some("xhigh"));

    let driver =
        PlanningCommandDriver::new(cmd_store.clone(), fx.service.clone()).expect("create driver");
    let executed = driver
        .execute_command(BINDING_ID, THREAD_UUID, &enqueued.command_id)
        .await
        .expect("execute codex orchestrator command");
    assert_eq!(executed.command.status, PlanningCommandStatus::Completed);
    assert!(!executed.was_already_completed);
    assert!(!executed.reconciled_from_existing);

    let PlanningCommandResultRef::OrchestratorPlan {
        job_id: ref res_job_id,
        ref planning_result,
        artifact_verified,
    } = executed
        .command
        .result_ref
        .as_ref()
        .expect("completed command result_ref")
    else {
        panic!("expected OrchestratorPlan result_ref");
    };
    assert!(artifact_verified);
    assert_eq!(res_job_id, &job.job_id);
    assert_eq!(planning_result.grant_id, recorded_grant.grant_id);
    assert_eq!(planning_result.plan_revision, 1);

    let published_plan = fx
        .workflow
        .get_plan(&planning_result.plan_id, None, None, None)
        .expect("fetch published plan from workflow");
    assert_eq!(published_plan.header.status, PlanStatus::Published);
    assert_eq!(published_plan.header.revision, 1);
    assert_eq!(published_plan.tasks.len(), 3);

    let codex_invocations: u32 = fs::read_to_string(&codex_count_file)
        .expect("read codex count")
        .trim()
        .parse()
        .expect("parse codex count");
    assert_eq!(codex_invocations, 1);
    assert_eq!(fx.invocation_count(), 0);

    // Reopen stores and verify duplicate enqueue + re-executing completed command + interrupted Unknown reconciliation
    // all reuse the persisted result without a second Codex CLI invocation.
    let (reopened_store, reopened_service, reopened_wf) = fx.reopen_all();
    let reopened_cmd_store = PlanningCommandStore::open_existing(
        cmd_store.commands_dir(),
        reopened_store,
        &fx.registry_dir,
    )
    .expect("reopen cmd store")
    .with_artifacts_dir(&fx.artifacts_dir)
    .expect("configure artifacts dir");
    let dup_enqueued = reopened_cmd_store
        .enqueue_command(&enqueue_req)
        .expect("idempotent duplicate enqueue");
    assert_eq!(dup_enqueued.command_id, enqueued.command_id);
    assert_eq!(dup_enqueued.status, PlanningCommandStatus::Completed);

    let reopened_driver = PlanningCommandDriver::new(reopened_cmd_store.clone(), reopened_service)
        .expect("reopen driver");
    let replayed = reopened_driver
        .execute_command(BINDING_ID, THREAD_UUID, &enqueued.command_id)
        .await
        .expect("replay completed codex command");
    assert_eq!(replayed.command.status, PlanningCommandStatus::Completed);
    assert!(replayed.was_already_completed);
    assert!(!replayed.reconciled_from_existing);
    assert_eq!(replayed.command.result_ref, executed.command.result_ref);

    // Simulate an interrupted Unknown command after publication/link and reconcile on reopen.
    let command_path = reopened_cmd_store
        .ledger_path_for_binding(BINDING_ID)
        .expect("command ledger path");
    let mut interrupted: PlanningCommandBindingLedger =
        serde_json::from_slice(&fs::read(&command_path).expect("read command ledger"))
            .expect("parse command ledger");
    let cmd_mut = interrupted
        .commands
        .iter_mut()
        .find(|c| c.command_id == enqueued.command_id)
        .expect("find command");
    cmd_mut.status = PlanningCommandStatus::Unknown;
    cmd_mut.pending_reconciliation = true;
    cmd_mut.result_ref = None;
    cmd_mut.completed_at_ms = None;
    cmd_mut.diagnostic = Some("synthetic crash before command completion".to_string());
    cmd_mut.validate().expect("valid interrupted command");
    fs::write(
        &command_path,
        serde_json::to_vec(&interrupted).expect("serialize interrupted command ledger"),
    )
    .expect("write interrupted command ledger");

    let (reopened_store_2, reopened_service_2, _) = fx.reopen_all();
    let reopened_cmd_store_2 = PlanningCommandStore::open_existing(
        cmd_store.commands_dir(),
        reopened_store_2,
        &fx.registry_dir,
    )
    .expect("reopen cmd store 2")
    .with_artifacts_dir(&fx.artifacts_dir)
    .expect("configure artifacts dir 2");
    let reopened_driver_2 =
        PlanningCommandDriver::new(reopened_cmd_store_2, reopened_service_2).expect("driver 2");
    let reconciled = reopened_driver_2
        .execute_command(BINDING_ID, THREAD_UUID, &enqueued.command_id)
        .await
        .expect("reconcile interrupted codex command");
    assert_eq!(reconciled.command.status, PlanningCommandStatus::Completed);
    assert!(reconciled.reconciled_from_existing);
    assert!(!reconciled.was_already_completed);
    assert_eq!(reconciled.command.result_ref, executed.command.result_ref);
    assert_eq!(
        reopened_wf
            .list_plans(None, None)
            .expect("list plans after reconciliation")
            .plans
            .len(),
        1
    );

    let codex_invocations_after: u32 = fs::read_to_string(&codex_count_file)
        .expect("read codex count after replay and reconciliation")
        .trim()
        .parse()
        .expect("parse codex count after replay and reconciliation");
    assert_eq!(
        codex_invocations_after, 1,
        "reopen/duplicate replay and reconciliation must not invoke Codex CLI a second time"
    );

    // Inspect Activity observation journal: requested model/effort is gpt-6.1-sol/xhigh,
    // observed_model and observed_effort remain None (not fabricated), and genuine stream usage is recorded.
    let (records, _) =
        read_journal_records_readonly(&fx.plans_dir).expect("read observation journal");
    let finish_rec = records
        .iter()
        .find(|r| {
            r.phase == ObservationRecordPhase::Finish
                && r.operation == "coworker.planning.orchestrator"
        })
        .expect("orchestrator finish observation record");
    let exec = finish_rec.executor.as_ref().expect("executor metadata");
    assert_eq!(exec.provider.as_deref(), Some("codex_cli"));
    assert_eq!(exec.requested_model.as_deref(), Some("gpt-6.1-sol"));
    assert_eq!(exec.requested_effort.as_deref(), Some("xhigh"));
    assert_eq!(
        exec.observed_model, None,
        "requested Codex launch configuration must never be fabricated as observed_model"
    );
    assert_eq!(
        exec.observed_effort, None,
        "requested Codex launch configuration must never be fabricated as observed_effort"
    );
    let usage = finish_rec
        .usage
        .as_ref()
        .expect("genuine stream token usage");
    assert_eq!(usage.input_tokens, Some(1420));
    assert_eq!(usage.output_tokens, Some(410));
    assert_eq!(usage.cache_read_tokens, Some(320));
    assert_eq!(usage.total_tokens, Some(1830));
}
