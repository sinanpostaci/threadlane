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
    if [ -f "$PROMPTS_DIR/raw_after_init_$c.jsonl" ]; then
        cat "$PROMPTS_DIR/raw_after_init_$c.jsonl"
        exit 0
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

OUT_LAST_MSG=""
while [ "$#" -gt 0 ]; do
    case "$1" in
        --output-last-message|-o)
            OUT_LAST_MSG="$2"
            shift 2
            ;;
        --)
            shift
            break
            ;;
        *)
            shift 1
            ;;
    esac
done

if [ -f "$PROMPTS_DIR/raw_stream_$c.jsonl" ]; then
    cat "$PROMPTS_DIR/raw_stream_$c.jsonl"
    if [ -n "$OUT_LAST_MSG" ] && [ -f "$PROMPTS_DIR/last_message_$c.json" ]; then
        cp "$PROMPTS_DIR/last_message_$c.json" "$OUT_LAST_MSG"
    fi
    exit 0
fi

resp_file="$PROMPTS_DIR/resp_$c.json"
printf '%s' '{{"outcome":"plan","schema_version":1,"summary":"Compile two independent Coder tasks and one dependent exporter Coder task for sample-telemetry-service using read-only Codex Orchestrator advisory execution.","tasks":[{{"task_id":"task_parse","title":"Implement binary frame header parser","description":"Parse binary telemetry frames in src/parser.rs and expose the stable ParsedFrame struct for downstream batch export.","role":"coder","difficulty":"medium","depends_on":[],"allowed_write_paths":["src/parser.rs"],"owned_interfaces":["iface_parsed_frame"],"consumed_interfaces":[]}},{{"task_id":"task_metrics","title":"Add frame counter helper","description":"Update src/metrics.rs to track total parsed telemetry frames without overlapping parser or exporter files.","role":"coder","difficulty":"easy","depends_on":[],"allowed_write_paths":["src/metrics.rs"],"owned_interfaces":[],"consumed_interfaces":[]}},{{"task_id":"task_export","title":"Wire batch exporter to ParsedFrame","description":"Consume iface_parsed_frame in src/exporter.rs and emit framed telemetry batches to the configured sink.","role":"coder","difficulty":"hard","depends_on":["task_parse"],"allowed_write_paths":["src/exporter.rs"],"owned_interfaces":["iface_export_sink"],"consumed_interfaces":["iface_parsed_frame"]}}],"check_suggestions":[{{"check_id":"chk_telemetry_unit","command_summary":"cargo test --workspace","required":true}}],"unconfirmed_proposal_notes":["Keep optional pushgateway exporter out of scope until human approval."],"resolved_open_questions":[],"deferred_proposals":[]}}' > "$resp_file"

esc=$(sed 's/\\/\\\\/g; s/"/\\"/g' "$resp_file")
printf '{{"type":"thread.started","thread_id":"01972d12-3456-7000-8000-00000000000%s"}}\n' "$c"
printf '{{"type":"turn.started"}}\n'
printf '{{"type":"item.completed","item":{{"id":"item_%s","type":"agent_message","text":"%s"}}}}\n' "$c" "$esc"
if [ -n "$OUT_LAST_MSG" ]; then
    cp "$resp_file" "$OUT_LAST_MSG"
fi
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

#[tokio::test]
async fn proof_bound_grant_allows_unrelated_profile_deletion_before_and_during_call_and_replays_idempotently(
) {
    use threadlane_native::coworker_grants::{
        prepare_planning_grant_preview_with_store, record_planning_grant,
        LocalOperatorGrantAttestation, PreparePlanningGrantRequest, RecordPlanningGrantRequest,
    };
    use threadlane_native::profile_flow::RolePreflightRequest;
    use threadlane_native::{
        EnqueuePlanningCommandRequest, PlanningCommandDriver, PlanningCommandStore,
    };

    let fx = OrchestratorTestFixture::new();

    // Seed an unrelated tool, model, and profile before preparing the proof-bound grant.
    let snap0 = fx.registry.snapshot().expect("snapshot 0");
    let snap1 = fx
        .registry
        .apply(
            snap0.revision,
            RegistryChange::UpsertTool {
                tool: ToolDefinition {
                    id: "tool-unused".to_string(),
                    label: "Unused Tool".to_string(),
                    adapter: ToolAdapter::Antigravity,
                    executable_path: Some(fx.root_dir.join("bin/fake-agy")),
                    enabled: true,
                },
            },
        )
        .expect("upsert tool-unused");
    let snap2 = fx
        .registry
        .apply(
            snap1.revision,
            RegistryChange::UpsertModels {
                models: vec![ModelDefinition {
                    id: "model-unused".to_string(),
                    tool_id: "tool-unused".to_string(),
                    label: "Unused Model".to_string(),
                    model_arg: "unused-model-v1".to_string(),
                    efforts: vec!["high".to_string()],
                    allow_no_effort: false,
                }],
            },
        )
        .expect("upsert model-unused");
    let grant_snap = fx
        .registry
        .apply(
            snap2.revision,
            RegistryChange::UpsertProfile {
                profile: WorkflowProfile {
                    decision_advisor: None,
                    id: "profile-unused".to_string(),
                    name: "Unused Profile".to_string(),
                    assignments: vec![RoleAssignment {
                        role: WorkflowRole::Coder,
                        difficulty: None,
                        choice: ModelChoice {
                            model_id: "model-unused".to_string(),
                            effort: Some("high".to_string()),
                        },
                    }],
                },
            },
        )
        .expect("upsert profile-unused");

    let prepare_req = PreparePlanningGrantRequest {
        request_key: "prep_proof_bound_grant_1".to_string(),
        binding_id: BINDING_ID.to_string(),
        thread_uuid: THREAD_UUID.to_string(),
        repository_path: fx.repo_dir.clone(),
        base_commit: fx.project_base.base_commit.clone(),
        expected_base_tree_sha: Some(fx.project_base.base_tree_sha.clone()),
        profile_id: "profile-main".to_string(),
        expected_registry_revision: Some(grant_snap.revision),
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
        policy: OrchestratorTestFixture::default_policy(),
        capabilities: PlanningReadOnlyCapabilities::strict_read_only(),
        budget: PlanningCallBudget {
            max_total_calls: 8,
            max_orchestrator_calls: 4,
            max_architect_calls: 4,
            max_timeout_ms_per_call: 30_000,
            max_output_bytes_per_call: 65_536,
        },
        created_at_ms: 1_700_000_000_000,
        expires_at_ms: 1_800_000_000_000,
    };
    let preview = prepare_planning_grant_preview_with_store(&fx.store, &grant_snap, &prepare_req)
        .expect("prepare proof-bound grant preview");
    let confirmed_digest = preview.preview_digest.clone();
    let grant_summary = record_planning_grant(
        &fx.store,
        &grant_snap,
        &RecordPlanningGrantRequest {
            request_key: "prep_proof_bound_grant_1".to_string(),
            expected_ledger_revision: None,
            preview,
            confirmed_preview_digest: confirmed_digest,
            attestation: LocalOperatorGrantAttestation::attested(),
        },
    )
    .expect("record proof-bound grant");

    // Metadata summary must never leak scoped_profile_dependency_proof or hidden executable paths.
    let summary_json = serde_json::to_string(&grant_summary).expect("serialize grant summary");
    assert!(
        !summary_json.contains("scoped_profile_dependency_proof")
            && !summary_json.contains("fake-agy"),
        "PlanningGrantSummary must not expose scoped_profile_dependency_proof or executable_path"
    );

    // Unrelated profile deletion and upsert BEFORE the call advances the global registry revision.
    let after_del = fx
        .registry
        .apply(
            grant_snap.revision,
            RegistryChange::DeleteProfile {
                profile_id: "profile-unused".to_string(),
            },
        )
        .expect("delete unrelated profile before call");
    let after_upsert = fx
        .registry
        .apply(
            after_del.revision,
            RegistryChange::UpsertProfile {
                profile: WorkflowProfile {
                    decision_advisor: None,
                    id: "profile-other".to_string(),
                    name: "Other Unrelated Profile".to_string(),
                    assignments: vec![RoleAssignment {
                        role: WorkflowRole::Coder,
                        difficulty: None,
                        choice: ModelChoice {
                            model_id: "model-unused".to_string(),
                            effort: Some("high".to_string()),
                        },
                    }],
                },
            },
        )
        .expect("upsert another unrelated profile before call");
    assert!(after_upsert.revision > grant_summary.registry_revision);

    let job = fx.create_initial_job(
        "job_proof_bound_isolated_1",
        vec![
            "Implement binary frame header parser in src/parser.rs".to_string(),
            "Add frame counter helper in src/metrics.rs".to_string(),
            "Wire batch exporter in src/exporter.rs to consume parsed frames".to_string(),
        ],
        Vec::new(),
        Vec::new(),
    );

    // Configure a mid-flight hook so that an unrelated profile deletion + upsert also happens
    // while the Orchestrator fake provider call is in flight, before plan save/publish/linkage.
    let in_flight_marker = fx.agy_prompts_dir.join("in_flight_unrelated.marker");
    let resume_marker = fx.agy_prompts_dir.join("resume_unrelated.marker");
    let hook_script = format!(
        "#!/bin/sh\nset -eu\n: > \"{}\"\ni=0\nwhile [ ! -f \"{}\" ] && [ \"$i\" -lt 250 ]; do\n  sleep 0.02\n  i=$((i + 1))\ndone\n",
        in_flight_marker.display(),
        resume_marker.display(),
    );
    fs::write(fx.agy_prompts_dir.join("hook_1.sh"), hook_script).expect("write hook_1.sh");
    fx.write_custom_call_response(
        1,
        r#"{"outcome":"plan","schema_version":1,"summary":"Compile parser and exporter DAG under proof-bound grant.","tasks":[{"task_id":"task_parse","title":"Implement binary frame header parser","description":"Parse binary frames in src/parser.rs and expose ParsedFrame.","role":"coder","difficulty":"medium","depends_on":[],"allowed_write_paths":["src/parser.rs"],"owned_interfaces":["iface_parsed_frame"],"consumed_interfaces":[]},{"task_id":"task_export","title":"Wire batch exporter","description":"Consume iface_parsed_frame in src/exporter.rs.","role":"coder","difficulty":"hard","depends_on":["task_parse"],"allowed_write_paths":["src/exporter.rs"],"owned_interfaces":[],"consumed_interfaces":["iface_parsed_frame"]}],"check_suggestions":[{"check_id":"chk_unit","command_summary":"cargo test","required":true}]}"#,
        false,
    );

    let commands = receipt_command_store(&fx);
    let orch_req = OrchestratorTestFixture::plan_req(
        &grant_summary.grant_id,
        &job.job_id,
        "orch_proof_bound_isolated_1",
        0,
    );
    let enqueued = commands
        .enqueue_command(&EnqueuePlanningCommandRequest::from_orchestrator(
            orch_req.clone(),
        ))
        .expect("enqueue command succeeds despite unrelated profile deletion/upsert");

    let driver = PlanningCommandDriver::new(commands.clone(), fx.service.clone())
        .expect("create planning command driver");

    let mid_flight_unrelated_edit = async {
        let deadline = Instant::now() + Duration::from_secs(5);
        while !in_flight_marker.exists() {
            assert!(
                Instant::now() < deadline,
                "timed out waiting for in-flight marker"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let cur = fx.registry.snapshot().expect("mid-flight snapshot");
        let mid1 = fx
            .registry
            .apply(
                cur.revision,
                RegistryChange::DeleteProfile {
                    profile_id: "profile-other".to_string(),
                },
            )
            .expect("delete unrelated profile-other mid-flight");
        fx.registry
            .apply(
                mid1.revision,
                RegistryChange::UpsertProfile {
                    profile: WorkflowProfile {
                        decision_advisor: None,
                        id: "profile-midflight".to_string(),
                        name: "Midflight Unrelated Profile".to_string(),
                        assignments: vec![RoleAssignment {
                            role: WorkflowRole::Coder,
                            difficulty: None,
                            choice: ModelChoice {
                                model_id: "model-unused".to_string(),
                                effort: Some("high".to_string()),
                            },
                        }],
                    },
                },
            )
            .expect("upsert unrelated profile-midflight during call");
        fs::write(&resume_marker, "go").expect("write resume marker");
    };

    let (exec_res, ()) = tokio::join!(
        driver.execute_command(BINDING_ID, THREAD_UUID, &enqueued.command_id),
        mid_flight_unrelated_edit
    );
    let exec_report =
        exec_res.expect("driver execution succeeds with unrelated mid-flight registry edits");
    assert!(!exec_report.was_already_completed);
    assert_eq!(fx.invocation_count(), 1);
    assert_eq!(
        exec_report.command.status,
        threadlane_native::PlanningCommandStatus::Completed
    );

    let ledger = fx.store.read_binding_ledger(BINDING_ID).expect("ledger");
    let updated_job = ledger
        .jobs
        .iter()
        .find(|j| j.job_id == job.job_id)
        .expect("updated job");
    let planning_result = updated_job
        .planning_result
        .as_ref()
        .expect("linked planning result")
        .clone();
    assert_eq!(planning_result.plan_revision, 1);
    assert_eq!(
        planning_result.registry_revision,
        grant_summary.registry_revision
    );

    // Mutate an unrelated profile again AFTER publication and verify ordinary retrieval paths.
    let post_pub_snap = fx.registry.snapshot().expect("post-publish snapshot");
    fx.registry
        .apply(
            post_pub_snap.revision,
            RegistryChange::DeleteProfile {
                profile_id: "profile-midflight".to_string(),
            },
        )
        .expect("delete unrelated profile-midflight after publication");

    let fetched_plan = fx
        .workflow
        .get_plan(&planning_result.plan_id, None, None, None)
        .expect("get_plan succeeds after unrelated post-publish registry edit");
    assert_eq!(fetched_plan.header.revision, 1);
    assert_eq!(
        fetched_plan.header.profile_revision,
        grant_summary.registry_revision
    );

    let listed = fx
        .workflow
        .list_plans(None, None)
        .expect("list_plans succeeds after unrelated post-publish registry edit");
    assert_eq!(listed.plans.len(), 1);
    assert_eq!(listed.plans[0].plan_id, planning_result.plan_id);

    let readonly_summary = ScopedWorkflow::read_plan_summary_readonly_at(
        &fx.registry_dir,
        &fx.plans_dir,
        BINDING_ID,
        &planning_result.plan_id,
        1,
    )
    .expect("read_plan_summary_readonly_at succeeds after unrelated registry edit");
    assert_eq!(readonly_summary.revision, 1);
    assert_eq!(
        readonly_summary.profile_revision,
        grant_summary.registry_revision
    );

    // Reopen stores and verify idempotent replay across both driver and direct service calls.
    let (reopened_store, reopened_service, reopened_wf) = fx.reopen_all();
    let reopened_commands = PlanningCommandStore::open_existing(
        commands.commands_dir(),
        reopened_store,
        &fx.registry_dir,
    )
    .expect("reopen command store")
    .with_artifacts_dir(&fx.artifacts_dir)
    .expect("configure artifacts dir");
    let reopened_driver = PlanningCommandDriver::new(reopened_commands, reopened_service.clone())
        .expect("reopen planning command driver");
    let replayed_cmd = reopened_driver
        .execute_command(BINDING_ID, THREAD_UUID, &enqueued.command_id)
        .await
        .expect("replaying completed command after reopen succeeds");
    assert!(replayed_cmd.was_already_completed);
    assert!(!replayed_cmd.reconciled_from_existing);
    assert_eq!(
        replayed_cmd.command.result_ref,
        exec_report.command.result_ref
    );
    assert_eq!(fx.invocation_count(), 1);

    let threadlane_native::PlanningCommandPayload::Orchestrator(mut frozen_orch_req) =
        enqueued.payload.clone()
    else {
        unreachable!("expected Orchestrator payload");
    };
    frozen_orch_req.request_key = enqueued.service_request_key.clone();
    frozen_orch_req.current_time_ms = enqueued.enqueued_at_ms;
    let replayed_orch = reopened_service
        .orchestrate_brief_to_plan(&reopened_wf, &frozen_orch_req)
        .await
        .expect("replaying orchestrate_brief_to_plan after reopen succeeds");
    assert_eq!(fx.invocation_count(), 1);
    assert_eq!(
        replayed_orch
            .require_published_plan()
            .expect("replayed published plan")
            .planning_result,
        planning_result
    );
}

#[tokio::test]
async fn proof_bound_grant_rejects_selected_dependency_drift_before_call_and_mid_flight_and_tampered_or_legacy_grants(
) {
    use threadlane_native::coworker_grants::{
        prepare_planning_grant_preview_with_store, record_planning_grant,
        LocalOperatorGrantAttestation, PreparePlanningGrantRequest, RecordPlanningGrantRequest,
    };
    use threadlane_native::profile_flow::RolePreflightRequest;
    use threadlane_native::EnqueuePlanningCommandRequest;

    let fx = OrchestratorTestFixture::new();
    let base_snap = fx.registry.snapshot().expect("base snapshot");

    let prepare_req = PreparePlanningGrantRequest {
        request_key: "prep_proof_drift_1".to_string(),
        binding_id: BINDING_ID.to_string(),
        thread_uuid: THREAD_UUID.to_string(),
        repository_path: fx.repo_dir.clone(),
        base_commit: fx.project_base.base_commit.clone(),
        expected_base_tree_sha: Some(fx.project_base.base_tree_sha.clone()),
        profile_id: "profile-main".to_string(),
        expected_registry_revision: Some(base_snap.revision),
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
        policy: OrchestratorTestFixture::default_policy(),
        capabilities: PlanningReadOnlyCapabilities::strict_read_only(),
        budget: PlanningCallBudget {
            max_total_calls: 8,
            max_orchestrator_calls: 4,
            max_architect_calls: 4,
            max_timeout_ms_per_call: 30_000,
            max_output_bytes_per_call: 65_536,
        },
        created_at_ms: 1_700_000_000_000,
        expires_at_ms: 1_800_000_000_000,
    };
    let preview = prepare_planning_grant_preview_with_store(&fx.store, &base_snap, &prepare_req)
        .expect("prepare preview");

    // 1. Tampered preview proof must fail closed on record_planning_grant.
    let mut tampered_preview = preview.clone();
    tampered_preview
        .scoped_profile_dependency_proof
        .spec
        .profile_id = "profile-tampered".to_string();
    let tamper_err = record_planning_grant(
        &fx.store,
        &base_snap,
        &RecordPlanningGrantRequest {
            request_key: "prep_proof_drift_1".to_string(),
            expected_ledger_revision: None,
            preview: tampered_preview,
            confirmed_preview_digest: preview.preview_digest.clone(),
            attestation: LocalOperatorGrantAttestation::attested(),
        },
    )
    .expect_err("tampered preview proof must be rejected");
    assert_eq!(tamper_err.code, NativeErrorCode::ValidationFailed);

    // 2. Stale preview recording when registry revision has advanced must fail closed.
    let unrelated_snap = fx
        .registry
        .apply(
            base_snap.revision,
            RegistryChange::UpsertProfile {
                profile: WorkflowProfile {
                    decision_advisor: None,
                    id: "profile-unrelated".to_string(),
                    name: "Unrelated Profile".to_string(),
                    assignments: vec![RoleAssignment {
                        role: WorkflowRole::Coder,
                        difficulty: None,
                        choice: ModelChoice {
                            model_id: "model-argon".to_string(),
                            effort: Some("high".to_string()),
                        },
                    }],
                },
            },
        )
        .expect("upsert unrelated profile");
    let stale_preview_err = record_planning_grant(
        &fx.store,
        &unrelated_snap,
        &RecordPlanningGrantRequest {
            request_key: "prep_proof_drift_1".to_string(),
            expected_ledger_revision: None,
            preview: preview.clone(),
            confirmed_preview_digest: preview.preview_digest.clone(),
            attestation: LocalOperatorGrantAttestation::attested(),
        },
    )
    .expect_err("recording a preview against a newer registry revision must fail closed");
    assert_eq!(stale_preview_err.code, NativeErrorCode::ValidationFailed);

    // Prepare and record a fresh proof-bound grant at `unrelated_snap`.
    let mut prepare_req2 = prepare_req.clone();
    prepare_req2.request_key = "prep_proof_drift_2".to_string();
    prepare_req2.expected_registry_revision = Some(unrelated_snap.revision);
    let preview2 =
        prepare_planning_grant_preview_with_store(&fx.store, &unrelated_snap, &prepare_req2)
            .expect("prepare preview 2");
    let confirmed_digest2 = preview2.preview_digest.clone();
    let grant_summary = record_planning_grant(
        &fx.store,
        &unrelated_snap,
        &RecordPlanningGrantRequest {
            request_key: "prep_proof_drift_2".to_string(),
            expected_ledger_revision: None,
            preview: preview2,
            confirmed_preview_digest: confirmed_digest2,
            attestation: LocalOperatorGrantAttestation::attested(),
        },
    )
    .expect("record proof-bound grant 2");

    let job = fx.create_initial_job(
        "job_proof_drift_1",
        vec!["Implement binary frame header parser in src/parser.rs".to_string()],
        Vec::new(),
        Vec::new(),
    );
    let commands = receipt_command_store(&fx);
    let original_profile = unrelated_snap.profiles[0].clone();
    let original_model = unrelated_snap.models[0].clone();
    let original_tool = unrelated_snap.tools[0].clone();

    // 3a. Selected model config drift before call must reject.
    let mut drifted_model = original_model.clone();
    drifted_model.model_arg = "gemini-4-argon-low".to_string();
    let rev = fx.registry.snapshot().expect("snap").revision;
    fx.registry
        .apply(
            rev,
            RegistryChange::UpsertModels {
                models: vec![drifted_model],
            },
        )
        .expect("drift model-argon");
    let req_model_drift = OrchestratorTestFixture::plan_req(
        &grant_summary.grant_id,
        &job.job_id,
        "orch_model_drift",
        0,
    );
    assert_eq!(
        commands
            .enqueue_command(&EnqueuePlanningCommandRequest::from_orchestrator(
                req_model_drift.clone(),
            ))
            .expect_err("enqueue must reject selected model drift")
            .code,
        NativeErrorCode::ValidationFailed
    );
    assert_eq!(
        fx.service
            .orchestrate_brief_to_plan(&fx.workflow, &req_model_drift)
            .await
            .expect_err("orchestrate must reject selected model drift")
            .code,
        NativeErrorCode::ValidationFailed
    );
    assert_eq!(fx.invocation_count(), 0);

    // Restore model-argon, then test selected tool disablement before call.
    let rev = fx.registry.snapshot().expect("snap").revision;
    fx.registry
        .apply(
            rev,
            RegistryChange::UpsertModels {
                models: vec![original_model.clone()],
            },
        )
        .expect("restore model-argon");
    let mut disabled_tool = original_tool.clone();
    disabled_tool.enabled = false;
    let rev = fx.registry.snapshot().expect("snap").revision;
    fx.registry
        .apply(
            rev,
            RegistryChange::UpsertTool {
                tool: disabled_tool,
            },
        )
        .expect("disable tool-agy");
    let req_tool_drift = OrchestratorTestFixture::plan_req(
        &grant_summary.grant_id,
        &job.job_id,
        "orch_tool_drift",
        0,
    );
    assert_eq!(
        fx.service
            .orchestrate_brief_to_plan(&fx.workflow, &req_tool_drift)
            .await
            .expect_err("orchestrate must reject selected tool disablement")
            .code,
        NativeErrorCode::ValidationFailed
    );
    assert_eq!(fx.invocation_count(), 0);

    // Restore tool-agy, then test selected profile deletion before call.
    let rev = fx.registry.snapshot().expect("snap").revision;
    fx.registry
        .apply(
            rev,
            RegistryChange::UpsertTool {
                tool: original_tool.clone(),
            },
        )
        .expect("restore tool-agy");
    let rev = fx.registry.snapshot().expect("snap").revision;
    fx.registry
        .apply(
            rev,
            RegistryChange::DeleteProfile {
                profile_id: "profile-main".to_string(),
            },
        )
        .expect("delete profile-main");
    let req_prof_del = OrchestratorTestFixture::plan_req(
        &grant_summary.grant_id,
        &job.job_id,
        "orch_prof_deleted",
        0,
    );
    assert_eq!(
        fx.service
            .orchestrate_brief_to_plan(&fx.workflow, &req_prof_del)
            .await
            .expect_err("orchestrate must reject deleted selected profile")
            .code,
        NativeErrorCode::ValidationFailed
    );
    assert_eq!(fx.invocation_count(), 0);

    // Restore profile-main and verify mid-flight selected dependency drift refuses publication/linkage.
    let rev = fx.registry.snapshot().expect("snap").revision;
    fx.registry
        .apply(
            rev,
            RegistryChange::UpsertProfile {
                profile: original_profile.clone(),
            },
        )
        .expect("restore profile-main");

    let in_flight_marker = fx.agy_prompts_dir.join("in_flight_selected_drift.marker");
    let resume_marker = fx.agy_prompts_dir.join("resume_selected_drift.marker");
    let hook_script = format!(
        "#!/bin/sh\nset -eu\n: > \"{}\"\ni=0\nwhile [ ! -f \"{}\" ] && [ \"$i\" -lt 250 ]; do\n  sleep 0.02\n  i=$((i + 1))\ndone\n",
        in_flight_marker.display(),
        resume_marker.display(),
    );
    fs::write(fx.agy_prompts_dir.join("hook_1.sh"), hook_script).expect("write hook_1.sh");
    fx.write_custom_call_response(
        1,
        r#"{"outcome":"plan","schema_version":1,"summary":"Plan compiled before mid-flight selected profile drift.","tasks":[{"task_id":"task_parse","title":"Implement binary frame header parser","description":"Parse binary frames in src/parser.rs.","role":"coder","difficulty":"medium","depends_on":[],"allowed_write_paths":["src/parser.rs"]}]}"#,
        false,
    );
    let req_mid_drift = OrchestratorTestFixture::plan_req(
        &grant_summary.grant_id,
        &job.job_id,
        "orch_mid_selected_drift",
        0,
    );
    let mid_flight_selected_drift = async {
        let deadline = Instant::now() + Duration::from_secs(5);
        while !in_flight_marker.exists() {
            assert!(Instant::now() < deadline, "timed out waiting for marker");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let mut drifted_profile = original_profile.clone();
        drifted_profile
            .assignments
            .retain(|a| a.role != WorkflowRole::Coder);
        let cur = fx.registry.snapshot().expect("mid-flight snap");
        fx.registry
            .apply(
                cur.revision,
                RegistryChange::UpsertProfile {
                    profile: drifted_profile,
                },
            )
            .expect("mutate selected profile assignments mid-flight");
        fs::write(&resume_marker, "go").expect("write resume marker");
    };
    let (mid_res, ()) = tokio::join!(
        fx.service
            .orchestrate_brief_to_plan(&fx.workflow, &req_mid_drift),
        mid_flight_selected_drift
    );
    assert_eq!(
        mid_res
            .expect_err("mid-flight selected profile drift must refuse plan publication/linkage")
            .code,
        NativeErrorCode::ValidationFailed
    );
    assert_eq!(fx.invocation_count(), 1);
    assert!(fx
        .workflow
        .list_plans(None, None)
        .expect("list plans")
        .plans
        .is_empty());

    // Restore profile-main, then verify legacy proof-less grants still enforce strict global revision equality.
    let rev = fx.registry.snapshot().expect("snap").revision;
    fx.registry
        .apply(
            rev,
            RegistryChange::UpsertProfile {
                profile: original_profile,
            },
        )
        .expect("restore profile-main after mid-flight test");
    let legacy_grant = fx.create_default_grant("grant_legacy_proofless_1");
    assert!(legacy_grant.scoped_profile_dependency_proof.is_none());
    let rev = fx.registry.snapshot().expect("snap").revision;
    fx.registry
        .apply(
            rev,
            RegistryChange::DeleteProfile {
                profile_id: "profile-unrelated".to_string(),
            },
        )
        .expect("delete unrelated profile after legacy grant");
    let req_legacy = OrchestratorTestFixture::plan_req(
        &legacy_grant.grant_id,
        &job.job_id,
        "orch_legacy_proofless_drift",
        0,
    );
    assert_eq!(
        fx.service
            .orchestrate_brief_to_plan(&fx.workflow, &req_legacy)
            .await
            .expect_err("legacy grant without proof must strictly reject global registry drift")
            .code,
        NativeErrorCode::ValidationFailed
    );
    assert_eq!(fx.invocation_count(), 1);
}

fn setup_codex_planning_fixture() -> (OrchestratorTestFixture, PathBuf, PathBuf, ExecutionPolicy) {
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
                    executable_path: Some(fake_codex_exe),
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

    (fx, codex_count_file, codex_prompts_dir, codex_policy)
}

fn create_codex_grant_with_budget(
    fx: &OrchestratorTestFixture,
    codex_policy: &ExecutionPolicy,
    request_key: &str,
    budget: PlanningCallBudget,
) -> threadlane_native::coworker_grants::PlanningGrantSummary {
    use threadlane_native::coworker_grants::{
        prepare_planning_grant_preview_with_store, record_planning_grant,
        LocalOperatorGrantAttestation, PreparePlanningGrantRequest, RecordPlanningGrantRequest,
    };
    use threadlane_native::profile_flow::RolePreflightRequest;

    let mut roles = Vec::new();
    if budget.max_orchestrator_calls > 0 {
        roles.push(RolePreflightRequest {
            role: WorkflowRole::Orchestrator,
            difficulty: None,
        });
    }
    if budget.max_architect_calls > 0 {
        roles.push(RolePreflightRequest {
            role: WorkflowRole::Architect,
            difficulty: None,
        });
    }

    let mut matching_policy = codex_policy.clone();
    for rule in &mut matching_policy.allow_rules {
        rule.roles.retain(|role| match role {
            WorkflowRole::Orchestrator => budget.max_orchestrator_calls > 0,
            WorkflowRole::Architect => budget.max_architect_calls > 0,
            _ => false,
        });
    }
    matching_policy
        .allow_rules
        .retain(|rule| !rule.roles.is_empty());

    let snapshot = fx.registry.snapshot().expect("snapshot with codex sol");
    let preview = prepare_planning_grant_preview_with_store(
        &fx.store,
        &snapshot,
        &PreparePlanningGrantRequest {
            request_key: request_key.to_string(),
            binding_id: BINDING_ID.to_string(),
            thread_uuid: THREAD_UUID.to_string(),
            repository_path: fx.repo_dir.clone(),
            base_commit: fx.project_base.base_commit.clone(),
            expected_base_tree_sha: Some(fx.project_base.base_tree_sha.clone()),
            profile_id: "profile-main".to_string(),
            expected_registry_revision: Some(snapshot.revision),
            roles,
            policy: matching_policy,
            capabilities: PlanningReadOnlyCapabilities::strict_read_only(),
            budget,
            created_at_ms: 1_700_000_000_000,
            expires_at_ms: 1_800_000_000_000,
        },
    )
    .expect("prepare codex planning grant preview");

    let ledger = fx.store.read_binding_ledger(BINDING_ID).ok();
    let expected_ledger_revision = ledger.map(|l| l.ledger_revision);
    record_planning_grant(
        &fx.store,
        &snapshot,
        &RecordPlanningGrantRequest {
            request_key: request_key.to_string(),
            expected_ledger_revision,
            preview: preview.clone(),
            confirmed_preview_digest: preview.preview_digest,
            attestation: LocalOperatorGrantAttestation::attested(),
        },
    )
    .expect("record codex planning grant")
}

fn format_codex_command_execution_line(
    item_id: &str,
    command: &str,
    aggregated_output: &str,
) -> String {
    serde_json::to_string(&serde_json::json!({
        "type": "item.completed",
        "item": {
            "id": item_id,
            "type": "command_execution",
            "command": command,
            "aggregated_output": aggregated_output,
            "exit_code": 0,
            "status": "completed"
        }
    }))
    .expect("serialize codex command_execution line")
}

fn format_codex_agent_message_line(item_id: &str, structured_json: &str) -> String {
    serde_json::to_string(&serde_json::json!({
        "type": "item.completed",
        "item": {
            "id": item_id,
            "type": "agent_message",
            "text": structured_json
        }
    }))
    .expect("serialize codex agent_message line")
}

#[tokio::test]
async fn codex_planning_accepts_completed_command_jsonl_event_above_256_kib_within_approved_budget()
{
    use threadlane_native::{
        EnqueuePlanningCommandRequest, PlanningCommandDriver, PlanningCommandResultRef,
        PlanningCommandStatus,
    };
    use threadlane_workers::{NormalizedEventKind, WorkerRunStatus, DEFAULT_MAX_LINE_BYTES};

    let (fx, codex_count_file, codex_prompts_dir, codex_policy) = setup_codex_planning_fixture();
    let recorded_grant = create_codex_grant_with_budget(
        &fx,
        &codex_policy,
        "req_grant_codex_large_line_1",
        PlanningCallBudget {
            max_total_calls: 4,
            max_orchestrator_calls: 2,
            max_architect_calls: 2,
            max_timeout_ms_per_call: 600_000,
            max_output_bytes_per_call: 4_194_304,
        },
    );

    let large_tool_output = "telemetry_symbol_inspection_line_0123456789abcdef\n".repeat(6_400);
    assert!(
        large_tool_output.len() > DEFAULT_MAX_LINE_BYTES,
        "command_execution payload must exceed legacy 256 KiB line limit"
    );

    // Call 1 (Orchestrator round 0): emits a >256 KiB completed command_execution line and requests Architect advice.
    let orch_round_0_json = r#"{"outcome":"requires_architect_advice","schema_version":1,"summary":"Consult Architect on ParsedFrame ownership boundary after inspecting large repository output.","questions":[{"question_id":"q_parsed_frame","question_summary":"Should ParsedFrame in src/parser.rs be a copyable header struct?","dependency_files":["src/parser.rs","src/exporter.rs"],"interfaces":["iface_parsed_frame"]}]}"#;
    let call_1_cmd_line = format_codex_command_execution_line(
        "cmd_orch_r0_large",
        "git grep -n ParsedFrame",
        &large_tool_output,
    );
    assert!(call_1_cmd_line.len() > DEFAULT_MAX_LINE_BYTES);
    let call_1_stream = format!(
        "{}\n{}\n{}\n{}\n{}\n",
        r#"{"type":"thread.started","thread_id":"01972d12-3456-7000-8000-000000000101"}"#,
        r#"{"type":"turn.started"}"#,
        call_1_cmd_line,
        format_codex_agent_message_line("msg_orch_r0", orch_round_0_json),
        r#"{"type":"turn.completed","usage":{"input_tokens":1500,"cached_input_tokens":300,"output_tokens":320}}"#,
    );
    fs::write(codex_prompts_dir.join("raw_stream_1.jsonl"), &call_1_stream)
        .expect("write raw_stream_1.jsonl");
    fs::write(
        codex_prompts_dir.join("last_message_1.json"),
        orch_round_0_json,
    )
    .expect("write last_message_1.json");

    // Call 2 (Architect round 1): emits a >256 KiB completed command_execution line and returns StructuredArchitectAdviceOutput.
    let arch_round_1_json = r#"{"schema_version":1,"decision_summary":"Keep ParsedFrame as a fixed-size header struct in src/parser.rs.","rationale":"Decouples frame header validation from exporter buffer lifetimes.","recommended_write_paths":["src/parser.rs","src/exporter.rs"],"interface_contracts":{"iface_parsed_frame":"pub struct ParsedFrame { pub version: u8, pub payload_len: u16 }"},"open_questions":[]}"#;
    let call_2_cmd_line = format_codex_command_execution_line(
        "cmd_arch_r1_large",
        "cargo metadata --format-version 1",
        &large_tool_output,
    );
    assert!(call_2_cmd_line.len() > DEFAULT_MAX_LINE_BYTES);
    let call_2_stream = format!(
        "{}\n{}\n{}\n{}\n{}\n",
        r#"{"type":"thread.started","thread_id":"01972d12-3456-7000-8000-000000000102"}"#,
        r#"{"type":"turn.started"}"#,
        call_2_cmd_line,
        format_codex_agent_message_line("msg_arch_r1", arch_round_1_json),
        r#"{"type":"turn.completed","usage":{"input_tokens":1620,"cached_input_tokens":400,"output_tokens":380}}"#,
    );
    fs::write(codex_prompts_dir.join("raw_stream_2.jsonl"), &call_2_stream)
        .expect("write raw_stream_2.jsonl");
    fs::write(
        codex_prompts_dir.join("last_message_2.json"),
        arch_round_1_json,
    )
    .expect("write last_message_2.json");

    // Call 3 (Orchestrator round 1): emits seven completed command_execution events including a >256 KiB line,
    // followed by the final valid structured plan and turn.completed.
    let orch_round_1_plan_json = r#"{"outcome":"plan","schema_version":1,"summary":"Compile parser, metrics, and exporter DAG after >256 KiB read-only Codex command inspection and Architect consultation.","tasks":[{"task_id":"task_parse","title":"Implement ParsedFrame header parser","description":"Define copyable ParsedFrame in src/parser.rs per Architect contract.","role":"coder","difficulty":"medium","depends_on":[],"allowed_write_paths":["src/parser.rs"],"owned_interfaces":["iface_parsed_frame"],"consumed_interfaces":[]},{"task_id":"task_metrics","title":"Add frame counter helper","description":"Update src/metrics.rs to track total parsed telemetry frames.","role":"coder","difficulty":"easy","depends_on":[],"allowed_write_paths":["src/metrics.rs"],"owned_interfaces":[],"consumed_interfaces":[]},{"task_id":"task_export","title":"Consume ParsedFrame in batch exporter","description":"Wire src/exporter.rs to iface_parsed_frame.","role":"coder","difficulty":"hard","depends_on":["task_parse"],"allowed_write_paths":["src/exporter.rs"],"owned_interfaces":["iface_export_sink"],"consumed_interfaces":["iface_parsed_frame"]}],"check_suggestions":[{"check_id":"chk_telemetry_unit","command_summary":"cargo test --workspace","required":true}],"unconfirmed_proposal_notes":["Keep optional pushgateway exporter out of scope until human approval."],"resolved_open_questions":["Should ParsedFrame use a value header struct?"],"deferred_proposals":["Add optional Prometheus pushgateway sink"]}"#;
    let mut call_3_lines = vec![
        r#"{"type":"thread.started","thread_id":"01972d12-3456-7000-8000-000000000103"}"#
            .to_string(),
        r#"{"type":"turn.started"}"#.to_string(),
    ];
    for idx in 1..=6 {
        call_3_lines.push(format_codex_command_execution_line(
            &format!("cmd_orch_r1_{idx}"),
            "git status --porcelain",
            "clean_tree_check\n",
        ));
    }
    let call_3_large_cmd_line =
        format_codex_command_execution_line("cmd_orch_r1_7", "git log -p -n 5", &large_tool_output);
    assert!(call_3_large_cmd_line.len() > DEFAULT_MAX_LINE_BYTES);
    call_3_lines.push(call_3_large_cmd_line);
    call_3_lines.push(format_codex_agent_message_line(
        "msg_orch_r1_plan",
        orch_round_1_plan_json,
    ));
    call_3_lines.push(
        r#"{"type":"turn.completed","usage":{"input_tokens":1840,"cached_input_tokens":512,"output_tokens":450}}"#
            .to_string(),
    );
    let call_3_stream = format!("{}\n", call_3_lines.join("\n"));
    fs::write(codex_prompts_dir.join("raw_stream_3.jsonl"), &call_3_stream)
        .expect("write raw_stream_3.jsonl");
    fs::write(
        codex_prompts_dir.join("last_message_3.json"),
        orch_round_1_plan_json,
    )
    .expect("write last_message_3.json");

    let job = fx.create_initial_job(
        "job_codex_large_line_1",
        vec![
            "Implement binary frame header parser in src/parser.rs".to_string(),
            "Add frame counter helper in src/metrics.rs".to_string(),
            "Wire batch exporter in src/exporter.rs to consume parsed frames".to_string(),
        ],
        vec!["Should ParsedFrame use a value header struct?".to_string()],
        vec!["Add optional Prometheus pushgateway sink".to_string()],
    );

    let cmd_store = receipt_command_store(&fx);
    let plan_req = OrchestratorTestFixture::plan_req(
        &recorded_grant.grant_id,
        &job.job_id,
        "cmd_orch_codex_large_line_1",
        1,
    );
    let enqueued = cmd_store
        .enqueue_command(&EnqueuePlanningCommandRequest::from_orchestrator(plan_req))
        .expect("enqueue codex orchestrator command");
    let driver =
        PlanningCommandDriver::new(cmd_store.clone(), fx.service.clone()).expect("create driver");
    let executed = driver
        .execute_command(BINDING_ID, THREAD_UUID, &enqueued.command_id)
        .await
        .expect("execute codex planning command with >256 KiB tool-result JSONL events");
    assert_eq!(executed.command.status, PlanningCommandStatus::Completed);

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
    assert_eq!(planning_result.plan_revision, 1);
    assert_eq!(planning_result.architect_advice_ids.len(), 1);

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
    assert_eq!(codex_invocations, 3);

    // Verify all 3 reservations (Orchestrator round 0, Architect round 1, Orchestrator round 1)
    // genuinely ingested the >256 KiB stdout streams and recorded Tool events without overflow.
    let ledger = fx.store.read_binding_ledger(BINDING_ID).expect("ledger");
    let grant_after = ledger
        .planning_grants
        .iter()
        .find(|g| g.grant_id == recorded_grant.grant_id)
        .expect("find grant");
    assert_eq!(grant_after.reservations.len(), 3);
    for (idx, expected_stream_len) in [
        (0usize, call_1_stream.len()),
        (1usize, call_2_stream.len()),
        (2usize, call_3_stream.len()),
    ] {
        let res = &grant_after.reservations[idx];
        assert_eq!(res.status, PlanningReservationStatus::Completed);
        let art_id = res
            .report_artifact_id
            .as_deref()
            .expect("reservation artifact id");
        let artifact = fx
            .service
            .read_planning_artifact(art_id)
            .expect("read persisted planning artifact");
        assert_eq!(artifact.execution.status, WorkerRunStatus::Succeeded);
        assert!(artifact.execution.overflow.is_none());
        assert_eq!(
            artifact.execution.stdout_bytes_captured,
            expected_stream_len
        );
        assert!(
            artifact.execution.stdout_bytes_captured > DEFAULT_MAX_LINE_BYTES,
            "reservation {idx} must have captured >256 KiB of stdout"
        );
        assert!(artifact.execution.evidence.saw_init_event);
        assert!(artifact.execution.evidence.saw_result_event);
        assert_eq!(artifact.execution.evidence.malformed_lines_count, 0);
        assert!(artifact
            .execution
            .evidence
            .retained_events
            .iter()
            .any(|ev| {
                ev.kind == NormalizedEventKind::Tool
                    && ev.raw_type == "item.completed:command_execution"
            }));
    }
    assert_eq!(
        fx.service
            .read_planning_artifact(
                grant_after.reservations[2]
                    .report_artifact_id
                    .as_deref()
                    .expect("round 1 artifact id"),
            )
            .expect("round 1 artifact")
            .execution
            .evidence
            .total_lines,
        11
    );
}

#[tokio::test]
async fn codex_planning_preserves_total_overflow_hard_line_ceiling_unsealed_rejection_and_legacy_provider_line_limit(
) {
    use threadlane_workers::{
        AdvisoryResponseErrorCode, OutputOverflowKind, WorkerRunStatus, DEFAULT_MAX_LINE_BYTES,
        MAX_LINE_BYTES,
    };

    let (fx, _codex_count_file, codex_prompts_dir, codex_policy) = setup_codex_planning_fixture();
    let job = fx.create_initial_job(
        "job_codex_overflow_and_rules_1",
        vec!["Implement binary frame header parser in src/parser.rs".to_string()],
        Vec::new(),
        Vec::new(),
    );
    let valid_plan_json = r#"{"outcome":"plan","schema_version":1,"summary":"Plan summary.","tasks":[{"task_id":"task_parse","title":"Implement parser","description":"Parse frames in src/parser.rs.","role":"coder","difficulty":"medium","depends_on":[],"allowed_write_paths":["src/parser.rs"]}]}"#;

    // 1. Whole-stream total stdout budget overflow (`StdoutTotalExceeded`):
    // Approved total cap is 512 KiB (524,288 bytes). Emit two ~300 KiB command_execution lines
    // (each individual line <= 512 KiB line budget, but combined > 512 KiB total budget).
    let total_cap_grant = create_codex_grant_with_budget(
        &fx,
        &codex_policy,
        "req_grant_codex_total_overflow_1",
        PlanningCallBudget {
            max_total_calls: 2,
            max_orchestrator_calls: 2,
            max_architect_calls: 0,
            max_timeout_ms_per_call: 60_000,
            max_output_bytes_per_call: 524_288,
        },
    );
    let chunk_300_kib = "x".repeat(300_000);
    let cmd_line_a =
        format_codex_command_execution_line("cmd_tot_1", "git log -n 20", &chunk_300_kib);
    let cmd_line_b =
        format_codex_command_execution_line("cmd_tot_2", "git diff HEAD~1", &chunk_300_kib);
    assert!(cmd_line_a.len() > DEFAULT_MAX_LINE_BYTES && cmd_line_a.len() < 524_288);
    assert!(cmd_line_a.len() + cmd_line_b.len() > 524_288);
    let call_1_total_overflow_stream = format!(
        "{}\n{}\n{}\n{}\n{}\n{}\n",
        r#"{"type":"thread.started","thread_id":"01972d12-3456-7000-8000-000000000201"}"#,
        r#"{"type":"turn.started"}"#,
        cmd_line_a,
        cmd_line_b,
        format_codex_agent_message_line("msg_tot", valid_plan_json),
        r#"{"type":"turn.completed","usage":{"input_tokens":100,"cached_input_tokens":0,"output_tokens":50}}"#,
    );
    fs::write(
        codex_prompts_dir.join("raw_stream_1.jsonl"),
        &call_1_total_overflow_stream,
    )
    .expect("write raw_stream_1.jsonl");

    let req_total_overflow = OrchestratorTestFixture::plan_req(
        &total_cap_grant.grant_id,
        &job.job_id,
        "orch_codex_total_overflow_1",
        0,
    );
    let err_total = fx
        .service
        .orchestrate_brief_to_plan(&fx.workflow, &req_total_overflow)
        .await
        .expect_err("total stdout budget overflow must fail closed");
    assert_eq!(err_total.code, NativeErrorCode::ValidationFailed);

    let ledger = fx.store.read_binding_ledger(BINDING_ID).expect("ledger");
    let res_total = &ledger
        .planning_grants
        .iter()
        .find(|g| g.grant_id == total_cap_grant.grant_id)
        .expect("find total_cap_grant")
        .reservations[0];
    assert_eq!(
        res_total.status,
        PlanningReservationStatus::FailedAfterEffect
    );
    let art_total = fx
        .service
        .read_planning_artifact(res_total.report_artifact_id.as_deref().expect("art_total"))
        .expect("read art_total");
    assert_eq!(art_total.execution.status, WorkerRunStatus::OutputOverflow);
    assert_eq!(
        art_total.execution.overflow,
        Some(OutputOverflowKind::StdoutTotalExceeded {
            max_stdout_bytes: 524_288
        })
    );
    let PlanningArtifactOutcomeKind::FailedAfterEffect {
        response_error: Some(ref resp_err_total),
        ..
    } = art_total.outcome
    else {
        panic!("expected FailedAfterEffect with response_error");
    };
    assert_eq!(
        resp_err_total.code,
        AdvisoryResponseErrorCode::StreamOutputOverflow
    );

    // 2. Hard library line ceiling (`MAX_LINE_BYTES` = 4 MiB) when approved total stdout budget is larger (8 MiB),
    // plus unsealed and malformed terminal stream rejection after a valid >256 KiB command_execution event:
    let ceiling_grant = create_codex_grant_with_budget(
        &fx,
        &codex_policy,
        "req_grant_codex_hard_ceiling_1",
        PlanningCallBudget {
            max_total_calls: 4,
            max_orchestrator_calls: 4,
            max_architect_calls: 0,
            max_timeout_ms_per_call: 60_000,
            max_output_bytes_per_call: 8 * 1024 * 1024,
        },
    );
    let over_4_mib_payload = "y".repeat(MAX_LINE_BYTES + 4_096);
    let over_4_mib_line =
        format_codex_command_execution_line("cmd_over_4mib", "git log -p", &over_4_mib_payload);
    assert!(over_4_mib_line.len() > MAX_LINE_BYTES);
    assert!(over_4_mib_line.len() < 8 * 1024 * 1024);
    let call_2_line_ceiling_stream = format!(
        "{}\n{}\n{}\n{}\n{}\n",
        r#"{"type":"thread.started","thread_id":"01972d12-3456-7000-8000-000000000202"}"#,
        r#"{"type":"turn.started"}"#,
        over_4_mib_line,
        format_codex_agent_message_line("msg_ceil", valid_plan_json),
        r#"{"type":"turn.completed","usage":{"input_tokens":100,"cached_input_tokens":0,"output_tokens":50}}"#,
    );
    fs::write(
        codex_prompts_dir.join("raw_stream_2.jsonl"),
        &call_2_line_ceiling_stream,
    )
    .expect("write raw_stream_2.jsonl");

    let req_ceiling = OrchestratorTestFixture::plan_req(
        &ceiling_grant.grant_id,
        &job.job_id,
        "orch_codex_hard_line_ceiling_2",
        0,
    );
    let err_ceiling = fx
        .service
        .orchestrate_brief_to_plan(&fx.workflow, &req_ceiling)
        .await
        .expect_err(
            "single line above MAX_LINE_BYTES must fail closed even under 8 MiB stdout cap",
        );
    assert_eq!(err_ceiling.code, NativeErrorCode::ValidationFailed);

    let ledger = fx.store.read_binding_ledger(BINDING_ID).expect("ledger");
    let res_ceiling = &ledger
        .planning_grants
        .iter()
        .find(|g| g.grant_id == ceiling_grant.grant_id)
        .expect("find ceiling_grant")
        .reservations[0];
    assert_eq!(
        res_ceiling.status,
        PlanningReservationStatus::FailedAfterEffect
    );
    let art_ceiling = fx
        .service
        .read_planning_artifact(
            res_ceiling
                .report_artifact_id
                .as_deref()
                .expect("art_ceiling"),
        )
        .expect("read art_ceiling");
    assert_eq!(
        art_ceiling.execution.status,
        WorkerRunStatus::OutputOverflow
    );
    assert_eq!(
        art_ceiling.execution.overflow,
        Some(OutputOverflowKind::StdoutLineExceeded {
            max_line_bytes: MAX_LINE_BYTES
        })
    );

    // 3. Unsealed terminal stream (missing `turn.completed`) and malformed JSON line after a >256 KiB event line:
    let large_reasoning_line = serde_json::to_string(&serde_json::json!({
        "type": "item.completed",
        "item": {
            "id": "rsn_unsealed_large",
            "type": "reasoning",
            "text": chunk_300_kib
        }
    }))
    .expect("serialize codex reasoning line");
    assert!(large_reasoning_line.len() > DEFAULT_MAX_LINE_BYTES);
    let call_3_unsealed_stream = format!(
        "{}\n{}\n{}\n{}\n",
        r#"{"type":"thread.started","thread_id":"01972d12-3456-7000-8000-000000000203"}"#,
        r#"{"type":"turn.started"}"#,
        large_reasoning_line,
        format_codex_agent_message_line("msg_unsealed", valid_plan_json),
    );
    fs::write(
        codex_prompts_dir.join("raw_stream_3.jsonl"),
        &call_3_unsealed_stream,
    )
    .expect("write raw_stream_3.jsonl");
    let req_unsealed = OrchestratorTestFixture::plan_req(
        &ceiling_grant.grant_id,
        &job.job_id,
        "orch_codex_unsealed_3",
        0,
    );
    assert_eq!(
        fx.service
            .orchestrate_brief_to_plan(&fx.workflow, &req_unsealed)
            .await
            .expect_err("unsealed Codex stream missing turn.completed must fail closed")
            .code,
        NativeErrorCode::ValidationFailed
    );

    let call_4_malformed_stream = format!(
        "{}\n{}\n{}\n{}\n{}\n{}\n",
        r#"{"type":"thread.started","thread_id":"01972d12-3456-7000-8000-000000000204"}"#,
        r#"{"type":"turn.started"}"#,
        cmd_line_a,
        r#"{"type":"item.completed","item":{"id":"bad_json""#,
        format_codex_agent_message_line("msg_malformed", valid_plan_json),
        r#"{"type":"turn.completed","usage":{"input_tokens":100,"cached_input_tokens":0,"output_tokens":50}}"#,
    );
    fs::write(
        codex_prompts_dir.join("raw_stream_4.jsonl"),
        &call_4_malformed_stream,
    )
    .expect("write raw_stream_4.jsonl");
    let req_malformed = OrchestratorTestFixture::plan_req(
        &ceiling_grant.grant_id,
        &job.job_id,
        "orch_codex_malformed_4",
        0,
    );
    assert_eq!(
        fx.service
            .orchestrate_brief_to_plan(&fx.workflow, &req_malformed)
            .await
            .expect_err("malformed JSON line in Codex stream must fail closed")
            .code,
        NativeErrorCode::ValidationFailed
    );

    let ledger = fx.store.read_binding_ledger(BINDING_ID).expect("ledger");
    let ceiling_grant_after = ledger
        .planning_grants
        .iter()
        .find(|g| g.grant_id == ceiling_grant.grant_id)
        .expect("find ceiling_grant after unsealed/malformed");
    let art_unsealed = fx
        .service
        .read_planning_artifact(
            ceiling_grant_after.reservations[1]
                .report_artifact_id
                .as_deref()
                .expect("art_unsealed"),
        )
        .expect("read art_unsealed");
    assert_eq!(
        art_unsealed.execution.status,
        WorkerRunStatus::MissingResultEvidence
    );
    assert!(!art_unsealed.execution.evidence.saw_result_event);
    let PlanningArtifactOutcomeKind::FailedAfterEffect {
        response_error: Some(ref err_unsealed_resp),
        ..
    } = art_unsealed.outcome
    else {
        panic!("expected FailedAfterEffect on unsealed stream");
    };
    assert_eq!(
        err_unsealed_resp.code,
        AdvisoryResponseErrorCode::MissingTerminalFraming
    );

    let art_malformed = fx
        .service
        .read_planning_artifact(
            ceiling_grant_after.reservations[2]
                .report_artifact_id
                .as_deref()
                .expect("art_malformed"),
        )
        .expect("read art_malformed");
    assert_eq!(
        art_malformed.execution.status,
        WorkerRunStatus::MalformedStream
    );
    assert_eq!(art_malformed.execution.evidence.malformed_lines_count, 1);
    let PlanningArtifactOutcomeKind::FailedAfterEffect {
        response_error: Some(ref err_malformed_resp),
        ..
    } = art_malformed.outcome
    else {
        panic!("expected FailedAfterEffect on malformed stream");
    };
    assert_eq!(
        err_malformed_resp.code,
        AdvisoryResponseErrorCode::MalformedStream
    );

    // 4. Legacy provider (`WorkerProvider::Antigravity`) retains `DEFAULT_MAX_LINE_BYTES` (256 KiB)
    // strictness even when `max_output_bytes_per_call` is 4 MiB:
    let agy_fx = OrchestratorTestFixture::new();
    let agy_grant = agy_fx.create_grant_with_budget(
        "grant_agy_legacy_line_limit_1",
        PlanningCallBudget {
            max_total_calls: 2,
            max_orchestrator_calls: 2,
            max_architect_calls: 0,
            max_timeout_ms_per_call: 60_000,
            max_output_bytes_per_call: 4_194_304,
        },
        1_800_000_000_000,
    );
    let agy_job = agy_fx.create_initial_job(
        "job_agy_legacy_line_limit_1",
        vec!["Implement binary frame header parser in src/parser.rs".to_string()],
        Vec::new(),
        Vec::new(),
    );
    agy_fx.set_mode("custom_file");
    let agy_large_step_line = serde_json::to_string(&serde_json::json!({
        "event": "step_update",
        "step_update": {
            "step_type": "tool_result",
            "tool_name": "read_file",
            "output": chunk_300_kib
        }
    }))
    .expect("serialize agy large step line");
    assert!(agy_large_step_line.len() > DEFAULT_MAX_LINE_BYTES);
    let agy_result_line = serde_json::to_string(&serde_json::json!({
        "event": "result",
        "result": {
            "status": "SUCCESS",
            "response": valid_plan_json
        }
    }))
    .expect("serialize agy result line");
    fs::write(
        agy_fx.agy_prompts_dir.join("raw_after_init_1.jsonl"),
        format!("{agy_large_step_line}\n{agy_result_line}\n"),
    )
    .expect("write raw_after_init_1.jsonl");

    let req_agy = OrchestratorTestFixture::plan_req(
        &agy_grant.grant_id,
        &agy_job.job_id,
        "orch_agy_legacy_line_limit_1",
        0,
    );
    assert_eq!(
        agy_fx
            .service
            .orchestrate_brief_to_plan(&agy_fx.workflow, &req_agy)
            .await
            .expect_err("Antigravity must retain 256 KiB DEFAULT_MAX_LINE_BYTES limit")
            .code,
        NativeErrorCode::ValidationFailed
    );
    let agy_ledger = agy_fx
        .store
        .read_binding_ledger(BINDING_ID)
        .expect("agy ledger");
    let agy_res = &agy_ledger
        .planning_grants
        .iter()
        .find(|g| g.grant_id == agy_grant.grant_id)
        .expect("find agy_grant")
        .reservations[0];
    let agy_art = agy_fx
        .service
        .read_planning_artifact(agy_res.report_artifact_id.as_deref().expect("agy_art"))
        .expect("read agy_art");
    assert_eq!(agy_art.execution.status, WorkerRunStatus::OutputOverflow);
    assert_eq!(
        agy_art.execution.overflow,
        Some(OutputOverflowKind::StdoutLineExceeded {
            max_line_bytes: DEFAULT_MAX_LINE_BYTES
        })
    );
    assert!(fx
        .workflow
        .list_plans(None, None)
        .expect("list plans")
        .plans
        .is_empty());
    assert!(agy_fx
        .workflow
        .list_plans(None, None)
        .expect("list agy plans")
        .plans
        .is_empty());
}

#[tokio::test]
async fn planning_health_uses_actual_command_guard_and_reservation_and_reopens_unknown_history() {
    use threadlane_native::coworker_workflow::ReservePlanningCallRequest;
    use threadlane_native::provider_health::{supervise_health, PlanningHealthOwner};
    use threadlane_native::{
        current_wall_clock_ms, EnqueuePlanningCommandRequest, PlanningCommandStatus,
    };
    let fx = OrchestratorTestFixture::new();
    let grant = fx.create_default_grant("health-command-grant");
    let job = fx.create_initial_job(
        "health-command-job",
        vec!["Implement telemetry parser".into()],
        vec![],
        vec![],
    );
    let commands = receipt_command_store(&fx);
    let request =
        OrchestratorTestFixture::plan_req(&grant.grant_id, &job.job_id, "health-command", 0);
    let queued = commands
        .enqueue_command(&EnqueuePlanningCommandRequest::from_orchestrator(request))
        .expect("enqueue");
    let key = queued.ownership_key().expect("command key");
    let now = current_wall_clock_ms().expect("clock");
    let guard = commands
        .ownership_store()
        .try_acquire_job(&key, now)
        .expect("acquire")
        .into_guard()
        .expect("physical guard");
    let command = commands
        .claim_running(&guard, BINDING_ID, THREAD_UUID, &queued.command_id, None)
        .expect("claim command");
    let reservation = fx
        .store
        .reserve_planning_call(&ReservePlanningCallRequest {
            binding_id: BINDING_ID.into(),
            thread_uuid: THREAD_UUID.into(),
            grant_id: grant.grant_id,
            request_key: command.service_request_key.clone(),
            expected_ledger_revision: None,
            role: command.role,
            difficulty: command.payload.difficulty(),
            selected_choice: command.selected_choice.clone(),
            context_digest: command.context_digest.clone(),
            call_project_base: Some(command.call_project_base.clone()),
            current_time_ms: now,
        })
        .expect("actual reservation");
    let owner = PlanningHealthOwner::new(&commands, &guard, &command)
        .expect("exact physical command owner");
    let sink = owner.invocation(&reservation).expect("exact reservation");
    let command_path = commands
        .ledger_path_for_binding(BINDING_ID)
        .expect("command path");
    let before = fs::read(&command_path).expect("command bytes before rejection");
    let mut wrong_reservation = reservation.clone();
    wrong_reservation.reservation_id = "prs_wrong_reservation".into();
    assert!(owner.invocation(&wrong_reservation).is_err());
    let mut wrong_subject = reservation.clone();
    wrong_subject.context_digest = "f".repeat(64);
    assert!(owner.invocation(&wrong_subject).is_err());
    assert_eq!(
        before,
        fs::read(&command_path).expect("command bytes after rejection")
    );

    let mut parser =
        threadlane_workers::antigravity_diagnostics::AntigravityDiagnosticParser::new();
    parser.ingest_at(0, b"I0101 00:00:01.000000 1 run.go:315] Run: attempt 1 failed (stream reading error: stream idle timeout (5m0s): context deadline exceeded), retrying in 4s.\n", 10);
    let snapshot = parser.snapshot();
    let expected = snapshot.clone();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let invocation = supervise_health(
        Some(&sink),
        |observer| async move {
            observer.send_replace(Some(snapshot.clone()));
            release_rx.await.expect("release finite fake invocation");
            Ok::<_, ()>(snapshot)
        },
        |snapshot| Some(snapshot),
    );
    let inspect = async {
        let deadline = Instant::now() + Duration::from_secs(3);
        loop {
            let current = commands
                .get_command(BINDING_ID, THREAD_UUID, &command.command_id)
                .expect("read live command")
                .expect("command");
            if !current.provider_health.is_empty() {
                assert_eq!(current.status, PlanningCommandStatus::Running);
                assert_eq!(current.command_digest, command.command_digest);
                assert_eq!(current.provider_health[0].snapshot, expected);
                assert!(!current.provider_health[0].terminal_observed);
                break;
            }
            assert!(
                Instant::now() < deadline,
                "missing durable live command diagnostic"
            );
            tokio::task::yield_now().await;
        }
        release_tx.send(()).expect("release fake invocation");
    };
    let (result, ()) = tokio::join!(invocation, inspect);
    assert_eq!(result.expect("independent result"), expected);
    let finished = commands
        .get_command(BINDING_ID, THREAD_UUID, &command.command_id)
        .expect("terminal history")
        .expect("command");
    assert!(finished.provider_health[0].terminal_observed);
    assert_eq!(finished.command_digest, command.command_digest);
    assert_eq!(
        finished.status,
        PlanningCommandStatus::Running,
        "a diagnostic return has no command lifecycle authority"
    );
    assert_eq!(
        fx.invocation_count(),
        0,
        "scope checks and metadata never dispatch a provider"
    );

    drop(sink);
    drop(guard);
    let reopened = receipt_command_store(&fx)
        .get_command(BINDING_ID, THREAD_UUID, &command.command_id)
        .expect("reopen")
        .expect("historical command");
    assert_eq!(reopened.status, PlanningCommandStatus::Unknown);
    assert!(reopened.pending_reconciliation);
    assert_eq!(reopened.provider_health, finished.provider_health);
    assert_eq!(reopened.command_digest, command.command_digest);
}

#[tokio::test]
async fn reconciliation_preserves_old_health_owner_without_live_update_or_redispatch_authority() {
    use threadlane_native::provider_health::{
        PlanningHealthOwner, ProviderHealthInvocation, ScopedProviderHealth,
    };
    use threadlane_native::{
        current_wall_clock_ms, EnqueuePlanningCommandRequest, PlanningCommandBindingLedger,
        PlanningCommandStatus,
    };

    let fx = OrchestratorTestFixture::new();
    let grant = fx.create_default_grant("health-history-grant");
    let job = fx.create_initial_job(
        "health-history-job",
        vec!["Implement telemetry parser".into()],
        vec![],
        vec![],
    );
    let driver =
        threadlane_native::open_planning_driver_for_plans_dir(&fx.plans_dir, &fx.registry_dir)
            .unwrap();
    let commands = driver.command_store().clone();
    let queued = commands
        .enqueue_command(&EnqueuePlanningCommandRequest::from_orchestrator(
            OrchestratorTestFixture::plan_req(
                &grant.grant_id,
                &job.job_id,
                "health-history-command",
                0,
            ),
        ))
        .unwrap();
    let completed = driver
        .execute_command(BINDING_ID, THREAD_UUID, &queued.command_id)
        .await
        .unwrap();
    let original = completed.command;
    let original_owner = original.owner_summary.clone().unwrap();
    let calls_before_recovery = fx.invocation_count();
    let threadlane_native::PlanningCommandResultRef::OrchestratorPlan {
        planning_result, ..
    } = original.result_ref.as_ref().unwrap()
    else {
        panic!("expected plan receipt");
    };

    // Model the crash window after a verified receipt but before command completion.
    // All edits are confined to this disposable command ledger; no provider is redispatched.
    let path = commands.ledger_path_for_binding(BINDING_ID).unwrap();
    let mut ledger: PlanningCommandBindingLedger =
        serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
    let interrupted = ledger
        .commands
        .iter_mut()
        .find(|c| c.command_id == queued.command_id)
        .unwrap();
    let mut parser =
        threadlane_workers::antigravity_diagnostics::AntigravityDiagnosticParser::new();
    parser.ingest_at(0, b"I0101 00:00:01.000000 1 run.go:315] Run: attempt 1 failed (stream reading error: stream idle timeout (5m0s): context deadline exceeded), retrying in 4s.\n", 10);
    interrupted.provider_health = vec![ScopedProviderHealth {
        ownership_key: original.ownership_key().unwrap(),
        owner: original_owner.clone(),
        lease_epoch: None,
        invocation: ProviderHealthInvocation::Planning {
            grant_id: planning_result.grant_id.clone(),
            reservation_id: planning_result.reservation_id.clone(),
            request_key: planning_result.request_key.clone(),
            context_digest: original.context_digest.clone(),
            role: original.role,
        },
        snapshot: parser.snapshot(),
        terminal_observed: false,
    }];
    interrupted.status = PlanningCommandStatus::Unknown;
    interrupted.pending_reconciliation = true;
    interrupted.result_ref = None;
    interrupted.completed_at_ms = None;
    interrupted.diagnostic = Some("synthetic crash after receipt".into());
    interrupted.validate().unwrap();
    let history = interrupted.provider_health.clone();
    fs::write(&path, serde_json::to_vec(&ledger).unwrap()).unwrap();

    let recovered = driver
        .execute_command(BINDING_ID, THREAD_UUID, &queued.command_id)
        .await
        .unwrap();
    assert!(recovered.reconciled_from_existing);
    assert_eq!(recovered.command.status, PlanningCommandStatus::Completed);
    assert_eq!(recovered.command.result_ref, original.result_ref);
    assert_eq!(recovered.command.command_digest, original.command_digest);
    assert_ne!(
        recovered.command.owner_summary.as_ref(),
        Some(&original_owner)
    );
    assert_eq!(recovered.command.provider_health, history);
    assert_eq!(fx.invocation_count(), calls_before_recovery);

    // A physically held new guard does not make retained old-owner hints live.
    let guard = commands
        .ownership_store()
        .try_acquire_job(
            &original.ownership_key().unwrap(),
            current_wall_clock_ms().unwrap(),
        )
        .unwrap()
        .into_guard()
        .unwrap();
    assert!(PlanningHealthOwner::new(&commands, &guard, &recovered.command).is_err());
    let summary = threadlane_native::get_planning_command_summary_readonly(
        &fx.plans_dir,
        &fx.registry_dir,
        BINDING_ID,
        THREAD_UUID,
        &queued.command_id,
    )
    .unwrap()
    .unwrap();
    assert_eq!(summary.provider_health, history);
    assert!(summary.provider_health_views.iter().all(|view| !view.live));
    assert!(summary.provider_health[0]
        .snapshot
        .current_failure
        .is_some());
    assert_eq!(
        commands
            .get_command(BINDING_ID, THREAD_UUID, &queued.command_id)
            .unwrap()
            .unwrap()
            .provider_health,
        history,
    );
}

#[tokio::test]
async fn codex_planning_accepts_intermediate_completed_agent_messages_with_designated_last_message_and_rejects_missing_or_mismatched_capture(
) {
    use threadlane_native::coworker_workflow::StructuredOrchestratorOutput;
    use threadlane_native::{
        EnqueuePlanningCommandRequest, PlanningCommandDriver, PlanningCommandResultRef,
        PlanningCommandStatus,
    };
    use threadlane_workers::{AdvisoryResponseErrorCode, AdvisoryResponseSource, WorkerRunStatus};

    let (fx, codex_count_file, codex_prompts_dir, codex_policy) = setup_codex_planning_fixture();
    let recorded_grant = create_codex_grant_with_budget(
        &fx,
        &codex_policy,
        "req_grant_codex_designated_final_1",
        PlanningCallBudget {
            max_total_calls: 6,
            max_orchestrator_calls: 4,
            max_architect_calls: 2,
            max_timeout_ms_per_call: 60_000,
            max_output_bytes_per_call: 262_144,
        },
    );

    // 1. Orchestrator round 0 -> Architect round 1 -> Orchestrator round 1 with bounded intermediate
    //    completed `agent_message` items (including intermediate JSON objects and fenced notes) plus
    //    verified `--output-last-message` outputs.
    let orch_r0_intermediate = r#"{"stage":"inspecting_repo","final":false}"#;
    let orch_r0_final = r#"{"outcome":"requires_architect_advice","schema_version":1,"summary":"Consult Architect on ParsedFrame header contract.","questions":[{"question_id":"q_header","question_summary":"Should ParsedFrame expose copyable header fields?","dependency_files":["src/parser.rs","src/exporter.rs"],"interfaces":["iface_parsed_frame"]}]}"#;
    let call_1_stream = format!(
        "{}\n{}\n{}\n{}\n{}\n{}\n",
        r#"{"type":"thread.started","thread_id":"01972d12-3456-7000-8000-000000000951"}"#,
        r#"{"type":"turn.started"}"#,
        format_codex_agent_message_line("msg_orch_r0_note", orch_r0_intermediate),
        format_codex_command_execution_line("cmd_orch_r0", "git status --short", ""),
        format_codex_agent_message_line("msg_orch_r0_final", orch_r0_final),
        r#"{"type":"turn.completed","usage":{"input_tokens":420,"cached_input_tokens":100,"output_tokens":110}}"#,
    );
    fs::write(codex_prompts_dir.join("raw_stream_1.jsonl"), &call_1_stream).unwrap();
    fs::write(codex_prompts_dir.join("last_message_1.json"), orch_r0_final).unwrap();

    let arch_r1_note = "Inspecting interface boundaries:\n```json\n{\"draft\":true}\n```";
    let arch_r1_final = r#"{"schema_version":1,"decision_summary":"Keep ParsedFrame as a fixed-size header struct in src/parser.rs.","rationale":"Avoids coupling frame parsing to exporter batch buffers.","recommended_write_paths":["src/parser.rs","src/exporter.rs"],"interface_contracts":{"iface_parsed_frame":"pub struct ParsedFrame { pub version: u8, pub payload_len: u16 }"},"open_questions":[]}"#;
    let call_2_stream = format!(
        "{}\n{}\n{}\n{}\n{}\n",
        r#"{"type":"thread.started","thread_id":"01972d12-3456-7000-8000-000000000952"}"#,
        r#"{"type":"turn.started"}"#,
        format_codex_agent_message_line("msg_arch_r1_note", arch_r1_note),
        format_codex_agent_message_line("msg_arch_r1_final", arch_r1_final),
        r#"{"type":"turn.completed","usage":{"input_tokens":510,"cached_input_tokens":128,"output_tokens":140}}"#,
    );
    fs::write(codex_prompts_dir.join("raw_stream_2.jsonl"), &call_2_stream).unwrap();
    fs::write(codex_prompts_dir.join("last_message_2.json"), arch_r1_final).unwrap();

    let orch_r1_draft = r#"{"checkpoint":"post_architect_synthesis","final":false}"#;
    let orch_r1_final = r#"{"outcome":"plan","schema_version":1,"summary":"Compile parser and exporter DAG from designated Codex final response.","tasks":[{"task_id":"task_parse","title":"Implement ParsedFrame header parser","description":"Define ParsedFrame in src/parser.rs.","role":"coder","difficulty":"medium","depends_on":[],"allowed_write_paths":["src/parser.rs"],"owned_interfaces":["iface_parsed_frame"],"consumed_interfaces":[]},{"task_id":"task_export","title":"Consume ParsedFrame in exporter","description":"Wire src/exporter.rs to iface_parsed_frame.","role":"coder","difficulty":"hard","depends_on":["task_parse"],"allowed_write_paths":["src/exporter.rs"],"owned_interfaces":[],"consumed_interfaces":["iface_parsed_frame"]}],"check_suggestions":[{"check_id":"chk_telemetry_unit","command_summary":"cargo test --workspace","required":true}],"unconfirmed_proposal_notes":[],"resolved_open_questions":["Should ParsedFrame expose copyable header fields?"],"deferred_proposals":[]}"#;
    let call_3_stream = format!(
        "{}\n{}\n{}\n{}\n{}\n",
        r#"{"type":"thread.started","thread_id":"01972d12-3456-7000-8000-000000000953"}"#,
        r#"{"type":"turn.started"}"#,
        format_codex_agent_message_line("msg_orch_r1_draft", orch_r1_draft),
        format_codex_agent_message_line("msg_orch_r1_final", orch_r1_final),
        r#"{"type":"turn.completed","usage":{"input_tokens":610,"cached_input_tokens":160,"output_tokens":185}}"#,
    );
    fs::write(codex_prompts_dir.join("raw_stream_3.jsonl"), &call_3_stream).unwrap();
    fs::write(codex_prompts_dir.join("last_message_3.json"), orch_r1_final).unwrap();

    let job_ok = fx.create_initial_job(
        "job_codex_designated_ok",
        vec![
            "Implement binary frame header parser in src/parser.rs".to_string(),
            "Wire batch exporter in src/exporter.rs".to_string(),
        ],
        vec!["Should ParsedFrame expose copyable header fields?".to_string()],
        Vec::new(),
    );
    let cmd_store = receipt_command_store(&fx);
    let driver =
        PlanningCommandDriver::new(cmd_store.clone(), fx.service.clone()).expect("create driver");
    let enqueued_ok = cmd_store
        .enqueue_command(&EnqueuePlanningCommandRequest::from_orchestrator(
            OrchestratorTestFixture::plan_req(
                &recorded_grant.grant_id,
                &job_ok.job_id,
                "cmd_orch_codex_designated_ok",
                1,
            ),
        ))
        .expect("enqueue codex designated command");
    let executed_ok = driver
        .execute_command(BINDING_ID, THREAD_UUID, &enqueued_ok.command_id)
        .await
        .expect("execute codex planning command with intermediate completed agent_messages");
    assert_eq!(executed_ok.command.status, PlanningCommandStatus::Completed);
    let Some(PlanningCommandResultRef::OrchestratorPlan {
        ref planning_result,
        artifact_verified,
        ..
    }) = executed_ok.command.result_ref
    else {
        panic!("expected OrchestratorPlan result_ref");
    };
    assert!(artifact_verified);
    assert_eq!(planning_result.architect_advice_ids.len(), 1);
    let ok_count: u32 = fs::read_to_string(&codex_count_file)
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    assert_eq!(ok_count, 3);

    let ledger_after_ok = fx.store.read_binding_ledger(BINDING_ID).unwrap();
    let grant_after_ok = ledger_after_ok
        .planning_grants
        .iter()
        .find(|g| g.grant_id == recorded_grant.grant_id)
        .unwrap();
    assert_eq!(grant_after_ok.reservations.len(), 3);
    for (idx, expected_role, expected_final_text) in [
        (0usize, WorkflowRole::Orchestrator, orch_r0_final),
        (1usize, WorkflowRole::Architect, arch_r1_final),
        (2usize, WorkflowRole::Orchestrator, orch_r1_final),
    ] {
        assert_eq!(
            grant_after_ok.reservations[idx].status,
            PlanningReservationStatus::Completed
        );
        let art = fx
            .service
            .read_planning_artifact(
                grant_after_ok.reservations[idx]
                    .report_artifact_id
                    .as_deref()
                    .unwrap(),
            )
            .unwrap();
        assert_eq!(art.role, expected_role);
        assert_eq!(art.execution.status, WorkerRunStatus::Succeeded);
        assert_eq!(art.execution.effective_observed_model(), None);
        assert_eq!(art.execution.effective_observed_effort(), None);
        let verified_response = art
            .verified_response
            .as_ref()
            .expect("successful planning artifact must persist verified_response");
        assert_eq!(verified_response.text, expected_final_text);
        assert_eq!(
            verified_response.source,
            AdvisoryResponseSource::CodexAgentMessageTurnCompleted
        );
        match idx {
            0 => assert!(matches!(
                art.outcome,
                PlanningArtifactOutcomeKind::OrchestratorOutput {
                    structured_output: StructuredOrchestratorOutput::RequiresArchitectAdvice { .. },
                    ..
                }
            )),
            1 => assert!(matches!(
                art.outcome,
                PlanningArtifactOutcomeKind::ArchitectAdvice { .. }
            )),
            2 => assert!(matches!(
                art.outcome,
                PlanningArtifactOutcomeKind::OrchestratorOutput {
                    structured_output: StructuredOrchestratorOutput::Plan { .. },
                    ..
                }
            )),
            _ => unreachable!(),
        }
    }

    // 2. Missing `--output-last-message` (call 4) and stale/mismatched `--output-last-message` (call 5)
    //    never grant fallback trust, persist `FailedAfterEffect` without `verified_response`, and never auto-replay.
    let job_fail = fx.create_initial_job(
        "job_codex_designated_fail",
        vec!["Implement binary frame header parser in src/parser.rs".to_string()],
        Vec::new(),
        Vec::new(),
    );
    let call_4_missing_capture_stream = format!(
        "{}\n{}\n{}\n{}\n",
        r#"{"type":"thread.started","thread_id":"01972d12-3456-7000-8000-000000000954"}"#,
        r#"{"type":"turn.started"}"#,
        format_codex_agent_message_line("msg_only_stream", orch_r1_final),
        r#"{"type":"turn.completed","usage":{"input_tokens":100,"output_tokens":40}}"#,
    );
    fs::write(
        codex_prompts_dir.join("raw_stream_4.jsonl"),
        &call_4_missing_capture_stream,
    )
    .unwrap();

    let call_5_mismatch_stream = format!(
        "{}\n{}\n{}\n{}\n{}\n",
        r#"{"type":"thread.started","thread_id":"01972d12-3456-7000-8000-000000000955"}"#,
        r#"{"type":"turn.started"}"#,
        format_codex_agent_message_line("msg_stale_1", orch_r1_draft),
        format_codex_agent_message_line("msg_final_2", orch_r1_final),
        r#"{"type":"turn.completed","usage":{"input_tokens":100,"output_tokens":40}}"#,
    );
    fs::write(
        codex_prompts_dir.join("raw_stream_5.jsonl"),
        &call_5_mismatch_stream,
    )
    .unwrap();
    fs::write(codex_prompts_dir.join("last_message_5.json"), orch_r1_draft).unwrap();

    for (req_key, expected_code, expected_count, res_idx) in [
        (
            "orch_codex_missing_cap_4",
            AdvisoryResponseErrorCode::MissingResponseOutput,
            4u32,
            3usize,
        ),
        (
            "orch_codex_mismatch_cap_5",
            AdvisoryResponseErrorCode::ConflictingTerminalOutput,
            5u32,
            4usize,
        ),
    ] {
        let err = fx
            .service
            .orchestrate_brief_to_plan(
                &fx.workflow,
                &OrchestratorTestFixture::plan_req(
                    &recorded_grant.grant_id,
                    &job_fail.job_id,
                    req_key,
                    0,
                ),
            )
            .await
            .expect_err("missing or mismatched --output-last-message must fail closed");
        assert_eq!(err.code, NativeErrorCode::ValidationFailed);
        let count_now: u32 = fs::read_to_string(&codex_count_file)
            .unwrap()
            .trim()
            .parse()
            .unwrap();
        assert_eq!(count_now, expected_count);

        let ledger_now = fx.store.read_binding_ledger(BINDING_ID).unwrap();
        let res = &ledger_now
            .planning_grants
            .iter()
            .find(|g| g.grant_id == recorded_grant.grant_id)
            .unwrap()
            .reservations[res_idx];
        assert_eq!(res.status, PlanningReservationStatus::FailedAfterEffect);
        let art = fx
            .service
            .read_planning_artifact(res.report_artifact_id.as_deref().unwrap())
            .unwrap();
        assert!(art.verified_response.is_none());
        let PlanningArtifactOutcomeKind::FailedAfterEffect {
            response_error: Some(ref resp_err),
            ..
        } = art.outcome
        else {
            panic!("expected FailedAfterEffect with response_error");
        };
        assert_eq!(resp_err.code, expected_code);
    }
}
