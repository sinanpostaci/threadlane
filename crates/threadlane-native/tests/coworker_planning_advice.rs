//! Focused behavioral tests for [`CoworkerPlanningService`] Architect advice semantic guards,
//! verified `prior_advice_context` resolution and cache identity, cross-instance reservation
//! acquisition (`FreshlyReserved` vs `ExistingTerminal`), artifact/budget tamper rejection, and
//! `FailedAfterEffect` terminal accounting.
//!
//! Note: These tests use a disposable local Git repository and a local fake Antigravity script
//! supervised by `threadlane_workers::run_advisory_worker_with_structured_result` (never a paid
//! provider). They verify middleware reservation, artifact, and replay invariants and do not prove
//! live external provider write denial.

use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use tempfile::TempDir;
use threadlane_native::coworker_workflow::{
    sha256_hex, ArchitectContextDependencies, ArchitectDecisionStatus, CanonicalProjectBaseRef,
    CoworkerLedgerMutationOutcome, CoworkerPlanningService, CoworkerWorkflowStore,
    CreatePlanningGrantRequest, PlanningAdviceGrant, PlanningCallBudget,
    PlanningReadOnlyCapabilities, PlanningReservationStatus, PriorArchitectAdviceContext,
    ReservePlanningCallRequest, RunArchitectAdviceRequest,
};
use threadlane_native::profile_flow::EXECUTION_POLICY_SCHEMA_VERSION;
use threadlane_native::{
    ExecutionPolicy, NativeErrorCode, PolicyAllowRule, RegistryStore, RoleExecutionRoute,
    SelectedRoleChoice, ToolAdapter, WorkflowProfile, WorkflowRole,
};
use threadlane_workers::{
    resolve_exact_commit_sha, resolve_exact_tree_sha, run_sanitized_git, WorkerRunStatus,
    DEFAULT_GIT_COMMAND_TIMEOUT_MS,
};
use threadlane_workflow::{
    ModelChoice, ModelDefinition, PlanStore, RegistryChange, RoleAssignment, ToolDefinition,
};

const THREAD_UUID: &str = "018f5a30-1111-7000-8000-000000000001";
const BINDING_ID: &str = "bnd_coworker_planning_advice";

struct AdviceTestFixture {
    _tmp: TempDir,
    project_base: CanonicalProjectBaseRef,
    registry_dir: PathBuf,
    plans_dir: PathBuf,
    artifacts_dir: PathBuf,
    agy_count_file: PathBuf,
    agy_mode_file: PathBuf,
    registry: Arc<RegistryStore>,
    _plans: Arc<PlanStore>,
    store: CoworkerWorkflowStore,
    service: CoworkerPlanningService,
}

fn write_fake_agy_advisory_script(script_path: &Path, count_file: &Path, mode_file: &Path) {
    if let Some(parent) = script_path.parent() {
        fs::create_dir_all(parent).expect("create script parent dir");
    }
    let script = format!(
        r#"#!/bin/sh
set -eu
COUNT_FILE="{count_file}"
MODE_FILE="{mode_file}"
c=0
if [ -f "$COUNT_FILE" ]; then
    c=$(cat "$COUNT_FILE")
fi
c=$((c + 1))
printf "%s" "$c" > "$COUNT_FILE"

MODE="valid"
if [ -f "$MODE_FILE" ]; then
    MODE=$(cat "$MODE_FILE")
fi

MODEL="gemini-4-argon-high"
while [ "$#" -gt 0 ]; do
    case "$1" in
        --model)
            MODEL="$2"
            shift 2
            ;;
        *)
            shift 1
            ;;
    esac
done

printf '{{"event":"init","init":{{"session_id":"agy-adv-sess-%s","model":"%s"}}}}\n' "$c" "$MODEL"

if [ "$MODE" = "malformed_json" ]; then
    printf '{{"event":"result","result":{{"status":"SUCCESS","response":"{{\\"schema_version\\":1,\\"unknown_field\\":\\"bad\\"}}"}}}}\n'
    exit 0
fi

RATIONALE="Architectural rationale for invocation #$c: enforce strict module boundaries across crates/threadlane-native/src/coworker_workflow/planning.rs, verify dependency-bound cache identity including verified prior_advice_context digests, and keep read-only snapshot inspection isolated from live repository mutations so that payloads well above 256 bytes are preserved verbatim without truncation."
RESPONSE_JSON=$(printf '{{\\"schema_version\\":1,\\"decision_summary\\":\\"Keep strict module boundary (call #%s)\\",\\"rationale\\":\\"%s\\",\\"recommended_write_paths\\":[\\"src/lib.rs\\"],\\"interface_contracts\\":{{\\"iface_core\\":\\"Stable read-only contract v%s\\"}},\\"open_questions\\":[]}}' "$c" "$RATIONALE" "$c")

printf '{{"event":"result","result":{{"status":"SUCCESS","response":"%s"}}}}\n' "$RESPONSE_JSON"
exit 0
"#,
        count_file = count_file.display(),
        mode_file = mode_file.display(),
    );
    let mut f = File::create(script_path).expect("create fake-agy");
    f.write_all(script.as_bytes()).expect("write fake-agy");
    f.sync_all().expect("sync fake-agy");
    fs::set_permissions(script_path, fs::Permissions::from_mode(0o755))
        .expect("chmod 0755 fake-agy");
}

fn init_git_repo(repo_dir: &Path) -> CanonicalProjectBaseRef {
    fs::create_dir_all(repo_dir.join("src")).expect("create repo src");
    fs::write(
        repo_dir.join("src/lib.rs"),
        "pub fn core_entry() -> u32 {\n    7\n}\n",
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
    let _tree = resolve_exact_tree_sha(repo_dir, &commit, DEFAULT_GIT_COMMAND_TIMEOUT_MS)
        .expect("resolve HEAD tree");
    CanonicalProjectBaseRef::resolve_and_validate(repo_dir, &commit)
        .expect("resolve canonical project base")
}

impl AdviceTestFixture {
    fn new() -> Self {
        let tmp = TempDir::new().expect("tempdir");
        let root = fs::canonicalize(tmp.path()).expect("canonicalize temp root");

        let repo_dir = root.join("repo");
        let project_base = init_git_repo(&repo_dir);

        let bin_dir = root.join("bin");
        let fake_agy_exe = bin_dir.join("fake-agy");
        let agy_count_file = root.join("agy-invocations.txt");
        let agy_mode_file = root.join("agy-mode.txt");
        write_fake_agy_advisory_script(&fake_agy_exe, &agy_count_file, &agy_mode_file);

        let registry_dir = root.join("registry-store");
        let plans_dir = root.join("plans-store");
        let artifacts_dir = root.join("planning-artifacts");

        let registry = Arc::new(RegistryStore::open(&registry_dir).expect("open registry"));
        let plans = Arc::new(PlanStore::open(&plans_dir).expect("open plans"));

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
            project_base,
            registry_dir,
            plans_dir,
            artifacts_dir,
            agy_count_file,
            agy_mode_file,
            registry,
            _plans: plans,
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

    fn reopen_service(&self) -> (CoworkerWorkflowStore, CoworkerPlanningService) {
        let store = CoworkerWorkflowStore::new(&self.plans_dir).expect("reopen store");
        let service =
            CoworkerPlanningService::new(store.clone(), &self.registry_dir, &self.artifacts_dir)
                .expect("reopen service");
        (store, service)
    }

    fn default_policy() -> ExecutionPolicy {
        ExecutionPolicy {
            schema_version: EXECUTION_POLICY_SCHEMA_VERSION,
            policy_id: "pol-planning-advice".to_string(),
            allow_rules: vec![PolicyAllowRule {
                tool_id: "tool-agy".to_string(),
                model_id: "model-argon".to_string(),
                efforts: vec!["high".to_string()],
                allow_no_effort: false,
                roles: vec![WorkflowRole::Architect, WorkflowRole::Orchestrator],
            }],
        }
    }

    fn create_grant_with_output_cap(
        &self,
        request_key: &str,
        max_output_bytes_per_call: usize,
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
                budget: PlanningCallBudget {
                    max_total_calls: 8,
                    max_orchestrator_calls: 4,
                    max_architect_calls: 8,
                    max_timeout_ms_per_call: 30_000,
                    max_output_bytes_per_call,
                },
                created_at_ms: 1_000,
                expires_at_ms: 900_000,
                operator_attested: true,
            })
            .expect("create planning grant")
    }

    fn context_dependencies(&self, grant: &PlanningAdviceGrant) -> ArchitectContextDependencies {
        let blobs = CoworkerPlanningService::compute_dependency_file_blobs(
            &self.project_base,
            &["src/lib.rs".to_string()],
        )
        .expect("compute file blobs");
        ArchitectContextDependencies {
            project_base: self.project_base.clone(),
            work_brief_digest: sha256_hex(b"brief-v1"),
            plan_digest: None,
            policy_digest: grant.policy_digest.clone(),
            dependency_file_blobs: blobs,
            interface_ownership_digests: BTreeMap::new(),
            accepted_predecessor_trees: BTreeMap::new(),
        }
    }
}

#[tokio::test]
async fn architect_before_orchestrator_captures_large_structured_json_and_reuses_exact_advice_without_second_launch(
) {
    let fx = AdviceTestFixture::new();
    let grant = fx.create_grant_with_output_cap("req_grant_1", 32_768);
    let deps = fx.context_dependencies(&grant);

    let req = RunArchitectAdviceRequest {
        binding_id: BINDING_ID.to_string(),
        thread_uuid: THREAD_UUID.to_string(),
        grant_id: grant.grant_id.clone(),
        request_key: "req_arch_initial".to_string(),
        expected_ledger_revision: None,
        difficulty: None,
        context_dependencies: deps.clone(),
        question_summary: "How should we structure the read-only planning boundary?".to_string(),
        prior_advice_context: Vec::new(),
        accept_into_brief: true,
        current_time_ms: 2_000,
    };

    let outcome = fx
        .service
        .obtain_architect_advice(&req)
        .await
        .expect("obtain initial architect advice");

    assert!(!outcome.reused_existing);
    assert_eq!(fx.invocation_count(), 1);
    assert_eq!(
        outcome.record.status,
        ArchitectDecisionStatus::AcceptedIntoBrief
    );
    assert!(
        outcome.structured_advice.rationale.len() > 256,
        "structured Architect rationale must preserve >256-byte response payload without event-summary truncation"
    );

    // Same request_key idempotent replay must not invoke the provider again.
    let idempotent_replay = fx
        .service
        .obtain_architect_advice(&req)
        .await
        .expect("idempotent replay of architect advice");
    assert!(idempotent_replay.reused_existing);
    assert_eq!(idempotent_replay.record.advice_id, outcome.record.advice_id);
    assert_eq!(fx.invocation_count(), 1);

    // New request_key with identical dependencies, question, and prior_advice_context reuses cache.
    let mut reuse_req = req.clone();
    reuse_req.request_key = "req_arch_second_key_same_inputs".to_string();
    reuse_req.current_time_ms = 2_500;
    let cached = fx
        .service
        .obtain_architect_advice(&reuse_req)
        .await
        .expect("dependency-bound cache reuse");
    assert!(cached.reused_existing);
    assert_eq!(cached.record.advice_id, outcome.record.advice_id);
    assert_eq!(fx.invocation_count(), 1);

    let ledger = fx
        .store
        .read_binding_ledger(BINDING_ID)
        .expect("read ledger");
    assert_eq!(ledger.architect_advice.len(), 1);
    assert_eq!(ledger.planning_grants[0].usage.reserved_total_calls, 1);
    assert_eq!(ledger.planning_grants[0].usage.completed_calls, 1);
}

#[tokio::test]
async fn architect_prior_advice_context_rejects_fabricated_refs_and_invalidates_cache_when_valid_prior_changes(
) {
    let fx = AdviceTestFixture::new();
    let grant = fx.create_grant_with_output_cap("req_grant_prior", 32_768);
    let deps = fx.context_dependencies(&grant);

    let adv_1 = fx
        .service
        .obtain_architect_advice(&RunArchitectAdviceRequest {
            binding_id: BINDING_ID.to_string(),
            thread_uuid: THREAD_UUID.to_string(),
            grant_id: grant.grant_id.clone(),
            request_key: "req_adv_base_1".to_string(),
            expected_ledger_revision: None,
            difficulty: None,
            context_dependencies: deps.clone(),
            question_summary: "Base question 1: module ownership?".to_string(),
            prior_advice_context: Vec::new(),
            accept_into_brief: true,
            current_time_ms: 2_000,
        })
        .await
        .expect("obtain adv_1");

    let adv_2 = fx
        .service
        .obtain_architect_advice(&RunArchitectAdviceRequest {
            binding_id: BINDING_ID.to_string(),
            thread_uuid: THREAD_UUID.to_string(),
            grant_id: grant.grant_id.clone(),
            request_key: "req_adv_base_2".to_string(),
            expected_ledger_revision: None,
            difficulty: None,
            context_dependencies: deps.clone(),
            question_summary: "Base question 2: error taxonomy?".to_string(),
            prior_advice_context: Vec::new(),
            accept_into_brief: true,
            current_time_ms: 2_100,
        })
        .await
        .expect("obtain adv_2");

    assert_eq!(fx.invocation_count(), 2);

    let valid_prior_1 = PriorArchitectAdviceContext {
        advice_id: adv_1.record.advice_id.clone(),
        context_digest: adv_1.record.context_digest.clone(),
        decision_digest: adv_1.record.decision_digest.clone(),
        decision_summary: adv_1.record.decision_summary.clone(),
    };
    let valid_prior_2 = PriorArchitectAdviceContext {
        advice_id: adv_2.record.advice_id.clone(),
        context_digest: adv_2.record.context_digest.clone(),
        decision_digest: adv_2.record.decision_digest.clone(),
        decision_summary: adv_2.record.decision_summary.clone(),
    };

    // 1) Fabricated advice_id must fail before any provider launch.
    let mut bad_id_prior = valid_prior_1.clone();
    bad_id_prior.advice_id = "adv_00000000000000000000000000000000".to_string();
    let err = fx
        .service
        .obtain_architect_advice(&RunArchitectAdviceRequest {
            binding_id: BINDING_ID.to_string(),
            thread_uuid: THREAD_UUID.to_string(),
            grant_id: grant.grant_id.clone(),
            request_key: "req_bad_prior_id".to_string(),
            expected_ledger_revision: None,
            difficulty: None,
            context_dependencies: deps.clone(),
            question_summary: "Follow-up question with prior context?".to_string(),
            prior_advice_context: vec![bad_id_prior],
            accept_into_brief: false,
            current_time_ms: 2_200,
        })
        .await
        .expect_err("fabricated advice_id must be rejected");
    assert_eq!(err.code, NativeErrorCode::ValidationFailed);
    assert_eq!(fx.invocation_count(), 2);

    // 2) Mismatched decision_digest or decision_summary must fail before any provider launch.
    let mut bad_digest_prior = valid_prior_1.clone();
    bad_digest_prior.decision_digest = sha256_hex(b"forged-decision-digest");
    let err = fx
        .service
        .obtain_architect_advice(&RunArchitectAdviceRequest {
            binding_id: BINDING_ID.to_string(),
            thread_uuid: THREAD_UUID.to_string(),
            grant_id: grant.grant_id.clone(),
            request_key: "req_bad_prior_digest".to_string(),
            expected_ledger_revision: None,
            difficulty: None,
            context_dependencies: deps.clone(),
            question_summary: "Follow-up question with prior context?".to_string(),
            prior_advice_context: vec![bad_digest_prior],
            accept_into_brief: false,
            current_time_ms: 2_300,
        })
        .await
        .expect_err("mismatched decision_digest must be rejected");
    assert_eq!(err.code, NativeErrorCode::ValidationFailed);

    let mut bad_summary_prior = valid_prior_1.clone();
    bad_summary_prior.decision_summary =
        "Forged summary pretending to be verified advice".to_string();
    let err = fx
        .service
        .obtain_architect_advice(&RunArchitectAdviceRequest {
            binding_id: BINDING_ID.to_string(),
            thread_uuid: THREAD_UUID.to_string(),
            grant_id: grant.grant_id.clone(),
            request_key: "req_bad_prior_summary".to_string(),
            expected_ledger_revision: None,
            difficulty: None,
            context_dependencies: deps.clone(),
            question_summary: "Follow-up question with prior context?".to_string(),
            prior_advice_context: vec![bad_summary_prior],
            accept_into_brief: false,
            current_time_ms: 2_400,
        })
        .await
        .expect_err("mismatched decision_summary must be rejected");
    assert_eq!(err.code, NativeErrorCode::ValidationFailed);
    assert_eq!(fx.invocation_count(), 2);

    // 3) Valid prior_advice_context [valid_prior_1] executes and binds into question/cache identity.
    let with_prior_1 = fx
        .service
        .obtain_architect_advice(&RunArchitectAdviceRequest {
            binding_id: BINDING_ID.to_string(),
            thread_uuid: THREAD_UUID.to_string(),
            grant_id: grant.grant_id.clone(),
            request_key: "req_with_prior_1".to_string(),
            expected_ledger_revision: None,
            difficulty: None,
            context_dependencies: deps.clone(),
            question_summary: "Follow-up question with prior context?".to_string(),
            prior_advice_context: vec![valid_prior_1.clone()],
            accept_into_brief: false,
            current_time_ms: 2_500,
        })
        .await
        .expect("call with valid_prior_1");
    assert!(!with_prior_1.reused_existing);
    assert_eq!(fx.invocation_count(), 3);

    // Exact same question + dependencies + [valid_prior_1] reuses cached outcome without launch.
    let reused_prior_1 = fx
        .service
        .obtain_architect_advice(&RunArchitectAdviceRequest {
            binding_id: BINDING_ID.to_string(),
            thread_uuid: THREAD_UUID.to_string(),
            grant_id: grant.grant_id.clone(),
            request_key: "req_with_prior_1_reuse".to_string(),
            expected_ledger_revision: None,
            difficulty: None,
            context_dependencies: deps.clone(),
            question_summary: "Follow-up question with prior context?".to_string(),
            prior_advice_context: vec![valid_prior_1],
            accept_into_brief: false,
            current_time_ms: 2_600,
        })
        .await
        .expect("reuse with identical valid_prior_1");
    assert!(reused_prior_1.reused_existing);
    assert_eq!(
        reused_prior_1.record.advice_id,
        with_prior_1.record.advice_id
    );
    assert_eq!(fx.invocation_count(), 3);

    // Same question + dependencies, but CHANGED valid prior context [valid_prior_2] must NOT reuse stale cache.
    let with_prior_2 = fx
        .service
        .obtain_architect_advice(&RunArchitectAdviceRequest {
            binding_id: BINDING_ID.to_string(),
            thread_uuid: THREAD_UUID.to_string(),
            grant_id: grant.grant_id.clone(),
            request_key: "req_with_prior_2".to_string(),
            expected_ledger_revision: None,
            difficulty: None,
            context_dependencies: deps,
            question_summary: "Follow-up question with prior context?".to_string(),
            prior_advice_context: vec![valid_prior_2],
            accept_into_brief: false,
            current_time_ms: 2_700,
        })
        .await
        .expect("call with changed valid_prior_2");
    assert!(!with_prior_2.reused_existing);
    assert_ne!(with_prior_2.record.advice_id, with_prior_1.record.advice_id);
    assert_eq!(fx.invocation_count(), 4);
}

#[tokio::test]
async fn independent_services_and_store_reopens_prevent_double_submit_and_recover_existing_terminal_reservations(
) {
    let fx = AdviceTestFixture::new();
    let grant = fx.create_grant_with_output_cap("req_grant_indep", 32_768);
    let deps = fx.context_dependencies(&grant);

    let (_store_a, service_a) = fx.reopen_service();
    let (store_b, service_b) = fx.reopen_service();

    let req_shared = RunArchitectAdviceRequest {
        binding_id: BINDING_ID.to_string(),
        thread_uuid: THREAD_UUID.to_string(),
        grant_id: grant.grant_id.clone(),
        request_key: "req_shared_key_1".to_string(),
        expected_ledger_revision: None,
        difficulty: None,
        context_dependencies: deps.clone(),
        question_summary: "Shared key across independent services?".to_string(),
        prior_advice_context: Vec::new(),
        accept_into_brief: true,
        current_time_ms: 3_000,
    };

    let out_a = service_a
        .obtain_architect_advice(&req_shared)
        .await
        .expect("service_a execution");
    assert!(!out_a.reused_existing);
    assert_eq!(fx.invocation_count(), 1);

    // Reopened independent service_b with same request_key recovers existing record without provider launch.
    let out_b = service_b
        .obtain_architect_advice(&req_shared)
        .await
        .expect("service_b replay");
    assert!(out_b.reused_existing);
    assert_eq!(out_b.record.advice_id, out_a.record.advice_id);
    assert_eq!(fx.invocation_count(), 1);

    // Simulate interruption after reservation Completed + artifact persisted, before ArchitectAdviceRecord
    // is recorded in ledger.architect_advice: acquire_planning_call_reservation returns ExistingTerminal
    // and service_b recovers and records the advice without launching the provider again.
    store_b
        .transact_binding_ledger(BINDING_ID, None, |ledger| {
            ledger.architect_advice.clear();
            Ok(CoworkerLedgerMutationOutcome::Mutated(()))
        })
        .expect("clear architect_advice to exercise ExistingTerminal recovery");

    let recovered = service_b
        .obtain_architect_advice(&req_shared)
        .await
        .expect("recover from ExistingTerminal acquisition");
    assert!(recovered.reused_existing);
    assert_eq!(
        recovered.record.decision_digest,
        out_a.record.decision_digest
    );
    assert_eq!(fx.invocation_count(), 1);

    // Now reserve an unresolved ReservedBeforeEffect reservation without artifact and verify both
    // same-key and different-key calls on a reopened service are blocked with 0 launches.
    let choice = SelectedRoleChoice {
        tool_id: "tool-agy".to_string(),
        adapter: ToolAdapter::Antigravity,
        route: RoleExecutionRoute::HeadlessWorker,
        model_id: "model-argon".to_string(),
        model_arg: "gemini-4-argon-high".to_string(),
        effort: Some("high".to_string()),
        matched_difficulty: None,
    };
    let _ambiguous_res = store_b
        .acquire_planning_call_reservation(&ReservePlanningCallRequest {
            binding_id: BINDING_ID.to_string(),
            thread_uuid: THREAD_UUID.to_string(),
            grant_id: grant.grant_id.clone(),
            request_key: "req_in_flight_unresolved".to_string(),
            expected_ledger_revision: None,
            role: WorkflowRole::Architect,
            difficulty: None,
            selected_choice: choice,
            context_digest: deps.context_digest().expect("context digest"),
            call_project_base: Some(fx.project_base.clone()),
            current_time_ms: 3_400,
        })
        .expect("create unresolved reservation");

    let same_key_err = service_a
        .obtain_architect_advice(&RunArchitectAdviceRequest {
            binding_id: BINDING_ID.to_string(),
            thread_uuid: THREAD_UUID.to_string(),
            grant_id: grant.grant_id.clone(),
            request_key: "req_in_flight_unresolved".to_string(),
            expected_ledger_revision: None,
            difficulty: None,
            context_dependencies: deps.clone(),
            question_summary: "Unresolved same key?".to_string(),
            prior_advice_context: Vec::new(),
            accept_into_brief: false,
            current_time_ms: 3_500,
        })
        .await
        .expect_err("unresolved same-key reservation must block");
    assert_eq!(
        same_key_err.code,
        NativeErrorCode::AmbiguousWakeupPendingReconciliation
    );

    let bypass_err = service_b
        .obtain_architect_advice(&RunArchitectAdviceRequest {
            binding_id: BINDING_ID.to_string(),
            thread_uuid: THREAD_UUID.to_string(),
            grant_id: grant.grant_id.clone(),
            request_key: "req_different_key_bypass_attempt".to_string(),
            expected_ledger_revision: None,
            difficulty: None,
            context_dependencies: deps,
            question_summary: "Different key while another reservation is ambiguous?".to_string(),
            prior_advice_context: Vec::new(),
            accept_into_brief: false,
            current_time_ms: 3_600,
        })
        .await
        .expect_err("different-key bypass while ambiguous reservation exists must block");
    assert_eq!(
        bypass_err.code,
        NativeErrorCode::AmbiguousWakeupPendingReconciliation
    );
    assert_eq!(fx.invocation_count(), 1);
}

#[tokio::test]
async fn persisted_artifact_tampering_or_lower_frozen_output_cap_fails_closed_without_provider_replay(
) {
    let fx = AdviceTestFixture::new();
    let grant_high = fx.create_grant_with_output_cap("req_grant_high_cap", 32_768);
    let deps = fx.context_dependencies(&grant_high);

    let initial = fx
        .service
        .obtain_architect_advice(&RunArchitectAdviceRequest {
            binding_id: BINDING_ID.to_string(),
            thread_uuid: THREAD_UUID.to_string(),
            grant_id: grant_high.grant_id.clone(),
            request_key: "req_cap_and_tamper_1".to_string(),
            expected_ledger_revision: None,
            difficulty: None,
            context_dependencies: deps.clone(),
            question_summary: "Question for output cap and tamper verification?".to_string(),
            prior_advice_context: Vec::new(),
            accept_into_brief: true,
            current_time_ms: 4_000,
        })
        .await
        .expect("obtain initial advice");
    assert_eq!(fx.invocation_count(), 1);

    // Create a second grant with a lower frozen max_output_bytes_per_call (4096 vs 32768).
    // Dependency-bound reuse under the lower output cap must fail closed without calling the provider.
    let grant_low = fx.create_grant_with_output_cap("req_grant_low_cap", 4_096);
    let low_cap_err = fx
        .service
        .obtain_architect_advice(&RunArchitectAdviceRequest {
            binding_id: BINDING_ID.to_string(),
            thread_uuid: THREAD_UUID.to_string(),
            grant_id: grant_low.grant_id.clone(),
            request_key: "req_under_lower_cap".to_string(),
            expected_ledger_revision: None,
            difficulty: None,
            context_dependencies: deps.clone(),
            question_summary: "Question for output cap and tamper verification?".to_string(),
            prior_advice_context: Vec::new(),
            accept_into_brief: true,
            current_time_ms: 4_100,
        })
        .await
        .expect_err("replay under lower frozen output cap must fail closed");
    assert_eq!(low_cap_err.code, NativeErrorCode::ProtocolError);
    assert_eq!(fx.invocation_count(), 1);

    // Tamper 1: lower frozen max_output_bytes_per_call in the persisted artifact below the actual
    // response byte count and verify replay fails closed without provider execution.
    let artifact_id = initial
        .record
        .report_artifact_id
        .as_deref()
        .expect("report_artifact_id");
    let artifact_path = fx.artifacts_dir.join(format!("{artifact_id}.json"));
    let raw_artifact = fs::read_to_string(&artifact_path).expect("read artifact file");
    let lowered_cap_artifact = raw_artifact.replace(
        "\"max_output_bytes_per_call\": 32768",
        "\"max_output_bytes_per_call\": 128",
    );
    assert_ne!(raw_artifact, lowered_cap_artifact);
    fs::write(&artifact_path, lowered_cap_artifact.as_bytes()).expect("write lowered-cap artifact");

    let lowered_art_err = fx
        .service
        .obtain_architect_advice(&RunArchitectAdviceRequest {
            binding_id: BINDING_ID.to_string(),
            thread_uuid: THREAD_UUID.to_string(),
            grant_id: grant_high.grant_id.clone(),
            request_key: "req_cap_and_tamper_1".to_string(),
            expected_ledger_revision: None,
            difficulty: None,
            context_dependencies: deps.clone(),
            question_summary: "Question for output cap and tamper verification?".to_string(),
            prior_advice_context: Vec::new(),
            accept_into_brief: true,
            current_time_ms: 4_150,
        })
        .await
        .expect_err("artifact exceeding frozen output cap must fail closed");
    assert_eq!(lowered_art_err.code, NativeErrorCode::ProtocolError);
    assert_eq!(fx.invocation_count(), 1);

    // Tamper 2: tamper with the persisted artifact's parsed decision_summary on disk and verify both
    // cache reuse and same-key replay fail closed without provider execution.
    let tampered = raw_artifact.replace(
        "Keep strict module boundary (call #1)",
        "Tampered decision summary (call #1)",
    );
    assert_ne!(raw_artifact, tampered);
    fs::write(&artifact_path, tampered.as_bytes()).expect("write tampered artifact");

    let tamper_err = fx
        .service
        .obtain_architect_advice(&RunArchitectAdviceRequest {
            binding_id: BINDING_ID.to_string(),
            thread_uuid: THREAD_UUID.to_string(),
            grant_id: grant_high.grant_id.clone(),
            request_key: "req_cap_and_tamper_1".to_string(),
            expected_ledger_revision: None,
            difficulty: None,
            context_dependencies: deps,
            question_summary: "Question for output cap and tamper verification?".to_string(),
            prior_advice_context: Vec::new(),
            accept_into_brief: true,
            current_time_ms: 4_200,
        })
        .await
        .expect_err("tampered artifact must fail closed");
    assert_eq!(tamper_err.code, NativeErrorCode::ProtocolError);
    assert_eq!(fx.invocation_count(), 1);
}

#[tokio::test]
async fn malformed_terminal_provider_output_records_failed_after_effect_and_blocks_same_key_replay()
{
    let fx = AdviceTestFixture::new();
    fx.set_mode("malformed_json");
    let grant = fx.create_grant_with_output_cap("req_grant_malformed", 32_768);
    let deps = fx.context_dependencies(&grant);

    let req = RunArchitectAdviceRequest {
        binding_id: BINDING_ID.to_string(),
        thread_uuid: THREAD_UUID.to_string(),
        grant_id: grant.grant_id.clone(),
        request_key: "req_malformed_arch_1".to_string(),
        expected_ledger_revision: None,
        difficulty: None,
        context_dependencies: deps,
        question_summary: "What happens when the provider returns malformed JSON?".to_string(),
        prior_advice_context: Vec::new(),
        accept_into_brief: true,
        current_time_ms: 5_000,
    };

    let err = fx
        .service
        .obtain_architect_advice(&req)
        .await
        .expect_err("malformed provider JSON must fail closed");
    assert_eq!(err.code, NativeErrorCode::ValidationFailed);
    assert_eq!(fx.invocation_count(), 1);

    let ledger = fx
        .store
        .read_binding_ledger(BINDING_ID)
        .expect("read ledger");
    assert!(ledger.architect_advice.is_empty());
    let updated_grant = &ledger.planning_grants[0];
    assert_eq!(updated_grant.usage.reserved_total_calls, 1);
    assert_eq!(updated_grant.usage.failed_calls, 1);
    assert_eq!(updated_grant.usage.completed_calls, 0);
    assert_eq!(
        updated_grant.reservations[0].status,
        PlanningReservationStatus::FailedAfterEffect
    );

    // Switch fake executable back to valid mode and replay the same request_key on a reopened service:
    // it must return the known FailedAfterEffect error without launching the provider or fabricating a valid answer.
    fx.set_mode("valid");
    let (_store2, service2) = fx.reopen_service();
    let replay_err = service2.obtain_architect_advice(&req).await.expect_err(
        "same-key replay of FailedAfterEffect reservation must fail without re-running",
    );
    assert_eq!(replay_err.code, NativeErrorCode::ValidationFailed);
    assert_eq!(fx.invocation_count(), 1);
}

#[tokio::test]
async fn frozen_sub_4096_stdout_cap_enforces_actual_capture_limit_and_records_terminal_failure_without_replay(
) {
    let fx = AdviceTestFixture::new();
    let sub_4096_cap = 320usize;
    let grant = fx.create_grant_with_output_cap("req_grant_sub_4096", sub_4096_cap);
    let deps = fx.context_dependencies(&grant);

    let req = RunArchitectAdviceRequest {
        binding_id: BINDING_ID.to_string(),
        thread_uuid: THREAD_UUID.to_string(),
        grant_id: grant.grant_id.clone(),
        request_key: "req_sub_4096_overflow".to_string(),
        expected_ledger_revision: None,
        difficulty: None,
        context_dependencies: deps,
        question_summary: "Does a <4096-byte frozen cap stay enforced without upward clamping?"
            .to_string(),
        prior_advice_context: Vec::new(),
        accept_into_brief: true,
        current_time_ms: 6_000,
    };

    let err = fx
        .service
        .obtain_architect_advice(&req)
        .await
        .expect_err("oversized output under <4096-byte frozen cap must be rejected");
    assert_eq!(err.code, NativeErrorCode::ValidationFailed);
    assert_eq!(fx.invocation_count(), 1);

    let ledger = fx
        .store
        .read_binding_ledger(BINDING_ID)
        .expect("read ledger");
    assert!(ledger.architect_advice.is_empty());
    let updated_grant = &ledger.planning_grants[0];
    assert_eq!(updated_grant.usage.reserved_total_calls, 1);
    assert_eq!(updated_grant.usage.failed_calls, 1);
    assert_eq!(updated_grant.usage.completed_calls, 0);

    let res = &updated_grant.reservations[0];
    assert_eq!(res.status, PlanningReservationStatus::FailedAfterEffect);
    let artifact_id = res
        .report_artifact_id
        .as_deref()
        .expect("FailedAfterEffect report_artifact_id");
    let artifact = fx
        .service
        .read_planning_artifact(artifact_id)
        .expect("read FailedAfterEffect artifact");
    assert_eq!(artifact.max_output_bytes_per_call, sub_4096_cap);
    assert!(
        artifact.execution.stdout_bytes_captured <= sub_4096_cap,
        "captured stdout bytes ({}) must not exceed frozen sub-4096 cap ({sub_4096_cap})",
        artifact.execution.stdout_bytes_captured
    );
    assert_eq!(artifact.execution.status, WorkerRunStatus::OutputOverflow);

    let (_store2, service2) = fx.reopen_service();
    let replay_err = service2
        .obtain_architect_advice(&req)
        .await
        .expect_err("replay of sub-4096 cap failure must not launch another provider");
    assert_eq!(replay_err.code, NativeErrorCode::ValidationFailed);
    assert_eq!(fx.invocation_count(), 1);
}
