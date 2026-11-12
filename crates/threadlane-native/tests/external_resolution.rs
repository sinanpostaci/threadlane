//! Behavioral tests for `external_resolution_receipts_v1` over the existing Coworker workflow
//! ledger, authority, plan, host, ownership, and planning-command stores.

use std::collections::BTreeMap;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::Duration;

use serde::Serialize;
use serde_json::json;
use tempfile::TempDir;
use threadlane_authority::{ApprovalRecord, AuthorityStore, OperatorApprovalRequest};
use threadlane_core::Core;
use threadlane_host::{HostPhase, HostSessionManifest};
use threadlane_native::coworker_workflow::{
    deterministic_prefixed_id, sha256_hex, CanonicalProjectBaseRef, ControllerPolicyStore,
    ControllerRoleExecutionBudget, CoworkerBindingLedger, CoworkerJobOwnershipKey,
    CoworkerJobOwnershipStore, CoworkerJobPhase, CoworkerJobRecord, CoworkerLedgerMutationOutcome,
    CoworkerOwnershipAcquireOutcome, CoworkerTaskRecord, CoworkerTaskStage, CoworkerWorkflowStore,
    CreatePlanningGrantRequest, FinalizePlanningCallRequest,
    FreezeControllerExecutionEnvelopeRequest, FrozenApprovalLinkage, FrozenCheckCommandSpec,
    FrozenCheckKind, FrozenControllerCapabilityEnvelope, OutboxDispatchKind, OutboxDispatchRecord,
    OutboxDispatchStatus, PendingControllerEffect, PendingControllerEffectKind,
    PendingControllerEffectStatus, PlanningCallBudget, PlanningReadOnlyCapabilities,
    PlanningReservationStatus, ReservePlanningCallRequest, WorkBrief, WorkBriefKind,
};
use threadlane_native::error::NativeErrorCode;
use threadlane_native::external_resolution::{
    ExternalResolutionApplicability, ExternalResolutionConfirmation, ExternalResolutionDurability,
    ExternalResolutionEvidenceRef, ExternalResolutionPreview, ExternalResolutionProjectRef,
    ExternalResolutionReceipt, ExternalResolutionScope, ExternalResolutionService,
    ExternalResolutionStaleReason, ExternalResolutionTarget, ReconcileExternalResolutionRequest,
    RecordExternalResolutionRequest, EXTERNAL_RESOLUTION_ID_DOMAIN,
    EXTERNAL_RESOLUTION_INTERFACE_VERSION, EXTERNAL_RESOLUTION_PREVIEW_DOMAIN,
    EXTERNAL_RESOLUTION_RECEIPT_DOMAIN, EXTERNAL_RESOLUTION_SCHEMA_VERSION,
    MAX_EXTERNAL_RESOLUTIONS_PER_BINDING,
};
use threadlane_native::fs_safety::acquire_cross_process_lock;
use threadlane_native::planning_driver::planning_commands_dir;
use threadlane_native::profile_flow::{ExecutionPolicy, PolicyAllowRule};
use threadlane_native::{
    Difficulty, NativeError, RegistryStore, RoleExecutionRoute, SelectedRoleChoice, ToolAdapter,
    WorkflowProfile, WorkflowRole,
};
use threadlane_operations::IntegrationStrategy;
use threadlane_workers::{
    resolve_exact_commit_sha, resolve_exact_tree_sha, run_sanitized_git, ExecutionBudgets,
    DEFAULT_GIT_COMMAND_TIMEOUT_MS,
};
use threadlane_workflow::{
    compile_plan_revision, ModelChoice, ModelDefinition, PlanChange, PlanRevision, PlanStore,
    PlanTask, RegistryChange, RoleAssignment, ToolDefinition,
};

const BINDING: &str = "bnd_external_resolution";
const THREAD: &str = "550e8400-e29b-41d4-a716-446655440000";
const OTHER_THREAD: &str = "6ba7b810-9dad-11d1-80b4-00c04fd430c8";
const HEX64_A: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const HEX64_B: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

struct ExternalFixture {
    _tmp: TempDir,
    root: PathBuf,
    repo: PathBuf,
    plans: PathBuf,
    authority_dir: PathBuf,
    registry_dir: PathBuf,
    sessions: PathBuf,
    registry_revision: u64,
    store: CoworkerWorkflowStore,
    approval: ApprovalRecord,
    plan: PlanRevision,
    project_base: CanonicalProjectBaseRef,
    job: CoworkerJobRecord,
    provider_marker: PathBuf,
}

fn make_private_dir(path: &Path) -> PathBuf {
    fs::create_dir_all(path).expect("create test directory");
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))
        .expect("chmod 0700 test directory");
    fs::canonicalize(path).expect("canonicalize test directory")
}

fn write_private_json<T: Serialize>(path: &Path, value: &T) {
    let bytes = serde_json::to_vec_pretty(value).expect("serialize test JSON");
    fs::write(path, bytes).expect("write private JSON");
    fs::set_permissions(path, fs::Permissions::from_mode(0o600)).expect("chmod 0600 test JSON");
}

fn write_executable_script(path: &Path, body: &str) {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).expect("create executable parent");
    }
    fs::write(path, body).expect("write executable script");
    fs::set_permissions(path, fs::Permissions::from_mode(0o700)).expect("chmod executable script");
}

fn init_repo(repo: &Path) -> (String, String) {
    fs::create_dir_all(repo.join("src")).expect("create repo src");
    fs::write(repo.join("src/lib.rs"), "pub fn alpha() -> u32 { 1 }\n").expect("write src/lib.rs");
    for args in [
        &["init", "--quiet"][..],
        &["config", "user.email", "test@threadlane.local"][..],
        &["config", "user.name", "Threadlane Test"][..],
        &["add", "."][..],
        &["commit", "-m", "initial commit", "--quiet"][..],
    ] {
        run_sanitized_git(repo, args, DEFAULT_GIT_COMMAND_TIMEOUT_MS).expect("git init step");
    }
    let commit = resolve_exact_commit_sha(repo, "HEAD", DEFAULT_GIT_COMMAND_TIMEOUT_MS)
        .expect("resolve HEAD commit");
    let tree = resolve_exact_tree_sha(repo, &commit, DEFAULT_GIT_COMMAND_TIMEOUT_MS)
        .expect("resolve HEAD tree");
    (commit, tree)
}

fn headless_choice(difficulty: Difficulty) -> SelectedRoleChoice {
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

fn planning_policy() -> ExecutionPolicy {
    ExecutionPolicy {
        schema_version: 1,
        policy_id: "policy-external-planning".to_string(),
        allow_rules: vec![PolicyAllowRule {
            tool_id: "tool-agy".to_string(),
            model_id: "model-argon".to_string(),
            efforts: vec!["high".to_string()],
            allow_no_effort: false,
            roles: vec![WorkflowRole::Orchestrator, WorkflowRole::Architect],
        }],
    }
}

fn controller_policy() -> ExecutionPolicy {
    ExecutionPolicy {
        schema_version: 1,
        policy_id: "policy-external-controller".to_string(),
        allow_rules: vec![PolicyAllowRule {
            tool_id: "tool-agy".to_string(),
            model_id: "model-argon".to_string(),
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

fn setup_fixture() -> ExternalFixture {
    let tmp = TempDir::new().expect("tempdir");
    let root = fs::canonicalize(tmp.path()).expect("canonicalize temp root");
    let repo = root.join("repo");
    let (base_commit, base_tree_sha) = init_repo(&repo);

    let provider_marker = root.join("bin/provider-invoked");
    let fake_agy = root.join("bin/fake-agy");
    write_executable_script(
        &fake_agy,
        &format!(
            "#!/bin/sh\nprintf 'invoked\\n' > '{}'\nexit 0\n",
            provider_marker.display()
        ),
    );

    let plans_dir = make_private_dir(&root.join("plans"));
    let authority_dir = make_private_dir(&root.join("authority"));
    let registry_dir = make_private_dir(&root.join("registry"));
    let sessions = make_private_dir(&root.join("sessions"));

    let registry = Arc::new(RegistryStore::open(&registry_dir).expect("open registry"));
    registry
        .apply(
            0,
            RegistryChange::UpsertTool {
                tool: ToolDefinition {
                    id: "tool-agy".to_string(),
                    label: "Antigravity CLI".to_string(),
                    adapter: ToolAdapter::Antigravity,
                    executable_path: Some(fake_agy),
                    enabled: true,
                },
            },
        )
        .expect("upsert tool");
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
        .expect("upsert model");
    registry
        .apply(
            2,
            RegistryChange::UpsertProfile {
                profile: WorkflowProfile {
                    decision_advisor: None,
                    id: "profile-external".to_string(),
                    name: "External Resolution Profile".to_string(),
                    assignments: vec![RoleAssignment {
                        role: WorkflowRole::Coder,
                        difficulty: Some(Difficulty::Hard),
                        choice: ModelChoice {
                            model_id: "model-argon".to_string(),
                            effort: Some("high".to_string()),
                        },
                    }],
                },
            },
        )
        .expect("upsert profile");
    let registry_snapshot = registry.snapshot().expect("registry snapshot");
    let registry_revision = registry_snapshot.revision;

    let plans = Arc::new(PlanStore::open(&plans_dir).expect("open plans"));
    let draft = plans
        .change_at(
            0,
            PlanChange::SaveDraft {
                plan_id: "plan_external_01".to_string(),
                binding_id: BINDING.to_string(),
                profile_id: "profile-external".to_string(),
                profile_revision: registry_revision,
                summary: "External resolution fixture plan".to_string(),
                tasks: vec![
                    PlanTask {
                        id: "task-alpha".to_string(),
                        title: "Alpha task".to_string(),
                        instruction: "Update src/lib.rs".to_string(),
                        role: WorkflowRole::Coder,
                        difficulty: Difficulty::Hard,
                        depends_on: Vec::new(),
                        allowed_write_paths: vec!["src/lib.rs".to_string()],
                    },
                    PlanTask {
                        id: "task-beta".to_string(),
                        title: "Beta task".to_string(),
                        instruction: "Update src/lib.rs too".to_string(),
                        role: WorkflowRole::Coder,
                        difficulty: Difficulty::Hard,
                        depends_on: Vec::new(),
                        allowed_write_paths: vec!["src/lib.rs".to_string()],
                    },
                ],
            },
            1_700_000_000_000,
        )
        .expect("save plan draft");
    let plan = plans
        .change(
            draft.revision,
            PlanChange::Publish {
                plan_id: draft.plan_id.clone(),
                revision: draft.revision,
            },
        )
        .expect("publish plan");
    let compiled = compile_plan_revision(&plan, &registry_snapshot).expect("compile plan");

    let authority = Arc::new(
        AuthorityStore::open(&authority_dir, Arc::clone(&registry), Arc::clone(&plans))
            .expect("open authority"),
    );
    let summary = authority
        .approve_at(
            OperatorApprovalRequest {
                binding_id: BINDING.to_string(),
                thread_uuid: THREAD.to_string(),
                plan_id: plan.plan_id.clone(),
                plan_revision: plan.revision,
                expected_registry_revision: registry_revision,
                expected_plan_digest: compiled.plan_digest.clone(),
                repository_path: repo.clone(),
                base_commit: base_commit.clone(),
                approved_task_ids: vec!["task-alpha".to_string(), "task-beta".to_string()],
                approved_write_paths: vec!["src/lib.rs".to_string()],
                max_parallel: 2,
                budgets: ExecutionBudgets::default(),
            },
            1_700_000_001_000,
        )
        .expect("approve plan");
    let approval = authority
        .get_for_operator(&summary.approval_id)
        .expect("load approval record");

    let project_base = CanonicalProjectBaseRef {
        repository_path: repo.to_string_lossy().into_owned(),
        base_commit: base_commit.clone(),
        base_tree_sha: base_tree_sha.clone(),
    };
    let policy = controller_policy();
    let capability_envelope = FrozenControllerCapabilityEnvelope {
        allowed_reviewer_choices: vec![headless_choice(Difficulty::Hard)],
        allowed_closure_choices: vec![headless_choice(Difficulty::Hard)],
        max_in_scope_correction_attempts: 1,
        allowed_check_ids: vec!["cargo_test".to_string()],
        local_git_integration_allowed: true,
        allow_check_failure_corrections: false,
    };
    let approval_linkage =
        FrozenApprovalLinkage::from_approval_and_policy(&approval, base_tree_sha, &policy)
            .expect("bind approval linkage")
            .with_controller_envelope(capability_envelope, &policy)
            .expect("bind controller capability envelope");
    let tasks: Vec<CoworkerTaskRecord> = plan
        .tasks
        .iter()
        .map(|task| {
            let approved = approval
                .tasks
                .iter()
                .find(|candidate| candidate.task_id == task.id)
                .expect("approved task matching plan task");
            CoworkerTaskRecord {
                task_id: task.id.clone(),
                task_digest: approved.task_digest.clone(),
                role: task.role,
                difficulty: task.difficulty,
                selected_choice: headless_choice(task.difficulty),
                depends_on: approved.depends_on.clone(),
                write_paths: approved.allowed_write_paths.clone(),
                owned_interfaces: Vec::new(),
                consumed_interfaces: Vec::new(),
                stage: CoworkerTaskStage::ReadyToDispatch,
                stage_before_hold: None,
                current_attempt_no: 0,
                active_candidate_id: None,
                accepted_candidate_id: None,
                dependency_input_commit: None,
                dependency_input_tree_sha: None,
                accepted_predecessor_receipts: Vec::new(),
                attempts: Vec::new(),
                candidates: Vec::new(),
                active_hold_ids: Vec::new(),
                unresolved_outbox_id: None,
            }
        })
        .collect();
    let work_brief = WorkBrief {
        brief_id: "brf_external_01".to_string(),
        binding_id: BINDING.to_string(),
        thread_uuid: THREAD.to_string(),
        project_base: project_base.clone(),
        kind: WorkBriefKind::NewJob,
        summary: "External resolution fixture job".to_string(),
        confirmed_requirements: vec!["Resolve the external interruption".to_string()],
        proposed_additions: Vec::new(),
        assumptions: Vec::new(),
        open_questions: Vec::new(),
        accepted_architect_decisions: Vec::new(),
    };
    let work_brief_digest = work_brief.digest().expect("work brief digest");
    let job = CoworkerJobRecord {
        job_id: "job_0123456789abcdef0123456789abcdef".to_string(),
        job_revision: 1,
        create_request_key: "req_create_external_01".to_string(),
        binding_id: BINDING.to_string(),
        thread_uuid: THREAD.to_string(),
        phase: CoworkerJobPhase::Executing,
        work_brief,
        work_brief_digest,
        supersedes_job_id: None,
        planning_result: None,
        approval_linkage: Some(approval_linkage),
        tasks,
        holds: Vec::new(),
        outbox: Vec::new(),
        combined_candidates: Vec::new(),
    };

    let store = CoworkerWorkflowStore::new(&plans_dir).expect("open workflow store");
    let ledger = CoworkerBindingLedger {
        schema_version: 1,
        binding_id: BINDING.to_string(),
        thread_uuid: Some(THREAD.to_string()),
        ledger_revision: 1,
        planning_grants: Vec::new(),
        architect_advice: Vec::new(),
        jobs: vec![job.clone()],
        admission_markers: BTreeMap::new(),
        external_resolutions: Vec::new(),
    };
    ledger.validate(BINDING).expect("valid fixture ledger");
    write_private_json(
        &store
            .ledger_path_for_binding(BINDING)
            .expect("workflow ledger path"),
        &ledger,
    );

    ExternalFixture {
        _tmp: tmp,
        root,
        repo,
        plans: plans_dir,
        authority_dir,
        registry_dir,
        sessions,
        registry_revision,
        store,
        approval,
        plan,
        project_base,
        job,
        provider_marker,
    }
}

fn service(fixture: &ExternalFixture) -> ExternalResolutionService {
    ExternalResolutionService::new(
        &fixture.plans,
        &fixture.authority_dir,
        &fixture.registry_dir,
        Some(fixture.sessions.as_path()),
    )
    .expect("open external resolution service")
}

fn install_stopped_host_session(fixture: &ExternalFixture) -> (PathBuf, String) {
    let session_dir = make_private_dir(&fixture.sessions.join(&fixture.approval.approval_id));
    let runtime_state_dir = make_private_dir(&session_dir.join("runtime-state"));
    let database_path = runtime_state_dir.join("state.db");
    let mut core = Core::open(&database_path).expect("open disposable core runtime");
    let binding = core
        .create_binding(THREAD, "External resolution fixture runtime")
        .expect("create runtime binding");
    drop(core);

    fs::write(session_dir.join("host.lock"), b"").expect("write empty host.lock");
    fs::set_permissions(
        session_dir.join("host.lock"),
        fs::Permissions::from_mode(0o600),
    )
    .expect("chmod host.lock");

    let manifest = HostSessionManifest::from_approval_record(
        &fixture.approval,
        Some(binding.binding_id.clone()),
        HostPhase::Stopped,
        None,
        None,
        1_700_000_050_000,
        1_700_000_060_000,
    );
    write_private_json(&session_dir.join("host-manifest.json"), &manifest);
    (session_dir, binding.binding_id)
}

fn runtime_database_path(session_dir: &Path) -> PathBuf {
    session_dir.join("runtime-state").join("state.db")
}

fn set_runtime_wakeup_status(session_dir: &Path, status: &str) {
    let connection = rusqlite::Connection::open(runtime_database_path(session_dir))
        .expect("open runtime database for fixture mutation");
    connection
        .execute(
            "UPDATE wakeups SET status = ?1 WHERE binding_id IN (SELECT binding_id FROM bindings)",
            rusqlite::params![status],
        )
        .expect("update fixture wakeup status");
}

fn insert_runtime_run_and_worker(
    session_dir: &Path,
    binding_id: &str,
    run_status: &str,
    worker_status: &str,
) {
    let connection = rusqlite::Connection::open(runtime_database_path(session_dir))
        .expect("open runtime database for fixture insertion");
    connection
        .execute(
            "INSERT INTO runs (
                run_id, binding_id, revision, status, instruction, step_seq, step_delay_ms,
                next_step_at_ms, active_question_id, started_at_ms, updated_at_ms,
                completed_at_ms, cancelled_at_ms, cancel_reason
             ) VALUES (
                'run_external_runtime_01', ?1, 1, ?2, 'fixture run', 0, 0,
                NULL, NULL, 1700000000000, 1700000000000, NULL, NULL, NULL
             )",
            rusqlite::params![binding_id, run_status],
        )
        .expect("insert fixture run");
    connection
        .execute(
            "INSERT INTO workers (
                worker_id, run_id, binding_id, revision, status, profile_id, profile_digest,
                provider, requested_model, requested_effort, observed_model, observed_effort,
                instruction, claim_id, attempt_count, execution_binding_json, worktree_json,
                progress_json, outcome_json, created_at_ms, updated_at_ms, started_at_ms,
                completed_at_ms, cancel_requested_at_ms, cancel_reason
             ) VALUES (
                'wrk_external_runtime_01', 'run_external_runtime_01', ?1, 1, ?2,
                'profile-external', ?3, 'antigravity', 'gemini-4-argon-high', 'high',
                NULL, NULL, 'fixture worker', NULL, 1, NULL, '{}', '{}', NULL,
                1700000000000, 1700000000000, 1700000000000, NULL, NULL, NULL
             )",
            rusqlite::params![binding_id, worker_status, HEX64_A],
        )
        .expect("insert fixture worker");
}

fn update_runtime_run_and_worker_status(session_dir: &Path, run_status: &str, worker_status: &str) {
    let connection = rusqlite::Connection::open(runtime_database_path(session_dir))
        .expect("open runtime database for fixture status update");
    connection
        .execute("UPDATE runs SET status = ?1", rusqlite::params![run_status])
        .expect("update fixture run status");
    connection
        .execute(
            "UPDATE workers SET status = ?1",
            rusqlite::params![worker_status],
        )
        .expect("update fixture worker status");
}

fn remove_runtime_run_and_worker(session_dir: &Path) {
    let connection = rusqlite::Connection::open(runtime_database_path(session_dir))
        .expect("open runtime database for fixture cleanup");
    connection
        .execute("DELETE FROM workers", [])
        .expect("remove fixture worker");
    connection
        .execute("DELETE FROM runs", [])
        .expect("remove fixture run");
}

fn controller_envelope_request(
    fixture: &ExternalFixture,
    operator_attested: bool,
    confirm_approval_subject_digest: Option<String>,
    confirm_envelope_digest: Option<String>,
) -> FreezeControllerExecutionEnvelopeRequest {
    let mut role_budgets = BTreeMap::new();
    for role in [
        WorkflowRole::Coder,
        WorkflowRole::Reviewer,
        WorkflowRole::Closure,
    ] {
        role_budgets.insert(
            role,
            ControllerRoleExecutionBudget::default_for_role(role, 1),
        );
    }

    let mut check_commands = BTreeMap::new();
    check_commands.insert(
        "cargo_test".to_string(),
        FrozenCheckCommandSpec {
            check_id: "cargo_test".to_string(),
            kind: FrozenCheckKind::Tests,
            executable: fixture.root.join("bin/fake-agy"),
            argv: vec!["test".to_string()],
            repo_relative_cwd: ".".to_string(),
            timeout_ms: 30_000,
            max_stdout_bytes: 65_536,
            max_stderr_bytes: 65_536,
            required_for_task_review: true,
            required_for_candidate_closure: true,
        },
    );

    FreezeControllerExecutionEnvelopeRequest {
        binding_id: BINDING.to_string(),
        thread_uuid: THREAD.to_string(),
        job_id: fixture.job.job_id.clone(),
        expected_job_revision: fixture.job.job_revision,
        policy: controller_policy(),
        reviewer_choice: headless_choice(Difficulty::Hard),
        closure_choice: headless_choice(Difficulty::Hard),
        role_budgets,
        max_in_scope_corrections_per_task: 0,
        max_total_corrections_per_job: 0,
        max_total_task_attempts_per_job: fixture.job.tasks.len() as u32,
        check_commands,
        allow_task_worktree_commits: true,
        allow_run_integration_execution: false,
        default_integration_strategy: IntegrationStrategy::FastForwardOnly,
        independence_completeness_receipt: None,
        shadow_jev_policy: None,
        created_at_ms: 1_700_000_100_000,
        operator_attested,
        confirm_approval_subject_digest,
        confirm_envelope_digest,
    }
}

fn install_controller_runtime(fixture: &ExternalFixture) -> ControllerPolicyStore {
    let policy_store =
        ControllerPolicyStore::new(&fixture.plans).expect("open controller policy store");
    let registry = Arc::new(RegistryStore::open(&fixture.registry_dir).expect("reopen registry"));
    let plans = Arc::new(PlanStore::open(&fixture.plans).expect("reopen plans"));
    let authority = AuthorityStore::open(
        &fixture.authority_dir,
        Arc::clone(&registry),
        Arc::clone(&plans),
    )
    .expect("reopen authority");

    let preview_request = controller_envelope_request(fixture, false, None, None);
    let preview = policy_store
        .preview_execution_envelope(&preview_request, &fixture.job, &authority)
        .expect("preview controller envelope");
    let recorded_request = controller_envelope_request(
        fixture,
        true,
        Some(preview.approval_subject_digest.clone()),
        Some(preview.envelope_digest.clone()),
    );
    policy_store
        .record_execution_envelope(&recorded_request, &fixture.job, &authority)
        .expect("record controller envelope");
    policy_store
}

fn set_controller_effect(store: &ControllerPolicyStore, status: PendingControllerEffectStatus) {
    store
        .mutate_job_runtime_for_operator(BINDING, "job_0123456789abcdef0123456789abcdef", |state| {
            let job_id = state.job_id.clone();
            state.pending_effects.insert(
                "eff_0123456789abcdef0123456789abcdef".to_string(),
                PendingControllerEffect {
                    effect_id: "eff_0123456789abcdef0123456789abcdef".to_string(),
                    request_key: "req_external_runtime_effect".to_string(),
                    job_id,
                    task_id: Some("task-alpha".to_string()),
                    attempt_no: Some(1),
                    kind: PendingControllerEffectKind::CoderDispatch,
                    status,
                    subject_digest: HEX64_B.to_string(),
                    external_correlation_id: None,
                    created_at_ms: 1_700_000_100_000,
                    updated_at_ms: 1_700_000_100_000,
                    diagnostic: Some("fixture controller effect".to_string()),
                },
            );
            Ok(())
        })
        .expect("insert controller effect");
}

fn clear_controller_effects(store: &ControllerPolicyStore) {
    store
        .mutate_job_runtime_for_operator(BINDING, "job_0123456789abcdef0123456789abcdef", |state| {
            state.pending_effects.clear();
            Ok(())
        })
        .expect("clear controller effects");
}

fn binding_tag(domain: &[u8], binding_id: &str) -> String {
    let mut seed = Vec::new();
    seed.extend_from_slice(domain);
    seed.extend_from_slice(binding_id.as_bytes());
    let digest = sha256_hex(&seed);
    digest[..32].to_string()
}

fn planning_command_lock_name(binding_id: &str) -> String {
    let mut seed = Vec::new();
    seed.extend_from_slice(b"threadlane-native:planning-command-ledger:v1\0");
    seed.extend_from_slice(&(binding_id.len() as u64).to_le_bytes());
    seed.extend_from_slice(binding_id.as_bytes());
    let digest = sha256_hex(&seed);
    format!("planning-commands-{}.lock", &digest[..32])
}

fn conversation_change_lock_name(binding_id: &str) -> String {
    format!(
        "changes-{}.lock",
        binding_tag(b"threadlane-native:conversation-changes:v1\0", binding_id)
    )
}

fn job_request(
    fixture: &ExternalFixture,
    request_key: &str,
    scope: ExternalResolutionScope,
    expected_ledger_revision: Option<u64>,
    expected_job_revision: Option<u64>,
) -> RecordExternalResolutionRequest {
    RecordExternalResolutionRequest {
        binding_id: BINDING.to_string(),
        thread_uuid: THREAD.to_string(),
        canonical_repo_root: fixture.repo.clone(),
        target: ExternalResolutionTarget::Job {
            job_id: fixture.job.job_id.clone(),
        },
        scope,
        expected_ledger_revision,
        expected_job_revision,
        expected_runtime_revision: None,
        request_key: request_key.to_string(),
        created_at_ms: 1_700_000_200_000,
        explanation: "Resolved externally by the trusted operator".to_string(),
        evidence_refs: vec![ExternalResolutionEvidenceRef::ArtifactRef {
            reference: "issue-4242".to_string(),
        }],
    }
}

fn legacy_request(
    fixture: &ExternalFixture,
    plan_id: &str,
    plan_revision: u64,
    request_key: &str,
) -> RecordExternalResolutionRequest {
    RecordExternalResolutionRequest {
        binding_id: BINDING.to_string(),
        thread_uuid: THREAD.to_string(),
        canonical_repo_root: fixture.repo.clone(),
        target: ExternalResolutionTarget::LegacyPlan {
            plan_id: plan_id.to_string(),
            plan_revision,
        },
        scope: ExternalResolutionScope::FullJob,
        expected_ledger_revision: None,
        expected_job_revision: None,
        expected_runtime_revision: None,
        request_key: request_key.to_string(),
        created_at_ms: 1_700_000_200_000,
        explanation: "Legacy plan resolved externally by the trusted operator".to_string(),
        evidence_refs: vec![ExternalResolutionEvidenceRef::ArtifactRef {
            reference: "pr-77".to_string(),
        }],
    }
}

fn confirmation(preview: &ExternalResolutionPreview) -> ExternalResolutionConfirmation {
    ExternalResolutionConfirmation {
        trusted_local_attested: true,
        thread_uuid: preview.thread_uuid.clone(),
        preview_digest: preview.preview_digest.clone(),
    }
}

fn compute_preview_digest(preview: &ExternalResolutionPreview) -> String {
    let mut canonical = preview.clone();
    canonical.preview_digest.clear();
    sha256_hex(
        &serde_json::to_vec(&(EXTERNAL_RESOLUTION_PREVIEW_DOMAIN, &canonical))
            .expect("serialize preview digest seed"),
    )
}

fn compute_receipt_digest(receipt: &ExternalResolutionReceipt) -> String {
    let mut canonical = receipt.clone();
    canonical.receipt_digest.clear();
    sha256_hex(
        &serde_json::to_vec(&(EXTERNAL_RESOLUTION_RECEIPT_DOMAIN, &canonical))
            .expect("serialize receipt digest seed"),
    )
}

fn manual_job_receipt(
    fixture: &ExternalFixture,
    request_key: &str,
    source_ledger_revision: u64,
) -> ExternalResolutionReceipt {
    let mut resolved_task_ids: Vec<String> = fixture
        .job
        .tasks
        .iter()
        .map(|task| task.task_id.clone())
        .collect();
    resolved_task_ids.sort();
    let mut preview = ExternalResolutionPreview {
        interface_version: EXTERNAL_RESOLUTION_INTERFACE_VERSION.to_string(),
        schema_version: EXTERNAL_RESOLUTION_SCHEMA_VERSION,
        binding_id: BINDING.to_string(),
        thread_uuid: THREAD.to_string(),
        project: ExternalResolutionProjectRef {
            canonical_repo_root: fixture.repo.clone(),
            base_commit: fixture.project_base.base_commit.clone(),
            base_tree_sha: Some(fixture.project_base.base_tree_sha.clone()),
        },
        target: ExternalResolutionTarget::Job {
            job_id: fixture.job.job_id.clone(),
        },
        scope: ExternalResolutionScope::FullJob,
        resolved_task_ids: resolved_task_ids.clone(),
        source_ledger_revision,
        resulting_ledger_revision: source_ledger_revision + 1,
        source_job_revision: Some(fixture.job.job_revision),
        source_controller_ledger_revision: 0,
        source_runtime_revision: None,
        work_brief_digest: Some(fixture.job.work_brief_digest.clone()),
        plan_id: None,
        plan_revision: None,
        plan_digest: None,
        approval_id: None,
        approval_subject_digest: None,
        envelope_digest: None,
        request_key: request_key.to_string(),
        created_at_ms: 1_700_000_200_000,
        explanation: "Capacity fixture receipt".to_string(),
        evidence_refs: Vec::new(),
        request_digest: HEX64_A.to_string(),
        preview_digest: String::new(),
    };
    preview.preview_digest = compute_preview_digest(&preview);
    let mut receipt = ExternalResolutionReceipt {
        schema_version: EXTERNAL_RESOLUTION_SCHEMA_VERSION,
        interface_version: EXTERNAL_RESOLUTION_INTERFACE_VERSION.to_string(),
        receipt_id: deterministic_prefixed_id(
            EXTERNAL_RESOLUTION_ID_DOMAIN,
            "exr",
            &[BINDING, THREAD, request_key],
        ),
        binding_id: BINDING.to_string(),
        thread_uuid: THREAD.to_string(),
        request_key: request_key.to_string(),
        created_at_ms: preview.created_at_ms,
        project: preview.project.clone(),
        target: preview.target.clone(),
        scope: preview.scope.clone(),
        resolved_task_ids: preview.resolved_task_ids.clone(),
        source_ledger_revision: preview.source_ledger_revision,
        recorded_ledger_revision: preview.resulting_ledger_revision,
        source_job_revision: preview.source_job_revision,
        source_controller_ledger_revision: preview.source_controller_ledger_revision,
        source_runtime_revision: preview.source_runtime_revision,
        work_brief_digest: preview.work_brief_digest.clone(),
        plan_id: preview.plan_id.clone(),
        plan_revision: preview.plan_revision,
        plan_digest: preview.plan_digest.clone(),
        approval_id: preview.approval_id.clone(),
        approval_subject_digest: preview.approval_subject_digest.clone(),
        envelope_digest: preview.envelope_digest.clone(),
        explanation: preview.explanation.clone(),
        evidence_refs: preview.evidence_refs.clone(),
        request_digest: preview.request_digest.clone(),
        preview_digest: preview.preview_digest.clone(),
        receipt_digest: String::new(),
    };
    receipt.receipt_digest = compute_receipt_digest(&receipt);
    receipt.validate_integrity().expect("valid manual receipt");
    receipt
}

fn dir_entries(path: &Path) -> Vec<String> {
    let mut entries: Vec<String> = fs::read_dir(path)
        .map(|read| {
            read.filter_map(Result::ok)
                .map(|entry| entry.file_name().to_string_lossy().into_owned())
                .collect()
        })
        .unwrap_or_default();
    entries.sort();
    entries
}

#[test]
fn preview_is_read_only_and_proves_job_scope_without_starting_git_provider_or_host() {
    let fixture = setup_fixture();
    let service = service(&fixture);

    // Park the real Git directory and replace `.git` with a plain marker file. Preview and record
    // must rely only on store-proven base commit/tree identity, never on a live Git invocation.
    fs::rename(fixture.repo.join(".git"), fixture.repo.join(".git-parked"))
        .expect("park real .git");
    let fake_git_marker = b"not a git repository; external resolution must not spawn git\n";
    fs::write(fixture.repo.join(".git"), fake_git_marker).expect("write fake .git marker");

    let plans_before = dir_entries(&fixture.plans);
    let sessions_before = dir_entries(&fixture.sessions);
    let repo_before = dir_entries(&fixture.repo);

    let request = job_request(
        &fixture,
        "req_preview_readonly_01",
        ExternalResolutionScope::FullJob,
        Some(1),
        Some(1),
    );
    let preview = service.preview(&request).expect("read-only preview");

    assert_eq!(
        preview.interface_version,
        EXTERNAL_RESOLUTION_INTERFACE_VERSION
    );
    assert_eq!(preview.schema_version, EXTERNAL_RESOLUTION_SCHEMA_VERSION);
    assert_eq!(preview.binding_id, BINDING);
    assert_eq!(preview.thread_uuid, THREAD);
    assert_eq!(preview.project.canonical_repo_root, fixture.repo);
    assert_eq!(preview.target, request.target);
    assert_eq!(
        preview.resolved_task_ids,
        vec!["task-alpha".to_string(), "task-beta".to_string()]
    );
    assert_eq!(preview.source_ledger_revision, 1);
    assert_eq!(preview.resulting_ledger_revision, 2);
    assert_eq!(preview.source_job_revision, Some(1));
    assert_eq!(preview.source_runtime_revision, None);
    assert_eq!(
        preview.work_brief_digest.as_deref(),
        Some(fixture.job.work_brief_digest.as_str())
    );
    assert_eq!(
        preview.plan_id.as_deref(),
        Some(fixture.plan.plan_id.as_str())
    );
    assert_eq!(preview.plan_revision, Some(fixture.plan.revision));
    assert_eq!(
        preview.approval_id.as_deref(),
        Some(fixture.approval.approval_id.as_str())
    );
    assert_eq!(preview.request_key, request.request_key);
    assert_eq!(preview.created_at_ms, request.created_at_ms);
    assert_eq!(preview.preview_digest.len(), 64);
    assert_eq!(preview.request_digest.len(), 64);

    assert_eq!(
        dir_entries(&fixture.plans),
        plans_before,
        "preview must not create locks, journals, directories, or sibling state files"
    );
    assert_eq!(dir_entries(&fixture.sessions), sessions_before);
    assert_eq!(dir_entries(&fixture.repo), repo_before);
    assert!(!fixture.plans.join("coworker-code-driver").exists());
    assert!(!fixture.plans.join("coworker-planning-commands").exists());
    assert!(!fixture.plans.join("conversation-changes").exists());
    assert!(!fixture
        .sessions
        .join(&fixture.approval.approval_id)
        .exists());
    assert!(!fixture.sessions.join(".launch-intents").exists());
    assert!(!fixture.provider_marker.exists());
    assert_eq!(
        fs::read(fixture.repo.join(".git")).expect("read fake .git marker"),
        fake_git_marker
    );

    let second = service.preview(&request).expect("repeated pure preview");
    assert_eq!(
        preview, second,
        "preview must be deterministic and read-only"
    );

    let receipt = service
        .record(&request, &confirmation(&preview))
        .expect("record must not require a live Git repository");
    assert_eq!(receipt.request_key, request.request_key);
    assert!(!fixture.provider_marker.exists());
    assert!(!fixture
        .sessions
        .join(&fixture.approval.approval_id)
        .exists());
    assert_eq!(
        fs::read(fixture.repo.join(".git")).expect("read fake .git marker after record"),
        fake_git_marker
    );
}

#[test]
fn record_requires_separate_confirmation_and_appends_one_immutable_receipt() {
    let fixture = setup_fixture();
    let service = service(&fixture);
    let request = job_request(
        &fixture,
        "req_record_primary_01",
        ExternalResolutionScope::FullJob,
        Some(1),
        Some(1),
    );
    let preview = service.preview(&request).expect("preview");
    let plans_before_failure = dir_entries(&fixture.plans);
    let ledger_before = fixture
        .store
        .read_binding_ledger(BINDING)
        .expect("ledger before");

    let mut untrusted = confirmation(&preview);
    untrusted.trusted_local_attested = false;
    let err = service
        .record(&request, &untrusted)
        .expect_err("untrusted confirmation must be rejected");
    assert_eq!(err.code(), NativeErrorCode::UnauthorizedOperator.as_str());

    let mut wrong_thread = confirmation(&preview);
    wrong_thread.thread_uuid = OTHER_THREAD.to_string();
    let err = service
        .record(&request, &wrong_thread)
        .expect_err("wrong confirmation thread must be rejected");
    assert_eq!(err.code(), NativeErrorCode::UnauthorizedOperator.as_str());

    let mut tampered_preview = preview.clone();
    tampered_preview.explanation = "attacker-controlled explanation".to_string();
    let mut wrong_preview = confirmation(&preview);
    wrong_preview.preview_digest = compute_preview_digest(&tampered_preview);
    let err = service
        .record(&request, &wrong_preview)
        .expect_err("tampered preview digest must be rejected");
    assert_eq!(err.code(), NativeErrorCode::UnauthorizedOperator.as_str());

    assert_eq!(
        dir_entries(&fixture.plans),
        plans_before_failure,
        "rejected confirmation must not acquire locks or create state"
    );
    assert_eq!(
        fixture
            .store
            .read_binding_ledger(BINDING)
            .expect("ledger after rejected confirmations"),
        ledger_before
    );

    let repo_head_before =
        fs::read_to_string(fixture.repo.join(".git/HEAD")).expect("read git HEAD before");
    let sessions_before = dir_entries(&fixture.sessions);
    let receipt = service
        .record(&request, &confirmation(&preview))
        .expect("record receipt");

    assert_eq!(receipt.binding_id, BINDING);
    assert_eq!(receipt.thread_uuid, THREAD);
    assert_eq!(receipt.request_key, request.request_key);
    assert_eq!(receipt.created_at_ms, request.created_at_ms);
    assert_eq!(receipt.preview_digest, preview.preview_digest);
    assert_eq!(receipt.request_digest, preview.request_digest);
    assert_eq!(receipt.source_ledger_revision, 1);
    assert_eq!(receipt.recorded_ledger_revision, 2);
    assert_eq!(receipt.target, request.target);
    assert_eq!(receipt.receipt_digest.len(), 64);

    let after = fixture
        .store
        .read_binding_ledger(BINDING)
        .expect("ledger after record");
    assert_eq!(after.ledger_revision, ledger_before.ledger_revision + 1);
    assert_eq!(after.jobs, ledger_before.jobs);
    assert_eq!(after.planning_grants, ledger_before.planning_grants);
    assert_eq!(after.architect_advice, ledger_before.architect_advice);
    assert_eq!(after.admission_markers, ledger_before.admission_markers);
    assert_eq!(after.external_resolutions.len(), 1);
    assert_eq!(after.external_resolutions[0], receipt);

    assert!(
        !fixture
            .sessions
            .join(&fixture.approval.approval_id)
            .exists(),
        "recording must not create a live host session directory"
    );
    assert!(
        !fixture
            .sessions
            .join(&fixture.approval.approval_id)
            .join("host-manifest.json")
            .exists(),
        "recording must not start a threadlane-host process"
    );
    let controller_ledger_path = ControllerPolicyStore::new(&fixture.plans)
        .expect("controller policy store")
        .ledger_path_for_binding(BINDING)
        .expect("controller ledger path");
    assert!(
        !controller_ledger_path.exists(),
        "recording must not mutate controller runtime/envelope state"
    );
    assert!(!fixture.provider_marker.exists());
    assert_eq!(
        fs::read_to_string(fixture.repo.join(".git/HEAD")).expect("read git HEAD after"),
        repo_head_before
    );
    let sessions_after = dir_entries(&fixture.sessions);
    assert!(
        sessions_after
            .iter()
            .all(|entry| entry.as_str() == ".launch-intents"),
        "only the durable launch-intent guard directory may appear, never host session state"
    );
    assert!(sessions_after.len() <= sessions_before.len() + 1);

    let inspection = service.inspect(BINDING, THREAD).expect("inspect");
    assert_eq!(inspection.receipts.len(), 1);
    assert_eq!(
        inspection.receipts[0].durability,
        ExternalResolutionDurability::VerifiedDurable
    );
    assert_eq!(
        inspection.receipts[0].applicability,
        ExternalResolutionApplicability::Applicable
    );

    let plans_after = dir_entries(&fixture.plans);
    let sessions_after_record = dir_entries(&fixture.sessions);
    let ledger_bytes_after_record = fs::read(
        fixture
            .store
            .ledger_path_for_binding(BINDING)
            .expect("ledger path"),
    )
    .expect("read ledger bytes");
    let duplicate = service
        .record(&request, &confirmation(&preview))
        .expect("exact idempotent replay");
    assert_eq!(duplicate, receipt);
    assert_eq!(
        fs::read(
            fixture
                .store
                .ledger_path_for_binding(BINDING)
                .expect("ledger path")
        )
        .expect("reread ledger bytes"),
        ledger_bytes_after_record,
        "exact duplicate replay must not rewrite the ledger"
    );
    assert_eq!(
        dir_entries(&fixture.plans),
        plans_after,
        "exact duplicate replay must not acquire locks or create journals"
    );
    assert_eq!(dir_entries(&fixture.sessions), sessions_after_record);

    let mut conflicting = request.clone();
    conflicting.explanation = "Different request content under the same key".to_string();
    let err = service
        .record(&conflicting, &confirmation(&preview))
        .expect_err("request_key conflict must be rejected");
    assert_eq!(err.code(), NativeErrorCode::ValidationFailed.as_str());
    assert_eq!(
        fs::read(
            fixture
                .store
                .ledger_path_for_binding(BINDING)
                .expect("ledger path")
        )
        .expect("reread ledger bytes after conflict"),
        ledger_bytes_after_record
    );

    let err = fixture
        .store
        .transact_binding_ledger(BINDING, None, |ledger| {
            ledger.external_resolutions.clear();
            Ok(CoworkerLedgerMutationOutcome::Mutated(()))
        })
        .expect_err("removing an existing receipt must fail");
    assert_eq!(err.code(), NativeErrorCode::ValidationFailed.as_str());
    assert!(err.message().contains("append-only"));

    let err = fixture
        .store
        .transact_binding_ledger(BINDING, None, |ledger| {
            ledger.external_resolutions[0]
                .explanation
                .push_str(" tampered");
            Ok(CoworkerLedgerMutationOutcome::Mutated(()))
        })
        .expect_err("mutating an existing receipt must fail");
    assert_eq!(err.code(), NativeErrorCode::ValidationFailed.as_str());
    assert!(err.message().contains("append-only"));
    assert_eq!(
        fs::read(
            fixture
                .store
                .ledger_path_for_binding(BINDING)
                .expect("ledger path")
        )
        .expect("reread ledger bytes after append-only failures"),
        ledger_bytes_after_record
    );
}

#[test]
fn legacy_ledger_without_external_resolutions_field_round_trips_and_records_first_receipt() {
    let fixture = setup_fixture();
    let ledger_path = fixture
        .store
        .ledger_path_for_binding(BINDING)
        .expect("ledger path");
    let ledger = fixture
        .store
        .read_binding_ledger(BINDING)
        .expect("read fixture ledger");
    let mut legacy = serde_json::to_value(&ledger).expect("serialize fixture ledger");
    legacy
        .as_object_mut()
        .expect("ledger object")
        .remove("external_resolutions");
    write_private_json(&ledger_path, &legacy);

    let reopened = fixture
        .store
        .read_binding_ledger(BINDING)
        .expect("legacy ledger remains readable");
    assert!(reopened.external_resolutions.is_empty());

    let service = service(&fixture);
    let request = job_request(
        &fixture,
        "req_legacy_ledger_01",
        ExternalResolutionScope::FullJob,
        Some(1),
        Some(1),
    );
    let preview = service.preview(&request).expect("preview legacy ledger");
    service
        .record(&request, &confirmation(&preview))
        .expect("record on legacy ledger");

    let raw = fs::read(&ledger_path).expect("read updated ledger");
    let parsed: serde_json::Value = serde_json::from_slice(&raw).expect("parse updated ledger");
    assert_eq!(
        parsed["external_resolutions"]
            .as_array()
            .expect("external resolutions array")
            .len(),
        1
    );
    let inspection = service.inspect(BINDING, THREAD).expect("inspect");
    assert_eq!(inspection.receipts.len(), 1);
    assert_eq!(
        inspection.receipts[0].applicability,
        ExternalResolutionApplicability::Applicable
    );
}

#[test]
fn legacy_plan_requires_unique_verified_approval_and_supports_full_task_scope() {
    let fixture = setup_fixture();
    let ledger = CoworkerBindingLedger {
        schema_version: 1,
        binding_id: BINDING.to_string(),
        thread_uuid: Some(THREAD.to_string()),
        ledger_revision: 1,
        planning_grants: Vec::new(),
        architect_advice: Vec::new(),
        jobs: Vec::new(),
        admission_markers: BTreeMap::new(),
        external_resolutions: Vec::new(),
    };
    ledger.validate(BINDING).expect("valid legacy ledger");
    write_private_json(
        &fixture
            .store
            .ledger_path_for_binding(BINDING)
            .expect("ledger path"),
        &ledger,
    );

    let service = service(&fixture);
    let request = legacy_request(
        &fixture,
        &fixture.plan.plan_id,
        fixture.plan.revision,
        "req_legacy_plan_01",
    );
    let preview = service.preview(&request).expect("legacy preview");
    let mut wrong_legacy_scope = request.clone();
    wrong_legacy_scope.scope = ExternalResolutionScope::Tasks {
        task_ids: vec!["task-not-approved".to_string()],
    };
    let err = service
        .preview(&wrong_legacy_scope)
        .expect_err("legacy scope outside approved tasks must fail");
    assert_eq!(err.code(), NativeErrorCode::ValidationFailed.as_str());
    assert_eq!(
        preview.project.canonical_repo_root,
        fixture.approval.repository_path
    );
    assert_eq!(preview.project.base_commit, fixture.approval.base_commit);
    assert_eq!(preview.project.base_tree_sha, None);
    assert_eq!(
        preview.resolved_task_ids,
        vec!["task-alpha".to_string(), "task-beta".to_string()]
    );
    assert_eq!(
        preview.approval_id.as_deref(),
        Some(fixture.approval.approval_id.as_str())
    );
    assert_eq!(
        preview.approval_subject_digest.as_deref(),
        Some(fixture.approval.subject_digest.as_str())
    );
    assert_eq!(preview.source_job_revision, None);
    assert_eq!(preview.source_runtime_revision, None);

    let receipt = service
        .record(&request, &confirmation(&preview))
        .expect("record legacy receipt");
    assert_eq!(receipt.target, request.target);
    let inspection = service
        .inspect(BINDING, THREAD)
        .expect("inspect legacy receipt");
    assert_eq!(inspection.receipts.len(), 1);
    assert_eq!(
        inspection.receipts[0].applicability,
        ExternalResolutionApplicability::Applicable
    );

    // A published legacy plan without a uniquely matching ApprovalRecord is explicitly unsupported.
    let plans = PlanStore::open(&fixture.plans).expect("reopen plan store");
    let draft = plans
        .change_at(
            0,
            PlanChange::SaveDraft {
                plan_id: "plan_external_unapproved".to_string(),
                binding_id: BINDING.to_string(),
                profile_id: "profile-external".to_string(),
                profile_revision: fixture.registry_revision,
                summary: "Unapproved legacy plan".to_string(),
                tasks: vec![PlanTask {
                    id: "task-unapproved".to_string(),
                    title: "Unapproved task".to_string(),
                    instruction: "No approval exists".to_string(),
                    role: WorkflowRole::Coder,
                    difficulty: Difficulty::Hard,
                    depends_on: Vec::new(),
                    allowed_write_paths: vec!["src/lib.rs".to_string()],
                }],
            },
            1_700_000_400_000,
        )
        .expect("save unapproved draft");
    let published = plans
        .change(
            draft.revision,
            PlanChange::Publish {
                plan_id: draft.plan_id.clone(),
                revision: draft.revision,
            },
        )
        .expect("publish unapproved plan");
    let unapproved = legacy_request(
        &fixture,
        &published.plan_id,
        published.revision,
        "req_legacy_unapproved_01",
    );
    let err = service
        .preview(&unapproved)
        .expect_err("legacy plan without unique approval must be refused");
    assert_eq!(err.code(), NativeErrorCode::UnsupportedCapability.as_str());
    assert_eq!(
        service
            .inspect(BINDING, THREAD)
            .expect("inspect after unapproved preview")
            .receipts
            .len(),
        1
    );
}

#[test]
fn receipt_capacity_is_enforced_without_creating_lock_or_journal_state() {
    let fixture = setup_fixture();
    let mut ledger = fixture
        .store
        .read_binding_ledger(BINDING)
        .expect("read fixture ledger");
    ledger.external_resolutions = (0..MAX_EXTERNAL_RESOLUTIONS_PER_BINDING)
        .map(|index| {
            manual_job_receipt(
                &fixture,
                &format!("req_capacity_{index:02}"),
                index as u64 + 1,
            )
        })
        .collect();
    ledger.ledger_revision = MAX_EXTERNAL_RESOLUTIONS_PER_BINDING as u64 + 1;
    ledger.validate(BINDING).expect("capacity fixture ledger");
    write_private_json(
        &fixture
            .store
            .ledger_path_for_binding(BINDING)
            .expect("ledger path"),
        &ledger,
    );

    let service = service(&fixture);
    let plans_before = dir_entries(&fixture.plans);
    let request = job_request(
        &fixture,
        "req_capacity_overflow_01",
        ExternalResolutionScope::FullJob,
        None,
        None,
    );
    let err = service
        .preview(&request)
        .expect_err("capacity must reject preview");
    assert_eq!(err.code(), NativeErrorCode::ValidationFailed.as_str());
    assert!(err
        .message()
        .contains("MAX_EXTERNAL_RESOLUTIONS_PER_BINDING"));
    assert_eq!(dir_entries(&fixture.plans), plans_before);
    assert_eq!(
        fixture
            .store
            .read_binding_ledger(BINDING)
            .expect("capacity ledger remains readable")
            .external_resolutions
            .len(),
        MAX_EXTERNAL_RESOLUTIONS_PER_BINDING
    );
}

#[test]
fn wrong_scope_thread_repository_and_non_proof_inputs_are_refused() {
    let fixture = setup_fixture();
    let service = service(&fixture);
    let ledger_before = fixture
        .store
        .read_binding_ledger(BINDING)
        .expect("ledger before");

    let wrong_scope = job_request(
        &fixture,
        "req_wrong_scope_01",
        ExternalResolutionScope::Tasks {
            task_ids: vec!["task-not-in-job".to_string()],
        },
        None,
        None,
    );
    let err = service
        .preview(&wrong_scope)
        .expect_err("scope referencing an absent task must fail");
    assert_eq!(err.code(), NativeErrorCode::ValidationFailed.as_str());

    let mut wrong_thread = job_request(
        &fixture,
        "req_wrong_thread_01",
        ExternalResolutionScope::FullJob,
        None,
        None,
    );
    wrong_thread.thread_uuid = OTHER_THREAD.to_string();
    let err = service
        .preview(&wrong_thread)
        .expect_err("thread mismatch must fail closed");
    assert_eq!(err.code(), NativeErrorCode::SecurityViolation.as_str());

    let other_repo = make_private_dir(&fixture.root.join("other-repo"));
    fs::write(other_repo.join(".git"), b"not a git repository\n").expect("fake other .git");
    let mut wrong_repo = job_request(
        &fixture,
        "req_wrong_repo_01",
        ExternalResolutionScope::FullJob,
        None,
        None,
    );
    wrong_repo.canonical_repo_root = other_repo;
    let err = service
        .preview(&wrong_repo)
        .expect_err("repository mismatch must fail");
    assert_eq!(err.code(), NativeErrorCode::ValidationFailed.as_str());

    let mut commit_only = job_request(
        &fixture,
        "req_commit_only_01",
        ExternalResolutionScope::FullJob,
        None,
        None,
    );
    commit_only.target = ExternalResolutionTarget::CommitOnly {
        commit_sha: "1234567890abcdef1234567890abcdef12345678".to_string(),
    };
    let err = service
        .preview(&commit_only)
        .expect_err("raw commit-only target must be refused");
    assert_eq!(err.code(), NativeErrorCode::UnsupportedCapability.as_str());

    let mut legacy_with_job_revision = legacy_request(
        &fixture,
        &fixture.plan.plan_id,
        fixture.plan.revision,
        "req_legacy_job_revision_01",
    );
    legacy_with_job_revision.expected_job_revision = Some(1);
    let err = service
        .preview(&legacy_with_job_revision)
        .expect_err("legacy target with job revision guard must fail");
    assert_eq!(err.code(), NativeErrorCode::ValidationFailed.as_str());

    let mut provider_text = job_request(
        &fixture,
        "req_provider_text_01",
        ExternalResolutionScope::FullJob,
        None,
        None,
    );
    provider_text.evidence_refs = vec![ExternalResolutionEvidenceRef::ProviderText {
        text: "The model says the task is done".to_string(),
    }];
    let err = service
        .preview(&provider_text)
        .expect_err("provider prose is not valid evidence");
    assert_eq!(err.code(), NativeErrorCode::UnsupportedCapability.as_str());

    let mut json_attestation = job_request(
        &fixture,
        "req_json_attestation_01",
        ExternalResolutionScope::FullJob,
        None,
        None,
    );
    json_attestation.evidence_refs = vec![ExternalResolutionEvidenceRef::JsonSelfAttestation {
        claim: "{\"resolved\":true}".to_string(),
    }];
    let err = service
        .preview(&json_attestation)
        .expect_err("caller JSON self-attestation is not valid evidence");
    assert_eq!(err.code(), NativeErrorCode::UnsupportedCapability.as_str());

    assert_eq!(
        fixture
            .store
            .read_binding_ledger(BINDING)
            .expect("ledger after refusals"),
        ledger_before
    );
}

#[test]
fn live_owner_and_ambiguous_planning_reservation_block_record_until_reconciled() {
    let fixture = setup_fixture();
    let service = service(&fixture);

    let ownership_dir = fixture.plans.join("coworker-code-driver");
    make_private_dir(&ownership_dir);
    let ownership = CoworkerJobOwnershipStore::new(&ownership_dir).expect("ownership store");
    let key = CoworkerJobOwnershipKey::new(BINDING, THREAD, fixture.job.job_id.clone())
        .expect("ownership key");
    let guard = match ownership
        .try_acquire_job(&key, 1_700_000_500_000)
        .expect("acquire owner guard")
    {
        CoworkerOwnershipAcquireOutcome::Acquired(guard) => guard,
        CoworkerOwnershipAcquireOutcome::Busy { .. } => panic!("fresh owner guard must acquire"),
    };

    let request = job_request(
        &fixture,
        "req_owner_blocked_01",
        ExternalResolutionScope::FullJob,
        None,
        None,
    );
    let err = service
        .preview(&request)
        .expect_err("live owner must block preview");
    assert_eq!(err.code(), NativeErrorCode::ValidationFailed.as_str());
    assert!(err.message().contains("owned by a live"));
    let err = service
        .record(
            &request,
            &ExternalResolutionConfirmation {
                trusted_local_attested: true,
                thread_uuid: THREAD.to_string(),
                preview_digest: HEX64_A.to_string(),
            },
        )
        .expect_err("live owner must block record before any write");
    assert_eq!(err.code(), NativeErrorCode::ValidationFailed.as_str());
    assert_eq!(
        fixture
            .store
            .read_binding_ledger(BINDING)
            .expect("ledger during owner guard")
            .external_resolutions
            .len(),
        0
    );
    drop(guard);

    let preview = service
        .preview(&request)
        .expect("preview recovers after owner release");
    assert_eq!(preview.resulting_ledger_revision, 2);

    let grant = fixture
        .store
        .create_planning_grant(&CreatePlanningGrantRequest {
            binding_id: BINDING.to_string(),
            thread_uuid: THREAD.to_string(),
            request_key: "req_external_grant_01".to_string(),
            expected_ledger_revision: Some(1),
            project_base: fixture.project_base.clone(),
            profile_id: "profile-external".to_string(),
            registry_revision: fixture.registry_revision,
            policy: planning_policy(),
            capabilities: PlanningReadOnlyCapabilities::strict_read_only(),
            budget: PlanningCallBudget {
                max_total_calls: 4,
                max_orchestrator_calls: 2,
                max_architect_calls: 2,
                max_timeout_ms_per_call: 30_000,
                max_output_bytes_per_call: 65_536,
            },
            created_at_ms: 1_700_000_300_000,
            expires_at_ms: 1_700_003_600_000,
            operator_attested: true,
        })
        .expect("create planning grant");
    let reservation = fixture
        .store
        .reserve_planning_call(&ReservePlanningCallRequest {
            binding_id: BINDING.to_string(),
            thread_uuid: THREAD.to_string(),
            grant_id: grant.grant_id.clone(),
            request_key: "req_external_planning_01".to_string(),
            expected_ledger_revision: Some(2),
            role: WorkflowRole::Orchestrator,
            difficulty: Some(Difficulty::Hard),
            selected_choice: headless_choice(Difficulty::Hard),
            context_digest: HEX64_A.to_string(),
            call_project_base: Some(fixture.project_base.clone()),
            current_time_ms: 1_700_000_300_100,
        })
        .expect("reserve planning call");
    assert_eq!(
        reservation.status,
        PlanningReservationStatus::ReservedBeforeEffect
    );

    let err = service
        .preview(&request)
        .expect_err("ambiguous planning reservation must block preview");
    assert_eq!(
        err.code(),
        NativeErrorCode::AmbiguousWakeupPendingReconciliation.as_str()
    );

    fixture
        .store
        .finalize_planning_call(&FinalizePlanningCallRequest {
            binding_id: BINDING.to_string(),
            thread_uuid: THREAD.to_string(),
            grant_id: grant.grant_id,
            reservation_id: reservation.reservation_id,
            expected_ledger_revision: Some(3),
            status: PlanningReservationStatus::RefusedBeforeEffect,
            updated_at_ms: 1_700_000_300_200,
            run_id: None,
            report_artifact_id: None,
            output_digest: None,
            diagnostic: Some("refused before effect during test".to_string()),
        })
        .expect("finalize refused planning call");
    service
        .preview(&request)
        .expect("preview recovers after planning reconciliation");
}

#[test]
fn unknown_outbox_effect_and_in_flight_task_stage_block_record_until_reconciled() {
    let fixture = setup_fixture();
    let service = service(&fixture);
    let outbox_id = "obx_0123456789abcdef0123456789abcdef";
    fixture
        .store
        .transact_binding_ledger(BINDING, None, |ledger| {
            let job = ledger
                .jobs
                .iter_mut()
                .find(|job| job.job_id == fixture.job.job_id)
                .expect("fixture job");
            job.job_revision = 2;
            job.outbox.push(OutboxDispatchRecord {
                outbox_id: outbox_id.to_string(),
                request_key: "req_unknown_outbox_01".to_string(),
                kind: OutboxDispatchKind::TaskAttemptDispatch,
                task_id: "task-alpha".to_string(),
                attempt_no: 1,
                role: WorkflowRole::Coder,
                subject_digest: HEX64_B.to_string(),
                status: OutboxDispatchStatus::EffectUnknown,
                reserved_at_job_revision: 2,
                updated_at_job_revision: 2,
                run_id: None,
                worker_id: None,
                receipt_id: None,
                diagnostic: Some("external outcome unknown".to_string()),
            });
            job.tasks[0].unresolved_outbox_id = Some(outbox_id.to_string());
            Ok(CoworkerLedgerMutationOutcome::Mutated(()))
        })
        .expect("write unknown outbox fixture");

    let request = job_request(
        &fixture,
        "req_unknown_outbox_01",
        ExternalResolutionScope::FullJob,
        None,
        Some(2),
    );
    let err = service
        .preview(&request)
        .expect_err("EffectUnknown outbox must block preview");
    assert_eq!(
        err.code(),
        NativeErrorCode::AmbiguousWakeupPendingReconciliation.as_str()
    );
    let err = service
        .record(
            &request,
            &ExternalResolutionConfirmation {
                trusted_local_attested: true,
                thread_uuid: THREAD.to_string(),
                preview_digest: HEX64_A.to_string(),
            },
        )
        .expect_err("EffectUnknown outbox must block record");
    assert_eq!(
        err.code(),
        NativeErrorCode::AmbiguousWakeupPendingReconciliation.as_str()
    );
    assert_eq!(
        service
            .inspect(BINDING, THREAD)
            .expect("inspect after unknown refusal")
            .receipts
            .len(),
        0
    );

    fixture
        .store
        .transact_binding_ledger(BINDING, Some(2), |ledger| {
            let job = ledger
                .jobs
                .iter_mut()
                .find(|job| job.job_id == fixture.job.job_id)
                .expect("fixture job");
            job.job_revision = 3;
            let outbox = job
                .outbox
                .iter_mut()
                .find(|outbox| outbox.outbox_id == outbox_id)
                .expect("unknown outbox");
            outbox.status = OutboxDispatchStatus::ReconciledNotExecuted;
            outbox.updated_at_job_revision = 3;
            job.tasks[0].unresolved_outbox_id = None;
            Ok(CoworkerLedgerMutationOutcome::Mutated(()))
        })
        .expect("reconcile unknown outbox");
    let mut recovered = request.clone();
    recovered.expected_job_revision = Some(3);
    service
        .preview(&recovered)
        .expect("preview recovers after outbox reconciliation");

    fixture
        .store
        .transact_binding_ledger(BINDING, Some(3), |ledger| {
            let job = ledger
                .jobs
                .iter_mut()
                .find(|job| job.job_id == fixture.job.job_id)
                .expect("fixture job");
            job.job_revision = 4;
            let outbox = job
                .outbox
                .iter_mut()
                .find(|outbox| outbox.outbox_id == outbox_id)
                .expect("reconciled outbox");
            outbox.status = OutboxDispatchStatus::DispatchedEffectConfirmed;
            outbox.updated_at_job_revision = 4;
            job.tasks[0].stage = CoworkerTaskStage::DispatchReserved;
            job.tasks[0].unresolved_outbox_id = Some(outbox_id.to_string());
            Ok(CoworkerLedgerMutationOutcome::Mutated(()))
        })
        .expect("write in-flight task fixture");
    let mut in_flight = request.clone();
    in_flight.expected_job_revision = Some(4);
    let err = service
        .preview(&in_flight)
        .expect_err("in-flight task attempt must block preview");
    assert_eq!(err.code(), NativeErrorCode::ValidationFailed.as_str());
    assert_eq!(
        service
            .inspect(BINDING, THREAD)
            .expect("inspect after in-flight refusal")
            .receipts
            .len(),
        0
    );
}

#[test]
fn unrelated_appends_keep_receipts_applicable_while_target_changes_preserve_stale_history() {
    let fixture = setup_fixture();
    let service = service(&fixture);

    let request_one = job_request(
        &fixture,
        "req_stale_history_01",
        ExternalResolutionScope::FullJob,
        None,
        Some(1),
    );
    let preview_one = service.preview(&request_one).expect("preview receipt one");
    let receipt_one = service
        .record(&request_one, &confirmation(&preview_one))
        .expect("record receipt one");

    let request_two = job_request(
        &fixture,
        "req_stale_history_02",
        ExternalResolutionScope::Tasks {
            task_ids: vec!["task-beta".to_string(), "task-alpha".to_string()],
        },
        None,
        Some(1),
    );
    let preview_two = service.preview(&request_two).expect("preview receipt two");
    let receipt_two = service
        .record(&request_two, &confirmation(&preview_two))
        .expect("record receipt two");
    assert_ne!(receipt_one.receipt_id, receipt_two.receipt_id);

    let inspection = service
        .inspect(BINDING, THREAD)
        .expect("inspect two receipts");
    assert_eq!(inspection.receipts.len(), 2);
    assert_eq!(
        inspection.receipts[0].applicability,
        ExternalResolutionApplicability::Applicable,
        "a later receipt or its ledger revision must not stale an earlier target"
    );
    assert_eq!(
        inspection.receipts[1].applicability,
        ExternalResolutionApplicability::Applicable
    );
    let created_one = inspection.receipts[0].receipt.created_at_ms;
    let created_two = inspection.receipts[1].receipt.created_at_ms;
    let receipts_before_change = inspection
        .receipts
        .iter()
        .map(|entry| entry.receipt.clone())
        .collect::<Vec<_>>();

    fixture
        .store
        .transact_binding_ledger(BINDING, None, |ledger| {
            let job = ledger
                .jobs
                .iter_mut()
                .find(|job| job.job_id == fixture.job.job_id)
                .expect("fixture job");
            job.job_revision += 1;
            Ok(CoworkerLedgerMutationOutcome::Mutated(()))
        })
        .expect("advance target job revision");
    let after = fixture
        .store
        .read_binding_ledger(BINDING)
        .expect("ledger after target change");
    assert_eq!(
        after.external_resolutions, receipts_before_change,
        "target changes must never rewrite historical receipts"
    );

    let stale = service.inspect(BINDING, THREAD).expect("inspect stale");
    assert_eq!(stale.receipts.len(), 2);
    for entry in &stale.receipts {
        assert_eq!(entry.receipt.created_at_ms, created_one.min(created_two));
        assert!(matches!(
            &entry.applicability,
            ExternalResolutionApplicability::Stale { .. }
        ));
        let ExternalResolutionApplicability::Stale { reasons } = &entry.applicability else {
            unreachable!();
        };
        assert!(reasons.contains(&ExternalResolutionStaleReason::JobRevisionAdvanced));
    }
    assert_eq!(stale.receipts[0].receipt, receipts_before_change[0]);
    assert_eq!(stale.receipts[1].receipt, receipts_before_change[1]);
}

#[test]
fn stale_preview_after_competing_append_is_rejected_without_a_second_receipt() {
    let fixture = setup_fixture();
    let service = service(&fixture);
    let request_a = job_request(
        &fixture,
        "req_concurrent_record_a",
        ExternalResolutionScope::FullJob,
        Some(1),
        Some(1),
    );
    let request_b = job_request(
        &fixture,
        "req_concurrent_record_b",
        ExternalResolutionScope::Tasks {
            task_ids: vec!["task-alpha".to_string()],
        },
        None,
        Some(1),
    );
    let preview_a = service.preview(&request_a).expect("preview A");
    let preview_b = service.preview(&request_b).expect("preview B");
    assert_eq!(preview_a.source_ledger_revision, 1);
    assert_eq!(preview_b.source_ledger_revision, 1);
    assert_ne!(preview_a.preview_digest, preview_b.preview_digest);

    service
        .record(&request_a, &confirmation(&preview_a))
        .expect("record A");
    let err = service
        .record(&request_b, &confirmation(&preview_b))
        .expect_err("stale preview B must not pass the CAS append");
    assert_eq!(err.code(), NativeErrorCode::UnauthorizedOperator.as_str());

    let ledger = fixture
        .store
        .read_binding_ledger(BINDING)
        .expect("ledger after competing append");
    assert_eq!(ledger.ledger_revision, 2);
    assert_eq!(ledger.external_resolutions.len(), 1);
    assert_eq!(
        ledger.external_resolutions[0].request_key,
        request_a.request_key
    );
}

#[test]
fn restart_and_injected_persist_failure_are_reconciled_without_blind_retry_or_revision_bump() {
    let fixture = setup_fixture();
    let first_service = service(&fixture);
    let request_one = job_request(
        &fixture,
        "req_restart_01",
        ExternalResolutionScope::FullJob,
        Some(1),
        Some(1),
    );
    let preview_one = first_service.preview(&request_one).expect("preview one");
    let receipt_one = first_service
        .record(&request_one, &confirmation(&preview_one))
        .expect("record one");
    drop(first_service);

    let second_service = service(&fixture);
    let uncertain = second_service
        .inspect(BINDING, THREAD)
        .expect("inspect after restart");
    assert_eq!(uncertain.receipts.len(), 1);
    assert!(uncertain.has_uncertain_durability);
    assert_eq!(
        uncertain.receipts[0].durability,
        ExternalResolutionDurability::PersistenceUncertain
    );

    let ledger_path = fixture
        .store
        .ledger_path_for_binding(BINDING)
        .expect("ledger path");
    let ledger_bytes = fs::read(&ledger_path).expect("ledger bytes before replay");
    let plans_before = dir_entries(&fixture.plans);
    let replay = second_service
        .record(&request_one, &confirmation(&preview_one))
        .expect_err("unwitnessed duplicate must not claim success");
    assert_eq!(
        replay.code(),
        NativeErrorCode::PersistenceUncertain.as_str()
    );
    assert_eq!(
        fs::read(&ledger_path).expect("ledger bytes after replay"),
        ledger_bytes
    );
    assert_eq!(dir_entries(&fixture.plans), plans_before);

    let wrong = second_service
        .reconcile(&ReconcileExternalResolutionRequest {
            trusted_local_attested: true,
            binding_id: BINDING.to_string(),
            thread_uuid: THREAD.to_string(),
            request_key: request_one.request_key.clone(),
            expected_preview_digest: HEX64_A.to_string(),
            expected_receipt_digest: None,
        })
        .expect_err("wrong original digest must not reconcile");
    assert_eq!(wrong.code(), NativeErrorCode::UnauthorizedOperator.as_str());

    let reconciled = second_service
        .reconcile(&ReconcileExternalResolutionRequest {
            trusted_local_attested: true,
            binding_id: BINDING.to_string(),
            thread_uuid: THREAD.to_string(),
            request_key: receipt_one.request_key.clone(),
            expected_preview_digest: receipt_one.preview_digest.clone(),
            expected_receipt_digest: Some(receipt_one.receipt_digest.clone()),
        })
        .expect("reconcile original bytes");
    assert_eq!(reconciled, receipt_one);
    let verified = second_service
        .inspect(BINDING, THREAD)
        .expect("inspect reconciled");
    assert_eq!(
        verified.receipts[0].durability,
        ExternalResolutionDurability::VerifiedDurable
    );
    assert_eq!(
        fixture
            .store
            .read_binding_ledger(BINDING)
            .expect("ledger after reconcile")
            .ledger_revision,
        2
    );
    assert_eq!(
        fs::read(&ledger_path).expect("ledger bytes after reconcile"),
        ledger_bytes
    );

    let request_two = job_request(
        &fixture,
        "req_injected_uncertainty_01",
        ExternalResolutionScope::FullJob,
        None,
        Some(1),
    );
    let preview_two = second_service.preview(&request_two).expect("preview two");
    let failing_service =
        service(&fixture).with_post_persist_probe(|_: &Path| -> Result<(), NativeError> {
            Err(NativeError::persistence_uncertain(
                "injected post-persist durability failure",
            ))
        });
    let failed = failing_service
        .record(&request_two, &confirmation(&preview_two))
        .expect_err("post-persist probe failure must surface uncertainty");
    assert_eq!(
        failed.code(),
        NativeErrorCode::PersistenceUncertain.as_str()
    );
    let after_failure = second_service
        .inspect(BINDING, THREAD)
        .expect("inspect after injected failure");
    assert_eq!(after_failure.receipts.len(), 2);
    assert_eq!(
        after_failure.receipts[1].durability,
        ExternalResolutionDurability::PersistenceUncertain
    );
    let ledger_after_failure = fs::read(&ledger_path).expect("ledger bytes after failure");
    let plans_after_failure = dir_entries(&fixture.plans);
    let replay_two = second_service
        .record(&request_two, &confirmation(&preview_two))
        .expect_err("replay after uncertain append must not blind-write");
    assert_eq!(
        replay_two.code(),
        NativeErrorCode::PersistenceUncertain.as_str()
    );
    assert_eq!(
        fs::read(&ledger_path).expect("ledger bytes after uncertain replay"),
        ledger_after_failure
    );
    assert_eq!(dir_entries(&fixture.plans), plans_after_failure);

    let receipt_two = second_service
        .reconcile(&ReconcileExternalResolutionRequest {
            trusted_local_attested: true,
            binding_id: BINDING.to_string(),
            thread_uuid: THREAD.to_string(),
            request_key: preview_two.request_key.clone(),
            expected_preview_digest: preview_two.preview_digest.clone(),
            expected_receipt_digest: None,
        })
        .expect("reconcile injected uncertain receipt");
    assert_eq!(receipt_two.request_key, request_two.request_key);
    let final_inspection = second_service
        .inspect(BINDING, THREAD)
        .expect("inspect final");
    assert_eq!(final_inspection.receipts.len(), 2);
    assert!(final_inspection
        .receipts
        .iter()
        .all(|entry| entry.durability == ExternalResolutionDurability::VerifiedDurable));
    assert_eq!(
        fixture
            .store
            .read_binding_ledger(BINDING)
            .expect("final ledger")
            .ledger_revision,
        3
    );
    assert_eq!(
        fs::read(&ledger_path).expect("final ledger bytes"),
        ledger_after_failure
    );
}

#[test]
fn tampered_persisted_receipt_fails_closed_and_preserves_tampered_bytes() {
    let fixture = setup_fixture();
    let service = service(&fixture);
    let request = job_request(
        &fixture,
        "req_tampered_receipt_01",
        ExternalResolutionScope::FullJob,
        Some(1),
        Some(1),
    );
    let preview = service.preview(&request).expect("preview");
    service
        .record(&request, &confirmation(&preview))
        .expect("record");

    let ledger_path = fixture
        .store
        .ledger_path_for_binding(BINDING)
        .expect("ledger path");
    let mut raw: serde_json::Value =
        serde_json::from_slice(&fs::read(&ledger_path).expect("read ledger"))
            .expect("parse ledger");
    raw["external_resolutions"][0]["explanation"] =
        json!("tampered explanation that breaks receipt integrity");
    write_private_json(&ledger_path, &raw);
    let tampered_bytes = fs::read(&ledger_path).expect("read tampered bytes");

    let err = service
        .inspect(BINDING, THREAD)
        .expect_err("tampered receipt must fail closed");
    assert_eq!(err.code(), NativeErrorCode::ProtocolError.as_str());
    assert_eq!(
        fs::read(&ledger_path).expect("reread tampered ledger"),
        tampered_bytes,
        "tampered ledger must be preserved untouched for operator diagnosis"
    );
    let err = service
        .preview(&job_request(
            &fixture,
            "req_after_tamper_01",
            ExternalResolutionScope::FullJob,
            None,
            None,
        ))
        .expect_err("preview on tampered ledger must fail closed");
    assert_eq!(err.code(), NativeErrorCode::ProtocolError.as_str());
}

#[test]
fn frozen_approval_linkage_and_job_task_digest_drift_are_refused() {
    let fixture = setup_fixture();
    let service = service(&fixture);
    let request = job_request(
        &fixture,
        "req_linkage_drift_01",
        ExternalResolutionScope::FullJob,
        None,
        None,
    );
    let source_before =
        fs::read(fixture.repo.join("src/lib.rs")).expect("read source before linkage drift");
    let original_job = fixture.job.clone();
    let mut drift_cases = Vec::new();

    let mut plan_digest_job = original_job.clone();
    plan_digest_job
        .approval_linkage
        .as_mut()
        .expect("fixture approval linkage")
        .plan_digest = HEX64_B.to_string();
    drift_cases.push(plan_digest_job);

    let mut profile_job = original_job.clone();
    profile_job
        .approval_linkage
        .as_mut()
        .expect("fixture approval linkage")
        .profile_id = "profile-drifted".to_string();
    drift_cases.push(profile_job);

    let mut registry_job = original_job.clone();
    registry_job
        .approval_linkage
        .as_mut()
        .expect("fixture approval linkage")
        .registry_revision = fixture.registry_revision + 1;
    drift_cases.push(registry_job);

    let mut approved_task_job = original_job.clone();
    approved_task_job
        .approval_linkage
        .as_mut()
        .expect("fixture approval linkage")
        .approved_task_ids = vec!["task-alpha".to_string()];
    drift_cases.push(approved_task_job);

    let mut write_scope_job = original_job.clone();
    write_scope_job
        .approval_linkage
        .as_mut()
        .expect("fixture approval linkage")
        .approved_write_paths = vec!["src/unapproved.rs".to_string()];
    drift_cases.push(write_scope_job);

    let mut task_digest_job = original_job.clone();
    task_digest_job.tasks[0].task_digest = sha256_hex(b"drifted task digest");
    drift_cases.push(task_digest_job);

    let mut task_scope_job = original_job.clone();
    task_scope_job.tasks[0].write_paths = vec!["src/unapproved.rs".to_string()];
    drift_cases.push(task_scope_job);

    let mut task_dependency_job = original_job.clone();
    task_dependency_job.tasks[0].depends_on = vec!["task-beta".to_string()];
    drift_cases.push(task_dependency_job);

    for drifted_job in drift_cases {
        fixture
            .store
            .transact_binding_ledger(BINDING, None, |ledger| {
                ledger.jobs = vec![drifted_job.clone()];
                Ok(CoworkerLedgerMutationOutcome::Mutated(()))
            })
            .expect("write drifted job fixture");
        let err = service
            .preview(&request)
            .expect_err("immutable approval/task drift must be refused");
        assert_eq!(err.code(), NativeErrorCode::ValidationFailed.as_str());
        assert_eq!(
            fixture
                .store
                .read_binding_ledger(BINDING)
                .expect("ledger during drift attempts")
                .external_resolutions
                .len(),
            0
        );
    }

    fixture
        .store
        .transact_binding_ledger(BINDING, None, |ledger| {
            ledger.jobs = vec![original_job];
            Ok(CoworkerLedgerMutationOutcome::Mutated(()))
        })
        .expect("restore original job");
    let preview = service
        .preview(&request)
        .expect("preview recovers with exact frozen approval and task digests");
    assert_eq!(
        preview.plan_digest.as_deref(),
        Some(fixture.approval.plan_digest.as_str())
    );

    let receipt = service
        .record(&request, &confirmation(&preview))
        .expect("record with exact real frozen approval fields");
    assert_eq!(receipt.request_key, request.request_key);
    assert_eq!(
        fs::read(fixture.repo.join("src/lib.rs")).expect("read source after linkage drift"),
        source_before
    );
    assert!(!fixture.provider_marker.exists());
}

#[test]
fn stopped_host_runtime_proof_rejects_active_unknown_and_nonquiescent_evidence() {
    let fixture = setup_fixture();
    let (session_dir, runtime_binding_id) = install_stopped_host_session(&fixture);
    let service = service(&fixture);
    let request = job_request(
        &fixture,
        "req_host_runtime_proof_01",
        ExternalResolutionScope::FullJob,
        None,
        None,
    );
    let repo_source_before =
        fs::read(fixture.repo.join("src/lib.rs")).expect("read source before proof");
    let preview = service
        .preview(&request)
        .expect("stopped host with idle runtime and no active work must preview");
    assert_eq!(
        fixture
            .store
            .read_binding_ledger(BINDING)
            .expect("ledger before host proof attempts")
            .external_resolutions
            .len(),
        0
    );

    let runtime_state_dir = session_dir.join("runtime-state");
    let supervisor_guard = acquire_cross_process_lock(
        &runtime_state_dir,
        "supervisor.lock",
        "test_supervisor_lock",
    )
    .expect("hold supervisor.lock");
    let err = service
        .preview(&request)
        .expect_err("held supervisor.lock must refuse stopped-runtime proof");
    assert_eq!(
        err.code(),
        NativeErrorCode::AmbiguousWakeupPendingReconciliation.as_str()
    );
    drop(supervisor_guard);
    fs::remove_file(runtime_state_dir.join("supervisor.lock"))
        .expect("remove test supervisor.lock");

    set_runtime_wakeup_status(&session_dir, "pending");
    let err = service
        .preview(&request)
        .expect_err("Pending wakeup evidence must be refused");
    assert_eq!(
        err.code(),
        NativeErrorCode::AmbiguousWakeupPendingReconciliation.as_str()
    );
    set_runtime_wakeup_status(&session_dir, "idle");

    insert_runtime_run_and_worker(&session_dir, &runtime_binding_id, "running", "running");
    let err = service
        .preview(&request)
        .expect_err("active run and worker must be refused");
    assert_eq!(err.code(), NativeErrorCode::ValidationFailed.as_str());

    update_runtime_run_and_worker_status(&session_dir, "completed", "unknown");
    let err = service
        .preview(&request)
        .expect_err("Unknown worker must be refused");
    assert_eq!(
        err.code(),
        NativeErrorCode::AmbiguousWakeupPendingReconciliation.as_str()
    );

    update_runtime_run_and_worker_status(&session_dir, "unknown", "completed");
    let err = service
        .preview(&request)
        .expect_err("Unknown run must be refused");
    assert_eq!(
        err.code(),
        NativeErrorCode::AmbiguousWakeupPendingReconciliation.as_str()
    );

    update_runtime_run_and_worker_status(&session_dir, "failed", "failed");
    service
        .preview(&request)
        .expect("terminal Failed runtime records are quiescent worker/run evidence");

    remove_runtime_run_and_worker(&session_dir);
    set_runtime_wakeup_status(&session_dir, "submitted");
    let err = service
        .preview(&request)
        .expect_err("Submitted outstanding wakeup delivery must be refused");
    assert_eq!(
        err.code(),
        NativeErrorCode::AmbiguousWakeupPendingReconciliation.as_str()
    );
    set_runtime_wakeup_status(&session_dir, "idle");

    let database_path = runtime_database_path(&session_dir);
    let parked_database = database_path.with_extension("parked");
    fs::rename(&database_path, &parked_database).expect("park runtime state.db");
    let err = service
        .preview(&request)
        .expect_err("missing runtime state.db must not be treated as quiescent");
    assert_eq!(
        err.code(),
        NativeErrorCode::AmbiguousWakeupPendingReconciliation.as_str()
    );
    fs::rename(&parked_database, &database_path).expect("restore runtime state.db");

    let receipt = service
        .record(&request, &confirmation(&preview))
        .expect("record with proven stopped runtime quiescence");
    assert_eq!(receipt.request_key, request.request_key);
    assert_eq!(
        fs::read(fixture.repo.join("src/lib.rs")).expect("read source after proof"),
        repo_source_before
    );
    assert!(!fixture.provider_marker.exists());
    assert_eq!(
        fixture
            .store
            .read_binding_ledger(BINDING)
            .expect("ledger after host proof record")
            .external_resolutions
            .len(),
        1
    );
}

#[test]
fn pending_and_unknown_controller_effects_block_record_until_reconciled() {
    let fixture = setup_fixture();
    let controller = install_controller_runtime(&fixture);
    let service = service(&fixture);
    let request = job_request(
        &fixture,
        "req_controller_effect_01",
        ExternalResolutionScope::FullJob,
        None,
        None,
    );

    set_controller_effect(
        &controller,
        PendingControllerEffectStatus::ReservedBeforeEffect,
    );
    let err = service
        .preview(&request)
        .expect_err("reserved controller effect must block preview");
    assert_eq!(err.code(), NativeErrorCode::ValidationFailed.as_str());

    set_controller_effect(&controller, PendingControllerEffectStatus::EffectUnknown);
    let err = service
        .preview(&request)
        .expect_err("Unknown controller effect must block preview");
    assert_eq!(
        err.code(),
        NativeErrorCode::AmbiguousWakeupPendingReconciliation.as_str()
    );

    clear_controller_effects(&controller);
    let preview = service
        .preview(&request)
        .expect("preview recovers after controller effect reconciliation");
    let receipt = service
        .record(&request, &confirmation(&preview))
        .expect("record after controller effect reconciliation");
    assert_eq!(receipt.request_key, request.request_key);
    assert!(!fixture.provider_marker.exists());
}

#[test]
fn record_reacquires_physical_owner_and_planning_command_guards() {
    let fixture = setup_fixture();
    let service = service(&fixture);
    let request = job_request(
        &fixture,
        "req_guard_exclusion_01",
        ExternalResolutionScope::FullJob,
        None,
        None,
    );
    let preview = service
        .preview(&request)
        .expect("preview before held guards");
    let ledger_bytes_before = fs::read(
        fixture
            .store
            .ledger_path_for_binding(BINDING)
            .expect("ledger path"),
    )
    .expect("ledger bytes before held guards");

    let ownership_dir = fixture.plans.join("coworker-code-driver");
    make_private_dir(&ownership_dir);
    let ownership = CoworkerJobOwnershipStore::new(&ownership_dir).expect("ownership store");
    let key = CoworkerJobOwnershipKey::new(BINDING, THREAD, fixture.job.job_id.clone())
        .expect("ownership key");
    let owner_guard = match ownership
        .try_acquire_job(&key, 1_700_000_500_000)
        .expect("acquire owner guard")
    {
        CoworkerOwnershipAcquireOutcome::Acquired(guard) => guard,
        CoworkerOwnershipAcquireOutcome::Busy { .. } => panic!("fresh owner guard must acquire"),
    };
    let err = service
        .record(&request, &confirmation(&preview))
        .expect_err("record must reacquire and observe the physical owner guard");
    assert_eq!(err.code(), NativeErrorCode::ValidationFailed.as_str());
    assert!(err.message().contains("owned by a live"));
    assert_eq!(
        fs::read(
            fixture
                .store
                .ledger_path_for_binding(BINDING)
                .expect("ledger path")
        )
        .expect("ledger bytes after owner refusal"),
        ledger_bytes_before
    );
    drop(owner_guard);

    let commands_dir = planning_commands_dir(&fixture.plans);
    make_private_dir(&commands_dir);
    let planning_guard = acquire_cross_process_lock(
        &commands_dir,
        &planning_command_lock_name(BINDING),
        "test_planning_command_guard",
    )
    .expect("hold planning command lock");
    let record_service = service.clone();
    let record_request = request.clone();
    let record_confirmation = confirmation(&preview);
    let (record_done_tx, record_done_rx) = mpsc::channel();
    let record_thread = thread::spawn(move || {
        let result = record_service.record(&record_request, &record_confirmation);
        let _ = record_done_tx.send(result);
    });
    assert!(
        record_done_rx
            .recv_timeout(Duration::from_millis(250))
            .is_err(),
        "record must wait for the held planning command lock"
    );
    assert_eq!(
        fixture
            .store
            .read_binding_ledger(BINDING)
            .expect("ledger during held planning lock")
            .external_resolutions
            .len(),
        0
    );
    drop(planning_guard);
    let receipt = record_done_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("record completes after planning lock release")
        .expect("record succeeds after planning lock release");
    record_thread.join().expect("join record thread");
    assert_eq!(receipt.request_key, request.request_key);
    assert!(!fixture.provider_marker.exists());
}

#[test]
fn record_waits_for_held_conversation_advancement_guard() {
    let fixture = setup_fixture();
    let service = service(&fixture);
    let request = job_request(
        &fixture,
        "req_conversation_guard_01",
        ExternalResolutionScope::FullJob,
        None,
        None,
    );
    let preview = service
        .preview(&request)
        .expect("preview before held guard");
    let namespace_dir = make_private_dir(&fixture.plans.join("conversation-changes"));
    let conversation_guard = acquire_cross_process_lock(
        &namespace_dir,
        &conversation_change_lock_name(BINDING),
        "test_conversation_advancement_guard",
    )
    .expect("hold conversation advancement lock");
    let ledger_bytes_before = fs::read(
        fixture
            .store
            .ledger_path_for_binding(BINDING)
            .expect("ledger path"),
    )
    .expect("ledger bytes before held conversation guard");

    let record_service = service.clone();
    let record_request = request.clone();
    let record_confirmation = confirmation(&preview);
    let (record_done_tx, record_done_rx) = mpsc::channel();
    let record_thread = thread::spawn(move || {
        let result = record_service.record(&record_request, &record_confirmation);
        let _ = record_done_tx.send(result);
    });
    assert!(
        record_done_rx
            .recv_timeout(Duration::from_millis(250))
            .is_err(),
        "record must wait for the held conversation advancement lock"
    );
    assert_eq!(
        fs::read(
            fixture
                .store
                .ledger_path_for_binding(BINDING)
                .expect("ledger path")
        )
        .expect("ledger bytes during held conversation guard"),
        ledger_bytes_before
    );
    drop(conversation_guard);
    let receipt = record_done_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("record completes after conversation lock release")
        .expect("record succeeds after conversation lock release");
    record_thread.join().expect("join record thread");
    assert_eq!(receipt.request_key, request.request_key);
    assert!(!fixture.provider_marker.exists());
}

#[test]
fn concurrent_plan_publication_and_record_linearize_under_plan_store_lock() {
    let fixture = setup_fixture();
    let (probe_tx, probe_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let probe_tx = Arc::new(Mutex::new(probe_tx));
    let release_rx = Arc::new(Mutex::new(release_rx));
    let service = service(&fixture).with_post_persist_probe(move |_| {
        probe_tx
            .lock()
            .map_err(|_| invalid_probe_error())?
            .send(())
            .map_err(|_| {
                NativeError::persistence_uncertain("plan race probe receiver disappeared")
            })?;
        release_rx
            .lock()
            .map_err(|_| invalid_probe_error())?
            .recv()
            .map_err(|_| {
                NativeError::persistence_uncertain("plan race probe release disappeared")
            })?;
        Ok(())
    });
    let request = job_request(
        &fixture,
        "req_plan_race_01",
        ExternalResolutionScope::FullJob,
        Some(1),
        Some(1),
    );
    let preview = service.preview(&request).expect("preview before plan race");
    let record_confirmation = confirmation(&preview);
    let record_service = service.clone();
    let record_request = request.clone();
    let record_thread =
        thread::spawn(move || record_service.record(&record_request, &record_confirmation));
    probe_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("record reaches post-persist probe with all guards held");

    let (published_tx, published_rx) = mpsc::channel();
    let (publisher_started_tx, publisher_started_rx) = mpsc::channel();
    let plans_dir = fixture.plans.clone();
    let plan_id = fixture.plan.plan_id.clone();
    let plan_tasks = fixture.plan.tasks.clone();
    let profile_revision = fixture.registry_revision;
    let plan_revision = fixture.plan.revision;
    let publisher_thread = thread::spawn(move || {
        let _ = publisher_started_tx.send(());
        let plans = PlanStore::open(&plans_dir).expect("publisher opens plan store");
        let draft = plans
            .change_at(
                plan_revision,
                PlanChange::SaveDraft {
                    plan_id: plan_id.clone(),
                    binding_id: BINDING.to_string(),
                    profile_id: "profile-external".to_string(),
                    profile_revision,
                    summary: "Concurrent successor draft".to_string(),
                    tasks: plan_tasks,
                },
                1_700_000_900_000,
            )
            .expect("concurrent publisher saves successor draft");
        let published = plans
            .change(
                draft.revision,
                PlanChange::Publish {
                    plan_id: draft.plan_id.clone(),
                    revision: draft.revision,
                },
            )
            .expect("concurrent publisher publishes successor");
        let _ = published_tx.send(published.revision);
        published.revision
    });
    publisher_started_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("publisher thread starts while plans.lock is held");
    assert!(
        published_rx
            .recv_timeout(Duration::from_millis(250))
            .is_err(),
        "publisher must not supersede the target while the receipt append holds plans.lock"
    );

    release_tx.send(()).expect("release record probe");
    let receipt = record_thread
        .join()
        .expect("join record thread")
        .expect("record linearizes before concurrent publication");
    let published_revision = published_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("publisher completes after plans.lock release");
    assert_eq!(published_revision, fixture.plan.revision + 1);
    publisher_thread.join().expect("join publisher thread");
    assert_eq!(receipt.request_key, request.request_key);

    let inspection = service
        .inspect(BINDING, THREAD)
        .expect("inspect after race");
    assert_eq!(inspection.receipts.len(), 1);
    let ExternalResolutionApplicability::Stale { reasons } = &inspection.receipts[0].applicability
    else {
        panic!("published successor must make the preserved receipt stale");
    };
    assert!(reasons.contains(&ExternalResolutionStaleReason::BoundPlanChanged));
    assert!(!fixture.provider_marker.exists());
}

#[test]
fn record_refuses_confirmed_target_after_successor_publication() {
    let fixture = setup_fixture();
    let service = service(&fixture);
    let request = job_request(
        &fixture,
        "req_plan_successor_refusal_01",
        ExternalResolutionScope::FullJob,
        Some(1),
        Some(1),
    );
    let preview = service
        .preview(&request)
        .expect("preview before successor publication");

    let plans = PlanStore::open(&fixture.plans).expect("open plans for successor");
    let draft = plans
        .change_at(
            fixture.plan.revision,
            PlanChange::SaveDraft {
                plan_id: fixture.plan.plan_id.clone(),
                binding_id: BINDING.to_string(),
                profile_id: "profile-external".to_string(),
                profile_revision: fixture.registry_revision,
                summary: "Successor published after confirmation".to_string(),
                tasks: fixture.plan.tasks.clone(),
            },
            1_700_000_900_000,
        )
        .expect("save successor draft");
    plans
        .change(
            draft.revision,
            PlanChange::Publish {
                plan_id: draft.plan_id.clone(),
                revision: draft.revision,
            },
        )
        .expect("publish successor");

    let err = service
        .record(&request, &confirmation(&preview))
        .expect_err("record must refuse a confirmed target superseded by publication");
    assert_eq!(err.code(), NativeErrorCode::ValidationFailed.as_str());
    assert_eq!(
        fixture
            .store
            .read_binding_ledger(BINDING)
            .expect("ledger after successor refusal")
            .external_resolutions
            .len(),
        0
    );
    assert!(!fixture.provider_marker.exists());
}

fn invalid_probe_error() -> NativeError {
    NativeError::persistence_uncertain("plan race probe lock poisoned")
}
