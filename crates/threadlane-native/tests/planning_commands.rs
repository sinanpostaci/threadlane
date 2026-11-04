use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path;
use std::sync::Arc;

use tempfile::TempDir;
use threadlane_native::coworker_workflow::{
    sha256_hex, ArchitectContextDependencies, CanonicalProjectBaseRef, CoworkerJobOwnershipKey,
    CoworkerJobOwnershipStore, CoworkerPlanningService, CoworkerWorkflowStore,
    CreateCoworkerJobRequest, CreatePlanningGrantRequest, OrchestrateWorkBriefPlanRequest,
    PersistedPlanningCallArtifact, PlanningCallBudget, PlanningGrantStatus,
    PlanningReadOnlyCapabilities, RevokePlanningGrantRequest, RunArchitectAdviceRequest, WorkBrief,
    WorkBriefKind,
};
use threadlane_native::{
    current_wall_clock_ms, open_workflow_stores, Difficulty, EnqueuePlanningCommandRequest,
    ExecutionPolicy, NativeErrorCode, PlanStatus, PlanningCommandDriver, PlanningCommandPayload,
    PlanningCommandResultRef, PlanningCommandStatus, PlanningCommandStore, PolicyAllowRule,
    RegistryStore, ScopedWorkflow, ToolAdapter, WorkflowProfile, WorkflowRole,
};
use threadlane_workers::{
    resolve_exact_commit_sha, resolve_exact_tree_sha, run_sanitized_git,
    DEFAULT_GIT_COMMAND_TIMEOUT_MS,
};
use threadlane_workflow::{
    ModelChoice, ModelDefinition, RegistryChange, RoleAssignment, ToolDefinition,
};

const THREAD_UUID_A: &str = "550e8400-e29b-41d4-a716-446655440000";
const THREAD_UUID_B: &str = "6ba7b810-9dad-11d1-80b4-00c04fd430c8";
const BINDING_A: &str = "bnd_plan_cmd_alpha";
const BINDING_B: &str = "bnd_plan_cmd_beta";

struct CommandTestFixture {
    _tmp: TempDir,
    project_base: CanonicalProjectBaseRef,
    registry_dir: std::path::PathBuf,
    commands_dir: std::path::PathBuf,
    workflow_store: CoworkerWorkflowStore,
    ownership_store: CoworkerJobOwnershipStore,
    command_store: PlanningCommandStore,
    dep_blobs: BTreeMap<String, String>,
}

fn init_git_repo(repo_dir: &Path) -> CanonicalProjectBaseRef {
    fs::create_dir_all(repo_dir.join("src")).expect("create repo src");
    fs::write(
        repo_dir.join("src/lib.rs"),
        "pub fn planning_command_seed() -> u32 { 42 }\n",
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
    CanonicalProjectBaseRef {
        repository_path: repo_dir.to_string_lossy().into_owned(),
        base_commit: commit,
        base_tree_sha: tree,
    }
}

fn write_executable_stub(path: &Path) {
    use std::io::Write;
    use std::os::unix::fs::PermissionsExt;

    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).expect("create stub parent");
    }
    let script = r#"#!/bin/sh
set -eu
printf '{"event":"init","init":{"session_id":"agy-cmd-sess-1","model":"gemini-4-argon-high"}}\n'
RESPONSE_JSON='{\"schema_version\":1,\"decision_summary\":\"Keep strict planning command boundary\",\"rationale\":\"Verify completed command result against stored advice, reservation, and private artifact.\",\"recommended_write_paths\":[\"src/lib.rs\"],\"interface_contracts\":{\"iface_core\":\"Stable contract v1\"},\"open_questions\":[]}'
printf '{"event":"result","result":{"status":"SUCCESS","response":"%s"}}\n' "$RESPONSE_JSON"
exit 0
"#;
    let mut file = fs::File::create(path).expect("create stub");
    file.write_all(script.as_bytes()).expect("write stub");
    file.sync_all().expect("sync stub");
    fs::set_permissions(path, fs::Permissions::from_mode(0o755)).expect("chmod 0755 stub");
}

fn make_private_dir(path: &Path) {
    use std::os::unix::fs::PermissionsExt;
    fs::create_dir_all(path).expect("create private dir");
    fs::set_permissions(path, fs::Permissions::from_mode(0o700)).expect("chmod 0700 private dir");
}

fn list_dir_entries(dir: &Path) -> BTreeSet<String> {
    fs::read_dir(dir)
        .expect("read_dir")
        .map(|entry| {
            entry
                .expect("dir entry")
                .file_name()
                .to_string_lossy()
                .into_owned()
        })
        .collect()
}

fn default_budget() -> PlanningCallBudget {
    PlanningCallBudget {
        max_total_calls: 6,
        max_orchestrator_calls: 4,
        max_architect_calls: 2,
        max_timeout_ms_per_call: 60_000,
        max_output_bytes_per_call: 65_536,
    }
}

fn default_policy() -> ExecutionPolicy {
    ExecutionPolicy {
        schema_version: 1,
        policy_id: "policy-planning-commands".to_string(),
        allow_rules: vec![PolicyAllowRule {
            tool_id: "tool-agy".to_string(),
            model_id: "model-argon".to_string(),
            efforts: vec!["high".to_string()],
            allow_no_effort: false,
            roles: vec![WorkflowRole::Orchestrator, WorkflowRole::Architect],
        }],
    }
}

fn setup_fixture() -> CommandTestFixture {
    let tmp = TempDir::new().expect("tempdir");
    let root = fs::canonicalize(tmp.path()).expect("canonicalize temp root");

    let repo_dir = root.join("repo");
    let project_base = init_git_repo(&repo_dir);

    let fake_agy = root.join("bin/fake-agy");
    write_executable_stub(&fake_agy);

    let registry_dir = root.join("registry-store");
    let plans_dir = root.join("plans-store");
    let commands_dir = root.join("commands-store");
    make_private_dir(&plans_dir);
    make_private_dir(&commands_dir);

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
                    ],
                },
            },
        )
        .expect("upsert profile-coworker");

    let workflow_store =
        CoworkerWorkflowStore::new(&plans_dir).expect("open CoworkerWorkflowStore");
    let ownership_store =
        CoworkerJobOwnershipStore::new(&commands_dir).expect("open CoworkerJobOwnershipStore");
    let command_store = PlanningCommandStore::new(
        &commands_dir,
        workflow_store.clone(),
        &registry_dir,
        ownership_store.clone(),
    )
    .expect("open PlanningCommandStore");

    let dep_blobs = CoworkerPlanningService::compute_dependency_file_blobs(
        &project_base,
        &["src/lib.rs".to_string()],
    )
    .expect("compute dependency file blobs");

    CommandTestFixture {
        _tmp: tmp,
        project_base,
        registry_dir,
        commands_dir,
        workflow_store,
        ownership_store,
        command_store,
        dep_blobs,
    }
}

fn create_grant_in_fixture(
    fixture: &CommandTestFixture,
    binding_id: &str,
    thread_uuid: &str,
    request_key: &str,
    created_at_ms: u64,
    expires_at_ms: u64,
) -> String {
    fixture
        .workflow_store
        .create_planning_grant(&CreatePlanningGrantRequest {
            binding_id: binding_id.to_string(),
            thread_uuid: thread_uuid.to_string(),
            request_key: request_key.to_string(),
            expected_ledger_revision: None,
            project_base: fixture.project_base.clone(),
            profile_id: "profile-coworker".to_string(),
            registry_revision: 3,
            policy: default_policy(),
            capabilities: PlanningReadOnlyCapabilities::strict_read_only(),
            budget: default_budget(),
            created_at_ms,
            expires_at_ms,
            operator_attested: true,
        })
        .expect("create planning grant")
        .grant_id
}

fn create_job_in_fixture(
    fixture: &CommandTestFixture,
    binding_id: &str,
    thread_uuid: &str,
    request_key: &str,
) -> (String, String) {
    let brief = WorkBrief {
        brief_id: format!("brf_{request_key}"),
        binding_id: binding_id.to_string(),
        thread_uuid: thread_uuid.to_string(),
        project_base: fixture.project_base.clone(),
        kind: WorkBriefKind::NewJob,
        summary: "Implement durable planning command ledger".to_string(),
        confirmed_requirements: vec![
            "Validate active grant and context before queueing".to_string()
        ],
        proposed_additions: Vec::new(),
        assumptions: Vec::new(),
        open_questions: Vec::new(),
        accepted_architect_decisions: Vec::new(),
    };
    let job = fixture
        .workflow_store
        .create_or_amend_job(&CreateCoworkerJobRequest {
            binding_id: binding_id.to_string(),
            thread_uuid: thread_uuid.to_string(),
            request_key: request_key.to_string(),
            expected_ledger_revision: None,
            work_brief: brief,
            amended_target_task_ids: Vec::new(),
            amended_write_paths: Vec::new(),
            amended_interfaces: Vec::new(),
        })
        .expect("create job");
    (job.job_id, job.work_brief_digest)
}

fn sample_architect_request(
    fixture: &CommandTestFixture,
    binding_id: &str,
    thread_uuid: &str,
    grant_id: &str,
    request_key: &str,
    work_brief_digest: &str,
    question_summary: &str,
) -> RunArchitectAdviceRequest {
    let policy_digest = default_policy().digest().expect("policy digest");
    RunArchitectAdviceRequest {
        binding_id: binding_id.to_string(),
        thread_uuid: thread_uuid.to_string(),
        grant_id: grant_id.to_string(),
        request_key: request_key.to_string(),
        expected_ledger_revision: None,
        difficulty: Some(Difficulty::Hard),
        context_dependencies: ArchitectContextDependencies {
            project_base: fixture.project_base.clone(),
            work_brief_digest: work_brief_digest.to_string(),
            plan_digest: None,
            policy_digest,
            dependency_file_blobs: fixture.dep_blobs.clone(),
            interface_ownership_digests: BTreeMap::new(),
            accepted_predecessor_trees: BTreeMap::new(),
        },
        question_summary: question_summary.to_string(),
        prior_advice_context: Vec::new(),
        accept_into_brief: false,
        current_time_ms: current_wall_clock_ms().expect("clock"),
    }
}

#[test]
fn readonly_lookup_and_list_absence_do_not_create_files_or_locks() {
    let fixture = setup_fixture();
    assert!(list_dir_entries(&fixture.commands_dir).is_empty());

    let reopened = PlanningCommandStore::open_existing(
        &fixture.commands_dir,
        fixture.workflow_store.clone(),
        &fixture.registry_dir,
    )
    .expect("open_existing without creating files");

    let empty_ledger = reopened
        .read_binding_ledger(BINDING_A)
        .expect("read absent binding ledger");
    assert_eq!(empty_ledger.revision, 0);
    assert!(empty_ledger.commands.is_empty());

    assert!(reopened
        .get_command(
            BINDING_A,
            THREAD_UUID_A,
            "pcm_00000000000000000000000000000000"
        )
        .expect("get_command absent")
        .is_none());
    assert!(reopened
        .get_command_by_key(BINDING_A, THREAD_UUID_A, "req-missing")
        .expect("get_command_by_key absent")
        .is_none());
    assert!(reopened
        .list_commands(BINDING_A, THREAD_UUID_A)
        .expect("list_commands absent")
        .is_empty());

    assert!(
        list_dir_entries(&fixture.commands_dir).is_empty(),
        "readonly queries on absent binding must not create state files or lockfiles"
    );
}

#[test]
fn enqueue_same_key_returns_same_command_changed_body_is_refused_and_grant_budget_stays_unreserved()
{
    let fixture = setup_fixture();
    let now_ms = current_wall_clock_ms().expect("clock");
    let grant_id = create_grant_in_fixture(
        &fixture,
        BINDING_A,
        THREAD_UUID_A,
        "req-grant-a1",
        now_ms.saturating_sub(5_000),
        now_ms + 3_600_000,
    );
    let (_job_id, brief_digest) =
        create_job_in_fixture(&fixture, BINDING_A, THREAD_UUID_A, "req-job-a1");

    let arch_req = sample_architect_request(
        &fixture,
        BINDING_A,
        THREAD_UUID_A,
        &grant_id,
        "cmd-arch-key-1",
        &brief_digest,
        "Should the command ledger validate dependency blobs on enqueue?",
    );
    let enqueued = fixture
        .command_store
        .enqueue_command(&EnqueuePlanningCommandRequest::from_architect(
            arch_req.clone(),
        ))
        .expect("enqueue Architect command");

    assert_eq!(enqueued.status, PlanningCommandStatus::Queued);
    assert!(!enqueued.pending_reconciliation);
    assert_eq!(enqueued.command_key, "cmd-arch-key-1");
    assert!(enqueued.command_id.starts_with("pcm_"));
    assert!(enqueued.service_request_key.starts_with("psr_"));
    assert_eq!(enqueued.payload.request_key(), enqueued.service_request_key);

    // Enqueue must NOT reserve provider call budget or mutate grant usage counters.
    let wf_ledger = fixture
        .workflow_store
        .read_binding_ledger(BINDING_A)
        .expect("read workflow ledger");
    let grant = wf_ledger
        .planning_grants
        .iter()
        .find(|g| g.grant_id == grant_id)
        .expect("grant exists");
    assert_eq!(grant.usage.reserved_total_calls, 0);
    assert_eq!(grant.usage.reserved_architect_calls, 0);
    assert!(grant.reservations.is_empty());

    // Exact duplicate caller key with identical body returns the exact same command without mutating revision.
    let mut retry_arch = arch_req.clone();
    retry_arch.current_time_ms = now_ms + 999;
    let duplicate = fixture
        .command_store
        .enqueue_command(&EnqueuePlanningCommandRequest::from_architect(retry_arch))
        .expect("idempotent duplicate enqueue");
    assert_eq!(duplicate, enqueued);

    let cmd_ledger = fixture
        .command_store
        .read_binding_ledger(BINDING_A)
        .expect("read command ledger");
    assert_eq!(cmd_ledger.revision, 1);
    assert_eq!(cmd_ledger.commands.len(), 1);

    // Same caller key with changed body must be refused.
    let mut changed_arch = arch_req;
    changed_arch.question_summary = "Changed question body with the same caller key".to_string();
    let changed_err = fixture
        .command_store
        .enqueue_command(&EnqueuePlanningCommandRequest::from_architect(changed_arch))
        .expect_err("changed body with same command_key must be refused");
    assert_eq!(changed_err.error_code(), NativeErrorCode::ValidationFailed);

    // Verify Orchestrator payload validation and changed-body refusal.
    let (job_b_id, _) = create_job_in_fixture(&fixture, BINDING_B, THREAD_UUID_B, "req-job-b1");
    let grant_b_id = create_grant_in_fixture(
        &fixture,
        BINDING_B,
        THREAD_UUID_B,
        "req-grant-b1",
        now_ms.saturating_sub(5_000),
        now_ms + 3_600_000,
    );
    let orch_req = OrchestrateWorkBriefPlanRequest {
        binding_id: BINDING_B.to_string(),
        thread_uuid: THREAD_UUID_B.to_string(),
        grant_id: grant_b_id,
        job_id: job_b_id,
        request_key: "cmd-orch-key-1".to_string(),
        expected_ledger_revision: None,
        difficulty: Some(Difficulty::Hard),
        plan_id: None,
        expected_plan_revision: 0,
        architect_advice_ids: Vec::new(),
        max_architect_rounds: 1,
        architect_difficulty: Some(Difficulty::Hard),
        current_time_ms: now_ms,
    };
    let orch_cmd = fixture
        .command_store
        .enqueue_command(&EnqueuePlanningCommandRequest::from_orchestrator(
            orch_req.clone(),
        ))
        .expect("enqueue Orchestrator command");
    assert_eq!(orch_cmd.role, WorkflowRole::Orchestrator);
    assert_eq!(orch_cmd.status, PlanningCommandStatus::Queued);

    let mut changed_orch = orch_req;
    changed_orch.max_architect_rounds = 0;
    let orch_changed_err = fixture
        .command_store
        .enqueue_command(&EnqueuePlanningCommandRequest::from_orchestrator(
            changed_orch,
        ))
        .expect_err("changed Orchestrator body with same key must be refused");
    assert_eq!(
        orch_changed_err.error_code(),
        NativeErrorCode::ValidationFailed
    );
}

#[test]
fn missing_expired_revoked_or_wrong_scope_grants_are_rejected_without_execution() {
    let fixture = setup_fixture();
    let now_ms = current_wall_clock_ms().expect("clock");
    let (_job_id, brief_digest) =
        create_job_in_fixture(&fixture, BINDING_A, THREAD_UUID_A, "req-job-scope");

    // 1. Missing grant is rejected without persisting a command.
    let missing_req = sample_architect_request(
        &fixture,
        BINDING_A,
        THREAD_UUID_A,
        "pgr_00000000000000000000000000000000",
        "cmd-missing-grant",
        &brief_digest,
        "Question under missing grant",
    );
    let missing_err = fixture
        .command_store
        .enqueue_command(&EnqueuePlanningCommandRequest::from_architect(missing_req))
        .expect_err("missing grant must be rejected");
    assert_eq!(missing_err.error_code(), NativeErrorCode::ValidationFailed);
    assert!(fixture
        .command_store
        .list_commands(BINDING_A, THREAD_UUID_A)
        .expect("list commands")
        .is_empty());

    // 2. Expired grant (relative to real local clock) is rejected.
    let expired_grant_id = create_grant_in_fixture(
        &fixture,
        BINDING_A,
        THREAD_UUID_A,
        "req-grant-expired",
        now_ms.saturating_sub(60_000),
        now_ms.saturating_sub(1_000),
    );
    let expired_req = sample_architect_request(
        &fixture,
        BINDING_A,
        THREAD_UUID_A,
        &expired_grant_id,
        "cmd-expired-grant",
        &brief_digest,
        "Question under expired grant",
    );
    let expired_err = fixture
        .command_store
        .enqueue_command(&EnqueuePlanningCommandRequest::from_architect(expired_req))
        .expect_err("expired grant must be rejected");
    assert_eq!(expired_err.error_code(), NativeErrorCode::ValidationFailed);

    // 3. Wrong thread scope and grant revocation reject new enqueues and claim_running.
    let active_grant_id = create_grant_in_fixture(
        &fixture,
        BINDING_A,
        THREAD_UUID_A,
        "req-grant-revocable",
        now_ms.saturating_sub(5_000),
        now_ms + 3_600_000,
    );
    let wrong_thread_req = sample_architect_request(
        &fixture,
        BINDING_A,
        THREAD_UUID_B,
        &active_grant_id,
        "cmd-wrong-thread",
        &brief_digest,
        "Question with mismatched thread_uuid",
    );
    let wrong_thread_err = fixture
        .command_store
        .enqueue_command(&EnqueuePlanningCommandRequest::from_architect(
            wrong_thread_req,
        ))
        .expect_err("wrong thread_uuid must be rejected");
    assert_eq!(
        wrong_thread_err.error_code(),
        NativeErrorCode::ValidationFailed
    );

    let queued_before_revoke = fixture
        .command_store
        .enqueue_command(&EnqueuePlanningCommandRequest::from_architect(
            sample_architect_request(
                &fixture,
                BINDING_A,
                THREAD_UUID_A,
                &active_grant_id,
                "cmd-queued-before-revoke",
                &brief_digest,
                "Question enqueued before revocation",
            ),
        ))
        .expect("enqueue under active grant");

    let revoked = fixture
        .workflow_store
        .revoke_planning_grant(&RevokePlanningGrantRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            grant_id: active_grant_id.clone(),
            expected_ledger_revision: None,
            revoked_at_ms: current_wall_clock_ms().expect("clock"),
            reason: "operator revoked grant before driver claim".to_string(),
        })
        .expect("revoke grant");
    assert_eq!(revoked.status, PlanningGrantStatus::Revoked);

    let post_revoke_enqueue_err = fixture
        .command_store
        .enqueue_command(&EnqueuePlanningCommandRequest::from_architect(
            sample_architect_request(
                &fixture,
                BINDING_A,
                THREAD_UUID_A,
                &active_grant_id,
                "cmd-after-revoke-new-key",
                &brief_digest,
                "Question enqueued after revocation",
            ),
        ))
        .expect_err("revoked grant must reject new command enqueue");
    assert_eq!(
        post_revoke_enqueue_err.error_code(),
        NativeErrorCode::ValidationFailed
    );

    let guard = fixture
        .ownership_store
        .try_acquire_job(
            &queued_before_revoke.ownership_key().expect("key"),
            current_wall_clock_ms().expect("clock"),
        )
        .expect("acquire ownership guard")
        .into_guard()
        .expect("guard acquired");
    let claim_revoked_err = fixture
        .command_store
        .claim_running(
            &guard,
            BINDING_A,
            THREAD_UUID_A,
            &queued_before_revoke.command_id,
            None,
        )
        .expect_err("claim_running must fail when standing grant has been revoked");
    assert_eq!(
        claim_revoked_err.error_code(),
        NativeErrorCode::ValidationFailed
    );
}

#[test]
fn held_ownership_guard_enforces_scope_and_reopen_running_remains_unknown_blocking_replay() {
    let fixture = setup_fixture();
    let now_ms = current_wall_clock_ms().expect("clock");
    let grant_id = create_grant_in_fixture(
        &fixture,
        BINDING_A,
        THREAD_UUID_A,
        "req-grant-own",
        now_ms.saturating_sub(5_000),
        now_ms + 3_600_000,
    );
    let (_job_id, brief_digest) =
        create_job_in_fixture(&fixture, BINDING_A, THREAD_UUID_A, "req-job-own");

    let cmd = fixture
        .command_store
        .enqueue_command(&EnqueuePlanningCommandRequest::from_architect(
            sample_architect_request(
                &fixture,
                BINDING_A,
                THREAD_UUID_A,
                &grant_id,
                "cmd-run-unknown-1",
                &brief_digest,
                "Verify ownership guard exclusivity and crash recovery to Unknown",
            ),
        ))
        .expect("enqueue command");

    // Guard for a different command_id is rejected when claiming `cmd`.
    let wrong_key =
        CoworkerJobOwnershipKey::new(BINDING_A, THREAD_UUID_A, "pcm_wrong_command_scope")
            .expect("wrong key");
    let wrong_guard = fixture
        .ownership_store
        .try_acquire_job(&wrong_key, now_ms)
        .expect("acquire wrong guard")
        .into_guard()
        .expect("wrong guard");
    let wrong_scope_err = fixture
        .command_store
        .claim_running(
            &wrong_guard,
            BINDING_A,
            THREAD_UUID_A,
            &cmd.command_id,
            None,
        )
        .expect_err("mismatched guard scope must be rejected");
    assert_eq!(
        wrong_scope_err.error_code(),
        NativeErrorCode::SecurityViolation
    );

    // Acquire live OS file-lock guard for `cmd.ownership_key()`.
    let cmd_key = cmd.ownership_key().expect("ownership key");
    let guard = fixture
        .ownership_store
        .try_acquire_job(&cmd_key, now_ms)
        .expect("acquire command guard")
        .into_guard()
        .expect("acquired guard");
    assert!(
        fixture
            .ownership_store
            .try_acquire_job(&cmd_key, now_ms)
            .expect("second try_acquire_job")
            .is_busy(),
        "held OS lock must be exclusive"
    );

    let running = fixture
        .command_store
        .claim_running(&guard, BINDING_A, THREAD_UUID_A, &cmd.command_id, Some(1))
        .expect("claim_running with valid guard");
    assert_eq!(running.status, PlanningCommandStatus::Running);
    assert_eq!(
        running
            .owner_summary
            .as_ref()
            .map(|s| s.instance_id.as_str()),
        Some(guard.instance_id())
    );

    // Completing without a real service outcome in CoworkerWorkflowStore is rejected.
    let unverified_complete_err = fixture
        .command_store
        .record_completed(
            &guard,
            BINDING_A,
            THREAD_UUID_A,
            &cmd.command_id,
            PlanningCommandResultRef::ArchitectAdvice {
                advice_id: "adv_00000000000000000000000000000000".to_string(),
                reservation_id: "prs_00000000000000000000000000000000".to_string(),
                report_artifact_id: "pln_00000000000000000000000000000000".to_string(),
                context_digest: cmd.context_digest.clone(),
                decision_digest: sha256_hex(b"unverified"),
                reused_existing: false,
                artifact_verified: false,
            },
            None,
        )
        .expect_err("Completing without real service record in workflow store must fail");
    assert_eq!(
        unverified_complete_err.error_code(),
        NativeErrorCode::ValidationFailed
    );

    // Drop guard while still `Running`, then reopen the store.
    drop(guard);

    let reopened = PlanningCommandStore::open_existing(
        &fixture.commands_dir,
        fixture.workflow_store.clone(),
        &fixture.registry_dir,
    )
    .expect("reopen PlanningCommandStore");

    let after_reopen = reopened
        .get_command(BINDING_A, THREAD_UUID_A, &cmd.command_id)
        .expect("get_command after reopen")
        .expect("command exists");
    assert_eq!(after_reopen.status, PlanningCommandStatus::Unknown);
    assert!(after_reopen.pending_reconciliation);
    assert!(after_reopen.diagnostic.is_some());

    // Attempting to re-claim the command with a newly acquired guard fails and keeps it Unknown.
    let new_guard = reopened
        .ownership_store()
        .try_acquire_job(&cmd_key, current_wall_clock_ms().expect("clock"))
        .expect("acquire new guard after drop")
        .into_guard()
        .expect("new guard");
    let reclaim_err = reopened
        .claim_running(&new_guard, BINDING_A, THREAD_UUID_A, &cmd.command_id, None)
        .expect_err("Unknown command must not be claimed again");
    assert_eq!(
        reclaim_err.error_code(),
        NativeErrorCode::AmbiguousWakeupPendingReconciliation
    );

    // Attempting to bypass the Unknown command with a different command_key on the binding fails.
    let bypass_err = reopened
        .enqueue_command(&EnqueuePlanningCommandRequest::from_architect(
            sample_architect_request(
                &fixture,
                BINDING_A,
                THREAD_UUID_A,
                &grant_id,
                "cmd-run-unknown-bypass-key-2",
                &brief_digest,
                "Attempt to bypass Unknown command with a new key",
            ),
        ))
        .expect_err("new key must be blocked while Unknown command is pending reconciliation");
    assert_eq!(
        bypass_err.error_code(),
        NativeErrorCode::AmbiguousWakeupPendingReconciliation
    );

    // Same-key replay returns the persisted Unknown command without resetting or creating a second record.
    let same_key_unknown = reopened
        .enqueue_command(&EnqueuePlanningCommandRequest::from_architect(
            sample_architect_request(
                &fixture,
                BINDING_A,
                THREAD_UUID_A,
                &grant_id,
                "cmd-run-unknown-1",
                &brief_digest,
                "Verify ownership guard exclusivity and crash recovery to Unknown",
            ),
        ))
        .expect("same-key lookup on Unknown command");
    assert_eq!(same_key_unknown.command_id, cmd.command_id);
    assert_eq!(same_key_unknown.status, PlanningCommandStatus::Unknown);
    assert!(same_key_unknown.pending_reconciliation);
}

#[test]
fn claim_running_refuses_superseded_job_or_drifted_context_before_running() {
    let fixture = setup_fixture();
    let now_ms = current_wall_clock_ms().expect("clock");
    let grant_id = create_grant_in_fixture(
        &fixture,
        BINDING_A,
        THREAD_UUID_A,
        "req-grant-drift",
        now_ms.saturating_sub(5_000),
        now_ms + 3_600_000,
    );
    let (job_id, _brief_digest) =
        create_job_in_fixture(&fixture, BINDING_A, THREAD_UUID_A, "req-job-drift-base");

    let orch_cmd = fixture
        .command_store
        .enqueue_command(&EnqueuePlanningCommandRequest::from_orchestrator(
            OrchestrateWorkBriefPlanRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                grant_id: grant_id.clone(),
                job_id: job_id.clone(),
                request_key: "cmd-orch-superseded-before-claim".to_string(),
                expected_ledger_revision: None,
                difficulty: Some(Difficulty::Hard),
                plan_id: None,
                expected_plan_revision: 0,
                architect_advice_ids: Vec::new(),
                max_architect_rounds: 0,
                architect_difficulty: None,
                current_time_ms: now_ms,
            },
        ))
        .expect("enqueue Orchestrator command");
    assert_eq!(orch_cmd.status, PlanningCommandStatus::Queued);

    // Amend and supersede the target job before the command is claimed for Running.
    let amend_brief = WorkBrief {
        brief_id: "brf_amend_supersede".to_string(),
        binding_id: BINDING_A.to_string(),
        thread_uuid: THREAD_UUID_A.to_string(),
        project_base: fixture.project_base.clone(),
        kind: WorkBriefKind::Amendment {
            target_job_id: job_id,
            target_job_revision: 1,
            amendment_reason: "Updated scope before planning claim".to_string(),
        },
        summary: "Amended work brief superseding queued planning job".to_string(),
        confirmed_requirements: vec!["Re-verify context before Running transition".to_string()],
        proposed_additions: Vec::new(),
        assumptions: Vec::new(),
        open_questions: Vec::new(),
        accepted_architect_decisions: Vec::new(),
    };
    fixture
        .workflow_store
        .create_or_amend_job(&CreateCoworkerJobRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            request_key: "req-job-amend-1".to_string(),
            expected_ledger_revision: None,
            work_brief: amend_brief,
            amended_target_task_ids: Vec::new(),
            amended_write_paths: Vec::new(),
            amended_interfaces: Vec::new(),
        })
        .expect("create superseding amendment job");

    let guard = fixture
        .ownership_store
        .try_acquire_job(
            &orch_cmd.ownership_key().expect("ownership key"),
            current_wall_clock_ms().expect("clock"),
        )
        .expect("acquire ownership guard")
        .into_guard()
        .expect("guard");
    let claim_err = fixture
        .command_store
        .claim_running(&guard, BINDING_A, THREAD_UUID_A, &orch_cmd.command_id, None)
        .expect_err("claim_running must refuse superseded job before Running");
    assert_eq!(claim_err.error_code(), NativeErrorCode::ValidationFailed);

    let persisted = fixture
        .command_store
        .get_command(BINDING_A, THREAD_UUID_A, &orch_cmd.command_id)
        .expect("get_command")
        .expect("command exists");
    assert_eq!(persisted.status, PlanningCommandStatus::Queued);
}

#[tokio::test]
async fn record_completed_verifies_real_service_artifact_and_rejects_other_question_or_missing_artifacts_dir(
) {
    let fixture = setup_fixture();
    let now_ms = current_wall_clock_ms().expect("wall clock");
    let grant_id = create_grant_in_fixture(
        &fixture,
        BINDING_A,
        THREAD_UUID_A,
        "req-grant-complete-1",
        now_ms.saturating_sub(5_000),
        now_ms + 3_600_000,
    );
    let (_job_id, brief_digest) =
        create_job_in_fixture(&fixture, BINDING_A, THREAD_UUID_A, "req-job-complete-1");

    let artifacts_dir = fixture._tmp.path().join("planning-artifacts");
    let service = CoworkerPlanningService::new(
        fixture.workflow_store.clone(),
        fixture.registry_dir.clone(),
        artifacts_dir.clone(),
    )
    .expect("create planning service");
    let store_with_artifacts = PlanningCommandStore::new(
        fixture.commands_dir.clone(),
        fixture.workflow_store.clone(),
        fixture.registry_dir.clone(),
        fixture.ownership_store.clone(),
    )
    .expect("create command store")
    .with_artifacts_dir(artifacts_dir)
    .expect("with_artifacts_dir");

    let arch_req_q1 = sample_architect_request(
        &fixture,
        BINDING_A,
        THREAD_UUID_A,
        &grant_id,
        "req-cmd-complete-q1",
        &brief_digest,
        "Question A: how should durable command completion verify persisted artifacts?",
    );
    let cmd_q1 = store_with_artifacts
        .enqueue_command(&EnqueuePlanningCommandRequest::from_architect(
            arch_req_q1.clone(),
        ))
        .expect("enqueue command q1");
    let guard = fixture
        .ownership_store
        .try_acquire_job(
            &cmd_q1.ownership_key().expect("ownership key"),
            current_wall_clock_ms().expect("clock"),
        )
        .expect("acquire ownership guard")
        .into_guard()
        .expect("guard");
    let running_q1 = store_with_artifacts
        .claim_running(&guard, BINDING_A, THREAD_UUID_A, &cmd_q1.command_id, None)
        .expect("claim_running q1");

    // Execute a different Architect question on the exact same context dependencies.
    let mut other_q_req = arch_req_q1.clone();
    other_q_req.request_key = "req-other-question-direct".to_string();
    other_q_req.question_summary =
        "Question B: unrelated question on identical repository context dependencies?".to_string();
    let other_question_outcome = service
        .obtain_architect_advice(&other_q_req)
        .await
        .expect("obtain architect advice for question B");

    let wrong_question_ref = PlanningCommandResultRef::ArchitectAdvice {
        advice_id: other_question_outcome.record.advice_id.clone(),
        reservation_id: other_question_outcome.reservation_id.clone(),
        report_artifact_id: other_question_outcome
            .record
            .report_artifact_id
            .clone()
            .expect("report_artifact_id"),
        context_digest: other_question_outcome.record.context_digest.clone(),
        decision_digest: other_question_outcome.record.decision_digest.clone(),
        reused_existing: other_question_outcome.reused_existing,
        artifact_verified: false,
    };

    // Completing Question A's command with Question B's advice/artifact must fail.
    let wrong_q_err = store_with_artifacts
        .record_completed(
            &guard,
            BINDING_A,
            THREAD_UUID_A,
            &cmd_q1.command_id,
            wrong_question_ref,
            None,
        )
        .expect_err("must reject advice produced for a different question_summary");
    assert_eq!(wrong_q_err.error_code(), NativeErrorCode::ValidationFailed);

    // Execute the exact frozen command payload for Question A via the real planning service.
    let frozen_arch_req = match &running_q1.payload {
        PlanningCommandPayload::Architect(req) => req.clone(),
        other => panic!("expected Architect payload, got {other:?}"),
    };
    let valid_outcome = service
        .obtain_architect_advice(&frozen_arch_req)
        .await
        .expect("obtain architect advice for frozen command payload");
    assert!(!valid_outcome.reused_existing);

    let valid_result_ref = PlanningCommandResultRef::ArchitectAdvice {
        advice_id: valid_outcome.record.advice_id.clone(),
        reservation_id: valid_outcome.reservation_id.clone(),
        report_artifact_id: valid_outcome
            .record
            .report_artifact_id
            .clone()
            .expect("report_artifact_id"),
        context_digest: valid_outcome.record.context_digest.clone(),
        decision_digest: valid_outcome.record.decision_digest.clone(),
        reused_existing: valid_outcome.reused_existing,
        artifact_verified: false,
    };

    // Without artifacts_dir configured, record_completed must refuse even a valid result ref.
    let no_artifacts_err = fixture
        .command_store
        .record_completed(
            &guard,
            BINDING_A,
            THREAD_UUID_A,
            &cmd_q1.command_id,
            valid_result_ref.clone(),
            None,
        )
        .expect_err("record_completed without configured artifacts_dir must fail");
    assert_eq!(
        no_artifacts_err.error_code(),
        NativeErrorCode::ValidationFailed
    );

    // With artifacts_dir configured, record_completed verifies the real artifact and sets artifact_verified = true.
    let completed = store_with_artifacts
        .record_completed(
            &guard,
            BINDING_A,
            THREAD_UUID_A,
            &cmd_q1.command_id,
            valid_result_ref,
            None,
        )
        .expect("record_completed with verified service artifact");
    assert_eq!(completed.status, PlanningCommandStatus::Completed);
    assert!(completed
        .result_ref
        .as_ref()
        .expect("result_ref")
        .is_artifact_verified());
}

fn write_orchestrator_plan_stub(path: &Path) {
    use std::io::Write;
    use std::os::unix::fs::PermissionsExt;

    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).expect("create stub parent");
    }
    let script = r#"#!/bin/sh
set -eu
printf '{"event":"init","init":{"session_id":"agy-orch-cmd-sess-1","model":"gemini-4-argon-high"}}\n'
RESPONSE_JSON='{\"outcome\":\"plan\",\"schema_version\":1,\"summary\":\"Implement planning command runner slice\",\"tasks\":[{\"task_id\":\"task_runner_core\",\"title\":\"Implement planning command driver\",\"description\":\"Execute one queued command under live ownership guard.\",\"role\":\"coder\",\"difficulty\":\"hard\",\"depends_on\":[],\"allowed_write_paths\":[\"src/lib.rs\"],\"owned_interfaces\":[\"iface_planning_driver\"],\"consumed_interfaces\":[]}],\"check_suggestions\":[{\"check_id\":\"cargo_test_planning_commands\",\"command_summary\":\"cargo test -p threadlane-native --test planning_commands\",\"required\":true}],\"unconfirmed_proposal_notes\":[],\"resolved_open_questions\":[],\"deferred_proposals\":[]}'
printf '{"event":"result","result":{"status":"SUCCESS","response":"%s"}}\n' "$RESPONSE_JSON"
exit 0
"#;
    let tmp_path = path.with_extension("tmp");
    let mut file = fs::File::create(&tmp_path).expect("create orchestrator stub");
    file.write_all(script.as_bytes())
        .expect("write orchestrator stub");
    file.sync_all().expect("sync orchestrator stub");
    drop(file);
    fs::set_permissions(&tmp_path, fs::Permissions::from_mode(0o755))
        .expect("chmod 0755 orchestrator stub");
    fs::rename(&tmp_path, path).expect("replace orchestrator stub atomically");
}

#[tokio::test]
async fn driver_executes_queued_architect_command_and_reopen_reexecution_reuses_completed_without_second_call(
) {
    let fixture = setup_fixture();
    let repo_dir = std::path::Path::new(&fixture.project_base.repository_path);
    let exclude_path = repo_dir.join(".git/info/exclude");
    std::fs::create_dir_all(exclude_path.parent().expect("exclude parent"))
        .expect("create .git/info dir");
    std::fs::write(&exclude_path, "private-cache/\n").expect("write .git/info/exclude");
    let ignored_marker = repo_dir.join("private-cache/marker");
    std::fs::create_dir_all(ignored_marker.parent().expect("marker parent"))
        .expect("create private-cache dir");
    std::fs::write(&ignored_marker, b"preserved-ignored-cache").expect("write ignored marker");
    let now_ms = current_wall_clock_ms().expect("wall clock");
    let grant_id = create_grant_in_fixture(
        &fixture,
        BINDING_A,
        THREAD_UUID_A,
        "req-grant-driver-arch-1",
        now_ms.saturating_sub(5_000),
        now_ms + 3_600_000,
    );
    let (_job_id, brief_digest) =
        create_job_in_fixture(&fixture, BINDING_A, THREAD_UUID_A, "req-job-driver-arch-1");

    let artifacts_dir = fixture._tmp.path().join("planning-artifacts");
    let service = CoworkerPlanningService::new(
        fixture.workflow_store.clone(),
        fixture.registry_dir.clone(),
        artifacts_dir,
    )
    .expect("create planning service");
    let store_with_artifacts = fixture
        .command_store
        .clone()
        .with_artifacts_dir(service.artifacts_dir())
        .expect("with_artifacts_dir");
    let driver = PlanningCommandDriver::new(store_with_artifacts.clone(), service.clone())
        .expect("create PlanningCommandDriver");

    let arch_cmd = store_with_artifacts
        .enqueue_command(&EnqueuePlanningCommandRequest::from_architect(
            sample_architect_request(
                &fixture,
                BINDING_A,
                THREAD_UUID_A,
                &grant_id,
                "cmd-driver-arch-1",
                &brief_digest,
                "How should PlanningCommandDriver execute one queued Architect command?",
            ),
        ))
        .expect("enqueue Architect command");
    assert_eq!(arch_cmd.status, PlanningCommandStatus::Queued);

    let outcome_1 = driver
        .execute_command(BINDING_A, THREAD_UUID_A, &arch_cmd.command_id)
        .await
        .expect("execute queued Architect command");
    assert!(!outcome_1.was_already_completed);
    assert!(!outcome_1.reconciled_from_existing);
    assert_eq!(outcome_1.command.status, PlanningCommandStatus::Completed);
    assert_eq!(
        std::fs::read(&ignored_marker).expect("read ignored marker after planning command"),
        b"preserved-ignored-cache"
    );
    assert!(!outcome_1.command.pending_reconciliation);

    let (advice_id, reservation_id, report_artifact_id, context_digest, decision_digest) =
        match outcome_1.command.result_ref.as_ref().expect("result_ref") {
            PlanningCommandResultRef::ArchitectAdvice {
                advice_id,
                reservation_id,
                report_artifact_id,
                context_digest,
                decision_digest,
                reused_existing,
                artifact_verified,
            } => {
                assert!(!reused_existing);
                assert!(artifact_verified);
                (
                    advice_id.clone(),
                    reservation_id.clone(),
                    report_artifact_id.clone(),
                    context_digest.clone(),
                    decision_digest.clone(),
                )
            }
            other => panic!("expected ArchitectAdvice result_ref, got {other:?}"),
        };

    let wf_ledger = fixture
        .workflow_store
        .read_binding_ledger(BINDING_A)
        .expect("read workflow ledger");
    assert_eq!(wf_ledger.architect_advice.len(), 1);
    let stored_adv = &wf_ledger.architect_advice[0];
    assert_eq!(stored_adv.advice_id, advice_id);
    assert_eq!(stored_adv.reservation_id, reservation_id);
    assert_eq!(
        stored_adv.report_artifact_id.as_deref(),
        Some(report_artifact_id.as_str())
    );
    assert_eq!(stored_adv.request_key, arch_cmd.service_request_key);
    assert_eq!(stored_adv.context_digest, context_digest);
    assert_eq!(stored_adv.decision_digest, decision_digest);

    let grant = wf_ledger
        .planning_grants
        .iter()
        .find(|g| g.grant_id == grant_id)
        .expect("grant in ledger");
    assert_eq!(grant.usage.reserved_total_calls, 1);
    assert_eq!(grant.usage.reserved_architect_calls, 1);
    assert_eq!(grant.reservations.len(), 1);
    assert_eq!(grant.reservations[0].reservation_id, reservation_id);
    assert_eq!(
        grant.reservations[0].request_key,
        arch_cmd.service_request_key
    );

    let artifact_path = service
        .artifacts_dir()
        .join(format!("{report_artifact_id}.json"));
    let artifact_bytes = fs::read(&artifact_path).expect("read persisted private artifact");
    let artifact: PersistedPlanningCallArtifact =
        serde_json::from_slice(&artifact_bytes).expect("parse PersistedPlanningCallArtifact");
    artifact
        .validate_against_grant(&report_artifact_id, grant)
        .expect("validate persisted artifact against grant");
    assert_eq!(artifact.reservation_id, reservation_id);
    assert_eq!(artifact.request_key, arch_cmd.service_request_key);

    // Reopen store and driver, then repeat the same command: must return Completed with no second call.
    let reopened_store = PlanningCommandStore::open_existing(
        &fixture.commands_dir,
        fixture.workflow_store.clone(),
        &fixture.registry_dir,
    )
    .expect("reopen PlanningCommandStore")
    .with_artifacts_dir(service.artifacts_dir())
    .expect("with_artifacts_dir on reopened store");
    let reopened_driver = PlanningCommandDriver::new(reopened_store, service)
        .expect("create reopened PlanningCommandDriver");

    let outcome_2 = reopened_driver
        .execute_command(BINDING_A, THREAD_UUID_A, &arch_cmd.command_id)
        .await
        .expect("re-execute Completed Architect command");
    assert!(outcome_2.was_already_completed);
    assert!(!outcome_2.reconciled_from_existing);
    assert_eq!(outcome_2.command.result_ref, outcome_1.command.result_ref);

    let wf_ledger_after = fixture
        .workflow_store
        .read_binding_ledger(BINDING_A)
        .expect("read workflow ledger after re-execution");
    let grant_after = wf_ledger_after
        .planning_grants
        .iter()
        .find(|g| g.grant_id == grant_id)
        .expect("grant after re-execution");
    assert_eq!(grant_after.usage.reserved_total_calls, 1);
    assert_eq!(grant_after.usage.reserved_architect_calls, 1);
    assert_eq!(grant_after.reservations.len(), 1);
    assert_eq!(wf_ledger_after.architect_advice.len(), 1);
}

#[tokio::test]
async fn driver_executes_queued_orchestrator_command_publishing_plan_dag_and_reexecution_is_idempotent(
) {
    let fixture = setup_fixture();
    let root = fs::canonicalize(fixture._tmp.path()).expect("canonicalize temp root");
    write_orchestrator_plan_stub(&root.join("bin/fake-agy"));

    let registry = RegistryStore::open(&fixture.registry_dir).expect("open registry");
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
                    ],
                },
            },
        )
        .expect("upsert profile-coworker with Coder role assignment at revision 4");

    let now_ms = current_wall_clock_ms().expect("wall clock");
    let grant_id = fixture
        .workflow_store
        .create_planning_grant(&CreatePlanningGrantRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            request_key: "req-grant-driver-orch-1".to_string(),
            expected_ledger_revision: None,
            project_base: fixture.project_base.clone(),
            profile_id: "profile-coworker".to_string(),
            registry_revision: 4,
            policy: default_policy(),
            capabilities: PlanningReadOnlyCapabilities::strict_read_only(),
            budget: default_budget(),
            created_at_ms: now_ms.saturating_sub(5_000),
            expires_at_ms: now_ms + 3_600_000,
            operator_attested: true,
        })
        .expect("create planning grant at registry_revision 4")
        .grant_id;
    let (job_id, brief_digest) =
        create_job_in_fixture(&fixture, BINDING_A, THREAD_UUID_A, "req-job-driver-orch-1");

    let artifacts_dir = fixture._tmp.path().join("planning-artifacts-orch");
    let service = CoworkerPlanningService::new(
        fixture.workflow_store.clone(),
        fixture.registry_dir.clone(),
        artifacts_dir,
    )
    .expect("create planning service");
    let store_with_artifacts = fixture
        .command_store
        .clone()
        .with_artifacts_dir(service.artifacts_dir())
        .expect("with_artifacts_dir");
    let driver = PlanningCommandDriver::new(store_with_artifacts.clone(), service.clone())
        .expect("create PlanningCommandDriver");

    let orch_cmd = store_with_artifacts
        .enqueue_command(&EnqueuePlanningCommandRequest::from_orchestrator(
            OrchestrateWorkBriefPlanRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                grant_id: grant_id.clone(),
                job_id: job_id.clone(),
                request_key: "cmd-driver-orch-1".to_string(),
                expected_ledger_revision: None,
                difficulty: Some(Difficulty::Hard),
                plan_id: None,
                expected_plan_revision: 0,
                architect_advice_ids: Vec::new(),
                max_architect_rounds: 0,
                architect_difficulty: None,
                current_time_ms: now_ms,
            },
        ))
        .expect("enqueue Orchestrator command");
    assert_eq!(orch_cmd.status, PlanningCommandStatus::Queued);

    let outcome_1 = driver
        .execute_command(BINDING_A, THREAD_UUID_A, &orch_cmd.command_id)
        .await
        .expect("execute queued Orchestrator command");
    assert!(!outcome_1.was_already_completed);
    assert!(!outcome_1.reconciled_from_existing);
    assert!(outcome_1.orchestrator_outcome.is_some());
    assert_eq!(outcome_1.command.status, PlanningCommandStatus::Completed);

    let planning_result = match outcome_1.command.result_ref.as_ref().expect("result_ref") {
        PlanningCommandResultRef::OrchestratorPlan {
            job_id: ref_job_id,
            planning_result,
            artifact_verified,
        } => {
            assert_eq!(ref_job_id, &job_id);
            assert!(artifact_verified);
            planning_result.as_ref().clone()
        }
        other => panic!("expected OrchestratorPlan result_ref, got {other:?}"),
    };
    assert_eq!(planning_result.grant_id, grant_id);
    assert_eq!(planning_result.request_key, orch_cmd.service_request_key);
    assert_eq!(planning_result.work_brief_digest, brief_digest);
    assert_eq!(planning_result.project_base, fixture.project_base);
    assert_eq!(planning_result.profile_id, "profile-coworker");
    assert_eq!(planning_result.registry_revision, 4);
    assert_eq!(planning_result.plan_revision, 1);
    assert_eq!(planning_result.plan_status, PlanStatus::Published);

    let wf_ledger = fixture
        .workflow_store
        .read_binding_ledger(BINDING_A)
        .expect("read workflow ledger");
    let stored_job = wf_ledger
        .jobs
        .iter()
        .find(|j| j.job_id == job_id)
        .expect("job in ledger");
    assert_eq!(stored_job.planning_result.as_ref(), Some(&planning_result));
    let grant = wf_ledger
        .planning_grants
        .iter()
        .find(|g| g.grant_id == grant_id)
        .expect("grant in ledger");
    assert_eq!(grant.usage.reserved_total_calls, 1);
    assert_eq!(grant.usage.reserved_orchestrator_calls, 1);
    assert_eq!(grant.reservations.len(), 1);

    let (reg_arc, plans_arc) = open_workflow_stores(
        Some(&fixture.registry_dir),
        Some(fixture.workflow_store.plans_dir()),
    )
    .expect("open workflow stores")
    .expect("workflow stores configured");
    let scoped =
        ScopedWorkflow::new(reg_arc, plans_arc, BINDING_A.to_string()).expect("ScopedWorkflow");
    let plan_page = scoped
        .get_plan(
            &planning_result.plan_id,
            Some(planning_result.plan_revision),
            None,
            None,
        )
        .expect("get published plan from PlanStore");
    assert_eq!(plan_page.header.plan_id, planning_result.plan_id);
    assert_eq!(plan_page.header.binding_id, BINDING_A);
    assert_eq!(plan_page.header.revision, 1);
    assert_eq!(plan_page.header.status, PlanStatus::Published);
    assert_eq!(
        plan_page.header.plan_digest.as_deref(),
        Some(planning_result.plan_digest.as_str())
    );
    assert_eq!(plan_page.tasks.len(), 1);
    assert_eq!(plan_page.tasks[0].id, "task_runner_core");
    assert_eq!(plan_page.tasks[0].role, WorkflowRole::Coder);
    assert_eq!(plan_page.tasks[0].difficulty, Difficulty::Hard);
    assert_eq!(
        plan_page.tasks[0].allowed_write_paths,
        vec!["src/lib.rs".to_string()]
    );

    // Re-executing the completed Orchestrator command must not invoke another call or create revision 2.
    let outcome_2 = driver
        .execute_command(BINDING_A, THREAD_UUID_A, &orch_cmd.command_id)
        .await
        .expect("re-execute Completed Orchestrator command");
    assert!(outcome_2.was_already_completed);
    assert!(!outcome_2.reconciled_from_existing);
    assert!(outcome_2.orchestrator_outcome.is_none());
    assert_eq!(outcome_2.command.result_ref, outcome_1.command.result_ref);

    let wf_ledger_after = fixture
        .workflow_store
        .read_binding_ledger(BINDING_A)
        .expect("read workflow ledger after re-execution");
    let grant_after = wf_ledger_after
        .planning_grants
        .iter()
        .find(|g| g.grant_id == grant_id)
        .expect("grant after re-execution");
    assert_eq!(grant_after.usage.reserved_total_calls, 1);
    assert_eq!(grant_after.usage.reserved_orchestrator_calls, 1);
    assert_eq!(grant_after.reservations.len(), 1);

    let latest_page = scoped
        .get_plan(&planning_result.plan_id, None, None, None)
        .expect("get latest plan revision");
    assert_eq!(latest_page.header.revision, 1);
}
