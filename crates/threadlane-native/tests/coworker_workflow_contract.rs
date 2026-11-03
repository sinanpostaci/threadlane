use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use tempfile::TempDir;
use threadlane_authority::{AuthorityStore, OperatorApprovalRequest};
use threadlane_native::coworker_workflow::{
    evaluate_declared_task_independence, prove_task_independence, sha256_hex,
    validate_non_native_worker_choice, validate_write_scope_at_project_base,
    AcceptedPredecessorReceiptRef, ApplyWorkflowHoldRequest, ArchitectContextDependencies,
    ArchitectDecisionStatus, BindJobApprovalRequest, CandidateCheckEvidence,
    CanonicalProjectBaseRef, CombinedCandidateStatus, CoworkerBindingLedger, CoworkerJobPhase,
    CoworkerTaskStage, CoworkerWorkflowStore, CreateCoworkerJobRequest, CreatePlanningGrantRequest,
    FinalizePlanningCallRequest, FinalizeTaskOutboxRequest, FrozenControllerCapabilityEnvelope,
    OutboxDispatchKind, OutboxDispatchStatus, PlanningCallBudget, PlanningGrantStatus,
    PlanningReadOnlyCapabilities, PlanningReservationStatus, RecordArchitectAdviceRequest,
    RecordCombinedCandidateRequest, RecordTaskReviewVerdictRequest, ReservePlanningCallRequest,
    ReserveTaskOutboxRequest, ResolveWorkflowHoldRequest, ReviewVerdictOutcome,
    RevokePlanningGrantRequest, StructuredReviewVerdict, TaskCandidateRef,
    TaskInterfaceOwnershipSpec, WorkBrief, WorkBriefKind, WorkflowHoldReason,
    COWORKER_IS_CONVERSATIONAL_INTERFACE_ONLY, COWORKER_WORKFLOW_SCHEMA_VERSION,
};
use threadlane_native::{
    Difficulty, ExecutionPolicy, NativeErrorCode, PolicyAllowRule, RegistryStore,
    RoleExecutionRoute, SelectedRoleChoice, ToolAdapter, WorkflowProfile, WorkflowRole,
};
use threadlane_workers::{
    resolve_exact_commit_sha, resolve_exact_tree_sha, run_sanitized_git, ExecutionBudgets,
    DEFAULT_GIT_COMMAND_TIMEOUT_MS,
};
use threadlane_workflow::{
    compile_plan_revision, ModelChoice, ModelDefinition, PlanChange, PlanStore, PlanTask,
    RegistryChange, RoleAssignment, ToolDefinition,
};

const THREAD_UUID_A: &str = "550e8400-e29b-41d4-a716-446655440000";
const THREAD_UUID_B: &str = "6ba7b810-9dad-11d1-80b4-00c04fd430c8";
const BINDING_A: &str = "bnd_coworker_alpha";
const BINDING_B: &str = "bnd_coworker_beta";

struct CoworkerTestFixture {
    _tmp: TempDir,
    repo_dir: PathBuf,
    base_commit: String,
    base_tree_sha: String,
    registry: Arc<RegistryStore>,
    plans: Arc<PlanStore>,
    authority: Arc<AuthorityStore>,
    store: CoworkerWorkflowStore,
}

fn init_git_repo(repo_dir: &Path) -> (String, String) {
    fs::create_dir_all(repo_dir.join("src")).expect("create repo src");
    fs::create_dir_all(repo_dir.join("docs")).expect("create repo docs");
    fs::write(repo_dir.join("src/lib.rs"), "pub fn alpha() -> u32 { 1 }\n")
        .expect("write src/lib.rs");
    fs::write(
        repo_dir.join("src/extra.rs"),
        "pub fn extra() -> u32 { 2 }\n",
    )
    .expect("write src/extra.rs");
    fs::write(repo_dir.join("docs/guide.md"), "# Guide\n").expect("write docs/guide.md");

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

fn write_executable_stub(path: &Path) {
    use std::io::Write;
    use std::os::unix::fs::PermissionsExt;

    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).expect("create stub parent");
    }
    let mut file = fs::File::create(path).expect("create stub");
    file.write_all(b"#!/bin/sh\nexit 0\n").expect("write stub");
    file.sync_all().expect("sync stub");
    fs::set_permissions(path, fs::Permissions::from_mode(0o755)).expect("chmod 0755 stub");
}

fn setup_fixture() -> CoworkerTestFixture {
    let tmp = TempDir::new().expect("tempdir");
    let root = fs::canonicalize(tmp.path()).expect("canonicalize temp root");

    let repo_dir = root.join("repo");
    let (base_commit, base_tree_sha) = init_git_repo(&repo_dir);

    let bin_dir = root.join("bin");
    let fake_agy = bin_dir.join("fake-agy");
    let fake_oc = bin_dir.join("fake-opencode");
    write_executable_stub(&fake_agy);
    write_executable_stub(&fake_oc);

    let registry_dir = root.join("registry-store");
    let plans_dir = root.join("plans-store");
    let authority_dir = root.join("authority-store");

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
                    executable_path: Some(fake_agy),
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
                    id: "tool-oc".to_string(),
                    label: "OpenCode v2 CLI".to_string(),
                    adapter: ToolAdapter::OpencodeV2,
                    executable_path: Some(fake_oc),
                    enabled: true,
                },
            },
        )
        .expect("upsert tool-oc");

    registry
        .apply(
            2,
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
            3,
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
                        id: "model-deepseek".to_string(),
                        tool_id: "tool-oc".to_string(),
                        label: "DeepSeek v4.1 Flash".to_string(),
                        model_arg: "opencode-go/deepseek-v4.1-flash".to_string(),
                        efforts: vec!["max".to_string()],
                        allow_no_effort: false,
                    },
                    ModelDefinition {
                        id: "model-codex-native".to_string(),
                        tool_id: "tool-codex".to_string(),
                        label: "Native Codex Conversational Interface".to_string(),
                        model_arg: "gpt-6-astra".to_string(),
                        efforts: vec!["high".to_string(), "max".to_string()],
                        allow_no_effort: true,
                    },
                ],
            },
        )
        .expect("upsert models");

    registry
        .apply(
            4,
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
                            role: WorkflowRole::Coder,
                            difficulty: Some(Difficulty::Easy),
                            choice: ModelChoice {
                                model_id: "model-deepseek".to_string(),
                                effort: Some("max".to_string()),
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
    let store = CoworkerWorkflowStore::new(&plans_dir).expect("open CoworkerWorkflowStore");

    CoworkerTestFixture {
        _tmp: tmp,
        repo_dir,
        base_commit,
        base_tree_sha,
        registry,
        plans,
        authority,
        store,
    }
}

fn project_base(fixture: &CoworkerTestFixture) -> CanonicalProjectBaseRef {
    CanonicalProjectBaseRef::resolve_and_validate(&fixture.repo_dir, &fixture.base_commit)
        .expect("resolve canonical project_base")
}

fn headless_argon_choice() -> SelectedRoleChoice {
    SelectedRoleChoice {
        tool_id: "tool-agy".to_string(),
        adapter: ToolAdapter::Antigravity,
        route: RoleExecutionRoute::HeadlessWorker,
        model_id: "model-argon".to_string(),
        model_arg: "gemini-4-argon-high".to_string(),
        effort: Some("high".to_string()),
        matched_difficulty: Some(Difficulty::Hard),
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
        allow_rules: vec![
            PolicyAllowRule {
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
            },
            PolicyAllowRule {
                tool_id: "tool-oc".to_string(),
                model_id: "model-deepseek".to_string(),
                efforts: vec!["max".to_string()],
                allow_no_effort: false,
                roles: vec![WorkflowRole::Coder],
            },
        ],
    }
}

fn sample_ready_brief(
    fixture: &CoworkerTestFixture,
    brief_id: &str,
    binding_id: &str,
    thread_uuid: &str,
) -> WorkBrief {
    WorkBrief {
        brief_id: brief_id.to_string(),
        binding_id: binding_id.to_string(),
        thread_uuid: thread_uuid.to_string(),
        project_base: project_base(fixture),
        kind: WorkBriefKind::NewJob,
        summary: "Implement and review Coworker foundation".to_string(),
        confirmed_requirements: vec![
            "Separate planning grants from execution approvals".to_string(),
            "Require structured review verdicts".to_string(),
        ],
        proposed_additions: Vec::new(),
        assumptions: vec!["Trusted-local Linux workspace".to_string()],
        open_questions: Vec::new(),
        accepted_architect_decisions: vec!["adv_0123456789abcdef0123456789abcdef".to_string()],
    }
}

fn publish_and_approve_plan(
    fixture: &CoworkerTestFixture,
    plan_id: &str,
    binding_id: &str,
    thread_uuid: &str,
    tasks: Vec<PlanTask>,
    approved_write_paths: Vec<String>,
) -> String {
    let reg_snap = fixture.registry.snapshot().expect("registry snapshot");
    let latest_rev = fixture
        .plans
        .latest_revision(plan_id)
        .expect("query latest plan revision")
        .map(|v| v.revision)
        .unwrap_or(0);

    let draft = fixture
        .plans
        .change_at(
            latest_rev,
            PlanChange::SaveDraft {
                plan_id: plan_id.to_string(),
                binding_id: binding_id.to_string(),
                profile_id: "profile-coworker".to_string(),
                profile_revision: reg_snap.revision,
                summary: format!("Plan {plan_id}"),
                tasks: tasks.clone(),
            },
            1_700_000_000_000,
        )
        .expect("save draft");

    let published = fixture
        .plans
        .change(
            draft.revision,
            PlanChange::Publish {
                plan_id: plan_id.to_string(),
                revision: draft.revision,
            },
        )
        .expect("publish plan");

    let compiled = compile_plan_revision(&published, &reg_snap).expect("compile plan");
    let approved_task_ids = tasks.iter().map(|t| t.id.clone()).collect();
    let summary = fixture
        .authority
        .approve_at(
            OperatorApprovalRequest {
                binding_id: binding_id.to_string(),
                thread_uuid: thread_uuid.to_string(),
                plan_id: plan_id.to_string(),
                plan_revision: published.revision,
                expected_registry_revision: reg_snap.revision,
                expected_plan_digest: compiled.plan_digest,
                repository_path: fixture.repo_dir.clone(),
                base_commit: fixture.base_commit.clone(),
                approved_task_ids,
                approved_write_paths,
                max_parallel: 2,
                budgets: ExecutionBudgets::default(),
            },
            1_700_000_001_000,
        )
        .expect("approve plan");
    summary.approval_id
}

#[test]
fn native_coworker_rejected_as_headless_worker_or_grant_model_and_provenance_honest() {
    let fixture = setup_fixture();
    const { assert!(COWORKER_IS_CONVERSATIONAL_INTERFACE_ONLY) };

    let native_choice = SelectedRoleChoice {
        tool_id: "tool-codex".to_string(),
        adapter: ToolAdapter::CodexCli,
        route: RoleExecutionRoute::NativeHandoff,
        model_id: "model-codex-native".to_string(),
        model_arg: "gpt-6-astra".to_string(),
        effort: Some("high".to_string()),
        matched_difficulty: None,
    };

    // Native Coworker (`CodexCli` / `Follower`) is rejected for headless worker roles.
    let err = validate_non_native_worker_choice(WorkflowRole::Coder, &native_choice)
        .expect_err("CodexCli must be rejected as a headless execution model");
    assert_eq!(err.code(), NativeErrorCode::ValidationFailed.as_str());

    let follower_err =
        validate_non_native_worker_choice(WorkflowRole::Follower, &headless_argon_choice())
            .expect_err("WorkflowRole::Follower must be rejected as a headless worker role");
    assert_eq!(
        follower_err.code(),
        NativeErrorCode::ValidationFailed.as_str()
    );

    // PlanningAdviceGrant rejects `operator_attested = false`.
    let unattested_err = fixture
        .store
        .create_planning_grant(&CreatePlanningGrantRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            request_key: "req-grant-unattested".to_string(),
            expected_ledger_revision: Some(0),
            project_base: project_base(&fixture),
            profile_id: "profile-coworker".to_string(),
            registry_revision: 5,
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
            operator_attested: false,
        })
        .expect_err("unattested planning grant must fail");
    assert_eq!(
        unattested_err.code(),
        NativeErrorCode::UnauthorizedOperator.as_str()
    );

    // PlanningAdviceGrant rejects write or execution capabilities.
    let mut bad_caps = PlanningReadOnlyCapabilities::strict_read_only();
    bad_caps.write_worktree_allowed = true;
    let caps_err = fixture
        .store
        .create_planning_grant(&CreatePlanningGrantRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            request_key: "req-grant-bad-caps".to_string(),
            expected_ledger_revision: Some(0),
            project_base: project_base(&fixture),
            profile_id: "profile-coworker".to_string(),
            registry_revision: 5,
            policy: planning_only_policy(),
            capabilities: bad_caps,
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
        .expect_err("grant with write capabilities must be rejected");
    assert_eq!(caps_err.code(), NativeErrorCode::SecurityViolation.as_str());

    // PlanningAdviceGrant rejects policies that allow Coder/Reviewer/Follower roles.
    let broad_policy_err = fixture
        .store
        .create_planning_grant(&CreatePlanningGrantRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            request_key: "req-grant-broad-policy".to_string(),
            expected_ledger_revision: Some(0),
            project_base: project_base(&fixture),
            profile_id: "profile-coworker".to_string(),
            registry_revision: 5,
            policy: execution_policy(),
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
        .expect_err("planning grant policy allowing Coder must be rejected");
    assert_eq!(
        broad_policy_err.code(),
        NativeErrorCode::ValidationFailed.as_str()
    );

    // Valid grant records `native_per_call_identity_verified: false` and rejects forged `true`.
    let mut grant = fixture
        .store
        .create_planning_grant(&CreatePlanningGrantRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            request_key: "req-grant-valid".to_string(),
            expected_ledger_revision: Some(0),
            project_base: project_base(&fixture),
            profile_id: "profile-coworker".to_string(),
            registry_revision: 5,
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
        .expect("create valid planning grant");
    assert!(!grant.native_per_call_identity_verified);
    grant.native_per_call_identity_verified = true;
    assert!(grant.validate().is_err());
}

#[test]
fn work_brief_validation_digest_readiness_and_amendment_holds_affected_tasks() {
    let fixture = setup_fixture();
    let pb = project_base(&fixture);
    assert_eq!(pb.base_tree_sha, fixture.base_tree_sha);
    assert_eq!(
        validate_write_scope_at_project_base(&pb, "src/lib.rs", "write_scope")
            .expect("valid write scope"),
        "src/lib.rs"
    );

    let mut brief = sample_ready_brief(&fixture, "brf_01", BINDING_A, THREAD_UUID_A);
    assert!(brief.is_ready_for_execution_planning());
    assert!(brief.is_ready_for_execution_approval());
    let d1 = brief.digest().expect("digest 1");
    let d2 = brief.digest().expect("digest 2");
    assert_eq!(d1, d2);

    // Adding a technical open question and an unapproved proposed addition changes the digest,
    // still allows the brief to reach `CoworkerJobPhase::Planning` (`is_ready_for_execution_planning`),
    // but keeps `is_ready_for_execution_approval() == false` so unapproved proposals or open
    // questions never authorize execution approval.
    brief
        .open_questions
        .push("Should we support multi-repo workspaces?".to_string());
    brief
        .proposed_additions
        .push("Optional telemetry export hook".to_string());
    assert!(brief.is_ready_for_execution_planning());
    assert!(!brief.is_ready_for_execution_approval());
    let d3 = brief.digest().expect("digest 3");
    assert_ne!(d1, d3);

    // Creating a job with confirmed requirements plus open questions/proposals reaches `CoworkerJobPhase::Planning`,
    // while strictly refusing execution approval binding until questions/proposals are resolved.
    let planning_job_with_questions = fixture
        .store
        .create_or_amend_job(&CreateCoworkerJobRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            request_key: "req-job-briefing".to_string(),
            expected_ledger_revision: Some(0),
            work_brief: brief.clone(),
            amended_target_task_ids: Vec::new(),
            amended_write_paths: Vec::new(),
            amended_interfaces: Vec::new(),
        })
        .expect("create planning job with technical questions/proposals");
    assert_eq!(
        planning_job_with_questions.phase,
        CoworkerJobPhase::Planning
    );

    let approval_id = publish_and_approve_plan(
        &fixture,
        "plan-amend-test",
        BINDING_A,
        THREAD_UUID_A,
        vec![
            PlanTask {
                id: "task-alpha".to_string(),
                title: "Alpha slice".to_string(),
                instruction: "Update src/lib.rs".to_string(),
                role: WorkflowRole::Coder,
                difficulty: Difficulty::Hard,
                depends_on: Vec::new(),
                allowed_write_paths: vec!["src/lib.rs".to_string()],
            },
            PlanTask {
                id: "task-docs".to_string(),
                title: "Docs slice".to_string(),
                instruction: "Update docs/guide.md".to_string(),
                role: WorkflowRole::Coder,
                difficulty: Difficulty::Easy,
                depends_on: Vec::new(),
                allowed_write_paths: vec!["docs/guide.md".to_string()],
            },
        ],
        vec!["src/".to_string(), "docs/".to_string()],
    );

    let bind_err = fixture
        .store
        .bind_job_approval(
            &BindJobApprovalRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                job_id: planning_job_with_questions.job_id.clone(),
                expected_job_revision: planning_job_with_questions.job_revision,
                expected_ledger_revision: None,
                approval_id: approval_id.clone(),
                policy: execution_policy(),
                controller_envelope: None,
                task_interfaces: BTreeMap::new(),
            },
            &fixture.authority,
        )
        .expect_err("cannot bind approval while brief has open_questions or proposed_additions");
    assert_eq!(bind_err.code(), NativeErrorCode::ValidationFailed.as_str());

    // Create a ready job, bind approval with complete interface declarations, and then amend only `task-alpha` (`src/lib.rs`).
    let ready_brief = sample_ready_brief(&fixture, "brf_02", BINDING_A, THREAD_UUID_A);
    let ready_job = fixture
        .store
        .create_or_amend_job(&CreateCoworkerJobRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            request_key: "req-job-ready".to_string(),
            expected_ledger_revision: None,
            work_brief: ready_brief,
            amended_target_task_ids: Vec::new(),
            amended_write_paths: Vec::new(),
            amended_interfaces: Vec::new(),
        })
        .expect("create ready job");
    assert_eq!(ready_job.phase, CoworkerJobPhase::Planning);

    let mut amend_task_interfaces = BTreeMap::new();
    amend_task_interfaces.insert(
        "task-alpha".to_string(),
        TaskInterfaceOwnershipSpec {
            owned_interfaces: vec!["iface_alpha".to_string()],
            consumed_interfaces: Vec::new(),
        },
    );
    amend_task_interfaces.insert(
        "task-docs".to_string(),
        TaskInterfaceOwnershipSpec {
            owned_interfaces: vec!["iface_docs".to_string()],
            consumed_interfaces: Vec::new(),
        },
    );

    let bound_job = fixture
        .store
        .bind_job_approval(
            &BindJobApprovalRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                job_id: ready_job.job_id.clone(),
                expected_job_revision: ready_job.job_revision,
                expected_ledger_revision: None,
                approval_id,
                policy: execution_policy(),
                controller_envelope: None,
                task_interfaces: amend_task_interfaces,
            },
            &fixture.authority,
        )
        .expect("bind approval to ready job");
    assert_eq!(bound_job.phase, CoworkerJobPhase::Executing);
    assert_eq!(bound_job.job_revision, 2);

    // Stale `target_job_revision` on an amendment must fail closed.
    let stale_amend_brief = WorkBrief {
        brief_id: "brf_amend_stale".to_string(),
        binding_id: BINDING_A.to_string(),
        thread_uuid: THREAD_UUID_A.to_string(),
        project_base: pb.clone(),
        kind: WorkBriefKind::Amendment {
            target_job_id: bound_job.job_id.clone(),
            target_job_revision: 1, // stale (current is 2)
            amendment_reason: "Adjust alpha contract".to_string(),
        },
        summary: "Amended brief".to_string(),
        confirmed_requirements: vec!["Updated requirement".to_string()],
        proposed_additions: Vec::new(),
        assumptions: Vec::new(),
        open_questions: Vec::new(),
        accepted_architect_decisions: Vec::new(),
    };
    let stale_err = fixture
        .store
        .create_or_amend_job(&CreateCoworkerJobRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            request_key: "req-job-amend-stale".to_string(),
            expected_ledger_revision: None,
            work_brief: stale_amend_brief,
            amended_target_task_ids: vec!["task-alpha".to_string()],
            amended_write_paths: vec!["src/lib.rs".to_string()],
            amended_interfaces: vec!["iface_alpha".to_string()],
        })
        .expect_err("stale target_job_revision must be rejected");
    assert_eq!(stale_err.code(), NativeErrorCode::ValidationFailed.as_str());

    // Valid amendment targeting `task-alpha` places `task-alpha` on hold while `task-docs` stays `ReadyToDispatch`.
    let valid_amend_brief = WorkBrief {
        brief_id: "brf_amend_valid".to_string(),
        binding_id: BINDING_A.to_string(),
        thread_uuid: THREAD_UUID_A.to_string(),
        project_base: pb,
        kind: WorkBriefKind::Amendment {
            target_job_id: bound_job.job_id.clone(),
            target_job_revision: bound_job.job_revision,
            amendment_reason: "Adjust alpha contract".to_string(),
        },
        summary: "Amended brief".to_string(),
        confirmed_requirements: vec!["Updated requirement".to_string()],
        proposed_additions: Vec::new(),
        assumptions: Vec::new(),
        open_questions: Vec::new(),
        accepted_architect_decisions: Vec::new(),
    };
    let amended_job = fixture
        .store
        .create_or_amend_job(&CreateCoworkerJobRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            request_key: "req-job-amend-valid".to_string(),
            expected_ledger_revision: None,
            work_brief: valid_amend_brief,
            amended_target_task_ids: vec!["task-alpha".to_string()],
            amended_write_paths: vec!["src/lib.rs".to_string()],
            amended_interfaces: vec!["iface_alpha".to_string()],
        })
        .expect("create amendment job");
    assert_eq!(
        amended_job.supersedes_job_id.as_deref(),
        Some(bound_job.job_id.as_str())
    );

    let ledger = fixture
        .store
        .read_binding_ledger(BINDING_A)
        .expect("read ledger");
    let target_after = ledger
        .jobs
        .iter()
        .find(|j| j.job_id == bound_job.job_id)
        .expect("find target job");
    assert_eq!(target_after.job_revision, 3);
    assert_eq!(target_after.holds.len(), 1);
    assert_eq!(
        target_after.holds[0].reason,
        WorkflowHoldReason::BriefAmendmentPending
    );

    let alpha_task = target_after
        .tasks
        .iter()
        .find(|t| t.task_id == "task-alpha")
        .expect("task-alpha");
    let docs_task = target_after
        .tasks
        .iter()
        .find(|t| t.task_id == "task-docs")
        .expect("task-docs");
    assert_eq!(alpha_task.stage, CoworkerTaskStage::Held);
    assert_eq!(docs_task.stage, CoworkerTaskStage::ReadyToDispatch);
}

#[test]
fn planning_grant_reserves_budget_before_effect_and_handles_refusal_uncertainty_expiry_revocation()
{
    let fixture = setup_fixture();
    let grant = fixture
        .store
        .create_planning_grant(&CreatePlanningGrantRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            request_key: "req-grant-budget".to_string(),
            expected_ledger_revision: Some(0),
            project_base: project_base(&fixture),
            profile_id: "profile-coworker".to_string(),
            registry_revision: 5,
            policy: planning_only_policy(),
            capabilities: PlanningReadOnlyCapabilities::strict_read_only(),
            budget: PlanningCallBudget {
                max_total_calls: 2,
                max_orchestrator_calls: 1,
                max_architect_calls: 2,
                max_timeout_ms_per_call: 30_000,
                max_output_bytes_per_call: 65_536,
            },
            created_at_ms: 1_000,
            expires_at_ms: 10_000,
            operator_attested: true,
        })
        .expect("create planning grant");

    let ctx_1 = sha256_hex(b"context-1");
    let res_1 = fixture
        .store
        .reserve_planning_call(&ReservePlanningCallRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            grant_id: grant.grant_id.clone(),
            request_key: "req-orch-1".to_string(),
            expected_ledger_revision: Some(1),
            role: WorkflowRole::Orchestrator,
            difficulty: Some(Difficulty::Hard),
            selected_choice: headless_argon_choice(),
            context_digest: ctx_1.clone(),
            call_project_base: Some(project_base(&fixture)),
            current_time_ms: 2_000,
        })
        .expect("reserve orchestrator call 1");
    assert_eq!(
        res_1.status,
        PlanningReservationStatus::ReservedBeforeEffect
    );

    // Pre-effect refusal releases reserved budget, but rejects if `run_id` is present.
    let bad_refusal = fixture
        .store
        .finalize_planning_call(&FinalizePlanningCallRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            grant_id: grant.grant_id.clone(),
            reservation_id: res_1.reservation_id.clone(),
            expected_ledger_revision: None,
            status: PlanningReservationStatus::RefusedBeforeEffect,
            updated_at_ms: 2_100,
            run_id: Some("run_started".to_string()),
            report_artifact_id: None,
            output_digest: None,
            diagnostic: Some("cannot refuse with run_id".to_string()),
        })
        .expect_err("RefusedBeforeEffect with run_id must fail");
    assert_eq!(
        bad_refusal.code(),
        NativeErrorCode::ValidationFailed.as_str()
    );

    let refused = fixture
        .store
        .finalize_planning_call(&FinalizePlanningCallRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            grant_id: grant.grant_id.clone(),
            reservation_id: res_1.reservation_id,
            expected_ledger_revision: None,
            status: PlanningReservationStatus::RefusedBeforeEffect,
            updated_at_ms: 2_100,
            run_id: None,
            report_artifact_id: None,
            output_digest: None,
            diagnostic: Some("rejected before spawn".to_string()),
        })
        .expect("finalize RefusedBeforeEffect");
    assert_eq!(
        refused.status,
        PlanningReservationStatus::RefusedBeforeEffect
    );

    // Now reserve and complete the 1 allowed orchestrator call.
    let res_2 = fixture
        .store
        .reserve_planning_call(&ReservePlanningCallRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            grant_id: grant.grant_id.clone(),
            request_key: "req-orch-2".to_string(),
            expected_ledger_revision: None,
            role: WorkflowRole::Orchestrator,
            difficulty: Some(Difficulty::Hard),
            selected_choice: headless_argon_choice(),
            context_digest: ctx_1,
            call_project_base: Some(project_base(&fixture)),
            current_time_ms: 2_200,
        })
        .expect("reserve orchestrator call 2");

    let out_digest = sha256_hex(b"orch-output");
    fixture
        .store
        .finalize_planning_call(&FinalizePlanningCallRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            grant_id: grant.grant_id.clone(),
            reservation_id: res_2.reservation_id,
            expected_ledger_revision: None,
            status: PlanningReservationStatus::Completed,
            updated_at_ms: 2_500,
            run_id: Some("run_orch_2".to_string()),
            report_artifact_id: Some("art_orch_2".to_string()),
            output_digest: Some(out_digest),
            diagnostic: None,
        })
        .expect("complete orchestrator call 2");

    // Second orchestrator call is rejected because `max_orchestrator_calls == 1`.
    let orch_exhausted = fixture
        .store
        .reserve_planning_call(&ReservePlanningCallRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            grant_id: grant.grant_id.clone(),
            request_key: "req-orch-3".to_string(),
            expected_ledger_revision: None,
            role: WorkflowRole::Orchestrator,
            difficulty: Some(Difficulty::Hard),
            selected_choice: headless_argon_choice(),
            context_digest: sha256_hex(b"context-2"),
            call_project_base: Some(project_base(&fixture)),
            current_time_ms: 2_600,
        })
        .expect_err("orchestrator role budget exhausted");
    assert_eq!(
        orch_exhausted.code(),
        NativeErrorCode::ValidationFailed.as_str()
    );

    // Reserve an Architect call (reaches `max_total_calls == 2`) and mark `EffectUnknown`.
    let arch_ctx = sha256_hex(b"arch-context-1");
    let res_arch = fixture
        .store
        .reserve_planning_call(&ReservePlanningCallRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            grant_id: grant.grant_id.clone(),
            request_key: "req-arch-1".to_string(),
            expected_ledger_revision: None,
            role: WorkflowRole::Architect,
            difficulty: Some(Difficulty::Hard),
            selected_choice: headless_argon_choice(),
            context_digest: arch_ctx.clone(),
            call_project_base: Some(project_base(&fixture)),
            current_time_ms: 3_000,
        })
        .expect("reserve architect call");

    fixture
        .store
        .finalize_planning_call(&FinalizePlanningCallRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            grant_id: grant.grant_id.clone(),
            reservation_id: res_arch.reservation_id.clone(),
            expected_ledger_revision: None,
            status: PlanningReservationStatus::EffectUnknown,
            updated_at_ms: 3_500,
            run_id: None,
            report_artifact_id: None,
            output_digest: None,
            diagnostic: Some("timeout waiting for provider stdout".to_string()),
        })
        .expect("mark architect call EffectUnknown");

    // Retrying an `EffectUnknown` reservation must fail with `AmbiguousWakeupPendingReconciliation`.
    let replay_err = fixture
        .store
        .reserve_planning_call(&ReservePlanningCallRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            grant_id: grant.grant_id.clone(),
            request_key: "req-arch-1".to_string(),
            expected_ledger_revision: None,
            role: WorkflowRole::Architect,
            difficulty: Some(Difficulty::Hard),
            selected_choice: headless_argon_choice(),
            context_digest: arch_ctx,
            call_project_base: Some(project_base(&fixture)),
            current_time_ms: 3_600,
        })
        .expect_err("EffectUnknown reservation must block automatic replay");
    assert_eq!(
        replay_err.code(),
        NativeErrorCode::AmbiguousWakeupPendingReconciliation.as_str()
    );

    // Reconcile `EffectUnknown` -> `Completed`.
    fixture
        .store
        .finalize_planning_call(&FinalizePlanningCallRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            grant_id: grant.grant_id.clone(),
            reservation_id: res_arch.reservation_id,
            expected_ledger_revision: None,
            status: PlanningReservationStatus::Completed,
            updated_at_ms: 4_000,
            run_id: Some("run_arch_1".to_string()),
            report_artifact_id: Some("art_arch_1".to_string()),
            output_digest: Some(sha256_hex(b"arch-output")),
            diagnostic: None,
        })
        .expect("reconcile EffectUnknown to Completed");

    // Revoking the grant transitions status to `Revoked`.
    let revoked = fixture
        .store
        .revoke_planning_grant(&RevokePlanningGrantRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            grant_id: grant.grant_id,
            expected_ledger_revision: None,
            revoked_at_ms: 5_000,
            reason: "Planning phase finished".to_string(),
        })
        .expect("revoke grant");
    assert_eq!(revoked.status, PlanningGrantStatus::Revoked);
}

#[test]
fn architect_advice_reuse_requires_exact_context_dependencies_never_text_similarity() {
    let fixture = setup_fixture();
    let pb = project_base(&fixture);
    let policy = planning_only_policy();
    let policy_digest = policy.digest().expect("policy digest");
    let brief = sample_ready_brief(&fixture, "brf_arch", BINDING_A, THREAD_UUID_A);
    let brief_digest = brief.digest().expect("brief digest");

    let grant = fixture
        .store
        .create_planning_grant(&CreatePlanningGrantRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            request_key: "req-grant-arch".to_string(),
            expected_ledger_revision: Some(0),
            project_base: pb.clone(),
            profile_id: "profile-coworker".to_string(),
            registry_revision: 5,
            policy,
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

    let mut files = BTreeMap::new();
    files.insert(
        "src/lib.rs".to_string(),
        sha256_hex(b"pub fn alpha() -> u32 { 1 }\n"),
    );
    let mut interfaces = BTreeMap::new();
    interfaces.insert("iface_coworker_store".to_string(), sha256_hex(b"trait v1"));
    let mut pred_trees = BTreeMap::new();
    pred_trees.insert("task-0".to_string(), fixture.base_tree_sha.clone());

    let deps = ArchitectContextDependencies {
        project_base: pb,
        work_brief_digest: brief_digest,
        plan_digest: Some(sha256_hex(b"plan-v1")),
        policy_digest,
        dependency_file_blobs: files,
        interface_ownership_digests: interfaces,
        accepted_predecessor_trees: pred_trees,
    };
    let ctx_digest = deps.context_digest().expect("context digest");

    let res = fixture
        .store
        .reserve_planning_call(&ReservePlanningCallRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            grant_id: grant.grant_id.clone(),
            request_key: "req-arch-advice-call".to_string(),
            expected_ledger_revision: None,
            role: WorkflowRole::Architect,
            difficulty: Some(Difficulty::Hard),
            selected_choice: headless_argon_choice(),
            context_digest: ctx_digest,
            call_project_base: Some(deps.project_base.clone()),
            current_time_ms: 2_000,
        })
        .expect("reserve architect call");

    let decision_digest =
        sha256_hex(b"Use per-binding CAS ledger with reserve-before-effect outbox");
    fixture
        .store
        .finalize_planning_call(&FinalizePlanningCallRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            grant_id: grant.grant_id.clone(),
            reservation_id: res.reservation_id.clone(),
            expected_ledger_revision: None,
            status: PlanningReservationStatus::Completed,
            updated_at_ms: 2_500,
            run_id: Some("run_arch_advice".to_string()),
            report_artifact_id: Some("art_arch_advice".to_string()),
            output_digest: Some(decision_digest.clone()),
            diagnostic: None,
        })
        .expect("complete architect reservation");

    let advice = fixture
        .store
        .record_architect_advice(&RecordArchitectAdviceRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            request_key: "req-record-advice-1".to_string(),
            expected_ledger_revision: None,
            grant_id: grant.grant_id,
            reservation_id: res.reservation_id,
            context_dependencies: deps.clone(),
            decision_summary: "Use per-binding CAS ledger with reserve-before-effect outbox"
                .to_string(),
            decision_digest,
            report_artifact_id: None,
            status: ArchitectDecisionStatus::AcceptedIntoBrief,
            created_at_ms: 2_600,
        })
        .expect("record architect advice");

    // Exact dependency match reuses the recorded advice.
    let reused = fixture
        .store
        .find_reusable_architect_advice(BINDING_A, THREAD_UUID_A, &deps)
        .expect("find reusable advice")
        .expect("advice must be reused when dependencies match");
    assert_eq!(reused.advice_id, advice.advice_id);

    // Changing any real dependency (file blob digest, interface digest, predecessor tree, brief digest)
    // must forbid reuse even when prose/summary is identical.
    let mut drifted_file_deps = deps.clone();
    drifted_file_deps.dependency_file_blobs.insert(
        "src/lib.rs".to_string(),
        sha256_hex(b"pub fn alpha() -> u32 { 2 }\n"),
    );
    assert!(!advice
        .can_reuse_for(&drifted_file_deps)
        .expect("check drifted file blob"));
    assert!(fixture
        .store
        .find_reusable_architect_advice(BINDING_A, THREAD_UUID_A, &drifted_file_deps)
        .expect("lookup drifted file blob")
        .is_none());

    let mut drifted_iface_deps = deps.clone();
    drifted_iface_deps
        .interface_ownership_digests
        .insert("iface_coworker_store".to_string(), sha256_hex(b"trait v2"));
    assert!(!advice
        .can_reuse_for(&drifted_iface_deps)
        .expect("check drifted interface digest"));
}

#[test]
fn immutable_approval_linkage_routine_outbox_verdict_and_combined_candidate_lifecycle() {
    let fixture = setup_fixture();
    let brief = sample_ready_brief(&fixture, "brf_lifecycle", BINDING_A, THREAD_UUID_A);
    let job = fixture
        .store
        .create_or_amend_job(&CreateCoworkerJobRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            request_key: "req-job-lifecycle".to_string(),
            expected_ledger_revision: Some(0),
            work_brief: brief,
            amended_target_task_ids: Vec::new(),
            amended_write_paths: Vec::new(),
            amended_interfaces: Vec::new(),
        })
        .expect("create job");

    let tasks = vec![
        PlanTask {
            id: "task-root".to_string(),
            title: "Root task".to_string(),
            instruction: "Implement alpha in src/lib.rs".to_string(),
            role: WorkflowRole::Coder,
            difficulty: Difficulty::Hard,
            depends_on: Vec::new(),
            allowed_write_paths: vec!["src/lib.rs".to_string()],
        },
        PlanTask {
            id: "task-child".to_string(),
            title: "Child task".to_string(),
            instruction: "Implement extra in src/extra.rs".to_string(),
            role: WorkflowRole::Coder,
            difficulty: Difficulty::Easy,
            depends_on: vec!["task-root".to_string()],
            allowed_write_paths: vec!["src/extra.rs".to_string()],
        },
    ];
    let approval_id = publish_and_approve_plan(
        &fixture,
        "plan-lifecycle",
        BINDING_A,
        THREAD_UUID_A,
        tasks.clone(),
        vec!["src/".to_string()],
    );

    let policy = execution_policy();
    let policy_digest = policy.digest().expect("policy digest");
    let job = fixture
        .store
        .bind_job_approval(
            &BindJobApprovalRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                job_id: job.job_id.clone(),
                expected_job_revision: job.job_revision,
                expected_ledger_revision: None,
                approval_id: approval_id.clone(),
                policy: policy.clone(),
                controller_envelope: None,
                task_interfaces: BTreeMap::new(),
            },
            &fixture.authority,
        )
        .expect("bind approval");

    assert_eq!(job.tasks.len(), 2);
    assert_eq!(job.tasks[0].stage, CoworkerTaskStage::ReadyToDispatch);
    assert_eq!(job.tasks[1].stage, CoworkerTaskStage::PendingDependencies);

    // Rebinding a different approval or policy to the same job is rejected (approval_linkage is immutable).
    let other_approval_id = publish_and_approve_plan(
        &fixture,
        "plan-lifecycle-other",
        BINDING_A,
        THREAD_UUID_A,
        tasks,
        vec!["src/".to_string()],
    );
    let rebind_err = fixture
        .store
        .bind_job_approval(
            &BindJobApprovalRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                job_id: job.job_id.clone(),
                expected_job_revision: job.job_revision,
                expected_ledger_revision: None,
                approval_id: other_approval_id,
                policy,
                controller_envelope: None,
                task_interfaces: BTreeMap::new(),
            },
            &fixture.authority,
        )
        .expect_err("rebinding a different approval to the same job must fail");
    assert_eq!(
        rebind_err.code(),
        NativeErrorCode::ValidationFailed.as_str()
    );

    // Dependent `task-child` cannot dispatch before `DependencyPreparation`.
    let premature_child_err = fixture
        .store
        .reserve_task_outbox(&ReserveTaskOutboxRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            job_id: job.job_id.clone(),
            expected_job_revision: job.job_revision,
            expected_ledger_revision: None,
            request_key: "req-child-premature".to_string(),
            kind: OutboxDispatchKind::TaskAttemptDispatch,
            task_id: "task-child".to_string(),
            role: WorkflowRole::Coder,
            subject_digest: sha256_hex(b"child-dispatch"),
            dependency_input_commit: None,
            dependency_input_tree_sha: None,
            accepted_predecessor_receipts: Vec::new(),
        })
        .expect_err("dependent task cannot dispatch while PendingDependencies");
    assert_eq!(
        premature_child_err.code(),
        NativeErrorCode::ValidationFailed.as_str()
    );

    // 1. Reserve outbox for `task-root` attempt 1.
    let (job, outbox_1) = fixture
        .store
        .reserve_task_outbox(&ReserveTaskOutboxRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            job_id: job.job_id.clone(),
            expected_job_revision: job.job_revision,
            expected_ledger_revision: None,
            request_key: "req-root-attempt-1".to_string(),
            kind: OutboxDispatchKind::TaskAttemptDispatch,
            task_id: "task-root".to_string(),
            role: WorkflowRole::Coder,
            subject_digest: sha256_hex(b"root-attempt-1"),
            dependency_input_commit: None,
            dependency_input_tree_sha: None,
            accepted_predecessor_receipts: Vec::new(),
        })
        .expect("reserve root attempt 1");
    assert_eq!(outbox_1.status, OutboxDispatchStatus::ReservedBeforeEffect);

    // Mark outbox_1 as `EffectUnknown` -> blocks replay and new dispatch until reconciled.
    let (job, _) = fixture
        .store
        .reconcile_or_finalize_outbox(&FinalizeTaskOutboxRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            job_id: job.job_id.clone(),
            expected_job_revision: job.job_revision,
            expected_ledger_revision: None,
            outbox_id: outbox_1.outbox_id.clone(),
            status: OutboxDispatchStatus::EffectUnknown,
            next_task_stage: CoworkerTaskStage::EffectUnknown,
            run_id: None,
            worker_id: None,
            task_worktree_id: None,
            head_commit: None,
            tree_sha: None,
            diff_digest: None,
            report_artifact_id: None,
            observed_model: None,
            observed_effort: None,
            receipt_id: None,
            diagnostic: Some("lost connection after submit".to_string()),
        })
        .expect("mark outbox EffectUnknown");

    let blocked_retry = fixture
        .store
        .reserve_task_outbox(&ReserveTaskOutboxRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            job_id: job.job_id.clone(),
            expected_job_revision: job.job_revision,
            expected_ledger_revision: None,
            request_key: "req-root-attempt-new-while-unknown".to_string(),
            kind: OutboxDispatchKind::TaskAttemptDispatch,
            task_id: "task-root".to_string(),
            role: WorkflowRole::Coder,
            subject_digest: sha256_hex(b"root-attempt-1"),
            dependency_input_commit: None,
            dependency_input_tree_sha: None,
            accepted_predecessor_receipts: Vec::new(),
        })
        .expect_err("EffectUnknown must block new dispatch");
    assert_eq!(
        blocked_retry.code(),
        NativeErrorCode::AmbiguousWakeupPendingReconciliation.as_str()
    );

    // Reconcile `outbox_1` as `ReconciledConfirmed` into `ReadyForReview`.
    let root_head_1 = "1111111111111111111111111111111111111111".to_string();
    let root_tree_1 = "2222222222222222222222222222222222222222".to_string();
    let root_diff_1 = sha256_hex(b"diff-root-1");
    let (job, _) = fixture
        .store
        .reconcile_or_finalize_outbox(&FinalizeTaskOutboxRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            job_id: job.job_id.clone(),
            expected_job_revision: job.job_revision,
            expected_ledger_revision: None,
            outbox_id: outbox_1.outbox_id,
            status: OutboxDispatchStatus::ReconciledConfirmed,
            next_task_stage: CoworkerTaskStage::ReadyForReview,
            run_id: Some("run_root_1".to_string()),
            worker_id: Some("wrk_root_1".to_string()),
            task_worktree_id: Some("wt_root_1".to_string()),
            head_commit: Some(root_head_1.clone()),
            tree_sha: Some(root_tree_1.clone()),
            diff_digest: Some(root_diff_1.clone()),
            report_artifact_id: Some("art_root_1".to_string()),
            observed_model: Some("gemini-4-argon-high".to_string()),
            observed_effort: Some("high".to_string()),
            receipt_id: None,
            diagnostic: None,
        })
        .expect("reconcile root attempt 1 to ReadyForReview");

    // Reserve and confirm review dispatch for `task-root` attempt 1.
    let (job, rev_outbox_1) = fixture
        .store
        .reserve_task_outbox(&ReserveTaskOutboxRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            job_id: job.job_id.clone(),
            expected_job_revision: job.job_revision,
            expected_ledger_revision: None,
            request_key: "req-root-review-1".to_string(),
            kind: OutboxDispatchKind::TaskReviewDispatch,
            task_id: "task-root".to_string(),
            role: WorkflowRole::Reviewer,
            subject_digest: sha256_hex(b"review-root-1"),
            dependency_input_commit: None,
            dependency_input_tree_sha: None,
            accepted_predecessor_receipts: Vec::new(),
        })
        .expect("reserve root review 1");
    let (job, _) = fixture
        .store
        .reconcile_or_finalize_outbox(&FinalizeTaskOutboxRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            job_id: job.job_id.clone(),
            expected_job_revision: job.job_revision,
            expected_ledger_revision: None,
            outbox_id: rev_outbox_1.outbox_id,
            status: OutboxDispatchStatus::DispatchedEffectConfirmed,
            next_task_stage: CoworkerTaskStage::UnderReview,
            run_id: Some("run_rev_1".to_string()),
            worker_id: Some("wrk_rev_1".to_string()),
            task_worktree_id: None,
            head_commit: None,
            tree_sha: None,
            diff_digest: None,
            report_artifact_id: None,
            observed_model: Some("gemini-4-argon-high".to_string()),
            observed_effort: Some("high".to_string()),
            receipt_id: None,
            diagnostic: None,
        })
        .expect("confirm root review 1");

    // Positive prose with `CorrectInScope` never advances acceptance!
    let subj_1 = sha256_hex(b"review-subject-root-1");
    let mut verdict_1 = StructuredReviewVerdict {
        verdict_id: "vrd_root_1".to_string(),
        task_id: "task-root".to_string(),
        attempt_no: 1,
        candidate_id: "cnd_root_1".to_string(),
        review_subject_digest: subj_1.clone(),
        base_commit: fixture.base_commit.clone(),
        task_head_commit: root_head_1.clone(),
        task_tree_sha: root_tree_1.clone(),
        diff_digest: root_diff_1.clone(),
        policy_digest: policy_digest.clone(),
        outcome: ReviewVerdictOutcome::CorrectInScope,
        findings: vec!["Add edge-case bounds check".to_string()],
        required_in_scope_fixes: vec!["Check empty slice in src/lib.rs".to_string()],
        conflicting_paths_or_interfaces: Vec::new(),
        prose_summary: Some("LGTM! Looks great overall, ship it!".to_string()),
        prose_may_advance_acceptance: true,
    };
    assert!(
        verdict_1.validate().is_err(),
        "prose_may_advance_acceptance = true must be rejected"
    );
    verdict_1.prose_may_advance_acceptance = false;
    assert!(!verdict_1.allows_acceptance());

    let (job, _) = fixture
        .store
        .record_task_candidate_and_verdict(&RecordTaskReviewVerdictRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            job_id: job.job_id.clone(),
            expected_job_revision: job.job_revision,
            expected_ledger_revision: None,
            candidate: TaskCandidateRef {
                candidate_id: "cnd_root_1".to_string(),
                task_id: "task-root".to_string(),
                attempt_no: 1,
                base_commit: fixture.base_commit.clone(),
                head_commit: root_head_1,
                tree_sha: root_tree_1,
                diff_digest: root_diff_1,
                write_paths: vec!["src/lib.rs".to_string()],
                check_evidence_digests: Vec::new(),
                review_subject_digest: Some(subj_1),
                review_verdict: Some(verdict_1),
                review_receipt_id: None,
                integration_receipt_id: None,
            },
        })
        .expect("record CorrectInScope verdict");
    assert_eq!(
        job.tasks[0].stage,
        CoworkerTaskStage::CorrectInScopeRequired
    );
    assert!(job.tasks[0].accepted_candidate_id.is_none());

    // 2. Run in-scope fix attempt 2 for `task-root`, review with `Accept`, and integrate.
    let (job, outbox_2) = fixture
        .store
        .reserve_task_outbox(&ReserveTaskOutboxRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            job_id: job.job_id.clone(),
            expected_job_revision: job.job_revision,
            expected_ledger_revision: None,
            request_key: "req-root-attempt-2".to_string(),
            kind: OutboxDispatchKind::TaskAttemptDispatch,
            task_id: "task-root".to_string(),
            role: WorkflowRole::Coder,
            subject_digest: sha256_hex(b"root-attempt-2"),
            dependency_input_commit: None,
            dependency_input_tree_sha: None,
            accepted_predecessor_receipts: Vec::new(),
        })
        .expect("reserve root attempt 2");
    let root_head_2 = "3333333333333333333333333333333333333333".to_string();
    let root_tree_2 = "4444444444444444444444444444444444444444".to_string();
    let root_diff_2 = sha256_hex(b"diff-root-2");

    // Fast-completed attempt from `DispatchReserved` to `ReadyForReview` must fail closed if
    // any required completion evidence (e.g. `diff_digest` or `report_artifact_id`) is missing,
    // and must never allow jumping directly to `Accepted`.
    let missing_evidence_err = fixture
        .store
        .reconcile_or_finalize_outbox(&FinalizeTaskOutboxRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            job_id: job.job_id.clone(),
            expected_job_revision: job.job_revision,
            expected_ledger_revision: None,
            outbox_id: outbox_2.outbox_id.clone(),
            status: OutboxDispatchStatus::DispatchedEffectConfirmed,
            next_task_stage: CoworkerTaskStage::ReadyForReview,
            run_id: Some("run_root_2".to_string()),
            worker_id: Some("wrk_root_2".to_string()),
            task_worktree_id: Some("wt_root_2".to_string()),
            head_commit: Some(root_head_2.clone()),
            tree_sha: Some(root_tree_2.clone()),
            diff_digest: None, // missing diff_digest evidence
            report_artifact_id: Some("art_root_2".to_string()),
            observed_model: Some("gemini-4-argon-high".to_string()),
            observed_effort: Some("high".to_string()),
            receipt_id: None,
            diagnostic: None,
        })
        .expect_err("transition to ReadyForReview without complete diff_digest evidence must fail");
    assert_eq!(
        missing_evidence_err.code(),
        NativeErrorCode::ValidationFailed.as_str()
    );

    let illegal_jump_err = fixture
        .store
        .reconcile_or_finalize_outbox(&FinalizeTaskOutboxRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            job_id: job.job_id.clone(),
            expected_job_revision: job.job_revision,
            expected_ledger_revision: None,
            outbox_id: outbox_2.outbox_id.clone(),
            status: OutboxDispatchStatus::DispatchedEffectConfirmed,
            next_task_stage: CoworkerTaskStage::Accepted,
            run_id: Some("run_root_2".to_string()),
            worker_id: Some("wrk_root_2".to_string()),
            task_worktree_id: Some("wt_root_2".to_string()),
            head_commit: Some(root_head_2.clone()),
            tree_sha: Some(root_tree_2.clone()),
            diff_digest: Some(root_diff_2.clone()),
            report_artifact_id: Some("art_root_2".to_string()),
            observed_model: Some("gemini-4-argon-high".to_string()),
            observed_effort: Some("high".to_string()),
            receipt_id: None,
            diagnostic: None,
        })
        .expect_err("DispatchReserved must never jump directly to Accepted");
    assert_eq!(
        illegal_jump_err.code(),
        NativeErrorCode::ValidationFailed.as_str()
    );

    let (job, _) = fixture
        .store
        .reconcile_or_finalize_outbox(&FinalizeTaskOutboxRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            job_id: job.job_id.clone(),
            expected_job_revision: job.job_revision,
            expected_ledger_revision: None,
            outbox_id: outbox_2.outbox_id,
            status: OutboxDispatchStatus::DispatchedEffectConfirmed,
            next_task_stage: CoworkerTaskStage::ReadyForReview,
            run_id: Some("run_root_2".to_string()),
            worker_id: Some("wrk_root_2".to_string()),
            task_worktree_id: Some("wt_root_2".to_string()),
            head_commit: Some(root_head_2.clone()),
            tree_sha: Some(root_tree_2.clone()),
            diff_digest: Some(root_diff_2.clone()),
            report_artifact_id: Some("art_root_2".to_string()),
            observed_model: Some("gemini-4-argon-high".to_string()),
            observed_effort: Some("high".to_string()),
            receipt_id: None,
            diagnostic: None,
        })
        .expect("confirm root attempt 2");

    let (job, rev_outbox_2) = fixture
        .store
        .reserve_task_outbox(&ReserveTaskOutboxRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            job_id: job.job_id.clone(),
            expected_job_revision: job.job_revision,
            expected_ledger_revision: None,
            request_key: "req-root-review-2".to_string(),
            kind: OutboxDispatchKind::TaskReviewDispatch,
            task_id: "task-root".to_string(),
            role: WorkflowRole::Reviewer,
            subject_digest: sha256_hex(b"review-root-2"),
            dependency_input_commit: None,
            dependency_input_tree_sha: None,
            accepted_predecessor_receipts: Vec::new(),
        })
        .expect("reserve root review 2");
    let (job, _) = fixture
        .store
        .reconcile_or_finalize_outbox(&FinalizeTaskOutboxRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            job_id: job.job_id.clone(),
            expected_job_revision: job.job_revision,
            expected_ledger_revision: None,
            outbox_id: rev_outbox_2.outbox_id,
            status: OutboxDispatchStatus::DispatchedEffectConfirmed,
            next_task_stage: CoworkerTaskStage::UnderReview,
            run_id: Some("run_rev_2".to_string()),
            worker_id: Some("wrk_rev_2".to_string()),
            task_worktree_id: None,
            head_commit: None,
            tree_sha: None,
            diff_digest: None,
            report_artifact_id: None,
            observed_model: Some("gemini-4-argon-high".to_string()),
            observed_effort: Some("high".to_string()),
            receipt_id: None,
            diagnostic: None,
        })
        .expect("confirm root review 2");

    let subj_2 = sha256_hex(b"review-subject-root-2");
    let verdict_2 = StructuredReviewVerdict {
        verdict_id: "vrd_root_2".to_string(),
        task_id: "task-root".to_string(),
        attempt_no: 2,
        candidate_id: "cnd_root_2".to_string(),
        review_subject_digest: subj_2.clone(),
        base_commit: fixture.base_commit.clone(),
        task_head_commit: root_head_2.clone(),
        task_tree_sha: root_tree_2.clone(),
        diff_digest: root_diff_2.clone(),
        policy_digest: policy_digest.clone(),
        outcome: ReviewVerdictOutcome::Accept,
        findings: Vec::new(),
        required_in_scope_fixes: Vec::new(),
        conflicting_paths_or_interfaces: Vec::new(),
        prose_summary: Some("All in-scope fixes verified.".to_string()),
        prose_may_advance_acceptance: false,
    };
    let (job, _) = fixture
        .store
        .record_task_candidate_and_verdict(&RecordTaskReviewVerdictRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            job_id: job.job_id.clone(),
            expected_job_revision: job.job_revision,
            expected_ledger_revision: None,
            candidate: TaskCandidateRef {
                candidate_id: "cnd_root_2".to_string(),
                task_id: "task-root".to_string(),
                attempt_no: 2,
                base_commit: fixture.base_commit.clone(),
                head_commit: root_head_2.clone(),
                tree_sha: root_tree_2.clone(),
                diff_digest: root_diff_2,
                write_paths: vec!["src/lib.rs".to_string()],
                check_evidence_digests: Vec::new(),
                review_subject_digest: Some(subj_2.clone()),
                review_verdict: Some(verdict_2),
                review_receipt_id: Some("rev_receipt_root_2".to_string()),
                integration_receipt_id: None,
            },
        })
        .expect("accept root candidate 2");
    assert_eq!(job.tasks[0].stage, CoworkerTaskStage::Accepted);
    assert_eq!(
        job.tasks[0].accepted_candidate_id.as_deref(),
        Some("cnd_root_2")
    );

    // Integrate `task-root`.
    let (job, int_outbox_root) = fixture
        .store
        .reserve_task_outbox(&ReserveTaskOutboxRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            job_id: job.job_id.clone(),
            expected_job_revision: job.job_revision,
            expected_ledger_revision: None,
            request_key: "req-root-integrate".to_string(),
            kind: OutboxDispatchKind::TaskIntegration,
            task_id: "task-root".to_string(),
            role: WorkflowRole::Coder,
            subject_digest: sha256_hex(b"integrate-root-2"),
            dependency_input_commit: None,
            dependency_input_tree_sha: None,
            accepted_predecessor_receipts: Vec::new(),
        })
        .expect("reserve root integration");
    let (job, _) = fixture
        .store
        .reconcile_or_finalize_outbox(&FinalizeTaskOutboxRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            job_id: job.job_id.clone(),
            expected_job_revision: job.job_revision,
            expected_ledger_revision: None,
            outbox_id: int_outbox_root.outbox_id,
            status: OutboxDispatchStatus::DispatchedEffectConfirmed,
            next_task_stage: CoworkerTaskStage::Integrated,
            run_id: None,
            worker_id: None,
            task_worktree_id: None,
            head_commit: None,
            tree_sha: None,
            diff_digest: None,
            report_artifact_id: None,
            observed_model: None,
            observed_effort: None,
            receipt_id: Some("int_receipt_root_2".to_string()),
            diagnostic: None,
        })
        .expect("complete root integration");
    assert_eq!(job.tasks[0].stage, CoworkerTaskStage::Integrated);

    // 3. Prepare dependency input for `task-child`, then execute, review (`Accept`), and integrate `task-child`.
    let root_receipt_ref = AcceptedPredecessorReceiptRef {
        task_id: "task-root".to_string(),
        candidate_id: "cnd_root_2".to_string(),
        head_commit: root_head_2.clone(),
        tree_sha: root_tree_2.clone(),
        review_subject_digest: subj_2,
        review_receipt_id: "rev_receipt_root_2".to_string(),
        integration_receipt_id: "int_receipt_root_2".to_string(),
    };
    let (job, _) = fixture
        .store
        .reserve_task_outbox(&ReserveTaskOutboxRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            job_id: job.job_id.clone(),
            expected_job_revision: job.job_revision,
            expected_ledger_revision: None,
            request_key: "req-child-dep-prep".to_string(),
            kind: OutboxDispatchKind::DependencyPreparation,
            task_id: "task-child".to_string(),
            role: WorkflowRole::Coder,
            subject_digest: sha256_hex(b"dep-prep-child"),
            dependency_input_commit: Some(root_head_2.clone()),
            dependency_input_tree_sha: Some(root_tree_2),
            accepted_predecessor_receipts: vec![root_receipt_ref.clone()],
        })
        .expect("prepare child dependencies");
    assert_eq!(job.tasks[1].stage, CoworkerTaskStage::ReadyToDispatch);

    let (job, child_outbox) = fixture
        .store
        .reserve_task_outbox(&ReserveTaskOutboxRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            job_id: job.job_id.clone(),
            expected_job_revision: job.job_revision,
            expected_ledger_revision: None,
            request_key: "req-child-attempt-1".to_string(),
            kind: OutboxDispatchKind::TaskAttemptDispatch,
            task_id: "task-child".to_string(),
            role: WorkflowRole::Coder,
            subject_digest: sha256_hex(b"child-attempt-1"),
            dependency_input_commit: None,
            dependency_input_tree_sha: None,
            accepted_predecessor_receipts: Vec::new(),
        })
        .expect("reserve child attempt 1");
    let child_head = "5555555555555555555555555555555555555555".to_string();
    let child_tree = "6666666666666666666666666666666666666666".to_string();
    let child_diff = sha256_hex(b"diff-child-1");
    let (job, _) = fixture
        .store
        .reconcile_or_finalize_outbox(&FinalizeTaskOutboxRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            job_id: job.job_id.clone(),
            expected_job_revision: job.job_revision,
            expected_ledger_revision: None,
            outbox_id: child_outbox.outbox_id,
            status: OutboxDispatchStatus::DispatchedEffectConfirmed,
            next_task_stage: CoworkerTaskStage::ReadyForReview,
            run_id: Some("run_child_1".to_string()),
            worker_id: Some("wrk_child_1".to_string()),
            task_worktree_id: Some("wt_child_1".to_string()),
            head_commit: Some(child_head.clone()),
            tree_sha: Some(child_tree.clone()),
            diff_digest: Some(child_diff.clone()),
            report_artifact_id: Some("art_child_1".to_string()),
            observed_model: Some("opencode-go/deepseek-v4.1-flash".to_string()),
            observed_effort: Some("max".to_string()),
            receipt_id: None,
            diagnostic: None,
        })
        .expect("confirm child attempt 1");

    let (job, child_rev_outbox) = fixture
        .store
        .reserve_task_outbox(&ReserveTaskOutboxRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            job_id: job.job_id.clone(),
            expected_job_revision: job.job_revision,
            expected_ledger_revision: None,
            request_key: "req-child-review-1".to_string(),
            kind: OutboxDispatchKind::TaskReviewDispatch,
            task_id: "task-child".to_string(),
            role: WorkflowRole::Reviewer,
            subject_digest: sha256_hex(b"review-child-1"),
            dependency_input_commit: None,
            dependency_input_tree_sha: None,
            accepted_predecessor_receipts: Vec::new(),
        })
        .expect("reserve child review 1");
    let (job, _) = fixture
        .store
        .reconcile_or_finalize_outbox(&FinalizeTaskOutboxRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            job_id: job.job_id.clone(),
            expected_job_revision: job.job_revision,
            expected_ledger_revision: None,
            outbox_id: child_rev_outbox.outbox_id,
            status: OutboxDispatchStatus::DispatchedEffectConfirmed,
            next_task_stage: CoworkerTaskStage::UnderReview,
            run_id: Some("run_child_rev_1".to_string()),
            worker_id: Some("wrk_child_rev_1".to_string()),
            task_worktree_id: None,
            head_commit: None,
            tree_sha: None,
            diff_digest: None,
            report_artifact_id: None,
            observed_model: Some("gemini-4-argon-high".to_string()),
            observed_effort: Some("high".to_string()),
            receipt_id: None,
            diagnostic: None,
        })
        .expect("confirm child review 1");

    let child_subj = sha256_hex(b"review-subject-child-1");
    let child_verdict = StructuredReviewVerdict {
        verdict_id: "vrd_child_1".to_string(),
        task_id: "task-child".to_string(),
        attempt_no: 1,
        candidate_id: "cnd_child_1".to_string(),
        review_subject_digest: child_subj.clone(),
        base_commit: root_head_2,
        task_head_commit: child_head.clone(),
        task_tree_sha: child_tree.clone(),
        diff_digest: child_diff.clone(),
        policy_digest,
        outcome: ReviewVerdictOutcome::Accept,
        findings: Vec::new(),
        required_in_scope_fixes: Vec::new(),
        conflicting_paths_or_interfaces: Vec::new(),
        prose_summary: None,
        prose_may_advance_acceptance: false,
    };
    let (job, _) = fixture
        .store
        .record_task_candidate_and_verdict(&RecordTaskReviewVerdictRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            job_id: job.job_id.clone(),
            expected_job_revision: job.job_revision,
            expected_ledger_revision: None,
            candidate: TaskCandidateRef {
                candidate_id: "cnd_child_1".to_string(),
                task_id: "task-child".to_string(),
                attempt_no: 1,
                base_commit: "3333333333333333333333333333333333333333".to_string(),
                head_commit: child_head.clone(),
                tree_sha: child_tree.clone(),
                diff_digest: child_diff,
                write_paths: vec!["src/extra.rs".to_string()],
                check_evidence_digests: Vec::new(),
                review_subject_digest: Some(child_subj.clone()),
                review_verdict: Some(child_verdict),
                review_receipt_id: Some("rev_receipt_child_1".to_string()),
                integration_receipt_id: None,
            },
        })
        .expect("accept child candidate");

    let (job, int_outbox_child) = fixture
        .store
        .reserve_task_outbox(&ReserveTaskOutboxRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            job_id: job.job_id.clone(),
            expected_job_revision: job.job_revision,
            expected_ledger_revision: None,
            request_key: "req-child-integrate".to_string(),
            kind: OutboxDispatchKind::TaskIntegration,
            task_id: "task-child".to_string(),
            role: WorkflowRole::Coder,
            subject_digest: sha256_hex(b"integrate-child-1"),
            dependency_input_commit: None,
            dependency_input_tree_sha: None,
            accepted_predecessor_receipts: Vec::new(),
        })
        .expect("reserve child integration");
    let (job, _) = fixture
        .store
        .reconcile_or_finalize_outbox(&FinalizeTaskOutboxRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            job_id: job.job_id.clone(),
            expected_job_revision: job.job_revision,
            expected_ledger_revision: None,
            outbox_id: int_outbox_child.outbox_id,
            status: OutboxDispatchStatus::DispatchedEffectConfirmed,
            next_task_stage: CoworkerTaskStage::Integrated,
            run_id: None,
            worker_id: None,
            task_worktree_id: None,
            head_commit: None,
            tree_sha: None,
            diff_digest: None,
            report_artifact_id: None,
            observed_model: None,
            observed_effort: None,
            receipt_id: Some("int_receipt_child_1".to_string()),
            diagnostic: None,
        })
        .expect("complete child integration");

    let child_receipt_ref = AcceptedPredecessorReceiptRef {
        task_id: "task-child".to_string(),
        candidate_id: "cnd_child_1".to_string(),
        head_commit: child_head.clone(),
        tree_sha: child_tree.clone(),
        review_subject_digest: child_subj,
        review_receipt_id: "rev_receipt_child_1".to_string(),
        integration_receipt_id: "int_receipt_child_1".to_string(),
    };

    // Failing check evidence must be rejected when assembling a CombinedCandidateIdentity.
    let failing_check_err = fixture
        .store
        .record_combined_candidate(&RecordCombinedCandidateRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            job_id: job.job_id.clone(),
            expected_job_revision: job.job_revision,
            expected_ledger_revision: None,
            request_key: "req-combined-failing".to_string(),
            final_commit_sha: Some(child_head.clone()),
            final_tree_sha: child_tree.clone(),
            accepted_predecessor_receipts: vec![
                root_receipt_ref.clone(),
                child_receipt_ref.clone(),
            ],
            check_evidence: vec![CandidateCheckEvidence {
                check_id: "cargo_test".to_string(),
                command_summary: "cargo test --workspace".to_string(),
                passed: false,
                exit_code: 101,
                evidence_digest: sha256_hex(b"failed test log"),
            }],
            closure_reservation_id: None,
        })
        .expect_err("failing check_evidence must be rejected");
    assert_eq!(
        failing_check_err.code(),
        NativeErrorCode::ValidationFailed.as_str()
    );

    let check_fmt = CandidateCheckEvidence {
        check_id: "cargo_fmt".to_string(),
        command_summary: "cargo fmt --check".to_string(),
        passed: true,
        exit_code: 0,
        evidence_digest: sha256_hex(b"fmt ok"),
    };
    let check_test = CandidateCheckEvidence {
        check_id: "cargo_test".to_string(),
        command_summary: "cargo test --workspace".to_string(),
        passed: true,
        exit_code: 0,
        evidence_digest: sha256_hex(b"test ok"),
    };

    let (job, combined) = fixture
        .store
        .record_combined_candidate(&RecordCombinedCandidateRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            job_id: job.job_id.clone(),
            expected_job_revision: job.job_revision,
            expected_ledger_revision: None,
            request_key: "req-combined-1".to_string(),
            final_commit_sha: Some(child_head),
            final_tree_sha: child_tree,
            accepted_predecessor_receipts: vec![
                child_receipt_ref.clone(),
                root_receipt_ref.clone(),
            ],
            check_evidence: vec![check_test.clone(), check_fmt.clone()],
            closure_reservation_id: Some("cls_0123456789abcdef0123456789abcdef".to_string()),
        })
        .expect("record combined candidate");
    assert_eq!(combined.status, CombinedCandidateStatus::ClosureReserved);
    assert_eq!(job.phase, CoworkerJobPhase::ClosureReview);

    // Reordering `accepted_predecessor_receipts` and `check_evidence` yields the exact same digest.
    let mut reordered = combined.clone();
    reordered.accepted_predecessor_receipts = vec![root_receipt_ref, child_receipt_ref];
    reordered.check_evidence = vec![check_fmt, check_test];
    assert_eq!(
        reordered
            .compute_candidate_digest()
            .expect("reordered digest"),
        combined.candidate_digest
    );
}

#[test]
fn workflow_holds_and_task_independence_proof_block_overlapping_or_dependent_tasks_only() {
    let fixture = setup_fixture();
    let brief = sample_ready_brief(&fixture, "brf_indep", BINDING_A, THREAD_UUID_A);
    let job = fixture
        .store
        .create_or_amend_job(&CreateCoworkerJobRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            request_key: "req-job-indep".to_string(),
            expected_ledger_revision: Some(0),
            work_brief: brief,
            amended_target_task_ids: Vec::new(),
            amended_write_paths: Vec::new(),
            amended_interfaces: Vec::new(),
        })
        .expect("create job");

    let tasks = vec![
        PlanTask {
            id: "task-a".to_string(),
            title: "Task A".to_string(),
            instruction: "Edit src/lib.rs".to_string(),
            role: WorkflowRole::Coder,
            difficulty: Difficulty::Hard,
            depends_on: Vec::new(),
            allowed_write_paths: vec!["src/lib.rs".to_string()],
        },
        PlanTask {
            id: "task-b".to_string(),
            title: "Task B (depends on A)".to_string(),
            instruction: "Edit src/extra.rs".to_string(),
            role: WorkflowRole::Coder,
            difficulty: Difficulty::Easy,
            depends_on: vec!["task-a".to_string()],
            allowed_write_paths: vec!["src/extra.rs".to_string()],
        },
        PlanTask {
            id: "task-c".to_string(),
            title: "Task C (consumes iface_auth)".to_string(),
            instruction: "Edit src/extra.rs with iface_auth".to_string(),
            role: WorkflowRole::Coder,
            difficulty: Difficulty::Easy,
            depends_on: Vec::new(),
            allowed_write_paths: vec!["src/extra.rs".to_string()],
        },
        PlanTask {
            id: "task-d".to_string(),
            title: "Task D (disjoint docs)".to_string(),
            instruction: "Edit docs/guide.md".to_string(),
            role: WorkflowRole::Coder,
            difficulty: Difficulty::Easy,
            depends_on: Vec::new(),
            allowed_write_paths: vec!["docs/guide.md".to_string()],
        },
    ];
    let approval_id = publish_and_approve_plan(
        &fixture,
        "plan-indep",
        BINDING_A,
        THREAD_UUID_A,
        tasks,
        vec!["src/".to_string(), "docs/".to_string()],
    );

    let mut task_interfaces = BTreeMap::new();
    task_interfaces.insert(
        "task-a".to_string(),
        TaskInterfaceOwnershipSpec {
            owned_interfaces: vec!["iface_auth".to_string()],
            consumed_interfaces: Vec::new(),
        },
    );
    task_interfaces.insert(
        "task-b".to_string(),
        TaskInterfaceOwnershipSpec {
            owned_interfaces: vec!["iface_session".to_string()],
            consumed_interfaces: vec!["iface_auth".to_string()],
        },
    );
    task_interfaces.insert(
        "task-c".to_string(),
        TaskInterfaceOwnershipSpec {
            owned_interfaces: Vec::new(),
            consumed_interfaces: vec!["iface_auth".to_string()],
        },
    );
    task_interfaces.insert(
        "task-d".to_string(),
        TaskInterfaceOwnershipSpec {
            owned_interfaces: vec!["iface_docs".to_string()],
            consumed_interfaces: Vec::new(),
        },
    );

    let job = fixture
        .store
        .bind_job_approval(
            &BindJobApprovalRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                job_id: job.job_id.clone(),
                expected_job_revision: job.job_revision,
                expected_ledger_revision: None,
                approval_id,
                policy: execution_policy(),
                controller_envelope: None,
                task_interfaces,
            },
            &fixture.authority,
        )
        .expect("bind approval");

    // Apply a hold targeting `task-a` and `iface_auth`.
    let (job, hold) = fixture
        .store
        .apply_workflow_hold(&ApplyWorkflowHoldRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            job_id: job.job_id.clone(),
            expected_job_revision: job.job_revision,
            expected_ledger_revision: None,
            request_key: "req-hold-auth".to_string(),
            reason: WorkflowHoldReason::ConversationClarificationPending,
            detail: "Clarify auth error format".to_string(),
            directly_affected_task_ids: vec!["task-a".to_string()],
            affected_write_paths: vec!["src/lib.rs".to_string()],
            affected_interfaces: vec!["iface_auth".to_string()],
            holds_entire_job: false,
        })
        .expect("apply workflow hold");

    let stage_map: BTreeMap<&str, CoworkerTaskStage> = job
        .tasks
        .iter()
        .map(|t| (t.task_id.as_str(), t.stage))
        .collect();
    assert_eq!(stage_map["task-a"], CoworkerTaskStage::Held);
    assert_eq!(stage_map["task-b"], CoworkerTaskStage::Held);
    assert_eq!(stage_map["task-c"], CoworkerTaskStage::Held);
    assert_eq!(stage_map["task-d"], CoworkerTaskStage::ReadyToDispatch);

    let proof_c = prove_task_independence(&job.tasks[2], &job.tasks, &job.holds)
        .expect("prove task-c independence");
    assert!(!proof_c.independent);
    assert!(!proof_c.disjoint_interface_ownership);

    let proof_d = prove_task_independence(&job.tasks[3], &job.tasks, &job.holds)
        .expect("prove task-d independence");
    assert!(proof_d.independent);

    // Held `task-c` is blocked from reserving outbox, whereas independent `task-d` succeeds!
    let held_err = fixture
        .store
        .reserve_task_outbox(&ReserveTaskOutboxRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            job_id: job.job_id.clone(),
            expected_job_revision: job.job_revision,
            expected_ledger_revision: None,
            request_key: "req-dispatch-c-while-held".to_string(),
            kind: OutboxDispatchKind::TaskAttemptDispatch,
            task_id: "task-c".to_string(),
            role: WorkflowRole::Coder,
            subject_digest: sha256_hex(b"c-dispatch"),
            dependency_input_commit: None,
            dependency_input_tree_sha: None,
            accepted_predecessor_receipts: Vec::new(),
        })
        .expect_err("held task-c must not dispatch");
    assert_eq!(held_err.code(), NativeErrorCode::ValidationFailed.as_str());

    let (job, outbox_d) = fixture
        .store
        .reserve_task_outbox(&ReserveTaskOutboxRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            job_id: job.job_id.clone(),
            expected_job_revision: job.job_revision,
            expected_ledger_revision: None,
            request_key: "req-dispatch-d-independent".to_string(),
            kind: OutboxDispatchKind::TaskAttemptDispatch,
            task_id: "task-d".to_string(),
            role: WorkflowRole::Coder,
            subject_digest: sha256_hex(b"d-dispatch"),
            dependency_input_commit: None,
            dependency_input_tree_sha: None,
            accepted_predecessor_receipts: Vec::new(),
        })
        .expect("independent task-d can reserve and dispatch while others are held");
    assert_eq!(outbox_d.status, OutboxDispatchStatus::ReservedBeforeEffect);

    // Resolve the hold and verify `task-a`, `task-b`, and `task-c` return to their prior stages.
    let (job, resolved_hold) = fixture
        .store
        .resolve_workflow_hold(&ResolveWorkflowHoldRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            job_id: job.job_id.clone(),
            expected_job_revision: job.job_revision,
            expected_ledger_revision: None,
            hold_id: hold.hold_id,
            resolution_note: "Human confirmed auth error format".to_string(),
        })
        .expect("resolve workflow hold");
    assert!(!resolved_hold.active);
    let stage_map_after: BTreeMap<&str, CoworkerTaskStage> = job
        .tasks
        .iter()
        .map(|t| (t.task_id.as_str(), t.stage))
        .collect();
    assert_eq!(
        stage_map_after["task-a"],
        CoworkerTaskStage::ReadyToDispatch
    );
    assert_eq!(
        stage_map_after["task-b"],
        CoworkerTaskStage::PendingDependencies
    );
    assert_eq!(
        stage_map_after["task-c"],
        CoworkerTaskStage::ReadyToDispatch
    );
}

#[test]
fn store_cas_stale_revision_conflicting_duplicate_isolation_and_corrupt_ledger_preservation() {
    let fixture = setup_fixture();

    // Pure read on an uninitialized binding never creates a ledger file or lockfile on disk.
    let ledger_a_path = fixture
        .store
        .ledger_path_for_binding(BINDING_A)
        .expect("ledger A path");
    let lock_a_path = fixture
        .store
        .lock_path_for_binding(BINDING_A)
        .expect("lock A path");
    let empty_a = fixture
        .store
        .read_binding_ledger(BINDING_A)
        .expect("read empty ledger A");
    assert_eq!(empty_a.ledger_revision, 0);
    assert_eq!(empty_a.schema_version, COWORKER_WORKFLOW_SCHEMA_VERSION);
    assert!(!ledger_a_path.exists());
    assert!(!lock_a_path.exists());

    // Create a job on BINDING_A (`ledger_revision` becomes 1).
    let brief_a = sample_ready_brief(&fixture, "brf_iso_a", BINDING_A, THREAD_UUID_A);
    let create_req = CreateCoworkerJobRequest {
        binding_id: BINDING_A.to_string(),
        thread_uuid: THREAD_UUID_A.to_string(),
        request_key: "req-iso-job-1".to_string(),
        expected_ledger_revision: Some(0),
        work_brief: brief_a.clone(),
        amended_target_task_ids: Vec::new(),
        amended_write_paths: Vec::new(),
        amended_interfaces: Vec::new(),
    };
    let job_a = fixture
        .store
        .create_or_amend_job(&create_req)
        .expect("create job A");

    // Idempotent retry with the same `request_key` and `WorkBrief` succeeds even with `expected_ledger_revision: Some(0)`
    // and does not increment `ledger_revision`.
    let job_a_retry = fixture
        .store
        .create_or_amend_job(&create_req)
        .expect("idempotent job creation retry");
    assert_eq!(job_a_retry, job_a);
    assert_eq!(
        fixture
            .store
            .read_binding_ledger(BINDING_A)
            .expect("read ledger A")
            .ledger_revision,
        1
    );

    // Conflicting duplicate `request_key` with a different `WorkBrief` fails closed.
    let mut conflicting_brief = brief_a;
    conflicting_brief.summary = "Different summary with reused request_key".to_string();
    let conflict_err = fixture
        .store
        .create_or_amend_job(&CreateCoworkerJobRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            request_key: "req-iso-job-1".to_string(),
            expected_ledger_revision: None,
            work_brief: conflicting_brief,
            amended_target_task_ids: Vec::new(),
            amended_write_paths: Vec::new(),
            amended_interfaces: Vec::new(),
        })
        .expect_err("conflicting duplicate request_key must fail");
    assert_eq!(
        conflict_err.code(),
        NativeErrorCode::ValidationFailed.as_str()
    );

    // Stale `expected_ledger_revision` fails closed without modifying the ledger.
    let stale_ledger_err = fixture
        .store
        .create_or_amend_job(&CreateCoworkerJobRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            request_key: "req-iso-job-2".to_string(),
            expected_ledger_revision: Some(0), // current is 1
            work_brief: sample_ready_brief(&fixture, "brf_iso_a2", BINDING_A, THREAD_UUID_A),
            amended_target_task_ids: Vec::new(),
            amended_write_paths: Vec::new(),
            amended_interfaces: Vec::new(),
        })
        .expect_err("stale expected_ledger_revision must fail");
    assert_eq!(
        stale_ledger_err.code(),
        NativeErrorCode::ValidationFailed.as_str()
    );

    // Cross-thread mutation on `BINDING_A` (`THREAD_UUID_B` instead of pinned `THREAD_UUID_A`) fails closed.
    let wrong_thread_err = fixture
        .store
        .create_or_amend_job(&CreateCoworkerJobRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_B.to_string(),
            request_key: "req-iso-wrong-thread".to_string(),
            expected_ledger_revision: Some(1),
            work_brief: sample_ready_brief(&fixture, "brf_wrong_t", BINDING_A, THREAD_UUID_B),
            amended_target_task_ids: Vec::new(),
            amended_write_paths: Vec::new(),
            amended_interfaces: Vec::new(),
        })
        .expect_err("cross-thread mutation on pinned binding ledger must fail");
    assert_eq!(
        wrong_thread_err.code(),
        NativeErrorCode::ValidationFailed.as_str()
    );

    // `BINDING_B` is completely isolated from `BINDING_A`.
    let ledger_b = fixture
        .store
        .read_binding_ledger(BINDING_B)
        .expect("read ledger B");
    assert_eq!(ledger_b.ledger_revision, 0);
    assert!(ledger_b.jobs.is_empty());

    // Unsupported `schema_version` or corrupt JSON in `BINDING_B` fails closed with `ProtocolError`
    // and preserves the existing file on disk untouched.
    let ledger_b_path = fixture
        .store
        .ledger_path_for_binding(BINDING_B)
        .expect("ledger B path");
    let mut future_ledger = CoworkerBindingLedger::empty(BINDING_B).expect("empty B");
    future_ledger.schema_version = 999;
    let future_bytes = serde_json::to_vec_pretty(&future_ledger).expect("serialize future ledger");
    fs::write(&ledger_b_path, &future_bytes).expect("write future ledger");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&ledger_b_path, fs::Permissions::from_mode(0o600))
            .expect("chmod 0600 future ledger");
    }

    let schema_err = fixture
        .store
        .read_binding_ledger(BINDING_B)
        .expect_err("unsupported schema_version must fail closed");
    assert_eq!(schema_err.code(), NativeErrorCode::ProtocolError.as_str());
    assert_eq!(
        fs::read(&ledger_b_path).expect("read preserved future ledger"),
        future_bytes,
        "unsupported schema_version ledger file must be preserved untouched on disk"
    );

    let corrupt_bytes = b"{\"schema_version\": 1, \"unknown_field\": true}".to_vec();
    fs::write(&ledger_b_path, &corrupt_bytes).expect("write unknown-field ledger");
    let unknown_field_err = fixture
        .store
        .create_or_amend_job(&CreateCoworkerJobRequest {
            binding_id: BINDING_B.to_string(),
            thread_uuid: THREAD_UUID_B.to_string(),
            request_key: "req-on-corrupt".to_string(),
            expected_ledger_revision: None,
            work_brief: sample_ready_brief(&fixture, "brf_iso_b", BINDING_B, THREAD_UUID_B),
            amended_target_task_ids: Vec::new(),
            amended_write_paths: Vec::new(),
            amended_interfaces: Vec::new(),
        })
        .expect_err("unknown field / corrupt ledger must fail closed on transaction");
    assert_eq!(
        unknown_field_err.code(),
        NativeErrorCode::ProtocolError.as_str()
    );
    assert_eq!(
        fs::read(&ledger_b_path).expect("read preserved corrupt ledger"),
        corrupt_bytes,
        "corrupt or unknown-field ledger file must never be overwritten"
    );
}

#[test]
fn work_brief_allows_open_questions_and_unapproved_proposals_into_planning_while_blocking_execution_approval(
) {
    let fixture = setup_fixture();
    let mut brief = sample_ready_brief(&fixture, "brf_planner_ctx", BINDING_A, THREAD_UUID_A);
    brief.open_questions =
        vec!["Should retry backoff use exponential jitter or fixed intervals?".to_string()];
    brief.proposed_additions =
        vec!["Add optional Prometheus metrics endpoint (unapproved suggestion)".to_string()];

    // Valid brief with confirmed requirements plus open questions/proposals reaches `Planning`
    // so the Coworker does not have to solve technical planning before the Orchestrator.
    assert!(brief.is_ready_for_execution_planning());
    assert!(!brief.is_ready_for_execution_approval());

    // Proposals must remain explicitly distinct from confirmed requirements and cannot silently
    // duplicate or become confirmed scope.
    let mut overlapping_proposal_brief = brief.clone();
    overlapping_proposal_brief
        .proposed_additions
        .push(brief.confirmed_requirements[0].clone());
    let dup_err = overlapping_proposal_brief
        .validate()
        .expect_err("proposed_additions duplicating confirmed_requirements must fail");
    assert_eq!(dup_err.code(), NativeErrorCode::ValidationFailed.as_str());

    let job_with_both = fixture
        .store
        .create_or_amend_job(&CreateCoworkerJobRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            request_key: "req-job-planner-ctx-both".to_string(),
            expected_ledger_revision: Some(0),
            work_brief: brief.clone(),
            amended_target_task_ids: Vec::new(),
            amended_write_paths: Vec::new(),
            amended_interfaces: Vec::new(),
        })
        .expect("create job with questions and proposals");
    assert_eq!(job_with_both.phase, CoworkerJobPhase::Planning);
    assert_eq!(
        job_with_both.work_brief.confirmed_requirements,
        brief.confirmed_requirements
    );
    assert_eq!(
        job_with_both.work_brief.proposed_additions,
        brief.proposed_additions
    );

    // Even when `open_questions` are empty, unconfirmed `proposed_additions` still allow planning
    // context (`CoworkerJobPhase::Planning`) while strictly blocking execution approval (`bind_job_approval`).
    let mut proposal_only_brief =
        sample_ready_brief(&fixture, "brf_proposal_only", BINDING_A, THREAD_UUID_A);
    proposal_only_brief.proposed_additions =
        vec!["Unapproved proposal that must never authorize writes".to_string()];
    assert!(proposal_only_brief.is_ready_for_execution_planning());
    assert!(!proposal_only_brief.is_ready_for_execution_approval());

    let job_proposal_only = fixture
        .store
        .create_or_amend_job(&CreateCoworkerJobRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            request_key: "req-job-proposal-only".to_string(),
            expected_ledger_revision: None,
            work_brief: proposal_only_brief,
            amended_target_task_ids: Vec::new(),
            amended_write_paths: Vec::new(),
            amended_interfaces: Vec::new(),
        })
        .expect("create job with unapproved proposal only");
    assert_eq!(job_proposal_only.phase, CoworkerJobPhase::Planning);

    let approval_id = publish_and_approve_plan(
        &fixture,
        "plan-proposal-gate",
        BINDING_A,
        THREAD_UUID_A,
        vec![PlanTask {
            id: "task-1".to_string(),
            title: "Task 1".to_string(),
            instruction: "Update src/lib.rs".to_string(),
            role: WorkflowRole::Coder,
            difficulty: Difficulty::Hard,
            depends_on: Vec::new(),
            allowed_write_paths: vec!["src/lib.rs".to_string()],
        }],
        vec!["src/".to_string()],
    );

    let proposal_bind_err = fixture
        .store
        .bind_job_approval(
            &BindJobApprovalRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                job_id: job_proposal_only.job_id,
                expected_job_revision: job_proposal_only.job_revision,
                expected_ledger_revision: None,
                approval_id,
                policy: execution_policy(),
                controller_envelope: None,
                task_interfaces: BTreeMap::new(),
            },
            &fixture.authority,
        )
        .expect_err("unconfirmed proposed_additions must block execution approval binding");
    assert_eq!(
        proposal_bind_err.code(),
        NativeErrorCode::ValidationFailed.as_str()
    );
}

#[test]
fn standing_planning_grant_allows_same_repo_commit_progression_and_rejects_cross_repository_context(
) {
    let fixture = setup_fixture();
    let origin_base = project_base(&fixture);
    let policy = planning_only_policy();
    let policy_digest = policy.digest().expect("policy digest");

    // Issue a standing planning grant at `origin_base`.
    let grant = fixture
        .store
        .create_planning_grant(&CreatePlanningGrantRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            request_key: "req-standing-grant".to_string(),
            expected_ledger_revision: Some(0),
            project_base: origin_base.clone(),
            profile_id: "profile-coworker".to_string(),
            registry_revision: 5,
            policy,
            capabilities: PlanningReadOnlyCapabilities::strict_read_only(),
            budget: PlanningCallBudget {
                max_total_calls: 6,
                max_orchestrator_calls: 3,
                max_architect_calls: 3,
                max_timeout_ms_per_call: 30_000,
                max_output_bytes_per_call: 65_536,
            },
            created_at_ms: 1_000,
            expires_at_ms: 60_000,
            operator_attested: true,
        })
        .expect("create standing grant");

    // Advance the canonical repository with a normal commit so HEAD has a new commit & tree SHA.
    fs::write(
        fixture.repo_dir.join("src/lib.rs"),
        "pub fn alpha() -> u32 { 42 }\n",
    )
    .expect("update src/lib.rs");
    for args in [
        &["add", "src/lib.rs"][..],
        &["commit", "-m", "normal commit progression", "--quiet"][..],
    ] {
        run_sanitized_git(&fixture.repo_dir, args, DEFAULT_GIT_COMMAND_TIMEOUT_MS)
            .expect("commit in canonical repo");
    }
    let next_commit =
        resolve_exact_commit_sha(&fixture.repo_dir, "HEAD", DEFAULT_GIT_COMMAND_TIMEOUT_MS)
            .expect("resolve next commit");
    assert_ne!(next_commit, origin_base.base_commit);
    let advanced_base =
        CanonicalProjectBaseRef::resolve_and_validate(&fixture.repo_dir, &next_commit)
            .expect("resolve advanced project_base");
    assert_eq!(advanced_base.repository_path, origin_base.repository_path);
    assert_ne!(advanced_base.base_tree_sha, origin_base.base_tree_sha);

    // 1. Backwards-compatible reservation with `call_project_base: None` still succeeds.
    let legacy_res = fixture
        .store
        .reserve_planning_call(&ReservePlanningCallRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            grant_id: grant.grant_id.clone(),
            request_key: "req-orch-legacy-default".to_string(),
            expected_ledger_revision: None,
            role: WorkflowRole::Orchestrator,
            difficulty: Some(Difficulty::Hard),
            selected_choice: headless_argon_choice(),
            context_digest: sha256_hex(b"orch-legacy-ctx"),
            call_project_base: None,
            current_time_ms: 2_000,
        })
        .expect("reserve planning call with backwards-compatible call_project_base: None");
    assert!(legacy_res.call_project_base.is_none());
    fixture
        .store
        .finalize_planning_call(&FinalizePlanningCallRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            grant_id: grant.grant_id.clone(),
            reservation_id: legacy_res.reservation_id,
            expected_ledger_revision: None,
            status: PlanningReservationStatus::Completed,
            updated_at_ms: 2_100,
            run_id: Some("run_orch_legacy".to_string()),
            report_artifact_id: None,
            output_digest: Some(sha256_hex(b"orch-legacy-out")),
            diagnostic: None,
        })
        .expect("complete legacy reservation");

    // 2. Reserving and recording Architect advice on `advanced_base` (new commit in the SAME
    // canonical repository) succeeds under the standing grant without requiring a new human grant.
    let mut files = BTreeMap::new();
    files.insert(
        "src/lib.rs".to_string(),
        sha256_hex(b"pub fn alpha() -> u32 { 42 }\n"),
    );
    let advanced_deps = ArchitectContextDependencies {
        project_base: advanced_base.clone(),
        work_brief_digest: sha256_hex(b"brief-advanced"),
        plan_digest: None,
        policy_digest: policy_digest.clone(),
        dependency_file_blobs: files.clone(),
        interface_ownership_digests: BTreeMap::new(),
        accepted_predecessor_trees: BTreeMap::new(),
    };
    let advanced_ctx_digest = advanced_deps.context_digest().expect("advanced ctx digest");

    let arch_res = fixture
        .store
        .reserve_planning_call(&ReservePlanningCallRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            grant_id: grant.grant_id.clone(),
            request_key: "req-arch-advanced-commit".to_string(),
            expected_ledger_revision: None,
            role: WorkflowRole::Architect,
            difficulty: Some(Difficulty::Hard),
            selected_choice: headless_argon_choice(),
            context_digest: advanced_ctx_digest,
            call_project_base: Some(advanced_base.clone()),
            current_time_ms: 3_000,
        })
        .expect("standing grant must allow new commit in the same canonical repository");
    assert_eq!(arch_res.call_project_base, Some(advanced_base.clone()));

    let decision_digest = sha256_hex(b"advice-on-advanced-commit");
    fixture
        .store
        .finalize_planning_call(&FinalizePlanningCallRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            grant_id: grant.grant_id.clone(),
            reservation_id: arch_res.reservation_id.clone(),
            expected_ledger_revision: None,
            status: PlanningReservationStatus::Completed,
            updated_at_ms: 3_100,
            run_id: Some("run_arch_adv_commit".to_string()),
            report_artifact_id: None,
            output_digest: Some(decision_digest.clone()),
            diagnostic: None,
        })
        .expect("complete advanced architect call");

    let recorded_advice = fixture
        .store
        .record_architect_advice(&RecordArchitectAdviceRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            request_key: "req-record-adv-advanced".to_string(),
            expected_ledger_revision: None,
            grant_id: grant.grant_id.clone(),
            reservation_id: arch_res.reservation_id,
            context_dependencies: advanced_deps,
            decision_summary: "Verified against advanced commit in same canonical repository"
                .to_string(),
            decision_digest,
            report_artifact_id: None,
            status: ArchitectDecisionStatus::Proposed,
            created_at_ms: 3_200,
        })
        .expect("record architect advice on advanced commit in same repo");
    assert_eq!(
        recorded_advice.context_dependencies.project_base,
        advanced_base
    );

    // 3. Create a second separate Git repository and verify that cross-repository context is
    // rejected at both `reserve_planning_call` and `record_architect_advice`.
    let other_repo_dir = fixture
        .repo_dir
        .parent()
        .expect("temp root")
        .join("other-repo");
    let (other_commit, _) = init_git_repo(&other_repo_dir);
    let other_repo_base =
        CanonicalProjectBaseRef::resolve_and_validate(&other_repo_dir, &other_commit)
            .expect("resolve other_repo_base");
    assert_ne!(other_repo_base.repository_path, origin_base.repository_path);

    let cross_repo_reserve_err = fixture
        .store
        .reserve_planning_call(&ReservePlanningCallRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            grant_id: grant.grant_id.clone(),
            request_key: "req-cross-repo-reserve".to_string(),
            expected_ledger_revision: None,
            role: WorkflowRole::Orchestrator,
            difficulty: Some(Difficulty::Hard),
            selected_choice: headless_argon_choice(),
            context_digest: sha256_hex(b"cross-repo-ctx"),
            call_project_base: Some(other_repo_base.clone()),
            current_time_ms: 4_000,
        })
        .expect_err("cross-repository call_project_base must not widen a standing grant");
    assert_eq!(
        cross_repo_reserve_err.code(),
        NativeErrorCode::SecurityViolation.as_str()
    );

    // Even if a caller reserved with `call_project_base: None`, `record_architect_advice` still
    // enforces canonical repository binding against `grant.project_base.repository_path`.
    let cross_repo_deps = ArchitectContextDependencies {
        project_base: other_repo_base,
        work_brief_digest: sha256_hex(b"brief-other-repo"),
        plan_digest: None,
        policy_digest,
        dependency_file_blobs: files,
        interface_ownership_digests: BTreeMap::new(),
        accepted_predecessor_trees: BTreeMap::new(),
    };
    let cross_repo_ctx_digest = cross_repo_deps
        .context_digest()
        .expect("cross repo ctx digest");
    let unpinned_res = fixture
        .store
        .reserve_planning_call(&ReservePlanningCallRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            grant_id: grant.grant_id.clone(),
            request_key: "req-arch-unpinned-base".to_string(),
            expected_ledger_revision: None,
            role: WorkflowRole::Architect,
            difficulty: Some(Difficulty::Hard),
            selected_choice: headless_argon_choice(),
            context_digest: cross_repo_ctx_digest,
            call_project_base: None,
            current_time_ms: 4_100,
        })
        .expect("reserve with call_project_base: None");
    fixture
        .store
        .finalize_planning_call(&FinalizePlanningCallRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            grant_id: grant.grant_id.clone(),
            reservation_id: unpinned_res.reservation_id.clone(),
            expected_ledger_revision: None,
            status: PlanningReservationStatus::Completed,
            updated_at_ms: 4_200,
            run_id: Some("run_arch_unpinned".to_string()),
            report_artifact_id: None,
            output_digest: Some(sha256_hex(b"cross-repo-out")),
            diagnostic: None,
        })
        .expect("finalize unpinned reservation");

    let cross_repo_advice_err = fixture
        .store
        .record_architect_advice(&RecordArchitectAdviceRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            request_key: "req-record-cross-repo-advice".to_string(),
            expected_ledger_revision: None,
            grant_id: grant.grant_id,
            reservation_id: unpinned_res.reservation_id,
            context_dependencies: cross_repo_deps,
            decision_summary: "Attempt cross-repository advice under standing grant".to_string(),
            decision_digest: sha256_hex(b"cross-repo-out"),
            report_artifact_id: None,
            status: ArchitectDecisionStatus::Proposed,
            created_at_ms: 4_300,
        })
        .expect_err("cross-repository ArchitectContextDependencies must be rejected");
    assert_eq!(
        cross_repo_advice_err.code(),
        NativeErrorCode::SecurityViolation.as_str()
    );
}

#[test]
fn task_independence_requires_complete_declared_evidence_and_conservatively_holds_when_interface_impact_incomplete(
) {
    let fixture = setup_fixture();
    let brief = sample_ready_brief(&fixture, "brf_incomplete_indep", BINDING_A, THREAD_UUID_A);
    let job = fixture
        .store
        .create_or_amend_job(&CreateCoworkerJobRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            request_key: "req-job-incomplete-indep".to_string(),
            expected_ledger_revision: Some(0),
            work_brief: brief,
            amended_target_task_ids: Vec::new(),
            amended_write_paths: Vec::new(),
            amended_interfaces: Vec::new(),
        })
        .expect("create job");

    // Two tasks with completely disjoint file `write_paths` (`src/lib.rs` vs `docs/guide.md`).
    let tasks = vec![
        PlanTask {
            id: "task-code".to_string(),
            title: "Code task".to_string(),
            instruction: "Edit src/lib.rs".to_string(),
            role: WorkflowRole::Coder,
            difficulty: Difficulty::Hard,
            depends_on: Vec::new(),
            allowed_write_paths: vec!["src/lib.rs".to_string()],
        },
        PlanTask {
            id: "task-docs".to_string(),
            title: "Docs task".to_string(),
            instruction: "Edit docs/guide.md".to_string(),
            role: WorkflowRole::Coder,
            difficulty: Difficulty::Easy,
            depends_on: Vec::new(),
            allowed_write_paths: vec!["docs/guide.md".to_string()],
        },
    ];
    let approval_id = publish_and_approve_plan(
        &fixture,
        "plan-incomplete-indep",
        BINDING_A,
        THREAD_UUID_A,
        tasks.clone(),
        vec!["src/".to_string(), "docs/".to_string()],
    );

    // Case 1: `task-docs` has disjoint `write_paths` (`docs/guide.md`) from `task-code` (`src/lib.rs`),
    // but `task-docs` has NO declared `owned_interfaces` or `consumed_interfaces` (incomplete interface impact).
    let mut incomplete_task_interfaces = BTreeMap::new();
    incomplete_task_interfaces.insert(
        "task-code".to_string(),
        TaskInterfaceOwnershipSpec {
            owned_interfaces: vec!["iface_core".to_string()],
            consumed_interfaces: Vec::new(),
        },
    );
    // `task-docs` intentionally omitted -> empty `owned_interfaces` and `consumed_interfaces`.

    let job = fixture
        .store
        .bind_job_approval(
            &BindJobApprovalRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                job_id: job.job_id.clone(),
                expected_job_revision: job.job_revision,
                expected_ledger_revision: None,
                approval_id,
                policy: execution_policy(),
                controller_envelope: None,
                task_interfaces: incomplete_task_interfaces,
            },
            &fixture.authority,
        )
        .expect("bind approval with incomplete task-docs interface declaration");

    // Apply a hold targeting `task-code` (`src/lib.rs`, `iface_core`). Because `task-docs` has
    // incomplete interface declarations, `prove_task_independence` must NOT release `task-docs`!
    let (job, _) = fixture
        .store
        .apply_workflow_hold(&ApplyWorkflowHoldRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            job_id: job.job_id.clone(),
            expected_job_revision: job.job_revision,
            expected_ledger_revision: None,
            request_key: "req-hold-code-1".to_string(),
            reason: WorkflowHoldReason::ReviewScopeConflict,
            detail: "Scope conflict on src/lib.rs".to_string(),
            directly_affected_task_ids: vec!["task-code".to_string()],
            affected_write_paths: vec!["src/lib.rs".to_string()],
            affected_interfaces: vec!["iface_core".to_string()],
            holds_entire_job: false,
        })
        .expect("apply hold");

    let docs_task = job
        .tasks
        .iter()
        .find(|t| t.task_id == "task-docs")
        .expect("task-docs");
    assert_eq!(
        docs_task.stage,
        CoworkerTaskStage::Held,
        "disjoint write_paths plus incomplete task interface declarations must conservatively hold the task"
    );

    let proof_incomplete_task =
        evaluate_declared_task_independence(docs_task, &job.tasks, &job.holds)
            .expect("evaluate declared independence");
    assert!(!proof_incomplete_task.independent);
    assert!(!proof_incomplete_task.complete_declared_evidence);
    assert!(proof_incomplete_task.disjoint_write_scopes);
    assert!(!proof_incomplete_task.disjoint_interface_ownership);

    // Case 2: Even when both tasks have complete interface declarations, an active non-job-wide
    // hold with empty `affected_interfaces` (incomplete hold interface impact) must also
    // conservatively hold both tasks rather than releasing `task-docs`.
    let brief_2 = sample_ready_brief(&fixture, "brf_incomplete_hold", BINDING_A, THREAD_UUID_A);
    let job_2 = fixture
        .store
        .create_or_amend_job(&CreateCoworkerJobRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            request_key: "req-job-incomplete-hold".to_string(),
            expected_ledger_revision: None,
            work_brief: brief_2,
            amended_target_task_ids: Vec::new(),
            amended_write_paths: Vec::new(),
            amended_interfaces: Vec::new(),
        })
        .expect("create job 2");
    let approval_id_2 = publish_and_approve_plan(
        &fixture,
        "plan-incomplete-hold",
        BINDING_A,
        THREAD_UUID_A,
        tasks,
        vec!["src/".to_string(), "docs/".to_string()],
    );
    let mut complete_task_interfaces = BTreeMap::new();
    complete_task_interfaces.insert(
        "task-code".to_string(),
        TaskInterfaceOwnershipSpec {
            owned_interfaces: vec!["iface_core".to_string()],
            consumed_interfaces: Vec::new(),
        },
    );
    complete_task_interfaces.insert(
        "task-docs".to_string(),
        TaskInterfaceOwnershipSpec {
            owned_interfaces: vec!["iface_docs".to_string()],
            consumed_interfaces: Vec::new(),
        },
    );
    let job_2 = fixture
        .store
        .bind_job_approval(
            &BindJobApprovalRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                job_id: job_2.job_id.clone(),
                expected_job_revision: job_2.job_revision,
                expected_ledger_revision: None,
                approval_id: approval_id_2,
                policy: execution_policy(),
                controller_envelope: None,
                task_interfaces: complete_task_interfaces,
            },
            &fixture.authority,
        )
        .expect("bind approval 2");

    let (job_2, _) = fixture
        .store
        .apply_workflow_hold(&ApplyWorkflowHoldRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            job_id: job_2.job_id.clone(),
            expected_job_revision: job_2.job_revision,
            expected_ledger_revision: None,
            request_key: "req-hold-incomplete-iface".to_string(),
            reason: WorkflowHoldReason::ConversationClarificationPending,
            detail: "Clarification with unknown interface impact".to_string(),
            directly_affected_task_ids: vec!["task-code".to_string()],
            affected_write_paths: vec!["src/lib.rs".to_string()],
            affected_interfaces: Vec::new(), // incomplete interface impact on hold!
            holds_entire_job: false,
        })
        .expect("apply hold with empty affected_interfaces");

    let docs_task_2 = job_2
        .tasks
        .iter()
        .find(|t| t.task_id == "task-docs")
        .expect("task-docs in job_2");
    assert_eq!(
        docs_task_2.stage,
        CoworkerTaskStage::Held,
        "disjoint write_paths plus incomplete hold affected_interfaces must not release task-docs"
    );
    let proof_incomplete_hold = prove_task_independence(docs_task_2, &job_2.tasks, &job_2.holds)
        .expect("prove independence on job_2");
    assert!(!proof_incomplete_hold.independent);
    assert!(!proof_incomplete_hold.complete_declared_evidence);
    assert!(!proof_incomplete_hold.disjoint_interface_ownership);
}

#[test]
fn frozen_approval_linkage_requires_explicit_controller_capability_envelope_for_reviewer_closure_fixes_checks_and_git(
) {
    let fixture = setup_fixture();
    let brief = sample_ready_brief(&fixture, "brf_ctrl_env", BINDING_A, THREAD_UUID_A);
    let job_coder_only = fixture
        .store
        .create_or_amend_job(&CreateCoworkerJobRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            request_key: "req-job-coder-only".to_string(),
            expected_ledger_revision: Some(0),
            work_brief: brief.clone(),
            amended_target_task_ids: Vec::new(),
            amended_write_paths: Vec::new(),
            amended_interfaces: Vec::new(),
        })
        .expect("create coder-only job");

    let tasks = vec![PlanTask {
        id: "task-1".to_string(),
        title: "Task 1".to_string(),
        instruction: "Edit src/lib.rs".to_string(),
        role: WorkflowRole::Coder,
        difficulty: Difficulty::Hard,
        depends_on: Vec::new(),
        allowed_write_paths: vec!["src/lib.rs".to_string()],
    }];
    let approval_1 = publish_and_approve_plan(
        &fixture,
        "plan-ctrl-env-1",
        BINDING_A,
        THREAD_UUID_A,
        tasks.clone(),
        vec!["src/".to_string()],
    );

    // 1. Default `controller_envelope: None`: even though `execution_policy()` allows Reviewer
    // and Closure, `policy_digest` (and the saved `WorkflowProfile`) by itself grants NO authority
    // for Reviewer, Closure, automatic in-scope corrections, check commands, or local Git integration.
    let bound_coder_only = fixture
        .store
        .bind_job_approval(
            &BindJobApprovalRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                job_id: job_coder_only.job_id,
                expected_job_revision: job_coder_only.job_revision,
                expected_ledger_revision: None,
                approval_id: approval_1,
                policy: execution_policy(),
                controller_envelope: None,
                task_interfaces: BTreeMap::new(),
            },
            &fixture.authority,
        )
        .expect("bind coder-only approval");
    let linkage_none = bound_coder_only
        .approval_linkage
        .as_ref()
        .expect("approval linkage");
    assert!(linkage_none.controller_envelope.is_none());
    assert!(!linkage_none.authorizes_reviewer_dispatch(&headless_argon_choice()));
    assert!(!linkage_none.authorizes_closure_dispatch(&headless_argon_choice()));
    assert!(!linkage_none.authorizes_in_scope_correction(2));
    assert!(!linkage_none.authorizes_check_command("cargo_test"));
    assert!(!linkage_none.authorizes_local_git_integration());

    // 2. Explicit `FrozenControllerCapabilityEnvelope` grants only the exact frozen capabilities
    // and rejects any choice not permitted by the frozen `ExecutionPolicy`.
    let job_with_env = fixture
        .store
        .create_or_amend_job(&CreateCoworkerJobRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            request_key: "req-job-with-ctrl-env".to_string(),
            expected_ledger_revision: None,
            work_brief: sample_ready_brief(&fixture, "brf_ctrl_env_2", BINDING_A, THREAD_UUID_A),
            amended_target_task_ids: Vec::new(),
            amended_write_paths: Vec::new(),
            amended_interfaces: Vec::new(),
        })
        .expect("create job 2");
    let approval_2 = publish_and_approve_plan(
        &fixture,
        "plan-ctrl-env-2",
        BINDING_A,
        THREAD_UUID_A,
        tasks,
        vec!["src/".to_string()],
    );

    // `model-deepseek` is only allowed for `Coder` in `execution_policy()`, not `Reviewer`.
    let unauthorized_reviewer_choice = SelectedRoleChoice {
        tool_id: "tool-oc".to_string(),
        adapter: ToolAdapter::OpencodeV2,
        route: RoleExecutionRoute::HeadlessWorker,
        model_id: "model-deepseek".to_string(),
        model_arg: "opencode-go/deepseek-v4.1-flash".to_string(),
        effort: Some("max".to_string()),
        matched_difficulty: Some(Difficulty::Easy),
    };
    let bad_env_err = fixture
        .store
        .bind_job_approval(
            &BindJobApprovalRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                job_id: job_with_env.job_id.clone(),
                expected_job_revision: job_with_env.job_revision,
                expected_ledger_revision: None,
                approval_id: approval_2.clone(),
                policy: execution_policy(),
                controller_envelope: Some(FrozenControllerCapabilityEnvelope {
                    allowed_reviewer_choices: vec![unauthorized_reviewer_choice.clone()],
                    allowed_closure_choices: Vec::new(),
                    max_in_scope_correction_attempts: 1,
                    allowed_check_ids: vec!["cargo_test".to_string()],
                    local_git_integration_allowed: true,
                    allow_check_failure_corrections: false,
                }),
                task_interfaces: BTreeMap::new(),
            },
            &fixture.authority,
        )
        .expect_err("reviewer choice not permitted by ExecutionPolicy must be rejected");
    assert_eq!(
        bad_env_err.code(),
        NativeErrorCode::SecurityViolation.as_str()
    );

    let valid_env = FrozenControllerCapabilityEnvelope {
        allowed_reviewer_choices: vec![headless_argon_choice()],
        allowed_closure_choices: vec![headless_argon_choice()],
        max_in_scope_correction_attempts: 2,
        allowed_check_ids: vec!["cargo_fmt".to_string(), "cargo_test".to_string()],
        local_git_integration_allowed: true,
        allow_check_failure_corrections: false,
    };
    let bound_with_env = fixture
        .store
        .bind_job_approval(
            &BindJobApprovalRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                job_id: job_with_env.job_id,
                expected_job_revision: job_with_env.job_revision,
                expected_ledger_revision: None,
                approval_id: approval_2,
                policy: execution_policy(),
                controller_envelope: Some(valid_env.clone()),
                task_interfaces: BTreeMap::new(),
            },
            &fixture.authority,
        )
        .expect("bind approval with valid controller envelope");

    let linkage = bound_with_env
        .approval_linkage
        .as_ref()
        .expect("approval linkage with envelope");
    assert_eq!(linkage.controller_envelope.as_ref(), Some(&valid_env));
    assert!(linkage.authorizes_reviewer_dispatch(&headless_argon_choice()));
    assert!(!linkage.authorizes_reviewer_dispatch(&unauthorized_reviewer_choice));
    assert!(linkage.authorizes_closure_dispatch(&headless_argon_choice()));
    assert!(!linkage.authorizes_in_scope_correction(1)); // attempt 1 is initial attempt, not correction
    assert!(linkage.authorizes_in_scope_correction(2)); // correction #1
    assert!(linkage.authorizes_in_scope_correction(3)); // correction #2
    assert!(!linkage.authorizes_in_scope_correction(4)); // exceeds max_in_scope_correction_attempts = 2
    assert!(linkage.authorizes_check_command("cargo_fmt"));
    assert!(linkage.authorizes_check_command("cargo_test"));
    assert!(!linkage.authorizes_check_command("arbitrary_shell_cmd"));
    assert!(linkage.authorizes_local_git_integration());
}

#[test]
fn orchestrator_reservation_in_binding_allows_independent_manual_job_approval() {
    let fixture = setup_fixture();
    let pb = project_base(&fixture);

    // 1. Create planning job A and record a completed Orchestrator grant/reservation in BINDING_A.
    let job_a = fixture
        .store
        .create_or_amend_job(&CreateCoworkerJobRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            request_key: "req-job-planned-a".to_string(),
            expected_ledger_revision: Some(0),
            work_brief: sample_ready_brief(&fixture, "brf_planned_a", BINDING_A, THREAD_UUID_A),
            amended_target_task_ids: Vec::new(),
            amended_write_paths: Vec::new(),
            amended_interfaces: Vec::new(),
        })
        .expect("create planning job A");

    let grant = fixture
        .store
        .create_planning_grant(&CreatePlanningGrantRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            request_key: "req-grant-orch-compat".to_string(),
            expected_ledger_revision: None,
            project_base: pb.clone(),
            profile_id: "profile-coworker".to_string(),
            registry_revision: 5,
            policy: planning_only_policy(),
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
        .expect("create planning grant");

    let orch_res = fixture
        .store
        .reserve_planning_call(&ReservePlanningCallRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            grant_id: grant.grant_id.clone(),
            request_key: "req-orch-job-a".to_string(),
            expected_ledger_revision: None,
            role: WorkflowRole::Orchestrator,
            difficulty: Some(Difficulty::Hard),
            selected_choice: headless_argon_choice(),
            context_digest: job_a.work_brief_digest.clone(),
            call_project_base: Some(pb.clone()),
            current_time_ms: 2_000,
        })
        .expect("reserve orchestrator call for job A");
    fixture
        .store
        .finalize_planning_call(&FinalizePlanningCallRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            grant_id: grant.grant_id,
            reservation_id: orch_res.reservation_id,
            expected_ledger_revision: None,
            status: PlanningReservationStatus::Completed,
            updated_at_ms: 2_500,
            run_id: Some("run_orch_job_a".to_string()),
            report_artifact_id: Some("art_orch_job_a".to_string()),
            output_digest: Some(sha256_hex(b"orch-plan-job-a")),
            diagnostic: None,
        })
        .expect("complete orchestrator reservation for job A");

    // 2. An independent legacy/manual job B in the same binding with its own explicit immutable
    // human approval (`planning_result == None`) must still bind normally.
    let job_b = fixture
        .store
        .create_or_amend_job(&CreateCoworkerJobRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            request_key: "req-job-manual-b".to_string(),
            expected_ledger_revision: None,
            work_brief: sample_ready_brief(&fixture, "brf_manual_b", BINDING_A, THREAD_UUID_A),
            amended_target_task_ids: Vec::new(),
            amended_write_paths: Vec::new(),
            amended_interfaces: Vec::new(),
        })
        .expect("create independent manual job B");
    assert!(job_b.planning_result.is_none());

    let tasks_b = vec![PlanTask {
        id: "task-manual-b".to_string(),
        title: "Manual task B".to_string(),
        instruction: "Edit src/lib.rs".to_string(),
        role: WorkflowRole::Coder,
        difficulty: Difficulty::Hard,
        depends_on: Vec::new(),
        allowed_write_paths: vec!["src/lib.rs".to_string()],
    }];
    let approval_b = publish_and_approve_plan(
        &fixture,
        "plan-manual-b",
        BINDING_A,
        THREAD_UUID_A,
        tasks_b.clone(),
        vec!["src/".to_string()],
    );

    let bound_b = fixture
        .store
        .bind_job_approval(
            &BindJobApprovalRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                job_id: job_b.job_id.clone(),
                expected_job_revision: job_b.job_revision,
                expected_ledger_revision: None,
                approval_id: approval_b.clone(),
                policy: execution_policy(),
                controller_envelope: None,
                task_interfaces: BTreeMap::new(),
            },
            &fixture.authority,
        )
        .expect("bind independent manual job B approval");
    assert_eq!(bound_b.phase, CoworkerJobPhase::Executing);
    assert_eq!(bound_b.tasks.len(), 1);

    // 3. Preserve wrong-job plan ownership, stale revision, and superseded-job refusals.
    let wrong_job_err = fixture
        .store
        .bind_job_approval(
            &BindJobApprovalRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                job_id: job_a.job_id.clone(),
                expected_job_revision: job_a.job_revision,
                expected_ledger_revision: None,
                approval_id: approval_b,
                policy: execution_policy(),
                controller_envelope: None,
                task_interfaces: BTreeMap::new(),
            },
            &fixture.authority,
        )
        .expect_err("plan_id owned by a different job must fail");
    assert_eq!(
        wrong_job_err.code(),
        NativeErrorCode::ValidationFailed.as_str()
    );

    let approval_a = publish_and_approve_plan(
        &fixture,
        "plan-job-a",
        BINDING_A,
        THREAD_UUID_A,
        tasks_b,
        vec!["src/".to_string()],
    );
    let stale_rev_err = fixture
        .store
        .bind_job_approval(
            &BindJobApprovalRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                job_id: job_a.job_id.clone(),
                expected_job_revision: job_a.job_revision + 1,
                expected_ledger_revision: None,
                approval_id: approval_a.clone(),
                policy: execution_policy(),
                controller_envelope: None,
                task_interfaces: BTreeMap::new(),
            },
            &fixture.authority,
        )
        .expect_err("stale expected_job_revision must fail closed");
    assert_eq!(
        stale_rev_err.code(),
        NativeErrorCode::ValidationFailed.as_str()
    );

    fixture
        .store
        .create_or_amend_job(&CreateCoworkerJobRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            request_key: "req-job-a-supersede".to_string(),
            expected_ledger_revision: None,
            work_brief: WorkBrief {
                brief_id: "brf_planned_a_v2".to_string(),
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                project_base: pb,
                kind: WorkBriefKind::Amendment {
                    target_job_id: job_a.job_id.clone(),
                    target_job_revision: job_a.job_revision,
                    amendment_reason: "Supersede job A before approval".to_string(),
                },
                summary: "Amended job A".to_string(),
                confirmed_requirements: vec!["Updated requirement".to_string()],
                proposed_additions: Vec::new(),
                assumptions: Vec::new(),
                open_questions: Vec::new(),
                accepted_architect_decisions: Vec::new(),
            },
            amended_target_task_ids: Vec::new(),
            amended_write_paths: Vec::new(),
            amended_interfaces: Vec::new(),
        })
        .expect("supersede job A");

    let superseded_err = fixture
        .store
        .bind_job_approval(
            &BindJobApprovalRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                job_id: job_a.job_id,
                expected_job_revision: 2,
                expected_ledger_revision: None,
                approval_id: approval_a,
                policy: execution_policy(),
                controller_envelope: None,
                task_interfaces: BTreeMap::new(),
            },
            &fixture.authority,
        )
        .expect_err("superseded job must not bind execution approval");
    assert_eq!(
        superseded_err.code(),
        NativeErrorCode::ValidationFailed.as_str()
    );
}
