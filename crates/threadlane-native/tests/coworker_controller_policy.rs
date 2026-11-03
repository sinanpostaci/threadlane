use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use tempfile::TempDir;
use threadlane_authority::{AuthorityStore, OperatorApprovalRequest};
use threadlane_native::coworker_workflow::controller_policy::{
    ControllerConfigurationPreview, ControllerPolicyStore, ControllerRoleExecutionBudget,
    FreezeControllerExecutionEnvelopeRequest, FrozenCheckCommandSpec, FrozenCheckKind,
};
use threadlane_native::coworker_workflow::{
    BindJobApprovalRequest, CanonicalProjectBaseRef, CoworkerJobRecord, CoworkerWorkflowStore,
    CreateCoworkerJobRequest, FrozenControllerCapabilityEnvelope, WorkBrief, WorkBriefKind,
};
use threadlane_native::{
    Difficulty, ExecutionPolicy, NativeErrorCode, PolicyAllowRule, RegistryStore,
    RoleExecutionRoute, SelectedRoleChoice, ToolAdapter, WorkflowProfile, WorkflowRole,
};
use threadlane_operations::IntegrationStrategy;
use threadlane_workers::{
    resolve_exact_commit_sha, run_sanitized_git, ExecutionBudgets, DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    DEFAULT_MAX_STDERR_BYTES, DEFAULT_MAX_STDOUT_BYTES, DEFAULT_TIMEOUT_MS,
};
use threadlane_workflow::{
    compile_plan_revision, ModelChoice, ModelDefinition, PlanChange, PlanStore, PlanTask,
    RegistryChange, RoleAssignment, ToolDefinition,
};

const THREAD_UUID_A: &str = "550e8400-e29b-41d4-a716-446655440000";
const THREAD_UUID_B: &str = "6ba7b810-9dad-11d1-80b4-00c04fd430c8";
const BINDING_A: &str = "bnd_controller_policy_alpha";

struct ControllerPolicyFixture {
    _tmp: TempDir,
    repo_dir: PathBuf,
    base_commit: String,
    fake_cargo_exe: PathBuf,
    fake_cargo_alt_exe: PathBuf,
    fake_sh_exe: PathBuf,
    registry: Arc<RegistryStore>,
    plans: Arc<PlanStore>,
    authority: Arc<AuthorityStore>,
    workflow_store: CoworkerWorkflowStore,
    policy_store: ControllerPolicyStore,
}

fn init_git_repo(repo_dir: &Path) -> String {
    fs::create_dir_all(repo_dir.join("src")).expect("create repo src");
    fs::write(repo_dir.join("src/lib.rs"), "pub fn alpha() -> u32 { 1 }\n")
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
    resolve_exact_commit_sha(repo_dir, "HEAD", DEFAULT_GIT_COMMAND_TIMEOUT_MS)
        .expect("resolve HEAD commit")
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

fn setup_fixture() -> ControllerPolicyFixture {
    let tmp = TempDir::new().expect("tempdir");
    let root = fs::canonicalize(tmp.path()).expect("canonicalize temp root");

    let repo_dir = root.join("repo");
    let base_commit = init_git_repo(&repo_dir);

    let bin_dir = root.join("bin");
    let fake_agy = bin_dir.join("fake-agy");
    let fake_oc = bin_dir.join("fake-opencode");
    let fake_cargo_exe = bin_dir.join("fake-cargo");
    let fake_cargo_alt_exe = bin_dir.join("fake-cargo-nextest");
    let fake_sh_exe = bin_dir.join("sh");
    write_executable_stub(&fake_agy);
    write_executable_stub(&fake_oc);
    write_executable_stub(&fake_cargo_exe);
    write_executable_stub(&fake_cargo_alt_exe);
    write_executable_stub(&fake_sh_exe);

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
    let workflow_store = CoworkerWorkflowStore::new(&plans_dir).expect("open workflow store");
    let policy_store = ControllerPolicyStore::new(&plans_dir).expect("open policy store");

    ControllerPolicyFixture {
        _tmp: tmp,
        repo_dir,
        base_commit,
        fake_cargo_exe,
        fake_cargo_alt_exe,
        fake_sh_exe,
        registry,
        plans,
        authority,
        workflow_store,
        policy_store,
    }
}

fn project_base(fixture: &ControllerPolicyFixture) -> CanonicalProjectBaseRef {
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

fn headless_deepseek_choice() -> SelectedRoleChoice {
    SelectedRoleChoice {
        tool_id: "tool-oc".to_string(),
        adapter: ToolAdapter::OpencodeV2,
        route: RoleExecutionRoute::HeadlessWorker,
        model_id: "model-deepseek".to_string(),
        model_arg: "opencode-go/deepseek-v4.1-flash".to_string(),
        effort: Some("max".to_string()),
        matched_difficulty: Some(Difficulty::Easy),
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
                roles: vec![
                    WorkflowRole::Coder,
                    WorkflowRole::Reviewer,
                    WorkflowRole::Closure,
                ],
            },
        ],
    }
}

fn create_bound_job(
    fixture: &ControllerPolicyFixture,
    brief_id: &str,
    plan_id: &str,
) -> CoworkerJobRecord {
    let brief = WorkBrief {
        brief_id: brief_id.to_string(),
        binding_id: BINDING_A.to_string(),
        thread_uuid: THREAD_UUID_A.to_string(),
        project_base: project_base(fixture),
        kind: WorkBriefKind::NewJob,
        summary: "Verify controller policy authority guard".to_string(),
        confirmed_requirements: vec!["Freeze full execution envelope digest".to_string()],
        proposed_additions: Vec::new(),
        assumptions: vec!["Trusted-local Linux workspace".to_string()],
        open_questions: Vec::new(),
        accepted_architect_decisions: vec!["adv_0123456789abcdef0123456789abcdef".to_string()],
    };
    let job = fixture
        .workflow_store
        .create_or_amend_job(&CreateCoworkerJobRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            request_key: format!("req-job-{brief_id}"),
            expected_ledger_revision: None,
            work_brief: brief,
            amended_target_task_ids: Vec::new(),
            amended_write_paths: Vec::new(),
            amended_interfaces: Vec::new(),
        })
        .expect("create job");

    let tasks = vec![PlanTask {
        id: "task-1".to_string(),
        title: "Task 1".to_string(),
        instruction: "Update src/lib.rs".to_string(),
        role: WorkflowRole::Coder,
        difficulty: Difficulty::Hard,
        depends_on: Vec::new(),
        allowed_write_paths: vec!["src/lib.rs".to_string()],
    }];
    let reg_snap = fixture.registry.snapshot().expect("registry snapshot");
    let draft = fixture
        .plans
        .change_at(
            0,
            PlanChange::SaveDraft {
                plan_id: plan_id.to_string(),
                binding_id: BINDING_A.to_string(),
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
    let summary = fixture
        .authority
        .approve_at(
            OperatorApprovalRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                plan_id: plan_id.to_string(),
                plan_revision: published.revision,
                expected_registry_revision: reg_snap.revision,
                expected_plan_digest: compiled.plan_digest,
                repository_path: fixture.repo_dir.clone(),
                base_commit: fixture.base_commit.clone(),
                approved_task_ids: vec!["task-1".to_string()],
                approved_write_paths: vec!["src/".to_string()],
                max_parallel: 1,
                budgets: ExecutionBudgets::default(),
            },
            1_700_000_001_000,
        )
        .expect("approve plan");

    let cap_env = FrozenControllerCapabilityEnvelope {
        allowed_reviewer_choices: vec![headless_argon_choice(), headless_deepseek_choice()],
        allowed_closure_choices: vec![headless_argon_choice(), headless_deepseek_choice()],
        max_in_scope_correction_attempts: 2,
        allowed_check_ids: vec!["cargo_fmt".to_string(), "cargo_test".to_string()],
        local_git_integration_allowed: true,
    };

    fixture
        .workflow_store
        .bind_job_approval(
            &BindJobApprovalRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                job_id: job.job_id,
                expected_job_revision: job.job_revision,
                expected_ledger_revision: None,
                approval_id: summary.approval_id,
                policy: execution_policy(),
                controller_envelope: Some(cap_env),
                task_interfaces: BTreeMap::new(),
            },
            &fixture.authority,
        )
        .expect("bind job approval")
}

fn default_role_budgets() -> BTreeMap<WorkflowRole, ControllerRoleExecutionBudget> {
    let mut budgets = BTreeMap::new();
    budgets.insert(
        WorkflowRole::Coder,
        ControllerRoleExecutionBudget::default_for_role(WorkflowRole::Coder, 4),
    );
    budgets.insert(
        WorkflowRole::Reviewer,
        ControllerRoleExecutionBudget::default_for_role(WorkflowRole::Reviewer, 4),
    );
    budgets.insert(
        WorkflowRole::Closure,
        ControllerRoleExecutionBudget::default_for_role(WorkflowRole::Closure, 2),
    );
    budgets
}

fn default_check_commands(
    fixture: &ControllerPolicyFixture,
) -> BTreeMap<String, FrozenCheckCommandSpec> {
    let mut checks = BTreeMap::new();
    checks.insert(
        "cargo_fmt".to_string(),
        FrozenCheckCommandSpec {
            check_id: "cargo_fmt".to_string(),
            kind: FrozenCheckKind::Formatting,
            executable: fixture.fake_cargo_exe.clone(),
            argv: vec![
                "fmt".to_string(),
                "--all".to_string(),
                "--check".to_string(),
            ],
            repo_relative_cwd: ".".to_string(),
            timeout_ms: DEFAULT_TIMEOUT_MS,
            max_stdout_bytes: DEFAULT_MAX_STDOUT_BYTES,
            max_stderr_bytes: DEFAULT_MAX_STDERR_BYTES,
            required_for_task_review: true,
            required_for_candidate_closure: true,
        },
    );
    checks.insert(
        "cargo_test".to_string(),
        FrozenCheckCommandSpec {
            check_id: "cargo_test".to_string(),
            kind: FrozenCheckKind::Tests,
            executable: fixture.fake_cargo_exe.clone(),
            argv: vec!["test".to_string(), "--workspace".to_string()],
            repo_relative_cwd: ".".to_string(),
            timeout_ms: DEFAULT_TIMEOUT_MS,
            max_stdout_bytes: DEFAULT_MAX_STDOUT_BYTES,
            max_stderr_bytes: DEFAULT_MAX_STDERR_BYTES,
            required_for_task_review: true,
            required_for_candidate_closure: true,
        },
    );
    checks
}

fn sample_preview_request(
    fixture: &ControllerPolicyFixture,
    job: &CoworkerJobRecord,
) -> FreezeControllerExecutionEnvelopeRequest {
    FreezeControllerExecutionEnvelopeRequest {
        binding_id: BINDING_A.to_string(),
        thread_uuid: THREAD_UUID_A.to_string(),
        job_id: job.job_id.clone(),
        expected_job_revision: job.job_revision,
        policy: execution_policy(),
        reviewer_choice: headless_argon_choice(),
        closure_choice: headless_argon_choice(),
        role_budgets: default_role_budgets(),
        max_in_scope_corrections_per_task: 1,
        max_total_corrections_per_job: 2,
        max_total_task_attempts_per_job: 4,
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
    }
}

fn attested_record_request_from_preview(
    base_req: &FreezeControllerExecutionEnvelopeRequest,
    preview: &ControllerConfigurationPreview,
) -> FreezeControllerExecutionEnvelopeRequest {
    let mut req = base_req.clone();
    req.operator_attested = true;
    req.confirm_approval_subject_digest = Some(preview.approval_subject_digest.clone());
    req.confirm_envelope_digest = Some(preview.envelope_digest.clone());
    req
}

#[test]
fn preview_is_pure_read_only_and_coheres_with_attested_record_and_idempotent_replay() {
    let fixture = setup_fixture();
    let job = create_bound_job(&fixture, "brf_preview_record", "plan-preview-record");
    let preview_req = sample_preview_request(&fixture, &job);

    // 1. Pure preview with `operator_attested = false` never creates or mutates controller ledger state.
    let preview_unattested = fixture
        .policy_store
        .preview_execution_envelope(&preview_req, &job, &fixture.authority)
        .expect("preview execution envelope");
    assert!(!preview_unattested.envelope.operator_attested);
    assert!(
        !preview_unattested
            .envelope
            .native_per_call_identity_verified
    );
    assert_eq!(preview_unattested.envelope_digest.len(), 64);
    assert!(!fixture
        .policy_store
        .ledger_path_for_binding(BINDING_A)
        .expect("ledger path")
        .exists());
    assert!(fixture
        .policy_store
        .get_job_runtime_readonly(BINDING_A, &job.job_id)
        .expect("readonly lookup")
        .is_none());

    // Unattested preview envelope must be rejected as active execution authority by `verify_matches_job`.
    let unattested_exec_err = preview_unattested
        .envelope
        .verify_matches_job(&job)
        .expect_err("unattested preview envelope must not grant execution authority");
    assert_eq!(
        unattested_exec_err.code(),
        NativeErrorCode::UnauthorizedOperator.as_str()
    );

    // 2. Preview with `operator_attested = true` produces the exact same authority `envelope_digest`
    // and still does not write anything to disk.
    let mut preview_true_req = preview_req.clone();
    preview_true_req.operator_attested = true;
    let preview_attested = fixture
        .policy_store
        .preview_execution_envelope(&preview_true_req, &job, &fixture.authority)
        .expect("preview with operator_attested = true");
    assert_eq!(
        preview_attested.envelope_digest,
        preview_unattested.envelope_digest
    );
    assert_eq!(preview_attested.envelope_id, preview_unattested.envelope_id);
    assert!(fixture
        .policy_store
        .get_job_runtime_readonly(BINDING_A, &job.job_id)
        .expect("readonly lookup after second preview")
        .is_none());

    // 3. Recording with `operator_attested = true` and both confirmed digests from `preview(false)`
    // succeeds and persists the attested envelope.
    let record_req = attested_record_request_from_preview(&preview_req, &preview_unattested);
    let recorded = fixture
        .policy_store
        .record_execution_envelope(&record_req, &job, &fixture.authority)
        .expect("record attested execution envelope");
    assert_eq!(recorded.runtime_revision, 1);
    assert!(recorded.envelope.operator_attested);
    assert_eq!(
        recorded.envelope.envelope_digest,
        preview_unattested.envelope_digest
    );
    recorded
        .envelope
        .verify_matches_job(&job)
        .expect("recorded attested envelope passes verify_matches_job");

    // 4. Exact replay is idempotent and preserves `runtime_revision == 1`.
    let replayed = fixture
        .policy_store
        .record_execution_envelope(&record_req, &job, &fixture.authority)
        .expect("idempotent replay of identical record request");
    assert_eq!(replayed, recorded);
    assert_eq!(replayed.runtime_revision, 1);

    // 5. Attempting to overwrite an already-recorded job envelope with a different valid envelope
    // (even with a freshly previewed and confirmed digest) fails as immutable.
    let mut conflicting_preview_req = preview_req;
    conflicting_preview_req.max_in_scope_corrections_per_task = 2;
    let conflicting_preview = fixture
        .policy_store
        .preview_execution_envelope(&conflicting_preview_req, &job, &fixture.authority)
        .expect("preview conflicting envelope");
    let conflicting_record_req =
        attested_record_request_from_preview(&conflicting_preview_req, &conflicting_preview);
    let overwrite_err = fixture
        .policy_store
        .record_execution_envelope(&conflicting_record_req, &job, &fixture.authority)
        .expect_err("recorded execution envelope must be immutable");
    assert_eq!(
        overwrite_err.code(),
        NativeErrorCode::ValidationFailed.as_str()
    );
    assert_eq!(
        fixture
            .policy_store
            .get_job_runtime_readonly(BINDING_A, &job.job_id)
            .expect("read preserved state")
            .expect("exists"),
        recorded
    );
}

#[test]
fn record_rejects_missing_or_mismatched_confirmations_and_unattested_requests_even_with_valid_coder_approval_digest(
) {
    let fixture = setup_fixture();
    let job = create_bound_job(&fixture, "brf_missing_confirm", "plan-missing-confirm");
    let preview_req = sample_preview_request(&fixture, &job);
    let preview = fixture
        .policy_store
        .preview_execution_envelope(&preview_req, &job, &fixture.authority)
        .expect("preview");

    // 1. `operator_attested = false` rejected even when both digests match.
    let mut unattested_req = attested_record_request_from_preview(&preview_req, &preview);
    unattested_req.operator_attested = false;
    let err = fixture
        .policy_store
        .record_execution_envelope(&unattested_req, &job, &fixture.authority)
        .expect_err("unattested record must fail");
    assert_eq!(err.code(), NativeErrorCode::UnauthorizedOperator.as_str());

    // 2. Missing `confirm_envelope_digest` rejected even when `confirm_approval_subject_digest` matches.
    let mut missing_env_digest_req = attested_record_request_from_preview(&preview_req, &preview);
    missing_env_digest_req.confirm_envelope_digest = None;
    let err = fixture
        .policy_store
        .record_execution_envelope(&missing_env_digest_req, &job, &fixture.authority)
        .expect_err("missing confirm_envelope_digest must fail");
    assert_eq!(err.code(), NativeErrorCode::UnauthorizedOperator.as_str());

    // 3. Wrong `confirm_envelope_digest` rejected even when `confirm_approval_subject_digest` matches.
    let mut wrong_env_digest_req = attested_record_request_from_preview(&preview_req, &preview);
    wrong_env_digest_req.confirm_envelope_digest = Some("0".repeat(64));
    let err = fixture
        .policy_store
        .record_execution_envelope(&wrong_env_digest_req, &job, &fixture.authority)
        .expect_err("wrong confirm_envelope_digest must fail");
    assert_eq!(err.code(), NativeErrorCode::UnauthorizedOperator.as_str());

    // 4. Missing or wrong `confirm_approval_subject_digest` rejected even when `confirm_envelope_digest` matches.
    let mut missing_apr_digest_req = attested_record_request_from_preview(&preview_req, &preview);
    missing_apr_digest_req.confirm_approval_subject_digest = None;
    let err = fixture
        .policy_store
        .record_execution_envelope(&missing_apr_digest_req, &job, &fixture.authority)
        .expect_err("missing confirm_approval_subject_digest must fail");
    assert_eq!(err.code(), NativeErrorCode::UnauthorizedOperator.as_str());

    let mut wrong_apr_digest_req = attested_record_request_from_preview(&preview_req, &preview);
    wrong_apr_digest_req.confirm_approval_subject_digest = Some("f".repeat(64));
    let err = fixture
        .policy_store
        .record_execution_envelope(&wrong_apr_digest_req, &job, &fixture.authority)
        .expect_err("wrong confirm_approval_subject_digest must fail");
    assert_eq!(err.code(), NativeErrorCode::UnauthorizedOperator.as_str());

    assert!(fixture
        .policy_store
        .get_job_runtime_readonly(BINDING_A, &job.job_id)
        .expect("readonly lookup")
        .is_none());
}

#[test]
fn record_rejects_post_preview_drift_across_checks_roles_caps_budgets_and_git_powers_with_old_preview_digest(
) {
    let fixture = setup_fixture();
    let job = create_bound_job(
        &fixture,
        "brf_post_preview_drift",
        "plan-post-preview-drift",
    );
    let preview_req = sample_preview_request(&fixture, &job);
    let preview = fixture
        .policy_store
        .preview_execution_envelope(&preview_req, &job, &fixture.authority)
        .expect("preview");
    let base_record = attested_record_request_from_preview(&preview_req, &preview);

    // Every mutation below remains valid under the job's approval linkage, policy, and capability
    // envelope, and still carries the valid `confirm_approval_subject_digest`, but mutates a
    // controller-specific field after preview while reusing `preview.envelope_digest`.
    let mut mutated_requests: Vec<(&str, FreezeControllerExecutionEnvelopeRequest)> = Vec::new();

    // 1. Check command argv changed.
    let mut req_argv = base_record.clone();
    req_argv
        .check_commands
        .get_mut("cargo_test")
        .expect("cargo_test")
        .argv = vec!["test".to_string(), "--lib".to_string()];
    mutated_requests.push(("check_argv", req_argv));

    // 2. Check command executable changed to another valid canonical executable.
    let mut req_exe = base_record.clone();
    req_exe
        .check_commands
        .get_mut("cargo_test")
        .expect("cargo_test")
        .executable = fixture.fake_cargo_alt_exe.clone();
    mutated_requests.push(("check_executable", req_exe));

    // 3. Check command repo_relative_cwd changed.
    let mut req_cwd = base_record.clone();
    req_cwd
        .check_commands
        .get_mut("cargo_test")
        .expect("cargo_test")
        .repo_relative_cwd = "src".to_string();
    mutated_requests.push(("check_cwd", req_cwd));

    // 4. Check command timeout / output budget changed.
    let mut req_check_budget = base_record.clone();
    req_check_budget
        .check_commands
        .get_mut("cargo_test")
        .expect("cargo_test")
        .timeout_ms = DEFAULT_TIMEOUT_MS / 2;
    mutated_requests.push(("check_timeout_budget", req_check_budget));

    // 5. Reviewer choice changed (model/tool/effort).
    let mut req_reviewer = base_record.clone();
    req_reviewer.reviewer_choice = headless_deepseek_choice();
    mutated_requests.push(("reviewer_choice", req_reviewer));

    // 6. Closure choice changed (model/tool/effort).
    let mut req_closure = base_record.clone();
    req_closure.closure_choice = headless_deepseek_choice();
    mutated_requests.push(("closure_choice", req_closure));

    // 7. Per-task correction cap changed.
    let mut req_task_corr = base_record.clone();
    req_task_corr.max_in_scope_corrections_per_task = 2;
    mutated_requests.push(("max_in_scope_corrections_per_task", req_task_corr));

    // 8. Job-wide correction cap changed.
    let mut req_job_corr = base_record.clone();
    req_job_corr.max_total_corrections_per_job = 3;
    mutated_requests.push(("max_total_corrections_per_job", req_job_corr));

    // 9. Job-wide total task attempts cap changed.
    let mut req_job_attempts = base_record.clone();
    req_job_attempts.max_total_task_attempts_per_job = 6;
    mutated_requests.push(("max_total_task_attempts_per_job", req_job_attempts));

    // 10. Role execution budget changed.
    let mut req_role_budget = base_record.clone();
    req_role_budget
        .role_budgets
        .get_mut(&WorkflowRole::Reviewer)
        .expect("reviewer budget")
        .max_total_calls = 8;
    mutated_requests.push(("reviewer_role_budget", req_role_budget));

    // 11. Git powers changed (task worktree commits, RunIntegration execution, strategy).
    let mut req_git_commit = base_record.clone();
    req_git_commit.allow_task_worktree_commits = false;
    mutated_requests.push(("allow_task_worktree_commits", req_git_commit));

    let mut req_git_integrate = base_record.clone();
    req_git_integrate.allow_run_integration_execution = false;
    mutated_requests.push(("allow_run_integration_execution", req_git_integrate));

    let mut req_git_strategy = base_record.clone();
    req_git_strategy.default_integration_strategy = IntegrationStrategy::MergeNoFf;
    mutated_requests.push(("default_integration_strategy", req_git_strategy));

    for (label, mutated_req) in mutated_requests {
        let err = fixture
            .policy_store
            .record_execution_envelope(&mutated_req, &job, &fixture.authority)
            .expect_err("post-preview mutation with stale preview digest must be rejected");
        assert_eq!(
            err.code(),
            NativeErrorCode::UnauthorizedOperator.as_str(),
            "post-preview mutation `{label}` must fail digest confirmation"
        );
    }

    // Shell wrapper binaries (`sh`) and shell-eval flags (`-c`) in check specs are rejected as security violations.
    let mut shell_exe_req = base_record.clone();
    shell_exe_req
        .check_commands
        .get_mut("cargo_test")
        .expect("cargo_test")
        .executable = fixture.fake_sh_exe.clone();
    let shell_err = fixture
        .policy_store
        .preview_execution_envelope(&shell_exe_req, &job, &fixture.authority)
        .expect_err("shell binary in check spec must be rejected");
    assert_eq!(
        shell_err.code(),
        NativeErrorCode::SecurityViolation.as_str()
    );

    let mut shell_flag_req = base_record;
    shell_flag_req
        .check_commands
        .get_mut("cargo_test")
        .expect("cargo_test")
        .argv = vec!["-c".to_string(), "cargo test".to_string()];
    let flag_err = fixture
        .policy_store
        .preview_execution_envelope(&shell_flag_req, &job, &fixture.authority)
        .expect_err("-c shell evaluation flag in check spec must be rejected");
    assert_eq!(flag_err.code(), NativeErrorCode::SecurityViolation.as_str());

    assert!(fixture
        .policy_store
        .get_job_runtime_readonly(BINDING_A, &job.job_id)
        .expect("readonly lookup")
        .is_none());
}

#[test]
fn preview_and_record_reject_stale_registry_superseded_plan_and_scope_drift() {
    let fixture = setup_fixture();
    let job = create_bound_job(&fixture, "brf_stale_guards", "plan-stale-guards");
    let preview_req = sample_preview_request(&fixture, &job);
    let preview = fixture
        .policy_store
        .preview_execution_envelope(&preview_req, &job, &fixture.authority)
        .expect("initial preview succeeds");

    // 1. Thread UUID scope mismatch is rejected.
    let mut wrong_thread_req = attested_record_request_from_preview(&preview_req, &preview);
    wrong_thread_req.thread_uuid = THREAD_UUID_B.to_string();
    let thread_err = fixture
        .policy_store
        .record_execution_envelope(&wrong_thread_req, &job, &fixture.authority)
        .expect_err("wrong thread_uuid must fail");
    assert_eq!(
        thread_err.code(),
        NativeErrorCode::ValidationFailed.as_str()
    );

    // 2. Stale expected_job_revision is rejected.
    let mut wrong_rev_req = attested_record_request_from_preview(&preview_req, &preview);
    wrong_rev_req.expected_job_revision = job.job_revision + 1;
    let rev_err = fixture
        .policy_store
        .record_execution_envelope(&wrong_rev_req, &job, &fixture.authority)
        .expect_err("mismatched expected_job_revision must fail");
    assert_eq!(rev_err.code(), NativeErrorCode::ValidationFailed.as_str());

    // 3. Live RegistryStore drift after job approval binding is rejected.
    let current_reg_rev = fixture.registry.snapshot().expect("snap").revision;
    fixture
        .registry
        .apply(
            current_reg_rev,
            RegistryChange::SetDefaultProfile {
                profile_id: Some("profile-coworker".to_string()),
            },
        )
        .expect("advance registry revision");
    let reg_drift_err = fixture
        .policy_store
        .record_execution_envelope(
            &attested_record_request_from_preview(&preview_req, &preview),
            &job,
            &fixture.authority,
        )
        .expect_err("stale registry revision must be rejected");
    assert_eq!(
        reg_drift_err.code(),
        NativeErrorCode::ValidationFailed.as_str()
    );

    // 4. On a fresh fixture, superseding the approved PlanRevision in PlanStore rejects recording.
    let fixture_plan = setup_fixture();
    let job_plan = create_bound_job(&fixture_plan, "brf_superseded_plan", "plan-superseded");
    let preview_req_plan = sample_preview_request(&fixture_plan, &job_plan);
    let preview_plan = fixture_plan
        .policy_store
        .preview_execution_envelope(&preview_req_plan, &job_plan, &fixture_plan.authority)
        .expect("preview before plan supersede");

    let reg_rev = fixture_plan.registry.snapshot().expect("snap").revision;
    let draft2 = fixture_plan
        .plans
        .change(
            1,
            PlanChange::SaveDraft {
                plan_id: "plan-superseded".to_string(),
                binding_id: BINDING_A.to_string(),
                profile_id: "profile-coworker".to_string(),
                profile_revision: reg_rev,
                summary: "Superseding revision".to_string(),
                tasks: vec![PlanTask {
                    id: "task-1".to_string(),
                    title: "Task 1 v2".to_string(),
                    instruction: "Updated instruction".to_string(),
                    role: WorkflowRole::Coder,
                    difficulty: Difficulty::Hard,
                    depends_on: Vec::new(),
                    allowed_write_paths: vec!["src/lib.rs".to_string()],
                }],
            },
        )
        .expect("save draft 2");
    fixture_plan
        .plans
        .change(
            draft2.revision,
            PlanChange::Publish {
                plan_id: "plan-superseded".to_string(),
                revision: draft2.revision,
            },
        )
        .expect("publish revision 2");

    let superseded_err = fixture_plan
        .policy_store
        .record_execution_envelope(
            &attested_record_request_from_preview(&preview_req_plan, &preview_plan),
            &job_plan,
            &fixture_plan.authority,
        )
        .expect_err("superseded plan revision must be rejected");
    assert_eq!(
        superseded_err.code(),
        NativeErrorCode::ValidationFailed.as_str()
    );
}
