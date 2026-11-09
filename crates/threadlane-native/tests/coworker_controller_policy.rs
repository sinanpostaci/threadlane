use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use tempfile::TempDir;
use threadlane_authority::{AuthorityStore, OperatorApprovalRequest};
use threadlane_native::coworker_workflow::controller_policy::{
    deterministic_controller_instance_id, CheckFailureCorrectionBasis, CheckFailureReportRef,
    ControllerConfigurationPreview, ControllerPolicyStore, ControllerRoleExecutionBudget,
    DerivedTaskAttemptReceipt, FreezeControllerExecutionEnvelopeRequest, FrozenCheckCommandSpec,
    FrozenCheckKind,
};
use threadlane_native::coworker_workflow::{
    sha256_hex, BindJobApprovalRequest, CanonicalProjectBaseRef, CoworkerJobRecord,
    CoworkerLockFileIdentity, CoworkerTaskStage, CoworkerWorkflowStore, CreateCoworkerJobRequest,
    FrozenCheckExecutionReport, FrozenCheckExecutionStatus, FrozenCheckSubject,
    FrozenCheckTargetKind, FrozenControllerCapabilityEnvelope, WorkBrief, WorkBriefKind,
    FROZEN_CHECK_EXECUTION_SCHEMA_VERSION, MAX_COWORKER_ID_LEN,
};
use threadlane_native::{
    Difficulty, ExecutionPolicy, NativeErrorCode, PolicyAllowRule, RegistryStore,
    RoleExecutionRoute, SelectedRoleChoice, ToolAdapter, WorkflowProfile, WorkflowRole,
};
use threadlane_operations::IntegrationStrategy;
use threadlane_workers::{
    resolve_exact_commit_sha, run_sanitized_git, BoundedCommandOverflow, ExecutionBudgets,
    DEFAULT_GIT_COMMAND_TIMEOUT_MS, DEFAULT_MAX_STDERR_BYTES, DEFAULT_MAX_STDOUT_BYTES,
    DEFAULT_TIMEOUT_MS,
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
                        id: "opencode-go/deepseek-v4.1-flash".to_string(),
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
                                model_id: "opencode-go/deepseek-v4.1-flash".to_string(),
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
        model_id: "opencode-go/deepseek-v4.1-flash".to_string(),
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
                model_id: "opencode-go/deepseek-v4.1-flash".to_string(),
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
    let (_, request) = prepare_job_for_binding(fixture, brief_id, plan_id);
    fixture
        .workflow_store
        .bind_job_approval(&request, &fixture.authority)
        .expect("bind job approval")
}

fn prepare_job_for_binding(
    fixture: &ControllerPolicyFixture,
    brief_id: &str,
    plan_id: &str,
) -> (CoworkerJobRecord, BindJobApprovalRequest) {
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
        allow_check_failure_corrections: false,
    };

    let request = BindJobApprovalRequest {
        binding_id: BINDING_A.to_string(),
        thread_uuid: THREAD_UUID_A.to_string(),
        job_id: job.job_id.clone(),
        expected_job_revision: job.job_revision,
        expected_ledger_revision: None,
        approval_id: summary.approval_id,
        policy: execution_policy(),
        controller_envelope: Some(cap_env),
        task_interfaces: BTreeMap::new(),
    };
    (job, request)
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

mod admission_regressions {
    use super::*;
    use threadlane_native::coworker_admission::{
        verify_complete_admission, AdmissionConfirmation, AdmissionInspectionState,
        AdmissionPersistenceBoundary, AdmissionPreview, AdmissionReceipt, AdmissionRequest,
        AdmissionStage, CoworkerAdmissionService,
    };
    use threadlane_native::coworker_workflow::{
        CoworkerLedgerMutationOutcome, CreatePlanningGrantRequest, FinalizePlanningCallRequest,
        PlanningCallBudget, PlanningReadOnlyCapabilities, PlanningReservationStatus,
        PlanningResultRef, RecordJobPlanningResultRequest, ReservePlanningCallRequest,
    };

    fn prepare(f: &ControllerPolicyFixture) -> AdmissionRequest {
        let (job, mut binding) = prepare_job_for_binding(f, "brief-admission", "plan-admission");
        let mut planning_policy = execution_policy();
        planning_policy.policy_id = "policy-admission-planning".into();
        planning_policy.allow_rules.truncate(1);
        planning_policy.allow_rules[0].roles =
            vec![WorkflowRole::Orchestrator, WorkflowRole::Architect];
        let grant = f
            .workflow_store
            .create_planning_grant(&CreatePlanningGrantRequest {
                binding_id: BINDING_A.into(),
                thread_uuid: THREAD_UUID_A.into(),
                request_key: "grant-admission".into(),
                expected_ledger_revision: None,
                project_base: project_base(f),
                profile_id: "profile-coworker".into(),
                registry_revision: f.registry.snapshot_readonly().unwrap().revision,
                policy: planning_policy,
                capabilities: PlanningReadOnlyCapabilities::strict_read_only(),
                budget: PlanningCallBudget {
                    max_total_calls: 2,
                    max_orchestrator_calls: 1,
                    max_architect_calls: 1,
                    max_timeout_ms_per_call: 30_000,
                    max_output_bytes_per_call: 65_536,
                },
                created_at_ms: 1_000,
                expires_at_ms: 10_000,
                operator_attested: true,
            })
            .unwrap();
        let reservation = f
            .workflow_store
            .reserve_planning_call(&ReservePlanningCallRequest {
                binding_id: BINDING_A.into(),
                thread_uuid: THREAD_UUID_A.into(),
                grant_id: grant.grant_id.clone(),
                request_key: "planning-admission".into(),
                expected_ledger_revision: None,
                role: WorkflowRole::Orchestrator,
                difficulty: Some(Difficulty::Hard),
                selected_choice: headless_argon_choice(),
                context_digest: job.work_brief_digest.clone(),
                call_project_base: Some(project_base(f)),
                current_time_ms: 2_000,
            })
            .unwrap();
        let output_digest = sha256_hex(b"admission planning fixture output");
        f.workflow_store
            .finalize_planning_call(&FinalizePlanningCallRequest {
                binding_id: BINDING_A.into(),
                thread_uuid: THREAD_UUID_A.into(),
                grant_id: grant.grant_id.clone(),
                reservation_id: reservation.reservation_id.clone(),
                expected_ledger_revision: None,
                status: PlanningReservationStatus::Completed,
                updated_at_ms: 2_100,
                run_id: Some("run-admission".into()),
                report_artifact_id: Some("report-admission".into()),
                output_digest: Some(output_digest.clone()),
                diagnostic: None,
            })
            .unwrap();
        let approval = f
            .authority
            .get_for_operator_readonly(&binding.approval_id)
            .unwrap();
        let planning = PlanningResultRef {
            grant_id: grant.grant_id,
            reservation_id: reservation.reservation_id,
            request_key: reservation.request_key,
            run_id: "run-admission".into(),
            report_artifact_id: "report-admission".into(),
            output_digest,
            work_brief_digest: job.work_brief_digest.clone(),
            project_base: project_base(f),
            profile_id: approval.profile_id,
            registry_revision: approval.registry_revision,
            policy_digest: grant.policy_digest,
            selected_choice: headless_argon_choice(),
            plan_id: approval.plan_id,
            plan_revision: approval.plan_revision,
            plan_digest: approval.plan_digest,
            plan_status: threadlane_workflow::PlanStatus::Published,
            task_interfaces: BTreeMap::new(),
            architect_advice_ids: vec![],
            resolved_open_questions: vec![],
            deferred_proposals: vec![],
            recorded_at_ms: 2_100,
        };
        let job = f
            .workflow_store
            .record_job_planning_result(&RecordJobPlanningResultRequest {
                binding_id: BINDING_A.into(),
                thread_uuid: THREAD_UUID_A.into(),
                job_id: job.job_id,
                expected_job_revision: Some(job.job_revision),
                expected_ledger_revision: None,
                planning_result: planning.clone(),
            })
            .unwrap();
        binding.expected_job_revision = job.job_revision;
        binding.expected_ledger_revision = Some(
            f.workflow_store
                .read_binding_ledger(BINDING_A)
                .unwrap()
                .ledger_revision,
        );
        let mut execution = sample_preview_request(f, &job);
        execution.closure_choice.matched_difficulty = Some(Difficulty::Extreme);
        binding
            .controller_envelope
            .as_mut()
            .unwrap()
            .allowed_closure_choices
            .push(execution.closure_choice.clone());
        AdmissionRequest {
            request_key: "admission-1".into(),
            binding,
            planning_result: planning,
            work_brief_digest: job.work_brief_digest,
            execution,
            reviewer_difficulty: Some(Difficulty::Hard),
            closure_difficulty: Some(Difficulty::Extreme),
        }
    }

    fn service(f: &ControllerPolicyFixture) -> CoworkerAdmissionService {
        CoworkerAdmissionService::new(f.plans.dir(), f.authority.dir(), f.registry.config_dir())
            .unwrap()
    }

    fn confirmation(p: &AdmissionPreview) -> AdmissionConfirmation {
        AdmissionConfirmation {
            trusted_local_attested: true,
            preview_digest: p.preview_digest.clone(),
            approval_subject_digest: p.approval_subject_digest.clone(),
            envelope_digest: p.envelope.envelope_digest.clone(),
        }
    }

    fn snapshot(dir: &Path) -> BTreeMap<PathBuf, (Vec<u8>, std::time::SystemTime)> {
        let mut files = BTreeMap::new();
        for entry in fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                files.extend(snapshot(&path));
            } else {
                files.insert(
                    path.clone(),
                    (
                        fs::read(&path).unwrap(),
                        fs::metadata(&path).unwrap().modified().unwrap(),
                    ),
                );
            }
        }
        files
    }

    fn receipt_path(f: &ControllerPolicyFixture) -> PathBuf {
        fs::read_dir(f.plans.dir())
            .unwrap()
            .map(|e| e.unwrap().path())
            .find(|p| {
                p.file_name()
                    .unwrap()
                    .to_string_lossy()
                    .starts_with("coworker-admission-")
                    && p.extension().is_some_and(|e| e == "json")
            })
            .unwrap()
    }

    fn unrelated_unknown_job(f: &ControllerPolicyFixture) -> CoworkerJobRecord {
        use threadlane_native::coworker_workflow::{
            FinalizeTaskOutboxRequest, OutboxDispatchKind, OutboxDispatchStatus,
            ReserveTaskOutboxRequest,
        };
        let unrelated = create_bound_job(f, "brief-unrelated", "plan-unrelated");
        let (job, outbox) = f
            .workflow_store
            .reserve_task_outbox(&ReserveTaskOutboxRequest {
                binding_id: BINDING_A.into(),
                thread_uuid: THREAD_UUID_A.into(),
                job_id: unrelated.job_id,
                expected_job_revision: unrelated.job_revision,
                expected_ledger_revision: None,
                request_key: "unrelated-unknown-attempt".into(),
                kind: OutboxDispatchKind::TaskAttemptDispatch,
                task_id: "task-1".into(),
                role: WorkflowRole::Coder,
                subject_digest: sha256_hex(b"unknown unrelated effect"),
                dependency_input_commit: None,
                dependency_input_tree_sha: None,
                accepted_predecessor_receipts: vec![],
            })
            .unwrap();
        f.workflow_store
            .reconcile_or_finalize_outbox(&FinalizeTaskOutboxRequest {
                binding_id: BINDING_A.into(),
                thread_uuid: THREAD_UUID_A.into(),
                job_id: job.job_id,
                expected_job_revision: job.job_revision,
                expected_ledger_revision: None,
                outbox_id: outbox.outbox_id,
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
                diagnostic: Some("Unknown fixture effect; never replay".into()),
            })
            .unwrap()
            .0
    }

    #[test]
    fn preview_is_pure_and_recorded_duplicates_preserve_bytes_mtimes_and_runtime_progress() {
        let f = setup_fixture();
        let unrelated = unrelated_unknown_job(&f);
        let request = prepare(&f);
        let api = service(&f);
        fs::write(
            f.plans.dir().join("unrelated-artifact"),
            b"immutable private fixture",
        )
        .unwrap();
        let before = snapshot(f._tmp.path());
        let preview = api.preview(&request).unwrap();
        assert_eq!(snapshot(f._tmp.path()), before);
        assert_eq!(
            preview.proposed_job.job_revision,
            request.binding.expected_job_revision + 1
        );
        assert!(preview.proposed_job.approval_linkage.is_some());
        assert!(!preview.envelope.operator_attested);
        let consent = confirmation(&preview);
        let receipt = api.record(&request, &consent).unwrap();
        assert_eq!(receipt.stage, AdmissionStage::Complete);
        f.policy_store
            .mutate_job_runtime_for_operator(BINDING_A, &request.binding.job_id, |state| {
                state.stop_requested = true;
                state.total_task_attempts_reserved = 1;
                Ok(())
            })
            .unwrap();
        let after = snapshot(f._tmp.path());
        // A witnessed duplicate must not acquire the admission lock or sync existing files.
        // Holding that lock here would make a false fast path contend with itself.
        let admission_lock = fs::read_dir(f.plans.dir())
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .find(|path| {
                path.file_name()
                    .unwrap()
                    .to_string_lossy()
                    .starts_with("coworker-admission-")
                    && path
                        .extension()
                        .is_some_and(|extension| extension == "lock")
            })
            .unwrap();
        let lock = threadlane_native::fs_safety::acquire_cross_process_lock(
            f.plans.dir(),
            admission_lock.file_name().unwrap().to_str().unwrap(),
            "duplicate fixture lock",
        )
        .unwrap();
        let duplicate = std::thread::scope(|scope| {
            let (sender, receiver) = std::sync::mpsc::sync_channel(1);
            let (api, request, consent) = (&api, &request, &consent);
            scope.spawn(move || {
                let _ = sender.send(api.record(request, consent));
            });
            let duplicate = receiver.recv_timeout(std::time::Duration::from_secs(5));
            // Release before propagating a failed assertion so a regressed waiter is joined.
            drop(lock);
            duplicate
        });
        assert_eq!(
            duplicate
                .expect("witnessed duplicate must not wait for admission lock")
                .unwrap(),
            receipt
        );
        assert_eq!(snapshot(f._tmp.path()), after);
        let ledger = f.workflow_store.read_binding_ledger(BINDING_A).unwrap();
        assert_eq!(
            ledger
                .jobs
                .iter()
                .find(|j| j.job_id == unrelated.job_id)
                .unwrap(),
            &unrelated
        );
        let job = ledger
            .jobs
            .iter()
            .find(|j| j.job_id == request.binding.job_id)
            .unwrap();
        verify_complete_admission(&f.workflow_store, &f.policy_store, job).unwrap();
        assert!(
            f.policy_store
                .get_job_runtime_readonly(BINDING_A, &job.job_id)
                .unwrap()
                .unwrap()
                .stop_requested
        );
        assert_eq!(
            fs::read(f.plans.dir().join("unrelated-artifact")).unwrap(),
            b"immutable private fixture"
        );
    }

    #[test]
    fn preview_rejects_identity_scope_revision_and_allowlisted_but_unresolved_choices() {
        let f = setup_fixture();
        let request = prepare(&f);
        let api = service(&f);
        let baseline = snapshot(f._tmp.path());
        let mut cases = Vec::new();
        let mut r = request.clone();
        r.binding.thread_uuid = THREAD_UUID_B.into();
        cases.push(r);
        let mut r = request.clone();
        r.binding.expected_job_revision += 1;
        r.execution.expected_job_revision += 1;
        cases.push(r);
        let mut r = request.clone();
        *r.binding.expected_ledger_revision.as_mut().unwrap() += 1;
        cases.push(r);
        let mut r = request.clone();
        r.work_brief_digest = sha256_hex(b"wrong brief");
        cases.push(r);
        let mut r = request.clone();
        r.planning_result.plan_digest = sha256_hex(b"wrong plan");
        cases.push(r);
        let mut r = request.clone();
        r.planning_result.registry_revision += 1;
        cases.push(r);
        let mut r = request.clone();
        r.planning_result.project_base.base_tree_sha = "1".repeat(40);
        cases.push(r);
        let mut r = request.clone();
        r.execution.reviewer_choice = headless_deepseek_choice();
        cases.push(r);
        let mut r = request.clone();
        r.execution.closure_choice = headless_deepseek_choice();
        cases.push(r);
        let mut r = request.clone();
        r.binding
            .task_interfaces
            .insert("task-not-approved".into(), Default::default());
        cases.push(r);
        for changed in cases {
            assert!(api.preview(&changed).is_err());
        }
        assert_eq!(snapshot(f._tmp.path()), baseline);
    }

    #[test]
    fn every_serialized_consent_field_is_bound_and_confirmation_flags_are_not_authority() {
        let f = setup_fixture();
        let request = prepare(&f);
        let api = service(&f);
        let preview = api.preview(&request).unwrap();
        let consent = confirmation(&preview);
        let mut changed = request.clone();
        changed.execution.operator_attested = true;
        changed.execution.confirm_approval_subject_digest = Some("irrelevant".into());
        assert_eq!(
            api.preview(&changed).unwrap().preview_digest,
            preview.preview_digest
        );
        // Every scalar in every nested consent-bearing field is included in the preview subject.
        fn leaves(
            value: &serde_json::Value,
            path: &mut Vec<String>,
            output: &mut Vec<Vec<String>>,
        ) {
            match value {
                serde_json::Value::Object(map) => {
                    for (key, value) in map {
                        path.push(key.clone());
                        leaves(value, path, output);
                        path.pop();
                    }
                }
                serde_json::Value::Array(values) => {
                    for (index, value) in values.iter().enumerate() {
                        path.push(index.to_string());
                        leaves(value, path, output);
                        path.pop();
                    }
                }
                _ => output.push(path.clone()),
            }
        }
        let original = serde_json::to_value(&preview).unwrap();
        let mut paths = Vec::new();
        leaves(&original, &mut Vec::new(), &mut paths);
        for path in paths {
            if path
                .last()
                .is_some_and(|p| p == "preview_digest" || p == "operator_attested")
            {
                continue;
            }
            let mut value = original.clone();
            let mut target = &mut value;
            for key in &path {
                target = match target {
                    serde_json::Value::Array(values) => &mut values[key.parse::<usize>().unwrap()],
                    serde_json::Value::Object(map) => map.get_mut(key).unwrap(),
                    _ => unreachable!(),
                };
            }
            *target = match target.clone() {
                serde_json::Value::String(mut text) => {
                    text.push('x');
                    text.into()
                }
                serde_json::Value::Number(number) => (number.as_u64().unwrap_or(0) + 1).into(),
                serde_json::Value::Bool(boolean) => (!boolean).into(),
                _ => "changed".into(),
            };
            if let Ok(changed) = serde_json::from_value::<AdmissionPreview>(value) {
                assert_ne!(
                    changed.compute_digest().ok().as_ref(),
                    Some(&preview.preview_digest),
                    "unbound field {path:?}"
                );
            }
        }
        let mut changed = request.clone();
        changed.execution.created_at_ms += 1;
        assert!(api.record(&changed, &consent).is_err());
        let mut missing = consent;
        missing.trusted_local_attested = false;
        assert!(api.record(&request, &missing).is_err());
    }

    #[test]
    fn missing_tampered_or_unsupported_receipts_fail_closed_without_repair() {
        let f = setup_fixture();
        let request = prepare(&f);
        let api = service(&f);
        let preview = api.preview(&request).unwrap();
        api.record(&request, &confirmation(&preview)).unwrap();
        let path = receipt_path(&f);
        let bytes = fs::read(&path).unwrap();
        fs::remove_file(&path).unwrap();
        assert_eq!(
            api.inspect(BINDING_A, &request.binding.job_id)
                .unwrap()
                .state,
            AdmissionInspectionState::Conflicting
        );
        let ledger = f.workflow_store.read_binding_ledger(BINDING_A).unwrap();
        let job = ledger
            .jobs
            .iter()
            .find(|j| j.job_id == request.binding.job_id)
            .unwrap();
        assert!(verify_complete_admission(&f.workflow_store, &f.policy_store, job).is_err());
        assert!(api.reconcile(BINDING_A, &request.binding.job_id).is_err());
        for corrupt in [b"broken-json".to_vec(), {
            let mut value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            value["schema_version"] = 99.into();
            serde_json::to_vec(&value).unwrap()
        }] {
            threadlane_native::fs_safety::write_private_file_atomic(
                f.plans.dir(),
                path.file_name().unwrap().to_str().unwrap(),
                &corrupt,
                "fixture",
            )
            .unwrap();
            assert!(api.inspect(BINDING_A, &request.binding.job_id).is_err());
            assert_eq!(fs::read(&path).unwrap(), corrupt);
        }
    }

    #[test]
    fn concurrent_identical_records_create_one_admission_and_conflicting_reuse_is_refused() {
        let f = setup_fixture();
        let request = prepare(&f);
        let api = service(&f);
        let consent = confirmation(&api.preview(&request).unwrap());
        let barrier = std::sync::Barrier::new(2);
        std::thread::scope(|scope| {
            let first = scope.spawn(|| {
                barrier.wait();
                api.record(&request, &consent)
            });
            let second = scope.spawn(|| {
                barrier.wait();
                api.record(&request, &consent)
            });
            assert_eq!(
                first.join().unwrap().unwrap(),
                second.join().unwrap().unwrap()
            );
        });
        let before = snapshot(f._tmp.path());
        let mut changed = request.clone();
        changed.request_key = "conflicting-key".into();
        assert!(api.record(&changed, &consent).is_err());
        assert_eq!(snapshot(f._tmp.path()), before);
    }

    #[test]
    fn partial_intent_reconciliation_preserves_original_subject_and_never_dispatches() {
        let f = setup_fixture();
        let request = prepare(&f);
        let api = service(&f);
        let preview = api.preview(&request).unwrap();
        let consent = confirmation(&preview);
        // A restart after intent persistence, before linkage, has this exact private state.
        let receipt = AdmissionReceipt {
            schema_version: 1,
            preview: preview.clone(),
            confirmation: consent,
            stage: AdmissionStage::Intent,
            runtime_initialized: false,
        };
        let seed = format!(
            "threadlane-native:admission-path:v1:{}:{}:{}",
            BINDING_A.len(),
            BINDING_A,
            request.binding.job_id
        );
        let filename = format!("coworker-admission-{}.json", sha256_hex(seed.as_bytes()));
        threadlane_native::fs_safety::write_private_file_atomic(
            f.plans.dir(),
            &filename,
            &serde_json::to_vec(&receipt).unwrap(),
            "fixture intent",
        )
        .unwrap();
        assert_eq!(
            api.inspect(BINDING_A, &request.binding.job_id)
                .unwrap()
                .state,
            AdmissionInspectionState::PartiallyRecorded
        );
        assert!(api.record(&request, &receipt.confirmation).is_err());
        let reopened = service(&f);
        let complete = reopened
            .reconcile(BINDING_A, &request.binding.job_id)
            .unwrap();
        assert_eq!(complete.preview, preview);
        assert_eq!(complete.stage, AdmissionStage::Complete);
        let runtime = f
            .policy_store
            .get_job_runtime_readonly(BINDING_A, &request.binding.job_id)
            .unwrap()
            .unwrap();
        assert_eq!(runtime.total_task_attempts_reserved, 0);
        assert!(runtime.pending_effects.is_empty());
        assert!(runtime.lease.is_none());
        assert!(runtime.attempt_artifacts.is_empty());
        // Visible bytes explicitly labelled uncertain cannot authorize an effect.
        let mut uncertain = complete;
        uncertain.stage = AdmissionStage::Uncertain;
        threadlane_native::fs_safety::write_private_file_atomic(
            f.plans.dir(),
            &filename,
            &serde_json::to_vec(&uncertain).unwrap(),
            "uncertain fixture",
        )
        .unwrap();
        assert_eq!(
            reopened
                .inspect(BINDING_A, &request.binding.job_id)
                .unwrap()
                .state,
            AdmissionInspectionState::Uncertain
        );
        let before = snapshot(f._tmp.path());
        reopened
            .inspect(BINDING_A, &request.binding.job_id)
            .unwrap();
        assert_eq!(snapshot(f._tmp.path()), before);
        reopened
            .reconcile(BINDING_A, &request.binding.job_id)
            .unwrap();
    }

    thread_local! {
        static FAIL_AT: std::cell::Cell<Option<AdmissionPersistenceBoundary>> = const { std::cell::Cell::new(None) };
    }

    fn failure_probe(
        point: AdmissionPersistenceBoundary,
    ) -> Result<(), threadlane_native::NativeError> {
        if FAIL_AT.with(|slot| slot.get()) != Some(point) {
            return Ok(());
        }
        match point {
            AdmissionPersistenceBoundary::BeforeIntent
            | AdmissionPersistenceBoundary::BeforeLinkage
            | AdmissionPersistenceBoundary::BeforeRuntime
            | AdmissionPersistenceBoundary::BeforeComplete => {
                Err(threadlane_native::NativeError::new(
                    NativeErrorCode::IoError,
                    "Injected pre-write refusal",
                ))
            }
            _ => Err(threadlane_native::NativeError::persistence_uncertain(
                "Injected lost durability acknowledgement",
            )),
        }
    }

    #[test]
    fn failures_at_every_admission_boundary_remain_fenced_and_reconcile_without_replaying_effects()
    {
        use AdmissionPersistenceBoundary::*;
        for point in [
            BeforeIntent,
            AfterIntent,
            BeforeLinkage,
            AfterLinkage,
            BeforeRuntime,
            AfterRuntime,
            BeforeComplete,
            AfterComplete,
        ] {
            let f = setup_fixture();
            let unrelated = unrelated_unknown_job(&f);
            let request = prepare(&f);
            let api = service(&f).with_failure_probe(failure_probe);
            let preview = api.preview(&request).unwrap();
            let consent = confirmation(&preview);
            FAIL_AT.with(|slot| slot.set(Some(point)));
            assert!(
                api.record(&request, &consent).is_err(),
                "boundary {point:?}"
            );
            FAIL_AT.with(|slot| slot.set(None));
            let inspection = api.inspect(BINDING_A, &request.binding.job_id).unwrap();
            assert_ne!(inspection.state, AdmissionInspectionState::Complete);
            let reopened = service(&f);
            let complete = if point == BeforeIntent {
                reopened.record(&request, &consent).unwrap()
            } else {
                reopened
                    .reconcile(BINDING_A, &request.binding.job_id)
                    .unwrap()
            };
            assert_eq!(complete.preview, preview);
            assert_eq!(
                complete.preview.request.execution.created_at_ms,
                request.execution.created_at_ms
            );
            let ledger = f.workflow_store.read_binding_ledger(BINDING_A).unwrap();
            assert_eq!(
                ledger
                    .jobs
                    .iter()
                    .find(|j| j.job_id == unrelated.job_id)
                    .unwrap(),
                &unrelated
            );
            let runtime = f
                .policy_store
                .get_job_runtime_readonly(BINDING_A, &request.binding.job_id)
                .unwrap()
                .unwrap();
            assert_eq!(runtime.total_task_attempts_reserved, 0);
            assert_eq!(runtime.total_corrections_reserved, 0);
            assert!(runtime.pending_effects.is_empty());
            assert!(runtime.attempt_artifacts.is_empty());
            assert!(runtime.lease.is_none());
            assert_eq!(runtime.runtime_revision, 1);
        }
    }

    #[test]
    fn marker_removal_is_refused_and_common_effectful_gate_enforces_missing_receipt() {
        let f = setup_fixture();
        let request = prepare(&f);
        let api = service(&f);
        let preview = api.preview(&request).unwrap();
        api.record(&request, &confirmation(&preview)).unwrap();
        let before =
            fs::read(f.workflow_store.ledger_path_for_binding(BINDING_A).unwrap()).unwrap();
        assert!(f
            .workflow_store
            .transact_binding_ledger(BINDING_A, None, |ledger| {
                ledger.admission_markers.clear();
                Ok(CoworkerLedgerMutationOutcome::Mutated(()))
            })
            .is_err());
        assert_eq!(
            fs::read(f.workflow_store.ledger_path_for_binding(BINDING_A).unwrap()).unwrap(),
            before
        );
        fs::remove_file(receipt_path(&f)).unwrap();
        let ledger = f.workflow_store.read_binding_ledger(BINDING_A).unwrap();
        let mut job = ledger
            .jobs
            .iter()
            .find(|j| j.job_id == request.binding.job_id)
            .unwrap()
            .clone();
        let mut runtime = f
            .policy_store
            .get_job_runtime_readonly(BINDING_A, &job.job_id)
            .unwrap()
            .unwrap();
        let approval = f
            .authority
            .get_for_operator_readonly(&request.binding.approval_id)
            .unwrap();
        assert!(threadlane_native::coworker_workflow::controller_policy::verify_frozen_runtime_registry_or_apply_config_hold(
            &approval, &mut job, &mut runtime, &f.authority, &f.workflow_store, &f.policy_store).is_err());
        // Explicit stop remains an operator mutation, independent of admission execution proof.
        f.policy_store
            .mutate_job_runtime_for_operator(BINDING_A, &job.job_id, |state| {
                state.stop_requested = true;
                Ok(())
            })
            .unwrap();
    }

    fn receipt_path_for_root(plans: &Path) -> PathBuf {
        fs::read_dir(plans)
            .unwrap()
            .map(|e| e.unwrap().path())
            .find(|p| {
                p.file_name()
                    .unwrap()
                    .to_string_lossy()
                    .starts_with("coworker-admission-")
                    && p.extension().is_some_and(|e| e == "json")
            })
            .unwrap()
    }

    #[test]
    fn restart_child_probe() {
        let Some(root) = std::env::var_os("THREADLANE_ADMISSION_FIXTURE_ROOT") else {
            return;
        };
        let root = PathBuf::from(root);
        let plans = root.join("plans-store");
        let api = CoworkerAdmissionService::new(
            &plans,
            &root.join("authority-store"),
            &root.join("registry-store"),
        )
        .unwrap();
        let workflow = CoworkerWorkflowStore::new(&plans).unwrap();
        let controller = ControllerPolicyStore::new(&plans).unwrap();
        let ledger = workflow.read_binding_ledger(BINDING_A).unwrap();
        let job = ledger
            .jobs
            .iter()
            .find(|j| ledger.admission_markers.contains_key(&j.job_id))
            .unwrap();
        let before = snapshot(&root);
        assert_eq!(
            api.inspect(BINDING_A, &job.job_id).unwrap().state,
            AdmissionInspectionState::Uncertain
        );
        // Pure inspection must leave even locks and mtimes unchanged in this fresh process.
        assert_eq!(snapshot(&root), before);
        // A deliberate execution boundary can sync Complete existing consent without rewriting
        // any subject, lease, counter, file bytes or mtime.
        let receipt = api
            .inspect(BINDING_A, &job.job_id)
            .unwrap()
            .receipt
            .unwrap();
        assert_eq!(
            api.record(&receipt.preview.request, &receipt.confirmation)
                .unwrap(),
            receipt
        );
        assert_eq!(snapshot(&root), before);
        verify_complete_admission(&workflow, &controller, job).unwrap();
        assert_eq!(snapshot(&root), before);
        let runtime_before =
            fs::read(controller.ledger_path_for_binding(BINDING_A).unwrap()).unwrap();
        api.reconcile(BINDING_A, &job.job_id).unwrap();
        assert_eq!(snapshot(&root), before);
        verify_complete_admission(&workflow, &controller, job).unwrap();
        // An explicitly Uncertain stage is never promoted by the execution guard, even when
        // this process previously witnessed the same Complete receipt.
        let receipt_file = receipt_path_for_root(&plans);
        let mut uncertain: AdmissionReceipt =
            serde_json::from_slice(&fs::read(&receipt_file).unwrap()).unwrap();
        uncertain.stage = AdmissionStage::Uncertain;
        threadlane_native::fs_safety::write_private_file_atomic(
            &plans,
            receipt_file.file_name().unwrap().to_str().unwrap(),
            &serde_json::to_vec(&uncertain).unwrap(),
            "uncertain restart fixture",
        )
        .unwrap();
        let uncertain_files = snapshot(&root);
        assert!(verify_complete_admission(&workflow, &controller, job).is_err());
        assert_eq!(snapshot(&root), uncertain_files);
        api.reconcile(BINDING_A, &job.job_id).unwrap();
        assert_eq!(
            fs::read(controller.ledger_path_for_binding(BINDING_A).unwrap()).unwrap(),
            runtime_before
        );
    }

    #[test]
    fn new_process_witnesses_complete_consent_and_explicitly_reconciles_uncertain_without_reset() {
        let f = setup_fixture();
        let request = prepare(&f);
        let api = service(&f);
        let preview = api.preview(&request).unwrap();
        api.record(&request, &confirmation(&preview)).unwrap();
        let result = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "admission_regressions::restart_child_probe",
                "--nocapture",
            ])
            .env("THREADLANE_ADMISSION_FIXTURE_ROOT", f._tmp.path())
            .output()
            .unwrap();
        assert!(
            result.status.success(),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
    }

    #[test]
    fn completed_admission_with_lost_runtime_never_gets_fresh_counters() {
        let f = setup_fixture();
        let request = prepare(&f);
        let api = service(&f);
        let preview = api.preview(&request).unwrap();
        let consent = confirmation(&preview);
        api.record(&request, &consent).unwrap();
        f.policy_store
            .mutate_job_runtime_for_operator(BINDING_A, &request.binding.job_id, |state| {
                state.total_task_attempts_reserved = 3;
                state.stop_requested = true;
                Ok(())
            })
            .unwrap();
        fs::remove_file(f.policy_store.ledger_path_for_binding(BINDING_A).unwrap()).unwrap();
        let before = snapshot(f._tmp.path());
        assert_eq!(
            api.inspect(BINDING_A, &request.binding.job_id)
                .unwrap()
                .state,
            AdmissionInspectionState::Conflicting
        );
        assert!(api.record(&request, &consent).is_err());
        assert!(api.reconcile(BINDING_A, &request.binding.job_id).is_err());
        assert_eq!(snapshot(f._tmp.path()), before);
    }

    #[test]
    fn historical_manual_jobs_still_pass_the_new_readonly_guard() {
        let f = setup_fixture();
        let job = create_bound_job(&f, "brief-historical", "plan-historical");
        assert!(job.planning_result.is_none());
        let before = snapshot(f._tmp.path());
        verify_complete_admission(&f.workflow_store, &f.policy_store, &job).unwrap();
        assert_eq!(snapshot(f._tmp.path()), before);
        assert!(f
            .workflow_store
            .read_binding_ledger(BINDING_A)
            .unwrap()
            .admission_markers
            .is_empty());
    }
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

#[test]
fn derived_task_attempt_receipt_preserves_v1_digest_and_validates_check_failure_correction_basis_contract(
) {
    assert!(CoworkerTaskStage::ReadyForReview
        .can_transition_to(CoworkerTaskStage::CorrectInScopeRequired));
    assert!(!CoworkerTaskStage::ReadyForReview.can_transition_to(CoworkerTaskStage::Accepted));

    let fixture = setup_fixture();
    let job = create_bound_job(
        &fixture,
        "brf_check_failure_basis",
        "plan-check-failure-basis",
    );
    let mut preview_req = sample_preview_request(&fixture, &job);
    preview_req.max_in_scope_corrections_per_task = 2;
    let preview = fixture
        .policy_store
        .preview_execution_envelope(&preview_req, &job, &fixture.authority)
        .expect("preview envelope");
    let record_req = attested_record_request_from_preview(&preview_req, &preview);
    let runtime = fixture
        .policy_store
        .record_execution_envelope(&record_req, &job, &fixture.authority)
        .expect("record envelope");
    let envelope = &runtime.envelope;
    let task = &job.tasks[0];

    let task_manifest_id = "w0123456789abcdef";
    assert_ne!(task.task_id, task_manifest_id);

    let committed_head = "1111111111111111111111111111111111111111";
    let committed_tree = "2222222222222222222222222222222222222222";
    let prior_candidate_id = "tcd_0123456789abcdef0123456789abcdef";
    let prior_verdict_id = "rvd_fedcba9876543210fedcba9876543210";
    let prior_review_subject_digest = sha256_hex(b"prior-review-subject-v1");

    // 1. Legacy v1 receipt with `prior_check_failure_basis: None` omits the field in JSON
    //    and preserves the exact v1 SHA-256 formula.
    let mut legacy_receipt = DerivedTaskAttemptReceipt {
        receipt_id: "dar_00112233445566778899aabbccddeeff".to_string(),
        binding_id: envelope.binding_id.clone(),
        thread_uuid: envelope.thread_uuid.clone(),
        job_id: envelope.job_id.clone(),
        approval_id: envelope.approval_id.clone(),
        approval_subject_digest: envelope.approval_subject_digest.clone(),
        envelope_id: envelope.envelope_id.clone(),
        envelope_digest: envelope.envelope_digest.clone(),
        task_id: task.task_id.clone(),
        task_digest: task.task_digest.clone(),
        attempt_no: 2,
        prior_rejected_candidate_id: Some(prior_candidate_id.to_string()),
        prior_review_verdict_id: Some(prior_verdict_id.to_string()),
        prior_check_failure_basis: None,
        prior_review_subject_digest: Some(prior_review_subject_digest.clone()),
        required_in_scope_fixes: vec!["Fix off-by-one return value in alpha()".to_string()],
        frozen_instruction: "Update src/lib.rs".to_string(),
        derived_prompt: "Update src/lib.rs\nRequired fix: off-by-one in alpha()".to_string(),
        write_paths: task.write_paths.clone(),
        coder_choice: task.selected_choice.clone(),
        execution_base_commit: fixture.base_commit.clone(),
        run_id: "r0123456789abcdef".to_string(),
        worker_id: task_manifest_id.to_string(),
        task_worktree_id: "wt-task-0123456789abcdef".to_string(),
        integration_worktree_id: "wt-run-0123456789abcdef".to_string(),
        recorded_at_ms: 1_700_000_200_000,
        receipt_digest: String::new(),
    };
    let expected_v1_seed = (
        (
            "threadlane-native:derived_task_attempt_receipt:v1",
            legacy_receipt.receipt_id.as_str(),
            legacy_receipt.binding_id.as_str(),
            legacy_receipt.thread_uuid.as_str(),
            legacy_receipt.job_id.as_str(),
            legacy_receipt.approval_id.as_str(),
            legacy_receipt.approval_subject_digest.as_str(),
            legacy_receipt.envelope_id.as_str(),
            legacy_receipt.envelope_digest.as_str(),
            legacy_receipt.task_id.as_str(),
            legacy_receipt.task_digest.as_str(),
            legacy_receipt.attempt_no,
            legacy_receipt.prior_rejected_candidate_id.as_deref(),
        ),
        (
            legacy_receipt.prior_review_verdict_id.as_deref(),
            legacy_receipt.prior_review_subject_digest.as_deref(),
            &legacy_receipt.required_in_scope_fixes,
            legacy_receipt.frozen_instruction.as_str(),
            legacy_receipt.derived_prompt.as_str(),
            &legacy_receipt.write_paths,
            &legacy_receipt.coder_choice,
            legacy_receipt.execution_base_commit.as_str(),
            legacy_receipt.run_id.as_str(),
            legacy_receipt.worker_id.as_str(),
            legacy_receipt.task_worktree_id.as_str(),
            legacy_receipt.integration_worktree_id.as_str(),
            legacy_receipt.recorded_at_ms,
        ),
    );
    let expected_v1_digest = sha256_hex(&serde_json::to_vec(&expected_v1_seed).expect("v1 bytes"));
    legacy_receipt.receipt_digest = legacy_receipt.compute_digest().expect("compute v1 digest");
    assert_eq!(legacy_receipt.receipt_digest, expected_v1_digest);

    let legacy_json = serde_json::to_string(&legacy_receipt).expect("serialize legacy receipt");
    assert!(!legacy_json.contains("prior_check_failure_basis"));
    let decoded_legacy: DerivedTaskAttemptReceipt =
        serde_json::from_str(&legacy_json).expect("deserialize legacy receipt");
    assert_eq!(decoded_legacy, legacy_receipt);
    decoded_legacy
        .validate_against_task(task, envelope)
        .expect("valid v1 reviewer-based correction receipt must pass");

    // 2. Construct an internally consistent check-failure correction basis contract fixture
    //    with distinct logical `task_id` ("task-1") and compact `task_manifest_id` ("w0123456789abcdef").
    let cargo_test_spec = envelope
        .check_commands
        .get("cargo_test")
        .expect("cargo_test spec in envelope");
    let cargo_test_spec_digest = cargo_test_spec.spec_digest().expect("spec digest");
    let work_area_root = fixture
        .repo_dir
        .parent()
        .expect("temp root")
        .join("work-area");
    fs::create_dir_all(&work_area_root).expect("create work_area_root");
    let work_area_root = fs::canonicalize(&work_area_root).expect("canonicalize work_area_root");
    let checkout_path = work_area_root
        .join("checkouts")
        .join(&legacy_receipt.task_worktree_id);
    fs::create_dir_all(&checkout_path).expect("create checkout_path");
    let checkout_path = fs::canonicalize(&checkout_path).expect("canonicalize checkout_path");

    let check_subject = FrozenCheckSubject {
        binding_id: envelope.binding_id.clone(),
        thread_uuid: envelope.thread_uuid.clone(),
        job_id: envelope.job_id.clone(),
        target_kind: FrozenCheckTargetKind::TaskWorktree,
        repository_path: fixture.repo_dir.clone(),
        work_area_root,
        worktree_id: legacy_receipt.task_worktree_id.clone(),
        expected_run_id: legacy_receipt.run_id.clone(),
        expected_task_id: Some(task_manifest_id.to_string()),
        expected_base_commit: fixture.base_commit.clone(),
        expected_head_commit: committed_head.to_string(),
        expected_tree_sha: committed_tree.to_string(),
    };
    let check_subject_digest = check_subject
        .subject_digest()
        .expect("check subject digest");
    let report_path = fixture
        .policy_store
        .plans_dir()
        .join("coworker-check-report-eff_0123456789abcdef0123456789abcdef.json");
    let stdout_bytes = b"running 1 test\ntest alpha_test ... FAILED\n".to_vec();
    let stderr_bytes = b"error: test failed, to rerun pass `--lib`\n".to_vec();
    let mut sample_report = FrozenCheckExecutionReport {
        schema_version: FROZEN_CHECK_EXECUTION_SCHEMA_VERSION,
        subject: check_subject.clone(),
        ownership_instance_id:
            "cwo_0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".to_string(),
        ownership_challenge_digest: sha256_hex(b"ownership-challenge-v1"),
        ownership_lock_file_identity: CoworkerLockFileIdentity { dev: 1, ino: 42 },
        ownership_verified_post_run: true,
        check_id: "cargo_test".to_string(),
        kind: FrozenCheckKind::Tests,
        spec_digest: cargo_test_spec_digest.clone(),
        executable: cargo_test_spec.executable.clone(),
        argv: cargo_test_spec.argv.clone(),
        repo_relative_cwd: cargo_test_spec.repo_relative_cwd.clone(),
        resolved_working_directory: checkout_path.clone(),
        timeout_ms: cargo_test_spec.timeout_ms,
        max_stdout_bytes: cargo_test_spec.max_stdout_bytes,
        max_stderr_bytes: cargo_test_spec.max_stderr_bytes,
        checkout_path,
        branch_name: "threadlane/task/w0123456789abcdef".to_string(),
        status: FrozenCheckExecutionStatus::NonZeroExit,
        exit_code: Some(101),
        signal: None,
        process_group_id: Some(4242),
        duration_ms: 420,
        stdout_bytes_captured: stdout_bytes.len(),
        stderr_bytes_captured: stderr_bytes.len(),
        stdout_sha256: sha256_hex(&stdout_bytes),
        stderr_sha256: sha256_hex(&stderr_bytes),
        stdout_bytes,
        stderr_bytes,
        stdout_overflowed: false,
        stderr_overflowed: false,
        overflow: None,
        descendants_terminated_after_exit: false,
        cleanup_incomplete: false,
        redacted_stdout_summary: Some("running 1 test; test alpha_test ... FAILED".to_string()),
        redacted_stderr_summary: Some("error: test failed, to rerun pass `--lib`".to_string()),
        post_worktree_clean: true,
        post_head_commit: Some(committed_head.to_string()),
        post_tree_sha: Some(committed_tree.to_string()),
        subject_drifted: false,
        drift_reason: None,
        command_summary: "cargo_test (tests): fake-cargo test --workspace [cwd=.]".to_string(),
        evidence_digest: String::new(),
    };
    sample_report.evidence_digest = sample_report
        .compute_evidence_digest()
        .expect("compute report evidence digest");
    sample_report
        .validate_integrity()
        .expect("sample report passes integrity check");

    let report_ref = CheckFailureReportRef::from_execution_report(
        "eff_0123456789abcdef0123456789abcdef",
        &report_path.to_string_lossy(),
        &sample_report,
        "cargo_test exited with status 101: test alpha_test FAILED",
    )
    .expect("build CheckFailureReportRef");
    report_ref
        .verify_against_execution_report(&check_subject_digest, &sample_report)
        .expect("report_ref matches sample_report");

    let valid_basis = CheckFailureCorrectionBasis {
        task_id: task.task_id.clone(),
        task_manifest_id: task_manifest_id.to_string(),
        prior_attempt_no: 1,
        prior_candidate_id: prior_candidate_id.to_string(),
        execution_base_commit: fixture.base_commit.clone(),
        committed_head_commit: committed_head.to_string(),
        committed_tree_sha: committed_tree.to_string(),
        check_subject_digest: check_subject_digest.clone(),
        prior_review_subject_digest: prior_review_subject_digest.clone(),
        failed_checks: vec![report_ref.clone()],
        diagnostic_summary:
            "Task verification blocked by failed required check `cargo_test` (exit 101)".to_string(),
    };
    valid_basis
        .verify_check_subject(&check_subject)
        .expect("basis matches check_subject via task_manifest_id");

    // Subject whose expected_task_id uses the logical task_id instead of task_manifest_id must fail verify_check_subject.
    let mut wrong_manifest_subject = check_subject.clone();
    wrong_manifest_subject.expected_task_id = Some(task.task_id.clone());
    assert!(valid_basis
        .verify_check_subject(&wrong_manifest_subject)
        .is_err());

    let mut v2_receipt = DerivedTaskAttemptReceipt {
        prior_review_verdict_id: None,
        prior_check_failure_basis: Some(valid_basis.clone()),
        required_in_scope_fixes: vec![
            "Resolve `cargo_test` failure (exit 101: test alpha_test FAILED) within approved write scope".to_string(),
        ],
        receipt_digest: String::new(),
        ..legacy_receipt.clone()
    };
    let v1_without_basis_digest = {
        let mut clone_none = v2_receipt.clone();
        clone_none.prior_check_failure_basis = None;
        clone_none.compute_digest().expect("digest when None")
    };
    v2_receipt.receipt_digest = v2_receipt.compute_digest().expect("compute v2 digest");
    assert_ne!(v2_receipt.receipt_digest, v1_without_basis_digest);
    assert_ne!(v2_receipt.receipt_digest, legacy_receipt.receipt_digest);

    let v2_json = serde_json::to_string(&v2_receipt).expect("serialize v2 receipt");
    assert!(v2_json.contains("prior_check_failure_basis"));
    let decoded_v2: DerivedTaskAttemptReceipt =
        serde_json::from_str(&v2_json).expect("deserialize v2 receipt");
    assert_eq!(decoded_v2, v2_receipt);
    decoded_v2
        .validate_against_task(task, envelope)
        .expect("valid v2 check-failure correction receipt (attempt 2) must pass");

    // 3. Clean unchanged candidate (`committed_head_commit == execution_base_commit`) with failing tests is allowed.
    let mut unchanged_head_subject = check_subject.clone();
    unchanged_head_subject.expected_head_commit = fixture.base_commit.clone();
    let unchanged_head_subject_digest = unchanged_head_subject
        .subject_digest()
        .expect("unchanged head subject digest");
    let mut unchanged_head_receipt = v2_receipt.clone();
    {
        let basis = unchanged_head_receipt
            .prior_check_failure_basis
            .as_mut()
            .expect("basis");
        basis.committed_head_commit = fixture.base_commit.clone();
        basis.check_subject_digest = unchanged_head_subject_digest;
        basis
            .verify_check_subject(&unchanged_head_subject)
            .expect("unchanged head subject matches");
    }
    unchanged_head_receipt.receipt_digest = unchanged_head_receipt
        .compute_digest()
        .expect("compute unchanged head digest");
    unchanged_head_receipt
        .validate_against_task(task, envelope)
        .expect("unchanged head commit equal to execution_base_commit must be valid");

    // 4. Under cap 2 (`max_in_scope_corrections_per_task == 2`), both first correction (attempt 2, prior 1)
    //    and second correction (attempt 3, prior 2) are allowed, while attempt 4 (prior 3) is refused.
    let mut attempt3_receipt = v2_receipt.clone();
    attempt3_receipt.attempt_no = 3;
    attempt3_receipt
        .prior_check_failure_basis
        .as_mut()
        .expect("basis")
        .prior_attempt_no = 2;
    attempt3_receipt.receipt_digest = attempt3_receipt
        .compute_digest()
        .expect("compute attempt 3 digest");
    attempt3_receipt
        .validate_against_task(task, envelope)
        .expect("second correction (attempt 3, prior 2) must be valid when cap == 2");

    let mut attempt4_receipt = v2_receipt.clone();
    attempt4_receipt.attempt_no = 4;
    attempt4_receipt
        .prior_check_failure_basis
        .as_mut()
        .expect("basis")
        .prior_attempt_no = 3;
    attempt4_receipt.receipt_digest = attempt4_receipt
        .compute_digest()
        .expect("compute attempt 4 digest");
    assert_eq!(
        attempt4_receipt
            .validate_against_task(task, envelope)
            .expect_err("third correction (attempt 4, prior 3) must be refused under hard max 2")
            .code(),
        NativeErrorCode::ValidationFailed.as_str()
    );

    // Under a tighter per-task cap of 1, attempt 2 is allowed but attempt 3 is refused.
    let mut cap1_envelope = envelope.clone();
    cap1_envelope.max_in_scope_corrections_per_task = 1;
    v2_receipt
        .validate_against_task(task, &cap1_envelope)
        .expect("attempt 2 allowed under cap 1");
    assert!(attempt3_receipt
        .validate_against_task(task, &cap1_envelope)
        .is_err());

    // Persist and reload via ControllerPolicyStore runtime state to confirm bounded ledger round-trip.
    let instance_id = deterministic_controller_instance_id(BINDING_A, &job.job_id, "test-driver");
    let lease = fixture
        .policy_store
        .acquire_or_renew_lease(
            BINDING_A,
            &job.job_id,
            &instance_id,
            1_700_000_200_000,
            60_000,
        )
        .expect("acquire lease");
    fixture
        .policy_store
        .mutate_job_runtime(
            BINDING_A,
            &job.job_id,
            &instance_id,
            lease.lease_epoch,
            |state| {
                state.derived_attempts.push(v2_receipt.clone());
                state.derived_attempts.push(attempt3_receipt.clone());
                Ok(())
            },
        )
        .expect("persist v2 receipts in runtime state");
    let reloaded = fixture
        .policy_store
        .get_job_runtime_readonly(BINDING_A, &job.job_id)
        .expect("read runtime")
        .expect("runtime exists");
    assert_eq!(
        reloaded.derived_attempts,
        vec![v2_receipt.clone(), attempt3_receipt]
    );

    // 5. Rejection cases: attempt_no == 1 cannot carry prior_check_failure_basis.
    let mut attempt1_with_basis = v2_receipt.clone();
    attempt1_with_basis.attempt_no = 1;
    attempt1_with_basis.receipt_digest = attempt1_with_basis.compute_digest().expect("digest");
    assert_eq!(
        attempt1_with_basis
            .validate_against_task(task, envelope)
            .expect_err("attempt 1 cannot carry check failure basis")
            .code(),
        NativeErrorCode::ValidationFailed.as_str()
    );

    // 6. Dual verdict + check-failure basis (violating XOR) and missing both must fail.
    let mut dual_basis = v2_receipt.clone();
    dual_basis.prior_review_verdict_id = Some(prior_verdict_id.to_string());
    dual_basis.receipt_digest = dual_basis.compute_digest().expect("digest");
    assert_eq!(
        dual_basis
            .validate_against_task(task, envelope)
            .expect_err("dual verdict and check failure basis must fail")
            .code(),
        NativeErrorCode::ValidationFailed.as_str()
    );

    let mut neither_basis = v2_receipt.clone();
    neither_basis.prior_check_failure_basis = None;
    neither_basis.prior_review_verdict_id = None;
    neither_basis.receipt_digest = neither_basis.compute_digest().expect("digest");
    assert_eq!(
        neither_basis
            .validate_against_task(task, envelope)
            .expect_err("missing both verdict and check failure basis must fail")
            .code(),
        NativeErrorCode::ValidationFailed.as_str()
    );

    // 7. Wrong task_id, mismatched prior_attempt_no, wrong prior_candidate_id, wrong prior_review_subject_digest,
    //    or execution_base_commit set to previous attempt head must fail.
    let mut wrong_task_basis = v2_receipt.clone();
    wrong_task_basis
        .prior_check_failure_basis
        .as_mut()
        .expect("basis")
        .task_id = "task-other".to_string();
    wrong_task_basis.receipt_digest = wrong_task_basis.compute_digest().expect("digest");
    assert!(wrong_task_basis
        .validate_against_task(task, envelope)
        .is_err());

    let mut wrong_attempt_basis = v2_receipt.clone();
    wrong_attempt_basis
        .prior_check_failure_basis
        .as_mut()
        .expect("basis")
        .prior_attempt_no = 2; // receipt.attempt_no is 2, so prior_attempt_no == 2 mismatches attempt_no - 1
    wrong_attempt_basis.receipt_digest = wrong_attempt_basis.compute_digest().expect("digest");
    assert!(wrong_attempt_basis
        .validate_against_task(task, envelope)
        .is_err());

    let mut wrong_candidate_basis = v2_receipt.clone();
    wrong_candidate_basis
        .prior_check_failure_basis
        .as_mut()
        .expect("basis")
        .prior_candidate_id = "tcd_ffffffffffffffffffffffffffffffff".to_string();
    wrong_candidate_basis.receipt_digest = wrong_candidate_basis.compute_digest().expect("digest");
    assert!(wrong_candidate_basis
        .validate_against_task(task, envelope)
        .is_err());

    let mut wrong_subject_basis = v2_receipt.clone();
    wrong_subject_basis
        .prior_check_failure_basis
        .as_mut()
        .expect("basis")
        .prior_review_subject_digest = sha256_hex(b"different-review-subject");
    wrong_subject_basis.receipt_digest = wrong_subject_basis.compute_digest().expect("digest");
    assert!(wrong_subject_basis
        .validate_against_task(task, envelope)
        .is_err());

    let mut head_as_base_receipt = v2_receipt.clone();
    head_as_base_receipt.execution_base_commit = committed_head.to_string();
    head_as_base_receipt
        .prior_check_failure_basis
        .as_mut()
        .expect("basis")
        .execution_base_commit = committed_head.to_string();
    head_as_base_receipt.receipt_digest = head_as_base_receipt.compute_digest().expect("digest");
    assert!(head_as_base_receipt
        .validate_against_task(task, envelope)
        .is_err());

    // 8. Non-natural or non-clean check statuses (TimedOut, OutputOverflow, DescendantsTerminatedAfterExit,
    //    Passed, signal crash, missing/zero exit_code, dirty worktree, subject drift) are rejected both
    //    at the contract reference level and when constructing/verifying against FrozenCheckExecutionReport.
    for ineligible_status in [
        FrozenCheckExecutionStatus::Passed,
        FrozenCheckExecutionStatus::TimedOut,
        FrozenCheckExecutionStatus::OutputOverflow,
        FrozenCheckExecutionStatus::DescendantsTerminatedAfterExit,
        FrozenCheckExecutionStatus::Cancelled,
        FrozenCheckExecutionStatus::IncompleteCleanup,
        FrozenCheckExecutionStatus::SubjectMutatedOrDrifted,
        FrozenCheckExecutionStatus::OwnershipOrContextDrifted,
    ] {
        let mut bad_status_receipt = v2_receipt.clone();
        bad_status_receipt
            .prior_check_failure_basis
            .as_mut()
            .expect("basis")
            .failed_checks[0]
            .status = ineligible_status;
        bad_status_receipt.receipt_digest = bad_status_receipt.compute_digest().expect("digest");
        assert!(
            bad_status_receipt
                .validate_against_task(task, envelope)
                .is_err(),
            "status {ineligible_status:?} must not be eligible for auto-correction"
        );
    }

    // Signalled termination, missing exit_code, or zero exit_code with NonZeroExit status must fail.
    let mut signalled_ref = v2_receipt.clone();
    {
        let fc = &mut signalled_ref
            .prior_check_failure_basis
            .as_mut()
            .expect("basis")
            .failed_checks[0];
        fc.signal = Some(9);
    }
    signalled_ref.receipt_digest = signalled_ref.compute_digest().expect("digest");
    assert!(signalled_ref.validate_against_task(task, envelope).is_err());

    let mut missing_exit_ref = v2_receipt.clone();
    {
        let fc = &mut missing_exit_ref
            .prior_check_failure_basis
            .as_mut()
            .expect("basis")
            .failed_checks[0];
        fc.exit_code = None;
    }
    missing_exit_ref.receipt_digest = missing_exit_ref.compute_digest().expect("digest");
    assert!(missing_exit_ref
        .validate_against_task(task, envelope)
        .is_err());

    let mut zero_exit_ref = v2_receipt.clone();
    {
        let fc = &mut zero_exit_ref
            .prior_check_failure_basis
            .as_mut()
            .expect("basis")
            .failed_checks[0];
        fc.exit_code = Some(0);
    }
    zero_exit_ref.receipt_digest = zero_exit_ref.compute_digest().expect("digest");
    assert!(zero_exit_ref.validate_against_task(task, envelope).is_err());

    // Full FrozenCheckExecutionReport with overflow, forced descendants, incomplete cleanup, or signal
    // is rejected by CheckFailureReportRef::from_execution_report and verify_against_execution_report.
    let mut overflow_report = sample_report.clone();
    overflow_report.stdout_overflowed = true;
    overflow_report.overflow = Some(BoundedCommandOverflow::StdoutExceeded {
        max_stdout_bytes: overflow_report.max_stdout_bytes,
    });
    overflow_report.evidence_digest = overflow_report
        .compute_evidence_digest()
        .expect("overflow digest");
    assert!(CheckFailureReportRef::from_execution_report(
        "eff_0123456789abcdef0123456789abcdef",
        &report_path.to_string_lossy(),
        &overflow_report,
        "overflowed",
    )
    .is_err());

    let mut forced_descendants_report = sample_report.clone();
    forced_descendants_report.descendants_terminated_after_exit = true;
    forced_descendants_report.evidence_digest = forced_descendants_report
        .compute_evidence_digest()
        .expect("descendants digest");
    assert!(CheckFailureReportRef::from_execution_report(
        "eff_0123456789abcdef0123456789abcdef",
        &report_path.to_string_lossy(),
        &forced_descendants_report,
        "forced descendants",
    )
    .is_err());

    let mut incomplete_cleanup_report = sample_report.clone();
    incomplete_cleanup_report.cleanup_incomplete = true;
    incomplete_cleanup_report.evidence_digest = incomplete_cleanup_report
        .compute_evidence_digest()
        .expect("cleanup digest");
    assert!(CheckFailureReportRef::from_execution_report(
        "eff_0123456789abcdef0123456789abcdef",
        &report_path.to_string_lossy(),
        &incomplete_cleanup_report,
        "cleanup incomplete",
    )
    .is_err());

    let mut signalled_report = sample_report.clone();
    signalled_report.signal = Some(15);
    signalled_report.evidence_digest = signalled_report
        .compute_evidence_digest()
        .expect("signal digest");
    assert!(CheckFailureReportRef::from_execution_report(
        "eff_0123456789abcdef0123456789abcdef",
        &report_path.to_string_lossy(),
        &signalled_report,
        "signalled",
    )
    .is_err());

    // Unknown check_id, wrong spec_digest, dirty worktree, or drifted subject fail.
    let mut unknown_check = v2_receipt.clone();
    unknown_check
        .prior_check_failure_basis
        .as_mut()
        .expect("basis")
        .failed_checks[0]
        .check_id = "undeclared_check".to_string();
    unknown_check.receipt_digest = unknown_check.compute_digest().expect("digest");
    assert!(unknown_check.validate_against_task(task, envelope).is_err());

    let mut wrong_spec_digest = v2_receipt.clone();
    wrong_spec_digest
        .prior_check_failure_basis
        .as_mut()
        .expect("basis")
        .failed_checks[0]
        .spec_digest = sha256_hex(b"tampered-spec-digest");
    wrong_spec_digest.receipt_digest = wrong_spec_digest.compute_digest().expect("digest");
    assert!(wrong_spec_digest
        .validate_against_task(task, envelope)
        .is_err());

    let mut dirty_worktree = v2_receipt.clone();
    dirty_worktree
        .prior_check_failure_basis
        .as_mut()
        .expect("basis")
        .failed_checks[0]
        .post_worktree_clean = false;
    dirty_worktree.receipt_digest = dirty_worktree.compute_digest().expect("digest");
    assert!(dirty_worktree
        .validate_against_task(task, envelope)
        .is_err());

    let mut drifted_subject = v2_receipt.clone();
    drifted_subject
        .prior_check_failure_basis
        .as_mut()
        .expect("basis")
        .failed_checks[0]
        .subject_drifted = true;
    drifted_subject.receipt_digest = drifted_subject.compute_digest().expect("digest");
    assert!(drifted_subject
        .validate_against_task(task, envelope)
        .is_err());

    // 9. Non-required task-review check in envelope is rejected as a task check-failure basis.
    let mut non_required_envelope = envelope.clone();
    non_required_envelope
        .check_commands
        .get_mut("cargo_test")
        .expect("cargo_test")
        .required_for_task_review = false;
    assert!(valid_basis
        .validate_against_task_and_envelope(task, &non_required_envelope)
        .is_err());

    // 10. Mutating the basis without recomputing receipt_digest is caught by digest verification.
    let mut tampered_basis_digest = v2_receipt.clone();
    tampered_basis_digest
        .prior_check_failure_basis
        .as_mut()
        .expect("basis")
        .diagnostic_summary = "Tampered summary after sealing".to_string();
    assert!(tampered_basis_digest
        .validate_against_task(task, envelope)
        .is_err());

    // 11. Standalone structural validation (`validate` / `validate_structure`) on CheckFailureReportRef
    //     and CheckFailureCorrectionBasis accepts valid fixtures and rejects malformed identifiers,
    //     attempt bounds, SHAs/digests, failed_checks bounds/deduplication, and diagnostics without
    //     requiring a Task or FrozenControllerExecutionEnvelope.
    report_ref
        .validate()
        .expect("valid CheckFailureReportRef passes standalone validate");
    report_ref
        .validate_structure()
        .expect("valid CheckFailureReportRef passes standalone validate_structure");
    valid_basis
        .validate()
        .expect("valid CheckFailureCorrectionBasis passes standalone validate");
    valid_basis
        .validate_structure()
        .expect("valid CheckFailureCorrectionBasis passes standalone validate_structure");

    let mut bad_task_id_basis = valid_basis.clone();
    bad_task_id_basis.task_id.clear();
    assert_eq!(
        bad_task_id_basis
            .validate()
            .expect_err("empty task_id must fail standalone validate")
            .code(),
        NativeErrorCode::ValidationFailed.as_str()
    );

    let mut bad_manifest_id_basis = valid_basis.clone();
    bad_manifest_id_basis.task_manifest_id = "bad/manifest/id".to_string();
    assert!(bad_manifest_id_basis.validate().is_err());

    for out_of_bounds_attempt in [0, 3] {
        let mut bad_attempt_basis = valid_basis.clone();
        bad_attempt_basis.prior_attempt_no = out_of_bounds_attempt;
        assert_eq!(
            bad_attempt_basis
                .validate()
                .expect_err("out-of-bounds prior_attempt_no must fail standalone validate")
                .code(),
            NativeErrorCode::ValidationFailed.as_str()
        );
    }

    let mut bad_sha_basis = valid_basis.clone();
    bad_sha_basis.committed_head_commit = "not-a-40-hex-sha".to_string();
    assert_eq!(
        bad_sha_basis
            .validate()
            .expect_err("non-40-hex commit SHA must fail standalone validate")
            .code(),
        NativeErrorCode::ValidationFailed.as_str()
    );

    let mut bad_digest_basis = valid_basis.clone();
    bad_digest_basis.check_subject_digest = "not-a-64-hex-sha256".to_string();
    assert_eq!(
        bad_digest_basis
            .validate()
            .expect_err("non-64-hex check_subject_digest must fail standalone validate")
            .code(),
        NativeErrorCode::ValidationFailed.as_str()
    );

    let mut empty_checks_basis = valid_basis.clone();
    empty_checks_basis.failed_checks.clear();
    assert_eq!(
        empty_checks_basis
            .validate()
            .expect_err("empty failed_checks must fail standalone validate")
            .code(),
        NativeErrorCode::ValidationFailed.as_str()
    );

    let mut dup_check_id_basis = valid_basis.clone();
    let mut second_ref = report_ref.clone();
    second_ref.effect_id = "eff_fedcba9876543210fedcba9876543210".to_string();
    dup_check_id_basis.failed_checks.push(second_ref);
    assert_eq!(
        dup_check_id_basis
            .validate()
            .expect_err("duplicate check_id in failed_checks must fail standalone validate")
            .code(),
        NativeErrorCode::ValidationFailed.as_str()
    );

    let mut dup_effect_id_basis = valid_basis.clone();
    let mut second_ref_same_eff = report_ref.clone();
    second_ref_same_eff.check_id = "cargo_clippy".to_string();
    dup_effect_id_basis.failed_checks.push(second_ref_same_eff);
    assert_eq!(
        dup_effect_id_basis
            .validate()
            .expect_err("duplicate effect_id in failed_checks must fail standalone validate")
            .code(),
        NativeErrorCode::ValidationFailed.as_str()
    );

    let mut bad_eff_prefix_ref = report_ref.clone();
    bad_eff_prefix_ref.effect_id = "obx_0123456789abcdef0123456789abcdef".to_string();
    assert_eq!(
        bad_eff_prefix_ref
            .validate()
            .expect_err("effect_id without eff_ prefix must fail standalone validate")
            .code(),
        NativeErrorCode::ValidationFailed.as_str()
    );

    let mut relative_path_ref = report_ref.clone();
    relative_path_ref.report_path = "relative/check-report.json".to_string();
    assert!(relative_path_ref.validate().is_err());

    let mut multiline_diag_basis = valid_basis.clone();
    multiline_diag_basis.diagnostic_summary = "line one\nline two".to_string();
    assert!(multiline_diag_basis.validate().is_err());
}

#[test]
fn namespaced_coder_job_freezes_controller_envelope_and_reopens_preserving_exact_model_id_and_max_effort(
) {
    let fixture = setup_fixture();

    let brief = WorkBrief {
        brief_id: "brf_namespaced_coder_env".to_string(),
        binding_id: BINDING_A.to_string(),
        thread_uuid: THREAD_UUID_A.to_string(),
        project_base: project_base(&fixture),
        kind: WorkBriefKind::NewJob,
        summary: "Verify namespaced OpenCode Coder model_id through frozen controller envelope"
            .to_string(),
        confirmed_requirements: vec![
            "Admit opencode-go/deepseek-v4.1-flash with effort max across approval, job binding, and controller envelope".to_string(),
        ],
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
            request_key: "req-job-namespaced-coder-env".to_string(),
            expected_ledger_revision: None,
            work_brief: brief,
            amended_target_task_ids: Vec::new(),
            amended_write_paths: Vec::new(),
            amended_interfaces: Vec::new(),
        })
        .expect("create job");

    // `WorkflowRole::Coder` with `Difficulty::Easy` resolves to `opencode-go/deepseek-v4.1-flash`
    // (`effort: Some("max")`, `adapter: ToolAdapter::OpencodeV2`) in `profile-coworker`.
    let tasks = vec![PlanTask {
        id: "task-namespaced-coder".to_string(),
        title: "Namespaced Coder slice".to_string(),
        instruction: "Update src/lib.rs".to_string(),
        role: WorkflowRole::Coder,
        difficulty: Difficulty::Easy,
        depends_on: Vec::new(),
        allowed_write_paths: vec!["src/lib.rs".to_string()],
    }];
    let reg_snap = fixture.registry.snapshot().expect("registry snapshot");
    let draft = fixture
        .plans
        .change_at(
            0,
            PlanChange::SaveDraft {
                plan_id: "plan-namespaced-coder-env".to_string(),
                binding_id: BINDING_A.to_string(),
                profile_id: "profile-coworker".to_string(),
                profile_revision: reg_snap.revision,
                summary: "Plan with namespaced OpenCode Coder task".to_string(),
                tasks,
            },
            1_700_000_000_000,
        )
        .expect("save draft");
    let published = fixture
        .plans
        .change(
            draft.revision,
            PlanChange::Publish {
                plan_id: "plan-namespaced-coder-env".to_string(),
                revision: draft.revision,
            },
        )
        .expect("publish plan");
    let compiled = compile_plan_revision(&published, &reg_snap).expect("compile plan");
    assert_eq!(
        compiled.tasks[0].choice.model_id,
        "opencode-go/deepseek-v4.1-flash"
    );
    assert_eq!(
        compiled.tasks[0].model_arg,
        "opencode-go/deepseek-v4.1-flash"
    );
    assert_eq!(compiled.tasks[0].choice.effort.as_deref(), Some("max"));

    let summary = fixture
        .authority
        .approve_at(
            OperatorApprovalRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                plan_id: "plan-namespaced-coder-env".to_string(),
                plan_revision: published.revision,
                expected_registry_revision: reg_snap.revision,
                expected_plan_digest: compiled.plan_digest,
                repository_path: fixture.repo_dir.clone(),
                base_commit: fixture.base_commit.clone(),
                approved_task_ids: vec!["task-namespaced-coder".to_string()],
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
                approval_id: summary.approval_id,
                policy: execution_policy(),
                controller_envelope: Some(cap_env),
                task_interfaces: BTreeMap::new(),
            },
            &fixture.authority,
        )
        .expect("bind job approval for namespaced Coder job");

    let coder_task = bound_job
        .tasks
        .iter()
        .find(|task| task.task_id == "task-namespaced-coder")
        .expect("bound namespaced Coder task");
    assert_eq!(coder_task.selected_choice, headless_deepseek_choice());
    assert_eq!(
        coder_task.selected_choice.model_id,
        "opencode-go/deepseek-v4.1-flash"
    );
    assert_eq!(
        coder_task.selected_choice.model_arg,
        "opencode-go/deepseek-v4.1-flash"
    );
    assert_eq!(coder_task.selected_choice.effort.as_deref(), Some("max"));

    // Invalid registry model identifiers on controller choices must still fail preview before any disk write.
    let mut base_preview_req = sample_preview_request(&fixture, &bound_job);
    base_preview_req.reviewer_choice = headless_deepseek_choice();
    base_preview_req.closure_choice = headless_deepseek_choice();

    let overflow_id = "m".repeat(MAX_COWORKER_ID_LEN + 1);
    for invalid_model_id in [
        "",
        " opencode-go/deepseek-v4.1-flash",
        "opencode-go/deepseek-v4.1-flash ",
        "opencode-go/deepseek\0flash",
        "opencode-go/deepseek\nflash",
        overflow_id.as_str(),
    ] {
        let mut bad_req = base_preview_req.clone();
        bad_req.reviewer_choice.model_id = invalid_model_id.to_string();
        let err = fixture
            .policy_store
            .preview_execution_envelope(&bad_req, &bound_job, &fixture.authority)
            .expect_err("invalid registry model_id in controller envelope must fail");
        assert_eq!(err.code(), NativeErrorCode::ValidationFailed.as_str());
    }
    assert!(!fixture
        .policy_store
        .ledger_path_for_binding(BINDING_A)
        .expect("policy ledger path")
        .exists());

    // Preview and record the frozen controller execution envelope with namespaced Coder, Reviewer, and Closure choices.
    let preview = fixture
        .policy_store
        .preview_execution_envelope(&base_preview_req, &bound_job, &fixture.authority)
        .expect("preview controller execution envelope with namespaced choices");
    assert_eq!(
        preview
            .envelope
            .task_coder_choices
            .get("task-namespaced-coder"),
        Some(&headless_deepseek_choice())
    );
    assert_eq!(preview.envelope.reviewer_choice, headless_deepseek_choice());
    assert_eq!(preview.envelope.closure_choice, headless_deepseek_choice());

    let record_req = attested_record_request_from_preview(&base_preview_req, &preview);
    let recorded = fixture
        .policy_store
        .record_execution_envelope(&record_req, &bound_job, &fixture.authority)
        .expect("record controller execution envelope with namespaced choices");
    recorded
        .envelope
        .verify_matches_job(&bound_job)
        .expect("recorded envelope matches bound namespaced Coder job");

    // Reopen both stores from disk and verify exact model_id and max effort preservation.
    let plans_dir = fixture.workflow_store.plans_dir();
    let reopened_workflow =
        CoworkerWorkflowStore::new(plans_dir).expect("reopen CoworkerWorkflowStore");
    let reopened_policy =
        ControllerPolicyStore::new(plans_dir).expect("reopen ControllerPolicyStore");

    let reopened_ledger = reopened_workflow
        .read_binding_ledger(BINDING_A)
        .expect("read reopened workflow ledger");
    reopened_ledger
        .validate(BINDING_A)
        .expect("reopened workflow ledger validates");
    let reopened_job = reopened_ledger
        .jobs
        .iter()
        .find(|job| job.job_id == bound_job.job_id)
        .expect("reopened job exists");
    let reopened_coder_choice = &reopened_job
        .tasks
        .iter()
        .find(|task| task.task_id == "task-namespaced-coder")
        .expect("reopened task exists")
        .selected_choice;
    assert_eq!(
        reopened_coder_choice.model_id,
        "opencode-go/deepseek-v4.1-flash"
    );
    assert_eq!(
        reopened_coder_choice.model_arg,
        "opencode-go/deepseek-v4.1-flash"
    );
    assert_eq!(reopened_coder_choice.effort.as_deref(), Some("max"));

    let reopened_runtime = reopened_policy
        .get_job_runtime_readonly(BINDING_A, &bound_job.job_id)
        .expect("read reopened controller runtime")
        .expect("reopened controller runtime exists");
    reopened_runtime
        .envelope
        .verify_matches_job(reopened_job)
        .expect("reopened controller envelope matches reopened job");

    let env_coder_choice = reopened_runtime
        .envelope
        .task_coder_choices
        .get("task-namespaced-coder")
        .expect("envelope coder choice exists");
    assert_eq!(env_coder_choice.model_id, "opencode-go/deepseek-v4.1-flash");
    assert_eq!(
        env_coder_choice.model_arg,
        "opencode-go/deepseek-v4.1-flash"
    );
    assert_eq!(env_coder_choice.effort.as_deref(), Some("max"));
    assert_eq!(
        reopened_runtime.envelope.reviewer_choice.model_id,
        "opencode-go/deepseek-v4.1-flash"
    );
    assert_eq!(
        reopened_runtime.envelope.reviewer_choice.effort.as_deref(),
        Some("max")
    );
    assert_eq!(
        reopened_runtime.envelope.closure_choice.model_id,
        "opencode-go/deepseek-v4.1-flash"
    );
    assert_eq!(
        reopened_runtime.envelope.closure_choice.effort.as_deref(),
        Some("max")
    );
}
