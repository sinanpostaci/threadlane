use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use tempfile::TempDir;
use threadlane_authority::{AuthorityStore, OperatorApprovalRequest};
use threadlane_native::coworker_workflow::candidate::{
    AssembleJobAggregateCandidateRequest, AssembleOperationsFinalCandidateRequest,
    AssembleVerifiedCandidateRequest, CoworkerCandidateService,
    ExecuteFinalCandidateClosureRequest, StructuredClosureModelResponse,
    STRUCTURED_CLOSURE_RESPONSE_SCHEMA_VERSION,
};
use threadlane_native::coworker_workflow::controller_policy::{
    ClosureExecutionArtifactRecord, ControllerConfigurationPreview, ControllerPolicyStore,
    ControllerRoleExecutionBudget, FreezeControllerExecutionEnvelopeRequest,
    FrozenCheckCommandSpec, FrozenCheckKind, PendingControllerEffect, PendingControllerEffectKind,
    PendingControllerEffectStatus, PersistClosureExecutionArtifactRequest,
};
use threadlane_native::coworker_workflow::{
    ApplyWorkflowHoldRequest, BindJobApprovalRequest, CandidateCheckEvidence,
    CanonicalProjectBaseRef, CombinedCandidateStatus, CoworkerJobPhase, CoworkerJobRecord,
    CoworkerTaskStage, CoworkerWorkflowStore, CreateCoworkerJobRequest, FinalizeTaskOutboxRequest,
    FrozenControllerCapabilityEnvelope, OutboxDispatchKind, OutboxDispatchStatus,
    RecordTaskReviewVerdictRequest, ReserveTaskOutboxRequest, ReviewVerdictOutcome,
    StructuredReviewVerdict, TaskCandidateRef, TaskInterfaceOwnershipSpec, WorkBrief,
    WorkBriefKind, WorkflowHoldReason,
};
use threadlane_native::role_flow::RoleFlowStore;
use threadlane_native::{
    Difficulty, ExecutionPolicy, NativeErrorCode, PolicyAllowRule, RegistryStore,
    RoleExecutionRoute, SelectedRoleChoice, ToolAdapter, WorkflowProfile, WorkflowRole,
};
use threadlane_operations::{
    accept_review_subject, commit_task_worktree_changes, execute_integration,
    execute_job_integration, inspect_review_subject, load_integration_receipt,
    verify_final_candidate_subject_readonly, ClosureAttemptState, ClosureVerdictKind,
    CommitTaskChangesRequest, ExecuteIntegrationRequest, ExecuteJobIntegrationRequest,
    FinalCandidateClosureStore, FinalCandidateSubject, InspectReviewSubjectRequest,
    IntegrationReceipt, IntegrationStrategy, JobIntegrationScopeProvenance,
    JobIntegrationTaskInput, MarkClosureSubmittedRequest, ReserveClosureAttemptRequest,
    ReviewAcceptanceRequest, ReviewPolicyBinding, ReviewReceipt, ReviewSubject,
    TestEvidenceSummary,
};
use threadlane_workers::{
    resolve_exact_commit_sha, resolve_exact_tree_sha, run_sanitized_git, ExecutionBudgets,
    WorkerFilePermissions, WorkerRunStatus, WorktreeManager, WorktreeManifest,
    DEFAULT_GIT_COMMAND_TIMEOUT_MS, DEFAULT_MAX_STDERR_BYTES, DEFAULT_MAX_STDOUT_BYTES,
    DEFAULT_TIMEOUT_MS,
};
use threadlane_workflow::{
    compile_plan_revision, ModelChoice, ModelDefinition, PlanChange, PlanStore, PlanTask,
    RegistryChange, RoleAssignment, ToolDefinition,
};

const THREAD_UUID: &str = "550e8400-e29b-41d4-a716-446655440000";
const BINDING_ID: &str = "bnd_coworker_candidate_alpha";

struct CandidateServiceFixture {
    _tmp: TempDir,
    repo_dir: PathBuf,
    work_area_root: PathBuf,
    base_commit: String,
    base_tree_sha: String,
    run_id: String,
    integration_manifest: WorktreeManifest,
    fake_agy_exe: PathBuf,
    fake_cargo_exe: PathBuf,
    launch_count_file: PathBuf,
    control_dir: PathBuf,
    registry: Arc<RegistryStore>,
    plans: Arc<PlanStore>,
    authority: Arc<AuthorityStore>,
    workflow_store: CoworkerWorkflowStore,
    policy_store: ControllerPolicyStore,
    candidate_service: CoworkerCandidateService,
}

struct IntegratedPredecessorTask {
    task_id: String,
    task_manifest: WorktreeManifest,
    review_receipt: ReviewReceipt,
    integration_receipt: IntegrationReceipt,
}

fn init_git_repo(repo_dir: &Path) -> (String, String) {
    fs::create_dir_all(repo_dir.join("src")).expect("create repo src");
    fs::write(
        repo_dir.join("src/alpha.rs"),
        "pub fn alpha() -> u32 {\n    1\n}\n",
    )
    .expect("write src/alpha.rs");
    fs::write(
        repo_dir.join("src/beta.rs"),
        "pub fn beta() -> u32 {\n    2\n}\n",
    )
    .expect("write src/beta.rs");

    for args in [
        &["init", "-b", "main", "--quiet"][..],
        &["config", "user.email", "test@threadlane.local"][..],
        &["config", "user.name", "Threadlane Test"][..],
        &["add", "."][..],
        &["commit", "-m", "initial base commit", "--quiet"][..],
    ] {
        run_sanitized_git(repo_dir, args, DEFAULT_GIT_COMMAND_TIMEOUT_MS).expect("git init step");
    }
    let commit = resolve_exact_commit_sha(repo_dir, "HEAD", DEFAULT_GIT_COMMAND_TIMEOUT_MS)
        .expect("resolve HEAD commit");
    let tree = resolve_exact_tree_sha(repo_dir, "HEAD", DEFAULT_GIT_COMMAND_TIMEOUT_MS)
        .expect("resolve HEAD tree");
    (commit, tree)
}

fn write_executable_script(path: &Path, body: &str) {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).expect("create script parent");
    }
    let mut file = fs::File::create(path).expect("create script");
    file.write_all(body.as_bytes()).expect("write script");
    file.sync_all().expect("sync script");
    fs::set_permissions(path, fs::Permissions::from_mode(0o755)).expect("chmod 0755 script");
}

fn read_launch_count(path: &Path) -> usize {
    match fs::read_to_string(path) {
        Ok(text) => text.lines().filter(|l| !l.trim().is_empty()).count(),
        Err(_) => 0,
    }
}

fn write_fake_agy_stream_script(
    fake_agy_exe: &Path,
    launch_count_file: &Path,
    init_line: &str,
    result_line: &str,
    exit_code: i32,
) {
    let script = format!(
        "#!/bin/sh\n\
         echo 1 >> \"{}\"\n\
         printf '%s\\n' '{}'\n\
         printf '%s\\n' '{}'\n\
         exit {}\n",
        launch_count_file.display(),
        init_line.replace('\'', "'\\''"),
        result_line.replace('\'', "'\\''"),
        exit_code,
    );
    write_executable_script(fake_agy_exe, &script);
}

fn configure_fake_agy_valid_closure(
    fixture: &CandidateServiceFixture,
    subject: &FinalCandidateSubject,
    outcome: ClosureVerdictKind,
    summary: &str,
    findings: Vec<String>,
) {
    let payload = StructuredClosureModelResponse {
        schema_version: STRUCTURED_CLOSURE_RESPONSE_SCHEMA_VERSION,
        subject_digest: subject.subject_digest.clone(),
        candidate_tree_sha: subject.candidate_tree_sha.clone(),
        outcome,
        summary: summary.to_string(),
        findings,
    };
    let payload_str = serde_json::to_string(&payload).expect("serialize closure response");
    let init_line = serde_json::json!({
        "event": "init",
        "init": {
            "conversation_id": "ses_closure_valid_1",
            "model": "gemini-4-argon-high",
            "effort": "high"
        }
    })
    .to_string();
    let result_line = serde_json::json!({
        "event": "result",
        "result": {
            "status": "SUCCESS",
            "conversation_id": "ses_closure_valid_1",
            "model": "gemini-4-argon-high",
            "effort": "high",
            "response": payload_str
        }
    })
    .to_string();
    write_fake_agy_stream_script(
        &fixture.fake_agy_exe,
        &fixture.launch_count_file,
        &init_line,
        &result_line,
        0,
    );
}

fn setup_fixture() -> CandidateServiceFixture {
    let tmp = TempDir::new().expect("tempdir");
    let root = fs::canonicalize(tmp.path()).expect("canonicalize temp root");

    let repo_dir = root.join("repo");
    let (base_commit, base_tree_sha) = init_git_repo(&repo_dir);
    let canonical_repo = fs::canonicalize(&repo_dir).expect("canonicalize repo");

    let work_area_root = root.join("work-area");
    fs::create_dir_all(&work_area_root).expect("create work_area_root");
    fs::set_permissions(&work_area_root, fs::Permissions::from_mode(0o700))
        .expect("chmod 0700 work_area_root");
    let canonical_work_area = fs::canonicalize(&work_area_root).expect("canonicalize work_area");

    let run_id = "run_candidate_cycle_1".to_string();
    let manager = WorktreeManager::new(&canonical_repo, &canonical_work_area, &base_commit)
        .expect("WorktreeManager::new");
    let integration_manifest = manager
        .create_run_worktree_with_token(&run_id, "int01")
        .expect("create RunIntegration worktree");

    let bin_dir = root.join("bin");
    let fake_agy_exe = bin_dir.join("fake-agy");
    let fake_cargo_exe = bin_dir.join("fake-cargo");
    write_executable_script(&fake_agy_exe, "#!/bin/sh\nexit 0\n");
    write_executable_script(&fake_cargo_exe, "#!/bin/sh\nexit 0\n");

    let control_dir = root.join("control");
    fs::create_dir_all(&control_dir).expect("create control dir");
    let launch_count_file = control_dir.join("launch_count.txt");

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
                    id: "profile-coworker".to_string(),
                    name: "Coworker Headless Profile".to_string(),
                    assignments: vec![
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
    let role_flow = RoleFlowStore::new(&plans_dir).expect("open role flow store");
    let candidate_service =
        CoworkerCandidateService::new(workflow_store.clone(), role_flow, &registry_dir)
            .expect("create CoworkerCandidateService");

    CandidateServiceFixture {
        _tmp: tmp,
        repo_dir: canonical_repo,
        work_area_root: canonical_work_area,
        base_commit,
        base_tree_sha,
        run_id,
        integration_manifest,
        fake_agy_exe,
        fake_cargo_exe,
        launch_count_file,
        control_dir,
        registry,
        plans,
        authority,
        workflow_store,
        policy_store,
        candidate_service,
    }
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
                WorkflowRole::Coder,
                WorkflowRole::Reviewer,
                WorkflowRole::Closure,
            ],
        }],
    }
}

fn default_check_commands(
    fixture: &CandidateServiceFixture,
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

fn default_check_evidence() -> Vec<CandidateCheckEvidence> {
    vec![
        CandidateCheckEvidence {
            check_id: "cargo_fmt".to_string(),
            command_summary: "cargo fmt --all --check".to_string(),
            passed: true,
            exit_code: 0,
            evidence_digest: "aa".repeat(32),
        },
        CandidateCheckEvidence {
            check_id: "cargo_test".to_string(),
            command_summary: "cargo test --workspace".to_string(),
            passed: true,
            exit_code: 0,
            evidence_digest: "bb".repeat(32),
        },
    ]
}

fn create_job_and_freeze_envelope(
    fixture: &CandidateServiceFixture,
    brief_id: &str,
    plan_id: &str,
    closure_max_calls: u32,
    closure_max_response_bytes: usize,
) -> CoworkerJobRecord {
    let project_base =
        CanonicalProjectBaseRef::resolve_and_validate(&fixture.repo_dir, &fixture.base_commit)
            .expect("resolve canonical project_base");
    let brief = WorkBrief {
        brief_id: brief_id.to_string(),
        binding_id: BINDING_ID.to_string(),
        thread_uuid: THREAD_UUID.to_string(),
        project_base,
        kind: WorkBriefKind::NewJob,
        summary: "Verify final candidate and closure service".to_string(),
        confirmed_requirements: vec!["Require owned RunIntegration candidate".to_string()],
        proposed_additions: Vec::new(),
        assumptions: vec!["Disposable test repository".to_string()],
        open_questions: Vec::new(),
        accepted_architect_decisions: Vec::new(),
    };
    let job = fixture
        .workflow_store
        .create_or_amend_job(&CreateCoworkerJobRequest {
            binding_id: BINDING_ID.to_string(),
            thread_uuid: THREAD_UUID.to_string(),
            request_key: format!("req-job-{brief_id}"),
            expected_ledger_revision: None,
            work_brief: brief,
            amended_target_task_ids: Vec::new(),
            amended_write_paths: Vec::new(),
            amended_interfaces: Vec::new(),
        })
        .expect("create job");

    let tasks = vec![
        PlanTask {
            id: "task-alpha".to_string(),
            title: "Task Alpha".to_string(),
            instruction: "Update src/alpha.rs".to_string(),
            role: WorkflowRole::Coder,
            difficulty: Difficulty::Hard,
            depends_on: Vec::new(),
            allowed_write_paths: vec!["src/alpha.rs".to_string()],
        },
        PlanTask {
            id: "task-beta".to_string(),
            title: "Task Beta".to_string(),
            instruction: "Update src/beta.rs".to_string(),
            role: WorkflowRole::Coder,
            difficulty: Difficulty::Hard,
            depends_on: Vec::new(),
            allowed_write_paths: vec!["src/beta.rs".to_string()],
        },
    ];
    let reg_snap = fixture.registry.snapshot().expect("registry snapshot");
    let draft = fixture
        .plans
        .change_at(
            0,
            PlanChange::SaveDraft {
                plan_id: plan_id.to_string(),
                binding_id: BINDING_ID.to_string(),
                profile_id: "profile-coworker".to_string(),
                profile_revision: reg_snap.revision,
                summary: format!("Plan {plan_id}"),
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
                binding_id: BINDING_ID.to_string(),
                thread_uuid: THREAD_UUID.to_string(),
                plan_id: plan_id.to_string(),
                plan_revision: published.revision,
                expected_registry_revision: reg_snap.revision,
                expected_plan_digest: compiled.plan_digest,
                repository_path: fixture.repo_dir.clone(),
                base_commit: fixture.base_commit.clone(),
                approved_task_ids: vec!["task-alpha".to_string(), "task-beta".to_string()],
                approved_write_paths: vec!["src/alpha.rs".to_string(), "src/beta.rs".to_string()],
                max_parallel: 2,
                budgets: ExecutionBudgets::default(),
            },
            1_700_000_001_000,
        )
        .expect("approve plan");
    finish_binding_and_freezing_job(
        fixture,
        job,
        &summary.approval_id,
        closure_max_calls,
        closure_max_response_bytes,
    )
}

fn finish_binding_and_freezing_job(
    fixture: &CandidateServiceFixture,
    job: CoworkerJobRecord,
    approval_id: &str,
    closure_max_calls: u32,
    closure_max_response_bytes: usize,
) -> CoworkerJobRecord {
    let cap_env = FrozenControllerCapabilityEnvelope {
        allowed_reviewer_choices: vec![headless_argon_choice(Difficulty::Hard)],
        allowed_closure_choices: vec![headless_argon_choice(Difficulty::Extreme)],
        max_in_scope_correction_attempts: 2,
        allowed_check_ids: vec!["cargo_fmt".to_string(), "cargo_test".to_string()],
        local_git_integration_allowed: true,
        allow_check_failure_corrections: false,
    };
    let mut task_interfaces = BTreeMap::new();
    task_interfaces.insert(
        "task-alpha".to_string(),
        TaskInterfaceOwnershipSpec {
            owned_interfaces: vec!["iface_alpha".to_string()],
            consumed_interfaces: Vec::new(),
        },
    );
    task_interfaces.insert(
        "task-beta".to_string(),
        TaskInterfaceOwnershipSpec {
            owned_interfaces: vec!["iface_beta".to_string()],
            consumed_interfaces: Vec::new(),
        },
    );

    let bound_job = fixture
        .workflow_store
        .bind_job_approval(
            &BindJobApprovalRequest {
                binding_id: BINDING_ID.to_string(),
                thread_uuid: THREAD_UUID.to_string(),
                job_id: job.job_id,
                expected_job_revision: job.job_revision,
                expected_ledger_revision: None,
                approval_id: approval_id.to_string(),
                policy: execution_policy(),
                controller_envelope: Some(cap_env),
                task_interfaces,
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
    let mut closure_budget =
        ControllerRoleExecutionBudget::default_for_role(WorkflowRole::Closure, closure_max_calls);
    closure_budget.max_structured_response_bytes = closure_max_response_bytes;
    role_budgets.insert(WorkflowRole::Closure, closure_budget);

    let preview_req = FreezeControllerExecutionEnvelopeRequest {
        binding_id: BINDING_ID.to_string(),
        thread_uuid: THREAD_UUID.to_string(),
        job_id: bound_job.job_id.clone(),
        expected_job_revision: bound_job.job_revision,
        policy: execution_policy(),
        reviewer_choice: headless_argon_choice(Difficulty::Hard),
        closure_choice: headless_argon_choice(Difficulty::Extreme),
        role_budgets,
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
        .expect("freeze execution envelope");

    bound_job
}

#[allow(clippy::too_many_arguments)]
fn commit_review_and_integrate_task_in_work_area(
    fixture: &CandidateServiceFixture,
    task_id: &str,
    token: &str,
    rel_file: &str,
    new_contents: &str,
    strategy: IntegrationStrategy,
    timestamp_ms: i64,
) -> IntegratedPredecessorTask {
    let manager = WorktreeManager::new(
        &fixture.repo_dir,
        &fixture.work_area_root,
        &fixture.base_commit,
    )
    .expect("WorktreeManager::new");
    let mut allowed = BTreeSet::new();
    allowed.insert(rel_file.to_string());

    let task_manifest = manager
        .create_task_worktree_with_token(&fixture.run_id, task_id, token, &allowed)
        .expect("create task worktree");
    fs::write(task_manifest.checkout_path.join(rel_file), new_contents)
        .expect("write task modification");

    commit_task_worktree_changes(&CommitTaskChangesRequest {
        work_area_root: fixture.work_area_root.clone(),
        task_worktree_id: task_manifest.worktree_id.clone(),
        commit_message: format!("Implement {task_id}"),
        worker_status: Some(WorkerRunStatus::Succeeded),
        allow_missing_effort_evidence: false,
    })
    .expect("commit task changes");

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
        passed_test_count: 8,
        failed_test_count: 0,
        evidence_summary: "cargo fmt and cargo test passed".to_string(),
    };
    let subject = inspect_review_subject(&InspectReviewSubjectRequest {
        work_area_root: fixture.work_area_root.clone(),
        task_worktree_id: task_manifest.worktree_id.clone(),
        policy,
        test_evidence,
        worker_status: Some(WorkerRunStatus::Succeeded),
    })
    .expect("inspect review subject");

    let review_receipt = accept_review_subject(&ReviewAcceptanceRequest {
        subject: subject.clone(),
        expected_subject_digest: subject.subject_digest.clone(),
        reviewer_id: "op_controller".to_string(),
        review_note: format!("Approved {task_id}"),
        approved_at_ms: timestamp_ms,
        receipt_id: Some(format!("rev_{}_{token}", task_id.replace('-', "_"))),
    })
    .expect("accept review subject");

    let current_int_head = resolve_exact_commit_sha(
        &fixture.integration_manifest.checkout_path,
        "HEAD",
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .expect("resolve current integration HEAD");

    let integration_receipt = execute_integration(&ExecuteIntegrationRequest {
        work_area_root: fixture.work_area_root.clone(),
        integration_worktree_id: fixture.integration_manifest.worktree_id.clone(),
        task_worktree_id: task_manifest.worktree_id.clone(),
        subject,
        review_receipt: review_receipt.clone(),
        strategy,
        expected_integration_head_commit: current_int_head,
        expected_task_head_commit: review_receipt.task_head_commit.clone(),
        expected_source_head_commit: fixture.base_commit.clone(),
        operator_id: "op_controller".to_string(),
        integrated_at_ms: timestamp_ms + 10,
        receipt_id: Some(format!("int_{}_{token}", task_id.replace('-', "_"))),
    })
    .expect("execute integration");

    IntegratedPredecessorTask {
        task_id: task_id.to_string(),
        task_manifest,
        review_receipt,
        integration_receipt,
    }
}

fn record_integrated_task_on_job(
    fixture: &CandidateServiceFixture,
    job: CoworkerJobRecord,
    pred: &IntegratedPredecessorTask,
    rel_file: &str,
) -> CoworkerJobRecord {
    let policy_digest = job
        .approval_linkage
        .as_ref()
        .expect("approval linkage")
        .policy_digest
        .clone();
    let tag = pred.task_id.replace('-', "_");

    let (job, attempt_outbox) = fixture
        .workflow_store
        .reserve_task_outbox(&ReserveTaskOutboxRequest {
            binding_id: BINDING_ID.to_string(),
            thread_uuid: THREAD_UUID.to_string(),
            job_id: job.job_id.clone(),
            expected_job_revision: job.job_revision,
            expected_ledger_revision: None,
            request_key: format!("req_att_{tag}"),
            kind: OutboxDispatchKind::TaskAttemptDispatch,
            task_id: pred.task_id.clone(),
            role: WorkflowRole::Coder,
            subject_digest: pred.review_receipt.subject_digest.clone(),
            dependency_input_commit: None,
            dependency_input_tree_sha: None,
            accepted_predecessor_receipts: Vec::new(),
        })
        .expect("reserve task attempt outbox");

    let (job, _) = fixture
        .workflow_store
        .reconcile_or_finalize_outbox(&FinalizeTaskOutboxRequest {
            binding_id: BINDING_ID.to_string(),
            thread_uuid: THREAD_UUID.to_string(),
            job_id: job.job_id.clone(),
            expected_job_revision: job.job_revision,
            expected_ledger_revision: None,
            outbox_id: attempt_outbox.outbox_id,
            status: OutboxDispatchStatus::DispatchedEffectConfirmed,
            next_task_stage: CoworkerTaskStage::ReadyForReview,
            run_id: Some(fixture.run_id.clone()),
            worker_id: Some(format!("wrk_coder_{tag}")),
            task_worktree_id: Some(pred.task_manifest.worktree_id.clone()),
            head_commit: Some(pred.review_receipt.task_head_commit.clone()),
            tree_sha: Some(pred.review_receipt.task_tree_sha.clone()),
            diff_digest: Some(pred.review_receipt.diff_digest.clone()),
            report_artifact_id: Some(format!("art_coder_{tag}")),
            observed_model: Some("gemini-4-argon-high".to_string()),
            observed_effort: Some("high".to_string()),
            receipt_id: None,
            diagnostic: None,
        })
        .expect("finalize task attempt outbox");

    finish_task_review_and_integration_on_job(fixture, job, pred, rel_file, &policy_digest, &tag)
}

fn finish_task_review_and_integration_on_job(
    fixture: &CandidateServiceFixture,
    job: CoworkerJobRecord,
    pred: &IntegratedPredecessorTask,
    rel_file: &str,
    policy_digest: &str,
    tag: &str,
) -> CoworkerJobRecord {
    let (job, rev_outbox) = fixture
        .workflow_store
        .reserve_task_outbox(&ReserveTaskOutboxRequest {
            binding_id: BINDING_ID.to_string(),
            thread_uuid: THREAD_UUID.to_string(),
            job_id: job.job_id.clone(),
            expected_job_revision: job.job_revision,
            expected_ledger_revision: None,
            request_key: format!("req_rev_{tag}"),
            kind: OutboxDispatchKind::TaskReviewDispatch,
            task_id: pred.task_id.clone(),
            role: WorkflowRole::Reviewer,
            subject_digest: pred.review_receipt.subject_digest.clone(),
            dependency_input_commit: None,
            dependency_input_tree_sha: None,
            accepted_predecessor_receipts: Vec::new(),
        })
        .expect("reserve review outbox");

    let (job, _) = fixture
        .workflow_store
        .reconcile_or_finalize_outbox(&FinalizeTaskOutboxRequest {
            binding_id: BINDING_ID.to_string(),
            thread_uuid: THREAD_UUID.to_string(),
            job_id: job.job_id.clone(),
            expected_job_revision: job.job_revision,
            expected_ledger_revision: None,
            outbox_id: rev_outbox.outbox_id,
            status: OutboxDispatchStatus::DispatchedEffectConfirmed,
            next_task_stage: CoworkerTaskStage::UnderReview,
            run_id: Some(fixture.run_id.clone()),
            worker_id: Some(format!("wrk_rev_{tag}")),
            task_worktree_id: Some(pred.task_manifest.worktree_id.clone()),
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

    let candidate_id = format!("cnd_{tag}_01");
    let verdict = StructuredReviewVerdict {
        verdict_id: format!("vrd_{tag}_01"),
        task_id: pred.task_id.clone(),
        attempt_no: 1,
        candidate_id: candidate_id.clone(),
        review_subject_digest: pred.review_receipt.subject_digest.clone(),
        base_commit: pred.review_receipt.base_commit.clone(),
        task_head_commit: pred.review_receipt.task_head_commit.clone(),
        task_tree_sha: pred.review_receipt.task_tree_sha.clone(),
        diff_digest: pred.review_receipt.diff_digest.clone(),
        policy_digest: policy_digest.to_string(),
        outcome: ReviewVerdictOutcome::Accept,
        findings: Vec::new(),
        required_in_scope_fixes: Vec::new(),
        conflicting_paths_or_interfaces: Vec::new(),
        prose_summary: Some("Structured review accepted".to_string()),
        prose_may_advance_acceptance: false,
    };
    let candidate = TaskCandidateRef {
        candidate_id,
        task_id: pred.task_id.clone(),
        attempt_no: 1,
        base_commit: pred.review_receipt.base_commit.clone(),
        head_commit: pred.review_receipt.task_head_commit.clone(),
        tree_sha: pred.review_receipt.task_tree_sha.clone(),
        diff_digest: pred.review_receipt.diff_digest.clone(),
        write_paths: vec![rel_file.to_string()],
        check_evidence_digests: vec!["aa".repeat(32)],
        review_subject_digest: Some(pred.review_receipt.subject_digest.clone()),
        review_verdict: Some(verdict),
        review_receipt_id: Some(pred.review_receipt.receipt_id.clone()),
        integration_receipt_id: None,
    };
    let (job, _) = fixture
        .workflow_store
        .record_task_candidate_and_verdict(&RecordTaskReviewVerdictRequest {
            binding_id: BINDING_ID.to_string(),
            thread_uuid: THREAD_UUID.to_string(),
            job_id: job.job_id.clone(),
            expected_job_revision: job.job_revision,
            expected_ledger_revision: None,
            candidate,
        })
        .expect("record task candidate and verdict");
    complete_task_integration_on_job(fixture, job, pred, tag)
}

fn complete_task_integration_on_job(
    fixture: &CandidateServiceFixture,
    job: CoworkerJobRecord,
    pred: &IntegratedPredecessorTask,
    tag: &str,
) -> CoworkerJobRecord {
    let (job, int_outbox) = fixture
        .workflow_store
        .reserve_task_outbox(&ReserveTaskOutboxRequest {
            binding_id: BINDING_ID.to_string(),
            thread_uuid: THREAD_UUID.to_string(),
            job_id: job.job_id.clone(),
            expected_job_revision: job.job_revision,
            expected_ledger_revision: None,
            request_key: format!("req_int_{tag}"),
            kind: OutboxDispatchKind::TaskIntegration,
            task_id: pred.task_id.clone(),
            role: WorkflowRole::Reviewer,
            subject_digest: pred.review_receipt.subject_digest.clone(),
            dependency_input_commit: None,
            dependency_input_tree_sha: None,
            accepted_predecessor_receipts: Vec::new(),
        })
        .expect("reserve integration outbox");

    let (job, _) = fixture
        .workflow_store
        .reconcile_or_finalize_outbox(&FinalizeTaskOutboxRequest {
            binding_id: BINDING_ID.to_string(),
            thread_uuid: THREAD_UUID.to_string(),
            job_id: job.job_id.clone(),
            expected_job_revision: job.job_revision,
            expected_ledger_revision: None,
            outbox_id: int_outbox.outbox_id,
            status: OutboxDispatchStatus::DispatchedEffectConfirmed,
            next_task_stage: CoworkerTaskStage::Integrated,
            run_id: Some(fixture.run_id.clone()),
            worker_id: None,
            task_worktree_id: Some(pred.task_manifest.worktree_id.clone()),
            head_commit: None,
            tree_sha: None,
            diff_digest: None,
            report_artifact_id: None,
            observed_model: None,
            observed_effort: None,
            receipt_id: Some(pred.integration_receipt.receipt_id.clone()),
            diagnostic: None,
        })
        .expect("finalize integration outbox");
    job
}

fn prepare_integrated_job_with_receipts(
    fixture: &CandidateServiceFixture,
    brief_id: &str,
    plan_id: &str,
    closure_max_calls: u32,
    closure_max_response_bytes: usize,
) -> (
    CoworkerJobRecord,
    IntegratedPredecessorTask,
    IntegratedPredecessorTask,
) {
    let job = create_job_and_freeze_envelope(
        fixture,
        brief_id,
        plan_id,
        closure_max_calls,
        closure_max_response_bytes,
    );
    let pred_alpha = commit_review_and_integrate_task_in_work_area(
        fixture,
        "task-alpha",
        "t01",
        "src/alpha.rs",
        "pub fn alpha() -> u32 {\n    100\n}\n",
        IntegrationStrategy::FastForwardOnly,
        1_000,
    );
    let pred_beta = commit_review_and_integrate_task_in_work_area(
        fixture,
        "task-beta",
        "t02",
        "src/beta.rs",
        "pub fn beta() -> u32 {\n    200\n}\n",
        IntegrationStrategy::MergeNoFf,
        2_000,
    );
    let job = record_integrated_task_on_job(fixture, job, &pred_alpha, "src/alpha.rs");
    let job = record_integrated_task_on_job(fixture, job, &pred_beta, "src/beta.rs");
    (job, pred_alpha, pred_beta)
}

fn assert_primary_checkout_untouched(fixture: &CandidateServiceFixture) {
    let head = resolve_exact_commit_sha(&fixture.repo_dir, "HEAD", DEFAULT_GIT_COMMAND_TIMEOUT_MS)
        .expect("resolve primary HEAD");
    assert_eq!(
        head, fixture.base_commit,
        "primary checkout HEAD must never move"
    );
    let tree = resolve_exact_tree_sha(&fixture.repo_dir, "HEAD", DEFAULT_GIT_COMMAND_TIMEOUT_MS)
        .expect("resolve primary tree");
    assert_eq!(
        tree, fixture.base_tree_sha,
        "primary checkout tree must never change"
    );
    let status = run_sanitized_git(
        &fixture.repo_dir,
        &["status", "--porcelain=v1", "-uall"],
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .expect("primary git status");
    assert!(
        status.trim().is_empty(),
        "primary checkout must remain completely clean, got: {status}"
    );
}

#[test]
fn final_owned_integration_subject_and_receipts_required_and_legacy_or_wrong_scope_rejected() {
    let fixture = setup_fixture();
    let (job, pred_alpha, pred_beta) = prepare_integrated_job_with_receipts(
        &fixture,
        "brf_cand_1",
        "plan-cand-1",
        2,
        threadlane_workers::DEFAULT_MAX_ADVISORY_RESPONSE_BYTES,
    );

    // 1. Legacy `RoleFlowStore` base-tree closure reservation (`reserve_package_closure: true`)
    // must be rejected on a job with a frozen controller capability envelope.
    let legacy_err = fixture
        .candidate_service
        .assemble_verified_candidate(
            &fixture.authority,
            &AssembleVerifiedCandidateRequest {
                binding_id: BINDING_ID.to_string(),
                thread_uuid: THREAD_UUID.to_string(),
                job_id: job.job_id.clone(),
                expected_job_revision: job.job_revision,
                expected_ledger_revision: None,
                request_key: "req_legacy_base_closure".to_string(),
                final_commit_sha: Some(
                    pred_beta
                        .integration_receipt
                        .integrated_target_commit
                        .clone(),
                ),
                final_tree_sha: pred_beta.integration_receipt.integrated_tree_sha.clone(),
                check_evidence: default_check_evidence(),
                reserve_package_closure: true,
                role_flow_cycle_id: Some("flw_legacy_cycle_1".to_string()),
                closure_task_id: Some("task-closure".to_string()),
                closure_summary: Some("Legacy closure attempt".to_string()),
            },
        )
        .expect_err("legacy ApprovedCycleBase closure reservation must be rejected");
    assert_eq!(
        legacy_err.code(),
        NativeErrorCode::ValidationFailed.as_str()
    );

    // 2. Omitting a required frozen check (`cargo_test`) must be rejected before candidate assembly.
    let missing_check_err = fixture
        .candidate_service
        .assemble_and_inspect_operations_candidate(
            &fixture.authority,
            &fixture.policy_store,
            &AssembleOperationsFinalCandidateRequest {
                binding_id: BINDING_ID.to_string(),
                thread_uuid: THREAD_UUID.to_string(),
                job_id: job.job_id.clone(),
                expected_job_revision: job.job_revision,
                expected_ledger_revision: None,
                request_key: "req_missing_check".to_string(),
                work_area_root: fixture.work_area_root.clone(),
                run_id: fixture.run_id.clone(),
                integration_worktree_id: fixture.integration_manifest.worktree_id.clone(),
                check_evidence: vec![default_check_evidence().remove(0)],
                closure_task_id: "task-closure".to_string(),
                reserve_closure_attempt: true,
                reserved_at_ms: 1_700_000_200_000,
            },
        )
        .expect_err("missing required frozen check must be rejected");
    assert_eq!(
        missing_check_err.code(),
        NativeErrorCode::ValidationFailed.as_str()
    );
    finish_test_1_assertions(&fixture, &job, &pred_alpha, &pred_beta);
}

fn finish_test_1_assertions(
    fixture: &CandidateServiceFixture,
    job: &CoworkerJobRecord,
    pred_alpha: &IntegratedPredecessorTask,
    pred_beta: &IntegratedPredecessorTask,
) {
    // 3. Supplying a task worktree instead of the owned RunIntegration worktree must be rejected.
    let wrong_wt_err = fixture
        .candidate_service
        .assemble_and_inspect_operations_candidate(
            &fixture.authority,
            &fixture.policy_store,
            &AssembleOperationsFinalCandidateRequest {
                binding_id: BINDING_ID.to_string(),
                thread_uuid: THREAD_UUID.to_string(),
                job_id: job.job_id.clone(),
                expected_job_revision: job.job_revision,
                expected_ledger_revision: None,
                request_key: "req_wrong_wt".to_string(),
                work_area_root: fixture.work_area_root.clone(),
                run_id: fixture.run_id.clone(),
                integration_worktree_id: pred_alpha.task_manifest.worktree_id.clone(),
                check_evidence: default_check_evidence(),
                closure_task_id: "task-closure".to_string(),
                reserve_closure_attempt: true,
                reserved_at_ms: 1_700_000_200_000,
            },
        )
        .expect_err("non-RunIntegration worktree must be rejected");
    assert_eq!(
        wrong_wt_err.code(),
        NativeErrorCode::SecurityViolation.as_str()
    );

    // 4. Valid owned RunIntegration worktree with accepted predecessor receipts and frozen checks succeeds.
    let assembled = fixture
        .candidate_service
        .assemble_and_inspect_operations_candidate(
            &fixture.authority,
            &fixture.policy_store,
            &AssembleOperationsFinalCandidateRequest {
                binding_id: BINDING_ID.to_string(),
                thread_uuid: THREAD_UUID.to_string(),
                job_id: job.job_id.clone(),
                expected_job_revision: job.job_revision,
                expected_ledger_revision: None,
                request_key: "req_assemble_valid_1".to_string(),
                work_area_root: fixture.work_area_root.clone(),
                run_id: fixture.run_id.clone(),
                integration_worktree_id: fixture.integration_manifest.worktree_id.clone(),
                check_evidence: default_check_evidence(),
                closure_task_id: "task-closure".to_string(),
                reserve_closure_attempt: true,
                reserved_at_ms: 1_700_000_200_000,
            },
        )
        .expect("assemble valid operations final candidate");

    verify_final_candidate_subject_readonly(&assembled.final_subject)
        .expect("verify_final_candidate_subject_readonly");
    assert_eq!(
        assembled.combined_candidate.final_tree_sha,
        pred_beta.integration_receipt.integrated_tree_sha
    );
    assert_ne!(
        assembled.combined_candidate.final_tree_sha,
        fixture.base_tree_sha
    );
    assert_eq!(
        assembled.combined_candidate.status,
        CombinedCandidateStatus::ClosureReserved
    );
    assert!(assembled
        .combined_candidate
        .closure_reservation_id
        .as_deref()
        .is_some_and(|id| id.starts_with("cat_")));

    let runtime = fixture
        .policy_store
        .get_job_runtime_readonly(BINDING_ID, &job.job_id)
        .expect("read job runtime")
        .expect("runtime exists");
    assert_eq!(
        runtime.final_candidate_subject.as_ref(),
        Some(&assembled.final_subject)
    );
    assert_eq!(runtime.candidate_check_evidence, default_check_evidence());
    assert_primary_checkout_untouched(fixture);
}

#[tokio::test]
async fn valid_complete_subject_bound_closure_json_accepted_and_cached_on_repeat_without_relaunch()
{
    let fixture = setup_fixture();
    let (job, _, _) = prepare_integrated_job_with_receipts(
        &fixture,
        "brf_cand_2",
        "plan-cand-2",
        2,
        threadlane_workers::DEFAULT_MAX_ADVISORY_RESPONSE_BYTES,
    );

    let assembled = fixture
        .candidate_service
        .assemble_and_inspect_operations_candidate(
            &fixture.authority,
            &fixture.policy_store,
            &AssembleOperationsFinalCandidateRequest {
                binding_id: BINDING_ID.to_string(),
                thread_uuid: THREAD_UUID.to_string(),
                job_id: job.job_id.clone(),
                expected_job_revision: job.job_revision,
                expected_ledger_revision: None,
                request_key: "req_assemble_closure_2".to_string(),
                work_area_root: fixture.work_area_root.clone(),
                run_id: fixture.run_id.clone(),
                integration_worktree_id: fixture.integration_manifest.worktree_id.clone(),
                check_evidence: default_check_evidence(),
                closure_task_id: "task-closure".to_string(),
                reserve_closure_attempt: true,
                reserved_at_ms: 1_700_000_200_000,
            },
        )
        .expect("assemble operations candidate");

    configure_fake_agy_valid_closure(
        &fixture,
        &assembled.final_subject,
        ClosureVerdictKind::Accepted,
        "All integrated tasks and checks verified cleanly.",
        Vec::new(),
    );

    let first_outcome = fixture
        .candidate_service
        .execute_final_candidate_closure(
            &fixture.authority,
            &fixture.policy_store,
            &ExecuteFinalCandidateClosureRequest {
                binding_id: BINDING_ID.to_string(),
                thread_uuid: THREAD_UUID.to_string(),
                job_id: job.job_id.clone(),
                expected_job_revision: assembled.job.job_revision,
                expected_ledger_revision: None,
                command_key: "req_assemble_closure_2".to_string(),
                combined_candidate_id: assembled.combined_candidate.combined_candidate_id.clone(),
                final_subject: assembled.final_subject.clone(),
                current_time_ms: 1_700_000_210_000,
            },
        )
        .await
        .expect("execute first closure");

    assert!(!first_outcome.reused_cached_verdict);
    assert_eq!(first_outcome.attempt.state, ClosureAttemptState::Terminal);
    assert!(first_outcome.attempt.terminal_failure.is_none());
    let verdict = first_outcome.verdict.as_ref().expect("accepted verdict");
    assert_eq!(verdict.kind, ClosureVerdictKind::Accepted);
    assert!(!verdict.execution_evidence.grants_merge_or_push_authority);
    assert!(!verdict.execution_evidence.substitutes_for_required_checks);
    assert_eq!(
        verdict.execution_evidence.observed_model.as_deref(),
        Some("gemini-4-argon-high")
    );
    assert_eq!(
        verdict.execution_evidence.observed_effort.as_deref(),
        Some("high")
    );
    assert!(verdict
        .execution_evidence
        .pinned_launch_configuration
        .is_none());
    assert_eq!(first_outcome.job.phase, CoworkerJobPhase::Completed);
    assert_eq!(
        first_outcome.job.combined_candidates[0].status,
        CombinedCandidateStatus::ClosureAccepted
    );
    assert_eq!(read_launch_count(&fixture.launch_count_file), 1);

    fs::remove_file(&fixture.fake_agy_exe)
        .expect("remove fake_agy_exe before cached closure reuse");

    finish_test_2_cache_and_artifact_assertions(&fixture, &job.job_id, &assembled, &first_outcome)
        .await;
}

async fn finish_test_2_cache_and_artifact_assertions(
    fixture: &CandidateServiceFixture,
    job_id: &str,
    assembled: &threadlane_native::coworker_workflow::candidate::AssembleOperationsFinalCandidateOutcome,
    first_outcome: &threadlane_native::coworker_workflow::candidate::ExecuteFinalCandidateClosureOutcome,
) {
    let runtime = fixture
        .policy_store
        .get_job_runtime_readonly(BINDING_ID, job_id)
        .expect("read runtime")
        .expect("runtime exists");
    let closure_usage = runtime
        .role_usage
        .get(&WorkflowRole::Closure)
        .expect("closure role usage");
    assert_eq!(closure_usage.calls_reserved, 1);
    assert_eq!(closure_usage.calls_completed, 1);

    let artifact = runtime
        .closure_artifacts
        .get(&first_outcome.attempt.attempt_id)
        .expect("persisted closure execution artifact");
    assert!(!artifact.fenced_from_promotion);
    assert!(artifact.artifact_ref.is_some());
    assert!(artifact.execution_report.is_none());
    assert!(artifact.advisory_response.is_none());
    assert_eq!(
        artifact.observed_model.as_deref(),
        Some("gemini-4-argon-high")
    );
    assert_eq!(artifact.observed_effort.as_deref(), Some("high"));
    let loaded_artifact = fixture
        .policy_store
        .load_closure_execution_artifact(
            BINDING_ID,
            THREAD_UUID,
            job_id,
            &first_outcome.attempt.attempt_id,
        )
        .expect("load full closure execution artifact");
    let health = loaded_artifact
        .execution_report
        .provider_health
        .as_ref()
        .expect("direct Closure final diagnostic coverage");
    assert!(health.is_valid());
    assert_eq!(health.evidence_class,
        threadlane_workers::provider_health::ProviderHealthEvidenceClass::UnverifiedProviderDiagnosticHint);
    assert!(
        fixture
            .policy_store
            .get_job_runtime_readonly(BINDING_ID, job_id)
            .expect("direct Closure runtime")
            .expect("runtime")
            .provider_health
            .is_empty(),
        "unowned direct primitive must not invent a live role owner"
    );
    assert!(!loaded_artifact.fenced_from_promotion);
    assert_eq!(
        loaded_artifact.execution_report.status,
        WorkerRunStatus::Succeeded
    );
    let bounded_resp = loaded_artifact
        .advisory_response
        .as_ref()
        .expect("persisted bounded advisory response");
    bounded_resp
        .validate_integrity(threadlane_workers::DEFAULT_MAX_ADVISORY_RESPONSE_BYTES)
        .expect("persisted bounded response integrity");

    // Repeat call with identical verified subject under a new command key must reuse cached verdict
    // without launching the fake provider a second time.
    let second_outcome = fixture
        .candidate_service
        .execute_final_candidate_closure(
            &fixture.authority,
            &fixture.policy_store,
            &ExecuteFinalCandidateClosureRequest {
                binding_id: BINDING_ID.to_string(),
                thread_uuid: THREAD_UUID.to_string(),
                job_id: job_id.to_string(),
                expected_job_revision: first_outcome.job.job_revision,
                expected_ledger_revision: None,
                command_key: "req_assemble_closure_2_repeat".to_string(),
                combined_candidate_id: assembled.combined_candidate.combined_candidate_id.clone(),
                final_subject: assembled.final_subject.clone(),
                current_time_ms: 1_700_000_220_000,
            },
        )
        .await
        .expect("execute second closure (cached)");

    assert!(second_outcome.reused_cached_verdict);
    assert_eq!(second_outcome.verdict, first_outcome.verdict);
    assert_eq!(
        read_launch_count(&fixture.launch_count_file),
        1,
        "cached final candidate subject must not invoke provider a second time"
    );
    let runtime_after = fixture
        .policy_store
        .get_job_runtime_readonly(BINDING_ID, job_id)
        .expect("read runtime after cache hit")
        .expect("runtime exists");
    assert_eq!(
        runtime_after.role_usage[&WorkflowRole::Closure].calls_reserved,
        1
    );
    assert_primary_checkout_untouched(fixture);
}

#[tokio::test]
async fn wrong_subject_model_effort_or_malformed_oversized_partial_output_fails_terminally_consumes_cap_and_blocks_new_key_bypass(
) {
    // Part A: Wrong subject_digest in StructuredClosureModelResponse records a known local terminal
    // failure (not permanent unknown), consumes 1 call from the finite Closure cap, and blocks
    // blind retry under a new command_key for the same subject.
    let fixture_a = setup_fixture();
    let (job_a, _, _) = prepare_integrated_job_with_receipts(
        &fixture_a,
        "brf_cand_3a",
        "plan-cand-3a",
        2,
        threadlane_workers::DEFAULT_MAX_ADVISORY_RESPONSE_BYTES,
    );
    let assembled_a = fixture_a
        .candidate_service
        .assemble_and_inspect_operations_candidate(
            &fixture_a.authority,
            &fixture_a.policy_store,
            &AssembleOperationsFinalCandidateRequest {
                binding_id: BINDING_ID.to_string(),
                thread_uuid: THREAD_UUID.to_string(),
                job_id: job_a.job_id.clone(),
                expected_job_revision: job_a.job_revision,
                expected_ledger_revision: None,
                request_key: "req_cand_3a".to_string(),
                work_area_root: fixture_a.work_area_root.clone(),
                run_id: fixture_a.run_id.clone(),
                integration_worktree_id: fixture_a.integration_manifest.worktree_id.clone(),
                check_evidence: default_check_evidence(),
                closure_task_id: "task-closure".to_string(),
                reserve_closure_attempt: false,
                reserved_at_ms: 1_700_000_300_000,
            },
        )
        .expect("assemble candidate 3a");

    let mut wrong_subject = assembled_a.final_subject.clone();
    wrong_subject.subject_digest = "ff".repeat(32);
    configure_fake_agy_valid_closure(
        &fixture_a,
        &wrong_subject,
        ClosureVerdictKind::Accepted,
        "Looks good but bound to wrong subject digest",
        Vec::new(),
    );

    let failed_outcome = fixture_a
        .candidate_service
        .execute_final_candidate_closure(
            &fixture_a.authority,
            &fixture_a.policy_store,
            &ExecuteFinalCandidateClosureRequest {
                binding_id: BINDING_ID.to_string(),
                thread_uuid: THREAD_UUID.to_string(),
                job_id: job_a.job_id.clone(),
                expected_job_revision: assembled_a.job.job_revision,
                expected_ledger_revision: None,
                command_key: "cmd_closure_wrong_subj_1".to_string(),
                combined_candidate_id: assembled_a.combined_candidate.combined_candidate_id.clone(),
                final_subject: assembled_a.final_subject.clone(),
                current_time_ms: 1_700_000_310_000,
            },
        )
        .await
        .expect("wrong subject digest returns known local terminal failure outcome");

    assert!(failed_outcome.verdict.is_none());
    assert_eq!(failed_outcome.attempt.state, ClosureAttemptState::Terminal);
    let tf = failed_outcome
        .attempt
        .terminal_failure
        .as_ref()
        .expect("terminal_failure recorded");
    assert_eq!(tf.failure_code, "response_validation_failed");
    assert!(!tf.fenced_late_output);
    assert_eq!(
        failed_outcome.job.combined_candidates[0].status,
        CombinedCandidateStatus::Rejected
    );
    assert_eq!(read_launch_count(&fixture_a.launch_count_file), 1);

    finish_test_3_cap_and_fencing_assertions(
        &fixture_a,
        &job_a.job_id,
        &assembled_a,
        &failed_outcome,
    )
    .await;
}

async fn finish_test_3_cap_and_fencing_assertions(
    fixture_a: &CandidateServiceFixture,
    job_id_a: &str,
    assembled_a: &threadlane_native::coworker_workflow::candidate::AssembleOperationsFinalCandidateOutcome,
    failed_outcome: &threadlane_native::coworker_workflow::candidate::ExecuteFinalCandidateClosureOutcome,
) {
    let runtime_a = fixture_a
        .policy_store
        .get_job_runtime_readonly(BINDING_ID, job_id_a)
        .expect("read runtime 3a")
        .expect("runtime 3a exists");
    assert_eq!(
        runtime_a.role_usage[&WorkflowRole::Closure].calls_reserved,
        1
    );
    assert_eq!(
        runtime_a.role_usage[&WorkflowRole::Closure].calls_completed,
        1
    );
    assert!(runtime_a
        .pending_effects
        .values()
        .any(|e| e.status == PendingControllerEffectStatus::EffectFailedTerminal));

    // Blind retry with a new command_key on the same failed subject_digest must be blocked without
    // launching the fake provider a second time.
    let retry_err = fixture_a
        .candidate_service
        .execute_final_candidate_closure(
            &fixture_a.authority,
            &fixture_a.policy_store,
            &ExecuteFinalCandidateClosureRequest {
                binding_id: BINDING_ID.to_string(),
                thread_uuid: THREAD_UUID.to_string(),
                job_id: job_id_a.to_string(),
                expected_job_revision: failed_outcome.job.job_revision,
                expected_ledger_revision: None,
                command_key: "cmd_closure_wrong_subj_bypass_2".to_string(),
                combined_candidate_id: assembled_a.combined_candidate.combined_candidate_id.clone(),
                final_subject: assembled_a.final_subject.clone(),
                current_time_ms: 1_700_000_320_000,
            },
        )
        .await
        .expect_err("new command_key must not bypass un-reconciled terminal failure");
    assert_eq!(
        retry_err.code(),
        NativeErrorCode::AmbiguousWakeupPendingReconciliation.as_str()
    );
    assert_eq!(read_launch_count(&fixture_a.launch_count_file), 1);

    verify_oversized_and_unresolved_fencing().await;
}

async fn verify_oversized_and_unresolved_fencing() {
    // Part B: Oversized response (> 256-byte cap) with closure_max_calls = 1 fails terminally
    // (`response_too_large`) and exhausts the 1-call cap so a subsequent call is refused pre-effect.
    let fixture_b = setup_fixture();
    let (job_b, _, _) =
        prepare_integrated_job_with_receipts(&fixture_b, "brf_cand_3b", "plan-cand-3b", 1, 256);
    let assembled_b = fixture_b
        .candidate_service
        .assemble_and_inspect_operations_candidate(
            &fixture_b.authority,
            &fixture_b.policy_store,
            &AssembleOperationsFinalCandidateRequest {
                binding_id: BINDING_ID.to_string(),
                thread_uuid: THREAD_UUID.to_string(),
                job_id: job_b.job_id.clone(),
                expected_job_revision: job_b.job_revision,
                expected_ledger_revision: None,
                request_key: "req_cand_3b".to_string(),
                work_area_root: fixture_b.work_area_root.clone(),
                run_id: fixture_b.run_id.clone(),
                integration_worktree_id: fixture_b.integration_manifest.worktree_id.clone(),
                check_evidence: default_check_evidence(),
                closure_task_id: "task-closure".to_string(),
                reserve_closure_attempt: false,
                reserved_at_ms: 1_700_000_330_000,
            },
        )
        .expect("assemble candidate 3b");

    configure_fake_agy_valid_closure(
        &fixture_b,
        &assembled_b.final_subject,
        ClosureVerdictKind::Accepted,
        &"x".repeat(320),
        Vec::new(),
    );

    let oversized_out = fixture_b
        .candidate_service
        .execute_final_candidate_closure(
            &fixture_b.authority,
            &fixture_b.policy_store,
            &ExecuteFinalCandidateClosureRequest {
                binding_id: BINDING_ID.to_string(),
                thread_uuid: THREAD_UUID.to_string(),
                job_id: job_b.job_id.clone(),
                expected_job_revision: assembled_b.job.job_revision,
                expected_ledger_revision: None,
                command_key: "cmd_closure_oversized_1".to_string(),
                combined_candidate_id: assembled_b.combined_candidate.combined_candidate_id.clone(),
                final_subject: assembled_b.final_subject.clone(),
                current_time_ms: 1_700_000_340_000,
            },
        )
        .await
        .expect("oversized response records terminal failure");
    assert!(oversized_out.verdict.is_none());
    assert_eq!(
        oversized_out
            .attempt
            .terminal_failure
            .as_ref()
            .expect("terminal failure")
            .failure_code,
        "response_too_large"
    );
    assert_eq!(read_launch_count(&fixture_b.launch_count_file), 1);

    // Exhausted 1-call cap refuses any further closure execution before effect.
    let cap_err = fixture_b
        .candidate_service
        .execute_final_candidate_closure(
            &fixture_b.authority,
            &fixture_b.policy_store,
            &ExecuteFinalCandidateClosureRequest {
                binding_id: BINDING_ID.to_string(),
                thread_uuid: THREAD_UUID.to_string(),
                job_id: job_b.job_id.clone(),
                expected_job_revision: oversized_out.job.job_revision,
                expected_ledger_revision: None,
                command_key: "cmd_closure_after_cap_2".to_string(),
                combined_candidate_id: assembled_b.combined_candidate.combined_candidate_id.clone(),
                final_subject: assembled_b.final_subject.clone(),
                current_time_ms: 1_700_000_350_000,
            },
        )
        .await
        .expect_err("exhausted closure role call cap must refuse before effect");
    assert_eq!(cap_err.code(), NativeErrorCode::ValidationFailed.as_str());
    assert_eq!(read_launch_count(&fixture_b.launch_count_file), 1);

    verify_cross_key_unresolved_submission_fenced().await;
}

async fn verify_cross_key_unresolved_submission_fenced() {
    let fixture_c = setup_fixture();
    let (job_c, _, _) = prepare_integrated_job_with_receipts(
        &fixture_c,
        "brf_cand_3c",
        "plan-cand-3c",
        3,
        threadlane_workers::DEFAULT_MAX_ADVISORY_RESPONSE_BYTES,
    );
    let assembled_c = fixture_c
        .candidate_service
        .assemble_and_inspect_operations_candidate(
            &fixture_c.authority,
            &fixture_c.policy_store,
            &AssembleOperationsFinalCandidateRequest {
                binding_id: BINDING_ID.to_string(),
                thread_uuid: THREAD_UUID.to_string(),
                job_id: job_c.job_id.clone(),
                expected_job_revision: job_c.job_revision,
                expected_ledger_revision: None,
                request_key: "req_cand_3c".to_string(),
                work_area_root: fixture_c.work_area_root.clone(),
                run_id: fixture_c.run_id.clone(),
                integration_worktree_id: fixture_c.integration_manifest.worktree_id.clone(),
                check_evidence: default_check_evidence(),
                closure_task_id: "task-closure".to_string(),
                reserve_closure_attempt: false,
                reserved_at_ms: 1_700_000_360_000,
            },
        )
        .expect("assemble candidate 3c");

    // Pre-reserve and mark Submitted an unresolved attempt under `cmd_unresolved_1`.
    let closure_store =
        FinalCandidateClosureStore::open(&fixture_c.repo_dir, &fixture_c.work_area_root)
            .expect("open closure store");
    let res = closure_store
        .reserve_closure_attempt(&ReserveClosureAttemptRequest {
            expected_ledger_revision: 0,
            command_key: "cmd_unresolved_1".to_string(),
            attempt_id: Some("cla_unresolved_1".to_string()),
            subject: assembled_c.final_subject.clone(),
            legacy_base_tree_digest: Some(fixture_c.base_tree_sha.clone()),
            reserved_at_ms: 1_700_000_361_000,
        })
        .expect("reserve unresolved attempt");
    closure_store
        .mark_closure_attempt_submitted(&MarkClosureSubmittedRequest {
            expected_ledger_revision: res.ledger_revision(),
            attempt_id: "cla_unresolved_1".to_string(),
            command_key: "cmd_unresolved_1".to_string(),
            expected_subject_digest: assembled_c.final_subject.subject_digest.clone(),
            external_request_id: "ext_unresolved_1".to_string(),
            submitted_at_ms: 1_700_000_362_000,
        })
        .expect("mark unresolved attempt submitted");

    configure_fake_agy_valid_closure(
        &fixture_c,
        &assembled_c.final_subject,
        ClosureVerdictKind::Accepted,
        "Should never be invoked",
        Vec::new(),
    );

    let bypass_err = fixture_c
        .candidate_service
        .execute_final_candidate_closure(
            &fixture_c.authority,
            &fixture_c.policy_store,
            &ExecuteFinalCandidateClosureRequest {
                binding_id: BINDING_ID.to_string(),
                thread_uuid: THREAD_UUID.to_string(),
                job_id: job_c.job_id.clone(),
                expected_job_revision: assembled_c.job.job_revision,
                expected_ledger_revision: None,
                command_key: "cmd_unresolved_bypass_2".to_string(),
                combined_candidate_id: assembled_c.combined_candidate.combined_candidate_id.clone(),
                final_subject: assembled_c.final_subject.clone(),
                current_time_ms: 1_700_000_365_000,
            },
        )
        .await
        .expect_err("changed command_key must not bypass unresolved Submitted attempt");
    assert_eq!(
        bypass_err.code(),
        NativeErrorCode::AmbiguousWakeupPendingReconciliation.as_str()
    );
    assert_eq!(read_launch_count(&fixture_c.launch_count_file), 0);
}

#[tokio::test]
async fn active_hold_revision_or_candidate_drift_during_closure_run_fences_advancement_and_preserves_late_evidence(
) {
    let fixture = setup_fixture();
    let (job, _, _) = prepare_integrated_job_with_receipts(
        &fixture,
        "brf_cand_4a",
        "plan-cand-4a",
        2,
        threadlane_workers::DEFAULT_MAX_ADVISORY_RESPONSE_BYTES,
    );
    let assembled = fixture
        .candidate_service
        .assemble_and_inspect_operations_candidate(
            &fixture.authority,
            &fixture.policy_store,
            &AssembleOperationsFinalCandidateRequest {
                binding_id: BINDING_ID.to_string(),
                thread_uuid: THREAD_UUID.to_string(),
                job_id: job.job_id.clone(),
                expected_job_revision: job.job_revision,
                expected_ledger_revision: None,
                request_key: "req_cand_4a".to_string(),
                work_area_root: fixture.work_area_root.clone(),
                run_id: fixture.run_id.clone(),
                integration_worktree_id: fixture.integration_manifest.worktree_id.clone(),
                check_evidence: default_check_evidence(),
                closure_task_id: "task-closure".to_string(),
                reserve_closure_attempt: true,
                reserved_at_ms: 1_700_000_400_000,
            },
        )
        .expect("assemble candidate 4a");

    let started_flag = fixture.control_dir.join("closure_started.flag");
    let proceed_flag = fixture.control_dir.join("closure_proceed.flag");
    let payload = StructuredClosureModelResponse {
        schema_version: STRUCTURED_CLOSURE_RESPONSE_SCHEMA_VERSION,
        subject_digest: assembled.final_subject.subject_digest.clone(),
        candidate_tree_sha: assembled.final_subject.candidate_tree_sha.clone(),
        outcome: ClosureVerdictKind::Accepted,
        summary: "Late valid closure output during active hold".to_string(),
        findings: Vec::new(),
    };
    let payload_str = serde_json::to_string(&payload).expect("serialize payload");
    let init_line = serde_json::json!({
        "event": "init",
        "init": {
            "conversation_id": "ses_closure_hold_1",
            "model": "gemini-4-argon-high",
            "effort": "high"
        }
    })
    .to_string();
    let result_line = serde_json::json!({
        "event": "result",
        "result": {
            "status": "SUCCESS",
            "conversation_id": "ses_closure_hold_1",
            "model": "gemini-4-argon-high",
            "effort": "high",
            "response": payload_str
        }
    })
    .to_string();

    let script = format!(
        "#!/bin/sh\n\
         echo 1 >> \"{}\"\n\
         echo 1 > \"{}\"\n\
         i=0\n\
         while [ ! -f \"{}\" ] && [ $i -lt 200 ]; do\n\
           sleep 0.01\n\
           i=$((i + 1))\n\
         done\n\
         printf '%s\\n' '{}'\n\
         printf '%s\\n' '{}'\n\
         exit 0\n",
        fixture.launch_count_file.display(),
        started_flag.display(),
        proceed_flag.display(),
        init_line.replace('\'', "'\\''"),
        result_line.replace('\'', "'\\''"),
    );
    write_executable_script(&fixture.fake_agy_exe, &script);

    finish_test_4_hold_and_drift_assertions(
        &fixture,
        &job.job_id,
        &assembled,
        started_flag,
        proceed_flag,
    )
    .await;
}

async fn finish_test_4_hold_and_drift_assertions(
    fixture: &CandidateServiceFixture,
    job_id: &str,
    assembled: &threadlane_native::coworker_workflow::candidate::AssembleOperationsFinalCandidateOutcome,
    started_flag: PathBuf,
    proceed_flag: PathBuf,
) {
    let store_for_hold = fixture.workflow_store.clone();
    let job_id_owned = job_id.to_string();
    let expected_rev = assembled.job.job_revision;
    let hold_thread = std::thread::spawn(move || {
        for _ in 0..200 {
            if started_flag.exists() {
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        store_for_hold
            .apply_workflow_hold(&ApplyWorkflowHoldRequest {
                binding_id: BINDING_ID.to_string(),
                thread_uuid: THREAD_UUID.to_string(),
                job_id: job_id_owned,
                expected_job_revision: expected_rev,
                expected_ledger_revision: None,
                request_key: "req_hold_midflight_4a".to_string(),
                reason: WorkflowHoldReason::ConversationReplanRequired,
                detail: "Operator hold applied while closure worker is running".to_string(),
                directly_affected_task_ids: vec!["task-alpha".to_string()],
                affected_write_paths: vec!["src/alpha.rs".to_string()],
                affected_interfaces: vec!["iface_alpha".to_string()],
                holds_entire_job: true,
            })
            .expect("apply workflow hold mid-flight");
        fs::write(&proceed_flag, "1").expect("write proceed_flag");
    });

    let fenced_outcome = fixture
        .candidate_service
        .execute_final_candidate_closure(
            &fixture.authority,
            &fixture.policy_store,
            &ExecuteFinalCandidateClosureRequest {
                binding_id: BINDING_ID.to_string(),
                thread_uuid: THREAD_UUID.to_string(),
                job_id: job_id.to_string(),
                expected_job_revision: assembled.job.job_revision,
                expected_ledger_revision: None,
                command_key: "req_cand_4a".to_string(),
                combined_candidate_id: assembled.combined_candidate.combined_candidate_id.clone(),
                final_subject: assembled.final_subject.clone(),
                current_time_ms: 1_700_000_410_000,
            },
        )
        .await
        .expect("mid-flight hold fences advancement and records terminal failure");

    hold_thread.join().expect("join hold_thread");

    assert!(fenced_outcome.verdict.is_none());
    assert_eq!(fenced_outcome.attempt.state, ClosureAttemptState::Terminal);
    let tf = fenced_outcome
        .attempt
        .terminal_failure
        .as_ref()
        .expect("fenced terminal_failure");
    assert_eq!(tf.failure_code, "post_completion_fenced");
    assert!(tf.fenced_late_output);
    assert!(tf.response_sha256_digest.is_some());
    assert_eq!(fenced_outcome.job.phase, CoworkerJobPhase::Held);
    assert_ne!(
        fenced_outcome.job.combined_candidates[0].status,
        CombinedCandidateStatus::ClosureAccepted
    );

    let runtime = fixture
        .policy_store
        .get_job_runtime_readonly(BINDING_ID, job_id)
        .expect("read runtime 4a")
        .expect("runtime 4a exists");
    let art = runtime
        .closure_artifacts
        .get(&fenced_outcome.attempt.attempt_id)
        .expect("late closure artifact preserved");
    assert!(art.fenced_from_promotion);
    assert!(art.artifact_ref.is_some());
    assert!(art.execution_report.is_none());
    assert!(art.advisory_response.is_none());
    let loaded_art = fixture
        .policy_store
        .load_closure_execution_artifact(
            BINDING_ID,
            THREAD_UUID,
            job_id,
            &fenced_outcome.attempt.attempt_id,
        )
        .expect("load full fenced closure execution artifact");
    assert!(loaded_art.fenced_from_promotion);
    assert!(loaded_art.advisory_response.is_some());

    verify_candidate_worktree_drift_during_closure_run().await;
}

async fn verify_candidate_worktree_drift_during_closure_run() {
    let fixture = setup_fixture();
    let (job, _, _) = prepare_integrated_job_with_receipts(
        &fixture,
        "brf_cand_4b",
        "plan-cand-4b",
        2,
        threadlane_workers::DEFAULT_MAX_ADVISORY_RESPONSE_BYTES,
    );
    let assembled = fixture
        .candidate_service
        .assemble_and_inspect_operations_candidate(
            &fixture.authority,
            &fixture.policy_store,
            &AssembleOperationsFinalCandidateRequest {
                binding_id: BINDING_ID.to_string(),
                thread_uuid: THREAD_UUID.to_string(),
                job_id: job.job_id.clone(),
                expected_job_revision: job.job_revision,
                expected_ledger_revision: None,
                request_key: "req_cand_4b".to_string(),
                work_area_root: fixture.work_area_root.clone(),
                run_id: fixture.run_id.clone(),
                integration_worktree_id: fixture.integration_manifest.worktree_id.clone(),
                check_evidence: default_check_evidence(),
                closure_task_id: "task-closure".to_string(),
                reserve_closure_attempt: true,
                reserved_at_ms: 1_700_000_420_000,
            },
        )
        .expect("assemble candidate 4b");

    let payload = StructuredClosureModelResponse {
        schema_version: STRUCTURED_CLOSURE_RESPONSE_SCHEMA_VERSION,
        subject_digest: assembled.final_subject.subject_digest.clone(),
        candidate_tree_sha: assembled.final_subject.candidate_tree_sha.clone(),
        outcome: ClosureVerdictKind::Accepted,
        summary: "Late valid closure output during candidate drift".to_string(),
        findings: Vec::new(),
    };
    let payload_str = serde_json::to_string(&payload).expect("serialize payload");
    let init_line = serde_json::json!({
        "event": "init",
        "init": {
            "conversation_id": "ses_closure_drift_1",
            "model": "gemini-4-argon-high",
            "effort": "high"
        }
    })
    .to_string();
    let result_line = serde_json::json!({
        "event": "result",
        "result": {
            "status": "SUCCESS",
            "conversation_id": "ses_closure_drift_1",
            "model": "gemini-4-argon-high",
            "effort": "high",
            "response": payload_str
        }
    })
    .to_string();

    let drift_file = fixture
        .integration_manifest
        .checkout_path
        .join("untracked_midflight_drift.txt");
    let script = format!(
        "#!/bin/sh\n\
         echo 1 >> \"{}\"\n\
         echo drift > \"{}\"\n\
         printf '%s\\n' '{}'\n\
         printf '%s\\n' '{}'\n\
         exit 0\n",
        fixture.launch_count_file.display(),
        drift_file.display(),
        init_line.replace('\'', "'\\''"),
        result_line.replace('\'', "'\\''"),
    );
    write_executable_script(&fixture.fake_agy_exe, &script);

    let drift_outcome = fixture
        .candidate_service
        .execute_final_candidate_closure(
            &fixture.authority,
            &fixture.policy_store,
            &ExecuteFinalCandidateClosureRequest {
                binding_id: BINDING_ID.to_string(),
                thread_uuid: THREAD_UUID.to_string(),
                job_id: job.job_id.clone(),
                expected_job_revision: assembled.job.job_revision,
                expected_ledger_revision: None,
                command_key: "req_cand_4b".to_string(),
                combined_candidate_id: assembled.combined_candidate.combined_candidate_id.clone(),
                final_subject: assembled.final_subject.clone(),
                current_time_ms: 1_700_000_430_000,
            },
        )
        .await
        .expect("mid-flight candidate drift fences advancement");

    assert!(drift_outcome.verdict.is_none());
    let tf = drift_outcome
        .attempt
        .terminal_failure
        .as_ref()
        .expect("drift terminal failure");
    assert_eq!(tf.failure_code, "post_completion_fenced");
    assert!(tf.fenced_late_output);
    assert_ne!(drift_outcome.job.phase, CoworkerJobPhase::Completed);
    assert_primary_checkout_untouched(&fixture);
}

struct WorkerRunPredecessorTask {
    logical_task_id: String,
    worker_run_id: String,
    task_manifest: WorktreeManifest,
    subject: ReviewSubject,
    review_receipt: ReviewReceipt,
    orig_integration_receipt: IntegrationReceipt,
}

#[allow(clippy::too_many_arguments)]
fn commit_review_and_integrate_worker_run_task(
    fixture: &CandidateServiceFixture,
    logical_task_id: &str,
    manifest_task_id: &str,
    worker_run_id: &str,
    token: &str,
    rel_file: &str,
    new_contents: &str,
    timestamp_ms: i64,
) -> WorkerRunPredecessorTask {
    let manager = WorktreeManager::new(
        &fixture.repo_dir,
        &fixture.work_area_root,
        &fixture.base_commit,
    )
    .expect("WorktreeManager::new");
    let mut allowed = BTreeSet::new();
    allowed.insert(rel_file.to_string());

    let worker_int_manifest = manager
        .create_run_worktree_with_token(worker_run_id, &format!("wint_{token}"))
        .expect("create per-worker RunIntegration worktree");
    let task_manifest = manager
        .create_task_worktree_with_token(worker_run_id, manifest_task_id, token, &allowed)
        .expect("create per-worker Task worktree");
    fs::write(task_manifest.checkout_path.join(rel_file), new_contents)
        .expect("write task modification");

    commit_task_worktree_changes(&CommitTaskChangesRequest {
        work_area_root: fixture.work_area_root.clone(),
        task_worktree_id: task_manifest.worktree_id.clone(),
        commit_message: format!("Implement {logical_task_id} ({manifest_task_id})"),
        worker_status: Some(WorkerRunStatus::Succeeded),
        allow_missing_effort_evidence: false,
    })
    .expect("commit task changes");

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
        passed_test_count: 8,
        failed_test_count: 0,
        evidence_summary: "cargo fmt and cargo test passed".to_string(),
    };
    let subject = inspect_review_subject(&InspectReviewSubjectRequest {
        work_area_root: fixture.work_area_root.clone(),
        task_worktree_id: task_manifest.worktree_id.clone(),
        policy,
        test_evidence,
        worker_status: Some(WorkerRunStatus::Succeeded),
    })
    .expect("inspect review subject");

    let review_receipt = accept_review_subject(&ReviewAcceptanceRequest {
        subject: subject.clone(),
        expected_subject_digest: subject.subject_digest.clone(),
        reviewer_id: "op_controller".to_string(),
        review_note: format!("Approved {logical_task_id}"),
        approved_at_ms: timestamp_ms,
        receipt_id: Some(format!("rev_orig_{token}")),
    })
    .expect("accept review subject");

    let orig_integration_receipt = execute_integration(&ExecuteIntegrationRequest {
        work_area_root: fixture.work_area_root.clone(),
        integration_worktree_id: worker_int_manifest.worktree_id.clone(),
        task_worktree_id: task_manifest.worktree_id.clone(),
        subject: subject.clone(),
        review_receipt: review_receipt.clone(),
        strategy: IntegrationStrategy::FastForwardOnly,
        expected_integration_head_commit: fixture.base_commit.clone(),
        expected_task_head_commit: review_receipt.task_head_commit.clone(),
        expected_source_head_commit: fixture.base_commit.clone(),
        operator_id: "op_controller".to_string(),
        integrated_at_ms: timestamp_ms + 10,
        receipt_id: Some(format!("int_orig_{token}")),
    })
    .expect("execute original per-worker integration");
    assert!(orig_integration_receipt.job_aggregation.is_none());

    WorkerRunPredecessorTask {
        logical_task_id: logical_task_id.to_string(),
        worker_run_id: worker_run_id.to_string(),
        task_manifest,
        subject,
        review_receipt,
        orig_integration_receipt,
    }
}

#[tokio::test]
async fn test_5_job_aggregated_candidate_assembly_preserves_original_receipts_and_rejects_tampered_reports(
) {
    let fixture = setup_fixture();
    let job = create_job_and_freeze_envelope(
        &fixture,
        "brf_cand_5",
        "plan-cand-5",
        2,
        threadlane_workers::DEFAULT_MAX_ADVISORY_RESPONSE_BYTES,
    );

    let worker_a = commit_review_and_integrate_worker_run_task(
        &fixture,
        "task-alpha",
        "t_alpha",
        "run_worker_alpha_01",
        "tok5a",
        "src/alpha.rs",
        "pub fn alpha() -> u32 {\n    501\n}\n",
        1_700_000_500_000,
    );
    let worker_b = commit_review_and_integrate_worker_run_task(
        &fixture,
        "task-beta",
        "t_beta",
        "run_worker_beta_02",
        "tok5b",
        "src/beta.rs",
        "pub fn beta() -> u32 {\n    502\n}\n",
        1_700_000_501_000,
    );

    let pred_a = IntegratedPredecessorTask {
        task_id: worker_a.logical_task_id.clone(),
        task_manifest: worker_a.task_manifest.clone(),
        review_receipt: worker_a.review_receipt.clone(),
        integration_receipt: worker_a.orig_integration_receipt.clone(),
    };
    let pred_b = IntegratedPredecessorTask {
        task_id: worker_b.logical_task_id.clone(),
        task_manifest: worker_b.task_manifest.clone(),
        review_receipt: worker_b.review_receipt.clone(),
        integration_receipt: worker_b.orig_integration_receipt.clone(),
    };
    let job = record_integrated_task_on_job(&fixture, job, &pred_a, "src/alpha.rs");
    let job = record_integrated_task_on_job(&fixture, job, &pred_b, "src/beta.rs");
    assert_eq!(job.phase, CoworkerJobPhase::Executing);
    assert!(job
        .tasks
        .iter()
        .all(|t| t.stage == CoworkerTaskStage::Integrated));

    let linkage = job.approval_linkage.as_ref().expect("approval linkage");
    let mut aggregate_write_paths = BTreeSet::new();
    aggregate_write_paths.insert("src/alpha.rs".to_string());
    aggregate_write_paths.insert("src/beta.rs".to_string());

    let job_report = execute_job_integration(&ExecuteJobIntegrationRequest {
        repository_path: fixture.repo_dir.clone(),
        work_area_root: fixture.work_area_root.clone(),
        integration_worktree_id: fixture.integration_manifest.worktree_id.clone(),
        approved_base_commit: fixture.base_commit.clone(),
        expected_source_head_commit: fixture.base_commit.clone(),
        expected_integration_head_commit: fixture.base_commit.clone(),
        job_scope: JobIntegrationScopeProvenance {
            job_id: job.job_id.clone(),
            job_revision: job.job_revision,
            approval_id: linkage.approval_id.clone(),
            approval_subject_digest: linkage.approval_subject_digest.clone(),
            target_integration_run_id: fixture.run_id.clone(),
            aggregate_allowed_write_paths: aggregate_write_paths,
        },
        tasks: vec![
            JobIntegrationTaskInput {
                original_run_id: worker_a.worker_run_id.clone(),
                task_id: "task-alpha".to_string(),
                task_digest: job.tasks[0].task_digest.clone(),
                task_worktree_id: worker_a.task_manifest.worktree_id.clone(),
                subject: worker_a.subject.clone(),
                review_receipt: worker_a.review_receipt.clone(),
                expected_task_head_commit: worker_a.review_receipt.task_head_commit.clone(),
                strategy: IntegrationStrategy::FastForwardOnly,
                receipt_id: Some("int_agg_tok5a".to_string()),
                dependency_input: None,
            },
            JobIntegrationTaskInput {
                original_run_id: worker_b.worker_run_id.clone(),
                task_id: "task-beta".to_string(),
                task_digest: job.tasks[1].task_digest.clone(),
                task_worktree_id: worker_b.task_manifest.worktree_id.clone(),
                subject: worker_b.subject.clone(),
                review_receipt: worker_b.review_receipt.clone(),
                expected_task_head_commit: worker_b.review_receipt.task_head_commit.clone(),
                strategy: IntegrationStrategy::MergeNoFf,
                receipt_id: Some("int_agg_tok5b".to_string()),
                dependency_input: None,
            },
        ],
        command_key: "cmd_job_int_5".to_string(),
        intent_id: Some("jint_cand_test_5".to_string()),
        operator_id: "op_controller".to_string(),
        integrated_at_ms: 1_700_000_510_000,
    })
    .expect("execute_job_integration across distinct worker runs");

    let base_candidate_req = AssembleOperationsFinalCandidateRequest {
        binding_id: BINDING_ID.to_string(),
        thread_uuid: THREAD_UUID.to_string(),
        job_id: job.job_id.clone(),
        expected_job_revision: job.job_revision,
        expected_ledger_revision: None,
        request_key: "req_cand_5_agg".to_string(),
        work_area_root: fixture.work_area_root.clone(),
        run_id: fixture.run_id.clone(),
        integration_worktree_id: fixture.integration_manifest.worktree_id.clone(),
        check_evidence: default_check_evidence(),
        closure_task_id: "task-closure".to_string(),
        reserve_closure_attempt: true,
        reserved_at_ms: 1_700_000_520_000,
    };

    // 1. Negative: partial report (missing second task receipt) is rejected without advancing job.
    let mut partial_report = job_report.clone();
    partial_report.receipts.pop();
    let partial_err = fixture
        .candidate_service
        .assemble_and_inspect_job_aggregate_candidate(
            &fixture.authority,
            &fixture.policy_store,
            &AssembleJobAggregateCandidateRequest {
                candidate: base_candidate_req.clone(),
                job_integration_report: partial_report,
            },
        )
        .expect_err("partial job integration report must be refused");
    assert_eq!(
        partial_err.code(),
        NativeErrorCode::ValidationFailed.as_str()
    );

    // 2. Negative: relabeled logical_task_id in report receipt is rejected.
    let mut relabeled_report = job_report.clone();
    relabeled_report.receipts[0]
        .job_aggregation
        .as_mut()
        .expect("agg provenance")
        .logical_task_id = "task-beta".to_string();
    let relabeled_err = fixture
        .candidate_service
        .assemble_and_inspect_job_aggregate_candidate(
            &fixture.authority,
            &fixture.policy_store,
            &AssembleJobAggregateCandidateRequest {
                candidate: base_candidate_req.clone(),
                job_integration_report: relabeled_report,
            },
        )
        .expect_err("relabeled logical_task_id must be refused");
    assert_eq!(
        relabeled_err.code(),
        NativeErrorCode::ValidationFailed.as_str()
    );

    // 3. Negative: mutated receipt field in caller report vs persisted receipt on disk is rejected.
    let mut mutated_report = job_report.clone();
    mutated_report.receipts[1].operator_id = "op_forged".to_string();
    let mutated_err = fixture
        .candidate_service
        .assemble_and_inspect_job_aggregate_candidate(
            &fixture.authority,
            &fixture.policy_store,
            &AssembleJobAggregateCandidateRequest {
                candidate: base_candidate_req.clone(),
                job_integration_report: mutated_report,
            },
        )
        .expect_err("mutated receipt in report must be refused");
    assert_eq!(
        mutated_err.code(),
        NativeErrorCode::ValidationFailed.as_str()
    );

    let after_negatives = fixture
        .workflow_store
        .read_binding_ledger(BINDING_ID)
        .expect("read binding ledger after negatives")
        .jobs
        .into_iter()
        .find(|j| j.job_id == job.job_id)
        .expect("job exists");
    assert!(after_negatives.combined_candidates.is_empty());
    assert_eq!(after_negatives.job_revision, job.job_revision);

    // 4. Positive: assemble_and_inspect_job_aggregate_candidate binds aggregate receipts on
    // FinalCandidateSubject while preserving original per-worker integration_receipt_id on
    // task candidates and CombinedCandidateIdentity.
    let assembled = fixture
        .candidate_service
        .assemble_and_inspect_job_aggregate_candidate(
            &fixture.authority,
            &fixture.policy_store,
            &AssembleJobAggregateCandidateRequest {
                candidate: base_candidate_req,
                job_integration_report: job_report.clone(),
            },
        )
        .expect("assemble job-aggregated final candidate");

    assert_eq!(
        assembled.final_subject.candidate_head_commit,
        job_report.final_integrated_target_commit
    );
    assert_eq!(
        assembled.final_subject.candidate_tree_sha,
        job_report.final_integrated_tree_sha
    );
    assert_eq!(assembled.final_subject.predecessors.len(), 2);
    assert_eq!(
        assembled.final_subject.predecessors[0].task_id,
        "task-alpha"
    );
    assert_eq!(
        assembled.final_subject.predecessors[0].integration_receipt_id,
        "int_agg_tok5a"
    );
    assert_eq!(assembled.final_subject.predecessors[1].task_id, "task-beta");
    assert_eq!(
        assembled.final_subject.predecessors[1].integration_receipt_id,
        "int_agg_tok5b"
    );

    // Original per-worker integration receipts remain untouched on task candidates,
    // CombinedCandidateIdentity, and on disk for dependency seals.
    assert_eq!(
        assembled.job.tasks[0].candidates[0]
            .integration_receipt_id
            .as_deref(),
        Some("int_orig_tok5a")
    );
    assert_eq!(
        assembled.job.tasks[1].candidates[0]
            .integration_receipt_id
            .as_deref(),
        Some("int_orig_tok5b")
    );
    assert_eq!(
        assembled.combined_candidate.accepted_predecessor_receipts[0].integration_receipt_id,
        "int_orig_tok5a"
    );
    assert_eq!(
        assembled.combined_candidate.accepted_predecessor_receipts[1].integration_receipt_id,
        "int_orig_tok5b"
    );
    let orig_a_disk = load_integration_receipt(&fixture.work_area_root, "int_orig_tok5a")
        .expect("load original receipt a");
    let orig_b_disk = load_integration_receipt(&fixture.work_area_root, "int_orig_tok5b")
        .expect("load original receipt b");
    assert!(orig_a_disk.job_aggregation.is_none());
    assert!(orig_b_disk.job_aggregation.is_none());

    let reserved = assembled
        .closure_reservation
        .as_ref()
        .expect("closure attempt reserved");
    assert_eq!(
        reserved.attempt().subject_digest,
        assembled.final_subject.subject_digest
    );
    assert_eq!(
        reserved.attempt().candidate_tree_sha,
        job_report.final_integrated_tree_sha
    );

    // Execute Closure over the job-aggregated FinalCandidateSubject.
    configure_fake_agy_valid_closure(
        &fixture,
        &assembled.final_subject,
        ClosureVerdictKind::Accepted,
        "Verified job-aggregated final candidate across two worker runs",
        Vec::new(),
    );
    let closure_outcome = fixture
        .candidate_service
        .execute_final_candidate_closure(
            &fixture.authority,
            &fixture.policy_store,
            &ExecuteFinalCandidateClosureRequest {
                binding_id: BINDING_ID.to_string(),
                thread_uuid: THREAD_UUID.to_string(),
                job_id: job.job_id.clone(),
                expected_job_revision: assembled.job.job_revision,
                expected_ledger_revision: None,
                command_key: "req_cand_5_agg".to_string(),
                combined_candidate_id: assembled.combined_candidate.combined_candidate_id.clone(),
                final_subject: assembled.final_subject.clone(),
                current_time_ms: 1_700_000_530_000,
            },
        )
        .await
        .expect("execute_final_candidate_closure on job-aggregated candidate");
    assert_eq!(closure_outcome.job.phase, CoworkerJobPhase::Completed);
    assert_eq!(
        closure_outcome.verdict.expect("verdict").kind,
        ClosureVerdictKind::Accepted
    );
    assert_primary_checkout_untouched(&fixture);
}

fn configure_fake_agy_large_valid_closure(
    fixture: &CandidateServiceFixture,
    subject: &FinalCandidateSubject,
    stream_file_name: &str,
    ready_file: Option<&Path>,
    sleep_ms: Option<u64>,
) -> usize {
    let findings: Vec<String> = (0..180)
        .map(|idx| format!("closure-verification-note-{idx:03}-{}", "x".repeat(6_000)))
        .collect();
    let payload = StructuredClosureModelResponse {
        schema_version: STRUCTURED_CLOSURE_RESPONSE_SCHEMA_VERSION,
        subject_digest: subject.subject_digest.clone(),
        candidate_tree_sha: subject.candidate_tree_sha.clone(),
        outcome: ClosureVerdictKind::Accepted,
        summary: "Large multi-megabyte closure verification report accepted".to_string(),
        findings,
    };
    let payload_str = serde_json::to_string(&payload).expect("serialize large closure response");
    assert!(
        payload_str.len() > 1_048_576,
        "expected structured closure JSON payload to exceed 1 MiB (got {} bytes)",
        payload_str.len()
    );

    let stream_path = fixture.control_dir.join(stream_file_name);
    let mut stream_file = fs::File::create(&stream_path).expect("create large stream file");
    let init_line = serde_json::json!({
        "event": "init",
        "init": {
            "conversation_id": "ses_closure_large_1",
            "model": "gemini-4-argon-high",
            "effort": "high"
        }
    })
    .to_string();
    writeln!(stream_file, "{init_line}").expect("write init line");

    for step in 0..250 {
        let step_line = serde_json::json!({
            "event": "step_update",
            "step_update": {
                "conversation_id": "ses_closure_large_1",
                "model": "gemini-4-argon-high",
                "effort": "high",
                "message": format!("step-{step:04}-{}", "p".repeat(1_024))
            }
        })
        .to_string();
        writeln!(stream_file, "{step_line}").expect("write step_update line");
    }

    let result_line = serde_json::json!({
        "event": "result",
        "result": {
            "status": "SUCCESS",
            "conversation_id": "ses_closure_large_1",
            "model": "gemini-4-argon-high",
            "effort": "high",
            "response": payload_str
        }
    })
    .to_string();
    writeln!(stream_file, "{result_line}").expect("write result line");
    stream_file.sync_all().expect("sync large stream file");

    let ready_cmd = ready_file
        .map(|p| format!("echo ready > \"{}\"\n", p.display()))
        .unwrap_or_default();
    let sleep_cmd = sleep_ms
        .map(|ms| format!("sleep {:.3}\n", (ms as f64) / 1000.0))
        .unwrap_or_default();
    let script = format!(
        "#!/bin/sh\n\
         echo 1 >> \"{}\"\n\
         {}{}\
         cat \"{}\"\n\
         exit 0\n",
        fixture.launch_count_file.display(),
        ready_cmd,
        sleep_cmd,
        stream_path.display(),
    );
    write_executable_script(&fixture.fake_agy_exe, &script);
    payload_str.len()
}

#[tokio::test]
async fn large_closure_output_exceeding_one_mib_persists_full_private_sidecar_keeps_policy_ledger_compact_and_reopens_without_reinvocation_or_double_charge(
) {
    let max_response_bytes = 2 * 1024 * 1024;
    let fixture = setup_fixture();
    let (job, _, _) = prepare_integrated_job_with_receipts(
        &fixture,
        "brf_cand_6_large",
        "plan-cand-6-large",
        2,
        max_response_bytes,
    );

    let assembled = fixture
        .candidate_service
        .assemble_and_inspect_operations_candidate(
            &fixture.authority,
            &fixture.policy_store,
            &AssembleOperationsFinalCandidateRequest {
                binding_id: BINDING_ID.to_string(),
                thread_uuid: THREAD_UUID.to_string(),
                job_id: job.job_id.clone(),
                expected_job_revision: job.job_revision,
                expected_ledger_revision: None,
                request_key: "req_cand_6_assemble".to_string(),
                work_area_root: fixture.work_area_root.clone(),
                run_id: fixture.run_id.clone(),
                integration_worktree_id: fixture.integration_manifest.worktree_id.clone(),
                check_evidence: default_check_evidence(),
                closure_task_id: "task-closure".to_string(),
                reserve_closure_attempt: false,
                reserved_at_ms: 1_700_000_600_000,
            },
        )
        .expect("assemble candidate 6");

    let expected_payload_len = configure_fake_agy_large_valid_closure(
        &fixture,
        &assembled.final_subject,
        "large_closure_stream_6.ndjson",
        None,
        None,
    );

    let first_outcome = fixture
        .candidate_service
        .execute_final_candidate_closure(
            &fixture.authority,
            &fixture.policy_store,
            &ExecuteFinalCandidateClosureRequest {
                binding_id: BINDING_ID.to_string(),
                thread_uuid: THREAD_UUID.to_string(),
                job_id: job.job_id.clone(),
                expected_job_revision: assembled.job.job_revision,
                expected_ledger_revision: None,
                command_key: "req_cand_6_exec".to_string(),
                combined_candidate_id: assembled.combined_candidate.combined_candidate_id.clone(),
                final_subject: assembled.final_subject.clone(),
                current_time_ms: 1_700_000_610_000,
            },
        )
        .await
        .expect("large >1MiB closure output must persist sidecar and succeed");

    assert!(!first_outcome.reused_cached_verdict);
    assert_eq!(first_outcome.job.phase, CoworkerJobPhase::Completed);
    assert_eq!(
        first_outcome.job.combined_candidates[0].status,
        CombinedCandidateStatus::ClosureAccepted
    );
    assert_eq!(
        first_outcome.verdict.as_ref().expect("verdict").kind,
        ClosureVerdictKind::Accepted
    );
    assert_eq!(read_launch_count(&fixture.launch_count_file), 1);

    let ledger_path = fixture
        .policy_store
        .ledger_path_for_binding(BINDING_ID)
        .expect("ledger path");
    let ledger_meta = fs::symlink_metadata(&ledger_path).expect("stat policy ledger");
    assert!(
        ledger_meta.len() < 64 * 1024,
        "controller policy ledger must remain compact under its 1 MiB cap (got {} bytes)",
        ledger_meta.len()
    );

    let sidecar_path = fixture
        .policy_store
        .closure_artifact_path_for_attempt(
            BINDING_ID,
            &job.job_id,
            &first_outcome.attempt.attempt_id,
        )
        .expect("sidecar path");
    let sidecar_meta = fs::symlink_metadata(&sidecar_path).expect("stat sidecar file");
    assert!(!sidecar_meta.file_type().is_symlink());
    assert_eq!(sidecar_meta.permissions().mode() & 0o777, 0o600);
    assert!(
        sidecar_meta.len() > 1_048_576,
        "private sidecar must hold >1 MiB full report + response (got {} bytes)",
        sidecar_meta.len()
    );

    let runtime = fixture
        .policy_store
        .get_job_runtime_readonly(BINDING_ID, &job.job_id)
        .expect("read runtime")
        .expect("runtime exists");
    assert_eq!(runtime.role_usage[&WorkflowRole::Closure].calls_reserved, 1);
    assert_eq!(
        runtime.role_usage[&WorkflowRole::Closure].calls_completed,
        1
    );
    let compact_record = runtime
        .closure_artifacts
        .get(&first_outcome.attempt.attempt_id)
        .expect("compact artifact record in ledger");
    assert!(compact_record.artifact_ref.is_some());
    assert!(compact_record.execution_report.is_none());
    assert!(compact_record.advisory_response.is_none());

    let loaded = fixture
        .candidate_service
        .load_closure_execution_artifact(
            &fixture.policy_store,
            BINDING_ID,
            THREAD_UUID,
            &job.job_id,
            &first_outcome.attempt.attempt_id,
        )
        .expect("load verified full closure artifact from sidecar");
    assert_eq!(loaded.attempt_id, first_outcome.attempt.attempt_id);
    assert_eq!(loaded.command_key, "req_cand_6_exec");
    assert_eq!(
        loaded.subject_digest,
        assembled.final_subject.subject_digest
    );
    assert_eq!(
        loaded.candidate_tree_sha,
        assembled.final_subject.candidate_tree_sha
    );
    assert_eq!(
        loaded.observed_model.as_deref(),
        Some("gemini-4-argon-high")
    );
    assert_eq!(loaded.observed_effort.as_deref(), Some("high"));
    assert!(loaded.execution_report.stdout_bytes_captured > 1_048_576);
    assert_eq!(loaded.execution_report.evidence.retained_events.len(), 252);
    let loaded_resp = loaded
        .advisory_response
        .as_ref()
        .expect("full advisory response");
    assert_eq!(loaded_resp.byte_len(), expected_payload_len);
    loaded_resp
        .validate_integrity(max_response_bytes)
        .expect("validate loaded >1MiB response integrity");

    // Reopening the same subject performs no second provider invocation and no second completion count.
    let reopen_outcome = fixture
        .candidate_service
        .execute_final_candidate_closure(
            &fixture.authority,
            &fixture.policy_store,
            &ExecuteFinalCandidateClosureRequest {
                binding_id: BINDING_ID.to_string(),
                thread_uuid: THREAD_UUID.to_string(),
                job_id: job.job_id.clone(),
                expected_job_revision: first_outcome.job.job_revision,
                expected_ledger_revision: None,
                command_key: "req_cand_6_reopen".to_string(),
                combined_candidate_id: assembled.combined_candidate.combined_candidate_id.clone(),
                final_subject: assembled.final_subject.clone(),
                current_time_ms: 1_700_000_620_000,
            },
        )
        .await
        .expect("reopen accepted >1MiB closure subject");
    assert!(reopen_outcome.reused_cached_verdict);
    assert_eq!(read_launch_count(&fixture.launch_count_file), 1);
    let runtime_after_reopen = fixture
        .policy_store
        .get_job_runtime_readonly(BINDING_ID, &job.job_id)
        .expect("read runtime after reopen")
        .expect("runtime exists");
    assert_eq!(
        runtime_after_reopen.role_usage[&WorkflowRole::Closure].calls_reserved,
        1
    );
    assert_eq!(
        runtime_after_reopen.role_usage[&WorkflowRole::Closure].calls_completed,
        1
    );

    verify_post_sidecar_crash_recovery_without_reinvocation_or_double_charge(
        &loaded,
        max_response_bytes,
    )
    .await;
    assert_primary_checkout_untouched(&fixture);
}

async fn verify_post_sidecar_crash_recovery_without_reinvocation_or_double_charge(
    template_loaded: &threadlane_native::coworker_workflow::controller_policy::LoadedClosureExecutionArtifact,
    max_response_bytes: usize,
) {
    let fixture = setup_fixture();
    let (job, _, _) = prepare_integrated_job_with_receipts(
        &fixture,
        "brf_cand_6_recover",
        "plan-cand-6-recover",
        2,
        max_response_bytes,
    );
    let assembled = fixture
        .candidate_service
        .assemble_and_inspect_operations_candidate(
            &fixture.authority,
            &fixture.policy_store,
            &AssembleOperationsFinalCandidateRequest {
                binding_id: BINDING_ID.to_string(),
                thread_uuid: THREAD_UUID.to_string(),
                job_id: job.job_id.clone(),
                expected_job_revision: job.job_revision,
                expected_ledger_revision: None,
                request_key: "req_cand_6_rec_assemble".to_string(),
                work_area_root: fixture.work_area_root.clone(),
                run_id: fixture.run_id.clone(),
                integration_worktree_id: fixture.integration_manifest.worktree_id.clone(),
                check_evidence: default_check_evidence(),
                closure_task_id: "task-closure".to_string(),
                reserve_closure_attempt: true,
                reserved_at_ms: 1_700_000_630_000,
            },
        )
        .expect("assemble candidate 6 recover");

    let reserved = assembled
        .closure_reservation
        .as_ref()
        .expect("reserved attempt");
    let attempt_id = reserved.attempt().attempt_id.clone();
    let command_key = reserved.attempt().command_key.clone();
    let effect_id = threadlane_native::coworker_workflow::contracts::deterministic_prefixed_id(
        "threadlane-native:controller-closure-effect:v1",
        "eff",
        &[&job.job_id, &attempt_id, &command_key],
    );

    // Simulate pre-effect reservation + Submitted state + persisted >1MiB sidecar before crash.
    fixture
        .policy_store
        .mutate_job_runtime_for_operator(BINDING_ID, &job.job_id, |state| {
            let _ = state.reserve_role_call(WorkflowRole::Closure)?;
            state.closure_attempt_id = Some(attempt_id.clone());
            state.pending_effects.insert(
                effect_id.clone(),
                PendingControllerEffect {
                    effect_id: effect_id.clone(),
                    request_key: command_key.clone(),
                    job_id: job.job_id.clone(),
                    task_id: None,
                    attempt_no: None,
                    kind: PendingControllerEffectKind::ClosureInvocation,
                    status: PendingControllerEffectStatus::ReservedBeforeEffect,
                    subject_digest: assembled.final_subject.subject_digest.clone(),
                    external_correlation_id: Some(attempt_id.clone()),
                    created_at_ms: 1_700_000_631_000,
                    updated_at_ms: 1_700_000_631_000,
                    diagnostic: None,
                },
            );
            Ok(())
        })
        .expect("reserve intent and role budget before effect");

    let closure_store =
        FinalCandidateClosureStore::open(&fixture.repo_dir, &fixture.work_area_root)
            .expect("open closure store");
    closure_store
        .mark_closure_attempt_submitted(&MarkClosureSubmittedRequest {
            expected_ledger_revision: reserved.ledger_revision(),
            attempt_id: attempt_id.clone(),
            command_key: command_key.clone(),
            expected_subject_digest: assembled.final_subject.subject_digest.clone(),
            external_request_id: "crq_SimulatedCrashBeforeLedgerUpdate".to_string(),
            submitted_at_ms: 1_700_000_632_000,
        })
        .expect("mark submitted");

    let rec_payload = StructuredClosureModelResponse {
        schema_version: STRUCTURED_CLOSURE_RESPONSE_SCHEMA_VERSION,
        subject_digest: assembled.final_subject.subject_digest.clone(),
        candidate_tree_sha: assembled.final_subject.candidate_tree_sha.clone(),
        outcome: ClosureVerdictKind::Accepted,
        summary: "Recovered closure verdict from pre-crash sidecar".to_string(),
        findings: vec!["note".repeat(260_000)],
    };
    let rec_text = serde_json::to_string(&rec_payload).expect("serialize rec_payload");
    let rec_sha = threadlane_native::coworker_workflow::contracts::sha256_hex(rec_text.as_bytes());
    let mut rec_resp = template_loaded
        .advisory_response
        .clone()
        .expect("template response");
    rec_resp.text = rec_text.clone();
    rec_resp.byte_count = rec_text.len();
    rec_resp.sha256_digest = rec_sha;

    fixture
        .policy_store
        .persist_closure_execution_artifact(&PersistClosureExecutionArtifactRequest {
            binding_id: BINDING_ID.to_string(),
            thread_uuid: THREAD_UUID.to_string(),
            job_id: job.job_id.clone(),
            effect_id: effect_id.clone(),
            attempt_id: attempt_id.clone(),
            command_key: command_key.clone(),
            subject_digest: assembled.final_subject.subject_digest.clone(),
            candidate_tree_sha: assembled.final_subject.candidate_tree_sha.clone(),
            execution_report: template_loaded.execution_report.clone(),
            advisory_response: Some(rec_resp),
            response_error: None,
            observed_model: Some("gemini-4-argon-high".to_string()),
            observed_effort: Some("high".to_string()),
            recorded_at_ms: 1_700_000_633_000,
        })
        .expect("persist sidecar before simulated crash");

    fs::remove_file(&fixture.fake_agy_exe)
        .expect("remove fake_agy_exe before persisted sidecar recovery");

    let recovered_outcome = fixture
        .candidate_service
        .execute_final_candidate_closure(
            &fixture.authority,
            &fixture.policy_store,
            &ExecuteFinalCandidateClosureRequest {
                binding_id: BINDING_ID.to_string(),
                thread_uuid: THREAD_UUID.to_string(),
                job_id: job.job_id.clone(),
                expected_job_revision: assembled.job.job_revision,
                expected_ledger_revision: None,
                command_key: command_key.clone(),
                combined_candidate_id: assembled.combined_candidate.combined_candidate_id.clone(),
                final_subject: assembled.final_subject.clone(),
                current_time_ms: 1_700_000_635_000,
            },
        )
        .await
        .expect("recover from persisted sidecar without provider replay");

    assert_eq!(read_launch_count(&fixture.launch_count_file), 0);
    assert_eq!(recovered_outcome.job.phase, CoworkerJobPhase::Completed);
    let runtime_rec = fixture
        .policy_store
        .get_job_runtime_readonly(BINDING_ID, &job.job_id)
        .expect("read recovered runtime")
        .expect("runtime exists");
    assert_eq!(
        runtime_rec.role_usage[&WorkflowRole::Closure].calls_reserved,
        1
    );
    assert_eq!(
        runtime_rec.role_usage[&WorkflowRole::Closure].calls_completed,
        1
    );
}

#[tokio::test]
async fn tampered_or_missing_closure_sidecar_and_immutable_conflict_fail_closed_without_provider_replay_and_preserve_large_fenced_output(
) {
    let max_response_bytes = 2 * 1024 * 1024;
    let fixture = setup_fixture();
    let (job, _, _) = prepare_integrated_job_with_receipts(
        &fixture,
        "brf_cand_7_tamper",
        "plan-cand-7-tamper",
        2,
        max_response_bytes,
    );
    let assembled = fixture
        .candidate_service
        .assemble_and_inspect_operations_candidate(
            &fixture.authority,
            &fixture.policy_store,
            &AssembleOperationsFinalCandidateRequest {
                binding_id: BINDING_ID.to_string(),
                thread_uuid: THREAD_UUID.to_string(),
                job_id: job.job_id.clone(),
                expected_job_revision: job.job_revision,
                expected_ledger_revision: None,
                request_key: "req_cand_7_assemble".to_string(),
                work_area_root: fixture.work_area_root.clone(),
                run_id: fixture.run_id.clone(),
                integration_worktree_id: fixture.integration_manifest.worktree_id.clone(),
                check_evidence: default_check_evidence(),
                closure_task_id: "task-closure".to_string(),
                reserve_closure_attempt: false,
                reserved_at_ms: 1_700_000_700_000,
            },
        )
        .expect("assemble candidate 7");

    configure_fake_agy_valid_closure(
        &fixture,
        &assembled.final_subject,
        ClosureVerdictKind::Accepted,
        "Accepted closure for sidecar immutability and tamper checks",
        Vec::new(),
    );
    let outcome = fixture
        .candidate_service
        .execute_final_candidate_closure(
            &fixture.authority,
            &fixture.policy_store,
            &ExecuteFinalCandidateClosureRequest {
                binding_id: BINDING_ID.to_string(),
                thread_uuid: THREAD_UUID.to_string(),
                job_id: job.job_id.clone(),
                expected_job_revision: assembled.job.job_revision,
                expected_ledger_revision: None,
                command_key: "req_cand_7_exec".to_string(),
                combined_candidate_id: assembled.combined_candidate.combined_candidate_id.clone(),
                final_subject: assembled.final_subject.clone(),
                current_time_ms: 1_700_000_710_000,
            },
        )
        .await
        .expect("execute closure 7");
    assert_eq!(read_launch_count(&fixture.launch_count_file), 1);

    let attempt_id = outcome.attempt.attempt_id.clone();
    let sidecar_path = fixture
        .policy_store
        .closure_artifact_path_for_attempt(BINDING_ID, &job.job_id, &attempt_id)
        .expect("sidecar path");
    let original_bytes = fs::read(&sidecar_path).expect("read original sidecar bytes");
    let loaded = fixture
        .policy_store
        .load_closure_execution_artifact(BINDING_ID, THREAD_UUID, &job.job_id, &attempt_id)
        .expect("load original sidecar");

    // 1. Attempting to publish a different artifact for the same attempt must fail closed and preserve bytes.
    let mut conflicting_report = loaded.execution_report.clone();
    conflicting_report.duration_ms = conflicting_report.duration_ms.saturating_add(999);
    let conflict_err = fixture
        .policy_store
        .persist_closure_execution_artifact(&PersistClosureExecutionArtifactRequest {
            binding_id: BINDING_ID.to_string(),
            thread_uuid: THREAD_UUID.to_string(),
            job_id: job.job_id.clone(),
            effect_id: loaded.artifact_ref.as_ref().expect("ref").effect_id.clone(),
            attempt_id: attempt_id.clone(),
            command_key: loaded.command_key.clone(),
            subject_digest: loaded.subject_digest.clone(),
            candidate_tree_sha: loaded.candidate_tree_sha.clone(),
            execution_report: conflicting_report,
            advisory_response: loaded.advisory_response.clone(),
            response_error: None,
            observed_model: loaded.observed_model.clone(),
            observed_effort: loaded.observed_effort.clone(),
            recorded_at_ms: loaded.recorded_at_ms,
        })
        .expect_err("conflicting artifact publication must fail closed");
    assert_eq!(conflict_err.code, NativeErrorCode::SecurityViolation);
    assert_eq!(
        fs::read(&sidecar_path).expect("read preserved bytes"),
        original_bytes,
        "conflicting publication must preserve existing sidecar bytes untouched"
    );

    // 2. Tampering with the sidecar file must fail closed on both loader and reopen without provider replay.
    let mut tampered_bytes = original_bytes.clone();
    let last_idx = tampered_bytes.len() - 1;
    tampered_bytes[last_idx] = if tampered_bytes[last_idx] == b' ' {
        b'\n'
    } else {
        b' '
    };
    fs::write(&sidecar_path, &tampered_bytes).expect("write tampered sidecar");
    fs::set_permissions(&sidecar_path, fs::Permissions::from_mode(0o600))
        .expect("chmod 0600 tampered sidecar");

    let tamper_load_err = fixture
        .policy_store
        .load_closure_execution_artifact(BINDING_ID, THREAD_UUID, &job.job_id, &attempt_id)
        .expect_err("tampered sidecar must fail loader verification");
    assert_eq!(tamper_load_err.code, NativeErrorCode::ValidationFailed);

    let tamper_reopen_err = fixture
        .candidate_service
        .execute_final_candidate_closure(
            &fixture.authority,
            &fixture.policy_store,
            &ExecuteFinalCandidateClosureRequest {
                binding_id: BINDING_ID.to_string(),
                thread_uuid: THREAD_UUID.to_string(),
                job_id: job.job_id.clone(),
                expected_job_revision: outcome.job.job_revision,
                expected_ledger_revision: None,
                command_key: "req_cand_7_reopen_tampered".to_string(),
                combined_candidate_id: assembled.combined_candidate.combined_candidate_id.clone(),
                final_subject: assembled.final_subject.clone(),
                current_time_ms: 1_700_000_720_000,
            },
        )
        .await
        .expect_err("reopening subject with tampered sidecar must fail closed");
    assert_eq!(tamper_reopen_err.code, NativeErrorCode::ValidationFailed);
    assert_eq!(read_launch_count(&fixture.launch_count_file), 1);

    // 3. Removing the sidecar file must fail closed on both loader and reopen without provider replay.
    fs::remove_file(&sidecar_path).expect("remove sidecar file");
    let missing_load_err = fixture
        .policy_store
        .load_closure_execution_artifact(BINDING_ID, THREAD_UUID, &job.job_id, &attempt_id)
        .expect_err("missing sidecar must fail loader verification");
    assert_eq!(missing_load_err.code, NativeErrorCode::ValidationFailed);

    let missing_reopen_err = fixture
        .candidate_service
        .execute_final_candidate_closure(
            &fixture.authority,
            &fixture.policy_store,
            &ExecuteFinalCandidateClosureRequest {
                binding_id: BINDING_ID.to_string(),
                thread_uuid: THREAD_UUID.to_string(),
                job_id: job.job_id.clone(),
                expected_job_revision: outcome.job.job_revision,
                expected_ledger_revision: None,
                command_key: "req_cand_7_reopen_missing".to_string(),
                combined_candidate_id: assembled.combined_candidate.combined_candidate_id.clone(),
                final_subject: assembled.final_subject.clone(),
                current_time_ms: 1_700_000_730_000,
            },
        )
        .await
        .expect_err("reopening subject with missing sidecar must fail closed");
    assert_eq!(missing_reopen_err.code, NativeErrorCode::ValidationFailed);
    assert_eq!(read_launch_count(&fixture.launch_count_file), 1);

    verify_large_fenced_late_output_preserved(max_response_bytes).await;
}

async fn verify_large_fenced_late_output_preserved(max_response_bytes: usize) {
    let fixture = setup_fixture();
    let (job, _, _) = prepare_integrated_job_with_receipts(
        &fixture,
        "brf_cand_7_fenced",
        "plan-cand-7-fenced",
        2,
        max_response_bytes,
    );
    let assembled = fixture
        .candidate_service
        .assemble_and_inspect_operations_candidate(
            &fixture.authority,
            &fixture.policy_store,
            &AssembleOperationsFinalCandidateRequest {
                binding_id: BINDING_ID.to_string(),
                thread_uuid: THREAD_UUID.to_string(),
                job_id: job.job_id.clone(),
                expected_job_revision: job.job_revision,
                expected_ledger_revision: None,
                request_key: "req_cand_7_fenced_assemble".to_string(),
                work_area_root: fixture.work_area_root.clone(),
                run_id: fixture.run_id.clone(),
                integration_worktree_id: fixture.integration_manifest.worktree_id.clone(),
                check_evidence: default_check_evidence(),
                closure_task_id: "task-closure".to_string(),
                reserve_closure_attempt: false,
                reserved_at_ms: 1_700_000_740_000,
            },
        )
        .expect("assemble candidate 7 fenced");

    let ready_file = fixture.control_dir.join("closure_7_fenced_ready.txt");
    let expected_payload_len = configure_fake_agy_large_valid_closure(
        &fixture,
        &assembled.final_subject,
        "large_closure_stream_7_fenced.ndjson",
        Some(&ready_file),
        Some(350),
    );

    let store_for_hold = fixture.workflow_store.clone();
    let job_id_for_hold = job.job_id.clone();
    let rev_for_hold = assembled.job.job_revision;
    let ready_for_hold = ready_file.clone();
    let hold_thread = std::thread::spawn(move || {
        for _ in 0..200 {
            if ready_for_hold.exists() {
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        store_for_hold
            .apply_workflow_hold(&ApplyWorkflowHoldRequest {
                binding_id: BINDING_ID.to_string(),
                thread_uuid: THREAD_UUID.to_string(),
                job_id: job_id_for_hold,
                expected_job_revision: rev_for_hold,
                expected_ledger_revision: None,
                request_key: "req_hold_during_large_closure_7".to_string(),
                reason: WorkflowHoldReason::ConversationReplanRequired,
                detail: "Operator hold applied during >1MiB closure worker run".to_string(),
                directly_affected_task_ids: vec!["task-alpha".to_string()],
                affected_write_paths: vec!["src/alpha.rs".to_string()],
                affected_interfaces: vec!["iface_alpha".to_string()],
                holds_entire_job: true,
            })
            .expect("apply mid-flight hold during >1MiB closure");
    });

    let fenced_outcome = fixture
        .candidate_service
        .execute_final_candidate_closure(
            &fixture.authority,
            &fixture.policy_store,
            &ExecuteFinalCandidateClosureRequest {
                binding_id: BINDING_ID.to_string(),
                thread_uuid: THREAD_UUID.to_string(),
                job_id: job.job_id.clone(),
                expected_job_revision: assembled.job.job_revision,
                expected_ledger_revision: None,
                command_key: "req_cand_7_fenced_exec".to_string(),
                combined_candidate_id: assembled.combined_candidate.combined_candidate_id.clone(),
                final_subject: assembled.final_subject.clone(),
                current_time_ms: 1_700_000_750_000,
            },
        )
        .await
        .expect("large fenced closure must persist sidecar and record terminal failure");
    hold_thread.join().expect("join hold_thread");

    assert!(fenced_outcome.verdict.is_none());
    assert_eq!(fenced_outcome.job.phase, CoworkerJobPhase::Held);
    let loaded_fenced = fixture
        .policy_store
        .load_closure_execution_artifact(
            BINDING_ID,
            THREAD_UUID,
            &job.job_id,
            &fenced_outcome.attempt.attempt_id,
        )
        .expect("load full >1MiB fenced closure artifact");
    assert!(loaded_fenced.fenced_from_promotion);
    assert_eq!(
        loaded_fenced
            .advisory_response
            .as_ref()
            .expect("fenced response")
            .byte_len(),
        expected_payload_len
    );
}

#[tokio::test]
async fn legacy_inline_closure_artifact_remains_readable_without_rewriting_unrelated_records_and_inconsistent_records_fail_closed(
) {
    let fixture = setup_fixture();
    let (job_main, _, _) = prepare_integrated_job_with_receipts(
        &fixture,
        "brf_cand_8_main",
        "plan-cand-8-main",
        2,
        threadlane_workers::DEFAULT_MAX_ADVISORY_RESPONSE_BYTES,
    );
    let job_unrelated = create_job_and_freeze_envelope(
        &fixture,
        "brf_cand_8_other",
        "plan-cand-8-other",
        2,
        threadlane_workers::DEFAULT_MAX_ADVISORY_RESPONSE_BYTES,
    );

    let assembled = fixture
        .candidate_service
        .assemble_and_inspect_operations_candidate(
            &fixture.authority,
            &fixture.policy_store,
            &AssembleOperationsFinalCandidateRequest {
                binding_id: BINDING_ID.to_string(),
                thread_uuid: THREAD_UUID.to_string(),
                job_id: job_main.job_id.clone(),
                expected_job_revision: job_main.job_revision,
                expected_ledger_revision: None,
                request_key: "req_cand_8_assemble".to_string(),
                work_area_root: fixture.work_area_root.clone(),
                run_id: fixture.run_id.clone(),
                integration_worktree_id: fixture.integration_manifest.worktree_id.clone(),
                check_evidence: default_check_evidence(),
                closure_task_id: "task-closure".to_string(),
                reserve_closure_attempt: false,
                reserved_at_ms: 1_700_000_800_000,
            },
        )
        .expect("assemble candidate 8");

    configure_fake_agy_valid_closure(
        &fixture,
        &assembled.final_subject,
        ClosureVerdictKind::Accepted,
        "Closure output to construct historical inline ledger fixture",
        Vec::new(),
    );
    let outcome = fixture
        .candidate_service
        .execute_final_candidate_closure(
            &fixture.authority,
            &fixture.policy_store,
            &ExecuteFinalCandidateClosureRequest {
                binding_id: BINDING_ID.to_string(),
                thread_uuid: THREAD_UUID.to_string(),
                job_id: job_main.job_id.clone(),
                expected_job_revision: assembled.job.job_revision,
                expected_ledger_revision: None,
                command_key: "req_cand_8_exec".to_string(),
                combined_candidate_id: assembled.combined_candidate.combined_candidate_id.clone(),
                final_subject: assembled.final_subject.clone(),
                current_time_ms: 1_700_000_810_000,
            },
        )
        .await
        .expect("execute closure 8");

    let attempt_id = outcome.attempt.attempt_id.clone();
    let loaded_from_sidecar = fixture
        .policy_store
        .load_closure_execution_artifact(BINDING_ID, THREAD_UUID, &job_main.job_id, &attempt_id)
        .expect("load sidecar artifact 8");
    let sidecar_path = fixture
        .policy_store
        .closure_artifact_path_for_attempt(BINDING_ID, &job_main.job_id, &attempt_id)
        .expect("sidecar path 8");

    let unrelated_before = fixture
        .policy_store
        .get_job_runtime_readonly(BINDING_ID, &job_unrelated.job_id)
        .expect("read unrelated runtime before")
        .expect("unrelated runtime exists");

    // Convert job_main's record to a historical inline record (removing sidecar and artifact_ref).
    fs::remove_file(&sidecar_path).expect("remove sidecar for legacy inline conversion");
    fixture
        .policy_store
        .mutate_job_runtime_for_operator(BINDING_ID, &job_main.job_id, |state| {
            state.closure_artifacts.insert(
                attempt_id.clone(),
                ClosureExecutionArtifactRecord {
                    attempt_id: attempt_id.clone(),
                    command_key: loaded_from_sidecar.command_key.clone(),
                    subject_digest: loaded_from_sidecar.subject_digest.clone(),
                    candidate_tree_sha: loaded_from_sidecar.candidate_tree_sha.clone(),
                    artifact_ref: None,
                    execution_report: Some(loaded_from_sidecar.execution_report.clone()),
                    advisory_response: loaded_from_sidecar.advisory_response.clone(),
                    response_error: None,
                    observed_model: loaded_from_sidecar.observed_model.clone(),
                    observed_effort: loaded_from_sidecar.observed_effort.clone(),
                    fenced_from_promotion: false,
                    diagnostic: None,
                    recorded_at_ms: loaded_from_sidecar.recorded_at_ms,
                },
            );
            Ok(())
        })
        .expect("write valid legacy inline record");

    let loaded_legacy = fixture
        .policy_store
        .load_closure_execution_artifact(BINDING_ID, THREAD_UUID, &job_main.job_id, &attempt_id)
        .expect("load valid legacy inline record");
    assert!(loaded_legacy.artifact_ref.is_none());
    assert_eq!(
        loaded_legacy.execution_report,
        loaded_from_sidecar.execution_report
    );
    assert_eq!(
        loaded_legacy.advisory_response,
        loaded_from_sidecar.advisory_response
    );

    let unrelated_after = fixture
        .policy_store
        .get_job_runtime_readonly(BINDING_ID, &job_unrelated.job_id)
        .expect("read unrelated runtime after")
        .expect("unrelated runtime exists");
    assert_eq!(
        unrelated_after, unrelated_before,
        "reading legacy inline record must not rewrite unrelated runtime records"
    );

    // Reopening the same subject with a valid legacy inline record must also succeed without provider replay.
    let reopen_legacy = fixture
        .candidate_service
        .execute_final_candidate_closure(
            &fixture.authority,
            &fixture.policy_store,
            &ExecuteFinalCandidateClosureRequest {
                binding_id: BINDING_ID.to_string(),
                thread_uuid: THREAD_UUID.to_string(),
                job_id: job_main.job_id.clone(),
                expected_job_revision: outcome.job.job_revision,
                expected_ledger_revision: None,
                command_key: "req_cand_8_reopen_legacy".to_string(),
                combined_candidate_id: assembled.combined_candidate.combined_candidate_id.clone(),
                final_subject: assembled.final_subject.clone(),
                current_time_ms: 1_700_000_820_000,
            },
        )
        .await
        .expect("reopen valid legacy inline closure record");
    assert!(reopen_legacy.reused_cached_verdict);
    assert_eq!(read_launch_count(&fixture.launch_count_file), 1);

    // Mutually inconsistent record (both artifact_ref and inline execution_report) must fail closed.
    let ledger_path = fixture
        .policy_store
        .ledger_path_for_binding(BINDING_ID)
        .expect("ledger path");
    let valid_ledger_bytes = fs::read(&ledger_path).expect("read valid ledger bytes");
    let mut raw_ledger: serde_json::Value =
        serde_json::from_slice(&valid_ledger_bytes).expect("parse ledger JSON");
    raw_ledger["jobs"][&job_main.job_id]["closure_artifacts"][&attempt_id]["artifact_ref"] =
        serde_json::to_value(loaded_from_sidecar.artifact_ref.as_ref().expect("ref"))
            .expect("serialize artifact_ref");
    fs::write(
        &ledger_path,
        serde_json::to_vec_pretty(&raw_ledger).expect("serialize inconsistent ledger"),
    )
    .expect("write inconsistent ledger");
    fs::set_permissions(&ledger_path, fs::Permissions::from_mode(0o600))
        .expect("chmod 0600 inconsistent ledger");

    let both_err = fixture
        .policy_store
        .load_closure_execution_artifact(BINDING_ID, THREAD_UUID, &job_main.job_id, &attempt_id)
        .expect_err("record with both artifact_ref and inline execution_report must fail closed");
    assert_eq!(both_err.code, NativeErrorCode::ValidationFailed);

    // Record with neither artifact_ref nor inline execution_report must fail closed.
    let mut neither_ledger: serde_json::Value =
        serde_json::from_slice(&valid_ledger_bytes).expect("parse ledger JSON");
    neither_ledger["jobs"][&job_main.job_id]["closure_artifacts"][&attempt_id]
        .as_object_mut()
        .expect("artifact object")
        .remove("execution_report");
    fs::write(
        &ledger_path,
        serde_json::to_vec_pretty(&neither_ledger).expect("serialize neither ledger"),
    )
    .expect("write neither ledger");
    fs::set_permissions(&ledger_path, fs::Permissions::from_mode(0o600))
        .expect("chmod 0600 neither ledger");

    let neither_err = fixture
        .policy_store
        .load_closure_execution_artifact(BINDING_ID, THREAD_UUID, &job_main.job_id, &attempt_id)
        .expect_err("record with neither artifact_ref nor execution_report must fail closed");
    assert_eq!(neither_err.code, NativeErrorCode::ValidationFailed);
}

#[tokio::test]
async fn codex_cli_final_candidate_closure_persists_pinned_launch_configuration_with_absent_observed_telemetry_and_honest_observation(
) {
    let fixture = setup_fixture();
    let fake_codex_exe = fixture
        .fake_agy_exe
        .parent()
        .expect("bin dir")
        .join("fake-codex");
    write_executable_script(&fake_codex_exe, "#!/bin/sh\nexit 0\n");

    let snap0 = fixture.registry.snapshot().expect("snapshot 0");
    fixture
        .registry
        .apply(
            snap0.revision,
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
    let snap1 = fixture.registry.snapshot().expect("snapshot 1");
    fixture
        .registry
        .apply(
            snap1.revision,
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
                        id: "model-sol".to_string(),
                        tool_id: "tool-codex".to_string(),
                        label: "GPT 6.1 Sol".to_string(),
                        model_arg: "gpt-6.1-sol".to_string(),
                        efforts: vec!["xhigh".to_string()],
                        allow_no_effort: false,
                    },
                ],
            },
        )
        .expect("upsert models with model-sol");
    let snap2 = fixture.registry.snapshot().expect("snapshot 2");
    fixture
        .registry
        .apply(
            snap2.revision,
            RegistryChange::UpsertProfile {
                profile: WorkflowProfile {
                    decision_advisor: None,
                    id: "profile-coworker".to_string(),
                    name: "Coworker Headless Profile".to_string(),
                    assignments: vec![
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
                                model_id: "model-sol".to_string(),
                                effort: Some("xhigh".to_string()),
                            },
                        },
                    ],
                },
            },
        )
        .expect("upsert profile with Codex Closure");

    let codex_closure_choice = SelectedRoleChoice {
        tool_id: "tool-codex".to_string(),
        adapter: ToolAdapter::CodexCli,
        route: RoleExecutionRoute::HeadlessWorker,
        model_id: "model-sol".to_string(),
        model_arg: "gpt-6.1-sol".to_string(),
        effort: Some("xhigh".to_string()),
        matched_difficulty: Some(Difficulty::Extreme),
    };
    let codex_policy = ExecutionPolicy {
        schema_version: 1,
        policy_id: "policy-coworker-exec".to_string(),
        allow_rules: vec![
            PolicyAllowRule {
                tool_id: "tool-agy".to_string(),
                model_id: "model-argon".to_string(),
                efforts: vec!["high".to_string()],
                allow_no_effort: false,
                roles: vec![WorkflowRole::Coder, WorkflowRole::Reviewer],
            },
            PolicyAllowRule {
                tool_id: "tool-codex".to_string(),
                model_id: "model-sol".to_string(),
                efforts: vec!["xhigh".to_string()],
                allow_no_effort: false,
                roles: vec![WorkflowRole::Closure],
            },
        ],
    };

    let project_base =
        CanonicalProjectBaseRef::resolve_and_validate(&fixture.repo_dir, &fixture.base_commit)
            .expect("resolve project_base");
    let job = fixture
        .workflow_store
        .create_or_amend_job(&CreateCoworkerJobRequest {
            binding_id: BINDING_ID.to_string(),
            thread_uuid: THREAD_UUID.to_string(),
            request_key: "req-job-codex-closure".to_string(),
            expected_ledger_revision: None,
            work_brief: WorkBrief {
                brief_id: "brf_codex_closure".to_string(),
                binding_id: BINDING_ID.to_string(),
                thread_uuid: THREAD_UUID.to_string(),
                project_base,
                kind: WorkBriefKind::NewJob,
                summary: "Verify Codex CLI read-only closure with pinned launch configuration"
                    .to_string(),
                confirmed_requirements: vec!["Honest telemetry".to_string()],
                proposed_additions: Vec::new(),
                assumptions: Vec::new(),
                open_questions: Vec::new(),
                accepted_architect_decisions: Vec::new(),
            },
            amended_target_task_ids: Vec::new(),
            amended_write_paths: Vec::new(),
            amended_interfaces: Vec::new(),
        })
        .expect("create job");

    let tasks = vec![
        PlanTask {
            id: "task-alpha".to_string(),
            title: "Task Alpha".to_string(),
            instruction: "Update src/alpha.rs".to_string(),
            role: WorkflowRole::Coder,
            difficulty: Difficulty::Hard,
            depends_on: Vec::new(),
            allowed_write_paths: vec!["src/alpha.rs".to_string()],
        },
        PlanTask {
            id: "task-beta".to_string(),
            title: "Task Beta".to_string(),
            instruction: "Update src/beta.rs".to_string(),
            role: WorkflowRole::Coder,
            difficulty: Difficulty::Hard,
            depends_on: Vec::new(),
            allowed_write_paths: vec!["src/beta.rs".to_string()],
        },
    ];
    let reg_snap = fixture.registry.snapshot().expect("registry snapshot");
    let draft = fixture
        .plans
        .change_at(
            0,
            PlanChange::SaveDraft {
                plan_id: "plan-codex-closure".to_string(),
                binding_id: BINDING_ID.to_string(),
                profile_id: "profile-coworker".to_string(),
                profile_revision: reg_snap.revision,
                summary: "Plan codex closure".to_string(),
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
                plan_id: "plan-codex-closure".to_string(),
                revision: draft.revision,
            },
        )
        .expect("publish plan");
    let compiled = compile_plan_revision(&published, &reg_snap).expect("compile plan");
    let summary = fixture
        .authority
        .approve_at(
            OperatorApprovalRequest {
                binding_id: BINDING_ID.to_string(),
                thread_uuid: THREAD_UUID.to_string(),
                plan_id: "plan-codex-closure".to_string(),
                plan_revision: published.revision,
                expected_registry_revision: reg_snap.revision,
                expected_plan_digest: compiled.plan_digest,
                repository_path: fixture.repo_dir.clone(),
                base_commit: fixture.base_commit.clone(),
                approved_task_ids: vec!["task-alpha".to_string(), "task-beta".to_string()],
                approved_write_paths: vec!["src/alpha.rs".to_string(), "src/beta.rs".to_string()],
                max_parallel: 2,
                budgets: ExecutionBudgets::default(),
            },
            1_700_000_001_000,
        )
        .expect("approve plan");

    let cap_env = FrozenControllerCapabilityEnvelope {
        allowed_reviewer_choices: vec![headless_argon_choice(Difficulty::Hard)],
        allowed_closure_choices: vec![codex_closure_choice.clone()],
        max_in_scope_correction_attempts: 2,
        allowed_check_ids: vec!["cargo_fmt".to_string(), "cargo_test".to_string()],
        local_git_integration_allowed: true,
        allow_check_failure_corrections: false,
    };
    let mut task_interfaces = BTreeMap::new();
    task_interfaces.insert(
        "task-alpha".to_string(),
        TaskInterfaceOwnershipSpec {
            owned_interfaces: vec!["iface_alpha".to_string()],
            consumed_interfaces: Vec::new(),
        },
    );
    task_interfaces.insert(
        "task-beta".to_string(),
        TaskInterfaceOwnershipSpec {
            owned_interfaces: vec!["iface_beta".to_string()],
            consumed_interfaces: Vec::new(),
        },
    );
    let bound_job = fixture
        .workflow_store
        .bind_job_approval(
            &BindJobApprovalRequest {
                binding_id: BINDING_ID.to_string(),
                thread_uuid: THREAD_UUID.to_string(),
                job_id: job.job_id,
                expected_job_revision: job.job_revision,
                expected_ledger_revision: None,
                approval_id: summary.approval_id,
                policy: codex_policy.clone(),
                controller_envelope: Some(cap_env),
                task_interfaces,
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
        binding_id: BINDING_ID.to_string(),
        thread_uuid: THREAD_UUID.to_string(),
        job_id: bound_job.job_id.clone(),
        expected_job_revision: bound_job.job_revision,
        policy: codex_policy,
        reviewer_choice: headless_argon_choice(Difficulty::Hard),
        closure_choice: codex_closure_choice,
        role_budgets,
        max_in_scope_corrections_per_task: 1,
        max_total_corrections_per_job: 2,
        max_total_task_attempts_per_job: 4,
        check_commands: default_check_commands(&fixture),
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
        .expect("preview codex closure envelope");
    let mut freeze_req = preview_req;
    freeze_req.operator_attested = true;
    freeze_req.confirm_approval_subject_digest = Some(preview.approval_subject_digest);
    freeze_req.confirm_envelope_digest = Some(preview.envelope_digest);
    fixture
        .policy_store
        .record_execution_envelope(&freeze_req, &bound_job, &fixture.authority)
        .expect("freeze codex closure envelope");

    let pred_a = commit_review_and_integrate_task_in_work_area(
        &fixture,
        "task-alpha",
        "tokca",
        "src/alpha.rs",
        "pub fn alpha() -> u32 {\n    77\n}\n",
        IntegrationStrategy::FastForwardOnly,
        1_700_000_110_000,
    );
    let pred_b = commit_review_and_integrate_task_in_work_area(
        &fixture,
        "task-beta",
        "tokcb",
        "src/beta.rs",
        "pub fn beta() -> u32 {\n    88\n}\n",
        IntegrationStrategy::MergeNoFf,
        1_700_000_120_000,
    );
    let job = record_integrated_task_on_job(&fixture, bound_job, &pred_a, "src/alpha.rs");
    let job = record_integrated_task_on_job(&fixture, job, &pred_b, "src/beta.rs");

    let assembled = fixture
        .candidate_service
        .assemble_and_inspect_operations_candidate(
            &fixture.authority,
            &fixture.policy_store,
            &AssembleOperationsFinalCandidateRequest {
                binding_id: BINDING_ID.to_string(),
                thread_uuid: THREAD_UUID.to_string(),
                job_id: job.job_id.clone(),
                expected_job_revision: job.job_revision,
                expected_ledger_revision: None,
                request_key: "req_codex_closure_assemble".to_string(),
                work_area_root: fixture.work_area_root.clone(),
                run_id: fixture.run_id.clone(),
                integration_worktree_id: fixture.integration_manifest.worktree_id.clone(),
                check_evidence: default_check_evidence(),
                closure_task_id: "task-closure".to_string(),
                reserve_closure_attempt: true,
                reserved_at_ms: 1_700_000_900_000,
            },
        )
        .expect("assemble candidate for codex closure");

    assert_eq!(
        assembled.final_subject.policy.requested_provider,
        "codex_cli"
    );
    assert_eq!(
        assembled.final_subject.policy.requested_model,
        "gpt-6.1-sol"
    );
    assert_eq!(
        assembled.final_subject.policy.requested_effort.as_deref(),
        Some("xhigh")
    );
    assert!(assembled.final_subject.policy.require_effort_evidence);

    let payload = StructuredClosureModelResponse {
        schema_version: STRUCTURED_CLOSURE_RESPONSE_SCHEMA_VERSION,
        subject_digest: assembled.final_subject.subject_digest.clone(),
        candidate_tree_sha: assembled.final_subject.candidate_tree_sha.clone(),
        outcome: ClosureVerdictKind::Accepted,
        summary: "Codex read-only closure verified final candidate.".to_string(),
        findings: Vec::new(),
    };
    let payload_str = serde_json::to_string(&payload).expect("serialize codex closure response");
    let line_thread = serde_json::json!({
        "type": "thread.started",
        "thread_id": "019d8e5f-1111-7222-8333-444455556666"
    })
    .to_string();
    let line_turn_start = serde_json::json!({
        "type": "turn.started"
    })
    .to_string();
    let line_item = serde_json::json!({
        "type": "item.completed",
        "item": {
            "id": "item_1",
            "type": "agent_message",
            "text": payload_str
        }
    })
    .to_string();
    let line_turn_done = serde_json::json!({
        "type": "turn.completed",
        "usage": {
            "input_tokens": 1200,
            "cached_input_tokens": 400,
            "output_tokens": 95
        }
    })
    .to_string();
    let codex_script = format!(
        "#!/bin/sh\n\
         echo 1 >> \"{}\"\n\
         printf '%s\\n' '{}'\n\
         printf '%s\\n' '{}'\n\
         printf '%s\\n' '{}'\n\
         printf '%s\\n' '{}'\n\
         exit 0\n",
        fixture.launch_count_file.display(),
        line_thread.replace('\'', "'\\''"),
        line_turn_start.replace('\'', "'\\''"),
        line_item.replace('\'', "'\\''"),
        line_turn_done.replace('\'', "'\\''"),
    );
    write_executable_script(&fake_codex_exe, &codex_script);

    let outcome = fixture
        .candidate_service
        .execute_final_candidate_closure(
            &fixture.authority,
            &fixture.policy_store,
            &ExecuteFinalCandidateClosureRequest {
                binding_id: BINDING_ID.to_string(),
                thread_uuid: THREAD_UUID.to_string(),
                job_id: job.job_id.clone(),
                expected_job_revision: assembled.job.job_revision,
                expected_ledger_revision: None,
                command_key: "req_codex_closure_assemble".to_string(),
                combined_candidate_id: assembled.combined_candidate.combined_candidate_id.clone(),
                final_subject: assembled.final_subject.clone(),
                current_time_ms: 1_700_000_910_000,
            },
        )
        .await
        .expect("execute codex_cli final candidate closure");

    let verdict = outcome.verdict.as_ref().expect("accepted codex verdict");
    assert_eq!(verdict.kind, ClosureVerdictKind::Accepted);
    assert_eq!(verdict.execution_evidence.observed_provider, "codex_cli");
    assert_eq!(
        verdict.execution_evidence.observed_model, None,
        "codex_cli closure must never fabricate runtime observed_model telemetry"
    );
    assert_eq!(
        verdict.execution_evidence.observed_effort, None,
        "codex_cli closure must never fabricate runtime observed_effort telemetry"
    );
    let pin = verdict
        .execution_evidence
        .pinned_launch_configuration
        .as_ref()
        .expect("pinned_launch_configuration must be recorded for codex_cli closure");
    assert_eq!(pin.requested_model, "gpt-6.1-sol");
    assert_eq!(pin.requested_effort, "xhigh");
    assert_eq!(pin.constructed_argv_sha256.len(), 64);

    let loaded_artifact = fixture
        .policy_store
        .load_closure_execution_artifact(
            BINDING_ID,
            THREAD_UUID,
            &job.job_id,
            &outcome.attempt.attempt_id,
        )
        .expect("load codex closure sidecar artifact");
    let health = loaded_artifact
        .execution_report
        .provider_health
        .as_ref()
        .expect("unsupported provider coverage is explicit");
    assert_eq!(
        health.availability,
        threadlane_workers::provider_health::ProviderDiagnosticAvailability::Unavailable
    );
    assert_eq!(health.unavailable_reason,
        Some(threadlane_workers::provider_health::ProviderDiagnosticUnavailableReason::UnsupportedProvider));
    assert_eq!(health.retry_count, 0);
    assert!(!loaded_artifact
        .execution_report
        .constructed_argv
        .iter()
        .any(|arg| arg == "--log-file"));
    assert_eq!(loaded_artifact.observed_model, None);
    assert_eq!(loaded_artifact.observed_effort, None);
    assert!(loaded_artifact
        .execution_report
        .has_pinned_codex_read_only_launch_config());
}
