//! Focused Phase 1 and Phase 2 tests for scoped read-only workspace job history projection
//! and store-backed historical acceptance verification.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::io::Write;
use std::os::unix::fs::{symlink, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde::Serialize;
use tempfile::TempDir;
use threadlane_authority::{AuthorityStore, OperatorApprovalRequest};
use threadlane_core::observation::{
    ObservationJobState, WorkspaceAcceptanceProof, WorkspaceAcceptanceStatus,
    WorkspaceAttentionKind, WorkspaceChatIdentity,
};
use threadlane_host::{prepare_session_dir, HostSessionLock, WORK_AREA_SUBDIR};
use threadlane_native::coworker_observation::workspace_history::{
    project_coworker_job_history, project_coworker_job_record, read_scoped_workspace_job_histories,
    WorkspaceHistoryDiagnosticKind, WorkspaceHistoryStores, WorkspaceJobLineageContext,
};
use threadlane_native::coworker_workflow::candidate::{
    AssembleOperationsFinalCandidateRequest, CoworkerCandidateService,
    ExecuteFinalCandidateClosureRequest, StructuredClosureModelResponse,
    STRUCTURED_CLOSURE_RESPONSE_SCHEMA_VERSION,
};
use threadlane_native::coworker_workflow::{
    candidate_check_report_file_path, run_frozen_check, verify_and_promote_check_reports,
    AcceptedPredecessorReceiptRef, ArchitectAdviceRecord, ArchitectContextDependencies,
    ArchitectDecisionStatus, BindJobApprovalRequest, CandidateCheckEvidence,
    CanonicalProjectBaseRef, CombinedCandidateIdentity, CombinedCandidateStatus,
    ControllerConfigurationPreview, ControllerJobRuntimeState, ControllerPolicyStore,
    ControllerRoleExecutionBudget, ControllerRoleUsageCounters, CoworkerBindingLedger,
    CoworkerJobOwnershipKey, CoworkerJobOwnershipStore, CoworkerJobPhase, CoworkerJobRecord,
    CoworkerTaskRecord, CoworkerTaskStage, CoworkerWorkflowStore, CreateCoworkerJobRequest,
    FinalizeTaskOutboxRequest, FreezeControllerExecutionEnvelopeRequest, FrozenApprovalLinkage,
    FrozenCheckCommandSpec, FrozenCheckExecutionStatus, FrozenCheckKind, FrozenCheckSubject,
    FrozenCheckTargetKind, FrozenControllerCapabilityEnvelope, FrozenControllerExecutionEnvelope,
    OutboxDispatchKind, OutboxDispatchRecord, OutboxDispatchStatus, PendingControllerEffect,
    PendingControllerEffectKind, PendingControllerEffectStatus, PlanningAdviceGrant,
    PlanningCallBudget, PlanningCallReservation, PlanningGrantStatus, PlanningGrantUsage,
    PlanningReadOnlyCapabilities, PlanningReservationStatus, PlanningResultRef,
    RecordTaskReviewVerdictRequest, ReserveTaskOutboxRequest, ReviewVerdictOutcome,
    StructuredReviewVerdict, TaskAttemptExecutionArtifacts, TaskAttemptRef, TaskCandidateRef,
    TaskInterfaceOwnershipSpec, WorkBrief, WorkBriefKind, WorkflowHoldReason, WorkflowHoldRecord,
};
use threadlane_native::profile_flow::{
    ExecutionPolicy, PolicyAllowRule, RoleExecutionRoute, SelectedRoleChoice,
};
use threadlane_native::role_flow::RoleFlowStore;
use threadlane_operations::{
    accept_review_subject, commit_task_worktree_changes, execute_integration,
    inspect_review_subject, load_review_receipt, ClosureVerdictKind, CommitTaskChangesRequest,
    ExecuteIntegrationRequest, FinalCandidateSubject, InspectReviewSubjectRequest,
    IntegrationReceipt, IntegrationStrategy, ReviewAcceptanceRequest, ReviewPolicyBinding,
    ReviewReceipt, TestEvidenceSummary,
};
use threadlane_workers::{
    resolve_exact_commit_sha, resolve_exact_tree_sha, run_sanitized_git, ExecutionBudgets,
    WorkerFilePermissions, WorkerRunStatus, WorktreeManager, WorktreeManifest,
    DEFAULT_GIT_COMMAND_TIMEOUT_MS, DEFAULT_MAX_STDERR_BYTES, DEFAULT_MAX_STDOUT_BYTES,
    DEFAULT_TIMEOUT_MS,
};
use threadlane_workflow::{
    compile_plan_revision, Difficulty, ModelChoice, ModelDefinition, PlanChange, PlanStatus,
    PlanStore, PlanTask, RegistryChange, RegistryStore, RoleAssignment, ToolAdapter,
    ToolDefinition, WorkflowProfile, WorkflowRole,
};

const SOURCE_A: &str = "default";
const BINDING_A: &str = "bnd_hist_alpha01";
const BINDING_B: &str = "bnd_hist_bravo02";
const THREAD_A: &str = "550e8400-e29b-41d4-a716-446655440000";
const THREAD_B: &str = "6ba7b810-9dad-11d1-80b4-00c04fd430c8";

const SHA40_BASE: &str = "1111111111111111111111111111111111111111";
const SHA40_BASE_TREE: &str = "2222222222222222222222222222222222222222";
const SHA40_HEAD_1: &str = "3333333333333333333333333333333333333333";
const SHA40_TREE_1: &str = "4444444444444444444444444444444444444444";
const SHA40_HEAD_2: &str = "5555555555555555555555555555555555555555";
const SHA40_TREE_2: &str = "6666666666666666666666666666666666666666";

const HEX64_A: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const HEX64_B: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const HEX64_C: &str = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
const HEX64_D: &str = "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd";

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

fn headless_route() -> RoleExecutionRoute {
    serde_json::from_str("\"headless_worker\"").expect("deserialize headless_worker route")
}

fn sample_orchestrator_choice() -> SelectedRoleChoice {
    SelectedRoleChoice {
        tool_id: "tool-agy".to_string(),
        adapter: ToolAdapter::Antigravity,
        route: headless_route(),
        model_id: "model-argon".to_string(),
        model_arg: "gemini-4-argon-high".to_string(),
        effort: Some("high".to_string()),
        matched_difficulty: Some(Difficulty::Hard),
    }
}

fn sample_architect_choice() -> SelectedRoleChoice {
    SelectedRoleChoice {
        tool_id: "tool-agy".to_string(),
        adapter: ToolAdapter::Antigravity,
        route: headless_route(),
        model_id: "model-argon".to_string(),
        model_arg: "gemini-4-argon-high".to_string(),
        effort: Some("high".to_string()),
        matched_difficulty: Some(Difficulty::Hard),
    }
}

fn sample_planning_policy() -> ExecutionPolicy {
    ExecutionPolicy {
        schema_version: 1,
        policy_id: "policy-planning-test".to_string(),
        allow_rules: vec![PolicyAllowRule {
            tool_id: "tool-agy".to_string(),
            model_id: "model-argon".to_string(),
            efforts: vec!["high".to_string()],
            allow_no_effort: false,
            roles: vec![WorkflowRole::Orchestrator, WorkflowRole::Architect],
        }],
    }
}

fn sample_coder_choice() -> SelectedRoleChoice {
    SelectedRoleChoice {
        tool_id: "tool-agy".to_string(),
        adapter: ToolAdapter::Antigravity,
        route: headless_route(),
        model_id: "model-argon".to_string(),
        model_arg: "gemini-4-argon-high".to_string(),
        effort: Some("high".to_string()),
        matched_difficulty: Some(Difficulty::Hard),
    }
}

fn sample_reviewer_choice() -> SelectedRoleChoice {
    SelectedRoleChoice {
        tool_id: "tool-codex".to_string(),
        adapter: ToolAdapter::CodexCli,
        route: headless_route(),
        model_id: "model-astra".to_string(),
        model_arg: "gpt-6-astra".to_string(),
        effort: Some("high".to_string()),
        matched_difficulty: Some(Difficulty::Hard),
    }
}

fn sample_closure_choice() -> SelectedRoleChoice {
    SelectedRoleChoice {
        tool_id: "tool-codex".to_string(),
        adapter: ToolAdapter::CodexCli,
        route: headless_route(),
        model_id: "model-astra".to_string(),
        model_arg: "gpt-6-astra".to_string(),
        effort: Some("high".to_string()),
        matched_difficulty: Some(Difficulty::Extreme),
    }
}

fn sample_project_base(canonical_repo: &Path) -> CanonicalProjectBaseRef {
    CanonicalProjectBaseRef {
        repository_path: canonical_repo.to_string_lossy().into_owned(),
        base_commit: SHA40_BASE.to_string(),
        base_tree_sha: SHA40_BASE_TREE.to_string(),
    }
}

fn sample_work_brief(
    brief_id: &str,
    binding_id: &str,
    thread_uuid: &str,
    project_base: CanonicalProjectBaseRef,
    kind: WorkBriefKind,
    summary: &str,
) -> (WorkBrief, String) {
    let brief = WorkBrief {
        brief_id: brief_id.to_string(),
        binding_id: binding_id.to_string(),
        thread_uuid: thread_uuid.to_string(),
        project_base,
        kind,
        summary: summary.to_string(),
        confirmed_requirements: vec!["Project workspace history safely".to_string()],
        proposed_additions: Vec::new(),
        assumptions: Vec::new(),
        open_questions: Vec::new(),
        accepted_architect_decisions: Vec::new(),
    };
    let digest = brief.digest().expect("compute work brief digest");
    (brief, digest)
}

fn sample_approval_linkage(
    project_base: CanonicalProjectBaseRef,
    policy_digest: &str,
    task_ids: Vec<String>,
) -> FrozenApprovalLinkage {
    FrozenApprovalLinkage {
        approval_id: "apr_0123456789abcdef0123456789abcdef".to_string(),
        approval_subject_digest: HEX64_A.to_string(),
        plan_id: "plan_hist_01".to_string(),
        plan_revision: 1,
        plan_digest: HEX64_B.to_string(),
        project_base,
        profile_id: "profile-default".to_string(),
        registry_revision: 1,
        policy_id: "policy-test".to_string(),
        policy_digest: policy_digest.to_string(),
        approved_task_ids: task_ids,
        approved_write_paths: vec!["crates/threadlane-native/src/".to_string()],
        controller_envelope: None,
    }
}

fn sample_role_budget(role: WorkflowRole) -> ControllerRoleExecutionBudget {
    ControllerRoleExecutionBudget {
        role,
        max_total_calls: 8,
        max_timeout_ms_per_call: 60_000,
        max_total_runtime_ms: 240_000,
        max_stdout_bytes_per_call: 65_536,
        max_stderr_bytes_per_call: 65_536,
        max_structured_response_bytes: 32_768,
    }
}

fn sample_runtime_state(
    job: &CoworkerJobRecord,
    policy: ExecutionPolicy,
    policy_digest: &str,
) -> ControllerJobRuntimeState {
    let linkage = job
        .approval_linkage
        .as_ref()
        .expect("approved job linkage for runtime state");
    let mut task_coder_choices = BTreeMap::new();
    for task in &job.tasks {
        task_coder_choices.insert(task.task_id.clone(), task.selected_choice.clone());
    }
    let mut role_budgets = BTreeMap::new();
    role_budgets.insert(WorkflowRole::Coder, sample_role_budget(WorkflowRole::Coder));
    role_budgets.insert(
        WorkflowRole::Reviewer,
        sample_role_budget(WorkflowRole::Reviewer),
    );
    role_budgets.insert(
        WorkflowRole::Closure,
        sample_role_budget(WorkflowRole::Closure),
    );

    let mut envelope = FrozenControllerExecutionEnvelope {
        schema_version: 1,
        envelope_id: "env_0123456789abcdef0123456789abcdef".to_string(),
        binding_id: job.binding_id.clone(),
        thread_uuid: job.thread_uuid.clone(),
        job_id: job.job_id.clone(),
        work_brief_id: job.work_brief.brief_id.clone(),
        work_brief_digest: job.work_brief_digest.clone(),
        approval_id: linkage.approval_id.clone(),
        approval_subject_digest: linkage.approval_subject_digest.clone(),
        plan_id: linkage.plan_id.clone(),
        plan_revision: linkage.plan_revision,
        plan_digest: linkage.plan_digest.clone(),
        profile_id: linkage.profile_id.clone(),
        registry_revision: linkage.registry_revision,
        project_base: job.work_brief.project_base.clone(),
        policy,
        policy_digest: policy_digest.to_string(),
        task_coder_choices,
        reviewer_choice: sample_reviewer_choice(),
        closure_choice: sample_closure_choice(),
        role_budgets,
        max_in_scope_corrections_per_task: 2,
        max_total_corrections_per_job: 4,
        max_total_task_attempts_per_job: 8,
        check_commands: BTreeMap::new(),
        allow_task_worktree_commits: true,
        allow_run_integration_execution: true,
        allow_check_failure_corrections: false,
        default_integration_strategy: IntegrationStrategy::FastForwardOnly,
        independence_completeness_receipt: None,
        shadow_jev_policy: None,
        created_at_ms: 1_700_000_100_000,
        operator_attested: true,
        native_per_call_identity_verified: false,
        capability_envelope_digest: HEX64_C.to_string(),
        envelope_digest: String::new(),
    };
    envelope.envelope_digest = envelope.compute_digest().expect("compute envelope digest");

    let mut role_usage = BTreeMap::new();
    role_usage.insert(
        WorkflowRole::Coder,
        ControllerRoleUsageCounters {
            calls_reserved: 1,
            calls_completed: 1,
            total_runtime_ms: 1_500,
        },
    );

    ControllerJobRuntimeState {
        job_id: job.job_id.clone(),
        runtime_revision: 2,
        envelope,
        lease: None,
        role_usage,
        total_task_attempts_reserved: 1,
        total_corrections_reserved: 0,
        derived_attempts: Vec::new(),
        attempt_artifacts: BTreeMap::new(),
        pending_effects: BTreeMap::new(),
        provider_health: BTreeMap::new(),
        original_worker_provider_health: BTreeMap::new(),
        candidate_check_evidence: Vec::new(),
        final_candidate_subject: None,
        closure_attempt_id: None,
        closure_verdict_id: None,
        closure_artifacts: BTreeMap::new(),
        shadow_jev_observations: Vec::new(),
        synced_change_revision: 0,
        stop_requested: false,
        stop_or_block_reason: None,
    }
}

fn write_controller_ledger_file(
    plans_dir: &Path,
    binding_id: &str,
    jobs: BTreeMap<String, ControllerJobRuntimeState>,
) {
    let policy_store = ControllerPolicyStore::new(plans_dir).expect("open controller policy store");
    let ledger_path = policy_store
        .ledger_path_for_binding(binding_id)
        .expect("controller ledger path");
    let payload = serde_json::json!({
        "schema_version": 1,
        "binding_id": binding_id,
        "ledger_revision": 1,
        "jobs": jobs,
    });
    write_private_json(&ledger_path, &payload);
}

#[test]
fn populated_valid_history_projects_tasks_attempts_candidates_outbox_runs_and_links() {
    let tmp = TempDir::new().expect("tempdir");
    let plans_dir = make_private_dir(&tmp.path().join("plans"));
    let repo_dir = make_private_dir(&tmp.path().join("repo-alpha"));

    let chat =
        WorkspaceChatIdentity::new(SOURCE_A, BINDING_A, THREAD_A, &repo_dir.to_string_lossy())
            .expect("valid chat identity");

    let policy = ExecutionPolicy::deny_all("policy-test").expect("deny_all policy");
    let policy_digest = policy.digest().expect("policy digest");
    let project_base = sample_project_base(&repo_dir);

    let (brief_1, brief_digest_1) = sample_work_brief(
        "brf_hist_01",
        BINDING_A,
        THREAD_A,
        project_base.clone(),
        WorkBriefKind::NewJob,
        "Initial history job",
    );
    let job_1 = CoworkerJobRecord {
        job_id: "job_00000000000000000000000000000001".to_string(),
        job_revision: 1,
        create_request_key: "req_create_job_01".to_string(),
        binding_id: BINDING_A.to_string(),
        thread_uuid: THREAD_A.to_string(),
        phase: CoworkerJobPhase::Cancelled,
        work_brief: brief_1,
        work_brief_digest: brief_digest_1,
        supersedes_job_id: None,
        planning_result: None,
        approval_linkage: None,
        tasks: Vec::new(),
        holds: Vec::new(),
        outbox: Vec::new(),
        combined_candidates: Vec::new(),
    };

    let (brief_2, brief_digest_2) = sample_work_brief(
        "brf_hist_02",
        BINDING_A,
        THREAD_A,
        project_base.clone(),
        WorkBriefKind::Amendment {
            target_job_id: job_1.job_id.clone(),
            target_job_revision: 1,
            amendment_reason: "Follow-up scoped slice".to_string(),
        },
        "Amended history job",
    );
    let planning_policy = sample_planning_policy();
    let planning_policy_digest = planning_policy.digest().expect("planning policy digest");
    let orchestrator_choice = sample_orchestrator_choice();
    let architect_choice = sample_architect_choice();

    let mut dependency_file_blobs = BTreeMap::new();
    dependency_file_blobs.insert(
        "crates/threadlane-native/src/lib.rs".to_string(),
        HEX64_A.to_string(),
    );
    let architect_context = ArchitectContextDependencies {
        project_base: project_base.clone(),
        work_brief_digest: brief_digest_2.clone(),
        plan_digest: None,
        policy_digest: planning_policy_digest.clone(),
        dependency_file_blobs,
        interface_ownership_digests: BTreeMap::new(),
        accepted_predecessor_trees: BTreeMap::new(),
    };
    let architect_context_digest = architect_context.context_digest().expect("context digest");

    let architect_advice = ArchitectAdviceRecord {
        advice_id: "adv_0123456789abcdef0123456789abcdef".to_string(),
        request_key: "req_arch_job_02".to_string(),
        grant_id: "grt_0123456789abcdef0123456789abcdef".to_string(),
        reservation_id: "rsv_arch_0123456789abcdef01234567".to_string(),
        binding_id: BINDING_A.to_string(),
        thread_uuid: THREAD_A.to_string(),
        selected_choice: architect_choice.clone(),
        context_dependencies: architect_context,
        context_digest: architect_context_digest.clone(),
        decision_summary: "Keep workspace history projection read-only".to_string(),
        decision_digest: HEX64_A.to_string(),
        report_artifact_id: Some("pca_arch_0123456789abcdef01234567".to_string()),
        status: ArchitectDecisionStatus::Proposed,
        created_at_ms: 1_700_000_045_000,
    };
    architect_advice.validate().expect("valid architect advice");

    let planning_grant = PlanningAdviceGrant {
        grant_id: "grt_0123456789abcdef0123456789abcdef".to_string(),
        create_request_key: "req_grant_hist_01".to_string(),
        binding_id: BINDING_A.to_string(),
        thread_uuid: THREAD_A.to_string(),
        project_base: project_base.clone(),
        profile_id: "profile-default".to_string(),
        registry_revision: 1,
        policy: planning_policy,
        policy_digest: planning_policy_digest.clone(),
        capabilities: PlanningReadOnlyCapabilities::strict_read_only(),
        budget: PlanningCallBudget {
            max_total_calls: 4,
            max_orchestrator_calls: 2,
            max_architect_calls: 2,
            max_timeout_ms_per_call: DEFAULT_TIMEOUT_MS,
            max_output_bytes_per_call: DEFAULT_MAX_STDOUT_BYTES,
        },
        usage: PlanningGrantUsage {
            reserved_total_calls: 2,
            completed_calls: 2,
            failed_calls: 0,
            unknown_effect_calls: 0,
            refused_before_effect_calls: 0,
            reserved_orchestrator_calls: 1,
            reserved_architect_calls: 1,
        },
        status: PlanningGrantStatus::Active,
        created_at_ms: 1_700_000_040_000,
        expires_at_ms: 1_700_003_640_000,
        revoked_at_ms: None,
        revoked_reason: None,
        operator_attested: true,
        native_per_call_identity_verified: false,
        scoped_profile_dependency_proof: None,
        reservations: vec![
            PlanningCallReservation {
                reservation_id: "rsv_arch_0123456789abcdef01234567".to_string(),
                request_key: "req_arch_job_02".to_string(),
                role: WorkflowRole::Architect,
                difficulty: Some(Difficulty::Hard),
                selected_choice: architect_choice,
                context_digest: architect_context_digest,
                call_project_base: Some(project_base.clone()),
                status: PlanningReservationStatus::Completed,
                reserved_at_ms: 1_700_000_044_000,
                updated_at_ms: 1_700_000_045_000,
                run_id: Some("run_00000000-0000-4000-8000-000000000009".to_string()),
                report_artifact_id: Some("pca_arch_0123456789abcdef01234567".to_string()),
                output_digest: Some(HEX64_A.to_string()),
                diagnostic: None,
            },
            PlanningCallReservation {
                reservation_id: "rsv_0123456789abcdef0123456789abcdef".to_string(),
                request_key: "req_plan_job_02".to_string(),
                role: WorkflowRole::Orchestrator,
                difficulty: Some(Difficulty::Hard),
                selected_choice: orchestrator_choice.clone(),
                context_digest: brief_digest_2.clone(),
                call_project_base: Some(project_base.clone()),
                status: PlanningReservationStatus::Completed,
                reserved_at_ms: 1_700_000_048_000,
                updated_at_ms: 1_700_000_050_000,
                run_id: Some("run_00000000-0000-4000-8000-000000000010".to_string()),
                report_artifact_id: Some("pca_0123456789abcdef0123456789abcdef".to_string()),
                output_digest: Some(HEX64_C.to_string()),
                diagnostic: None,
            },
        ],
    };
    planning_grant.validate().expect("valid planning grant");

    let planning_result = PlanningResultRef {
        grant_id: "grt_0123456789abcdef0123456789abcdef".to_string(),
        reservation_id: "rsv_0123456789abcdef0123456789abcdef".to_string(),
        request_key: "req_plan_job_02".to_string(),
        run_id: "run_00000000-0000-4000-8000-000000000010".to_string(),
        report_artifact_id: "pca_0123456789abcdef0123456789abcdef".to_string(),
        output_digest: HEX64_C.to_string(),
        work_brief_digest: brief_digest_2.clone(),
        project_base: project_base.clone(),
        profile_id: "profile-default".to_string(),
        registry_revision: 1,
        policy_digest: planning_policy_digest,
        selected_choice: orchestrator_choice,
        plan_id: "plan_hist_01".to_string(),
        plan_revision: 1,
        plan_digest: HEX64_B.to_string(),
        plan_status: PlanStatus::Published,
        task_interfaces: BTreeMap::new(),
        architect_advice_ids: vec!["adv_0123456789abcdef0123456789abcdef".to_string()],
        resolved_open_questions: Vec::new(),
        deferred_proposals: Vec::new(),
        recorded_at_ms: 1_700_000_050_000,
    };

    let task_record = CoworkerTaskRecord {
        task_id: "task-reader".to_string(),
        task_digest: HEX64_D.to_string(),
        role: WorkflowRole::Coder,
        difficulty: Difficulty::Hard,
        selected_choice: sample_coder_choice(),
        depends_on: Vec::new(),
        write_paths: vec!["crates/threadlane-native/src/".to_string()],
        owned_interfaces: Vec::new(),
        consumed_interfaces: Vec::new(),
        stage: CoworkerTaskStage::ReadyForReview,
        stage_before_hold: None,
        current_attempt_no: 1,
        active_candidate_id: Some("cand_reader_a1".to_string()),
        accepted_candidate_id: None,
        dependency_input_commit: None,
        dependency_input_tree_sha: None,
        accepted_predecessor_receipts: Vec::new(),
        attempts: vec![TaskAttemptRef {
            attempt_no: 1,
            role: WorkflowRole::Coder,
            request_key: "req_task_reader_a1".to_string(),
            outbox_id: "outbox_reader_a1".to_string(),
            run_id: Some("run_00000000-0000-4000-8000-000000000011".to_string()),
            worker_id: Some("wrk_00000000-0000-4000-8000-000000000011".to_string()),
            task_worktree_id: Some("wt_reader_a1".to_string()),
            base_commit: SHA40_BASE.to_string(),
            head_commit: Some(SHA40_HEAD_1.to_string()),
            tree_sha: Some(SHA40_TREE_1.to_string()),
            diff_digest: Some(HEX64_C.to_string()),
            report_artifact_id: Some("trr_0123456789abcdef0123456789abcdef".to_string()),
            observed_model: Some("gemini-4-argon-high".to_string()),
            observed_effort: Some("high".to_string()),
        }],
        candidates: vec![TaskCandidateRef {
            candidate_id: "cand_reader_a1".to_string(),
            task_id: "task-reader".to_string(),
            attempt_no: 1,
            base_commit: SHA40_BASE.to_string(),
            head_commit: SHA40_HEAD_1.to_string(),
            tree_sha: SHA40_TREE_1.to_string(),
            diff_digest: HEX64_C.to_string(),
            write_paths: vec!["crates/threadlane-native/src/".to_string()],
            check_evidence_digests: vec![HEX64_D.to_string()],
            review_subject_digest: Some(HEX64_C.to_string()),
            review_verdict: None,
            review_receipt_id: None,
            integration_receipt_id: None,
        }],
        active_hold_ids: Vec::new(),
        unresolved_outbox_id: None,
    };

    let outbox_record = OutboxDispatchRecord {
        outbox_id: "outbox_reader_a1".to_string(),
        request_key: "req_task_reader_a1".to_string(),
        kind: OutboxDispatchKind::TaskAttemptDispatch,
        task_id: "task-reader".to_string(),
        attempt_no: 1,
        role: WorkflowRole::Coder,
        subject_digest: HEX64_D.to_string(),
        status: OutboxDispatchStatus::DispatchedEffectConfirmed,
        reserved_at_job_revision: 2,
        updated_at_job_revision: 3,
        run_id: Some("run_00000000-0000-4000-8000-000000000011".to_string()),
        worker_id: Some("wrk_00000000-0000-4000-8000-000000000011".to_string()),
        receipt_id: None,
        diagnostic: None,
    };

    let job_2 = CoworkerJobRecord {
        job_id: "job_00000000000000000000000000000002".to_string(),
        job_revision: 3,
        create_request_key: "req_create_job_02".to_string(),
        binding_id: BINDING_A.to_string(),
        thread_uuid: THREAD_A.to_string(),
        phase: CoworkerJobPhase::Executing,
        work_brief: brief_2,
        work_brief_digest: brief_digest_2,
        supersedes_job_id: Some(job_1.job_id.clone()),
        planning_result: Some(planning_result),
        approval_linkage: Some(sample_approval_linkage(
            project_base,
            &policy_digest,
            vec!["task-reader".to_string()],
        )),
        tasks: vec![task_record],
        holds: Vec::new(),
        outbox: vec![outbox_record],
        combined_candidates: Vec::new(),
    };

    let workflow_store = CoworkerWorkflowStore::new(&plans_dir).expect("open workflow store");
    let ledger = CoworkerBindingLedger {
        schema_version: 1,
        binding_id: BINDING_A.to_string(),
        thread_uuid: Some(THREAD_A.to_string()),
        ledger_revision: 2,
        planning_grants: vec![planning_grant],
        architect_advice: vec![architect_advice],
        jobs: vec![job_1.clone(), job_2.clone()],
        admission_markers: BTreeMap::new(),
    };
    ledger
        .validate(BINDING_A)
        .expect("test workflow ledger must pass full CoworkerBindingLedger validation");
    let workflow_ledger_path = workflow_store
        .ledger_path_for_binding(BINDING_A)
        .expect("workflow ledger path");
    write_private_json(&workflow_ledger_path, &ledger);

    let mut rt_2 = sample_runtime_state(&job_2, policy, &policy_digest);
    rt_2.attempt_artifacts.insert(
        ControllerJobRuntimeState::artifact_key("task-reader", 1),
        TaskAttemptExecutionArtifacts {
            task_id: "task-reader".to_string(),
            attempt_no: 1,
            run_id: "run_00000000-0000-4000-8000-000000000011".to_string(),
            worker_id: "wrk_00000000-0000-4000-8000-000000000011".to_string(),
            task_worktree_id: "wt_reader_a1".to_string(),
            integration_worktree_id: "wt_int_01".to_string(),
            execution_base_commit: SHA40_BASE.to_string(),
            observed_provider: "antigravity".to_string(),
            observed_model: Some("gemini-4-argon-high".to_string()),
            codex_launch_report: None,
            codex_source_report_sha256: None,
            codex_source_effect_id: None,
            observed_effort: Some("high".to_string()),
            committed_head_commit: Some(SHA40_HEAD_1.to_string()),
            committed_tree_sha: Some(SHA40_TREE_1.to_string()),
            candidate_id: Some("cand_reader_a1".to_string()),
            check_evidence: vec![CandidateCheckEvidence {
                check_id: "cargo_test".to_string(),
                command_summary: "cargo test --workspace".to_string(),
                passed: true,
                exit_code: 0,
                evidence_digest: HEX64_D.to_string(),
            }],
            test_evidence_summary: None,
            review_subject: None,
            reviewer_response_digest: None,
            review_receipt: None,
            integration_receipt: None,
            is_derived_attempt: false,
        },
    );
    rt_2.pending_effects.insert(
        "eff_00000000000000000000000000000001".to_string(),
        PendingControllerEffect {
            effect_id: "eff_00000000000000000000000000000001".to_string(),
            request_key: "req_task_reader_a1".to_string(),
            job_id: job_2.job_id.clone(),
            task_id: Some("task-reader".to_string()),
            attempt_no: Some(1),
            kind: PendingControllerEffectKind::CoderDispatch,
            status: PendingControllerEffectStatus::EffectConfirmed,
            subject_digest: HEX64_D.to_string(),
            external_correlation_id: Some("run_00000000-0000-4000-8000-000000000011".to_string()),
            created_at_ms: 1_700_000_110_000,
            updated_at_ms: 1_700_000_120_000,
            diagnostic: None,
        },
    );

    let mut rt_map = BTreeMap::new();
    rt_map.insert(job_2.job_id.clone(), rt_2);
    write_controller_ledger_file(&plans_dir, BINDING_A, rt_map);

    let files_before: Vec<_> = fs::read_dir(&plans_dir)
        .expect("read plans_dir before")
        .map(|e| e.expect("dir entry").file_name())
        .collect();

    let stores = WorkspaceHistoryStores::new(&plans_dir);
    let read = read_scoped_workspace_job_histories(&chat, &stores)
        .expect("scoped read of valid populated history");

    let files_after: Vec<_> = fs::read_dir(&plans_dir)
        .expect("read plans_dir after")
        .map(|e| e.expect("dir entry").file_name())
        .collect();
    assert_eq!(
        files_before, files_after,
        "read_scoped_workspace_job_histories must never create lockfiles or sidecar caches"
    );
    assert!(!read.degraded);
    assert_eq!(read.rejected_scope_count, 0);
    assert_eq!(read.jobs_omitted, 0);
    assert_eq!(read.jobs.len(), 2);

    let hist_2 = read
        .jobs
        .iter()
        .find(|j| j.job.job_id == job_2.job_id)
        .expect("amended job projected");
    assert_eq!(hist_2.job.state, ObservationJobState::Reviewing);
    assert_eq!(hist_2.job.execution_status, "ready_for_review");
    assert_eq!(hist_2.job.approval_status, "approved");
    assert_eq!(hist_2.links.supersedes_job_id, Some(job_1.job_id.clone()));
    assert_eq!(hist_2.links.source_plan_id.as_deref(), Some("plan_hist_01"));
    assert_eq!(
        hist_2.links.source_approval_id.as_deref(),
        Some("apr_0123456789abcdef0123456789abcdef")
    );
    assert_eq!(
        hist_2.links.planning_grant_id.as_deref(),
        Some("grt_0123456789abcdef0123456789abcdef")
    );
    assert_eq!(
        hist_2.links.architect_advice_ids,
        vec!["adv_0123456789abcdef0123456789abcdef".to_string()]
    );
    assert_eq!(hist_2.acceptance_proof, WorkspaceAcceptanceProof::unknown());

    assert_eq!(hist_2.tasks.len(), 1);
    let task_hist = &hist_2.tasks[0];
    assert_eq!(task_hist.task_id, "task-reader");
    assert_eq!(task_hist.current_attempt_no, 1);
    assert_eq!(
        task_hist.active_candidate_id.as_deref(),
        Some("cand_reader_a1")
    );
    assert_eq!(task_hist.accepted_candidate_id, None);
    assert_eq!(
        task_hist.acceptance_proof,
        WorkspaceAcceptanceProof::unknown()
    );
    assert_eq!(task_hist.attempts.len(), 1);
    assert_eq!(task_hist.attempts[0].status, "completed");
    assert_eq!(
        task_hist.attempts[0].run_id.as_deref(),
        Some("run_00000000-0000-4000-8000-000000000011")
    );
    assert_eq!(
        task_hist.attempts[0].worker_id.as_deref(),
        Some("wrk_00000000-0000-4000-8000-000000000011")
    );
    assert_eq!(
        task_hist.attempts[0].outbox_id.as_deref(),
        Some("outbox_reader_a1")
    );
    assert_eq!(task_hist.attempts[0].started_at_ms, Some(1_700_000_110_000));
    assert_eq!(
        task_hist.attempts[0].finished_at_ms,
        Some(1_700_000_120_000)
    );
    assert_eq!(task_hist.candidates.len(), 1);
    assert_eq!(task_hist.candidates[0].verification_passed, Some(true));
    assert_eq!(task_hist.candidates[0].review_status, "pending");
    assert_eq!(hist_2.outbox.len(), 1);
    assert_eq!(hist_2.outbox[0].kind, "coder_dispatch");
    assert_eq!(hist_2.outbox[0].status, "dispatched");
    assert_eq!(hist_2.runs.len(), 2); // Orchestrator planning run + Coder attempt run

    let hist_1 = read
        .jobs
        .iter()
        .find(|j| j.job.job_id == job_1.job_id)
        .expect("initial job projected");
    assert_eq!(
        hist_1.links.superseded_by_job_id,
        Some(job_2.job_id.clone())
    );
    assert_eq!(hist_1.links.follow_up_job_ids, vec![job_2.job_id]);
    assert_eq!(hist_1.job.approval_status, "superseded");
}

#[test]
fn rejects_wrong_binding_thread_repository_source_and_mismatched_approval() {
    let tmp = TempDir::new().expect("tempdir");
    let plans_dir = make_private_dir(&tmp.path().join("plans"));
    let auth_dir = make_private_dir(&tmp.path().join("authority"));
    let repo_a = make_private_dir(&tmp.path().join("repo-a"));
    let repo_b = make_private_dir(&tmp.path().join("repo-b"));
    let symlink_repo = tmp.path().join("repo-symlink");
    symlink(&repo_a, &symlink_repo).expect("create symlink repo");

    let (brief_a, brief_digest_a) = sample_work_brief(
        "brf_scope_01",
        BINDING_A,
        THREAD_A,
        sample_project_base(&repo_a),
        WorkBriefKind::NewJob,
        // Even if summary/title mentions repo_b, repository scope must never be inferred from prose.
        &format!("Work mentioning {} in summary", repo_b.display()),
    );
    let policy = ExecutionPolicy::deny_all("policy-test").expect("policy");
    let policy_digest = policy.digest().expect("policy digest");
    let mut scope_linkage = sample_approval_linkage(
        sample_project_base(&repo_a),
        &policy_digest,
        vec!["task-scope-01".to_string()],
    );
    scope_linkage.plan_id = "plan_scope_01".to_string();
    let job_a = CoworkerJobRecord {
        job_id: "job_00000000000000000000000000000010".to_string(),
        job_revision: 1,
        create_request_key: "req_scope_01".to_string(),
        binding_id: BINDING_A.to_string(),
        thread_uuid: THREAD_A.to_string(),
        phase: CoworkerJobPhase::Executing,
        work_brief: brief_a,
        work_brief_digest: brief_digest_a,
        supersedes_job_id: None,
        planning_result: None,
        approval_linkage: Some(scope_linkage),
        tasks: Vec::new(),
        holds: Vec::new(),
        outbox: Vec::new(),
        combined_candidates: Vec::new(),
    };

    // 1. Invalid source_id on raw WorkspaceChatIdentity is rejected.
    let bad_source_identity = WorkspaceChatIdentity {
        source_id: String::new(),
        binding_id: BINDING_A.to_string(),
        thread_uuid: THREAD_A.to_string(),
        canonical_project_scope: repo_a.to_string_lossy().into_owned(),
    };
    let err = project_coworker_job_record(&bad_source_identity, &job_a, None)
        .expect_err("empty source_id must fail");
    assert_eq!(err.kind, WorkspaceHistoryDiagnosticKind::ScopeRejected);

    // 2. Wrong binding_id is rejected.
    let wrong_binding =
        WorkspaceChatIdentity::new(SOURCE_A, BINDING_B, THREAD_A, &repo_a.to_string_lossy())
            .expect("wrong_binding chat identity");
    let err = project_coworker_job_record(&wrong_binding, &job_a, None)
        .expect_err("wrong binding must fail");
    assert_eq!(err.kind, WorkspaceHistoryDiagnosticKind::ScopeRejected);

    // 3. Wrong thread_uuid is rejected.
    let wrong_thread =
        WorkspaceChatIdentity::new(SOURCE_A, BINDING_A, THREAD_B, &repo_a.to_string_lossy())
            .expect("wrong_thread chat identity");
    let err = project_coworker_job_record(&wrong_thread, &job_a, None)
        .expect_err("wrong thread must fail");
    assert_eq!(err.kind, WorkspaceHistoryDiagnosticKind::ScopeRejected);

    // 4. Wrong canonical_project_scope is rejected despite summary mentioning repo_b.
    let wrong_repo =
        WorkspaceChatIdentity::new(SOURCE_A, BINDING_A, THREAD_A, &repo_b.to_string_lossy())
            .expect("wrong_repo chat identity");
    let err = project_coworker_job_record(&wrong_repo, &job_a, None)
        .expect_err("wrong repository must fail");
    assert_eq!(err.kind, WorkspaceHistoryDiagnosticKind::ScopeRejected);

    // 5. Symlink canonical_project_scope is rejected on disk read.
    let symlink_identity = WorkspaceChatIdentity::new(
        SOURCE_A,
        BINDING_A,
        THREAD_A,
        &symlink_repo.to_string_lossy(),
    )
    .expect("syntactically valid path");
    let stores = WorkspaceHistoryStores::new(&plans_dir);
    let err = read_scoped_workspace_job_histories(&symlink_identity, &stores)
        .expect_err("symlink repository scope must be rejected");
    assert_eq!(err.kind, WorkspaceHistoryDiagnosticKind::ScopeRejected);

    // 6. Store-backed read with wrong thread or missing approval omits out-of-scope records safely.
    let workflow_store = CoworkerWorkflowStore::new(&plans_dir).expect("workflow store");
    let ledger = CoworkerBindingLedger {
        schema_version: 1,
        binding_id: BINDING_A.to_string(),
        thread_uuid: Some(THREAD_A.to_string()),
        ledger_revision: 1,
        planning_grants: Vec::new(),
        architect_advice: Vec::new(),
        jobs: vec![job_a],
        admission_markers: BTreeMap::new(),
    };
    ledger
        .validate(BINDING_A)
        .expect("scope test workflow ledger must pass full CoworkerBindingLedger validation");
    write_private_json(
        &workflow_store
            .ledger_path_for_binding(BINDING_A)
            .expect("ledger path"),
        &ledger,
    );

    let wrong_thread_read = read_scoped_workspace_job_histories(&wrong_thread, &stores)
        .expect("wrong thread store read returns empty scoped set");
    assert!(!wrong_thread_read.degraded);
    assert!(wrong_thread_read.jobs.is_empty());
    assert_eq!(wrong_thread_read.rejected_scope_count, 1);
    assert!(wrong_thread_read
        .diagnostics
        .iter()
        .any(|d| d.kind == WorkspaceHistoryDiagnosticKind::ScopeRejected));

    // Empty authority_dir has no matching ApprovalRecord -> job with approval_linkage is omitted.
    let valid_chat =
        WorkspaceChatIdentity::new(SOURCE_A, BINDING_A, THREAD_A, &repo_a.to_string_lossy())
            .expect("valid chat");
    let stores_with_auth = WorkspaceHistoryStores::new(&plans_dir).with_authority_dir(&auth_dir);
    let unverified_auth_read = read_scoped_workspace_job_histories(&valid_chat, &stores_with_auth)
        .expect("read with empty authority store");
    assert!(unverified_auth_read.jobs.is_empty());
    assert_eq!(unverified_auth_read.rejected_scope_count, 1);
    assert!(unverified_auth_read
        .diagnostics
        .iter()
        .any(|d| d.kind == WorkspaceHistoryDiagnosticKind::ApprovalVerificationFailed));
}

#[test]
fn missing_store_and_missing_runtime_degrade_honestly_without_creating_files_or_fabricating_acceptance(
) {
    let tmp = TempDir::new().expect("tempdir");
    let missing_plans_dir = tmp.path().join("nonexistent-plans");
    let repo_dir = make_private_dir(&tmp.path().join("repo-missing-rt"));

    let chat =
        WorkspaceChatIdentity::new(SOURCE_A, BINDING_A, THREAD_A, &repo_dir.to_string_lossy())
            .expect("valid chat");

    // 1. Missing store directory does not create directories and degrades honestly.
    let missing_read = read_scoped_workspace_job_histories(
        &chat,
        &WorkspaceHistoryStores::new(&missing_plans_dir),
    )
    .expect("missing store degrades gracefully");
    assert!(!missing_plans_dir.exists());
    assert!(missing_read.degraded);
    assert!(missing_read.jobs.is_empty());
    assert!(missing_read
        .diagnostics
        .iter()
        .any(|d| d.kind == WorkspaceHistoryDiagnosticKind::MissingStore));

    // 2. Existing workflow ledger with missing ControllerJobRuntimeState degrades Running/Completed honestly.
    let plans_dir = make_private_dir(&tmp.path().join("plans-present"));
    let policy = ExecutionPolicy::deny_all("policy-test").expect("policy");
    let policy_digest = policy.digest().expect("policy digest");
    let project_base = sample_project_base(&repo_dir);
    let (brief, brief_digest) = sample_work_brief(
        "brf_missing_rt",
        BINDING_A,
        THREAD_A,
        project_base.clone(),
        WorkBriefKind::NewJob,
        "Job missing controller runtime",
    );

    let task = CoworkerTaskRecord {
        task_id: "task-rt-missing".to_string(),
        task_digest: HEX64_D.to_string(),
        role: WorkflowRole::Coder,
        difficulty: Difficulty::Medium,
        selected_choice: sample_coder_choice(),
        depends_on: Vec::new(),
        write_paths: vec!["src/lib.rs".to_string()],
        owned_interfaces: Vec::new(),
        consumed_interfaces: Vec::new(),
        stage: CoworkerTaskStage::Running,
        stage_before_hold: None,
        current_attempt_no: 1,
        active_candidate_id: None,
        accepted_candidate_id: None,
        dependency_input_commit: None,
        dependency_input_tree_sha: None,
        accepted_predecessor_receipts: Vec::new(),
        attempts: vec![TaskAttemptRef {
            attempt_no: 1,
            role: WorkflowRole::Coder,
            request_key: "req_rt_missing_a1".to_string(),
            outbox_id: "outbox_rt_missing_a1".to_string(),
            run_id: Some("run_00000000-0000-4000-8000-000000000021".to_string()),
            worker_id: Some("wrk_00000000-0000-4000-8000-000000000021".to_string()),
            task_worktree_id: None,
            base_commit: SHA40_BASE.to_string(),
            head_commit: None,
            tree_sha: None,
            diff_digest: None,
            report_artifact_id: None,
            observed_model: None,
            observed_effort: None,
        }],
        candidates: Vec::new(),
        active_hold_ids: Vec::new(),
        unresolved_outbox_id: None,
    };

    let job = CoworkerJobRecord {
        job_id: "job_00000000000000000000000000000021".to_string(),
        job_revision: 2,
        create_request_key: "req_job_21".to_string(),
        binding_id: BINDING_A.to_string(),
        thread_uuid: THREAD_A.to_string(),
        phase: CoworkerJobPhase::Executing,
        work_brief: brief,
        work_brief_digest: brief_digest,
        supersedes_job_id: None,
        planning_result: None,
        approval_linkage: Some(sample_approval_linkage(
            project_base,
            &policy_digest,
            vec!["task-rt-missing".to_string()],
        )),
        tasks: vec![task],
        holds: Vec::new(),
        outbox: vec![OutboxDispatchRecord {
            outbox_id: "outbox_rt_missing_a1".to_string(),
            request_key: "req_rt_missing_a1".to_string(),
            kind: OutboxDispatchKind::TaskAttemptDispatch,
            task_id: "task-rt-missing".to_string(),
            attempt_no: 1,
            role: WorkflowRole::Coder,
            subject_digest: HEX64_D.to_string(),
            status: OutboxDispatchStatus::DispatchedEffectConfirmed,
            reserved_at_job_revision: 1,
            updated_at_job_revision: 2,
            run_id: Some("run_00000000-0000-4000-8000-000000000021".to_string()),
            worker_id: Some("wrk_00000000-0000-4000-8000-000000000021".to_string()),
            receipt_id: None,
            diagnostic: None,
        }],
        combined_candidates: Vec::new(),
    };

    let workflow_store = CoworkerWorkflowStore::new(&plans_dir).expect("workflow store");
    let ledger = CoworkerBindingLedger {
        schema_version: 1,
        binding_id: BINDING_A.to_string(),
        thread_uuid: Some(THREAD_A.to_string()),
        ledger_revision: 1,
        planning_grants: Vec::new(),
        architect_advice: Vec::new(),
        jobs: vec![job],
        admission_markers: BTreeMap::new(),
    };
    write_private_json(
        &workflow_store
            .ledger_path_for_binding(BINDING_A)
            .expect("ledger path"),
        &ledger,
    );

    let read = read_scoped_workspace_job_histories(&chat, &WorkspaceHistoryStores::new(&plans_dir))
        .expect("missing runtime degrades without error");
    assert!(read.degraded);
    assert!(read
        .diagnostics
        .iter()
        .any(|d| d.kind == WorkspaceHistoryDiagnosticKind::MissingRuntime));
    assert_eq!(read.jobs.len(), 1);
    let projected = &read.jobs[0];
    assert_eq!(projected.job.state, ObservationJobState::Unknown);
    assert_eq!(projected.job.execution_status, "unknown");
    assert_eq!(projected.tasks[0].status, "unknown");
    assert_eq!(projected.tasks[0].attempts[0].status, "unknown");
    assert_eq!(
        projected.acceptance_proof,
        WorkspaceAcceptanceProof::unknown()
    );
}

#[test]
fn preserves_historical_failure_across_retry_while_separating_current_attention() {
    let tmp = TempDir::new().expect("tempdir");
    let repo_dir = make_private_dir(&tmp.path().join("repo-retry"));
    let chat =
        WorkspaceChatIdentity::new(SOURCE_A, BINDING_A, THREAD_A, &repo_dir.to_string_lossy())
            .expect("chat identity");

    let policy = ExecutionPolicy::deny_all("policy-test").expect("policy");
    let policy_digest = policy.digest().expect("policy digest");
    let project_base = sample_project_base(&repo_dir);
    let (brief, brief_digest) = sample_work_brief(
        "brf_retry_01",
        BINDING_A,
        THREAD_A,
        project_base.clone(),
        WorkBriefKind::NewJob,
        "Retry and attention separation job",
    );

    // Task 1: Attempt 1 failed review (CorrectInScope), Attempt 2 succeeded to Integrated.
    let verdict_a1 = StructuredReviewVerdict {
        verdict_id: "vrd_0123456789abcdef0123456789abcdef".to_string(),
        task_id: "task-retried".to_string(),
        attempt_no: 1,
        candidate_id: "cand_a1".to_string(),
        review_subject_digest: HEX64_C.to_string(),
        base_commit: SHA40_BASE.to_string(),
        task_head_commit: SHA40_HEAD_1.to_string(),
        task_tree_sha: SHA40_TREE_1.to_string(),
        diff_digest: HEX64_D.to_string(),
        policy_digest: policy_digest.clone(),
        outcome: ReviewVerdictOutcome::CorrectInScope,
        findings: vec!["Missing scope check on foreign binding".to_string()],
        required_in_scope_fixes: vec!["Reject mismatched binding_id before projecting".to_string()],
        conflicting_paths_or_interfaces: Vec::new(),
        prose_summary: Some("Attempt 1 requires bounded scope fix".to_string()),
        prose_may_advance_acceptance: false,
    };

    let task_retried = CoworkerTaskRecord {
        task_id: "task-retried".to_string(),
        task_digest: HEX64_D.to_string(),
        role: WorkflowRole::Coder,
        difficulty: Difficulty::Hard,
        selected_choice: sample_coder_choice(),
        depends_on: Vec::new(),
        write_paths: vec!["src/a.rs".to_string()],
        owned_interfaces: Vec::new(),
        consumed_interfaces: Vec::new(),
        stage: CoworkerTaskStage::Integrated,
        stage_before_hold: None,
        current_attempt_no: 2,
        active_candidate_id: None,
        accepted_candidate_id: Some("cand_a2".to_string()),
        dependency_input_commit: None,
        dependency_input_tree_sha: None,
        accepted_predecessor_receipts: Vec::new(),
        attempts: vec![
            TaskAttemptRef {
                attempt_no: 1,
                role: WorkflowRole::Coder,
                request_key: "req_retry_a1".to_string(),
                outbox_id: "outbox_retry_a1".to_string(),
                run_id: Some("run_00000000-0000-4000-8000-000000000031".to_string()),
                worker_id: Some("wrk_00000000-0000-4000-8000-000000000031".to_string()),
                task_worktree_id: None,
                base_commit: SHA40_BASE.to_string(),
                head_commit: Some(SHA40_HEAD_1.to_string()),
                tree_sha: Some(SHA40_TREE_1.to_string()),
                diff_digest: Some(HEX64_D.to_string()),
                report_artifact_id: None,
                observed_model: None,
                observed_effort: None,
            },
            TaskAttemptRef {
                attempt_no: 2,
                role: WorkflowRole::Coder,
                request_key: "req_retry_a2".to_string(),
                outbox_id: "outbox_retry_a2".to_string(),
                run_id: Some("run_00000000-0000-4000-8000-000000000032".to_string()),
                worker_id: Some("wrk_00000000-0000-4000-8000-000000000032".to_string()),
                task_worktree_id: None,
                base_commit: SHA40_BASE.to_string(),
                head_commit: Some(SHA40_HEAD_2.to_string()),
                tree_sha: Some(SHA40_TREE_2.to_string()),
                diff_digest: Some(HEX64_C.to_string()),
                report_artifact_id: None,
                observed_model: None,
                observed_effort: None,
            },
        ],
        candidates: vec![
            TaskCandidateRef {
                candidate_id: "cand_a1".to_string(),
                task_id: "task-retried".to_string(),
                attempt_no: 1,
                base_commit: SHA40_BASE.to_string(),
                head_commit: SHA40_HEAD_1.to_string(),
                tree_sha: SHA40_TREE_1.to_string(),
                diff_digest: HEX64_D.to_string(),
                write_paths: vec!["src/a.rs".to_string()],
                check_evidence_digests: Vec::new(),
                review_subject_digest: Some(HEX64_C.to_string()),
                review_verdict: Some(verdict_a1),
                review_receipt_id: None,
                integration_receipt_id: None,
            },
            TaskCandidateRef {
                candidate_id: "cand_a2".to_string(),
                task_id: "task-retried".to_string(),
                attempt_no: 2,
                base_commit: SHA40_BASE.to_string(),
                head_commit: SHA40_HEAD_2.to_string(),
                tree_sha: SHA40_TREE_2.to_string(),
                diff_digest: HEX64_C.to_string(),
                write_paths: vec!["src/a.rs".to_string()],
                check_evidence_digests: Vec::new(),
                review_subject_digest: Some(HEX64_C.to_string()),
                review_verdict: None,
                review_receipt_id: Some("rev_plausible_a2".to_string()),
                integration_receipt_id: Some("int_plausible_a2".to_string()),
            },
        ],
        active_hold_ids: Vec::new(),
        unresolved_outbox_id: None,
    };

    // Task 2: Unstarted task (PendingDependencies -> "not_started").
    let task_unstarted = CoworkerTaskRecord {
        task_id: "task-unstarted".to_string(),
        task_digest: HEX64_D.to_string(),
        role: WorkflowRole::Coder,
        difficulty: Difficulty::Easy,
        selected_choice: sample_coder_choice(),
        depends_on: vec!["task-retried".to_string()],
        write_paths: vec!["src/b.rs".to_string()],
        owned_interfaces: Vec::new(),
        consumed_interfaces: Vec::new(),
        stage: CoworkerTaskStage::PendingDependencies,
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
    };

    // Task 3: Cancelled task -> explicit "cancelled".
    let task_cancelled = CoworkerTaskRecord {
        task_id: "task-cancelled".to_string(),
        task_digest: HEX64_D.to_string(),
        role: WorkflowRole::Coder,
        difficulty: Difficulty::Easy,
        selected_choice: sample_coder_choice(),
        depends_on: Vec::new(),
        write_paths: vec!["src/c.rs".to_string()],
        owned_interfaces: Vec::new(),
        consumed_interfaces: Vec::new(),
        stage: CoworkerTaskStage::Cancelled,
        stage_before_hold: None,
        current_attempt_no: 1,
        active_candidate_id: None,
        accepted_candidate_id: None,
        dependency_input_commit: None,
        dependency_input_tree_sha: None,
        accepted_predecessor_receipts: Vec::new(),
        attempts: Vec::new(),
        candidates: Vec::new(),
        active_hold_ids: Vec::new(),
        unresolved_outbox_id: None,
    };

    // Task 4: Held waiting on user clarification -> NeedsUserInput attention.
    let task_held = CoworkerTaskRecord {
        task_id: "task-held".to_string(),
        task_digest: HEX64_D.to_string(),
        role: WorkflowRole::Coder,
        difficulty: Difficulty::Medium,
        selected_choice: sample_coder_choice(),
        depends_on: Vec::new(),
        write_paths: vec!["src/d.rs".to_string()],
        owned_interfaces: Vec::new(),
        consumed_interfaces: Vec::new(),
        stage: CoworkerTaskStage::Held,
        stage_before_hold: Some(CoworkerTaskStage::ReadyToDispatch),
        current_attempt_no: 0,
        active_candidate_id: None,
        accepted_candidate_id: None,
        dependency_input_commit: None,
        dependency_input_tree_sha: None,
        accepted_predecessor_receipts: Vec::new(),
        attempts: Vec::new(),
        candidates: Vec::new(),
        active_hold_ids: vec!["hold_clarify_01".to_string()],
        unresolved_outbox_id: None,
    };

    let job = CoworkerJobRecord {
        job_id: "job_00000000000000000000000000000030".to_string(),
        job_revision: 5,
        create_request_key: "req_job_30".to_string(),
        binding_id: BINDING_A.to_string(),
        thread_uuid: THREAD_A.to_string(),
        phase: CoworkerJobPhase::Executing,
        work_brief: brief,
        work_brief_digest: brief_digest,
        supersedes_job_id: None,
        planning_result: None,
        approval_linkage: Some(sample_approval_linkage(
            project_base,
            &policy_digest,
            vec![
                "task-retried".to_string(),
                "task-unstarted".to_string(),
                "task-cancelled".to_string(),
                "task-held".to_string(),
            ],
        )),
        tasks: vec![task_retried, task_unstarted, task_cancelled, task_held],
        holds: vec![WorkflowHoldRecord {
            hold_id: "hold_clarify_01".to_string(),
            request_key: "req_hold_01".to_string(),
            reason: WorkflowHoldReason::ConversationClarificationPending,
            detail: "Clarify behavior on malformed legacy receipt".to_string(),
            directly_affected_task_ids: vec!["task-held".to_string()],
            affected_write_paths: vec!["src/d.rs".to_string()],
            affected_interfaces: Vec::new(),
            holds_entire_job: false,
            active: true,
            created_at_job_revision: 4,
            resolved_at_job_revision: None,
            resolution_note: None,
        }],
        outbox: vec![
            OutboxDispatchRecord {
                outbox_id: "outbox_retry_a1".to_string(),
                request_key: "req_retry_a1".to_string(),
                kind: OutboxDispatchKind::TaskAttemptDispatch,
                task_id: "task-retried".to_string(),
                attempt_no: 1,
                role: WorkflowRole::Coder,
                subject_digest: HEX64_D.to_string(),
                status: OutboxDispatchStatus::DispatchedEffectConfirmed,
                reserved_at_job_revision: 1,
                updated_at_job_revision: 2,
                run_id: Some("run_00000000-0000-4000-8000-000000000031".to_string()),
                worker_id: Some("wrk_00000000-0000-4000-8000-000000000031".to_string()),
                receipt_id: None,
                diagnostic: None,
            },
            OutboxDispatchRecord {
                outbox_id: "outbox_retry_a2".to_string(),
                request_key: "req_retry_a2".to_string(),
                kind: OutboxDispatchKind::TaskAttemptDispatch,
                task_id: "task-retried".to_string(),
                attempt_no: 2,
                role: WorkflowRole::Coder,
                subject_digest: HEX64_C.to_string(),
                status: OutboxDispatchStatus::DispatchedEffectConfirmed,
                reserved_at_job_revision: 3,
                updated_at_job_revision: 4,
                run_id: Some("run_00000000-0000-4000-8000-000000000032".to_string()),
                worker_id: Some("wrk_00000000-0000-4000-8000-000000000032".to_string()),
                receipt_id: None,
                diagnostic: None,
            },
        ],
        combined_candidates: Vec::new(),
    };

    let rt = sample_runtime_state(&job, policy, &policy_digest);
    let projected =
        project_coworker_job_record(&chat, &job, Some(&rt)).expect("project retry history");

    // Task 1: Attempt 1 failed, Attempt 2 completed; historical_failure preserved & superseded; NO live attention.
    let t1 = &projected.tasks[0];
    assert_eq!(t1.current_attempt_no, 2);
    assert_eq!(t1.attempts.len(), 2);
    assert_eq!(t1.attempts[0].status, "failed");
    assert_eq!(t1.attempts[1].status, "completed");
    let hist_fail = t1
        .historical_failure
        .as_ref()
        .expect("historical failure on attempt 1 must be preserved");
    assert_eq!(hist_fail.attempt_no, Some(1));
    assert_eq!(hist_fail.stage, "review");
    assert!(hist_fail.superseded_or_resolved);
    assert_eq!(
        t1.current_attention, None,
        "superseded past failure must never generate live attention or NeedsApproval"
    );

    // Task 2: Unstarted -> "not_started".
    assert_eq!(projected.tasks[1].status, "not_started");
    // Task 3: Cancelled -> "cancelled".
    assert_eq!(projected.tasks[2].status, "cancelled");
    // Task 4: Active clarification hold -> NeedsUserInput with blocking_hold_ids.
    let t4_attn = projected.tasks[3]
        .current_attention
        .as_ref()
        .expect("active clarification hold produces attention");
    assert_eq!(t4_attn.kind, WorkspaceAttentionKind::NeedsUserInput);
    assert_eq!(
        t4_attn.blocking_hold_ids,
        vec!["hold_clarify_01".to_string()]
    );

    // Job-level historical failure is preserved (superseded), while job current_attention comes from the active hold (not NeedsApproval).
    assert!(projected
        .historical_failure
        .as_ref()
        .is_some_and(|f| f.superseded_or_resolved));
    let job_attn = projected
        .current_attention
        .as_ref()
        .expect("job current attention from active hold");
    assert_eq!(job_attn.kind, WorkspaceAttentionKind::NeedsUserInput);
    assert_ne!(job_attn.kind, WorkspaceAttentionKind::NeedsApproval);
}

#[test]
fn plausible_verdict_and_receipt_ids_never_fabricate_verified_acceptance_in_phase1() {
    let tmp = TempDir::new().expect("tempdir");
    let repo_dir = make_private_dir(&tmp.path().join("repo-plausible"));
    let chat =
        WorkspaceChatIdentity::new(SOURCE_A, BINDING_A, THREAD_A, &repo_dir.to_string_lossy())
            .expect("chat identity");

    let policy = ExecutionPolicy::deny_all("policy-test").expect("policy");
    let policy_digest = policy.digest().expect("policy digest");
    let project_base = sample_project_base(&repo_dir);
    let (brief, brief_digest) = sample_work_brief(
        "brf_plausible_01",
        BINDING_A,
        THREAD_A,
        project_base.clone(),
        WorkBriefKind::NewJob,
        "Plausible IDs without receipt verification",
    );

    let accept_verdict = StructuredReviewVerdict {
        verdict_id: "vrd_11111111111111111111111111111111".to_string(),
        task_id: "task-plausible".to_string(),
        attempt_no: 1,
        candidate_id: "cand_11111111111111111111111111111111".to_string(),
        review_subject_digest: HEX64_C.to_string(),
        base_commit: SHA40_BASE.to_string(),
        task_head_commit: SHA40_HEAD_1.to_string(),
        task_tree_sha: SHA40_TREE_1.to_string(),
        diff_digest: HEX64_D.to_string(),
        policy_digest: policy_digest.clone(),
        outcome: ReviewVerdictOutcome::Accept,
        findings: Vec::new(),
        required_in_scope_fixes: Vec::new(),
        conflicting_paths_or_interfaces: Vec::new(),
        prose_summary: Some("Looks good in prose".to_string()),
        prose_may_advance_acceptance: false,
    };

    let task = CoworkerTaskRecord {
        task_id: "task-plausible".to_string(),
        task_digest: HEX64_D.to_string(),
        role: WorkflowRole::Coder,
        difficulty: Difficulty::Hard,
        selected_choice: sample_coder_choice(),
        depends_on: Vec::new(),
        write_paths: vec!["src/lib.rs".to_string()],
        owned_interfaces: Vec::new(),
        consumed_interfaces: Vec::new(),
        stage: CoworkerTaskStage::Integrated,
        stage_before_hold: None,
        current_attempt_no: 1,
        active_candidate_id: None,
        accepted_candidate_id: Some("cand_11111111111111111111111111111111".to_string()),
        dependency_input_commit: None,
        dependency_input_tree_sha: None,
        accepted_predecessor_receipts: Vec::new(),
        attempts: vec![TaskAttemptRef {
            attempt_no: 1,
            role: WorkflowRole::Coder,
            request_key: "req_plausible_a1".to_string(),
            outbox_id: "outbox_plausible_a1".to_string(),
            run_id: Some("run_00000000-0000-4000-8000-000000000041".to_string()),
            worker_id: Some("wrk_00000000-0000-4000-8000-000000000041".to_string()),
            task_worktree_id: None,
            base_commit: SHA40_BASE.to_string(),
            head_commit: Some(SHA40_HEAD_1.to_string()),
            tree_sha: Some(SHA40_TREE_1.to_string()),
            diff_digest: Some(HEX64_D.to_string()),
            report_artifact_id: None,
            observed_model: None,
            observed_effort: None,
        }],
        candidates: vec![TaskCandidateRef {
            candidate_id: "cand_11111111111111111111111111111111".to_string(),
            task_id: "task-plausible".to_string(),
            attempt_no: 1,
            base_commit: SHA40_BASE.to_string(),
            head_commit: SHA40_HEAD_1.to_string(),
            tree_sha: SHA40_TREE_1.to_string(),
            diff_digest: HEX64_D.to_string(),
            write_paths: vec!["src/lib.rs".to_string()],
            check_evidence_digests: vec![HEX64_D.to_string()],
            review_subject_digest: Some(HEX64_C.to_string()),
            review_verdict: Some(accept_verdict),
            review_receipt_id: Some("rev_11111111111111111111111111111111".to_string()),
            integration_receipt_id: Some("int_11111111111111111111111111111111".to_string()),
        }],
        active_hold_ids: Vec::new(),
        unresolved_outbox_id: None,
    };

    let mut combined = CombinedCandidateIdentity {
        combined_candidate_id: "cmb_11111111111111111111111111111111".to_string(),
        request_key: "req_cmb_plausible".to_string(),
        job_id: "job_00000000000000000000000000000040".to_string(),
        job_revision: 3,
        project_base: project_base.clone(),
        final_commit_sha: Some(SHA40_HEAD_1.to_string()),
        final_tree_sha: SHA40_TREE_1.to_string(),
        plan_id: "plan_hist_01".to_string(),
        plan_revision: 1,
        plan_digest: HEX64_B.to_string(),
        approval_id: "apr_0123456789abcdef0123456789abcdef".to_string(),
        approval_subject_digest: HEX64_A.to_string(),
        scope_policy_revision: 1,
        policy_id: "policy-test".to_string(),
        policy_digest: policy_digest.clone(),
        accepted_predecessor_receipts: vec![AcceptedPredecessorReceiptRef {
            task_id: "task-plausible".to_string(),
            candidate_id: "cand_11111111111111111111111111111111".to_string(),
            head_commit: SHA40_HEAD_1.to_string(),
            tree_sha: SHA40_TREE_1.to_string(),
            review_subject_digest: HEX64_C.to_string(),
            review_receipt_id: "rev_11111111111111111111111111111111".to_string(),
            integration_receipt_id: "int_11111111111111111111111111111111".to_string(),
        }],
        check_evidence: vec![CandidateCheckEvidence {
            check_id: "cargo_test".to_string(),
            command_summary: "cargo test".to_string(),
            passed: true,
            exit_code: 0,
            evidence_digest: HEX64_D.to_string(),
        }],
        candidate_digest: String::new(),
        closure_reservation_id: Some("cls_11111111111111111111111111111111".to_string()),
        status: CombinedCandidateStatus::ClosureAccepted,
    };
    combined.candidate_digest = combined
        .compute_candidate_digest()
        .expect("compute combined candidate digest");

    let job = CoworkerJobRecord {
        job_id: "job_00000000000000000000000000000040".to_string(),
        job_revision: 4,
        create_request_key: "req_job_40".to_string(),
        binding_id: BINDING_A.to_string(),
        thread_uuid: THREAD_A.to_string(),
        phase: CoworkerJobPhase::Completed,
        work_brief: brief,
        work_brief_digest: brief_digest,
        supersedes_job_id: None,
        planning_result: None,
        approval_linkage: Some(sample_approval_linkage(
            project_base,
            &policy_digest,
            vec!["task-plausible".to_string()],
        )),
        tasks: vec![task],
        holds: Vec::new(),
        outbox: vec![OutboxDispatchRecord {
            outbox_id: "outbox_plausible_a1".to_string(),
            request_key: "req_plausible_a1".to_string(),
            kind: OutboxDispatchKind::TaskAttemptDispatch,
            task_id: "task-plausible".to_string(),
            attempt_no: 1,
            role: WorkflowRole::Coder,
            subject_digest: HEX64_D.to_string(),
            status: OutboxDispatchStatus::DispatchedEffectConfirmed,
            reserved_at_job_revision: 1,
            updated_at_job_revision: 2,
            run_id: Some("run_00000000-0000-4000-8000-000000000041".to_string()),
            worker_id: Some("wrk_00000000-0000-4000-8000-000000000041".to_string()),
            receipt_id: Some("rev_11111111111111111111111111111111".to_string()),
            diagnostic: None,
        }],
        combined_candidates: vec![combined],
    };

    let mut rt = sample_runtime_state(&job, policy, &policy_digest);
    rt.closure_attempt_id = Some("cat_11111111111111111111111111111111".to_string());
    rt.closure_verdict_id = Some("cva_11111111111111111111111111111111".to_string());

    let projected =
        project_coworker_job_record(&chat, &job, Some(&rt)).expect("project plausible job");

    // Phase 1 hard requirement: plausible IDs NEVER fabricate VerifiedAccepted or green Completed.
    assert_eq!(
        projected.acceptance_proof,
        WorkspaceAcceptanceProof::unknown()
    );
    assert_eq!(
        projected.acceptance_proof.status,
        WorkspaceAcceptanceStatus::Unknown
    );
    assert_eq!(projected.job.state, ObservationJobState::Unknown);
    assert_ne!(projected.job.execution_status, "accepted");
    assert_eq!(
        projected.tasks[0].acceptance_proof,
        WorkspaceAcceptanceProof::unknown()
    );
    assert_eq!(projected.tasks[0].accepted_candidate_id, None);
    assert_ne!(projected.tasks[0].status, "accepted");
    assert_eq!(projected.tasks[0].candidates[0].review_status, "unknown");
    assert_eq!(projected.combined_candidates[0].status, "unknown");
}

#[test]
fn enforces_core_normalization_bounds_and_records_omission_counts() {
    let tmp = TempDir::new().expect("tempdir");
    let repo_dir = make_private_dir(&tmp.path().join("repo-bounds"));
    let chat =
        WorkspaceChatIdentity::new(SOURCE_A, BINDING_A, THREAD_A, &repo_dir.to_string_lossy())
            .expect("chat identity");

    let policy = ExecutionPolicy::deny_all("policy-test").expect("policy");
    let policy_digest = policy.digest().expect("policy digest");
    let project_base = sample_project_base(&repo_dir);
    let (brief, brief_digest) = sample_work_brief(
        "brf_bounds_01",
        BINDING_A,
        THREAD_A,
        project_base.clone(),
        WorkBriefKind::NewJob,
        "Over-capacity bounds verification job",
    );

    let mut approved_task_ids = Vec::new();
    let mut tasks = Vec::new();
    let mut outbox = Vec::new();

    // 70 tasks (> 64 cap -> 6 omitted); task 0 has 20 attempts (> 16 cap -> 4 omitted) and 19 candidates (> 16 cap -> 3 omitted).
    for idx in 0..70usize {
        let tid = format!("task_{idx:03}");
        approved_task_ids.push(tid.clone());

        let mut attempts = Vec::new();
        let mut candidates = Vec::new();
        if idx == 0 {
            for att_no in 1..=20u32 {
                let ob_id = format!("outbox_t0_{att_no:03}");
                let rid = format!("run_00000000-0000-4000-8000-{att_no:012}");
                attempts.push(TaskAttemptRef {
                    attempt_no: att_no,
                    role: WorkflowRole::Coder,
                    request_key: format!("req_t0_{att_no:03}"),
                    outbox_id: ob_id.clone(),
                    run_id: Some(rid.clone()),
                    worker_id: Some(format!("wrk_00000000-0000-4000-8000-{att_no:012}")),
                    task_worktree_id: None,
                    base_commit: SHA40_BASE.to_string(),
                    head_commit: Some(SHA40_HEAD_1.to_string()),
                    tree_sha: Some(SHA40_TREE_1.to_string()),
                    diff_digest: Some(HEX64_D.to_string()),
                    report_artifact_id: None,
                    observed_model: None,
                    observed_effort: None,
                });
                outbox.push(OutboxDispatchRecord {
                    outbox_id: ob_id,
                    request_key: format!("req_t0_{att_no:03}"),
                    kind: OutboxDispatchKind::TaskAttemptDispatch,
                    task_id: tid.clone(),
                    attempt_no: att_no,
                    role: WorkflowRole::Coder,
                    subject_digest: HEX64_D.to_string(),
                    status: OutboxDispatchStatus::DispatchedEffectConfirmed,
                    reserved_at_job_revision: 1,
                    updated_at_job_revision: 2,
                    run_id: Some(rid),
                    worker_id: None,
                    receipt_id: None,
                    diagnostic: None,
                });
            }
            for cand_no in 1..=19u32 {
                candidates.push(TaskCandidateRef {
                    candidate_id: format!("cand_t0_{cand_no:03}"),
                    task_id: tid.clone(),
                    attempt_no: cand_no,
                    base_commit: SHA40_BASE.to_string(),
                    head_commit: SHA40_HEAD_1.to_string(),
                    tree_sha: SHA40_TREE_1.to_string(),
                    diff_digest: HEX64_D.to_string(),
                    write_paths: vec!["src/lib.rs".to_string()],
                    check_evidence_digests: Vec::new(),
                    review_subject_digest: None,
                    review_verdict: None,
                    review_receipt_id: None,
                    integration_receipt_id: None,
                });
            }
        }

        tasks.push(CoworkerTaskRecord {
            task_id: tid,
            task_digest: HEX64_D.to_string(),
            role: WorkflowRole::Coder,
            difficulty: Difficulty::Easy,
            selected_choice: sample_coder_choice(),
            depends_on: Vec::new(),
            write_paths: vec!["src/lib.rs".to_string()],
            owned_interfaces: Vec::new(),
            consumed_interfaces: Vec::new(),
            stage: CoworkerTaskStage::ReadyToDispatch,
            stage_before_hold: None,
            current_attempt_no: if idx == 0 { 20 } else { 0 },
            active_candidate_id: None,
            accepted_candidate_id: None,
            dependency_input_commit: None,
            dependency_input_tree_sha: None,
            accepted_predecessor_receipts: Vec::new(),
            attempts,
            candidates,
            active_hold_ids: Vec::new(),
            unresolved_outbox_id: None,
        });
    }

    // Add 245 review outbox records with distinct run_ids -> total outbox = 265 (> 256 cap -> 9 omitted),
    // total runs = 20 (attempt runs) + 245 (review runs) = 265 (> 256 cap -> 9 omitted).
    for extra in 1..=245u32 {
        outbox.push(OutboxDispatchRecord {
            outbox_id: format!("outbox_rev_{extra:03}"),
            request_key: format!("req_rev_{extra:03}"),
            kind: OutboxDispatchKind::TaskReviewDispatch,
            task_id: "task_001".to_string(),
            attempt_no: 1,
            role: WorkflowRole::Reviewer,
            subject_digest: HEX64_D.to_string(),
            status: OutboxDispatchStatus::DispatchedEffectConfirmed,
            reserved_at_job_revision: 1,
            updated_at_job_revision: 2,
            run_id: Some(format!("run_11111111-0000-4000-8000-{extra:012}")),
            worker_id: None,
            receipt_id: None,
            diagnostic: None,
        });
    }

    // 35 combined candidates (> 32 cap -> 3 omitted).
    let mut combined_candidates = Vec::new();
    for cmb_idx in 1..=35u32 {
        combined_candidates.push(CombinedCandidateIdentity {
            combined_candidate_id: format!("cmb_{cmb_idx:03}"),
            request_key: format!("req_cmb_{cmb_idx:03}"),
            job_id: "job_00000000000000000000000000000050".to_string(),
            job_revision: u64::from(cmb_idx),
            project_base: project_base.clone(),
            final_commit_sha: Some(SHA40_HEAD_1.to_string()),
            final_tree_sha: SHA40_TREE_1.to_string(),
            plan_id: "plan_hist_01".to_string(),
            plan_revision: 1,
            plan_digest: HEX64_B.to_string(),
            approval_id: "apr_0123456789abcdef0123456789abcdef".to_string(),
            approval_subject_digest: HEX64_A.to_string(),
            scope_policy_revision: 1,
            policy_id: "policy-test".to_string(),
            policy_digest: policy_digest.clone(),
            accepted_predecessor_receipts: Vec::new(),
            check_evidence: Vec::new(),
            candidate_digest: HEX64_C.to_string(),
            closure_reservation_id: None,
            status: CombinedCandidateStatus::ChecksVerified,
        });
    }

    let job = CoworkerJobRecord {
        job_id: "job_00000000000000000000000000000050".to_string(),
        job_revision: 40,
        create_request_key: "req_job_50".to_string(),
        binding_id: BINDING_A.to_string(),
        thread_uuid: THREAD_A.to_string(),
        phase: CoworkerJobPhase::Executing,
        work_brief: brief,
        work_brief_digest: brief_digest,
        supersedes_job_id: None,
        planning_result: None,
        approval_linkage: Some(sample_approval_linkage(
            project_base,
            &policy_digest,
            approved_task_ids,
        )),
        tasks,
        holds: Vec::new(),
        outbox,
        combined_candidates,
    };

    let ctx = WorkspaceJobLineageContext {
        runtime_missing_or_degraded: true,
        ..WorkspaceJobLineageContext::default()
    };
    let projected =
        project_coworker_job_history(&chat, &job, None, Some(&ctx)).expect("bounded projection");

    assert_eq!(projected.tasks.len(), 64);
    assert_eq!(projected.truncation.tasks_omitted, 6);
    assert_eq!(projected.tasks[0].attempts.len(), 16);
    assert_eq!(projected.truncation.attempts_omitted, 4);
    assert_eq!(projected.tasks[0].candidates.len(), 16);
    assert_eq!(projected.truncation.candidates_omitted, 3);
    assert_eq!(projected.outbox.len(), 256);
    assert_eq!(projected.truncation.outbox_omitted, 9);
    assert_eq!(projected.runs.len(), 256);
    assert_eq!(projected.truncation.runs_omitted, 9); // 20 task runs + 245 review runs = 265 -> 9 omitted
    assert_eq!(projected.combined_candidates.len(), 32);
    assert_eq!(projected.truncation.combined_candidates_omitted, 3);
}

// ============================================================================
// Phase 2: Store-backed historical acceptance verification & negative fixtures
// ============================================================================

struct AcceptedHistoryTaskArtifacts {
    task_id: String,
    candidate_id: String,
    verdict_id: String,
    task_manifest: WorktreeManifest,
    review_receipt: ReviewReceipt,
    integration_receipt: IntegrationReceipt,
}

struct AcceptedHistoryStoreFixture {
    _tmp: TempDir,
    repo_dir: PathBuf,
    plans_dir: PathBuf,
    authority_dir: PathBuf,
    sessions_dir: PathBuf,
    work_area_root: PathBuf,
    base_commit: String,
    base_tree_sha: String,
    approval_id: String,
    plan_id: String,
    run_id: String,
    integration_manifest: WorktreeManifest,
    fake_agy_exe: PathBuf,
    fake_cargo_exe: PathBuf,
    registry: Arc<RegistryStore>,
    plans: Arc<PlanStore>,
    authority: Arc<AuthorityStore>,
    workflow_store: CoworkerWorkflowStore,
    policy_store: ControllerPolicyStore,
    job_id: String,
    combined_candidate_id: String,
    closure_reservation_id: String,
    closure_verdict_id: String,
    closure_recorded_at_ms: i64,
    final_subject: FinalCandidateSubject,
    pred_alpha: AcceptedHistoryTaskArtifacts,
    pred_beta: AcceptedHistoryTaskArtifacts,
}

fn init_phase2_git_repo(repo_dir: &Path) -> (String, String) {
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
    let mut file = File::create(path).expect("create script");
    file.write_all(body.as_bytes()).expect("write script");
    file.sync_all().expect("sync script");
    fs::set_permissions(path, fs::Permissions::from_mode(0o755)).expect("chmod 0755 script");
}

fn configure_fake_agy_closure_response(
    fake_agy_exe: &Path,
    subject: &FinalCandidateSubject,
    outcome: ClosureVerdictKind,
    summary: &str,
) {
    let payload = StructuredClosureModelResponse {
        schema_version: STRUCTURED_CLOSURE_RESPONSE_SCHEMA_VERSION,
        subject_digest: subject.subject_digest.clone(),
        candidate_tree_sha: subject.candidate_tree_sha.clone(),
        outcome,
        summary: summary.to_string(),
        findings: Vec::new(),
    };
    let payload_str = serde_json::to_string(&payload).expect("serialize closure response");
    let init_line = serde_json::json!({
        "event": "init",
        "init": {
            "conversation_id": "ses_hist_closure_1",
            "model": "gemini-4-argon-high",
            "effort": "high"
        }
    })
    .to_string();
    let result_line = serde_json::json!({
        "event": "result",
        "result": {
            "status": "SUCCESS",
            "conversation_id": "ses_hist_closure_1",
            "model": "gemini-4-argon-high",
            "effort": "high",
            "response": payload_str
        }
    })
    .to_string();
    let script = format!(
        "#!/bin/sh\nprintf '%s\\n' '{}'\nprintf '%s\\n' '{}'\nexit 0\n",
        init_line.replace('\'', "'\\''"),
        result_line.replace('\'', "'\\''"),
    );
    write_executable_script(fake_agy_exe, &script);
}

fn phase2_argon_choice(difficulty: Difficulty) -> SelectedRoleChoice {
    SelectedRoleChoice {
        tool_id: "tool-agy".to_string(),
        adapter: ToolAdapter::Antigravity,
        route: headless_route(),
        model_id: "model-argon".to_string(),
        model_arg: "gemini-4-argon-high".to_string(),
        effort: Some("high".to_string()),
        matched_difficulty: Some(difficulty),
    }
}

fn phase2_execution_policy() -> ExecutionPolicy {
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

fn phase2_check_commands(fake_cargo_exe: &Path) -> BTreeMap<String, FrozenCheckCommandSpec> {
    let mut checks = BTreeMap::new();
    checks.insert(
        "cargo_fmt".to_string(),
        FrozenCheckCommandSpec {
            check_id: "cargo_fmt".to_string(),
            kind: FrozenCheckKind::Formatting,
            executable: fake_cargo_exe.to_path_buf(),
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
            executable: fake_cargo_exe.to_path_buf(),
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

#[allow(clippy::too_many_arguments)]
fn commit_review_and_integrate_phase2_task(
    repo_dir: &Path,
    work_area_root: &Path,
    base_commit: &str,
    run_id: &str,
    integration_manifest: &WorktreeManifest,
    workflow_store: &CoworkerWorkflowStore,
    job: CoworkerJobRecord,
    task_id: &str,
    token: &str,
    rel_file: &str,
    new_contents: &str,
    strategy: IntegrationStrategy,
    timestamp_ms: i64,
) -> (CoworkerJobRecord, AcceptedHistoryTaskArtifacts) {
    let manager =
        WorktreeManager::new(repo_dir, work_area_root, base_commit).expect("WorktreeManager::new");
    let mut allowed = BTreeSet::new();
    allowed.insert(rel_file.to_string());

    let task_manifest = manager
        .create_task_worktree_with_token(run_id, task_id, token, &allowed)
        .expect("create task worktree");
    fs::write(task_manifest.checkout_path.join(rel_file), new_contents)
        .expect("write task modification");

    commit_task_worktree_changes(&CommitTaskChangesRequest {
        work_area_root: work_area_root.to_path_buf(),
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
        passed_test_count: 4,
        failed_test_count: 0,
        evidence_summary: "cargo fmt and cargo test passed".to_string(),
    };
    let subject = inspect_review_subject(&InspectReviewSubjectRequest {
        work_area_root: work_area_root.to_path_buf(),
        task_worktree_id: task_manifest.worktree_id.clone(),
        policy,
        test_evidence,
        worker_status: Some(WorkerRunStatus::Succeeded),
    })
    .expect("inspect review subject");

    let tag = task_id.replace('-', "_");
    let review_receipt = accept_review_subject(&ReviewAcceptanceRequest {
        subject: subject.clone(),
        expected_subject_digest: subject.subject_digest.clone(),
        reviewer_id: "op_controller".to_string(),
        review_note: format!("Approved {task_id}"),
        approved_at_ms: timestamp_ms,
        receipt_id: Some(format!("rev_{tag}_{token}")),
    })
    .expect("accept review subject");

    let current_int_head = resolve_exact_commit_sha(
        &integration_manifest.checkout_path,
        "HEAD",
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .expect("resolve integration HEAD");

    let integration_receipt = execute_integration(&ExecuteIntegrationRequest {
        work_area_root: work_area_root.to_path_buf(),
        integration_worktree_id: integration_manifest.worktree_id.clone(),
        task_worktree_id: task_manifest.worktree_id.clone(),
        subject,
        review_receipt: review_receipt.clone(),
        strategy,
        expected_integration_head_commit: current_int_head,
        expected_task_head_commit: review_receipt.task_head_commit.clone(),
        expected_source_head_commit: base_commit.to_string(),
        operator_id: "op_controller".to_string(),
        integrated_at_ms: timestamp_ms + 10,
        receipt_id: Some(format!("int_{tag}_{token}")),
    })
    .expect("execute integration");

    let policy_digest = job
        .approval_linkage
        .as_ref()
        .expect("approval linkage")
        .policy_digest
        .clone();

    let (job, attempt_outbox) = workflow_store
        .reserve_task_outbox(&ReserveTaskOutboxRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_A.to_string(),
            job_id: job.job_id.clone(),
            expected_job_revision: job.job_revision,
            expected_ledger_revision: None,
            request_key: format!("req_att_{tag}"),
            kind: OutboxDispatchKind::TaskAttemptDispatch,
            task_id: task_id.to_string(),
            role: WorkflowRole::Coder,
            subject_digest: review_receipt.subject_digest.clone(),
            dependency_input_commit: None,
            dependency_input_tree_sha: None,
            accepted_predecessor_receipts: Vec::new(),
        })
        .expect("reserve coder outbox");

    let (job, _) = workflow_store
        .reconcile_or_finalize_outbox(&FinalizeTaskOutboxRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_A.to_string(),
            job_id: job.job_id.clone(),
            expected_job_revision: job.job_revision,
            expected_ledger_revision: None,
            outbox_id: attempt_outbox.outbox_id,
            status: OutboxDispatchStatus::DispatchedEffectConfirmed,
            next_task_stage: CoworkerTaskStage::ReadyForReview,
            run_id: Some(run_id.to_string()),
            worker_id: Some(format!("wrk_coder_{tag}")),
            task_worktree_id: Some(task_manifest.worktree_id.clone()),
            head_commit: Some(review_receipt.task_head_commit.clone()),
            tree_sha: Some(review_receipt.task_tree_sha.clone()),
            diff_digest: Some(review_receipt.diff_digest.clone()),
            report_artifact_id: Some(format!("art_coder_{tag}")),
            observed_model: Some("gemini-4-argon-high".to_string()),
            observed_effort: Some("high".to_string()),
            receipt_id: None,
            diagnostic: None,
        })
        .expect("finalize coder outbox");

    let (job, rev_outbox) = workflow_store
        .reserve_task_outbox(&ReserveTaskOutboxRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_A.to_string(),
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
        .expect("reserve reviewer outbox");

    let (job, _) = workflow_store
        .reconcile_or_finalize_outbox(&FinalizeTaskOutboxRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_A.to_string(),
            job_id: job.job_id.clone(),
            expected_job_revision: job.job_revision,
            expected_ledger_revision: None,
            outbox_id: rev_outbox.outbox_id,
            status: OutboxDispatchStatus::DispatchedEffectConfirmed,
            next_task_stage: CoworkerTaskStage::UnderReview,
            run_id: Some(run_id.to_string()),
            worker_id: Some(format!("wrk_rev_{tag}")),
            task_worktree_id: Some(task_manifest.worktree_id.clone()),
            head_commit: None,
            tree_sha: None,
            diff_digest: None,
            report_artifact_id: Some(format!("art_rev_{tag}")),
            observed_model: Some("gemini-4-argon-high".to_string()),
            observed_effort: Some("high".to_string()),
            receipt_id: None,
            diagnostic: None,
        })
        .expect("finalize reviewer outbox");

    let candidate_id = format!("cnd_{tag}_01");
    let verdict_id = format!("vrd_{tag}_01");
    let verdict = StructuredReviewVerdict {
        verdict_id: verdict_id.clone(),
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
        prose_summary: Some("Structured review accepted".to_string()),
        prose_may_advance_acceptance: false,
    };
    let candidate = TaskCandidateRef {
        candidate_id: candidate_id.clone(),
        task_id: task_id.to_string(),
        attempt_no: 1,
        base_commit: review_receipt.base_commit.clone(),
        head_commit: review_receipt.task_head_commit.clone(),
        tree_sha: review_receipt.task_tree_sha.clone(),
        diff_digest: review_receipt.diff_digest.clone(),
        write_paths: vec![rel_file.to_string()],
        check_evidence_digests: vec![HEX64_A.to_string()],
        review_subject_digest: Some(review_receipt.subject_digest.clone()),
        review_verdict: Some(verdict),
        review_receipt_id: Some(review_receipt.receipt_id.clone()),
        integration_receipt_id: None,
    };
    let (job, _) = workflow_store
        .record_task_candidate_and_verdict(&RecordTaskReviewVerdictRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_A.to_string(),
            job_id: job.job_id.clone(),
            expected_job_revision: job.job_revision,
            expected_ledger_revision: None,
            candidate,
        })
        .expect("record task candidate and verdict");

    let (job, int_outbox) = workflow_store
        .reserve_task_outbox(&ReserveTaskOutboxRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_A.to_string(),
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

    let (job, _) = workflow_store
        .reconcile_or_finalize_outbox(&FinalizeTaskOutboxRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_A.to_string(),
            job_id: job.job_id.clone(),
            expected_job_revision: job.job_revision,
            expected_ledger_revision: None,
            outbox_id: int_outbox.outbox_id,
            status: OutboxDispatchStatus::DispatchedEffectConfirmed,
            next_task_stage: CoworkerTaskStage::Integrated,
            run_id: Some(run_id.to_string()),
            worker_id: None,
            task_worktree_id: Some(task_manifest.worktree_id.clone()),
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
        AcceptedHistoryTaskArtifacts {
            task_id: task_id.to_string(),
            candidate_id,
            verdict_id,
            task_manifest,
            review_receipt,
            integration_receipt,
        },
    )
}

async fn setup_accepted_history_fixture() -> AcceptedHistoryStoreFixture {
    let tmp = TempDir::new().expect("tempdir");
    let root = fs::canonicalize(tmp.path()).expect("canonicalize temp root");

    let raw_repo = root.join("repo");
    let (base_commit, base_tree_sha) = init_phase2_git_repo(&raw_repo);
    let repo_dir = fs::canonicalize(&raw_repo).expect("canonicalize repo");

    let bin_dir = make_private_dir(&root.join("bin"));
    let fake_agy_exe = bin_dir.join("fake-agy");
    let fake_cargo_exe = bin_dir.join("fake-cargo");
    write_executable_script(&fake_agy_exe, "#!/bin/sh\nexit 0\n");
    write_executable_script(
        &fake_cargo_exe,
        "#!/bin/sh\nprintf 'checks passed\\n'\nexit 0\n",
    );
    let fake_agy_exe = fs::canonicalize(&fake_agy_exe).expect("canonicalize fake-agy");
    let fake_cargo_exe = fs::canonicalize(&fake_cargo_exe).expect("canonicalize fake-cargo");

    let registry_dir = make_private_dir(&root.join("registry-store"));
    let plans_dir = make_private_dir(&root.join("plans-store"));
    let authority_dir = make_private_dir(&root.join("authority-store"));
    let sessions_dir = make_private_dir(&root.join("sessions-store"));

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

    let plan_id = "plan-hist-acc-01".to_string();
    let reg_snap = registry.snapshot().expect("registry snapshot");
    let draft = plans
        .change_at(
            0,
            PlanChange::SaveDraft {
                plan_id: plan_id.clone(),
                binding_id: BINDING_A.to_string(),
                profile_id: "profile-coworker".to_string(),
                profile_revision: reg_snap.revision,
                summary: "Accepted workspace history plan".to_string(),
                tasks: vec![
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
                ],
            },
            1_700_000_000_000,
        )
        .expect("save plan draft");
    let published = plans
        .change(
            draft.revision,
            PlanChange::Publish {
                plan_id: plan_id.clone(),
                revision: draft.revision,
            },
        )
        .expect("publish plan");
    let compiled = compile_plan_revision(&published, &reg_snap).expect("compile plan");
    let approval_summary = authority
        .approve_at(
            OperatorApprovalRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_A.to_string(),
                plan_id: plan_id.clone(),
                plan_revision: published.revision,
                expected_registry_revision: reg_snap.revision,
                expected_plan_digest: compiled.plan_digest,
                repository_path: repo_dir.clone(),
                base_commit: base_commit.clone(),
                approved_task_ids: vec!["task-alpha".to_string(), "task-beta".to_string()],
                approved_write_paths: vec!["src/alpha.rs".to_string(), "src/beta.rs".to_string()],
                max_parallel: 2,
                budgets: ExecutionBudgets::default(),
            },
            1_700_000_001_000,
        )
        .expect("approve plan");
    let approval_id = approval_summary.approval_id;

    // Prepare real host session directory `<sessions_dir>/<approval_id>/work-area` using shared WORK_AREA_SUBDIR.
    let session_dir =
        prepare_session_dir(&sessions_dir, &approval_id).expect("prepare host session dir");
    let session_lock =
        HostSessionLock::acquire(&session_dir, &approval_id).expect("acquire host session lock");
    drop(session_lock);
    let work_area_root = make_private_dir(&session_dir.join(WORK_AREA_SUBDIR));

    let run_id = "run_hist_cycle_01".to_string();
    let manager =
        WorktreeManager::new(&repo_dir, &work_area_root, &base_commit).expect("WorktreeManager");
    let integration_manifest = manager
        .create_run_worktree_with_token(&run_id, "int01")
        .expect("create RunIntegration worktree");

    let project_base = CanonicalProjectBaseRef::resolve_and_validate(&repo_dir, &base_commit)
        .expect("resolve canonical project_base");
    let brief = WorkBrief {
        brief_id: "brf_hist_acc_01".to_string(),
        binding_id: BINDING_A.to_string(),
        thread_uuid: THREAD_A.to_string(),
        project_base,
        kind: WorkBriefKind::NewJob,
        summary: "Verify store-backed historical acceptance".to_string(),
        confirmed_requirements: vec!["Verify real receipts and Closure ledger".to_string()],
        proposed_additions: Vec::new(),
        assumptions: Vec::new(),
        open_questions: Vec::new(),
        accepted_architect_decisions: Vec::new(),
    };
    let job = workflow_store
        .create_or_amend_job(&CreateCoworkerJobRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_A.to_string(),
            request_key: "req_job_hist_acc_01".to_string(),
            expected_ledger_revision: None,
            work_brief: brief,
            amended_target_task_ids: Vec::new(),
            amended_write_paths: Vec::new(),
            amended_interfaces: Vec::new(),
        })
        .expect("create job");

    let cap_env = FrozenControllerCapabilityEnvelope {
        allowed_reviewer_choices: vec![phase2_argon_choice(Difficulty::Hard)],
        allowed_closure_choices: vec![phase2_argon_choice(Difficulty::Extreme)],
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

    let bound_job = workflow_store
        .bind_job_approval(
            &BindJobApprovalRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_A.to_string(),
                job_id: job.job_id,
                expected_job_revision: job.job_revision,
                expected_ledger_revision: None,
                approval_id: approval_id.clone(),
                policy: phase2_execution_policy(),
                controller_envelope: Some(cap_env),
                task_interfaces,
            },
            &authority,
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

    let check_commands = phase2_check_commands(&fake_cargo_exe);
    let preview_req = FreezeControllerExecutionEnvelopeRequest {
        binding_id: BINDING_A.to_string(),
        thread_uuid: THREAD_A.to_string(),
        job_id: bound_job.job_id.clone(),
        expected_job_revision: bound_job.job_revision,
        policy: phase2_execution_policy(),
        reviewer_choice: phase2_argon_choice(Difficulty::Hard),
        closure_choice: phase2_argon_choice(Difficulty::Extreme),
        role_budgets,
        max_in_scope_corrections_per_task: 1,
        max_total_corrections_per_job: 2,
        max_total_task_attempts_per_job: 4,
        check_commands: check_commands.clone(),
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
    let preview: ControllerConfigurationPreview = policy_store
        .preview_execution_envelope(&preview_req, &bound_job, &authority)
        .expect("preview execution envelope");
    let mut freeze_req = preview_req;
    freeze_req.operator_attested = true;
    freeze_req.confirm_approval_subject_digest = Some(preview.approval_subject_digest);
    freeze_req.confirm_envelope_digest = Some(preview.envelope_digest);
    policy_store
        .record_execution_envelope(&freeze_req, &bound_job, &authority)
        .expect("freeze execution envelope");

    let (job, pred_alpha) = commit_review_and_integrate_phase2_task(
        &repo_dir,
        &work_area_root,
        &base_commit,
        &run_id,
        &integration_manifest,
        &workflow_store,
        bound_job,
        "task-alpha",
        "t01",
        "src/alpha.rs",
        "pub fn alpha() -> u32 {\n    100\n}\n",
        IntegrationStrategy::FastForwardOnly,
        1_700_000_110_000,
    );
    let (job, pred_beta) = commit_review_and_integrate_phase2_task(
        &repo_dir,
        &work_area_root,
        &base_commit,
        &run_id,
        &integration_manifest,
        &workflow_store,
        job,
        "task-beta",
        "t02",
        "src/beta.rs",
        "pub fn beta() -> u32 {\n    200\n}\n",
        IntegrationStrategy::MergeNoFf,
        1_700_000_120_000,
    );

    // Execute real frozen candidate checks on the owned RunIntegration worktree and persist 0600 reports.
    let check_subject = FrozenCheckSubject {
        binding_id: BINDING_A.to_string(),
        thread_uuid: THREAD_A.to_string(),
        job_id: job.job_id.clone(),
        target_kind: FrozenCheckTargetKind::RunIntegrationWorktree,
        repository_path: repo_dir.clone(),
        work_area_root: work_area_root.clone(),
        worktree_id: integration_manifest.worktree_id.clone(),
        expected_run_id: run_id.clone(),
        expected_task_id: None,
        expected_base_commit: base_commit.clone(),
        expected_head_commit: pred_beta
            .integration_receipt
            .integrated_target_commit
            .clone(),
        expected_tree_sha: pred_beta.integration_receipt.integrated_tree_sha.clone(),
    };
    let check_subject_digest = check_subject
        .subject_digest()
        .expect("check subject digest");
    let ownership_store = CoworkerJobOwnershipStore::new(&plans_dir).expect("open ownership store");
    let ownership_key =
        CoworkerJobOwnershipKey::new(BINDING_A, THREAD_A, &job.job_id).expect("ownership key");
    let guard = ownership_store
        .try_acquire_job_option(&ownership_key, 1_700_000_150_000)
        .expect("try_acquire_job_option")
        .expect("acquire job ownership guard");

    let fmt_spec = check_commands.get("cargo_fmt").expect("cargo_fmt spec");
    let test_spec = check_commands.get("cargo_test").expect("cargo_test spec");
    let fmt_report = run_frozen_check(&guard, &check_subject, fmt_spec)
        .await
        .expect("run passing cargo_fmt check");
    let test_report = run_frozen_check(&guard, &check_subject, test_spec)
        .await
        .expect("run passing cargo_test check");
    assert_eq!(fmt_report.status, FrozenCheckExecutionStatus::Passed);
    assert_eq!(test_report.status, FrozenCheckExecutionStatus::Passed);
    drop(guard);

    let fmt_effect_id = "eff_cand_chk_fmt_01";
    let test_effect_id = "eff_cand_chk_test_01";
    write_private_json(
        &candidate_check_report_file_path(&plans_dir, fmt_effect_id),
        &fmt_report,
    );
    write_private_json(
        &candidate_check_report_file_path(&plans_dir, test_effect_id),
        &test_report,
    );

    let promoted_check_evidence = verify_and_promote_check_reports(
        &check_subject,
        &check_commands,
        &[fmt_report, test_report],
    )
    .expect("promote candidate check reports");

    let lease = policy_store
        .acquire_or_renew_lease(
            BINDING_A,
            &job.job_id,
            "ctl_hist_setup_01",
            1_700_000_160_000,
            60_000,
        )
        .expect("acquire controller lease");
    policy_store
        .mutate_job_runtime(
            BINDING_A,
            &job.job_id,
            &lease.instance_id,
            lease.lease_epoch,
            |state| {
                state.pending_effects.insert(
                    fmt_effect_id.to_string(),
                    PendingControllerEffect {
                        effect_id: fmt_effect_id.to_string(),
                        request_key: "req_cand_chk_fmt_01".to_string(),
                        job_id: job.job_id.clone(),
                        task_id: None,
                        attempt_no: None,
                        kind: PendingControllerEffectKind::CheckExecution,
                        status: PendingControllerEffectStatus::EffectConfirmed,
                        subject_digest: check_subject_digest.clone(),
                        external_correlation_id: Some("cargo_fmt".to_string()),
                        created_at_ms: 1_700_000_160_000,
                        updated_at_ms: 1_700_000_161_000,
                        diagnostic: None,
                    },
                );
                state.pending_effects.insert(
                    test_effect_id.to_string(),
                    PendingControllerEffect {
                        effect_id: test_effect_id.to_string(),
                        request_key: "req_cand_chk_test_01".to_string(),
                        job_id: job.job_id.clone(),
                        task_id: None,
                        attempt_no: None,
                        kind: PendingControllerEffectKind::CheckExecution,
                        status: PendingControllerEffectStatus::EffectConfirmed,
                        subject_digest: check_subject_digest.clone(),
                        external_correlation_id: Some("cargo_test".to_string()),
                        created_at_ms: 1_700_000_161_000,
                        updated_at_ms: 1_700_000_162_000,
                        diagnostic: None,
                    },
                );
                Ok(())
            },
        )
        .expect("record candidate CheckExecution effects");
    policy_store
        .release_lease(
            BINDING_A,
            &job.job_id,
            &lease.instance_id,
            lease.lease_epoch,
        )
        .expect("release controller lease");

    let assembled = candidate_service
        .assemble_and_inspect_operations_candidate(
            &authority,
            &policy_store,
            &AssembleOperationsFinalCandidateRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_A.to_string(),
                job_id: job.job_id.clone(),
                expected_job_revision: job.job_revision,
                expected_ledger_revision: None,
                request_key: "req_assemble_hist_acc_01".to_string(),
                work_area_root: work_area_root.clone(),
                run_id: run_id.clone(),
                integration_worktree_id: integration_manifest.worktree_id.clone(),
                check_evidence: promoted_check_evidence,
                closure_task_id: "task-closure".to_string(),
                reserve_closure_attempt: true,
                reserved_at_ms: 1_700_000_200_000,
            },
        )
        .expect("assemble valid operations final candidate");

    configure_fake_agy_closure_response(
        &fake_agy_exe,
        &assembled.final_subject,
        ClosureVerdictKind::Accepted,
        "All integrated tasks and candidate checks verified cleanly.",
    );

    let closure_outcome = candidate_service
        .execute_final_candidate_closure(
            &authority,
            &policy_store,
            &ExecuteFinalCandidateClosureRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_A.to_string(),
                job_id: job.job_id.clone(),
                expected_job_revision: assembled.job.job_revision,
                expected_ledger_revision: None,
                command_key: "req_assemble_hist_acc_01".to_string(),
                combined_candidate_id: assembled.combined_candidate.combined_candidate_id.clone(),
                final_subject: assembled.final_subject.clone(),
                current_time_ms: 1_700_000_210_000,
            },
        )
        .await
        .expect("execute final candidate closure");

    assert_eq!(closure_outcome.job.phase, CoworkerJobPhase::Completed);
    let closure_verdict = closure_outcome.verdict.expect("accepted closure verdict");
    assert_eq!(closure_verdict.kind, ClosureVerdictKind::Accepted);
    let closure_recorded_at_ms = closure_verdict.recorded_at_ms;
    assert!(closure_recorded_at_ms > 0);

    AcceptedHistoryStoreFixture {
        _tmp: tmp,
        repo_dir,
        plans_dir,
        authority_dir,
        sessions_dir,
        work_area_root,
        base_commit,
        base_tree_sha,
        approval_id,
        plan_id,
        run_id,
        integration_manifest,
        fake_agy_exe,
        fake_cargo_exe,
        registry,
        plans,
        authority,
        workflow_store,
        policy_store,
        job_id: job.job_id,
        combined_candidate_id: assembled.combined_candidate.combined_candidate_id,
        closure_reservation_id: assembled
            .combined_candidate
            .closure_reservation_id
            .expect("closure_reservation_id"),
        closure_verdict_id: closure_verdict.verdict_id,
        closure_recorded_at_ms,
        final_subject: assembled.final_subject,
        pred_alpha,
        pred_beta,
    }
}

fn remove_lockfiles_in_dir(dir: &Path) {
    if let Ok(entries) = fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path
                .file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|name| name.ends_with(".lock"))
            {
                let _ = fs::remove_file(path);
            }
        }
    }
}

fn assert_no_lock_or_wal_files(dir: &Path) {
    if let Ok(entries) = fs::read_dir(dir) {
        for entry in entries.flatten() {
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            assert!(
                !name_str.ends_with(".lock")
                    && !name_str.ends_with(".tmp")
                    && !name_str.ends_with("-wal")
                    && !name_str.ends_with("-shm"),
                "read-only observation must never create lock/temp/WAL file `{name_str}` in {}",
                dir.display()
            );
        }
    }
}

#[tokio::test]
async fn store_backed_accepted_job_projects_verified_completed_and_survives_later_head_plan_and_registry_edits(
) {
    let fixture = setup_accepted_history_fixture().await;
    let chat = WorkspaceChatIdentity::new(
        SOURCE_A,
        BINDING_A,
        THREAD_A,
        &fixture.repo_dir.to_string_lossy(),
    )
    .expect("valid chat identity");

    // Pure in-memory projection without store-backed receipt verification must stay Unknown.
    let raw_ledger = fixture
        .workflow_store
        .read_binding_ledger(BINDING_A)
        .expect("read binding ledger");
    let raw_job = raw_ledger
        .jobs
        .iter()
        .find(|j| j.job_id == fixture.job_id)
        .expect("completed job in ledger");
    let raw_runtime = fixture
        .policy_store
        .get_job_runtime_readonly(BINDING_A, &fixture.job_id)
        .expect("read runtime")
        .expect("runtime exists");
    let pure_projected = project_coworker_job_record(&chat, raw_job, Some(&raw_runtime))
        .expect("pure projection succeeds");
    assert_eq!(
        pure_projected.acceptance_proof,
        WorkspaceAcceptanceProof::unknown(),
        "pure in-memory projection without store verification must never fabricate acceptance"
    );
    assert_eq!(pure_projected.job.state, ObservationJobState::Unknown);

    // Remove setup lockfiles to verify read_scoped_workspace_job_histories creates zero locks/WALs.
    remove_lockfiles_in_dir(&fixture.plans_dir);
    remove_lockfiles_in_dir(&fixture.authority_dir);
    remove_lockfiles_in_dir(&fixture.sessions_dir);
    remove_lockfiles_in_dir(&fixture.work_area_root);
    remove_lockfiles_in_dir(&fixture.work_area_root.join("manifests"));

    let stores = WorkspaceHistoryStores::new(&fixture.plans_dir)
        .with_plan_store(&fixture.plans)
        .with_authority_store(&fixture.authority)
        .with_sessions_dir(&fixture.sessions_dir);

    let first_read =
        read_scoped_workspace_job_histories(&chat, &stores).expect("store-backed accepted read");
    assert!(
        !first_read.degraded,
        "unexpected degraded diagnostics: {:?}",
        first_read.diagnostics
    );
    assert!(first_read.diagnostics.is_empty());
    assert_eq!(first_read.rejected_scope_count, 0);
    assert_eq!(first_read.jobs.len(), 1);

    assert_no_lock_or_wal_files(&fixture.plans_dir);
    assert_no_lock_or_wal_files(&fixture.authority_dir);
    assert_no_lock_or_wal_files(&fixture.sessions_dir);
    assert_no_lock_or_wal_files(&fixture.work_area_root);
    assert_no_lock_or_wal_files(&fixture.work_area_root.join("manifests"));

    let hist = &first_read.jobs[0];
    assert_eq!(hist.job.state, ObservationJobState::Completed);
    assert_eq!(hist.job.execution_status, "completed");
    assert_eq!(hist.job.review_state.as_deref(), Some("accepted"));
    assert_eq!(hist.current_attention, None);
    assert_eq!(
        hist.acceptance_proof.status,
        WorkspaceAcceptanceStatus::VerifiedAccepted
    );
    assert_eq!(
        hist.acceptance_proof.combined_candidate_id.as_deref(),
        Some(fixture.combined_candidate_id.as_str())
    );
    assert_eq!(
        hist.acceptance_proof.closure_reservation_id.as_deref(),
        Some(fixture.closure_reservation_id.as_str())
    );
    assert_eq!(
        hist.acceptance_proof.closure_verdict_id.as_deref(),
        Some(fixture.closure_verdict_id.as_str())
    );
    assert_eq!(
        hist.acceptance_proof.verified_at_ms,
        Some(fixture.closure_recorded_at_ms)
    );

    let task_alpha = hist
        .tasks
        .iter()
        .find(|t| t.task_id == fixture.pred_alpha.task_id)
        .expect("projected task-alpha");
    assert_eq!(task_alpha.status, "completed");
    assert_eq!(
        task_alpha.acceptance_proof.status,
        WorkspaceAcceptanceStatus::VerifiedAccepted
    );
    assert_eq!(
        task_alpha.acceptance_proof.accepted_candidate_id.as_deref(),
        Some(fixture.pred_alpha.candidate_id.as_str())
    );
    assert_eq!(
        task_alpha.acceptance_proof.review_verdict_id.as_deref(),
        Some(fixture.pred_alpha.verdict_id.as_str())
    );
    assert_eq!(
        task_alpha.acceptance_proof.review_receipt_id.as_deref(),
        Some(fixture.pred_alpha.review_receipt.receipt_id.as_str())
    );
    assert_eq!(
        task_alpha
            .acceptance_proof
            .integration_receipt_id
            .as_deref(),
        Some(fixture.pred_alpha.integration_receipt.receipt_id.as_str())
    );

    let task_beta = hist
        .tasks
        .iter()
        .find(|t| t.task_id == fixture.pred_beta.task_id)
        .expect("projected task-beta");
    assert_eq!(task_beta.status, "completed");
    assert_eq!(
        task_beta.acceptance_proof.status,
        WorkspaceAcceptanceStatus::VerifiedAccepted
    );
    assert_eq!(
        task_beta.acceptance_proof.integration_receipt_id.as_deref(),
        Some(fixture.pred_beta.integration_receipt.receipt_id.as_str())
    );

    // Advance primary source repository HEAD, publish a newer plan revision, mutate RegistryStore
    // profile/models, and delete worker/check executables to prove historical acceptance survives.
    fs::write(
        fixture.repo_dir.join("src/later_change.rs"),
        "pub fn later() -> u32 {\n    999\n}\n",
    )
    .expect("write post-completion source file");
    run_sanitized_git(
        &fixture.repo_dir,
        &["add", "src/later_change.rs"],
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .expect("git add later_change");
    run_sanitized_git(
        &fixture.repo_dir,
        &[
            "commit",
            "-m",
            "post-completion primary HEAD advance",
            "--quiet",
        ],
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .expect("git commit later_change");
    let moved_head =
        resolve_exact_commit_sha(&fixture.repo_dir, "HEAD", DEFAULT_GIT_COMMAND_TIMEOUT_MS)
            .expect("resolve moved HEAD");
    let moved_tree =
        resolve_exact_tree_sha(&fixture.repo_dir, "HEAD", DEFAULT_GIT_COMMAND_TIMEOUT_MS)
            .expect("resolve moved tree");
    assert_ne!(moved_head, fixture.base_commit);
    assert_ne!(moved_tree, fixture.base_tree_sha);

    let current_reg = fixture.registry.snapshot().expect("registry snapshot");
    let updated_reg = fixture
        .registry
        .apply(
            current_reg.revision,
            RegistryChange::UpsertModels {
                models: vec![ModelDefinition {
                    id: "model-next-gen".to_string(),
                    tool_id: "tool-agy".to_string(),
                    label: "Gemini Next Gen".to_string(),
                    model_arg: "gemini-4-carbon-high".to_string(),
                    efforts: vec!["high".to_string()],
                    allow_no_effort: false,
                }],
            },
        )
        .expect("upsert newer model");
    let updated_reg = fixture
        .registry
        .apply(
            updated_reg.revision,
            RegistryChange::UpsertProfile {
                profile: WorkflowProfile {
                    decision_advisor: None,
                    id: "profile-coworker".to_string(),
                    name: "Updated Coworker Profile".to_string(),
                    assignments: vec![RoleAssignment {
                        role: WorkflowRole::Coder,
                        difficulty: Some(Difficulty::Easy),
                        choice: ModelChoice {
                            model_id: "model-next-gen".to_string(),
                            effort: Some("high".to_string()),
                        },
                    }],
                },
            },
        )
        .expect("mutate profile assignments");

    let latest_plan = fixture
        .plans
        .latest_revision(&fixture.plan_id)
        .expect("read latest plan")
        .expect("plan exists");
    let next_draft = fixture
        .plans
        .change_at(
            latest_plan.revision,
            PlanChange::SaveDraft {
                plan_id: fixture.plan_id.clone(),
                binding_id: BINDING_A.to_string(),
                profile_id: "profile-coworker".to_string(),
                profile_revision: updated_reg.revision,
                summary: "Subsequent follow-up plan revision".to_string(),
                tasks: vec![PlanTask {
                    id: "task-followup".to_string(),
                    title: "Followup Task".to_string(),
                    instruction: "Update src/later_change.rs".to_string(),
                    role: WorkflowRole::Coder,
                    difficulty: Difficulty::Easy,
                    depends_on: Vec::new(),
                    allowed_write_paths: vec!["src/later_change.rs".to_string()],
                }],
            },
            1_700_000_300_000,
        )
        .expect("save follow-up plan draft");
    fixture
        .plans
        .change(
            next_draft.revision,
            PlanChange::Publish {
                plan_id: fixture.plan_id.clone(),
                revision: next_draft.revision,
            },
        )
        .expect("publish follow-up plan revision");

    fs::remove_file(&fixture.fake_agy_exe).expect("remove fake-agy");
    fs::remove_file(&fixture.fake_cargo_exe).expect("remove fake-cargo");
    assert!(!fixture.fake_agy_exe.exists());
    assert!(!fixture.fake_cargo_exe.exists());
    remove_lockfiles_in_dir(&fixture.plans_dir);

    let second_read = read_scoped_workspace_job_histories(&chat, &stores)
        .expect("historical read after source HEAD, plan, and registry edits");
    assert!(
        !second_read.degraded,
        "historical acceptance must survive later legitimate HEAD/plan/profile edits: {:?}",
        second_read.diagnostics
    );
    assert_eq!(second_read.jobs.len(), 1);
    assert_eq!(
        second_read.jobs[0].job.state,
        ObservationJobState::Completed
    );
    assert_eq!(second_read.jobs[0].job.execution_status, "completed");
    assert_eq!(
        second_read.jobs[0].acceptance_proof.status,
        WorkspaceAcceptanceStatus::VerifiedAccepted
    );
    assert_eq!(
        second_read.jobs[0].acceptance_proof.verified_at_ms,
        Some(fixture.closure_recorded_at_ms)
    );
    let second_tasks = &second_read.jobs[0].tasks;
    assert!(
        second_tasks
            .iter()
            .all(|t| t.acceptance_proof.status == WorkspaceAcceptanceStatus::VerifiedAccepted),
        "all integrated task proofs must remain VerifiedAccepted after executable removal"
    );
    assert!(!fixture.fake_agy_exe.exists());
    assert!(!fixture.fake_cargo_exe.exists());
    assert_no_lock_or_wal_files(&fixture.plans_dir);
    assert_no_lock_or_wal_files(&fixture.authority_dir);
    assert_no_lock_or_wal_files(&fixture.sessions_dir);
    assert_no_lock_or_wal_files(&fixture.work_area_root);
    assert_no_lock_or_wal_files(&fixture.work_area_root.join("manifests"));

    // Active execution/admission validators MUST still reject the missing executable fail-closed.
    let post_remove_ledger = fixture
        .workflow_store
        .read_binding_ledger(BINDING_A)
        .expect("read binding ledger after executable removal");
    let post_remove_job = post_remove_ledger
        .jobs
        .iter()
        .find(|j| j.job_id == fixture.job_id)
        .expect("completed job in ledger");
    let post_remove_runtime = fixture
        .policy_store
        .get_job_runtime_readonly(BINDING_A, &fixture.job_id)
        .expect("read runtime after executable removal")
        .expect("runtime exists");
    let post_remove_spec = post_remove_runtime
        .envelope
        .check_commands
        .get("cargo_test")
        .expect("frozen cargo_test spec");
    assert!(
        post_remove_spec.validate().is_err(),
        "active FrozenCheckCommandSpec::validate must still reject removed executable"
    );
    assert!(
        post_remove_spec.spec_digest().is_err(),
        "active FrozenCheckCommandSpec::spec_digest must still reject removed executable"
    );
    assert!(
        post_remove_runtime.envelope.compute_digest().is_err(),
        "active FrozenControllerExecutionEnvelope::compute_digest must still reject removed executable"
    );
    assert!(
        post_remove_runtime
            .envelope
            .validate_against_job(post_remove_job)
            .is_err(),
        "active FrozenControllerExecutionEnvelope::validate_against_job must still reject removed executable"
    );
    assert!(
        post_remove_runtime
            .envelope
            .verify_matches_job(post_remove_job)
            .is_err(),
        "active FrozenControllerExecutionEnvelope::verify_matches_job must still reject removed executable"
    );

    // Observer-only historical verification MUST still fail closed on tampered check specs,
    // non-normalized/disallowed executable paths, tampered envelope digests, or missing receipts.
    let saved_spec = post_remove_spec.clone();
    let saved_digest = post_remove_runtime.envelope.envelope_digest.clone();
    let saved_health = post_remove_runtime.provider_health.clone();

    // 1. Tampered check command argv while executables remain deleted must degrade to Unknown.
    fixture
        .policy_store
        .mutate_job_runtime_for_operator(BINDING_A, &fixture.job_id, |state| {
            state
                .envelope
                .check_commands
                .get_mut("cargo_test")
                .expect("cargo_test spec")
                .argv
                .push("--tampered".to_string());
            Ok(())
        })
        .expect("tamper check command argv");
    let tampered_argv_read =
        read_scoped_workspace_job_histories(&chat, &stores).expect("tampered argv historical read");
    assert!(tampered_argv_read.degraded);
    assert!(tampered_argv_read
        .diagnostics
        .iter()
        .any(|d| d.kind == WorkspaceHistoryDiagnosticKind::AcceptanceVerificationFailed));
    assert_eq!(
        tampered_argv_read.jobs[0].job.state,
        ObservationJobState::Unknown
    );
    assert_eq!(
        tampered_argv_read.jobs[0].acceptance_proof,
        WorkspaceAcceptanceProof::unknown()
    );

    // 2. Non-normalized, relative, traversal, or shell-wrapper executable paths must fail closed.
    for bad_exe in [
        "",
        "relative/fake-cargo",
        "/tmp/../bin/fake-cargo",
        "/tmp/./bin/fake-cargo",
        "/tmp//bin/fake-cargo",
        "/tmp/bin/fake-cargo/",
        "/usr/bin/sh",
    ] {
        fixture
            .policy_store
            .mutate_job_runtime_for_operator(BINDING_A, &fixture.job_id, |state| {
                let spec = state
                    .envelope
                    .check_commands
                    .get_mut("cargo_test")
                    .expect("cargo_test spec");
                *spec = saved_spec.clone();
                spec.executable = PathBuf::from(bad_exe);
                Ok(())
            })
            .expect("tamper stored executable path");
        let bad_exe_read = read_scoped_workspace_job_histories(&chat, &stores)
            .expect("invalid stored executable historical read");
        assert!(
            bad_exe_read.degraded,
            "stored executable `{bad_exe}` must be rejected in historical verification"
        );
        assert!(bad_exe_read
            .diagnostics
            .iter()
            .any(|d| d.kind == WorkspaceHistoryDiagnosticKind::AcceptanceVerificationFailed));
        assert_eq!(bad_exe_read.jobs[0].job.state, ObservationJobState::Unknown);
        assert_eq!(
            bad_exe_read.jobs[0].acceptance_proof,
            WorkspaceAcceptanceProof::unknown()
        );
    }

    // 3. Tampered envelope_digest with valid check specs must degrade to Unknown.
    fixture
        .policy_store
        .mutate_job_runtime_for_operator(BINDING_A, &fixture.job_id, |state| {
            state
                .envelope
                .check_commands
                .insert("cargo_test".to_string(), saved_spec.clone());
            state.envelope.envelope_digest = HEX64_D.to_string();
            state.provider_health.clear();
            Ok(())
        })
        .expect("tamper envelope_digest");
    let tampered_digest_read = read_scoped_workspace_job_histories(&chat, &stores)
        .expect("tampered envelope_digest historical read");
    assert!(tampered_digest_read.degraded);
    assert!(tampered_digest_read
        .diagnostics
        .iter()
        .any(|d| d.kind == WorkspaceHistoryDiagnosticKind::AcceptanceVerificationFailed));
    assert_eq!(
        tampered_digest_read.jobs[0].job.state,
        ObservationJobState::Unknown
    );
    assert_eq!(
        tampered_digest_read.jobs[0].acceptance_proof,
        WorkspaceAcceptanceProof::unknown()
    );

    // Restore untampered runtime envelope before testing missing check report / review receipt.
    fixture
        .policy_store
        .mutate_job_runtime_for_operator(BINDING_A, &fixture.job_id, |state| {
            state
                .envelope
                .check_commands
                .insert("cargo_test".to_string(), saved_spec.clone());
            state.envelope.envelope_digest = saved_digest.clone();
            state.provider_health = saved_health.clone();
            Ok(())
        })
        .expect("restore untampered runtime envelope");

    // 4. Missing persisted candidate check report while executables are deleted must not verify.
    let cand_report_path =
        candidate_check_report_file_path(&fixture.plans_dir, "eff_cand_chk_test_01");
    let cand_report_bytes = fs::read(&cand_report_path).expect("read candidate check report");
    fs::remove_file(&cand_report_path).expect("remove candidate check report");
    let missing_report_read = read_scoped_workspace_job_histories(&chat, &stores)
        .expect("missing candidate check report read");
    assert!(missing_report_read.degraded);
    assert!(missing_report_read
        .diagnostics
        .iter()
        .any(|d| d.kind == WorkspaceHistoryDiagnosticKind::AcceptanceVerificationFailed));
    assert_eq!(
        missing_report_read.jobs[0].job.state,
        ObservationJobState::Unknown
    );
    assert_eq!(
        missing_report_read.jobs[0].acceptance_proof,
        WorkspaceAcceptanceProof::unknown()
    );
    fs::write(&cand_report_path, &cand_report_bytes).expect("restore candidate check report");
    fs::set_permissions(&cand_report_path, fs::Permissions::from_mode(0o600))
        .expect("chmod 0600 restored candidate check report");

    // 5. Missing persisted ReviewReceipt while executables are deleted must not verify.
    let alpha_receipt_id = &fixture.pred_alpha.review_receipt.receipt_id;
    let alpha_receipt_path = fixture
        .work_area_root
        .join("manifests/receipts")
        .join(format!("review-{alpha_receipt_id}.json"));
    let alpha_receipt_bytes = fs::read(&alpha_receipt_path).expect("read alpha review receipt");
    fs::remove_file(&alpha_receipt_path).expect("remove alpha review receipt");
    let missing_receipt_read =
        read_scoped_workspace_job_histories(&chat, &stores).expect("missing review receipt read");
    assert!(missing_receipt_read.degraded);
    assert!(missing_receipt_read
        .diagnostics
        .iter()
        .any(|d| d.kind == WorkspaceHistoryDiagnosticKind::AcceptanceVerificationFailed));
    assert_eq!(
        missing_receipt_read.jobs[0].job.state,
        ObservationJobState::Unknown
    );
    assert_eq!(
        missing_receipt_read.jobs[0].acceptance_proof,
        WorkspaceAcceptanceProof::unknown()
    );
    fs::write(&alpha_receipt_path, &alpha_receipt_bytes).expect("restore alpha review receipt");
    fs::set_permissions(&alpha_receipt_path, fs::Permissions::from_mode(0o600))
        .expect("chmod 0600 restored alpha review receipt");

    remove_lockfiles_in_dir(&fixture.plans_dir);
    let recovered_read = read_scoped_workspace_job_histories(&chat, &stores)
        .expect("recovered historical read with deleted executables");
    assert!(!recovered_read.degraded);
    assert_eq!(
        recovered_read.jobs[0].job.state,
        ObservationJobState::Completed
    );
    assert_eq!(
        recovered_read.jobs[0].acceptance_proof.status,
        WorkspaceAcceptanceStatus::VerifiedAccepted
    );
    assert_eq!(
        recovered_read.jobs[0].acceptance_proof.verified_at_ms,
        Some(fixture.closure_recorded_at_ms)
    );
    assert!(!fixture.fake_agy_exe.exists());
    assert!(!fixture.fake_cargo_exe.exists());
    assert_no_lock_or_wal_files(&fixture.plans_dir);
}

#[tokio::test]
async fn store_backed_accepted_job_rejects_wrong_source_binding_thread_or_repo_and_degrades_on_missing_runtime(
) {
    let fixture = setup_accepted_history_fixture().await;
    let stores = WorkspaceHistoryStores::new(&fixture.plans_dir)
        .with_plan_store(&fixture.plans)
        .with_authority_store(&fixture.authority)
        .with_sessions_dir(&fixture.sessions_dir);

    // 1. Invalid source_id is rejected before reading stores.
    assert!(WorkspaceChatIdentity::new(
        "invalid source",
        BINDING_A,
        THREAD_A,
        &fixture.repo_dir.to_string_lossy()
    )
    .is_err());

    // 2. Different source_id never attributes the accepted job to another source label.
    let other_source_chat = WorkspaceChatIdentity::new(
        "secondary_source",
        BINDING_B,
        THREAD_A,
        &fixture.repo_dir.to_string_lossy(),
    )
    .expect("valid secondary source chat");
    let wrong_binding_read = read_scoped_workspace_job_histories(&other_source_chat, &stores)
        .expect("wrong binding read");
    assert!(wrong_binding_read.jobs.is_empty());

    // 3. Wrong thread_uuid is scope-rejected and never shows accepted.
    let wrong_thread_chat = WorkspaceChatIdentity::new(
        SOURCE_A,
        BINDING_A,
        THREAD_B,
        &fixture.repo_dir.to_string_lossy(),
    )
    .expect("wrong thread chat");
    let wrong_thread_read = read_scoped_workspace_job_histories(&wrong_thread_chat, &stores)
        .expect("wrong thread read");
    assert!(wrong_thread_read.jobs.is_empty());
    assert_eq!(wrong_thread_read.rejected_scope_count, 1);
    assert!(wrong_thread_read
        .diagnostics
        .iter()
        .any(|d| d.kind == WorkspaceHistoryDiagnosticKind::ScopeRejected));

    // 4. Wrong canonical_project_scope (foreign repository) is scope-rejected and never shows accepted.
    let foreign_repo = make_private_dir(&fixture.repo_dir.parent().expect("root").join("foreign"));
    let wrong_repo_chat = WorkspaceChatIdentity::new(
        SOURCE_A,
        BINDING_A,
        THREAD_A,
        &foreign_repo.to_string_lossy(),
    )
    .expect("wrong repo chat");
    let wrong_repo_read =
        read_scoped_workspace_job_histories(&wrong_repo_chat, &stores).expect("wrong repo read");
    assert!(wrong_repo_read.jobs.is_empty());
    assert_eq!(wrong_repo_read.rejected_scope_count, 1);
    assert!(wrong_repo_read
        .diagnostics
        .iter()
        .any(|d| d.kind == WorkspaceHistoryDiagnosticKind::ScopeRejected));

    // 5. Missing ControllerJobRuntimeState degrades to Unknown and never shows VerifiedAccepted/Completed.
    let valid_chat = WorkspaceChatIdentity::new(
        SOURCE_A,
        BINDING_A,
        THREAD_A,
        &fixture.repo_dir.to_string_lossy(),
    )
    .expect("valid chat");
    let runtime_ledger_path = fixture
        .policy_store
        .ledger_path_for_binding(BINDING_A)
        .expect("runtime ledger path");
    fs::remove_file(&runtime_ledger_path).expect("remove controller runtime ledger");

    let missing_rt_read =
        read_scoped_workspace_job_histories(&valid_chat, &stores).expect("missing runtime read");
    assert!(missing_rt_read.degraded);
    assert!(missing_rt_read
        .diagnostics
        .iter()
        .any(|d| d.kind == WorkspaceHistoryDiagnosticKind::MissingRuntime));
    assert!(missing_rt_read
        .diagnostics
        .iter()
        .any(|d| d.kind == WorkspaceHistoryDiagnosticKind::AcceptanceVerificationFailed));
    assert_eq!(missing_rt_read.jobs.len(), 1);
    assert_eq!(
        missing_rt_read.jobs[0].job.state,
        ObservationJobState::Unknown
    );
    assert_eq!(missing_rt_read.jobs[0].job.execution_status, "unknown");
    assert_eq!(
        missing_rt_read.jobs[0].acceptance_proof,
        WorkspaceAcceptanceProof::unknown()
    );
}

#[tokio::test]
async fn forged_or_duplicate_receipts_never_verify_historical_acceptance() {
    let fixture = setup_accepted_history_fixture().await;
    let chat = WorkspaceChatIdentity::new(
        SOURCE_A,
        BINDING_A,
        THREAD_A,
        &fixture.repo_dir.to_string_lossy(),
    )
    .expect("valid chat");
    let stores = WorkspaceHistoryStores::new(&fixture.plans_dir)
        .with_plan_store(&fixture.plans)
        .with_authority_store(&fixture.authority)
        .with_sessions_dir(&fixture.sessions_dir);

    // 1. Forged ReviewReceipt on disk (even with recomputed receipt_digest) must fail verification.
    let alpha_receipt_id = &fixture.pred_alpha.review_receipt.receipt_id;
    let original_receipt = load_review_receipt(&fixture.work_area_root, alpha_receipt_id)
        .expect("load review receipt");
    let receipt_path = fixture
        .work_area_root
        .join("manifests/receipts")
        .join(format!("review-{alpha_receipt_id}.json"));
    let original_bytes = fs::read(&receipt_path).expect("read original receipt bytes");

    let mut forged_receipt = original_receipt;
    forged_receipt.policy.policy_id = "policy-forged".to_string();
    forged_receipt.receipt_digest = forged_receipt
        .compute_digest()
        .expect("recompute forged receipt digest");
    forged_receipt
        .verify_integrity()
        .expect("forged receipt passes standalone digest check");
    write_private_json(&receipt_path, &forged_receipt);

    let forged_read =
        read_scoped_workspace_job_histories(&chat, &stores).expect("forged receipt read");
    assert!(forged_read.degraded);
    assert!(forged_read
        .diagnostics
        .iter()
        .any(|d| d.kind == WorkspaceHistoryDiagnosticKind::AcceptanceVerificationFailed));
    assert_eq!(forged_read.jobs[0].job.state, ObservationJobState::Unknown);
    assert_eq!(
        forged_read.jobs[0].acceptance_proof,
        WorkspaceAcceptanceProof::unknown()
    );

    // Restore authentic receipt and verify acceptance recovers.
    fs::write(&receipt_path, &original_bytes).expect("restore original receipt");
    fs::set_permissions(&receipt_path, fs::Permissions::from_mode(0o600))
        .expect("chmod 0600 restored receipt");
    let restored_read =
        read_scoped_workspace_job_histories(&chat, &stores).expect("restored receipt read");
    assert!(!restored_read.degraded);
    assert_eq!(
        restored_read.jobs[0].acceptance_proof.status,
        WorkspaceAcceptanceStatus::VerifiedAccepted
    );

    // 2. Duplicate receipt reference across combined-candidate predecessors must fail closed.
    let mut ledger = fixture
        .workflow_store
        .read_binding_ledger(BINDING_A)
        .expect("read binding ledger");
    let job_mut = ledger
        .jobs
        .iter_mut()
        .find(|j| j.job_id == fixture.job_id)
        .expect("find job");
    let comb_mut = &mut job_mut.combined_candidates[0];
    comb_mut.accepted_predecessor_receipts[1].review_receipt_id = comb_mut
        .accepted_predecessor_receipts[0]
        .review_receipt_id
        .clone();
    comb_mut.candidate_digest = comb_mut
        .compute_candidate_digest()
        .expect("recompute combined candidate digest");
    let ledger_path = fixture
        .workflow_store
        .ledger_path_for_binding(BINDING_A)
        .expect("workflow ledger path");
    write_private_json(&ledger_path, &ledger);

    let dup_read =
        read_scoped_workspace_job_histories(&chat, &stores).expect("duplicate receipt read");
    assert!(dup_read.degraded);
    assert_eq!(dup_read.jobs[0].job.state, ObservationJobState::Unknown);
    assert_eq!(
        dup_read.jobs[0].acceptance_proof,
        WorkspaceAcceptanceProof::unknown()
    );
}

#[tokio::test]
async fn failed_candidate_checks_and_fenced_closure_artifacts_never_show_accepted() {
    let fixture = setup_accepted_history_fixture().await;
    let chat = WorkspaceChatIdentity::new(
        SOURCE_A,
        BINDING_A,
        THREAD_A,
        &fixture.repo_dir.to_string_lossy(),
    )
    .expect("valid chat");
    let stores = WorkspaceHistoryStores::new(&fixture.plans_dir)
        .with_plan_store(&fixture.plans)
        .with_authority_store(&fixture.authority)
        .with_sessions_dir(&fixture.sessions_dir);

    let _ = (
        &fixture.approval_id,
        &fixture.final_subject,
        &fixture.pred_alpha.task_manifest,
        &fixture.pred_beta.task_manifest,
    );

    // 1. Authentic failing FrozenCheckExecutionReport on disk keeps acceptance Unknown.
    let test_report_path =
        candidate_check_report_file_path(&fixture.plans_dir, "eff_cand_chk_test_01");
    let passing_report_bytes = fs::read(&test_report_path).expect("read passing check report");

    write_executable_script(
        &fixture.fake_cargo_exe,
        "#!/bin/sh\nprintf 'unit test failure\\n' >&2\nexit 1\n",
    );
    let check_commands = phase2_check_commands(&fixture.fake_cargo_exe);
    let test_spec = check_commands.get("cargo_test").expect("cargo_test spec");
    let check_subject = FrozenCheckSubject {
        binding_id: BINDING_A.to_string(),
        thread_uuid: THREAD_A.to_string(),
        job_id: fixture.job_id.clone(),
        target_kind: FrozenCheckTargetKind::RunIntegrationWorktree,
        repository_path: fixture.repo_dir.clone(),
        work_area_root: fixture.work_area_root.clone(),
        worktree_id: fixture.integration_manifest.worktree_id.clone(),
        expected_run_id: fixture.run_id.clone(),
        expected_task_id: None,
        expected_base_commit: fixture.base_commit.clone(),
        expected_head_commit: fixture
            .pred_beta
            .integration_receipt
            .integrated_target_commit
            .clone(),
        expected_tree_sha: fixture
            .pred_beta
            .integration_receipt
            .integrated_tree_sha
            .clone(),
    };
    let ownership_store =
        CoworkerJobOwnershipStore::new(&fixture.plans_dir).expect("open ownership store");
    let ownership_key =
        CoworkerJobOwnershipKey::new(BINDING_A, THREAD_A, &fixture.job_id).expect("ownership key");
    let guard = ownership_store
        .try_acquire_job_option(&ownership_key, 1_700_000_250_000)
        .expect("try_acquire_job_option")
        .expect("acquire guard for failing check");
    let failing_report = run_frozen_check(&guard, &check_subject, test_spec)
        .await
        .expect("execute failing check");
    assert_eq!(
        failing_report.status,
        FrozenCheckExecutionStatus::NonZeroExit
    );
    assert!(!failing_report.passed());
    drop(guard);

    write_private_json(&test_report_path, &failing_report);

    let failed_check_read =
        read_scoped_workspace_job_histories(&chat, &stores).expect("failed check report read");
    assert!(failed_check_read.degraded);
    assert!(failed_check_read
        .diagnostics
        .iter()
        .any(|d| d.kind == WorkspaceHistoryDiagnosticKind::AcceptanceVerificationFailed));
    assert_eq!(
        failed_check_read.jobs[0].job.state,
        ObservationJobState::Unknown
    );
    assert_eq!(
        failed_check_read.jobs[0].acceptance_proof,
        WorkspaceAcceptanceProof::unknown()
    );

    // Restore passing check report and verify acceptance recovers before testing fenced Closure.
    fs::write(&test_report_path, &passing_report_bytes).expect("restore passing check report");
    fs::set_permissions(&test_report_path, fs::Permissions::from_mode(0o600))
        .expect("chmod 0600 restored check report");
    let recovered_read =
        read_scoped_workspace_job_histories(&chat, &stores).expect("recovered check read");
    assert_eq!(
        recovered_read.jobs[0].acceptance_proof.status,
        WorkspaceAcceptanceStatus::VerifiedAccepted
    );

    // 2. Fenced ClosureExecutionArtifactRecord (`fenced_from_promotion == true`) must never show accepted.
    let lease = fixture
        .policy_store
        .acquire_or_renew_lease(
            BINDING_A,
            &fixture.job_id,
            "ctl_hist_fence_01",
            1_700_000_260_000,
            60_000,
        )
        .expect("acquire lease");
    fixture
        .policy_store
        .mutate_job_runtime(
            BINDING_A,
            &fixture.job_id,
            &lease.instance_id,
            lease.lease_epoch,
            |state| {
                let art = state
                    .closure_artifacts
                    .get_mut(&fixture.closure_reservation_id)
                    .expect("selected closure artifact exists");
                art.fenced_from_promotion = true;
                Ok(())
            },
        )
        .expect("fence closure artifact");
    fixture
        .policy_store
        .release_lease(
            BINDING_A,
            &fixture.job_id,
            &lease.instance_id,
            lease.lease_epoch,
        )
        .expect("release lease");

    let fenced_read =
        read_scoped_workspace_job_histories(&chat, &stores).expect("fenced closure read");
    assert!(fenced_read.degraded);
    assert!(fenced_read
        .diagnostics
        .iter()
        .any(|d| d.kind == WorkspaceHistoryDiagnosticKind::AcceptanceVerificationFailed));
    assert_eq!(fenced_read.jobs[0].job.state, ObservationJobState::Unknown);
    assert_eq!(fenced_read.jobs[0].job.execution_status, "unknown");
    assert_eq!(
        fenced_read.jobs[0].acceptance_proof,
        WorkspaceAcceptanceProof::unknown()
    );
}
