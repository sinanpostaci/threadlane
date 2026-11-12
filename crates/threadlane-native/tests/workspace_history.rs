//! Focused Phase 1 and Phase 2 tests for scoped read-only workspace job history projection
//! and store-backed historical acceptance verification.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::io::Write;
use std::os::unix::fs::{symlink, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde::Serialize;
use sha2::{Digest, Sha256};
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
use threadlane_native::error::NativeErrorCode;
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

// ============================================================================
// Obsolete planning supersession retirement & unresolved-effect preservation
// ============================================================================

fn sample_amendment_hold(
    hold_id: &str,
    request_key: &str,
    created_at_job_revision: u64,
    detail: &str,
) -> WorkflowHoldRecord {
    WorkflowHoldRecord {
        hold_id: hold_id.to_string(),
        request_key: request_key.to_string(),
        reason: WorkflowHoldReason::BriefAmendmentPending,
        detail: detail.to_string(),
        directly_affected_task_ids: Vec::new(),
        affected_write_paths: Vec::new(),
        affected_interfaces: Vec::new(),
        holds_entire_job: true,
        active: true,
        created_at_job_revision,
        resolved_at_job_revision: None,
        resolution_note: None,
    }
}

#[allow(clippy::too_many_arguments)]
fn production_orchestrator_context_digest(
    binding_id: &str,
    thread_uuid: &str,
    grant_id: &str,
    profile_id: &str,
    registry_revision: u64,
    policy_digest: &str,
    job_id: &str,
    work_brief_digest: &str,
    project_base: &CanonicalProjectBaseRef,
    plan_id: Option<&str>,
    expected_plan_revision: u64,
    selected_choice: &SelectedRoleChoice,
) -> String {
    let empty_advice: Vec<(&str, &str, &str)> = Vec::new();
    let seed = (
        "threadlane-native:coworker-orchestrator-context:v1",
        binding_id,
        thread_uuid,
        grant_id,
        profile_id,
        registry_revision,
        policy_digest,
        job_id,
        work_brief_digest,
        project_base,
        plan_id,
        expected_plan_revision,
        selected_choice,
        &empty_advice,
    );
    let bytes = serde_json::to_vec(&seed).expect("serialize orchestrator context seed");
    let mut hasher = Sha256::new();
    hasher.update(&bytes);
    format!("{:x}", hasher.finalize())
}

#[allow(clippy::too_many_arguments)]
fn sample_completed_orchestrator_reservation_and_result(
    binding_id: &str,
    thread_uuid: &str,
    grant_id: &str,
    job_id: &str,
    suffix: &str,
    work_brief_digest: &str,
    project_base: &CanonicalProjectBaseRef,
    planning_policy_digest: &str,
    plan_id: &str,
    run_id: &str,
) -> (PlanningCallReservation, PlanningResultRef) {
    let orchestrator_choice = sample_orchestrator_choice();
    let reservation_id = format!("rsv_{suffix}");
    let request_key = format!("req_plan_{suffix}");
    let report_artifact_id = format!("pca_{suffix}");
    let context_digest = production_orchestrator_context_digest(
        binding_id,
        thread_uuid,
        grant_id,
        "profile-default",
        1,
        planning_policy_digest,
        job_id,
        work_brief_digest,
        project_base,
        None,
        0,
        &orchestrator_choice,
    );
    let reservation = PlanningCallReservation {
        reservation_id: reservation_id.clone(),
        request_key: request_key.clone(),
        role: WorkflowRole::Orchestrator,
        difficulty: Some(Difficulty::Hard),
        selected_choice: orchestrator_choice.clone(),
        context_digest,
        call_project_base: Some(project_base.clone()),
        status: PlanningReservationStatus::Completed,
        reserved_at_ms: 1_700_000_048_000,
        updated_at_ms: 1_700_000_050_000,
        run_id: Some(run_id.to_string()),
        report_artifact_id: Some(report_artifact_id.clone()),
        output_digest: Some(HEX64_C.to_string()),
        diagnostic: None,
    };
    let result = PlanningResultRef {
        grant_id: grant_id.to_string(),
        reservation_id,
        request_key,
        run_id: run_id.to_string(),
        report_artifact_id,
        output_digest: HEX64_C.to_string(),
        work_brief_digest: work_brief_digest.to_string(),
        project_base: project_base.clone(),
        profile_id: "profile-default".to_string(),
        registry_revision: 1,
        policy_digest: planning_policy_digest.to_string(),
        selected_choice: orchestrator_choice,
        plan_id: plan_id.to_string(),
        plan_revision: 1,
        plan_digest: HEX64_B.to_string(),
        plan_status: PlanStatus::Published,
        task_interfaces: BTreeMap::new(),
        architect_advice_ids: Vec::new(),
        resolved_open_questions: Vec::new(),
        deferred_proposals: Vec::new(),
        recorded_at_ms: 1_700_000_050_000,
    };
    (reservation, result)
}

#[test]
fn retires_attention_for_store_verified_obsolete_planning_jobs_across_two_step_lineage_without_mutation(
) {
    let tmp = TempDir::new().expect("tempdir");
    let plans_dir = make_private_dir(&tmp.path().join("plans"));
    let repo_dir = make_private_dir(&tmp.path().join("repo-obsolete-lineage"));
    let chat =
        WorkspaceChatIdentity::new(SOURCE_A, BINDING_A, THREAD_A, &repo_dir.to_string_lossy())
            .expect("valid chat identity");

    let project_base = sample_project_base(&repo_dir);
    let planning_policy = sample_planning_policy();
    let planning_policy_digest = planning_policy.digest().expect("planning policy digest");
    let grant_id = "grt_obsolete_lineage_0123456789abcd";

    // Step 1: Held effect-free planning job carrying only a BriefAmendmentPending hold.
    let (brief_1, brief_digest_1) = sample_work_brief(
        "brf_obs_01",
        BINDING_A,
        THREAD_A,
        project_base.clone(),
        WorkBriefKind::NewJob,
        "Initial held planning brief",
    );
    let job_1 = CoworkerJobRecord {
        job_id: "job_obs_000000000000000000000001".to_string(),
        job_revision: 2,
        create_request_key: "req_obs_job_01".to_string(),
        binding_id: BINDING_A.to_string(),
        thread_uuid: THREAD_A.to_string(),
        phase: CoworkerJobPhase::Held,
        work_brief: brief_1,
        work_brief_digest: brief_digest_1,
        supersedes_job_id: None,
        planning_result: None,
        approval_linkage: None,
        tasks: Vec::new(),
        holds: vec![sample_amendment_hold(
            "hld_obs_amend_01",
            "amend_req_obs_job_02",
            2,
            "Refine initial scope before approval",
        )],
        outbox: Vec::new(),
        combined_candidates: Vec::new(),
    };

    // Step 2: AwaitingApproval effect-free planning job amending job_1, subsequently amended by job_3.
    let (brief_2, brief_digest_2) = sample_work_brief(
        "brf_obs_02",
        BINDING_A,
        THREAD_A,
        project_base.clone(),
        WorkBriefKind::Amendment {
            target_job_id: job_1.job_id.clone(),
            target_job_revision: 1,
            amendment_reason: "Refine initial scope before approval".to_string(),
        },
        "Intermediate superseded awaiting-approval plan",
    );
    let job_2_id = "job_obs_000000000000000000000002";
    let (res_2, pr_2) = sample_completed_orchestrator_reservation_and_result(
        BINDING_A,
        THREAD_A,
        grant_id,
        job_2_id,
        "obs_02",
        &brief_digest_2,
        &project_base,
        &planning_policy_digest,
        "plan_obs_02",
        "run_00000000-0000-4000-8000-000000000202",
    );
    let job_2 = CoworkerJobRecord {
        job_id: job_2_id.to_string(),
        job_revision: 3,
        create_request_key: "req_obs_job_02".to_string(),
        binding_id: BINDING_A.to_string(),
        thread_uuid: THREAD_A.to_string(),
        phase: CoworkerJobPhase::AwaitingApproval,
        work_brief: brief_2,
        work_brief_digest: brief_digest_2,
        supersedes_job_id: Some(job_1.job_id.clone()),
        planning_result: Some(pr_2),
        approval_linkage: None,
        tasks: Vec::new(),
        holds: vec![sample_amendment_hold(
            "hld_obs_amend_02",
            "amend_req_obs_job_03",
            3,
            "Supersede intermediate plan with final slice",
        )],
        outbox: Vec::new(),
        combined_candidates: Vec::new(),
    };

    // Step 3: Active head amendment job awaiting human approval.
    let (brief_3, brief_digest_3) = sample_work_brief(
        "brf_obs_03",
        BINDING_A,
        THREAD_A,
        project_base.clone(),
        WorkBriefKind::Amendment {
            target_job_id: job_2.job_id.clone(),
            target_job_revision: 2,
            amendment_reason: "Supersede intermediate plan with final slice".to_string(),
        },
        "Current head plan awaiting approval",
    );
    let job_3_id = "job_obs_000000000000000000000003";
    let (res_3, pr_3) = sample_completed_orchestrator_reservation_and_result(
        BINDING_A,
        THREAD_A,
        grant_id,
        job_3_id,
        "obs_03",
        &brief_digest_3,
        &project_base,
        &planning_policy_digest,
        "plan_obs_03",
        "run_00000000-0000-4000-8000-000000000203",
    );
    let job_3 = CoworkerJobRecord {
        job_id: job_3_id.to_string(),
        job_revision: 2,
        create_request_key: "req_obs_job_03".to_string(),
        binding_id: BINDING_A.to_string(),
        thread_uuid: THREAD_A.to_string(),
        phase: CoworkerJobPhase::AwaitingApproval,
        work_brief: brief_3,
        work_brief_digest: brief_digest_3,
        supersedes_job_id: Some(job_2.job_id.clone()),
        planning_result: Some(pr_3),
        approval_linkage: None,
        tasks: Vec::new(),
        holds: Vec::new(),
        outbox: Vec::new(),
        combined_candidates: Vec::new(),
    };

    // Unretired independent planning approval in the same ledger must keep NeedsApproval.
    let (brief_4, brief_digest_4) = sample_work_brief(
        "brf_obs_04",
        BINDING_A,
        THREAD_A,
        project_base.clone(),
        WorkBriefKind::NewJob,
        "Older unretired independent plan without successor",
    );
    let job_4_id = "job_obs_000000000000000000000004";
    let (res_4, pr_4) = sample_completed_orchestrator_reservation_and_result(
        BINDING_A,
        THREAD_A,
        grant_id,
        job_4_id,
        "obs_04",
        &brief_digest_4,
        &project_base,
        &planning_policy_digest,
        "plan_obs_04",
        "run_00000000-0000-4000-8000-000000000204",
    );
    let job_4 = CoworkerJobRecord {
        job_id: job_4_id.to_string(),
        job_revision: 2,
        create_request_key: "req_obs_job_04".to_string(),
        binding_id: BINDING_A.to_string(),
        thread_uuid: THREAD_A.to_string(),
        phase: CoworkerJobPhase::AwaitingApproval,
        work_brief: brief_4,
        work_brief_digest: brief_digest_4,
        supersedes_job_id: None,
        planning_result: Some(pr_4),
        approval_linkage: None,
        tasks: Vec::new(),
        holds: Vec::new(),
        outbox: Vec::new(),
        combined_candidates: Vec::new(),
    };

    let planning_grant = PlanningAdviceGrant {
        grant_id: grant_id.to_string(),
        create_request_key: "req_grant_obs_lineage".to_string(),
        binding_id: BINDING_A.to_string(),
        thread_uuid: THREAD_A.to_string(),
        project_base,
        profile_id: "profile-default".to_string(),
        registry_revision: 1,
        policy: planning_policy,
        policy_digest: planning_policy_digest,
        capabilities: PlanningReadOnlyCapabilities::strict_read_only(),
        budget: PlanningCallBudget {
            max_total_calls: 8,
            max_orchestrator_calls: 8,
            max_architect_calls: 2,
            max_timeout_ms_per_call: DEFAULT_TIMEOUT_MS,
            max_output_bytes_per_call: DEFAULT_MAX_STDOUT_BYTES,
        },
        usage: PlanningGrantUsage {
            reserved_total_calls: 3,
            completed_calls: 3,
            failed_calls: 0,
            unknown_effect_calls: 0,
            refused_before_effect_calls: 0,
            reserved_orchestrator_calls: 3,
            reserved_architect_calls: 0,
        },
        status: PlanningGrantStatus::Active,
        created_at_ms: 1_700_000_040_000,
        expires_at_ms: 1_700_003_640_000,
        revoked_at_ms: None,
        revoked_reason: None,
        operator_attested: true,
        native_per_call_identity_verified: false,
        scoped_profile_dependency_proof: None,
        reservations: vec![res_2, res_3, res_4],
    };
    planning_grant.validate().expect("valid planning grant");

    let workflow_store = CoworkerWorkflowStore::new(&plans_dir).expect("workflow store");
    let ledger = CoworkerBindingLedger {
        schema_version: 1,
        binding_id: BINDING_A.to_string(),
        thread_uuid: Some(THREAD_A.to_string()),
        ledger_revision: 4,
        planning_grants: vec![planning_grant],
        architect_advice: Vec::new(),
        jobs: vec![job_1.clone(), job_2.clone(), job_3.clone(), job_4.clone()],
        admission_markers: BTreeMap::new(),
    };
    ledger.validate(BINDING_A).expect("valid workflow ledger");
    let ledger_path = workflow_store
        .ledger_path_for_binding(BINDING_A)
        .expect("workflow ledger path");
    write_private_json(&ledger_path, &ledger);

    let mut entries_before: Vec<_> = fs::read_dir(&plans_dir)
        .expect("read plans_dir before")
        .map(|e| e.expect("entry").file_name())
        .collect();
    entries_before.sort();
    let ledger_bytes_before = fs::read(&ledger_path).expect("read ledger bytes before");

    let stores = WorkspaceHistoryStores::new(&plans_dir);
    let read = read_scoped_workspace_job_histories(&chat, &stores)
        .expect("scoped read with obsolete planning lineage");

    let mut entries_after: Vec<_> = fs::read_dir(&plans_dir)
        .expect("read plans_dir after")
        .map(|e| e.expect("entry").file_name())
        .collect();
    entries_after.sort();
    let ledger_bytes_after = fs::read(&ledger_path).expect("read ledger bytes after");
    assert_eq!(
        entries_before, entries_after,
        "read-only history projection must not create lockfiles or sidecars"
    );
    assert_eq!(
        ledger_bytes_before, ledger_bytes_after,
        "read-only history projection must not mutate stored ledgers"
    );
    assert!(!read.degraded);
    assert_eq!(read.rejected_scope_count, 0);
    assert_eq!(read.jobs.len(), 4);

    let find_job = |id: &str| {
        read.jobs
            .iter()
            .find(|j| j.job.job_id == id)
            .expect("projected job exists")
    };

    let h1 = find_job(&job_1.job_id);
    assert_eq!(h1.current_attention, None);
    assert_eq!(h1.job.phase.as_deref(), Some("held"));
    assert_eq!(h1.job.approval_status, "superseded");
    assert_eq!(h1.job.execution_status, "not_started");
    assert_eq!(h1.job.review_state, None);
    assert_eq!(h1.job.state, ObservationJobState::Unknown);
    assert_ne!(h1.job.state, ObservationJobState::Failed);
    assert_ne!(h1.job.state, ObservationJobState::NeedsUser);
    assert_ne!(h1.job.state, ObservationJobState::Completed);
    assert_eq!(h1.acceptance_proof, WorkspaceAcceptanceProof::unknown());
    assert_eq!(h1.result, None);
    assert_eq!(
        h1.links.superseded_by_job_id.as_deref(),
        Some(job_2.job_id.as_str())
    );
    assert_eq!(h1.links.follow_up_job_ids, vec![job_2.job_id.clone()]);

    let h2 = find_job(&job_2.job_id);
    assert_eq!(h2.current_attention, None);
    assert_eq!(h2.job.phase.as_deref(), Some("awaiting_approval"));
    assert_eq!(h2.job.approval_status, "superseded");
    assert_eq!(h2.job.execution_status, "not_started");
    assert_eq!(h2.job.review_state, None);
    assert_eq!(h2.job.state, ObservationJobState::Unknown);
    assert_ne!(h2.job.state, ObservationJobState::Failed);
    assert_ne!(h2.job.state, ObservationJobState::NeedsUser);
    assert_ne!(h2.job.state, ObservationJobState::Completed);
    assert_eq!(h2.acceptance_proof, WorkspaceAcceptanceProof::unknown());
    assert_eq!(h2.result, None);
    assert_eq!(
        h2.links.supersedes_job_id.as_deref(),
        Some(job_1.job_id.as_str())
    );
    assert_eq!(
        h2.links.superseded_by_job_id.as_deref(),
        Some(job_3.job_id.as_str())
    );
    assert_eq!(h2.links.source_plan_id.as_deref(), Some("plan_obs_02"));
    assert_eq!(h2.runs.len(), 1);

    let h3 = find_job(&job_3.job_id);
    assert_eq!(
        h3.current_attention.as_ref().map(|a| a.kind),
        Some(WorkspaceAttentionKind::NeedsApproval)
    );
    assert_eq!(h3.job.state, ObservationJobState::NeedsUser);
    assert_eq!(h3.job.approval_status, "pending_approval");

    let h4 = find_job(&job_4.job_id);
    assert_eq!(
        h4.current_attention.as_ref().map(|a| a.kind),
        Some(WorkspaceAttentionKind::NeedsApproval)
    );
    assert_eq!(h4.job.state, ObservationJobState::NeedsUser);

    let persisted_ledger = workflow_store
        .read_binding_ledger(BINDING_A)
        .expect("re-read stored ledger");
    assert_eq!(persisted_ledger.jobs[0].holds, job_1.holds);
    assert_eq!(persisted_ledger.jobs[1].holds, job_2.holds);
}

#[test]
fn forged_lineage_context_unrelated_jobs_and_scope_or_revision_mismatches_do_not_suppress_attention(
) {
    let tmp = TempDir::new().expect("tempdir");
    let plans_dir = make_private_dir(&tmp.path().join("plans"));
    let auth_dir = make_private_dir(&tmp.path().join("authority"));
    let repo_a = make_private_dir(&tmp.path().join("repo-a"));
    let repo_b = make_private_dir(&tmp.path().join("repo-b"));
    let chat_a =
        WorkspaceChatIdentity::new(SOURCE_A, BINDING_A, THREAD_A, &repo_a.to_string_lossy())
            .expect("chat identity A");

    let base_a = sample_project_base(&repo_a);
    let base_b = sample_project_base(&repo_b);
    let policy = ExecutionPolicy::deny_all("policy-test").expect("policy");
    let policy_digest = policy.digest().expect("policy digest");

    // 1. Forged caller-supplied public WorkspaceJobLineageContext on pure projection must NEVER suppress attention.
    let (forged_brief_await, forged_digest_await) = sample_work_brief(
        "brf_forged_await",
        BINDING_A,
        THREAD_A,
        base_a.clone(),
        WorkBriefKind::NewJob,
        "Awaiting approval tested against forged lineage context",
    );
    let job_forged_await = CoworkerJobRecord {
        job_id: "job_forged_await_01".to_string(),
        job_revision: 1,
        create_request_key: "req_forged_await_01".to_string(),
        binding_id: BINDING_A.to_string(),
        thread_uuid: THREAD_A.to_string(),
        phase: CoworkerJobPhase::AwaitingApproval,
        work_brief: forged_brief_await,
        work_brief_digest: forged_digest_await,
        supersedes_job_id: None,
        planning_result: None,
        approval_linkage: None,
        tasks: Vec::new(),
        holds: Vec::new(),
        outbox: Vec::new(),
        combined_candidates: Vec::new(),
    };
    let (forged_brief_held, forged_digest_held) = sample_work_brief(
        "brf_forged_held",
        BINDING_A,
        THREAD_A,
        base_a.clone(),
        WorkBriefKind::NewJob,
        "Held planning job tested against forged lineage context",
    );
    let job_forged_held = CoworkerJobRecord {
        job_id: "job_forged_held_01".to_string(),
        job_revision: 2,
        create_request_key: "req_forged_held_01".to_string(),
        binding_id: BINDING_A.to_string(),
        thread_uuid: THREAD_A.to_string(),
        phase: CoworkerJobPhase::Held,
        work_brief: forged_brief_held,
        work_brief_digest: forged_digest_held,
        supersedes_job_id: None,
        planning_result: None,
        approval_linkage: None,
        tasks: Vec::new(),
        holds: vec![sample_amendment_hold(
            "hld_forged_01",
            "amend_forged_01",
            2,
            "Claimed amendment hold",
        )],
        outbox: Vec::new(),
        combined_candidates: Vec::new(),
    };
    let forged_ctx = WorkspaceJobLineageContext {
        superseded_by_job_id: Some("job_claimed_successor_99".to_string()),
        follow_up_job_ids: vec!["job_claimed_successor_99".to_string()],
        ..WorkspaceJobLineageContext::default()
    };
    let pure_await =
        project_coworker_job_history(&chat_a, &job_forged_await, None, Some(&forged_ctx))
            .expect("pure await projection");
    assert_eq!(pure_await.job.approval_status, "superseded");
    assert_eq!(pure_await.job.state, ObservationJobState::NeedsUser);
    assert_eq!(
        pure_await.current_attention.as_ref().map(|a| a.kind),
        Some(WorkspaceAttentionKind::NeedsApproval)
    );
    let pure_held =
        project_coworker_job_history(&chat_a, &job_forged_held, None, Some(&forged_ctx))
            .expect("pure held projection");
    assert_eq!(pure_held.job.state, ObservationJobState::NeedsUser);
    assert_eq!(
        pure_held.current_attention.as_ref().map(|a| a.kind),
        Some(WorkspaceAttentionKind::OnHold)
    );

    // 2. Store-backed checks: unrelated NewJob, revision mismatch, cross-repo successor, and missing-approval successor.
    let (b_unrel_old, d_unrel_old) = sample_work_brief(
        "brf_unrel_old",
        BINDING_A,
        THREAD_A,
        base_a.clone(),
        WorkBriefKind::NewJob,
        "Old planning job followed only by unrelated NewJob",
    );
    let job_unrel_old = CoworkerJobRecord {
        job_id: "job_unrel_old_01".to_string(),
        job_revision: 1,
        create_request_key: "req_unrel_old_01".to_string(),
        binding_id: BINDING_A.to_string(),
        thread_uuid: THREAD_A.to_string(),
        phase: CoworkerJobPhase::AwaitingApproval,
        work_brief: b_unrel_old,
        work_brief_digest: d_unrel_old,
        supersedes_job_id: None,
        planning_result: None,
        approval_linkage: None,
        tasks: Vec::new(),
        holds: Vec::new(),
        outbox: Vec::new(),
        combined_candidates: Vec::new(),
    };
    let (b_unrel_new, d_unrel_new) = sample_work_brief(
        "brf_unrel_new",
        BINDING_A,
        THREAD_A,
        base_a.clone(),
        WorkBriefKind::NewJob,
        "Newer independent job in same scope",
    );
    let job_unrel_new = CoworkerJobRecord {
        job_id: "job_unrel_new_02".to_string(),
        job_revision: 1,
        create_request_key: "req_unrel_new_02".to_string(),
        binding_id: BINDING_A.to_string(),
        thread_uuid: THREAD_A.to_string(),
        phase: CoworkerJobPhase::Planning,
        work_brief: b_unrel_new,
        work_brief_digest: d_unrel_new,
        supersedes_job_id: None,
        planning_result: None,
        approval_linkage: None,
        tasks: Vec::new(),
        holds: Vec::new(),
        outbox: Vec::new(),
        combined_candidates: Vec::new(),
    };

    // target_job_revision > old_job.job_revision mismatch
    let (b_rev_old, d_rev_old) = sample_work_brief(
        "brf_rev_old",
        BINDING_A,
        THREAD_A,
        base_a.clone(),
        WorkBriefKind::NewJob,
        "Old planning job with revision 1",
    );
    let job_rev_old = CoworkerJobRecord {
        job_id: "job_rev_old_01".to_string(),
        job_revision: 1,
        create_request_key: "req_rev_old_01".to_string(),
        binding_id: BINDING_A.to_string(),
        thread_uuid: THREAD_A.to_string(),
        phase: CoworkerJobPhase::AwaitingApproval,
        work_brief: b_rev_old,
        work_brief_digest: d_rev_old,
        supersedes_job_id: None,
        planning_result: None,
        approval_linkage: None,
        tasks: Vec::new(),
        holds: Vec::new(),
        outbox: Vec::new(),
        combined_candidates: Vec::new(),
    };
    let (b_rev_succ, d_rev_succ) = sample_work_brief(
        "brf_rev_succ",
        BINDING_A,
        THREAD_A,
        base_a.clone(),
        WorkBriefKind::Amendment {
            target_job_id: job_rev_old.job_id.clone(),
            target_job_revision: 9,
            amendment_reason: "Impossible future target revision".to_string(),
        },
        "Successor claiming future target_job_revision",
    );
    let job_rev_succ = CoworkerJobRecord {
        job_id: "job_rev_succ_02".to_string(),
        job_revision: 1,
        create_request_key: "req_rev_succ_02".to_string(),
        binding_id: BINDING_A.to_string(),
        thread_uuid: THREAD_A.to_string(),
        phase: CoworkerJobPhase::Planning,
        work_brief: b_rev_succ,
        work_brief_digest: d_rev_succ,
        supersedes_job_id: Some(job_rev_old.job_id.clone()),
        planning_result: None,
        approval_linkage: None,
        tasks: Vec::new(),
        holds: Vec::new(),
        outbox: Vec::new(),
        combined_candidates: Vec::new(),
    };

    // Cross-repository successor (repo_b) targeting repo_a old job
    let (b_repo_old, d_repo_old) = sample_work_brief(
        "brf_repo_old",
        BINDING_A,
        THREAD_A,
        base_a.clone(),
        WorkBriefKind::NewJob,
        "Held job in repo_a targeted by foreign-repo amendment",
    );
    let job_repo_old = CoworkerJobRecord {
        job_id: "job_repo_old_01".to_string(),
        job_revision: 2,
        create_request_key: "req_repo_old_01".to_string(),
        binding_id: BINDING_A.to_string(),
        thread_uuid: THREAD_A.to_string(),
        phase: CoworkerJobPhase::Held,
        work_brief: b_repo_old,
        work_brief_digest: d_repo_old,
        supersedes_job_id: None,
        planning_result: None,
        approval_linkage: None,
        tasks: Vec::new(),
        holds: vec![sample_amendment_hold(
            "hld_repo_old_01",
            "amend_repo_old_01",
            2,
            "Pending amendment",
        )],
        outbox: Vec::new(),
        combined_candidates: Vec::new(),
    };
    let (b_repo_succ, d_repo_succ) = sample_work_brief(
        "brf_repo_succ",
        BINDING_A,
        THREAD_A,
        base_b,
        WorkBriefKind::Amendment {
            target_job_id: job_repo_old.job_id.clone(),
            target_job_revision: 1,
            amendment_reason: "Mismatched canonical project scope".to_string(),
        },
        "Foreign repository successor",
    );
    let job_repo_succ = CoworkerJobRecord {
        job_id: "job_repo_succ_02".to_string(),
        job_revision: 1,
        create_request_key: "req_repo_succ_02".to_string(),
        binding_id: BINDING_A.to_string(),
        thread_uuid: THREAD_A.to_string(),
        phase: CoworkerJobPhase::Planning,
        work_brief: b_repo_succ,
        work_brief_digest: d_repo_succ,
        supersedes_job_id: Some(job_repo_old.job_id.clone()),
        planning_result: None,
        approval_linkage: None,
        tasks: Vec::new(),
        holds: Vec::new(),
        outbox: Vec::new(),
        combined_candidates: Vec::new(),
    };

    // Successor with unverified/missing approval_id in configured AuthorityStore must be rejected before supersession.
    let (b_auth_old, d_auth_old) = sample_work_brief(
        "brf_auth_old",
        BINDING_A,
        THREAD_A,
        base_a.clone(),
        WorkBriefKind::NewJob,
        "Old planning job targeted by successor with missing approval",
    );
    let job_auth_old = CoworkerJobRecord {
        job_id: "job_auth_old_01".to_string(),
        job_revision: 1,
        create_request_key: "req_auth_old_01".to_string(),
        binding_id: BINDING_A.to_string(),
        thread_uuid: THREAD_A.to_string(),
        phase: CoworkerJobPhase::AwaitingApproval,
        work_brief: b_auth_old,
        work_brief_digest: d_auth_old,
        supersedes_job_id: None,
        planning_result: None,
        approval_linkage: None,
        tasks: Vec::new(),
        holds: Vec::new(),
        outbox: Vec::new(),
        combined_candidates: Vec::new(),
    };
    let (b_auth_succ, d_auth_succ) = sample_work_brief(
        "brf_auth_succ",
        BINDING_A,
        THREAD_A,
        base_a.clone(),
        WorkBriefKind::Amendment {
            target_job_id: job_auth_old.job_id.clone(),
            target_job_revision: 1,
            amendment_reason: "Successor claiming missing approval".to_string(),
        },
        "Successor with missing authority record",
    );
    let mut missing_linkage = sample_approval_linkage(
        base_a,
        &policy_digest,
        vec!["task-missing-auth-01".to_string()],
    );
    missing_linkage.plan_id = "plan_missing_auth_02".to_string();
    let job_auth_succ = CoworkerJobRecord {
        job_id: "job_auth_succ_02".to_string(),
        job_revision: 2,
        create_request_key: "req_auth_succ_02".to_string(),
        binding_id: BINDING_A.to_string(),
        thread_uuid: THREAD_A.to_string(),
        phase: CoworkerJobPhase::Executing,
        work_brief: b_auth_succ,
        work_brief_digest: d_auth_succ,
        supersedes_job_id: Some(job_auth_old.job_id.clone()),
        planning_result: None,
        approval_linkage: Some(missing_linkage),
        tasks: Vec::new(),
        holds: Vec::new(),
        outbox: Vec::new(),
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
        jobs: vec![
            job_unrel_old.clone(),
            job_unrel_new,
            job_rev_old.clone(),
            job_rev_succ,
            job_repo_old.clone(),
            job_repo_succ,
            job_auth_old.clone(),
            job_auth_succ,
        ],
        admission_markers: BTreeMap::new(),
    };
    ledger.validate(BINDING_A).expect("valid ledger");
    write_private_json(
        &workflow_store
            .ledger_path_for_binding(BINDING_A)
            .expect("ledger path"),
        &ledger,
    );

    let stores = WorkspaceHistoryStores::new(&plans_dir).with_authority_dir(&auth_dir);
    let read = read_scoped_workspace_job_histories(&chat_a, &stores).expect("scoped read");
    assert_eq!(read.rejected_scope_count, 2);

    let find_job = |id: &str| {
        read.jobs
            .iter()
            .find(|j| j.job.job_id == id)
            .expect("projected job exists")
    };

    let h_unrel = find_job(&job_unrel_old.job_id);
    assert_eq!(
        h_unrel.current_attention.as_ref().map(|a| a.kind),
        Some(WorkspaceAttentionKind::NeedsApproval)
    );
    assert_eq!(h_unrel.links.superseded_by_job_id, None);

    let h_rev = find_job(&job_rev_old.job_id);
    assert_eq!(
        h_rev.current_attention.as_ref().map(|a| a.kind),
        Some(WorkspaceAttentionKind::NeedsApproval)
    );
    assert_eq!(h_rev.links.superseded_by_job_id, None);

    let h_repo = find_job(&job_repo_old.job_id);
    assert_eq!(
        h_repo.current_attention.as_ref().map(|a| a.kind),
        Some(WorkspaceAttentionKind::OnHold)
    );
    assert_eq!(h_repo.links.superseded_by_job_id, None);

    let h_auth = find_job(&job_auth_old.job_id);
    assert_eq!(
        h_auth.current_attention.as_ref().map(|a| a.kind),
        Some(WorkspaceAttentionKind::NeedsApproval)
    );
    assert_eq!(h_auth.links.superseded_by_job_id, None);
}

#[test]
fn non_amendment_holds_execution_linkage_runtime_degradation_and_ambiguous_reservations_preserve_attention(
) {
    let tmp = TempDir::new().expect("tempdir");
    let plans_dir = make_private_dir(&tmp.path().join("plans"));
    let repo_dir = make_private_dir(&tmp.path().join("repo-non-effect-free"));
    let chat =
        WorkspaceChatIdentity::new(SOURCE_A, BINDING_A, THREAD_A, &repo_dir.to_string_lossy())
            .expect("chat identity");

    let base = sample_project_base(&repo_dir);
    let policy = ExecutionPolicy::deny_all("policy-test").expect("policy");
    let policy_digest = policy.digest().expect("policy digest");
    let planning_policy = sample_planning_policy();
    let planning_policy_digest = planning_policy.digest().expect("planning policy digest");

    // 1. Extra ConversationClarificationPending hold alongside BriefAmendmentPending -> retains NeedsUserInput.
    let (b_clar_old, d_clar_old) = sample_work_brief(
        "brf_clar_old",
        BINDING_A,
        THREAD_A,
        base.clone(),
        WorkBriefKind::NewJob,
        "Held job with active user clarification hold",
    );
    let job_clar_old = CoworkerJobRecord {
        job_id: "job_clar_old_01".to_string(),
        job_revision: 3,
        create_request_key: "req_clar_old_01".to_string(),
        binding_id: BINDING_A.to_string(),
        thread_uuid: THREAD_A.to_string(),
        phase: CoworkerJobPhase::Held,
        work_brief: b_clar_old,
        work_brief_digest: d_clar_old,
        supersedes_job_id: None,
        planning_result: None,
        approval_linkage: None,
        tasks: Vec::new(),
        holds: vec![
            sample_amendment_hold("hld_clar_amend", "amend_clar_01", 2, "Amended"),
            WorkflowHoldRecord {
                hold_id: "hld_clar_user".to_string(),
                request_key: "req_clar_hold".to_string(),
                reason: WorkflowHoldReason::ConversationClarificationPending,
                detail: "Confirm migration target before continuing".to_string(),
                directly_affected_task_ids: Vec::new(),
                affected_write_paths: Vec::new(),
                affected_interfaces: Vec::new(),
                holds_entire_job: true,
                active: true,
                created_at_job_revision: 3,
                resolved_at_job_revision: None,
                resolution_note: None,
            },
        ],
        outbox: Vec::new(),
        combined_candidates: Vec::new(),
    };
    let (b_clar_succ, d_clar_succ) = sample_work_brief(
        "brf_clar_succ",
        BINDING_A,
        THREAD_A,
        base.clone(),
        WorkBriefKind::Amendment {
            target_job_id: job_clar_old.job_id.clone(),
            target_job_revision: 3,
            amendment_reason: "Amended while clarification still active".to_string(),
        },
        "Successor to clarification-held job",
    );
    let job_clar_succ = CoworkerJobRecord {
        job_id: "job_clar_succ_02".to_string(),
        job_revision: 1,
        create_request_key: "req_clar_succ_02".to_string(),
        binding_id: BINDING_A.to_string(),
        thread_uuid: THREAD_A.to_string(),
        phase: CoworkerJobPhase::Planning,
        work_brief: b_clar_succ,
        work_brief_digest: d_clar_succ,
        supersedes_job_id: Some(job_clar_old.job_id.clone()),
        planning_result: None,
        approval_linkage: None,
        tasks: Vec::new(),
        holds: Vec::new(),
        outbox: Vec::new(),
        combined_candidates: Vec::new(),
    };

    // 2. Held phase with zero active holds -> lacks BriefAmendmentPending proof, retains OnHold.
    let (b_nohold_old, d_nohold_old) = sample_work_brief(
        "brf_nohold_old",
        BINDING_A,
        THREAD_A,
        base.clone(),
        WorkBriefKind::NewJob,
        "Held job without active BriefAmendmentPending hold",
    );
    let job_nohold_old = CoworkerJobRecord {
        job_id: "job_nohold_old_01".to_string(),
        job_revision: 1,
        create_request_key: "req_nohold_old_01".to_string(),
        binding_id: BINDING_A.to_string(),
        thread_uuid: THREAD_A.to_string(),
        phase: CoworkerJobPhase::Held,
        work_brief: b_nohold_old,
        work_brief_digest: d_nohold_old,
        supersedes_job_id: None,
        planning_result: None,
        approval_linkage: None,
        tasks: Vec::new(),
        holds: Vec::new(),
        outbox: Vec::new(),
        combined_candidates: Vec::new(),
    };
    let (b_nohold_succ, d_nohold_succ) = sample_work_brief(
        "brf_nohold_succ",
        BINDING_A,
        THREAD_A,
        base.clone(),
        WorkBriefKind::Amendment {
            target_job_id: job_nohold_old.job_id.clone(),
            target_job_revision: 1,
            amendment_reason: "Amend held job without hold record".to_string(),
        },
        "Successor to zero-hold held job",
    );
    let job_nohold_succ = CoworkerJobRecord {
        job_id: "job_nohold_succ_02".to_string(),
        job_revision: 1,
        create_request_key: "req_nohold_succ_02".to_string(),
        binding_id: BINDING_A.to_string(),
        thread_uuid: THREAD_A.to_string(),
        phase: CoworkerJobPhase::Planning,
        work_brief: b_nohold_succ,
        work_brief_digest: d_nohold_succ,
        supersedes_job_id: Some(job_nohold_old.job_id.clone()),
        planning_result: None,
        approval_linkage: None,
        tasks: Vec::new(),
        holds: Vec::new(),
        outbox: Vec::new(),
        combined_candidates: Vec::new(),
    };

    // 3. Approved job with task/attempt/controller runtime stop -> retains attention despite amendment successor.
    let (b_exec_old, d_exec_old) = sample_work_brief(
        "brf_exec_old",
        BINDING_A,
        THREAD_A,
        base.clone(),
        WorkBriefKind::NewJob,
        "Approved job with controller stop fence",
    );
    let mut exec_linkage = sample_approval_linkage(
        base.clone(),
        &policy_digest,
        vec!["task-exec-01".to_string()],
    );
    exec_linkage.plan_id = "plan_exec_old_01".to_string();
    let job_exec_old = CoworkerJobRecord {
        job_id: "job_exec_old_01".to_string(),
        job_revision: 3,
        create_request_key: "req_exec_old_01".to_string(),
        binding_id: BINDING_A.to_string(),
        thread_uuid: THREAD_A.to_string(),
        phase: CoworkerJobPhase::Executing,
        work_brief: b_exec_old,
        work_brief_digest: d_exec_old,
        supersedes_job_id: None,
        planning_result: None,
        approval_linkage: Some(exec_linkage),
        tasks: vec![CoworkerTaskRecord {
            task_id: "task-exec-01".to_string(),
            task_digest: HEX64_D.to_string(),
            role: WorkflowRole::Coder,
            difficulty: Difficulty::Hard,
            selected_choice: sample_coder_choice(),
            depends_on: Vec::new(),
            write_paths: vec!["src/lib.rs".to_string()],
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
        }],
        holds: Vec::new(),
        outbox: Vec::new(),
        combined_candidates: Vec::new(),
    };
    let (b_exec_succ, d_exec_succ) = sample_work_brief(
        "brf_exec_succ",
        BINDING_A,
        THREAD_A,
        base.clone(),
        WorkBriefKind::Amendment {
            target_job_id: job_exec_old.job_id.clone(),
            target_job_revision: 3,
            amendment_reason: "Follow-up amendment after approval".to_string(),
        },
        "Successor to approved job",
    );
    let job_exec_succ = CoworkerJobRecord {
        job_id: "job_exec_succ_02".to_string(),
        job_revision: 1,
        create_request_key: "req_exec_succ_02".to_string(),
        binding_id: BINDING_A.to_string(),
        thread_uuid: THREAD_A.to_string(),
        phase: CoworkerJobPhase::Planning,
        work_brief: b_exec_succ,
        work_brief_digest: d_exec_succ,
        supersedes_job_id: Some(job_exec_old.job_id.clone()),
        planning_result: None,
        approval_linkage: None,
        tasks: Vec::new(),
        holds: Vec::new(),
        outbox: Vec::new(),
        combined_candidates: Vec::new(),
    };

    // 4. Non-causal manual BriefAmendmentPending hold (request_key does not match amend_<succ>) -> retains OnHold.
    let (b_uncausal_old, d_uncausal_old) = sample_work_brief(
        "brf_uncausal_old",
        BINDING_A,
        THREAD_A,
        base.clone(),
        WorkBriefKind::NewJob,
        "Held job with non-causal BriefAmendmentPending hold",
    );
    let job_uncausal_old = CoworkerJobRecord {
        job_id: "job_uncausal_old_01".to_string(),
        job_revision: 2,
        create_request_key: "req_uncausal_old_01".to_string(),
        binding_id: BINDING_A.to_string(),
        thread_uuid: THREAD_A.to_string(),
        phase: CoworkerJobPhase::Held,
        work_brief: b_uncausal_old,
        work_brief_digest: d_uncausal_old,
        supersedes_job_id: None,
        planning_result: None,
        approval_linkage: None,
        tasks: Vec::new(),
        holds: vec![sample_amendment_hold(
            "hld_uncausal_01",
            "req_manual_hold_unrelated",
            2,
            "Manual hold unrelated to successor request_key",
        )],
        outbox: Vec::new(),
        combined_candidates: Vec::new(),
    };
    let (b_uncausal_succ, d_uncausal_succ) = sample_work_brief(
        "brf_uncausal_succ",
        BINDING_A,
        THREAD_A,
        base.clone(),
        WorkBriefKind::Amendment {
            target_job_id: job_uncausal_old.job_id.clone(),
            target_job_revision: 1,
            amendment_reason: "Amendment alongside unrelated manual hold".to_string(),
        },
        "Successor to job with non-causal hold",
    );
    let job_uncausal_succ = CoworkerJobRecord {
        job_id: "job_uncausal_succ_02".to_string(),
        job_revision: 1,
        create_request_key: "req_uncausal_succ_02".to_string(),
        binding_id: BINDING_A.to_string(),
        thread_uuid: THREAD_A.to_string(),
        phase: CoworkerJobPhase::Planning,
        work_brief: b_uncausal_succ,
        work_brief_digest: d_uncausal_succ,
        supersedes_job_id: Some(job_uncausal_old.job_id.clone()),
        planning_result: None,
        approval_linkage: None,
        tasks: Vec::new(),
        holds: Vec::new(),
        outbox: Vec::new(),
        combined_candidates: Vec::new(),
    };

    // 5. Clean superseded planning job in BINDING_A (with an unrelated-base reservation on the same grant
    //    to prove base isolation, then used to verify degraded controller runtime refuses retirement).
    let (b_clean_old, d_clean_old) = sample_work_brief(
        "brf_clean_old",
        BINDING_A,
        THREAD_A,
        base.clone(),
        WorkBriefKind::NewJob,
        "Clean superseded planning job",
    );
    let job_clean_old = CoworkerJobRecord {
        job_id: "job_clean_old_01".to_string(),
        job_revision: 2,
        create_request_key: "req_clean_old_01".to_string(),
        binding_id: BINDING_A.to_string(),
        thread_uuid: THREAD_A.to_string(),
        phase: CoworkerJobPhase::Held,
        work_brief: b_clean_old,
        work_brief_digest: d_clean_old.clone(),
        supersedes_job_id: None,
        planning_result: None,
        approval_linkage: None,
        tasks: Vec::new(),
        holds: vec![sample_amendment_hold(
            "hld_clean_01",
            "amend_req_clean_succ_02",
            2,
            "Clean amendment",
        )],
        outbox: Vec::new(),
        combined_candidates: Vec::new(),
    };
    let (b_clean_succ, d_clean_succ) = sample_work_brief(
        "brf_clean_succ",
        BINDING_A,
        THREAD_A,
        base.clone(),
        WorkBriefKind::Amendment {
            target_job_id: job_clean_old.job_id.clone(),
            target_job_revision: 1,
            amendment_reason: "Clean amendment".to_string(),
        },
        "Successor to clean planning job",
    );
    let job_clean_succ = CoworkerJobRecord {
        job_id: "job_clean_succ_02".to_string(),
        job_revision: 1,
        create_request_key: "req_clean_succ_02".to_string(),
        binding_id: BINDING_A.to_string(),
        thread_uuid: THREAD_A.to_string(),
        phase: CoworkerJobPhase::Planning,
        work_brief: b_clean_succ,
        work_brief_digest: d_clean_succ,
        supersedes_job_id: Some(job_clean_old.job_id.clone()),
        planning_result: None,
        approval_linkage: None,
        tasks: Vec::new(),
        holds: Vec::new(),
        outbox: Vec::new(),
        combined_candidates: Vec::new(),
    };

    let unrelated_call_base = CanonicalProjectBaseRef {
        repository_path: repo_dir.to_string_lossy().into_owned(),
        base_commit: SHA40_HEAD_1.to_string(),
        base_tree_sha: SHA40_TREE_1.to_string(),
    };
    let grant_other_base_id = "grt_other_base_0123456789abcdef";
    let other_base_choice = sample_orchestrator_choice();
    let other_base_digest = production_orchestrator_context_digest(
        BINDING_A,
        THREAD_A,
        grant_other_base_id,
        "profile-default",
        1,
        &planning_policy_digest,
        "job_other_base_99",
        &d_clean_old,
        &unrelated_call_base,
        None,
        0,
        &other_base_choice,
    );
    let grant_other_base = PlanningAdviceGrant {
        grant_id: grant_other_base_id.to_string(),
        create_request_key: "req_grant_other_base".to_string(),
        binding_id: BINDING_A.to_string(),
        thread_uuid: THREAD_A.to_string(),
        project_base: base.clone(),
        profile_id: "profile-default".to_string(),
        registry_revision: 1,
        policy: planning_policy.clone(),
        policy_digest: planning_policy_digest.clone(),
        capabilities: PlanningReadOnlyCapabilities::strict_read_only(),
        budget: PlanningCallBudget {
            max_total_calls: 4,
            max_orchestrator_calls: 4,
            max_architect_calls: 2,
            max_timeout_ms_per_call: DEFAULT_TIMEOUT_MS,
            max_output_bytes_per_call: DEFAULT_MAX_STDOUT_BYTES,
        },
        usage: PlanningGrantUsage {
            reserved_total_calls: 1,
            completed_calls: 0,
            failed_calls: 0,
            unknown_effect_calls: 0,
            refused_before_effect_calls: 0,
            reserved_orchestrator_calls: 1,
            reserved_architect_calls: 0,
        },
        status: PlanningGrantStatus::Active,
        created_at_ms: 1_700_000_040_000,
        expires_at_ms: 1_700_003_640_000,
        revoked_at_ms: None,
        revoked_reason: None,
        operator_attested: true,
        native_per_call_identity_verified: false,
        scoped_profile_dependency_proof: None,
        reservations: vec![PlanningCallReservation {
            reservation_id: "rsv_other_base_01".to_string(),
            request_key: "req_rsv_other_base_01".to_string(),
            role: WorkflowRole::Orchestrator,
            difficulty: Some(Difficulty::Hard),
            selected_choice: other_base_choice,
            context_digest: other_base_digest,
            call_project_base: Some(unrelated_call_base),
            status: PlanningReservationStatus::ReservedBeforeEffect,
            reserved_at_ms: 1_700_000_045_000,
            updated_at_ms: 1_700_000_045_000,
            run_id: None,
            report_artifact_id: None,
            output_digest: None,
            diagnostic: None,
        }],
    };
    grant_other_base
        .validate()
        .expect("valid grant with unrelated call_project_base reservation");

    let workflow_store = CoworkerWorkflowStore::new(&plans_dir).expect("workflow store");
    let ledger = CoworkerBindingLedger {
        schema_version: 1,
        binding_id: BINDING_A.to_string(),
        thread_uuid: Some(THREAD_A.to_string()),
        ledger_revision: 2,
        planning_grants: vec![grant_other_base],
        architect_advice: Vec::new(),
        jobs: vec![
            job_clar_old.clone(),
            job_clar_succ,
            job_nohold_old.clone(),
            job_nohold_succ,
            job_exec_old.clone(),
            job_exec_succ,
            job_uncausal_old.clone(),
            job_uncausal_succ,
            job_clean_old.clone(),
            job_clean_succ,
        ],
        admission_markers: BTreeMap::new(),
    };
    ledger.validate(BINDING_A).expect("valid ledger");
    write_private_json(
        &workflow_store
            .ledger_path_for_binding(BINDING_A)
            .expect("ledger path"),
        &ledger,
    );

    let mut exec_rt = sample_runtime_state(&job_exec_old, policy, &policy_digest);
    exec_rt.stop_requested = true;
    exec_rt.stop_or_block_reason = Some("Operator requested stop".to_string());
    let mut rt_map = BTreeMap::new();
    rt_map.insert(job_exec_old.job_id.clone(), exec_rt);
    write_controller_ledger_file(&plans_dir, BINDING_A, rt_map);

    // 6. Production-shaped composite-hash Orchestrator reservations (with no planning_result or
    //    architect_advice) in BOTH ReservedBeforeEffect and EffectUnknown states, followed by real
    //    causal amendment holds -> must conservatively retain OnHold attention without mutation.
    for (binding_id, status_label, status, unknown_calls) in [
        (
            "bnd_hist_ambig_reserved",
            "reserved_before_effect",
            PlanningReservationStatus::ReservedBeforeEffect,
            0u32,
        ),
        (
            "bnd_hist_ambig_unknown",
            "effect_unknown",
            PlanningReservationStatus::EffectUnknown,
            1u32,
        ),
    ] {
        let ambig_chat =
            WorkspaceChatIdentity::new(SOURCE_A, binding_id, THREAD_A, &repo_dir.to_string_lossy())
                .expect("ambig chat identity");

        let (b_ambig_old, d_ambig_old) = sample_work_brief(
            &format!("brf_{status_label}_old"),
            binding_id,
            THREAD_A,
            base.clone(),
            WorkBriefKind::NewJob,
            "Planning job interrupted before Orchestrator result and then amended",
        );
        let succ_req_key = format!("req_{status_label}_succ_02");
        let job_ambig_old = CoworkerJobRecord {
            job_id: format!("job_{status_label}_old_01"),
            job_revision: 2,
            create_request_key: format!("req_{status_label}_old_01"),
            binding_id: binding_id.to_string(),
            thread_uuid: THREAD_A.to_string(),
            phase: CoworkerJobPhase::Held,
            work_brief: b_ambig_old,
            work_brief_digest: d_ambig_old.clone(),
            supersedes_job_id: None,
            planning_result: None,
            approval_linkage: None,
            tasks: Vec::new(),
            holds: vec![sample_amendment_hold(
                &format!("hld_{status_label}_01"),
                &format!("amend_{succ_req_key}"),
                2,
                "Real causal amendment hold after interrupted Orchestrator call",
            )],
            outbox: Vec::new(),
            combined_candidates: Vec::new(),
        };
        let (b_ambig_succ, d_ambig_succ) = sample_work_brief(
            &format!("brf_{status_label}_succ"),
            binding_id,
            THREAD_A,
            base.clone(),
            WorkBriefKind::Amendment {
                target_job_id: job_ambig_old.job_id.clone(),
                target_job_revision: 1,
                amendment_reason: "Real causal amendment hold after interrupted Orchestrator call"
                    .to_string(),
            },
            "Amendment successor after interrupted Orchestrator reservation",
        );
        let job_ambig_succ = CoworkerJobRecord {
            job_id: format!("job_{status_label}_succ_02"),
            job_revision: 1,
            create_request_key: succ_req_key,
            binding_id: binding_id.to_string(),
            thread_uuid: THREAD_A.to_string(),
            phase: CoworkerJobPhase::Planning,
            work_brief: b_ambig_succ,
            work_brief_digest: d_ambig_succ,
            supersedes_job_id: Some(job_ambig_old.job_id.clone()),
            planning_result: None,
            approval_linkage: None,
            tasks: Vec::new(),
            holds: Vec::new(),
            outbox: Vec::new(),
            combined_candidates: Vec::new(),
        };

        let grant_id = format!("grt_{status_label}_0123456789ab");
        let choice = sample_orchestrator_choice();
        let composite_digest = production_orchestrator_context_digest(
            binding_id,
            THREAD_A,
            &grant_id,
            "profile-default",
            1,
            &planning_policy_digest,
            &job_ambig_old.job_id,
            &d_ambig_old,
            &base,
            Some("plan_interrupted_draft"),
            2,
            &choice,
        );
        assert_ne!(
            composite_digest, d_ambig_old,
            "production Orchestrator context_digest must be a composite hash, never raw work_brief_digest"
        );

        let grant = PlanningAdviceGrant {
            grant_id,
            create_request_key: format!("req_grant_{status_label}"),
            binding_id: binding_id.to_string(),
            thread_uuid: THREAD_A.to_string(),
            project_base: base.clone(),
            profile_id: "profile-default".to_string(),
            registry_revision: 1,
            policy: planning_policy.clone(),
            policy_digest: planning_policy_digest.clone(),
            capabilities: PlanningReadOnlyCapabilities::strict_read_only(),
            budget: PlanningCallBudget {
                max_total_calls: 4,
                max_orchestrator_calls: 4,
                max_architect_calls: 2,
                max_timeout_ms_per_call: DEFAULT_TIMEOUT_MS,
                max_output_bytes_per_call: DEFAULT_MAX_STDOUT_BYTES,
            },
            usage: PlanningGrantUsage {
                reserved_total_calls: 1,
                completed_calls: 0,
                failed_calls: 0,
                unknown_effect_calls: unknown_calls,
                refused_before_effect_calls: 0,
                reserved_orchestrator_calls: 1,
                reserved_architect_calls: 0,
            },
            status: PlanningGrantStatus::Active,
            created_at_ms: 1_700_000_040_000,
            expires_at_ms: 1_700_003_640_000,
            revoked_at_ms: None,
            revoked_reason: None,
            operator_attested: true,
            native_per_call_identity_verified: false,
            scoped_profile_dependency_proof: None,
            reservations: vec![PlanningCallReservation {
                reservation_id: format!("rsv_{status_label}_01"),
                request_key: format!("req_rsv_{status_label}_01"),
                role: WorkflowRole::Orchestrator,
                difficulty: Some(Difficulty::Hard),
                selected_choice: choice,
                context_digest: composite_digest,
                call_project_base: Some(base.clone()),
                status,
                reserved_at_ms: 1_700_000_045_000,
                updated_at_ms: 1_700_000_046_000,
                run_id: None,
                report_artifact_id: None,
                output_digest: None,
                diagnostic: Some("Interrupted orchestrator call".to_string()),
            }],
        };
        grant
            .validate()
            .expect("valid grant with unresolved reservation");

        let ambig_ledger = CoworkerBindingLedger {
            schema_version: 1,
            binding_id: binding_id.to_string(),
            thread_uuid: Some(THREAD_A.to_string()),
            ledger_revision: 2,
            planning_grants: vec![grant],
            architect_advice: Vec::new(),
            jobs: vec![job_ambig_old.clone(), job_ambig_succ],
            admission_markers: BTreeMap::new(),
        };
        ambig_ledger
            .validate(binding_id)
            .expect("valid ambiguous-reservation ledger");
        let ambig_ledger_path = workflow_store
            .ledger_path_for_binding(binding_id)
            .expect("ambig ledger path");
        write_private_json(&ambig_ledger_path, &ambig_ledger);
        let bytes_before = fs::read(&ambig_ledger_path).expect("read ambig ledger before");

        let stores = WorkspaceHistoryStores::new(&plans_dir);
        let ambig_read = read_scoped_workspace_job_histories(&ambig_chat, &stores)
            .expect("scoped read with unresolved planning reservation");
        let bytes_after = fs::read(&ambig_ledger_path).expect("read ambig ledger after");
        assert_eq!(
            bytes_before, bytes_after,
            "read-only history projection must not resolve or mutate unresolved reservations"
        );

        let h_ambig_old = ambig_read
            .jobs
            .iter()
            .find(|j| j.job.job_id == job_ambig_old.job_id)
            .expect("projected ambiguous old job");
        assert_eq!(
            h_ambig_old.current_attention.as_ref().map(|a| a.kind),
            Some(WorkspaceAttentionKind::OnHold),
            "unresolved {status_label} reservation with composite Orchestrator digest must block retirement"
        );
        assert_eq!(h_ambig_old.job.state, ObservationJobState::NeedsUser);
        assert_eq!(h_ambig_old.job.phase.as_deref(), Some("held"));
    }

    let stores = WorkspaceHistoryStores::new(&plans_dir);
    let read = read_scoped_workspace_job_histories(&chat, &stores).expect("healthy read");
    assert!(!read.degraded);

    fn find_in<'a>(
        jobs: &'a [threadlane_core::observation::WorkspaceJobHistory],
        id: &str,
    ) -> &'a threadlane_core::observation::WorkspaceJobHistory {
        jobs.iter()
            .find(|j| j.job.job_id == id)
            .expect("job exists")
    }

    assert_eq!(
        find_in(&read.jobs, &job_clar_old.job_id)
            .current_attention
            .as_ref()
            .map(|a| a.kind),
        Some(WorkspaceAttentionKind::NeedsUserInput)
    );
    assert_eq!(
        find_in(&read.jobs, &job_nohold_old.job_id)
            .current_attention
            .as_ref()
            .map(|a| a.kind),
        Some(WorkspaceAttentionKind::OnHold)
    );
    assert_eq!(
        find_in(&read.jobs, &job_exec_old.job_id)
            .current_attention
            .as_ref()
            .map(|a| a.kind),
        Some(WorkspaceAttentionKind::BlockedFailure)
    );
    assert_eq!(
        find_in(&read.jobs, &job_uncausal_old.job_id)
            .current_attention
            .as_ref()
            .map(|a| a.kind),
        Some(WorkspaceAttentionKind::OnHold)
    );
    let h_clean = find_in(&read.jobs, &job_clean_old.job_id);
    assert_eq!(h_clean.current_attention, None);
    assert_eq!(h_clean.job.state, ObservationJobState::Unknown);
    assert_ne!(h_clean.job.state, ObservationJobState::Failed);
    assert_eq!(h_clean.job.approval_status, "superseded");
    assert_eq!(h_clean.job.execution_status, "not_started");

    // Corrupt controller runtime ledger -> degraded store must refuse to treat job_clean_old as proven effect-free.
    let policy_store = ControllerPolicyStore::new(&plans_dir).expect("policy store");
    let ctl_ledger_path = policy_store
        .ledger_path_for_binding(BINDING_A)
        .expect("controller ledger path");
    fs::write(&ctl_ledger_path, b"{not valid json").expect("corrupt controller ledger");
    fs::set_permissions(&ctl_ledger_path, fs::Permissions::from_mode(0o600))
        .expect("chmod 0600 corrupt controller ledger");

    let degraded_read =
        read_scoped_workspace_job_histories(&chat, &stores).expect("degraded controller read");
    assert!(degraded_read.degraded);
    let h_clean_degraded = find_in(&degraded_read.jobs, &job_clean_old.job_id);
    assert_eq!(
        h_clean_degraded.current_attention.as_ref().map(|a| a.kind),
        Some(WorkspaceAttentionKind::OnHold),
        "degraded controller runtime store must never prove effect-free retirement"
    );
    assert_eq!(h_clean_degraded.job.state, ObservationJobState::NeedsUser);
    let persisted_clean = workflow_store
        .read_binding_ledger(BINDING_A)
        .expect("re-read clean workflow ledger");
    assert_eq!(persisted_clean.jobs[8].holds, job_clean_old.holds);
}

#[test]
fn preserves_unresolved_effect_attention_across_stop_supersession_and_completed_acceptance_candidates(
) {
    let tmp = TempDir::new().expect("tempdir");
    let plans_dir = make_private_dir(&tmp.path().join("plans"));
    let repo_dir = make_private_dir(&tmp.path().join("repo-unresolved"));
    let chat =
        WorkspaceChatIdentity::new(SOURCE_A, BINDING_A, THREAD_A, &repo_dir.to_string_lossy())
            .expect("chat identity");

    let base = sample_project_base(&repo_dir);
    let policy = ExecutionPolicy::deny_all("policy-test").expect("policy");
    let policy_digest = policy.digest().expect("policy digest");

    let make_task =
        |task_id: &str, stage: CoworkerTaskStage, unresolved_outbox_id: Option<&str>| {
            CoworkerTaskRecord {
                task_id: task_id.to_string(),
                task_digest: HEX64_D.to_string(),
                role: WorkflowRole::Coder,
                difficulty: Difficulty::Hard,
                selected_choice: sample_coder_choice(),
                depends_on: Vec::new(),
                write_paths: vec!["src/lib.rs".to_string()],
                owned_interfaces: Vec::new(),
                consumed_interfaces: Vec::new(),
                stage,
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
                unresolved_outbox_id: unresolved_outbox_id.map(str::to_string),
            }
        };

    // 1. Cancelled job with OutboxDispatchStatus::EffectUnknown must still require UnresolvedEffect reconciliation.
    let (b_cancel, d_cancel) = sample_work_brief(
        "brf_cancel_unknown",
        BINDING_A,
        THREAD_A,
        base.clone(),
        WorkBriefKind::NewJob,
        "Cancelled job with unresolved outbox effect",
    );
    let mut link_cancel = sample_approval_linkage(
        base.clone(),
        &policy_digest,
        vec!["task-cancel-u1".to_string()],
    );
    link_cancel.plan_id = "plan_cancel_u1".to_string();
    let job_cancelled_unknown = CoworkerJobRecord {
        job_id: "job_cancel_unknown_01".to_string(),
        job_revision: 4,
        create_request_key: "req_cancel_unknown_01".to_string(),
        binding_id: BINDING_A.to_string(),
        thread_uuid: THREAD_A.to_string(),
        phase: CoworkerJobPhase::Cancelled,
        work_brief: b_cancel,
        work_brief_digest: d_cancel,
        supersedes_job_id: None,
        planning_result: None,
        approval_linkage: Some(link_cancel),
        tasks: vec![make_task(
            "task-cancel-u1",
            CoworkerTaskStage::EffectUnknown,
            Some("outbox_cancel_u1"),
        )],
        holds: Vec::new(),
        outbox: vec![OutboxDispatchRecord {
            outbox_id: "outbox_cancel_u1".to_string(),
            request_key: "req_outbox_cancel_u1".to_string(),
            kind: OutboxDispatchKind::TaskAttemptDispatch,
            task_id: "task-cancel-u1".to_string(),
            attempt_no: 1,
            role: WorkflowRole::Coder,
            subject_digest: HEX64_D.to_string(),
            status: OutboxDispatchStatus::EffectUnknown,
            reserved_at_job_revision: 2,
            updated_at_job_revision: 3,
            run_id: None,
            worker_id: None,
            receipt_id: None,
            diagnostic: Some("Interrupted dispatch before receipt".to_string()),
        }],
        combined_candidates: Vec::new(),
    };

    // 2. Superseded job with stop_requested AND PendingControllerEffectStatus::EffectUnknown must retain UnresolvedEffect.
    let (b_sup_u, d_sup_u) = sample_work_brief(
        "brf_sup_unknown",
        BINDING_A,
        THREAD_A,
        base.clone(),
        WorkBriefKind::NewJob,
        "Superseded job with controller EffectUnknown",
    );
    let mut link_sup_u = sample_approval_linkage(
        base.clone(),
        &policy_digest,
        vec!["task-sup-u1".to_string()],
    );
    link_sup_u.plan_id = "plan_sup_u1".to_string();
    let job_sup_unknown = CoworkerJobRecord {
        job_id: "job_sup_unknown_01".to_string(),
        job_revision: 4,
        create_request_key: "req_sup_unknown_01".to_string(),
        binding_id: BINDING_A.to_string(),
        thread_uuid: THREAD_A.to_string(),
        phase: CoworkerJobPhase::Held,
        work_brief: b_sup_u,
        work_brief_digest: d_sup_u,
        supersedes_job_id: None,
        planning_result: None,
        approval_linkage: Some(link_sup_u),
        tasks: vec![make_task(
            "task-sup-u1",
            CoworkerTaskStage::EffectUnknown,
            None,
        )],
        holds: vec![sample_amendment_hold(
            "hld_sup_u1",
            "amend_sup_u1",
            4,
            "Amended after interrupted effect",
        )],
        outbox: Vec::new(),
        combined_candidates: Vec::new(),
    };
    let (b_sup_succ, d_sup_succ) = sample_work_brief(
        "brf_sup_succ",
        BINDING_A,
        THREAD_A,
        base.clone(),
        WorkBriefKind::Amendment {
            target_job_id: job_sup_unknown.job_id.clone(),
            target_job_revision: 3,
            amendment_reason: "Amended after interrupted effect".to_string(),
        },
        "Successor after interrupted effect",
    );
    let job_sup_succ = CoworkerJobRecord {
        job_id: "job_sup_succ_02".to_string(),
        job_revision: 1,
        create_request_key: "req_sup_succ_02".to_string(),
        binding_id: BINDING_A.to_string(),
        thread_uuid: THREAD_A.to_string(),
        phase: CoworkerJobPhase::Planning,
        work_brief: b_sup_succ,
        work_brief_digest: d_sup_succ,
        supersedes_job_id: Some(job_sup_unknown.job_id.clone()),
        planning_result: None,
        approval_linkage: None,
        tasks: Vec::new(),
        holds: Vec::new(),
        outbox: Vec::new(),
        combined_candidates: Vec::new(),
    };

    // 3. Latest Completed acceptance job with PendingControllerEffectStatus::EffectUnknown must stay actionable UnresolvedEffect.
    let (b_comp_u, d_comp_u) = sample_work_brief(
        "brf_comp_unknown",
        BINDING_A,
        THREAD_A,
        base.clone(),
        WorkBriefKind::NewJob,
        "Latest acceptance job with EffectUnknown",
    );
    let mut link_comp_u =
        sample_approval_linkage(base, &policy_digest, vec!["task-comp-u1".to_string()]);
    link_comp_u.plan_id = "plan_comp_u1".to_string();
    let job_completed_unknown = CoworkerJobRecord {
        job_id: "job_completed_unknown_03".to_string(),
        job_revision: 5,
        create_request_key: "req_comp_unknown_03".to_string(),
        binding_id: BINDING_A.to_string(),
        thread_uuid: THREAD_A.to_string(),
        phase: CoworkerJobPhase::Completed,
        work_brief: b_comp_u,
        work_brief_digest: d_comp_u,
        supersedes_job_id: None,
        planning_result: None,
        approval_linkage: Some(link_comp_u),
        tasks: vec![make_task(
            "task-comp-u1",
            CoworkerTaskStage::Integrated,
            None,
        )],
        holds: Vec::new(),
        outbox: Vec::new(),
        combined_candidates: Vec::new(),
    };

    let workflow_store = CoworkerWorkflowStore::new(&plans_dir).expect("workflow store");
    let ledger = CoworkerBindingLedger {
        schema_version: 1,
        binding_id: BINDING_A.to_string(),
        thread_uuid: Some(THREAD_A.to_string()),
        ledger_revision: 3,
        planning_grants: Vec::new(),
        architect_advice: Vec::new(),
        jobs: vec![
            job_cancelled_unknown.clone(),
            job_sup_unknown.clone(),
            job_sup_succ,
            job_completed_unknown.clone(),
        ],
        admission_markers: BTreeMap::new(),
    };
    ledger.validate(BINDING_A).expect("valid ledger");
    write_private_json(
        &workflow_store
            .ledger_path_for_binding(BINDING_A)
            .expect("ledger path"),
        &ledger,
    );

    let rt_cancel = sample_runtime_state(&job_cancelled_unknown, policy.clone(), &policy_digest);
    let mut rt_sup = sample_runtime_state(&job_sup_unknown, policy.clone(), &policy_digest);
    rt_sup.stop_requested = true;
    rt_sup.pending_effects.insert(
        "eff_sup_unknown_01".to_string(),
        PendingControllerEffect {
            effect_id: "eff_sup_unknown_01".to_string(),
            request_key: "req_eff_sup_u1".to_string(),
            job_id: job_sup_unknown.job_id.clone(),
            task_id: Some("task-sup-u1".to_string()),
            attempt_no: Some(1),
            kind: PendingControllerEffectKind::CoderDispatch,
            status: PendingControllerEffectStatus::EffectUnknown,
            subject_digest: HEX64_D.to_string(),
            external_correlation_id: None,
            created_at_ms: 1_700_000_200_000,
            updated_at_ms: 1_700_000_205_000,
            diagnostic: Some("Unknown dispatch effect after stop".to_string()),
        },
    );
    let mut rt_comp = sample_runtime_state(&job_completed_unknown, policy, &policy_digest);
    rt_comp.pending_effects.insert(
        "eff_comp_unknown_01".to_string(),
        PendingControllerEffect {
            effect_id: "eff_comp_unknown_01".to_string(),
            request_key: "req_eff_comp_u1".to_string(),
            job_id: job_completed_unknown.job_id.clone(),
            task_id: None,
            attempt_no: None,
            kind: PendingControllerEffectKind::ClosureInvocation,
            status: PendingControllerEffectStatus::EffectUnknown,
            subject_digest: HEX64_C.to_string(),
            external_correlation_id: None,
            created_at_ms: 1_700_000_210_000,
            updated_at_ms: 1_700_000_215_000,
            diagnostic: Some("Closure effect receipt uncertain".to_string()),
        },
    );

    let mut rt_map = BTreeMap::new();
    rt_map.insert(job_cancelled_unknown.job_id.clone(), rt_cancel);
    rt_map.insert(job_sup_unknown.job_id.clone(), rt_sup);
    rt_map.insert(job_completed_unknown.job_id.clone(), rt_comp);
    write_controller_ledger_file(&plans_dir, BINDING_A, rt_map);

    let stores = WorkspaceHistoryStores::new(&plans_dir);
    let read = read_scoped_workspace_job_histories(&chat, &stores).expect("scoped read");
    assert!(read.degraded);

    for expected_id in [
        &job_cancelled_unknown.job_id,
        &job_sup_unknown.job_id,
        &job_completed_unknown.job_id,
    ] {
        let hist = read
            .jobs
            .iter()
            .find(|j| &j.job.job_id == expected_id)
            .expect("projected job exists");
        assert_eq!(
            hist.current_attention.as_ref().map(|a| a.kind),
            Some(WorkspaceAttentionKind::UnresolvedEffect),
            "job {expected_id} must preserve UnresolvedEffect reconciliation attention"
        );
        assert_eq!(hist.job.state, ObservationJobState::Unknown);
        assert_eq!(hist.acceptance_proof, WorkspaceAcceptanceProof::unknown());
    }
}

fn collect_dir_sha256(base: &Path, dir: &Path, hashes: &mut BTreeMap<String, String>) {
    for entry in fs::read_dir(dir).expect("read directory for sha256 snapshot") {
        let entry = entry.expect("directory entry");
        let path = entry.path();
        let meta = fs::symlink_metadata(&path).expect("entry metadata");
        if meta.is_dir() {
            collect_dir_sha256(base, &path, hashes);
        } else {
            let rel = path
                .strip_prefix(base)
                .expect("relative store path")
                .to_string_lossy()
                .into_owned();
            let bytes = fs::read(&path).expect("read store file bytes");
            let digest = threadlane_native::coworker_workflow::sha256_hex(&bytes);
            hashes.insert(rel, digest);
        }
    }
}

fn snapshot_private_dir_sha256(dir: &Path) -> BTreeMap<String, String> {
    let mut hashes = BTreeMap::new();
    collect_dir_sha256(dir, dir, &mut hashes);
    hashes
}

#[allow(clippy::too_many_arguments)]
fn create_public_store_job(
    workflow_store: &CoworkerWorkflowStore,
    project_base: &CanonicalProjectBaseRef,
    binding_id: &str,
    thread_uuid: &str,
    expected_ledger_revision: u64,
    request_key: &str,
    brief_id: &str,
    kind: WorkBriefKind,
    summary: &str,
) -> CoworkerJobRecord {
    let (brief, _) = sample_work_brief(
        brief_id,
        binding_id,
        thread_uuid,
        project_base.clone(),
        kind,
        summary,
    );
    workflow_store
        .create_or_amend_job(&CreateCoworkerJobRequest {
            binding_id: binding_id.to_string(),
            thread_uuid: thread_uuid.to_string(),
            request_key: request_key.to_string(),
            expected_ledger_revision: Some(expected_ledger_revision),
            work_brief: brief,
            amended_target_task_ids: Vec::new(),
            amended_write_paths: Vec::new(),
            amended_interfaces: Vec::new(),
        })
        .expect("public create_or_amend_job")
}

fn find_projected_job(
    jobs: &[threadlane_core::observation::WorkspaceJobHistory],
    id: &str,
) -> threadlane_core::observation::WorkspaceJobHistory {
    jobs.iter()
        .find(|j| j.job.job_id == id)
        .unwrap_or_else(|| panic!("missing projected job {id}"))
        .clone()
}

#[test]
fn conflicting_public_store_amendments_do_not_select_arbitrary_successor_or_retire_attention() {
    let tmp = TempDir::new().expect("tempdir");
    let root = fs::canonicalize(tmp.path()).expect("canonicalize temp root");
    let plans_dir = make_private_dir(&root.join("plans-branch-regression"));

    let raw_repo = root.join("repo-branch-regression");
    let (base_commit, _) = init_phase2_git_repo(&raw_repo);
    let repo_dir = fs::canonicalize(&raw_repo).expect("canonicalize git repo");
    let project_base = CanonicalProjectBaseRef::resolve_and_validate(&repo_dir, &base_commit)
        .expect("resolve live git project base");

    let chat_a =
        WorkspaceChatIdentity::new(SOURCE_A, BINDING_A, THREAD_A, &repo_dir.to_string_lossy())
            .expect("chat_a identity");
    let chat_b =
        WorkspaceChatIdentity::new(SOURCE_A, BINDING_B, THREAD_B, &repo_dir.to_string_lossy())
            .expect("chat_b identity");

    let workflow_store = CoworkerWorkflowStore::new(&plans_dir).expect("open workflow store");

    // Component 1: Real public-store consecutive amendments of the same target (`branch_root`).
    // First amendment advances `branch_root` from rev 1 -> rev 2 (`Held`); second amendment
    // advances `branch_root` from rev 2 -> rev 3 (`Held` with two active causal amendment holds).
    let branch_root = create_public_store_job(
        &workflow_store,
        &project_base,
        BINDING_A,
        THREAD_A,
        0,
        "req_branch_root_01",
        "brf_branch_root_01",
        WorkBriefKind::NewJob,
        "Root planning job with competing amendments",
    );
    assert_eq!(branch_root.job_revision, 1);

    let branch_alpha = create_public_store_job(
        &workflow_store,
        &project_base,
        BINDING_A,
        THREAD_A,
        1,
        "req_branch_alpha_02",
        "brf_branch_alpha_02",
        WorkBriefKind::Amendment {
            target_job_id: branch_root.job_id.clone(),
            target_job_revision: 1,
            amendment_reason: "First competing amendment branch".to_string(),
        },
        "Competing amendment branch Alpha",
    );
    let branch_bravo = create_public_store_job(
        &workflow_store,
        &project_base,
        BINDING_A,
        THREAD_A,
        2,
        "req_branch_bravo_03",
        "brf_branch_bravo_03",
        WorkBriefKind::Amendment {
            target_job_id: branch_root.job_id.clone(),
            target_job_revision: 2,
            amendment_reason: "Second competing amendment branch".to_string(),
        },
        "Competing amendment branch Bravo",
    );

    // Component 2: Two-step chain (`chain_conflict_1 -> chain_conflict_2`) whose downstream head
    // branches into `chain_conflict_3a` and `chain_conflict_3b`, tainting that connected component.
    let chain_conflict_1 = create_public_store_job(
        &workflow_store,
        &project_base,
        BINDING_A,
        THREAD_A,
        3,
        "req_cc_01",
        "brf_cc_01",
        WorkBriefKind::NewJob,
        "Upstream step in conflicted lineage component",
    );
    let chain_conflict_2 = create_public_store_job(
        &workflow_store,
        &project_base,
        BINDING_A,
        THREAD_A,
        4,
        "req_cc_02",
        "brf_cc_02",
        WorkBriefKind::Amendment {
            target_job_id: chain_conflict_1.job_id.clone(),
            target_job_revision: 1,
            amendment_reason: "Linear step before downstream fork".to_string(),
        },
        "Midpoint step that forks into two competing amendments",
    );
    let chain_conflict_3a = create_public_store_job(
        &workflow_store,
        &project_base,
        BINDING_A,
        THREAD_A,
        5,
        "req_cc_03a",
        "brf_cc_03a",
        WorkBriefKind::Amendment {
            target_job_id: chain_conflict_2.job_id.clone(),
            target_job_revision: 1,
            amendment_reason: "Downstream fork option A".to_string(),
        },
        "Downstream fork option A",
    );
    let chain_conflict_3b = create_public_store_job(
        &workflow_store,
        &project_base,
        BINDING_A,
        THREAD_A,
        6,
        "req_cc_03b",
        "brf_cc_03b",
        WorkBriefKind::Amendment {
            target_job_id: chain_conflict_2.job_id.clone(),
            target_job_revision: 2,
            amendment_reason: "Downstream fork option B".to_string(),
        },
        "Downstream fork option B",
    );

    // Component 3: Unrelated valid two-step chain (`valid_1 -> valid_2 -> valid_3`) in the same
    // binding ledger, which must still retire obsolete planning attention cleanly.
    let valid_1 = create_public_store_job(
        &workflow_store,
        &project_base,
        BINDING_A,
        THREAD_A,
        7,
        "req_valid_chain_01",
        "brf_valid_chain_01",
        WorkBriefKind::NewJob,
        "Unrelated valid chain root",
    );
    let valid_2 = create_public_store_job(
        &workflow_store,
        &project_base,
        BINDING_A,
        THREAD_A,
        8,
        "req_valid_chain_02",
        "brf_valid_chain_02",
        WorkBriefKind::Amendment {
            target_job_id: valid_1.job_id.clone(),
            target_job_revision: 1,
            amendment_reason: "Valid chain step 2".to_string(),
        },
        "Unrelated valid chain midpoint",
    );
    let valid_3 = create_public_store_job(
        &workflow_store,
        &project_base,
        BINDING_A,
        THREAD_A,
        9,
        "req_valid_chain_03",
        "brf_valid_chain_03",
        WorkBriefKind::Amendment {
            target_job_id: valid_2.job_id.clone(),
            target_job_revision: 1,
            amendment_reason: "Valid chain step 3".to_string(),
        },
        "Unrelated valid chain current head",
    );

    // Separate binding (`BINDING_B` / `THREAD_B`) to prove cross-binding isolation.
    let bravo_old = create_public_store_job(
        &workflow_store,
        &project_base,
        BINDING_B,
        THREAD_B,
        0,
        "req_bravo_chain_01",
        "brf_bravo_chain_01",
        WorkBriefKind::NewJob,
        "Separate binding superseded planning job",
    );
    let bravo_new = create_public_store_job(
        &workflow_store,
        &project_base,
        BINDING_B,
        THREAD_B,
        1,
        "req_bravo_chain_02",
        "brf_bravo_chain_02",
        WorkBriefKind::Amendment {
            target_job_id: bravo_old.job_id.clone(),
            target_job_revision: 1,
            amendment_reason: "Separate binding clean amendment".to_string(),
        },
        "Separate binding active planning head",
    );

    let stored_ledger_a = workflow_store
        .read_binding_ledger(BINDING_A)
        .expect("read binding A ledger");
    let stored_branch_root = stored_ledger_a
        .jobs
        .iter()
        .find(|j| j.job_id == branch_root.job_id)
        .expect("branch_root stored");
    assert_eq!(stored_branch_root.job_revision, 3);
    assert_eq!(stored_branch_root.phase, CoworkerJobPhase::Held);
    assert_eq!(stored_branch_root.holds.len(), 2);

    // Caller-supplied `WorkspaceJobLineageContext` never mints the private retirement witness.
    let forged_ctx = WorkspaceJobLineageContext {
        superseded_by_job_id: Some(branch_alpha.job_id.clone()),
        follow_up_job_ids: vec![branch_alpha.job_id.clone()],
        ..WorkspaceJobLineageContext::default()
    };
    let caller_projected =
        project_coworker_job_history(&chat_a, stored_branch_root, None, Some(&forged_ctx))
            .expect("caller projection");
    assert_eq!(
        caller_projected.current_attention.as_ref().map(|a| a.kind),
        Some(WorkspaceAttentionKind::OnHold)
    );
    assert_eq!(caller_projected.job.state, ObservationJobState::NeedsUser);

    remove_lockfiles_in_dir(&plans_dir);
    let hashes_before = snapshot_private_dir_sha256(&plans_dir);

    let stores = WorkspaceHistoryStores::new(&plans_dir);
    let forward_read =
        read_scoped_workspace_job_histories(&chat_a, &stores).expect("forward scoped read");
    let bravo_read =
        read_scoped_workspace_job_histories(&chat_b, &stores).expect("bravo scoped read");

    assert_no_lock_or_wal_files(&plans_dir);
    assert_eq!(
        snapshot_private_dir_sha256(&plans_dir),
        hashes_before,
        "read_scoped_workspace_job_histories must not mutate any ledger file bytes"
    );

    assert!(!forward_read.degraded);
    assert_eq!(forward_read.rejected_scope_count, 0);
    assert_eq!(forward_read.jobs.len(), 10);

    // Conflicting branch root: must NOT pick last-write-wins `superseded_by_job_id` or retire attention.
    let h_root = find_projected_job(&forward_read.jobs, &branch_root.job_id);
    assert_eq!(
        h_root.links.superseded_by_job_id, None,
        "conflicting admitted successors must not select an arbitrary current successor"
    );
    assert_eq!(
        h_root.links.follow_up_job_ids,
        vec![branch_alpha.job_id.clone(), branch_bravo.job_id.clone()],
        "all verified historical follow-up links must remain visible in deterministic order"
    );
    assert_eq!(
        h_root.current_attention.as_ref().map(|a| a.kind),
        Some(WorkspaceAttentionKind::OnHold),
        "conflicting amendment branches must preserve live OnHold attention"
    );
    assert_eq!(
        h_root
            .current_attention
            .as_ref()
            .expect("branch_root attention")
            .blocking_hold_ids
            .len(),
        2
    );
    assert_eq!(h_root.job.state, ObservationJobState::NeedsUser);
    assert_ne!(h_root.job.state, ObservationJobState::Completed);
    assert_ne!(h_root.job.state, ObservationJobState::Failed);
    assert_eq!(h_root.job.approval_status, "draft");
    assert_eq!(h_root.acceptance_proof, WorkspaceAcceptanceProof::unknown());

    for branch_id in [&branch_alpha.job_id, &branch_bravo.job_id] {
        let h_branch = find_projected_job(&forward_read.jobs, branch_id);
        assert_eq!(
            h_branch.links.supersedes_job_id.as_deref(),
            Some(branch_root.job_id.as_str())
        );
        assert_eq!(h_branch.links.superseded_by_job_id, None);
        assert_eq!(h_branch.job.state, ObservationJobState::Planning);
        assert_eq!(
            h_branch.acceptance_proof,
            WorkspaceAcceptanceProof::unknown()
        );
    }

    // Connected component with downstream fork: both `chain_conflict_1` and `chain_conflict_2`
    // conservatively clear `superseded_by_job_id` and preserve live `OnHold` attention.
    let h_cc_1 = find_projected_job(&forward_read.jobs, &chain_conflict_1.job_id);
    assert_eq!(h_cc_1.links.superseded_by_job_id, None);
    assert_eq!(
        h_cc_1.links.follow_up_job_ids,
        vec![chain_conflict_2.job_id.clone()]
    );
    assert_eq!(
        h_cc_1.current_attention.as_ref().map(|a| a.kind),
        Some(WorkspaceAttentionKind::OnHold)
    );
    assert_eq!(h_cc_1.job.state, ObservationJobState::NeedsUser);
    assert_eq!(h_cc_1.job.approval_status, "draft");

    let h_cc_2 = find_projected_job(&forward_read.jobs, &chain_conflict_2.job_id);
    assert_eq!(h_cc_2.links.superseded_by_job_id, None);
    assert_eq!(
        h_cc_2.links.follow_up_job_ids,
        vec![
            chain_conflict_3a.job_id.clone(),
            chain_conflict_3b.job_id.clone(),
        ]
    );
    assert_eq!(
        h_cc_2.current_attention.as_ref().map(|a| a.kind),
        Some(WorkspaceAttentionKind::OnHold)
    );
    assert_eq!(h_cc_2.job.state, ObservationJobState::NeedsUser);
    assert_eq!(h_cc_2.job.approval_status, "draft");

    // Unrelated valid two-step chain in the same ledger still retires obsolete planning attention.
    let h_valid_1 = find_projected_job(&forward_read.jobs, &valid_1.job_id);
    assert_eq!(h_valid_1.current_attention, None);
    assert_eq!(h_valid_1.job.state, ObservationJobState::Unknown);
    assert_eq!(h_valid_1.job.approval_status, "superseded");
    assert_eq!(
        h_valid_1.links.superseded_by_job_id.as_deref(),
        Some(valid_2.job_id.as_str())
    );
    let h_valid_2 = find_projected_job(&forward_read.jobs, &valid_2.job_id);
    assert_eq!(h_valid_2.current_attention, None);
    assert_eq!(h_valid_2.job.state, ObservationJobState::Unknown);
    assert_eq!(h_valid_2.job.approval_status, "superseded");
    assert_eq!(
        h_valid_2.links.superseded_by_job_id.as_deref(),
        Some(valid_3.job_id.as_str())
    );
    let h_valid_3 = find_projected_job(&forward_read.jobs, &valid_3.job_id);
    assert_eq!(h_valid_3.job.state, ObservationJobState::Planning);
    assert_eq!(h_valid_3.links.superseded_by_job_id, None);

    // Separate binding remains isolated and retires its own valid 1-step chain.
    assert!(!bravo_read.degraded);
    assert_eq!(bravo_read.rejected_scope_count, 0);
    assert_eq!(bravo_read.jobs.len(), 2);
    let h_bravo_old = find_projected_job(&bravo_read.jobs, &bravo_old.job_id);
    assert_eq!(h_bravo_old.current_attention, None);
    assert_eq!(
        h_bravo_old.links.superseded_by_job_id.as_deref(),
        Some(bravo_new.job_id.as_str())
    );

    // Reverse `ledger.jobs` order, validate via `CoworkerBindingLedger::validate` and
    // `workflow_store.read_binding_ledger`, and prove identical neutral results.
    let mut reversed_ledger = stored_ledger_a;
    reversed_ledger.jobs.reverse();
    reversed_ledger
        .validate(BINDING_A)
        .expect("reversed binding ledger must remain typed-valid");
    let ledger_path_a = workflow_store
        .ledger_path_for_binding(BINDING_A)
        .expect("binding A ledger path");
    write_private_json(&ledger_path_a, &reversed_ledger);
    let reread_reversed = workflow_store
        .read_binding_ledger(BINDING_A)
        .expect("read reversed binding ledger");
    assert_eq!(reread_reversed.jobs.len(), 10);

    remove_lockfiles_in_dir(&plans_dir);
    let rev_hashes_before = snapshot_private_dir_sha256(&plans_dir);
    let reversed_read =
        read_scoped_workspace_job_histories(&chat_a, &stores).expect("reversed scoped read");
    assert_no_lock_or_wal_files(&plans_dir);
    assert_eq!(
        snapshot_private_dir_sha256(&plans_dir),
        rev_hashes_before,
        "reversed ledger read must not mutate stored files"
    );
    assert_eq!(
        reversed_read, forward_read,
        "reversing ledger.jobs order must produce identical neutral projection and follow-up order"
    );
}

#[test]
fn cyclic_and_dangling_amendment_lineages_preserve_attention_and_isolate_foreign_records() {
    let tmp = TempDir::new().expect("tempdir");
    let root = fs::canonicalize(tmp.path()).expect("canonicalize temp root");
    let plans_dir = make_private_dir(&root.join("plans-cycle-dangling"));
    let auth_dir = make_private_dir(&root.join("authority-empty"));
    let reg_dir = make_private_dir(&root.join("reg"));
    let pln_dir = make_private_dir(&root.join("pln"));
    let reg = Arc::new(RegistryStore::open(&reg_dir).expect("open registry"));
    let pln = Arc::new(PlanStore::open(&pln_dir).expect("open plans"));
    let _ = AuthorityStore::open(&auth_dir, reg, pln).expect("initialize empty authority store");
    remove_lockfiles_in_dir(&auth_dir);

    let raw_repo_a = root.join("repo-cycle-a");
    let (base_commit_a, _) = init_phase2_git_repo(&raw_repo_a);
    let repo_a = fs::canonicalize(&raw_repo_a).expect("canonicalize repo_a");
    let base_a = CanonicalProjectBaseRef::resolve_and_validate(&repo_a, &base_commit_a)
        .expect("resolve base_a");

    let repo_b = make_private_dir(&root.join("repo-foreign-b"));
    let base_b = sample_project_base(&repo_b);

    let chat_a =
        WorkspaceChatIdentity::new(SOURCE_A, BINDING_A, THREAD_A, &repo_a.to_string_lossy())
            .expect("chat_a");

    let workflow_store = CoworkerWorkflowStore::new(&plans_dir).expect("open workflow store");

    // Verify public `create_or_amend_job` rejects missing targets (`ValidationFailed`), so cyclic
    // and dangling-target histories are exercised via typed-validated `CoworkerBindingLedger`.
    let (missing_target_brief, _) = sample_work_brief(
        "brf_public_missing_target",
        BINDING_A,
        THREAD_A,
        base_a.clone(),
        WorkBriefKind::Amendment {
            target_job_id: "job_missing_000000000000000000000000".to_string(),
            target_job_revision: 1,
            amendment_reason: "Attempt amendment against missing target".to_string(),
        },
        "Rejected public missing-target amendment",
    );
    let missing_err = workflow_store
        .create_or_amend_job(&CreateCoworkerJobRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_A.to_string(),
            request_key: "req_public_missing_target".to_string(),
            expected_ledger_revision: Some(0),
            work_brief: missing_target_brief,
            amended_target_task_ids: Vec::new(),
            amended_write_paths: Vec::new(),
            amended_interfaces: Vec::new(),
        })
        .expect_err("public create_or_amend_job must reject missing target_job_id");
    assert_eq!(missing_err.error_code(), NativeErrorCode::ValidationFailed);

    // 1. Typed-validated 2-job cycle (`cycle_a <-> cycle_b`), with matching causal amendment holds.
    let cycle_a_id = "job_cycle_alpha_01".to_string();
    let cycle_b_id = "job_cycle_bravo_02".to_string();
    let (b_cyc_a, d_cyc_a) = sample_work_brief(
        "brf_cycle_alpha",
        BINDING_A,
        THREAD_A,
        base_a.clone(),
        WorkBriefKind::Amendment {
            target_job_id: cycle_b_id.clone(),
            target_job_revision: 1,
            amendment_reason: "Cycle edge A -> B".to_string(),
        },
        "Cyclic awaiting-approval job A",
    );
    let job_cycle_a = CoworkerJobRecord {
        job_id: cycle_a_id.clone(),
        job_revision: 2,
        create_request_key: "req_cyc_a".to_string(),
        binding_id: BINDING_A.to_string(),
        thread_uuid: THREAD_A.to_string(),
        phase: CoworkerJobPhase::AwaitingApproval,
        work_brief: b_cyc_a,
        work_brief_digest: d_cyc_a,
        supersedes_job_id: Some(cycle_b_id.clone()),
        planning_result: None,
        approval_linkage: None,
        tasks: Vec::new(),
        holds: vec![sample_amendment_hold(
            "hld_cyc_a",
            "amend_req_cyc_b",
            2,
            "Cycle hold from B",
        )],
        outbox: Vec::new(),
        combined_candidates: Vec::new(),
    };
    let (b_cyc_b, d_cyc_b) = sample_work_brief(
        "brf_cycle_bravo",
        BINDING_A,
        THREAD_A,
        base_a.clone(),
        WorkBriefKind::Amendment {
            target_job_id: cycle_a_id.clone(),
            target_job_revision: 1,
            amendment_reason: "Cycle edge B -> A".to_string(),
        },
        "Cyclic held job B",
    );
    let job_cycle_b = CoworkerJobRecord {
        job_id: cycle_b_id.clone(),
        job_revision: 2,
        create_request_key: "req_cyc_b".to_string(),
        binding_id: BINDING_A.to_string(),
        thread_uuid: THREAD_A.to_string(),
        phase: CoworkerJobPhase::Held,
        work_brief: b_cyc_b,
        work_brief_digest: d_cyc_b,
        supersedes_job_id: Some(cycle_a_id.clone()),
        planning_result: None,
        approval_linkage: None,
        tasks: Vec::new(),
        holds: vec![sample_amendment_hold(
            "hld_cyc_b",
            "amend_req_cyc_a",
            2,
            "Cycle hold from A",
        )],
        outbox: Vec::new(),
        combined_candidates: Vec::new(),
    };

    // 2. Dangling missing upstream target (`missing <- dangling_mid <- dangling_head`).
    let (b_dang_mid, d_dang_mid) = sample_work_brief(
        "brf_dangling_mid",
        BINDING_A,
        THREAD_A,
        base_a.clone(),
        WorkBriefKind::Amendment {
            target_job_id: "job_missing_upstream_00".to_string(),
            target_job_revision: 1,
            amendment_reason: "References missing upstream target".to_string(),
        },
        "Midpoint with missing upstream target",
    );
    let job_dangling_mid = CoworkerJobRecord {
        job_id: "job_dangling_mid_01".to_string(),
        job_revision: 2,
        create_request_key: "req_dangling_mid".to_string(),
        binding_id: BINDING_A.to_string(),
        thread_uuid: THREAD_A.to_string(),
        phase: CoworkerJobPhase::Held,
        work_brief: b_dang_mid,
        work_brief_digest: d_dang_mid,
        supersedes_job_id: Some("job_missing_upstream_00".to_string()),
        planning_result: None,
        approval_linkage: None,
        tasks: Vec::new(),
        holds: vec![sample_amendment_hold(
            "hld_dangling_mid",
            "amend_req_dangling_head",
            2,
            "Amended by dangling_head despite missing upstream target",
        )],
        outbox: Vec::new(),
        combined_candidates: Vec::new(),
    };
    let (b_dang_head, d_dang_head) = sample_work_brief(
        "brf_dangling_head",
        BINDING_A,
        THREAD_A,
        base_a.clone(),
        WorkBriefKind::Amendment {
            target_job_id: job_dangling_mid.job_id.clone(),
            target_job_revision: 1,
            amendment_reason: "Follow-up on dangling component".to_string(),
        },
        "Head of dangling component",
    );
    let job_dangling_head = CoworkerJobRecord {
        job_id: "job_dangling_head_02".to_string(),
        job_revision: 1,
        create_request_key: "req_dangling_head".to_string(),
        binding_id: BINDING_A.to_string(),
        thread_uuid: THREAD_A.to_string(),
        phase: CoworkerJobPhase::Planning,
        work_brief: b_dang_head,
        work_brief_digest: d_dang_head,
        supersedes_job_id: Some(job_dangling_mid.job_id.clone()),
        planning_result: None,
        approval_linkage: None,
        tasks: Vec::new(),
        holds: Vec::new(),
        outbox: Vec::new(),
        combined_candidates: Vec::new(),
    };

    // 3. Scope-rejected upstream target (`job_foreign_upstream` in `repo_b`) referenced by an
    // admitted local chain (`job_unverified_mid <- job_unverified_head`).
    let (b_for_up, d_for_up) = sample_work_brief(
        "brf_foreign_upstream",
        BINDING_A,
        THREAD_A,
        base_b.clone(),
        WorkBriefKind::NewJob,
        "Foreign repository job",
    );
    let job_foreign_upstream = CoworkerJobRecord {
        job_id: "job_foreign_upstream_00".to_string(),
        job_revision: 2,
        create_request_key: "req_foreign_upstream".to_string(),
        binding_id: BINDING_A.to_string(),
        thread_uuid: THREAD_A.to_string(),
        phase: CoworkerJobPhase::Planning,
        work_brief: b_for_up,
        work_brief_digest: d_for_up,
        supersedes_job_id: None,
        planning_result: None,
        approval_linkage: None,
        tasks: Vec::new(),
        holds: Vec::new(),
        outbox: Vec::new(),
        combined_candidates: Vec::new(),
    };
    let (b_unv_mid, d_unv_mid) = sample_work_brief(
        "brf_unverified_mid",
        BINDING_A,
        THREAD_A,
        base_a.clone(),
        WorkBriefKind::Amendment {
            target_job_id: job_foreign_upstream.job_id.clone(),
            target_job_revision: 1,
            amendment_reason: "References rejected foreign-repo target".to_string(),
        },
        "Local job referencing unverified foreign target",
    );
    let job_unverified_mid = CoworkerJobRecord {
        job_id: "job_unverified_mid_01".to_string(),
        job_revision: 2,
        create_request_key: "req_unverified_mid".to_string(),
        binding_id: BINDING_A.to_string(),
        thread_uuid: THREAD_A.to_string(),
        phase: CoworkerJobPhase::Held,
        work_brief: b_unv_mid,
        work_brief_digest: d_unv_mid,
        supersedes_job_id: Some(job_foreign_upstream.job_id.clone()),
        planning_result: None,
        approval_linkage: None,
        tasks: Vec::new(),
        holds: vec![sample_amendment_hold(
            "hld_unverified_mid",
            "amend_req_unverified_head",
            2,
            "Amended by unverified_head",
        )],
        outbox: Vec::new(),
        combined_candidates: Vec::new(),
    };
    let (b_unv_head, d_unv_head) = sample_work_brief(
        "brf_unverified_head",
        BINDING_A,
        THREAD_A,
        base_a.clone(),
        WorkBriefKind::Amendment {
            target_job_id: job_unverified_mid.job_id.clone(),
            target_job_revision: 1,
            amendment_reason: "Follow-up on unverified-target component".to_string(),
        },
        "Head of unverified-target component",
    );
    let job_unverified_head = CoworkerJobRecord {
        job_id: "job_unverified_head_02".to_string(),
        job_revision: 1,
        create_request_key: "req_unverified_head".to_string(),
        binding_id: BINDING_A.to_string(),
        thread_uuid: THREAD_A.to_string(),
        phase: CoworkerJobPhase::Planning,
        work_brief: b_unv_head,
        work_brief_digest: d_unv_head,
        supersedes_job_id: Some(job_unverified_mid.job_id.clone()),
        planning_result: None,
        approval_linkage: None,
        tasks: Vec::new(),
        holds: Vec::new(),
        outbox: Vec::new(),
        combined_candidates: Vec::new(),
    };

    // 4. Valid local chain (`job_clean_local_old -> job_clean_local_new`) plus rejected foreign
    // and unverified-approval records claiming `supersedes_job_id = Some(job_clean_local_old)`.
    // Rejected records must neither mint proof nor poison the verified local component.
    let (b_clean_old, d_clean_old) = sample_work_brief(
        "brf_clean_local_old",
        BINDING_A,
        THREAD_A,
        base_a.clone(),
        WorkBriefKind::NewJob,
        "Valid local chain root",
    );
    let job_clean_local_old = CoworkerJobRecord {
        job_id: "job_clean_local_old_01".to_string(),
        job_revision: 2,
        create_request_key: "req_clean_local_old".to_string(),
        binding_id: BINDING_A.to_string(),
        thread_uuid: THREAD_A.to_string(),
        phase: CoworkerJobPhase::Held,
        work_brief: b_clean_old,
        work_brief_digest: d_clean_old,
        supersedes_job_id: None,
        planning_result: None,
        approval_linkage: None,
        tasks: Vec::new(),
        holds: vec![sample_amendment_hold(
            "hld_clean_local_old",
            "amend_req_clean_local_new",
            2,
            "Verified local amendment",
        )],
        outbox: Vec::new(),
        combined_candidates: Vec::new(),
    };
    let (b_clean_new, d_clean_new) = sample_work_brief(
        "brf_clean_local_new",
        BINDING_A,
        THREAD_A,
        base_a.clone(),
        WorkBriefKind::Amendment {
            target_job_id: job_clean_local_old.job_id.clone(),
            target_job_revision: 1,
            amendment_reason: "Verified local amendment".to_string(),
        },
        "Valid local chain head",
    );
    let job_clean_local_new = CoworkerJobRecord {
        job_id: "job_clean_local_new_02".to_string(),
        job_revision: 1,
        create_request_key: "req_clean_local_new".to_string(),
        binding_id: BINDING_A.to_string(),
        thread_uuid: THREAD_A.to_string(),
        phase: CoworkerJobPhase::Planning,
        work_brief: b_clean_new,
        work_brief_digest: d_clean_new,
        supersedes_job_id: Some(job_clean_local_old.job_id.clone()),
        planning_result: None,
        approval_linkage: None,
        tasks: Vec::new(),
        holds: Vec::new(),
        outbox: Vec::new(),
        combined_candidates: Vec::new(),
    };

    let (b_for_poison, d_for_poison) = sample_work_brief(
        "brf_foreign_poison",
        BINDING_A,
        THREAD_A,
        base_b,
        WorkBriefKind::Amendment {
            target_job_id: job_clean_local_old.job_id.clone(),
            target_job_revision: 1,
            amendment_reason: "Foreign repo record targeting local chain".to_string(),
        },
        "Rejected foreign-repo poison candidate",
    );
    let job_foreign_poison = CoworkerJobRecord {
        job_id: "job_foreign_poison_03".to_string(),
        job_revision: 1,
        create_request_key: "req_foreign_poison".to_string(),
        binding_id: BINDING_A.to_string(),
        thread_uuid: THREAD_A.to_string(),
        phase: CoworkerJobPhase::Planning,
        work_brief: b_for_poison,
        work_brief_digest: d_for_poison,
        supersedes_job_id: Some(job_clean_local_old.job_id.clone()),
        planning_result: None,
        approval_linkage: None,
        tasks: Vec::new(),
        holds: Vec::new(),
        outbox: Vec::new(),
        combined_candidates: Vec::new(),
    };

    let policy = ExecutionPolicy::deny_all("policy-test").expect("policy");
    let policy_digest = policy.digest().expect("policy digest");
    let mut unverified_linkage = sample_approval_linkage(
        base_a.clone(),
        &policy_digest,
        vec!["task-p-01".to_string()],
    );
    unverified_linkage.plan_id = "plan_unverified_poison_01".to_string();
    let (b_auth_poison, d_auth_poison) = sample_work_brief(
        "brf_auth_poison",
        BINDING_A,
        THREAD_A,
        base_a,
        WorkBriefKind::Amendment {
            target_job_id: job_clean_local_old.job_id.clone(),
            target_job_revision: 1,
            amendment_reason: "Unverified approval record targeting local chain".to_string(),
        },
        "Rejected unverified-approval poison candidate",
    );
    let job_auth_poison = CoworkerJobRecord {
        job_id: "job_auth_poison_04".to_string(),
        job_revision: 1,
        create_request_key: "req_auth_poison".to_string(),
        binding_id: BINDING_A.to_string(),
        thread_uuid: THREAD_A.to_string(),
        phase: CoworkerJobPhase::Executing,
        work_brief: b_auth_poison,
        work_brief_digest: d_auth_poison,
        supersedes_job_id: Some(job_clean_local_old.job_id.clone()),
        planning_result: None,
        approval_linkage: Some(unverified_linkage),
        tasks: Vec::new(),
        holds: Vec::new(),
        outbox: Vec::new(),
        combined_candidates: Vec::new(),
    };

    let ledger = CoworkerBindingLedger {
        schema_version: 1,
        binding_id: BINDING_A.to_string(),
        thread_uuid: Some(THREAD_A.to_string()),
        ledger_revision: 1,
        planning_grants: Vec::new(),
        architect_advice: Vec::new(),
        jobs: vec![
            job_cycle_a.clone(),
            job_cycle_b.clone(),
            job_dangling_mid.clone(),
            job_dangling_head.clone(),
            job_foreign_upstream,
            job_unverified_mid.clone(),
            job_unverified_head.clone(),
            job_clean_local_old.clone(),
            job_clean_local_new.clone(),
            job_foreign_poison,
            job_auth_poison,
        ],
        admission_markers: BTreeMap::new(),
    };
    ledger
        .validate(BINDING_A)
        .expect("malformed-lineage fixture must pass CoworkerBindingLedger::validate");

    let ledger_path = workflow_store
        .ledger_path_for_binding(BINDING_A)
        .expect("ledger path");
    write_private_json(&ledger_path, &ledger);
    let _ = workflow_store
        .read_binding_ledger(BINDING_A)
        .expect("workflow_store.read_binding_ledger accepts typed-valid fixture");

    remove_lockfiles_in_dir(&plans_dir);
    let hashes_before = snapshot_private_dir_sha256(&plans_dir);
    let auth_hashes_before = snapshot_private_dir_sha256(&auth_dir);

    let stores = WorkspaceHistoryStores::new(&plans_dir).with_authority_dir(&auth_dir);
    let forward_read =
        read_scoped_workspace_job_histories(&chat_a, &stores).expect("forward malformed read");

    assert_no_lock_or_wal_files(&plans_dir);
    assert_no_lock_or_wal_files(&auth_dir);
    assert_eq!(snapshot_private_dir_sha256(&plans_dir), hashes_before);
    assert_eq!(snapshot_private_dir_sha256(&auth_dir), auth_hashes_before);
    assert_eq!(forward_read.rejected_scope_count, 3);
    assert_eq!(forward_read.jobs.len(), 8);

    // Cycle: neither job selects `superseded_by_job_id` or retires attention; follow-ups remain.
    let h_cyc_a = find_projected_job(&forward_read.jobs, &job_cycle_a.job_id);
    assert_eq!(h_cyc_a.links.superseded_by_job_id, None);
    assert_eq!(
        h_cyc_a.links.follow_up_job_ids,
        vec![job_cycle_b.job_id.clone()]
    );
    assert_eq!(
        h_cyc_a.current_attention.as_ref().map(|a| a.kind),
        Some(WorkspaceAttentionKind::OnHold)
    );
    assert_eq!(h_cyc_a.job.state, ObservationJobState::NeedsUser);
    assert_eq!(h_cyc_a.job.approval_status, "pending_approval");
    assert_eq!(
        h_cyc_a.acceptance_proof,
        WorkspaceAcceptanceProof::unknown()
    );

    let h_cyc_b = find_projected_job(&forward_read.jobs, &job_cycle_b.job_id);
    assert_eq!(h_cyc_b.links.superseded_by_job_id, None);
    assert_eq!(
        h_cyc_b.links.follow_up_job_ids,
        vec![job_cycle_a.job_id.clone()]
    );
    assert_eq!(
        h_cyc_b.current_attention.as_ref().map(|a| a.kind),
        Some(WorkspaceAttentionKind::OnHold)
    );
    assert_eq!(h_cyc_b.job.state, ObservationJobState::NeedsUser);
    assert_eq!(h_cyc_b.job.approval_status, "draft");

    // Missing upstream target taints its connected component (`dangling_mid` keeps OnHold).
    let h_dang_mid = find_projected_job(&forward_read.jobs, &job_dangling_mid.job_id);
    assert_eq!(h_dang_mid.links.superseded_by_job_id, None);
    assert_eq!(
        h_dang_mid.links.follow_up_job_ids,
        vec![job_dangling_head.job_id.clone()]
    );
    assert_eq!(
        h_dang_mid.current_attention.as_ref().map(|a| a.kind),
        Some(WorkspaceAttentionKind::OnHold)
    );
    assert_eq!(h_dang_mid.job.state, ObservationJobState::NeedsUser);
    assert_eq!(h_dang_mid.job.approval_status, "draft");

    // Scope-rejected upstream target also taints the referencing local component.
    let h_unv_mid = find_projected_job(&forward_read.jobs, &job_unverified_mid.job_id);
    assert_eq!(h_unv_mid.links.superseded_by_job_id, None);
    assert_eq!(
        h_unv_mid.links.follow_up_job_ids,
        vec![job_unverified_head.job_id.clone()]
    );
    assert_eq!(
        h_unv_mid.current_attention.as_ref().map(|a| a.kind),
        Some(WorkspaceAttentionKind::OnHold)
    );
    assert_eq!(h_unv_mid.job.state, ObservationJobState::NeedsUser);

    // Rejected foreign and unverified-approval records do NOT poison the valid local chain.
    let h_clean_old = find_projected_job(&forward_read.jobs, &job_clean_local_old.job_id);
    assert_eq!(h_clean_old.current_attention, None);
    assert_eq!(h_clean_old.job.state, ObservationJobState::Unknown);
    assert_eq!(h_clean_old.job.approval_status, "superseded");
    assert_eq!(
        h_clean_old.links.superseded_by_job_id.as_deref(),
        Some(job_clean_local_new.job_id.as_str())
    );
    assert_eq!(
        h_clean_old.links.follow_up_job_ids,
        vec![job_clean_local_new.job_id.clone()]
    );

    // Reverse `ledger.jobs` order and prove identical projected job histories.
    let mut reversed_ledger = ledger;
    reversed_ledger.jobs.reverse();
    reversed_ledger
        .validate(BINDING_A)
        .expect("reversed malformed-lineage ledger remains typed-valid");
    write_private_json(&ledger_path, &reversed_ledger);
    let _ = workflow_store
        .read_binding_ledger(BINDING_A)
        .expect("read reversed malformed-lineage ledger");

    remove_lockfiles_in_dir(&plans_dir);
    let rev_hashes_before = snapshot_private_dir_sha256(&plans_dir);
    let reversed_read =
        read_scoped_workspace_job_histories(&chat_a, &stores).expect("reversed malformed read");
    assert_no_lock_or_wal_files(&plans_dir);
    assert_eq!(snapshot_private_dir_sha256(&plans_dir), rev_hashes_before);
    assert_eq!(reversed_read.rejected_scope_count, 3);
    assert_eq!(
        reversed_read.jobs, forward_read.jobs,
        "reversed ledger order must yield identical projections for cycles and dangling links"
    );
}
