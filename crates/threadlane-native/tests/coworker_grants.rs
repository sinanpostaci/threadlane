use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use tempfile::TempDir;
use threadlane_native::coworker_grants::{
    get_planning_grant_summary, list_planning_grant_summaries, prepare_planning_grant_preview,
    prepare_planning_grant_preview_from_dir_readonly, prepare_planning_grant_preview_with_store,
    record_planning_grant, record_planning_grant_from_registry_dir, revoke_planning_grant_scoped,
    LocalOperatorGrantAttestation, PlanningGrantPreviewStatus, PreparePlanningGrantRequest,
    RecordPlanningGrantRequest, TrustedGrantScope,
};
use threadlane_native::coworker_workflow::{
    CoworkerWorkflowStore, PlanningCallBudget, PlanningGrantStatus, PlanningReadOnlyCapabilities,
    RevokePlanningGrantRequest,
};
use threadlane_native::{
    Difficulty, ExecutionPolicy, NativeErrorCode, PolicyAllowRule, RegistryStore,
    RoleExecutionRoute, RolePreflightRequest, ToolAdapter, WorkflowProfile, WorkflowRole,
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
const BINDING_A: &str = "bnd_grant_alpha";
const BINDING_B: &str = "bnd_grant_beta";

struct GrantFixture {
    _tmp: TempDir,
    repo_dir: PathBuf,
    base_commit: String,
    base_tree_sha: String,
    registry_dir: PathBuf,
    plans_dir: PathBuf,
    registry: Arc<RegistryStore>,
    store: CoworkerWorkflowStore,
}

fn init_git_repo(repo_dir: &Path) -> (String, String) {
    fs::create_dir_all(repo_dir.join("src")).expect("create repo src");
    fs::write(
        repo_dir.join("src/lib.rs"),
        "pub fn plan_seed() -> u32 { 7 }\n",
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

fn make_private_dir(path: &Path) {
    use std::os::unix::fs::PermissionsExt;
    fs::create_dir_all(path).expect("create private dir");
    fs::set_permissions(path, fs::Permissions::from_mode(0o700)).expect("chmod 0700 private dir");
}

fn setup_fixture() -> GrantFixture {
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
    make_private_dir(&plans_dir);

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
                        efforts: vec!["high".to_string(), "medium".to_string()],
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
        .expect("upsert profile-coworker");

    let store = CoworkerWorkflowStore::new(&plans_dir).expect("open CoworkerWorkflowStore");

    GrantFixture {
        _tmp: tmp,
        repo_dir,
        base_commit,
        base_tree_sha,
        registry_dir,
        plans_dir,
        registry,
        store,
    }
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
        policy_id: "policy-planning-grant".to_string(),
        allow_rules: vec![PolicyAllowRule {
            tool_id: "tool-agy".to_string(),
            model_id: "model-argon".to_string(),
            efforts: vec!["high".to_string()],
            allow_no_effort: false,
            roles: vec![WorkflowRole::Orchestrator, WorkflowRole::Architect],
        }],
    }
}

fn sample_prepare_request(fixture: &GrantFixture) -> PreparePlanningGrantRequest {
    PreparePlanningGrantRequest {
        request_key: "req-grant-alpha-1".to_string(),
        binding_id: BINDING_A.to_string(),
        thread_uuid: THREAD_UUID_A.to_string(),
        repository_path: fixture.repo_dir.clone(),
        base_commit: fixture.base_commit.clone(),
        expected_base_tree_sha: Some(fixture.base_tree_sha.clone()),
        profile_id: "profile-coworker".to_string(),
        expected_registry_revision: Some(5),
        roles: vec![
            RolePreflightRequest::new(WorkflowRole::Orchestrator, Some(Difficulty::Hard)),
            RolePreflightRequest::new(WorkflowRole::Architect, Some(Difficulty::Hard)),
        ],
        policy: default_policy(),
        capabilities: PlanningReadOnlyCapabilities::strict_read_only(),
        budget: default_budget(),
        created_at_ms: 1_760_000_000_000,
        expires_at_ms: 1_760_003_600_000,
    }
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

#[test]
fn preview_and_readonly_queries_leave_directories_unchanged_without_lockfiles() {
    let fixture = setup_fixture();
    let lock_in_registry = fixture.registry_dir.join("registry.lock");
    if lock_in_registry.exists() {
        fs::remove_file(&lock_in_registry).expect("remove initial registry.lock");
    }

    let before_plans = list_dir_entries(&fixture.plans_dir);
    let before_registry = list_dir_entries(&fixture.registry_dir);
    assert!(before_plans.is_empty(), "plans_dir starts empty");
    assert!(
        !before_registry.contains("registry.lock"),
        "registry.lock removed before readonly preview"
    );

    let req = sample_prepare_request(&fixture);
    let preview = prepare_planning_grant_preview_from_dir_readonly(&fixture.registry_dir, &req)
        .expect("readonly preview from registry_dir");
    assert_eq!(
        preview.status,
        PlanningGrantPreviewStatus::ProposedCandidateNotGranted
    );
    assert!(!preview.grant_active);
    assert!(!preview.capabilities.code_execution_approval_granted);
    assert!(preview.capabilities.read_only_filesystem);
    assert_eq!(preview.registry_revision, 5);
    assert_eq!(preview.frozen_role_choices.len(), 2);
    for frozen in &preview.frozen_role_choices {
        assert_eq!(frozen.selected_choice.tool_id, "tool-agy");
        assert_eq!(frozen.selected_choice.model_id, "model-argon");
        assert_eq!(frozen.selected_choice.model_arg, "gemini-4-argon-high");
        assert_eq!(frozen.selected_choice.effort.as_deref(), Some("high"));
        assert_eq!(
            frozen.selected_choice.route,
            RoleExecutionRoute::HeadlessWorker
        );
    }

    let snapshot = fixture
        .registry
        .snapshot_readonly()
        .expect("snapshot_readonly");
    let store_preview = prepare_planning_grant_preview_with_store(&fixture.store, &snapshot, &req)
        .expect("preview with store");
    assert_eq!(store_preview, preview);

    let scope = TrustedGrantScope::new(BINDING_A, THREAD_UUID_A).expect("scope");
    let listed = list_planning_grant_summaries(&fixture.store, &scope).expect("list empty");
    assert!(listed.is_empty());
    let fetched = get_planning_grant_summary(
        &fixture.store,
        &scope,
        "pgr_00000000000000000000000000000000",
    )
    .expect("get missing");
    assert!(fetched.is_none());

    let after_plans = list_dir_entries(&fixture.plans_dir);
    let after_registry = list_dir_entries(&fixture.registry_dir);
    assert_eq!(
        after_plans, before_plans,
        "plans_dir must remain completely empty"
    );
    assert_eq!(
        after_registry, before_registry,
        "registry_dir must not gain registry.lock or extra files"
    );
}

#[test]
fn record_planning_grant_succeeds_and_identical_replay_is_idempotent() {
    let fixture = setup_fixture();
    let req = sample_prepare_request(&fixture);
    let preview = prepare_planning_grant_preview_from_dir_readonly(&fixture.registry_dir, &req)
        .expect("prepare preview");

    let record_req = RecordPlanningGrantRequest {
        request_key: "req-grant-alpha-1".to_string(),
        expected_ledger_revision: Some(0),
        confirmed_preview_digest: preview.preview_digest.clone(),
        preview: preview.clone(),
        attestation: LocalOperatorGrantAttestation::attested(),
    };

    let summary =
        record_planning_grant_from_registry_dir(&fixture.store, &fixture.registry_dir, &record_req)
            .expect("record planning grant");
    assert!(summary.grant_id.starts_with("pgr_"));
    assert_eq!(summary.ledger_revision, 1);
    assert_eq!(summary.status, PlanningGrantStatus::Active);
    assert!(summary.operator_attested);
    assert!(!summary.native_per_call_identity_verified);
    assert_eq!(summary.reservation_count, 0);
    assert_eq!(summary.policy_digest, preview.policy_digest);
    assert_eq!(summary.project_base, preview.project_base);

    // Identical replay with the same request_key (even with expected_ledger_revision: Some(0))
    // returns the existing grant without incrementing ledger_revision.
    let replayed =
        record_planning_grant_from_registry_dir(&fixture.store, &fixture.registry_dir, &record_req)
            .expect("idempotent replay must succeed");
    assert_eq!(replayed, summary);

    // Passing the same confirmed preview with a different request_key to mint a second full-budget
    // grant must be rejected.
    let changed_key_err = record_planning_grant_from_registry_dir(
        &fixture.store,
        &fixture.registry_dir,
        &RecordPlanningGrantRequest {
            request_key: "req-grant-alpha-2".to_string(),
            expected_ledger_revision: None,
            confirmed_preview_digest: preview.preview_digest.clone(),
            preview: preview.clone(),
            attestation: LocalOperatorGrantAttestation::attested(),
        },
    )
    .expect_err("changing request_key after preview must be rejected");
    assert_eq!(
        changed_key_err.error_code(),
        NativeErrorCode::ValidationFailed
    );

    let scope = TrustedGrantScope::new(BINDING_A, THREAD_UUID_A).expect("scope");
    let listed = list_planning_grant_summaries(&fixture.store, &scope).expect("list summaries");
    assert_eq!(listed, vec![summary.clone()]);
    let got = get_planning_grant_summary(&fixture.store, &scope, &summary.grant_id)
        .expect("get summary")
        .expect("summary present");
    assert_eq!(got, summary);

    // Reusing the same request_key with a different expiry must fail closed.
    let mut different_req = req.clone();
    different_req.expires_at_ms += 60_000;
    let different_preview =
        prepare_planning_grant_preview_from_dir_readonly(&fixture.registry_dir, &different_req)
            .expect("prepare second preview");
    let conflicting_replay = RecordPlanningGrantRequest {
        request_key: "req-grant-alpha-1".to_string(),
        expected_ledger_revision: None,
        confirmed_preview_digest: different_preview.preview_digest.clone(),
        preview: different_preview,
        attestation: LocalOperatorGrantAttestation::attested(),
    };
    let err = record_planning_grant_from_registry_dir(
        &fixture.store,
        &fixture.registry_dir,
        &conflicting_replay,
    )
    .expect_err("conflicting request_key replay must fail");
    assert_eq!(err.error_code(), NativeErrorCode::ValidationFailed);
}

#[test]
fn tampered_expiry_model_effort_caps_policy_project_and_stale_registry_after_preview_are_rejected()
{
    let fixture = setup_fixture();
    let snapshot = fixture.registry.snapshot_readonly().expect("snapshot");
    let req = sample_prepare_request(&fixture);
    let preview = prepare_planning_grant_preview(&snapshot, &req).expect("preview");

    // 0. Changed request_key on preview after preparation.
    let mut bad_key_preview = preview.clone();
    bad_key_preview.request_key = "req-tamper-key".to_string();
    let err = record_planning_grant(
        &fixture.store,
        &snapshot,
        &RecordPlanningGrantRequest {
            request_key: "req-tamper-key".to_string(),
            expected_ledger_revision: Some(0),
            confirmed_preview_digest: preview.preview_digest.clone(),
            preview: bad_key_preview,
            attestation: LocalOperatorGrantAttestation::attested(),
        },
    )
    .expect_err("tampered request_key on preview must be rejected");
    assert_eq!(err.error_code(), NativeErrorCode::ValidationFailed);

    // 1. Changed expiry after preview.
    let mut bad_expiry = preview.clone();
    bad_expiry.expires_at_ms += 1_000;
    let err = record_planning_grant(
        &fixture.store,
        &snapshot,
        &RecordPlanningGrantRequest {
            request_key: preview.request_key.clone(),
            expected_ledger_revision: Some(0),
            confirmed_preview_digest: preview.preview_digest.clone(),
            preview: bad_expiry,
            attestation: LocalOperatorGrantAttestation::attested(),
        },
    )
    .expect_err("tampered expiry must be rejected");
    assert_eq!(err.error_code(), NativeErrorCode::ValidationFailed);

    // 2. Changed model after preview (even if caller recomputes preview_digest on forged struct).
    let mut bad_model = preview.clone();
    bad_model.frozen_role_choices[0].selected_choice.model_id = "model-deepseek".to_string();
    bad_model.frozen_role_choices[0].selected_choice.model_arg =
        "opencode-go/deepseek-v4.1-flash".to_string();
    let err = record_planning_grant(
        &fixture.store,
        &snapshot,
        &RecordPlanningGrantRequest {
            request_key: preview.request_key.clone(),
            expected_ledger_revision: Some(0),
            confirmed_preview_digest: preview.preview_digest.clone(),
            preview: bad_model,
            attestation: LocalOperatorGrantAttestation::attested(),
        },
    )
    .expect_err("tampered model must be rejected");
    assert_eq!(err.error_code(), NativeErrorCode::ValidationFailed);

    // 3. Changed effort after preview.
    let mut bad_effort = preview.clone();
    bad_effort.frozen_role_choices[0].selected_choice.effort = Some("medium".to_string());
    let err = record_planning_grant(
        &fixture.store,
        &snapshot,
        &RecordPlanningGrantRequest {
            request_key: preview.request_key.clone(),
            expected_ledger_revision: Some(0),
            confirmed_preview_digest: preview.preview_digest.clone(),
            preview: bad_effort,
            attestation: LocalOperatorGrantAttestation::attested(),
        },
    )
    .expect_err("tampered effort must be rejected");
    assert_eq!(err.error_code(), NativeErrorCode::ValidationFailed);

    // 4. Changed capabilities after preview.
    let mut bad_caps = preview.clone();
    bad_caps.capabilities.write_worktree_allowed = true;
    let err = record_planning_grant(
        &fixture.store,
        &snapshot,
        &RecordPlanningGrantRequest {
            request_key: preview.request_key.clone(),
            expected_ledger_revision: Some(0),
            confirmed_preview_digest: preview.preview_digest.clone(),
            preview: bad_caps,
            attestation: LocalOperatorGrantAttestation::attested(),
        },
    )
    .expect_err("tampered capabilities must be rejected");
    assert_eq!(err.error_code(), NativeErrorCode::SecurityViolation);

    // 5. Changed policy after preview.
    let mut bad_policy = preview.clone();
    bad_policy.policy.policy_id = "policy-mutated".to_string();
    bad_policy.policy_digest = bad_policy.policy.digest().expect("recompute policy digest");
    let err = record_planning_grant(
        &fixture.store,
        &snapshot,
        &RecordPlanningGrantRequest {
            request_key: preview.request_key.clone(),
            expected_ledger_revision: Some(0),
            confirmed_preview_digest: preview.preview_digest.clone(),
            preview: bad_policy,
            attestation: LocalOperatorGrantAttestation::attested(),
        },
    )
    .expect_err("tampered policy must be rejected");
    assert_eq!(err.error_code(), NativeErrorCode::ValidationFailed);

    // 6. Changed budget after preview.
    let mut bad_budget = preview.clone();
    bad_budget.budget.max_total_calls = 12;
    let err = record_planning_grant(
        &fixture.store,
        &snapshot,
        &RecordPlanningGrantRequest {
            request_key: preview.request_key.clone(),
            expected_ledger_revision: Some(0),
            confirmed_preview_digest: preview.preview_digest.clone(),
            preview: bad_budget,
            attestation: LocalOperatorGrantAttestation::attested(),
        },
    )
    .expect_err("tampered budget must be rejected");
    assert_eq!(err.error_code(), NativeErrorCode::ValidationFailed);

    // 7. Changed project_base after preview.
    let mut bad_project = preview.clone();
    bad_project.project_base.base_commit = "0123456789abcdef0123456789abcdef01234567".to_string();
    let err = record_planning_grant(
        &fixture.store,
        &snapshot,
        &RecordPlanningGrantRequest {
            request_key: preview.request_key.clone(),
            expected_ledger_revision: Some(0),
            confirmed_preview_digest: preview.preview_digest.clone(),
            preview: bad_project,
            attestation: LocalOperatorGrantAttestation::attested(),
        },
    )
    .expect_err("tampered project_base must be rejected");
    assert_eq!(err.error_code(), NativeErrorCode::ValidationFailed);

    // 8. Changed live registry after preview (revision bump + profile effort change).
    fixture
        .registry
        .apply(
            5,
            RegistryChange::UpsertProfile {
                profile: WorkflowProfile {
                    decision_advisor: None,
                    id: "profile-coworker".to_string(),
                    name: "Coworker Headless Profile Updated".to_string(),
                    assignments: vec![
                        RoleAssignment {
                            role: WorkflowRole::Orchestrator,
                            difficulty: Some(Difficulty::Hard),
                            choice: ModelChoice {
                                model_id: "model-argon".to_string(),
                                effort: Some("medium".to_string()),
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
        .expect("mutate registry after preview");

    let err = record_planning_grant_from_registry_dir(
        &fixture.store,
        &fixture.registry_dir,
        &RecordPlanningGrantRequest {
            request_key: preview.request_key.clone(),
            expected_ledger_revision: Some(0),
            confirmed_preview_digest: preview.preview_digest.clone(),
            preview,
            attestation: LocalOperatorGrantAttestation::attested(),
        },
    )
    .expect_err("stale registry revision after preview must be rejected");
    assert_eq!(err.error_code(), NativeErrorCode::ValidationFailed);
}

#[test]
fn missing_or_dishonest_operator_attestation_is_rejected() {
    let fixture = setup_fixture();
    let snapshot = fixture.registry.snapshot_readonly().expect("snapshot");
    let req = sample_prepare_request(&fixture);
    let preview = prepare_planning_grant_preview(&snapshot, &req).expect("preview");

    // 1. Missing operator_attested = false.
    let unattested_err = record_planning_grant(
        &fixture.store,
        &snapshot,
        &RecordPlanningGrantRequest {
            request_key: preview.request_key.clone(),
            expected_ledger_revision: Some(0),
            confirmed_preview_digest: preview.preview_digest.clone(),
            preview: preview.clone(),
            attestation: LocalOperatorGrantAttestation::unattested(),
        },
    )
    .expect_err("missing operator attestation must be rejected");
    assert_eq!(
        unattested_err.error_code(),
        NativeErrorCode::UnauthorizedOperator
    );

    // 2. Model/MCP prose self-approval claim must be rejected.
    let prose_err = record_planning_grant(
        &fixture.store,
        &snapshot,
        &RecordPlanningGrantRequest {
            request_key: preview.request_key.clone(),
            expected_ledger_revision: Some(0),
            confirmed_preview_digest: preview.preview_digest.clone(),
            preview: preview.clone(),
            attestation: LocalOperatorGrantAttestation {
                operator_attested: true,
                derived_from_model_or_mcp_prose: true,
                native_per_call_identity_verified: false,
                note: None,
            },
        },
    )
    .expect_err("model/MCP prose self-approval must be rejected");
    assert_eq!(
        prose_err.error_code(),
        NativeErrorCode::UnauthorizedOperator
    );

    // 3. Dishonest native_per_call_identity_verified = true must be rejected.
    let fake_id_err = record_planning_grant(
        &fixture.store,
        &snapshot,
        &RecordPlanningGrantRequest {
            request_key: preview.request_key.clone(),
            expected_ledger_revision: Some(0),
            confirmed_preview_digest: preview.preview_digest.clone(),
            preview,
            attestation: LocalOperatorGrantAttestation {
                operator_attested: true,
                derived_from_model_or_mcp_prose: false,
                native_per_call_identity_verified: true,
                note: None,
            },
        },
    )
    .expect_err("native_per_call_identity_verified = true must be rejected");
    assert_eq!(fake_id_err.error_code(), NativeErrorCode::ValidationFailed);

    let scope = TrustedGrantScope::new(BINDING_A, THREAD_UUID_A).expect("scope");
    assert!(
        list_planning_grant_summaries(&fixture.store, &scope)
            .expect("list")
            .is_empty(),
        "no grant may be persisted after rejected attestations"
    );
}

#[test]
fn wrong_binding_or_thread_revoke_is_rejected_and_rightful_revoke_works() {
    let fixture = setup_fixture();
    let snapshot = fixture.registry.snapshot_readonly().expect("snapshot");
    let req = sample_prepare_request(&fixture);
    let preview = prepare_planning_grant_preview(&snapshot, &req).expect("preview");

    let recorded = record_planning_grant(
        &fixture.store,
        &snapshot,
        &RecordPlanningGrantRequest {
            request_key: preview.request_key.clone(),
            expected_ledger_revision: Some(0),
            confirmed_preview_digest: preview.preview_digest.clone(),
            preview,
            attestation: LocalOperatorGrantAttestation::attested(),
        },
    )
    .expect("record grant");
    assert_eq!(recorded.ledger_revision, 1);

    let scope_a = TrustedGrantScope::new(BINDING_A, THREAD_UUID_A).expect("scope A");
    let scope_b = TrustedGrantScope::new(BINDING_B, THREAD_UUID_B).expect("scope B");

    // 1. Caller attempts to pass BINDING_B in request while scoped to BINDING_A.
    let cross_binding_err = revoke_planning_grant_scoped(
        &fixture.store,
        &scope_a,
        &RevokePlanningGrantRequest {
            binding_id: BINDING_B.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            grant_id: recorded.grant_id.clone(),
            expected_ledger_revision: Some(1),
            revoked_at_ms: 1_760_000_050_000,
            reason: "Attempt cross-binding revoke".to_string(),
        },
    )
    .expect_err("mismatched request.binding_id must be rejected");
    assert_eq!(
        cross_binding_err.error_code(),
        NativeErrorCode::RebindRejected
    );

    // 2. Caller scoped to BINDING_B attempts to revoke BINDING_A's grant_id.
    let other_scope_err = revoke_planning_grant_scoped(
        &fixture.store,
        &scope_b,
        &RevokePlanningGrantRequest {
            binding_id: BINDING_B.to_string(),
            thread_uuid: THREAD_UUID_B.to_string(),
            grant_id: recorded.grant_id.clone(),
            expected_ledger_revision: Some(1),
            revoked_at_ms: 1_760_000_050_000,
            reason: "Attempt foreign grant revoke".to_string(),
        },
    )
    .expect_err("foreign binding cannot revoke BINDING_A grant");
    assert_eq!(
        other_scope_err.error_code(),
        NativeErrorCode::ValidationFailed
    );
    assert!(
        !fixture
            .store
            .ledger_path_for_binding(BINDING_B)
            .expect("path B")
            .exists(),
        "failed revoke on uninitialized BINDING_B must not create a ledger file"
    );

    // 3. Wrong thread_uuid in request or scope is rejected.
    let wrong_thread_err = revoke_planning_grant_scoped(
        &fixture.store,
        &scope_a,
        &RevokePlanningGrantRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_B.to_string(),
            grant_id: recorded.grant_id.clone(),
            expected_ledger_revision: Some(1),
            revoked_at_ms: 1_760_000_050_000,
            reason: "Wrong thread".to_string(),
        },
    )
    .expect_err("wrong thread_uuid must be rejected");
    assert_eq!(
        wrong_thread_err.error_code(),
        NativeErrorCode::RebindRejected
    );

    // 4. Stale or missing expected_ledger_revision is rejected.
    let stale_rev_err = revoke_planning_grant_scoped(
        &fixture.store,
        &scope_a,
        &RevokePlanningGrantRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            grant_id: recorded.grant_id.clone(),
            expected_ledger_revision: Some(99),
            revoked_at_ms: 1_760_000_050_000,
            reason: "Stale revision".to_string(),
        },
    )
    .expect_err("stale revision must be rejected");
    assert_eq!(
        stale_rev_err.error_code(),
        NativeErrorCode::ValidationFailed
    );

    // 5. Rightful revoke succeeds and increments ledger_revision to 2.
    let rightful_revoke = RevokePlanningGrantRequest {
        binding_id: BINDING_A.to_string(),
        thread_uuid: THREAD_UUID_A.to_string(),
        grant_id: recorded.grant_id.clone(),
        expected_ledger_revision: Some(1),
        revoked_at_ms: 1_760_000_050_000,
        reason: "Operator revoked standing grant after planning phase".to_string(),
    };
    let revoked_summary = revoke_planning_grant_scoped(&fixture.store, &scope_a, &rightful_revoke)
        .expect("rightful revoke succeeds");
    assert_eq!(revoked_summary.status, PlanningGrantStatus::Revoked);
    assert_eq!(revoked_summary.ledger_revision, 2);
    assert_eq!(revoked_summary.revoked_at_ms, Some(1_760_000_050_000));
    assert_eq!(
        revoked_summary.revoked_reason.as_deref(),
        Some("Operator revoked standing grant after planning phase")
    );

    // Identical revoke replay is idempotent and keeps ledger_revision = 2.
    let replayed_revoke = revoke_planning_grant_scoped(&fixture.store, &scope_a, &rightful_revoke)
        .expect("idempotent revoke replay");
    assert_eq!(replayed_revoke, revoked_summary);
}

#[test]
fn codex_readonly_planning_admission_succeeds_for_sol_xhigh_while_follower_and_coder_remain_rejected(
) {
    let fixture = setup_fixture();

    // 1. Requesting WorkflowRole::Follower or WorkflowRole::Coder in a planning grant is rejected.
    let snapshot = fixture.registry.snapshot_readonly().expect("snapshot");
    for forbidden_role in [WorkflowRole::Follower, WorkflowRole::Coder] {
        let mut bad_role_req = sample_prepare_request(&fixture);
        bad_role_req
            .roles
            .push(RolePreflightRequest::new(forbidden_role, None));
        let err = prepare_planning_grant_preview(&snapshot, &bad_role_req)
            .expect_err("Follower and Coder roles must be rejected in planning grant");
        assert_eq!(err.error_code(), NativeErrorCode::ValidationFailed);
    }

    // 2. Register default-style Codex gpt-6.1-sol model with xhigh/high efforts and a profile
    // assigning Orchestrator (and Architect) to Codex gpt-6.1-sol/xhigh.
    fixture
        .registry
        .apply(
            5,
            RegistryChange::UpsertModel {
                model: ModelDefinition {
                    id: "gpt-6.1-sol".to_string(),
                    tool_id: "tool-codex".to_string(),
                    label: "GPT-6.1 Sol".to_string(),
                    model_arg: "gpt-6.1-sol".to_string(),
                    efforts: vec!["high".to_string(), "xhigh".to_string()],
                    allow_no_effort: false,
                },
            },
        )
        .expect("upsert gpt-6.1-sol model");

    fixture
        .registry
        .apply(
            6,
            RegistryChange::UpsertProfile {
                profile: WorkflowProfile {
                    decision_advisor: None,
                    id: "profile-codex-sol-planning".to_string(),
                    name: "Default Codex Sol Read-Only Planning Profile".to_string(),
                    assignments: vec![
                        RoleAssignment {
                            role: WorkflowRole::Follower,
                            difficulty: None,
                            choice: ModelChoice {
                                model_id: "gpt-6.1-sol".to_string(),
                                effort: Some("xhigh".to_string()),
                            },
                        },
                        RoleAssignment {
                            role: WorkflowRole::Orchestrator,
                            difficulty: Some(Difficulty::Hard),
                            choice: ModelChoice {
                                model_id: "gpt-6.1-sol".to_string(),
                                effort: Some("xhigh".to_string()),
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
        .expect("upsert codex sol planning profile");

    let snap_with_sol = fixture.registry.snapshot_readonly().expect("snapshot");
    assert_eq!(snap_with_sol.revision, 7);

    let mut codex_req = sample_prepare_request(&fixture);
    codex_req.request_key = "req-grant-codex-sol-1".to_string();
    codex_req.profile_id = "profile-codex-sol-planning".to_string();
    codex_req.expected_registry_revision = Some(7);
    codex_req.policy = ExecutionPolicy {
        schema_version: 1,
        policy_id: "policy-codex-sol-planning".to_string(),
        allow_rules: vec![
            PolicyAllowRule {
                tool_id: "tool-codex".to_string(),
                model_id: "gpt-6.1-sol".to_string(),
                efforts: vec!["xhigh".to_string()],
                allow_no_effort: false,
                roles: vec![WorkflowRole::Orchestrator],
            },
            PolicyAllowRule {
                tool_id: "tool-agy".to_string(),
                model_id: "model-argon".to_string(),
                efforts: vec!["high".to_string()],
                allow_no_effort: false,
                roles: vec![WorkflowRole::Architect],
            },
        ],
    };

    let preview = prepare_planning_grant_preview(&snap_with_sol, &codex_req).expect(
        "read-only Codex gpt-6.1-sol/xhigh Orchestrator planning grant preview must succeed",
    );
    assert_eq!(
        preview.status,
        PlanningGrantPreviewStatus::ProposedCandidateNotGranted
    );
    assert!(!preview.grant_active);
    assert!(!preview.capabilities.write_worktree_allowed);
    assert!(!preview.capabilities.code_execution_approval_granted);

    let orch_choice = preview
        .frozen_role_choices
        .iter()
        .find(|c| c.role == WorkflowRole::Orchestrator)
        .expect("frozen Orchestrator choice");
    assert_eq!(orch_choice.selected_choice.tool_id, "tool-codex");
    assert_eq!(orch_choice.selected_choice.adapter, ToolAdapter::CodexCli);
    assert_eq!(
        orch_choice.selected_choice.route,
        RoleExecutionRoute::HeadlessWorker
    );
    assert_eq!(orch_choice.selected_choice.model_id, "gpt-6.1-sol");
    assert_eq!(orch_choice.selected_choice.model_arg, "gpt-6.1-sol");
    assert_eq!(orch_choice.selected_choice.effort.as_deref(), Some("xhigh"));

    let recorded = record_planning_grant(
        &fixture.store,
        &snap_with_sol,
        &RecordPlanningGrantRequest {
            request_key: preview.request_key.clone(),
            expected_ledger_revision: Some(0),
            confirmed_preview_digest: preview.preview_digest.clone(),
            preview: preview.clone(),
            attestation: LocalOperatorGrantAttestation::attested(),
        },
    )
    .expect("record read-only Codex planning grant");
    assert_eq!(recorded.status, PlanningGrantStatus::Active);
    assert_eq!(recorded.registry_revision, 7);

    // 3. Policy mismatch (allowing only `high` when profile selects `xhigh`) is rejected before grant creation.
    let mut mismatch_policy_req = codex_req.clone();
    mismatch_policy_req.policy.allow_rules[0].efforts = vec!["high".to_string()];
    let err = prepare_planning_grant_preview(&snap_with_sol, &mismatch_policy_req)
        .expect_err("policy effort mismatch for Codex Orchestrator must be rejected");
    assert_eq!(err.error_code(), NativeErrorCode::ValidationFailed);

    // 4. Stale expected_registry_revision is rejected.
    let mut stale_rev_req = codex_req.clone();
    stale_rev_req.expected_registry_revision = Some(6);
    let err = prepare_planning_grant_preview(&snap_with_sol, &stale_rev_req)
        .expect_err("stale expected_registry_revision must be rejected");
    assert_eq!(err.error_code(), NativeErrorCode::ValidationFailed);

    // 5. Codex policy rule with allow_no_effort = true or Coder/Follower role is rejected.
    let mut no_effort_req = codex_req.clone();
    no_effort_req.policy.allow_rules[0].allow_no_effort = true;
    let err = prepare_planning_grant_preview(&snap_with_sol, &no_effort_req)
        .expect_err("Codex allow_no_effort in planning grant policy must be rejected");
    assert_eq!(err.error_code(), NativeErrorCode::ValidationFailed);

    let mut coder_rule_req = codex_req;
    coder_rule_req.policy.allow_rules[0]
        .roles
        .push(WorkflowRole::Coder);
    let err = prepare_planning_grant_preview(&snap_with_sol, &coder_rule_req)
        .expect_err("Codex Coder role in planning grant policy must be rejected");
    assert_eq!(err.error_code(), NativeErrorCode::ValidationFailed);
}
