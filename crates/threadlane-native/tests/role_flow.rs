use std::fs::{self, File};
use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tempfile::TempDir;
use threadlane_authority::{AuthorityStore, OperatorApprovalRequest};
use threadlane_host::{
    ApprovedHost, ApprovedHostClient, ApprovedHostConfig, HostPhase, TaskExecutionStatus,
};
use threadlane_native::{
    preflight_from_dir_readonly, ClosureSubjectSource, ConversationChangeKind,
    ConversationChangeRequest, ConversationChangeRoute, ConversationChangeStore,
    ConversationDeliveryRequest, CreateRoleFlowCycleRequest, Difficulty, ExecutionPolicy,
    HandoffStatus, NativeErrorCode, PolicyAllowRule, PreflightRequest, ReconcileRoleHandoffRequest,
    RegistryStore, ReservePackageClosureRequest, RoleExecutionRoute, RoleFlowStore,
    RoleHandoffRequest, RolePreflightRequest, ToolAdapter, WorkflowProfile, WorkflowRole,
};
use threadlane_workers::{
    resolve_exact_commit_sha, resolve_exact_tree_sha, run_sanitized_git, ExecutionBudgets,
    DEFAULT_GIT_COMMAND_TIMEOUT_MS,
};
use threadlane_workflow::RegistryChange;
use threadlane_workflow::{
    compile_plan_revision, ModelChoice, ModelDefinition, PlanChange, PlanStore, PlanTask,
    RoleAssignment, ToolDefinition,
};

const THREAD_UUID_A: &str = "550e8400-e29b-41d4-a716-446655440000";
const THREAD_UUID_B: &str = "6ba7b810-9dad-11d1-80b4-00c04fd430c8";
const BINDING_A: &str = "bnd_role_flow_alpha";
const BINDING_B: &str = "bnd_role_flow_beta";

struct RoleFlowFixture {
    _tmp: TempDir,
    root: PathBuf,
    repo_dir: PathBuf,
    base_commit: String,
    base_tree_sha: String,
    registry_dir: PathBuf,
    plans_dir: PathBuf,
    authority_dir: PathBuf,
    sessions_dir: PathBuf,
    agy_count_file: PathBuf,
    oc_count_file: PathBuf,
    registry: Arc<RegistryStore>,
    plans: Arc<PlanStore>,
    authority: Arc<AuthorityStore>,
}

fn write_fake_agy_script(script_path: &Path, count_file: &Path) {
    if let Some(parent) = script_path.parent() {
        fs::create_dir_all(parent).expect("create script parent dir");
    }
    let script = format!(
        r#"#!/bin/sh
set -eu
COUNT_FILE="{count_file}"
c=0
if [ -f "$COUNT_FILE" ]; then
    c=$(cat "$COUNT_FILE")
fi
c=$((c + 1))
printf "%s" "$c" > "$COUNT_FILE"

MODEL=""
EFFORT=""
while [ "$#" -gt 0 ]; do
    case "$1" in
        --model)
            MODEL="$2"
            shift 2
            ;;
        --effort)
            EFFORT="$2"
            shift 2
            ;;
        *)
            shift 1
            ;;
    esac
done

printf '{{"type":"init","session_id":"sess-agy-rf","model":"%s","effort":"%s"}}\n' "$MODEL" "$EFFORT"
if [ -d "src" ]; then
    printf "// updated by fake-agy\npub fn alpha() -> u32 {{ 42 }}\n" > "src/lib.rs"
fi
printf '{{"type":"result","subtype":"success","session_id":"sess-agy-rf","model":"%s","effort":"%s","summary":"Completed bounded preview"}}\n' "$MODEL" "$EFFORT"
exit 0
"#,
        count_file = count_file.display(),
    );
    let mut f = File::create(script_path).expect("create fake-agy");
    f.write_all(script.as_bytes()).expect("write fake-agy");
    f.sync_all().expect("sync fake-agy");
    fs::set_permissions(script_path, fs::Permissions::from_mode(0o755))
        .expect("chmod 0755 fake-agy");
}

fn init_git_repo(repo_dir: &Path) -> (String, String) {
    fs::create_dir_all(repo_dir.join("src")).expect("create repo src");
    fs::write(repo_dir.join("src/lib.rs"), "pub fn alpha() -> u32 { 1 }\n")
        .expect("write src/lib.rs");
    fs::write(repo_dir.join("src/extra.rs"), "pub fn extra() {}\n").expect("write src/extra.rs");

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

fn setup_fixture() -> RoleFlowFixture {
    let tmp = TempDir::new().expect("tempdir");
    let root = fs::canonicalize(tmp.path()).expect("canonicalize temp root");

    let repo_dir = root.join("repo");
    let (base_commit, base_tree_sha) = init_git_repo(&repo_dir);

    let bin_dir = root.join("bin");
    let fake_agy_exe = bin_dir.join("fake-agy");
    let fake_oc_exe = bin_dir.join("fake-opencode");
    let agy_count_file = root.join("agy-invocations.txt");
    let oc_count_file = root.join("oc-invocations.txt");

    write_fake_agy_script(&fake_agy_exe, &agy_count_file);
    write_fake_agy_script(&fake_oc_exe, &oc_count_file);

    let registry_dir = root.join("registry-store");
    let plans_dir = root.join("plans-store");
    let authority_dir = root.join("authority-store");
    let sessions_dir = root.join("sessions-store");

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
                    executable_path: Some(fake_agy_exe),
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
                    executable_path: Some(fake_oc_exe),
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
                        efforts: vec!["medium".to_string(), "high".to_string()],
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
                        label: "Native Codex Follower".to_string(),
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
                    id: "profile-main".to_string(),
                    name: "Main M4 Profile".to_string(),
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
                            role: WorkflowRole::Coder,
                            difficulty: Some(Difficulty::Easy),
                            choice: ModelChoice {
                                model_id: "model-deepseek".to_string(),
                                effort: Some("max".to_string()),
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
                        RoleAssignment {
                            role: WorkflowRole::Reviewer,
                            difficulty: Some(Difficulty::Hard),
                            choice: ModelChoice {
                                model_id: "model-codex-native".to_string(),
                                effort: Some("max".to_string()),
                            },
                        },
                        RoleAssignment {
                            role: WorkflowRole::Closure,
                            difficulty: Some(Difficulty::Extreme),
                            choice: ModelChoice {
                                model_id: "model-codex-native".to_string(),
                                effort: Some("max".to_string()),
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

    RoleFlowFixture {
        _tmp: tmp,
        root,
        repo_dir,
        base_commit,
        base_tree_sha,
        registry_dir,
        plans_dir,
        authority_dir,
        sessions_dir,
        agy_count_file,
        oc_count_file,
        registry,
        plans,
        authority,
    }
}

fn default_policy() -> ExecutionPolicy {
    let policy = ExecutionPolicy {
        schema_version: 1,
        policy_id: "policy-m4-active".to_string(),
        allow_rules: vec![
            PolicyAllowRule {
                tool_id: "tool-agy".to_string(),
                model_id: "model-argon".to_string(),
                efforts: vec!["high".to_string()],
                allow_no_effort: false,
                roles: vec![],
            },
            PolicyAllowRule {
                tool_id: "tool-oc".to_string(),
                model_id: "model-deepseek".to_string(),
                efforts: vec!["max".to_string()],
                allow_no_effort: false,
                roles: vec![],
            },
            PolicyAllowRule {
                tool_id: "tool-codex".to_string(),
                model_id: "model-codex-native".to_string(),
                efforts: vec!["high".to_string(), "max".to_string()],
                allow_no_effort: false,
                roles: vec![],
            },
        ],
    };
    policy.validate().expect("valid default policy");
    policy
}

fn default_tasks() -> Vec<PlanTask> {
    vec![
        PlanTask {
            id: "task-coder-hard-1".to_string(),
            title: "Hard Coder Slice 1".to_string(),
            instruction: "Implement core alpha logic in src/lib.rs".to_string(),
            role: WorkflowRole::Coder,
            difficulty: Difficulty::Hard,
            depends_on: Vec::new(),
            allowed_write_paths: vec!["src/lib.rs".to_string()],
        },
        PlanTask {
            id: "task-coder-hard-2".to_string(),
            title: "Hard Coder Slice 2".to_string(),
            instruction: "Implement second hard coder slice in src/extra.rs".to_string(),
            role: WorkflowRole::Coder,
            difficulty: Difficulty::Hard,
            depends_on: Vec::new(),
            allowed_write_paths: vec!["src/extra.rs".to_string()],
        },
        PlanTask {
            id: "task-native-follower".to_string(),
            title: "Native Follower Coordination".to_string(),
            instruction: "Coordinate operator status in native Codex chat".to_string(),
            role: WorkflowRole::Follower,
            difficulty: Difficulty::Medium,
            depends_on: Vec::new(),
            allowed_write_paths: vec!["src/lib.rs".to_string()],
        },
        PlanTask {
            id: "task-closure".to_string(),
            title: "Package Closure Review".to_string(),
            instruction: "Perform single closing review of the candidate tree".to_string(),
            role: WorkflowRole::Closure,
            difficulty: Difficulty::Extreme,
            depends_on: Vec::new(),
            allowed_write_paths: vec!["src/lib.rs".to_string()],
        },
    ]
}

fn create_approved_plan_at_commit(
    fixture: &RoleFlowFixture,
    plan_id: &str,
    binding_id: &str,
    thread_uuid: &str,
    base_commit: &str,
    tasks: Vec<PlanTask>,
) -> String {
    let reg_snap = fixture.registry.snapshot().expect("registry snapshot");
    let draft = fixture
        .plans
        .change_at(
            0,
            PlanChange::SaveDraft {
                plan_id: plan_id.to_string(),
                binding_id: binding_id.to_string(),
                profile_id: "profile-main".to_string(),
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

    let compiled = compile_plan_revision(&published, &reg_snap).expect("compile published plan");
    let approved_task_ids: Vec<String> = tasks.iter().map(|t| t.id.clone()).collect();
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
                base_commit: base_commit.to_string(),
                approved_task_ids,
                approved_write_paths: vec!["src/".to_string()],
                max_parallel: 2,
                budgets: ExecutionBudgets::default(),
            },
            1_700_000_001_000,
        )
        .expect("approve plan");
    summary.approval_id
}

fn read_invocation_count(path: &Path) -> u32 {
    match fs::read_to_string(path) {
        Ok(s) => s.trim().parse::<u32>().unwrap_or(0),
        Err(_) => 0,
    }
}

async fn wait_for_task_ready(
    client: &ApprovedHostClient,
    approval_id: &str,
    task_id: &str,
) -> threadlane_host::TaskStateSnapshot {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let snap = client
            .get_task(approval_id, task_id)
            .await
            .expect("get_task while waiting");
        if snap.status == TaskExecutionStatus::ReadyForReview {
            return snap;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for task {task_id} to reach ReadyForReview (current: {:?})",
            snap.status
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

#[test]
fn readonly_store_and_preflight_create_no_files_and_create_cycle_enforces_cas_and_idempotency() {
    let fixture = setup_fixture();
    let policy = default_policy();

    // Remove registry.lock if created by RegistryStore::open during fixture setup and verify
    // preflight_from_dir_readonly does NOT recreate registry.lock.
    let reg_lock = fixture.registry_dir.join("registry.lock");
    let _ = fs::remove_file(&reg_lock);
    let report = preflight_from_dir_readonly(
        &fixture.registry_dir,
        &PreflightRequest {
            profile_id: Some("profile-main".to_string()),
            roles: vec![RolePreflightRequest {
                role: WorkflowRole::Coder,
                difficulty: Some(Difficulty::Hard),
            }],
        },
        &policy,
    )
    .expect("readonly preflight");
    assert!(report.ready);
    assert!(
        !reg_lock.exists(),
        "preflight_from_dir_readonly must not create registry.lock"
    );

    let store = RoleFlowStore::new(&fixture.plans_dir).expect("open RoleFlowStore");
    let ledger_path = store
        .ledger_path_for_binding(BINDING_A)
        .expect("ledger path");
    let lock_path = store.lock_path_for_binding(BINDING_A).expect("lock path");
    assert!(!ledger_path.exists());
    assert!(!lock_path.exists());

    // Read-only queries must not create ledger or lock files.
    let snap = store.get_binding_snapshot(BINDING_A).expect("snapshot");
    assert_eq!(snap.ledger_revision, 0);
    assert!(snap.cycles.is_empty());
    assert!(store.list_cycles(BINDING_A).expect("list").is_empty());
    assert!(store
        .get_cycle(BINDING_A, "flw_00000000000000000000000000000000")
        .expect("get_cycle")
        .is_none());
    assert!(store
        .list_closure_reservations(BINDING_A)
        .expect("list_closures")
        .is_empty());
    assert!(!ledger_path.exists());
    assert!(!lock_path.exists());

    let approval_id = create_approved_plan_at_commit(
        &fixture,
        "plan-alpha",
        BINDING_A,
        THREAD_UUID_A,
        &fixture.base_commit,
        default_tasks(),
    );

    // Multi-task cycle with two Hard Coder tasks + Native Follower + Closure task succeeds (deduplicates role preflight).
    let req = CreateRoleFlowCycleRequest {
        binding_id: BINDING_A.to_string(),
        thread_uuid: THREAD_UUID_A.to_string(),
        request_key: "req-cycle-1".to_string(),
        expected_ledger_revision: Some(0),
        approval_id: approval_id.clone(),
        task_ids: vec![
            "task-coder-hard-1".to_string(),
            "task-coder-hard-2".to_string(),
            "task-native-follower".to_string(),
        ],
        policy: policy.clone(),
    };
    let cycle = store
        .create_cycle(&req, &fixture.authority)
        .expect("create_cycle");
    assert_eq!(cycle.cycle_revision, 1);
    assert_eq!(cycle.tasks.len(), 3);
    assert_eq!(cycle.base_tree_sha, fixture.base_tree_sha);
    assert!(ledger_path.exists());
    assert!(lock_path.exists());

    // Idempotent replay with same request_key returns the exact same cycle without incrementing revision.
    let replay = store
        .create_cycle(&req, &fixture.authority)
        .expect("replay create_cycle");
    assert_eq!(replay, cycle);
    assert_eq!(
        store
            .get_binding_snapshot(BINDING_A)
            .expect("snap")
            .ledger_revision,
        1
    );

    // Reusing request_key with different task_ids fails with ValidationFailed.
    let mut bad_replay = req.clone();
    bad_replay.task_ids = vec!["task-coder-hard-1".to_string()];
    let conflict_err = store
        .create_cycle(&bad_replay, &fixture.authority)
        .expect_err("conflicting request_key must fail");
    assert_eq!(conflict_err.error_code(), NativeErrorCode::ValidationFailed);

    // Stale expected_ledger_revision fails.
    let mut stale_req = req.clone();
    stale_req.request_key = "req-cycle-stale".to_string();
    stale_req.expected_ledger_revision = Some(0);
    let stale_err = store
        .create_cycle(&stale_req, &fixture.authority)
        .expect_err("stale ledger revision must fail");
    assert_eq!(stale_err.error_code(), NativeErrorCode::ValidationFailed);
}

#[tokio::test]
async fn native_role_handoff_records_frozen_route_deduplicates_and_refuses_policy_scope_or_registry_drift(
) {
    let fixture = setup_fixture();
    let policy = default_policy();
    let store = RoleFlowStore::new(&fixture.plans_dir).expect("open RoleFlowStore");

    let approval_id = create_approved_plan_at_commit(
        &fixture,
        "plan-native-handoff",
        BINDING_A,
        THREAD_UUID_A,
        &fixture.base_commit,
        default_tasks(),
    );

    // Freeze two tasks sharing (WorkflowRole::Coder, Difficulty::Hard) plus a NativeFollower task.
    let cycle = store
        .create_cycle(
            &CreateRoleFlowCycleRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                request_key: "req-native-cycle-1".to_string(),
                expected_ledger_revision: Some(0),
                approval_id: approval_id.clone(),
                task_ids: vec![
                    "task-coder-hard-1".to_string(),
                    "task-coder-hard-2".to_string(),
                    "task-native-follower".to_string(),
                ],
                policy: policy.clone(),
            },
            &fixture.authority,
        )
        .expect("create_cycle with shared role/difficulty tasks");

    assert_eq!(cycle.tasks.len(), 3);
    assert_eq!(cycle.tasks[0].task_id, "task-coder-hard-1");
    assert_eq!(cycle.tasks[0].model_id, "model-argon");
    assert_eq!(cycle.tasks[0].model_arg, "gemini-4-argon-high");
    assert_eq!(cycle.tasks[0].effort.as_deref(), Some("high"));
    assert_eq!(cycle.tasks[0].write_paths, vec!["src/lib.rs".to_string()]);
    assert_eq!(cycle.tasks[1].task_id, "task-coder-hard-2");
    assert_eq!(cycle.tasks[1].model_id, "model-argon");
    assert_eq!(cycle.tasks[1].model_arg, "gemini-4-argon-high");
    assert_eq!(cycle.tasks[1].effort.as_deref(), Some("high"));
    assert_eq!(cycle.tasks[1].write_paths, vec!["src/extra.rs".to_string()]);
    assert_eq!(cycle.tasks[2].task_id, "task-native-follower");
    assert_eq!(cycle.tasks[2].route, RoleExecutionRoute::NativeHandoff);
    assert_eq!(cycle.tasks[2].model_id, "model-codex-native");
    assert_eq!(cycle.tasks[2].model_arg, "gpt-6-astra");
    assert_eq!(cycle.tasks[2].effort.as_deref(), Some("high"));

    // 1. Native role handoff without a host client records frozen instruction/model/effort and never invokes a worker.
    let handoff_req = RoleHandoffRequest {
        binding_id: BINDING_A.to_string(),
        thread_uuid: THREAD_UUID_A.to_string(),
        cycle_id: cycle.cycle_id.clone(),
        expected_cycle_revision: 1,
        request_key: "hnd-native-follower-1".to_string(),
        task_id: "task-native-follower".to_string(),
        current_policy: policy.clone(),
        operator_approved: false,
        confirm_approval_digest: None,
        closure_reservation_id: None,
    };
    let handoff = store
        .handoff_role(&handoff_req, &fixture.authority, None)
        .await
        .expect("native handoff without host client");
    assert_eq!(handoff.status, HandoffStatus::NativeHandoffRecorded);
    assert_eq!(handoff.route, RoleExecutionRoute::NativeHandoff);
    assert_eq!(handoff.created_at_cycle_revision, 2);
    assert_eq!(handoff.updated_at_cycle_revision, 2);
    assert!(handoff.run_id.is_none());
    assert!(handoff.worker_id.is_none());
    assert!(handoff.evidence_ref.is_none());
    assert!(handoff.task_snapshot.is_none());

    let native_payload = handoff
        .native_handoff
        .as_ref()
        .expect("native_handoff payload present");
    assert_eq!(native_payload.thread_uuid, THREAD_UUID_A);
    assert_eq!(native_payload.task_id, "task-native-follower");
    assert_eq!(native_payload.title, "Native Follower Coordination");
    assert_eq!(
        native_payload.frozen_instruction,
        "Coordinate operator status in native Codex chat"
    );
    assert_eq!(native_payload.requested_model_id, "model-codex-native");
    assert_eq!(native_payload.requested_model_arg, "gpt-6-astra");
    assert_eq!(native_payload.requested_effort.as_deref(), Some("high"));
    assert!(!native_payload.model_execution_verified);
    assert!(!native_payload.native_per_call_identity_verified);
    assert_eq!(read_invocation_count(&fixture.agy_count_file), 0);
    assert_eq!(read_invocation_count(&fixture.oc_count_file), 0);

    // 2. Duplicate same request_key or same task_id returns the recorded handoff without mutating revisions.
    let dup_by_key = store
        .handoff_role(&handoff_req, &fixture.authority, None)
        .await
        .expect("duplicate handoff by request_key");
    assert_eq!(dup_by_key, handoff);

    let mut dup_by_task_req = handoff_req.clone();
    dup_by_task_req.request_key = "hnd-native-follower-second-key".to_string();
    dup_by_task_req.expected_cycle_revision = 99;
    let dup_by_task = store
        .handoff_role(&dup_by_task_req, &fixture.authority, None)
        .await
        .expect("duplicate handoff by task_id");
    assert_eq!(dup_by_task, handoff);
    assert_eq!(
        store
            .get_cycle(BINDING_A, &cycle.cycle_id)
            .expect("get_cycle")
            .expect("cycle exists")
            .cycle_revision,
        2
    );

    // Conflicting reuse of existing request_key for a different task refuses with ValidationFailed.
    let mut key_conflict_req = handoff_req.clone();
    key_conflict_req.task_id = "task-coder-hard-1".to_string();
    key_conflict_req.expected_cycle_revision = 2;
    let key_conflict_err = store
        .handoff_role(&key_conflict_req, &fixture.authority, None)
        .await
        .expect_err("reusing request_key for different task must fail");
    assert_eq!(
        key_conflict_err.error_code(),
        NativeErrorCode::ValidationFailed
    );

    // 3. Changed policy, foreign binding/thread, wrong task, and wrong cycle revision refuse before effect.
    let mut changed_policy = policy.clone();
    changed_policy.policy_id = "policy-drifted".to_string();
    let policy_err = store
        .handoff_role(
            &RoleHandoffRequest {
                request_key: "hnd-bad-policy".to_string(),
                expected_cycle_revision: 2,
                task_id: "task-coder-hard-1".to_string(),
                current_policy: changed_policy,
                ..handoff_req.clone()
            },
            &fixture.authority,
            None,
        )
        .await
        .expect_err("changed policy must refuse");
    assert_eq!(policy_err.error_code(), NativeErrorCode::ValidationFailed);

    let foreign_binding_err = store
        .handoff_role(
            &RoleHandoffRequest {
                binding_id: BINDING_B.to_string(),
                request_key: "hnd-foreign-binding".to_string(),
                expected_cycle_revision: 2,
                task_id: "task-coder-hard-1".to_string(),
                ..handoff_req.clone()
            },
            &fixture.authority,
            None,
        )
        .await
        .expect_err("foreign binding must refuse");
    assert_eq!(
        foreign_binding_err.error_code(),
        NativeErrorCode::ValidationFailed
    );

    let foreign_thread_err = store
        .handoff_role(
            &RoleHandoffRequest {
                thread_uuid: THREAD_UUID_B.to_string(),
                request_key: "hnd-foreign-thread".to_string(),
                expected_cycle_revision: 2,
                task_id: "task-coder-hard-1".to_string(),
                ..handoff_req.clone()
            },
            &fixture.authority,
            None,
        )
        .await
        .expect_err("foreign thread_uuid must refuse");
    assert_eq!(
        foreign_thread_err.error_code(),
        NativeErrorCode::ValidationFailed
    );

    let wrong_task_err = store
        .handoff_role(
            &RoleHandoffRequest {
                request_key: "hnd-wrong-task".to_string(),
                expected_cycle_revision: 2,
                task_id: "task-closure".to_string(),
                ..handoff_req.clone()
            },
            &fixture.authority,
            None,
        )
        .await
        .expect_err("task not frozen in cycle must refuse");
    assert_eq!(
        wrong_task_err.error_code(),
        NativeErrorCode::ValidationFailed
    );

    let wrong_rev_err = store
        .handoff_role(
            &RoleHandoffRequest {
                request_key: "hnd-wrong-rev".to_string(),
                expected_cycle_revision: 1,
                task_id: "task-coder-hard-1".to_string(),
                ..handoff_req.clone()
            },
            &fixture.authority,
            None,
        )
        .await
        .expect_err("stale expected_cycle_revision must refuse");
    assert_eq!(
        wrong_rev_err.error_code(),
        NativeErrorCode::ValidationFailed
    );

    // 4. Registry drift or disabled/missing choice refuses with no silent model fallback.
    let current_reg_rev = fixture.registry.snapshot().expect("reg snap").revision;
    fixture
        .registry
        .apply(
            current_reg_rev,
            RegistryChange::UpsertTool {
                tool: ToolDefinition {
                    id: "tool-agy".to_string(),
                    label: "Antigravity CLI".to_string(),
                    adapter: ToolAdapter::Antigravity,
                    executable_path: Some(fixture.root.join("bin/fake-agy")),
                    enabled: false,
                },
            },
        )
        .expect("disable tool-agy");

    let disabled_report = preflight_from_dir_readonly(
        &fixture.registry_dir,
        &PreflightRequest {
            profile_id: Some("profile-main".to_string()),
            roles: vec![RolePreflightRequest {
                role: WorkflowRole::Coder,
                difficulty: Some(Difficulty::Hard),
            }],
        },
        &policy,
    )
    .expect("preflight after disabling tool-agy");
    assert!(!disabled_report.ready);
    assert!(disabled_report.roles[0].selected.is_none());

    let drift_handoff_err = store
        .handoff_role(
            &RoleHandoffRequest {
                request_key: "hnd-after-reg-drift".to_string(),
                expected_cycle_revision: 2,
                task_id: "task-coder-hard-1".to_string(),
                operator_approved: true,
                confirm_approval_digest: Some(cycle.approval_subject_digest.clone()),
                ..handoff_req
            },
            &fixture.authority,
            None,
        )
        .await
        .expect_err("registry drift must refuse handoff");
    assert_eq!(
        drift_handoff_err.error_code(),
        NativeErrorCode::ValidationFailed
    );
    assert_eq!(read_invocation_count(&fixture.agy_count_file), 0);
}

#[tokio::test]
async fn headless_role_handoff_dispatches_once_via_real_approved_host_and_populates_snapshot_evidence(
) {
    let fixture = setup_fixture();
    let policy = default_policy();
    let store = RoleFlowStore::new(&fixture.plans_dir).expect("open RoleFlowStore");

    let approval_id = create_approved_plan_at_commit(
        &fixture,
        "plan-headless-dispatch",
        BINDING_A,
        THREAD_UUID_A,
        &fixture.base_commit,
        default_tasks(),
    );

    let cycle = store
        .create_cycle(
            &CreateRoleFlowCycleRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                request_key: "req-headless-cycle-1".to_string(),
                expected_ledger_revision: Some(0),
                approval_id: approval_id.clone(),
                task_ids: vec!["task-coder-hard-1".to_string()],
                policy: policy.clone(),
            },
            &fixture.authority,
        )
        .expect("create_cycle");

    let host = ApprovedHost::start(ApprovedHostConfig {
        registry_dir: fixture.registry_dir.clone(),
        plans_dir: fixture.plans_dir.clone(),
        authority_dir: fixture.authority_dir.clone(),
        sessions_dir: fixture.sessions_dir.clone(),
        approval_id: approval_id.clone(),
        tick_interval: Duration::from_millis(25),
        notifications: None,
    })
    .await
    .expect("start ApprovedHost");
    assert_eq!(host.summary().phase, HostPhase::Ready);

    let client = ApprovedHostClient::new(
        Arc::clone(&fixture.authority),
        &fixture.sessions_dir,
        BINDING_A.to_string(),
        THREAD_UUID_A.to_string(),
    )
    .expect("create ApprovedHostClient");

    let base_req = RoleHandoffRequest {
        binding_id: BINDING_A.to_string(),
        thread_uuid: THREAD_UUID_A.to_string(),
        cycle_id: cycle.cycle_id.clone(),
        expected_cycle_revision: 1,
        request_key: "hnd-coder-hard-1".to_string(),
        task_id: "task-coder-hard-1".to_string(),
        current_policy: policy.clone(),
        operator_approved: true,
        confirm_approval_digest: Some(cycle.approval_subject_digest.clone()),
        closure_reservation_id: None,
    };

    // 1. Missing operator confirmation, wrong approval digest, missing host client, or wrong policy block dispatch.
    let unapproved_err = store
        .handoff_role(
            &RoleHandoffRequest {
                operator_approved: false,
                ..base_req.clone()
            },
            &fixture.authority,
            Some(&client),
        )
        .await
        .expect_err("operator_approved=false must refuse headless dispatch");
    assert_eq!(
        unapproved_err.error_code(),
        NativeErrorCode::UnauthorizedOperator
    );

    let bad_digest_err = store
        .handoff_role(
            &RoleHandoffRequest {
                confirm_approval_digest: Some("0".repeat(64)),
                ..base_req.clone()
            },
            &fixture.authority,
            Some(&client),
        )
        .await
        .expect_err("wrong confirm_approval_digest must refuse headless dispatch");
    assert_eq!(
        bad_digest_err.error_code(),
        NativeErrorCode::UnauthorizedOperator
    );

    let no_host_err = store
        .handoff_role(&base_req, &fixture.authority, None)
        .await
        .expect_err("missing host_client must refuse headless dispatch");
    assert_eq!(no_host_err.error_code(), NativeErrorCode::HostNotConfigured);

    let mut deny_policy = policy.clone();
    deny_policy.allow_rules.clear();
    let policy_err = store
        .handoff_role(
            &RoleHandoffRequest {
                current_policy: deny_policy,
                ..base_req.clone()
            },
            &fixture.authority,
            Some(&client),
        )
        .await
        .expect_err("wrong policy must refuse headless dispatch");
    assert_eq!(policy_err.error_code(), NativeErrorCode::ValidationFailed);
    assert_eq!(read_invocation_count(&fixture.agy_count_file), 0);

    // 2. Valid headless role handoff dispatches the approved task ONCE and returns real run_id/worker_id.
    let dispatched = store
        .handoff_role(&base_req, &fixture.authority, Some(&client))
        .await
        .expect("dispatch headless coder task");
    assert_eq!(dispatched.route, RoleExecutionRoute::HeadlessWorker);
    assert!(matches!(
        dispatched.status,
        HandoffStatus::Dispatched | HandoffStatus::ReadyForReview
    ));
    let run_id = dispatched
        .run_id
        .clone()
        .expect("sealed run_id returned from host");
    let worker_id = dispatched
        .worker_id
        .clone()
        .expect("sealed worker_id returned from host");
    assert!(run_id.starts_with("run_"));
    assert!(worker_id.starts_with("wrk_"));

    let ready_snap = wait_for_task_ready(&client, &approval_id, "task-coder-hard-1").await;
    assert_eq!(read_invocation_count(&fixture.agy_count_file), 1);
    assert_eq!(ready_snap.run_id.as_deref(), Some(run_id.as_str()));
    assert_eq!(ready_snap.worker_id.as_deref(), Some(worker_id.as_str()));

    // 3. Repeating handoff (same key or different key for same task) returns recorded state and never increments counter.
    let repeat_same_key = store
        .handoff_role(&base_req, &fixture.authority, Some(&client))
        .await
        .expect("repeat handoff with same request_key");
    assert_eq!(repeat_same_key, dispatched);

    let repeat_same_task = store
        .handoff_role(
            &RoleHandoffRequest {
                request_key: "hnd-coder-hard-1-repeat".to_string(),
                expected_cycle_revision: 2,
                ..base_req.clone()
            },
            &fixture.authority,
            Some(&client),
        )
        .await
        .expect("repeat handoff with same task_id");
    assert_eq!(repeat_same_task, dispatched);
    assert_eq!(read_invocation_count(&fixture.agy_count_file), 1);

    // 4. Reconciling observes the completed ReadyForReview snapshot and extracts actual worktree/artifact/telemetry evidence.
    let current_cycle_revision = store
        .get_cycle(BINDING_A, &cycle.cycle_id)
        .expect("read current cycle")
        .expect("cycle exists")
        .cycle_revision;
    let reconciled = store
        .reconcile_handoff(
            &ReconcileRoleHandoffRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                cycle_id: cycle.cycle_id.clone(),
                expected_cycle_revision: current_cycle_revision,
                handoff_id: dispatched.handoff_id.clone(),
            },
            &client,
        )
        .await
        .expect("reconcile completed handoff");
    assert_eq!(reconciled.status, HandoffStatus::ReadyForReview);
    assert_eq!(reconciled.run_id, ready_snap.run_id);
    assert_eq!(reconciled.worker_id, ready_snap.worker_id);
    assert_eq!(read_invocation_count(&fixture.agy_count_file), 1);

    let ev = reconciled
        .evidence_ref
        .as_ref()
        .expect("evidence_ref populated");
    let worker_snap = ready_snap
        .worker
        .as_ref()
        .expect("worker snapshot present on ReadyForReview");
    let outcome_snap = worker_snap
        .outcome
        .as_ref()
        .expect("worker outcome present on ReadyForReview");

    assert_eq!(ev.approval_id, approval_id);
    assert_eq!(ev.task_id, "task-coder-hard-1");
    assert_eq!(ev.run_id, ready_snap.run_id);
    assert_eq!(ev.worker_id, ready_snap.worker_id);
    assert_eq!(ev.task_worktree_id, worker_snap.worktree.task_worktree_id);
    assert!(ev.task_worktree_id.is_some());
    assert_eq!(ev.report_artifact_id, outcome_snap.report_artifact_id);
    assert!(ev.report_artifact_id.is_some());
    assert_eq!(ev.base_commit, fixture.base_commit);
    assert_eq!(ev.head_commit, worker_snap.worktree.head_commit);
    assert!(ev.head_commit.is_some());
    // Writing a candidate does not implicitly commit it; preserve actual HEAD
    // evidence and independently prove that scoped file changes were produced.
    assert_eq!(
        ev.head_commit.as_deref(),
        Some(fixture.base_commit.as_str())
    );
    assert!(outcome_snap.changed_paths_count > 0);
    assert_eq!(ev.observed_model.as_deref(), Some("gemini-4-argon-high"));
    assert_eq!(ev.observed_effort.as_deref(), Some("high"));
    assert_eq!(
        ev.final_summary_preview.as_deref(),
        Some("Completed bounded preview")
    );
    assert!(!ev.final_summary_is_complete_evidence);

    host.shutdown().await.expect("shutdown ApprovedHost");
}

#[tokio::test]
async fn unknown_handoff_blocks_replay_and_reconciles_sealed_or_unstarted_host_state() {
    let fixture = setup_fixture();
    let policy = default_policy();
    let store = RoleFlowStore::new(&fixture.plans_dir).expect("open RoleFlowStore");

    let approval_id = create_approved_plan_at_commit(
        &fixture,
        "plan-unknown-reconcile",
        BINDING_A,
        THREAD_UUID_A,
        &fixture.base_commit,
        default_tasks(),
    );

    let cycle = store
        .create_cycle(
            &CreateRoleFlowCycleRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                request_key: "req-unknown-cycle-1".to_string(),
                expected_ledger_revision: Some(0),
                approval_id: approval_id.clone(),
                task_ids: vec![
                    "task-coder-hard-1".to_string(),
                    "task-coder-hard-2".to_string(),
                ],
                policy: policy.clone(),
            },
            &fixture.authority,
        )
        .expect("create_cycle");

    fs::create_dir_all(&fixture.sessions_dir).expect("create sessions_dir");
    fs::set_permissions(&fixture.sessions_dir, fs::Permissions::from_mode(0o700))
        .expect("chmod 0700 sessions_dir");
    let client = ApprovedHostClient::new(
        Arc::clone(&fixture.authority),
        &fixture.sessions_dir,
        BINDING_A.to_string(),
        THREAD_UUID_A.to_string(),
    )
    .expect("create ApprovedHostClient");

    // 1. An absent host is a proven pre-effect refusal, distinct from an uncertain transport outcome.
    let unstarted_req = RoleHandoffRequest {
        binding_id: BINDING_A.to_string(),
        thread_uuid: THREAD_UUID_A.to_string(),
        cycle_id: cycle.cycle_id.clone(),
        expected_cycle_revision: 1,
        request_key: "hnd-coder-hard-2-unstarted".to_string(),
        task_id: "task-coder-hard-2".to_string(),
        current_policy: policy.clone(),
        operator_approved: true,
        confirm_approval_digest: Some(cycle.approval_subject_digest.clone()),
        closure_reservation_id: None,
    };
    let unstarted_err = store
        .handoff_role(&unstarted_req, &fixture.authority, Some(&client))
        .await
        .expect_err("dispatch before host start must be refused before effect");
    assert_eq!(
        unstarted_err.error_code(),
        NativeErrorCode::ValidationFailed
    );

    let cycle_after_unstarted = store
        .get_cycle(BINDING_A, &cycle.cycle_id)
        .expect("get_cycle")
        .expect("cycle exists");
    assert_eq!(cycle_after_unstarted.cycle_revision, 3);
    let unknown_hnd_2 = &cycle_after_unstarted.handoffs[0];
    assert_eq!(unknown_hnd_2.task_id, "task-coder-hard-2");
    assert_eq!(unknown_hnd_2.status, HandoffStatus::RefusedBeforeEffect);
    // Inject a crash marker ONLY in this disposable ledger to exercise genuine
    // uncertainty independently of a host's deterministic no-effect refusal.
    let path = store.ledger_path_for_binding(BINDING_A).expect("ledger");
    let mut value: serde_json::Value =
        serde_json::from_slice(&fs::read(&path).expect("read")).expect("json");
    let entry = value["cycles"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|c| c["cycle_id"] == cycle.cycle_id)
        .unwrap()["handoffs"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|h| h["handoff_id"] == unknown_hnd_2.handoff_id)
        .unwrap();
    entry["status"] = serde_json::to_value(HandoffStatus::OutcomeUnknown).unwrap();
    fs::write(&path, serde_json::to_vec_pretty(&value).unwrap()).expect("inject unknown");

    // Start the real ApprovedHost now.
    let host = ApprovedHost::start(ApprovedHostConfig {
        registry_dir: fixture.registry_dir.clone(),
        plans_dir: fixture.plans_dir.clone(),
        authority_dir: fixture.authority_dir.clone(),
        sessions_dir: fixture.sessions_dir.clone(),
        approval_id: approval_id.clone(),
        tick_interval: Duration::from_millis(25),
        notifications: None,
    })
    .await
    .expect("start ApprovedHost");

    // Replaying task-coder-hard-2 while OutcomeUnknown is refused; reconciling when host has no sealed run stays OutcomeUnknown.
    let replay_unstarted_err = store
        .handoff_role(&unstarted_req, &fixture.authority, Some(&client))
        .await
        .expect_err("unknown handoff must refuse replay");
    assert_eq!(
        replay_unstarted_err.error_code(),
        NativeErrorCode::AmbiguousWakeupPendingReconciliation
    );

    let reconciled_unstarted = store
        .reconcile_handoff(
            &ReconcileRoleHandoffRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                cycle_id: cycle.cycle_id.clone(),
                expected_cycle_revision: 3,
                handoff_id: unknown_hnd_2.handoff_id.clone(),
            },
            &client,
        )
        .await
        .expect("reconcile unstarted task");
    assert_eq!(reconciled_unstarted.status, HandoffStatus::OutcomeUnknown);
    assert_eq!(
        reconciled_unstarted.observed_task_status,
        Some(TaskExecutionStatus::NotStarted)
    );
    assert!(reconciled_unstarted.run_id.is_none());
    assert!(reconciled_unstarted.worker_id.is_none());
    assert_eq!(read_invocation_count(&fixture.agy_count_file), 0);

    // 2. Dispatch task-coder-hard-1 on the running host and wait for ReadyForReview.
    let hnd_1_req = RoleHandoffRequest {
        binding_id: BINDING_A.to_string(),
        thread_uuid: THREAD_UUID_A.to_string(),
        cycle_id: cycle.cycle_id.clone(),
        expected_cycle_revision: 4,
        request_key: "hnd-coder-hard-1-live".to_string(),
        task_id: "task-coder-hard-1".to_string(),
        current_policy: policy.clone(),
        operator_approved: true,
        confirm_approval_digest: Some(cycle.approval_subject_digest.clone()),
        closure_reservation_id: None,
    };
    let dispatched_1 = store
        .handoff_role(&hnd_1_req, &fixture.authority, Some(&client))
        .await
        .expect("dispatch task-coder-hard-1");
    let ready_snap = wait_for_task_ready(&client, &approval_id, "task-coder-hard-1").await;
    assert_eq!(read_invocation_count(&fixture.agy_count_file), 1);

    // 3. Simulate a crash/unknown outcome on task-coder-hard-1 by editing ONLY the disposable role ledger JSON.
    let ledger_path = store
        .ledger_path_for_binding(BINDING_A)
        .expect("ledger_path_for_binding");
    let mut ledger_json: serde_json::Value =
        serde_json::from_slice(&fs::read(&ledger_path).expect("read ledger json"))
            .expect("parse ledger json");
    {
        let handoffs = ledger_json["cycles"][0]["handoffs"]
            .as_array_mut()
            .expect("handoffs array");
        let hnd_entry = handoffs
            .iter_mut()
            .find(|h| h["handoff_id"] == dispatched_1.handoff_id)
            .expect("find task-coder-hard-1 handoff in ledger");
        hnd_entry["status"] =
            serde_json::to_value(HandoffStatus::OutcomeUnknown).expect("serialize OutcomeUnknown");
        hnd_entry["run_id"] = serde_json::Value::Null;
        hnd_entry["worker_id"] = serde_json::Value::Null;
        hnd_entry["evidence_ref"] = serde_json::Value::Null;
        hnd_entry["task_snapshot"] = serde_json::Value::Null;
    }
    fs::write(
        &ledger_path,
        serde_json::to_vec_pretty(&ledger_json).expect("serialize modified ledger"),
    )
    .expect("write modified ledger");
    fs::set_permissions(&ledger_path, fs::Permissions::from_mode(0o600))
        .expect("restore 0600 on ledger");

    // 4. Repeat handoff (same key or new key) refuses with AmbiguousWakeupPendingReconciliation; count stays 1.
    let replay_same_key_err = store
        .handoff_role(&hnd_1_req, &fixture.authority, Some(&client))
        .await
        .expect_err("replay with same key must refuse when OutcomeUnknown");
    assert_eq!(
        replay_same_key_err.error_code(),
        NativeErrorCode::AmbiguousWakeupPendingReconciliation
    );

    let replay_new_key_err = store
        .handoff_role(
            &RoleHandoffRequest {
                request_key: "hnd-coder-hard-1-retry-key".to_string(),
                expected_cycle_revision: dispatched_1.updated_at_cycle_revision,
                ..hnd_1_req
            },
            &fixture.authority,
            Some(&client),
        )
        .await
        .expect_err("replay with new key for same task must refuse when OutcomeUnknown");
    assert_eq!(
        replay_new_key_err.error_code(),
        NativeErrorCode::AmbiguousWakeupPendingReconciliation
    );
    assert_eq!(read_invocation_count(&fixture.agy_count_file), 1);

    // 5. Explicit reconcile recovers the exact existing host run without another provider invocation.
    let recovered = store
        .reconcile_handoff(
            &ReconcileRoleHandoffRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                cycle_id: cycle.cycle_id.clone(),
                expected_cycle_revision: dispatched_1.updated_at_cycle_revision,
                handoff_id: dispatched_1.handoff_id.clone(),
            },
            &client,
        )
        .await
        .expect("reconcile_handoff recovers sealed host run");
    assert_eq!(recovered.status, HandoffStatus::ReadyForReview);
    assert_eq!(recovered.run_id, ready_snap.run_id);
    assert_eq!(recovered.worker_id, ready_snap.worker_id);
    assert_eq!(read_invocation_count(&fixture.agy_count_file), 1);

    host.shutdown().await.expect("shutdown ApprovedHost");
}

#[tokio::test]
async fn closure_handoff_requires_bound_reservation_and_enforces_single_closure_per_package_tree() {
    let fixture = setup_fixture();
    let policy = default_policy();
    let store = RoleFlowStore::new(&fixture.plans_dir).expect("open RoleFlowStore");

    let approval_1 = create_approved_plan_at_commit(
        &fixture,
        "plan-closure-1",
        BINDING_A,
        THREAD_UUID_A,
        &fixture.base_commit,
        default_tasks(),
    );

    let cycle_1 = store
        .create_cycle(
            &CreateRoleFlowCycleRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                request_key: "req-closure-cycle-1".to_string(),
                expected_ledger_revision: Some(0),
                approval_id: approval_1.clone(),
                task_ids: vec![
                    "task-native-follower".to_string(),
                    "task-closure".to_string(),
                ],
                policy: policy.clone(),
            },
            &fixture.authority,
        )
        .expect("create cycle_1");

    // 1. Closure handoff without a reservation refuses BEFORE recording any native handoff or provider call.
    let missing_res_err = store
        .handoff_role(
            &RoleHandoffRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                cycle_id: cycle_1.cycle_id.clone(),
                expected_cycle_revision: 1,
                request_key: "hnd-closure-no-res".to_string(),
                task_id: "task-closure".to_string(),
                current_policy: policy.clone(),
                operator_approved: false,
                confirm_approval_digest: None,
                closure_reservation_id: None,
            },
            &fixture.authority,
            None,
        )
        .await
        .expect_err("closure handoff without reservation must refuse");
    assert_eq!(
        missing_res_err.error_code(),
        NativeErrorCode::ValidationFailed
    );
    assert!(store
        .get_cycle(BINDING_A, &cycle_1.cycle_id)
        .expect("get_cycle")
        .expect("cycle_1")
        .handoffs
        .is_empty());

    // 2. Reserving closure for a non-closure task, wrong subject_digest, or wrong expected tree refuses.
    let non_closure_task_err = store
        .reserve_closure(&ReservePackageClosureRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            cycle_id: cycle_1.cycle_id.clone(),
            expected_cycle_revision: 1,
            request_key: "res-wrong-role".to_string(),
            package_id: "pkg-m4-core".to_string(),
            source: ClosureSubjectSource::ApprovedCycleBase {
                subject_digest: cycle_1.approval_subject_digest.clone(),
                expected_candidate_tree_sha: Some(fixture.base_tree_sha.clone()),
            },
            task_id: Some("task-native-follower".to_string()),
        })
        .expect_err("non-closure task cannot reserve closure");
    assert_eq!(
        non_closure_task_err.error_code(),
        NativeErrorCode::ValidationFailed
    );

    let wrong_subject_err = store
        .reserve_closure(&ReservePackageClosureRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            cycle_id: cycle_1.cycle_id.clone(),
            expected_cycle_revision: 1,
            request_key: "res-wrong-subject".to_string(),
            package_id: "pkg-m4-core".to_string(),
            source: ClosureSubjectSource::ApprovedCycleBase {
                subject_digest: "a".repeat(64),
                expected_candidate_tree_sha: Some(fixture.base_tree_sha.clone()),
            },
            task_id: Some("task-closure".to_string()),
        })
        .expect_err("mismatched subject_digest must refuse closure reservation");
    assert_eq!(
        wrong_subject_err.error_code(),
        NativeErrorCode::ValidationFailed
    );

    // 3. Valid reservation against ApprovedCycleBase succeeds; duplicate request returns existing reservation.
    let res_req_1 = ReservePackageClosureRequest {
        binding_id: BINDING_A.to_string(),
        thread_uuid: THREAD_UUID_A.to_string(),
        cycle_id: cycle_1.cycle_id.clone(),
        expected_cycle_revision: 1,
        request_key: "res-pkg-m4-1".to_string(),
        package_id: "pkg-m4-core".to_string(),
        source: ClosureSubjectSource::ApprovedCycleBase {
            subject_digest: cycle_1.approval_subject_digest.clone(),
            expected_candidate_tree_sha: Some(fixture.base_tree_sha.clone()),
        },
        task_id: Some("task-closure".to_string()),
    };
    let res_1 = store
        .reserve_closure(&res_req_1)
        .expect("reserve closure for cycle_1");
    assert_eq!(res_1.candidate_tree_sha, fixture.base_tree_sha);
    assert_eq!(res_1.subject_digest, cycle_1.approval_subject_digest);

    let res_dup = store
        .reserve_closure(&res_req_1)
        .expect("idempotent replay of reserve_closure");
    assert_eq!(res_dup, res_1);

    // Non-closure task cannot spend a closure reservation.
    let non_closure_spend_err = store
        .handoff_role(
            &RoleHandoffRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                cycle_id: cycle_1.cycle_id.clone(),
                expected_cycle_revision: 2,
                request_key: "hnd-follower-with-closure-res".to_string(),
                task_id: "task-native-follower".to_string(),
                current_policy: policy.clone(),
                operator_approved: false,
                confirm_approval_digest: None,
                closure_reservation_id: Some(res_1.reservation_id.clone()),
            },
            &fixture.authority,
            None,
        )
        .await
        .expect_err("non-closure task must not spend closure reservation");
    assert_eq!(
        non_closure_spend_err.error_code(),
        NativeErrorCode::ValidationFailed
    );

    // 4. Across cycles: reserving the same (repository, package, tree) with the same approval_subject_digest
    //    returns the existing reservation, and a second cycle cannot spend cycle_1's reservation.
    let cycle_2 = store
        .create_cycle(
            &CreateRoleFlowCycleRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                request_key: "req-closure-cycle-2".to_string(),
                expected_ledger_revision: None,
                approval_id: approval_1.clone(),
                task_ids: vec!["task-closure".to_string()],
                policy: policy.clone(),
            },
            &fixture.authority,
        )
        .expect("create cycle_2");

    let cross_cycle_same_subject = store
        .reserve_closure(&ReservePackageClosureRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            cycle_id: cycle_2.cycle_id.clone(),
            expected_cycle_revision: 1,
            request_key: "res-pkg-m4-cycle-2".to_string(),
            package_id: "pkg-m4-core".to_string(),
            source: ClosureSubjectSource::ApprovedCycleBase {
                subject_digest: cycle_2.approval_subject_digest.clone(),
                expected_candidate_tree_sha: Some(fixture.base_tree_sha.clone()),
            },
            task_id: Some("task-closure".to_string()),
        })
        .expect("cross-cycle duplicate with same subject returns existing reservation");
    assert_eq!(cross_cycle_same_subject, res_1);
    assert_eq!(
        store
            .list_closure_reservations(BINDING_A)
            .expect("list_closures")
            .len(),
        1
    );

    let wrong_cycle_spend_err = store
        .handoff_role(
            &RoleHandoffRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                cycle_id: cycle_2.cycle_id.clone(),
                expected_cycle_revision: 1,
                request_key: "hnd-closure-wrong-cycle".to_string(),
                task_id: "task-closure".to_string(),
                current_policy: policy.clone(),
                operator_approved: false,
                confirm_approval_digest: None,
                closure_reservation_id: Some(res_1.reservation_id.clone()),
            },
            &fixture.authority,
            None,
        )
        .await
        .expect_err("cycle_2 must not spend cycle_1's closure reservation");
    assert_eq!(
        wrong_cycle_spend_err.error_code(),
        NativeErrorCode::ValidationFailed
    );

    // 5. A different approval at the SAME base_commit/tree has a different approval.subject_digest and conflicts.
    let approval_diff_subject = create_approved_plan_at_commit(
        &fixture,
        "plan-closure-diff-subject",
        BINDING_A,
        THREAD_UUID_A,
        &fixture.base_commit,
        default_tasks(),
    );
    let cycle_diff_subject = store
        .create_cycle(
            &CreateRoleFlowCycleRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                request_key: "req-closure-cycle-diff-subj".to_string(),
                expected_ledger_revision: None,
                approval_id: approval_diff_subject,
                task_ids: vec!["task-closure".to_string()],
                policy: policy.clone(),
            },
            &fixture.authority,
        )
        .expect("create cycle_diff_subject");
    assert_ne!(
        cycle_diff_subject.approval_subject_digest,
        cycle_1.approval_subject_digest
    );

    let conflict_subject_err = store
        .reserve_closure(&ReservePackageClosureRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            cycle_id: cycle_diff_subject.cycle_id.clone(),
            expected_cycle_revision: 1,
            request_key: "res-pkg-m4-conflict-subj".to_string(),
            package_id: "pkg-m4-core".to_string(),
            source: ClosureSubjectSource::ApprovedCycleBase {
                subject_digest: cycle_diff_subject.approval_subject_digest.clone(),
                expected_candidate_tree_sha: Some(fixture.base_tree_sha.clone()),
            },
            task_id: Some("task-closure".to_string()),
        })
        .expect_err("different approval subject_digest for same package/tree must conflict");
    assert_eq!(
        conflict_subject_err.error_code(),
        NativeErrorCode::ValidationFailed
    );

    // 6. Spending res_1 in cycle_1 for task-closure succeeds.
    let closure_hnd = store
        .handoff_role(
            &RoleHandoffRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                cycle_id: cycle_1.cycle_id.clone(),
                expected_cycle_revision: 2,
                request_key: "hnd-closure-cycle-1".to_string(),
                task_id: "task-closure".to_string(),
                current_policy: policy.clone(),
                operator_approved: false,
                confirm_approval_digest: None,
                closure_reservation_id: Some(res_1.reservation_id.clone()),
            },
            &fixture.authority,
            None,
        )
        .await
        .expect("valid closure handoff with bound reservation");
    assert_eq!(closure_hnd.status, HandoffStatus::NativeHandoffRecorded);
    assert_eq!(
        closure_hnd.closure_reservation_id.as_deref(),
        Some(res_1.reservation_id.as_str())
    );

    // 7. A changed candidate tree requires a new commit, explicit new approval, and new cycle.
    fs::write(
        fixture.repo_dir.join("src/lib.rs"),
        "pub fn alpha() -> u32 { 99 }\n",
    )
    .expect("update src/lib.rs for new candidate tree");
    run_sanitized_git(
        &fixture.repo_dir,
        &["add", "src/lib.rs"],
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .expect("git add");
    run_sanitized_git(
        &fixture.repo_dir,
        &["commit", "-m", "candidate v2", "--quiet"],
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .expect("git commit candidate v2");
    let new_commit =
        resolve_exact_commit_sha(&fixture.repo_dir, "HEAD", DEFAULT_GIT_COMMAND_TIMEOUT_MS)
            .expect("resolve new HEAD commit");
    let new_tree_sha = resolve_exact_tree_sha(
        &fixture.repo_dir,
        &new_commit,
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .expect("resolve new HEAD tree");
    assert_ne!(new_tree_sha, fixture.base_tree_sha);

    let approval_new_tree = create_approved_plan_at_commit(
        &fixture,
        "plan-closure-new-tree",
        BINDING_A,
        THREAD_UUID_A,
        &new_commit,
        default_tasks(),
    );
    let cycle_new_tree = store
        .create_cycle(
            &CreateRoleFlowCycleRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                request_key: "req-closure-cycle-new-tree".to_string(),
                expected_ledger_revision: None,
                approval_id: approval_new_tree,
                task_ids: vec!["task-closure".to_string()],
                policy: policy.clone(),
            },
            &fixture.authority,
        )
        .expect("create cycle_new_tree");

    let res_new_tree = store
        .reserve_closure(&ReservePackageClosureRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            cycle_id: cycle_new_tree.cycle_id.clone(),
            expected_cycle_revision: 1,
            request_key: "res-pkg-m4-new-tree".to_string(),
            package_id: "pkg-m4-core".to_string(),
            source: ClosureSubjectSource::ApprovedCycleBase {
                subject_digest: cycle_new_tree.approval_subject_digest.clone(),
                expected_candidate_tree_sha: Some(new_tree_sha.clone()),
            },
            task_id: Some("task-closure".to_string()),
        })
        .expect("reserve closure for new candidate tree");
    assert_eq!(res_new_tree.candidate_tree_sha, new_tree_sha);
    assert_ne!(res_new_tree.reservation_id, res_1.reservation_id);
    assert_eq!(
        store
            .list_closure_reservations(BINDING_A)
            .expect("list_closures")
            .len(),
        2
    );
}

#[tokio::test]
async fn conversation_change_gate_allows_questions_and_blocks_handoff_on_pending_replan() {
    let fixture = setup_fixture();
    let policy = default_policy();
    let store = RoleFlowStore::new(&fixture.plans_dir).expect("open RoleFlowStore");
    let change_store =
        ConversationChangeStore::open(&fixture.plans_dir).expect("open ConversationChangeStore");

    fs::create_dir_all(&fixture.sessions_dir).expect("create sessions_dir");
    fs::set_permissions(&fixture.sessions_dir, fs::Permissions::from_mode(0o700))
        .expect("chmod 0700 sessions_dir");

    let approval_id = create_approved_plan_at_commit(
        &fixture,
        "plan-conv-gate",
        BINDING_A,
        THREAD_UUID_A,
        &fixture.base_commit,
        default_tasks(),
    );
    let approval_rec = fixture
        .authority
        .get_for_operator(&approval_id)
        .expect("get approval record");

    let cycle = store
        .create_cycle(
            &CreateRoleFlowCycleRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                request_key: "req-conv-gate-cycle".to_string(),
                expected_ledger_revision: Some(0),
                approval_id: approval_id.clone(),
                task_ids: vec![
                    "task-native-follower".to_string(),
                    "task-coder-hard-1".to_string(),
                ],
                policy: policy.clone(),
            },
            &fixture.authority,
        )
        .expect("create_cycle");

    let client = ApprovedHostClient::new(
        Arc::clone(&fixture.authority),
        &fixture.sessions_dir,
        BINDING_A.to_string(),
        THREAD_UUID_A.to_string(),
    )
    .expect("create ApprovedHostClient");

    // 1. A question/status inquiry takes ReadOnlyObservation, never claims live input/pause/resume, and never blocks handoff.
    let q_record = change_store
        .record_change(
            BINDING_A,
            THREAD_UUID_A,
            &ConversationChangeRequest {
                request_key: "chg-question-status-1".to_string(),
                expected_change_revision: 0,
                approval_id: Some(approval_id.clone()),
                plan_id: Some("plan-conv-gate".to_string()),
                expected_plan_revision: Some(approval_rec.plan_revision),
                task_id: Some("task-native-follower".to_string()),
                expected_task_revision: None,
                kind: ConversationChangeKind::QuestionOrStatus,
                route: Some(ConversationChangeRoute::ReadOnlyObservation),
                summary: "Check current status of follower coordination".to_string(),
                detail: None,
                proposed_write_paths: Vec::new(),
                proposed_profile_id: None,
                proposed_model_id: None,
                proposed_effort: None,
                requested_delivery: ConversationDeliveryRequest::SafeCheckpointOrReplan,
                cancel_action: None,
            },
            &fixture.plans,
            &fixture.authority,
            &client,
        )
        .await
        .expect("record question_or_status change");

    assert!(!q_record.decision.blocks_task_advancement);
    assert!(!q_record.decision.cancel_requested);
    assert!(!q_record.decision.replan_required);
    assert!(!q_record.decision.new_approval_required);
    assert!(q_record.decision.frozen_approval_immutable);
    assert!(!q_record.decision.live_input_delivered);
    assert!(!q_record.decision.process_paused);
    assert!(!q_record.decision.provider_resumed);

    let follower_handoff = store
        .handoff_role(
            &RoleHandoffRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                cycle_id: cycle.cycle_id.clone(),
                expected_cycle_revision: 1,
                request_key: "hnd-follower-after-question".to_string(),
                task_id: "task-native-follower".to_string(),
                current_policy: policy.clone(),
                operator_approved: false,
                confirm_approval_digest: None,
                closure_reservation_id: None,
            },
            &fixture.authority,
            None,
        )
        .await
        .expect("question_or_status must not block role handoff");
    assert_eq!(
        follower_handoff.status,
        HandoffStatus::NativeHandoffRecorded
    );

    // 2. A recorded scope/model change creates a PendingReplan record that blocks role handoff and cycle creation.
    let scope_record = change_store
        .record_change(
            BINDING_A,
            THREAD_UUID_A,
            &ConversationChangeRequest {
                request_key: "chg-scope-replan-1".to_string(),
                expected_change_revision: 1,
                approval_id: Some(approval_id.clone()),
                plan_id: Some("plan-conv-gate".to_string()),
                expected_plan_revision: Some(approval_rec.plan_revision),
                task_id: Some("task-coder-hard-1".to_string()),
                expected_task_revision: None,
                kind: ConversationChangeKind::ScopeChange,
                route: Some(ConversationChangeRoute::ScopeOrModelReplan),
                summary: "Expand coder scope to src/new_feature.rs".to_string(),
                detail: Some("Requires replan and new operator approval".to_string()),
                proposed_write_paths: vec![
                    "src/lib.rs".to_string(),
                    "src/new_feature.rs".to_string(),
                ],
                proposed_profile_id: None,
                proposed_model_id: None,
                proposed_effort: None,
                requested_delivery: ConversationDeliveryRequest::LiveContinuationIfSupported,
                cancel_action: None,
            },
            &fixture.plans,
            &fixture.authority,
            &client,
        )
        .await
        .expect("record scope_change");

    assert!(scope_record.decision.blocks_task_advancement);
    assert!(scope_record.decision.replan_required);
    assert!(scope_record.decision.new_approval_required);
    assert!(scope_record.decision.frozen_approval_immutable);
    assert!(!scope_record.decision.live_input_delivered);
    assert!(!scope_record.decision.process_paused);
    assert!(!scope_record.decision.provider_resumed);

    let blocked_handoff_err = store
        .handoff_role(
            &RoleHandoffRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                cycle_id: cycle.cycle_id.clone(),
                expected_cycle_revision: 2,
                request_key: "hnd-coder-blocked-by-scope-change".to_string(),
                task_id: "task-coder-hard-1".to_string(),
                current_policy: policy.clone(),
                operator_approved: true,
                confirm_approval_digest: Some(cycle.approval_subject_digest.clone()),
                closure_reservation_id: None,
            },
            &fixture.authority,
            Some(&client),
        )
        .await
        .expect_err("pending scope_change must block role handoff");
    assert_eq!(
        blocked_handoff_err.error_code(),
        NativeErrorCode::ValidationFailed
    );
    assert_eq!(read_invocation_count(&fixture.agy_count_file), 0);
}

#[tokio::test]
async fn cross_cycle_replay_alias_closure_and_known_refusal_boundaries() {
    let fixture = setup_fixture();
    let store = RoleFlowStore::new(&fixture.plans_dir).expect("store");
    let policy = default_policy();
    let mut tasks = default_tasks();
    tasks
        .iter_mut()
        .find(|t| t.id == "task-closure")
        .unwrap()
        .depends_on = vec!["task-coder-hard-1".into()];
    tasks
        .iter_mut()
        .find(|t| t.id == "task-coder-hard-2")
        .unwrap()
        .depends_on = vec!["task-coder-hard-1".into()];
    let approval = create_approved_plan_at_commit(
        &fixture,
        "boundary-regressions",
        BINDING_A,
        THREAD_UUID_A,
        &fixture.base_commit,
        tasks,
    );
    let cycle_request = CreateRoleFlowCycleRequest {
        binding_id: BINDING_A.to_string(),
        thread_uuid: THREAD_UUID_A.to_string(),
        request_key: "boundaries-cycle-one".into(),
        expected_ledger_revision: Some(0),
        approval_id: approval.clone(),
        policy: policy.clone(),
        task_ids: vec![
            "task-coder-hard-1".into(),
            "task-coder-hard-2".into(),
            "task-native-follower".into(),
            "task-closure".into(),
        ],
    };
    let cycle = store
        .create_cycle(&cycle_request, &fixture.authority)
        .expect("cycle");
    let make_request = |task: &str, key: &str, revision| RoleHandoffRequest {
        binding_id: BINDING_A.to_string(),
        thread_uuid: THREAD_UUID_A.to_string(),
        cycle_id: cycle.cycle_id.clone(),
        expected_cycle_revision: revision,
        request_key: key.into(),
        task_id: task.into(),
        current_policy: policy.clone(),
        operator_approved: true,
        confirm_approval_digest: Some(cycle.approval_subject_digest.clone()),
        closure_reservation_id: None,
    };
    let reserve = ReservePackageClosureRequest {
        binding_id: BINDING_A.to_string(),
        thread_uuid: THREAD_UUID_A.to_string(),
        cycle_id: cycle.cycle_id.clone(),
        expected_cycle_revision: 1,
        request_key: "boundary-reserve".into(),
        package_id: "package-original".into(),
        source: ClosureSubjectSource::ApprovedCycleBase {
            subject_digest: cycle.approval_subject_digest.clone(),
            expected_candidate_tree_sha: None,
        },
        task_id: Some("task-closure".into()),
    };
    assert!(store
        .reserve_closure(&ReservePackageClosureRequest {
            task_id: None,
            ..reserve.clone()
        })
        .is_err());
    let reservation = store.reserve_closure(&reserve).expect("bound reservation");
    let current = store
        .get_cycle(BINDING_A, &cycle.cycle_id)
        .unwrap()
        .unwrap();
    assert!(
        store
            .reserve_closure(&ReservePackageClosureRequest {
                request_key: "alias-reservation".into(),
                package_id: "package-renamed".into(),
                expected_cycle_revision: current.cycle_revision,
                ..reserve.clone()
            })
            .is_err(),
        "renaming cannot mint closure authority"
    );
    let mut closure = make_request(
        "task-closure",
        "premature-native-close",
        current.cycle_revision,
    );
    closure.closure_reservation_id = Some(reservation.reservation_id.clone());
    let error = store
        .handoff_role(&closure, &fixture.authority, None)
        .await
        .expect_err("unverified native dependency");
    assert_eq!(error.error_code(), NativeErrorCode::UnsupportedCapability);
    assert!(store
        .get_cycle(BINDING_A, &cycle.cycle_id)
        .unwrap()
        .unwrap()
        .handoffs
        .is_empty());
    assert_eq!(store.list_closure_reservations(BINDING_A).unwrap().len(), 1);

    let native = make_request(
        "task-native-follower",
        "native-first",
        current.cycle_revision,
    );
    store
        .handoff_role(&native, &fixture.authority, None)
        .await
        .expect("native record");
    let second = store
        .create_cycle(
            &CreateRoleFlowCycleRequest {
                request_key: "boundaries-cycle-two".into(),
                expected_ledger_revision: None,
                ..cycle_request.clone()
            },
            &fixture.authority,
        )
        .expect("second cycle");
    assert!(store
        .handoff_role(
            &RoleHandoffRequest {
                cycle_id: second.cycle_id.clone(),
                expected_cycle_revision: second.cycle_revision,
                request_key: "native-alias-cycle".into(),
                ..native.clone()
            },
            &fixture.authority,
            None
        )
        .await
        .is_err());
    assert!(store
        .get_cycle(BINDING_A, &second.cycle_id)
        .unwrap()
        .unwrap()
        .handoffs
        .is_empty());

    fs::create_dir_all(&fixture.sessions_dir).expect("sessions");
    fs::set_permissions(&fixture.sessions_dir, fs::Permissions::from_mode(0o700)).unwrap();
    let client = ApprovedHostClient::new(
        Arc::clone(&fixture.authority),
        &fixture.sessions_dir,
        BINDING_A.to_string(),
        THREAD_UUID_A.to_string(),
    )
    .expect("client");
    let current = store
        .get_cycle(BINDING_A, &cycle.cycle_id)
        .unwrap()
        .unwrap();
    let first_request = make_request(
        "task-coder-hard-1",
        "host-not-ready",
        current.cycle_revision,
    );
    assert!(store
        .handoff_role(&first_request, &fixture.authority, Some(&client))
        .await
        .is_err());
    let refused = store
        .handoff_role(&first_request, &fixture.authority, Some(&client))
        .await
        .expect("idempotent refusal read");
    assert_eq!(refused.status, HandoffStatus::RefusedBeforeEffect);
    assert_eq!(read_invocation_count(&fixture.agy_count_file), 0);
    let host = ApprovedHost::start(ApprovedHostConfig {
        registry_dir: fixture.registry_dir.clone(),
        plans_dir: fixture.plans_dir.clone(),
        authority_dir: fixture.authority_dir.clone(),
        sessions_dir: fixture.sessions_dir.clone(),
        approval_id: approval.clone(),
        tick_interval: Duration::from_millis(25),
        notifications: None,
    })
    .await
    .expect("host");
    let current = store
        .get_cycle(BINDING_A, &cycle.cycle_id)
        .unwrap()
        .unwrap();
    let retry = make_request(
        "task-coder-hard-1",
        "explicit-new-ready-attempt",
        current.cycle_revision,
    );
    let dispatched = store
        .handoff_role(&retry, &fixture.authority, Some(&client))
        .await
        .expect("safe new attempt");
    wait_for_task_ready(&client, &approval, "task-coder-hard-1").await;
    assert_eq!(read_invocation_count(&fixture.agy_count_file), 1);
    let current = store
        .get_cycle(BINDING_A, &cycle.cycle_id)
        .unwrap()
        .unwrap();
    let dependent = make_request(
        "task-coder-hard-2",
        "unreviewed-dependency",
        current.cycle_revision,
    );
    assert!(store
        .handoff_role(&dependent, &fixture.authority, Some(&client))
        .await
        .is_err());
    let current = store
        .get_cycle(BINDING_A, &cycle.cycle_id)
        .unwrap()
        .unwrap();
    assert_eq!(
        current.handoffs.last().unwrap().status,
        HandoffStatus::RefusedBeforeEffect
    );
    assert_eq!(read_invocation_count(&fixture.agy_count_file), 1);
    // An actual uncertain marker still blocks another cycle even though a real
    // no-effect refusal is safely retried with a distinct explicit request key.
    let path = store.ledger_path_for_binding(BINDING_A).unwrap();
    let mut ledger: serde_json::Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
    let item = ledger["cycles"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|c| c["cycle_id"] == cycle.cycle_id)
        .unwrap()["handoffs"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|h| h["handoff_id"] == dispatched.handoff_id)
        .unwrap();
    item["status"] = serde_json::to_value(HandoffStatus::OutcomeUnknown).unwrap();
    fs::write(&path, serde_json::to_vec_pretty(&ledger).unwrap()).unwrap();
    let error = store
        .handoff_role(
            &RoleHandoffRequest {
                cycle_id: second.cycle_id,
                expected_cycle_revision: 1,
                request_key: "cross-cycle-unknown".into(),
                ..retry
            },
            &fixture.authority,
            Some(&client),
        )
        .await
        .expect_err("cross-cycle uncertain outcome cannot replay");
    assert_eq!(
        error.error_code(),
        NativeErrorCode::AmbiguousWakeupPendingReconciliation
    );
    assert_eq!(read_invocation_count(&fixture.agy_count_file), 1);
    host.shutdown().await.expect("shutdown host");
}
