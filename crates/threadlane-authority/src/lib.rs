#![forbid(unsafe_code)]
#![deny(missing_docs)]

//! `threadlane-authority`: operator-only workflow approval artifacts and deterministic frozen
//! worker-config synthesis for Threadlane Milestone 4.
//!
//! ## Core Invariants
//!
//! - **Operator-only approval boundary**: [`AuthorityStore::approve`] and
//!   [`AuthorityStore::approve_confirmed`] may only be called by the trusted operator CLI or
//!   Tauri control panel after explicit display and confirmation of the request parameters. MCP
//!   tools must never call or expose `approve`, and worker/chat prose never grants authority.
//! - **Complete frozen subject**: Every [`ApprovalRecord`] freezes a full copy of the resolved
//!   model, effort, canonical CLI executable path (resolved once at approval time), exact task
//!   write scope, operator allowlist, task instruction, dependencies, execution budgets,
//!   `max_parallel`, canonical Git repository path, exact 40-hex `base_commit`, canonical non-nil
//!   `thread_uuid`, `binding_id`, `plan_id`, `plan_revision`, `registry_revision`, `plan_digest`,
//!   and `policy_version` under a 64-character lowercase hex SHA-256 `subject_digest`. Subsequent
//!   edits to [`threadlane_registry::RegistryStore`] do not alter existing approvals.
//! - **Approval != execution or acceptance**: Creating or materializing an approval never mutates
//!   `PlanStatus::Received` or `PlanStatus::Applied`, never spawns worker processes, never creates
//!   Git worktrees, and never performs merge or push operations.
//! - **Side-effect-free materialization**: [`AuthorityStore::materialize`] loads the trusted
//!   [`ApprovalRecord`] by `approval_id`, enforces `session_binding.thread_uuid == approved thread_uuid`,
//!   binds generated worker profiles exclusively to `session_binding.binding_id`, rejects
//!   `Superseded` source plans (while allowing older published revisions when only a newer `Draft`
//!   exists), synthesizes [`threadlane::WorkersConfig`] for selected `antigravity` and
//!   `opencode_v2` tasks only (`None` for native-only `codex_cli` approvals), and validates the
//!   configuration via side-effect-free [`threadlane::WorkersConfig::validate`].

/// Structured domain, validation, store, and materialization error types.
pub mod error;
/// Persistent cross-process locked authority store (`AuthorityStore`).
pub mod store;
/// Serializable approval request, record, summary, and materialized plan types.
pub mod types;
/// Identifier, UUID, Git repository/commit, write scope, executable, and subject-digest validators.
pub mod validation;

pub use error::AuthorityError;
pub use store::{
    build_approved_worker_profile_config, compute_recorded_history_worker_profile_digest,
    AuthorityStore,
};
pub use types::{
    ApprovalRecord, ApprovalSummary, ApprovedTaskDispatch, ApprovedTaskRecord, MaterializedPlan,
    NativeRoute, OperatorApprovalIntent, OperatorApprovalIntentV2, OperatorApprovalPreview,
    OperatorApprovalRequest, OperatorApprovalRequestV2, APPROVALS_FILE_NAME,
    APPROVALS_LOCK_FILE_NAME, AUTHORITY_POLICY_VERSION, AUTHORITY_REQUEST_V2_SCHEMA_VERSION,
    AUTHORITY_SCHEMA_VERSION, MAX_APPROVALS_IN_STORE, MAX_APPROVAL_MAX_PARALLEL,
    MAX_APPROVED_TASKS, MAX_APPROVED_WRITE_PATHS, MAX_AUTHORITY_STORE_BYTES,
    MAX_EXECUTABLE_PROFILES_PER_APPROVAL, MAX_REQUIRED_CHANGED_PATHS, MIN_APPROVAL_MAX_PARALLEL,
};
pub use validation::{
    compute_authority_core_digest, compute_record_subject_digest, derive_approval_id,
    derive_command_key, derive_profile_id, is_task_scope_contained_in_operator_allowlist,
    resolve_and_validate_worker_executable, validate_approved_executable_task_instructions,
    validate_canonical_thread_uuid, validate_executable_worker_instruction,
    validate_repository_and_base_commit, validate_required_changed_paths_at_base_commit,
    validate_v2_schema_version, validate_write_scope_entry,
    validate_write_scope_entry_at_base_commit, verify_approval_record_integrity,
};

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::fs::{self, File};
    use std::io::Write;
    use std::os::unix::fs::{symlink, PermissionsExt};
    use std::path::{Path, PathBuf};
    use std::sync::Arc;

    use tempfile::TempDir;
    use threadlane_core::Binding;
    use threadlane_plans::{
        compile_plan_revision, Difficulty, ModelChoice, ModelDefinition, PlanChange, PlanStatus,
        PlanStore, PlanTask, RoleAssignment, ToolAdapter, ToolDefinition, WorkflowProfile,
        WorkflowRole,
    };
    use threadlane_registry::{
        RegistryChange, RegistryStore, ScopedModelDependency, ScopedProfileDependencySpec,
        ScopedToolDependency,
    };
    use threadlane_workers::{
        resolve_exact_commit_sha, run_sanitized_git, ExecutionBudgets, WorkerFilePermissions,
        WorkerProvider, DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    };

    use super::*;

    const THREAD_UUID_A: &str = "550e8400-e29b-41d4-a716-446655440000";
    const THREAD_UUID_B: &str = "6ba7b810-9dad-11d1-80b4-00c04fd430c8";

    struct TestFixture {
        _tmp: TempDir,
        root: PathBuf,
        repo_dir: PathBuf,
        base_commit: String,
        second_commit: String,
        fake_agy_exe: PathBuf,
        fake_oc_exe: PathBuf,
        registry: Arc<RegistryStore>,
        plans: Arc<PlanStore>,
        authority: AuthorityStore,
    }

    fn create_executable_script(path: &Path) {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).expect("create script parent dir");
        }
        let mut f = File::create(path).expect("create fake executable");
        f.write_all(b"#!/bin/sh\nexit 0\n")
            .expect("write fake executable");
        f.sync_all().expect("sync fake executable");
        fs::set_permissions(path, fs::Permissions::from_mode(0o755))
            .expect("chmod 0755 fake executable");
    }

    fn init_disposable_git_repo(repo_dir: &Path) -> (String, String) {
        fs::create_dir_all(repo_dir.join("src/sub")).expect("create repo src/sub");
        fs::create_dir_all(repo_dir.join("unapproved")).expect("create repo unapproved");
        fs::write(repo_dir.join("src/lib.rs"), "pub fn alpha() -> u32 { 1 }\n")
            .expect("write src/lib.rs");
        fs::write(repo_dir.join("src/extra.rs"), "pub fn extra() {}\n")
            .expect("write src/extra.rs");
        fs::write(repo_dir.join("src/sub/mod.rs"), "pub mod inner;\n")
            .expect("write src/sub/mod.rs");
        fs::write(repo_dir.join("unapproved/secret.rs"), "pub fn nope() {}\n")
            .expect("write unapproved/secret.rs");

        run_sanitized_git(
            repo_dir,
            &["init", "--quiet"],
            DEFAULT_GIT_COMMAND_TIMEOUT_MS,
        )
        .expect("git init");
        run_sanitized_git(
            repo_dir,
            &["config", "user.email", "test@threadlane.local"],
            DEFAULT_GIT_COMMAND_TIMEOUT_MS,
        )
        .expect("git config email");
        run_sanitized_git(
            repo_dir,
            &["config", "user.name", "Threadlane Test"],
            DEFAULT_GIT_COMMAND_TIMEOUT_MS,
        )
        .expect("git config name");
        run_sanitized_git(repo_dir, &["add", "."], DEFAULT_GIT_COMMAND_TIMEOUT_MS)
            .expect("git add");
        run_sanitized_git(
            repo_dir,
            &["commit", "-m", "initial commit", "--quiet"],
            DEFAULT_GIT_COMMAND_TIMEOUT_MS,
        )
        .expect("git commit 1");

        let commit1 = resolve_exact_commit_sha(repo_dir, "HEAD", DEFAULT_GIT_COMMAND_TIMEOUT_MS)
            .expect("resolve HEAD commit 1");

        fs::write(repo_dir.join("src/lib.rs"), "pub fn alpha() -> u32 { 2 }\n")
            .expect("update src/lib.rs");
        run_sanitized_git(repo_dir, &["add", "."], DEFAULT_GIT_COMMAND_TIMEOUT_MS)
            .expect("git add 2");
        run_sanitized_git(
            repo_dir,
            &["commit", "-m", "second commit", "--quiet"],
            DEFAULT_GIT_COMMAND_TIMEOUT_MS,
        )
        .expect("git commit 2");

        let commit2 = resolve_exact_commit_sha(repo_dir, "HEAD", DEFAULT_GIT_COMMAND_TIMEOUT_MS)
            .expect("resolve HEAD commit 2");

        (commit1, commit2)
    }

    fn setup_fixture() -> TestFixture {
        let tmp = TempDir::new().expect("tempdir");
        let root = fs::canonicalize(tmp.path()).expect("canonicalize temp root");

        let repo_dir = root.join("repo");
        let (base_commit, second_commit) = init_disposable_git_repo(&repo_dir);

        let bin_dir = root.join("bin");
        let fake_agy_exe = bin_dir.join("fake-agy");
        let fake_oc_exe = bin_dir.join("fake-opencode");
        create_executable_script(&fake_agy_exe);
        create_executable_script(&fake_oc_exe);

        let registry_dir = root.join("registry-store");
        let plans_dir = root.join("plans-store");
        let authority_dir = root.join("authority-store");

        let registry = Arc::new(RegistryStore::open(&registry_dir).expect("open registry"));
        let plans = Arc::new(PlanStore::open(&plans_dir).expect("open plans"));

        // Populate registry tools, models, and workflow profile (revisions 1..=5).
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
                RegistryChange::UpsertTool {
                    tool: ToolDefinition {
                        id: "tool-oc".to_string(),
                        label: "OpenCode v2 CLI".to_string(),
                        adapter: ToolAdapter::OpencodeV2,
                        executable_path: Some(fake_oc_exe.clone()),
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
                            model_arg: "gpt-5-codex".to_string(),
                            efforts: vec!["high".to_string()],
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
                                role: WorkflowRole::Reviewer,
                                difficulty: None,
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

        let authority =
            AuthorityStore::open(&authority_dir, Arc::clone(&registry), Arc::clone(&plans))
                .expect("open authority store");

        TestFixture {
            _tmp: tmp,
            root,
            repo_dir,
            base_commit,
            second_commit,
            fake_agy_exe,
            fake_oc_exe,
            registry,
            plans,
            authority,
        }
    }

    fn save_and_publish_plan(
        fixture: &TestFixture,
        plan_id: &str,
        binding_id: &str,
        profile_revision: u64,
        tasks: Vec<PlanTask>,
    ) -> (u64, String) {
        let draft = fixture
            .plans
            .change_at(
                0,
                PlanChange::SaveDraft {
                    plan_id: plan_id.to_string(),
                    binding_id: binding_id.to_string(),
                    profile_id: "profile-main".to_string(),
                    profile_revision,
                    summary: format!("Summary for {plan_id}"),
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

        let reg_snap = fixture.registry.snapshot().expect("registry snapshot");
        let compiled =
            compile_plan_revision(&published, &reg_snap).expect("compile published plan");
        (published.revision, compiled.plan_digest)
    }

    fn sample_binding(binding_id: &str, thread_uuid: &str) -> Binding {
        Binding {
            binding_id: binding_id.to_string(),
            thread_uuid: thread_uuid.to_string(),
            label: "Session Binding".to_string(),
            delivered_seq: 0,
            acked_seq: 0,
        }
    }

    #[test]
    fn store_open_permissions_idempotency_reopen_and_corruption_preservation() {
        let fixture = setup_fixture();

        // Authority directory and lock file must have private 0700 / 0600 permissions.
        let dir_mode = fs::symlink_metadata(fixture.authority.dir())
            .expect("authority dir metadata")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(dir_mode, 0o700);

        let lock_mode = fs::symlink_metadata(fixture.authority.lock_path())
            .expect("authority lock metadata")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(lock_mode, 0o600);

        // Sharing directory with registry or plan store or symlink directory must be refused.
        let overlap_reg = AuthorityStore::open(
            fixture.registry.config_dir(),
            Arc::clone(&fixture.registry),
            Arc::clone(&fixture.plans),
        )
        .expect_err("must reject overlapping registry dir");
        assert_eq!(overlap_reg.code(), "security_violation");

        let overlap_plans = AuthorityStore::open(
            fixture.plans.dir(),
            Arc::clone(&fixture.registry),
            Arc::clone(&fixture.plans),
        )
        .expect_err("must reject overlapping plans dir");
        assert_eq!(overlap_plans.code(), "security_violation");

        let symlink_auth_dir = fixture.root.join("symlink-authority");
        symlink(fixture.authority.dir(), &symlink_auth_dir).expect("create symlink dir");
        let symlink_err = AuthorityStore::open(
            &symlink_auth_dir,
            Arc::clone(&fixture.registry),
            Arc::clone(&fixture.plans),
        )
        .expect_err("must reject symlink authority dir");
        assert_eq!(symlink_err.code(), "security_violation");

        // Configure `/bin/sh` as executable on tool-agy to verify NixOS symlink canonicalization.
        let snap = fixture
            .registry
            .apply(
                5,
                RegistryChange::UpsertTool {
                    tool: ToolDefinition {
                        id: "tool-agy".to_string(),
                        label: "Antigravity via /bin/sh fixture".to_string(),
                        adapter: ToolAdapter::Antigravity,
                        executable_path: Some(PathBuf::from("/bin/sh")),
                        enabled: true,
                    },
                },
            )
            .expect("update tool-agy to /bin/sh");

        let (plan_rev, plan_digest) = save_and_publish_plan(
            &fixture,
            "plan-sh",
            "chat-binding-1",
            snap.revision,
            vec![PlanTask {
                id: "task-1".to_string(),
                title: "Implement slice".to_string(),
                instruction: "Write deterministic authority logic".to_string(),
                role: WorkflowRole::Coder,
                difficulty: Difficulty::Hard,
                depends_on: vec![],
                allowed_write_paths: vec!["src/lib.rs".to_string()],
            }],
        );

        let req = OperatorApprovalRequest {
            binding_id: "chat-binding-1".to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            plan_id: "plan-sh".to_string(),
            plan_revision: plan_rev,
            expected_registry_revision: snap.revision,
            expected_plan_digest: plan_digest,
            repository_path: fixture.repo_dir.clone(),
            base_commit: fixture.base_commit.clone(),
            approved_task_ids: vec!["task-1".to_string()],
            approved_write_paths: vec!["src/lib.rs".to_string()],
            max_parallel: 2,
            budgets: ExecutionBudgets::default(),
        };

        let first = fixture
            .authority
            .approve_at(req.clone(), 1_700_000_111_000)
            .expect("first approval");
        assert!(first.approval_id.starts_with("apr_"));
        assert_eq!(first.subject_digest.len(), 64);
        assert_eq!(first.created_at_ms, 1_700_000_111_000);
        assert_eq!(first.worker_profile_count, 1);
        assert_eq!(first.native_route_count, 0);

        let file_mode = fs::symlink_metadata(fixture.authority.approvals_path())
            .expect("approvals.json metadata")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(file_mode, 0o600);

        // Exact repeat is idempotent and preserves original creation time.
        let second = fixture
            .authority
            .approve_at(req, 1_700_000_999_999)
            .expect("idempotent repeat approval");
        assert_eq!(second, first);
        assert_eq!(second.created_at_ms, 1_700_000_111_000);

        // Reopen store and verify full operator record vs bounded summary.
        let reopened = AuthorityStore::open(
            fixture.authority.dir(),
            Arc::clone(&fixture.registry),
            Arc::clone(&fixture.plans),
        )
        .expect("reopen authority store");

        let full = reopened
            .get_for_operator(&first.approval_id)
            .expect("get_for_operator");
        let canonical_sh = fs::canonicalize("/bin/sh").expect("canonicalize /bin/sh");
        assert_eq!(
            full.tasks[0].canonical_executable.as_deref(),
            Some(canonical_sh.as_path())
        );
        assert!(
            !fs::symlink_metadata(&canonical_sh)
                .expect("canonical /bin/sh meta")
                .file_type()
                .is_symlink(),
            "canonicalized /bin/sh target must not be a symlink"
        );

        let summaries = reopened
            .list_for_binding("chat-binding-1")
            .expect("list_for_binding");
        assert_eq!(summaries, vec![first.clone()]);
        let summary_json = serde_json::to_string(&summaries).expect("serialize summaries");
        assert!(!summary_json.contains("instruction"));
        assert!(!summary_json.contains("canonical_executable"));
        assert!(!summary_json.contains(canonical_sh.to_str().unwrap()));

        // Tamper with payload on disk without updating digest -> must fail with CorruptStore and preserve file.
        let original_bytes =
            fs::read(reopened.approvals_path()).expect("read valid approvals.json");
        let tampered_str = String::from_utf8(original_bytes.clone())
            .expect("utf8")
            .replace(
                "Write deterministic authority logic",
                "Tampered instruction payload",
            );
        fs::write(reopened.approvals_path(), tampered_str.as_bytes())
            .expect("write tampered approvals.json");

        let tamper_err = reopened
            .get_for_operator(&first.approval_id)
            .expect_err("tampered payload must fail integrity verification");
        assert_eq!(tamper_err.code(), "corrupt_store");
        assert_eq!(
            fs::read(reopened.approvals_path()).expect("read preserved tampered file"),
            tampered_str.as_bytes(),
            "corrupt store file must be preserved untouched"
        );

        // Restore valid bytes, then test malformed JSON corruption preservation on open.
        fs::write(reopened.approvals_path(), &original_bytes).expect("restore valid bytes");
        assert_eq!(
            reopened
                .get_for_operator(&first.approval_id)
                .expect("restored")
                .approval_id,
            first.approval_id
        );

        let corrupt_json = b"{corrupt-authority-ledger";
        fs::write(reopened.approvals_path(), corrupt_json).expect("write corrupt json");
        let open_corrupt = AuthorityStore::open(
            fixture.authority.dir(),
            Arc::clone(&fixture.registry),
            Arc::clone(&fixture.plans),
        )
        .expect_err("open on corrupt json must fail");
        assert_eq!(open_corrupt.code(), "corrupt_store");
        assert_eq!(
            fs::read(reopened.approvals_path()).expect("corrupt bytes preserved"),
            corrupt_json
        );
    }

    #[test]
    fn plan_binding_status_digest_and_supersede_guards() {
        let fixture = setup_fixture();
        let reg_rev = fixture.registry.snapshot().expect("snap").revision;

        let task = PlanTask {
            id: "task-1".to_string(),
            title: "Task 1".to_string(),
            instruction: "Implement task 1".to_string(),
            role: WorkflowRole::Coder,
            difficulty: Difficulty::Hard,
            depends_on: vec![],
            allowed_write_paths: vec!["src/lib.rs".to_string()],
        };

        // 1. Draft plan cannot be approved.
        let draft = fixture
            .plans
            .change(
                0,
                PlanChange::SaveDraft {
                    plan_id: "plan-lifecycle".to_string(),
                    binding_id: "binding-owner".to_string(),
                    profile_id: "profile-main".to_string(),
                    profile_revision: reg_rev,
                    summary: "Draft rev 1".to_string(),
                    tasks: vec![task.clone()],
                },
            )
            .expect("save draft 1");

        let base_req = OperatorApprovalRequest {
            binding_id: "binding-owner".to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            plan_id: "plan-lifecycle".to_string(),
            plan_revision: 1,
            expected_registry_revision: reg_rev,
            expected_plan_digest: "0".repeat(64),
            repository_path: fixture.repo_dir.clone(),
            base_commit: fixture.base_commit.clone(),
            approved_task_ids: vec!["task-1".to_string()],
            approved_write_paths: vec!["src/lib.rs".to_string()],
            max_parallel: 1,
            budgets: ExecutionBudgets::default(),
        };

        let draft_err = fixture
            .authority
            .approve(base_req.clone())
            .expect_err("draft plan must be rejected");
        assert_eq!(draft_err.code(), "unpublished_or_superseded_plan");

        // Publish rev 1 and compute digest.
        let published = fixture
            .plans
            .change(
                draft.revision,
                PlanChange::Publish {
                    plan_id: "plan-lifecycle".to_string(),
                    revision: 1,
                },
            )
            .expect("publish rev 1");
        let compiled = compile_plan_revision(
            &published,
            &fixture.registry.snapshot().expect("registry snap"),
        )
        .expect("compile rev 1");

        let mut valid_req = base_req;
        valid_req.expected_plan_digest = compiled.plan_digest.clone();

        // 2. Wrong binding_id must be rejected.
        let mut wrong_binding_req = valid_req.clone();
        wrong_binding_req.binding_id = "other-binding".to_string();
        let binding_err = fixture
            .authority
            .approve(wrong_binding_req)
            .expect_err("cross-binding approval must fail");
        assert_eq!(binding_err.code(), "binding_mismatch");

        // 3. Stale registry revision must be rejected.
        let mut wrong_reg_req = valid_req.clone();
        wrong_reg_req.expected_registry_revision = reg_rev + 1;
        let reg_err = fixture
            .authority
            .approve(wrong_reg_req)
            .expect_err("mismatched registry revision must fail");
        assert_eq!(reg_err.code(), "stale_registry_revision");

        // 4. Wrong compiled plan digest must be rejected.
        let mut wrong_digest_req = valid_req.clone();
        wrong_digest_req.expected_plan_digest = "f".repeat(64);
        let digest_err = fixture
            .authority
            .approve(wrong_digest_req)
            .expect_err("mismatched plan digest must fail");
        assert_eq!(digest_err.code(), "plan_digest_mismatch");

        // 5. Approve rev 1 while Published -> succeeds and does NOT mutate PlanStatus to Received/Applied.
        let summary = fixture
            .authority
            .approve(valid_req.clone())
            .expect("approve published rev 1");
        assert_eq!(
            fixture
                .plans
                .get_revision("plan-lifecycle", 1)
                .expect("get rev 1")
                .expect("exists")
                .status,
            PlanStatus::Published,
            "approve must never mutate PlanStatus"
        );

        // Transition rev 1 to Received and Applied -> approve remains idempotent and valid.
        fixture
            .plans
            .change(
                1,
                PlanChange::Receive {
                    plan_id: "plan-lifecycle".to_string(),
                    revision: 1,
                },
            )
            .expect("receive rev 1");
        assert_eq!(
            fixture
                .authority
                .approve(valid_req.clone())
                .expect("approve received rev 1"),
            summary
        );

        fixture
            .plans
            .change(
                1,
                PlanChange::Apply {
                    plan_id: "plan-lifecycle".to_string(),
                    revision: 1,
                },
            )
            .expect("apply rev 1");
        assert_eq!(
            fixture
                .authority
                .approve(valid_req.clone())
                .expect("approve applied rev 1"),
            summary
        );

        // 6. Save a newer Draft (rev 2) without publishing -> rev 1 stays Applied and materialization still succeeds!
        let _draft2 = fixture
            .plans
            .change(
                1,
                PlanChange::SaveDraft {
                    plan_id: "plan-lifecycle".to_string(),
                    binding_id: "binding-owner".to_string(),
                    profile_id: "profile-main".to_string(),
                    profile_revision: reg_rev,
                    summary: "Draft rev 2".to_string(),
                    tasks: vec![task],
                },
            )
            .expect("save draft rev 2");

        let session_binding = sample_binding("bnd_session_1", THREAD_UUID_A);
        let work_area = fixture.root.join("work-area-1");
        let materialized = fixture
            .authority
            .materialize(&summary.approval_id, &session_binding, &work_area)
            .expect("newer unpublished draft alone must leave old published revision valid");
        assert_eq!(materialized.approval_id, summary.approval_id);

        // 7. Publish rev 2 -> rev 1 becomes Superseded, so materializing or approving rev 1 now fails!
        fixture
            .plans
            .change(
                2,
                PlanChange::Publish {
                    plan_id: "plan-lifecycle".to_string(),
                    revision: 2,
                },
            )
            .expect("publish rev 2");

        let mat_superseded_err = fixture
            .authority
            .materialize(&summary.approval_id, &session_binding, &work_area)
            .expect_err("superseded source plan must reject new materialization");
        assert_eq!(mat_superseded_err.code(), "unpublished_or_superseded_plan");

        let approve_superseded_err = fixture
            .authority
            .approve(valid_req)
            .expect_err("superseded source plan must reject approval");
        assert_eq!(
            approve_superseded_err.code(),
            "unpublished_or_superseded_plan"
        );
    }

    #[test]
    fn thread_uuid_and_session_binding_materialization_isolation() {
        let fixture = setup_fixture();
        let reg_rev = fixture.registry.snapshot().expect("snap").revision;

        let (plan_rev, plan_digest) = save_and_publish_plan(
            &fixture,
            "plan-thread",
            "paired-chat-binding",
            reg_rev,
            vec![PlanTask {
                id: "task-agy".to_string(),
                title: "AGY Task".to_string(),
                instruction: "Run isolated task".to_string(),
                role: WorkflowRole::Coder,
                difficulty: Difficulty::Hard,
                depends_on: vec![],
                allowed_write_paths: vec!["src/lib.rs".to_string()],
            }],
        );

        let base_req = OperatorApprovalRequest {
            binding_id: "paired-chat-binding".to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            plan_id: "plan-thread".to_string(),
            plan_revision: plan_rev,
            expected_registry_revision: reg_rev,
            expected_plan_digest: plan_digest,
            repository_path: fixture.repo_dir.clone(),
            base_commit: fixture.base_commit.clone(),
            approved_task_ids: vec!["task-agy".to_string()],
            approved_write_paths: vec!["src/lib.rs".to_string()],
            max_parallel: 2,
            budgets: ExecutionBudgets::default(),
        };

        // Reject nil UUID and uppercase UUID.
        for bad_uuid in [
            "00000000-0000-0000-0000-000000000000",
            "550E8400-E29B-41D4-A716-446655440000",
            "not-a-uuid",
        ] {
            let mut bad_req = base_req.clone();
            bad_req.thread_uuid = bad_uuid.to_string();
            let err = fixture
                .authority
                .approve(bad_req)
                .expect_err("invalid or nil thread_uuid must be rejected");
            assert_eq!(err.code(), "validation");
        }

        let summary = fixture
            .authority
            .approve(base_req)
            .expect("approve valid thread_uuid");

        // Materialize with different thread_uuid -> rejected.
        let wrong_thread_binding = sample_binding("bnd_runtime_other", THREAD_UUID_B);
        let work_area = fixture.root.join("work-area-thread");
        let mismatch_err = fixture
            .authority
            .materialize(&summary.approval_id, &wrong_thread_binding, &work_area)
            .expect_err("mismatched session thread_uuid must fail");
        assert_eq!(mismatch_err.code(), "thread_uuid_mismatch");

        // Materialize with matching thread_uuid and software-generated session binding_id -> succeeds
        // and session_binding.binding_id becomes the ONLY allowed_binding_ids entry.
        let session_binding = sample_binding("bnd_runtime_session_99", THREAD_UUID_A);
        let materialized = fixture
            .authority
            .materialize(&summary.approval_id, &session_binding, &work_area)
            .expect("matching thread_uuid must materialize");
        let workers = materialized.workers.expect("workers config present");
        assert_eq!(workers.profiles.len(), 1);
        assert_eq!(
            workers.profiles[0].allowed_binding_ids,
            BTreeSet::from(["bnd_runtime_session_99".to_string()])
        );
        assert!(!workers.profiles[0]
            .allowed_binding_ids
            .contains("paired-chat-binding"));
    }

    #[test]
    fn write_scope_containment_ambiguous_dirs_symlinks_and_unsafe_controls() {
        let fixture = setup_fixture();
        let reg_rev = fixture.registry.snapshot().expect("snap").revision;

        let (plan_rev, plan_digest) = save_and_publish_plan(
            &fixture,
            "plan-scopes",
            "binding-1",
            reg_rev,
            vec![
                PlanTask {
                    id: "task-selected".to_string(),
                    title: "Selected task".to_string(),
                    instruction: "Modify src files".to_string(),
                    role: WorkflowRole::Coder,
                    difficulty: Difficulty::Hard,
                    depends_on: vec![],
                    allowed_write_paths: vec![
                        "src/lib.rs".to_string(),
                        "src/sub/mod.rs".to_string(),
                    ],
                },
                PlanTask {
                    id: "task-unselected".to_string(),
                    title: "Unselected task".to_string(),
                    instruction: "Modify unapproved file".to_string(),
                    role: WorkflowRole::Coder,
                    difficulty: Difficulty::Easy,
                    depends_on: vec!["task-selected".to_string()],
                    allowed_write_paths: vec!["unapproved/secret.rs".to_string()],
                },
            ],
        );

        let base_req = OperatorApprovalRequest {
            binding_id: "binding-1".to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            plan_id: "plan-scopes".to_string(),
            plan_revision: plan_rev,
            expected_registry_revision: reg_rev,
            expected_plan_digest: plan_digest,
            repository_path: fixture.repo_dir.clone(),
            base_commit: fixture.base_commit.clone(),
            approved_task_ids: vec!["task-selected".to_string()],
            approved_write_paths: vec!["src/".to_string()],
            max_parallel: 2,
            budgets: ExecutionBudgets::default(),
        };

        // 1. Partial coverage (missing src/sub/mod.rs) -> rejected.
        let mut partial_req = base_req.clone();
        partial_req.approved_write_paths = vec!["src/lib.rs".to_string()];
        let partial_err = fixture
            .authority
            .approve(partial_req)
            .expect_err("partial task write scope coverage must fail");
        assert_eq!(partial_err.code(), "unapproved_task_write_scope");

        // 2. Ambiguous directory scope without trailing slash ("src" when repo/src is a directory) -> rejected.
        let mut no_slash_dir_req = base_req.clone();
        no_slash_dir_req.approved_write_paths = vec!["src".to_string()];
        let no_slash_err = fixture
            .authority
            .approve(no_slash_dir_req)
            .expect_err("directory scope without trailing slash must fail");
        assert_eq!(no_slash_err.code(), "invalid_write_scope");

        // 3. Componentwise mismatch ("src2/" does not contain "src/lib.rs") -> rejected.
        let mut prefix_mismatch_req = base_req.clone();
        prefix_mismatch_req.approved_write_paths = vec!["src2/".to_string()];
        let prefix_err = fixture
            .authority
            .approve(prefix_mismatch_req)
            .expect_err("componentwise prefix mismatch must fail");
        assert_eq!(prefix_err.code(), "unapproved_task_write_scope");

        // 4. Trailing slash on an existing regular file ("src/lib.rs/") -> rejected.
        let mut file_slash_req = base_req.clone();
        file_slash_req.approved_write_paths =
            vec!["src/lib.rs/".to_string(), "src/sub/".to_string()];
        let file_slash_err = fixture
            .authority
            .approve(file_slash_req)
            .expect_err("trailing slash on existing file must fail");
        assert_eq!(file_slash_err.code(), "invalid_write_scope");

        // 5. Protected control paths, traversal, and metacharacters -> rejected.
        for bad_scope in [
            ".git/",
            "AGENTS.md",
            ".env",
            ".opencode/",
            ".cargo/config.toml",
            "../outside.rs",
            "src/*.rs",
            "/abs/path.rs",
        ] {
            let mut bad_req = base_req.clone();
            bad_req.approved_write_paths = vec!["src/".to_string(), bad_scope.to_string()];
            let err = fixture
                .authority
                .approve(bad_req)
                .expect_err("unsafe or protected operator write scope must fail");
            assert_eq!(err.code(), "invalid_write_scope");
        }

        // 6. Symlink inside repository write scope -> rejected.
        let symlink_in_repo = fixture.repo_dir.join("src/symlink_file.rs");
        symlink(fixture.repo_dir.join("src/lib.rs"), &symlink_in_repo)
            .expect("create symlink in repo");
        let mut symlink_scope_req = base_req.clone();
        symlink_scope_req.approved_write_paths =
            vec!["src/".to_string(), "src/symlink_file.rs".to_string()];
        let symlink_err = fixture
            .authority
            .approve(symlink_scope_req)
            .expect_err("symlink write scope must fail");
        assert_eq!(symlink_err.code(), "invalid_write_scope");
        fs::remove_file(&symlink_in_repo).expect("remove temporary symlink");

        // 7. Selecting task-unselected without approving "unapproved/secret.rs" -> rejected.
        let mut both_tasks_req = base_req.clone();
        both_tasks_req.approved_task_ids =
            vec!["task-selected".to_string(), "task-unselected".to_string()];
        let unapproved_task_err = fixture
            .authority
            .approve(both_tasks_req)
            .expect_err("unapproved write path on second selected task must fail");
        assert_eq!(unapproved_task_err.code(), "unapproved_task_write_scope");

        // 8. Valid approval of only task-selected with trailing-slash "src/" -> succeeds, and
        // task-unselected never becomes authorized!
        let summary = fixture
            .authority
            .approve(base_req)
            .expect("approve selected task under src/");
        assert_eq!(summary.approved_task_ids, vec!["task-selected".to_string()]);
        let record = fixture
            .authority
            .get_for_operator(&summary.approval_id)
            .expect("get record");
        assert_eq!(record.tasks.len(), 1);
        assert_eq!(record.tasks[0].task_id, "task-selected");

        let materialized = fixture
            .authority
            .materialize(
                &summary.approval_id,
                &sample_binding("bnd_1", THREAD_UUID_A),
                &fixture.root.join("work-area-scopes"),
            )
            .expect("materialize selected task");
        assert_eq!(materialized.task_dispatch.len(), 1);
        assert_eq!(materialized.task_dispatch[0].task_id, "task-selected");
        let workers = materialized.workers.expect("workers");
        assert_eq!(workers.profiles.len(), 1);
        assert_eq!(
            workers.profiles[0].spec.allowed_write_paths,
            BTreeSet::from(["src/lib.rs".to_string(), "src/sub/mod.rs".to_string()]),
            "worker profile must freeze the exact task write scope, not the broader operator allowlist"
        );
    }

    #[test]
    fn subject_digest_covers_all_authority_fields_and_base_commit_validation() {
        let fixture = setup_fixture();
        let reg_rev = fixture.registry.snapshot().expect("snap").revision;

        // Symbolic refs, short SHAs, and unknown 40-hex SHAs must be rejected.
        let (plan_rev, plan_digest) = save_and_publish_plan(
            &fixture,
            "plan-subject",
            "binding-1",
            reg_rev,
            vec![
                PlanTask {
                    id: "task-1".to_string(),
                    title: "Task 1".to_string(),
                    instruction: "Instruction 1".to_string(),
                    role: WorkflowRole::Coder,
                    difficulty: Difficulty::Hard,
                    depends_on: vec![],
                    allowed_write_paths: vec!["src/lib.rs".to_string()],
                },
                PlanTask {
                    id: "task-2".to_string(),
                    title: "Task 2".to_string(),
                    instruction: "Instruction 2".to_string(),
                    role: WorkflowRole::Coder,
                    difficulty: Difficulty::Easy,
                    depends_on: vec!["task-1".to_string()],
                    allowed_write_paths: vec!["src/extra.rs".to_string()],
                },
            ],
        );

        let base_req = OperatorApprovalRequest {
            binding_id: "binding-1".to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            plan_id: "plan-subject".to_string(),
            plan_revision: plan_rev,
            expected_registry_revision: reg_rev,
            expected_plan_digest: plan_digest,
            repository_path: fixture.repo_dir.clone(),
            base_commit: fixture.base_commit.clone(),
            approved_task_ids: vec!["task-1".to_string()],
            approved_write_paths: vec!["src/".to_string()],
            max_parallel: 2,
            budgets: ExecutionBudgets::default(),
        };

        for bad_commit in [
            "HEAD",
            "main",
            &fixture.base_commit[..12],
            "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef",
        ] {
            let mut req = base_req.clone();
            req.base_commit = bad_commit.to_string();
            let err = fixture
                .authority
                .approve(req)
                .expect_err("non-40-hex or unknown commit must fail");
            assert_eq!(err.code(), "invalid_base_commit");
        }

        let baseline = fixture
            .authority
            .approve(base_req.clone())
            .expect("baseline approval");

        // Changing base_commit changes subject_digest and approval_id.
        let mut commit2_req = base_req.clone();
        commit2_req.base_commit = fixture.second_commit.clone();
        let commit2_summary = fixture
            .authority
            .approve(commit2_req)
            .expect("commit2 approval");
        assert_ne!(commit2_summary.subject_digest, baseline.subject_digest);
        assert_ne!(commit2_summary.approval_id, baseline.approval_id);

        // Changing thread_uuid changes subject_digest.
        let mut thread2_req = base_req.clone();
        thread2_req.thread_uuid = THREAD_UUID_B.to_string();
        let thread2_summary = fixture
            .authority
            .approve(thread2_req)
            .expect("thread2 approval");
        assert_ne!(thread2_summary.subject_digest, baseline.subject_digest);

        // Changing operator approved_write_paths changes subject_digest.
        let mut scope2_req = base_req.clone();
        scope2_req.approved_write_paths = vec!["src/lib.rs".to_string()];
        let scope2_summary = fixture
            .authority
            .approve(scope2_req)
            .expect("scope2 approval");
        assert_ne!(scope2_summary.subject_digest, baseline.subject_digest);

        // Changing max_parallel changes subject_digest.
        let mut par_req = base_req.clone();
        par_req.max_parallel = 4;
        let par_summary = fixture.authority.approve(par_req).expect("par approval");
        assert_ne!(par_summary.subject_digest, baseline.subject_digest);

        // Changing budgets changes subject_digest.
        let mut budget_req = base_req.clone();
        budget_req.budgets.timeout_ms = 120_000;
        let budget_summary = fixture
            .authority
            .approve(budget_req)
            .expect("budget approval");
        assert_ne!(budget_summary.subject_digest, baseline.subject_digest);

        // Changing approved_task_ids changes subject_digest.
        let mut tasks2_req = base_req;
        tasks2_req.approved_task_ids = vec!["task-1".to_string(), "task-2".to_string()];
        let tasks2_summary = fixture
            .authority
            .approve(tasks2_req)
            .expect("tasks2 approval");
        assert_ne!(tasks2_summary.subject_digest, baseline.subject_digest);
    }

    #[test]
    fn cli_executable_resolution_nix_symlink_canonicalization_and_unsupported_adapters() {
        let fixture = setup_fixture();

        // Create a symlink to fake_agy_exe and verify symlink input is canonicalized once at approval.
        let symlink_exe = fixture.root.join("bin/agy-symlink");
        symlink(&fixture.fake_agy_exe, &symlink_exe).expect("create symlink exe");

        // Also create a non-executable regular file to verify rejection.
        let non_exec_file = fixture.root.join("bin/not-executable");
        fs::write(&non_exec_file, b"#!/bin/sh\n").expect("write non-executable");
        fs::set_permissions(&non_exec_file, fs::Permissions::from_mode(0o600)).expect("chmod 0600");

        let s1 = fixture
            .registry
            .apply(
                5,
                RegistryChange::UpsertTool {
                    tool: ToolDefinition {
                        id: "tool-agy".to_string(),
                        label: "AGY via symlink".to_string(),
                        adapter: ToolAdapter::Antigravity,
                        executable_path: Some(symlink_exe),
                        enabled: true,
                    },
                },
            )
            .expect("upsert symlink tool");

        let (rev_ok, digest_ok) = save_and_publish_plan(
            &fixture,
            "plan-symlink-exe",
            "binding-1",
            s1.revision,
            vec![PlanTask {
                id: "task-1".to_string(),
                title: "Symlink exe task".to_string(),
                instruction: "Run with canonicalized exe".to_string(),
                role: WorkflowRole::Coder,
                difficulty: Difficulty::Hard,
                depends_on: vec![],
                allowed_write_paths: vec!["src/lib.rs".to_string()],
            }],
        );

        let summary = fixture
            .authority
            .approve(OperatorApprovalRequest {
                binding_id: "binding-1".to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                plan_id: "plan-symlink-exe".to_string(),
                plan_revision: rev_ok,
                expected_registry_revision: s1.revision,
                expected_plan_digest: digest_ok,
                repository_path: fixture.repo_dir.clone(),
                base_commit: fixture.base_commit.clone(),
                approved_task_ids: vec!["task-1".to_string()],
                approved_write_paths: vec!["src/lib.rs".to_string()],
                max_parallel: 1,
                budgets: ExecutionBudgets::default(),
            })
            .expect("symlink CLI executable must canonicalize to real executable");

        let rec = fixture
            .authority
            .get_for_operator(&summary.approval_id)
            .expect("record");
        assert_eq!(
            rec.tasks[0].canonical_executable.as_deref(),
            Some(fixture.fake_agy_exe.as_path())
        );

        // Non-executable target must fail at approval time.
        let s2 = fixture
            .registry
            .apply(
                s1.revision,
                RegistryChange::UpsertTool {
                    tool: ToolDefinition {
                        id: "tool-agy".to_string(),
                        label: "AGY non-exec".to_string(),
                        adapter: ToolAdapter::Antigravity,
                        executable_path: Some(non_exec_file),
                        enabled: true,
                    },
                },
            )
            .expect("upsert non-exec tool");

        let (rev_bad_exe, digest_bad_exe) = save_and_publish_plan(
            &fixture,
            "plan-bad-exe",
            "binding-1",
            s2.revision,
            vec![PlanTask {
                id: "task-1".to_string(),
                title: "Bad exe task".to_string(),
                instruction: "Should fail approval".to_string(),
                role: WorkflowRole::Coder,
                difficulty: Difficulty::Hard,
                depends_on: vec![],
                allowed_write_paths: vec!["src/lib.rs".to_string()],
            }],
        );

        let exe_err = fixture
            .authority
            .approve(OperatorApprovalRequest {
                binding_id: "binding-1".to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                plan_id: "plan-bad-exe".to_string(),
                plan_revision: rev_bad_exe,
                expected_registry_revision: s2.revision,
                expected_plan_digest: digest_bad_exe,
                repository_path: fixture.repo_dir.clone(),
                base_commit: fixture.base_commit.clone(),
                approved_task_ids: vec!["task-1".to_string()],
                approved_write_paths: vec!["src/lib.rs".to_string()],
                max_parallel: 1,
                budgets: ExecutionBudgets::default(),
            })
            .expect_err("non-executable binary must fail approval");
        assert_eq!(exe_err.code(), "invalid_executable");

        // Unsupported adapters (API/git/tmux/codegraph) must be rejected for headless execution.
        let s3 = fixture
            .registry
            .apply(
                s2.revision,
                RegistryChange::UpsertTool {
                    tool: ToolDefinition {
                        id: "tool-jev".to_string(),
                        label: "Jev API".to_string(),
                        adapter: ToolAdapter::JevApi,
                        executable_path: None,
                        enabled: true,
                    },
                },
            )
            .expect("upsert jev tool");
        let s4 = fixture
            .registry
            .apply(
                s3.revision,
                RegistryChange::UpsertModel {
                    model: ModelDefinition {
                        id: "model-jev".to_string(),
                        tool_id: "tool-jev".to_string(),
                        label: "Jev Model".to_string(),
                        model_arg: "jev-1".to_string(),
                        efforts: vec!["max".to_string()],
                        allow_no_effort: true,
                    },
                },
            )
            .expect("upsert jev model");
        let s5 = fixture
            .registry
            .apply(
                s4.revision,
                RegistryChange::UpsertProfile {
                    profile: WorkflowProfile {
                        decision_advisor: None,
                        id: "profile-main".to_string(),
                        name: "Profile with Jev".to_string(),
                        assignments: vec![RoleAssignment {
                            role: WorkflowRole::Architect,
                            difficulty: None,
                            choice: ModelChoice {
                                model_id: "model-jev".to_string(),
                                effort: Some("max".to_string()),
                            },
                        }],
                    },
                },
            )
            .expect("upsert profile with jev");

        let (rev_api, digest_api) = save_and_publish_plan(
            &fixture,
            "plan-api-unsupported",
            "binding-1",
            s5.revision,
            vec![PlanTask {
                id: "task-api".to_string(),
                title: "API task".to_string(),
                instruction: "API tasks cannot run as headless CLI workers".to_string(),
                role: WorkflowRole::Architect,
                difficulty: Difficulty::Hard,
                depends_on: vec![],
                allowed_write_paths: vec!["src/lib.rs".to_string()],
            }],
        );

        let api_err = fixture
            .authority
            .approve(OperatorApprovalRequest {
                binding_id: "binding-1".to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                plan_id: "plan-api-unsupported".to_string(),
                plan_revision: rev_api,
                expected_registry_revision: s5.revision,
                expected_plan_digest: digest_api,
                repository_path: fixture.repo_dir.clone(),
                base_commit: fixture.base_commit.clone(),
                approved_task_ids: vec!["task-api".to_string()],
                approved_write_paths: vec!["src/lib.rs".to_string()],
                max_parallel: 1,
                budgets: ExecutionBudgets::default(),
            })
            .expect_err("API adapter task must be rejected for headless execution");
        assert_eq!(api_err.code(), "unsupported_task_adapter");
    }

    #[test]
    fn native_only_and_mixed_materialization_workers_config_validation_and_side_effect_freedom() {
        let fixture = setup_fixture();
        let reg_rev = fixture.registry.snapshot().expect("snap").revision;

        // 1. Native-only plan (`CodexCli` task only) -> `workers` is `None`.
        let (native_rev, native_digest) = save_and_publish_plan(
            &fixture,
            "plan-native-only",
            "binding-1",
            reg_rev,
            vec![PlanTask {
                id: "task-native".to_string(),
                title: "Native Follower Step".to_string(),
                instruction: "Summarize progress in native Codex chat".to_string(),
                role: WorkflowRole::Follower,
                difficulty: Difficulty::Easy,
                depends_on: vec![],
                allowed_write_paths: vec!["src/lib.rs".to_string()],
            }],
        );

        let native_summary = fixture
            .authority
            .approve(OperatorApprovalRequest {
                binding_id: "binding-1".to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                plan_id: "plan-native-only".to_string(),
                plan_revision: native_rev,
                expected_registry_revision: reg_rev,
                expected_plan_digest: native_digest,
                repository_path: fixture.repo_dir.clone(),
                base_commit: fixture.base_commit.clone(),
                approved_task_ids: vec!["task-native".to_string()],
                approved_write_paths: vec!["src/lib.rs".to_string()],
                max_parallel: 1,
                budgets: ExecutionBudgets::default(),
            })
            .expect("approve native-only plan");
        assert_eq!(native_summary.worker_profile_count, 0);
        assert_eq!(native_summary.native_route_count, 1);

        let session_binding = sample_binding("bnd_session_native", THREAD_UUID_A);
        let non_existent_work_area = fixture.root.join("preview-work-area-not-created");
        let native_mat = fixture
            .authority
            .materialize(
                &native_summary.approval_id,
                &session_binding,
                &non_existent_work_area,
            )
            .expect("materialize native-only approval");
        assert!(
            native_mat.workers.is_none(),
            "native-only approval must have workers: None, not an empty WorkersConfig"
        );
        assert!(native_mat.task_dispatch.is_empty());
        assert_eq!(native_mat.native_routes.len(), 1);
        assert_eq!(native_mat.native_routes[0].task_id, "task-native");
        assert_eq!(native_mat.native_routes[0].adapter, ToolAdapter::CodexCli);
        assert_eq!(
            native_mat.native_routes[0].session_binding_id,
            "bnd_session_native"
        );
        assert!(
            !non_existent_work_area.exists(),
            "materialize must not create work_area_root on disk"
        );

        // 2. AGY worker approval -> `WorkersConfig::validate` succeeds directly and has zero side effects.
        let (agy_rev, agy_digest) = save_and_publish_plan(
            &fixture,
            "plan-agy-worker",
            "binding-1",
            reg_rev,
            vec![PlanTask {
                id: "task-agy".to_string(),
                title: "AGY Hard Implementation".to_string(),
                instruction: "Implement core slice in src/lib.rs".to_string(),
                role: WorkflowRole::Coder,
                difficulty: Difficulty::Hard,
                depends_on: vec![],
                allowed_write_paths: vec!["src/lib.rs".to_string()],
            }],
        );

        let agy_summary = fixture
            .authority
            .approve(OperatorApprovalRequest {
                binding_id: "binding-1".to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                plan_id: "plan-agy-worker".to_string(),
                plan_revision: agy_rev,
                expected_registry_revision: reg_rev,
                expected_plan_digest: agy_digest,
                repository_path: fixture.repo_dir.clone(),
                base_commit: fixture.base_commit.clone(),
                approved_task_ids: vec!["task-agy".to_string()],
                approved_write_paths: vec!["src/".to_string()],
                max_parallel: 3,
                budgets: ExecutionBudgets::default(),
            })
            .expect("approve AGY plan");

        let agy_mat = fixture
            .authority
            .materialize(
                &agy_summary.approval_id,
                &session_binding,
                &non_existent_work_area,
            )
            .expect("materialize AGY approval");
        let agy_workers = agy_mat.workers.expect("workers config present");
        let validated_agy_workers = agy_workers
            .validate()
            .expect("materialized WorkersConfig must pass actual WorkersConfig::validate");
        assert_eq!(validated_agy_workers.max_parallel, 3);
        assert_eq!(validated_agy_workers.profiles.len(), 1);
        assert!(
            !non_existent_work_area.exists(),
            "materialize and WorkersConfig::validate must be completely side-effect-free"
        );

        // 3. Mixed AGY + OpenCodeV2 + Native Codex plan with dependencies.
        let (mixed_rev, mixed_digest) = save_and_publish_plan(
            &fixture,
            "plan-mixed",
            "binding-1",
            reg_rev,
            vec![
                PlanTask {
                    id: "task-agy".to_string(),
                    title: "AGY Task".to_string(),
                    instruction: "Implement hard logic".to_string(),
                    role: WorkflowRole::Coder,
                    difficulty: Difficulty::Hard,
                    depends_on: vec![],
                    allowed_write_paths: vec!["src/lib.rs".to_string()],
                },
                PlanTask {
                    id: "task-oc".to_string(),
                    title: "OpenCode Task".to_string(),
                    instruction: "Implement easy helper".to_string(),
                    role: WorkflowRole::Coder,
                    difficulty: Difficulty::Easy,
                    depends_on: vec!["task-agy".to_string()],
                    allowed_write_paths: vec!["src/extra.rs".to_string()],
                },
                PlanTask {
                    id: "task-codex".to_string(),
                    title: "Codex Follower".to_string(),
                    instruction: "Report status in native chat".to_string(),
                    role: WorkflowRole::Follower,
                    difficulty: Difficulty::Easy,
                    depends_on: vec!["task-oc".to_string()],
                    allowed_write_paths: vec!["src/sub/mod.rs".to_string()],
                },
            ],
        );

        let mixed_summary = fixture
            .authority
            .approve(OperatorApprovalRequest {
                binding_id: "binding-1".to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                plan_id: "plan-mixed".to_string(),
                plan_revision: mixed_rev,
                expected_registry_revision: reg_rev,
                expected_plan_digest: mixed_digest,
                repository_path: fixture.repo_dir.clone(),
                base_commit: fixture.base_commit.clone(),
                approved_task_ids: vec![
                    "task-agy".to_string(),
                    "task-oc".to_string(),
                    "task-codex".to_string(),
                ],
                approved_write_paths: vec!["src/".to_string()],
                max_parallel: 2,
                budgets: ExecutionBudgets::default(),
            })
            .expect("approve mixed plan");
        assert_eq!(mixed_summary.worker_profile_count, 2);
        assert_eq!(mixed_summary.native_route_count, 1);

        let mixed_mat = fixture
            .authority
            .materialize(
                &mixed_summary.approval_id,
                &session_binding,
                &non_existent_work_area,
            )
            .expect("materialize mixed plan");
        let mixed_workers = mixed_mat.workers.expect("mixed workers");
        assert_eq!(mixed_workers.profiles.len(), 2);
        assert_eq!(mixed_mat.task_dispatch.len(), 2);
        assert_eq!(mixed_mat.native_routes.len(), 1);
        assert_eq!(
            mixed_mat.native_routes[0].depends_on,
            vec!["task-oc".to_string()]
        );

        let agy_prof = &mixed_workers.profiles[0];
        assert!(agy_prof.profile_id.len() <= 64);
        assert_eq!(agy_prof.spec.provider, WorkerProvider::Antigravity);
        assert_eq!(agy_prof.spec.executable, fixture.fake_agy_exe);
        assert!(!agy_prof.spec.standalone);
        assert_eq!(agy_prof.spec.session_id, None);
        assert_eq!(
            agy_prof.spec.file_permissions,
            WorkerFilePermissions::TaskFiles
        );
        assert_eq!(
            agy_prof.spec.allowed_models,
            BTreeSet::from(["gemini-4-argon-high".to_string()])
        );
        assert_eq!(
            agy_prof.spec.allowed_write_paths,
            BTreeSet::from(["src/lib.rs".to_string()])
        );
        assert!(
            agy_prof.dependency_input.is_none(),
            "root worker profile must not carry dependency_input policy"
        );

        let oc_prof = &mixed_workers.profiles[1];
        assert!(oc_prof.profile_id.len() <= 64);
        assert_eq!(oc_prof.spec.provider, WorkerProvider::OpenCodeV2);
        assert_eq!(oc_prof.spec.executable, fixture.fake_oc_exe);
        assert!(oc_prof.spec.standalone);
        assert_eq!(oc_prof.spec.session_id, None);
        assert_eq!(
            oc_prof.spec.file_permissions,
            WorkerFilePermissions::TaskFiles
        );
        assert_eq!(
            oc_prof.spec.allowed_models,
            BTreeSet::from(["opencode-go/deepseek-v4.1-flash".to_string()])
        );
        let oc_dep_policy = oc_prof
            .dependency_input
            .as_ref()
            .expect("dependent worker profile must carry dependency_input policy");
        assert_eq!(
            oc_dep_policy.seal_path,
            non_existent_work_area
                .join("manifests")
                .join("dependencies")
                .join("task-oc.json")
        );
        assert_eq!(oc_dep_policy.approval_id, mixed_summary.approval_id);
        assert_eq!(
            oc_dep_policy.approval_subject_digest,
            mixed_mat.subject_digest
        );
        assert_eq!(oc_dep_policy.task_id, "task-oc");
        assert_eq!(
            oc_dep_policy.command_key,
            mixed_mat.task_dispatch[1].command_key
        );
        assert_eq!(
            oc_dep_policy.task_digest,
            mixed_mat.task_dispatch[1].task_digest
        );
        assert_eq!(
            oc_prof.spec.allowed_write_paths,
            BTreeSet::from(["src/extra.rs".to_string()])
        );

        // Work area overlapping repository or .git must be rejected.
        for bad_work_area in [
            fixture.repo_dir.clone(),
            fixture.repo_dir.join(".git"),
            fixture.repo_dir.join("nested-work-area"),
        ] {
            let err = fixture
                .authority
                .materialize(&mixed_summary.approval_id, &session_binding, &bad_work_area)
                .expect_err("work_area_root overlapping repo must fail");
            assert_eq!(err.code(), "invalid_work_area");
        }
    }

    #[test]
    fn frozen_approval_is_immune_to_subsequent_registry_edits() {
        let fixture = setup_fixture();
        let reg_rev = fixture.registry.snapshot().expect("snap").revision;

        let (plan_rev, plan_digest) = save_and_publish_plan(
            &fixture,
            "plan-frozen",
            "binding-1",
            reg_rev,
            vec![PlanTask {
                id: "task-1".to_string(),
                title: "Frozen Task".to_string(),
                instruction: "Original frozen instruction".to_string(),
                role: WorkflowRole::Coder,
                difficulty: Difficulty::Hard,
                depends_on: vec![],
                allowed_write_paths: vec!["src/lib.rs".to_string()],
            }],
        );

        let summary = fixture
            .authority
            .approve(OperatorApprovalRequest {
                binding_id: "binding-1".to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                plan_id: "plan-frozen".to_string(),
                plan_revision: plan_rev,
                expected_registry_revision: reg_rev,
                expected_plan_digest: plan_digest,
                repository_path: fixture.repo_dir.clone(),
                base_commit: fixture.base_commit.clone(),
                approved_task_ids: vec!["task-1".to_string()],
                approved_write_paths: vec!["src/lib.rs".to_string()],
                max_parallel: 2,
                budgets: ExecutionBudgets::default(),
            })
            .expect("approve plan before registry edits");

        let session_binding = sample_binding("bnd_frozen", THREAD_UUID_A);
        let work_area = fixture.root.join("work-area-frozen");
        let before_mat = fixture
            .authority
            .materialize(&summary.approval_id, &session_binding, &work_area)
            .expect("materialize before registry edits");

        // Now mutate the global registry: change tool-agy's executable path and model-argon's model_arg/efforts.
        let other_exe = fixture.root.join("bin/other-agy");
        create_executable_script(&other_exe);

        let s1 = fixture
            .registry
            .apply(
                reg_rev,
                RegistryChange::UpsertTool {
                    tool: ToolDefinition {
                        id: "tool-agy".to_string(),
                        label: "Modified AGY".to_string(),
                        adapter: ToolAdapter::Antigravity,
                        executable_path: Some(other_exe),
                        enabled: true,
                    },
                },
            )
            .expect("mutate tool-agy in registry");

        let _s2 = fixture
            .registry
            .apply(
                s1.revision,
                RegistryChange::UpsertModel {
                    model: ModelDefinition {
                        id: "model-argon".to_string(),
                        tool_id: "tool-agy".to_string(),
                        label: "Modified Model".to_string(),
                        model_arg: "different-model-arg".to_string(),
                        efforts: vec!["low".to_string(), "high".to_string()],
                        allow_no_effort: false,
                    },
                },
            )
            .expect("mutate model-argon in registry");

        // Existing approval record and materialization must remain 100% unchanged!
        let after_record = fixture
            .authority
            .get_for_operator(&summary.approval_id)
            .expect("get_for_operator after registry edits");
        assert_eq!(
            after_record.tasks[0].canonical_executable.as_deref(),
            Some(fixture.fake_agy_exe.as_path())
        );
        assert_eq!(after_record.tasks[0].base_model, "gemini-4-argon-high");
        assert_eq!(after_record.tasks[0].effort.as_deref(), Some("high"));
        assert_eq!(
            after_record.tasks[0].instruction,
            "Original frozen instruction"
        );

        let after_mat = fixture
            .authority
            .materialize(&summary.approval_id, &session_binding, &work_area)
            .expect("materialize after registry edits");
        assert_eq!(after_mat, before_mat);
    }

    #[test]
    fn base_commit_directory_scope_validation_prevents_bare_directory_expansion_when_worktree_differs(
    ) {
        let fixture = setup_fixture();
        let reg_rev = fixture.registry.snapshot().expect("snap").revision;

        // Create a plan with a task targeting bare "src/sub" (which is a directory at base_commit)
        // and another plan targeting "src/sub/mod.rs" (an exact file inside "src/sub/").
        let (bare_task_rev, bare_task_digest) = save_and_publish_plan(
            &fixture,
            "plan-bare-dir-task",
            "binding-1",
            reg_rev,
            vec![PlanTask {
                id: "task-bare-dir".to_string(),
                title: "Bare Dir Task".to_string(),
                instruction: "Attempt bare directory scope".to_string(),
                role: WorkflowRole::Coder,
                difficulty: Difficulty::Hard,
                depends_on: vec![],
                allowed_write_paths: vec!["src/sub".to_string()],
            }],
        );

        let (file_task_rev, file_task_digest) = save_and_publish_plan(
            &fixture,
            "plan-sub-file-task",
            "binding-1",
            reg_rev,
            vec![PlanTask {
                id: "task-sub-file".to_string(),
                title: "Sub File Task".to_string(),
                instruction: "Modify src/sub/mod.rs".to_string(),
                role: WorkflowRole::Coder,
                difficulty: Difficulty::Hard,
                depends_on: vec![],
                allowed_write_paths: vec!["src/sub/mod.rs".to_string()],
            }],
        );

        // Mutate the mutable working tree of `repo_dir`:
        // 1) Delete directory `src/sub` so it is absent in the mutable working tree.
        fs::remove_dir_all(fixture.repo_dir.join("src/sub"))
            .expect("remove src/sub from mutable working tree");

        // Even though `src/sub` is now absent in the mutable working tree, at `fixture.base_commit`
        // `src/sub` is a directory (`040000 tree`). Approving bare `"src/sub"` must be rejected!
        let absent_bare_op_err = fixture
            .authority
            .approve(OperatorApprovalRequest {
                binding_id: "binding-1".to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                plan_id: "plan-bare-dir-task".to_string(),
                plan_revision: bare_task_rev,
                expected_registry_revision: reg_rev,
                expected_plan_digest: bare_task_digest.clone(),
                repository_path: fixture.repo_dir.clone(),
                base_commit: fixture.base_commit.clone(),
                approved_task_ids: vec!["task-bare-dir".to_string()],
                approved_write_paths: vec!["src/sub".to_string()],
                max_parallel: 1,
                budgets: ExecutionBudgets::default(),
            })
            .expect_err("bare scope that is a directory at base_commit must fail even when absent in working tree");
        assert_eq!(absent_bare_op_err.code(), "invalid_write_scope");

        // Even if operator passes explicit `"src/"`, a task with bare `"src/sub"` (directory at base_commit)
        // must also be rejected!
        let absent_bare_task_err = fixture
            .authority
            .approve(OperatorApprovalRequest {
                binding_id: "binding-1".to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                plan_id: "plan-bare-dir-task".to_string(),
                plan_revision: bare_task_rev,
                expected_registry_revision: reg_rev,
                expected_plan_digest: bare_task_digest.clone(),
                repository_path: fixture.repo_dir.clone(),
                base_commit: fixture.base_commit.clone(),
                approved_task_ids: vec!["task-bare-dir".to_string()],
                approved_write_paths: vec!["src/".to_string()],
                max_parallel: 1,
                budgets: ExecutionBudgets::default(),
            })
            .expect_err("task bare scope that is a directory at base_commit must fail");
        assert_eq!(absent_bare_task_err.code(), "invalid_write_scope");

        // 2) Replace `src/sub` in the mutable working tree with a regular file `src/sub`.
        fs::write(
            fixture.repo_dir.join("src/sub"),
            "replaced by regular file in dirty checkout\n",
        )
        .expect("write regular file at src/sub in working tree");

        // Approving bare `"src/sub"` against `fixture.base_commit` must STILL be rejected because
        // `src/sub` is a directory at `fixture.base_commit`!
        let file_in_wt_bare_err = fixture
            .authority
            .approve(OperatorApprovalRequest {
                binding_id: "binding-1".to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                plan_id: "plan-bare-dir-task".to_string(),
                plan_revision: bare_task_rev,
                expected_registry_revision: reg_rev,
                expected_plan_digest: bare_task_digest,
                repository_path: fixture.repo_dir.clone(),
                base_commit: fixture.base_commit.clone(),
                approved_task_ids: vec!["task-bare-dir".to_string()],
                approved_write_paths: vec!["src/sub".to_string()],
                max_parallel: 1,
                budgets: ExecutionBudgets::default(),
            })
            .expect_err("bare scope that is a directory at base_commit must fail even when regular file in working tree");
        assert_eq!(file_in_wt_bare_err.code(), "invalid_write_scope");

        // Meanwhile, explicit `"src/sub/"` (with trailing slash) retains directory intent at `base_commit`
        // and covers exact-file task `"src/sub/mod.rs"` when `src/sub` is absent in the working tree.
        fs::remove_file(fixture.repo_dir.join("src/sub")).expect("remove regular file src/sub");
        let explicit_dir_summary = fixture
            .authority
            .approve(OperatorApprovalRequest {
                binding_id: "binding-1".to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                plan_id: "plan-sub-file-task".to_string(),
                plan_revision: file_task_rev,
                expected_registry_revision: reg_rev,
                expected_plan_digest: file_task_digest,
                repository_path: fixture.repo_dir.clone(),
                base_commit: fixture.base_commit.clone(),
                approved_task_ids: vec!["task-sub-file".to_string()],
                approved_write_paths: vec!["src/sub/".to_string()],
                max_parallel: 1,
                budgets: ExecutionBudgets::default(),
            })
            .expect("explicit trailing-slash directory scope at base_commit must succeed");
        assert_eq!(
            explicit_dir_summary.approved_write_paths,
            vec!["src/sub/".to_string()]
        );

        // Static stored-record integrity rejects uncontained task scopes or protected control paths
        // without running Git subprocess probes on metadata reads.
        let mut uncontained_record = fixture
            .authority
            .get_for_operator(&explicit_dir_summary.approval_id)
            .expect("get valid record");
        uncontained_record.approved_write_paths = vec!["src/lib.rs".to_string()];
        let core_digest = compute_authority_core_digest(&uncontained_record).expect("core digest");
        uncontained_record.tasks[0].profile_id = Some(derive_profile_id(
            &core_digest,
            &uncontained_record.tasks[0].task_id,
            &uncontained_record.tasks[0].task_digest,
        ));
        uncontained_record.tasks[0].command_key = Some(derive_command_key(
            &core_digest,
            &uncontained_record.tasks[0].task_id,
            &uncontained_record.tasks[0].task_digest,
        ));
        let subj = compute_record_subject_digest(&uncontained_record).expect("subject digest");
        uncontained_record.approval_id = derive_approval_id(&subj);
        uncontained_record.subject_digest = subj;

        let integrity_err = verify_approval_record_integrity(
            &uncontained_record,
            fixture.authority.approvals_path(),
        )
        .expect_err(
            "stored record with uncontained task write path must fail static integrity check",
        );
        assert_eq!(integrity_err.code(), "corrupt_store");
    }

    #[test]
    fn metadata_reads_remain_available_when_one_historical_repo_git_dir_is_moved_while_materialize_fails_closed(
    ) {
        let fixture = setup_fixture();
        let reg_rev = fixture.registry.snapshot().expect("snap").revision;

        // Create a second independent repository for binding-2.
        let repo_dir_b = fixture.root.join("repo-b");
        let (base_commit_b, _) = init_disposable_git_repo(&repo_dir_b);

        let (rev_a, digest_a) = save_and_publish_plan(
            &fixture,
            "plan-history-a",
            "binding-1",
            reg_rev,
            vec![PlanTask {
                id: "task-a".to_string(),
                title: "Task A".to_string(),
                instruction: "Modify src/lib.rs in repo A".to_string(),
                role: WorkflowRole::Coder,
                difficulty: Difficulty::Hard,
                depends_on: vec![],
                allowed_write_paths: vec!["src/lib.rs".to_string()],
            }],
        );

        let (rev_b, digest_b) = save_and_publish_plan(
            &fixture,
            "plan-history-b",
            "binding-2",
            reg_rev,
            vec![PlanTask {
                id: "task-b".to_string(),
                title: "Task B".to_string(),
                instruction: "Modify src/lib.rs in repo B".to_string(),
                role: WorkflowRole::Coder,
                difficulty: Difficulty::Hard,
                depends_on: vec![],
                allowed_write_paths: vec!["src/lib.rs".to_string()],
            }],
        );

        let summary_a = fixture
            .authority
            .approve(OperatorApprovalRequest {
                binding_id: "binding-1".to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                plan_id: "plan-history-a".to_string(),
                plan_revision: rev_a,
                expected_registry_revision: reg_rev,
                expected_plan_digest: digest_a,
                repository_path: fixture.repo_dir.clone(),
                base_commit: fixture.base_commit.clone(),
                approved_task_ids: vec!["task-a".to_string()],
                approved_write_paths: vec!["src/lib.rs".to_string()],
                max_parallel: 1,
                budgets: ExecutionBudgets::default(),
            })
            .expect("approve plan A");

        let summary_b = fixture
            .authority
            .approve(OperatorApprovalRequest {
                binding_id: "binding-2".to_string(),
                thread_uuid: THREAD_UUID_B.to_string(),
                plan_id: "plan-history-b".to_string(),
                plan_revision: rev_b,
                expected_registry_revision: reg_rev,
                expected_plan_digest: digest_b,
                repository_path: repo_dir_b.clone(),
                base_commit: base_commit_b,
                approved_task_ids: vec!["task-b".to_string()],
                approved_write_paths: vec!["src/lib.rs".to_string()],
                max_parallel: 1,
                budgets: ExecutionBudgets::default(),
            })
            .expect("approve plan B");

        let ledger_bytes_before =
            fs::read(fixture.authority.approvals_path()).expect("read ledger bytes before");

        // Hide repo_dir_b's `.git` directory under a different name while `repo_dir_b` still exists.
        fs::rename(repo_dir_b.join(".git"), repo_dir_b.join(".git.hidden"))
            .expect("hide repo B .git directory");

        // Metadata reads (`get_for_operator` and `list_for_binding`) must remain available for
        // BOTH approvals and bindings without classifying the store as `CorruptStore`.
        let record_a = fixture
            .authority
            .get_for_operator(&summary_a.approval_id)
            .expect("get_for_operator for unaffected approval A must succeed");
        assert_eq!(record_a.approval_id, summary_a.approval_id);

        let record_b = fixture
            .authority
            .get_for_operator(&summary_b.approval_id)
            .expect("get_for_operator for historical approval B must still return cached metadata");
        assert_eq!(record_b.approval_id, summary_b.approval_id);

        let list_a = fixture
            .authority
            .list_for_binding("binding-1")
            .expect("list_for_binding for binding-1 must succeed");
        assert_eq!(list_a, vec![summary_a.clone()]);

        let list_b = fixture
            .authority
            .list_for_binding("binding-2")
            .expect("list_for_binding for binding-2 must succeed");
        assert_eq!(list_b, vec![summary_b.clone()]);

        // Materializing the affected approval B must fail closed without creating worktrees or
        // mutating the ledger, while materializing unaffected approval A succeeds.
        let work_area_b = fixture.root.join("work-area-b-not-created");
        let mat_b_err = fixture
            .authority
            .materialize(
                &summary_b.approval_id,
                &sample_binding("bnd_session_b", THREAD_UUID_B),
                &work_area_b,
            )
            .expect_err("materialize on approval B with missing .git must fail closed");
        assert_eq!(mat_b_err.code(), "invalid_repository");
        assert!(
            !work_area_b.exists(),
            "failed materialization must not create work_area_root"
        );

        let work_area_a = fixture.root.join("work-area-a-not-created");
        let mat_a = fixture
            .authority
            .materialize(
                &summary_a.approval_id,
                &sample_binding("bnd_session_a", THREAD_UUID_A),
                &work_area_a,
            )
            .expect("materialize on unaffected approval A must succeed");
        assert_eq!(mat_a.approval_id, summary_a.approval_id);
        assert!(!work_area_a.exists());

        let ledger_bytes_after =
            fs::read(fixture.authority.approvals_path()).expect("read ledger bytes after");
        assert_eq!(
            ledger_bytes_after, ledger_bytes_before,
            "ledger bytes on disk must remain unchanged"
        );
    }

    #[test]
    fn approve_rejects_missing_direct_or_transitive_predecessors_without_expanding_selection() {
        let fixture = setup_fixture();
        let reg_rev = fixture.registry.snapshot().expect("reg snap").revision;

        let (chain_rev, chain_digest) = save_and_publish_plan(
            &fixture,
            "plan-transitive-selection",
            "binding-1",
            reg_rev,
            vec![
                PlanTask {
                    id: "task-a".to_string(),
                    title: "Root A".to_string(),
                    instruction: "Implement root A".to_string(),
                    role: WorkflowRole::Coder,
                    difficulty: Difficulty::Hard,
                    depends_on: Vec::new(),
                    allowed_write_paths: vec!["src/lib.rs".to_string()],
                },
                PlanTask {
                    id: "task-b".to_string(),
                    title: "Middle B".to_string(),
                    instruction: "Implement middle B".to_string(),
                    role: WorkflowRole::Coder,
                    difficulty: Difficulty::Easy,
                    depends_on: vec!["task-a".to_string()],
                    allowed_write_paths: vec!["src/extra.rs".to_string()],
                },
                PlanTask {
                    id: "task-c".to_string(),
                    title: "Leaf C".to_string(),
                    instruction: "Implement leaf C".to_string(),
                    role: WorkflowRole::Coder,
                    difficulty: Difficulty::Hard,
                    depends_on: vec!["task-b".to_string()],
                    allowed_write_paths: vec!["src/sub/mod.rs".to_string()],
                },
            ],
        );

        let approve_selection = |ids: Vec<&str>| {
            fixture.authority.approve(OperatorApprovalRequest {
                binding_id: "binding-1".to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                plan_id: "plan-transitive-selection".to_string(),
                plan_revision: chain_rev,
                expected_registry_revision: reg_rev,
                expected_plan_digest: chain_digest.clone(),
                repository_path: fixture.repo_dir.clone(),
                base_commit: fixture.base_commit.clone(),
                approved_task_ids: ids.into_iter().map(str::to_string).collect(),
                approved_write_paths: vec!["src/".to_string()],
                max_parallel: 2,
                budgets: ExecutionBudgets::default(),
            })
        };

        // 1. Selecting dependent task-b without direct predecessor task-a is rejected.
        let direct_err =
            approve_selection(vec!["task-b"]).expect_err("missing direct predecessor rejected");
        assert_eq!(direct_err.code(), "validation");
        assert!(direct_err.to_string().contains("task-a"));

        // 2. Selecting task-b and task-c without transitive predecessor task-a is rejected.
        let transitive_err = approve_selection(vec!["task-b", "task-c"])
            .expect_err("missing transitive root predecessor rejected");
        assert_eq!(transitive_err.code(), "validation");
        assert!(transitive_err.to_string().contains("task-a"));

        // 3. Selecting task-a and task-c without intermediate predecessor task-b is rejected.
        let middle_err = approve_selection(vec!["task-a", "task-c"])
            .expect_err("missing intermediate predecessor rejected");
        assert_eq!(middle_err.code(), "validation");
        assert!(middle_err.to_string().contains("task-b"));

        // Store remains empty after rejected partial approvals (never silently expanded).
        assert!(fixture
            .authority
            .list_for_binding("binding-1")
            .expect("list approvals")
            .is_empty());

        // 4. Selecting the complete prefix [task-a, task-b] or full chain [task-a, task-b, task-c] succeeds.
        let prefix_summary =
            approve_selection(vec!["task-a", "task-b"]).expect("complete prefix selection valid");
        assert_eq!(
            prefix_summary.approved_task_ids,
            vec!["task-a".to_string(), "task-b".to_string()]
        );
    }

    #[test]
    fn shared_worker_profile_constructor_preserves_nondefault_budgets_effort_and_provider_standalone(
    ) {
        let fixture = setup_fixture();
        let reg_rev = fixture.registry.snapshot().expect("reg snap").revision;

        let (plan_rev, plan_digest) = save_and_publish_plan(
            &fixture,
            "plan-shared-profile-constructor",
            "binding-1",
            reg_rev,
            vec![
                PlanTask {
                    id: "task-agy".to_string(),
                    title: "AGY Hard Root".to_string(),
                    instruction: "Implement AGY root".to_string(),
                    role: WorkflowRole::Coder,
                    difficulty: Difficulty::Hard,
                    depends_on: Vec::new(),
                    allowed_write_paths: vec!["src/lib.rs".to_string()],
                },
                PlanTask {
                    id: "task-go".to_string(),
                    title: "Go Dependent".to_string(),
                    instruction: "Implement Go dependent".to_string(),
                    role: WorkflowRole::Coder,
                    difficulty: Difficulty::Easy,
                    depends_on: vec!["task-agy".to_string()],
                    allowed_write_paths: vec!["src/extra.rs".to_string()],
                },
            ],
        );

        let custom_budgets = ExecutionBudgets {
            progress_watchdog: None,
            timeout_ms: 123_456,
            max_stdout_bytes: 204_800,
            max_stderr_bytes: 102_400,
            max_line_bytes: 32_768,
            max_events: 512,
        };

        let summary = fixture
            .authority
            .approve(OperatorApprovalRequest {
                binding_id: "binding-1".to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                plan_id: "plan-shared-profile-constructor".to_string(),
                plan_revision: plan_rev,
                expected_registry_revision: reg_rev,
                expected_plan_digest: plan_digest,
                repository_path: fixture.repo_dir.clone(),
                base_commit: fixture.base_commit.clone(),
                approved_task_ids: vec!["task-agy".to_string(), "task-go".to_string()],
                approved_write_paths: vec!["src/".to_string()],
                max_parallel: 2,
                budgets: custom_budgets.clone(),
            })
            .expect("approve with custom budgets");

        let record = fixture
            .authority
            .get_for_operator(&summary.approval_id)
            .expect("get_for_operator");
        assert_eq!(record.budgets, custom_budgets);

        let session_binding = sample_binding("bnd_session_shared", THREAD_UUID_A);
        let work_area = fixture.root.join("work-area-shared");
        let materialized = fixture
            .authority
            .materialize(&summary.approval_id, &session_binding, &work_area)
            .expect("materialize");
        let mat_validated = materialized
            .workers
            .as_ref()
            .expect("workers present")
            .validate()
            .expect("validate materialized workers");

        for task in &record.tasks {
            let shared_cfg = build_approved_worker_profile_config(
                &record,
                task,
                &session_binding.binding_id,
                &work_area,
                fixture.authority.approvals_path(),
            )
            .expect("build_approved_worker_profile_config");

            assert_eq!(shared_cfg.spec.budgets, custom_budgets);
            assert_eq!(shared_cfg.spec.model, task.base_model);
            assert_eq!(shared_cfg.spec.effort, task.effort);
            assert_eq!(
                shared_cfg.spec.file_permissions,
                WorkerFilePermissions::TaskFiles
            );

            match task.provider {
                Some(WorkerProvider::Antigravity) => {
                    assert!(
                        !shared_cfg.spec.standalone,
                        "Antigravity profile must have standalone == false"
                    );
                    assert_eq!(shared_cfg.spec.effort.as_deref(), Some("high"));
                    assert!(shared_cfg.dependency_input.is_none());
                }
                Some(WorkerProvider::OpenCodeV2) => {
                    assert!(
                        shared_cfg.spec.standalone,
                        "OpenCode Go profile must have standalone == true"
                    );
                    assert_eq!(shared_cfg.spec.effort.as_deref(), Some("max"));
                    assert!(shared_cfg.dependency_input.is_some());
                }
                Some(WorkerProvider::CodexCli) => {
                    panic!("CodexCli is read-only advisory only, never an approved Coder task provider");
                }
                None => panic!("expected worker provider"),
            }

            let single_validated = threadlane::WorkersConfig {
                max_parallel: 1,
                profiles: vec![shared_cfg.clone()],
            }
            .validate()
            .expect("validate single profile");
            let single_prof = single_validated
                .profiles
                .get(&shared_cfg.profile_id)
                .expect("single validated profile");
            let mat_prof = mat_validated
                .profiles
                .get(&shared_cfg.profile_id)
                .expect("materialized validated profile");
            assert_eq!(
                single_prof.profile_digest, mat_prof.profile_digest,
                "static profile_digest must match AuthorityStore::materialize bit-for-bit"
            );

            let history_digest = compute_recorded_history_worker_profile_digest(
                &record,
                task,
                &session_binding.binding_id,
                &work_area,
                fixture.authority.approvals_path(),
            )
            .expect("compute_recorded_history_worker_profile_digest with executable present");
            assert_eq!(
                history_digest, mat_prof.profile_digest,
                "recorded-history profile digest must match materialized validated profile_digest"
            );
        }

        for task in &record.tasks {
            let exe_path = task
                .canonical_executable
                .as_ref()
                .expect("frozen canonical_executable");
            assert!(
                exe_path.starts_with(&fixture.root),
                "refusing to delete executable outside test fixture root"
            );
            if exe_path.exists() {
                fs::remove_file(exe_path).expect("remove owned fake executable");
            }
        }

        for task in &record.tasks {
            let profile_id = task.profile_id.as_ref().expect("frozen profile_id");
            let mat_prof = mat_validated
                .profiles
                .get(profile_id)
                .expect("materialized validated profile");
            let err = build_approved_worker_profile_config(
                &record,
                task,
                &session_binding.binding_id,
                &work_area,
                fixture.authority.approvals_path(),
            )
            .expect_err("strict builder must fail when frozen executable is removed");
            assert!(matches!(err, AuthorityError::WorkersConfigInvalid { .. }));

            let history_digest_after_gc = compute_recorded_history_worker_profile_digest(
                &record,
                task,
                &session_binding.binding_id,
                &work_area,
                fixture.authority.approvals_path(),
            )
            .expect("history digest must succeed after executable removal");
            assert_eq!(
                history_digest_after_gc, mat_prof.profile_digest,
                "recorded-history profile digest must stay equal after executable removal"
            );
        }
    }

    #[test]
    fn prepare_preview_and_approve_confirmed_enforce_digest_and_no_preview_persistence() {
        let fixture = setup_fixture();
        let reg_rev = fixture.registry.snapshot().expect("snap").revision;

        let (plan_rev, plan_digest) = save_and_publish_plan(
            &fixture,
            "plan-confirm",
            "binding-confirm",
            reg_rev,
            vec![PlanTask {
                id: "task-1".to_string(),
                title: "Confirm Task".to_string(),
                instruction: "Implement confirmed feature".to_string(),
                role: WorkflowRole::Coder,
                difficulty: Difficulty::Hard,
                depends_on: vec![],
                allowed_write_paths: vec!["src/lib.rs".to_string()],
            }],
        );

        let intent = OperatorApprovalIntent {
            binding_id: "binding-confirm".to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            plan_id: "plan-confirm".to_string(),
            plan_revision: plan_rev,
            expected_registry_revision: reg_rev,
            repository_path: fixture.repo_dir.clone(),
            base_commit: fixture.base_commit.clone(),
            approved_task_ids: vec!["task-1".to_string()],
            approved_write_paths: vec!["src/lib.rs".to_string()],
            max_parallel: 2,
            budgets: ExecutionBudgets::default(),
        };

        let preview = fixture
            .authority
            .prepare_for_operator(intent)
            .expect("prepare_for_operator");
        assert_eq!(preview.request.expected_plan_digest, plan_digest);
        assert_eq!(preview.subject_digest.len(), 64);
        assert_eq!(preview.tasks.len(), 1);
        assert_eq!(preview.tasks[0].task_id, "task-1");
        assert_eq!(preview.tasks[0].base_model, "gemini-4-argon-high");
        assert_eq!(preview.tasks[0].effort.as_deref(), Some("high"));
        assert!(
            !fixture.authority.approvals_path().exists(),
            "prepare_for_operator must never write approvals.json"
        );

        let direct_preview = fixture
            .authority
            .preview_for_operator(preview.request.clone())
            .expect("preview_for_operator");
        assert_eq!(direct_preview, preview);
        assert!(
            !fixture.authority.approvals_path().exists(),
            "preview_for_operator must never write approvals.json"
        );

        // Malformed expected_subject_digest must fail with validation on expected_subject_digest.
        let malformed_err = fixture
            .authority
            .approve_confirmed(preview.request.clone(), "not-a-sha256")
            .expect_err("malformed expected_subject_digest must fail");
        assert_eq!(malformed_err.code(), "validation");
        assert!(matches!(
            malformed_err,
            AuthorityError::Validation { ref field, .. } if field == "expected_subject_digest"
        ));
        assert!(!fixture.authority.approvals_path().exists());

        // Mismatched 64-hex expected_subject_digest must fail without writing approvals.json.
        let wrong_digest = "a".repeat(64);
        let mismatch_err = fixture
            .authority
            .approve_confirmed(preview.request.clone(), &wrong_digest)
            .expect_err("mismatched expected_subject_digest must fail");
        assert_eq!(mismatch_err.code(), "validation");
        assert!(matches!(
            mismatch_err,
            AuthorityError::Validation { ref field, .. } if field == "expected_subject_digest"
        ));
        assert!(!fixture.authority.approvals_path().exists());

        // Matching expected_subject_digest persists the record and is idempotent on repeat.
        let summary = fixture
            .authority
            .approve_confirmed(preview.request.clone(), &preview.subject_digest)
            .expect("approve_confirmed succeeds");
        assert_eq!(summary.subject_digest, preview.subject_digest);
        assert!(fixture.authority.approvals_path().exists());

        let repeat = fixture
            .authority
            .approve_confirmed(preview.request.clone(), &preview.subject_digest)
            .expect("approve_confirmed idempotent repeat");
        assert_eq!(repeat, summary);

        // Even when the record already exists in the ledger, a mismatched expected_subject_digest must still fail!
        let post_persist_mismatch = fixture
            .authority
            .approve_confirmed(preview.request, &wrong_digest)
            .expect_err("mismatched digest must fail even after record exists");
        assert!(matches!(
            post_persist_mismatch,
            AuthorityError::Validation { ref field, .. } if field == "expected_subject_digest"
        ));
    }
    #[test]
    fn codex_coder_preview_freezes_complete_pins_and_legacy_native_records_stay_native() {
        let fixture = setup_fixture();
        let snap = fixture.registry.snapshot().unwrap();
        fixture
            .registry
            .apply(
                snap.revision,
                RegistryChange::UpsertTool {
                    tool: ToolDefinition {
                        id: "tool-codex".to_string(),
                        label: "Offline Codex".to_string(),
                        adapter: ToolAdapter::CodexCli,
                        executable_path: Some(fixture.fake_agy_exe.clone()),
                        enabled: true,
                    },
                },
            )
            .unwrap();
        fixture
            .registry
            .apply(
                snap.revision + 1,
                RegistryChange::UpsertModels {
                    models: vec![ModelDefinition {
                        id: "model-luna".to_string(),
                        tool_id: "tool-codex".to_string(),
                        label: "Luna".to_string(),
                        model_arg: "gpt-6-luna".to_string(),
                        efforts: vec!["max".to_string()],
                        allow_no_effort: false,
                    }],
                },
            )
            .unwrap();
        let mut profile = snap.profiles[0].clone();
        for assignment in &mut profile.assignments {
            if assignment.role == WorkflowRole::Coder {
                assignment.choice = ModelChoice {
                    model_id: "model-luna".to_string(),
                    effort: Some("max".to_string()),
                };
            }
        }
        fixture
            .registry
            .apply(snap.revision + 2, RegistryChange::UpsertProfile { profile })
            .unwrap();
        let revision = fixture.registry.snapshot().unwrap().revision;
        let (plan_revision, digest) = save_and_publish_plan(
            &fixture,
            "plan-codex",
            "binding-1",
            revision,
            vec![PlanTask {
                id: "task-codex".to_string(),
                title: "Codex task".to_string(),
                instruction: "Edit the approved file".to_string(),
                role: WorkflowRole::Coder,
                difficulty: Difficulty::Easy,
                depends_on: vec![],
                allowed_write_paths: vec!["src/lib.rs".to_string()],
            }],
        );
        let request = OperatorApprovalRequest {
            binding_id: "binding-1".to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            plan_id: "plan-codex".to_string(),
            plan_revision,
            expected_registry_revision: revision,
            expected_plan_digest: digest,
            repository_path: fixture.repo_dir.clone(),
            base_commit: fixture.base_commit.clone(),
            approved_task_ids: vec!["task-codex".to_string()],
            approved_write_paths: vec!["src/lib.rs".to_string()],
            max_parallel: 1,
            budgets: ExecutionBudgets::default(),
        };
        let preview = fixture
            .authority
            .preview_for_operator(request.clone())
            .unwrap();
        assert!(preview.tasks[0].is_frozen_headless_worker());
        assert_eq!(preview.tasks[0].provider, Some(WorkerProvider::CodexCli));
        assert_eq!(preview.tasks[0].standalone, Some(false));
        assert_eq!(preview.tasks[0].base_model, "gpt-6-luna");
        let summary = fixture.authority.approve(request).unwrap();
        assert_eq!(summary.worker_profile_count, 1);
        let record = fixture
            .authority
            .get_for_operator(&summary.approval_id)
            .unwrap();
        let materialized = fixture
            .authority
            .materialize(
                &summary.approval_id,
                &sample_binding("bnd_codex", THREAD_UUID_A),
                &fixture.root.join("codex-work-area"),
            )
            .unwrap();
        assert_eq!(materialized.task_dispatch.len(), 1);
        assert!(materialized.native_routes.is_empty());
        assert_eq!(
            materialized.workers.unwrap().profiles[0].spec.provider,
            WorkerProvider::CodexCli
        );
        for corrupt in 0..5 {
            let mut task = record.tasks[0].clone();
            match corrupt {
                0 => task.canonical_executable = None,
                1 => task.cli_model_arg = None,
                2 => task.standalone = None,
                3 => task.file_permissions = None,
                _ => task.provider = Some(WorkerProvider::OpenCodeV2),
            }
            assert!(!task.is_frozen_headless_worker());
        }
        // Real-shaped pre-package Codex Coder approvals had no worker pins. Their existing
        // digest algorithm and read-only integrity remain valid; no worker authority is inferred.
        let mut legacy = record;
        let task = &mut legacy.tasks[0];
        task.provider = None;
        task.canonical_executable = None;
        task.cli_model_arg = None;
        task.standalone = None;
        task.file_permissions = None;
        task.profile_id = None;
        task.command_key = None;
        legacy.subject_digest = compute_record_subject_digest(&legacy).unwrap();
        legacy.approval_id = derive_approval_id(&legacy.subject_digest);
        let bytes = serde_json::to_vec(&legacy).unwrap();
        let decoded: ApprovalRecord = serde_json::from_slice(&bytes).unwrap();
        validation::verify_approval_record_integrity(&decoded, fixture.authority.approvals_path())
            .unwrap();
        assert_eq!(
            compute_record_subject_digest(&decoded).unwrap(),
            legacy.subject_digest
        );
        assert!(!decoded.tasks[0].is_frozen_headless_worker());
        let mut disk: serde_json::Value =
            serde_json::from_slice(&fs::read(fixture.authority.approvals_path()).unwrap()).unwrap();
        let entries = disk["approvals"].as_array_mut().unwrap();
        *entries = vec![serde_json::to_value(&legacy).unwrap()];
        fs::write(
            fixture.authority.approvals_path(),
            serde_json::to_vec(&disk).unwrap(),
        )
        .unwrap();
        let native = fixture
            .authority
            .materialize(
                &legacy.approval_id,
                &sample_binding("bnd_legacy", THREAD_UUID_A),
                &fixture.root.join("legacy-work-area"),
            )
            .unwrap();
        assert!(native.workers.is_none());
        assert!(native.task_dispatch.is_empty());
        assert_eq!(native.native_routes.len(), 1);
    }

    #[test]
    fn frozen_architect_approval_roundtrip_keeps_v1_bytes_and_subject_digest() {
        let fixture = setup_fixture();
        let current = fixture.registry.snapshot().expect("registry snapshot");
        let mut profile = current.profiles[0].clone();
        profile.assignments.push(RoleAssignment {
            role: WorkflowRole::Architect,
            difficulty: None,
            choice: ModelChoice {
                model_id: "model-codex-native".to_string(),
                effort: Some("high".to_string()),
            },
        });
        let updated = fixture
            .registry
            .apply(current.revision, RegistryChange::UpsertProfile { profile })
            .expect("upsert profile with architect");
        let registry_revision = updated.revision;

        let (plan_revision, plan_digest) = save_and_publish_plan(
            &fixture,
            "plan-frozen-architect",
            "binding-1",
            registry_revision,
            vec![PlanTask {
                id: "task-architect".to_string(),
                title: "Frozen advisor task".to_string(),
                instruction: "Read-only advisory task retained under the frozen v1 role key."
                    .to_string(),
                role: WorkflowRole::Architect,
                difficulty: Difficulty::Easy,
                depends_on: vec![],
                allowed_write_paths: vec!["src/lib.rs".to_string()],
            }],
        );

        let summary = fixture
            .authority
            .approve(OperatorApprovalRequest {
                binding_id: "binding-1".to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                plan_id: "plan-frozen-architect".to_string(),
                plan_revision,
                expected_registry_revision: registry_revision,
                expected_plan_digest: plan_digest,
                repository_path: fixture.repo_dir.clone(),
                base_commit: fixture.base_commit.clone(),
                approved_task_ids: vec!["task-architect".to_string()],
                approved_write_paths: vec!["src/lib.rs".to_string()],
                max_parallel: 1,
                budgets: ExecutionBudgets::default(),
            })
            .expect("approve frozen architect plan");
        let record = fixture
            .authority
            .get_for_operator(&summary.approval_id)
            .expect("load frozen approval");
        assert_eq!(record.tasks[0].role, WorkflowRole::Architect);

        let bytes = serde_json::to_vec(&record).expect("serialize frozen approval");
        let text = String::from_utf8(bytes.clone()).expect("approval JSON is UTF-8");
        assert!(text.contains(r#""role":"architect""#));
        assert!(!text.contains(r#""role":"advisor""#));

        let decoded: ApprovalRecord =
            serde_json::from_slice(&bytes).expect("deserialize frozen approval");
        assert_eq!(decoded, record);
        validation::verify_approval_record_integrity(&decoded, fixture.authority.approvals_path())
            .expect("frozen approval integrity");
        assert_eq!(
            compute_record_subject_digest(&decoded).expect("subject digest"),
            record.subject_digest
        );
    }

    #[test]
    fn scoped_profile_dependency_spec_golden_bytes_and_digest_remain_unchanged() {
        let spec = ScopedProfileDependencySpec {
            profile_id: "profile-main".to_string(),
            assignments: vec![
                RoleAssignment {
                    role: WorkflowRole::Architect,
                    difficulty: None,
                    choice: ModelChoice {
                        model_id: "model-codex-native".to_string(),
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
            models: vec![
                ScopedModelDependency {
                    id: "model-argon".to_string(),
                    tool_id: "tool-agy".to_string(),
                    model_arg: "gemini-4-argon-high".to_string(),
                    efforts: vec!["medium".to_string(), "high".to_string()],
                    allow_no_effort: false,
                },
                ScopedModelDependency {
                    id: "model-codex-native".to_string(),
                    tool_id: "tool-codex".to_string(),
                    model_arg: "gpt-5-codex".to_string(),
                    efforts: vec!["high".to_string()],
                    allow_no_effort: true,
                },
            ],
            tools: vec![
                ScopedToolDependency {
                    id: "tool-agy".to_string(),
                    adapter: ToolAdapter::Antigravity,
                    executable_path: Some(PathBuf::from("/opt/threadlane/bin/agy")),
                    enabled: true,
                },
                ScopedToolDependency {
                    id: "tool-codex".to_string(),
                    adapter: ToolAdapter::CodexCli,
                    executable_path: None,
                    enabled: true,
                },
            ],
        };
        spec.validate().expect("golden scoped spec validates");

        let expected_json = r#"{"profile_id":"profile-main","assignments":[{"role":"architect","difficulty":null,"choice":{"model_id":"model-codex-native","effort":"high"}},{"role":"coder","difficulty":"hard","choice":{"model_id":"model-argon","effort":"high"}}],"models":[{"id":"model-argon","tool_id":"tool-agy","model_arg":"gemini-4-argon-high","efforts":["medium","high"],"allow_no_effort":false},{"id":"model-codex-native","tool_id":"tool-codex","model_arg":"gpt-5-codex","efforts":["high"],"allow_no_effort":true}],"tools":[{"id":"tool-agy","adapter":"antigravity","executable_path":"/opt/threadlane/bin/agy","enabled":true},{"id":"tool-codex","adapter":"codex_cli","executable_path":null,"enabled":true}]}"#;
        let actual_json = serde_json::to_string(&spec).expect("serialize golden scoped spec");
        assert_eq!(
            actual_json, expected_json,
            "frozen scoped profile dependency bytes must keep architect and canonical ordering"
        );

        // Pre-transition global role and RoleAssignment bytes are exact v1 compatibility
        // goldens: a legacy Architect assignment still round-trips as `architect`.
        let golden_global_role = r#""architect""#;
        let golden_assignment = r#"{"role":"architect","difficulty":null,"choice":{"model_id":"model-codex-native","effort":"high"}}"#;
        assert_eq!(
            serde_json::to_string(&WorkflowRole::Architect).expect("serialize golden role"),
            golden_global_role
        );
        assert_eq!(
            serde_json::to_string(&spec.assignments[0]).expect("serialize golden assignment"),
            golden_assignment
        );
        let decoded_assignment: RoleAssignment = serde_json::from_str(golden_assignment)
            .expect("deserialize pre-transition RoleAssignment");
        assert_eq!(decoded_assignment.role, WorkflowRole::Architect);
        assert_eq!(
            serde_json::to_string(&decoded_assignment).expect("reserialize legacy assignment"),
            golden_assignment,
            "a deserialized legacy architect role must serialize back to architect"
        );

        fn sha256_hex(bytes: &[u8]) -> String {
            use sha2::{Digest, Sha256};
            use std::fmt::Write as _;

            let digest = Sha256::digest(bytes);
            let mut hex = String::with_capacity(64);
            for byte in digest {
                let _ = write!(&mut hex, "{byte:02x}");
            }
            hex
        }

        assert_eq!(
            sha256_hex(actual_json.as_bytes()),
            sha256_hex(expected_json.as_bytes())
        );
        assert_eq!(
            sha256_hex(
                serde_json::to_string(&WorkflowRole::Architect)
                    .expect("serialize role for digest")
                    .as_bytes()
            ),
            sha256_hex(golden_global_role.as_bytes())
        );
        assert_eq!(
            sha256_hex(
                serde_json::to_string(&spec.assignments[0])
                    .expect("serialize assignment for digest")
                    .as_bytes()
            ),
            sha256_hex(golden_assignment.as_bytes())
        );
    }

    #[test]
    fn worker_instruction_preflight_enforces_4096_utf8_bytes_preserves_native_and_legacy_records() {
        // 1. Direct guard enforces runtime UTF-8 byte length, NUL, and trimmed non-empty rules
        //    without echoing raw instruction prose.
        validate_executable_worker_instruction(&"x".repeat(threadlane_core::MAX_INSTRUCTION_LEN))
            .expect("4096 ASCII bytes accepted");
        let utf8_exact = "é".repeat(threadlane_core::MAX_INSTRUCTION_LEN / 2);
        assert_eq!(utf8_exact.chars().count(), 2_048);
        assert_eq!(utf8_exact.len(), threadlane_core::MAX_INSTRUCTION_LEN);
        validate_executable_worker_instruction(&utf8_exact)
            .expect("4096 UTF-8 bytes across 2048 chars accepted");

        for (bad_instruction, expected_fragment) in [
            (
                "x".repeat(threadlane_core::MAX_INSTRUCTION_LEN + 1),
                "exceeds maximum length of 4096 bytes",
            ),
            (
                format!("{utf8_exact}z"),
                "exceeds maximum length of 4096 bytes",
            ),
            ("€".repeat(1_366), "exceeds maximum length of 4096 bytes"),
            (
                "valid prefix\0SECRET_TAIL".to_string(),
                "must not contain NUL bytes",
            ),
            ("  \t\n  ".to_string(), "must not be empty"),
        ] {
            let err = validate_executable_worker_instruction(&bad_instruction)
                .expect_err("invalid worker instruction must fail");
            match err {
                AuthorityError::Validation { field, reason } => {
                    assert_eq!(field, "instruction");
                    assert!(
                        reason.contains(expected_fragment),
                        "unexpected reason: {reason}"
                    );
                    assert!(!reason.contains("SECRET_TAIL"));
                }
                other => panic!("expected AuthorityError::Validation, got {other:?}"),
            }
        }

        // 2. New authority preview/approve accepts 4096-byte ASCII & multibyte UTF-8 worker tasks
        //    alongside an 8192-byte manual native `codex_cli` route (`provider == None`), without
        //    truncating or normalizing frozen instructions.
        let fixture = setup_fixture();
        let reg_rev = fixture
            .registry
            .snapshot()
            .expect("registry snapshot")
            .revision;
        let ascii_4096 = format!(
            "  {}  ",
            "a".repeat(threadlane_core::MAX_INSTRUCTION_LEN - 4)
        );
        assert_eq!(ascii_4096.len(), threadlane_core::MAX_INSTRUCTION_LEN);
        let native_8192 = "n".repeat(8_192);
        let (plan_ok_rev, plan_ok_digest) = save_and_publish_plan(
            &fixture,
            "plan-inst-boundary-ok",
            "binding-1",
            reg_rev,
            vec![
                PlanTask {
                    id: "task-ascii-4096".to_string(),
                    title: "Exact 4096 ASCII bytes".to_string(),
                    instruction: ascii_4096.clone(),
                    role: WorkflowRole::Coder,
                    difficulty: Difficulty::Hard,
                    depends_on: Vec::new(),
                    allowed_write_paths: vec!["src/lib.rs".to_string()],
                },
                PlanTask {
                    id: "task-utf8-4096".to_string(),
                    title: "Exact 4096 UTF-8 bytes".to_string(),
                    instruction: utf8_exact.clone(),
                    role: WorkflowRole::Coder,
                    difficulty: Difficulty::Easy,
                    depends_on: Vec::new(),
                    allowed_write_paths: vec!["src/extra.rs".to_string()],
                },
                PlanTask {
                    id: "task-native-8192".to_string(),
                    title: "Manual native route >4096 bytes".to_string(),
                    instruction: native_8192.clone(),
                    role: WorkflowRole::Follower,
                    difficulty: Difficulty::Hard,
                    depends_on: Vec::new(),
                    allowed_write_paths: vec!["src/sub/".to_string()],
                },
            ],
        );

        let ok_req = OperatorApprovalRequest {
            binding_id: "binding-1".to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            plan_id: "plan-inst-boundary-ok".to_string(),
            plan_revision: plan_ok_rev,
            expected_registry_revision: reg_rev,
            expected_plan_digest: plan_ok_digest,
            repository_path: fixture.repo_dir.clone(),
            base_commit: fixture.base_commit.clone(),
            approved_task_ids: vec![
                "task-ascii-4096".to_string(),
                "task-utf8-4096".to_string(),
                "task-native-8192".to_string(),
            ],
            approved_write_paths: vec![
                "src/lib.rs".to_string(),
                "src/extra.rs".to_string(),
                "src/sub/".to_string(),
            ],
            max_parallel: 2,
            budgets: ExecutionBudgets::default(),
        };
        let preview_ok = fixture
            .authority
            .preview_for_operator(ok_req.clone())
            .expect("preview 4096-byte workers + 8192-byte native route");
        assert_eq!(preview_ok.tasks[0].instruction, ascii_4096);
        assert_eq!(preview_ok.tasks[1].instruction, utf8_exact);
        assert_eq!(preview_ok.tasks[2].instruction, native_8192);
        assert_eq!(preview_ok.tasks[2].provider, None);

        let ok_summary = fixture
            .authority
            .approve(ok_req)
            .expect("approve 4096-byte workers + 8192-byte native route");
        assert_eq!(ok_summary.worker_profile_count, 2);
        assert_eq!(ok_summary.native_route_count, 1);
        let ok_materialized = fixture
            .authority
            .materialize(
                &ok_summary.approval_id,
                &sample_binding("bnd_boundary_ok", THREAD_UUID_A),
                &fixture.root.join("boundary-ok-work-area"),
            )
            .expect("materialize boundary approval");
        assert_eq!(ok_materialized.task_dispatch.len(), 2);
        assert_eq!(ok_materialized.task_dispatch[0].instruction, ascii_4096);
        assert_eq!(ok_materialized.task_dispatch[1].instruction, utf8_exact);
        assert_eq!(ok_materialized.native_routes.len(), 1);
        assert_eq!(ok_materialized.native_routes[0].instruction, native_8192);

        // 3. Oversized LATER task (4097 ASCII bytes and 4098 UTF-8 bytes across 1366 chars) is
        //    rejected on preview and approve with zero new authority records written.
        let reject_fixture = setup_fixture();
        let reject_reg_rev = reject_fixture.registry.snapshot().unwrap().revision;
        let valid_first = "v".repeat(3_965);
        let oversized_ascii_4097 = "b".repeat(threadlane_core::MAX_INSTRUCTION_LEN + 1);
        let oversized_utf8_4098 = "€".repeat(1_366);
        assert_eq!(oversized_utf8_4098.chars().count(), 1_366);
        assert_eq!(oversized_utf8_4098.len(), 4_098);

        for (plan_id, later_instruction) in [
            ("plan-later-ascii-4097", oversized_ascii_4097),
            ("plan-later-utf8-4098", oversized_utf8_4098),
        ] {
            let (bad_rev, bad_digest) = save_and_publish_plan(
                &reject_fixture,
                plan_id,
                "binding-1",
                reject_reg_rev,
                vec![
                    PlanTask {
                        id: "task-root-first".to_string(),
                        title: "Valid first root".to_string(),
                        instruction: valid_first.clone(),
                        role: WorkflowRole::Coder,
                        difficulty: Difficulty::Hard,
                        depends_on: Vec::new(),
                        allowed_write_paths: vec!["src/lib.rs".to_string()],
                    },
                    PlanTask {
                        id: "task-later-oversized".to_string(),
                        title: "Oversized later worker".to_string(),
                        instruction: later_instruction,
                        role: WorkflowRole::Coder,
                        difficulty: Difficulty::Easy,
                        depends_on: vec!["task-root-first".to_string()],
                        allowed_write_paths: vec!["src/extra.rs".to_string()],
                    },
                ],
            );
            let bad_req = OperatorApprovalRequest {
                binding_id: "binding-1".to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                plan_id: plan_id.to_string(),
                plan_revision: bad_rev,
                expected_registry_revision: reject_reg_rev,
                expected_plan_digest: bad_digest,
                repository_path: reject_fixture.repo_dir.clone(),
                base_commit: reject_fixture.base_commit.clone(),
                approved_task_ids: vec![
                    "task-root-first".to_string(),
                    "task-later-oversized".to_string(),
                ],
                approved_write_paths: vec!["src/lib.rs".to_string(), "src/extra.rs".to_string()],
                max_parallel: 2,
                budgets: ExecutionBudgets::default(),
            };
            for err in [
                reject_fixture
                    .authority
                    .preview_for_operator(bad_req.clone())
                    .expect_err("preview must reject oversized later task"),
                reject_fixture
                    .authority
                    .approve(bad_req)
                    .expect_err("approve must reject oversized later task"),
            ] {
                match err {
                    AuthorityError::Validation { field, reason } => {
                        assert_eq!(field, "instruction");
                        assert!(reason.contains("task-later-oversized"));
                        assert!(reason.contains("4096 bytes"));
                    }
                    other => panic!("expected AuthorityError::Validation, got {other:?}"),
                }
            }
            assert!(
                !reject_fixture.authority.approvals_path().exists(),
                "rejected preview/approve must not create approvals.json"
            );
            assert!(reject_fixture
                .authority
                .list_for_binding("binding-1")
                .unwrap()
                .is_empty());
        }

        // 4. Historical validly sealed oversized ApprovalRecord remains readable and materializable
        //    without mutating or truncating stored instructions.
        let legacy_oversized = "L".repeat(4_365);
        let (legacy_rev, legacy_plan_digest) = save_and_publish_plan(
            &reject_fixture,
            "plan-legacy-sealed-4365",
            "binding-1",
            reject_reg_rev,
            vec![
                PlanTask {
                    id: "task-valid-a".to_string(),
                    title: "Valid root A".to_string(),
                    instruction: valid_first.clone(),
                    role: WorkflowRole::Coder,
                    difficulty: Difficulty::Hard,
                    depends_on: Vec::new(),
                    allowed_write_paths: vec!["src/lib.rs".to_string()],
                },
                PlanTask {
                    id: "task-legacy-b".to_string(),
                    title: "Historical 4365-byte root B".to_string(),
                    instruction: legacy_oversized.clone(),
                    role: WorkflowRole::Coder,
                    difficulty: Difficulty::Hard,
                    depends_on: Vec::new(),
                    allowed_write_paths: vec!["src/extra.rs".to_string()],
                },
            ],
        );
        let legacy_plan = reject_fixture
            .plans
            .get_revision("plan-legacy-sealed-4365", legacy_rev)
            .unwrap()
            .unwrap();
        let legacy_snap = reject_fixture.registry.snapshot().unwrap();
        let compiled_legacy = compile_plan_revision(&legacy_plan, &legacy_snap).unwrap();
        let canonical_repo = fs::canonicalize(&reject_fixture.repo_dir).unwrap();
        let canonical_agy = fs::canonicalize(&reject_fixture.fake_agy_exe).unwrap();
        let mut legacy_record = ApprovalRecord {
            approval_id: String::new(),
            subject_digest: String::new(),
            policy_version: AUTHORITY_POLICY_VERSION,
            binding_id: "binding-1".to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            plan_id: "plan-legacy-sealed-4365".to_string(),
            plan_revision: legacy_rev,
            profile_id: legacy_plan.profile_id.clone(),
            registry_revision: reject_reg_rev,
            plan_digest: legacy_plan_digest,
            repository_path: canonical_repo,
            base_commit: reject_fixture.base_commit.clone(),
            approved_task_ids: vec!["task-valid-a".to_string(), "task-legacy-b".to_string()],
            approved_write_paths: vec!["src/lib.rs".to_string(), "src/extra.rs".to_string()],
            max_parallel: 2,
            budgets: ExecutionBudgets::default(),
            tasks: compiled_legacy
                .tasks
                .iter()
                .map(|ct| ApprovedTaskRecord {
                    task_id: ct.id.clone(),
                    title: ct.title.clone(),
                    instruction: ct.instruction.clone(),
                    instruction_digest: ct.instruction_digest.clone(),
                    role: ct.role,
                    difficulty: ct.difficulty,
                    tool_id: ct.tool_id.clone(),
                    adapter: ct.adapter,
                    provider: Some(WorkerProvider::Antigravity),
                    canonical_executable: Some(canonical_agy.clone()),
                    model_id: ct.choice.model_id.clone(),
                    model_arg: ct.model_arg.clone(),
                    base_model: ct.model_arg.clone(),
                    effort: ct.choice.effort.clone(),
                    cli_model_arg: Some(ct.model_arg.clone()),
                    standalone: Some(false),
                    file_permissions: Some(WorkerFilePermissions::TaskFiles),
                    depends_on: ct.depends_on.clone(),
                    allowed_write_paths: ct.allowed_write_paths.clone(),
                    required_changed_paths: None,
                    task_digest: ct.task_digest.clone(),
                    profile_id: None,
                    command_key: None,
                })
                .collect(),
            created_at_ms: 1_700_000_050_000,
        };
        let core_digest = compute_authority_core_digest(&legacy_record).unwrap();
        for task in &mut legacy_record.tasks {
            task.profile_id = Some(derive_profile_id(
                &core_digest,
                &task.task_id,
                &task.task_digest,
            ));
            task.command_key = Some(derive_command_key(
                &core_digest,
                &task.task_id,
                &task.task_digest,
            ));
        }
        legacy_record.subject_digest = compute_record_subject_digest(&legacy_record).unwrap();
        legacy_record.approval_id = derive_approval_id(&legacy_record.subject_digest);
        verify_approval_record_integrity(&legacy_record, reject_fixture.authority.approvals_path())
            .expect("legacy oversized record integrity remains valid");

        let ledger_json = serde_json::json!({
            "schema_version": AUTHORITY_SCHEMA_VERSION,
            "approvals": [&legacy_record],
        });
        fs::write(
            reject_fixture.authority.approvals_path(),
            serde_json::to_vec(&ledger_json).unwrap(),
        )
        .unwrap();
        fs::set_permissions(
            reject_fixture.authority.approvals_path(),
            fs::Permissions::from_mode(0o600),
        )
        .unwrap();

        let reopened = AuthorityStore::open(
            reject_fixture.authority.dir(),
            Arc::clone(&reject_fixture.registry),
            Arc::clone(&reject_fixture.plans),
        )
        .expect("reopen AuthorityStore with historical oversized record");
        let loaded = reopened
            .get_for_operator(&legacy_record.approval_id)
            .expect("get_for_operator reads historical oversized record");
        assert_eq!(loaded, legacy_record);
        assert_eq!(loaded.tasks[1].instruction.len(), 4_365);
        assert_eq!(reopened.list_for_binding("binding-1").unwrap().len(), 1);
        let materialized_legacy = reopened
            .materialize(
                &legacy_record.approval_id,
                &sample_binding("bnd_legacy_mat", THREAD_UUID_A),
                &reject_fixture.root.join("legacy-oversized-work-area"),
            )
            .expect("materialize historical oversized record for session inspection/recovery");
        assert_eq!(materialized_legacy.task_dispatch.len(), 2);
        assert_eq!(
            materialized_legacy.task_dispatch[1].instruction.len(),
            4_365
        );
    }

    #[test]
    fn required_changed_paths_omitted_preserves_legacy_digests_and_byte_compatible_serialization() {
        let fixture = setup_fixture();
        let reg_rev = fixture.registry.snapshot().unwrap().revision;
        let (plan_rev, _plan_digest) = save_and_publish_plan(
            &fixture,
            "plan-req-legacy-compat",
            "binding-1",
            reg_rev,
            vec![PlanTask {
                id: "task-legacy".to_string(),
                title: "Legacy unconstrained task".to_string(),
                instruction: "Update src/lib.rs and optionally src/extra.rs.".to_string(),
                role: WorkflowRole::Coder,
                difficulty: Difficulty::Hard,
                depends_on: Vec::new(),
                allowed_write_paths: vec!["src/lib.rs".to_string(), "src/extra.rs".to_string()],
            }],
        );

        let intent = OperatorApprovalIntent {
            binding_id: "binding-1".to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            plan_id: "plan-req-legacy-compat".to_string(),
            plan_revision: plan_rev,
            expected_registry_revision: reg_rev,
            repository_path: fixture.repo_dir.clone(),
            base_commit: fixture.base_commit.clone(),
            approved_task_ids: vec!["task-legacy".to_string()],
            approved_write_paths: vec!["src/lib.rs".to_string(), "src/extra.rs".to_string()],
            max_parallel: 1,
            budgets: ExecutionBudgets::default(),
        };

        let preview = fixture
            .authority
            .prepare_for_operator(intent.clone())
            .expect("legacy prepare succeeds");
        assert!(preview.tasks[0].required_changed_paths.is_none());
        let preview_json = serde_json::to_string(&preview).expect("serialize preview");
        assert!(
            !preview_json.contains("required_changed_paths"),
            "legacy preview JSON must omit required_changed_paths when None"
        );

        let summary = fixture
            .authority
            .approve_confirmed(preview.request.clone(), &preview.subject_digest)
            .expect("legacy approve_confirmed succeeds");
        let record = fixture
            .authority
            .get_for_operator(&summary.approval_id)
            .expect("load persisted legacy record");
        assert!(record.tasks[0].required_changed_paths.is_none());
        let record_json = serde_json::to_string(&record).expect("serialize record");
        assert!(
            !record_json.contains("required_changed_paths"),
            "legacy ApprovalRecord JSON must omit required_changed_paths when None"
        );

        // Compute the exact legacy pre-change core and subject digests (without required_changed_paths).
        let t = &record.tasks[0];
        let legacy_core_canonical = serde_json::json!({
            "domain": "threadlane-authority-core-v1",
            "schema_version": AUTHORITY_SCHEMA_VERSION,
            "policy_version": record.policy_version,
            "binding_id": record.binding_id,
            "thread_uuid": record.thread_uuid,
            "plan_id": record.plan_id,
            "plan_revision": record.plan_revision,
            "profile_id": record.profile_id,
            "registry_revision": record.registry_revision,
            "plan_digest": record.plan_digest,
            "repository_path": record.repository_path.to_string_lossy(),
            "base_commit": record.base_commit,
            "approved_task_ids": record.approved_task_ids,
            "approved_write_paths": record.approved_write_paths,
            "max_parallel": record.max_parallel,
            "budgets": {
                "timeout_ms": record.budgets.timeout_ms,
                "max_line_bytes": record.budgets.max_line_bytes,
                "max_stdout_bytes": record.budgets.max_stdout_bytes,
                "max_stderr_bytes": record.budgets.max_stderr_bytes,
                "max_events": record.budgets.max_events,
            },
            "tasks": [{
                "task_id": t.task_id,
                "title": t.title,
                "instruction": t.instruction,
                "instruction_digest": t.instruction_digest,
                "role": t.role.as_str(),
                "difficulty": t.difficulty.as_str(),
                "tool_id": t.tool_id,
                "adapter": t.adapter.as_str(),
                "provider": t.provider.map(WorkerProvider::as_str),
                "canonical_executable": t.canonical_executable.as_ref().map(|p| p.to_string_lossy().to_string()),
                "model_id": t.model_id,
                "model_arg": t.model_arg,
                "base_model": t.base_model,
                "effort": t.effort,
                "cli_model_arg": t.cli_model_arg,
                "standalone": t.standalone,
                "file_permissions": t.file_permissions.map(WorkerFilePermissions::as_str),
                "depends_on": t.depends_on,
                "allowed_write_paths": t.allowed_write_paths,
                "task_digest": t.task_digest,
            }],
        });
        let expected_legacy_core =
            validation::sha256_hex(&serde_json::to_vec(&legacy_core_canonical).unwrap());
        assert_eq!(
            compute_authority_core_digest(&record).unwrap(),
            expected_legacy_core
        );
        assert_eq!(
            t.profile_id.as_deref(),
            Some(derive_profile_id(&expected_legacy_core, &t.task_id, &t.task_digest).as_str())
        );
        assert_eq!(
            t.command_key.as_deref(),
            Some(derive_command_key(&expected_legacy_core, &t.task_id, &t.task_digest).as_str())
        );

        let legacy_subject_canonical = serde_json::json!({
            "domain": "threadlane-authority-subject-v1",
            "schema_version": AUTHORITY_SCHEMA_VERSION,
            "policy_version": record.policy_version,
            "binding_id": record.binding_id,
            "thread_uuid": record.thread_uuid,
            "plan_id": record.plan_id,
            "plan_revision": record.plan_revision,
            "profile_id": record.profile_id,
            "registry_revision": record.registry_revision,
            "plan_digest": record.plan_digest,
            "repository_path": record.repository_path.to_string_lossy(),
            "base_commit": record.base_commit,
            "approved_task_ids": record.approved_task_ids,
            "approved_write_paths": record.approved_write_paths,
            "max_parallel": record.max_parallel,
            "budgets": {
                "timeout_ms": record.budgets.timeout_ms,
                "max_line_bytes": record.budgets.max_line_bytes,
                "max_stdout_bytes": record.budgets.max_stdout_bytes,
                "max_stderr_bytes": record.budgets.max_stderr_bytes,
                "max_events": record.budgets.max_events,
            },
            "tasks": [{
                "task_id": t.task_id,
                "title": t.title,
                "instruction": t.instruction,
                "instruction_digest": t.instruction_digest,
                "role": t.role.as_str(),
                "difficulty": t.difficulty.as_str(),
                "tool_id": t.tool_id,
                "adapter": t.adapter.as_str(),
                "provider": t.provider.map(WorkerProvider::as_str),
                "canonical_executable": t.canonical_executable.as_ref().map(|p| p.to_string_lossy().to_string()),
                "model_id": t.model_id,
                "model_arg": t.model_arg,
                "base_model": t.base_model,
                "effort": t.effort,
                "cli_model_arg": t.cli_model_arg,
                "standalone": t.standalone,
                "file_permissions": t.file_permissions.map(WorkerFilePermissions::as_str),
                "depends_on": t.depends_on,
                "allowed_write_paths": t.allowed_write_paths,
                "task_digest": t.task_digest,
                "profile_id": t.profile_id,
                "command_key": t.command_key,
            }],
        });
        let expected_legacy_subject =
            validation::sha256_hex(&serde_json::to_vec(&legacy_subject_canonical).unwrap());
        assert_eq!(record.subject_digest, expected_legacy_subject);
        assert_eq!(
            record.approval_id,
            derive_approval_id(&expected_legacy_subject)
        );

        // Old serialized ApprovalRecord JSON without required_changed_paths deserializes and verifies cleanly.
        let deserialized: ApprovalRecord =
            serde_json::from_str(&record_json).expect("deserialize old ApprovalRecord JSON");
        assert_eq!(deserialized, record);
        verify_approval_record_integrity(&deserialized, fixture.authority.approvals_path())
            .expect("legacy record integrity succeeds");

        // V2 intent/request with empty task_required_changed_paths matches the exact legacy digest.
        let v2_empty_preview = fixture
            .authority
            .prepare_for_operator_v2(OperatorApprovalIntentV2 {
                schema_version: AUTHORITY_REQUEST_V2_SCHEMA_VERSION,
                intent,
                task_required_changed_paths: BTreeMap::new(),
            })
            .expect("v2 empty map prepare succeeds");
        assert_eq!(v2_empty_preview, preview);
        let idempotent_v2 = fixture
            .authority
            .approve_confirmed_v2(preview.to_request_v2().unwrap(), &preview.subject_digest)
            .expect("v2 empty map approve_confirmed matches legacy idempotently");
        assert_eq!(idempotent_v2, summary);
    }

    #[test]
    fn required_changed_paths_selection_binds_digests_round_trips_and_keeps_permissions_a() {
        let fixture = setup_fixture();
        let reg_rev = fixture.registry.snapshot().unwrap().revision;
        let (plan_rev, _plan_digest) = save_and_publish_plan(
            &fixture,
            "plan-req-selected",
            "binding-1",
            reg_rev,
            vec![
                PlanTask {
                    id: "task-exact-a".to_string(),
                    title: "Exact file subset task".to_string(),
                    instruction: "Modify src/lib.rs and optionally touch src/extra.rs.".to_string(),
                    role: WorkflowRole::Coder,
                    difficulty: Difficulty::Hard,
                    depends_on: Vec::new(),
                    allowed_write_paths: vec!["src/lib.rs".to_string(), "src/extra.rs".to_string()],
                },
                PlanTask {
                    id: "task-dir-b".to_string(),
                    title: "Directory scope with exact required files".to_string(),
                    instruction: "Update src/sub/mod.rs and add src/sub/new_file.rs.".to_string(),
                    role: WorkflowRole::Coder,
                    difficulty: Difficulty::Easy,
                    depends_on: Vec::new(),
                    allowed_write_paths: vec!["src/sub/".to_string()],
                },
            ],
        );

        let base_intent = OperatorApprovalIntent {
            binding_id: "binding-1".to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            plan_id: "plan-req-selected".to_string(),
            plan_revision: plan_rev,
            expected_registry_revision: reg_rev,
            repository_path: fixture.repo_dir.clone(),
            base_commit: fixture.base_commit.clone(),
            approved_task_ids: vec!["task-exact-a".to_string(), "task-dir-b".to_string()],
            approved_write_paths: vec![
                "src/lib.rs".to_string(),
                "src/extra.rs".to_string(),
                "src/sub/".to_string(),
            ],
            max_parallel: 2,
            budgets: ExecutionBudgets::default(),
        };

        let legacy_preview = fixture
            .authority
            .prepare_for_operator(base_intent.clone())
            .expect("legacy preview without R");

        let mut required_map = BTreeMap::new();
        required_map.insert("task-exact-a".to_string(), vec!["src/lib.rs".to_string()]);
        required_map.insert(
            "task-dir-b".to_string(),
            vec![
                "src/sub/mod.rs".to_string(),
                "src/sub/new_file.rs".to_string(),
            ],
        );
        let r_preview = fixture
            .authority
            .prepare_for_operator_v2(OperatorApprovalIntentV2 {
                schema_version: AUTHORITY_REQUEST_V2_SCHEMA_VERSION,
                intent: base_intent,
                task_required_changed_paths: required_map.clone(),
            })
            .expect("v2 preview with accepted R selection");

        assert_eq!(
            r_preview.tasks[0].allowed_write_paths,
            vec!["src/lib.rs".to_string(), "src/extra.rs".to_string()]
        );
        assert_eq!(
            r_preview.tasks[0].required_changed_paths,
            Some(vec!["src/lib.rs".to_string()])
        );
        assert_eq!(
            r_preview.tasks[1].allowed_write_paths,
            vec!["src/sub/".to_string()]
        );
        assert_eq!(
            r_preview.tasks[1].required_changed_paths,
            Some(vec![
                "src/sub/mod.rs".to_string(),
                "src/sub/new_file.rs".to_string()
            ])
        );

        // Selecting R must change BOTH core digest (profile_id / command_key) and subject digest.
        assert_ne!(r_preview.subject_digest, legacy_preview.subject_digest);
        assert_ne!(
            r_preview.tasks[0].profile_id,
            legacy_preview.tasks[0].profile_id
        );
        assert_ne!(
            r_preview.tasks[0].command_key,
            legacy_preview.tasks[0].command_key
        );

        // Original request v2 round-trip reproduces the exact preview.
        let req_v2 = r_preview.to_request_v2().expect("to_request_v2");
        assert_eq!(req_v2.schema_version, AUTHORITY_REQUEST_V2_SCHEMA_VERSION);
        assert_eq!(req_v2.task_required_changed_paths, required_map);
        let reconstructed_preview = fixture
            .authority
            .preview_for_operator_v2(req_v2.clone())
            .expect("preview_for_operator_v2 round-trip");
        assert_eq!(reconstructed_preview, r_preview);

        // Commit via approve_confirmed_preview and verify idempotent replay via approve_confirmed_v2.
        let summary_preview = fixture
            .authority
            .approve_confirmed_preview(r_preview.clone(), &r_preview.subject_digest)
            .expect("approve_confirmed_preview with R succeeds");
        let summary_req_v2 = fixture
            .authority
            .approve_confirmed_v2(req_v2, &r_preview.subject_digest)
            .expect("approve_confirmed_v2 replays idempotently");
        assert_eq!(summary_req_v2, summary_preview);

        // Reopen store from disk and verify persisted required_changed_paths + integrity.
        let reopened = AuthorityStore::open(
            fixture.authority.dir(),
            Arc::clone(&fixture.registry),
            Arc::clone(&fixture.plans),
        )
        .expect("reopen AuthorityStore with persisted R");
        let record = reopened
            .get_for_operator(&summary_preview.approval_id)
            .expect("load persisted record with R");
        assert_eq!(
            record.tasks[0].required_changed_paths,
            Some(vec!["src/lib.rs".to_string()])
        );
        assert_eq!(
            record.tasks[1].required_changed_paths,
            Some(vec![
                "src/sub/mod.rs".to_string(),
                "src/sub/new_file.rs".to_string()
            ])
        );

        // Normal worker write permissions in materialized WorkersConfig MUST remain A, not R.
        let materialized = reopened
            .materialize(
                &record.approval_id,
                &sample_binding("bnd_mat_r", THREAD_UUID_A),
                &fixture.root.join("mat-r-work-area"),
            )
            .expect("materialize approval with R");
        let workers = materialized.workers.expect("materialized workers config");
        assert_eq!(workers.profiles.len(), 2);
        assert_eq!(
            workers.profiles[0].spec.allowed_write_paths,
            BTreeSet::from(["src/lib.rs".to_string(), "src/extra.rs".to_string()]),
            "worker write permissions must remain A, not narrowed to R"
        );
        assert_eq!(
            workers.profiles[1].spec.allowed_write_paths,
            BTreeSet::from(["src/sub/".to_string()]),
            "directory worker write permissions must remain A, not narrowed to R"
        );
    }

    #[test]
    fn required_changed_paths_rejects_missing_overbound_duplicate_directory_and_foreign() {
        let fixture = setup_fixture();
        let reg_rev = fixture.registry.snapshot().unwrap().revision;
        let (plan_rev, plan_digest) = save_and_publish_plan(
            &fixture,
            "plan-req-invalid",
            "binding-1",
            reg_rev,
            vec![
                PlanTask {
                    id: "task-a".to_string(),
                    title: "Task A".to_string(),
                    instruction: "Modify src/lib.rs or src/extra.rs.".to_string(),
                    role: WorkflowRole::Coder,
                    difficulty: Difficulty::Hard,
                    depends_on: Vec::new(),
                    allowed_write_paths: vec!["src/lib.rs".to_string(), "src/extra.rs".to_string()],
                },
                PlanTask {
                    id: "task-b".to_string(),
                    title: "Task B".to_string(),
                    instruction: "Modify files under src/sub/.".to_string(),
                    role: WorkflowRole::Coder,
                    difficulty: Difficulty::Easy,
                    depends_on: Vec::new(),
                    allowed_write_paths: vec!["src/sub/".to_string()],
                },
            ],
        );

        let make_intent = |approved_tasks: Vec<String>| OperatorApprovalIntent {
            binding_id: "binding-1".to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            plan_id: "plan-req-invalid".to_string(),
            plan_revision: plan_rev,
            expected_registry_revision: reg_rev,
            repository_path: fixture.repo_dir.clone(),
            base_commit: fixture.base_commit.clone(),
            approved_task_ids: approved_tasks,
            approved_write_paths: vec![
                "src/lib.rs".to_string(),
                "src/extra.rs".to_string(),
                "src/sub/".to_string(),
            ],
            max_parallel: 2,
            budgets: ExecutionBudgets::default(),
        };

        let make_request = |approved_tasks: Vec<String>| OperatorApprovalRequest {
            binding_id: "binding-1".to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            plan_id: "plan-req-invalid".to_string(),
            plan_revision: plan_rev,
            expected_registry_revision: reg_rev,
            expected_plan_digest: plan_digest.clone(),
            repository_path: fixture.repo_dir.clone(),
            base_commit: fixture.base_commit.clone(),
            approved_task_ids: approved_tasks,
            approved_write_paths: vec![
                "src/lib.rs".to_string(),
                "src/extra.rs".to_string(),
                "src/sub/".to_string(),
            ],
            max_parallel: 2,
            budgets: ExecutionBudgets::default(),
        };

        let both_tasks = vec!["task-a".to_string(), "task-b".to_string()];
        let overbound_paths: Vec<String> = (0..=MAX_REQUIRED_CHANGED_PATHS)
            .map(|i| format!("src/sub/file_{i}.rs"))
            .collect();

        let invalid_cases = [
            (
                "empty required_changed_paths",
                both_tasks.clone(),
                BTreeMap::from([("task-a".to_string(), Vec::new())]),
                "validation",
            ),
            (
                "empty path entry in required_changed_paths",
                both_tasks.clone(),
                BTreeMap::from([("task-a".to_string(), vec![String::new()])]),
                "invalid_write_scope",
            ),
            (
                "over-bound required_changed_paths",
                both_tasks.clone(),
                BTreeMap::from([("task-b".to_string(), overbound_paths)]),
                "validation",
            ),
            (
                "duplicate required_changed_paths",
                both_tasks.clone(),
                BTreeMap::from([(
                    "task-a".to_string(),
                    vec!["src/lib.rs".to_string(), "src/lib.rs".to_string()],
                )]),
                "validation",
            ),
            (
                "directory trailing slash in required_changed_paths",
                both_tasks.clone(),
                BTreeMap::from([("task-b".to_string(), vec!["src/sub/".to_string()])]),
                "validation",
            ),
            (
                "bare existing directory in required_changed_paths",
                both_tasks.clone(),
                BTreeMap::from([("task-b".to_string(), vec!["src/sub".to_string()])]),
                "invalid_write_scope",
            ),
            (
                "foreign path outside repository allowlist",
                both_tasks.clone(),
                BTreeMap::from([(
                    "task-a".to_string(),
                    vec!["unapproved/secret.rs".to_string()],
                )]),
                "validation",
            ),
            (
                "foreign path owned by another task outside this task allowed_write_paths",
                both_tasks.clone(),
                BTreeMap::from([("task-a".to_string(), vec!["src/sub/mod.rs".to_string()])]),
                "validation",
            ),
            (
                "missing/unknown task_id key",
                both_tasks.clone(),
                BTreeMap::from([("task-missing".to_string(), vec!["src/lib.rs".to_string()])]),
                "validation",
            ),
            (
                "unselected task_id key",
                vec!["task-a".to_string()],
                BTreeMap::from([("task-b".to_string(), vec!["src/sub/mod.rs".to_string()])]),
                "validation",
            ),
            (
                "protected control path in required_changed_paths",
                both_tasks.clone(),
                BTreeMap::from([("task-a".to_string(), vec![".git/config".to_string()])]),
                "invalid_write_scope",
            ),
        ];

        for (label, approved_tasks, required_map, expected_code) in invalid_cases {
            let Err(prep_err) =
                fixture
                    .authority
                    .prepare_for_operator_v2(OperatorApprovalIntentV2 {
                        schema_version: AUTHORITY_REQUEST_V2_SCHEMA_VERSION,
                        intent: make_intent(approved_tasks.clone()),
                        task_required_changed_paths: required_map.clone(),
                    })
            else {
                panic!("prepare_for_operator_v2 must reject {label}");
            };
            assert_eq!(
                prep_err.code(),
                expected_code,
                "prepare {label}: {prep_err}"
            );

            let Err(rec_err) = fixture.authority.approve_v2(OperatorApprovalRequestV2 {
                schema_version: AUTHORITY_REQUEST_V2_SCHEMA_VERSION,
                request: make_request(approved_tasks),
                task_required_changed_paths: required_map,
            }) else {
                panic!("approve_v2 must reject {label}");
            };
            assert_eq!(rec_err.code(), expected_code, "approve {label}: {rec_err}");
        }

        // Unsupported schema_version in v2 wrappers is rejected on both prepare and record.
        for bad_version in [0u32, 1, 3] {
            let prep_ver = fixture
                .authority
                .prepare_for_operator_v2(OperatorApprovalIntentV2 {
                    schema_version: bad_version,
                    intent: make_intent(both_tasks.clone()),
                    task_required_changed_paths: BTreeMap::from([(
                        "task-a".to_string(),
                        vec!["src/lib.rs".to_string()],
                    )]),
                })
                .expect_err("bad schema_version prepare must fail");
            assert_eq!(prep_ver.code(), "validation");

            let rec_ver = fixture
                .authority
                .approve_v2(OperatorApprovalRequestV2 {
                    schema_version: bad_version,
                    request: make_request(both_tasks.clone()),
                    task_required_changed_paths: BTreeMap::from([(
                        "task-a".to_string(),
                        vec!["src/lib.rs".to_string()],
                    )]),
                })
                .expect_err("bad schema_version approve must fail");
            assert_eq!(rec_ver.code(), "validation");
        }

        assert!(
            !fixture.authority.approvals_path().exists(),
            "rejected preparations and approvals must not create approvals.json"
        );

        // Stored-record integrity rejects corrupt required_changed_paths shapes on disk.
        let valid_preview = fixture
            .authority
            .prepare_for_operator_v2(OperatorApprovalIntentV2 {
                schema_version: AUTHORITY_REQUEST_V2_SCHEMA_VERSION,
                intent: make_intent(vec!["task-a".to_string()]),
                task_required_changed_paths: BTreeMap::from([(
                    "task-a".to_string(),
                    vec!["src/lib.rs".to_string()],
                )]),
            })
            .expect("valid preview for integrity checks");
        let valid_summary = fixture
            .authority
            .approve_confirmed_preview(valid_preview.clone(), &valid_preview.subject_digest)
            .expect("persist valid record for integrity mutation tests");
        let base_record = fixture
            .authority
            .get_for_operator(&valid_summary.approval_id)
            .expect("load valid record");

        let corrupt_variants: Vec<(&str, Option<Vec<String>>)> = vec![
            ("empty stored R", Some(Vec::new())),
            (
                "duplicate stored R",
                Some(vec!["src/lib.rs".to_string(), "src/lib.rs".to_string()]),
            ),
            ("directory stored R", Some(vec!["src/".to_string()])),
            ("foreign stored R", Some(vec!["src/sub/mod.rs".to_string()])),
        ];
        for (label, bad_r) in corrupt_variants {
            let mut mutated = base_record.clone();
            mutated.tasks[0].required_changed_paths = bad_r;
            let Err(err) =
                verify_approval_record_integrity(&mutated, fixture.authority.approvals_path())
            else {
                panic!("integrity must reject {label}");
            };
            assert_eq!(err.code(), "corrupt_store", "{label}: {err}");
        }
    }

    #[test]
    fn required_changed_paths_rejects_tampered_preview_and_request_even_when_subset_of_allowed() {
        let fixture = setup_fixture();
        let reg_rev = fixture.registry.snapshot().unwrap().revision;
        let (plan_rev, _plan_digest) = save_and_publish_plan(
            &fixture,
            "plan-req-tamper",
            "binding-1",
            reg_rev,
            vec![PlanTask {
                id: "task-a".to_string(),
                title: "Two-file task".to_string(),
                instruction: "Edit src/lib.rs and optionally src/extra.rs.".to_string(),
                role: WorkflowRole::Coder,
                difficulty: Difficulty::Hard,
                depends_on: Vec::new(),
                allowed_write_paths: vec!["src/lib.rs".to_string(), "src/extra.rs".to_string()],
            }],
        );

        let intent = OperatorApprovalIntent {
            binding_id: "binding-1".to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            plan_id: "plan-req-tamper".to_string(),
            plan_revision: plan_rev,
            expected_registry_revision: reg_rev,
            repository_path: fixture.repo_dir.clone(),
            base_commit: fixture.base_commit.clone(),
            approved_task_ids: vec!["task-a".to_string()],
            approved_write_paths: vec!["src/lib.rs".to_string(), "src/extra.rs".to_string()],
            max_parallel: 1,
            budgets: ExecutionBudgets::default(),
        };

        let preview_a = fixture
            .authority
            .prepare_for_operator_v2(OperatorApprovalIntentV2 {
                schema_version: AUTHORITY_REQUEST_V2_SCHEMA_VERSION,
                intent: intent.clone(),
                task_required_changed_paths: BTreeMap::from([(
                    "task-a".to_string(),
                    vec!["src/lib.rs".to_string()],
                )]),
            })
            .expect("preview with R=[src/lib.rs]");
        let preview_b = fixture
            .authority
            .prepare_for_operator_v2(OperatorApprovalIntentV2 {
                schema_version: AUTHORITY_REQUEST_V2_SCHEMA_VERSION,
                intent,
                task_required_changed_paths: BTreeMap::from([(
                    "task-a".to_string(),
                    vec!["src/extra.rs".to_string()],
                )]),
            })
            .expect("preview with R=[src/extra.rs]");
        assert_ne!(preview_a.subject_digest, preview_b.subject_digest);

        // 1. Tampered preview changing R to another valid subset of A while keeping original digest.
        let mut tampered_preview = preview_a.clone();
        tampered_preview.tasks[0].required_changed_paths = Some(vec!["src/extra.rs".to_string()]);
        let err = fixture
            .authority
            .approve_confirmed_preview(tampered_preview, &preview_a.subject_digest)
            .expect_err("tampered preview R must be rejected before commit");
        assert_eq!(err.code(), "validation");

        // 2. Self-consistent preview for R=[src/extra.rs] submitted with R=[src/lib.rs] subject digest.
        let err = fixture
            .authority
            .approve_confirmed_preview(preview_b, &preview_a.subject_digest)
            .expect_err("different R preview with old subject digest must be rejected");
        assert_eq!(err.code(), "validation");

        // 3. Preview with R silently stripped to None.
        let mut stripped_preview = preview_a.clone();
        stripped_preview.tasks[0].required_changed_paths = None;
        let err = fixture
            .authority
            .approve_confirmed_preview(stripped_preview, &preview_a.subject_digest)
            .expect_err("preview with stripped R must be rejected");
        assert_eq!(err.code(), "validation");

        // 4. Preview with duplicated task entry.
        let mut dup_task_preview = preview_a.clone();
        dup_task_preview.tasks.push(preview_a.tasks[0].clone());
        let err = fixture
            .authority
            .approve_confirmed_preview(dup_task_preview, &preview_a.subject_digest)
            .expect_err("preview with duplicate task_id must be rejected");
        assert_eq!(err.code(), "validation");

        // 5. Tampered OperatorApprovalRequestV2 changing R to another valid subset of A.
        let mut tampered_req_v2 = preview_a.to_request_v2().unwrap();
        tampered_req_v2
            .task_required_changed_paths
            .insert("task-a".to_string(), vec!["src/extra.rs".to_string()]);
        let err = fixture
            .authority
            .approve_confirmed_v2(tampered_req_v2, &preview_a.subject_digest)
            .expect_err("tampered request v2 R must be rejected before commit");
        assert_eq!(err.code(), "validation");

        // 6. Legacy OperatorApprovalRequest omitting R submitted with R-bound subject digest.
        let err = fixture
            .authority
            .approve_confirmed(preview_a.request.clone(), &preview_a.subject_digest)
            .expect_err("legacy request dropping R must be rejected");
        assert_eq!(err.code(), "validation");

        assert!(
            !fixture.authority.approvals_path().exists(),
            "all tampered commit attempts must leave zero approvals on disk"
        );

        // Genuine untampered preview commits cleanly with exact R.
        let committed = fixture
            .authority
            .approve_confirmed_preview(preview_a.clone(), &preview_a.subject_digest)
            .expect("genuine preview_a commits cleanly");
        assert_eq!(committed.subject_digest, preview_a.subject_digest);
        let persisted = fixture
            .authority
            .get_for_operator(&committed.approval_id)
            .expect("load committed approval");
        assert_eq!(
            persisted.tasks[0].required_changed_paths,
            Some(vec!["src/lib.rs".to_string()])
        );
        assert_eq!(
            persisted.tasks[0].allowed_write_paths,
            vec!["src/lib.rs".to_string(), "src/extra.rs".to_string()]
        );
    }
}
