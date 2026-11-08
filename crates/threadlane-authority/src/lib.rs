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
    NativeRoute, OperatorApprovalIntent, OperatorApprovalPreview, OperatorApprovalRequest,
    APPROVALS_FILE_NAME, APPROVALS_LOCK_FILE_NAME, AUTHORITY_POLICY_VERSION,
    AUTHORITY_SCHEMA_VERSION, MAX_APPROVALS_IN_STORE, MAX_APPROVAL_MAX_PARALLEL,
    MAX_APPROVED_TASKS, MAX_APPROVED_WRITE_PATHS, MAX_AUTHORITY_STORE_BYTES,
    MAX_EXECUTABLE_PROFILES_PER_APPROVAL, MIN_APPROVAL_MAX_PARALLEL,
};
pub use validation::{
    compute_authority_core_digest, compute_record_subject_digest, derive_approval_id,
    derive_command_key, derive_profile_id, is_task_scope_contained_in_operator_allowlist,
    resolve_and_validate_worker_executable, validate_canonical_thread_uuid,
    validate_repository_and_base_commit, validate_write_scope_entry,
    validate_write_scope_entry_at_base_commit, verify_approval_record_integrity,
};

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
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
    use threadlane_registry::{RegistryChange, RegistryStore};
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
}
