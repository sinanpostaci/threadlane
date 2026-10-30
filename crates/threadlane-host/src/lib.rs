#![forbid(unsafe_code)]
#![deny(missing_docs)]

//! `threadlane-host`: separately owned per-approval local execution host, durable session
//! manifest, and authoritative caller-scoped task dispatch client for Threadlane Milestone 4.
//!
//! ## Core Architecture & Security Invariants
//!
//! - **One separately owned local host per operator-approved immutable plan**: Each running
//!   [`ApprovedHost`] owns a private `<sessions_dir>/<approval_id>` directory protected by an
//!   exclusive OS-level [`HostSessionLock`] (`host.lock`) held for the entire host lifetime,
//!   along with a private `runtime-state` directory and `work-area` directory. Closing or
//!   dropping a desktop GUI or [`ApprovedHostClient`] never drops the host's supervisor handle or
//!   stops in-flight jobs.
//! - **Two-phase bootstrap & persisted single-binding recovery**: Because `threadlane`
//!   supervisors accept `WorkersConfig` at startup, [`ApprovedHost::start`] first starts a
//!   temporary bootstrap supervisor with `workers: None` (ensuring no worker can start during
//!   bootstrap and marking any prior in-flight runs `Unknown`), creates or recovers the **single**
//!   runtime session binding matching the approval's canonical `thread_uuid`, shuts down the
//!   bootstrap supervisor cleanly, calls [`threadlane_authority::AuthorityStore::materialize`]
//!   against that binding and private work area, starts the final supervisor with the frozen
//!   `WorkersConfig`, and commits [`HostPhase::Ready`] to `host-manifest.json` only after
//!   authenticated supervisor readiness succeeds.
//! - **Authoritative caller scope & root-only deterministic dispatch**: [`ApprovedHostClient`]
//!   is constructed from trusted stores and the caller's authoritative `control_binding_id` and
//!   `native_thread_uuid`. Every operation resolves the persisted [`ApprovalRecord`] and verifies
//!   both `binding_id` and `thread_uuid` before accessing the session directory.
//!   [`ApprovedHostClient::dispatch_task`] accepts only `(approval_id, task_id)`—no caller
//!   instruction, model, effort, executable, write scope, or idempotency key override is
//!   permitted. Dependent tasks (`!depends_on.is_empty()`) are rejected with
//!   [`HostError::DependencyInputsUnverified`], and manual `codex_cli` routes are rejected with
//!   [`HostError::NativeRouteRequired`].
//! - **ReadyForReview completion boundary**: Completed worker runs map to
//!   [`TaskExecutionStatus::ReadyForReview`] (`accepted: false`) and never automatically accept,
//!   merge, push, clean up, or mutate plan status to `Received` or `Applied`.

/// Command-line parser and `serve` runner for the `threadlane-host` binary.
pub mod cli;
/// Authoritative caller-scoped client (`ApprovedHostClient`) for task dispatch, inspection, and cancellation.
pub mod client;
/// Structured domain, security, scope, and runtime error types (`HostError`).
pub mod error;
/// Per-approval host lifecycle (`ApprovedHost`, `ApprovedHostConfig`, `ApprovedHostHandle`).
pub mod host;
/// Private `0700`/`0600` filesystem validation, atomic manifest/receipt I/O, and `host.lock` management.
pub mod storage;
/// Serializable manifest, summary, receipt, and task state snapshot types.
pub mod types;

pub use cli::{run_cli, run_serve, HostCli, HostCommand, ServeArgs};
pub use client::ApprovedHostClient;
pub use error::HostError;
pub use host::{ApprovedHost, ApprovedHostConfig, ApprovedHostHandle, DEFAULT_HOST_TICK_INTERVAL};
pub use storage::{
    derive_ack_command_key, derive_cancel_command_key, ensure_no_symlink_components,
    ensure_private_dir, is_host_lock_held, paths_overlap, prepare_session_dir,
    read_and_verify_host_manifest, read_host_manifest_optional, read_task_dispatch_intent,
    read_task_receipt, record_task_dispatch_intent, record_task_receipt, validate_approval_id,
    validate_canonical_thread_uuid, validate_hex_digest, validate_identifier,
    validate_sessions_root, verify_existing_private_dir, verify_existing_session_dir,
    write_host_manifest, HostSessionLock,
};
pub use threadlane::{DEFAULT_CODEX_TIMEOUT_MS, MAX_CODEX_TIMEOUT_MS};
pub use threadlane_core::{AckResult, EventBatch};
pub use types::{
    ApprovedHostNotificationConfig, HostExecutionGates, HostNotificationSummary, HostPhase,
    HostSessionManifest, HostSessionSummary, TaskDispatchIntent, TaskExecutionStatus,
    TaskRunReceipt, TaskStateSnapshot, APPROVED_EVENT_BATCH_MAX_BYTES,
    APPROVED_EVENT_BATCH_MAX_EVENTS, HOST_LOCK_FILENAME, HOST_MANIFEST_FILENAME,
    HOST_MANIFEST_SCHEMA_VERSION, MAX_HOST_MANIFEST_BYTES, MAX_TASK_RECEIPTS_BYTES,
    RUNTIME_STATE_SUBDIR, TASK_RECEIPTS_FILENAME, TASK_RECEIPTS_LOCK_FILENAME,
    TASK_RECEIPTS_SCHEMA_VERSION, WORK_AREA_SUBDIR,
};

#[cfg(test)]
mod tests {
    use std::fs::{self, File};
    use std::io::Write;
    use std::os::unix::fs::{symlink, PermissionsExt};
    use std::path::{Path, PathBuf};
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use clap::Parser;
    use tempfile::TempDir;
    use threadlane_authority::{AuthorityStore, OperatorApprovalRequest};
    use threadlane_plans::{
        compile_plan_revision, Difficulty, ModelChoice, ModelDefinition, PlanChange, PlanStatus,
        PlanStore, PlanTask, RoleAssignment, ToolAdapter, ToolDefinition, WorkflowProfile,
        WorkflowRole,
    };
    use threadlane_registry::{RegistryChange, RegistryStore};
    use threadlane_workers::{
        resolve_exact_commit_sha, run_sanitized_git, ExecutionBudgets,
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    };

    use super::*;

    const THREAD_UUID_A: &str = "550e8400-e29b-41d4-a716-446655440000";
    const THREAD_UUID_B: &str = "6ba7b810-9dad-11d1-80b4-00c04fd430c8";
    const BINDING_A: &str = "bnd_control_alpha";
    const BINDING_B: &str = "bnd_control_beta";

    struct HostTestFixture {
        _tmp: TempDir,
        root: PathBuf,
        repo_dir: PathBuf,
        base_commit: String,
        registry_dir: PathBuf,
        plans_dir: PathBuf,
        authority_dir: PathBuf,
        sessions_dir: PathBuf,
        fake_agy_exe: PathBuf,
        fake_oc_exe: PathBuf,
        agy_count_file: PathBuf,
        agy_prompt_file: PathBuf,
        oc_count_file: PathBuf,
        oc_prompt_file: PathBuf,
        registry: Arc<RegistryStore>,
        plans: Arc<PlanStore>,
        authority: Arc<AuthorityStore>,
    }

    fn write_fake_agy_script(script_path: &Path, count_file: &Path, prompt_file: &Path) {
        if let Some(parent) = script_path.parent() {
            fs::create_dir_all(parent).expect("create script parent dir");
        }
        let script = format!(
            r#"#!/bin/sh
set -eu
COUNT_FILE="{count_file}"
PROMPT_FILE="{prompt_file}"
c=0
if [ -f "$COUNT_FILE" ]; then
    c=$(cat "$COUNT_FILE")
fi
c=$((c + 1))
printf "%s" "$c" > "$COUNT_FILE"

MODEL=""
EFFORT=""
PROMPT=""
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
        --print)
            PROMPT="$2"
            shift 2
            ;;
        *)
            shift 1
            ;;
    esac
done

printf "%s" "$PROMPT" > "$PROMPT_FILE"
printf '{{"type":"init","session_id":"sess-agy-1","model":"%s","effort":"%s"}}\n' "$MODEL" "$EFFORT"

case "$PROMPT" in
    *SLOW_WAIT_FOR_CANCEL*)
        trap 'exit 0' TERM INT
        i=0
        while [ "$i" -lt 200 ]; do
            sleep 0.05
            i=$((i + 1))
        done
        ;;
    *SHORT_DELAY_FOR_CLIENT_DROP*)
        sleep 0.15
        ;;
esac

if [ -d "src" ]; then
    printf "// updated by fake-agy\npub fn alpha() -> u32 {{ 42 }}\n" > "src/lib.rs"
fi

printf '{{"type":"result","subtype":"success","session_id":"sess-agy-1","model":"%s","effort":"%s"}}\n' "$MODEL" "$EFFORT"
exit 0
"#,
            count_file = count_file.display(),
            prompt_file = prompt_file.display(),
        );

        let mut f = File::create(script_path).expect("create fake-agy");
        f.write_all(script.as_bytes()).expect("write fake-agy");
        f.sync_all().expect("sync fake-agy");
        fs::set_permissions(script_path, fs::Permissions::from_mode(0o755))
            .expect("chmod 0755 fake-agy");
    }

    fn write_fake_oc_script(script_path: &Path, count_file: &Path, prompt_file: &Path) {
        if let Some(parent) = script_path.parent() {
            fs::create_dir_all(parent).expect("create script parent dir");
        }
        let script = format!(
            r#"#!/bin/sh
set -eu
COUNT_FILE="{count_file}"
PROMPT_FILE="{prompt_file}"
c=0
if [ -f "$COUNT_FILE" ]; then
    c=$(cat "$COUNT_FILE")
fi
c=$((c + 1))
printf "%s" "$c" > "$COUNT_FILE"

PROMPT=""
while [ "$#" -gt 0 ]; do
    case "$1" in
        --)
            shift 1
            PROMPT="${{1:-}}"
            break
            ;;
        *)
            shift 1
            ;;
    esac
done

printf "%s" "$PROMPT" > "$PROMPT_FILE"
printf '{{"type":"init","session_id":"sess-oc-1","model":"opencode-go/deepseek-v4.1-flash","effort":"max"}}\n'

if [ -d "src" ]; then
    printf "// updated by fake-oc\npub fn extra() {{}}\n" > "src/extra.rs"
fi

printf '{{"type":"result","subtype":"success","session_id":"sess-oc-1","model":"opencode-go/deepseek-v4.1-flash","effort":"max"}}\n'
exit 0
"#,
            count_file = count_file.display(),
            prompt_file = prompt_file.display(),
        );

        let mut f = File::create(script_path).expect("create fake-oc");
        f.write_all(script.as_bytes()).expect("write fake-oc");
        f.sync_all().expect("sync fake-oc");
        fs::set_permissions(script_path, fs::Permissions::from_mode(0o755))
            .expect("chmod 0755 fake-oc");
    }

    fn init_disposable_git_repo(repo_dir: &Path) -> String {
        fs::create_dir_all(repo_dir.join("src")).expect("create repo src");
        fs::write(repo_dir.join("src/lib.rs"), "pub fn alpha() -> u32 { 1 }\n")
            .expect("write src/lib.rs");
        fs::write(repo_dir.join("src/extra.rs"), "pub fn extra() {}\n")
            .expect("write src/extra.rs");

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
        .expect("git commit");

        resolve_exact_commit_sha(repo_dir, "HEAD", DEFAULT_GIT_COMMAND_TIMEOUT_MS)
            .expect("resolve HEAD")
    }

    fn setup_fixture() -> HostTestFixture {
        let tmp = TempDir::new().expect("tempdir");
        let root = fs::canonicalize(tmp.path()).expect("canonicalize temp root");

        let repo_dir = root.join("repo");
        let base_commit = init_disposable_git_repo(&repo_dir);

        let bin_dir = root.join("bin");
        let fake_agy_exe = bin_dir.join("fake-agy");
        let fake_oc_exe = bin_dir.join("fake-opencode");
        let agy_count_file = root.join("agy-invocations.txt");
        let agy_prompt_file = root.join("agy-last-prompt.txt");
        let oc_count_file = root.join("oc-invocations.txt");
        let oc_prompt_file = root.join("oc-last-prompt.txt");

        write_fake_agy_script(&fake_agy_exe, &agy_count_file, &agy_prompt_file);
        write_fake_oc_script(&fake_oc_exe, &oc_count_file, &oc_prompt_file);

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
                        ],
                    },
                },
            )
            .expect("upsert profile");

        let authority = Arc::new(
            AuthorityStore::open(&authority_dir, Arc::clone(&registry), Arc::clone(&plans))
                .expect("open authority"),
        );

        HostTestFixture {
            _tmp: tmp,
            root,
            repo_dir,
            base_commit,
            registry_dir,
            plans_dir,
            authority_dir,
            sessions_dir,
            fake_agy_exe,
            fake_oc_exe,
            agy_count_file,
            agy_prompt_file,
            oc_count_file,
            oc_prompt_file,
            registry,
            plans,
            authority,
        }
    }

    fn default_tasks(agy_instruction: &str) -> Vec<PlanTask> {
        vec![
            PlanTask {
                id: "task-root-agy".to_string(),
                title: "Root AGY implementation".to_string(),
                instruction: agy_instruction.to_string(),
                role: WorkflowRole::Coder,
                difficulty: Difficulty::Hard,
                depends_on: Vec::new(),
                allowed_write_paths: vec!["src/lib.rs".to_string()],
            },
            PlanTask {
                id: "task-root-oc".to_string(),
                title: "Root OpenCode helper".to_string(),
                instruction: "Implement extra helper in src/extra.rs".to_string(),
                role: WorkflowRole::Coder,
                difficulty: Difficulty::Easy,
                depends_on: Vec::new(),
                allowed_write_paths: vec!["src/extra.rs".to_string()],
            },
            PlanTask {
                id: "task-dep-agy".to_string(),
                title: "Dependent AGY task".to_string(),
                instruction: "Follow up after task-root-agy".to_string(),
                role: WorkflowRole::Coder,
                difficulty: Difficulty::Hard,
                depends_on: vec!["task-root-agy".to_string()],
                allowed_write_paths: vec!["src/lib.rs".to_string()],
            },
            PlanTask {
                id: "task-native-codex".to_string(),
                title: "Manual Codex conversation task".to_string(),
                instruction: "Coordinate with operator in native Codex chat".to_string(),
                role: WorkflowRole::Follower,
                difficulty: Difficulty::Medium,
                depends_on: Vec::new(),
                allowed_write_paths: vec!["src/lib.rs".to_string()],
            },
        ]
    }

    fn create_approved_plan(
        fixture: &HostTestFixture,
        plan_id: &str,
        binding_id: &str,
        thread_uuid: &str,
        tasks: Vec<PlanTask>,
    ) -> String {
        let draft = fixture
            .plans
            .change_at(
                0,
                PlanChange::SaveDraft {
                    plan_id: plan_id.to_string(),
                    binding_id: binding_id.to_string(),
                    profile_id: "profile-main".to_string(),
                    profile_revision: 5,
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

        let reg_snap = fixture.registry.snapshot().expect("registry snapshot");
        let compiled =
            compile_plan_revision(&published, &reg_snap).expect("compile published plan");

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
                    base_commit: fixture.base_commit.clone(),
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

    async fn wait_for_task_status(
        client: &ApprovedHostClient,
        approval_id: &str,
        task_id: &str,
        predicate: impl Fn(TaskExecutionStatus) -> bool,
    ) -> TaskStateSnapshot {
        let deadline = Instant::now() + Duration::from_secs(8);
        loop {
            let snap = client
                .get_task(approval_id, task_id)
                .await
                .expect("get_task during wait");
            if predicate(snap.status) {
                return snap;
            }
            assert!(
                Instant::now() < deadline,
                "timed out waiting for task `{task_id}` status; last snapshot: {snap:?}"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    #[test]
    fn cli_serve_requires_explicit_flags_and_rejects_unknown_overrides() {
        let parsed = HostCli::try_parse_from([
            "threadlane-host",
            "serve",
            "--registry-dir",
            "/tmp/reg",
            "--plans-dir",
            "/tmp/plans",
            "--authority-dir",
            "/tmp/auth",
            "--sessions-dir",
            "/tmp/sess",
            "--approval-id",
            "apr_0123456789abcdef0123456789abcdef",
        ])
        .expect("parse valid serve args");

        match parsed.command {
            HostCommand::Serve(args) => {
                assert_eq!(args.registry_dir, PathBuf::from("/tmp/reg"));
                assert_eq!(args.plans_dir, PathBuf::from("/tmp/plans"));
                assert_eq!(args.authority_dir, PathBuf::from("/tmp/auth"));
                assert_eq!(args.sessions_dir, PathBuf::from("/tmp/sess"));
                assert_eq!(args.approval_id, "apr_0123456789abcdef0123456789abcdef");
            }
        }

        // Missing required flag (--approval-id) must be rejected.
        assert!(HostCli::try_parse_from([
            "threadlane-host",
            "serve",
            "--registry-dir",
            "/tmp/reg",
            "--plans-dir",
            "/tmp/plans",
            "--authority-dir",
            "/tmp/auth",
            "--sessions-dir",
            "/tmp/sess",
        ])
        .is_err());

        // Arbitrary model, prompt, or config override flags must be rejected.
        for forbidden_flag in ["--model", "--instruction", "--config", "--auto"] {
            assert!(HostCli::try_parse_from([
                "threadlane-host",
                "serve",
                "--registry-dir",
                "/tmp/reg",
                "--plans-dir",
                "/tmp/plans",
                "--authority-dir",
                "/tmp/auth",
                "--sessions-dir",
                "/tmp/sess",
                "--approval-id",
                "apr_0123456789abcdef0123456789abcdef",
                forbidden_flag,
                "override",
            ])
            .is_err());
        }
    }

    #[tokio::test]
    async fn private_permissions_symlinks_foreign_files_overlap_and_corruption_refused() {
        let fixture = setup_fixture();
        let approval_id = create_approved_plan(
            &fixture,
            "plan-sec",
            BINDING_A,
            THREAD_UUID_A,
            default_tasks("Implement security test"),
        );

        // 1. Overlapping sessions_dir with authority_dir or repository_path is refused.
        let overlap_err = ApprovedHost::start(ApprovedHostConfig::new(
            &fixture.registry_dir,
            &fixture.plans_dir,
            &fixture.authority_dir,
            fixture.authority_dir.join("nested-sessions"),
            &approval_id,
        ))
        .await
        .expect_err("overlapping authority_dir must fail");
        assert_eq!(overlap_err.code(), "security_violation");

        let repo_overlap_err = ApprovedHost::start(ApprovedHostConfig::new(
            &fixture.registry_dir,
            &fixture.plans_dir,
            &fixture.authority_dir,
            fixture.repo_dir.join("sessions"),
            &approval_id,
        ))
        .await
        .expect_err("overlapping repository_path must fail");
        assert_eq!(repo_overlap_err.code(), "security_violation");

        // 2. Symlink sessions_dir is refused.
        let real_alt = fixture.root.join("real-alt-sessions");
        fs::create_dir_all(&real_alt).expect("create real-alt");
        fs::set_permissions(&real_alt, fs::Permissions::from_mode(0o700)).expect("chmod 0700");
        let symlink_sessions = fixture.root.join("symlink-sessions");
        symlink(&real_alt, &symlink_sessions).expect("create symlink sessions_dir");

        let sym_err = ApprovedHost::start(ApprovedHostConfig::new(
            &fixture.registry_dir,
            &fixture.plans_dir,
            &fixture.authority_dir,
            &symlink_sessions,
            &approval_id,
        ))
        .await
        .expect_err("symlink sessions_dir must fail");
        assert_eq!(sym_err.code(), "security_violation");

        // 3. Foreign file directly inside sessions_dir is refused.
        ensure_private_dir(&fixture.sessions_dir).expect("create private sessions_dir");
        let foreign_root_file = fixture.sessions_dir.join("unexpected.txt");
        fs::write(&foreign_root_file, "foreign").expect("write foreign file");
        let foreign_root_err = ApprovedHost::start(ApprovedHostConfig::new(
            &fixture.registry_dir,
            &fixture.plans_dir,
            &fixture.authority_dir,
            &fixture.sessions_dir,
            &approval_id,
        ))
        .await
        .expect_err("foreign file in sessions_dir must fail");
        assert_eq!(foreign_root_err.code(), "security_violation");
        fs::remove_file(&foreign_root_file).expect("remove foreign root file");

        // 4. Pre-existing non-empty session directory without host.lock/host-manifest.json is refused.
        let session_dir = fixture.sessions_dir.join(&approval_id);
        ensure_private_dir(&session_dir).expect("create session_dir");
        ensure_private_dir(&session_dir.join(RUNTIME_STATE_SUBDIR))
            .expect("create premature runtime-state");
        let premature_err = ApprovedHost::start(ApprovedHostConfig::new(
            &fixture.registry_dir,
            &fixture.plans_dir,
            &fixture.authority_dir,
            &fixture.sessions_dir,
            &approval_id,
        ))
        .await
        .expect_err("premature non-empty session dir must fail");
        assert_eq!(premature_err.code(), "security_violation");
        fs::remove_dir_all(&session_dir).expect("clean premature session dir");

        // 5. Normal start creates 0700 directories and 0600 files, and never leaks tokens/paths in summary/Debug.
        let handle = ApprovedHost::start(
            ApprovedHostConfig::new(
                &fixture.registry_dir,
                &fixture.plans_dir,
                &fixture.authority_dir,
                &fixture.sessions_dir,
                &approval_id,
            )
            .with_tick_interval(Duration::from_millis(20)),
        )
        .await
        .expect("start valid host");

        let sess_mode = fs::symlink_metadata(&session_dir)
            .expect("stat session_dir")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(sess_mode, 0o700);

        let manifest_path = session_dir.join(HOST_MANIFEST_FILENAME);
        let manifest_mode = fs::symlink_metadata(&manifest_path)
            .expect("stat host-manifest.json")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(manifest_mode, 0o600);

        let lock_path = session_dir.join(HOST_LOCK_FILENAME);
        let lock_mode = fs::symlink_metadata(&lock_path)
            .expect("stat host.lock")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(lock_mode, 0o600);

        let summary_json =
            serde_json::to_string(&handle.summary()).expect("serialize HostSessionSummary");
        assert!(!summary_json.contains("tl_"));
        assert!(!summary_json.contains(&fixture.root.display().to_string()));
        let handle_debug = format!("{handle:?}");
        assert!(!handle_debug.contains("tl_"));
        assert!(!handle_debug.contains(&fixture.root.display().to_string()));

        let client = ApprovedHostClient::open(
            &fixture.registry_dir,
            &fixture.plans_dir,
            &fixture.authority_dir,
            &fixture.sessions_dir,
            BINDING_A,
            THREAD_UUID_A,
        )
        .expect("open client");
        let client_debug = format!("{client:?}");
        assert!(!client_debug.contains("tl_"));
        assert!(!client_debug.contains(&fixture.root.display().to_string()));

        handle.shutdown().await.expect("graceful shutdown");

        // 6. Corrupt host-manifest.json is preserved untouched and rejected with corrupt_manifest.
        let corrupt_bytes = b"{corrupt-json-manifest";
        fs::write(&manifest_path, corrupt_bytes).expect("write corrupt manifest");
        fs::set_permissions(&manifest_path, fs::Permissions::from_mode(0o600))
            .expect("chmod 0600 corrupt manifest");

        let corrupt_err = ApprovedHost::start(ApprovedHostConfig::new(
            &fixture.registry_dir,
            &fixture.plans_dir,
            &fixture.authority_dir,
            &fixture.sessions_dir,
            &approval_id,
        ))
        .await
        .expect_err("corrupt manifest must fail closed");
        assert_eq!(corrupt_err.code(), "corrupt_manifest");
        assert_eq!(
            fs::read(&manifest_path).expect("read preserved corrupt manifest"),
            corrupt_bytes
        );
    }

    #[tokio::test]
    async fn second_host_for_same_approval_fails_closed_with_lock_busy() {
        let fixture = setup_fixture();
        let approval_id = create_approved_plan(
            &fixture,
            "plan-lock",
            BINDING_A,
            THREAD_UUID_A,
            default_tasks("Lock contention check"),
        );

        let host1 = ApprovedHost::start(
            ApprovedHostConfig::new(
                &fixture.registry_dir,
                &fixture.plans_dir,
                &fixture.authority_dir,
                &fixture.sessions_dir,
                &approval_id,
            )
            .with_tick_interval(Duration::from_millis(20)),
        )
        .await
        .expect("start host 1");

        assert!(is_host_lock_held(host1.session_dir()).expect("check host lock held"));

        let err2 = ApprovedHost::start(ApprovedHostConfig::new(
            &fixture.registry_dir,
            &fixture.plans_dir,
            &fixture.authority_dir,
            &fixture.sessions_dir,
            &approval_id,
        ))
        .await
        .expect_err("second host for same approval must fail with lock_busy");
        assert_eq!(err2.code(), "lock_busy");

        // Host 1 remains ready and usable after contended startup attempt.
        let client = ApprovedHostClient::open(
            &fixture.registry_dir,
            &fixture.plans_dir,
            &fixture.authority_dir,
            &fixture.sessions_dir,
            BINDING_A,
            THREAD_UUID_A,
        )
        .expect("open client");
        let summary = client
            .session_summary(&approval_id)
            .expect("session summary");
        assert_eq!(summary.phase, HostPhase::Ready);
        assert!(summary.gates.supervisor_authenticated_ready);

        host1.shutdown().await.expect("shutdown host 1");
        assert!(!is_host_lock_held(&fixture.sessions_dir.join(&approval_id))
            .expect("lock released after shutdown"));
    }

    #[tokio::test]
    async fn no_auto_start_registry_mutations_ignored_draft_allowed_superseded_blocked() {
        let fixture = setup_fixture();
        let frozen_prompt = "Frozen instruction from approved revision 2";
        let approval_id = create_approved_plan(
            &fixture,
            "plan-freeze",
            BINDING_A,
            THREAD_UUID_A,
            default_tasks(frozen_prompt),
        );

        let host = ApprovedHost::start(
            ApprovedHostConfig::new(
                &fixture.registry_dir,
                &fixture.plans_dir,
                &fixture.authority_dir,
                &fixture.sessions_dir,
                &approval_id,
            )
            .with_tick_interval(Duration::from_millis(20)),
        )
        .await
        .expect("start host");

        // No provider starts automatically on host startup.
        assert_eq!(read_invocation_count(&fixture.agy_count_file), 0);
        assert_eq!(read_invocation_count(&fixture.oc_count_file), 0);
        assert!(!host.summary().gates.workers_auto_started_on_boot);

        let frozen_revision = host.approval().plan_revision;

        // Mutate RegistryStore after approval to point tool-agy to a broken path and different model.
        let bogus_exe = fixture.root.join("bin/nonexistent-agy");
        let cur_reg_rev = fixture.registry.snapshot().expect("reg snap").revision;
        fixture
            .registry
            .apply(
                cur_reg_rev,
                RegistryChange::UpsertTool {
                    tool: ToolDefinition {
                        id: "tool-agy".to_string(),
                        label: "Mutated AGY".to_string(),
                        adapter: ToolAdapter::Antigravity,
                        executable_path: Some(bogus_exe),
                        enabled: true,
                    },
                },
            )
            .expect("mutate registry tool-agy");

        // Save a newer unpublished Draft revision on the same plan: must NOT cancel the published approval.
        let latest_plan = fixture
            .plans
            .latest_revision("plan-freeze")
            .expect("get plan")
            .expect("plan exists");
        let draft_rev = fixture
            .plans
            .change(
                latest_plan.revision,
                PlanChange::SaveDraft {
                    plan_id: "plan-freeze".to_string(),
                    binding_id: BINDING_A.to_string(),
                    profile_id: "profile-main".to_string(),
                    profile_revision: 5,
                    summary: "Newer draft revision".to_string(),
                    tasks: default_tasks("Mutated draft instruction"),
                },
            )
            .expect("save newer draft");
        assert_eq!(draft_rev.status, PlanStatus::Draft);

        let client = ApprovedHostClient::open(
            &fixture.registry_dir,
            &fixture.plans_dir,
            &fixture.authority_dir,
            &fixture.sessions_dir,
            BINDING_A,
            THREAD_UUID_A,
        )
        .expect("open client");

        // Dispatch uses the frozen executable, model, and instruction despite registry mutation and newer Draft.
        let dispatched = client
            .dispatch_task(&approval_id, "task-root-agy")
            .await
            .expect("dispatch task-root-agy");
        assert!(dispatched.run_id.is_some());
        assert!(!dispatched.accepted);

        let completed = wait_for_task_status(&client, &approval_id, "task-root-agy", |st| {
            st == TaskExecutionStatus::ReadyForReview
        })
        .await;
        assert_eq!(completed.status, TaskExecutionStatus::ReadyForReview);
        assert!(!completed.accepted);
        assert_eq!(read_invocation_count(&fixture.agy_count_file), 1);
        assert_eq!(
            fs::read_to_string(&fixture.agy_prompt_file).expect("read captured prompt"),
            frozen_prompt
        );

        // The frozen source revision stays Published while a newer draft exists.
        let approved_rev = fixture
            .plans
            .get_revision("plan-freeze", frozen_revision)
            .expect("get frozen revision")
            .expect("frozen revision exists");
        assert_eq!(approved_rev.status, PlanStatus::Published);

        // Main repository checkout is untouched (no auto-merge/apply).
        assert_eq!(
            fs::read_to_string(fixture.repo_dir.join("src/lib.rs")).expect("read main repo lib.rs"),
            "pub fn alpha() -> u32 { 1 }\n"
        );

        // Publishing the newer draft supersedes the frozen source revision.
        fixture
            .plans
            .change(
                draft_rev.revision,
                PlanChange::Publish {
                    plan_id: "plan-freeze".to_string(),
                    revision: draft_rev.revision,
                },
            )
            .expect("publish newer revision");
        let superseded_rev = fixture
            .plans
            .get_revision("plan-freeze", frozen_revision)
            .expect("get frozen revision")
            .expect("frozen revision exists");
        assert_eq!(superseded_rev.status, PlanStatus::Superseded);

        // Dispatching another task on the superseded plan revision is blocked.
        let sup_dispatch_err = client
            .dispatch_task(&approval_id, "task-root-oc")
            .await
            .expect_err("superseded plan must reject new dispatch/materialization");
        assert_eq!(sup_dispatch_err.code(), "unpublished_or_superseded_plan");

        host.shutdown().await.expect("shutdown host");

        // Starting a host for a superseded plan revision is also blocked.
        let sup_start_err = ApprovedHost::start(ApprovedHostConfig::new(
            &fixture.registry_dir,
            &fixture.plans_dir,
            &fixture.authority_dir,
            &fixture.sessions_dir,
            &approval_id,
        ))
        .await
        .expect_err("superseded plan must reject host startup");
        assert_eq!(sup_start_err.code(), "unpublished_or_superseded_plan");
    }

    #[tokio::test]
    async fn deterministic_dispatch_idempotent_replay_and_receipt_recovery() {
        let fixture = setup_fixture();
        let approval_id = create_approved_plan(
            &fixture,
            "plan-dedup",
            BINDING_A,
            THREAD_UUID_A,
            default_tasks("Deterministic idempotent instruction"),
        );

        let host = ApprovedHost::start(
            ApprovedHostConfig::new(
                &fixture.registry_dir,
                &fixture.plans_dir,
                &fixture.authority_dir,
                &fixture.sessions_dir,
                &approval_id,
            )
            .with_tick_interval(Duration::from_millis(20)),
        )
        .await
        .expect("start host");

        let client = ApprovedHostClient::open(
            &fixture.registry_dir,
            &fixture.plans_dir,
            &fixture.authority_dir,
            &fixture.sessions_dir,
            BINDING_A,
            THREAD_UUID_A,
        )
        .expect("open client");

        let first = client
            .dispatch_task(&approval_id, "task-root-agy")
            .await
            .expect("first dispatch");
        let completed = wait_for_task_status(&client, &approval_id, "task-root-agy", |st| {
            st == TaskExecutionStatus::ReadyForReview
        })
        .await;
        assert_eq!(first.run_id, completed.run_id);
        assert_eq!(first.worker_id, completed.worker_id);
        assert_eq!(read_invocation_count(&fixture.agy_count_file), 1);

        // Retrying dispatch_task with the same (approval_id, task_id) replays the exact same
        // deterministic command_key and returns the same run_id/worker_id without spawning again.
        let second = client
            .dispatch_task(&approval_id, "task-root-agy")
            .await
            .expect("second idempotent dispatch");
        assert_eq!(second.run_id, first.run_id);
        assert_eq!(second.worker_id, first.worker_id);
        assert_eq!(second.status, TaskExecutionStatus::ReadyForReview);
        assert_eq!(read_invocation_count(&fixture.agy_count_file), 1);

        // Simulate losing the sealed receipt after the pre-dispatch intent and start_worker
        // succeeded: with the durable intent retained in task-receipts.json, get_task truthfully
        // reports Unknown (not NotStarted), cancel_task refuses ambiguous cancellation (distinct
        // from not_started), and an explicit dispatch_task retry recovers the exact same run_id
        // from runtime command deduplication without respawning.
        let receipts_path = host.session_dir().join(TASK_RECEIPTS_FILENAME);
        let retained_intent = read_task_dispatch_intent(
            host.session_dir(),
            host.approval(),
            host.session_binding_id(),
            "task-root-agy",
        )
        .expect("read dispatch intent")
        .expect("dispatch intent recorded before start_worker");
        let intent_only_ledger = serde_json::json!({
            "schema_version": TASK_RECEIPTS_SCHEMA_VERSION,
            "approval_id": host.approval_id(),
            "subject_digest": host.approval().subject_digest,
            "session_binding_id": host.session_binding_id(),
            "pending_intents": [retained_intent],
            "receipts": [],
        });
        fs::write(
            &receipts_path,
            serde_json::to_vec_pretty(&intent_only_ledger).expect("serialize intent-only ledger"),
        )
        .expect("write intent-only task-receipts.json to simulate unsealed receipt");
        fs::set_permissions(&receipts_path, fs::Permissions::from_mode(0o600))
            .expect("chmod 0600 intent-only receipts");

        let lost_receipt_snap = client
            .get_task(&approval_id, "task-root-agy")
            .await
            .expect("get_task with retained intent and missing receipt");
        assert_eq!(lost_receipt_snap.status, TaskExecutionStatus::Unknown);
        assert!(lost_receipt_snap.run_id.is_none());
        assert!(lost_receipt_snap.worker_id.is_none());
        assert_eq!(read_invocation_count(&fixture.agy_count_file), 1);

        let ambiguous_cancel_err = client
            .cancel_task(&approval_id, "task-root-agy", 1, None)
            .await
            .expect_err("cancel without sealed receipt must fail as ambiguous/unknown");
        assert_eq!(ambiguous_cancel_err.code(), "invalid_run_state");
        assert_ne!(ambiguous_cancel_err.code(), "not_started");

        let recovered = client
            .dispatch_task(&approval_id, "task-root-agy")
            .await
            .expect("recover dispatch via deterministic command_key replay");
        assert_eq!(recovered.run_id, first.run_id);
        assert_eq!(recovered.worker_id, first.worker_id);
        assert_eq!(recovered.status, TaskExecutionStatus::ReadyForReview);
        assert_eq!(read_invocation_count(&fixture.agy_count_file), 1);

        // Corrupting task-receipts.json must fail closed with corrupt_receipts and preserve the file.
        let corrupt_receipt_bytes = b"{broken-receipts-json";
        fs::write(&receipts_path, corrupt_receipt_bytes).expect("write corrupt task-receipts.json");
        fs::set_permissions(&receipts_path, fs::Permissions::from_mode(0o600))
            .expect("chmod 0600 corrupt receipts");
        let receipt_err = client
            .get_task(&approval_id, "task-root-agy")
            .await
            .expect_err("corrupt task-receipts.json must fail closed");
        assert_eq!(receipt_err.code(), "corrupt_receipts");
        assert_eq!(
            fs::read(&receipts_path).expect("read preserved corrupt receipts"),
            corrupt_receipt_bytes
        );

        host.shutdown().await.expect("shutdown host");
    }

    #[tokio::test]
    async fn scope_mismatch_denials_and_dependent_or_native_routes_blocked() {
        let fixture = setup_fixture();
        let approval_id = create_approved_plan(
            &fixture,
            "plan-gates",
            BINDING_A,
            THREAD_UUID_A,
            default_tasks("Root AGY task"),
        );

        let host = ApprovedHost::start(
            ApprovedHostConfig::new(
                &fixture.registry_dir,
                &fixture.plans_dir,
                &fixture.authority_dir,
                &fixture.sessions_dir,
                &approval_id,
            )
            .with_tick_interval(Duration::from_millis(20)),
        )
        .await
        .expect("start host");

        // Wrong control_binding_id is rejected before accessing session.
        let wrong_binding_client = ApprovedHostClient::open(
            &fixture.registry_dir,
            &fixture.plans_dir,
            &fixture.authority_dir,
            &fixture.sessions_dir,
            BINDING_B,
            THREAD_UUID_A,
        )
        .expect("open wrong binding client");
        let err_binding = wrong_binding_client
            .dispatch_task(&approval_id, "task-root-agy")
            .await
            .expect_err("wrong control_binding_id must be denied");
        assert_eq!(err_binding.code(), "scope_mismatch");

        // Wrong native_thread_uuid is rejected before accessing session.
        let wrong_thread_client = ApprovedHostClient::open(
            &fixture.registry_dir,
            &fixture.plans_dir,
            &fixture.authority_dir,
            &fixture.sessions_dir,
            BINDING_A,
            THREAD_UUID_B,
        )
        .expect("open wrong thread client");
        let err_thread = wrong_thread_client
            .get_task(&approval_id, "task-root-agy")
            .await
            .expect_err("wrong native_thread_uuid must be denied");
        assert_eq!(err_thread.code(), "scope_mismatch");

        let client = ApprovedHostClient::open(
            &fixture.registry_dir,
            &fixture.plans_dir,
            &fixture.authority_dir,
            &fixture.sessions_dir,
            BINDING_A,
            THREAD_UUID_A,
        )
        .expect("open valid client");

        // Unknown task ID is rejected with unknown_task.
        let unknown_err = client
            .dispatch_task(&approval_id, "task-does-not-exist")
            .await
            .expect_err("unknown task must fail");
        assert_eq!(unknown_err.code(), "unknown_task");

        // Dependent task is blocked with dependency_inputs_unverified.
        let dep_err = client
            .dispatch_task(&approval_id, "task-dep-agy")
            .await
            .expect_err("dependent task must fail with dependency_inputs_unverified");
        assert_eq!(dep_err.code(), "dependency_inputs_unverified");
        let dep_state = client
            .get_task(&approval_id, "task-dep-agy")
            .await
            .expect("get_task on dependent task");
        assert_eq!(
            dep_state.status,
            TaskExecutionStatus::DependencyInputsUnverified
        );

        // Manual native route (codex_cli) is blocked with native_route_required.
        let native_err = client
            .dispatch_task(&approval_id, "task-native-codex")
            .await
            .expect_err("native route must fail with native_route_required");
        assert_eq!(native_err.code(), "native_route_required");
        let native_state = client
            .get_task(&approval_id, "task-native-codex")
            .await
            .expect("get_task on native route");
        assert_eq!(
            native_state.status,
            TaskExecutionStatus::NativeRouteRequired
        );

        // Unstarted root worker task reports NotStarted on get_task and not_started on cancel/activity.
        let unstarted = client
            .get_task(&approval_id, "task-root-oc")
            .await
            .expect("get_task on unstarted root task");
        assert_eq!(unstarted.status, TaskExecutionStatus::NotStarted);
        assert!(unstarted.run_id.is_none());

        let cancel_unstarted_err = client
            .cancel_task(&approval_id, "task-root-oc", 1, None)
            .await
            .expect_err("cancel unstarted task must fail");
        assert_eq!(cancel_unstarted_err.code(), "not_started");

        let activity_unstarted_err = client
            .list_task_activity(&approval_id, "task-root-oc", None, None, None, None)
            .await
            .expect_err("list_task_activity on unstarted task must fail");
        assert_eq!(activity_unstarted_err.code(), "not_started");

        // Dispatch OpenCodeV2 root task and verify ReadyForReview completion.
        client
            .dispatch_task(&approval_id, "task-root-oc")
            .await
            .expect("dispatch task-root-oc");
        let oc_done = wait_for_task_status(&client, &approval_id, "task-root-oc", |st| {
            st == TaskExecutionStatus::ReadyForReview
        })
        .await;
        assert_eq!(oc_done.status, TaskExecutionStatus::ReadyForReview);
        assert!(!oc_done.accepted);
        assert_eq!(read_invocation_count(&fixture.oc_count_file), 1);
        let frozen_oc = host
            .approval()
            .tasks
            .iter()
            .find(|task| task.task_id == "task-root-oc")
            .expect("frozen OpenCode task");
        assert_eq!(
            frozen_oc.canonical_executable.as_ref(),
            Some(&fs::canonicalize(&fixture.fake_oc_exe).expect("canonical fake executable"))
        );
        assert_eq!(
            fs::read_to_string(&fixture.oc_prompt_file).expect("captured OpenCode instruction"),
            frozen_oc.instruction
        );

        host.shutdown().await.expect("shutdown host");
    }

    #[tokio::test]
    async fn separate_sessions_and_client_drop_leaves_host_running_to_completion() {
        let fixture = setup_fixture();
        let approval_a = create_approved_plan(
            &fixture,
            "plan-session-a",
            BINDING_A,
            THREAD_UUID_A,
            default_tasks("SHORT_DELAY_FOR_CLIENT_DROP session A"),
        );
        let approval_b = create_approved_plan(
            &fixture,
            "plan-session-b",
            BINDING_B,
            THREAD_UUID_B,
            default_tasks("Session B root task"),
        );

        let host_a = ApprovedHost::start(
            ApprovedHostConfig::new(
                &fixture.registry_dir,
                &fixture.plans_dir,
                &fixture.authority_dir,
                &fixture.sessions_dir,
                &approval_a,
            )
            .with_tick_interval(Duration::from_millis(20)),
        )
        .await
        .expect("start host A");

        let host_b = ApprovedHost::start(
            ApprovedHostConfig::new(
                &fixture.registry_dir,
                &fixture.plans_dir,
                &fixture.authority_dir,
                &fixture.sessions_dir,
                &approval_b,
            )
            .with_tick_interval(Duration::from_millis(20)),
        )
        .await
        .expect("start host B");

        assert_ne!(host_a.session_dir(), host_b.session_dir());
        assert_ne!(host_a.runtime_state_dir(), host_b.runtime_state_dir());
        assert_ne!(host_a.work_area_dir(), host_b.work_area_dir());

        // Start task in session A using a short-lived client, then immediately drop the client!
        {
            let ephemeral_client_a = ApprovedHostClient::open(
                &fixture.registry_dir,
                &fixture.plans_dir,
                &fixture.authority_dir,
                &fixture.sessions_dir,
                BINDING_A,
                THREAD_UUID_A,
            )
            .expect("open ephemeral client A");
            ephemeral_client_a
                .dispatch_task(&approval_a, "task-root-agy")
                .await
                .expect("dispatch on ephemeral client A");
        }

        // Host A continues running the job to ReadyForReview after the client was dropped.
        let fresh_client_a = ApprovedHostClient::open(
            &fixture.registry_dir,
            &fixture.plans_dir,
            &fixture.authority_dir,
            &fixture.sessions_dir,
            BINDING_A,
            THREAD_UUID_A,
        )
        .expect("open fresh client A");
        let completed_a =
            wait_for_task_status(&fresh_client_a, &approval_a, "task-root-agy", |st| {
                st == TaskExecutionStatus::ReadyForReview
            })
            .await;
        assert_eq!(completed_a.status, TaskExecutionStatus::ReadyForReview);

        let activity_page = fresh_client_a
            .list_task_activity(&approval_a, "task-root-agy", None, None, Some(10), None)
            .await
            .expect("list_task_activity for completed task");
        assert!(!activity_page.events.is_empty());

        // Client A cannot access Session B's approval.
        let cross_err = fresh_client_a
            .get_task(&approval_b, "task-root-agy")
            .await
            .expect_err("cross-session access must fail");
        assert_eq!(cross_err.code(), "scope_mismatch");

        host_a.shutdown().await.expect("shutdown host A");
        host_b.shutdown().await.expect("shutdown host B");
    }

    #[tokio::test]
    async fn startup_failure_leaves_failed_manifest_and_releases_lock() {
        let fixture = setup_fixture();
        let approval_id = create_approved_plan(
            &fixture,
            "plan-fail-boot",
            BINDING_A,
            THREAD_UUID_A,
            default_tasks("Boot failure test"),
        );

        // Remove the frozen executable file before starting the host so materialization fails.
        fs::remove_file(&fixture.fake_agy_exe).expect("remove fake-agy executable");

        let start_err = ApprovedHost::start(ApprovedHostConfig::new(
            &fixture.registry_dir,
            &fixture.plans_dir,
            &fixture.authority_dir,
            &fixture.sessions_dir,
            &approval_id,
        ))
        .await
        .expect_err("startup with missing frozen executable must fail");
        assert_eq!(start_err.code(), "authority_error");

        let session_dir = fixture.sessions_dir.join(&approval_id);
        assert!(
            !is_host_lock_held(&session_dir).expect("check lock after failed startup"),
            "host.lock must be released when startup fails"
        );

        let record = fixture
            .authority
            .get_for_operator(&approval_id)
            .expect("get record");
        let failed_manifest = read_host_manifest_optional(&session_dir, &record)
            .expect("read manifest")
            .expect("failed manifest recorded");
        assert_eq!(failed_manifest.phase, HostPhase::Failed);
        assert!(!failed_manifest.gates.supervisor_authenticated_ready);
        assert!(failed_manifest.last_error.is_some());

        let client = ApprovedHostClient::open(
            &fixture.registry_dir,
            &fixture.plans_dir,
            &fixture.authority_dir,
            &fixture.sessions_dir,
            BINDING_A,
            THREAD_UUID_A,
        )
        .expect("open client");
        let not_ready_err = client
            .get_task(&approval_id, "task-root-agy")
            .await
            .expect_err("failed host must reject client calls with host_not_ready");
        assert_eq!(not_ready_err.code(), "host_not_ready");
    }

    #[tokio::test]
    async fn graceful_restart_preserves_binding_marks_inflight_unknown_and_never_replays() {
        let fixture = setup_fixture();
        let approval_id = create_approved_plan(
            &fixture,
            "plan-restart",
            BINDING_A,
            THREAD_UUID_A,
            default_tasks("SLOW_WAIT_FOR_CANCEL restart test"),
        );

        let host1 = ApprovedHost::start(
            ApprovedHostConfig::new(
                &fixture.registry_dir,
                &fixture.plans_dir,
                &fixture.authority_dir,
                &fixture.sessions_dir,
                &approval_id,
            )
            .with_tick_interval(Duration::from_millis(20)),
        )
        .await
        .expect("start host 1");

        let original_session_binding_id = host1.session_binding_id().to_string();

        let client = ApprovedHostClient::open(
            &fixture.registry_dir,
            &fixture.plans_dir,
            &fixture.authority_dir,
            &fixture.sessions_dir,
            BINDING_A,
            THREAD_UUID_A,
        )
        .expect("open client");

        let started = client
            .dispatch_task(&approval_id, "task-root-agy")
            .await
            .expect("dispatch slow task");
        let running = wait_for_task_status(&client, &approval_id, "task-root-agy", |st| {
            st == TaskExecutionStatus::Running
        })
        .await;
        assert_eq!(running.run_id, started.run_id);
        // Running only proves the runtime recorded the task after spawning; the
        // fake child may not have written its heartbeat count yet.
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let count = read_invocation_count(&fixture.agy_count_file);
            if count == 1 {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "timed out waiting for exactly one invocation; last count: {count}"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(read_invocation_count(&fixture.agy_count_file), 1);

        // Gracefully shut down host 1 while the worker is in-flight.
        host1.shutdown().await.expect("shutdown host 1");

        // Restart host on the same session directory after the graceful shutdown.
        let host2 = ApprovedHost::start(
            ApprovedHostConfig::new(
                &fixture.registry_dir,
                &fixture.plans_dir,
                &fixture.authority_dir,
                &fixture.sessions_dir,
                &approval_id,
            )
            .with_tick_interval(Duration::from_millis(20)),
        )
        .await
        .expect("restart host 2");

        // Must preserve the single existing session_binding_id and never replay the worker.
        assert_eq!(host2.session_binding_id(), original_session_binding_id);
        assert_eq!(read_invocation_count(&fixture.agy_count_file), 1);

        let after_restart = client
            .get_task(&approval_id, "task-root-agy")
            .await
            .expect("get_task after restart");
        assert_eq!(after_restart.run_id, started.run_id);
        assert_eq!(after_restart.status, TaskExecutionStatus::Unknown);
        assert!(!after_restart.accepted);

        host2.shutdown().await.expect("shutdown host 2");
    }

    #[tokio::test]
    async fn revision_checked_cancellation_rejects_stale_and_cancels_matching_revision() {
        let fixture = setup_fixture();
        let approval_id = create_approved_plan(
            &fixture,
            "plan-cancel",
            BINDING_A,
            THREAD_UUID_A,
            default_tasks("SLOW_WAIT_FOR_CANCEL cancellation test"),
        );

        let host = ApprovedHost::start(
            ApprovedHostConfig::new(
                &fixture.registry_dir,
                &fixture.plans_dir,
                &fixture.authority_dir,
                &fixture.sessions_dir,
                &approval_id,
            )
            .with_tick_interval(Duration::from_millis(20)),
        )
        .await
        .expect("start host");

        let client = ApprovedHostClient::open(
            &fixture.registry_dir,
            &fixture.plans_dir,
            &fixture.authority_dir,
            &fixture.sessions_dir,
            BINDING_A,
            THREAD_UUID_A,
        )
        .expect("open client");

        client
            .dispatch_task(&approval_id, "task-root-agy")
            .await
            .expect("dispatch slow task");

        let running = wait_for_task_status(&client, &approval_id, "task-root-agy", |st| {
            st == TaskExecutionStatus::Running
        })
        .await;
        let current_revision = running
            .worker
            .as_ref()
            .expect("worker snapshot present")
            .revision;

        // Stale expected_revision must fail closed with stale_revision.
        let stale_err = client
            .cancel_task(
                &approval_id,
                "task-root-agy",
                current_revision + 50,
                Some("stale revision test".to_string()),
            )
            .await
            .expect_err("stale revision must fail");
        assert_eq!(stale_err.code(), "stale_revision");

        // Matching expected_revision requests cancellation and transitions the worker to Cancelled.
        client
            .cancel_task(
                &approval_id,
                "task-root-agy",
                current_revision,
                Some("operator requested cancellation".to_string()),
            )
            .await
            .expect("cancel with matching revision");

        let cancelled = wait_for_task_status(&client, &approval_id, "task-root-agy", |st| {
            st == TaskExecutionStatus::Cancelled
        })
        .await;
        assert_eq!(cancelled.status, TaskExecutionStatus::Cancelled);
        assert!(!cancelled.accepted);

        host.shutdown().await.expect("shutdown host");
    }

    #[tokio::test]
    async fn atomic_writer_temp_artifacts_validated_narrowly_without_false_anchor_or_bypass() {
        let fixture = setup_fixture();
        let approval_id = create_approved_plan(
            &fixture,
            "plan-atomic-tmp",
            BINDING_A,
            THREAD_UUID_A,
            default_tasks("Atomic temp race validation"),
        );
        let record = fixture
            .authority
            .get_for_operator(&approval_id)
            .expect("get approval record");

        ensure_private_dir(&fixture.sessions_dir).expect("create private sessions_dir");
        let session_dir = ensure_private_dir(&fixture.sessions_dir.join(&approval_id))
            .expect("create private session_dir");

        let valid_manifest_tmp = ".host-manifest.json.tmp.550e8400e29b41d4a716446655440000";
        let valid_receipts_tmp = ".task-receipts.json.tmp.550e8400e29b41d4a716446655440000";
        let manifest_tmp_path = session_dir.join(valid_manifest_tmp);
        let receipts_tmp_path = session_dir.join(valid_receipts_tmp);

        let write_private_file = |path: &Path, bytes: &[u8]| {
            fs::write(path, bytes).expect("write test file");
            fs::set_permissions(path, fs::Permissions::from_mode(0o600))
                .expect("chmod 0600 test file");
        };

        // 1. Temp-only directory with no real anchor (no host.lock or host-manifest.json) must be rejected.
        let fake_ready_manifest = HostSessionManifest::from_approval_record(
            &record,
            Some("sb_fake_uncommitted".to_string()),
            HostPhase::Ready,
            Some(12345),
            None,
            1_700_000_002_000,
            1_700_000_002_000,
        );
        let serialized_manifest =
            serde_json::to_vec_pretty(&fake_ready_manifest).expect("serialize fake manifest");
        write_private_file(&manifest_tmp_path, &serialized_manifest);

        let temp_only_verify_err = verify_existing_session_dir(&fixture.sessions_dir, &approval_id)
            .expect_err("temp-only session dir without anchor must fail verify");
        assert_eq!(temp_only_verify_err.code(), "security_violation");

        let temp_only_prepare_err = prepare_session_dir(&fixture.sessions_dir, &approval_id)
            .expect_err("temp-only session dir without anchor must fail prepare");
        assert_eq!(temp_only_prepare_err.code(), "security_violation");

        // 2. Alongside a real anchor (host.lock), private 0600 recognized temp artifacts within budget succeed,
        //    are preserved untouched, and are never parsed as committed state or readiness admission.
        let lock_path = session_dir.join(HOST_LOCK_FILENAME);
        write_private_file(&lock_path, b"");
        write_private_file(&receipts_tmp_path, &vec![b'r'; MAX_TASK_RECEIPTS_BYTES]);

        verify_existing_session_dir(&fixture.sessions_dir, &approval_id)
            .expect("verify_existing_session_dir succeeds with real anchor and valid temp files");
        prepare_session_dir(&fixture.sessions_dir, &approval_id)
            .expect("prepare_session_dir succeeds with real anchor and valid temp files");

        assert!(
            read_host_manifest_optional(&session_dir, &record)
                .expect("read_host_manifest_optional ignores temp file")
                .is_none(),
            "transient .host-manifest.json.tmp.<uuid> must never be parsed as committed manifest"
        );
        assert!(
            manifest_tmp_path.exists() && receipts_tmp_path.exists(),
            "recognized temp remnants must not be deleted by directory inspection"
        );

        let client = ApprovedHostClient::open(
            &fixture.registry_dir,
            &fixture.plans_dir,
            &fixture.authority_dir,
            &fixture.sessions_dir,
            BINDING_A,
            THREAD_UUID_A,
        )
        .expect("open client");
        let not_ready_err = client
            .get_task(&approval_id, "task-root-agy")
            .await
            .expect_err("temp manifest without committed Ready manifest must reject client calls");
        assert_eq!(not_ready_err.code(), "host_not_ready");

        fs::remove_file(&manifest_tmp_path).expect("remove manifest temp");
        fs::remove_file(&receipts_tmp_path).expect("remove receipts temp");

        // 3. Invalid UUID suffixes and invalid prefixes/names remain rejected alongside a real anchor.
        for invalid_name in [
            ".host-manifest.json.tmp.not-a-valid-uuid",
            ".host-manifest.json.tmp.550e8400-e29b-41d4-a716-446655440000",
            ".host-manifest.json.tmp.550E8400E29B41D4A716446655440000",
            ".host-manifest.json.tmp.6ba7b8109dad11d180b400c04fd430c8",
            ".host-manifest.json.tmp.00000000000000000000000000000000",
            ".host-manifest.json.tmp.550e8400e29b41d40716446655440000",
            ".host-manifest.json.tmp.550e8400e29b41d4a71644665544000",
            ".host-manifest.json.tmp.550e8400e29b41d4a716446655440000a",
            ".host.lock.tmp.550e8400e29b41d4a716446655440000",
            ".task-receipts.lock.tmp.550e8400e29b41d4a716446655440000",
            ".record.json.tmp.550e8400e29b41d4a716446655440000",
            "host-manifest.json.tmp.550e8400e29b41d4a716446655440000",
            ".foreign-hidden",
        ] {
            let bad_path = session_dir.join(invalid_name);
            write_private_file(&bad_path, b"{}");
            let err = verify_existing_session_dir(&fixture.sessions_dir, &approval_id)
                .expect_err("invalid temp/foreign name must be rejected");
            assert_eq!(err.code(), "security_violation", "failed on {invalid_name}");
            assert!(
                bad_path.exists(),
                "foreign file `{invalid_name}` must not be deleted"
            );
            fs::remove_file(&bad_path).expect("clean bad_path");
        }

        // 4. Directory with recognized temp name must be rejected.
        fs::create_dir(&manifest_tmp_path).expect("create dir with temp name");
        fs::set_permissions(&manifest_tmp_path, fs::Permissions::from_mode(0o700))
            .expect("chmod 0700 dir with temp name");
        let dir_err = verify_existing_session_dir(&fixture.sessions_dir, &approval_id)
            .expect_err("directory named like temp file must be rejected");
        assert_eq!(dir_err.code(), "security_violation");
        fs::remove_dir(&manifest_tmp_path).expect("remove temp-named dir");

        // 5. Symlink with recognized temp name must be rejected.
        let outside_target = fixture.root.join("outside-temp-target.json");
        write_private_file(&outside_target, b"{}");
        symlink(&outside_target, &manifest_tmp_path).expect("create symlink with temp name");
        let sym_err = verify_existing_session_dir(&fixture.sessions_dir, &approval_id)
            .expect_err("symlink named like temp file must be rejected");
        assert_eq!(sym_err.code(), "security_violation");
        fs::remove_file(&manifest_tmp_path).expect("remove temp-named symlink");

        // 6. Permissive permissions on recognized temp file must be rejected.
        for bad_mode in [0o644, 0o640, 0o604, 0o700] {
            fs::write(&manifest_tmp_path, b"{}").expect("write permissive temp file");
            fs::set_permissions(&manifest_tmp_path, fs::Permissions::from_mode(bad_mode))
                .expect("chmod permissive temp file");
            let perm_err = verify_existing_session_dir(&fixture.sessions_dir, &approval_id)
                .expect_err("permissive temp file must be rejected");
            assert_eq!(perm_err.code(), "security_violation");
            fs::remove_file(&manifest_tmp_path).expect("remove permissive temp file");
        }

        // 7. Oversize recognized temp files exceeding their respective byte budgets must be rejected.
        write_private_file(&manifest_tmp_path, &vec![b'a'; MAX_HOST_MANIFEST_BYTES + 1]);
        let oversize_err = verify_existing_session_dir(&fixture.sessions_dir, &approval_id)
            .expect_err("oversize manifest temp file must be rejected");
        assert_eq!(oversize_err.code(), "security_violation");
        fs::remove_file(&manifest_tmp_path).expect("remove oversize manifest temp");

        write_private_file(&receipts_tmp_path, &vec![b'b'; MAX_TASK_RECEIPTS_BYTES + 1]);
        let oversize_err = verify_existing_session_dir(&fixture.sessions_dir, &approval_id)
            .expect_err("oversize receipts temp file must be rejected");
        assert_eq!(oversize_err.code(), "security_violation");
        fs::remove_file(&receipts_tmp_path).expect("remove oversize receipts temp");

        // 8. Legitimate disappeared-temp observation between read_dir and metadata inspection:
        //    (a) Disappeared temp alongside existing host.lock succeeds without false failure.
        storage::inspect_session_dir_snapshot(
            &session_dir,
            [HOST_LOCK_FILENAME, valid_manifest_tmp],
        )
        .expect("disappeared temp file alongside host.lock must not fail");

        //    (b) Snapshot that observed only the temp file before it was atomically renamed to a valid
        //        0600 host-manifest.json validates the committed target and succeeds.
        fs::remove_file(&lock_path).expect("remove host.lock for rename-to-manifest test");
        let committed_manifest_path = session_dir.join(HOST_MANIFEST_FILENAME);
        write_private_file(&committed_manifest_path, &serialized_manifest);
        storage::inspect_session_dir_snapshot(&session_dir, [valid_manifest_tmp])
            .expect("disappeared temp file renamed to valid host-manifest.json must succeed");

        //    (c) If the committed target after rename has permissive permissions, security violation is not swallowed.
        fs::set_permissions(&committed_manifest_path, fs::Permissions::from_mode(0o644))
            .expect("chmod 0644 committed manifest");
        let rename_perm_err =
            storage::inspect_session_dir_snapshot(&session_dir, [valid_manifest_tmp])
                .expect_err("permissive committed target after temp rename must fail");
        assert_eq!(rename_perm_err.code(), "security_violation");
        fs::remove_file(&committed_manifest_path).expect("remove committed manifest");

        //    (d) If the temp file disappeared in an unanchored directory with neither host.lock nor host-manifest.json,
        //        it still fails closed with security_violation.
        let unanchored_err =
            storage::inspect_session_dir_snapshot(&session_dir, [valid_manifest_tmp])
                .expect_err("disappeared temp file without any committed anchor must fail");
        assert_eq!(unanchored_err.code(), "security_violation");
    }

    #[tokio::test]
    async fn dispatch_intent_before_effect_unknown_read_cancel_reopen_and_explicit_retry() {
        let fixture = setup_fixture();
        let approval_id = create_approved_plan(
            &fixture,
            "plan-intent-effect",
            BINDING_A,
            THREAD_UUID_A,
            default_tasks("Effect before seal with durable intent"),
        );

        let host = ApprovedHost::start(
            ApprovedHostConfig::new(
                &fixture.registry_dir,
                &fixture.plans_dir,
                &fixture.authority_dir,
                &fixture.sessions_dir,
                &approval_id,
            )
            .with_tick_interval(Duration::from_millis(20)),
        )
        .await
        .expect("start host");

        let client = ApprovedHostClient::open(
            &fixture.registry_dir,
            &fixture.plans_dir,
            &fixture.authority_dir,
            &fixture.sessions_dir,
            BINDING_A,
            THREAD_UUID_A,
        )
        .expect("open client");

        // Truly never-dispatched root worker tasks start in NotStarted.
        let initial_agy = client
            .get_task(&approval_id, "task-root-agy")
            .await
            .expect("get_task initial agy");
        assert_eq!(initial_agy.status, TaskExecutionStatus::NotStarted);
        assert!(initial_agy.run_id.is_none());

        let initial_oc = client
            .get_task(&approval_id, "task-root-oc")
            .await
            .expect("get_task initial oc");
        assert_eq!(initial_oc.status, TaskExecutionStatus::NotStarted);
        assert!(initial_oc.run_id.is_none());

        // 1. Simulate the crash window in dispatch_task where the durable TaskDispatchIntent is
        //    committed and ScopedClient::start_worker executes the real fake-agy worker, but the
        //    caller crashes before record_task_receipt seals the run_id mapping.
        let agy_dispatch = host
            .materialized_plan()
            .task_dispatch
            .iter()
            .find(|d| d.task_id == "task-root-agy")
            .expect("materialized dispatch entry for task-root-agy")
            .clone();

        let committed_intent = record_task_dispatch_intent(
            host.session_dir(),
            host.approval(),
            host.session_binding_id(),
            TaskDispatchIntent {
                task_id: agy_dispatch.task_id.clone(),
                task_digest: agy_dispatch.task_digest.clone(),
                profile_id: agy_dispatch.profile_id.clone(),
                command_key: agy_dispatch.command_key.clone(),
                recorded_at_ms: 1_700_000_010_000,
            },
        )
        .expect("commit durable dispatch intent before runtime start_worker");
        assert_eq!(committed_intent.task_id, "task-root-agy");

        let scoped = threadlane::ScopedClient::from_state_dir(
            host.runtime_state_dir(),
            host.session_binding_id(),
        )
        .expect("open scoped client");
        let started_worker = scoped
            .start_worker(
                agy_dispatch.command_key.clone(),
                agy_dispatch.profile_id.clone(),
                agy_dispatch.instruction.clone(),
            )
            .await
            .expect("start_worker runtime effect before receipt seal");

        // Wait for the live worker run in the runtime supervisor to finish executing fake-agy.
        let deadline = Instant::now() + Duration::from_secs(8);
        loop {
            let w = scoped
                .get_worker(started_worker.run_id.clone())
                .await
                .expect("get_worker on live runtime run");
            if w.status == threadlane::WorkerStatus::Completed {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "timed out waiting for runtime worker completion; last snapshot: {w:?}"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        assert_eq!(read_invocation_count(&fixture.agy_count_file), 1);

        // No sealed receipt exists yet, only the durable pre-effect dispatch intent.
        assert!(read_task_receipt(
            host.session_dir(),
            host.approval(),
            host.session_binding_id(),
            "task-root-agy"
        )
        .expect("read_task_receipt")
        .is_none());
        assert_eq!(
            read_task_dispatch_intent(
                host.session_dir(),
                host.approval(),
                host.session_binding_id(),
                "task-root-agy"
            )
            .expect("read_task_dispatch_intent"),
            Some(committed_intent.clone())
        );

        // 2. Read-only get_task reports TaskExecutionStatus::Unknown (never NotStarted),
        //    never spawns a worker, never mutates task-receipts.json, and never changes cursors.
        let receipts_path = host.session_dir().join(TASK_RECEIPTS_FILENAME);
        let receipts_bytes_before =
            fs::read(&receipts_path).expect("read receipts before get_task");
        let status_before = scoped
            .status()
            .await
            .expect("scoped status before get_task");
        let activity_before = client
            .list_activity(&approval_id, None, None, Some(20), None)
            .await
            .expect("list_activity before get_task");

        let unknown_snap = client
            .get_task(&approval_id, "task-root-agy")
            .await
            .expect("get_task on unsealed intent");
        assert_eq!(unknown_snap.status, TaskExecutionStatus::Unknown);
        assert!(unknown_snap.run_id.is_none());
        assert!(unknown_snap.worker_id.is_none());
        assert!(unknown_snap.worker.is_none());
        assert!(!unknown_snap.accepted);

        assert_eq!(read_invocation_count(&fixture.agy_count_file), 1);
        let status_after = scoped.status().await.expect("scoped status after get_task");
        assert_eq!(
            status_after.binding.delivered_seq,
            status_before.binding.delivered_seq
        );
        assert_eq!(
            status_after.binding.acked_seq,
            status_before.binding.acked_seq
        );
        let activity_after = client
            .list_activity(&approval_id, None, None, Some(20), None)
            .await
            .expect("list_activity after get_task");
        assert_eq!(
            activity_after.high_water_mark_seq,
            activity_before.high_water_mark_seq
        );
        assert_eq!(activity_after.next_cursor, activity_before.next_cursor);
        assert_eq!(activity_after.events, activity_before.events);
        assert_eq!(
            fs::read(&receipts_path).expect("read receipts after get_task"),
            receipts_bytes_before
        );

        // 3. Cancelling a task with a durable dispatch intent but no sealed receipt reports
        //    unknown/unresolved (invalid_run_state), distinct from not_started, without inventing a run_id.
        let cancel_ambiguous_err = client
            .cancel_task(
                &approval_id,
                "task-root-agy",
                1,
                Some("attempt cancel without sealed receipt".to_string()),
            )
            .await
            .expect_err("cancel with unsealed intent must fail");
        assert_eq!(cancel_ambiguous_err.code(), "invalid_run_state");
        assert_ne!(cancel_ambiguous_err.code(), "not_started");
        let cancel_msg = cancel_ambiguous_err.to_string();
        assert!(cancel_msg.contains("unresolved"));
        assert!(cancel_msg.contains("unknown"));
        assert!(!cancel_msg.contains(&started_worker.run_id));

        let activity_ambiguous_err = client
            .list_task_activity(&approval_id, "task-root-agy", None, None, None, None)
            .await
            .expect_err("list_task_activity with unsealed intent must fail");
        assert_eq!(activity_ambiguous_err.code(), "invalid_run_state");
        assert_ne!(activity_ambiguous_err.code(), "not_started");

        // 4. Truly never-dispatched sibling task ("task-root-oc") remains NotStarted / not_started.
        let unstarted_sibling = client
            .get_task(&approval_id, "task-root-oc")
            .await
            .expect("get_task on unstarted sibling");
        assert_eq!(unstarted_sibling.status, TaskExecutionStatus::NotStarted);
        let cancel_unstarted_err = client
            .cancel_task(&approval_id, "task-root-oc", 1, None)
            .await
            .expect_err("cancel truly unstarted sibling");
        assert_eq!(cancel_unstarted_err.code(), "not_started");

        // 5. Reopening the client and restarting the host retains the durable intent on disk.
        drop(client);
        let reopened_client = ApprovedHostClient::open(
            &fixture.registry_dir,
            &fixture.plans_dir,
            &fixture.authority_dir,
            &fixture.sessions_dir,
            BINDING_A,
            THREAD_UUID_A,
        )
        .expect("reopen client");
        assert_eq!(
            reopened_client
                .get_task(&approval_id, "task-root-agy")
                .await
                .expect("get_task after client reopen")
                .status,
            TaskExecutionStatus::Unknown
        );
        assert_eq!(
            reopened_client
                .get_task(&approval_id, "task-root-oc")
                .await
                .expect("get_task unstarted after client reopen")
                .status,
            TaskExecutionStatus::NotStarted
        );

        let session_dir = host.session_dir().to_path_buf();
        let runtime_state_dir = host.runtime_state_dir().to_path_buf();
        let session_binding_id = host.session_binding_id().to_string();
        let approval_record = host.approval().clone();
        let oc_dispatch = host
            .materialized_plan()
            .task_dispatch
            .iter()
            .find(|d| d.task_id == "task-root-oc")
            .expect("materialized dispatch entry for task-root-oc")
            .clone();

        host.shutdown().await.expect("shutdown host before reopen");
        let host = ApprovedHost::start(
            ApprovedHostConfig::new(
                &fixture.registry_dir,
                &fixture.plans_dir,
                &fixture.authority_dir,
                &fixture.sessions_dir,
                &approval_id,
            )
            .with_tick_interval(Duration::from_millis(20)),
        )
        .await
        .expect("restart host");
        assert_eq!(host.session_binding_id(), session_binding_id);
        assert_eq!(read_invocation_count(&fixture.agy_count_file), 1);
        assert_eq!(
            reopened_client
                .get_task(&approval_id, "task-root-agy")
                .await
                .expect("get_task after host restart")
                .status,
            TaskExecutionStatus::Unknown
        );

        // 6. Explicit dispatch_task retry uses the SAME frozen command_key to repair the sealed
        //    receipt mapping without spawning a new worker (invocation count stays 1).
        let repaired = reopened_client
            .dispatch_task(&approval_id, "task-root-agy")
            .await
            .expect("explicit dispatch_task retry repairs lost receipt");
        assert_eq!(repaired.run_id, Some(started_worker.run_id.clone()));
        assert_eq!(repaired.worker_id, Some(started_worker.worker_id.clone()));
        assert_eq!(repaired.status, TaskExecutionStatus::ReadyForReview);
        assert!(!repaired.accepted);
        assert_eq!(read_invocation_count(&fixture.agy_count_file), 1);

        let sealed_receipt = read_task_receipt(
            &session_dir,
            &approval_record,
            &session_binding_id,
            "task-root-agy",
        )
        .expect("read repaired receipt")
        .expect("sealed receipt now present");
        assert_eq!(sealed_receipt.run_id, started_worker.run_id);
        assert_eq!(sealed_receipt.worker_id, started_worker.worker_id);

        // 7. Also verify that ApprovedHostClient::dispatch_task itself commits the durable intent
        //    BEFORE start_worker returns, and retains the intent when start_worker errors.
        let scoped_restarted =
            threadlane::ScopedClient::from_state_dir(&runtime_state_dir, &session_binding_id)
                .expect("open scoped client after restart");
        scoped_restarted
            .start_worker(
                oc_dispatch.command_key.clone(),
                oc_dispatch.profile_id.clone(),
                "conflicting payload to force start_worker error in dispatch_task".to_string(),
            )
            .await
            .expect("seed command_key with conflicting instruction");

        let dispatch_oc_err = reopened_client
            .dispatch_task(&approval_id, "task-root-oc")
            .await
            .expect_err("dispatch_task must fail when start_worker returns idempotency_conflict");
        assert_eq!(dispatch_oc_err.code(), "idempotency_conflict");

        // Because dispatch_task committed the intent BEFORE calling start_worker, the intent is
        // retained on disk and get_task now reports Unknown (not NotStarted).
        let oc_intent = read_task_dispatch_intent(
            &session_dir,
            &approval_record,
            &session_binding_id,
            "task-root-oc",
        )
        .expect("read oc dispatch intent")
        .expect("oc dispatch intent retained after start_worker error");
        assert_eq!(oc_intent.command_key, oc_dispatch.command_key);
        assert_eq!(oc_intent.profile_id, oc_dispatch.profile_id);
        assert_eq!(oc_intent.task_digest, oc_dispatch.task_digest);
        assert!(read_task_receipt(
            &session_dir,
            &approval_record,
            &session_binding_id,
            "task-root-oc"
        )
        .expect("read oc receipt")
        .is_none());
        assert_eq!(
            reopened_client
                .get_task(&approval_id, "task-root-oc")
                .await
                .expect("get_task on task-root-oc after failed dispatch_task")
                .status,
            TaskExecutionStatus::Unknown
        );

        host.shutdown().await.expect("shutdown host");
    }

    #[tokio::test]
    async fn legacy_receipts_without_intents_and_malformed_or_foreign_intents_fail_closed() {
        let fixture = setup_fixture();
        let approval_id = create_approved_plan(
            &fixture,
            "plan-intent-validation",
            BINDING_A,
            THREAD_UUID_A,
            default_tasks("Intent ledger validation"),
        );

        let host = ApprovedHost::start(
            ApprovedHostConfig::new(
                &fixture.registry_dir,
                &fixture.plans_dir,
                &fixture.authority_dir,
                &fixture.sessions_dir,
                &approval_id,
            )
            .with_tick_interval(Duration::from_millis(20)),
        )
        .await
        .expect("start host");

        let client = ApprovedHostClient::open(
            &fixture.registry_dir,
            &fixture.plans_dir,
            &fixture.authority_dir,
            &fixture.sessions_dir,
            BINDING_A,
            THREAD_UUID_A,
        )
        .expect("open client");

        let dispatched = client
            .dispatch_task(&approval_id, "task-root-agy")
            .await
            .expect("dispatch task-root-agy");
        let completed = wait_for_task_status(&client, &approval_id, "task-root-agy", |st| {
            st == TaskExecutionStatus::ReadyForReview
        })
        .await;
        assert_eq!(completed.run_id, dispatched.run_id);

        let receipts_path = host.session_dir().join(TASK_RECEIPTS_FILENAME);
        let sealed_receipt = read_task_receipt(
            host.session_dir(),
            host.approval(),
            host.session_binding_id(),
            "task-root-agy",
        )
        .expect("read sealed receipt")
        .expect("receipt exists");

        let write_receipts_json = |value: serde_json::Value| -> Vec<u8> {
            let mut bytes = serde_json::to_vec_pretty(&value).expect("serialize ledger JSON");
            bytes.push(b'\n');
            fs::write(&receipts_path, &bytes).expect("write task-receipts.json");
            fs::set_permissions(&receipts_path, fs::Permissions::from_mode(0o600))
                .expect("chmod 0600 task-receipts.json");
            bytes
        };

        // 1. Older ledger format without `pending_intents` key still deserializes cleanly with
        //    #[serde(default)] empty intents.
        write_receipts_json(serde_json::json!({
            "schema_version": TASK_RECEIPTS_SCHEMA_VERSION,
            "approval_id": host.approval_id(),
            "subject_digest": host.approval().subject_digest,
            "session_binding_id": host.session_binding_id(),
            "receipts": [sealed_receipt],
        }));

        assert!(read_task_dispatch_intent(
            host.session_dir(),
            host.approval(),
            host.session_binding_id(),
            "task-root-agy"
        )
        .expect("read intent from legacy ledger")
        .is_none());
        let legacy_snap = client
            .get_task(&approval_id, "task-root-agy")
            .await
            .expect("get_task with legacy ledger");
        assert_eq!(legacy_snap.status, TaskExecutionStatus::ReadyForReview);
        assert_eq!(legacy_snap.run_id, dispatched.run_id);
        assert_eq!(
            client
                .get_task(&approval_id, "task-root-oc")
                .await
                .expect("get_task unstarted with legacy ledger")
                .status,
            TaskExecutionStatus::NotStarted
        );

        // 2. Malformed, foreign, dependent, native-route, mismatched, or duplicate intents in
        //    task-receipts.json must fail closed with corrupt_receipts and preserve the file.
        let valid_intent = serde_json::json!({
            "task_id": sealed_receipt.task_id,
            "task_digest": sealed_receipt.task_digest,
            "profile_id": sealed_receipt.profile_id,
            "command_key": sealed_receipt.command_key,
            "recorded_at_ms": 1_700_000_020_000i64,
        });

        let bad_intent_cases = vec![
            // Foreign task_id not in approval record
            serde_json::json!([{
                "task_id": "task-foreign-injected",
                "task_digest": sealed_receipt.task_digest,
                "profile_id": sealed_receipt.profile_id,
                "command_key": sealed_receipt.command_key,
                "recorded_at_ms": 1_700_000_020_000i64,
            }]),
            // Dependent task_id in pending_intents
            serde_json::json!([{
                "task_id": "task-dep-agy",
                "task_digest": sealed_receipt.task_digest,
                "profile_id": sealed_receipt.profile_id,
                "command_key": sealed_receipt.command_key,
                "recorded_at_ms": 1_700_000_020_000i64,
            }]),
            // Manual native route task_id in pending_intents
            serde_json::json!([{
                "task_id": "task-native-codex",
                "task_digest": sealed_receipt.task_digest,
                "profile_id": sealed_receipt.profile_id,
                "command_key": sealed_receipt.command_key,
                "recorded_at_ms": 1_700_000_020_000i64,
            }]),
            // Mismatched task_digest (valid hex, wrong digest for task)
            serde_json::json!([{
                "task_id": sealed_receipt.task_id,
                "task_digest": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
                "profile_id": sealed_receipt.profile_id,
                "command_key": sealed_receipt.command_key,
                "recorded_at_ms": 1_700_000_020_000i64,
            }]),
            // Mismatched profile_id
            serde_json::json!([{
                "task_id": sealed_receipt.task_id,
                "task_digest": sealed_receipt.task_digest,
                "profile_id": "prof_tampered_profile",
                "command_key": sealed_receipt.command_key,
                "recorded_at_ms": 1_700_000_020_000i64,
            }]),
            // Mismatched command_key
            serde_json::json!([{
                "task_id": sealed_receipt.task_id,
                "task_digest": sealed_receipt.task_digest,
                "profile_id": sealed_receipt.profile_id,
                "command_key": "cmd_tampered_command_key",
                "recorded_at_ms": 1_700_000_020_000i64,
            }]),
            // Duplicate intent entries for the same task_id
            serde_json::json!([valid_intent.clone(), valid_intent.clone()]),
            // Unknown field inside intent entry
            serde_json::json!([{
                "task_id": sealed_receipt.task_id,
                "task_digest": sealed_receipt.task_digest,
                "profile_id": sealed_receipt.profile_id,
                "command_key": sealed_receipt.command_key,
                "recorded_at_ms": 1_700_000_020_000i64,
                "foreign_field": "rejected",
            }]),
        ];

        for bad_intents in bad_intent_cases {
            let raw_bytes = write_receipts_json(serde_json::json!({
                "schema_version": TASK_RECEIPTS_SCHEMA_VERSION,
                "approval_id": host.approval_id(),
                "subject_digest": host.approval().subject_digest,
                "session_binding_id": host.session_binding_id(),
                "pending_intents": bad_intents,
                "receipts": [],
            }));

            let get_err = client
                .get_task(&approval_id, "task-root-agy")
                .await
                .expect_err("malformed/foreign intent must fail get_task closed");
            assert_eq!(get_err.code(), "corrupt_receipts");

            let dispatch_err = client
                .dispatch_task(&approval_id, "task-root-agy")
                .await
                .expect_err("malformed/foreign intent must fail dispatch_task closed");
            assert_eq!(dispatch_err.code(), "corrupt_receipts");

            let cancel_err = client
                .cancel_task(&approval_id, "task-root-agy", 1, None)
                .await
                .expect_err("malformed/foreign intent must fail cancel_task closed");
            assert_eq!(cancel_err.code(), "corrupt_receipts");

            assert_eq!(
                fs::read(&receipts_path).expect("read preserved corrupt task-receipts.json"),
                raw_bytes
            );
        }

        host.shutdown().await.expect("shutdown host");
    }

    fn write_fake_codex_script(script_path: &Path, log_file: &Path, sleep_seconds: Option<u32>) {
        if let Some(parent) = script_path.parent() {
            fs::create_dir_all(parent).expect("create fake codex parent dir");
        }
        let sleep_cmd = sleep_seconds
            .map(|secs| format!("sleep {secs}\n"))
            .unwrap_or_default();
        let script = format!(
            "#!/bin/sh\nprintf '%s|%s|%s|%s|%s|%s\\n' \"$1\" \"$2\" \"$3\" \"$4\" \"$5\" \"$#\" >> \"{}\"\n{}exit 0\n",
            log_file.display(),
            sleep_cmd
        );
        let mut f = File::create(script_path).expect("create fake-codex");
        f.write_all(script.as_bytes()).expect("write fake-codex");
        f.sync_all().expect("sync fake-codex");
        fs::set_permissions(script_path, fs::Permissions::from_mode(0o755))
            .expect("chmod 0755 fake-codex");
    }

    #[test]
    fn notification_config_cli_flags_and_legacy_manifest_compatibility() {
        let fixture = setup_fixture();
        let approval_id = create_approved_plan(
            &fixture,
            "plan-notify-compat",
            BINDING_A,
            THREAD_UUID_A,
            default_tasks("Notification config & manifest compatibility"),
        );

        let codex_log = fixture.root.join("codex-cli-parse.log");
        let fake_codex = fixture.root.join("bin/fake-codex-cli");
        write_fake_codex_script(&fake_codex, &codex_log, None);

        // Symlink launcher pointing to fake_codex is canonicalized to the real executable.
        let launcher_symlink = fixture.root.join("bin/codex-launcher-link");
        symlink(&fake_codex, &launcher_symlink).expect("create launcher symlink");

        let notify_cfg = ApprovedHostNotificationConfig::new(launcher_symlink.clone(), 15_000)
            .expect("canonicalize launcher symlink and validate executable");
        assert_eq!(
            notify_cfg.executable,
            fs::canonicalize(&fake_codex).expect("canonical fake_codex")
        );
        let summary = notify_cfg.summary();
        assert!(summary.enabled);
        assert_eq!(summary.timeout_ms, Some(15_000));
        let digest = summary
            .executable_digest
            .as_ref()
            .expect("executable_digest present");
        assert_eq!(digest.len(), 64);
        assert!(!serde_json::to_string(&summary)
            .expect("serialize summary")
            .contains(&fixture.root.display().to_string()));

        // Invalid executable paths and timeouts fail closed.
        assert_eq!(
            ApprovedHostNotificationConfig::new(PathBuf::new(), 1_000)
                .expect_err("empty path rejected")
                .code(),
            "validation"
        );
        assert_eq!(
            ApprovedHostNotificationConfig::new(PathBuf::from("relative/codex"), 1_000)
                .expect_err("relative path rejected")
                .code(),
            "validation"
        );
        assert_eq!(
            ApprovedHostNotificationConfig::new(
                fixture.root.join("bin/../bin/fake-codex-cli"),
                1_000
            )
            .expect_err("parent traversal rejected")
            .code(),
            "validation"
        );
        assert_eq!(
            ApprovedHostNotificationConfig::new(fake_codex.clone(), 0)
                .expect_err("0ms timeout rejected")
                .code(),
            "validation"
        );
        assert_eq!(
            ApprovedHostNotificationConfig::new(fake_codex.clone(), MAX_CODEX_TIMEOUT_MS + 1)
                .expect_err("excessive timeout rejected")
                .code(),
            "validation"
        );
        let non_exec = fixture.root.join("bin/non-exec-codex");
        fs::write(&non_exec, "#!/bin/sh\nexit 0\n").expect("write non-exec");
        fs::set_permissions(&non_exec, fs::Permissions::from_mode(0o600))
            .expect("chmod 0600 non-exec");
        assert_eq!(
            ApprovedHostNotificationConfig::new(non_exec, 1_000)
                .expect_err("non-executable file rejected")
                .code(),
            "validation"
        );

        // CLI flag parsing: --notify-timeout-ms without --notify-codex fails closed.
        let cli_timeout_only = HostCli::try_parse_from([
            "threadlane-host",
            "serve",
            "--registry-dir",
            fixture.registry_dir.to_str().unwrap(),
            "--plans-dir",
            fixture.plans_dir.to_str().unwrap(),
            "--authority-dir",
            fixture.authority_dir.to_str().unwrap(),
            "--sessions-dir",
            fixture.sessions_dir.to_str().unwrap(),
            "--approval-id",
            &approval_id,
            "--notify-timeout-ms",
            "5000",
        ])
        .expect("clap parses args");
        match cli_timeout_only.command {
            HostCommand::Serve(args) => {
                let err = args
                    .into_host_config()
                    .expect_err("--notify-timeout-ms without --notify-codex must fail");
                assert_eq!(err.code(), "validation");
            }
        }

        // CLI flag parsing: --notify-codex without --notify-timeout-ms uses DEFAULT_CODEX_TIMEOUT_MS.
        let cli_with_codex = HostCli::try_parse_from([
            "threadlane-host",
            "serve",
            "--registry-dir",
            fixture.registry_dir.to_str().unwrap(),
            "--plans-dir",
            fixture.plans_dir.to_str().unwrap(),
            "--authority-dir",
            fixture.authority_dir.to_str().unwrap(),
            "--sessions-dir",
            fixture.sessions_dir.to_str().unwrap(),
            "--approval-id",
            &approval_id,
            "--notify-codex",
            launcher_symlink.to_str().unwrap(),
        ])
        .expect("clap parses --notify-codex");
        match cli_with_codex.command {
            HostCommand::Serve(args) => {
                let cfg = args.into_host_config().expect("valid host config");
                let notif = cfg.notifications.expect("notifications enabled");
                assert_eq!(notif.timeout_ms, DEFAULT_CODEX_TIMEOUT_MS);
                assert_eq!(
                    notif.executable,
                    fs::canonicalize(&fake_codex).expect("canonical fake_codex")
                );
            }
        }

        // Legacy manifest JSON without `notifications` deserializes with disabled defaults.
        let record = fixture
            .authority
            .get_for_operator(&approval_id)
            .expect("get approval record");
        ensure_private_dir(&fixture.sessions_dir).expect("create sessions_dir");
        let session_dir =
            prepare_session_dir(&fixture.sessions_dir, &approval_id).expect("prepare session_dir");
        let manifest_path = session_dir.join(HOST_MANIFEST_FILENAME);

        let full_manifest = HostSessionManifest::from_approval_record(
            &record,
            Some("bnd_legacy_session".to_string()),
            HostPhase::Stopped,
            None,
            None,
            1_700_000_000_000,
            1_700_000_000_500,
        );
        let mut legacy_json: serde_json::Value =
            serde_json::to_value(&full_manifest).expect("manifest to value");
        legacy_json
            .as_object_mut()
            .expect("manifest object")
            .remove("notifications");
        let mut legacy_bytes = serde_json::to_vec_pretty(&legacy_json).expect("serialize legacy");
        legacy_bytes.push(b'\n');
        fs::write(&manifest_path, &legacy_bytes).expect("write legacy manifest");
        fs::set_permissions(&manifest_path, fs::Permissions::from_mode(0o600))
            .expect("chmod 0600 legacy manifest");

        let loaded_legacy =
            read_and_verify_host_manifest(&manifest_path, &record).expect("read legacy manifest");
        assert_eq!(
            loaded_legacy.notifications,
            HostNotificationSummary::disabled()
        );
        assert_eq!(
            loaded_legacy.to_summary().notifications,
            HostNotificationSummary::disabled()
        );

        // Inconsistent notifications summaries in manifest fail closed with corrupt_manifest.
        for bad_notifications in [
            serde_json::json!({
                "enabled": false,
                "executable_digest": digest,
                "timeout_ms": null
            }),
            serde_json::json!({
                "enabled": false,
                "executable_digest": null,
                "timeout_ms": 5000
            }),
            serde_json::json!({
                "enabled": true,
                "executable_digest": null,
                "timeout_ms": 5000
            }),
            serde_json::json!({
                "enabled": true,
                "executable_digest": digest,
                "timeout_ms": null
            }),
            serde_json::json!({
                "enabled": true,
                "executable_digest": "not-a-valid-sha256",
                "timeout_ms": 5000
            }),
            serde_json::json!({
                "enabled": true,
                "executable_digest": digest,
                "timeout_ms": 0
            }),
            serde_json::json!({
                "enabled": true,
                "executable_digest": digest,
                "timeout_ms": MAX_CODEX_TIMEOUT_MS + 1
            }),
        ] {
            let mut bad_manifest = legacy_json.clone();
            bad_manifest["notifications"] = bad_notifications;
            let raw = serde_json::to_vec_pretty(&bad_manifest).expect("serialize bad manifest");
            fs::write(&manifest_path, &raw).expect("write bad manifest");
            fs::set_permissions(&manifest_path, fs::Permissions::from_mode(0o600))
                .expect("chmod 0600 bad manifest");

            let err = read_and_verify_host_manifest(&manifest_path, &record)
                .expect_err("inconsistent notifications in manifest must fail closed");
            assert_eq!(err.code(), "corrupt_manifest");
            assert_eq!(
                fs::read(&manifest_path).expect("preserved corrupt manifest"),
                raw
            );
        }
    }

    #[tokio::test]
    async fn host_notifications_default_off_opt_in_argv_pull_ack_coalescing_and_restart_reset() {
        let fixture = setup_fixture();
        let confidential_instruction =
            "CONFIDENTIAL_WORKER_TASK_INSTRUCTION --model gemini-4-argon-high --effort high";
        let approval_id = create_approved_plan(
            &fixture,
            "plan-notify-e2e",
            BINDING_A,
            THREAD_UUID_A,
            default_tasks(confidential_instruction),
        );

        let codex_log = fixture.root.join("host-codex-invocations.log");
        let fake_codex = fixture.root.join("bin/fake-codex-host");
        write_fake_codex_script(&fake_codex, &codex_log, None);

        // 1. Default OFF: starting ApprovedHost without notifications sends zero Codex wake-ups.
        let host_off = ApprovedHost::start(
            ApprovedHostConfig::new(
                &fixture.registry_dir,
                &fixture.plans_dir,
                &fixture.authority_dir,
                &fixture.sessions_dir,
                &approval_id,
            )
            .with_tick_interval(Duration::from_millis(20)),
        )
        .await
        .expect("start host with default notifications off");

        assert_eq!(
            host_off.summary().notifications,
            HostNotificationSummary::disabled()
        );

        let client = ApprovedHostClient::open(
            &fixture.registry_dir,
            &fixture.plans_dir,
            &fixture.authority_dir,
            &fixture.sessions_dir,
            BINDING_A,
            THREAD_UUID_A,
        )
        .expect("open client");

        client
            .dispatch_task(&approval_id, "task-root-oc")
            .await
            .expect("dispatch task-root-oc while notifications disabled");
        wait_for_task_status(&client, &approval_id, "task-root-oc", |st| {
            st == TaskExecutionStatus::ReadyForReview
        })
        .await;
        tokio::time::sleep(Duration::from_millis(60)).await;
        assert!(
            !codex_log.exists(),
            "disabled notifications must never invoke codex"
        );

        // Pull and ack the events from task-root-oc while notifications are off, verifying
        // pull_events limit validation and scope checks.
        for bad_limit in [0usize, APPROVED_EVENT_BATCH_MAX_EVENTS + 1, 50] {
            let limit_err = client
                .pull_events(&approval_id, None, Some(bad_limit))
                .await
                .expect_err("invalid pull_events limit must fail before mutation");
            assert_eq!(limit_err.code(), "validation");
        }

        let wrong_client = ApprovedHostClient::open(
            &fixture.registry_dir,
            &fixture.plans_dir,
            &fixture.authority_dir,
            &fixture.sessions_dir,
            BINDING_B,
            THREAD_UUID_A,
        )
        .expect("open wrong binding client");
        assert_eq!(
            wrong_client
                .pull_events(&approval_id, None, None)
                .await
                .expect_err("wrong binding pull denied")
                .code(),
            "scope_mismatch"
        );
        assert_eq!(
            wrong_client
                .ack_events(&approval_id, "bat_fake".to_string())
                .await
                .expect_err("wrong binding ack denied")
                .code(),
            "scope_mismatch"
        );

        let initial_batch = client
            .pull_events(&approval_id, None, None)
            .await
            .expect("pull initial batch");
        assert!(!initial_batch.events.is_empty());
        assert!(initial_batch.events.len() <= APPROVED_EVENT_BATCH_MAX_EVENTS);
        let initial_ack = client
            .ack_events(&approval_id, initial_batch.batch_id.clone())
            .await
            .expect("ack initial batch");
        assert!(initial_ack.advanced);
        assert_eq!(initial_ack.acked_seq, initial_batch.to_seq);
        let prior_acked_seq = initial_ack.acked_seq;

        // Retrying ack_events with the same batch_id uses the deterministic derived command_key
        // and succeeds idempotently.
        let retry_ack = client
            .ack_events(&approval_id, initial_batch.batch_id.clone())
            .await
            .expect("idempotent retry ack_events");
        assert_eq!(retry_ack, initial_ack);

        host_off.shutdown().await.expect("shutdown host_off");

        // 2. Restart with explicit opt-in notifications:
        //    - Bootstrap phase must NOT send any wake-ups.
        //    - Final supervisor sends exact 5-element argv for new events on session_binding.
        let notify_cfg = ApprovedHostNotificationConfig::new(fake_codex.clone(), 2_000)
            .expect("create notify_cfg");
        let expected_notify_summary = notify_cfg.summary();

        let host_on = ApprovedHost::start(
            ApprovedHostConfig::new(
                &fixture.registry_dir,
                &fixture.plans_dir,
                &fixture.authority_dir,
                &fixture.sessions_dir,
                &approval_id,
            )
            .with_tick_interval(Duration::from_millis(20))
            .with_notifications(notify_cfg),
        )
        .await
        .expect("start host with notifications enabled");

        assert_eq!(host_on.summary().notifications, expected_notify_summary);
        assert_eq!(
            client
                .session_summary(&approval_id)
                .expect("client session_summary")
                .notifications,
            expected_notify_summary
        );
        // No wake-up was sent during bootstrap/startup when all prior events were already acked.
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            !codex_log.exists(),
            "bootstrap and idle startup must not emit wake-ups"
        );

        // Dispatch `task-root-agy` and wait for ReadyForReview.
        client
            .dispatch_task(&approval_id, "task-root-agy")
            .await
            .expect("dispatch task-root-agy with notifications enabled");
        wait_for_task_status(&client, &approval_id, "task-root-agy", |st| {
            st == TaskExecutionStatus::ReadyForReview
        })
        .await;

        let scoped = threadlane::ScopedClient::from_state_dir(
            host_on.runtime_state_dir(),
            host_on.session_binding_id(),
        )
        .expect("open scoped client");
        let deadline = Instant::now() + Duration::from_secs(5);
        let first_submitted = loop {
            let st = scoped.status().await.expect("scoped status");
            if st.wakeup.status == threadlane_core::WakeupStatus::Submitted {
                break st;
            }
            assert!(
                Instant::now() < deadline,
                "timed out waiting for Submitted wakeup status; last: {:?}",
                st.wakeup
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        };
        let first_claim_id = first_submitted
            .wakeup
            .claim_id
            .clone()
            .expect("first Submitted wakeup must retain its claim_id");
        let first_covered_up_to_seq = first_submitted.wakeup.covered_up_to_seq;
        let first_attempt_count = first_submitted.wakeup.attempt_count;
        let first_coalesced_count = first_submitted.wakeup.coalesced_count;
        assert_eq!(
            first_attempt_count, 1,
            "the first claimed wakeup is lifetime attempt 1"
        );
        assert!(
            first_covered_up_to_seq > prior_acked_seq,
            "first claim coverage must extend beyond the prior acknowledged cursor"
        );

        let expected_msg = format!(
            "Threadlane approved session {approval_id} has pending events. Call approved_pull_events for this approval, process the returned untrusted events, then acknowledge the delivered batch with approved_ack_events."
        );
        let log_contents = fs::read_to_string(&codex_log).expect("read codex invocation log");
        let lines: Vec<&str> = log_contents.lines().collect();
        assert_eq!(
            lines.len(),
            1,
            "multiple worker events must coalesce into a single wakeup while unacknowledged"
        );
        assert_eq!(
            lines[0],
            format!("queue|--thread|{THREAD_UUID_A}|--message|{expected_msg}|5")
        );
        assert!(!lines[0].contains("CONFIDENTIAL_WORKER_TASK_INSTRUCTION"));
        assert!(!lines[0].contains("gemini-4-argon-high"));
        assert!(!lines[0].contains("Root AGY implementation"));

        // Read-only history (`list_activity`, `list_task_activity`, `get_task`) NEVER advances
        // delivered_seq or acked_seq and NEVER releases wake-up claims.
        let status_before_ro = scoped.status().await.expect("status before ro");
        let _ = client
            .list_activity(&approval_id, None, None, Some(10), None)
            .await
            .expect("list_activity");
        let _ = client
            .list_task_activity(&approval_id, "task-root-agy", None, None, Some(10), None)
            .await
            .expect("list_task_activity");
        let _ = client
            .get_task(&approval_id, "task-root-agy")
            .await
            .expect("get_task");
        let status_after_ro = scoped.status().await.expect("status after ro");
        assert_eq!(
            status_after_ro.binding.delivered_seq,
            status_before_ro.binding.delivered_seq
        );
        assert_eq!(
            status_after_ro.binding.acked_seq,
            status_before_ro.binding.acked_seq
        );
        assert_eq!(
            status_after_ro.wakeup.status,
            threadlane_core::WakeupStatus::Submitted
        );
        assert_eq!(
            status_after_ro.wakeup.claim_id,
            Some(first_claim_id.clone())
        );

        // The first claim can already cover every event emitted by `task-root-agy`. Start a real
        // scoped demo run on the same host session after the claim so later important events exist,
        // and prove they coalesce into the existing unacknowledged claim without another invocation.
        let extra_demo_instruction =
            "CONFIDENTIAL_EXTRA_DEMO_PROSE_DO_NOT_LEAK --model gemini-4-argon-high";
        let extra_demo = scoped
            .start_demo(
                "notify-e2e-extra-demo-start".to_string(),
                extra_demo_instruction.to_string(),
                Some(10),
            )
            .await
            .expect("start extra scoped demo run");
        let demo_deadline = Instant::now() + Duration::from_secs(5);
        let extra_demo_waiting = loop {
            let run = scoped
                .get_run(extra_demo.run_id.clone())
                .await
                .expect("get_run extra demo");
            if run.status == threadlane_core::RunStatus::WaitingInput {
                break run;
            }
            assert!(
                Instant::now() < demo_deadline,
                "timed out waiting for extra demo WaitingInput; last run: {run:?}"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        };
        assert_eq!(extra_demo_waiting.revision, 1);
        assert!(extra_demo_waiting.active_question_id.is_some());
        tokio::time::sleep(Duration::from_millis(60)).await;

        let coalesced_status = scoped.status().await.expect("status after extra demo");
        assert_eq!(
            coalesced_status.wakeup.status,
            threadlane_core::WakeupStatus::Submitted
        );
        assert_eq!(
            coalesced_status.wakeup.claim_id.as_deref(),
            Some(first_claim_id.as_str())
        );
        assert_eq!(
            coalesced_status.wakeup.covered_up_to_seq,
            first_covered_up_to_seq
        );
        assert_eq!(
            coalesced_status.wakeup.attempt_count, first_attempt_count,
            "coalesced important events must not increment the lifetime attempt count"
        );
        assert!(
            coalesced_status.wakeup.latest_important_seq > first_covered_up_to_seq,
            "extra demo must emit a later important event beyond the first claim coverage"
        );
        assert!(
            coalesced_status.wakeup.coalesced_count > first_coalesced_count,
            "later important events must remain coalesced into the unacknowledged claim"
        );

        let log_after_coalescing =
            fs::read_to_string(&codex_log).expect("read codex log after coalescing");
        assert_eq!(
            log_after_coalescing.lines().count(),
            1,
            "later important events must coalesce into the existing single unacknowledged wakeup"
        );
        assert!(!log_after_coalescing.contains("CONFIDENTIAL_EXTRA_DEMO_PROSE_DO_NOT_LEAK"));

        // Acknowledge exactly through the first claim's coverage. The bounded count is derived from
        // the real prior acknowledged cursor, and the resulting batch must end at that exact seq.
        let first_claim_span = first_covered_up_to_seq - prior_acked_seq;
        assert!(
            (1..=APPROVED_EVENT_BATCH_MAX_EVENTS as u64).contains(&first_claim_span),
            "first claim span {first_claim_span} must fit one approved batch (1..={APPROVED_EVENT_BATCH_MAX_EVENTS})"
        );
        let batch_part1 = client
            .pull_events(
                &approval_id,
                Some(prior_acked_seq),
                Some(first_claim_span as usize),
            )
            .await
            .expect("pull through first claim coverage");
        assert_eq!(batch_part1.from_seq, prior_acked_seq + 1);
        assert_eq!(
            batch_part1.to_seq, first_covered_up_to_seq,
            "partial pull must end exactly at the first claim's covered_up_to_seq"
        );
        assert_eq!(batch_part1.events.len() as u64, first_claim_span);
        let first_ack = client
            .ack_events(&approval_id, batch_part1.batch_id.clone())
            .await
            .expect("ack first claim coverage");
        assert!(first_ack.advanced);
        assert_eq!(first_ack.acked_seq, first_covered_up_to_seq);

        let deadline_second = Instant::now() + Duration::from_secs(5);
        let second_submitted = loop {
            let st = scoped
                .status()
                .await
                .expect("scoped status after first coverage ack");
            if st.wakeup.status == threadlane_core::WakeupStatus::Submitted
                && st.wakeup.claim_id.as_deref() != Some(first_claim_id.as_str())
            {
                break st;
            }
            assert!(
                Instant::now() < deadline_second,
                "timed out waiting for the second Submitted wakeup claim; last: {:?}",
                st.wakeup
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        };
        let second_claim_id = second_submitted
            .wakeup
            .claim_id
            .clone()
            .expect("second Submitted wakeup must retain its claim_id");
        assert_ne!(second_claim_id, first_claim_id);
        assert_eq!(
            second_submitted.wakeup.attempt_count,
            first_attempt_count + 1,
            "attempt_count is cumulative across the binding lifetime"
        );
        assert!(
            second_submitted.wakeup.covered_up_to_seq > first_covered_up_to_seq,
            "second claim coverage must include the later important events"
        );
        let second_covered_up_to_seq = second_submitted.wakeup.covered_up_to_seq;

        let log_after_second =
            fs::read_to_string(&codex_log).expect("read codex log after second wakeup");
        let second_lines: Vec<&str> = log_after_second.lines().collect();
        assert_eq!(
            second_lines.len(),
            2,
            "releasing the first claim must produce exactly one additional wakeup invocation"
        );
        assert_eq!(
            second_lines[1],
            format!("queue|--thread|{THREAD_UUID_A}|--message|{expected_msg}|5")
        );
        assert!(!second_lines[1].contains("CONFIDENTIAL_EXTRA_DEMO_PROSE_DO_NOT_LEAK"));
        assert!(!second_lines[1].contains("gemini-4-argon-high"));
        assert!(!second_lines[1].contains("Root AGY implementation"));

        // Stop the extra demo while the second claim is still Submitted. The demo cancel endpoint
        // is revision-free, so fetch the real current Run first and bind the deterministic
        // idempotency key to its actual revision before cleaning up the fixture.
        let extra_demo_current = scoped
            .get_run(extra_demo_waiting.run_id.clone())
            .await
            .expect("get_run before cancelling extra demo");
        assert_eq!(
            extra_demo_current.status,
            threadlane_core::RunStatus::WaitingInput
        );
        assert_eq!(extra_demo_current.revision, extra_demo_waiting.revision);
        let cancelled_extra_demo = scoped
            .cancel(
                format!(
                    "notify-e2e-extra-demo-cancel-r{}",
                    extra_demo_current.revision
                ),
                extra_demo_current.run_id.clone(),
                Some("notification coalescing fixture cleanup".to_string()),
            )
            .await
            .expect("cancel extra demo before final ack");
        assert_eq!(
            cancelled_extra_demo.status,
            threadlane_core::RunStatus::Cancelled
        );
        assert_eq!(cancelled_extra_demo.revision, extra_demo_current.revision);
        let status_after_cancel = scoped
            .status()
            .await
            .expect("status after extra demo cancel");
        assert_eq!(
            status_after_cancel.wakeup.status,
            threadlane_core::WakeupStatus::Submitted
        );
        assert_eq!(
            status_after_cancel.wakeup.claim_id.as_deref(),
            Some(second_claim_id.as_str())
        );
        assert_eq!(
            status_after_cancel.wakeup.attempt_count,
            first_attempt_count + 1
        );
        assert_eq!(
            fs::read_to_string(&codex_log)
                .expect("read codex log after extra demo cancel")
                .lines()
                .count(),
            2,
            "cancelling the extra demo is not important and must not wake again"
        );

        let batch_rest = client
            .pull_events(&approval_id, None, Some(APPROVED_EVENT_BATCH_MAX_EVENTS))
            .await
            .expect("pull remaining events");
        assert_eq!(batch_rest.from_seq, first_covered_up_to_seq + 1);
        assert!(!batch_rest.events.is_empty());
        assert!(batch_rest.to_seq >= second_covered_up_to_seq);
        let ack_rest = client
            .ack_events(&approval_id, batch_rest.batch_id)
            .await
            .expect("ack remaining events");
        assert_eq!(ack_rest.acked_seq, batch_rest.to_seq);

        let st_idle = scoped.status().await.expect("status after full ack");
        assert_eq!(st_idle.wakeup.status, threadlane_core::WakeupStatus::Idle);
        assert_eq!(st_idle.wakeup.claim_id, None);
        assert_eq!(st_idle.binding.acked_seq, batch_rest.to_seq);

        host_on.shutdown().await.expect("shutdown host_on");

        // 3. Restarting without `.with_notifications(...)` must NOT silently restore notifications
        //    from the prior manifest; it resets to disabled (`enabled: false`).
        let host_restarted_default = ApprovedHost::start(
            ApprovedHostConfig::new(
                &fixture.registry_dir,
                &fixture.plans_dir,
                &fixture.authority_dir,
                &fixture.sessions_dir,
                &approval_id,
            )
            .with_tick_interval(Duration::from_millis(20)),
        )
        .await
        .expect("restart host with default config");
        assert_eq!(
            host_restarted_default.summary().notifications,
            HostNotificationSummary::disabled()
        );
        tokio::time::sleep(Duration::from_millis(60)).await;
        assert_eq!(
            fs::read_to_string(&codex_log)
                .expect("read codex log after default restart")
                .lines()
                .count(),
            2,
            "restarting without explicit opt-in must not restore notifications"
        );
        host_restarted_default
            .shutdown()
            .await
            .expect("shutdown restarted host");
    }

    #[tokio::test]
    async fn host_notification_timeout_records_unknown_without_replay_and_worker_completes() {
        let fixture = setup_fixture();
        let approval_id = create_approved_plan(
            &fixture,
            "plan-notify-timeout",
            BINDING_A,
            THREAD_UUID_A,
            default_tasks("Timeout notification test"),
        );

        let slow_codex_log = fixture.root.join("slow-codex-host.log");
        let slow_codex = fixture.root.join("bin/slow-codex-host");
        write_fake_codex_script(&slow_codex, &slow_codex_log, Some(30));

        let notify_cfg =
            ApprovedHostNotificationConfig::new(slow_codex, 80).expect("80ms timeout config");

        let host = ApprovedHost::start(
            ApprovedHostConfig::new(
                &fixture.registry_dir,
                &fixture.plans_dir,
                &fixture.authority_dir,
                &fixture.sessions_dir,
                &approval_id,
            )
            .with_tick_interval(Duration::from_millis(15))
            .with_notifications(notify_cfg),
        )
        .await
        .expect("start host with slow codex");

        let client = ApprovedHostClient::open(
            &fixture.registry_dir,
            &fixture.plans_dir,
            &fixture.authority_dir,
            &fixture.sessions_dir,
            BINDING_A,
            THREAD_UUID_A,
        )
        .expect("open client");

        client
            .dispatch_task(&approval_id, "task-root-agy")
            .await
            .expect("dispatch task-root-agy");

        let completed = wait_for_task_status(&client, &approval_id, "task-root-agy", |st| {
            st == TaskExecutionStatus::ReadyForReview
        })
        .await;
        assert_eq!(completed.status, TaskExecutionStatus::ReadyForReview);
        assert!(!completed.accepted);

        let scoped = threadlane::ScopedClient::from_state_dir(
            host.runtime_state_dir(),
            host.session_binding_id(),
        )
        .expect("open scoped client");
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let st = scoped.status().await.expect("scoped status");
            if st.wakeup.status == threadlane_core::WakeupStatus::Unknown {
                assert_eq!(st.wakeup.attempt_count, 1);
                break;
            }
            assert!(
                Instant::now() < deadline,
                "timed out waiting for Unknown wakeup status; last: {:?}",
                st.wakeup
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        // Wait several ticks to prove Unknown wake-up claim is never automatically replayed.
        tokio::time::sleep(Duration::from_millis(100)).await;
        let st_after = scoped.status().await.expect("scoped status after wait");
        assert_eq!(
            st_after.wakeup.status,
            threadlane_core::WakeupStatus::Unknown
        );
        assert_eq!(st_after.wakeup.attempt_count, 1);
        let logged = fs::read_to_string(&slow_codex_log).expect("read slow codex log");
        assert_eq!(logged.lines().count(), 1);

        host.shutdown().await.expect("shutdown host");
    }
}
