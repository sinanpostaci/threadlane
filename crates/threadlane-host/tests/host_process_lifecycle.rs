#![cfg(unix)]

//! Real process lifecycle regression for the `threadlane-host` CLI.
//!
//! This test spawns the actual binary built for the integration target, waits for
//! authenticated host-control readiness, stops the host through the public client,
//! verifies the real child exits naturally with status 0, and then performs an
//! explicit stopped-session restart. In-process `run_serve` calls cannot prove
//! process ownership or `host.lock` release, so the assertions here are tied to
//! the spawned `std::process::Child` and durable session state.

use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::Arc;
use std::time::Duration;

use tempfile::TempDir;
use threadlane_authority::{ApprovalRecord, AuthorityStore, OperatorApprovalRequest};
use threadlane_host::{
    is_host_lock_held, read_host_manifest_optional, read_task_dispatch_intent, read_task_receipt,
    ApprovedHostClient, HostControlRuntimeState, HostControlStatusResponse, HostError, HostPhase,
    HostSessionSummary, WORK_AREA_SUBDIR,
};
use threadlane_plans::{
    compile_plan_revision, Difficulty, ModelChoice, ModelDefinition, PlanChange, PlanStore,
    PlanTask, RoleAssignment, ToolAdapter, ToolDefinition, WorkflowProfile, WorkflowRole,
};
use threadlane_registry::{RegistryChange, RegistryStore};
use threadlane_workers::{
    resolve_exact_commit_sha, run_sanitized_git, ExecutionBudgets, WorkAreaOwnershipMarker,
    WorktreeManager, CHECKOUTS_SUBDIR, DEFAULT_GIT_COMMAND_TIMEOUT_MS, MANIFESTS_SUBDIR,
    WORK_AREA_MARKER_FILENAME,
};

const THREAD_UUID: &str = "550e8400-e29b-41d4-a716-446655440000";
const CONTROL_BINDING_ID: &str = "bnd_process_lifecycle";
const PLAN_ID: &str = "plan-host-process-lifecycle";
const TASK_ID: &str = "task-process-agy";
const START_TIMEOUT: Duration = Duration::from_secs(30);
const EXIT_TIMEOUT: Duration = Duration::from_secs(10);
const POLL_INTERVAL: Duration = Duration::from_millis(25);

struct HostProcessFixture {
    _tmp: TempDir,
    registry_dir: PathBuf,
    plans_dir: PathBuf,
    authority_dir: PathBuf,
    sessions_dir: PathBuf,
    unexpected_invocations: PathBuf,
    logs_dir: PathBuf,
    approval_id: String,
    authority: AuthorityStore,
}

impl HostProcessFixture {
    fn session_dir(&self) -> PathBuf {
        self.sessions_dir.join(&self.approval_id)
    }

    fn approval_record(&self) -> ApprovalRecord {
        self.authority
            .get_for_operator(&self.approval_id)
            .expect("load persisted approval record")
    }

    fn assert_no_provider_invocation(&self) {
        assert!(
            !self.unexpected_invocations.exists(),
            "fake AGY provider was invoked unexpectedly"
        );
    }
}

struct HostChild {
    child: Child,
    label: &'static str,
}

impl HostChild {
    fn pid(&self) -> u32 {
        self.child.id()
    }

    fn assert_running(&mut self) {
        if let Some(status) = self
            .child
            .try_wait()
            .expect("poll real host CLI child before lifecycle assertion")
        {
            panic!(
                "{} real host CLI child exited before lifecycle assertion with {status:?}",
                self.label
            );
        }
    }

    async fn wait_for_exit(&mut self, timeout: Duration) -> ExitStatus {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if let Some(status) = self
                .child
                .try_wait()
                .expect("poll real host CLI child exit")
            {
                return status;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "{} real host CLI child did not exit within {timeout:?}",
                self.label
            );
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    }
}

impl Drop for HostChild {
    fn drop(&mut self) {
        // Cleanup runs only when an assertion unwinds before the natural exit is observed.
        if matches!(self.child.try_wait(), Ok(Some(_))) {
            return;
        }
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn init_disposable_git_repo(repo_dir: &Path) -> String {
    fs::create_dir_all(repo_dir.join("src")).expect("create disposable repo src");
    fs::write(repo_dir.join("src/lib.rs"), "pub fn alpha() -> u32 { 1 }\n")
        .expect("write disposable repo src/lib.rs");

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
    run_sanitized_git(repo_dir, &["add", "."], DEFAULT_GIT_COMMAND_TIMEOUT_MS).expect("git add");
    run_sanitized_git(
        repo_dir,
        &["commit", "-m", "initial commit", "--quiet"],
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .expect("git commit");

    resolve_exact_commit_sha(repo_dir, "HEAD", DEFAULT_GIT_COMMAND_TIMEOUT_MS)
        .expect("resolve HEAD")
}

fn write_unexpected_provider_script(script_path: &Path) {
    // The host must never invoke this ignored provider: any invocation records evidence
    // and exits 97 instead of pretending to execute an approved task.
    let script = r#"#!/bin/sh
set -eu
marker="${0%/*}/unexpected-agy-invocations.txt"
printf 'unexpected fake AGY invocation\n' >> "$marker"
printf 'unexpected fake AGY invocation\n' >&2
exit 97
"#;

    let mut file = File::create(script_path).expect("create fake AGY executable");
    file.write_all(script.as_bytes())
        .expect("write fake AGY executable");
    file.sync_all().expect("sync fake AGY executable");
    drop(file);
    fs::set_permissions(script_path, fs::Permissions::from_mode(0o755))
        .expect("chmod fake AGY executable");
}

fn private_log_file(path: &Path) -> File {
    let file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(path)
        .unwrap_or_else(|err| panic!("create private fixture log `{}`: {err}", path.display()));
    file.set_permissions(fs::Permissions::from_mode(0o600))
        .expect("chmod private fixture log");
    file
}

fn setup_host_process_fixture() -> HostProcessFixture {
    let tmp = TempDir::new().expect("create private fixture tempdir");
    let root = fs::canonicalize(tmp.path()).expect("canonicalize fixture tempdir");
    fs::set_permissions(&root, fs::Permissions::from_mode(0o700))
        .expect("chmod fixture tempdir 0700");

    let repo_dir = root.join("repo");
    let base_commit = init_disposable_git_repo(&repo_dir);

    let bin_dir = root.join("bin");
    fs::create_dir_all(&bin_dir).expect("create fixture bin dir");
    fs::set_permissions(&bin_dir, fs::Permissions::from_mode(0o700))
        .expect("chmod fixture bin dir 0700");
    let fake_agy_exe = bin_dir.join("fake-agy");
    let unexpected_invocations = bin_dir.join("unexpected-agy-invocations.txt");
    write_unexpected_provider_script(&fake_agy_exe);

    let registry_dir = root.join("registry-store");
    let plans_dir = root.join("plans-store");
    let authority_dir = root.join("authority-store");
    let sessions_dir = root.join("sessions-store");
    let logs_dir = root.join("logs");
    fs::create_dir_all(&logs_dir).expect("create fixture log dir");
    fs::set_permissions(&logs_dir, fs::Permissions::from_mode(0o700))
        .expect("chmod fixture log dir 0700");

    let registry = Arc::new(RegistryStore::open(&registry_dir).expect("open registry store"));
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
            RegistryChange::UpsertModel {
                model: ModelDefinition {
                    id: "model-argon".to_string(),
                    tool_id: "tool-agy".to_string(),
                    label: "Gemini 4 Argon High".to_string(),
                    model_arg: "gemini-4-argon-high".to_string(),
                    efforts: vec!["high".to_string()],
                    allow_no_effort: false,
                },
            },
        )
        .expect("upsert model-argon");
    registry
        .apply(
            2,
            RegistryChange::UpsertProfile {
                profile: WorkflowProfile {
                    decision_advisor: None,
                    id: "profile-process".to_string(),
                    name: "Process Lifecycle Coder".to_string(),
                    assignments: vec![RoleAssignment {
                        role: WorkflowRole::Coder,
                        difficulty: Some(Difficulty::Hard),
                        choice: ModelChoice {
                            model_id: "model-argon".to_string(),
                            effort: Some("high".to_string()),
                        },
                    }],
                },
            },
        )
        .expect("upsert profile-process");

    let registry_snapshot = registry.snapshot().expect("snapshot registry");
    assert_eq!(registry_snapshot.revision, 3);

    let plans = Arc::new(PlanStore::open(&plans_dir).expect("open plan store"));
    let draft = plans
        .change_at(
            0,
            PlanChange::SaveDraft {
                plan_id: PLAN_ID.to_string(),
                binding_id: CONTROL_BINDING_ID.to_string(),
                profile_id: "profile-process".to_string(),
                profile_revision: registry_snapshot.revision,
                summary: "Process lifecycle plan".to_string(),
                tasks: vec![PlanTask {
                    id: TASK_ID.to_string(),
                    title: "Single fake AGY task".to_string(),
                    instruction: "Edit src/lib.rs only when explicitly dispatched.".to_string(),
                    role: WorkflowRole::Coder,
                    difficulty: Difficulty::Hard,
                    depends_on: Vec::new(),
                    allowed_write_paths: vec!["src/lib.rs".to_string()],
                }],
            },
            1_700_000_000_000,
        )
        .expect("save draft plan");
    let published = plans
        .change(
            draft.revision,
            PlanChange::Publish {
                plan_id: PLAN_ID.to_string(),
                revision: draft.revision,
            },
        )
        .expect("publish plan");
    let compiled =
        compile_plan_revision(&published, &registry_snapshot).expect("compile published plan");

    let authority = AuthorityStore::open(&authority_dir, Arc::clone(&registry), Arc::clone(&plans))
        .expect("open authority store");
    let approval = authority
        .approve_at(
            OperatorApprovalRequest {
                binding_id: CONTROL_BINDING_ID.to_string(),
                thread_uuid: THREAD_UUID.to_string(),
                plan_id: PLAN_ID.to_string(),
                plan_revision: published.revision,
                expected_registry_revision: registry_snapshot.revision,
                expected_plan_digest: compiled.plan_digest,
                repository_path: repo_dir,
                base_commit,
                approved_task_ids: vec![TASK_ID.to_string()],
                approved_write_paths: vec!["src/lib.rs".to_string()],
                max_parallel: 1,
                budgets: ExecutionBudgets::default(),
            },
            1_700_000_001_000,
        )
        .expect("persist operator approval");
    assert_eq!(approval.plan_id.as_str(), PLAN_ID);
    assert_eq!(approval.worker_profile_count, 1);
    assert_eq!(approval.native_route_count, 0);

    HostProcessFixture {
        _tmp: tmp,
        registry_dir,
        plans_dir,
        authority_dir,
        sessions_dir,
        unexpected_invocations,
        logs_dir,
        approval_id: approval.approval_id,
        authority,
    }
}

fn spawn_host_process(
    fixture: &HostProcessFixture,
    label: &'static str,
    restart_from_instance_id: Option<&str>,
) -> HostChild {
    let stdout_path = fixture.logs_dir.join(format!("{label}.stdout.jsonl"));
    let stderr_path = fixture.logs_dir.join(format!("{label}.stderr.log"));
    let stdout = private_log_file(&stdout_path);
    let stderr = private_log_file(&stderr_path);
    for path in [&stdout_path, &stderr_path] {
        let mode = fs::symlink_metadata(path)
            .expect("stat private fixture log")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o600, "fixture logs must be private");
    }

    let mut command = Command::new(env!("CARGO_BIN_EXE_threadlane-host"));
    command
        .arg("serve")
        .arg("--registry-dir")
        .arg(&fixture.registry_dir)
        .arg("--plans-dir")
        .arg(&fixture.plans_dir)
        .arg("--authority-dir")
        .arg(&fixture.authority_dir)
        .arg("--sessions-dir")
        .arg(&fixture.sessions_dir)
        .arg("--approval-id")
        .arg(&fixture.approval_id)
        .stdin(Stdio::null())
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr));
    if let Some(prior_instance_id) = restart_from_instance_id {
        command
            .arg("--restart-from-instance-id")
            .arg(prior_instance_id);
    }

    let child = command
        .spawn()
        .expect("spawn actual threadlane-host CLI process");
    HostChild { child, label }
}

async fn wait_for_authenticated_ready(
    client: &ApprovedHostClient,
    approval_id: &str,
    child: &mut HostChild,
    accept_instance: impl Fn(&str) -> bool,
) -> HostControlStatusResponse {
    let label = child.label;
    let deadline = tokio::time::Instant::now() + START_TIMEOUT;
    loop {
        child.assert_running();
        match client.host_control_status(approval_id).await {
            Ok(Some(status)) => {
                assert_eq!(
                    status.state,
                    HostControlRuntimeState::Ready,
                    "{label} reported a non-ready host-control state"
                );
                assert_eq!(status.approval_id.as_str(), approval_id);
                assert!(
                    accept_instance(&status.instance_id),
                    "{label} observed unexpected host instance `{}`",
                    status.instance_id
                );
                assert!(
                    status.idle,
                    "{label} authenticated host control is not idle"
                );
                assert!(
                    status.busy.is_idle(),
                    "{label} authenticated host control reports busy counts: {:?}",
                    status.busy
                );
                return status;
            }
            Ok(None) => {}
            Err(_) => {}
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "{label} did not reach authenticated host-control readiness within {START_TIMEOUT:?}"
        );
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

fn assert_live_ready(
    fixture: &HostProcessFixture,
    client: &ApprovedHostClient,
    child: &mut HostChild,
    status: &HostControlStatusResponse,
) {
    child.assert_running();

    let summary = client
        .session_summary(&fixture.approval_id)
        .expect("read live ready session summary");
    assert_eq!(summary.phase, HostPhase::Ready);
    assert_eq!(summary.plan_id.as_str(), PLAN_ID);
    assert_eq!(summary.plan_revision, 1);
    assert_eq!(summary.binding_id.as_str(), CONTROL_BINDING_ID);
    assert_eq!(summary.thread_uuid.as_str(), THREAD_UUID);
    assert_eq!(
        summary.instance_id.as_deref(),
        Some(status.instance_id.as_str())
    );
    assert_eq!(
        summary.session_binding_id.as_deref(),
        Some(status.session_binding_id.as_str())
    );
    assert!(summary.gates.supervisor_authenticated_ready);
    assert!(!summary.gates.workers_auto_started_on_boot);
    assert!(!summary.notifications.enabled);
    assert!(summary.notifications.executable_digest.is_none());
    assert!(summary.notifications.timeout_ms.is_none());
    assert!(summary.last_error.is_none());

    assert_eq!(status.control_binding_id.as_str(), CONTROL_BINDING_ID);
    assert_eq!(status.thread_uuid.as_str(), THREAD_UUID);
    assert!(
        is_host_lock_held(&fixture.session_dir()).expect("probe live host.lock"),
        "live host must hold host.lock"
    );

    let record = fixture.approval_record();
    let manifest = read_host_manifest_optional(&fixture.session_dir(), &record)
        .expect("read live host manifest")
        .expect("live host manifest exists");
    assert_eq!(manifest.phase, HostPhase::Ready);
    assert_eq!(manifest.control_binding_id.as_str(), CONTROL_BINDING_ID);
    assert_eq!(manifest.thread_uuid.as_str(), THREAD_UUID);
    assert_eq!(
        manifest.instance_id.as_deref(),
        Some(status.instance_id.as_str())
    );
    assert_eq!(
        manifest.session_binding_id.as_deref(),
        Some(status.session_binding_id.as_str())
    );
    assert_eq!(manifest.pid, Some(child.pid()));
    assert!(manifest.gates.supervisor_authenticated_ready);
    assert!(!manifest.gates.workers_auto_started_on_boot);
    assert!(!manifest.notifications.enabled);
    assert!(manifest.last_error.is_none());

    fixture.assert_no_provider_invocation();
    let work_area_dir = fixture.session_dir().join(WORK_AREA_SUBDIR);
    let marker_path = work_area_dir.join(WORK_AREA_MARKER_FILENAME);
    let marker_bytes =
        fs::read(&marker_path).expect("read Ready process .threadlane-work-area.json");
    let marker: WorkAreaOwnershipMarker =
        serde_json::from_slice(&marker_bytes).expect("parse Ready process WorkAreaOwnershipMarker");
    let manager =
        WorktreeManager::new(&record.repository_path, &work_area_dir, &record.base_commit)
            .expect("WorktreeManager::new on Ready process work-area");
    assert_eq!(marker.marker_version, 1);
    assert_eq!(marker.work_area_root, manager.work_area_root());
    assert_eq!(
        marker.repository_common_git_dir,
        manager.repository_common_git_dir()
    );
    assert_eq!(manager.resolved_base_commit(), record.base_commit);
    assert!(work_area_dir.join(MANIFESTS_SUBDIR).is_dir());
    assert!(work_area_dir.join(CHECKOUTS_SUBDIR).join("runs").is_dir());
    assert!(work_area_dir.join(CHECKOUTS_SUBDIR).join("tasks").is_dir());

    child.assert_running();
}

fn assert_durable_stopped(
    fixture: &HostProcessFixture,
    expected_instance_id: &str,
    expected_session_binding_id: &str,
    summary: &HostSessionSummary,
) {
    assert_eq!(summary.phase, HostPhase::Stopped);
    assert_eq!(summary.instance_id.as_deref(), Some(expected_instance_id));
    assert_eq!(
        summary.session_binding_id.as_deref(),
        Some(expected_session_binding_id)
    );
    assert!(!summary.gates.supervisor_authenticated_ready);
    assert!(!summary.notifications.enabled);
    assert!(summary.last_error.is_none());
    assert!(
        !is_host_lock_held(&fixture.session_dir()).expect("probe stopped host.lock"),
        "stopped host must release host.lock"
    );

    let record = fixture.approval_record();
    let manifest = read_host_manifest_optional(&fixture.session_dir(), &record)
        .expect("read stopped host manifest")
        .expect("stopped host manifest exists");
    assert_eq!(manifest.phase, HostPhase::Stopped);
    assert_eq!(manifest.instance_id.as_deref(), Some(expected_instance_id));
    assert_eq!(
        manifest.session_binding_id.as_deref(),
        Some(expected_session_binding_id)
    );
    assert!(manifest.pid.is_none());
    assert!(!manifest.gates.supervisor_authenticated_ready);
    assert!(!manifest.notifications.enabled);
    assert!(manifest.last_error.is_none());
}

fn assert_no_task_replay(fixture: &HostProcessFixture, session_binding_id: &str) {
    let record = fixture.approval_record();
    let session_dir = fixture.session_dir();
    let intent = read_task_dispatch_intent(&session_dir, &record, session_binding_id, TASK_ID)
        .expect("read task dispatch intent");
    assert!(
        intent.is_none(),
        "restart must not create or replay a task dispatch intent"
    );
    let receipt = read_task_receipt(&session_dir, &record, session_binding_id, TASK_ID)
        .expect("read task receipt");
    assert!(
        receipt.is_none(),
        "restart must not create or replay a task receipt"
    );
}

#[tokio::test]
async fn real_cli_process_idle_stop_and_restart_release_host_lock_without_task_replay() {
    let fixture = setup_host_process_fixture();
    fixture.assert_no_provider_invocation();

    let client = ApprovedHostClient::open(
        &fixture.registry_dir,
        &fixture.plans_dir,
        &fixture.authority_dir,
        &fixture.sessions_dir,
        CONTROL_BINDING_ID,
        THREAD_UUID,
    )
    .expect("open authoritative approved host client");

    let mut first = spawn_host_process(&fixture, "first", None);
    let first_status =
        wait_for_authenticated_ready(&client, &fixture.approval_id, &mut first, |_| true).await;
    first.assert_running();
    assert_live_ready(&fixture, &client, &mut first, &first_status);
    fixture.assert_no_provider_invocation();

    let first_instance_id = first_status.instance_id.clone();
    let first_session_binding_id = first_status.session_binding_id.clone();

    let stopped = client
        .stop_idle_session(&fixture.approval_id, &first_instance_id)
        .await
        .expect("authenticated idle stop must complete durably");
    let first_exit = first.wait_for_exit(EXIT_TIMEOUT).await;
    assert_eq!(
        first_exit.code(),
        Some(0),
        "first real host CLI process must exit naturally with status 0"
    );
    assert_durable_stopped(
        &fixture,
        &first_instance_id,
        &first_session_binding_id,
        &stopped,
    );
    fixture.assert_no_provider_invocation();

    let duplicate = client
        .stop_idle_session(&fixture.approval_id, &first_instance_id)
        .await
        .expect("duplicate idle stop must be idempotent after durable stop");
    assert_eq!(duplicate.phase, HostPhase::Stopped);
    assert_eq!(
        duplicate.instance_id.as_deref(),
        Some(first_instance_id.as_str())
    );
    assert!(!duplicate.notifications.enabled);
    assert!(
        !is_host_lock_held(&fixture.session_dir()).expect("probe host.lock after duplicate stop"),
        "duplicate stop must leave host.lock released"
    );

    let mut second = spawn_host_process(&fixture, "restart", Some(&first_instance_id));
    let second_status =
        wait_for_authenticated_ready(&client, &fixture.approval_id, &mut second, |instance_id| {
            instance_id != first_instance_id.as_str()
        })
        .await;
    second.assert_running();
    assert_ne!(second_status.instance_id, first_instance_id);
    assert_eq!(
        second_status.control_binding_id,
        first_status.control_binding_id
    );
    assert_eq!(second_status.thread_uuid, first_status.thread_uuid);
    assert_eq!(
        second_status.session_binding_id, first_session_binding_id,
        "restart must preserve the single runtime session binding"
    );
    assert_eq!(
        second_status.subject_digest, first_status.subject_digest,
        "restart must preserve the approved subject"
    );
    assert_live_ready(&fixture, &client, &mut second, &second_status);
    assert_no_task_replay(&fixture, &second_status.session_binding_id);
    fixture.assert_no_provider_invocation();

    let stale_stop = client
        .stop_idle_session(&fixture.approval_id, &first_instance_id)
        .await;
    match stale_stop {
        Err(HostError::InstanceMismatch {
            approval_id,
            expected_instance_id,
            actual_instance_id,
        }) => {
            assert_eq!(approval_id, fixture.approval_id);
            assert_eq!(expected_instance_id, first_instance_id);
            assert_eq!(
                actual_instance_id.as_deref(),
                Some(second_status.instance_id.as_str())
            );
        }
        other => {
            panic!("expected HostError::InstanceMismatch for stale instance stop, got {other:?}")
        }
    }
    second.assert_running();
    let still_ready = client
        .host_control_status(&fixture.approval_id)
        .await
        .expect("query host control after stale-instance stop")
        .expect("restarted host control must remain configured");
    assert_eq!(still_ready.instance_id, second_status.instance_id);
    assert_eq!(still_ready.state, HostControlRuntimeState::Ready);
    assert!(still_ready.idle);
    assert!(still_ready.busy.is_idle());
    assert!(
        is_host_lock_held(&fixture.session_dir()).expect("probe host.lock after stale stop"),
        "stale-instance stop must not release the new host's lock"
    );
    fixture.assert_no_provider_invocation();

    let second_instance_id = second_status.instance_id.clone();
    let stopped_second = client
        .stop_idle_session(&fixture.approval_id, &second_instance_id)
        .await
        .expect("stop restarted host with rotated instance id");
    let second_exit = second.wait_for_exit(EXIT_TIMEOUT).await;
    assert_eq!(
        second_exit.code(),
        Some(0),
        "restarted real host CLI process must exit naturally with status 0"
    );
    assert_durable_stopped(
        &fixture,
        &second_instance_id,
        &first_session_binding_id,
        &stopped_second,
    );
    fixture.assert_no_provider_invocation();
}
