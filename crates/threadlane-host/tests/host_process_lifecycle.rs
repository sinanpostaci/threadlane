#![cfg(unix)]

//! Real process lifecycle regression for the `threadlane-host` CLI.
//!
//! This test spawns the actual binary built for the integration target, waits for
//! authenticated host-control readiness, stops the host through the public client,
//! verifies the real child exits naturally with status 0, and then performs an
//! explicit stopped-session restart. It also verifies that one completed root
//! worker result remains readable after a clean stop with unchanged durable files
//! and no provider relaunch. A stale `Ready` manifest left after both process locks
//! were released is also read as orphaned terminal history without rewriting durable
//! files or bypassing a still-held lock. A host killed only after its fake provider
//! child completed is read through residual WAL/SHM with the same no-rewrite and
//! no-relaunch guarantees. Missing-SHM and corrupt-WAL fixtures refuse snapshots
//! without required terminal evidence; they do not validate every WAL byte. In-process
//! `run_serve` calls cannot prove process ownership or `host.lock` release, so the
//! assertions here are tied to the spawned `std::process::Child` and durable session
//! state.

use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::Arc;
use std::time::Duration;

use fs2::FileExt;
use tempfile::TempDir;
use threadlane::presence::{
    WorkerPresenceAvailability, WorkerPresenceFreshness, WorkerPresenceSnapshot,
};
use threadlane_authority::{ApprovalRecord, AuthorityStore, OperatorApprovalRequest};
use threadlane_host::{
    is_host_lock_held, read_host_manifest_optional, read_task_dispatch_intent, read_task_receipt,
    ApprovedHostClient, DeliveryInspection, DeliveryKind, HostControlRuntimeState,
    HostControlStatusResponse, HostError, HostPhase, HostSessionSummary, TaskExecutionStatus,
    TaskStateSnapshot, RUNTIME_STATE_SUBDIR, TASK_RECEIPTS_FILENAME, WORK_AREA_SUBDIR,
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
const FAILED_TASK_ID: &str = "task-process-failed";
const PRESENCE_TASK_ID: &str = "task-process-presence";
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
    presence_started: PathBuf,
    presence_release: PathBuf,
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

    fn provider_invocation_count(&self) -> usize {
        match fs::read_to_string(&self.unexpected_invocations) {
            Ok(text) => text.lines().count(),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => 0,
            Err(err) => panic!("read fake AGY invocation marker: {err}"),
        }
    }

    fn assert_no_provider_invocation(&self) {
        assert_eq!(
            self.provider_invocation_count(),
            0,
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

struct PresenceReleaseGuard {
    path: PathBuf,
}

impl PresenceReleaseGuard {
    fn new(path: PathBuf) -> Self {
        Self { path }
    }

    fn release(&self) {
        fs::write(&self.path, b"release\n").expect("write presence release marker");
    }
}

impl Drop for PresenceReleaseGuard {
    fn drop(&mut self) {
        // Bounded fake child cleanup: release the marker even when an assertion unwinds.
        if !self.path.exists() {
            let _ = fs::write(&self.path, b"release\n");
        }
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

fn write_fake_provider_script(script_path: &Path) {
    // Every invocation is recorded so lifecycle reads can prove that no provider was launched.
    // A deliberately dispatched worker completes with matching model/effort telemetry and one
    // bounded in-scope edit; a prompt tagged with FAILED_DELIVERY_TASK fails instead. A prompt
    // tagged with PRESENCE_HOLD_TASK stays silent and live until the fixture release marker, with
    // a bounded timeout so cleanup can never leave an unbounded fake child. No host startup,
    // stop, restart, presence, or result/delivery read invokes it.
    let script = r#"#!/bin/sh
set -eu
marker="${0%/*}/unexpected-agy-invocations.txt"
printf 'fake AGY invocation\n' >> "$marker"
MODEL=""
EFFORT=""
PROMPT=""
while [ "$#" -gt 0 ]; do
    case "$1" in
        --model) MODEL="$2"; shift 2 ;;
        --effort) EFFORT="$2"; shift 2 ;;
        --print) PROMPT="$2"; shift 2 ;;
        *) shift 1 ;;
    esac
done
case "$PROMPT" in
    *PRESENCE_HOLD_TASK*)
        : > "${0%/*}/presence-child-started.txt"
        i=0
        while [ ! -f "${0%/*}/presence-child-release.txt" ]; do
            i=$((i + 1))
            if [ "$i" -ge 400 ]; then
                exit 19
            fi
            sleep 0.05
        done
        if [ -d "src" ]; then
            printf "// completed by fake AGY presence worker\npub fn alpha() -> u32 { 43 }\n" > "src/lib.rs"
        fi
        printf '{"type":"init","session_id":"sess-lifecycle-presence","model":"%s","effort":"%s"}\n' "$MODEL" "$EFFORT"
        printf '{"type":"result","subtype":"success","session_id":"sess-lifecycle-presence","model":"%s","effort":"%s"}\n' "$MODEL" "$EFFORT"
        exit 0
        ;;
    *FAILED_DELIVERY_TASK*)
        printf '{"type":"init","session_id":"sess-lifecycle-1","model":"%s","effort":"%s"}\n' "$MODEL" "$EFFORT"
        printf '{"type":"error","error":{"code":"fixture_failure","message":"fake AGY failed task"}}\n'
        exit 17
        ;;
esac
printf '{"type":"init","session_id":"sess-lifecycle-1","model":"%s","effort":"%s"}\n' "$MODEL" "$EFFORT"
if [ -d "src" ]; then
    printf "// completed by fake AGY\npub fn alpha() -> u32 { 42 }\n" > "src/lib.rs"
fi
printf '{"type":"result","subtype":"success","session_id":"sess-lifecycle-1","model":"%s","effort":"%s"}\n' "$MODEL" "$EFFORT"
exit 0
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
    setup_host_process_fixture_with_tasks(false, false)
}

fn setup_host_process_fixture_with_failed_task() -> HostProcessFixture {
    setup_host_process_fixture_with_tasks(true, false)
}

fn setup_host_presence_fixture() -> HostProcessFixture {
    setup_host_process_fixture_with_tasks(true, true)
}

fn setup_host_process_fixture_with_tasks(
    include_failed_task: bool,
    include_presence_task: bool,
) -> HostProcessFixture {
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
    let presence_started = bin_dir.join("presence-child-started.txt");
    let presence_release = bin_dir.join("presence-child-release.txt");
    write_fake_provider_script(&fake_agy_exe);

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
    let mut plan_tasks = vec![PlanTask {
        id: TASK_ID.to_string(),
        title: "Single fake AGY task".to_string(),
        instruction: "Edit src/lib.rs only when explicitly dispatched.".to_string(),
        role: WorkflowRole::Coder,
        difficulty: Difficulty::Hard,
        depends_on: Vec::new(),
        allowed_write_paths: vec!["src/lib.rs".to_string()],
    }];
    if include_failed_task {
        plan_tasks.push(PlanTask {
            id: FAILED_TASK_ID.to_string(),
            title: "Failed fake AGY task".to_string(),
            instruction: "FAILED_DELIVERY_TASK: stop unsuccessfully without editing files."
                .to_string(),
            role: WorkflowRole::Coder,
            difficulty: Difficulty::Hard,
            depends_on: Vec::new(),
            allowed_write_paths: vec!["src/lib.rs".to_string()],
        });
    }
    if include_presence_task {
        plan_tasks.push(PlanTask {
            id: PRESENCE_TASK_ID.to_string(),
            title: "Marker-gated fake AGY presence task".to_string(),
            instruction:
                "PRESENCE_HOLD_TASK: remain live until the fixture writes its release marker."
                    .to_string(),
            role: WorkflowRole::Coder,
            difficulty: Difficulty::Hard,
            depends_on: Vec::new(),
            allowed_write_paths: vec!["src/lib.rs".to_string()],
        });
    }
    let draft = plans
        .change_at(
            0,
            PlanChange::SaveDraft {
                plan_id: PLAN_ID.to_string(),
                binding_id: CONTROL_BINDING_ID.to_string(),
                profile_id: "profile-process".to_string(),
                profile_revision: registry_snapshot.revision,
                summary: "Process lifecycle plan".to_string(),
                tasks: plan_tasks,
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
    let mut approved_task_ids = vec![TASK_ID.to_string()];
    if include_failed_task {
        approved_task_ids.push(FAILED_TASK_ID.to_string());
    }
    if include_presence_task {
        approved_task_ids.push(PRESENCE_TASK_ID.to_string());
    }
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
                approved_task_ids,
                approved_write_paths: vec!["src/lib.rs".to_string()],
                max_parallel: 1,
                budgets: ExecutionBudgets::default(),
            },
            1_700_000_001_000,
        )
        .expect("persist operator approval");
    assert_eq!(approval.plan_id.as_str(), PLAN_ID);
    let expected_worker_profiles =
        1 + (if include_failed_task { 1 } else { 0 }) + (if include_presence_task { 1 } else { 0 });
    assert_eq!(approval.worker_profile_count, expected_worker_profiles);
    assert_eq!(approval.native_route_count, 0);

    HostProcessFixture {
        _tmp: tmp,
        registry_dir,
        plans_dir,
        authority_dir,
        sessions_dir,
        unexpected_invocations,
        presence_started,
        presence_release,
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

async fn wait_for_presence_child_started(fixture: &HostProcessFixture, child: &mut HostChild) {
    let deadline = tokio::time::Instant::now() + START_TIMEOUT;
    loop {
        child.assert_running();
        if fixture.presence_started.exists() {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "marker-gated fake presence child did not start within {START_TIMEOUT:?}"
        );
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

async fn wait_for_live_presence(
    client: &ApprovedHostClient,
    approval_id: &str,
    task_id: &str,
    child: &mut HostChild,
) -> WorkerPresenceSnapshot {
    let deadline = tokio::time::Instant::now() + START_TIMEOUT;
    loop {
        child.assert_running();
        if let Ok(Some(presence)) = client.get_task_presence(approval_id, task_id).await {
            if presence.availability == WorkerPresenceAvailability::Live
                && presence.freshness == WorkerPresenceFreshness::Fresh
                && presence.live
            {
                return presence;
            }
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "task `{task_id}` did not expose live worker presence within {START_TIMEOUT:?}"
        );
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

fn assert_no_live_presence(
    result: Result<Option<WorkerPresenceSnapshot>, HostError>,
    context: &str,
) {
    match result {
        Ok(None) => {}
        Ok(Some(presence)) => panic!(
            "{context}: unexpected live presence for run `{}`",
            presence.run_id
        ),
        Err(err) => panic!("{context}: unexpected presence error {err:?}"),
    }
}

fn assert_not_live_presence(
    result: Result<Option<WorkerPresenceSnapshot>, HostError>,
    context: &str,
) {
    if let Ok(Some(presence)) = result {
        panic!(
            "{context}: unexpected live presence for run `{}`",
            presence.run_id
        );
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

async fn wait_for_host_idle(
    client: &ApprovedHostClient,
    approval_id: &str,
    child: &mut HostChild,
) -> HostControlStatusResponse {
    let deadline = tokio::time::Instant::now() + START_TIMEOUT;
    loop {
        child.assert_running();
        if let Ok(Some(status)) = client.host_control_status(approval_id).await {
            if status.state == HostControlRuntimeState::Ready
                && status.idle
                && status.busy.is_idle()
            {
                return status;
            }
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "host did not become idle after the completed task within {START_TIMEOUT:?}"
        );
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

async fn wait_for_task_status(
    client: &ApprovedHostClient,
    approval_id: &str,
    task_id: &str,
    child: &mut HostChild,
    expected: TaskExecutionStatus,
) -> TaskStateSnapshot {
    let deadline = tokio::time::Instant::now() + START_TIMEOUT;
    loop {
        child.assert_running();
        let snapshot = client
            .get_task_live_or_stopped(approval_id, task_id)
            .await
            .expect("read live task result");
        if snapshot.status == expected {
            return snapshot;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "task `{task_id}` did not become {expected:?}: {:?}",
            snapshot.status
        );
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

async fn wait_for_task_ready(
    client: &ApprovedHostClient,
    approval_id: &str,
    task_id: &str,
    child: &mut HostChild,
) -> TaskStateSnapshot {
    wait_for_task_status(
        client,
        approval_id,
        task_id,
        child,
        TaskExecutionStatus::ReadyForReview,
    )
    .await
}

#[derive(Debug, PartialEq, Eq)]
struct StoppedReadFingerprint {
    manifest: Option<Vec<u8>>,
    receipts: Option<Vec<u8>>,
    database: Option<Vec<u8>>,
    wal: Option<Vec<u8>>,
    shm: Option<Vec<u8>>,
}

fn read_optional_bytes(path: &Path) -> Option<Vec<u8>> {
    match fs::read(path) {
        Ok(bytes) => Some(bytes),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => None,
        Err(err) => panic!("read fixture file `{}`: {err}", path.display()),
    }
}

fn write_private_fixture_file(path: &Path, bytes: &[u8]) {
    fs::write(path, bytes)
        .unwrap_or_else(|err| panic!("write fixture file `{}`: {err}", path.display()));
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))
        .unwrap_or_else(|err| panic!("chmod fixture file `{}`: {err}", path.display()));
}

fn capture_stopped_read_fingerprint(fixture: &HostProcessFixture) -> StoppedReadFingerprint {
    let session_dir = fixture.session_dir();
    let runtime_state_dir = session_dir.join(RUNTIME_STATE_SUBDIR);
    StoppedReadFingerprint {
        manifest: read_optional_bytes(&session_dir.join("host-manifest.json")),
        receipts: read_optional_bytes(&session_dir.join(TASK_RECEIPTS_FILENAME)),
        database: read_optional_bytes(&runtime_state_dir.join("state.db")),
        wal: read_optional_bytes(&runtime_state_dir.join("state.db-wal")),
        shm: read_optional_bytes(&runtime_state_dir.join("state.db-shm")),
    }
}

fn stopped_read_backup_path(fixture: &HostProcessFixture, file_name: &str) -> PathBuf {
    let fixture_root = fixture
        .sessions_dir
        .parent()
        .expect("fixture sessions root parent");
    let backup_dir = fixture_root.join("stopped-read-backups");
    fs::create_dir_all(&backup_dir).expect("create out-of-session backup directory");
    fs::set_permissions(&backup_dir, fs::Permissions::from_mode(0o700))
        .expect("chmod out-of-session backup directory 0700");
    assert!(
        !backup_dir.starts_with(&fixture.sessions_dir),
        "missing-file backups must stay outside the guarded session tree"
    );
    backup_dir.join(file_name)
}

fn assert_scope_error(result: Result<TaskStateSnapshot, HostError>, expected_field: &str) {
    match result {
        Err(HostError::ScopeMismatch { field, .. }) => {
            assert_eq!(field, expected_field, "unexpected scope mismatch field");
        }
        other => panic!("expected ScopeMismatch on `{expected_field}`, got {other:?}"),
    }
}

fn assert_delivery_refused(result: Result<DeliveryInspection, HostError>, expected_code: &str) {
    match result {
        Err(err) => assert_eq!(
            err.code(),
            expected_code,
            "unexpected delivery inspection refusal: {err:?}"
        ),
        Ok(inspection) => {
            panic!("expected `{expected_code}` delivery refusal, got {inspection:?}");
        }
    }
}

#[tokio::test]
async fn completed_task_result_is_readable_live_and_after_real_cli_idle_stop_without_mutation() {
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

    let mut host = spawn_host_process(&fixture, "result", None);
    let status =
        wait_for_authenticated_ready(&client, &fixture.approval_id, &mut host, |_| true).await;
    host.assert_running();
    assert_live_ready(&fixture, &client, &mut host, &status);
    fixture.assert_no_provider_invocation();

    client
        .dispatch_task(&fixture.approval_id, TASK_ID)
        .await
        .expect("dispatch approved root task");
    let live_result = wait_for_task_ready(&client, &fixture.approval_id, TASK_ID, &mut host).await;
    assert_eq!(live_result.status, TaskExecutionStatus::ReadyForReview);
    assert!(!live_result.accepted);
    assert_eq!(live_result.task_id, TASK_ID);
    assert!(live_result.run_id.is_some());
    assert!(live_result.worker_id.is_some());
    let live_worker = live_result.worker.as_ref().expect("live worker snapshot");
    assert_eq!(live_worker.status, threadlane::WorkerStatus::Completed);
    assert_eq!(
        live_worker
            .outcome
            .as_ref()
            .expect("completed outcome")
            .acceptance_state,
        threadlane::WorkerAcceptanceState::ReadyForReview
    );
    let completed_provider_invocations = fixture.provider_invocation_count();
    assert!(
        completed_provider_invocations >= 1,
        "the dispatched fake worker must have invoked its provider at least once"
    );

    let idle_status = wait_for_host_idle(&client, &fixture.approval_id, &mut host).await;

    // Live regression: a valid-shaped receipt worker_id mismatch for the same run is still
    // accepted by legacy get_task, but the new exact result API must refuse it.
    let live_receipts_path = fixture.session_dir().join(TASK_RECEIPTS_FILENAME);
    let live_original_receipts = fs::read(&live_receipts_path).expect("read live receipts fixture");
    let actual_worker_id = live_result
        .worker_id
        .clone()
        .expect("live result worker id");
    let tampered_worker_id = if actual_worker_id == "wrk_00000000000000000000000000000001" {
        "wrk_ffffffffffffffffffffffffffffffff"
    } else {
        "wrk_00000000000000000000000000000001"
    };
    let mut live_ledger: serde_json::Value =
        serde_json::from_slice(&live_original_receipts).expect("parse live receipts fixture");
    let live_receipt_entry = live_ledger["receipts"]
        .as_array_mut()
        .expect("live receipts array")
        .first_mut()
        .expect("live sealed receipt entry");
    live_receipt_entry["worker_id"] = serde_json::Value::String(tampered_worker_id.to_string());
    fs::write(
        &live_receipts_path,
        serde_json::to_vec_pretty(&live_ledger).expect("serialize live tampered receipts"),
    )
    .expect("write live tampered receipts fixture");
    fs::set_permissions(&live_receipts_path, fs::Permissions::from_mode(0o600))
        .expect("keep live tampered receipts private");

    let legacy_live = client
        .get_task(&fixture.approval_id, TASK_ID)
        .await
        .expect("legacy get_task keeps accepting a receipt worker mismatch for the same run");
    assert_eq!(legacy_live.status, TaskExecutionStatus::ReadyForReview);
    assert_eq!(legacy_live.run_id, live_result.run_id);
    assert_eq!(legacy_live.worker_id.as_deref(), Some(tampered_worker_id));
    assert_eq!(
        legacy_live
            .worker
            .as_ref()
            .expect("legacy live worker snapshot")
            .worker_id,
        actual_worker_id
    );
    let live_mismatch_err = client
        .get_task_live_or_stopped(&fixture.approval_id, TASK_ID)
        .await
        .expect_err("new live result API must reject a mismatched receipt worker identity");
    assert_eq!(live_mismatch_err.code(), "corrupt_session_state");

    fs::write(&live_receipts_path, &live_original_receipts).expect("restore live receipts fixture");
    fs::set_permissions(&live_receipts_path, fs::Permissions::from_mode(0o600))
        .expect("restore live receipts permissions");
    let restored_live = client
        .get_task_live_or_stopped(&fixture.approval_id, TASK_ID)
        .await
        .expect("live exact result is readable after receipt restoration");
    assert_eq!(restored_live, live_result);
    assert_eq!(
        fixture.provider_invocation_count(),
        completed_provider_invocations,
        "live identity regression and restoration must not launch the provider"
    );

    let instance_id = idle_status.instance_id.clone();
    let session_binding_id = idle_status.session_binding_id.clone();
    let stopped = client
        .stop_idle_session(&fixture.approval_id, &instance_id)
        .await
        .expect("authenticated idle stop after completed result");
    let exit = host.wait_for_exit(EXIT_TIMEOUT).await;
    assert_eq!(
        exit.code(),
        Some(0),
        "host process must exit naturally after serving a completed result"
    );
    assert_durable_stopped(&fixture, &instance_id, &session_binding_id, &stopped);
    assert_eq!(
        fixture.provider_invocation_count(),
        completed_provider_invocations,
        "idle stop must not launch the provider again"
    );

    let receipt = read_task_receipt(
        &fixture.session_dir(),
        &fixture.approval_record(),
        &session_binding_id,
        TASK_ID,
    )
    .expect("read sealed stopped task receipt")
    .expect("completed task receipt exists");
    assert_eq!(receipt.run_id, live_result.run_id.clone().unwrap());
    assert_eq!(receipt.worker_id, live_result.worker_id.clone().unwrap());

    let fingerprint_before = capture_stopped_read_fingerprint(&fixture);
    let stopped_result = client
        .get_task_live_or_stopped(&fixture.approval_id, TASK_ID)
        .await
        .expect("read stopped task result");
    let fingerprint_after = capture_stopped_read_fingerprint(&fixture);

    assert_eq!(
        stopped_result, live_result,
        "stopped result must preserve the live task, run, worker and outcome identities"
    );
    assert_eq!(stopped_result.status, TaskExecutionStatus::ReadyForReview);
    assert!(!stopped_result.accepted);
    assert_eq!(
        fingerprint_before, fingerprint_after,
        "stopped result read must not change manifest, receipts, DB, cursors, or sidecars"
    );
    assert!(
        fingerprint_after.wal.is_none() && fingerprint_after.shm.is_none(),
        "clean stopped history must not gain WAL/SHM sidecars"
    );
    assert_eq!(
        fixture.provider_invocation_count(),
        completed_provider_invocations,
        "stopped result read must not launch the provider"
    );

    // A missing sealed receipt is explicit NotStarted, not an invented run or a synthetic Done.
    let receipts_path = fixture.session_dir().join(TASK_RECEIPTS_FILENAME);
    let missing_receipts_path = stopped_read_backup_path(&fixture, "task-receipts.json.missing");
    fs::rename(&receipts_path, &missing_receipts_path).expect("move receipts ledger");
    let not_started = client
        .get_task_live_or_stopped(&fixture.approval_id, TASK_ID)
        .await
        .expect("missing receipt reports an explicit not-started outcome");
    assert_eq!(not_started.status, TaskExecutionStatus::NotStarted);
    assert!(not_started.run_id.is_none());
    assert!(not_started.worker_id.is_none());
    assert!(not_started.worker.is_none());
    assert!(!not_started.accepted);
    fs::rename(&missing_receipts_path, &receipts_path).expect("restore receipts ledger");

    // An unresolved dispatch intent without a sealed receipt remains Unknown and never borrows a
    // run identity from another task or from the intent's command key.
    let original_receipts = fs::read(&receipts_path).expect("read original receipts fixture");
    let original_ledger: serde_json::Value =
        serde_json::from_slice(&original_receipts).expect("parse original receipts fixture");
    let sealed_receipt = original_ledger["receipts"]
        .as_array()
        .expect("receipts array")
        .first()
        .expect("sealed receipt entry")
        .clone();
    let unresolved_intent = serde_json::json!({
        "task_id": sealed_receipt["task_id"].clone(),
        "task_digest": sealed_receipt["task_digest"].clone(),
        "profile_id": sealed_receipt["profile_id"].clone(),
        "command_key": sealed_receipt["command_key"].clone(),
        "recorded_at_ms": sealed_receipt["recorded_at_ms"].clone(),
    });
    let mut intent_only_ledger = original_ledger.clone();
    intent_only_ledger["receipts"] = serde_json::Value::Array(Vec::new());
    intent_only_ledger["pending_intents"] = serde_json::Value::Array(vec![unresolved_intent]);
    let intent_only_bytes =
        serde_json::to_vec_pretty(&intent_only_ledger).expect("serialize intent-only ledger");
    fs::write(&receipts_path, intent_only_bytes).expect("write intent-only ledger fixture");
    fs::set_permissions(&receipts_path, fs::Permissions::from_mode(0o600))
        .expect("keep intent-only ledger private");
    let unknown_intent = client
        .get_task_live_or_stopped(&fixture.approval_id, TASK_ID)
        .await
        .expect("unresolved intent reports an explicit unknown outcome");
    assert_eq!(unknown_intent.status, TaskExecutionStatus::Unknown);
    assert!(unknown_intent.run_id.is_none());
    assert!(unknown_intent.worker_id.is_none());
    assert!(unknown_intent.worker.is_none());
    assert!(!unknown_intent.accepted);
    fs::write(&receipts_path, &original_receipts).expect("restore receipts ledger");
    fs::set_permissions(&receipts_path, fs::Permissions::from_mode(0o600))
        .expect("restore receipts permissions");

    // Caller scope remains authoritative: neither another binding nor another native thread can
    // read this task's stopped result.
    let wrong_binding = ApprovedHostClient::open(
        &fixture.registry_dir,
        &fixture.plans_dir,
        &fixture.authority_dir,
        &fixture.sessions_dir,
        "bnd_wrong_scope",
        THREAD_UUID,
    )
    .expect("open wrong-binding client");
    assert_scope_error(
        wrong_binding
            .get_task_live_or_stopped(&fixture.approval_id, TASK_ID)
            .await,
        "control_binding_id",
    );

    let other_thread_uuid = "660e8400-e29b-41d4-a716-446655440001";
    let wrong_thread = ApprovedHostClient::open(
        &fixture.registry_dir,
        &fixture.plans_dir,
        &fixture.authority_dir,
        &fixture.sessions_dir,
        CONTROL_BINDING_ID,
        other_thread_uuid,
    )
    .expect("open wrong-thread client");
    assert_scope_error(
        wrong_thread
            .get_task_live_or_stopped(&fixture.approval_id, TASK_ID)
            .await,
        "thread_uuid",
    );

    // A still-held session lifetime lock is not clean stopped history.
    let host_lock = OpenOptions::new()
        .read(true)
        .open(fixture.session_dir().join("host.lock"))
        .expect("open stopped host.lock");
    host_lock
        .lock_exclusive()
        .expect("hold stopped host.lock for rejection test");
    let locked_err = client
        .get_task_live_or_stopped(&fixture.approval_id, TASK_ID)
        .await
        .expect_err("held host.lock must refuse stopped result");
    assert_eq!(locked_err.code(), "host_not_ready");
    drop(host_lock);

    // Residual WAL frames are not a clean checkpointed history and must fail closed without
    // creating a SHM sidecar or mutating the database.
    let runtime_state_dir = fixture.session_dir().join(RUNTIME_STATE_SUBDIR);
    let wal_path = runtime_state_dir.join("state.db-wal");
    let shm_path = runtime_state_dir.join("state.db-shm");
    let db_path = runtime_state_dir.join("state.db");
    let wal = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&wal_path)
        .expect("create residual WAL fixture");
    wal.sync_all().expect("sync residual WAL fixture");
    drop(wal);
    let db_before_wal_rejection = read_optional_bytes(&db_path);
    let wal_err = client
        .get_task_live_or_stopped(&fixture.approval_id, TASK_ID)
        .await
        .expect_err("residual WAL must refuse stopped result");
    assert_eq!(wal_err.code(), "host_not_ready");
    assert!(!shm_path.exists(), "must not create a missing SHM sidecar");
    assert_eq!(read_optional_bytes(&db_path), db_before_wal_rejection);
    fs::remove_file(&wal_path).expect("remove residual WAL fixture");

    // Missing checkpointed database is missing stopped history, not an empty or fresh result.
    let db_missing_path = stopped_read_backup_path(&fixture, "state.db.missing");
    fs::rename(&db_path, &db_missing_path).expect("move state.db for missing-file rejection");
    let missing_err = client
        .get_task_live_or_stopped(&fixture.approval_id, TASK_ID)
        .await
        .expect_err("missing state.db must refuse stopped result");
    assert_eq!(missing_err.code(), "corrupt_session_state");
    fs::rename(&db_missing_path, &db_path).expect("restore state.db");

    // Missing manifest cannot authenticate a stopped session.
    let manifest_path = fixture.session_dir().join("host-manifest.json");
    let missing_manifest_path = stopped_read_backup_path(&fixture, "host-manifest.json.missing");
    fs::rename(&manifest_path, &missing_manifest_path).expect("move stopped manifest");
    let manifest_err = client
        .get_task_live_or_stopped(&fixture.approval_id, TASK_ID)
        .await
        .expect_err("missing stopped manifest must refuse stopped result");
    assert_eq!(manifest_err.code(), "host_not_ready");
    fs::rename(&missing_manifest_path, &manifest_path).expect("restore stopped manifest");

    // Tampered task/run/profile/command/worker receipt fields must never resolve to a result.
    let tampered_fields = [
        ("task_digest", serde_json::Value::String("f".repeat(64))),
        (
            "run_id",
            serde_json::Value::String("run_00000000000000000000000000000001".to_string()),
        ),
        (
            "worker_id",
            serde_json::Value::String("wrk_00000000000000000000000000000001".to_string()),
        ),
        (
            "profile_id",
            serde_json::Value::String("tampered-profile".to_string()),
        ),
        (
            "command_key",
            serde_json::Value::String("tampered-command-key".to_string()),
        ),
    ];
    for (field, tampered_value) in tampered_fields {
        let mut ledger: serde_json::Value =
            serde_json::from_slice(&original_receipts).expect("parse receipts fixture JSON");
        let receipt_entry = ledger["receipts"]
            .as_array_mut()
            .expect("receipts array")
            .first_mut()
            .expect("sealed receipt entry");
        receipt_entry[field] = tampered_value;
        let tampered_bytes =
            serde_json::to_vec_pretty(&ledger).expect("serialize tampered receipts");
        fs::write(&receipts_path, tampered_bytes).expect("write tampered receipts fixture");
        fs::set_permissions(&receipts_path, fs::Permissions::from_mode(0o600))
            .expect("keep tampered receipts private");

        let tampered_err = client
            .get_task_live_or_stopped(&fixture.approval_id, TASK_ID)
            .await
            .expect_err("tampered receipt field must refuse stopped result");
        assert!(
            matches!(
                &tampered_err,
                HostError::CorruptReceipts { .. } | HostError::CorruptSessionState { .. }
            ),
            "unexpected tampered `{field}` error: {tampered_err:?}"
        );

        fs::write(&receipts_path, &original_receipts).expect("restore receipts fixture");
        fs::set_permissions(&receipts_path, fs::Permissions::from_mode(0o600))
            .expect("restore receipts permissions");
    }

    let final_result = client
        .get_task_live_or_stopped(&fixture.approval_id, TASK_ID)
        .await
        .expect("stopped result remains readable after rejection probes");
    assert_eq!(final_result, live_result);
    assert_eq!(
        fixture.provider_invocation_count(),
        completed_provider_invocations,
        "rejection probes and final stopped read must not launch the provider"
    );
}

#[tokio::test]
async fn stale_ready_orphan_terminal_history_is_read_only_and_refuses_unproven_history() {
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

    let mut host = spawn_host_process(&fixture, "orphan", None);
    let status =
        wait_for_authenticated_ready(&client, &fixture.approval_id, &mut host, |_| true).await;
    assert_live_ready(&fixture, &client, &mut host, &status);

    client
        .dispatch_task(&fixture.approval_id, TASK_ID)
        .await
        .expect("dispatch approved root task");
    let live_result = wait_for_task_ready(&client, &fixture.approval_id, TASK_ID, &mut host).await;
    let live_worker = live_result
        .worker
        .clone()
        .expect("live terminal worker snapshot");
    assert_eq!(live_worker.attempt_count, 1);
    let idle = wait_for_host_idle(&client, &fixture.approval_id, &mut host).await;
    let live_delivery = client
        .inspect_task_delivery(&fixture.approval_id, TASK_ID)
        .await
        .expect("inspect live terminal delivery");
    let completed_provider_invocations = fixture.provider_invocation_count();
    assert!(completed_provider_invocations >= 1);

    let manifest_path = fixture.session_dir().join("host-manifest.json");
    let ready_manifest = fs::read(&manifest_path).expect("read live Ready manifest");
    let ready_manifest_json: serde_json::Value =
        serde_json::from_slice(&ready_manifest).expect("parse live Ready manifest");
    assert_eq!(ready_manifest_json["phase"], "ready");
    assert!(ready_manifest_json["pid"].as_u64().is_some());
    assert!(ready_manifest_json["instance_id"].as_str().is_some());

    let instance_id = idle.instance_id.clone();
    let session_binding_id = idle.session_binding_id.clone();
    let stopped = client
        .stop_idle_session(&fixture.approval_id, &instance_id)
        .await
        .expect("authenticated idle stop before orphan recovery");
    let exit = host.wait_for_exit(EXIT_TIMEOUT).await;
    assert_eq!(
        exit.code(),
        Some(0),
        "host process must exit naturally before stale-Ready recovery"
    );
    assert_durable_stopped(&fixture, &instance_id, &session_binding_id, &stopped);

    // Simulate a reboot/interruption that leaves the last Ready manifest with its old PID and
    // instance while both process-lifetime locks are already released.
    fs::write(&manifest_path, &ready_manifest).expect("restore stale Ready manifest bytes");
    fs::set_permissions(&manifest_path, fs::Permissions::from_mode(0o600))
        .expect("keep stale Ready manifest private");
    let stale = read_host_manifest_optional(&fixture.session_dir(), &fixture.approval_record())
        .expect("read restored stale Ready manifest")
        .expect("stale Ready manifest exists");
    assert_eq!(stale.phase, HostPhase::Ready);
    assert!(stale.gates.supervisor_authenticated_ready);
    assert_eq!(stale.instance_id.as_deref(), Some(instance_id.as_str()));
    assert_eq!(
        stale.session_binding_id.as_deref(),
        Some(session_binding_id.as_str())
    );
    assert!(stale.pid.is_some());
    assert!(
        !is_host_lock_held(&fixture.session_dir()).expect("probe stale host.lock"),
        "orphan recovery requires a released host.lock"
    );

    // The stale Ready result is the same exact terminal result and remains strictly read-only.
    let fingerprint_before = capture_stopped_read_fingerprint(&fixture);
    let orphan_result = client
        .get_task_live_or_stopped(&fixture.approval_id, TASK_ID)
        .await
        .expect("read stale Ready orphan terminal result");
    let fingerprint_after = capture_stopped_read_fingerprint(&fixture);
    assert_eq!(
        orphan_result, live_result,
        "orphan recovery must preserve the exact live task, run, worker, and result identities"
    );
    assert_eq!(
        fingerprint_before, fingerprint_after,
        "orphan result read must not change manifest, receipts, DB bytes, cursors, or sidecars"
    );
    assert!(
        fingerprint_after.wal.is_none() && fingerprint_after.shm.is_none(),
        "orphan result read must not gain WAL/SHM sidecars"
    );
    assert_eq!(
        fixture.provider_invocation_count(),
        completed_provider_invocations,
        "orphan result read must not launch the provider"
    );
    assert!(
        !is_host_lock_held(&fixture.session_dir()).expect("probe orphan host.lock"),
        "orphan result read must release the exclusive host.lock"
    );

    let orphan_delivery = client
        .inspect_task_delivery(&fixture.approval_id, TASK_ID)
        .await
        .expect("inspect orphan terminal delivery");
    assert_eq!(
        orphan_delivery, live_delivery,
        "orphan delivery inspection must preserve result and artifact identity"
    );
    client
        .recheck_task_delivery(&fixture.approval_id, TASK_ID, &orphan_delivery)
        .await
        .expect("recheck orphan terminal delivery");
    assert_eq!(
        capture_stopped_read_fingerprint(&fixture),
        fingerprint_after,
        "orphan delivery inspection and recheck must remain read-only"
    );
    assert_eq!(
        fixture.provider_invocation_count(),
        completed_provider_invocations,
        "orphan delivery inspection must not launch the provider"
    );

    // Actual changed artifact bytes are refused even when the stored result identity is unchanged.
    let checkout = fixture
        .session_dir()
        .join(WORK_AREA_SUBDIR)
        .join(CHECKOUTS_SUBDIR)
        .join("tasks")
        .join(&orphan_delivery.worktree_id);
    let delivered_file = checkout.join("src/lib.rs");
    let original_bytes = fs::read(&delivered_file).expect("read orphan delivered file");
    let mut changed_bytes = original_bytes.clone();
    changed_bytes[0] = if changed_bytes[0] == b'/' { b'#' } else { b'/' };
    assert_eq!(changed_bytes.len(), original_bytes.len());
    fs::write(&delivered_file, &changed_bytes).expect("write changed orphan delivery");
    let changed_delivery = client
        .inspect_task_delivery(&fixture.approval_id, TASK_ID)
        .await
        .expect("changed orphan delivery remains inspectable");
    assert_eq!(
        changed_delivery.result_digest, orphan_delivery.result_digest,
        "artifact mutation must not rewrite the terminal result identity"
    );
    assert_ne!(
        changed_delivery.delivery_digest,
        orphan_delivery.delivery_digest
    );
    let changed_recheck = client
        .recheck_task_delivery(&fixture.approval_id, TASK_ID, &orphan_delivery)
        .await
        .expect_err("changed orphan artifact must refuse delivery recheck");
    assert_eq!(changed_recheck.code(), "security_violation");
    fs::write(&delivered_file, &original_bytes).expect("restore exact orphan delivered bytes");

    // A still-held host.lock is never bypassed by the orphan fallback.
    let host_lock = OpenOptions::new()
        .read(true)
        .open(fixture.session_dir().join("host.lock"))
        .expect("open orphan host.lock");
    host_lock
        .lock_exclusive()
        .expect("hold orphan host.lock for rejection test");
    let locked_err = client
        .get_task_live_or_stopped(&fixture.approval_id, TASK_ID)
        .await
        .expect_err("held host.lock must refuse orphan result");
    assert_eq!(locked_err.code(), "host_not_ready");
    host_lock.unlock().expect("release orphan host.lock");

    // A surviving supervisor lock is not an orphaned host even when host.lock is free.
    let supervisor_lock_path = fixture
        .session_dir()
        .join(RUNTIME_STATE_SUBDIR)
        .join("supervisor.lock");
    let supervisor_lock = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .open(&supervisor_lock_path)
        .expect("open orphan supervisor.lock");
    supervisor_lock
        .set_permissions(fs::Permissions::from_mode(0o600))
        .expect("keep orphan supervisor.lock private");
    assert_eq!(
        fs::metadata(&supervisor_lock_path)
            .expect("stat orphan supervisor.lock")
            .len(),
        0,
        "supervisor.lock fixture must remain an empty lock file"
    );
    supervisor_lock
        .lock_exclusive()
        .expect("hold orphan supervisor.lock for rejection test");
    let supervisor_err = client
        .get_task_live_or_stopped(&fixture.approval_id, TASK_ID)
        .await
        .expect_err("held supervisor.lock must refuse orphan result");
    assert_eq!(supervisor_err.code(), "host_not_ready");
    supervisor_lock
        .unlock()
        .expect("release orphan supervisor.lock");

    // Caller scope remains authoritative for orphan reads.
    let wrong_binding = ApprovedHostClient::open(
        &fixture.registry_dir,
        &fixture.plans_dir,
        &fixture.authority_dir,
        &fixture.sessions_dir,
        "bnd_wrong_orphan_scope",
        THREAD_UUID,
    )
    .expect("open wrong-binding orphan client");
    assert_scope_error(
        wrong_binding
            .get_task_live_or_stopped(&fixture.approval_id, TASK_ID)
            .await,
        "control_binding_id",
    );
    let other_thread_uuid = "660e8400-e29b-41d4-a716-446655440001";
    let wrong_thread = ApprovedHostClient::open(
        &fixture.registry_dir,
        &fixture.plans_dir,
        &fixture.authority_dir,
        &fixture.sessions_dir,
        CONTROL_BINDING_ID,
        other_thread_uuid,
    )
    .expect("open wrong-thread orphan client");
    assert_scope_error(
        wrong_thread
            .get_task_live_or_stopped(&fixture.approval_id, TASK_ID)
            .await,
        "thread_uuid",
    );

    // Missing original receipt or a changed receipt identity is not terminal history.
    let receipts_path = fixture.session_dir().join(TASK_RECEIPTS_FILENAME);
    let original_receipts = fs::read(&receipts_path).expect("read orphan receipts fixture");
    let missing_receipts_path =
        stopped_read_backup_path(&fixture, "orphan-task-receipts.json.missing");
    fs::rename(&receipts_path, &missing_receipts_path).expect("move orphan receipts ledger");
    let missing_err = client
        .get_task_live_or_stopped(&fixture.approval_id, TASK_ID)
        .await
        .expect_err("missing orphan receipt must refuse terminal history");
    assert_eq!(missing_err.code(), "host_not_ready");
    fs::rename(&missing_receipts_path, &receipts_path).expect("restore orphan receipts ledger");

    let mut tampered_ledger: serde_json::Value =
        serde_json::from_slice(&original_receipts).expect("parse orphan receipts fixture");
    let tampered_entry = tampered_ledger["receipts"]
        .as_array_mut()
        .expect("orphan receipts array")
        .first_mut()
        .expect("orphan sealed receipt entry");
    tampered_entry["worker_id"] =
        serde_json::Value::String("wrk_ffffffffffffffffffffffffffffffff".to_string());
    fs::write(
        &receipts_path,
        serde_json::to_vec_pretty(&tampered_ledger).expect("serialize tampered orphan receipts"),
    )
    .expect("write tampered orphan receipts fixture");
    fs::set_permissions(&receipts_path, fs::Permissions::from_mode(0o600))
        .expect("keep tampered orphan receipts private");
    let tampered_err = client
        .get_task_live_or_stopped(&fixture.approval_id, TASK_ID)
        .await
        .expect_err("tampered orphan receipt must refuse terminal history");
    assert!(
        matches!(
            &tampered_err,
            HostError::CorruptReceipts { .. } | HostError::CorruptSessionState { .. }
        ),
        "unexpected tampered orphan receipt error: {tampered_err:?}"
    );
    fs::write(&receipts_path, &original_receipts).expect("restore orphan receipts fixture");
    fs::set_permissions(&receipts_path, fs::Permissions::from_mode(0o600))
        .expect("restore orphan receipts permissions");

    // Missing PID or instance identity alone never authorizes an orphan read.
    let ready_manifest_json: serde_json::Value =
        serde_json::from_slice(&ready_manifest).expect("parse Ready manifest fixture");
    for (field, label) in [("pid", "pid"), ("instance_id", "instance")] {
        let mut missing_identity = ready_manifest_json.clone();
        missing_identity[field] = serde_json::Value::Null;
        fs::write(
            &manifest_path,
            serde_json::to_vec_pretty(&missing_identity).expect("serialize missing identity"),
        )
        .expect("write missing identity manifest");
        fs::set_permissions(&manifest_path, fs::Permissions::from_mode(0o600))
            .expect("keep missing identity manifest private");
        let missing_identity_err = client
            .get_task_live_or_stopped(&fixture.approval_id, TASK_ID)
            .await
            .expect_err("missing orphan identity must refuse terminal history");
        assert_eq!(
            missing_identity_err.code(),
            "host_not_ready",
            "missing orphan {label} must refuse terminal history"
        );
    }
    fs::write(&manifest_path, &ready_manifest).expect("restore stale Ready manifest fixture");
    fs::set_permissions(&manifest_path, fs::Permissions::from_mode(0o600))
        .expect("restore stale Ready manifest permissions");

    // A real nonterminal worker record behind the original receipt is not a terminal result.
    let database_path = fixture
        .session_dir()
        .join(RUNTIME_STATE_SUBDIR)
        .join(threadlane::DATABASE_FILENAME);
    let mut core = threadlane_core::Core::open(&database_path).expect("open stopped runtime core");
    let queued_output = core
        .execute(
            &session_binding_id,
            threadlane_core::Command::StartWorker {
                command_key: "orphan-nonterminal-probe".to_string(),
                profile_id: live_worker.profile_id.clone(),
                profile_digest: live_worker.profile_digest.clone(),
                provider: live_worker.provider.clone(),
                requested_model: live_worker.requested_model.clone(),
                requested_effort: live_worker.requested_effort.clone(),
                base_commit: live_worker.worktree.base_commit.clone(),
                execution_binding: live_worker.execution_binding.clone(),
                instruction: live_worker.instruction.clone(),
                now_ms: 1_700_000_009_000,
            },
        )
        .expect("queue a real nonterminal worker fixture");
    let queued_worker = match queued_output {
        threadlane_core::CommandOutput::Worker(worker) => *worker,
        other => panic!("expected queued worker fixture, got {other:?}"),
    };
    drop(core);
    let runtime_state_dir = fixture.session_dir().join(RUNTIME_STATE_SUBDIR);
    assert!(
        !runtime_state_dir.join("state.db-wal").exists()
            && !runtime_state_dir.join("state.db-shm").exists(),
        "nonterminal fixture setup must leave a clean checkpointed database"
    );

    let mut nonterminal_ledger: serde_json::Value =
        serde_json::from_slice(&original_receipts).expect("parse nonterminal receipts fixture");
    let nonterminal_entry = nonterminal_ledger["receipts"]
        .as_array_mut()
        .expect("nonterminal receipts array")
        .first_mut()
        .expect("nonterminal sealed receipt entry");
    nonterminal_entry["run_id"] = serde_json::Value::String(queued_worker.run_id);
    nonterminal_entry["worker_id"] = serde_json::Value::String(queued_worker.worker_id);
    fs::write(
        &receipts_path,
        serde_json::to_vec_pretty(&nonterminal_ledger)
            .expect("serialize nonterminal receipts fixture"),
    )
    .expect("write nonterminal receipts fixture");
    fs::set_permissions(&receipts_path, fs::Permissions::from_mode(0o600))
        .expect("keep nonterminal receipts private");
    let nonterminal_err = client
        .get_task_live_or_stopped(&fixture.approval_id, TASK_ID)
        .await
        .expect_err("nonterminal orphan evidence must refuse terminal history");
    assert_eq!(nonterminal_err.code(), "host_not_ready");

    // Reopening the stopped database promotes that queued record to `Unknown`. Unknown is never
    // terminal evidence and must still refuse instead of becoming success or completion.
    let reopened_core =
        threadlane_core::Core::open(&database_path).expect("reopen stopped runtime core");
    drop(reopened_core);
    assert!(
        !runtime_state_dir.join("state.db-wal").exists()
            && !runtime_state_dir.join("state.db-shm").exists(),
        "unknown fixture setup must leave a clean checkpointed database"
    );
    let unknown_err = client
        .get_task_live_or_stopped(&fixture.approval_id, TASK_ID)
        .await
        .expect_err("unknown orphan evidence must refuse terminal history");
    assert_eq!(unknown_err.code(), "host_not_ready");

    assert_eq!(
        fixture.provider_invocation_count(),
        completed_provider_invocations,
        "orphan refusal probes must not launch the provider"
    );
    assert!(
        !is_host_lock_held(&fixture.session_dir()).expect("probe orphan host.lock after refusals"),
        "orphan refusal probes must not hold host.lock"
    );
}

#[tokio::test]
async fn stale_ready_killed_host_reads_terminal_result_from_residual_wal_without_mutation() {
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

    let mut host = spawn_host_process(&fixture, "orphan-wal", None);
    let status =
        wait_for_authenticated_ready(&client, &fixture.approval_id, &mut host, |_| true).await;
    assert_live_ready(&fixture, &client, &mut host, &status);

    client
        .dispatch_task(&fixture.approval_id, TASK_ID)
        .await
        .expect("dispatch approved root task");
    let live_result = wait_for_task_ready(&client, &fixture.approval_id, TASK_ID, &mut host).await;
    let live_worker = live_result
        .worker
        .clone()
        .expect("live terminal worker snapshot");
    assert_eq!(live_worker.attempt_count, 1);
    let idle = wait_for_host_idle(&client, &fixture.approval_id, &mut host).await;
    let live_delivery = client
        .inspect_task_delivery(&fixture.approval_id, TASK_ID)
        .await
        .expect("inspect live terminal delivery");
    let completed_provider_invocations = fixture.provider_invocation_count();
    assert!(completed_provider_invocations >= 1);

    let instance_id = idle.instance_id.clone();
    let session_binding_id = idle.session_binding_id.clone();
    let host_pid = host.pid();
    let runtime_state_dir = fixture.session_dir().join(RUNTIME_STATE_SUBDIR);
    let database_path = runtime_state_dir.join(threadlane::DATABASE_FILENAME);
    let wal_path = runtime_state_dir.join("state.db-wal");
    let shm_path = runtime_state_dir.join("state.db-shm");
    let wal_after_completion = fs::read(&wal_path).expect("read committed residual WAL");
    let shm_after_completion = fs::read(&shm_path).expect("read residual SHM");
    assert!(
        !wal_after_completion.is_empty(),
        "completed live host must leave committed WAL frames"
    );
    assert!(
        !shm_after_completion.is_empty(),
        "completed live host must leave an active shared-memory index"
    );
    assert!(
        wal_after_completion
            .windows(live_worker.worker_id.len())
            .any(|window| window == live_worker.worker_id.as_bytes()),
        "residual WAL must contain the committed worker identity"
    );

    // The fake provider child has already completed and the host is idle. Kill only the owning
    // host process so the session keeps its last Ready manifest and committed WAL frames. This is
    // a controlled crash fixture, not power-loss proof.
    host.child.kill().expect("kill completed host process");
    let killed = host.wait_for_exit(EXIT_TIMEOUT).await;
    assert_eq!(
        killed.code(),
        None,
        "killed host must not report a normal successful exit"
    );
    assert!(
        !is_host_lock_held(&fixture.session_dir()).expect("probe killed host.lock"),
        "killed host must release host.lock"
    );

    let stale = read_host_manifest_optional(&fixture.session_dir(), &fixture.approval_record())
        .expect("read killed Ready manifest")
        .expect("killed Ready manifest exists");
    assert_eq!(stale.phase, HostPhase::Ready);
    assert!(stale.gates.supervisor_authenticated_ready);
    assert_eq!(stale.instance_id.as_deref(), Some(instance_id.as_str()));
    assert_eq!(
        stale.session_binding_id.as_deref(),
        Some(session_binding_id.as_str())
    );
    assert_eq!(stale.pid, Some(host_pid));

    let wal_after_kill = fs::read(&wal_path).expect("read residual WAL after kill");
    let shm_after_kill = fs::read(&shm_path).expect("read residual SHM after kill");
    assert!(!wal_after_kill.is_empty());
    assert!(!shm_after_kill.is_empty());

    // The clean stopped reader refuses residual sidecars. Orphan recovery must use the existing
    // bounded WAL-capable reader rather than a main-database-only or checkpointing fallback.
    let clean_stopped_error = threadlane_core::Core::get_worker_readonly(
        &database_path,
        &session_binding_id,
        THREAD_UUID,
        &live_worker.run_id,
    )
    .expect_err("clean stopped reader must refuse residual WAL");
    assert!(
        matches!(
            &clean_stopped_error,
            threadlane_core::observation::ReadonlySqliteError::SnapshotUnavailable(_)
        ),
        "unexpected clean stopped residual-WAL error: {clean_stopped_error:?}"
    );

    let fingerprint_before = capture_stopped_read_fingerprint(&fixture);
    assert_eq!(fingerprint_before.wal.as_ref(), Some(&wal_after_kill));
    assert_eq!(fingerprint_before.shm.as_ref(), Some(&shm_after_kill));

    let orphan_result = client
        .get_task_live_or_stopped(&fixture.approval_id, TASK_ID)
        .await
        .expect("read killed-host terminal result from residual WAL");
    let fingerprint_after = capture_stopped_read_fingerprint(&fixture);
    assert_eq!(
        orphan_result, live_result,
        "residual-WAL orphan recovery must preserve the exact live task, run, worker, and result identities"
    );
    assert_eq!(
        &fingerprint_before.manifest, &fingerprint_after.manifest,
        "residual-WAL orphan read must not rewrite the stale Ready manifest"
    );
    assert_eq!(
        &fingerprint_before.receipts, &fingerprint_after.receipts,
        "residual-WAL orphan read must not rewrite sealed receipts"
    );
    assert_eq!(
        &fingerprint_before.database, &fingerprint_after.database,
        "residual-WAL orphan read must not rewrite the durable database"
    );
    assert_eq!(
        &fingerprint_before.wal, &fingerprint_after.wal,
        "residual-WAL orphan read must not rewrite or checkpoint the WAL"
    );
    assert!(
        fingerprint_after.wal.is_some() && fingerprint_after.shm.is_some(),
        "residual-WAL orphan read must not delete durable sidecars"
    );

    let orphan_delivery = client
        .inspect_task_delivery(&fixture.approval_id, TASK_ID)
        .await
        .expect("inspect killed-host terminal delivery");
    assert_eq!(
        orphan_delivery, live_delivery,
        "residual-WAL delivery inspection must preserve result and artifact identity"
    );
    client
        .recheck_task_delivery(&fixture.approval_id, TASK_ID, &orphan_delivery)
        .await
        .expect("recheck killed-host terminal delivery");
    let fingerprint_after_delivery = capture_stopped_read_fingerprint(&fixture);
    assert_eq!(
        &fingerprint_after_delivery.manifest, &fingerprint_after.manifest,
        "residual-WAL delivery recheck must not rewrite the stale Ready manifest"
    );
    assert_eq!(
        &fingerprint_after_delivery.receipts, &fingerprint_after.receipts,
        "residual-WAL delivery recheck must not rewrite sealed receipts"
    );
    assert_eq!(
        &fingerprint_after_delivery.database, &fingerprint_after.database,
        "residual-WAL delivery recheck must not rewrite the durable database"
    );
    assert_eq!(
        &fingerprint_after_delivery.wal, &fingerprint_after.wal,
        "residual-WAL delivery recheck must not rewrite or checkpoint the WAL"
    );
    assert_eq!(
        fixture.provider_invocation_count(),
        completed_provider_invocations,
        "residual-WAL result and delivery reads must not launch the provider"
    );

    // Restore the current sidecars, then prove a mismatched pair and corrupt WAL fail closed
    // when this fixture's required terminal evidence is available only through the WAL.
    let current_wal = fs::read(&wal_path).expect("read current residual WAL");
    let current_shm = fs::read(&shm_path).expect("read current residual SHM");

    fs::remove_file(&shm_path).expect("remove state.db-shm for sidecar mismatch probe");
    let missing_shm_error = client
        .get_task_live_or_stopped(&fixture.approval_id, TASK_ID)
        .await
        .expect_err("residual WAL without SHM must refuse terminal history");
    assert_eq!(
        missing_shm_error.code(),
        "corrupt_session_state",
        "unexpected missing-SHM error: {missing_shm_error:?}"
    );
    write_private_fixture_file(&shm_path, &current_shm);

    write_private_fixture_file(&wal_path, b"corrupt-wal-frames");
    let corrupt_wal_error = client
        .get_task_live_or_stopped(&fixture.approval_id, TASK_ID)
        .await
        .expect_err("corrupt residual WAL must refuse terminal history");
    assert_eq!(
        corrupt_wal_error.code(),
        "corrupt_session_state",
        "unexpected corrupt-WAL error: {corrupt_wal_error:?}"
    );
    write_private_fixture_file(&wal_path, &current_wal);
    write_private_fixture_file(&shm_path, &current_shm);

    let restored_result = client
        .get_task_live_or_stopped(&fixture.approval_id, TASK_ID)
        .await
        .expect("restored residual-WAL fixture remains readable");
    assert_eq!(restored_result, live_result);
    assert_eq!(
        fixture.provider_invocation_count(),
        completed_provider_invocations,
        "sidecar refusal probes and restored read must not launch the provider"
    );
    assert!(
        !is_host_lock_held(&fixture.session_dir()).expect("probe killed host.lock after probes"),
        "residual-WAL refusal probes must not hold host.lock"
    );
}

#[tokio::test]
async fn approved_host_delivery_inspection_reads_live_and_stopped_results_without_mutation() {
    let fixture = setup_host_process_fixture_with_failed_task();
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

    let mut host = spawn_host_process(&fixture, "delivery", None);
    let status =
        wait_for_authenticated_ready(&client, &fixture.approval_id, &mut host, |_| true).await;
    assert_live_ready(&fixture, &client, &mut host, &status);

    // No receipt means no delivery, even though the approved task and work area exist.
    assert_delivery_refused(
        client
            .inspect_task_delivery(&fixture.approval_id, TASK_ID)
            .await,
        "validation",
    );
    fixture.assert_no_provider_invocation();

    client
        .dispatch_task(&fixture.approval_id, TASK_ID)
        .await
        .expect("dispatch approved root task");
    let live_result = wait_for_task_ready(&client, &fixture.approval_id, TASK_ID, &mut host).await;
    wait_for_host_idle(&client, &fixture.approval_id, &mut host).await;
    let live_delivery = client
        .inspect_task_delivery(&fixture.approval_id, TASK_ID)
        .await
        .expect("inspect the live successful delivery");
    assert_eq!(live_delivery.approval_id, fixture.approval_id);
    assert_eq!(live_delivery.binding_id, CONTROL_BINDING_ID);
    assert_eq!(live_delivery.thread_uuid, THREAD_UUID);
    assert_eq!(live_delivery.task_id, TASK_ID);
    assert_eq!(
        live_delivery.run_id,
        live_result.run_id.clone().expect("live result run id")
    );
    assert_eq!(
        live_delivery.worker_id,
        live_result
            .worker_id
            .clone()
            .expect("live result worker id")
    );
    assert_eq!(live_delivery.delivery_kind, DeliveryKind::Patch);
    assert!(live_delivery.changed_paths_count >= 1);
    assert_eq!(live_delivery.result_digest.len(), 64);
    assert_eq!(live_delivery.delivery_digest.len(), 64);
    assert_eq!(
        client
            .inspect_task_delivery(&fixture.approval_id, TASK_ID)
            .await
            .expect("repeat live delivery inspection"),
        live_delivery,
        "a stable live delivery read must preserve result and artifact identity"
    );
    client
        .recheck_task_delivery(&fixture.approval_id, TASK_ID, &live_delivery)
        .await
        .expect("recheck stable live delivery");
    host.assert_running();
    let provider_invocations_after_success = fixture.provider_invocation_count();
    assert!(provider_invocations_after_success >= 1);
    assert_eq!(
        fixture.provider_invocation_count(),
        provider_invocations_after_success,
        "live delivery inspection must not launch the provider"
    );

    // A failed worker terminal result is refused before any physical delivery claim.
    client
        .dispatch_task(&fixture.approval_id, FAILED_TASK_ID)
        .await
        .expect("dispatch approved failing task");
    let failed_result = wait_for_task_status(
        &client,
        &fixture.approval_id,
        FAILED_TASK_ID,
        &mut host,
        TaskExecutionStatus::Failed,
    )
    .await;
    assert_eq!(failed_result.status, TaskExecutionStatus::Failed);
    assert!(!failed_result.accepted);
    assert_delivery_refused(
        client
            .inspect_task_delivery(&fixture.approval_id, FAILED_TASK_ID)
            .await,
        "validation",
    );
    let provider_invocations_after_failure = fixture.provider_invocation_count();
    assert!(provider_invocations_after_failure > provider_invocations_after_success);
    assert_eq!(
        fixture.provider_invocation_count(),
        provider_invocations_after_failure,
        "failed delivery refusal must not launch the provider"
    );

    // An unknown task in the authorized approval remains an explicit refusal.
    assert_delivery_refused(
        client
            .inspect_task_delivery(&fixture.approval_id, "task-not-approved")
            .await,
        "unknown_task",
    );

    // Foreign caller bindings cannot inspect this approval's delivery.
    let wrong_binding = ApprovedHostClient::open(
        &fixture.registry_dir,
        &fixture.plans_dir,
        &fixture.authority_dir,
        &fixture.sessions_dir,
        "bnd_wrong_delivery_scope",
        THREAD_UUID,
    )
    .expect("open wrong-binding client");
    match wrong_binding
        .inspect_task_delivery(&fixture.approval_id, TASK_ID)
        .await
    {
        Err(HostError::ScopeMismatch { field, .. }) => {
            assert_eq!(field, "control_binding_id");
        }
        other => panic!("expected foreign-binding scope refusal, got {other:?}"),
    }

    let idle_status = wait_for_host_idle(&client, &fixture.approval_id, &mut host).await;
    assert_eq!(
        idle_status.instance_id, status.instance_id,
        "delivery inspection and refusal probes must not restart the host"
    );
    let instance_id = idle_status.instance_id.clone();
    let session_binding_id = idle_status.session_binding_id.clone();
    let stopped = client
        .stop_idle_session(&fixture.approval_id, &instance_id)
        .await
        .expect("authenticated idle stop after delivery inspections");
    let exit = host.wait_for_exit(EXIT_TIMEOUT).await;
    assert_eq!(
        exit.code(),
        Some(0),
        "host process must exit naturally after delivery inspections"
    );
    assert_durable_stopped(&fixture, &instance_id, &session_binding_id, &stopped);
    assert_eq!(
        fixture.provider_invocation_count(),
        provider_invocations_after_failure,
        "idle stop must not launch the provider again"
    );

    let fingerprint_before = capture_stopped_read_fingerprint(&fixture);
    let stopped_delivery = client
        .inspect_task_delivery(&fixture.approval_id, TASK_ID)
        .await
        .expect("inspect the clean stopped delivery");
    assert_eq!(
        stopped_delivery, live_delivery,
        "clean stopped inspection must preserve the live result and artifact identity"
    );
    client
        .recheck_task_delivery(&fixture.approval_id, TASK_ID, &stopped_delivery)
        .await
        .expect("recheck clean stopped delivery");
    let fingerprint_after = capture_stopped_read_fingerprint(&fixture);
    assert_eq!(
        fingerprint_before, fingerprint_after,
        "stopped delivery inspection and recheck must not change manifest, receipts, DB, or sidecars"
    );
    assert!(
        fingerprint_after.wal.is_none() && fingerprint_after.shm.is_none(),
        "clean stopped delivery reads must not gain WAL/SHM sidecars"
    );
    assert!(
        !is_host_lock_held(&fixture.session_dir()).expect("probe stopped host.lock"),
        "delivery reads must not restart the host or reacquire host.lock"
    );
    assert_eq!(
        fixture.provider_invocation_count(),
        provider_invocations_after_failure,
        "stopped delivery inspection must not launch the provider"
    );

    // A missing sealed receipt remains NotStarted and cannot borrow a delivery.
    let receipts_path = fixture.session_dir().join(TASK_RECEIPTS_FILENAME);
    let missing_receipts_path =
        stopped_read_backup_path(&fixture, "delivery-task-receipts.json.missing");
    fs::rename(&receipts_path, &missing_receipts_path).expect("move receipts ledger");
    assert_delivery_refused(
        client
            .inspect_task_delivery(&fixture.approval_id, TASK_ID)
            .await,
        "validation",
    );
    fs::rename(&missing_receipts_path, &receipts_path).expect("restore receipts ledger");
    client
        .recheck_task_delivery(&fixture.approval_id, TASK_ID, &stopped_delivery)
        .await
        .expect("restored stopped delivery still rechecks");

    // Recheck must recompute the current artifact rather than trust the prior receipt.
    let task_checkout = fixture
        .session_dir()
        .join(WORK_AREA_SUBDIR)
        .join(CHECKOUTS_SUBDIR)
        .join("tasks")
        .join(&stopped_delivery.worktree_id);
    fs::write(
        task_checkout.join("src/lib.rs"),
        "pub fn alpha() -> u32 { 99 }\n",
    )
    .expect("change the inspected task checkout");
    let mismatch = client
        .recheck_task_delivery(&fixture.approval_id, TASK_ID, &stopped_delivery)
        .await
        .expect_err("changed artifact must refuse delivery recheck");
    assert_eq!(mismatch.code(), "security_violation");
    let changed_delivery = client
        .inspect_task_delivery(&fixture.approval_id, TASK_ID)
        .await
        .expect("changed checkout remains inspectable as a different delivery");
    assert_ne!(
        changed_delivery.delivery_digest,
        stopped_delivery.delivery_digest
    );
    assert_eq!(
        changed_delivery.result_digest, stopped_delivery.result_digest,
        "artifact mutation must not rewrite the terminal result identity"
    );
    assert_eq!(
        fixture.provider_invocation_count(),
        provider_invocations_after_failure,
        "artifact mismatch probes must not launch the provider"
    );
    assert!(
        !is_host_lock_held(&fixture.session_dir()).expect("probe stopped host.lock"),
        "artifact mismatch probes must not restart the host"
    );
}

#[tokio::test]
async fn actual_worker_presence_is_scoped_fresh_and_never_survives_clean_stop() {
    let fixture = setup_host_presence_fixture();
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
    let release = PresenceReleaseGuard::new(fixture.presence_release.clone());

    // Frozen authority and exact task resolution happen even before a host session exists.
    assert_no_live_presence(
        client
            .get_task_presence(&fixture.approval_id, PRESENCE_TASK_ID)
            .await,
        "pre-launch presence",
    );
    match client
        .get_task_presence(&fixture.approval_id, "task-not-approved")
        .await
    {
        Err(HostError::UnknownTask { .. }) => {}
        other => panic!("expected UnknownTask before launch, got {other:?}"),
    }
    let wrong_binding = ApprovedHostClient::open(
        &fixture.registry_dir,
        &fixture.plans_dir,
        &fixture.authority_dir,
        &fixture.sessions_dir,
        "bnd_wrong_presence_scope",
        THREAD_UUID,
    )
    .expect("open wrong-binding presence client");
    match wrong_binding
        .get_task_presence(&fixture.approval_id, PRESENCE_TASK_ID)
        .await
    {
        Err(HostError::ScopeMismatch { field, .. }) => {
            assert_eq!(field, "control_binding_id");
        }
        other => panic!("expected foreign-binding presence refusal, got {other:?}"),
    }
    let other_thread_uuid = "660e8400-e29b-41d4-a716-446655440001";
    let wrong_thread = ApprovedHostClient::open(
        &fixture.registry_dir,
        &fixture.plans_dir,
        &fixture.authority_dir,
        &fixture.sessions_dir,
        CONTROL_BINDING_ID,
        other_thread_uuid,
    )
    .expect("open wrong-thread presence client");
    match wrong_thread
        .get_task_presence(&fixture.approval_id, PRESENCE_TASK_ID)
        .await
    {
        Err(HostError::ScopeMismatch { field, .. }) => {
            assert_eq!(field, "thread_uuid");
        }
        other => panic!("expected foreign-thread presence refusal, got {other:?}"),
    }

    let mut host = spawn_host_process(&fixture, "presence", None);
    let status =
        wait_for_authenticated_ready(&client, &fixture.approval_id, &mut host, |_| true).await;
    assert_live_ready(&fixture, &client, &mut host, &status);
    assert_no_live_presence(
        client
            .get_task_presence(&fixture.approval_id, PRESENCE_TASK_ID)
            .await,
        "unstarted presence",
    );
    fixture.assert_no_provider_invocation();

    client
        .dispatch_task(&fixture.approval_id, PRESENCE_TASK_ID)
        .await
        .expect("dispatch marker-gated presence task");
    wait_for_presence_child_started(&fixture, &mut host).await;
    let live =
        wait_for_live_presence(&client, &fixture.approval_id, PRESENCE_TASK_ID, &mut host).await;

    let record = fixture.approval_record();
    let session_binding_id = status.session_binding_id.clone();
    let receipt = read_task_receipt(
        &fixture.session_dir(),
        &record,
        &session_binding_id,
        PRESENCE_TASK_ID,
    )
    .expect("read presence task receipt")
    .expect("presence task receipt exists");

    assert_eq!(live.binding_id, session_binding_id);
    assert_eq!(live.run_id, receipt.run_id);
    assert_eq!(live.worker_id, receipt.worker_id);
    assert_eq!(live.profile_id, receipt.profile_id);
    assert_eq!(live.execution_binding, receipt.execution_binding);
    assert!(!live.worker_status.is_terminal());
    assert!(live.claim_id.is_some());
    assert!(live.attempt_count >= 1);
    let expected_owner = threadlane::HostControlConfig {
        approval_id: fixture.approval_id.clone(),
        subject_digest: record.subject_digest.clone(),
        control_binding_id: record.binding_id.clone(),
        thread_uuid: record.thread_uuid.clone(),
        session_binding_id: session_binding_id.clone(),
        instance_id: status.instance_id.clone(),
    };
    assert_eq!(live.owner.as_ref(), Some(&expected_owner));
    let process = live.process.as_ref().expect("live owned process snapshot");
    assert!(process.is_valid());
    assert!(process.is_live());
    assert_eq!(
        process
            .process_group_id
            .and_then(|pgid| u32::try_from(pgid).ok()),
        process.child_pid
    );
    assert!(live.max_sample_age_ms > 0);
    assert!(
        live.sample_age_ms
            .is_some_and(|age| age <= live.max_sample_age_ms),
        "live presence sample age must stay within its bound"
    );
    assert!(live.sampled_at_ms.is_some());

    let running_task = client
        .get_task_live_or_stopped(&fixture.approval_id, PRESENCE_TASK_ID)
        .await
        .expect("read running presence task snapshot");
    let running_worker = running_task
        .worker
        .as_ref()
        .expect("running presence worker snapshot");
    assert_eq!(live.worker_revision, running_worker.revision);
    assert_eq!(live.worker_status, running_worker.status);
    assert_eq!(live.claim_id, running_worker.claim_id);
    assert_eq!(live.attempt_count, running_worker.attempt_count);
    assert_eq!(live.profile_digest, running_worker.profile_digest);
    assert_eq!(live.execution_binding, running_worker.execution_binding);

    let invocations_after_dispatch = fixture.provider_invocation_count();
    assert!(
        invocations_after_dispatch >= 1,
        "the marker-gated fake worker must have invoked its provider"
    );

    let manifest_path = fixture.session_dir().join("host-manifest.json");
    let receipts_path = fixture.session_dir().join(TASK_RECEIPTS_FILENAME);
    let manifest_before = read_optional_bytes(&manifest_path);
    let receipts_before = read_optional_bytes(&receipts_path);
    let repeated = client
        .get_task_presence(&fixture.approval_id, PRESENCE_TASK_ID)
        .await
        .expect("repeat live presence read")
        .expect("live presence remains available");
    assert_eq!(repeated.run_id, live.run_id);
    assert_eq!(repeated.worker_id, live.worker_id);
    assert_eq!(repeated.claim_id, live.claim_id);
    assert_eq!(repeated.attempt_count, live.attempt_count);
    assert!(
        repeated
            .sample_age_ms
            .is_some_and(|age| age <= repeated.max_sample_age_ms),
        "repeated presence sample age must stay within its bound"
    );
    assert_eq!(read_optional_bytes(&manifest_path), manifest_before);
    assert_eq!(read_optional_bytes(&receipts_path), receipts_before);
    assert_eq!(
        fixture.provider_invocation_count(),
        invocations_after_dispatch,
        "live presence reads must not launch the provider"
    );

    // A mismatched sealed receipt worker identity must not be adopted as live presence.
    let original_receipts = fs::read(&receipts_path).expect("read live receipts fixture");
    let mut ledger: serde_json::Value =
        serde_json::from_slice(&original_receipts).expect("parse live receipts fixture");
    let receipt_entry = ledger["receipts"]
        .as_array_mut()
        .expect("live receipts array")
        .iter_mut()
        .find(|entry| {
            entry.get("task_id").and_then(|value| value.as_str()) == Some(PRESENCE_TASK_ID)
        })
        .expect("presence task receipt entry");
    let actual_worker_id = receipt.worker_id.clone();
    let tampered_worker_id = if actual_worker_id == "wrk_00000000000000000000000000000001" {
        "wrk_ffffffffffffffffffffffffffffffff"
    } else {
        "wrk_00000000000000000000000000000001"
    };
    receipt_entry["worker_id"] = serde_json::Value::String(tampered_worker_id.to_string());
    fs::write(
        &receipts_path,
        serde_json::to_vec_pretty(&ledger).expect("serialize tampered presence receipts"),
    )
    .expect("write tampered presence receipts fixture");
    fs::set_permissions(&receipts_path, fs::Permissions::from_mode(0o600))
        .expect("keep tampered presence receipts private");
    assert_not_live_presence(
        client
            .get_task_presence(&fixture.approval_id, PRESENCE_TASK_ID)
            .await,
        "tampered receipt presence",
    );
    assert_eq!(
        fixture.provider_invocation_count(),
        invocations_after_dispatch,
        "receipt mismatch probes must not launch the provider"
    );
    fs::write(&receipts_path, &original_receipts).expect("restore live receipts fixture");
    fs::set_permissions(&receipts_path, fs::Permissions::from_mode(0o600))
        .expect("restore live receipts permissions");

    // A foreign current manifest instance must not match the runtime owner tuple.
    let original_manifest = fs::read(&manifest_path).expect("read live manifest fixture");
    let mut manifest_json: serde_json::Value =
        serde_json::from_slice(&original_manifest).expect("parse live manifest fixture");
    manifest_json["instance_id"] =
        serde_json::Value::String("hst_00000000000000000000000000000001".to_string());
    fs::write(
        &manifest_path,
        serde_json::to_vec_pretty(&manifest_json).expect("serialize tampered live manifest"),
    )
    .expect("write tampered live manifest fixture");
    fs::set_permissions(&manifest_path, fs::Permissions::from_mode(0o600))
        .expect("keep tampered live manifest private");
    assert_not_live_presence(
        client
            .get_task_presence(&fixture.approval_id, PRESENCE_TASK_ID)
            .await,
        "tampered instance presence",
    );
    assert_eq!(
        fixture.provider_invocation_count(),
        invocations_after_dispatch,
        "instance mismatch probes must not launch the provider"
    );
    fs::write(&manifest_path, &original_manifest).expect("restore live manifest fixture");
    fs::set_permissions(&manifest_path, fs::Permissions::from_mode(0o600))
        .expect("restore live manifest permissions");
    let restored = client
        .get_task_presence(&fixture.approval_id, PRESENCE_TASK_ID)
        .await
        .expect("restored live presence read")
        .expect("restored live presence remains available");
    assert_eq!(restored.run_id, live.run_id);

    release.release();
    let live_result =
        wait_for_task_ready(&client, &fixture.approval_id, PRESENCE_TASK_ID, &mut host).await;
    assert_eq!(live_result.status, TaskExecutionStatus::ReadyForReview);
    let invocations_after_success = fixture.provider_invocation_count();
    assert_eq!(
        invocations_after_success, invocations_after_dispatch,
        "completing the marker-gated worker must not relaunch the provider"
    );
    assert_no_live_presence(
        client
            .get_task_presence(&fixture.approval_id, PRESENCE_TASK_ID)
            .await,
        "completed presence",
    );

    client
        .dispatch_task(&fixture.approval_id, FAILED_TASK_ID)
        .await
        .expect("dispatch approved failing task");
    let failed_result = wait_for_task_status(
        &client,
        &fixture.approval_id,
        FAILED_TASK_ID,
        &mut host,
        TaskExecutionStatus::Failed,
    )
    .await;
    assert_eq!(failed_result.status, TaskExecutionStatus::Failed);
    assert_no_live_presence(
        client
            .get_task_presence(&fixture.approval_id, FAILED_TASK_ID)
            .await,
        "failed presence",
    );
    let invocations_after_failure = fixture.provider_invocation_count();
    assert!(invocations_after_failure > invocations_after_success);
    assert_eq!(
        fixture.provider_invocation_count(),
        invocations_after_failure,
        "failed presence reads must not launch the provider"
    );

    let idle = wait_for_host_idle(&client, &fixture.approval_id, &mut host).await;
    let instance_id = idle.instance_id.clone();
    let stopped = client
        .stop_idle_session(&fixture.approval_id, &instance_id)
        .await
        .expect("authenticated idle stop after presence observations");
    let exit = host.wait_for_exit(EXIT_TIMEOUT).await;
    assert_eq!(
        exit.code(),
        Some(0),
        "host process must exit naturally after presence observations"
    );
    assert_durable_stopped(&fixture, &instance_id, &session_binding_id, &stopped);
    assert_no_live_presence(
        client
            .get_task_presence(&fixture.approval_id, PRESENCE_TASK_ID)
            .await,
        "stopped presence",
    );
    assert!(
        !is_host_lock_held(&fixture.session_dir()).expect("probe stopped host.lock"),
        "presence reads must not revive the stopped host"
    );
    let stopped_result = client
        .get_task_live_or_stopped(&fixture.approval_id, PRESENCE_TASK_ID)
        .await
        .expect("original stopped task result remains readable");
    assert_eq!(stopped_result, live_result);
    assert_eq!(
        fixture.provider_invocation_count(),
        invocations_after_failure,
        "stopped presence and result reads must not launch the provider"
    );
}

#[derive(Debug, PartialEq, Eq)]
struct ReadonlyPresenceFileSnapshot {
    path: PathBuf,
    bytes: Option<Vec<u8>>,
    mode: Option<u32>,
}

fn snapshot_readonly_presence_file(path: &Path) -> ReadonlyPresenceFileSnapshot {
    let mode = match fs::symlink_metadata(path) {
        Ok(meta) => Some(meta.permissions().mode() & 0o777),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => None,
        Err(err) => panic!("stat presence fixture `{}`: {err}", path.display()),
    };
    ReadonlyPresenceFileSnapshot {
        path: path.to_path_buf(),
        bytes: read_optional_bytes(path),
        mode,
    }
}

fn capture_readonly_presence_files(
    fixture: &HostProcessFixture,
) -> Vec<ReadonlyPresenceFileSnapshot> {
    let session_dir = fixture.session_dir();
    [
        fixture.authority.approvals_path().to_path_buf(),
        fixture.authority.lock_path().to_path_buf(),
        session_dir.join("host.lock"),
        session_dir.join("host-manifest.json"),
        session_dir.join(TASK_RECEIPTS_FILENAME),
        session_dir.join("task-receipts.lock"),
    ]
    .into_iter()
    .map(|path| snapshot_readonly_presence_file(&path))
    .collect()
}

#[derive(Debug, PartialEq, Eq)]
struct ReadonlyPresenceDirSnapshot {
    path: PathBuf,
    mode: Option<u32>,
}

fn snapshot_readonly_presence_dir(path: &Path) -> ReadonlyPresenceDirSnapshot {
    let mode = match fs::symlink_metadata(path) {
        Ok(meta) => Some(meta.permissions().mode() & 0o777),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => None,
        Err(err) => panic!("stat presence directory `{}`: {err}", path.display()),
    };
    ReadonlyPresenceDirSnapshot {
        path: path.to_path_buf(),
        mode,
    }
}

fn capture_readonly_presence_dirs(
    fixture: &HostProcessFixture,
) -> Vec<ReadonlyPresenceDirSnapshot> {
    [
        fixture.authority.dir().to_path_buf(),
        fixture.sessions_dir.clone(),
        fixture.session_dir(),
    ]
    .into_iter()
    .map(|path| snapshot_readonly_presence_dir(&path))
    .collect()
}

async fn readonly_presence_at_fixture(
    fixture: &HostProcessFixture,
    sessions_dir: &Path,
    control_binding_id: &str,
    native_thread_uuid: &str,
    task_id: &str,
) -> Result<Option<WorkerPresenceSnapshot>, HostError> {
    ApprovedHostClient::get_task_presence_readonly_at(
        &fixture.authority_dir,
        sessions_dir,
        control_binding_id,
        native_thread_uuid,
        &fixture.approval_id,
        task_id,
    )
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn readonly_presence_static_api_is_noncreating_and_handles_live_stopped_and_tamper() {
    let fixture = setup_host_presence_fixture();
    fixture.assert_no_provider_invocation();

    // A missing sessions root is unavailable, but the read-only route must not initialize it.
    assert!(!fixture.sessions_dir.exists());
    let missing_root = readonly_presence_at_fixture(
        &fixture,
        &fixture.sessions_dir,
        CONTROL_BINDING_ID,
        THREAD_UUID,
        PRESENCE_TASK_ID,
    )
    .await;
    assert!(
        missing_root.is_err(),
        "missing sessions root must be unavailable, got {missing_root:?}"
    );
    assert!(
        !fixture.sessions_dir.exists(),
        "read-only presence must never create a missing sessions root"
    );

    fs::create_dir(&fixture.sessions_dir).expect("create existing private sessions root");
    fs::set_permissions(&fixture.sessions_dir, fs::Permissions::from_mode(0o700))
        .expect("chmod existing sessions root 0700");

    // A missing authority store is unavailable and must not be initialized either.
    let missing_authority_dir = fixture._tmp.path().join("missing-authority-store");
    let missing_authority = ApprovedHostClient::get_task_presence_readonly_at(
        &missing_authority_dir,
        &fixture.sessions_dir,
        CONTROL_BINDING_ID,
        THREAD_UUID,
        &fixture.approval_id,
        PRESENCE_TASK_ID,
    )
    .await;
    assert!(
        missing_authority.is_err(),
        "missing authority store must be unavailable, got {missing_authority:?}"
    );
    assert!(
        !missing_authority_dir.exists(),
        "read-only presence must never create a missing authority store"
    );

    // A missing authority lock must not be created by the read-only authority path.
    let authority_lock_backup = fixture._tmp.path().join("missing-authority.lock");
    fs::rename(fixture.authority.lock_path(), &authority_lock_backup)
        .expect("move authority.lock outside the authority store");
    assert_no_live_presence(
        readonly_presence_at_fixture(
            &fixture,
            &fixture.sessions_dir,
            CONTROL_BINDING_ID,
            THREAD_UUID,
            PRESENCE_TASK_ID,
        )
        .await,
        "missing authority.lock and session directory",
    );
    assert!(
        !fixture.authority.lock_path().exists(),
        "read-only presence must not create a missing authority.lock"
    );
    fs::rename(&authority_lock_backup, fixture.authority.lock_path())
        .expect("restore authority.lock");

    // A tampered authority ledger fails closed and is preserved byte-for-byte.
    let approvals_path = fixture.authority.approvals_path().to_path_buf();
    let original_approvals = fs::read(&approvals_path).expect("read authority ledger fixture");
    let tampered_approvals = b"{ not valid authority ledger json\n";
    fs::write(&approvals_path, tampered_approvals).expect("write tampered authority ledger");
    fs::set_permissions(&approvals_path, fs::Permissions::from_mode(0o600))
        .expect("keep tampered authority ledger private");
    let tampered_authority = readonly_presence_at_fixture(
        &fixture,
        &fixture.sessions_dir,
        CONTROL_BINDING_ID,
        THREAD_UUID,
        PRESENCE_TASK_ID,
    )
    .await;
    assert!(
        tampered_authority.is_err(),
        "tampered authority ledger must fail closed, got {tampered_authority:?}"
    );
    assert_eq!(
        fs::read(&approvals_path)
            .expect("reread tampered authority ledger")
            .as_slice(),
        tampered_approvals.as_slice(),
        "read-only presence must not rewrite a tampered authority ledger"
    );
    fs::write(&approvals_path, &original_approvals).expect("restore authority ledger fixture");
    fs::set_permissions(&approvals_path, fs::Permissions::from_mode(0o600))
        .expect("restore authority ledger permissions");

    // A missing session directory is simply not live and must not be created.
    assert_no_live_presence(
        readonly_presence_at_fixture(
            &fixture,
            &fixture.sessions_dir,
            CONTROL_BINDING_ID,
            THREAD_UUID,
            PRESENCE_TASK_ID,
        )
        .await,
        "missing session directory",
    );
    assert!(
        !fixture.session_dir().exists(),
        "read-only presence must never create a missing session directory"
    );

    // Explicit caller scope is checked before any missing sessions root would be touched.
    let unscoped_sessions = fixture._tmp.path().join("unscoped-sessions-must-not-exist");
    match ApprovedHostClient::get_task_presence_readonly_at(
        &fixture.authority_dir,
        &unscoped_sessions,
        "bnd_wrong_presence_scope",
        THREAD_UUID,
        &fixture.approval_id,
        PRESENCE_TASK_ID,
    )
    .await
    {
        Err(HostError::ScopeMismatch { field, .. }) => {
            assert_eq!(field, "control_binding_id");
        }
        other => panic!("expected foreign-binding presence refusal, got {other:?}"),
    }
    let other_thread_uuid = "660e8400-e29b-41d4-a716-446655440001";
    match ApprovedHostClient::get_task_presence_readonly_at(
        &fixture.authority_dir,
        &unscoped_sessions,
        CONTROL_BINDING_ID,
        other_thread_uuid,
        &fixture.approval_id,
        PRESENCE_TASK_ID,
    )
    .await
    {
        Err(HostError::ScopeMismatch { field, .. }) => {
            assert_eq!(field, "thread_uuid");
        }
        other => panic!("expected foreign-thread presence refusal, got {other:?}"),
    }
    assert!(
        !unscoped_sessions.exists(),
        "scope refusal must happen before touching a missing sessions root"
    );

    let client = ApprovedHostClient::open(
        &fixture.registry_dir,
        &fixture.plans_dir,
        &fixture.authority_dir,
        &fixture.sessions_dir,
        CONTROL_BINDING_ID,
        THREAD_UUID,
    )
    .expect("open authoritative approved host client");
    let release = PresenceReleaseGuard::new(fixture.presence_release.clone());

    let mut host = spawn_host_process(&fixture, "readonly-presence", None);
    let status =
        wait_for_authenticated_ready(&client, &fixture.approval_id, &mut host, |_| true).await;
    assert_live_ready(&fixture, &client, &mut host, &status);

    let unstarted_files = capture_readonly_presence_files(&fixture);
    let unstarted_dirs = capture_readonly_presence_dirs(&fixture);
    assert_no_live_presence(
        readonly_presence_at_fixture(
            &fixture,
            &fixture.sessions_dir,
            CONTROL_BINDING_ID,
            THREAD_UUID,
            PRESENCE_TASK_ID,
        )
        .await,
        "unstarted static presence",
    );
    assert_eq!(
        capture_readonly_presence_files(&fixture),
        unstarted_files,
        "unstarted read-only presence must preserve file bytes and permissions"
    );
    assert_eq!(
        capture_readonly_presence_dirs(&fixture),
        unstarted_dirs,
        "unstarted read-only presence must preserve directory permissions"
    );
    fixture.assert_no_provider_invocation();

    client
        .dispatch_task(&fixture.approval_id, PRESENCE_TASK_ID)
        .await
        .expect("dispatch marker-gated presence task");
    wait_for_presence_child_started(&fixture, &mut host).await;
    let live =
        wait_for_live_presence(&client, &fixture.approval_id, PRESENCE_TASK_ID, &mut host).await;
    let invocations_after_dispatch = fixture.provider_invocation_count();
    assert!(
        invocations_after_dispatch >= 1,
        "the marker-gated fake worker must have invoked its provider"
    );
    let static_live = readonly_presence_at_fixture(
        &fixture,
        &fixture.sessions_dir,
        CONTROL_BINDING_ID,
        THREAD_UUID,
        PRESENCE_TASK_ID,
    )
    .await
    .expect("read live presence through static read-only API")
    .expect("static read-only API exposes the live worker");
    assert_eq!(static_live.run_id, live.run_id);
    assert_eq!(static_live.worker_id, live.worker_id);
    assert_eq!(static_live.claim_id, live.claim_id);
    assert_eq!(static_live.attempt_count, live.attempt_count);
    assert_eq!(static_live.owner, live.owner);

    let live_files = capture_readonly_presence_files(&fixture);
    let live_dirs = capture_readonly_presence_dirs(&fixture);

    // Externally held store/receipt locks must not block or perturb the read-only route.
    let authority_lock = OpenOptions::new()
        .read(true)
        .write(true)
        .open(fixture.authority.lock_path())
        .expect("open authority.lock for external holder");
    authority_lock
        .try_lock_exclusive()
        .expect("hold authority.lock externally");
    let receipts_lock_path = fixture.session_dir().join("task-receipts.lock");
    let receipts_lock = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&receipts_lock_path)
        .expect("open task-receipts.lock for external holder");
    receipts_lock
        .try_lock_exclusive()
        .expect("hold task-receipts.lock externally");
    let externally_locked = tokio::time::timeout(
        Duration::from_secs(5),
        readonly_presence_at_fixture(
            &fixture,
            &fixture.sessions_dir,
            CONTROL_BINDING_ID,
            THREAD_UUID,
            PRESENCE_TASK_ID,
        ),
    )
    .await
    .expect("read-only presence must not block on externally held store or receipt locks")
    .expect("read-only presence under external locks")
    .expect("live presence remains available under external locks");
    assert_eq!(externally_locked.run_id, live.run_id);
    assert_eq!(externally_locked.worker_id, live.worker_id);
    assert_eq!(externally_locked.claim_id, live.claim_id);
    receipts_lock
        .unlock()
        .expect("release external task-receipts.lock");
    authority_lock
        .unlock()
        .expect("release external authority.lock");

    // A missing host.lock means non-live and must never be recreated by the presence read.
    let host_lock_path = fixture.session_dir().join("host.lock");
    let missing_host_lock_backup =
        stopped_read_backup_path(&fixture, "presence-live-host.lock.missing");
    fs::rename(&host_lock_path, &missing_host_lock_backup).expect("move live host.lock");
    assert_no_live_presence(
        readonly_presence_at_fixture(
            &fixture,
            &fixture.sessions_dir,
            CONTROL_BINDING_ID,
            THREAD_UUID,
            PRESENCE_TASK_ID,
        )
        .await,
        "missing host.lock presence",
    );
    assert!(
        !host_lock_path.exists(),
        "read-only presence must not recreate a missing host.lock"
    );
    fs::rename(&missing_host_lock_backup, &host_lock_path).expect("restore live host.lock");
    let restored = readonly_presence_at_fixture(
        &fixture,
        &fixture.sessions_dir,
        CONTROL_BINDING_ID,
        THREAD_UUID,
        PRESENCE_TASK_ID,
    )
    .await
    .expect("read restored live presence")
    .expect("restored host.lock makes live presence available again");
    assert_eq!(restored.run_id, live.run_id);

    // A tampered sealed receipt must never be adopted as a live observation.
    let receipts_path = fixture.session_dir().join(TASK_RECEIPTS_FILENAME);
    let original_receipts = fs::read(&receipts_path).expect("read live receipts fixture");
    let mut ledger: serde_json::Value =
        serde_json::from_slice(&original_receipts).expect("parse live receipts fixture");
    let receipt_entry = ledger["receipts"]
        .as_array_mut()
        .expect("live receipts array")
        .iter_mut()
        .find(|entry| {
            entry.get("task_id").and_then(|value| value.as_str()) == Some(PRESENCE_TASK_ID)
        })
        .expect("presence task receipt entry");
    let actual_worker_id = receipt_entry["worker_id"]
        .as_str()
        .expect("receipt worker_id")
        .to_string();
    let tampered_worker_id = if actual_worker_id == "wrk_00000000000000000000000000000001" {
        "wrk_ffffffffffffffffffffffffffffffff"
    } else {
        "wrk_00000000000000000000000000000001"
    };
    receipt_entry["worker_id"] = serde_json::Value::String(tampered_worker_id.to_string());
    fs::write(
        &receipts_path,
        serde_json::to_vec_pretty(&ledger).expect("serialize tampered presence receipts"),
    )
    .expect("write tampered presence receipts fixture");
    fs::set_permissions(&receipts_path, fs::Permissions::from_mode(0o600))
        .expect("keep tampered presence receipts private");
    assert_not_live_presence(
        readonly_presence_at_fixture(
            &fixture,
            &fixture.sessions_dir,
            CONTROL_BINDING_ID,
            THREAD_UUID,
            PRESENCE_TASK_ID,
        )
        .await,
        "tampered static receipt presence",
    );
    fs::write(&receipts_path, &original_receipts).expect("restore live receipts fixture");
    fs::set_permissions(&receipts_path, fs::Permissions::from_mode(0o600))
        .expect("restore live receipts permissions");

    // A stale/foreign manifest instance must not be adopted by the exact-instance check.
    let manifest_path = fixture.session_dir().join("host-manifest.json");
    let original_manifest = fs::read(&manifest_path).expect("read live manifest fixture");
    let mut manifest_json: serde_json::Value =
        serde_json::from_slice(&original_manifest).expect("parse live manifest fixture");
    let actual_instance_id = manifest_json["instance_id"]
        .as_str()
        .expect("live manifest instance_id")
        .to_string();
    let stale_instance_id = if actual_instance_id == "hst_00000000000000000000000000000001" {
        "hst_00000000000000000000000000000002"
    } else {
        "hst_00000000000000000000000000000001"
    };
    manifest_json["instance_id"] = serde_json::Value::String(stale_instance_id.to_string());
    fs::write(
        &manifest_path,
        serde_json::to_vec_pretty(&manifest_json).expect("serialize stale live manifest"),
    )
    .expect("write stale live manifest fixture");
    fs::set_permissions(&manifest_path, fs::Permissions::from_mode(0o600))
        .expect("keep stale live manifest private");
    assert_not_live_presence(
        readonly_presence_at_fixture(
            &fixture,
            &fixture.sessions_dir,
            CONTROL_BINDING_ID,
            THREAD_UUID,
            PRESENCE_TASK_ID,
        )
        .await,
        "stale instance static presence",
    );
    fs::write(&manifest_path, &original_manifest).expect("restore live manifest fixture");
    fs::set_permissions(&manifest_path, fs::Permissions::from_mode(0o600))
        .expect("restore live manifest permissions");

    let restored_after_tamper = readonly_presence_at_fixture(
        &fixture,
        &fixture.sessions_dir,
        CONTROL_BINDING_ID,
        THREAD_UUID,
        PRESENCE_TASK_ID,
    )
    .await
    .expect("read presence after restoring live fixtures")
    .expect("restored fixtures expose live presence");
    assert_eq!(restored_after_tamper.run_id, live.run_id);
    assert_eq!(
        capture_readonly_presence_files(&fixture),
        live_files,
        "read-only live presence probes must not change manifest, receipt, authority, or lock bytes/permissions"
    );
    assert_eq!(
        capture_readonly_presence_dirs(&fixture),
        live_dirs,
        "read-only live presence probes must not change authority, sessions, or session directory permissions"
    );
    assert_eq!(
        fixture.provider_invocation_count(),
        invocations_after_dispatch,
        "read-only live presence probes must not launch the provider"
    );

    release.release();
    let live_result =
        wait_for_task_ready(&client, &fixture.approval_id, PRESENCE_TASK_ID, &mut host).await;
    assert_eq!(live_result.status, TaskExecutionStatus::ReadyForReview);
    let invocations_after_success = fixture.provider_invocation_count();
    assert_no_live_presence(
        readonly_presence_at_fixture(
            &fixture,
            &fixture.sessions_dir,
            CONTROL_BINDING_ID,
            THREAD_UUID,
            PRESENCE_TASK_ID,
        )
        .await,
        "completed static presence",
    );
    assert_eq!(
        fixture.provider_invocation_count(),
        invocations_after_success,
        "completed static presence must not launch the provider"
    );

    let idle = wait_for_host_idle(&client, &fixture.approval_id, &mut host).await;
    let instance_id = idle.instance_id.clone();
    let session_binding_id = idle.session_binding_id.clone();
    let stopped = client
        .stop_idle_session(&fixture.approval_id, &instance_id)
        .await
        .expect("authenticated idle stop after static presence reads");
    let exit = host.wait_for_exit(EXIT_TIMEOUT).await;
    assert_eq!(
        exit.code(),
        Some(0),
        "host process must exit naturally after static presence reads"
    );
    assert_durable_stopped(&fixture, &instance_id, &session_binding_id, &stopped);

    let stopped_fingerprint = capture_stopped_read_fingerprint(&fixture);
    let stopped_files = capture_readonly_presence_files(&fixture);
    let stopped_dirs = capture_readonly_presence_dirs(&fixture);
    assert_no_live_presence(
        readonly_presence_at_fixture(
            &fixture,
            &fixture.sessions_dir,
            CONTROL_BINDING_ID,
            THREAD_UUID,
            PRESENCE_TASK_ID,
        )
        .await,
        "stopped static presence",
    );
    assert_eq!(
        capture_stopped_read_fingerprint(&fixture),
        stopped_fingerprint,
        "stopped static presence must not change manifest, receipts, DB, or sidecars"
    );
    assert_eq!(
        capture_readonly_presence_files(&fixture),
        stopped_files,
        "stopped static presence must not change authority, manifest, receipt, or lock bytes/permissions"
    );
    assert_eq!(
        capture_readonly_presence_dirs(&fixture),
        stopped_dirs,
        "stopped static presence must not change authority, sessions, or session directory permissions"
    );
    assert!(
        !is_host_lock_held(&fixture.session_dir()).expect("probe stopped host.lock"),
        "static presence must not revive the stopped host"
    );
    assert_eq!(
        fixture.provider_invocation_count(),
        invocations_after_success,
        "stopped static presence must not launch the provider"
    );
}
