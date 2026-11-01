#![forbid(unsafe_code)]
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

mod host_control;

use std::env;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::Arc;

use clap::Parser;
use host_control::HostControlHub;
use serde::{Deserialize, Serialize};
use tauri::{Manager, State, Window};
use threadlane_authority::{
    validate_canonical_thread_uuid, ApprovalSummary, AuthorityError, AuthorityStore,
    OperatorApprovalRequest,
};
use threadlane_connectors::{
    probe_tool, ConnectorError, CredentialStore, KeyringCredentialStore, ToolProbe,
};
use threadlane_decisions::{
    DecisionAdvice, DecisionClient, DecisionError, DecisionProvider, DecisionRequest,
};
use threadlane_desktop_bridge::{
    ActivityPage, AttachmentMode, ConnectionState, DesktopBridge, DesktopSnapshot, EventKind,
    RunDetail, ToolStatus, WindowAction,
};
use threadlane_host::{HostSessionSummary, TaskStateSnapshot};
use threadlane_native::{NativeClient, NativeConfig, NativeError, NativeThreadPage};
use threadlane_plans::{
    compile_plan_revision, Difficulty, PlanChange, PlanError, PlanRevision, PlanStatus, PlanStore,
    WorkflowRole,
};
use threadlane_registry::{
    ModelDefinition, RegistryChange, RegistryError, RegistrySnapshot, RegistryStore, ToolAdapter,
};

const SYNTHETIC_DECISION_TASK_SUMMARY: &str =
    "Synthetic desktop verification sample: classify a bounded single-file configuration update with unit test validation.";

/// Native Tauri 2 desktop control panel for Threadlane.
#[derive(Debug, Parser)]
#[command(
    name = "threadlane-desktop",
    version,
    about = "Threadlane native desktop control panel"
)]
struct DesktopCli {
    /// Optional path to an existing Threadlane runtime state directory to observe.
    #[arg(long = "state-dir")]
    state_dir: Option<PathBuf>,

    /// Optional path to a dedicated owned local demo runtime state directory.
    #[arg(long = "demo-state-dir")]
    demo_state_dir: Option<PathBuf>,

    /// Optional path to a dedicated private Threadlane registry configuration directory.
    #[arg(long = "registry-dir")]
    registry_dir: Option<PathBuf>,

    /// Optional path to a trusted local threadlane-host executable binary.
    #[arg(long = "host-executable")]
    host_executable: Option<PathBuf>,
}

/// Safe structured error returned to the desktop frontend over Tauri IPC.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct AppError {
    pub(crate) code: String,
    pub(crate) message: String,
}

impl AppError {
    pub(crate) fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code: code.to_string(),
            message: sanitize_error_message(&message.into(), "Operation failed.", None),
        }
    }

    fn from_registry_error(err: RegistryError) -> Self {
        let code = err.code();
        let raw = err.to_string();
        Self {
            code: code.to_string(),
            message: sanitize_error_message(&raw, "Registry operation failed.", None),
        }
    }

    fn from_connector_error(
        err: ConnectorError,
        default_code: &'static str,
        fallback_message: &'static str,
        redacted_secret: Option<&str>,
    ) -> Self {
        let raw = err.to_string();
        let code = classify_error_code(&raw, default_code);
        Self {
            code: code.to_string(),
            message: sanitize_error_message(&raw, fallback_message, redacted_secret),
        }
    }

    fn from_decision_error(err: DecisionError, default_code: &'static str) -> Self {
        let raw = err.to_string();
        let code = classify_error_code(&raw, default_code);
        Self {
            code: code.to_string(),
            message: sanitize_error_message(&raw, "Decision provider request failed.", None),
        }
    }

    fn from_plan_error(err: PlanError) -> Self {
        let raw = err.to_string();
        let code = classify_error_code(&raw, "plan_error");
        Self {
            code: code.to_string(),
            message: sanitize_error_message(&raw, "Plan ledger operation failed.", None),
        }
    }

    fn from_plan_preview_error(err: PlanError) -> Self {
        let code = match &err {
            PlanError::UnpublishedPlan { .. } => "unpublished_or_superseded_plan",
            PlanError::ProfileRevisionMismatch { .. } => "stale_registry_revision",
            other => other.code(),
        };
        Self {
            code: code.to_string(),
            message: err.safe_message().to_string(),
        }
    }

    pub(crate) fn from_authority_error(err: AuthorityError) -> Self {
        let (code, message) = match err {
            AuthorityError::Validation { field, .. } => {
                let msg = match field.as_str() {
                    "thread_uuid" | "session_binding.thread_uuid" => {
                        "Thread UUID must be a canonical lowercase non-nil UUID."
                    }
                    "max_parallel" => "Maximum parallel workers must be between 1 and 8.",
                    "approved_task_ids" => "Select at least one valid task to approve.",
                    "approved_write_paths" => {
                        "Specify at least one valid repository-relative write scope."
                    }
                    "task_worker_spec" => {
                        "Selected task worker specification or execution budget is invalid."
                    }
                    "expected_plan_digest" => {
                        "Expected plan digest must be a 64-character lowercase hexadecimal SHA-256 digest."
                    }
                    "expected_registry_revision" | "plan_revision" => {
                        "Revision number must be a valid non-negative safe integer."
                    }
                    _ => "Operator approval request failed validation.",
                };
                ("validation", msg)
            }
            AuthorityError::PlanNotFound { .. } => (
                "plan_not_found",
                "Requested plan revision was not found in the plan ledger.",
            ),
            AuthorityError::ApprovalNotFound { .. } => (
                "approval_not_found",
                "Requested approval artifact was not found.",
            ),
            AuthorityError::BindingMismatch { .. } => (
                "binding_mismatch",
                "Plan binding identifier does not match the approval request binding.",
            ),
            AuthorityError::ThreadUuidMismatch { .. } => (
                "thread_uuid_mismatch",
                "Runtime binding thread UUID does not match the approved thread UUID.",
            ),
            AuthorityError::UnpublishedOrSupersededPlan { .. } => (
                "unpublished_or_superseded_plan",
                "Only active published plan revisions can be approved for execution.",
            ),
            AuthorityError::StaleRegistryRevision { .. } => (
                "stale_registry_revision",
                "Registry revision changed since this plan or preview was captured; refresh execution preview.",
            ),
            AuthorityError::PlanDigestMismatch { .. } => (
                "plan_digest_mismatch",
                "Compiled plan digest does not match the previewed plan snapshot; refresh execution preview.",
            ),
            AuthorityError::UnknownTask { .. } => (
                "unknown_task",
                "One or more selected task IDs do not exist in the published plan revision.",
            ),
            AuthorityError::UnsupportedTaskAdapter { .. } => (
                "unsupported_task_adapter",
                "Selected task uses an adapter that cannot be approved for worker execution.",
            ),
            AuthorityError::InvalidExecutable { .. } => (
                "invalid_executable",
                "Configured worker tool executable could not be resolved to a valid local binary.",
            ),
            AuthorityError::InvalidRepository { .. } => (
                "invalid_repository",
                "Repository path must be an existing canonical non-symlink Git repository.",
            ),
            AuthorityError::InvalidBaseCommit { .. } => (
                "invalid_base_commit",
                "Base commit must be an exact 40-character hexadecimal commit SHA existing in the repository.",
            ),
            AuthorityError::InvalidWriteScope { .. } => (
                "invalid_write_scope",
                "Approved write paths must be safe repository-relative file or directory scopes.",
            ),
            AuthorityError::UnapprovedTaskWriteScope { .. } => (
                "unapproved_task_write_scope",
                "Every selected task write path must be contained within the operator approved write paths.",
            ),
            AuthorityError::InvalidWorkArea { .. } => (
                "invalid_work_area",
                "Work area directory failed security or non-overlap validation.",
            ),
            AuthorityError::WorkersConfigInvalid { .. } => (
                "workers_config_invalid",
                "Synthesized worker configuration failed validation.",
            ),
            AuthorityError::SecurityViolation { .. } => (
                "security_violation",
                "Authority store or target path failed security validation.",
            ),
            AuthorityError::CorruptStore { .. } => (
                "corrupt_store",
                "Persisted authority store failed integrity verification.",
            ),
            AuthorityError::Io { .. } => ("io_error", "Failed to read or write the authority store."),
            AuthorityError::Registry { .. } => (
                "registry_error",
                "Failed to read the tool and workflow registry.",
            ),
            AuthorityError::Plan { source } => (source.code(), source.safe_message()),
        };
        Self {
            code: code.to_string(),
            message: message.to_string(),
        }
    }

    fn from_native_error(err: NativeError) -> Self {
        let raw = err.to_string();
        let code = classify_error_code(&raw, "native_error");
        Self {
            code: code.to_string(),
            message: sanitize_error_message(&raw, "Native Codex metadata request failed.", None),
        }
    }

    pub(crate) fn internal_join_error() -> Self {
        Self {
            code: "internal_task_error".to_string(),
            message: "Background desktop worker task failed.".to_string(),
        }
    }
}

fn classify_error_code<'a>(raw: &str, default_code: &'a str) -> &'a str {
    let lower = raw.to_ascii_lowercase();
    if lower.contains("immutable") || lower.contains("already published") {
        "immutable_revision"
    } else if lower.contains("revision") || lower.contains("stale") || lower.contains("conflict") {
        "revision_conflict"
    } else if lower.contains("not configured") || lower.contains("not_configured") {
        "not_configured"
    } else if lower.contains("needs_verification") || lower.contains("needs verification") {
        "needs_verification"
    } else if lower.contains("not found")
        || lower.contains("not_found")
        || lower.contains("missing")
    {
        "not_found"
    } else if lower.contains("locked") {
        "vault_locked"
    } else if lower.contains("unavailable")
        || lower.contains("keychain")
        || lower.contains("keyring")
    {
        "unavailable"
    } else if lower.contains("unauthorized")
        || lower.contains("forbidden")
        || lower.contains("401")
        || lower.contains("403")
        || lower.contains("auth")
    {
        "unauthorized"
    } else if lower.contains("rate limit") || lower.contains("429") || lower.contains("529") {
        "rate_limited"
    } else if lower.contains("timeout") || lower.contains("timed out") {
        "timeout"
    } else if lower.contains("cycle") || lower.contains("dag") || lower.contains("depend") {
        "invalid_dag"
    } else if lower.contains("invalid") || lower.contains("empty") || lower.contains("bound") {
        "invalid_input"
    } else {
        default_code
    }
}

fn sanitize_error_message(raw: &str, fallback: &str, secret_to_strip: Option<&str>) -> String {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return fallback.to_string();
    }
    if let Some(secret) = secret_to_strip {
        let secret_trimmed = secret.trim();
        if !secret_trimmed.is_empty() && trimmed.contains(secret_trimmed) {
            return fallback.to_string();
        }
    }

    let lower = trimmed.to_ascii_lowercase();
    if lower.contains("bearer ")
        || lower.contains("sk-")
        || lower.contains("api_key=")
        || lower.contains("apikey=")
        || lower.contains("secret=")
        || lower.contains("token=")
        || lower.contains("password=")
    {
        return fallback.to_string();
    }

    let mut cleaned = String::with_capacity(trimmed.len().min(220));
    for (count, ch) in trimmed.chars().enumerate() {
        if count >= 217 {
            cleaned.push_str("...");
            break;
        }
        if ch.is_control() {
            cleaned.push(' ');
        } else {
            cleaned.push(ch);
        }
    }

    let final_str = cleaned.trim();
    if final_str.is_empty() {
        fallback.to_string()
    } else {
        final_str.to_string()
    }
}

fn resolve_executable_on_path(binary_name: &str) -> Option<PathBuf> {
    let path_var = env::var_os("PATH")?;
    for dir in env::split_paths(&path_var) {
        if dir.as_os_str().is_empty() {
            continue;
        }
        let candidate = dir.join(binary_name);
        if is_executable_file(&candidate) {
            return Some(candidate);
        }
    }
    None
}

fn is_executable_file(path: &Path) -> bool {
    let Ok(metadata) = std::fs::metadata(path) else {
        return false;
    };
    if !metadata.is_file() {
        return false;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        metadata.permissions().mode() & 0o111 != 0
    }
    #[cfg(not(unix))]
    {
        true
    }
}

/// Safe execution preview for a single compiled task in a published plan revision.
///
/// Intentionally omits configured or canonical `executable_path` and any secret material.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct PlanTaskExecutionPreview {
    id: String,
    title: String,
    instruction: String,
    instruction_digest: String,
    role: WorkflowRole,
    difficulty: Difficulty,
    tool_id: String,
    tool_label: String,
    adapter: ToolAdapter,
    model_id: String,
    model_label: String,
    model_arg: String,
    effort: Option<String>,
    depends_on: Vec<String>,
    allowed_write_paths: Vec<String>,
    task_digest: String,
}

/// Safe execution preview for an active published [`PlanRevision`] compiled against the
/// current [`RegistrySnapshot`].
///
/// Strips `executable_path` from all tasks and never exposes raw [`threadlane_plans::CompiledPlan`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct PlanExecutionPreview {
    plan_id: String,
    plan_revision: u64,
    binding_id: String,
    profile_id: String,
    registry_revision: u64,
    status: PlanStatus,
    topological_order: Vec<String>,
    tasks: Vec<PlanTaskExecutionPreview>,
    plan_digest: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct VerifiedBindingContext {
    pub(crate) state_dir: String,
    pub(crate) binding_id: String,
    pub(crate) thread_uuid: String,
    pub(crate) selected_binding_id: Option<String>,
}

fn build_plan_execution_preview(
    plans: &PlanStore,
    registry: &RegistryStore,
    plan_id: &str,
    revision: u64,
) -> Result<PlanExecutionPreview, AppError> {
    let trimmed_id = plan_id.trim();
    if trimmed_id.is_empty() {
        return Err(AppError::new(
            "invalid_input",
            "Plan identifier is required to preview execution.",
        ));
    }

    let plan = plans
        .get_revision(trimmed_id, revision)
        .map_err(AppError::from_plan_preview_error)?
        .ok_or_else(|| {
            AppError::new(
                "plan_not_found",
                "Requested plan revision was not found in the plan ledger.",
            )
        })?;

    if !plan.status.is_active_published() {
        return Err(AppError::new(
            "unpublished_or_superseded_plan",
            "Only active published plan revisions can be previewed or approved for execution.",
        ));
    }

    let registry_snapshot = registry.snapshot().map_err(AppError::from_registry_error)?;

    if registry_snapshot.revision != plan.profile_revision {
        return Err(AppError::new(
            "stale_registry_revision",
            "Plan profile revision does not match the current registry snapshot.",
        ));
    }

    let compiled = compile_plan_revision(&plan, &registry_snapshot)
        .map_err(AppError::from_plan_preview_error)?;

    let tasks = compiled
        .tasks
        .into_iter()
        .map(|task| {
            let tool_label = registry_snapshot
                .tools
                .iter()
                .find(|t| t.id == task.tool_id)
                .map(|t| t.label.clone())
                .unwrap_or_else(|| task.tool_id.clone());
            let model_label = registry_snapshot
                .models
                .iter()
                .find(|m| m.id == task.choice.model_id)
                .map(|m| m.label.clone())
                .unwrap_or_else(|| task.choice.model_id.clone());

            PlanTaskExecutionPreview {
                id: task.id,
                title: task.title,
                instruction: task.instruction,
                instruction_digest: task.instruction_digest,
                role: task.role,
                difficulty: task.difficulty,
                tool_id: task.tool_id,
                tool_label,
                adapter: task.adapter,
                model_id: task.choice.model_id,
                model_label,
                model_arg: task.model_arg,
                effort: task.choice.effort,
                depends_on: task.depends_on,
                allowed_write_paths: task.allowed_write_paths,
                task_digest: task.task_digest,
            }
        })
        .collect();

    Ok(PlanExecutionPreview {
        plan_id: compiled.plan_id,
        plan_revision: compiled.plan_revision,
        binding_id: compiled.binding_id,
        profile_id: compiled.profile_id,
        registry_revision: compiled.profile_revision,
        status: compiled.status,
        topological_order: compiled.topological_order,
        tasks,
        plan_digest: compiled.plan_digest,
    })
}

pub(crate) fn verify_snapshot_binding_for_approval(
    snapshot: &DesktopSnapshot,
    binding_id: &str,
    thread_uuid: &str,
) -> Result<VerifiedBindingContext, AppError> {
    let trimmed_binding_id = binding_id.trim();
    if trimmed_binding_id.is_empty() || trimmed_binding_id != binding_id {
        return Err(AppError::new(
            "invalid_input",
            "Binding identifier is required for operator approval.",
        ));
    }

    let canonical_request_uuid = validate_canonical_thread_uuid("thread_uuid", thread_uuid)
        .map_err(|_| {
            AppError::new(
                "invalid_thread_uuid",
                "Thread UUID must be a canonical lowercase non-nil UUID.",
            )
        })?;

    match snapshot.attachment.mode {
        AttachmentMode::Unconfigured => {
            return Err(AppError::new(
                "runtime_not_attached",
                "Attach a live Threadlane runtime before approving a plan for execution.",
            ));
        }
        AttachmentMode::Demo => {
            return Err(AppError::new(
                "demo_binding_forbidden",
                "Local demo mode cannot record operator execution approvals.",
            ));
        }
        AttachmentMode::Runtime => {}
    }

    if snapshot.attachment.connection != ConnectionState::Connected {
        return Err(AppError::new(
            "runtime_offline",
            "Attached Threadlane runtime must be connected to verify conversation binding identity.",
        ));
    }

    let state_dir = snapshot
        .attachment
        .state_dir
        .as_deref()
        .map(str::trim)
        .filter(|dir| !dir.is_empty())
        .ok_or_else(|| {
            AppError::new(
                "runtime_not_attached",
                "Attached runtime state directory is not configured.",
            )
        })?
        .to_string();

    let binding = snapshot
        .bindings
        .iter()
        .find(|candidate| candidate.binding_id == trimmed_binding_id)
        .ok_or_else(|| {
            AppError::new(
                "binding_not_found",
                "Requested conversation binding is not present in the attached runtime.",
            )
        })?;

    if binding.is_local_demo {
        return Err(AppError::new(
            "demo_binding_forbidden",
            "Local demo bindings cannot be approved for worker execution.",
        ));
    }

    let canonical_binding_uuid =
        validate_canonical_thread_uuid("binding.thread_uuid", &binding.thread_uuid).map_err(
            |_| {
                AppError::new(
                    "invalid_thread_uuid",
                    "Attached runtime binding does not have a valid canonical native thread UUID.",
                )
            },
        )?;

    if canonical_binding_uuid != canonical_request_uuid {
        return Err(AppError::new(
            "thread_uuid_mismatch",
            "Requested native thread UUID does not match the attached runtime binding.",
        ));
    }

    Ok(VerifiedBindingContext {
        state_dir,
        binding_id: binding.binding_id.clone(),
        thread_uuid: canonical_binding_uuid,
        selected_binding_id: snapshot.selected_binding_id.clone(),
    })
}

pub(crate) fn verify_snapshot_recheck(
    captured: &VerifiedBindingContext,
    latest_snapshot: &DesktopSnapshot,
) -> Result<(), AppError> {
    let rechecked = verify_snapshot_binding_for_approval(
        latest_snapshot,
        &captured.binding_id,
        &captured.thread_uuid,
    )?;

    if rechecked.state_dir != captured.state_dir
        || rechecked.binding_id != captured.binding_id
        || rechecked.thread_uuid != captured.thread_uuid
        || rechecked.selected_binding_id != captured.selected_binding_id
    {
        return Err(AppError::new(
            "snapshot_changed_during_approval",
            "Runtime attachment or binding selection changed while validating approval; review and retry.",
        ));
    }

    Ok(())
}

fn precheck_approval_against_stores(
    plans: &PlanStore,
    registry: &RegistryStore,
    request: &OperatorApprovalRequest,
) -> Result<(), AppError> {
    let preview =
        build_plan_execution_preview(plans, registry, &request.plan_id, request.plan_revision)?;

    if preview.binding_id != request.binding_id {
        return Err(AppError::new(
            "binding_mismatch",
            "Plan binding identifier does not match the approval request binding.",
        ));
    }

    if preview.registry_revision != request.expected_registry_revision {
        return Err(AppError::new(
            "stale_registry_revision",
            "Registry revision changed since this plan or preview was captured; refresh execution preview.",
        ));
    }

    if preview.plan_digest != request.expected_plan_digest {
        return Err(AppError::new(
            "plan_digest_mismatch",
            "Compiled plan digest does not match the previewed plan snapshot; refresh execution preview.",
        ));
    }

    Ok(())
}

#[derive(Clone)]
pub(crate) struct DesktopControlState {
    pub(crate) registry: Arc<RegistryStore>,
    pub(crate) plans: Arc<PlanStore>,
    pub(crate) authority: Arc<AuthorityStore>,
    pub(crate) vault: Arc<dyn CredentialStore>,
    pub(crate) sessions_dir: PathBuf,
    pub(crate) host_executable: Option<PathBuf>,
    pub(crate) host_control: Arc<HostControlHub>,
}

impl DesktopControlState {
    pub(crate) fn open(
        registry_dir: PathBuf,
        plans_dir: PathBuf,
        authority_dir: PathBuf,
        sessions_dir: PathBuf,
        host_executable: Option<PathBuf>,
    ) -> Result<Self, String> {
        let registry = Arc::new(
            RegistryStore::open(&registry_dir)
                .map_err(|err| format!("failed to open registry store: {err}"))?,
        );
        let plans = Arc::new(
            PlanStore::open(&plans_dir)
                .map_err(|err| format!("failed to open plans store: {err}"))?,
        );
        let authority = Arc::new(
            AuthorityStore::open(&authority_dir, Arc::clone(&registry), Arc::clone(&plans))
                .map_err(|err| format!("failed to open authority store: {err}"))?,
        );
        let sessions_dir = threadlane_host::validate_sessions_root(
            &sessions_dir,
            &[
                ("registry_dir", registry.config_dir()),
                ("plans_dir", plans.dir()),
                ("authority_dir", authority.dir()),
            ],
        )
        .map_err(|err| format!("failed to initialize sessions directory: {err}"))?;
        let vault: Arc<dyn CredentialStore> = Arc::new(KeyringCredentialStore::new());
        let host_control = Arc::new(HostControlHub::new());
        Ok(Self {
            registry,
            plans,
            authority,
            vault,
            sessions_dir,
            host_executable,
            host_control,
        })
    }
}

#[tauri::command]
async fn desktop_snapshot(
    bridge: State<'_, DesktopBridge>,
    binding_id: Option<String>,
) -> Result<DesktopSnapshot, String> {
    bridge
        .snapshot(binding_id)
        .await
        .map_err(|err| err.to_string())
}

#[tauri::command]
async fn desktop_attach(
    bridge: State<'_, DesktopBridge>,
    state_dir: String,
) -> Result<DesktopSnapshot, String> {
    bridge
        .attach(state_dir)
        .await
        .map_err(|err| err.to_string())
}

#[tauri::command]
async fn desktop_run_detail(
    bridge: State<'_, DesktopBridge>,
    binding_id: String,
    run_id: String,
) -> Result<RunDetail, String> {
    bridge
        .run_detail(binding_id, run_id)
        .await
        .map_err(|err| err.to_string())
}

#[tauri::command]
async fn desktop_activity_history(
    bridge: State<'_, DesktopBridge>,
    binding_id: String,
    after_seq: Option<u64>,
    cursor: Option<String>,
    limit: Option<usize>,
    run_id: Option<String>,
    kind: Option<EventKind>,
) -> Result<ActivityPage, String> {
    bridge
        .activity_history(binding_id, after_seq, cursor, limit, run_id, kind)
        .await
        .map_err(|err| err.to_string())
}

#[tauri::command]
async fn desktop_start_demo(bridge: State<'_, DesktopBridge>) -> Result<DesktopSnapshot, String> {
    bridge.start_demo().await.map_err(|err| err.to_string())
}

#[tauri::command]
async fn desktop_answer_demo(
    bridge: State<'_, DesktopBridge>,
    expected_revision: u64,
) -> Result<DesktopSnapshot, String> {
    bridge
        .answer_demo(expected_revision)
        .await
        .map_err(|err| err.to_string())
}

#[tauri::command]
async fn desktop_cancel_demo(
    bridge: State<'_, DesktopBridge>,
    expected_revision: u64,
) -> Result<DesktopSnapshot, String> {
    bridge
        .cancel_demo(expected_revision)
        .await
        .map_err(|err| err.to_string())
}

#[tauri::command]
fn desktop_tool_status(bridge: State<'_, DesktopBridge>) -> Result<Vec<ToolStatus>, String> {
    Ok(bridge.tool_status())
}

#[tauri::command]
fn desktop_window_action(window: Window, action: WindowAction) -> Result<(), String> {
    match action {
        WindowAction::Close => window
            .close()
            .map_err(|_| "Unable to close desktop window.".to_string()),
        WindowAction::Minimize => window
            .minimize()
            .map_err(|_| "Unable to minimize desktop window.".to_string()),
    }
}

#[tauri::command]
async fn desktop_registry_snapshot(
    control: State<'_, DesktopControlState>,
) -> Result<RegistrySnapshot, AppError> {
    let registry = control.registry.clone();
    tauri::async_runtime::spawn_blocking(move || registry.snapshot())
        .await
        .map_err(|_| AppError::internal_join_error())?
        .map_err(AppError::from_registry_error)
}

#[tauri::command]
async fn desktop_registry_apply(
    control: State<'_, DesktopControlState>,
    expected_revision: u64,
    change: RegistryChange,
) -> Result<RegistrySnapshot, AppError> {
    let registry = control.registry.clone();
    tauri::async_runtime::spawn_blocking(move || registry.apply(expected_revision, change))
        .await
        .map_err(|_| AppError::internal_join_error())?
        .map_err(AppError::from_registry_error)
}

#[tauri::command]
async fn desktop_probe_tool(
    control: State<'_, DesktopControlState>,
    tool_id: String,
) -> Result<ToolProbe, AppError> {
    let trimmed_id = tool_id.trim().to_string();
    if trimmed_id.is_empty() {
        return Err(AppError::new("invalid_input", "Tool ID is required."));
    }

    let registry = control.registry.clone();
    let lookup_id = trimmed_id.clone();
    let snapshot = tauri::async_runtime::spawn_blocking(move || registry.snapshot())
        .await
        .map_err(|_| AppError::internal_join_error())?
        .map_err(AppError::from_registry_error)?;

    let tool = snapshot
        .tools
        .into_iter()
        .find(|t| t.id == lookup_id)
        .ok_or_else(|| {
            AppError::new(
                "tool_not_found",
                "Requested tool was not found in the registry.",
            )
        })?;

    probe_tool(&tool).await.map_err(|err| {
        AppError::from_connector_error(err, "probe_failed", "Tool probe failed.", None)
    })
}

#[tauri::command]
async fn desktop_secret_status(
    control: State<'_, DesktopControlState>,
    tool_id: String,
) -> Result<String, AppError> {
    let trimmed_id = tool_id.trim().to_string();
    if trimmed_id.is_empty() {
        return Err(AppError::new("invalid_input", "Tool ID is required."));
    }

    let vault = control.vault.clone();
    tauri::async_runtime::spawn_blocking(move || vault.status(&trimmed_id))
        .await
        .map_err(|_| AppError::internal_join_error())?
        .map_err(|err| {
            AppError::from_connector_error(
                err,
                "secret_status_failed",
                "Unable to check OS credential store status.",
                None,
            )
        })
}

#[tauri::command]
async fn desktop_secret_set(
    control: State<'_, DesktopControlState>,
    tool_id: String,
    secret: String,
) -> Result<(), AppError> {
    let trimmed_id = tool_id.trim().to_string();
    if trimmed_id.is_empty() {
        return Err(AppError::new("invalid_input", "Tool ID is required."));
    }
    if secret.trim().is_empty() {
        return Err(AppError::new(
            "invalid_input",
            "Credential secret cannot be empty.",
        ));
    }

    let vault = control.vault.clone();
    let secret_for_redaction = secret.clone();
    tauri::async_runtime::spawn_blocking(move || vault.set(&trimmed_id, &secret))
        .await
        .map_err(|_| AppError::internal_join_error())?
        .map_err(|err| {
            AppError::from_connector_error(
                err,
                "secret_set_failed",
                "Unable to store credential in OS keychain.",
                Some(&secret_for_redaction),
            )
        })
}

#[tauri::command]
async fn desktop_secret_delete(
    control: State<'_, DesktopControlState>,
    tool_id: String,
) -> Result<(), AppError> {
    let trimmed_id = tool_id.trim().to_string();
    if trimmed_id.is_empty() {
        return Err(AppError::new("invalid_input", "Tool ID is required."));
    }

    let vault = control.vault.clone();
    tauri::async_runtime::spawn_blocking(move || vault.delete(&trimmed_id))
        .await
        .map_err(|_| AppError::internal_join_error())?
        .map_err(|err| {
            AppError::from_connector_error(
                err,
                "secret_delete_failed",
                "Unable to delete credential from OS keychain.",
                None,
            )
        })
}

#[tauri::command]
async fn desktop_decision_models(
    control: State<'_, DesktopControlState>,
    tool_id: String,
    provider: DecisionProvider,
) -> Result<Vec<ModelDefinition>, AppError> {
    let trimmed_id = tool_id.trim().to_string();
    if trimmed_id.is_empty() {
        return Err(AppError::new("invalid_input", "Tool ID is required."));
    }

    let client = DecisionClient::new(control.vault.clone())
        .map_err(|err| AppError::from_decision_error(err, "decision_client_error"))?;
    client
        .models(&trimmed_id, provider)
        .await
        .map_err(|err| AppError::from_decision_error(err, "decision_models_failed"))
}

#[tauri::command]
async fn desktop_test_decision(
    control: State<'_, DesktopControlState>,
    tool_id: String,
    provider: DecisionProvider,
    model: String,
) -> Result<DecisionAdvice, AppError> {
    let trimmed_id = tool_id.trim().to_string();
    let trimmed_model = model.trim().to_string();
    if trimmed_id.is_empty() || trimmed_model.is_empty() {
        return Err(AppError::new(
            "invalid_input",
            "Both tool ID and decision model are required.",
        ));
    }

    let client = DecisionClient::new(control.vault.clone())
        .map_err(|err| AppError::from_decision_error(err, "decision_client_error"))?;
    let request = DecisionRequest {
        tool_id: trimmed_id,
        provider,
        model: trimmed_model,
        task_summary: SYNTHETIC_DECISION_TASK_SUMMARY.to_string(),
    };
    client
        .classify(request)
        .await
        .map_err(|err| AppError::from_decision_error(err, "decision_classify_failed"))
}

#[tauri::command]
async fn desktop_plans_snapshot(
    control: State<'_, DesktopControlState>,
) -> Result<Vec<PlanRevision>, AppError> {
    let plans = control.plans.clone();
    tauri::async_runtime::spawn_blocking(move || plans.snapshot())
        .await
        .map_err(|_| AppError::internal_join_error())?
        .map_err(AppError::from_plan_error)
}

#[tauri::command]
async fn desktop_plan_change(
    control: State<'_, DesktopControlState>,
    expected_revision: u64,
    change: PlanChange,
) -> Result<PlanRevision, AppError> {
    let plans = control.plans.clone();
    tauri::async_runtime::spawn_blocking(move || plans.change(expected_revision, change))
        .await
        .map_err(|_| AppError::internal_join_error())?
        .map_err(AppError::from_plan_error)
}

#[tauri::command]
async fn desktop_plan_execution_preview(
    control: State<'_, DesktopControlState>,
    plan_id: String,
    revision: u64,
) -> Result<PlanExecutionPreview, AppError> {
    let plans = control.plans.clone();
    let registry = control.registry.clone();
    tauri::async_runtime::spawn_blocking(move || {
        build_plan_execution_preview(&plans, &registry, &plan_id, revision)
    })
    .await
    .map_err(|_| AppError::internal_join_error())?
}

#[tauri::command]
async fn desktop_approve_plan(
    bridge: State<'_, DesktopBridge>,
    control: State<'_, DesktopControlState>,
    request: OperatorApprovalRequest,
) -> Result<ApprovalSummary, AppError> {
    let initial_snapshot = bridge.snapshot(None).await.map_err(|_| {
        AppError::new(
            "bridge_unavailable",
            "Unable to inspect active runtime attachment snapshot.",
        )
    })?;
    let captured_ctx = verify_snapshot_binding_for_approval(
        &initial_snapshot,
        &request.binding_id,
        &request.thread_uuid,
    )?;

    let plans = control.plans.clone();
    let registry = control.registry.clone();
    let precheck_request = request.clone();
    tauri::async_runtime::spawn_blocking(move || {
        precheck_approval_against_stores(&plans, &registry, &precheck_request)
    })
    .await
    .map_err(|_| AppError::internal_join_error())??;

    let rechecked_snapshot = bridge.snapshot(None).await.map_err(|_| {
        AppError::new(
            "bridge_unavailable",
            "Unable to re-verify active runtime attachment snapshot before recording approval.",
        )
    })?;
    verify_snapshot_recheck(&captured_ctx, &rechecked_snapshot)?;

    let authority = control.authority.clone();
    tauri::async_runtime::spawn_blocking(move || authority.approve(request))
        .await
        .map_err(|_| AppError::internal_join_error())?
        .map_err(AppError::from_authority_error)
}

#[tauri::command]
async fn desktop_plan_approvals(
    control: State<'_, DesktopControlState>,
    binding_id: String,
) -> Result<Vec<ApprovalSummary>, AppError> {
    let trimmed = binding_id.trim().to_string();
    if trimmed.is_empty() {
        return Err(AppError::new(
            "invalid_input",
            "Binding identifier is required to list plan approvals.",
        ));
    }

    let authority = control.authority.clone();
    tauri::async_runtime::spawn_blocking(move || authority.list_for_binding(&trimmed))
        .await
        .map_err(|_| AppError::internal_join_error())?
        .map_err(AppError::from_authority_error)
}

#[tauri::command]
async fn desktop_start_approved_session(
    bridge: State<'_, DesktopBridge>,
    control: State<'_, DesktopControlState>,
    approval_id: String,
    notify_codex_tool_id: Option<String>,
) -> Result<HostSessionSummary, AppError> {
    match notify_codex_tool_id {
        Some(notify_codex_tool_id) => {
            host_control::start_approved_session_with_notifications(
                &bridge,
                &control,
                approval_id,
                Some(notify_codex_tool_id),
            )
            .await
        }
        None => host_control::start_approved_session(&bridge, &control, approval_id).await,
    }
}

#[tauri::command]
async fn desktop_stop_approved_session(
    bridge: State<'_, DesktopBridge>,
    control: State<'_, DesktopControlState>,
    approval_id: String,
    expected_instance_id: String,
) -> Result<HostSessionSummary, AppError> {
    host_control::stop_approved_session(&bridge, &control, approval_id, expected_instance_id).await
}

#[tauri::command]
async fn desktop_restart_approved_session(
    bridge: State<'_, DesktopBridge>,
    control: State<'_, DesktopControlState>,
    approval_id: String,
    expected_prior_instance_id: String,
    notify_codex_tool_id: Option<String>,
) -> Result<HostSessionSummary, AppError> {
    match notify_codex_tool_id {
        Some(notify_codex_tool_id) => {
            host_control::restart_approved_session_with_notifications(
                &bridge,
                &control,
                approval_id,
                expected_prior_instance_id,
                Some(notify_codex_tool_id),
            )
            .await
        }
        None => {
            host_control::restart_approved_session(
                &bridge,
                &control,
                approval_id,
                expected_prior_instance_id,
            )
            .await
        }
    }
}

#[tauri::command]
async fn desktop_approved_session(
    bridge: State<'_, DesktopBridge>,
    control: State<'_, DesktopControlState>,
    approval_id: String,
) -> Result<HostSessionSummary, AppError> {
    host_control::approved_session(&bridge, &control, approval_id).await
}

#[tauri::command]
async fn desktop_approved_task(
    bridge: State<'_, DesktopBridge>,
    control: State<'_, DesktopControlState>,
    approval_id: String,
    task_id: String,
) -> Result<TaskStateSnapshot, AppError> {
    host_control::approved_task(&bridge, &control, approval_id, task_id).await
}

#[tauri::command]
async fn desktop_approved_activity(
    bridge: State<'_, DesktopBridge>,
    control: State<'_, DesktopControlState>,
    approval_id: String,
    after_seq: Option<u64>,
    cursor: Option<String>,
    limit: Option<usize>,
) -> Result<ActivityPage, AppError> {
    host_control::approved_activity(&bridge, &control, approval_id, after_seq, cursor, limit).await
}

#[tauri::command]
async fn desktop_native_threads(
    control: State<'_, DesktopControlState>,
    tool_id: String,
    project_id: Option<String>,
    cursor: Option<String>,
) -> Result<NativeThreadPage, AppError> {
    let trimmed_id = tool_id.trim().to_string();
    if trimmed_id.is_empty() {
        return Err(AppError::new(
            "not_configured",
            "Select a configured Codex CLI tool to query native threads.",
        ));
    }

    let registry = control.registry.clone();
    let lookup_id = trimmed_id.clone();
    let snapshot = tauri::async_runtime::spawn_blocking(move || registry.snapshot())
        .await
        .map_err(|_| AppError::internal_join_error())?
        .map_err(AppError::from_registry_error)?;

    let tool = snapshot
        .tools
        .into_iter()
        .find(|t| t.id == lookup_id)
        .ok_or_else(|| {
            AppError::new(
                "not_configured",
                "Selected Codex tool is not registered in the tool registry.",
            )
        })?;

    if tool.adapter != ToolAdapter::CodexCli {
        return Err(AppError::new(
            "invalid_adapter",
            "Selected tool does not use the codex_cli adapter.",
        ));
    }

    if !tool.enabled {
        return Err(AppError::new(
            "tool_disabled",
            "Selected Codex CLI tool is disabled in the registry.",
        ));
    }

    let executable = match tool.executable_path {
        Some(path) if !path.as_os_str().is_empty() => path,
        _ => resolve_executable_on_path("codex").ok_or_else(|| {
            AppError::new(
                "not_configured",
                "Codex CLI executable path is not configured and was not found on PATH.",
            )
        })?,
    };

    let client = NativeClient::new(NativeConfig {
        executable,
        socket: None,
    })
    .map_err(AppError::from_native_error)?;

    let normalized_project = project_id
        .map(|p| p.trim().to_string())
        .filter(|p| !p.is_empty());
    let normalized_cursor = cursor
        .map(|c| c.trim().to_string())
        .filter(|c| !c.is_empty());

    client
        .threads(normalized_project, normalized_cursor)
        .await
        .map_err(AppError::from_native_error)
}

fn main() -> ExitCode {
    let cli = DesktopCli::parse();
    let bridge = match DesktopBridge::new(cli.state_dir, cli.demo_state_dir) {
        Ok(bridge) => bridge,
        Err(err) => {
            eprintln!("threadlane-desktop error: {err}");
            return ExitCode::FAILURE;
        }
    };

    let cli_registry_dir = cli.registry_dir;
    let cli_host_executable = cli.host_executable;

    let result = tauri::Builder::default()
        .manage(bridge)
        .setup(move |app| {
            let registry_dir = match cli_registry_dir {
                Some(dir) => dir,
                None => app
                    .path()
                    .app_config_dir()
                    .map_err(|_| "Failed to resolve application config directory.")?
                    .join("registry"),
            };
            let app_data_dir = app
                .path()
                .app_data_dir()
                .map_err(|_| "Failed to resolve application data directory.")?;
            let plans_dir = app_data_dir.join("plans");
            let authority_dir = app_data_dir.join("authority");
            let sessions_dir = app_data_dir.join("sessions");

            let control = DesktopControlState::open(
                registry_dir,
                plans_dir,
                authority_dir,
                sessions_dir,
                cli_host_executable,
            )?;
            app.manage(control);
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            desktop_snapshot,
            desktop_attach,
            desktop_run_detail,
            desktop_activity_history,
            desktop_start_demo,
            desktop_answer_demo,
            desktop_cancel_demo,
            desktop_tool_status,
            desktop_window_action,
            desktop_registry_snapshot,
            desktop_registry_apply,
            desktop_probe_tool,
            desktop_secret_status,
            desktop_secret_set,
            desktop_secret_delete,
            desktop_decision_models,
            desktop_test_decision,
            desktop_plans_snapshot,
            desktop_plan_change,
            desktop_plan_execution_preview,
            desktop_approve_plan,
            desktop_plan_approvals,
            desktop_start_approved_session,
            desktop_stop_approved_session,
            desktop_restart_approved_session,
            desktop_approved_session,
            desktop_approved_task,
            desktop_approved_activity,
            desktop_native_threads,
        ])
        .run(tauri::generate_context!());

    if result.is_err() {
        eprintln!("threadlane-desktop error: failed to run desktop application.");
        return ExitCode::FAILURE;
    }

    ExitCode::SUCCESS
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::sync::atomic::{AtomicU64, Ordering};
    use threadlane_desktop_bridge::{Attachment, BindingSummary, DemoControls};
    use threadlane_plans::PlanTask;
    use threadlane_registry::{ModelChoice, RoleAssignment, ToolDefinition, WorkflowProfile};

    static TEST_COUNTER: AtomicU64 = AtomicU64::new(1);

    struct ScopedTestDir {
        path: PathBuf,
    }

    impl ScopedTestDir {
        fn new(label: &str) -> Self {
            let seq = TEST_COUNTER.fetch_add(1, Ordering::Relaxed);
            let raw_base = fs::canonicalize(env::temp_dir()).unwrap_or_else(|_| env::temp_dir());
            let path = raw_base.join(format!(
                "threadlane-desktop-m4-{label}-{}-{seq}",
                std::process::id()
            ));
            if path.exists() {
                let _ = fs::remove_dir_all(&path);
            }
            fs::create_dir_all(&path).expect("create test temp dir");
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                fs::set_permissions(&path, fs::Permissions::from_mode(0o700))
                    .expect("chmod 0700 test temp dir");
            }
            Self { path }
        }
    }

    impl Drop for ScopedTestDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    fn sample_runtime_snapshot(
        mode: AttachmentMode,
        connection: ConnectionState,
        state_dir: Option<&str>,
        selected_binding_id: Option<&str>,
        bindings: Vec<BindingSummary>,
    ) -> DesktopSnapshot {
        DesktopSnapshot {
            attachment: Attachment {
                mode,
                connection,
                state_dir: state_dir.map(str::to_string),
                message: None,
                updated_at_ms: 1_700_000_000_000,
            },
            bindings,
            selected_binding_id: selected_binding_id.map(str::to_string),
            tasks: Vec::new(),
            total_runs: 0,
            active_runs: 0,
            has_more: false,
            demo_controls: DemoControls::default(),
        }
    }

    #[test]
    fn sanitizes_sensitive_tokens_and_echoed_secrets() {
        let fallback = "Unable to store credential in OS keychain.";
        assert_eq!(
            sanitize_error_message(
                "Keychain failed for value super-secret-key-123",
                fallback,
                Some("super-secret-key-123")
            ),
            fallback
        );
        assert_eq!(
            sanitize_error_message("Authorization: Bearer abc.def.ghi", fallback, None),
            fallback
        );
        assert_eq!(
            sanitize_error_message("rejected key sk-live-999999", fallback, None),
            fallback
        );
        assert_eq!(
            sanitize_error_message(
                "Keychain is locked by user.",
                fallback,
                Some("other-secret")
            ),
            "Keychain is locked by user."
        );
    }

    #[test]
    fn classifies_domain_error_codes_deterministically() {
        assert_eq!(
            classify_error_code("stale revision: expected 2, got 3", "plan_error"),
            "revision_conflict"
        );
        assert_eq!(
            classify_error_code("published revision 4 is immutable", "plan_error"),
            "immutable_revision"
        );
        assert_eq!(
            classify_error_code("task dependency cycle detected in DAG", "plan_error"),
            "invalid_dag"
        );
        assert_eq!(
            classify_error_code("OS keyring is locked", "secret_error"),
            "vault_locked"
        );
        assert_eq!(
            classify_error_code("upstream returned 429 rate limit", "decision_error"),
            "rate_limited"
        );
    }

    #[test]
    fn execution_preview_compiles_published_plan_and_strips_executable_paths() {
        let tmp = ScopedTestDir::new("preview");
        let registry = RegistryStore::open(tmp.path.join("registry")).expect("open registry");
        let plans = PlanStore::open(tmp.path.join("plans")).expect("open plans");

        registry
            .apply(
                0,
                RegistryChange::UpsertTool {
                    tool: ToolDefinition {
                        id: "tool-agy".to_string(),
                        label: "Antigravity CLI".to_string(),
                        adapter: ToolAdapter::Antigravity,
                        executable_path: Some(PathBuf::from("/private/internal/bin/agy-secret")),
                        enabled: true,
                    },
                },
            )
            .expect("upsert tool");
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
            .expect("upsert model");
        let reg_snap = registry
            .apply(
                2,
                RegistryChange::UpsertProfile {
                    profile: WorkflowProfile {
                        decision_advisor: None,
                        id: "profile-main".to_string(),
                        name: "Main M4 Profile".to_string(),
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
            .expect("upsert profile");

        let sample_task = PlanTask {
            id: "task-1".to_string(),
            title: "Implement approval slice".to_string(),
            instruction: "Build operator approval controls without auto-starting workers."
                .to_string(),
            role: WorkflowRole::Coder,
            difficulty: Difficulty::Hard,
            depends_on: Vec::new(),
            allowed_write_paths: vec!["apps/desktop/src/approval/".to_string()],
        };

        let draft = plans
            .change(
                0,
                PlanChange::SaveDraft {
                    plan_id: "plan-m4".to_string(),
                    binding_id: "bnd_live_1".to_string(),
                    profile_id: "profile-main".to_string(),
                    profile_revision: reg_snap.revision,
                    summary: "M4 approval slice".to_string(),
                    tasks: vec![sample_task.clone()],
                },
            )
            .expect("save draft");

        // Draft plans must be rejected by execution preview.
        let draft_err = build_plan_execution_preview(&plans, &registry, "plan-m4", draft.revision)
            .expect_err("draft plan must not preview");
        assert_eq!(draft_err.code, "unpublished_or_superseded_plan");

        // Publish revision 1 and verify safe execution preview.
        plans
            .change(
                draft.revision,
                PlanChange::Publish {
                    plan_id: "plan-m4".to_string(),
                    revision: draft.revision,
                },
            )
            .expect("publish r1");

        let preview = build_plan_execution_preview(&plans, &registry, "plan-m4", 1)
            .expect("preview published r1");
        assert_eq!(preview.plan_id, "plan-m4");
        assert_eq!(preview.plan_revision, 1);
        assert_eq!(preview.binding_id, "bnd_live_1");
        assert_eq!(preview.profile_id, "profile-main");
        assert_eq!(preview.registry_revision, reg_snap.revision);
        assert_eq!(preview.plan_digest.len(), 64);
        assert_eq!(preview.tasks.len(), 1);
        assert_eq!(preview.tasks[0].id, "task-1");
        assert_eq!(preview.tasks[0].model_id, "model-argon");
        assert_eq!(preview.tasks[0].model_arg, "gemini-4-argon-high");
        assert_eq!(preview.tasks[0].effort.as_deref(), Some("high"));

        let serialized = serde_json::to_string(&preview).expect("serialize preview DTO");
        assert!(!serialized.contains("executable_path"));
        assert!(!serialized.contains("/private/internal/bin/agy-secret"));

        // Save and publish revision 2 -> revision 1 becomes superseded and must be rejected.
        let draft2 = plans
            .change(
                1,
                PlanChange::SaveDraft {
                    plan_id: "plan-m4".to_string(),
                    binding_id: "bnd_live_1".to_string(),
                    profile_id: "profile-main".to_string(),
                    profile_revision: reg_snap.revision,
                    summary: "M4 approval slice v2".to_string(),
                    tasks: vec![sample_task],
                },
            )
            .expect("save draft 2");
        plans
            .change(
                draft2.revision,
                PlanChange::Publish {
                    plan_id: "plan-m4".to_string(),
                    revision: draft2.revision,
                },
            )
            .expect("publish r2");

        let superseded_err = build_plan_execution_preview(&plans, &registry, "plan-m4", 1)
            .expect_err("superseded revision must be rejected");
        assert_eq!(superseded_err.code, "unpublished_or_superseded_plan");

        // Advance registry revision -> preview of r2 must fail with stale_registry_revision.
        registry
            .apply(
                reg_snap.revision,
                RegistryChange::SetDefaultProfile {
                    profile_id: Some("profile-main".to_string()),
                },
            )
            .expect("advance registry");
        let stale_err = build_plan_execution_preview(&plans, &registry, "plan-m4", 2)
            .expect_err("mismatched registry revision must fail");
        assert_eq!(stale_err.code, "stale_registry_revision");
    }

    #[test]
    fn snapshot_binding_verification_rejects_invalid_attachments_and_rechecks_across_await() {
        let valid_uuid = "550e8400-e29b-41d4-a716-446655440000";
        let other_uuid = "6ba7b810-9dad-11d1-80b4-00c04fd430c8";

        let live_binding = BindingSummary {
            binding_id: "bnd_live_1".to_string(),
            thread_uuid: valid_uuid.to_string(),
            label: "Primary Chat".to_string(),
            is_local_demo: false,
        };
        let demo_binding = BindingSummary {
            binding_id: "bnd_demo_1".to_string(),
            thread_uuid: valid_uuid.to_string(),
            label: "Local demo".to_string(),
            is_local_demo: true,
        };

        // 1. Unconfigured attachment rejected.
        let unconfigured = sample_runtime_snapshot(
            AttachmentMode::Unconfigured,
            ConnectionState::Unconfigured,
            None,
            None,
            vec![live_binding.clone()],
        );
        assert_eq!(
            verify_snapshot_binding_for_approval(&unconfigured, "bnd_live_1", valid_uuid)
                .unwrap_err()
                .code,
            "runtime_not_attached"
        );

        // 2. Demo attachment mode rejected.
        let demo_mode = sample_runtime_snapshot(
            AttachmentMode::Demo,
            ConnectionState::Connected,
            Some("/tmp/demo-state"),
            Some("bnd_live_1"),
            vec![live_binding.clone()],
        );
        assert_eq!(
            verify_snapshot_binding_for_approval(&demo_mode, "bnd_live_1", valid_uuid)
                .unwrap_err()
                .code,
            "demo_binding_forbidden"
        );

        // 3. Disconnected runtime rejected.
        let offline = sample_runtime_snapshot(
            AttachmentMode::Runtime,
            ConnectionState::Disconnected,
            Some("/tmp/runtime-state"),
            Some("bnd_live_1"),
            vec![live_binding.clone()],
        );
        assert_eq!(
            verify_snapshot_binding_for_approval(&offline, "bnd_live_1", valid_uuid)
                .unwrap_err()
                .code,
            "runtime_offline"
        );

        // 4. Connected runtime with local demo binding rejected.
        let connected = sample_runtime_snapshot(
            AttachmentMode::Runtime,
            ConnectionState::Connected,
            Some("/tmp/runtime-state"),
            Some("bnd_live_1"),
            vec![live_binding.clone(), demo_binding],
        );
        assert_eq!(
            verify_snapshot_binding_for_approval(&connected, "bnd_demo_1", valid_uuid)
                .unwrap_err()
                .code,
            "demo_binding_forbidden"
        );

        // 5. Missing binding rejected.
        assert_eq!(
            verify_snapshot_binding_for_approval(&connected, "bnd_missing", valid_uuid)
                .unwrap_err()
                .code,
            "binding_not_found"
        );

        // 6. Cross-binding / mismatched thread UUID rejected.
        assert_eq!(
            verify_snapshot_binding_for_approval(&connected, "bnd_live_1", other_uuid)
                .unwrap_err()
                .code,
            "thread_uuid_mismatch"
        );

        // 7. Nil or non-canonical UUID rejected.
        assert_eq!(
            verify_snapshot_binding_for_approval(
                &connected,
                "bnd_live_1",
                "00000000-0000-0000-0000-000000000000"
            )
            .unwrap_err()
            .code,
            "invalid_thread_uuid"
        );

        // 8. Valid connected runtime binding succeeds and rechecks cleanly.
        let captured = verify_snapshot_binding_for_approval(&connected, "bnd_live_1", valid_uuid)
            .expect("valid runtime binding must verify");
        assert!(verify_snapshot_recheck(&captured, &connected).is_ok());

        // 9. Selection or state_dir changing across await is rejected by recheck.
        let selection_changed = sample_runtime_snapshot(
            AttachmentMode::Runtime,
            ConnectionState::Connected,
            Some("/tmp/runtime-state"),
            Some("bnd_other_selected"),
            vec![live_binding.clone()],
        );
        assert_eq!(
            verify_snapshot_recheck(&captured, &selection_changed)
                .unwrap_err()
                .code,
            "snapshot_changed_during_approval"
        );

        let dir_changed = sample_runtime_snapshot(
            AttachmentMode::Runtime,
            ConnectionState::Connected,
            Some("/tmp/other-runtime-state"),
            Some("bnd_live_1"),
            vec![live_binding],
        );
        assert_eq!(
            verify_snapshot_recheck(&captured, &dir_changed)
                .unwrap_err()
                .code,
            "snapshot_changed_during_approval"
        );
    }

    #[test]
    fn authority_error_mapping_is_static_and_redacts_raw_filesystem_paths() {
        let repo_err = AppError::from_authority_error(AuthorityError::InvalidRepository {
            path: "/home/secret-user/private-repo".to_string(),
            reason: "missing .git in /home/secret-user/private-repo".to_string(),
        });
        assert_eq!(repo_err.code, "invalid_repository");
        assert!(!repo_err.message.contains("/home/secret-user"));

        let exe_err = AppError::from_authority_error(AuthorityError::InvalidExecutable {
            tool_id: "tool-agy".to_string(),
            task_id: "task-1".to_string(),
            reason: "failed to canonicalize /nix/store/secret-hash-agy/bin/agy".to_string(),
        });
        assert_eq!(exe_err.code, "invalid_executable");
        assert!(!exe_err.message.contains("/nix/store"));

        let sec_err = AppError::from_authority_error(AuthorityError::SecurityViolation {
            path: "/etc/shadow".to_string(),
            reason: "symlink detected at /etc/shadow".to_string(),
        });
        assert_eq!(sec_err.code, "security_violation");
        assert!(!sec_err.message.contains("/etc/shadow"));
    }
}
