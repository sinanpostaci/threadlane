#![forbid(unsafe_code)]
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use std::env;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::Arc;

use clap::Parser;
use serde::{Deserialize, Serialize};
use tauri::{Manager, State, Window};
use threadlane_connectors::{
    probe_tool, ConnectorError, CredentialStore, KeyringCredentialStore, ToolProbe,
};
use threadlane_decisions::{
    DecisionAdvice, DecisionClient, DecisionError, DecisionProvider, DecisionRequest,
};
use threadlane_desktop_bridge::{
    DesktopBridge, DesktopSnapshot, RunDetail, ToolStatus, WindowAction,
};
use threadlane_native::{NativeClient, NativeConfig, NativeError, NativeThreadPage};
use threadlane_plans::{PlanChange, PlanError, PlanRevision, PlanStore};
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
}

/// Safe structured error returned to the desktop frontend over Tauri IPC.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct AppError {
    code: String,
    message: String,
}

impl AppError {
    fn new(code: &'static str, message: impl Into<String>) -> Self {
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

    fn from_native_error(err: NativeError) -> Self {
        let raw = err.to_string();
        let code = classify_error_code(&raw, "native_error");
        Self {
            code: code.to_string(),
            message: sanitize_error_message(&raw, "Native Codex metadata request failed.", None),
        }
    }

    fn internal_join_error() -> Self {
        Self {
            code: "internal_task_error".to_string(),
            message: "Background desktop worker task failed.".to_string(),
        }
    }
}

fn classify_error_code<'a>(raw: &str, default_code: &'a str) -> &'a str {
    let lower = raw.to_ascii_lowercase();
    if lower.contains("revision") || lower.contains("stale") || lower.contains("conflict") {
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
    } else if lower.contains("immutable") || lower.contains("already published") {
        "immutable_revision"
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

#[derive(Clone)]
struct DesktopControlState {
    registry: Arc<RegistryStore>,
    plans: Arc<PlanStore>,
    vault: Arc<dyn CredentialStore>,
}

impl DesktopControlState {
    fn open(registry_dir: PathBuf, plans_dir: PathBuf) -> Result<Self, String> {
        let registry = RegistryStore::open(&registry_dir)
            .map_err(|err| format!("failed to open registry store: {err}"))?;
        let plans = PlanStore::open(&plans_dir)
            .map_err(|err| format!("failed to open plans store: {err}"))?;
        let vault: Arc<dyn CredentialStore> = Arc::new(KeyringCredentialStore::new());
        Ok(Self {
            registry: Arc::new(registry),
            plans: Arc::new(plans),
            vault,
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
            let plans_dir = app
                .path()
                .app_data_dir()
                .map_err(|_| "Failed to resolve application data directory.")?
                .join("plans");

            let control = DesktopControlState::open(registry_dir, plans_dir)?;
            app.manage(control);
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            desktop_snapshot,
            desktop_attach,
            desktop_run_detail,
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
}
