use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use threadlane_native::codex_setup::{
    resolve_codex_home, resolve_default_runtime_state_dir, resolve_native_mcp_executable,
    run_codex_mcp_setup, CodexSetupMode, CodexSetupReport, CodexSetupRequest,
    CODEX_CONFIG_FILENAME,
};

use crate::AppError;

/// Subdirectory under `app_data_dir` used for the private native MCP pairing store.
pub(crate) const DESKTOP_PAIRING_SUBDIR: &str = "pairing";

/// Subdirectory under `app_data_dir` used for the private Codex setup management lock and receipt.
pub(crate) const DESKTOP_CODEX_SETUP_SUBDIR: &str = "codex-setup";

/// Configuration for the desktop startup Codex MCP registration service.
#[derive(Debug, Clone)]
pub(crate) struct CodexSetupServiceConfig {
    pub(crate) supplied_state_dir: Option<PathBuf>,
    pub(crate) registry_dir: PathBuf,
    pub(crate) plans_dir: PathBuf,
    pub(crate) authority_dir: PathBuf,
    pub(crate) sessions_dir: PathBuf,
    pub(crate) pairing_dir: PathBuf,
    pub(crate) management_dir: PathBuf,
    #[cfg(test)]
    pub(crate) codex_home_override: Option<PathBuf>,
    #[cfg(test)]
    pub(crate) mcp_executable_override: Option<PathBuf>,
}

impl CodexSetupServiceConfig {
    pub(crate) fn from_desktop_dirs(
        supplied_state_dir: Option<PathBuf>,
        registry_dir: PathBuf,
        plans_dir: PathBuf,
        authority_dir: PathBuf,
        sessions_dir: PathBuf,
        app_data_dir: &std::path::Path,
    ) -> Self {
        Self {
            supplied_state_dir,
            registry_dir,
            plans_dir,
            authority_dir,
            sessions_dir,
            pairing_dir: app_data_dir.join(DESKTOP_PAIRING_SUBDIR),
            management_dir: app_data_dir.join(DESKTOP_CODEX_SETUP_SUBDIR),
            #[cfg(test)]
            codex_home_override: None,
            #[cfg(test)]
            mcp_executable_override: None,
        }
    }
}

/// Managed desktop service that runs Codex MCP setup once in a background task at startup
/// and supports explicit operator recheck (`Check`) and retry (`Apply`) commands.
#[derive(Clone)]
pub(crate) struct CodexSetupService {
    config: CodexSetupServiceConfig,
    latest_report: Arc<Mutex<CodexSetupReport>>,
    run_gate: Arc<tokio::sync::Mutex<()>>,
}

impl CodexSetupService {
    pub(crate) fn new(config: CodexSetupServiceConfig) -> Self {
        let initial_config_path = initial_config_path_hint(&config);
        Self {
            config,
            latest_report: Arc::new(Mutex::new(CodexSetupReport::checking(initial_config_path))),
            run_gate: Arc::new(tokio::sync::Mutex::new(())),
        }
    }

    /// Spawns the one-time startup `Apply` task in the background without blocking UI initialization.
    pub(crate) fn spawn_startup_apply(&self) {
        let guard = self.run_gate.clone().try_lock_owned().ok();
        let config = self.config.clone();
        let latest_report = Arc::clone(&self.latest_report);

        tauri::async_runtime::spawn(async move {
            let _held_guard = guard;
            let report = tauri::async_runtime::spawn_blocking(move || {
                execute_desktop_codex_setup(&config, CodexSetupMode::Apply)
            })
            .await
            .unwrap_or_else(|_| {
                CodexSetupReport::error(
                    CodexSetupMode::Apply,
                    None,
                    "internal_join_error",
                    "Background Codex MCP setup task terminated unexpectedly.",
                )
            });

            if let Ok(mut slot) = latest_report.lock() {
                *slot = report;
            }
        });
    }

    #[cfg(test)]
    pub(crate) fn cached_report_snapshot(&self) -> CodexSetupReport {
        self.latest_report
            .lock()
            .expect("codex setup report mutex")
            .clone()
    }

    /// Returns the latest Codex setup report, awaiting the initial startup background task if still in flight.
    pub(crate) async fn status(&self) -> Result<CodexSetupReport, AppError> {
        let _guard = self.run_gate.lock().await;
        let report = self
            .latest_report
            .lock()
            .map_err(|_| AppError::new("internal_lock_error", "Codex setup state lock failed."))?
            .clone();
        Ok(report)
    }

    /// Runs an explicit read-only recheck (`CodexSetupMode::Check`) and updates the cached report.
    pub(crate) async fn recheck(&self) -> Result<CodexSetupReport, AppError> {
        self.run_mode(CodexSetupMode::Check).await
    }

    /// Runs an explicit setup retry (`CodexSetupMode::Apply`) and updates the cached report.
    pub(crate) async fn apply(&self) -> Result<CodexSetupReport, AppError> {
        self.run_mode(CodexSetupMode::Apply).await
    }

    async fn run_mode(&self, mode: CodexSetupMode) -> Result<CodexSetupReport, AppError> {
        let _guard = self.run_gate.lock().await;
        let config = self.config.clone();
        let report = tauri::async_runtime::spawn_blocking(move || {
            execute_desktop_codex_setup(&config, mode)
        })
        .await
        .map_err(|_| AppError::internal_join_error())?;

        if let Ok(mut slot) = self.latest_report.lock() {
            *slot = report.clone();
        }
        Ok(report)
    }
}

fn initial_config_path_hint(config: &CodexSetupServiceConfig) -> Option<String> {
    #[cfg(test)]
    if let Some(ref home) = config.codex_home_override {
        return home
            .join(CODEX_CONFIG_FILENAME)
            .to_str()
            .map(str::to_string);
    }
    let _ = config;
    resolve_codex_home().ok().and_then(|home| {
        home.join(CODEX_CONFIG_FILENAME)
            .to_str()
            .map(str::to_string)
    })
}

pub(crate) fn execute_desktop_codex_setup(
    config: &CodexSetupServiceConfig,
    mode: CodexSetupMode,
) -> CodexSetupReport {
    let codex_home_res = {
        #[cfg(test)]
        {
            if let Some(ref custom) = config.codex_home_override {
                Ok(custom.clone())
            } else {
                resolve_codex_home()
            }
        }
        #[cfg(not(test))]
        {
            resolve_codex_home()
        }
    };

    let codex_home = match codex_home_res {
        Ok(dir) => dir,
        Err(_) => {
            return CodexSetupReport::error(
                mode,
                None,
                "codex_home_unresolved",
                "Unable to resolve Codex home directory from CODEX_HOME or HOME.",
            );
        }
    };

    let config_path_display = codex_home
        .join(CODEX_CONFIG_FILENAME)
        .to_str()
        .map(str::to_string);

    let state_dir = match resolve_default_runtime_state_dir(config.supplied_state_dir.as_deref()) {
        Ok(dir) => dir,
        Err(_) => {
            return CodexSetupReport::error(
                mode,
                config_path_display,
                "state_dir_unresolved",
                "Unable to resolve Threadlane runtime state directory.",
            );
        }
    };

    let mcp_executable_res = {
        #[cfg(test)]
        {
            if let Some(ref custom) = config.mcp_executable_override {
                Ok(custom.clone())
            } else {
                resolve_native_mcp_executable()
            }
        }
        #[cfg(not(test))]
        {
            resolve_native_mcp_executable()
        }
    };

    let mcp_executable = match mcp_executable_res {
        Ok(exe) => exe,
        Err(_) => {
            return CodexSetupReport::error(
                mode,
                config_path_display,
                "invalid_executable",
                "Unable to resolve a valid threadlane-native-mcp executable on PATH or next to the desktop binary.",
            );
        }
    };

    let request = CodexSetupRequest {
        codex_home,
        mcp_executable,
        state_dir,
        pairing_dir: config.pairing_dir.clone(),
        registry_dir: config.registry_dir.clone(),
        plans_dir: config.plans_dir.clone(),
        authority_dir: config.authority_dir.clone(),
        sessions_dir: config.sessions_dir.clone(),
        management_dir: config.management_dir.clone(),
    };

    run_codex_mcp_setup(&request, mode)
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::os::unix::fs::PermissionsExt;

    use tempfile::TempDir;
    use threadlane_native::codex_setup::{CodexSetupState, DEFAULT_NATIVE_MCP_EXECUTABLE_NAME};

    use super::*;

    #[test]
    fn desktop_service_starts_in_checking_and_applies_using_desktop_store_paths() {
        let tmp = TempDir::new().expect("tempdir");
        let root = tmp.path();
        let bin_dir = root.join("bin");
        fs::create_dir_all(&bin_dir).unwrap();
        let mcp_exe = bin_dir.join(DEFAULT_NATIVE_MCP_EXECUTABLE_NAME);
        fs::write(&mcp_exe, b"#!/bin/sh\nexit 0\n").unwrap();
        fs::set_permissions(&mcp_exe, fs::Permissions::from_mode(0o755)).unwrap();

        let app_data = root.join("app-data");
        let mut config = CodexSetupServiceConfig::from_desktop_dirs(
            Some(root.join("runtime-state")),
            root.join("registry"),
            app_data.join("plans"),
            app_data.join("authority"),
            app_data.join("sessions"),
            &app_data,
        );
        config.codex_home_override = Some(root.join("codex-home"));
        config.mcp_executable_override = Some(mcp_exe);

        let service = CodexSetupService::new(config.clone());
        let initial = service.cached_report_snapshot();
        assert_eq!(initial.state, CodexSetupState::Checking);
        assert!(initial.pairing_required);

        let applied = execute_desktop_codex_setup(&config, CodexSetupMode::Apply);
        assert_eq!(applied.state, CodexSetupState::Registered);
        assert!(applied.registered);
        assert!(applied.config_modified);
        assert!(applied.restart_required);
        assert!(applied.pairing_required);

        let rechecked = execute_desktop_codex_setup(&config, CodexSetupMode::Check);
        assert_eq!(rechecked.state, CodexSetupState::Unchanged);
        assert!(rechecked.registered);
        assert!(!rechecked.config_modified);
        assert!(!rechecked.restart_required);
        assert!(rechecked.pairing_required);
    }
}
