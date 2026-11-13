use std::ffi::OsString;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use threadlane_native::codex_setup::{
    resolve_codex_home, resolve_default_runtime_state_dir, resolve_default_runtime_state_dir_with,
    resolve_native_mcp_executable, run_codex_mcp_setup, CodexSetupMode, CodexSetupReport,
    CodexSetupRequest, CODEX_CONFIG_FILENAME,
};
use threadlane_native::codex_skill::{
    resolve_codex_skills_dir, run_codex_skill_setup, CodexSkillSetupReport, CodexSkillSetupRequest,
};
use threadlane_native::NativeError;

use crate::AppError;

/// Combined desktop startup setup report flattening the MCP registration report
/// while attaching the independent bundled-skill installation report.
#[derive(Debug, Clone, serde::Serialize)]
pub(crate) struct DesktopCodexSetupReport {
    #[serde(flatten)]
    pub(crate) mcp: CodexSetupReport,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) skill_setup: Option<CodexSkillSetupReport>,
}

impl DesktopCodexSetupReport {
    pub(crate) fn checking(config_path: Option<String>) -> Self {
        Self {
            mcp: CodexSetupReport::checking(config_path),
            skill_setup: None,
        }
    }
}

impl Deref for DesktopCodexSetupReport {
    type Target = CodexSetupReport;

    fn deref(&self) -> &Self::Target {
        &self.mcp
    }
}

/// Subdirectory under `app_data_dir` used for the private native MCP pairing store.
pub(crate) const DESKTOP_PAIRING_SUBDIR: &str = "pairing";

/// Subdirectory under `app_data_dir` used for the private Codex setup management lock and receipt.
pub(crate) const DESKTOP_CODEX_SETUP_SUBDIR: &str = "codex-setup";

/// Resolves the runtime `state_dir` shared by managed Codex MCP setup and read-only workspace
/// observation for a desktop launch.
///
/// An explicit `--state-dir` always takes precedence. A demo-only launch (no explicit runtime
/// state directory, but a demo directory) returns `None` so observation is not silently attached
/// to the user's production runtime. Otherwise the same native default resolution as managed MCP
/// setup applies, without creating or starting a supervisor.
pub(crate) fn resolve_desktop_runtime_state_dir(
    supplied_state_dir: Option<&Path>,
    demo_state_dir: Option<&Path>,
) -> Result<Option<PathBuf>, NativeError> {
    resolve_desktop_runtime_state_dir_with(supplied_state_dir, demo_state_dir, |key| {
        std::env::var_os(key)
    })
}

/// Resolves the desktop runtime `state_dir` with an explicit environment lookup.
pub(crate) fn resolve_desktop_runtime_state_dir_with<F>(
    supplied_state_dir: Option<&Path>,
    demo_state_dir: Option<&Path>,
    get_env: F,
) -> Result<Option<PathBuf>, NativeError>
where
    F: FnMut(&str) -> Option<OsString>,
{
    if supplied_state_dir.is_none() && demo_state_dir.is_some() {
        return Ok(None);
    }
    resolve_default_runtime_state_dir_with(supplied_state_dir, get_env).map(Some)
}

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
    #[cfg(test)]
    pub(crate) skills_dir_override: Option<PathBuf>,
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
            #[cfg(test)]
            skills_dir_override: None,
        }
    }
}

/// Managed desktop service that runs Codex MCP setup once in a background task at startup
/// and supports explicit operator recheck (`Check`) and retry (`Apply`) commands.
#[derive(Clone)]
pub(crate) struct CodexSetupService {
    config: CodexSetupServiceConfig,
    latest_report: Arc<Mutex<DesktopCodexSetupReport>>,
    run_gate: Arc<tokio::sync::Mutex<()>>,
}

impl CodexSetupService {
    pub(crate) fn new(config: CodexSetupServiceConfig) -> Self {
        let initial_config_path = initial_config_path_hint(&config);
        Self {
            config,
            latest_report: Arc::new(Mutex::new(DesktopCodexSetupReport::checking(
                initial_config_path,
            ))),
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
            .unwrap_or_else(|_| DesktopCodexSetupReport {
                mcp: CodexSetupReport::error(
                    CodexSetupMode::Apply,
                    None,
                    "internal_join_error",
                    "Background Codex MCP setup task terminated unexpectedly.",
                ),
                skill_setup: Some(CodexSkillSetupReport::error(
                    CodexSetupMode::Apply,
                    None,
                    "internal_join_error",
                    "Background Codex skill setup task terminated unexpectedly.",
                )),
            });

            if let Ok(mut slot) = latest_report.lock() {
                *slot = report;
            }
        });
    }

    #[cfg(test)]
    pub(crate) fn cached_report_snapshot(&self) -> DesktopCodexSetupReport {
        self.latest_report
            .lock()
            .expect("codex setup report mutex")
            .clone()
    }

    /// Returns the latest Codex setup report, awaiting the initial startup background task if still in flight.
    pub(crate) async fn status(&self) -> Result<DesktopCodexSetupReport, AppError> {
        let _guard = self.run_gate.lock().await;
        let report = self
            .latest_report
            .lock()
            .map_err(|_| AppError::new("internal_lock_error", "Codex setup state lock failed."))?
            .clone();
        Ok(report)
    }

    /// Runs an explicit read-only recheck (`CodexSetupMode::Check`) and updates the cached report.
    pub(crate) async fn recheck(&self) -> Result<DesktopCodexSetupReport, AppError> {
        self.run_mode(CodexSetupMode::Check).await
    }

    /// Runs an explicit setup retry (`CodexSetupMode::Apply`) and updates the cached report.
    pub(crate) async fn apply(&self) -> Result<DesktopCodexSetupReport, AppError> {
        self.run_mode(CodexSetupMode::Apply).await
    }

    async fn run_mode(&self, mode: CodexSetupMode) -> Result<DesktopCodexSetupReport, AppError> {
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
) -> DesktopCodexSetupReport {
    let mcp = execute_desktop_codex_mcp_setup(config, mode);
    let skill_setup = execute_desktop_codex_skill_setup(config, mode);
    DesktopCodexSetupReport {
        mcp,
        skill_setup: Some(skill_setup),
    }
}

pub(crate) fn execute_desktop_codex_skill_setup(
    config: &CodexSetupServiceConfig,
    mode: CodexSetupMode,
) -> CodexSkillSetupReport {
    let skills_dir_res = {
        #[cfg(test)]
        {
            let _ = resolve_codex_skills_dir;
            config.skills_dir_override.clone().ok_or(())
        }
        #[cfg(not(test))]
        {
            let _ = config;
            resolve_codex_skills_dir().map_err(|_| ())
        }
    };

    let skills_dir = match skills_dir_res {
        Ok(dir) => dir,
        Err(()) => {
            return CodexSkillSetupReport::error(
                mode,
                None,
                "skills_dir_unresolved",
                "Unable to resolve Codex skills directory from HOME.",
            );
        }
    };

    let request = CodexSkillSetupRequest { skills_dir };
    run_codex_skill_setup(&request, mode)
}

pub(crate) fn execute_desktop_codex_mcp_setup(
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
    use std::ffi::OsString;
    use std::fs;
    use std::os::unix::fs::PermissionsExt;
    use std::path::Path;

    use tempfile::TempDir;
    use threadlane_native::codex_setup::{
        CodexSetupState, DEFAULT_NATIVE_MCP_EXECUTABLE_NAME, THREADLANE_CLI_XDG_STATE_HOME_ENV,
    };
    use threadlane_native::codex_skill::{CodexSkillSetupState, THREADLANE_SKILL_VERSION};

    use super::*;

    fn env_for_home(home: &Path, key: &str) -> Option<OsString> {
        (key == "HOME").then(|| OsString::from(home.as_os_str()))
    }

    fn build_isolated_config(root: &Path, name: &str) -> CodexSetupServiceConfig {
        let scoped = root.join(name);
        let bin_dir = scoped.join("bin");
        fs::create_dir_all(&bin_dir).unwrap();
        let mcp_exe = bin_dir.join(DEFAULT_NATIVE_MCP_EXECUTABLE_NAME);
        fs::write(&mcp_exe, b"#!/bin/sh\nexit 0\n").unwrap();
        fs::set_permissions(&mcp_exe, fs::Permissions::from_mode(0o755)).unwrap();

        let app_data = scoped.join("app-data");
        let mut config = CodexSetupServiceConfig::from_desktop_dirs(
            Some(scoped.join("runtime-state")),
            scoped.join("registry"),
            app_data.join("plans"),
            app_data.join("authority"),
            app_data.join("sessions"),
            &app_data,
        );
        config.codex_home_override = Some(scoped.join("codex-home"));
        config.mcp_executable_override = Some(mcp_exe);
        config.skills_dir_override = Some(scoped.join("skills"));
        config
    }

    fn resolve_skill_md_path(report: &CodexSkillSetupReport) -> PathBuf {
        let raw = PathBuf::from(report.skill_path.as_ref().expect("skill_path"));
        if raw.is_dir() {
            raw.join("SKILL.md")
        } else {
            raw
        }
    }

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
        config.skills_dir_override = Some(root.join("skills"));

        let service = CodexSetupService::new(config.clone());
        let initial = service.cached_report_snapshot();
        assert_eq!(initial.state, CodexSetupState::Checking);
        assert!(initial.pairing_required);
        assert!(initial.skill_setup.is_none());

        let initial_json = serde_json::to_value(&initial).expect("serialize initial");
        assert_eq!(initial_json["state"], "checking");
        assert_eq!(initial_json["pairing_required"], true);
        assert!(initial_json.get("skill_setup").is_none());

        let applied = execute_desktop_codex_setup(&config, CodexSetupMode::Apply);
        assert_eq!(applied.state, CodexSetupState::Registered);
        assert!(applied.registered);
        assert!(applied.config_modified);
        assert!(applied.restart_required);
        assert!(applied.pairing_required);
        let applied_skill = applied.skill_setup.as_ref().expect("skill setup report");
        assert_eq!(applied_skill.state, CodexSkillSetupState::Installed);
        assert!(applied_skill.installed);
        assert!(applied_skill.files_modified);
        assert_eq!(applied_skill.version, THREADLANE_SKILL_VERSION);

        let applied_json = serde_json::to_value(&applied).expect("serialize applied");
        assert_eq!(applied_json["state"], "registered");
        assert_eq!(applied_json["registered"], true);
        assert_eq!(applied_json["pairing_required"], true);
        assert_eq!(applied_json["skill_setup"]["state"], "installed");
        assert_eq!(applied_json["skill_setup"]["installed"], true);
        assert_eq!(
            applied_json["skill_setup"]["version"],
            THREADLANE_SKILL_VERSION
        );

        let rechecked = execute_desktop_codex_setup(&config, CodexSetupMode::Check);
        assert_eq!(rechecked.state, CodexSetupState::Unchanged);
        assert!(rechecked.registered);
        assert!(!rechecked.config_modified);
        assert!(!rechecked.restart_required);
        assert!(rechecked.pairing_required);
        let rechecked_skill = rechecked.skill_setup.as_ref().expect("skill setup report");
        assert_eq!(rechecked_skill.state, CodexSkillSetupState::Unchanged);
        assert!(rechecked_skill.installed);
        assert!(!rechecked_skill.files_modified);
    }

    #[test]
    fn skill_install_succeeds_independently_when_mcp_errors_or_is_manual_or_disabled() {
        let tmp = TempDir::new().expect("tempdir");
        let root = tmp.path();

        let mut error_config = build_isolated_config(root, "mcp-error");
        error_config.mcp_executable_override = Some(root.join("missing-mcp-binary"));
        let error_report = execute_desktop_codex_setup(&error_config, CodexSetupMode::Apply);
        assert_eq!(error_report.mcp.state, CodexSetupState::Error);
        assert!(!error_report.mcp.registered);
        assert!(error_report.mcp.pairing_required);
        let error_skill = error_report.skill_setup.as_ref().expect("skill report");
        assert_eq!(error_skill.state, CodexSkillSetupState::Installed);
        assert!(error_skill.installed);
        assert!(error_skill.files_modified);

        let manual_config = build_isolated_config(root, "mcp-manual");
        let manual_codex_home = manual_config.codex_home_override.as_ref().unwrap();
        fs::create_dir_all(manual_codex_home).unwrap();
        let manual_toml = "[mcp_servers.threadlane]\ncommand = \"/custom/manual/mcp\"\nargs = []\n";
        fs::write(manual_codex_home.join(CODEX_CONFIG_FILENAME), manual_toml).unwrap();
        let manual_report = execute_desktop_codex_setup(&manual_config, CodexSetupMode::Apply);
        assert_ne!(manual_report.mcp.state, CodexSetupState::Registered);
        assert!(!manual_report.mcp.registered);
        assert!(!manual_report.mcp.config_modified);
        let manual_skill = manual_report.skill_setup.as_ref().expect("skill report");
        assert_eq!(manual_skill.state, CodexSkillSetupState::Installed);
        assert!(manual_skill.installed);
        assert!(manual_skill.files_modified);

        let disabled_config = build_isolated_config(root, "mcp-disabled");
        let disabled_codex_home = disabled_config.codex_home_override.as_ref().unwrap();
        fs::create_dir_all(disabled_codex_home).unwrap();
        let disabled_toml = "[mcp_servers.threadlane]\nenabled = false\n";
        fs::write(
            disabled_codex_home.join(CODEX_CONFIG_FILENAME),
            disabled_toml,
        )
        .unwrap();
        let disabled_report = execute_desktop_codex_setup(&disabled_config, CodexSetupMode::Apply);
        assert_ne!(disabled_report.mcp.state, CodexSetupState::Registered);
        assert!(!disabled_report.mcp.registered);
        assert!(!disabled_report.mcp.config_modified);
        let disabled_skill = disabled_report.skill_setup.as_ref().expect("skill report");
        assert_eq!(disabled_skill.state, CodexSkillSetupState::Installed);
        assert!(disabled_skill.installed);
        assert!(disabled_skill.files_modified);
    }

    #[test]
    fn skill_conflict_preserves_edited_skill_without_affecting_mcp_registration() {
        let tmp = TempDir::new().expect("tempdir");
        let config = build_isolated_config(tmp.path(), "conflict-case");
        let skills_dir = config.skills_dir_override.clone().unwrap();

        let seed = run_codex_skill_setup(
            &CodexSkillSetupRequest {
                skills_dir: skills_dir.clone(),
            },
            CodexSetupMode::Apply,
        );
        assert_eq!(seed.state, CodexSkillSetupState::Installed);
        let skill_md = resolve_skill_md_path(&seed);
        let edited_contents = "# custom user edited threadlane skill\n";
        fs::write(&skill_md, edited_contents).unwrap();

        let report = execute_desktop_codex_setup(&config, CodexSetupMode::Apply);
        assert_eq!(report.mcp.state, CodexSetupState::Registered);
        assert!(report.mcp.registered);
        assert!(report.mcp.config_modified);
        assert!(report.mcp.pairing_required);

        let skill = report.skill_setup.as_ref().expect("skill report");
        assert_eq!(skill.state, CodexSkillSetupState::Conflict);
        assert!(!skill.files_modified);
        assert_eq!(fs::read_to_string(&skill_md).unwrap(), edited_contents);
    }

    #[test]
    fn check_mode_on_missing_paths_creates_nothing_under_either_target() {
        let tmp = TempDir::new().expect("tempdir");
        let config = build_isolated_config(tmp.path(), "check-missing");
        let codex_home = config.codex_home_override.as_ref().unwrap();
        let skills_dir = config.skills_dir_override.as_ref().unwrap();
        assert!(!codex_home.exists());
        assert!(!skills_dir.exists());

        let report = execute_desktop_codex_setup(&config, CodexSetupMode::Check);
        assert!(!report.mcp.registered);
        assert!(!report.mcp.config_modified);
        assert!(report.mcp.pairing_required);

        let skill = report.skill_setup.as_ref().expect("skill report");
        assert_eq!(skill.state, CodexSkillSetupState::NotInstalled);
        assert!(!skill.installed);
        assert!(!skill.files_modified);
        assert!(!codex_home.exists());
        assert!(!skills_dir.exists());
    }

    #[test]
    fn multiple_runtimes_share_skills_root_and_preserve_user_extra_files_on_repeated_apply() {
        let tmp = TempDir::new().expect("tempdir");
        let shared_skills = tmp.path().join("shared-skills");
        let mut config_a = build_isolated_config(tmp.path(), "runtime-a");
        let mut config_b = build_isolated_config(tmp.path(), "runtime-b");
        config_a.skills_dir_override = Some(shared_skills.clone());
        config_b.skills_dir_override = Some(shared_skills.clone());

        let other_skill_file = shared_skills.join("custom-user-skill").join("SKILL.md");
        fs::create_dir_all(other_skill_file.parent().unwrap()).unwrap();
        fs::write(&other_skill_file, "# user extra skill\n").unwrap();

        let first = execute_desktop_codex_setup(&config_a, CodexSetupMode::Apply);
        assert_eq!(first.mcp.state, CodexSetupState::Registered);
        let config_a_home = config_a.codex_home_override.as_ref().unwrap();
        let config_a_toml = fs::read_to_string(config_a_home.join(CODEX_CONFIG_FILENAME)).unwrap();
        assert!(config_a_toml.contains("runtime-a"));
        let first_skill = first.skill_setup.as_ref().expect("first skill report");
        assert_eq!(first_skill.state, CodexSkillSetupState::Installed);
        assert!(first_skill.installed);
        assert!(first_skill.files_modified);

        let skill_md = resolve_skill_md_path(first_skill);
        let extra_note = skill_md.parent().unwrap().join("USER_NOTES.md");
        fs::write(&extra_note, "preserve user note\n").unwrap();

        let second = execute_desktop_codex_setup(&config_b, CodexSetupMode::Apply);
        assert_eq!(second.mcp.state, CodexSetupState::Registered);
        let config_b_home = config_b.codex_home_override.as_ref().unwrap();
        let config_b_toml = fs::read_to_string(config_b_home.join(CODEX_CONFIG_FILENAME)).unwrap();
        assert!(config_b_toml.contains("runtime-b"));
        let second_skill = second.skill_setup.as_ref().expect("second skill report");
        assert_eq!(second_skill.state, CodexSkillSetupState::Unchanged);
        assert!(second_skill.installed);
        assert!(!second_skill.files_modified);

        assert_eq!(
            fs::read_to_string(&other_skill_file).unwrap(),
            "# user extra skill\n"
        );
        assert_eq!(
            fs::read_to_string(&extra_note).unwrap(),
            "preserve user note\n"
        );
    }

    #[test]
    fn desktop_runtime_resolution_matches_managed_setup_and_explicit_override_wins() {
        let tmp = TempDir::new().expect("tempdir");
        let root = tmp.path();
        let home = root.join("home");
        fs::create_dir_all(&home).expect("create home");

        let resolved =
            resolve_desktop_runtime_state_dir_with(None, None, |key| env_for_home(&home, key))
                .expect("resolve default runtime")
                .expect("default runtime present");
        let expected_default = home
            .join(".local")
            .join("state")
            .join("threadlane")
            .join("runtime");
        assert_eq!(resolved, expected_default);
        assert!(
            !resolved.exists(),
            "runtime resolution must not create the runtime directory"
        );

        let app_data = root.join("app-data");
        let bin_dir = root.join("bin");
        fs::create_dir_all(&bin_dir).expect("create bin dir");
        let mcp_exe = bin_dir.join(DEFAULT_NATIVE_MCP_EXECUTABLE_NAME);
        fs::write(&mcp_exe, b"#!/bin/sh\nexit 0\n").expect("write fake MCP");
        fs::set_permissions(&mcp_exe, fs::Permissions::from_mode(0o755)).expect("chmod fake MCP");

        let mut config = CodexSetupServiceConfig::from_desktop_dirs(
            Some(resolved.clone()),
            root.join("registry"),
            app_data.join("plans"),
            app_data.join("authority"),
            app_data.join("sessions"),
            &app_data,
        );
        config.codex_home_override = Some(root.join("codex-home"));
        config.mcp_executable_override = Some(mcp_exe);
        config.skills_dir_override = Some(root.join("skills"));

        let report = execute_desktop_codex_mcp_setup(&config, CodexSetupMode::Apply);
        assert_eq!(report.state, CodexSetupState::Registered);
        let applied = fs::read_to_string(root.join("codex-home").join(CODEX_CONFIG_FILENAME))
            .expect("read applied Codex config");
        let resolved_text = resolved.to_str().expect("UTF-8 runtime path");
        assert!(
            applied.contains(resolved_text),
            "managed MCP setup must use the same resolved runtime path as observation"
        );

        let explicit = root.join("explicit-runtime");
        let demo = root.join("demo-runtime");
        let resolved_explicit =
            resolve_desktop_runtime_state_dir_with(Some(&explicit), Some(&demo), |key| {
                env_for_home(&home, key)
            })
            .expect("resolve explicit runtime")
            .expect("explicit runtime present");
        assert_eq!(resolved_explicit, explicit);

        assert!(resolve_desktop_runtime_state_dir_with(
            Some(Path::new("relative-runtime")),
            None,
            |key| env_for_home(&home, key),
        )
        .is_err());

        #[cfg(unix)]
        {
            use std::os::unix::fs::symlink;

            let linked_home = root.join("linked-home");
            symlink(&home, &linked_home).expect("create symlinked home");
            assert!(resolve_desktop_runtime_state_dir_with(
                Some(&linked_home.join("runtime")),
                None,
                |key| env_for_home(&home, key),
            )
            .is_err());
        }
    }

    #[test]
    fn desktop_runtime_resolution_preserves_cli_xdg_marker_and_home_defaults() {
        let tmp = TempDir::new().expect("tempdir");
        let home = tmp.path().join("home");
        let isolated_xdg = tmp.path().join("isolated-xdg");
        let original_xdg = tmp.path().join("original-xdg");

        let without_marker = resolve_desktop_runtime_state_dir_with(None, None, |key| match key {
            "HOME" => Some(OsString::from(home.as_os_str())),
            "XDG_STATE_HOME" => Some(OsString::from(isolated_xdg.as_os_str())),
            _ => None,
        })
        .expect("resolve XDG runtime")
        .expect("XDG runtime present");
        assert_eq!(
            without_marker,
            isolated_xdg.join("threadlane").join("runtime")
        );

        let empty_marker = resolve_desktop_runtime_state_dir_with(None, None, |key| match key {
            "HOME" => Some(OsString::from(home.as_os_str())),
            "XDG_STATE_HOME" => Some(OsString::from(isolated_xdg.as_os_str())),
            THREADLANE_CLI_XDG_STATE_HOME_ENV => Some(OsString::new()),
            _ => None,
        })
        .expect("resolve HOME runtime")
        .expect("HOME runtime present");
        assert_eq!(
            empty_marker,
            home.join(".local")
                .join("state")
                .join("threadlane")
                .join("runtime")
        );

        let original_marker = resolve_desktop_runtime_state_dir_with(None, None, |key| match key {
            "HOME" => Some(OsString::from(home.as_os_str())),
            "XDG_STATE_HOME" => Some(OsString::from(isolated_xdg.as_os_str())),
            THREADLANE_CLI_XDG_STATE_HOME_ENV => Some(OsString::from(original_xdg.as_os_str())),
            _ => None,
        })
        .expect("resolve original CLI runtime")
        .expect("original CLI runtime present");
        assert_eq!(
            original_marker,
            original_xdg.join("threadlane").join("runtime")
        );
    }

    #[test]
    fn demo_only_launch_does_not_attach_to_default_production_runtime() {
        let tmp = TempDir::new().expect("tempdir");
        let home = tmp.path().join("home");
        let demo = tmp.path().join("demo-runtime");

        assert_eq!(
            resolve_desktop_runtime_state_dir_with(None, Some(&demo), |key| {
                env_for_home(&home, key)
            })
            .expect("resolve demo-only launch"),
            None
        );
        assert_eq!(
            resolve_desktop_runtime_state_dir_with(None, Some(&demo), |_| None)
                .expect("demo-only resolution must not require HOME"),
            None
        );
    }
}
