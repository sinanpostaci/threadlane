//! `threadlane-codex-setup`: trusted local CLI for inspecting (`--check`) or applying the
//! unbound paired `mcp_servers.threadlane` registration in Codex `config.toml`.
//!
//! The default target is the user-level Codex home. The explicit `--project-dir` opt-in selects
//! only an existing project's `.codex/config.toml` instead and is mutually exclusive with
//! `--codex-home`; project trust remains a separate Codex requirement.
//!
//! Uses [`threadlane_native::codex_setup`] directly without spawning a `codex` subprocess or
//! mutating the process environment, and prints only a sanitized [`CodexSetupReport`] JSON object.

#![deny(unsafe_code)]
#![deny(missing_docs)]

use std::fs;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use clap::Parser;
use threadlane_native::codex_setup::{
    resolve_codex_home, resolve_native_mcp_executable, run_codex_mcp_setup, CodexSetupMode,
    CodexSetupReport, CodexSetupRequest, CodexSetupState, CODEX_CONFIG_FILENAME,
};
use threadlane_native::fs_safety::validate_absolute_no_symlink_components;

const MSG_INVALID_PROJECT_DIR: &str = "The --project-dir path must be an existing canonical absolute project directory without symlink components; no files were modified.";
const MSG_CONFLICTING_CODEX_HOME: &str =
    "--project-dir cannot be combined with --codex-home; choose exactly one configuration location.";

/// Command-line arguments for `threadlane-codex-setup`.
#[derive(Debug, Clone, Parser)]
#[command(
    name = "threadlane-codex-setup",
    version,
    about = "Inspect or apply the unbound paired mcp_servers.threadlane entry in Codex config.toml"
)]
pub struct CodexSetupCli {
    /// Inspect registration state in read-only mode without creating or modifying any files or directories.
    #[arg(long = "check", default_value_t = false)]
    pub check: bool,

    /// Optional explicit path to an existing project directory. Selects only
    /// `<project-dir>/.codex/config.toml`; requires a clean canonical absolute non-symlink
    /// directory and is mutually exclusive with `--codex-home`.
    #[arg(
        long = "project-dir",
        value_name = "ABS",
        conflicts_with = "codex_home"
    )]
    pub project_dir: Option<PathBuf>,

    /// Optional explicit path to the Codex home directory (defaults to `CODEX_HOME`, then `$HOME/.codex`).
    #[arg(
        long = "codex-home",
        value_name = "DIR",
        conflicts_with = "project_dir"
    )]
    pub codex_home: Option<PathBuf>,

    /// Optional explicit path to the `threadlane-native-mcp` executable (defaults to `PATH` lookup, then canonical sibling of current executable).
    #[arg(
        long = "mcp-executable",
        visible_alias = "mcp-binary",
        value_name = "ABS"
    )]
    pub mcp_executable: Option<PathBuf>,

    /// Clean absolute path to the Threadlane runtime supervisor state directory (`--state-dir`).
    #[arg(long = "state-dir", value_name = "DIR")]
    pub state_dir: PathBuf,

    /// Clean absolute path to the private (`0700`) native MCP pairing store directory (`--pairing-dir`).
    #[arg(long = "pairing-dir", value_name = "DIR")]
    pub pairing_dir: PathBuf,

    /// Clean absolute path to the workflow registry directory (`--registry-dir`).
    #[arg(long = "registry-dir", value_name = "DIR")]
    pub registry_dir: PathBuf,

    /// Clean absolute path to the workflow plans store directory (`--plans-dir`).
    #[arg(long = "plans-dir", value_name = "DIR")]
    pub plans_dir: PathBuf,

    /// Clean absolute path to the operator approval authority directory (`--authority-dir`).
    #[arg(long = "authority-dir", value_name = "DIR")]
    pub authority_dir: PathBuf,

    /// Clean absolute path to the approved host sessions root directory (`--sessions-dir`).
    #[arg(long = "sessions-dir", value_name = "DIR")]
    pub sessions_dir: PathBuf,

    /// Clean absolute path to the private Codex setup management directory (`codex-setup.lock` and ownership receipt).
    #[arg(long = "management-dir", value_name = "DIR")]
    pub management_dir: PathBuf,
}

/// Validates an explicit `--project-dir` and derives its project-scoped Codex home.
///
/// The project directory must already exist as a clean canonical absolute directory with no
/// symlink component. This helper never creates the project directory and never changes project
/// trust or any Codex configuration; apply may create only `<project>/.codex` through the existing
/// setup path.
fn resolve_project_codex_home(project_dir: &Path) -> Result<PathBuf, &'static str> {
    let clean = validate_absolute_no_symlink_components(project_dir, "project directory")
        .map_err(|_| MSG_INVALID_PROJECT_DIR)?;
    let text = clean.to_str().ok_or(MSG_INVALID_PROJECT_DIR)?;
    if text.is_empty() || text.chars().any(char::is_control) {
        return Err(MSG_INVALID_PROJECT_DIR);
    }

    let meta = fs::symlink_metadata(&clean).map_err(|_| MSG_INVALID_PROJECT_DIR)?;
    if meta.file_type().is_symlink() || !meta.file_type().is_dir() {
        return Err(MSG_INVALID_PROJECT_DIR);
    }

    let canonical = fs::canonicalize(&clean).map_err(|_| MSG_INVALID_PROJECT_DIR)?;
    if canonical != clean {
        return Err(MSG_INVALID_PROJECT_DIR);
    }

    Ok(canonical.join(".codex"))
}

fn execute_cli(cli: CodexSetupCli) -> CodexSetupReport {
    let mode = if cli.check {
        CodexSetupMode::Check
    } else {
        CodexSetupMode::Apply
    };

    if cli.codex_home.is_some() && cli.project_dir.is_some() {
        return CodexSetupReport::error(
            mode,
            None,
            "conflicting_codex_home",
            MSG_CONFLICTING_CODEX_HOME,
        );
    }

    let codex_home = if let Some(project_dir) = cli.project_dir.as_deref() {
        match resolve_project_codex_home(project_dir) {
            Ok(dir) => dir,
            Err(message) => {
                return CodexSetupReport::error(mode, None, "invalid_project_dir", message);
            }
        }
    } else if let Some(dir) = cli.codex_home {
        dir
    } else {
        match resolve_codex_home() {
            Ok(dir) => dir,
            Err(_) => {
                return CodexSetupReport::error(
                    mode,
                    None,
                    "codex_home_unresolved",
                    "Unable to resolve Codex home directory from CODEX_HOME or HOME.",
                );
            }
        }
    };

    let config_path_display = codex_home
        .join(CODEX_CONFIG_FILENAME)
        .to_str()
        .map(str::to_string);

    let mcp_executable = match cli.mcp_executable {
        Some(path) => path,
        None => match resolve_native_mcp_executable() {
            Ok(path) => path,
            Err(_) => {
                return CodexSetupReport::error(
                    mode,
                    config_path_display,
                    "invalid_executable",
                    "Unable to resolve a valid threadlane-native-mcp executable on PATH or next to the current binary.",
                );
            }
        },
    };

    let request = CodexSetupRequest {
        codex_home,
        mcp_executable,
        state_dir: cli.state_dir,
        pairing_dir: cli.pairing_dir,
        registry_dir: cli.registry_dir,
        plans_dir: cli.plans_dir,
        authority_dir: cli.authority_dir,
        sessions_dir: cli.sessions_dir,
        management_dir: cli.management_dir,
    };

    run_codex_mcp_setup(&request, mode)
}

fn main() -> ExitCode {
    let cli = CodexSetupCli::parse();
    let report = execute_cli(cli);
    match serde_json::to_string_pretty(&report) {
        Ok(json) => println!("{json}"),
        Err(_) => {
            eprintln!(
                "{{\"state\":\"error\",\"message\":\"Failed to serialize Codex setup report.\"}}"
            );
            return ExitCode::FAILURE;
        }
    }

    if report.state == CodexSetupState::Error {
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::OsString;
    use std::fs;
    use std::os::unix::fs::PermissionsExt;
    use std::path::{Path, PathBuf};
    use std::sync::Mutex;

    use tempfile::TempDir;

    use super::*;

    // Serialize the test that mutates CODEX_HOME; other tests pass explicit paths.
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    fn write_mcp_executable(root: &Path) -> PathBuf {
        let bin_dir = root.join("bin");
        fs::create_dir_all(&bin_dir).unwrap();
        let mcp_exe = bin_dir.join("threadlane-native-mcp");
        fs::write(&mcp_exe, b"#!/bin/sh\nexit 0\n").unwrap();
        fs::set_permissions(&mcp_exe, fs::Permissions::from_mode(0o755)).unwrap();
        mcp_exe
    }

    fn store_args(root: &Path) -> Vec<String> {
        let mut args = Vec::new();
        for (flag, name) in [
            ("--state-dir", "state"),
            ("--pairing-dir", "pairing"),
            ("--registry-dir", "registry"),
            ("--plans-dir", "plans"),
            ("--authority-dir", "authority"),
            ("--sessions-dir", "sessions"),
            ("--management-dir", "management"),
        ] {
            args.push(flag.to_string());
            args.push(root.join(name).to_str().unwrap().to_string());
        }
        args
    }

    fn project_args(project: &Path, mcp_exe: &Path, store_root: &Path, check: bool) -> Vec<String> {
        let mut args = vec![
            "threadlane-codex-setup".to_string(),
            "--project-dir".to_string(),
            project.to_str().unwrap().to_string(),
            "--mcp-executable".to_string(),
            mcp_exe.to_str().unwrap().to_string(),
        ];
        if check {
            args.push("--check".to_string());
        }
        args.extend(store_args(store_root));
        args
    }

    struct EnvGuard {
        key: &'static str,
        original: Option<OsString>,
    }

    impl EnvGuard {
        fn set(key: &'static str, value: &Path) -> Self {
            let original = std::env::var_os(key);
            std::env::set_var(key, value);
            Self { key, original }
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            match &self.original {
                Some(value) => std::env::set_var(self.key, value),
                None => std::env::remove_var(self.key),
            }
        }
    }

    #[test]
    fn cli_parses_explicit_flags_and_executes_check_then_apply() {
        let tmp = TempDir::new().expect("tempdir");
        let root = tmp.path();
        let mcp_exe = write_mcp_executable(root);

        let codex_home = root.join("codex-home");
        let state_dir = root.join("state");
        let pairing_dir = root.join("pairing");
        let registry_dir = root.join("registry");
        let plans_dir = root.join("plans");
        let authority_dir = root.join("authority");
        let sessions_dir = root.join("sessions");
        let management_dir = root.join("management");

        let check_cli = CodexSetupCli::try_parse_from([
            "threadlane-codex-setup",
            "--check",
            "--codex-home",
            codex_home.to_str().unwrap(),
            "--mcp-executable",
            mcp_exe.to_str().unwrap(),
            "--state-dir",
            state_dir.to_str().unwrap(),
            "--pairing-dir",
            pairing_dir.to_str().unwrap(),
            "--registry-dir",
            registry_dir.to_str().unwrap(),
            "--plans-dir",
            plans_dir.to_str().unwrap(),
            "--authority-dir",
            authority_dir.to_str().unwrap(),
            "--sessions-dir",
            sessions_dir.to_str().unwrap(),
            "--management-dir",
            management_dir.to_str().unwrap(),
        ])
        .expect("parse check cli");

        let check_report = execute_cli(check_cli.clone());
        assert_eq!(check_report.mode, CodexSetupMode::Check);
        assert_eq!(check_report.state, CodexSetupState::NotRegistered);
        assert!(!codex_home.exists());
        assert!(!management_dir.exists());

        let mut apply_cli = check_cli;
        apply_cli.check = false;
        let apply_report = execute_cli(apply_cli);
        assert_eq!(apply_report.mode, CodexSetupMode::Apply);
        assert_eq!(apply_report.state, CodexSetupState::Registered);
        assert!(apply_report.registered);
        assert!(apply_report.config_modified);
        assert!(apply_report.restart_required);
        assert!(apply_report.pairing_required);
        assert!(codex_home.join(CODEX_CONFIG_FILENAME).exists());
    }

    #[test]
    fn project_dir_check_then_apply_uses_only_project_codex_home() {
        let tmp = TempDir::new().expect("tempdir");
        let root = tmp.path();
        let project = root.join("project");
        fs::create_dir_all(&project).unwrap();
        let mcp_exe = write_mcp_executable(root);

        let global_codex_home = root.join("global-codex-home");
        fs::create_dir_all(&global_codex_home).unwrap();
        let global_config = global_codex_home.join(CODEX_CONFIG_FILENAME);
        fs::write(&global_config, b"# global sentinel\n").unwrap();

        let check_cli = CodexSetupCli::try_parse_from(project_args(&project, &mcp_exe, root, true))
            .expect("parse project check cli");
        assert_eq!(check_cli.project_dir.as_deref(), Some(project.as_path()));
        assert!(check_cli.codex_home.is_none());

        let check_report = execute_cli(check_cli.clone());
        let expected_config = project.join(".codex").join(CODEX_CONFIG_FILENAME);
        assert_eq!(check_report.mode, CodexSetupMode::Check);
        assert_eq!(check_report.state, CodexSetupState::NotRegistered);
        assert_eq!(
            check_report.config_path.as_deref(),
            expected_config.to_str()
        );
        assert!(!project.join(".codex").exists());
        assert!(!root.join("management").exists());

        let mut apply_cli = check_cli;
        apply_cli.check = false;
        let apply_report = execute_cli(apply_cli);
        assert_eq!(apply_report.mode, CodexSetupMode::Apply);
        assert_eq!(apply_report.state, CodexSetupState::Registered);
        assert_eq!(
            apply_report.config_path.as_deref(),
            expected_config.to_str()
        );
        assert!(apply_report.config_modified);
        assert!(expected_config.exists());

        let content = fs::read_to_string(&expected_config).unwrap();
        assert!(content.contains(mcp_exe.to_str().unwrap()));
        for name in [
            "state",
            "pairing",
            "registry",
            "plans",
            "authority",
            "sessions",
        ] {
            assert!(content.contains(root.join(name).to_str().unwrap()));
        }
        assert_eq!(fs::read(&global_config).unwrap(), b"# global sentinel\n");
    }

    #[test]
    fn project_dir_and_codex_home_are_mutually_exclusive() {
        let mut args = vec![
            "threadlane-codex-setup".to_string(),
            "--project-dir".to_string(),
            "/absolute/project".to_string(),
            "--codex-home".to_string(),
            "/absolute/codex-home".to_string(),
        ];
        args.extend(store_args(Path::new("/absolute")));

        let err = CodexSetupCli::try_parse_from(args).expect_err("conflicting flags must fail");
        assert_eq!(err.kind(), clap::error::ErrorKind::ArgumentConflict);
    }

    #[test]
    fn project_dir_rejects_relative_symlink_and_missing_locations() {
        let tmp = TempDir::new().expect("tempdir");
        let root = tmp.path();
        let project = root.join("project");
        fs::create_dir_all(&project).unwrap();
        let linked_project = root.join("linked-project");
        std::os::unix::fs::symlink(&project, &linked_project).unwrap();
        let missing_project = root.join("missing-project");
        let mcp_exe = write_mcp_executable(root);

        for (label, candidate) in [
            ("relative", PathBuf::from("relative-project")),
            ("symlink", linked_project.clone()),
            ("missing", missing_project.clone()),
        ] {
            let cli =
                CodexSetupCli::try_parse_from(project_args(&candidate, &mcp_exe, root, false))
                    .expect("parse invalid project fixture");
            let report = execute_cli(cli);
            assert_eq!(report.mode, CodexSetupMode::Apply, "{label}");
            assert_eq!(report.state, CodexSetupState::Error, "{label}");
            assert_eq!(
                report.error_code.as_deref(),
                Some("invalid_project_dir"),
                "{label}"
            );
            assert!(report.config_path.is_none(), "{label}");
            assert!(!report.config_modified, "{label}");
        }

        assert!(!project.join(".codex").exists());
        assert!(!linked_project.join(".codex").exists());
        assert!(!missing_project.exists());
        assert!(!root.join("management").exists());
    }

    #[test]
    fn project_dir_apply_does_not_modify_global_codex_home() {
        let tmp = TempDir::new().expect("tempdir");
        let root = tmp.path();
        let project = root.join("project");
        fs::create_dir_all(&project).unwrap();
        let mcp_exe = write_mcp_executable(root);

        let global_codex_home = root.join("global-codex-home");
        fs::create_dir_all(&global_codex_home).unwrap();
        let global_config = global_codex_home.join(CODEX_CONFIG_FILENAME);
        fs::write(&global_config, b"# global sentinel\n").unwrap();

        let _env_lock = ENV_LOCK.lock().expect("env lock");
        let _guard = EnvGuard::set("CODEX_HOME", &global_codex_home);

        let cli = CodexSetupCli::try_parse_from(project_args(&project, &mcp_exe, root, false))
            .expect("parse project apply cli");
        let report = execute_cli(cli);

        assert_eq!(report.state, CodexSetupState::Registered);
        assert!(project.join(".codex").join(CODEX_CONFIG_FILENAME).exists());
        assert_eq!(fs::read(&global_config).unwrap(), b"# global sentinel\n");
    }
}
