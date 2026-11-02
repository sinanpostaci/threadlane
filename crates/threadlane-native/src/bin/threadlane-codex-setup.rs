//! `threadlane-codex-setup`: trusted local CLI for inspecting (`--check`) or applying the
//! unbound paired `mcp_servers.threadlane` registration in Codex `config.toml`.
//!
//! Uses [`threadlane_native::codex_setup`] directly without spawning a `codex` subprocess or
//! mutating the process environment, and prints only a sanitized [`CodexSetupReport`] JSON object.

#![deny(unsafe_code)]
#![deny(missing_docs)]

use std::path::PathBuf;
use std::process::ExitCode;

use clap::Parser;
use threadlane_native::codex_setup::{
    resolve_codex_home, resolve_native_mcp_executable, run_codex_mcp_setup, CodexSetupMode,
    CodexSetupReport, CodexSetupRequest, CodexSetupState, CODEX_CONFIG_FILENAME,
};

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

    /// Optional explicit path to the Codex home directory (defaults to `CODEX_HOME`, then `$HOME/.codex`).
    #[arg(long = "codex-home", value_name = "DIR")]
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

fn execute_cli(cli: CodexSetupCli) -> CodexSetupReport {
    let mode = if cli.check {
        CodexSetupMode::Check
    } else {
        CodexSetupMode::Apply
    };

    let codex_home = match cli.codex_home {
        Some(dir) => dir,
        None => match resolve_codex_home() {
            Ok(dir) => dir,
            Err(_) => {
                return CodexSetupReport::error(
                    mode,
                    None,
                    "codex_home_unresolved",
                    "Unable to resolve Codex home directory from CODEX_HOME or HOME.",
                );
            }
        },
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
    use std::fs;
    use std::os::unix::fs::PermissionsExt;

    use tempfile::TempDir;

    use super::*;

    #[test]
    fn cli_parses_explicit_flags_and_executes_check_then_apply() {
        let tmp = TempDir::new().expect("tempdir");
        let root = tmp.path();
        let bin_dir = root.join("bin");
        fs::create_dir_all(&bin_dir).unwrap();
        let mcp_exe = bin_dir.join("threadlane-native-mcp");
        fs::write(&mcp_exe, b"#!/bin/sh\nexit 0\n").unwrap();
        fs::set_permissions(&mcp_exe, fs::Permissions::from_mode(0o755)).unwrap();

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
}
