//! Local operator and diagnostic CLI for installing, updating, or inspecting the bundled
//! user-level Threadlane Codex skill.

use std::path::PathBuf;
use std::process::ExitCode;

use clap::Parser;
use threadlane_native::codex_setup::CodexSetupMode;
use threadlane_native::codex_skill::{
    resolve_codex_skills_dir, run_codex_skill_setup, CodexSkillSetupReport, CodexSkillSetupRequest,
    CodexSkillSetupState, THREADLANE_SKILL_VERSION,
};

/// Install, update, or inspect the user-level bundled Threadlane skill in `~/.agents/skills/threadlane`.
#[derive(Debug, Parser)]
#[command(
    name = "threadlane-skill-setup",
    about = "Install, update, or inspect the user-level bundled Threadlane Codex skill"
)]
struct Cli {
    /// Inspect the user-level skill directory in read-only mode without creating or modifying files.
    #[arg(long)]
    check: bool,

    /// Explicit parent skills directory (defaults to `$HOME/.agents/skills` or `$USERPROFILE/.agents/skills`).
    #[arg(long = "skills-dir", value_name = "DIR")]
    skills_dir: Option<PathBuf>,
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    let mode = if cli.check {
        CodexSetupMode::Check
    } else {
        CodexSetupMode::Apply
    };

    let report = match cli.skills_dir {
        Some(skills_dir) => run_codex_skill_setup(&CodexSkillSetupRequest { skills_dir }, mode),
        None => match resolve_codex_skills_dir() {
            Ok(skills_dir) => run_codex_skill_setup(&CodexSkillSetupRequest { skills_dir }, mode),
            Err(err) => CodexSkillSetupReport::error(
                mode,
                None,
                err.code.as_str(),
                "failed to resolve user-level Codex skills directory",
            ),
        },
    };

    match serde_json::to_string_pretty(&report) {
        Ok(json) => println!("{json}"),
        Err(_) => {
            println!(
                "{{\"state\":\"error\",\"mode\":\"{}\",\"skill_path\":null,\"version\":\"{}\",\"installed\":false,\"files_modified\":false,\"error_code\":\"protocol_error\",\"message\":\"failed to serialize skill setup report\"}}",
                mode.as_str(),
                THREADLANE_SKILL_VERSION
            );
            return ExitCode::from(1);
        }
    }

    match report.state {
        CodexSkillSetupState::Conflict => ExitCode::from(2),
        CodexSkillSetupState::Error => ExitCode::from(1),
        CodexSkillSetupState::Installed
        | CodexSkillSetupState::Updated
        | CodexSkillSetupState::Unchanged
        | CodexSkillSetupState::NotInstalled
        | CodexSkillSetupState::Outdated => ExitCode::SUCCESS,
    }
}
