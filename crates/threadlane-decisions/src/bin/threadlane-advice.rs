#![forbid(unsafe_code)]

//! `threadlane-advice`: shadow-mode Jev difficulty classification and catalog CLI.

use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;

use clap::error::ErrorKind;
use clap::{Parser, Subcommand};
use threadlane_connectors::{CredentialStore, KeyringCredentialStore};
use threadlane_decisions::{
    classify_advice, list_advice_models, preview_advice, AdviceError, AdviceScope, DecisionClient,
    DEFAULT_CACHE_TTL_SECONDS,
};

#[derive(Debug, Parser)]
#[command(
    name = "threadlane-advice",
    version,
    about = "Shadow-mode Jev difficulty advice and catalog CLI for Threadlane",
    disable_help_subcommand = true
)]
struct AdviceCli {
    /// Absolute path to the private Threadlane registry directory.
    #[arg(long = "registry-dir", value_name = "ABS")]
    registry_dir: Option<PathBuf>,

    /// Absolute path to the private shadow advice cache directory.
    #[arg(long = "cache-dir", value_name = "ABS")]
    cache_dir: Option<PathBuf>,

    /// Absolute path to the canonical project root directory.
    #[arg(long = "project-root", value_name = "ABS")]
    project_root: Option<PathBuf>,

    /// Local correlation context identifier.
    #[arg(long = "context-id", value_name = "ID")]
    context_id: Option<String>,

    /// Completed-result cache TTL in seconds (1..=86400, default 3600).
    #[arg(long = "cache-ttl-seconds", value_name = "SECONDS", global = true)]
    cache_ttl_seconds: Option<u64>,

    #[command(subcommand)]
    command: AdviceCommand,
}

#[derive(Debug, Subcommand)]
enum AdviceCommand {
    /// Validate and preview the selected Jev provider/model and task summary offline.
    Preview {
        /// Absolute path to the private advice request JSON file.
        #[arg(long = "request", value_name = "ABS")]
        request: PathBuf,
    },
    /// Execute a single-attempt shadow Jev difficulty classification or return a completed cache hit.
    Classify {
        /// Absolute path to the private advice request JSON file.
        #[arg(long = "request", value_name = "ABS")]
        request: PathBuf,
        /// Explicitly authorize replacing an expired, pending, uncertain, or completed cache entry.
        #[arg(long = "refresh")]
        refresh: bool,
    },
    /// Query Jev model catalog metadata for an enabled jev_api tool without mutating the registry.
    Models {
        /// Registry tool identifier for an enabled jev_api tool.
        #[arg(long = "tool-id", value_name = "ID")]
        tool_id: String,
    },
}

fn emit_error_and_exit(err: &AdviceError) -> ExitCode {
    if let Ok(json) = serde_json::to_string(err) {
        eprintln!("{json}");
    } else {
        eprintln!(r#"{{"code":"internal_error","message":"Failed to serialize advice error."}}"#);
    }
    ExitCode::FAILURE
}

fn emit_json_and_exit<T: serde::Serialize>(value: &T) -> ExitCode {
    match serde_json::to_string_pretty(value) {
        Ok(json) => {
            println!("{json}");
            ExitCode::SUCCESS
        }
        Err(_) => emit_error_and_exit(&AdviceError::new(
            "internal_error",
            "Failed to serialize advice output.",
        )),
    }
}

fn build_scope(cli: &AdviceCli) -> Result<AdviceScope, AdviceError> {
    let registry_dir = cli.registry_dir.clone().ok_or_else(|| {
        AdviceError::new("invalid_input", "Missing required --registry-dir argument.")
    })?;
    let cache_dir = cli.cache_dir.clone().ok_or_else(|| {
        AdviceError::new("invalid_input", "Missing required --cache-dir argument.")
    })?;
    let project_root = cli.project_root.clone().ok_or_else(|| {
        AdviceError::new("invalid_input", "Missing required --project-root argument.")
    })?;
    let context_id = cli.context_id.clone().ok_or_else(|| {
        AdviceError::new("invalid_input", "Missing required --context-id argument.")
    })?;

    Ok(AdviceScope {
        registry_dir,
        cache_dir,
        project_root,
        context_id,
        cache_ttl_seconds: cli.cache_ttl_seconds.unwrap_or(DEFAULT_CACHE_TTL_SECONDS),
    })
}

fn has_duplicate_cli_flags() -> bool {
    const SINGLETON_FLAGS: &[&str] = &[
        "--registry-dir",
        "--cache-dir",
        "--project-root",
        "--context-id",
        "--cache-ttl-seconds",
        "--request",
        "--tool-id",
        "--refresh",
    ];
    let mut seen: Vec<&'static str> = Vec::new();
    for raw_arg in std::env::args_os().skip(1) {
        let Some(arg) = raw_arg.to_str() else {
            return true;
        };
        if arg == "--" {
            break;
        }
        for &flag in SINGLETON_FLAGS {
            let matches_flag = arg == flag
                || (arg.starts_with(flag) && arg.as_bytes().get(flag.len()) == Some(&b'='));
            if matches_flag {
                if seen.contains(&flag) {
                    return true;
                }
                seen.push(flag);
            }
        }
    }
    false
}

fn main() -> ExitCode {
    let cli = match AdviceCli::try_parse() {
        Ok(cli) => cli,
        Err(err) => match err.kind() {
            ErrorKind::DisplayHelp | ErrorKind::DisplayVersion => {
                print!("{err}");
                return ExitCode::SUCCESS;
            }
            _ => {
                let sanitized = AdviceError::new(
                    "invalid_input",
                    "Invalid or incomplete command-line arguments.",
                );
                return emit_error_and_exit(&sanitized);
            }
        },
    };

    if has_duplicate_cli_flags() {
        return emit_error_and_exit(&AdviceError::new(
            "invalid_input",
            "Duplicate command-line flags are not permitted.",
        ));
    }

    let scope = match build_scope(&cli) {
        Ok(scope) => scope,
        Err(err) => return emit_error_and_exit(&err),
    };

    match cli.command {
        AdviceCommand::Preview { request } => match preview_advice(&scope, &request) {
            Ok(output) => emit_json_and_exit(&output),
            Err(err) => emit_error_and_exit(&err),
        },
        AdviceCommand::Models { tool_id } => {
            let runtime = match tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
            {
                Ok(rt) => rt,
                Err(_) => {
                    return emit_error_and_exit(&AdviceError::new(
                        "runtime_init_failed",
                        "Failed to initialize async runtime.",
                    ));
                }
            };
            let vault: Arc<dyn CredentialStore> = Arc::new(KeyringCredentialStore::new());
            let client = match DecisionClient::new(vault) {
                Ok(client) => client,
                Err(err) => return emit_error_and_exit(&AdviceError::from(err)),
            };
            match runtime.block_on(list_advice_models(&scope, &tool_id, &client)) {
                Ok(output) => emit_json_and_exit(&output),
                Err(err) => emit_error_and_exit(&err),
            }
        }
        AdviceCommand::Classify { request, refresh } => {
            let runtime = match tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
            {
                Ok(rt) => rt,
                Err(_) => {
                    return emit_error_and_exit(&AdviceError::new(
                        "runtime_init_failed",
                        "Failed to initialize async runtime.",
                    ));
                }
            };
            let vault: Arc<dyn CredentialStore> = Arc::new(KeyringCredentialStore::new());
            let client = match DecisionClient::new(Arc::clone(&vault)) {
                Ok(client) => client,
                Err(err) => return emit_error_and_exit(&AdviceError::from(err)),
            };
            match runtime.block_on(classify_advice(&scope, &request, refresh, vault, &client)) {
                Ok(output) => emit_json_and_exit(&output),
                Err(err) => emit_error_and_exit(&err),
            }
        }
    }
}
