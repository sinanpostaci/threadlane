use std::io::Write;
use std::path::PathBuf;

use clap::{Args, Parser, Subcommand};

use crate::error::HostError;
use crate::host::{ApprovedHost, ApprovedHostConfig};

/// Command-line interface for the `threadlane-host` per-approval local execution host.
#[derive(Debug, Clone, Parser)]
#[command(
    name = "threadlane-host",
    about = "Separately owned per-approval local execution host for Threadlane Milestone 4"
)]
pub struct HostCli {
    /// Subcommand to execute.
    #[command(subcommand)]
    pub command: HostCommand,
}

/// Supported `threadlane-host` CLI subcommands.
#[derive(Debug, Clone, Subcommand)]
pub enum HostCommand {
    /// Starts and hosts the local execution session for a persisted operator approval until
    /// explicit Ctrl-C (`SIGINT`) or `SIGTERM`.
    Serve(ServeArgs),
}

/// Explicit required store and approval arguments for `threadlane-host serve`.
///
/// Intentionally accepts no arbitrary shell arguments, config files, or model overrides.
#[derive(Debug, Clone, Args, PartialEq, Eq)]
pub struct ServeArgs {
    /// Global operator tool registry directory (`RegistryStore`).
    #[arg(long = "registry-dir")]
    pub registry_dir: PathBuf,

    /// Global operator plan store directory (`PlanStore`).
    #[arg(long = "plans-dir")]
    pub plans_dir: PathBuf,

    /// Global operator workflow authority directory (`AuthorityStore`).
    #[arg(long = "authority-dir")]
    pub authority_dir: PathBuf,

    /// Private root directory under which per-approval host session directories live.
    #[arg(long = "sessions-dir")]
    pub sessions_dir: PathBuf,

    /// Deterministic persisted approval identifier (`apr_<32hex>`) to host.
    #[arg(long = "approval-id")]
    pub approval_id: String,
}

/// Executes a parsed [`HostCli`] command on the current Tokio runtime.
pub async fn run_cli(cli: HostCli) -> Result<(), HostError> {
    match cli.command {
        HostCommand::Serve(args) => run_serve(args).await,
    }
}

/// Runs the `serve` command: starts the [`ApprovedHost`] for `args.approval_id`, emits a safe
/// token-free [`crate::HostSessionSummary`] JSON line (ignoring broken pipes if stdio is closed),
/// and keeps the host alive until explicit `SIGINT`/`SIGTERM` or a fatal supervisor error.
pub async fn run_serve(args: ServeArgs) -> Result<(), HostError> {
    let config = ApprovedHostConfig::new(
        args.registry_dir,
        args.plans_dir,
        args.authority_dir,
        args.sessions_dir,
        args.approval_id,
    );

    let handle = ApprovedHost::start(config).await?;
    let summary = handle.summary();
    if let Ok(line) = serde_json::to_string(&summary) {
        let mut stdout = std::io::stdout().lock();
        let _ = writeln!(stdout, "{line}");
        let _ = stdout.flush();
    }

    tokio::select! {
        _ = wait_for_shutdown_signal() => {
            handle.shutdown().await
        }
        fatal = handle.wait_for_fatal() => {
            let _ = handle.shutdown().await;
            Err(fatal.into())
        }
    }
}

async fn wait_for_shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        match signal(SignalKind::terminate()) {
            Ok(mut sigterm) => {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {}
                    _ = sigterm.recv() => {}
                }
            }
            Err(_) => {
                let _ = tokio::signal::ctrl_c().await;
            }
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}
