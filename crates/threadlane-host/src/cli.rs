use std::io::Write;
use std::path::PathBuf;

use clap::{Args, Parser, Subcommand};
use threadlane::DEFAULT_CODEX_TIMEOUT_MS;

use crate::error::HostError;
use crate::host::{ApprovedHost, ApprovedHostConfig};
use crate::types::ApprovedHostNotificationConfig;

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

    /// Optional absolute path to the Codex executable for approved-session wake-up queueing (OFF by default).
    #[arg(long = "notify-codex")]
    pub notify_codex: Option<PathBuf>,

    /// Optional bounded timeout in milliseconds for each Codex queue invocation (valid only with `--notify-codex`).
    #[arg(long = "notify-timeout-ms")]
    pub notify_timeout_ms: Option<u64>,
}

impl ServeArgs {
    /// Validates CLI arguments (including optional notification flags) and constructs an [`ApprovedHostConfig`].
    pub fn into_host_config(self) -> Result<ApprovedHostConfig, HostError> {
        let mut config = ApprovedHostConfig::new(
            self.registry_dir,
            self.plans_dir,
            self.authority_dir,
            self.sessions_dir,
            self.approval_id,
        );

        match (self.notify_codex, self.notify_timeout_ms) {
            (None, None) => {}
            (None, Some(_)) => {
                return Err(HostError::validation(
                    "notify_timeout_ms",
                    "`--notify-timeout-ms` cannot be used without `--notify-codex <ABS_EXEC>`",
                ));
            }
            (Some(executable), timeout_opt) => {
                let timeout_ms = timeout_opt.unwrap_or(DEFAULT_CODEX_TIMEOUT_MS);
                let notify_cfg = ApprovedHostNotificationConfig::new(executable, timeout_ms)?;
                config = config.with_notifications(notify_cfg);
            }
        }

        Ok(config)
    }
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
    let config = args.into_host_config()?;

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
