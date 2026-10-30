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

    /// Optional expected prior stopped host instance identifier (`hst_<32hex>`) to restart cleanly via `ApprovedHost::restart_stopped`.
    #[arg(long = "restart-from-instance-id")]
    pub restart_from_instance_id: Option<String>,

    /// Require a brand-new host session with no prior manifest, receipts, runtime state, or
    /// work area (mutually exclusive with `--restart-from-instance-id`).
    #[arg(
        long = "start-fresh-only",
        default_value_t = false,
        conflicts_with = "restart_from_instance_id"
    )]
    pub start_fresh_only: bool,
}

impl ServeArgs {
    /// Validates CLI arguments (including optional notification flags) and constructs an [`ApprovedHostConfig`].
    pub fn into_host_config(self) -> Result<ApprovedHostConfig, HostError> {
        if self.start_fresh_only && self.restart_from_instance_id.is_some() {
            return Err(HostError::validation(
                "start_fresh_only",
                "`--start-fresh-only` cannot be combined with `--restart-from-instance-id`",
            ));
        }

        if let Some(ref prior_id) = self.restart_from_instance_id {
            crate::storage::validate_host_instance_id("restart_from_instance_id", prior_id)?;
        }

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

/// Runs the `serve` command: starts (or restarts from a stopped instance) the [`ApprovedHost`] for
/// `args.approval_id`, emits a safe token-free [`crate::HostSessionSummary`] JSON line (ignoring
/// broken pipes if stdio is closed), and keeps the host alive until explicit `SIGINT`/`SIGTERM`,
/// an authenticated idle-stop request, or a fatal supervisor error.
pub async fn run_serve(args: ServeArgs) -> Result<(), HostError> {
    let start_fresh_only = args.start_fresh_only;
    let restart_from_instance_id = args.restart_from_instance_id.clone();
    let config = args.into_host_config()?;

    let handle = match (start_fresh_only, restart_from_instance_id.as_deref()) {
        (true, Some(_)) => {
            return Err(HostError::validation(
                "start_fresh_only",
                "`--start-fresh-only` cannot be combined with `--restart-from-instance-id`",
            ));
        }
        (true, None) => ApprovedHost::start_fresh_only(config).await?,
        (false, Some(expected_prior_instance_id)) => {
            ApprovedHost::restart_stopped(config, expected_prior_instance_id).await?
        }
        (false, None) => ApprovedHost::start(config).await?,
    };
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
        _ = handle.wait_for_stop_request() => {
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
