use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use clap::{Args, Parser, Subcommand};
use threadlane::{NativeToolOutputConfig, DEFAULT_CODEX_TIMEOUT_MS};
use threadlane_authority::AuthorityStore;
use threadlane_plans::PlanStore;
use threadlane_registry::RegistryStore;

use crate::client::ApprovedHostClient;
use crate::error::HostError;
use crate::host::{ApprovedHost, ApprovedHostConfig};
use crate::storage::{
    validate_approval_id, validate_hex_digest, validate_identifier, validate_sessions_root,
};
use crate::types::{
    ApprovedHostNotificationConfig, DependencyInputSummary, DependencyPredecessorSelection,
    HostNotificationRoute, PrepareDependencyInputsRequest,
};

const MAX_PREDECESSORS_FILE_BYTES: usize = 65_536;

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
    /// Verifies receipt-proven predecessor outputs and seals a dependent task's input commit
    /// on an active Ready host session.
    PrepareDependencies(PrepareDependenciesArgs),
}

/// Explicit required store, approval, task, and predecessor receipt arguments for
/// `threadlane-host prepare-dependencies`.
#[derive(Debug, Clone, Args, PartialEq, Eq)]
pub struct PrepareDependenciesArgs {
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

    /// Deterministic persisted approval identifier (`apr_<32hex>`).
    #[arg(long = "approval-id")]
    pub approval_id: String,

    /// Dependent task identifier (`!depends_on.is_empty()`).
    #[arg(long = "task-id")]
    pub task_id: String,

    /// Path to a private (`0600`) non-symlink regular JSON file (`<= 64 KiB`) containing
    /// `Vec<DependencyPredecessorSelection>`.
    #[arg(long = "predecessors")]
    pub predecessors: PathBuf,

    /// Explicit expected 64-hex approval subject digest (`--confirm-approval <SHA256>`) required
    /// to prepare and seal dependency inputs.
    #[arg(long = "confirm-approval")]
    pub confirm_approval: String,
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

    /// Optional fixed notification message route (`mcp` or `operator-cli`, valid only with `--notify-codex`).
    #[arg(long = "notify-route", value_parser = parse_notify_route)]
    pub notify_route: Option<HostNotificationRoute>,

    /// Optional path to a private (`0600`) non-symlink regular JSON file (`<= 16 KiB`) configuring
    /// native Unix WebSocket `turn/start.toolOutput` notifications (valid only with `--notify-codex`).
    #[arg(long = "notify-tool-output-context")]
    pub notify_tool_output_context: Option<PathBuf>,

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

fn parse_notify_route(raw: &str) -> Result<HostNotificationRoute, String> {
    raw.parse::<HostNotificationRoute>()
        .map_err(|err| err.to_string())
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

        match self.notify_codex {
            None => {
                if self.notify_timeout_ms.is_some() {
                    return Err(HostError::validation(
                        "notify_timeout_ms",
                        "`--notify-timeout-ms` cannot be used without `--notify-codex <ABS_EXEC>`",
                    ));
                }
                if self.notify_route.is_some() {
                    return Err(HostError::validation(
                        "notify_route",
                        "`--notify-route` cannot be used without `--notify-codex <ABS_EXEC>`",
                    ));
                }
                if self.notify_tool_output_context.is_some() {
                    return Err(HostError::validation(
                        "notify_tool_output_context",
                        "`--notify-tool-output-context` cannot be used without `--notify-codex <ABS_EXEC>`",
                    ));
                }
            }
            Some(executable) => {
                let timeout_ms = self.notify_timeout_ms.unwrap_or(DEFAULT_CODEX_TIMEOUT_MS);
                let route = self.notify_route.unwrap_or(HostNotificationRoute::Mcp);
                let mut notify_cfg =
                    ApprovedHostNotificationConfig::new(executable, timeout_ms)?.with_route(route);
                if let Some(ref context_path) = self.notify_tool_output_context {
                    let tool_output = NativeToolOutputConfig::load(context_path)?;
                    notify_cfg = notify_cfg.with_tool_output(tool_output)?;
                }
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
        HostCommand::PrepareDependencies(args) => {
            let summary = run_prepare_dependencies(args).await?;
            if let Ok(line) = serde_json::to_string(&summary) {
                let mut stdout = std::io::stdout().lock();
                let _ = writeln!(stdout, "{line}");
                let _ = stdout.flush();
            }
            Ok(())
        }
    }
}

/// Runs the `prepare-dependencies` command: validates the `--predecessors` private (`0600`)
/// JSON file, opens [`AuthorityStore`], resolves the frozen approval's authoritative
/// `(binding_id, thread_uuid)`, constructs [`ApprovedHostClient`], and invokes
/// [`ApprovedHostClient::prepare_dependency_inputs`].
pub async fn run_prepare_dependencies(
    args: PrepareDependenciesArgs,
) -> Result<DependencyInputSummary, HostError> {
    validate_approval_id(&args.approval_id)?;
    validate_identifier("task_id", &args.task_id)?;
    validate_hex_digest("confirm_approval", &args.confirm_approval)?;

    let predecessors = load_predecessors_file(&args.predecessors)?;

    let registry = Arc::new(RegistryStore::open(&args.registry_dir)?);
    let plans = Arc::new(PlanStore::open(&args.plans_dir)?);
    let authority = Arc::new(AuthorityStore::open(
        &args.authority_dir,
        Arc::clone(&registry),
        Arc::clone(&plans),
    )?);
    validate_sessions_root(
        &args.sessions_dir,
        &[
            ("registry_dir", registry.config_dir()),
            ("plans_dir", plans.dir()),
            ("authority_dir", authority.dir()),
        ],
    )?;

    let record = authority.get_for_operator(&args.approval_id)?;
    if record.subject_digest != args.confirm_approval {
        return Err(HostError::validation(
            "confirm_approval",
            format!(
                "--confirm-approval digest `{}` does not match frozen approval subject_digest `{}`",
                args.confirm_approval, record.subject_digest
            ),
        ));
    }
    let client = ApprovedHostClient::new(
        authority,
        &args.sessions_dir,
        record.binding_id,
        record.thread_uuid,
    )?;

    client
        .prepare_dependency_inputs(PrepareDependencyInputsRequest {
            approval_id: args.approval_id,
            task_id: args.task_id,
            expected_approval_digest: args.confirm_approval,
            predecessors,
        })
        .await
}

fn load_predecessors_file(path: &Path) -> Result<Vec<DependencyPredecessorSelection>, HostError> {
    if path.as_os_str().is_empty() {
        return Err(HostError::validation(
            "predecessors",
            "predecessors file path must not be empty",
        ));
    }
    if !path.is_absolute() {
        return Err(HostError::validation(
            "predecessors",
            "an absolute private JSON file is required",
        ));
    }
    let (bytes, _) = threadlane_workers::dependency::read_bounded_dependency_bytes(
        path,
        MAX_PREDECESSORS_FILE_BYTES,
    )
    .map_err(|_| HostError::SecurityViolation {
        path: "predecessors".to_string(),
        reason: "could not safely read a bounded private regular predecessors file".to_string(),
    })?;
    serde_json::from_slice(&bytes)
        .map_err(|_| HostError::validation("predecessors", "invalid predecessors JSON file"))
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

#[cfg(all(test, unix))]
mod dependency_cli_tests {
    use super::*;
    use std::fs;
    use std::os::unix::fs::{symlink, PermissionsExt};

    #[test]
    fn predecessors_errors_are_static_and_reader_rejects_unsafe_files() {
        let temp = tempfile::tempdir().expect("tempdir");
        let path = temp.path().join("predecessors.json");
        let marker = "PRIVATE_PREDECESSOR_CONTENT_MUST_NOT_BE_ECHOED";
        fs::write(&path, format!("[\"{marker}\"]")).expect("write fixture");
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).expect("private fixture");
        let error = load_predecessors_file(&path).expect_err("invalid element type");
        assert_eq!(error.code(), "validation");
        assert!(!error.to_string().contains(marker));
        assert!(!format!("{error:?}").contains(marker));
        let link = temp.path().join("linked.json");
        symlink(&path, &link).expect("symlink");
        assert_eq!(
            load_predecessors_file(&link)
                .expect_err("symlink rejected")
                .code(),
            "security_violation"
        );
        fs::write(&path, vec![b' '; MAX_PREDECESSORS_FILE_BYTES + 1]).expect("oversize fixture");
        assert_eq!(
            load_predecessors_file(&path)
                .expect_err("oversize rejected")
                .code(),
            "security_violation"
        );
        assert!(load_predecessors_file(Path::new("relative.json")).is_err());
    }
}
