use std::collections::BTreeSet;
use std::path::PathBuf;
use std::time::Duration;

use clap::{Parser, Subcommand};

use crate::client::{ScopedClient, SupervisorClient};
use crate::codex::{CodexWakeupConfig, DEFAULT_CODEX_TIMEOUT_MS};
use crate::error::Error;
use crate::mcp::{ThreadlaneMcpServer, OPERATING_GUIDE_MARKDOWN};
use crate::protocol::ScopedRequest;
use crate::supervisor::{Supervisor, SupervisorConfig, DEFAULT_TICK_INTERVAL_MS};
use crate::workers::WorkersConfig;

/// Threadlane Milestone 1 local execution supervisor, CLI, and scoped MCP adapter.
#[derive(Debug, Parser)]
#[command(
    name = "threadlane",
    version,
    about = "Threadlane Milestone 1 local supervisor, CLI, and scoped stdio MCP server"
)]
pub struct Cli {
    /// Subcommand to execute.
    #[command(subcommand)]
    pub command: Subcommands,
}

/// Supported CLI subcommands.
#[derive(Debug, Subcommand)]
pub enum Subcommands {
    /// Run the single-owner local supervisor (owns SQLite state, demo ticks, and optional Codex wake-ups).
    Serve {
        /// Private state directory path (created/enforced as 0700 on Unix).
        #[arg(long)]
        state_dir: PathBuf,

        /// Optional absolute path to the Codex executable for wake-up queueing (OFF by default).
        #[arg(long)]
        codex: Option<PathBuf>,

        /// Explicit allowlist of binding IDs eligible for Codex wake-up notifications (repeatable).
        #[arg(long = "notify-binding")]
        notify_bindings: Vec<String>,

        /// Background demo tick interval in milliseconds.
        #[arg(long, default_value_t = DEFAULT_TICK_INTERVAL_MS)]
        tick_interval_ms: u64,

        /// Bounded timeout in milliseconds for each Codex queue invocation.
        #[arg(long, default_value_t = DEFAULT_CODEX_TIMEOUT_MS)]
        codex_timeout_ms: u64,

        /// Optional path to a private (0600) startup worker profile configuration JSON file (OFF by default).
        #[arg(long = "workers-config")]
        workers_config: Option<PathBuf>,
    },

    /// Create a trusted binding between a validated Codex thread UUID and a display label.
    Bind {
        /// Private state directory path where the supervisor is running.
        #[arg(long)]
        state_dir: PathBuf,

        /// Canonical hyphenated non-nil Codex thread UUID.
        #[arg(long)]
        thread: String,

        /// Human-readable display label for the bound chat.
        #[arg(long)]
        label: String,
    },

    /// List all configured bindings in creation order (trusted CLI only).
    ListBindings {
        /// Private state directory path where the supervisor is running.
        #[arg(long)]
        state_dir: PathBuf,
    },

    /// Inspect compact status (binding metadata, wake-up state, and recent run summaries) for a binding.
    Status {
        /// Private state directory path where the supervisor is running.
        #[arg(long)]
        state_dir: PathBuf,

        /// Target binding identifier (`bnd_<uuid>`).
        #[arg(long)]
        binding: String,
    },

    /// Execute a binding-scoped operation from a JSON request payload.
    Call {
        /// Private state directory path where the supervisor is running.
        #[arg(long)]
        state_dir: PathBuf,

        /// Target binding identifier (`bnd_<uuid>`).
        #[arg(long)]
        binding: String,

        /// Scoped request JSON (for example `{"op":"start_demo","command_key":"k1","instruction":"..."}`).
        #[arg(long)]
        request: String,
    },

    /// Manually re-arm a failed (or explicitly unknown) wake-up claim for a binding (trusted CLI only).
    RetryWakeup {
        /// Private state directory path where the supervisor is running.
        #[arg(long)]
        state_dir: PathBuf,

        /// Target binding identifier (`bnd_<uuid>`).
        #[arg(long)]
        binding: String,

        /// Expected active claim identifier (`wcl_<uuid>`) to re-arm.
        #[arg(long = "expected-claim")]
        expected_claim: String,

        /// Explicitly allow retrying a claim in `unknown` status (may cause duplicate wake-up delivery).
        #[arg(long)]
        allow_unknown: bool,
    },

    /// Run the stdio MCP server scoped to a single binding.
    Mcp {
        /// Private state directory path where the supervisor is running.
        #[arg(long)]
        state_dir: PathBuf,

        /// Fixed binding identifier (`bnd_<uuid>`) for this MCP instance.
        #[arg(long)]
        binding: String,
    },

    /// Print the versioned Threadlane MCP operating guide offline without network or supervisor access.
    Guide,
}

/// Returns the canonical versioned operating guide Markdown printed by `threadlane guide`.
pub fn render_operating_guide() -> &'static str {
    OPERATING_GUIDE_MARKDOWN
}

/// Builds a validated [`SupervisorConfig`] from CLI `serve` arguments (without worker profiles).
pub fn build_supervisor_config(
    state_dir: PathBuf,
    codex: Option<PathBuf>,
    notify_bindings: Vec<String>,
    tick_interval_ms: u64,
    codex_timeout_ms: u64,
) -> Result<SupervisorConfig, Error> {
    build_supervisor_config_with_workers(
        state_dir,
        codex,
        notify_bindings,
        tick_interval_ms,
        codex_timeout_ms,
        None,
    )
}

/// Builds a validated [`SupervisorConfig`] from CLI `serve` arguments including optional
/// private `--workers-config` startup worker profiles.
pub fn build_supervisor_config_with_workers(
    state_dir: PathBuf,
    codex: Option<PathBuf>,
    notify_bindings: Vec<String>,
    tick_interval_ms: u64,
    codex_timeout_ms: u64,
    workers_config_path: Option<PathBuf>,
) -> Result<SupervisorConfig, Error> {
    let codex_config = match (codex, notify_bindings.is_empty()) {
        (None, true) => None,
        (None, false) => {
            return Err(Error::InvalidConfig(
                "`--notify-binding` cannot be used without `--codex <ABS_EXEC>`".to_string(),
            ));
        }
        (Some(_), true) => {
            return Err(Error::InvalidConfig(
                "`--codex` requires at least one `--notify-binding <BINDING_ID>`".to_string(),
            ));
        }
        (Some(executable), false) => {
            let allowlist: BTreeSet<String> = notify_bindings.into_iter().collect();
            let cfg = CodexWakeupConfig {
                executable,
                notify_bindings: allowlist,
                timeout: Duration::from_millis(codex_timeout_ms),
                approved_session: None,
            };
            cfg.validate()?;
            Some(cfg)
        }
    };

    let workers = match workers_config_path {
        Some(path) => Some(WorkersConfig::load_from_path(&path)?),
        None => None,
    };

    let config = SupervisorConfig {
        state_dir,
        tick_interval: Duration::from_millis(tick_interval_ms),
        codex: codex_config,
        workers,
    };
    config.validate()?;
    Ok(config)
}

/// Executes a parsed [`Cli`] command.
pub async fn run_cli(cli: Cli) -> Result<(), Error> {
    match cli.command {
        Subcommands::Serve {
            state_dir,
            codex,
            notify_bindings,
            tick_interval_ms,
            codex_timeout_ms,
            workers_config,
        } => {
            let config = build_supervisor_config_with_workers(
                state_dir,
                codex,
                notify_bindings,
                tick_interval_ms,
                codex_timeout_ms,
                workers_config,
            )?;
            let handle = Supervisor::start(config).await?;
            tokio::select! {
                _ = wait_for_shutdown_signal() => {}
                _ = handle.wait_for_fatal() => {}
            }
            if let Some(api_err) = handle.shutdown_and_take_fatal().await {
                return Err(Error::Remote(api_err));
            }
            Ok(())
        }
        Subcommands::Bind {
            state_dir,
            thread,
            label,
        } => {
            let client = SupervisorClient::from_state_dir(&state_dir)?;
            let binding = client.create_binding(&thread, &label).await?;
            println!("{}", serde_json::to_string_pretty(&binding)?);
            Ok(())
        }
        Subcommands::ListBindings { state_dir } => {
            let client = SupervisorClient::from_state_dir(&state_dir)?;
            let bindings = client.list_bindings().await?;
            println!("{}", serde_json::to_string_pretty(&bindings)?);
            Ok(())
        }
        Subcommands::Status { state_dir, binding } => {
            let client = SupervisorClient::from_state_dir(&state_dir)?;
            let status = client.status(&binding).await?;
            println!("{}", serde_json::to_string_pretty(&status)?);
            Ok(())
        }
        Subcommands::Call {
            state_dir,
            binding,
            request,
        } => {
            let parsed_req: ScopedRequest = serde_json::from_str(&request)?;
            let client = SupervisorClient::from_state_dir(&state_dir)?;
            let response = client.execute_scoped(&binding, &parsed_req).await?;
            println!("{}", serde_json::to_string_pretty(&response)?);
            Ok(())
        }
        Subcommands::RetryWakeup {
            state_dir,
            binding,
            expected_claim,
            allow_unknown,
        } => {
            let client = SupervisorClient::from_state_dir(&state_dir)?;
            let notice = client
                .retry_wakeup(&binding, &expected_claim, allow_unknown)
                .await?;
            println!("{}", serde_json::to_string_pretty(&notice)?);
            Ok(())
        }
        Subcommands::Mcp { state_dir, binding } => {
            let scoped = ScopedClient::from_state_dir_lazy(&state_dir, binding)?;
            let server = ThreadlaneMcpServer::new(scoped);
            server.run_stdio().await
        }
        Subcommands::Guide => {
            print!("{}", render_operating_guide());
            Ok(())
        }
    }
}

async fn wait_for_shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};

        let mut sigint = signal(SignalKind::interrupt()).ok();
        let mut sigterm = signal(SignalKind::terminate()).ok();

        tokio::select! {
            _ = async {
                if let Some(ref mut s) = sigint {
                    let _ = s.recv().await;
                } else {
                    let _ = tokio::signal::ctrl_c().await;
                }
            } => {}
            _ = async {
                if let Some(ref mut s) = sigterm {
                    let _ = s.recv().await;
                } else {
                    std::future::pending::<()>().await;
                }
            } => {}
        }
    }

    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mcp::OPERATING_GUIDE_VERSION;

    #[tokio::test]
    async fn cli_guide_subcommand_parses_without_args_and_executes_offline() {
        let parsed = Cli::try_parse_from(["threadlane", "guide"])
            .expect("`threadlane guide` must parse with no arguments");
        assert!(matches!(parsed.command, Subcommands::Guide));

        assert!(
            Cli::try_parse_from(["threadlane", "guide", "--unexpected"]).is_err(),
            "`threadlane guide` must reject unexpected arguments"
        );

        let rendered = render_operating_guide();
        assert_eq!(rendered, OPERATING_GUIDE_MARKDOWN);
        assert!(rendered.contains(OPERATING_GUIDE_VERSION));

        run_cli(parsed)
            .await
            .expect("`threadlane guide` must succeed offline without supervisor or state_dir");
    }
}
