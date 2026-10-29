//! `threadlane-native-mcp`: unbound paired-MCP stdio bootstrap server for native Codex threads.
//!
//! Starts in the `Unbound` state with a fresh CSPRNG `connection_nonce` and `epoch_id`,
//! exposing only `connection_identity` until the trusted local operator queries the nonce
//! via thread-scoped `mcpServer/tool/call` and enrolls the connection in [`PairingStore`].
//! After verified enrollment, delegates scoped tool calls to [`threadlane::ThreadlaneMcpServer`].

#![deny(unsafe_code)]
#![deny(missing_docs)]

use std::path::PathBuf;
use std::process::ExitCode;

use clap::Parser;
use threadlane_native::{open_workflow_stores, PairedMcpServer, PairingStore};

/// Command-line arguments for `threadlane-native-mcp`.
#[derive(Debug, Parser)]
#[command(
    name = "threadlane-native-mcp",
    about = "Unbound connection-epoch paired MCP stdio server for Threadlane native Codex integration"
)]
struct Cli {
    /// Path to the private Threadlane supervisor state directory (`supervisor.json`).
    #[arg(long, value_name = "DIR")]
    state_dir: PathBuf,

    /// Path to the private (`0700`) native MCP pairing store directory.
    #[arg(long, value_name = "DIR")]
    pairing_dir: PathBuf,

    /// Optional path to the private (`0700`) global workflow registry directory (`registry.json`).
    /// Must be provided together with `--plans-dir`.
    #[arg(long, value_name = "DIR", requires = "plans_dir")]
    registry_dir: Option<PathBuf>,

    /// Optional path to the private (`0700`) versioned workflow plan store directory (`plans.json`).
    /// Must be provided together with `--registry-dir`.
    #[arg(long, value_name = "DIR", requires = "registry_dir")]
    plans_dir: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();

    let workflow_stores = match open_workflow_stores(
        cli.registry_dir.as_deref(),
        cli.plans_dir.as_deref(),
    ) {
        Ok(stores) => stores,
        Err(err) => {
            eprintln!(
                "threadlane-native-mcp error [{}]: {}",
                err.code(),
                err.message()
            );
            return ExitCode::from(1);
        }
    };

    let pairing_store = match PairingStore::open(&cli.pairing_dir) {
        Ok(store) => store,
        Err(err) => {
            eprintln!(
                "threadlane-native-mcp error [{}]: {}",
                err.code(),
                err.message()
            );
            return ExitCode::from(1);
        }
    };

    let mut server = match PairedMcpServer::new(&cli.state_dir, pairing_store) {
        Ok(server) => server,
        Err(err) => {
            eprintln!(
                "threadlane-native-mcp error [{}]: {}",
                err.code(),
                err.message()
            );
            return ExitCode::from(1);
        }
    };

    if let Some((registry, plans)) = workflow_stores {
        server = server.with_workflow_stores(registry, plans);
    }

    if let Err(err) = server.run_stdio().await {
        eprintln!(
            "threadlane-native-mcp error [{}]: {}",
            err.code(),
            err.message()
        );
        return ExitCode::from(1);
    }

    ExitCode::SUCCESS
}
