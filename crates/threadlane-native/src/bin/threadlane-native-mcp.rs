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
use threadlane_native::{PairedMcpServer, PairingStore};

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
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();

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

    let server = match PairedMcpServer::new(&cli.state_dir, pairing_store) {
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
