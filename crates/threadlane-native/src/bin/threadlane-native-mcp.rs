//! `threadlane-native-mcp`: unbound paired-MCP stdio bootstrap server for native Codex threads.
//!
//! Starts in the `Unbound` state with a fresh CSPRNG `connection_nonce` and `epoch_id`,
//! exposing diagnostics and `connect_current_chat` for verified managed Linux desktop pairing.
//! Other hosts require a trusted local operator to query the thread-scoped connection and enroll it.
//! After verified enrollment, delegates scoped tool calls to [`threadlane::ThreadlaneMcpServer`].
//!
//! Auto-starting initial approved hosts is **disabled by default** (`--auto-start-approved-hosts`
//! is `false`); operators must explicitly pass `--auto-start-approved-hosts` together with
//! `--registry-dir`, `--plans-dir`, `--authority-dir`, and `--sessions-dir` (and optional
//! `--host-executable <ABS>`) to opt in.

#![deny(unsafe_code)]
#![deny(missing_docs)]

use std::process::ExitCode;

use clap::Parser;
use threadlane_native::NativeMcpCli;

#[tokio::main]
async fn main() -> ExitCode {
    let cli = NativeMcpCli::parse();

    let server = match cli.into_server() {
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
