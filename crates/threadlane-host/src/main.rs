#![forbid(unsafe_code)]
#![deny(missing_docs)]

//! Binary entrypoint for the `threadlane-host` per-approval local execution host.

use clap::Parser;
use serde::Serialize;
use threadlane_host::{run_cli, HostCli};

#[derive(Serialize)]
struct CliErrorEnvelope<'a> {
    code: &'a str,
    message: String,
}

#[tokio::main]
async fn main() {
    let cli = HostCli::parse();
    if let Err(err) = run_cli(cli).await {
        let envelope = CliErrorEnvelope {
            code: err.code(),
            message: err.to_string(),
        };
        if let Ok(json) = serde_json::to_string(&envelope) {
            eprintln!("{json}");
        } else {
            eprintln!("[{}] {err}", err.code());
        }
        std::process::exit(1);
    }
}
