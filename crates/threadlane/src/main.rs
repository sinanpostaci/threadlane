#![forbid(unsafe_code)]

use std::process::ExitCode;

use clap::Parser;
use threadlane::cli::{run_cli, Cli};

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    match run_cli(cli).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            let api_err = err.to_api_error();
            if let Ok(serialized) = serde_json::to_string_pretty(&api_err) {
                eprintln!("{serialized}");
            } else {
                eprintln!("threadlane error: {err}");
            }
            ExitCode::FAILURE
        }
    }
}
