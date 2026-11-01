#![forbid(unsafe_code)]
#![deny(missing_docs)]

//! Binary entrypoint for `threadlane-codex-bridge`.

fn main() {
    std::process::exit(threadlane_codex_bridge::run_from_env());
}
