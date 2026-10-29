#![forbid(unsafe_code)]
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use std::path::PathBuf;
use std::process::ExitCode;

use clap::Parser;
use tauri::{State, Window};
use threadlane_desktop_bridge::{
    DesktopBridge, DesktopSnapshot, RunDetail, ToolStatus, WindowAction,
};

/// Native Tauri 2 desktop control panel for Threadlane Milestone 3A.
#[derive(Debug, Parser)]
#[command(
    name = "threadlane-desktop",
    version,
    about = "Threadlane Milestone 3A native desktop control panel"
)]
struct DesktopCli {
    /// Optional path to an existing Threadlane runtime state directory to observe.
    #[arg(long = "state-dir")]
    state_dir: Option<PathBuf>,

    /// Optional path to a dedicated owned local demo runtime state directory.
    #[arg(long = "demo-state-dir")]
    demo_state_dir: Option<PathBuf>,
}

#[tauri::command]
async fn desktop_snapshot(
    bridge: State<'_, DesktopBridge>,
    binding_id: Option<String>,
) -> Result<DesktopSnapshot, String> {
    bridge
        .snapshot(binding_id)
        .await
        .map_err(|err| err.to_string())
}

#[tauri::command]
async fn desktop_attach(
    bridge: State<'_, DesktopBridge>,
    state_dir: String,
) -> Result<DesktopSnapshot, String> {
    bridge
        .attach(state_dir)
        .await
        .map_err(|err| err.to_string())
}

#[tauri::command]
async fn desktop_run_detail(
    bridge: State<'_, DesktopBridge>,
    binding_id: String,
    run_id: String,
) -> Result<RunDetail, String> {
    bridge
        .run_detail(binding_id, run_id)
        .await
        .map_err(|err| err.to_string())
}

#[tauri::command]
async fn desktop_start_demo(bridge: State<'_, DesktopBridge>) -> Result<DesktopSnapshot, String> {
    bridge.start_demo().await.map_err(|err| err.to_string())
}

#[tauri::command]
async fn desktop_answer_demo(
    bridge: State<'_, DesktopBridge>,
    expected_revision: u64,
) -> Result<DesktopSnapshot, String> {
    bridge
        .answer_demo(expected_revision)
        .await
        .map_err(|err| err.to_string())
}

#[tauri::command]
async fn desktop_cancel_demo(
    bridge: State<'_, DesktopBridge>,
    expected_revision: u64,
) -> Result<DesktopSnapshot, String> {
    bridge
        .cancel_demo(expected_revision)
        .await
        .map_err(|err| err.to_string())
}

#[tauri::command]
fn desktop_tool_status(bridge: State<'_, DesktopBridge>) -> Result<Vec<ToolStatus>, String> {
    Ok(bridge.tool_status())
}

#[tauri::command]
fn desktop_window_action(window: Window, action: WindowAction) -> Result<(), String> {
    match action {
        WindowAction::Close => window
            .close()
            .map_err(|_| "Unable to close desktop window.".to_string()),
        WindowAction::Minimize => window
            .minimize()
            .map_err(|_| "Unable to minimize desktop window.".to_string()),
    }
}

fn main() -> ExitCode {
    let cli = DesktopCli::parse();
    let bridge = match DesktopBridge::new(cli.state_dir, cli.demo_state_dir) {
        Ok(bridge) => bridge,
        Err(err) => {
            eprintln!("threadlane-desktop error: {err}");
            return ExitCode::FAILURE;
        }
    };

    let result = tauri::Builder::default()
        .manage(bridge)
        .invoke_handler(tauri::generate_handler![
            desktop_snapshot,
            desktop_attach,
            desktop_run_detail,
            desktop_start_demo,
            desktop_answer_demo,
            desktop_cancel_demo,
            desktop_tool_status,
            desktop_window_action,
        ])
        .run(tauri::generate_context!());

    if result.is_err() {
        eprintln!("threadlane-desktop error: failed to run desktop application.");
        return ExitCode::FAILURE;
    }

    ExitCode::SUCCESS
}
