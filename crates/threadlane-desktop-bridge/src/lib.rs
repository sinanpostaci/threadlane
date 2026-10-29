#![forbid(unsafe_code)]
#![deny(missing_docs)]

//! `threadlane-desktop-bridge`: pure Rust typed observer and local deterministic demo bridge
//! for the Threadlane Milestone 3A desktop panel.
//!
//! This crate is intentionally independent of Tauri and GTK so that all runtime observation,
//! binding scoping, worker-to-demo fallback enrichment, local demo lifecycle mutations,
//! error redaction, and `PATH` tool discovery can be tested in root Cargo workspace tests
//! without a GUI display server.

mod bridge;
mod error;
mod tools;
mod types;

pub use bridge::{
    DesktopBridge, DEMO_ANSWER, DEMO_BINDING_LABEL, DEMO_CANCEL_REASON, DEMO_INSTRUCTION,
    MAX_BINDINGS, MAX_RECENT_TASKS,
};
pub use error::BridgeError;
pub use tools::{discover_tool_status, discover_tool_status_from_path};
pub use types::{
    Attachment, AttachmentMode, BindingSummary, ConnectionState, DemoControls, DesktopSnapshot,
    RunDetail, RunEventCounts, RunQuestion, TaskKind, TaskSummary, ToolAvailability, ToolStatus,
    WindowAction,
};
