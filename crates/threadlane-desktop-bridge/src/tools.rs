use std::ffi::OsStr;
use std::fs;
use std::path::Path;

use crate::types::{ToolAvailability, ToolStatus};

struct ToolProbeSpec {
    id: &'static str,
    label: &'static str,
    binaries: &'static [&'static str],
    found_note: &'static str,
    missing_note: &'static str,
}

const TOOL_PROBES: &[ToolProbeSpec] = &[
    ToolProbeSpec {
        id: "codex",
        label: "Codex CLI",
        binaries: &["codex"],
        found_note: "Executable found on PATH; runtime version and login state are not probed.",
        missing_note: "No codex executable found on PATH.",
    },
    ToolProbeSpec {
        id: "opencode",
        label: "OpenCode CLI",
        binaries: &["opencode"],
        found_note: "Executable found on PATH; provider credentials and models are not probed.",
        missing_note: "No opencode executable found on PATH.",
    },
    ToolProbeSpec {
        id: "antigravity",
        label: "Antigravity CLI",
        binaries: &["agy", "antigravity"],
        found_note:
            "Executable found on PATH; session and effort evidence are verified per worker run.",
        missing_note: "No agy or antigravity executable found on PATH.",
    },
    ToolProbeSpec {
        id: "git",
        label: "Git",
        binaries: &["git"],
        found_note: "Executable found on PATH for worktree and post-run diff-gate inspection.",
        missing_note: "No git executable found on PATH.",
    },
];

/// Discovers local tool executables passively from the process `PATH` environment variable
/// without spawning any subprocesses or making network calls.
pub fn discover_tool_status() -> Vec<ToolStatus> {
    let path_var = std::env::var_os("PATH");
    discover_tool_status_from_path(path_var.as_deref())
}

/// Discovers local tool executables passively from an explicit `PATH` value without
/// spawning any subprocesses or making network calls.
pub fn discover_tool_status_from_path(path_var: Option<&OsStr>) -> Vec<ToolStatus> {
    let mut statuses = Vec::with_capacity(TOOL_PROBES.len() + 1);

    for spec in TOOL_PROBES {
        match find_executable_in_path(path_var, spec.binaries) {
            Some(exec_path) => statuses.push(ToolStatus {
                id: spec.id.to_string(),
                label: spec.label.to_string(),
                executable_path: Some(exec_path),
                availability: ToolAvailability::Found,
                note: spec.found_note.to_string(),
            }),
            None => statuses.push(ToolStatus {
                id: spec.id.to_string(),
                label: spec.label.to_string(),
                executable_path: None,
                availability: ToolAvailability::NotFound,
                note: spec.missing_note.to_string(),
            }),
        }
    }

    statuses.push(ToolStatus {
        id: "jev".to_string(),
        label: "Jev".to_string(),
        executable_path: None,
        availability: ToolAvailability::NotConfigured,
        note: "Jev shadow-mode API integration is planned for Milestone 4 and is not configured in M3A."
            .to_string(),
    });

    statuses
}

fn find_executable_in_path(path_var: Option<&OsStr>, binaries: &[&str]) -> Option<String> {
    let raw_path = path_var?;
    for dir in std::env::split_paths(raw_path) {
        if dir.as_os_str().is_empty() {
            continue;
        }
        for bin in binaries {
            let candidate = dir.join(bin);
            if is_executable_regular_file(&candidate) {
                return Some(candidate.to_string_lossy().into_owned());
            }
        }
    }
    None
}

fn is_executable_regular_file(path: &Path) -> bool {
    let Ok(meta) = fs::metadata(path) else {
        return false;
    };
    if !meta.is_file() {
        return false;
    }

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        (meta.permissions().mode() & 0o111) != 0
    }

    #[cfg(not(unix))]
    {
        true
    }
}
