use std::collections::BTreeSet;
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::error::WorkerError;
use crate::fs_safety::{validate_absolute_no_symlink_components, validate_existing_canonical_dir};
use crate::spec::{is_control_config_path, validate_repo_relative_path, WorkerProvider};

/// Documented Antigravity CLI read permission action identifier.
///
/// In Antigravity's documented action mapping, read-only file inspection tools map to `read_file`.
pub const AGY_READ_PERMISSION_TOOL: &str = "read_file";

/// Documented Antigravity CLI file-edit and file-write permission action identifiers.
pub const AGY_WRITE_PERMISSION_TOOLS: [&str; 5] = [
    "edit_file",
    "write_file",
    "write_to_file",
    "replace_file_content",
    "multi_replace_file_content",
];

/// Trusted provider file-permission scoping policy for a worker invocation.
///
/// Provider permission flags are session-only CLI hints; they are **not** an OS-level sandbox,
/// do **not** replace the mandatory post-run Git diff gate, and do **not** guarantee global
/// deny isolation if user-inherited provider settings grant broader rules.
#[derive(
    Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum WorkerFilePermissions {
    /// Preserve the default provider invocation without injecting `--settings`.
    #[default]
    Inherit,
    /// Pass a session-only `--settings <inline-json>` argument to Antigravity granting
    /// `read_file(<canonical_workdir>/**)` and exact plus descendant write/edit rules only for
    /// `allowed_write_paths`.
    TaskFiles,
}

impl WorkerFilePermissions {
    /// Returns the canonical `snake_case` representation (`"inherit"` or `"task_files"`).
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Inherit => "inherit",
            Self::TaskFiles => "task_files",
        }
    }

    /// Returns `true` when constructing the final launch argv requires the canonical task
    /// working directory (`TaskFiles`).
    pub fn requires_workdir_argv(self) -> bool {
        matches!(self, Self::TaskFiles)
    }

    /// Validates that this permission mode is supported for `provider` and that all
    /// `allowed_write_paths` entries are valid and free of unsafe glob/rule metacharacters.
    pub fn validate_for_provider_and_paths(
        self,
        provider: WorkerProvider,
        allowed_write_paths: &BTreeSet<String>,
    ) -> Result<(), WorkerError> {
        if self == Self::TaskFiles && provider != WorkerProvider::Antigravity {
            return Err(WorkerError::InvalidSpec(
                "`file_permissions: task_files` is only supported for provider `antigravity`"
                    .to_string(),
            ));
        }

        if allowed_write_paths.is_empty() {
            return Err(WorkerError::InvalidSpec(
                "allowed_write_paths must contain at least one repository-relative path"
                    .to_string(),
            ));
        }

        for path_entry in allowed_write_paths {
            validate_repo_relative_path(path_entry, "allowed_write_paths entry")?;
            if is_control_config_path(path_entry) {
                return Err(WorkerError::InvalidSpec(format!(
                    "allowed_write_paths entry `{path_entry}` targets protected repository or agent control configuration"
                )));
            }
            if self == Self::TaskFiles {
                validate_permission_pattern_str(path_entry, "allowed_write_paths entry")?;
            }
        }

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct AntigravitySessionSettings {
    permissions: AntigravityPermissionsBlock,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct AntigravityPermissionsBlock {
    allow: Vec<String>,
}

/// Returns `true` if `ch` is an ambiguous glob/rule metacharacter (`*`, `?`, `[`, `]`,
/// `{`, `}`, `(`, `)`, `\`) or a control character that must not be interpolated into a
/// provider permission pattern.
pub fn is_unsafe_permission_pattern_char(ch: char) -> bool {
    ch.is_control() || matches!(ch, '*' | '?' | '[' | ']' | '{' | '}' | '(' | ')' | '\\')
}

/// Validates that `value` is non-empty and contains no ambiguous glob/rule metacharacters
/// (`*`, `?`, `[`, `]`, `{`, `}`, `(`, `)`, `\`) or control characters.
pub fn validate_permission_pattern_str(value: &str, label: &str) -> Result<(), WorkerError> {
    if value.is_empty() {
        return Err(WorkerError::InvalidSpec(format!(
            "{label} must not be empty when `file_permissions` is `task_files`"
        )));
    }
    if let Some(bad_ch) = value
        .chars()
        .find(|&ch| is_unsafe_permission_pattern_char(ch))
    {
        return Err(WorkerError::InvalidSpec(format!(
            "{label} `{value}` contains unsafe glob or permission-rule metacharacter {:?} which is not permitted with `file_permissions: task_files`",
            bad_ch
        )));
    }
    Ok(())
}

/// Validates that `path` is valid UTF-8 and contains no ambiguous glob/rule metacharacters
/// (`*`, `?`, `[`, `]`, `{`, `}`, `(`, `)`, `\`) or control characters.
pub fn validate_permission_pattern_path(path: &Path, label: &str) -> Result<String, WorkerError> {
    let path_str = path.to_str().ok_or_else(|| {
        WorkerError::InvalidSpec(format!(
            "{label} `{}` is not valid UTF-8 and cannot be used with `file_permissions: task_files`",
            path.display()
        ))
    })?;
    validate_permission_pattern_str(path_str, label)?;
    Ok(path_str.to_string())
}

/// Builds the session-only Antigravity `--settings <inline-json>` payload for
/// [`WorkerFilePermissions::TaskFiles`] from an existing canonical task working directory and
/// validated `allowed_write_paths`.
///
/// Grants:
/// - `read_file(<canonical_workdir>/**)`
/// - Exact (`<tool>(<canonical_workdir>/<entry>)`) and descendant
///   (`<tool>(<canonical_workdir>/<entry>/**)`) rules for `edit_file`, `write_file`,
///   `write_to_file`, `replace_file_content`, and `multi_replace_file_content` for each entry in
///   `allowed_write_paths`.
///
/// Never includes shell, network, or MCP tool grants, and never modifies global provider settings.
pub fn build_antigravity_task_files_settings_json(
    working_directory: &Path,
    allowed_write_paths: &BTreeSet<String>,
) -> Result<String, WorkerError> {
    let canonical_workdir =
        validate_existing_canonical_dir(working_directory, "working_directory")?;
    if canonical_workdir.parent().is_none() {
        return Err(WorkerError::InvalidSpec(
            "refusing to grant `task_files` permissions on filesystem root directory `/`"
                .to_string(),
        ));
    }

    let workdir_str = validate_permission_pattern_path(&canonical_workdir, "working_directory")?;
    let clean_workdir = workdir_str.trim_end_matches('/');
    if clean_workdir.is_empty() {
        return Err(WorkerError::InvalidSpec(
            "refusing to grant `task_files` permissions on empty root directory".to_string(),
        ));
    }

    WorkerFilePermissions::TaskFiles
        .validate_for_provider_and_paths(WorkerProvider::Antigravity, allowed_write_paths)?;

    let mut allow =
        Vec::with_capacity(1 + allowed_write_paths.len() * AGY_WRITE_PERMISSION_TOOLS.len() * 2);
    allow.push(format!("{AGY_READ_PERMISSION_TOOL}({clean_workdir}/**)"));

    for entry in allowed_write_paths {
        let joined_target = canonical_workdir.join(entry);
        let _ = validate_absolute_no_symlink_components(
            &joined_target,
            "task_files allowed_write_paths target",
        )?;

        let target_str = format!("{clean_workdir}/{entry}");
        for tool in AGY_WRITE_PERMISSION_TOOLS {
            allow.push(format!("{tool}({target_str})"));
            allow.push(format!("{tool}({target_str}/**)"));
        }
    }

    let settings = AntigravitySessionSettings {
        permissions: AntigravityPermissionsBlock { allow },
    };
    let json = serde_json::to_string(&settings)?;
    Ok(json)
}
