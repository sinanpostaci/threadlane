use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Component, Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::error::WorkerError;
use crate::fs_safety::{validate_absolute_no_symlink_components, validate_existing_canonical_dir};
use crate::spec::{
    is_control_config_path, validate_repo_relative_path, validate_simple_identifier,
    ResolvedModelTarget, WorkerProvider, WriteScopeKind,
};

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

/// Non-read execution, notebook, knowledge, workflow, shell, subagent, MCP, web/browser,
/// marketplace/account, and interactive tool identifiers explicitly denied in Antigravity
/// [`WorkerFilePermissions::ReadOnly`] `--settings` payloads.
pub const AGY_DENIED_EXECUTION_AND_DELEGATION_TOOLS: &[&str] = &[
    "run_command",
    "bash",
    "shell",
    "execute",
    "run_in_terminal",
    "send_command_input",
    "manage_task",
    "invoke_subagent",
    "define_subagent",
    "manage_subagents",
    "send_message",
    "subagent",
    "task",
    "agent",
    "browser_subagent",
    "run_workflow",
    "notebook_edit",
    "notebook_execution",
    "delete_knowledge",
    "generate_image",
    "call_mcp_tool",
    "list_resources",
    "read_resource",
    "mcp",
    "search_web",
    "web",
    "webfetch",
    "websearch",
    "browser",
    "open_browser_url",
    "read_url_content",
    "execute_browser_javascript",
    "browser_click_element",
    "browser_drag_pixel_to_pixel",
    "browser_input",
    "browser_mouse_down",
    "browser_mouse_up",
    "browser_move_mouse",
    "browser_press_key",
    "browser_refresh_page",
    "browser_resize_window",
    "browser_scroll",
    "browser_scroll_dom",
    "browser_select_option",
    "click_browser_pixel",
    "manage_inbox",
    "search_marketplace",
    "list_plugin_accounts",
    "ask_question",
    "schedule",
];

/// Protected secret and control-directory glob suffixes explicitly denied for `read_file`
/// inside `<canonical_workdir>` in Antigravity [`WorkerFilePermissions::ReadOnly`] settings.
pub const AGY_PROTECTED_READ_DENY_PATTERNS: [&str; 28] = OPENCODE_PROTECTED_READ_AND_EDIT_PATTERNS;

/// Tool resources explicitly denied via `tool.use` policy rules when `file_permissions` is
/// [`WorkerFilePermissions::ReadOnly`] in OpenCode V2 configuration.
pub const OPENCODE_READ_ONLY_DENIED_TOOL_POLICIES: [&str; 16] = [
    "subagent",
    "task",
    "agent",
    "edit",
    "write",
    "patch",
    "apply_patch",
    "multiedit",
    "shell",
    "bash",
    "execute",
    "web",
    "webfetch",
    "websearch",
    "todowrite",
    "question",
];

/// Action identifiers explicitly denied on `"*"` in OpenCode V2 [`WorkerFilePermissions::ReadOnly`]
/// `permissions` rules before internal location-relative `read`/`glob`/`grep` rules are evaluated.
pub const OPENCODE_READ_ONLY_DENIED_PERMISSION_ACTIONS: [&str; 19] = [
    "*",
    "external_directory",
    "shell",
    "bash",
    "subagent",
    "task",
    "agent",
    "execute",
    "web",
    "webfetch",
    "websearch",
    "mcp",
    "edit",
    "write",
    "patch",
    "apply_patch",
    "multiedit",
    "todowrite",
    "question",
];

/// Environment variable consumed by OpenCode 2.2.0 (`opencode_v2`) for per-process inline JSON configuration.
pub const OPENCODE_CONFIG_ENV_VAR: &str = "OPENCODE_CONFIG_CONTENT";

/// Default execution agent pinned in OpenCode 2.2.0 (`opencode_v2`) per-child configuration.
pub const OPENCODE_DEFAULT_AGENT: &str = "build";

/// Primary and auxiliary OpenCode 2.2.0 (`opencode_v2`) agents whose models are always pinned
/// to prevent silent fallback to unapproved providers or auxiliary models.
pub const OPENCODE_PINNED_AGENTS: [&str; 4] = ["build", "title", "summary", "compaction"];

/// Maximum number of `allowed_write_paths` entries permitted when `file_permissions` is
/// [`WorkerFilePermissions::TaskFiles`].
///
/// Enforcing this finite ceiling before rule allocation prevents combinatorial rule expansion
/// while leaving [`WorkerFilePermissions::Inherit`] unaffected.
pub const MAX_TASK_FILES_WRITE_PATHS: usize = 128;

/// Maximum serialized UTF-8 byte length of the session-only Antigravity `--settings <inline-json>`
/// argument (`65_536` bytes / 64 KiB).
///
/// Kept conservatively below Linux's 128 KiB per-argument `MAX_ARG_STRLEN` (`E2BIG`) ceiling and
/// checked on the actual `serde_json`-serialized JSON string before returning launch argv.
pub const MAX_TASK_FILES_SETTINGS_BYTES: usize = 65_536;

/// Maximum serialized UTF-8 byte length of the per-child `OPENCODE_CONFIG_CONTENT` JSON
/// configuration (`65_536` bytes / 64 KiB).
pub const MAX_OPENCODE_CONFIG_BYTES: usize = 65_536;

/// Project-local Codex configuration and hook path names that must not exist in or above a
/// [`WorkerProvider::CodexCli`] read-only advisory working directory.
///
/// Because `--ignore-user-config` ignores user-level configuration (`~/.codex/config.toml`)
/// while project-local `.codex/` directories or `codex.toml` files in the checkout hierarchy
/// could otherwise define project hooks, MCP servers, or capability overrides, `codex_cli`
/// advisory validation fails closed if any of these markers exist in `working_directory` or
/// its enclosing project ancestors.
pub const CODEX_PROJECT_CONFIG_MARKERS: [&str; 3] = [".codex", "codex.toml", ".codex.toml"];

/// Protected control paths denied for `read`, `glob`, `grep`, and `edit` in OpenCode V2 `TaskFiles`.
const OPENCODE_PROTECTED_READ_AND_EDIT_PATTERNS: [&str; 28] = [
    ".env",
    ".env.*",
    "**/.env",
    "**/.env.*",
    ".git",
    ".git/**",
    "**/.git",
    "**/.git/**",
    ".opencode",
    ".opencode/**",
    "**/.opencode",
    "**/.opencode/**",
    ".codex",
    ".codex/**",
    "**/.codex",
    "**/.codex/**",
    ".gemini",
    ".gemini/**",
    "**/.gemini",
    "**/.gemini/**",
    ".agents",
    ".agents/**",
    "**/.agents",
    "**/.agents/**",
    ".claude",
    ".claude/**",
    "**/.claude",
    "**/.claude/**",
];

/// Additional protected repository and agent control paths denied for `edit` in OpenCode V2 `TaskFiles`.
const OPENCODE_PROTECTED_EDIT_ONLY_PATTERNS: [&str; 22] = [
    ".gitignore",
    "**/.gitignore",
    ".gitattributes",
    "**/.gitattributes",
    ".gitmodules",
    "**/.gitmodules",
    "AGENTS.md",
    "**/AGENTS.md",
    "agents.md",
    "**/agents.md",
    "CLAUDE.md",
    "**/CLAUDE.md",
    "claude.md",
    "**/claude.md",
    "opencode.json",
    "**/opencode.json",
    "opencode.jsonc",
    "**/opencode.jsonc",
    ".github",
    ".github/**",
    ".cargo/config",
    ".cargo/config.toml",
];

/// Trusted provider file-permission scoping policy for a worker invocation.
///
/// Provider permission flags and per-child configuration are session-only provider capability
/// policies; they are **not** an OS-level sandbox and do **not** replace the mandatory post-run
/// Git diff gate for write tasks. Static mode admission and generated fixture policies are
/// distinct from live provider runtime enforcement. [`WorkerFilePermissions::Inherit`] and
/// [`WorkerFilePermissions::TaskFiles`] do not configure a read-only advisory deny policy;
/// advisory roles must use [`WorkerFilePermissions::ReadOnly`].
#[derive(
    Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum WorkerFilePermissions {
    /// Preserve the default provider file-permission semantics without injecting new file grants
    /// (for OpenCode v2, still pins primary and auxiliary models and provider/MCP policies).
    #[default]
    Inherit,
    /// Scope session file permissions to the task working directory and `allowed_write_paths`
    /// (`--settings <inline-json>` for Antigravity; V2 `permissions` rules in
    /// `OPENCODE_CONFIG_CONTENT` for OpenCode v2).
    TaskFiles,
    /// Configure session permissions for read-only advisory inspection inside the canonical
    /// non-root working directory with zero allowed write scopes, explicit write/notebook/shell/
    /// delegation/MCP/web/browser denials, and protected-control read denials (`--mode plan` and
    /// `--settings <inline-json>` with scoped `allow` and `deny` for Antigravity; V2 `policies`
    /// and `permissions` in `OPENCODE_CONFIG_CONTENT` for OpenCode v2; `--no-daemon exec --json
    /// --ephemeral --ignore-user-config` with the session-only `threadlane-advisory` permission
    /// profile, disabled capability flags, and project-config fail-closed validation for
    /// `codex_cli`).
    ReadOnly,
}

impl WorkerFilePermissions {
    /// Returns the canonical `snake_case` representation (`"inherit"`, `"task_files"`, or `"read_only"`).
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Inherit => "inherit",
            Self::TaskFiles => "task_files",
            Self::ReadOnly => "read_only",
        }
    }

    /// Returns `true` when this mode is [`WorkerFilePermissions::ReadOnly`].
    pub fn is_read_only(self) -> bool {
        matches!(self, Self::ReadOnly)
    }

    /// Returns `true` when this mode selects the provider read-only advisory permission policy
    /// ([`WorkerFilePermissions::ReadOnly`]).
    ///
    /// [`WorkerFilePermissions::Inherit`] does not configure a read-only deny policy and
    /// [`WorkerFilePermissions::TaskFiles`] grants write permissions.
    pub fn enforces_preventive_read_only(self) -> bool {
        matches!(self, Self::ReadOnly)
    }

    /// Returns `true` when constructing the final launch argv for Antigravity requires the
    /// canonical working directory (`TaskFiles` or `ReadOnly`).
    pub fn requires_workdir_argv(self) -> bool {
        matches!(self, Self::TaskFiles | Self::ReadOnly)
    }

    /// Verifies that `(provider, self)` is admitted for read-only advisory provider configuration.
    ///
    /// Rejects non-`ReadOnly` modes (`Inherit` and `TaskFiles`) with
    /// [`WorkerError::UnsupportedCapability`] before launch. This checks provider/mode admission
    /// and generated policy support; it is not an OS sandbox or proof of live provider runtime
    /// enforcement.
    pub fn ensure_advisory_read_only_capability(
        self,
        provider: WorkerProvider,
    ) -> Result<(), WorkerError> {
        match self {
            Self::ReadOnly => match provider {
                WorkerProvider::Antigravity
                | WorkerProvider::OpenCodeV2
                | WorkerProvider::CodexCli => Ok(()),
            },
            Self::Inherit | Self::TaskFiles => Err(WorkerError::UnsupportedCapability {
                provider: provider.as_str().to_string(),
                capability: format!("advisory_file_permissions:{}", self.as_str()),
                reason: format!(
                    "advisory workers require preventive `file_permissions: read_only`; `{}` does not establish preventive read-only capability enforcement",
                    self.as_str()
                ),
            }),
        }
    }

    /// Validates that this permission mode is supported for `provider` and that
    /// `allowed_write_paths` satisfies the mode's invariants:
    /// - When `self` is [`WorkerFilePermissions::ReadOnly`], `allowed_write_paths` **must be empty**
    ///   (zero write scopes).
    /// - When `self` is [`WorkerFilePermissions::Inherit`] or [`WorkerFilePermissions::TaskFiles`],
    ///   `provider` must support mutable worker execution (`antigravity` or `opencode_v2`;
    ///   `codex_cli` is rejected with [`WorkerError::UnsupportedCapability`]) and
    ///   `allowed_write_paths` must be non-empty, within finite `TaskFiles` bounds, free of
    ///   protected control paths, and free of unsafe glob/rule metacharacters.
    pub fn validate_for_provider_and_paths(
        self,
        provider: WorkerProvider,
        allowed_write_paths: &BTreeSet<String>,
    ) -> Result<(), WorkerError> {
        if self == Self::ReadOnly {
            self.ensure_advisory_read_only_capability(provider)?;
            if !allowed_write_paths.is_empty() {
                return Err(WorkerError::InvalidSpec(format!(
                    "allowed_write_paths must be empty when `file_permissions` is `read_only` (got {} entries)",
                    allowed_write_paths.len()
                )));
            }
            return Ok(());
        }

        if provider == WorkerProvider::CodexCli {
            return Err(WorkerError::UnsupportedCapability {
                provider: provider.as_str().to_string(),
                capability: format!("worker_file_permissions:{}", self.as_str()),
                reason: format!(
                    "provider `codex_cli` supports only `file_permissions: read_only` advisory invocations via `AdvisoryWorkerSpec`; mutable `{}` worker invocations are not supported",
                    self.as_str()
                ),
            });
        }

        if allowed_write_paths.is_empty() {
            return Err(WorkerError::InvalidSpec(
                "allowed_write_paths must contain at least one repository-relative path"
                    .to_string(),
            ));
        }

        if self == Self::TaskFiles && allowed_write_paths.len() > MAX_TASK_FILES_WRITE_PATHS {
            return Err(WorkerError::InvalidSpec(format!(
                "allowed_write_paths contains {} entries, which exceeds the maximum of {MAX_TASK_FILES_WRITE_PATHS} entries permitted when `file_permissions` is `task_files`",
                allowed_write_paths.len()
            )));
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

/// Validates a raw provider identifier string and [`WorkerFilePermissions`] mode for read-only
/// advisory worker admission, failing closed with [`WorkerError::UnsupportedCapability`] when the
/// provider has no supported read-only advisory configuration (for example `jev_api`,
/// `openai_decisions_api`, `git`, `tmux`, `codegraph`, or unknown providers) or when the mode is
/// not [`WorkerFilePermissions::ReadOnly`].
///
/// This checks static provider/mode admission; it does not prove live provider runtime deny
/// behavior or establish an OS sandbox.
pub fn validate_advisory_provider_and_mode(
    provider_id: &str,
    file_permissions: WorkerFilePermissions,
) -> Result<WorkerProvider, WorkerError> {
    let trimmed = provider_id.trim();
    let provider = match trimmed {
        "antigravity" | "agy" => WorkerProvider::Antigravity,
        "opencode_v2" | "open_code_v2" | "opencode-v2" | "opencode" => WorkerProvider::OpenCodeV2,
        "codex_cli" | "codex-cli" | "codex" => WorkerProvider::CodexCli,
        other => {
            return Err(WorkerError::UnsupportedCapability {
                provider: if other.is_empty() {
                    "<empty>".to_string()
                } else {
                    other.to_string()
                },
                capability: "read_only_advisory_worker".to_string(),
                reason: format!(
                    "provider `{}` does not support preventive `read_only` advisory capability enforcement; only `antigravity`, `opencode_v2`, and `codex_cli` are supported",
                    if other.is_empty() { "<empty>" } else { other }
                ),
            });
        }
    };
    file_permissions.ensure_advisory_read_only_capability(provider)?;
    Ok(provider)
}

/// Exact provider, model, and optional variant target pinned in OpenCode V2 configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OpenCodeModelObject {
    /// Selected provider identifier (for example `"opencode-go"`).
    #[serde(rename = "providerID")]
    pub provider: String,
    /// Selected provider-scoped model identifier (for example `"deepseek-v4.1-flash"`).
    pub model: String,
    /// Optional reasoning effort / variant identifier (for example `"max"`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub variant: Option<String>,
}

/// Named agent override entry in OpenCode V2 `agents` configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OpenCodeAgentConfig {
    /// Exact model target pinned for this agent.
    pub model: OpenCodeModelObject,
    /// Optional agent-scoped V2 permission rules appended after global `permissions`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub permissions: Option<Vec<OpenCodePermissionRule>>,
}

/// Effect of an ordered rule in OpenCode V2 `policies` or `permissions`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OpenCodeRuleEffect {
    /// Allow the matching action and resource.
    Allow,
    /// Deny the matching action and resource.
    Deny,
}

/// Single ordered `{ action, resource, effect }` rule in OpenCode V2 `policies` or `permissions`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OpenCodePermissionRule {
    /// Action identifier (for example `"provider.use"`, `"mcp.use"`, `"read"`, `"edit"`, `"shell"`).
    pub action: String,
    /// Glob resource pattern (location-relative for internal file actions, `"*"` for global).
    pub resource: String,
    /// Rule effect (`"allow"` or `"deny"`).
    pub effect: OpenCodeRuleEffect,
}

/// Bounded per-child OpenCode 2.2.0 V2 configuration serialized into `OPENCODE_CONFIG_CONTENT`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OpenCodeConfigContent {
    /// Keep the installed CLI version fixed throughout a worker invocation.
    pub update: String,
    /// Default primary agent (`"build"`).
    pub default_agent: String,
    /// Top-level model pin.
    pub model: OpenCodeModelObject,
    /// Named agent overrides (`build`, `title`, `summary`, `compaction`) pinned to the exact model.
    pub agents: BTreeMap<String, OpenCodeAgentConfig>,
    /// Ordered provider and MCP policy rules (`provider.use` deny `*` then allow selected provider; `mcp.use` deny `*`).
    pub policies: Vec<OpenCodePermissionRule>,
    /// Optional ordered V2 permission rules (present when `file_permissions` is
    /// [`WorkerFilePermissions::TaskFiles`] or [`WorkerFilePermissions::ReadOnly`]).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub permissions: Option<Vec<OpenCodePermissionRule>>,
}

/// Structured session-only Antigravity `--settings <inline-json>` payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AntigravitySessionSettings {
    /// Session-scoped tool permission allow and deny rules.
    pub permissions: AntigravityPermissionsBlock,
}

/// Antigravity `permissions` block containing `allow` and optional `deny` rule lists.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AntigravityPermissionsBlock {
    /// Explicitly allowed tool rules (`<tool>(<pattern>)`).
    pub allow: Vec<String>,
    /// Explicitly denied tool rules (`<tool>(<pattern>)`).
    ///
    /// Omitted when empty so [`WorkerFilePermissions::TaskFiles`] serialization remains
    /// byte-for-byte identical to existing profiles and snapshots.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub deny: Vec<String>,
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

/// Validates that `working_directory` is an existing canonical non-root directory without symlink
/// components, glob/rule metacharacters, or protected control-path components (`.git`, `.codex`,
/// `.gemini`, `.opencode`, `.agents`, `.claude`, `.github`, `.cargo`, `.env`, etc.) for
/// [`WorkerFilePermissions::ReadOnly`].
pub fn validate_advisory_read_only_workdir(
    working_directory: &Path,
) -> Result<PathBuf, WorkerError> {
    let (canonical_workdir, _) = validate_read_only_canonical_workdir(working_directory)?;
    Ok(canonical_workdir)
}

/// Validates `working_directory` for a [`WorkerProvider::CodexCli`] read-only advisory invocation.
///
/// Performs all [`validate_advisory_read_only_workdir`] checks and additionally fails closed with
/// [`WorkerError::InvalidSpec`] if `working_directory` or any enclosing project directory up to
/// the nearest `.git` root (or non-system ancestor when outside Git) contains a project-local
/// Codex configuration or hooks path ([`CODEX_PROJECT_CONFIG_MARKERS`]: `.codex`, `codex.toml`,
/// `.codex.toml`). This prevents project config or hooks from expanding capabilities when
/// `--ignore-user-config` ignores user-level configuration.
pub fn validate_codex_advisory_workdir(working_directory: &Path) -> Result<PathBuf, WorkerError> {
    let canonical_workdir = validate_advisory_read_only_workdir(working_directory)?;
    ensure_no_codex_project_config_in_hierarchy(&canonical_workdir)?;
    Ok(canonical_workdir)
}

fn ensure_no_codex_project_config_in_hierarchy(
    canonical_workdir: &Path,
) -> Result<(), WorkerError> {
    let git_root = canonical_workdir
        .ancestors()
        .find(|dir| fs::symlink_metadata(dir.join(".git")).is_ok());

    let temp_root = std::env::temp_dir();
    let canonical_temp_root = fs::canonicalize(&temp_root).unwrap_or(temp_root);
    let home_root = std::env::var_os("HOME")
        .map(PathBuf::from)
        .map(|p| fs::canonicalize(&p).unwrap_or(p));

    for (depth, dir) in canonical_workdir.ancestors().enumerate() {
        if depth > 0
            && git_root.is_none()
            && (dir.parent().is_none()
                || dir == canonical_temp_root
                || dir == Path::new("/tmp")
                || home_root.as_deref() == Some(dir))
        {
            break;
        }
        for marker in CODEX_PROJECT_CONFIG_MARKERS {
            let candidate = dir.join(marker);
            if fs::symlink_metadata(&candidate).is_ok() {
                return Err(WorkerError::InvalidSpec(format!(
                    "refusing to launch `codex_cli` advisory worker in `{}`: project Codex configuration or hooks path `{}` exists and could expand capabilities under `--ignore-user-config`",
                    canonical_workdir.display(),
                    candidate.display()
                )));
            }
        }
        if Some(dir) == git_root {
            break;
        }
    }
    Ok(())
}

fn validate_read_only_canonical_workdir(
    working_directory: &Path,
) -> Result<(PathBuf, String), WorkerError> {
    let canonical_workdir =
        validate_existing_canonical_dir(working_directory, "working_directory")?;
    if canonical_workdir.parent().is_none() {
        return Err(WorkerError::InvalidSpec(
            "refusing to grant `read_only` permissions on filesystem root directory `/`"
                .to_string(),
        ));
    }

    let workdir_str = validate_permission_pattern_path(&canonical_workdir, "working_directory")?;
    let clean_workdir = workdir_str.trim_end_matches('/').to_string();
    if clean_workdir.is_empty() {
        return Err(WorkerError::InvalidSpec(
            "refusing to grant `read_only` permissions on empty root directory".to_string(),
        ));
    }

    for comp in canonical_workdir.components() {
        if let Component::Normal(part) = comp {
            let lowered = part.to_string_lossy().to_ascii_lowercase();
            if is_protected_control_workdir_component(&lowered) {
                return Err(WorkerError::InvalidSpec(format!(
                    "working_directory `{}` targets or resides inside protected control path component `{}`",
                    canonical_workdir.display(),
                    part.to_string_lossy()
                )));
            }
        }
    }

    Ok((canonical_workdir, clean_workdir))
}

fn is_protected_control_workdir_component(lowered_component: &str) -> bool {
    matches!(
        lowered_component,
        ".git"
            | ".gitmodules"
            | ".gitattributes"
            | ".gitignore"
            | "agents.md"
            | "claude.md"
            | "opencode.json"
            | "opencode.jsonc"
            | "codex.toml"
            | ".codex.toml"
            | ".codex"
            | ".gemini"
            | ".opencode"
            | ".agents"
            | ".claude"
            | ".github"
            | ".cargo"
    ) || lowered_component == ".env"
        || lowered_component.starts_with(".env.")
}

/// Builds the structured session-only Antigravity `--settings <inline-json>` [`AntigravitySessionSettings`]
/// for [`WorkerFilePermissions::ReadOnly`] from an existing canonical non-root `working_directory`.
///
/// Note: Session-only `--settings` is a generated provider-level capability policy, **not** an
/// OS-level filesystem or process sandbox, and generating this payload is distinct from live
/// runtime enforcement inside the external `agy` binary.
///
/// Populates in the generated settings payload:
/// - `allow`: only `read_file(<canonical_workdir>/**)` (zero write, notebook, shell, subagent,
///   MCP, or web/browser tools).
/// - `deny`:
///   - all file-edit and file-write tools (`edit_file`, `write_file`, `write_to_file`,
///     `replace_file_content`, `multi_replace_file_content`) and `notebook_edit` both globally
///     (`<tool>(*)`) and inside `<canonical_workdir>/**`;
///   - all shell/terminal, notebook execution, subagent/workflow, MCP, web/browser, marketplace/
///     account, and interactive tools in [`AGY_DENIED_EXECUTION_AND_DELEGATION_TOOLS`]
///     (`<tool>(*)`);
///   - `read_file(<canonical_workdir>/<pattern>)` for every protected secret and control path in
///     [`AGY_PROTECTED_READ_DENY_PATTERNS`].
pub fn build_antigravity_read_only_settings(
    working_directory: &Path,
) -> Result<AntigravitySessionSettings, WorkerError> {
    let (_canonical_workdir, clean_workdir) =
        validate_read_only_canonical_workdir(working_directory)?;

    let allow = vec![format!("{AGY_READ_PERMISSION_TOOL}({clean_workdir}/**)")];

    let mut deny = Vec::with_capacity(
        AGY_WRITE_PERMISSION_TOOLS.len() * 2
            + 1
            + AGY_DENIED_EXECUTION_AND_DELEGATION_TOOLS.len()
            + AGY_PROTECTED_READ_DENY_PATTERNS.len(),
    );
    for tool in AGY_WRITE_PERMISSION_TOOLS {
        deny.push(format!("{tool}(*)"));
        deny.push(format!("{tool}({clean_workdir}/**)"));
    }
    deny.push(format!("notebook_edit({clean_workdir}/**)"));
    for &tool in AGY_DENIED_EXECUTION_AND_DELEGATION_TOOLS {
        deny.push(format!("{tool}(*)"));
    }
    for pattern in AGY_PROTECTED_READ_DENY_PATTERNS {
        deny.push(format!(
            "{AGY_READ_PERMISSION_TOOL}({clean_workdir}/{pattern})"
        ));
    }

    Ok(AntigravitySessionSettings {
        permissions: AntigravityPermissionsBlock { allow, deny },
    })
}

/// Builds and serializes the session-only Antigravity `--settings <inline-json>` payload for
/// [`WorkerFilePermissions::ReadOnly`], enforcing [`MAX_TASK_FILES_SETTINGS_BYTES`] (`65_536` bytes).
pub fn build_antigravity_read_only_settings_json(
    working_directory: &Path,
) -> Result<String, WorkerError> {
    let settings = build_antigravity_read_only_settings(working_directory)?;
    let json = serde_json::to_string(&settings)?;
    if json.len() > MAX_TASK_FILES_SETTINGS_BYTES {
        return Err(WorkerError::InvalidSpec(format!(
            "serialized `read_only` `--settings` JSON payload ({} bytes) exceeds maximum of {MAX_TASK_FILES_SETTINGS_BYTES} bytes",
            json.len()
        )));
    }
    Ok(json)
}

/// Deterministically evaluates a generated [`AntigravitySessionSettings`] fixture structure for
/// `(tool, resource)` using a local explicit-deny-then-allow-then-deny fixture model:
/// - File/notebook path tools (`read_file`, [`AGY_WRITE_PERMISSION_TOOLS`], and `notebook_edit`)
///   require a non-empty clean absolute target path without `.` or `..` traversal components or
///   backslashes;
/// - If any rule in `settings.permissions.deny` matches `(tool, resource)`, returns
///   [`OpenCodeRuleEffect::Deny`];
/// - Otherwise, if any rule in `settings.permissions.allow` matches `(tool, resource)`, returns
///   [`OpenCodeRuleEffect::Allow`];
/// - Otherwise, returns [`OpenCodeRuleEffect::Deny`].
///
/// Note: This function only inspects our generated `--settings` fixture structure for unit tests.
/// It does **not** prove live Antigravity (`agy`) runtime enforcement, live default-denial
/// semantics for unlisted tools, or OS-level sandboxing; live provider deny behavior requires a
/// separate real provider capability probe.
pub fn evaluate_antigravity_settings(
    settings: &AntigravitySessionSettings,
    tool: &str,
    resource: Option<&str>,
) -> OpenCodeRuleEffect {
    if tool.is_empty() {
        return OpenCodeRuleEffect::Deny;
    }

    let is_file_tool = tool == AGY_READ_PERMISSION_TOOL
        || AGY_WRITE_PERMISSION_TOOLS.contains(&tool)
        || tool == "notebook_edit";
    let normalized_resource = match resource {
        Some(raw) => {
            if is_file_tool && !is_clean_absolute_permission_resource(raw) {
                return OpenCodeRuleEffect::Deny;
            }
            Some(raw)
        }
        None => {
            if is_file_tool {
                return OpenCodeRuleEffect::Deny;
            }
            None
        }
    };

    if settings
        .permissions
        .deny
        .iter()
        .any(|rule| antigravity_rule_matches(rule, tool, normalized_resource))
    {
        return OpenCodeRuleEffect::Deny;
    }

    if settings
        .permissions
        .allow
        .iter()
        .any(|rule| antigravity_rule_matches(rule, tool, normalized_resource))
    {
        return OpenCodeRuleEffect::Allow;
    }

    OpenCodeRuleEffect::Deny
}

impl AntigravitySessionSettings {
    /// Evaluates `(tool, resource)` against this generated settings structure using the local
    /// fixture rule model (`evaluate_antigravity_settings`). Does not prove live provider
    /// runtime enforcement.
    pub fn evaluate(&self, tool: &str, resource: Option<&str>) -> OpenCodeRuleEffect {
        evaluate_antigravity_settings(self, tool, resource)
    }
}

fn is_clean_absolute_permission_resource(resource: &str) -> bool {
    if resource.is_empty()
        || !resource.starts_with('/')
        || resource.contains('\\')
        || resource.contains('\0')
        || resource.contains("//")
    {
        return false;
    }
    let path = Path::new(resource);
    let mut saw_normal = false;
    for comp in path.components() {
        match comp {
            Component::RootDir => {}
            Component::Normal(part) => {
                let s = part.to_string_lossy();
                if s.is_empty() || s.chars().any(char::is_control) {
                    return false;
                }
                saw_normal = true;
            }
            Component::CurDir | Component::ParentDir | Component::Prefix(_) => return false,
        }
    }
    saw_normal
}

fn antigravity_rule_matches(rule: &str, tool: &str, resource: Option<&str>) -> bool {
    let (rule_tool, rule_pattern) = match rule.strip_suffix(')') {
        Some(prefix) => match prefix.split_once('(') {
            Some((t, p)) => (t.trim(), p),
            None => return false,
        },
        None => (rule.trim(), "*"),
    };

    if rule_tool != "*" && rule_tool != tool {
        return false;
    }
    if rule_pattern == "*" {
        return true;
    }
    match resource {
        Some(target) => opencode_glob_matches(rule_pattern, target),
        None => false,
    }
}

/// Builds the session-only Antigravity `--settings <inline-json>` payload for
/// [`WorkerFilePermissions::TaskFiles`] from an existing canonical task working directory and
/// validated `allowed_write_paths` using explicit trailing-slash directory syntax.
///
/// Because this direct builder does not receive a Git repository path or pinned `base_commit`,
/// it resolves write scopes syntactically from `allowed_write_paths`:
/// - Explicit directory entries ending with a trailing `/` (for example `"foo/"` or
///   `"crates/threadlane-workers/"`) resolve to [`WriteScopeKind::DirectorySubtree`] and grant
///   both exact (`<tool>(<canonical_workdir>/<dir>)`) and descendant
///   (`<tool>(<canonical_workdir>/<dir>/**)`) rules for `edit_file`, `write_file`,
///   `write_to_file`, `replace_file_content`, and `multi_replace_file_content`.
/// - Bare entries without a trailing `/` (for example `"foo"`, `"src/lib.rs"`, or `"new.txt"`)
///   resolve to [`WriteScopeKind::ExactFile`] and grant **only** the exact
///   `<tool>(<canonical_workdir>/<file>)` rule — never `<tool>(<canonical_workdir>/<file>/**)` —
///   regardless of whether the path currently exists or is a directory on disk.
///
/// When launching via [`crate::spec::WorkerSpec::build_argv_for_workdir`], `allowed_write_paths`
/// are first resolved against the immutable pinned `base_commit` tree via
/// [`crate::git::resolve_allowed_write_scopes_at_base`] and passed to
/// [`build_antigravity_task_files_settings_json_for_scopes`], preserving legacy bare
/// base-directory `WorkerSpec` entries while keeping exact files (existing or absent at
/// `base_commit`) strictly exact.
///
/// Always grants `read_file(<canonical_workdir>/**)`. Never includes shell, network, or MCP tool
/// grants, and never modifies global provider settings.
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

    let _ = validate_permission_pattern_path(&canonical_workdir, "working_directory")?;

    WorkerFilePermissions::TaskFiles
        .validate_for_provider_and_paths(WorkerProvider::Antigravity, allowed_write_paths)?;

    let mut resolved_write_scopes = BTreeMap::new();
    for entry in allowed_write_paths {
        let (clean_entry, kind) = match entry.strip_suffix('/') {
            Some(stripped) => (stripped.to_string(), WriteScopeKind::DirectorySubtree),
            None => (entry.clone(), WriteScopeKind::ExactFile),
        };
        resolved_write_scopes
            .entry(clean_entry)
            .and_modify(|existing| {
                if kind == WriteScopeKind::DirectorySubtree {
                    *existing = WriteScopeKind::DirectorySubtree;
                }
            })
            .or_insert(kind);
    }

    build_antigravity_task_files_settings_json_for_scopes(
        &canonical_workdir,
        &resolved_write_scopes,
    )
}

/// Builds the session-only Antigravity `--settings <inline-json>` payload for
/// [`WorkerFilePermissions::TaskFiles`] from an existing canonical task working directory and
/// resolved repository-relative [`WriteScopeKind`] entries.
///
/// Grants:
/// - `read_file(<canonical_workdir>/**)`
/// - Exact `<tool>(<canonical_workdir>/<entry>)` rules for `edit_file`, `write_file`,
///   `write_to_file`, `replace_file_content`, and `multi_replace_file_content` for every entry.
/// - Descendant `<tool>(<canonical_workdir>/<entry>/**)` rules **only** when the entry's scope
///   is [`WriteScopeKind::DirectorySubtree`]. [`WriteScopeKind::ExactFile`] entries never receive
///   descendant `/**` grants, even if the target path is replaced by a directory in the mutable
///   working tree.
///
/// Never includes shell, network, or MCP tool grants, and never modifies global provider settings.
pub fn build_antigravity_task_files_settings_json_for_scopes(
    working_directory: &Path,
    resolved_write_scopes: &BTreeMap<String, WriteScopeKind>,
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

    if resolved_write_scopes.is_empty() {
        return Err(WorkerError::InvalidSpec(
            "allowed_write_paths must contain at least one repository-relative path".to_string(),
        ));
    }
    if resolved_write_scopes.len() > MAX_TASK_FILES_WRITE_PATHS {
        return Err(WorkerError::InvalidSpec(format!(
            "allowed_write_paths contains {} entries, which exceeds the maximum of {MAX_TASK_FILES_WRITE_PATHS} entries permitted when `file_permissions` is `task_files`",
            resolved_write_scopes.len()
        )));
    }

    let mut allow =
        Vec::with_capacity(1 + resolved_write_scopes.len() * AGY_WRITE_PERMISSION_TOOLS.len() * 2);
    allow.push(format!("{AGY_READ_PERMISSION_TOOL}({clean_workdir}/**)"));

    for (raw_entry, &scope_kind) in resolved_write_scopes {
        validate_repo_relative_path(raw_entry, "allowed_write_paths entry")?;
        let clean_entry = raw_entry.strip_suffix('/').unwrap_or(raw_entry.as_str());
        if is_control_config_path(clean_entry) {
            return Err(WorkerError::InvalidSpec(format!(
                "allowed_write_paths entry `{raw_entry}` targets protected repository or agent control configuration"
            )));
        }
        validate_permission_pattern_str(clean_entry, "allowed_write_paths entry")?;

        let joined_target = canonical_workdir.join(clean_entry);
        let _ = validate_absolute_no_symlink_components(
            &joined_target,
            "task_files allowed_write_paths target",
        )?;
        if scope_kind == WriteScopeKind::DirectorySubtree {
            if let Ok(meta) = fs::symlink_metadata(&joined_target) {
                if !meta.file_type().is_dir() {
                    return Err(WorkerError::InvalidSpec(format!(
                        "allowed_write_paths directory entry `{clean_entry}/` targets existing non-directory `{}`",
                        joined_target.display()
                    )));
                }
            }
        }

        let target_str = format!("{clean_workdir}/{clean_entry}");
        for tool in AGY_WRITE_PERMISSION_TOOLS {
            allow.push(format!("{tool}({target_str})"));
            if scope_kind == WriteScopeKind::DirectorySubtree {
                allow.push(format!("{tool}({target_str}/**)"));
            }
        }
    }

    let settings = AntigravitySessionSettings {
        permissions: AntigravityPermissionsBlock {
            allow,
            deny: Vec::new(),
        },
    };
    let json = serde_json::to_string(&settings)?;
    if json.len() > MAX_TASK_FILES_SETTINGS_BYTES {
        return Err(WorkerError::InvalidSpec(format!(
            "serialized `task_files` `--settings` JSON payload ({} bytes) exceeds maximum of {MAX_TASK_FILES_SETTINGS_BYTES} bytes",
            json.len()
        )));
    }
    Ok(json)
}

/// Builds the structured OpenCode 2.2.0 V2 [`OpenCodeConfigContent`] for a validated model target,
/// [`WorkerFilePermissions`] mode, existing canonical task `working_directory`, and
/// `allowed_write_paths`.
///
/// Always pins:
/// - `default_agent: "build"`
/// - top-level `model: { provider, model, variant }`
/// - `agents.build`, `agents.title`, `agents.summary`, and `agents.compaction` to the exact same
///   `{ provider, model, variant }` object so auxiliary steps cannot silently invoke another
///   provider or model
/// - `policies`: `provider.use` deny `*`, then allow the selected provider, and `mcp.use` deny `*`.
///
/// When `file_permissions` is [`WorkerFilePermissions::Inherit`], `permissions` is `None` so
/// file-permission semantics remain unchanged. When `file_permissions` is
/// [`WorkerFilePermissions::TaskFiles`], populates V2 ordered `{ action, resource, effect }` rules
/// with location-relative internal resources, `external_directory` deny `*`, `shell`/`subagent`/
/// `execute`/`web` denied by baseline, internal `read`/`glob`/`grep` allowed on `*` with trailing
/// `.env` and control-directory denies, and `edit` granted only to the exact selected
/// repository-relative paths (plus `<dir>/**` descendants only for directory scopes).
/// When `file_permissions` is [`WorkerFilePermissions::ReadOnly`], requires zero write scopes,
/// denies all write/edit, shell, subagent, MCP, web, and interactive tools in both `policies`
/// and `permissions`, and allows only location-relative internal `read`/`glob`/`grep` inspection
/// with trailing `.env` and control-directory denies.
pub fn build_opencode_config_content(
    resolved_model: &ResolvedModelTarget,
    file_permissions: WorkerFilePermissions,
    working_directory: &Path,
    allowed_write_paths: &BTreeSet<String>,
) -> Result<OpenCodeConfigContent, WorkerError> {
    let canonical_workdir =
        validate_existing_canonical_dir(working_directory, "working_directory")?;
    if canonical_workdir.parent().is_none() {
        return Err(WorkerError::InvalidSpec(
            "refusing to generate OpenCode config on filesystem root directory `/`".to_string(),
        ));
    }

    file_permissions
        .validate_for_provider_and_paths(WorkerProvider::OpenCodeV2, allowed_write_paths)?;

    let (provider_id, model_id) = resolved_model.base_model.split_once('/').ok_or_else(|| {
        WorkerError::InvalidSpec(format!(
            "OpenCode model `{}` must use `provider/model` format",
            resolved_model.base_model
        ))
    })?;
    validate_simple_identifier(provider_id, "OpenCode provider")?;
    if model_id.is_empty() {
        return Err(WorkerError::InvalidSpec(format!(
            "OpenCode model `{}` is missing model segment",
            resolved_model.base_model
        )));
    }
    for segment in model_id.split('/') {
        validate_simple_identifier(segment, "OpenCode model segment")?;
    }
    validate_permission_pattern_str(provider_id, "OpenCode provider")?;
    validate_permission_pattern_str(model_id, "OpenCode model")?;
    if let Some(ref variant) = resolved_model.effort {
        validate_simple_identifier(variant, "OpenCode model variant")?;
        validate_permission_pattern_str(variant, "OpenCode model variant")?;
    }

    let model_obj = OpenCodeModelObject {
        provider: provider_id.to_string(),
        model: model_id.to_string(),
        variant: resolved_model.effort.clone(),
    };

    let mut agents = BTreeMap::new();
    for agent_name in OPENCODE_PINNED_AGENTS {
        agents.insert(
            agent_name.to_string(),
            OpenCodeAgentConfig {
                model: model_obj.clone(),
                permissions: None,
            },
        );
    }

    let mut policies = vec![
        OpenCodePermissionRule {
            action: "provider.use".to_string(),
            resource: "*".to_string(),
            effect: OpenCodeRuleEffect::Deny,
        },
        OpenCodePermissionRule {
            action: "provider.use".to_string(),
            resource: provider_id.to_string(),
            effect: OpenCodeRuleEffect::Allow,
        },
        OpenCodePermissionRule {
            action: "mcp.use".to_string(),
            resource: "*".to_string(),
            effect: OpenCodeRuleEffect::Deny,
        },
    ];
    let denied_tool_policies: &[&str] = match file_permissions {
        WorkerFilePermissions::Inherit | WorkerFilePermissions::TaskFiles => {
            &["subagent", "task", "agent"]
        }
        WorkerFilePermissions::ReadOnly => &OPENCODE_READ_ONLY_DENIED_TOOL_POLICIES,
    };
    for &resource in denied_tool_policies {
        policies.push(OpenCodePermissionRule {
            action: "tool.use".to_string(),
            resource: resource.to_string(),
            effect: OpenCodeRuleEffect::Deny,
        });
    }

    let permissions = match file_permissions {
        WorkerFilePermissions::Inherit => None,
        WorkerFilePermissions::TaskFiles => Some(build_opencode_task_files_rules(
            &canonical_workdir,
            allowed_write_paths,
        )?),
        WorkerFilePermissions::ReadOnly => {
            Some(build_opencode_read_only_rules(&canonical_workdir)?)
        }
    };
    if let Some(rules) = &permissions {
        for agent in agents.values_mut() {
            agent.permissions = Some(rules.clone());
        }
    }

    Ok(OpenCodeConfigContent {
        update: "disable".to_string(),
        default_agent: OPENCODE_DEFAULT_AGENT.to_string(),
        model: model_obj,
        agents,
        policies,
        permissions,
    })
}

/// Builds and serializes the bounded per-child `OPENCODE_CONFIG_CONTENT` JSON payload,
/// enforcing [`MAX_OPENCODE_CONFIG_BYTES`] (`65_536` bytes / 64 KiB).
pub fn build_opencode_config_content_json(
    resolved_model: &ResolvedModelTarget,
    file_permissions: WorkerFilePermissions,
    working_directory: &Path,
    allowed_write_paths: &BTreeSet<String>,
) -> Result<String, WorkerError> {
    let config = build_opencode_config_content(
        resolved_model,
        file_permissions,
        working_directory,
        allowed_write_paths,
    )?;
    let json = serde_json::to_string(&config)?;
    if json.len() > MAX_OPENCODE_CONFIG_BYTES {
        return Err(WorkerError::InvalidSpec(format!(
            "serialized `OPENCODE_CONFIG_CONTENT` JSON payload ({} bytes) exceeds maximum of {MAX_OPENCODE_CONFIG_BYTES} bytes",
            json.len()
        )));
    }
    Ok(json)
}

fn build_opencode_read_only_rules(
    canonical_workdir: &Path,
) -> Result<Vec<OpenCodePermissionRule>, WorkerError> {
    let (_canonical, _clean_workdir) = validate_read_only_canonical_workdir(canonical_workdir)?;

    let mut rules = Vec::new();

    // 1. Ordered deny* baseline and explicit non-file/external/write/shell/delegation action denies.
    for action in OPENCODE_READ_ONLY_DENIED_PERMISSION_ACTIONS {
        rules.push(OpenCodePermissionRule {
            action: action.to_string(),
            resource: "*".to_string(),
            effect: OpenCodeRuleEffect::Deny,
        });
    }

    // 2. Allow location-relative internal read/glob/grep inspection only (zero `edit` grants).
    for action in ["read", "glob", "grep"] {
        rules.push(OpenCodePermissionRule {
            action: action.to_string(),
            resource: "*".to_string(),
            effect: OpenCodeRuleEffect::Allow,
        });
    }

    // 3. Append `.env` and protected control denies LAST so last-matching-rule evaluation
    // overrides `read`/`glob`/`grep` `*` and keeps all `edit` patterns explicitly denied.
    for pattern in OPENCODE_PROTECTED_READ_AND_EDIT_PATTERNS {
        for action in ["read", "glob", "grep", "edit"] {
            rules.push(OpenCodePermissionRule {
                action: action.to_string(),
                resource: pattern.to_string(),
                effect: OpenCodeRuleEffect::Deny,
            });
        }
    }
    for pattern in OPENCODE_PROTECTED_EDIT_ONLY_PATTERNS {
        rules.push(OpenCodePermissionRule {
            action: "edit".to_string(),
            resource: pattern.to_string(),
            effect: OpenCodeRuleEffect::Deny,
        });
    }

    Ok(rules)
}

fn build_opencode_task_files_rules(
    canonical_workdir: &Path,
    allowed_write_paths: &BTreeSet<String>,
) -> Result<Vec<OpenCodePermissionRule>, WorkerError> {
    let workdir_str = validate_permission_pattern_path(canonical_workdir, "working_directory")?;
    if workdir_str.trim_end_matches('/').is_empty() {
        return Err(WorkerError::InvalidSpec(
            "refusing to grant `task_files` permissions on empty root directory".to_string(),
        ));
    }

    let mut rules = Vec::new();

    // 1. Ordered deny* baseline and explicit non-file/external action denies.
    for action in [
        "*",
        "external_directory",
        "shell",
        "subagent",
        "execute",
        "web",
        "edit",
    ] {
        rules.push(OpenCodePermissionRule {
            action: action.to_string(),
            resource: "*".to_string(),
            effect: OpenCodeRuleEffect::Deny,
        });
    }

    // 2. Allow location-relative internal read/glob/grep inspection.
    for action in ["read", "glob", "grep"] {
        rules.push(OpenCodePermissionRule {
            action: action.to_string(),
            resource: "*".to_string(),
            effect: OpenCodeRuleEffect::Allow,
        });
    }

    // 3. Grant exact repository-relative `edit` paths, plus `<dir>/**` descendants only when the
    // entry is an explicit trailing-slash directory scope.
    for entry in allowed_write_paths {
        let has_trailing_slash = entry.ends_with('/');
        let clean_entry = entry.strip_suffix('/').unwrap_or(entry.as_str());
        if clean_entry.is_empty() || is_control_config_path(clean_entry) {
            return Err(WorkerError::InvalidSpec(format!(
                "allowed_write_paths entry `{entry}` targets protected repository or agent control configuration"
            )));
        }
        validate_permission_pattern_str(clean_entry, "allowed_write_paths entry")?;

        let joined_target = canonical_workdir.join(clean_entry);
        let _ = validate_absolute_no_symlink_components(
            &joined_target,
            "task_files allowed_write_paths target",
        )?;

        let target_meta = fs::symlink_metadata(&joined_target).ok();
        if has_trailing_slash
            && target_meta
                .as_ref()
                .is_some_and(|meta| !meta.file_type().is_dir())
        {
            return Err(WorkerError::InvalidSpec(format!(
                "allowed_write_paths directory scope `{entry}` targets existing non-directory `{}`",
                joined_target.display()
            )));
        }

        rules.push(OpenCodePermissionRule {
            action: "edit".to_string(),
            resource: clean_entry.to_string(),
            effect: OpenCodeRuleEffect::Allow,
        });
        if has_trailing_slash {
            rules.push(OpenCodePermissionRule {
                action: "edit".to_string(),
                resource: format!("{clean_entry}/**"),
                effect: OpenCodeRuleEffect::Allow,
            });
        }
    }

    // 4. Append `.env` and protected control denies LAST so last-matching-rule evaluation
    // overrides both `read`/`glob`/`grep` `*` and any enclosing directory `edit <dir>/**` rule.
    for pattern in OPENCODE_PROTECTED_READ_AND_EDIT_PATTERNS {
        for action in ["read", "glob", "grep", "edit"] {
            rules.push(OpenCodePermissionRule {
                action: action.to_string(),
                resource: pattern.to_string(),
                effect: OpenCodeRuleEffect::Deny,
            });
        }
    }
    for pattern in OPENCODE_PROTECTED_EDIT_ONLY_PATTERNS {
        rules.push(OpenCodePermissionRule {
            action: "edit".to_string(),
            resource: pattern.to_string(),
            effect: OpenCodeRuleEffect::Deny,
        });
    }

    Ok(rules)
}

/// Deterministically evaluates an ordered slice of OpenCode V2 [`OpenCodePermissionRule`] rules
/// for `(action, resource)` using OpenCode 2.2.0 rule semantics over the generated configuration:
/// - `write`, `edit`, `patch`, `apply_patch`, and `multiedit` map to the `"edit"` action;
/// - `bash` and `shell` map to `"shell"`;
/// - `task` and `subagent` map to `"subagent"`;
/// - `webfetch`, `websearch`, and `web` map to `"web"`;
/// - internal file actions (`read`, `glob`, `grep`, `edit`) are location-relative, so absolute
///   paths or `..` traversal targets require `external_directory` permission first;
/// - rules are evaluated in order and the **last matching rule wins** (defaulting to [`OpenCodeRuleEffect::Deny`]).
///
/// Note: This helper evaluates generated `OPENCODE_CONFIG_CONTENT` rule structures for
/// deterministic unit and fixture verification; it is not live runtime proof of the external
/// OpenCode binary's internal evaluator, and provider configuration is a session capability
/// policy rather than an OS-level sandbox.
pub fn evaluate_opencode_rules(
    rules: &[OpenCodePermissionRule],
    action: &str,
    resource: &str,
) -> OpenCodeRuleEffect {
    let normalized_action = normalize_opencode_action(action);
    let normalized_resource = resource.strip_prefix("./").unwrap_or(resource);

    if matches!(normalized_action, "read" | "glob" | "grep" | "edit")
        && is_external_or_traversal_resource(normalized_resource)
        && evaluate_opencode_rules_raw(rules, "external_directory", normalized_resource)
            != OpenCodeRuleEffect::Allow
    {
        return OpenCodeRuleEffect::Deny;
    }

    evaluate_opencode_rules_raw(rules, normalized_action, normalized_resource)
}

impl OpenCodeConfigContent {
    /// Evaluates a policy action (such as `"provider.use"` or `"mcp.use"`) against `self.policies`
    /// using OpenCode V2 last-matching-rule semantics.
    pub fn evaluate_policy(&self, action: &str, resource: &str) -> OpenCodeRuleEffect {
        evaluate_opencode_rules(&self.policies, action, resource)
    }

    /// Evaluates a permission action against global `self.permissions` plus any agent-specific
    /// rules for `agent` (which append after global rules in OpenCode V2).
    pub fn evaluate_permission(
        &self,
        agent: Option<&str>,
        action: &str,
        resource: &str,
    ) -> OpenCodeRuleEffect {
        let mut combined: Vec<OpenCodePermissionRule> =
            self.permissions.clone().unwrap_or_default();
        if let Some(agent_name) = agent {
            if let Some(agent_cfg) = self.agents.get(agent_name) {
                if let Some(ref agent_rules) = agent_cfg.permissions {
                    combined.extend(agent_rules.iter().cloned());
                }
            }
        }
        evaluate_opencode_rules(&combined, action, resource)
    }
}

fn is_external_or_traversal_resource(resource: &str) -> bool {
    resource.is_empty()
        || resource.starts_with('/')
        || resource.contains('\\')
        || resource.split('/').any(|seg| seg == "..")
}

fn evaluate_opencode_rules_raw(
    rules: &[OpenCodePermissionRule],
    normalized_action: &str,
    normalized_resource: &str,
) -> OpenCodeRuleEffect {
    let mut effect = OpenCodeRuleEffect::Deny;

    for rule in rules {
        let action_matches = rule.action == "*" || rule.action == normalized_action;
        if action_matches && opencode_glob_matches(&rule.resource, normalized_resource) {
            effect = rule.effect;
        }
    }

    effect
}

fn normalize_opencode_action(action: &str) -> &str {
    match action {
        "write" | "edit" | "patch" | "apply_patch" | "multiedit" => "edit",
        "bash" | "shell" => "shell",
        "task" | "subagent" => "subagent",
        "webfetch" | "websearch" | "web" => "web",
        other => other,
    }
}

fn opencode_glob_matches(pattern: &str, target: &str) -> bool {
    if pattern == "*" || pattern == "**" {
        return true;
    }
    let pat_segments: Vec<&str> = pattern.split('/').collect();
    let tgt_segments: Vec<&str> = target.split('/').collect();
    match_segments(&pat_segments, &tgt_segments)
}

fn match_segments(pat: &[&str], tgt: &[&str]) -> bool {
    if pat.is_empty() {
        return tgt.is_empty();
    }
    if pat[0] == "**" {
        if pat.len() == 1 {
            return !tgt.is_empty() && tgt.iter().all(|s| !s.is_empty());
        }
        if match_segments(&pat[1..], tgt) {
            return true;
        }
        if !tgt.is_empty() {
            return match_segments(pat, &tgt[1..]);
        }
        return false;
    }
    if tgt.is_empty() {
        return false;
    }
    if match_single_segment(pat[0], tgt[0]) {
        match_segments(&pat[1..], &tgt[1..])
    } else {
        false
    }
}

fn match_single_segment(pat: &str, tgt: &str) -> bool {
    if pat == "*" {
        return !tgt.is_empty();
    }
    let p_bytes = pat.as_bytes();
    let t_bytes = tgt.as_bytes();
    let (mut p_idx, mut t_idx) = (0, 0);
    let (mut star_idx, mut match_idx) = (None, 0);

    while t_idx < t_bytes.len() {
        if p_idx < p_bytes.len() && p_bytes[p_idx] == t_bytes[t_idx] {
            p_idx += 1;
            t_idx += 1;
        } else if p_idx < p_bytes.len() && p_bytes[p_idx] == b'*' {
            star_idx = Some(p_idx);
            match_idx = t_idx;
            p_idx += 1;
        } else if let Some(s_idx) = star_idx {
            p_idx = s_idx + 1;
            match_idx += 1;
            t_idx = match_idx;
        } else {
            return false;
        }
    }

    while p_idx < p_bytes.len() && p_bytes[p_idx] == b'*' {
        p_idx += 1;
    }

    p_idx == p_bytes.len()
}
