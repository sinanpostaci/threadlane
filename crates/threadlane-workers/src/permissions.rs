use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::error::WorkerError;
use crate::fs_safety::{validate_absolute_no_symlink_components, validate_existing_canonical_dir};
use crate::spec::{
    is_control_config_path, validate_repo_relative_path, validate_simple_identifier,
    ResolvedModelTarget, WorkerProvider,
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
/// Provider permission flags and per-child configuration are session-only hints; they are **not**
/// an OS-level sandbox, do **not** replace the mandatory post-run Git diff gate, and do **not**
/// guarantee global deny isolation if user-inherited provider settings grant broader rules.
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
}

impl WorkerFilePermissions {
    /// Returns the canonical `snake_case` representation (`"inherit"` or `"task_files"`).
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Inherit => "inherit",
            Self::TaskFiles => "task_files",
        }
    }

    /// Returns `true` when constructing the final launch argv for Antigravity requires the
    /// canonical task working directory (`TaskFiles`).
    pub fn requires_workdir_argv(self) -> bool {
        matches!(self, Self::TaskFiles)
    }

    /// Validates that this permission mode is supported for `provider` and that all
    /// `allowed_write_paths` entries are valid, within finite `TaskFiles` bounds, and free of
    /// unsafe glob/rule metacharacters.
    pub fn validate_for_provider_and_paths(
        self,
        provider: WorkerProvider,
        allowed_write_paths: &BTreeSet<String>,
    ) -> Result<(), WorkerError> {
        let _ = provider;

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
    /// Optional ordered V2 permission rules (present when `file_permissions` is [`WorkerFilePermissions::TaskFiles`]).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub permissions: Option<Vec<OpenCodePermissionRule>>,
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
        let clean_entry = entry.strip_suffix('/').unwrap_or(entry.as_str());
        let joined_target = canonical_workdir.join(clean_entry);
        let _ = validate_absolute_no_symlink_components(
            &joined_target,
            "task_files allowed_write_paths target",
        )?;

        let target_str = format!("{clean_workdir}/{clean_entry}");
        for tool in AGY_WRITE_PERMISSION_TOOLS {
            allow.push(format!("{tool}({target_str})"));
            allow.push(format!("{tool}({target_str}/**)"));
        }
    }

    let settings = AntigravitySessionSettings {
        permissions: AntigravityPermissionsBlock { allow },
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
    // Only the parent supervisor may dispatch separately scoped workers.
    for resource in ["subagent", "task", "agent"] {
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

/// Evaluates an ordered slice of OpenCode V2 [`OpenCodePermissionRule`] rules for `(action, resource)`
/// using OpenCode 2.2.0 semantics:
/// - `write`, `edit`, `patch`, `apply_patch`, and `multiedit` map to the `"edit"` action;
/// - `bash` and `shell` map to `"shell"`;
/// - `task` and `subagent` map to `"subagent"`;
/// - `webfetch`, `websearch`, and `web` map to `"web"`;
/// - internal file actions (`read`, `glob`, `grep`, `edit`) are location-relative, so absolute
///   paths or `..` traversal targets require `external_directory` permission first;
/// - rules are evaluated in order and the **last matching rule wins** (defaulting to [`OpenCodeRuleEffect::Deny`]).
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
