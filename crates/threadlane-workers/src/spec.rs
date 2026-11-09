use std::collections::{BTreeMap, BTreeSet};
use std::path::{Component, Path, PathBuf};
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::error::WorkerError;
use crate::fs_safety::{
    ensure_platform_supported, validate_absolute_no_symlink_components, validate_executable_file,
    validate_existing_canonical_dir,
};
pub use crate::permissions::{
    build_antigravity_read_only_settings, build_antigravity_read_only_settings_json,
    build_codex_task_files_permission_profile_config,
    build_codex_task_files_permission_profile_config_for_scopes, build_opencode_config_content,
    build_opencode_config_content_json, evaluate_antigravity_settings,
    evaluate_codex_task_files_permission_profile_config, evaluate_opencode_rules,
    is_unsafe_codex_toml_scope_char, parse_and_validate_codex_task_files_permission_profile_config,
    validate_advisory_provider_and_mode, validate_advisory_read_only_workdir,
    validate_codex_advisory_workdir, validate_codex_task_files_scope_str,
    validate_codex_task_files_workdir, AntigravityPermissionsBlock, AntigravitySessionSettings,
    OpenCodeAgentConfig, OpenCodeConfigContent, OpenCodeModelObject, OpenCodePermissionRule,
    OpenCodeRuleEffect, WorkerFilePermissions, AGY_DENIED_EXECUTION_AND_DELEGATION_TOOLS,
    AGY_PROTECTED_READ_DENY_PATTERNS, CODEX_ADVISORY_GLOB_SCAN_DEPTH_MARGIN,
    CODEX_ADVISORY_GLOB_SCAN_MAX_DEPTH, CODEX_ADVISORY_MAX_WORKDIR_DEPTH,
    CODEX_ADVISORY_MAX_WORKDIR_ENTRIES, CODEX_PROJECT_CONFIG_MARKERS,
    CODEX_TASK_FILES_PERMISSION_PROFILE_NAME, CODEX_TASK_FILES_PROTECTED_DENY_PATTERNS,
    MAX_OPENCODE_CONFIG_BYTES, MAX_TASK_FILES_SETTINGS_BYTES, MAX_TASK_FILES_WRITE_PATHS,
    OPENCODE_CONFIG_ENV_VAR, OPENCODE_DEFAULT_AGENT, OPENCODE_PINNED_AGENTS,
    OPENCODE_READ_ONLY_DENIED_PERMISSION_ACTIONS, OPENCODE_READ_ONLY_DENIED_TOOL_POLICIES,
};
use crate::permissions::{
    build_antigravity_task_files_prompt_for_scopes,
    build_antigravity_task_files_settings_json_for_scopes, validate_permission_pattern_path,
};

/// Default execution timeout (5 minutes in milliseconds).
pub const DEFAULT_TIMEOUT_MS: u64 = 300_000;

/// Maximum permitted execution timeout (1 hour in milliseconds).
pub const MAX_TIMEOUT_MS: u64 = 3_600_000;

/// Default maximum byte length of a single NDJSON stdout line (256 KiB).
pub const DEFAULT_MAX_LINE_BYTES: usize = 262_144;

/// Maximum configurable byte length of a single NDJSON stdout line (4 MiB).
pub const MAX_LINE_BYTES: usize = 4_194_304;

/// Default maximum total stdout bytes captured from a worker run (4 MiB).
pub const DEFAULT_MAX_STDOUT_BYTES: usize = 4_194_304;

/// Maximum configurable total stdout bytes captured from a worker run (32 MiB).
pub const MAX_STDOUT_BYTES: usize = 33_554_432;

/// Default maximum total stderr bytes captured from a worker run (256 KiB).
pub const DEFAULT_MAX_STDERR_BYTES: usize = 262_144;

/// Maximum configurable total stderr bytes captured from a worker run (4 MiB).
pub const MAX_STDERR_BYTES: usize = 4_194_304;

/// Default maximum number of normalized stream events retained in memory.
pub const DEFAULT_MAX_EVENTS: usize = 256;

/// Maximum configurable number of normalized stream events retained in memory.
pub const MAX_EVENTS: usize = 4_096;

/// Maximum permitted prompt size in UTF-8 bytes (64 KiB).
///
/// Kept conservatively below Linux's 128 KiB per-argument `MAX_ARG_STRLEN` (`E2BIG`) kernel ceiling.
pub const MAX_PROMPT_BYTES: usize = 65_536;

/// Maximum byte length for model, effort, session, run, and task identifiers.
pub const MAX_IDENTIFIER_LEN: usize = 128;

/// Maximum byte length for a repository-relative allowed write path entry.
pub const MAX_PATH_ENTRY_LEN: usize = 512;

/// Session-only named permission profile selected for [`WorkerProvider::CodexCli`] read-only advisory runs.
pub const CODEX_ADVISORY_PERMISSION_PROFILE_NAME: &str = "threadlane-advisory";

/// Session-only TOML inline table `-c` override defining the `threadlane-advisory` read-only
/// permission profile for [`WorkerProvider::CodexCli`].
///
/// Scopes filesystem reads to `:minimal`, `/nix/store`, and `.` inside `:workspace_roots` with
/// explicit `glob_scan_max_depth=64` ([`CODEX_ADVISORY_GLOB_SCAN_MAX_DEPTH`]) and recursive exact
/// plus descendant denials (`**/.env`, `**/.env.*`, `**/codex.toml`, `**/.codex.toml`, `**/.git`,
/// `**/.git/**`, `**/.codex`, `**/.codex/**`, `**/.agents`, `**/.agents/**`, `**/.aws`,
/// `**/.aws/**`), and disables network access (`"network"={"enabled"=false}`). Never combined with
/// legacy `--sandbox` flags.
pub const CODEX_ADVISORY_PERMISSION_PROFILE_CONFIG: &str = r#"permissions.threadlane-advisory={"filesystem"={"glob_scan_max_depth"=64,":minimal"="read",":workspace_roots"={"."="read","**/.env"="deny","**/.env.*"="deny","**/codex.toml"="deny","**/.codex.toml"="deny","**/.git"="deny","**/.git/**"="deny","**/.codex"="deny","**/.codex/**"="deny","**/.agents"="deny","**/.agents/**"="deny","**/.aws"="deny","**/.aws/**"="deny"},"/nix/store"="read"},"network"={"enabled"=false}}"#;

/// Features explicitly disabled via `--disable <feature>` on [`WorkerProvider::CodexCli`]
/// read-only advisory invocations.
pub const CODEX_ADVISORY_DISABLED_FEATURES: [&str; 12] = [
    "hooks",
    "plugins",
    "apps",
    "multi_agent",
    "browser_use",
    "computer_use",
    "in_app_browser",
    "image_generation",
    "memories",
    "skill_search",
    "skill_mcp_dependency_install",
    "shell_snapshot",
];

/// Supported external worker CLI providers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum WorkerProvider {
    /// Google Antigravity (`agy`) CLI using `--output-format stream-json`.
    #[serde(rename = "antigravity", alias = "agy")]
    Antigravity,
    /// OpenCode v2 (`opencode`) CLI using `run --model provider/model#variant --format json`.
    #[serde(
        rename = "opencode_v2",
        alias = "open_code_v2",
        alias = "opencode-v2",
        alias = "opencode"
    )]
    OpenCodeV2,
    /// OpenAI Codex (`codex`) CLI in non-interactive scoped-task or read-only advisory mode.
    #[serde(rename = "codex_cli", alias = "codex-cli", alias = "codex")]
    CodexCli,
}

impl WorkerProvider {
    /// Returns the canonical snake_case provider identifier.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Antigravity => "antigravity",
            Self::OpenCodeV2 => "opencode_v2",
            Self::CodexCli => "codex_cli",
        }
    }
}

/// Finite execution timeout and stream capture budgets for a worker invocation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionBudgets {
    /// Maximum wall-clock runtime in milliseconds (`1..=MAX_TIMEOUT_MS`).
    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: u64,
    /// Maximum bytes allowed in a single stdout NDJSON line (`1..=MAX_LINE_BYTES`).
    #[serde(default = "default_max_line_bytes")]
    pub max_line_bytes: usize,
    /// Maximum total bytes allowed across all stdout output (`1..=MAX_STDOUT_BYTES`).
    #[serde(default = "default_max_stdout_bytes")]
    pub max_stdout_bytes: usize,
    /// Maximum total bytes allowed across all stderr output (`1..=MAX_STDERR_BYTES`).
    #[serde(default = "default_max_stderr_bytes")]
    pub max_stderr_bytes: usize,
    /// Maximum number of normalized NDJSON events retained in the report (`1..=MAX_EVENTS`).
    #[serde(default = "default_max_events")]
    pub max_events: usize,
    /// Optional advisory soft policy. Absent policies derive thresholds within `timeout_ms`.
    /// Omission preserves the serialized shape and digest of existing frozen budgets.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub progress_watchdog: Option<crate::progress_watchdog::ProgressWatchdogPolicy>,
}

const fn default_timeout_ms() -> u64 {
    DEFAULT_TIMEOUT_MS
}

const fn default_max_line_bytes() -> usize {
    DEFAULT_MAX_LINE_BYTES
}

const fn default_max_stdout_bytes() -> usize {
    DEFAULT_MAX_STDOUT_BYTES
}

const fn default_max_stderr_bytes() -> usize {
    DEFAULT_MAX_STDERR_BYTES
}

const fn default_max_events() -> usize {
    DEFAULT_MAX_EVENTS
}

impl Default for ExecutionBudgets {
    fn default() -> Self {
        Self {
            timeout_ms: DEFAULT_TIMEOUT_MS,
            max_line_bytes: DEFAULT_MAX_LINE_BYTES,
            max_stdout_bytes: DEFAULT_MAX_STDOUT_BYTES,
            max_stderr_bytes: DEFAULT_MAX_STDERR_BYTES,
            max_events: DEFAULT_MAX_EVENTS,
            progress_watchdog: None,
        }
    }
}

impl ExecutionBudgets {
    /// Validates that all execution and output budgets are finite, non-zero, and within hard ceilings.
    pub fn validate(&self) -> Result<(), WorkerError> {
        if self.timeout_ms == 0 || self.timeout_ms > MAX_TIMEOUT_MS {
            return Err(WorkerError::InvalidSpec(format!(
                "timeout_ms must be between 1 and {MAX_TIMEOUT_MS} (got {})",
                self.timeout_ms
            )));
        }
        if self.max_line_bytes == 0 || self.max_line_bytes > MAX_LINE_BYTES {
            return Err(WorkerError::InvalidSpec(format!(
                "max_line_bytes must be between 1 and {MAX_LINE_BYTES} (got {})",
                self.max_line_bytes
            )));
        }
        if self.max_stdout_bytes == 0 || self.max_stdout_bytes > MAX_STDOUT_BYTES {
            return Err(WorkerError::InvalidSpec(format!(
                "max_stdout_bytes must be between 1 and {MAX_STDOUT_BYTES} (got {})",
                self.max_stdout_bytes
            )));
        }
        if self.max_stdout_bytes < self.max_line_bytes {
            return Err(WorkerError::InvalidSpec(format!(
                "max_stdout_bytes ({}) must be greater than or equal to max_line_bytes ({})",
                self.max_stdout_bytes, self.max_line_bytes
            )));
        }
        if self.max_stderr_bytes == 0 || self.max_stderr_bytes > MAX_STDERR_BYTES {
            return Err(WorkerError::InvalidSpec(format!(
                "max_stderr_bytes must be between 1 and {MAX_STDERR_BYTES} (got {})",
                self.max_stderr_bytes
            )));
        }
        if self.max_events == 0 || self.max_events > MAX_EVENTS {
            return Err(WorkerError::InvalidSpec(format!(
                "max_events must be between 1 and {MAX_EVENTS} (got {})",
                self.max_events
            )));
        }
        if let Some(policy) = &self.progress_watchdog {
            policy.validate(self.timeout_ms)?;
        }
        Ok(())
    }

    /// Returns the configured timeout as a [`Duration`].
    pub fn timeout_duration(&self) -> Duration {
        Duration::from_millis(self.timeout_ms)
    }
}

/// Optional external trusted policy constraining permitted executables and models.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerPolicy {
    /// Explicit set of permitted canonical executable paths.
    #[serde(default)]
    pub allowed_executables: BTreeSet<PathBuf>,
    /// Explicit set of permitted model identifiers (`model` or `provider/model#variant`).
    #[serde(default)]
    pub allowed_models: BTreeSet<String>,
}

/// Validated, normalized model and effort/variant target derived from a [`WorkerSpec`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolvedModelTarget {
    /// Base requested model identifier (`gemini-4-argon-high` or `provider/model`).
    pub base_model: String,
    /// Exact requested effort or variant (`high`, `max`, etc.) if applicable.
    pub effort: Option<String>,
    /// Exact string passed to the provider CLI `--model` flag.
    pub cli_model_arg: String,
}

/// Typed, pre-validated specification for launching an isolated worker process.
///
/// Credentials are never embedded in `WorkerSpec` and remain strictly provider-managed.
/// All prompts and worker output are treated as untrusted data.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerSpec {
    /// Target worker CLI provider (`antigravity` or `opencode_v2`).
    pub provider: WorkerProvider,
    /// Absolute path to the non-symlink worker executable.
    pub executable: PathBuf,
    /// Exact requested model identifier (`gemini-4-argon-high` or `provider/model` / `provider/model#variant`).
    pub model: String,
    /// Exact requested effort or variant (`high` for Antigravity; `#variant` for OpenCode v2).
    #[serde(default, alias = "variant")]
    pub effort: Option<String>,
    /// Optional explicit trusted allowlist of permitted model identifiers.
    #[serde(default)]
    pub allowed_models: BTreeSet<String>,
    /// Optional session identifier (`--session <ID>`, supported only for `opencode_v2`).
    #[serde(default)]
    pub session_id: Option<String>,
    /// Whether to pass the global `--standalone` flag before `run` (required `true` for `opencode_v2`; must be `false` for `antigravity`).
    #[serde(default)]
    pub standalone: bool,
    /// Trusted canonical target Git repository path.
    pub repository_path: PathBuf,
    /// Trusted base commit reference or 40-hex commit SHA.
    pub base_commit: String,
    /// Trusted private work-area root directory where owned worktrees and manifests are stored.
    pub work_area_root: PathBuf,
    /// Non-empty set of repository-relative paths/directories the worker is permitted to modify.
    pub allowed_write_paths: BTreeSet<String>,
    /// Untrusted task prompt passed as data in the argument array.
    pub prompt: String,
    /// Trusted session-scoped provider file permission mode (`inherit` by default, or opt-in
    /// `task_files` for Antigravity).
    #[serde(default)]
    pub file_permissions: WorkerFilePermissions,
    /// Finite execution timeout and output capture budgets.
    #[serde(default)]
    pub budgets: ExecutionBudgets,
}

/// Structured result of statically validating a [`WorkerSpec`] without launching the worker.
///
/// Static validation is side-effect-free and does not create or require the per-task checkout
/// directory to exist yet. When `file_permissions` is [`WorkerFilePermissions::TaskFiles`],
/// `constructed_argv` is the static base preview without `--settings`,
/// `workdir_argv_resolution_required` is `true`, and `argv_resolution_note` documents that the
/// actual launch argv is resolved against the canonical task working directory via
/// [`WorkerSpec::build_argv_for_workdir`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SpecValidationReport {
    /// Validated provider.
    pub provider: WorkerProvider,
    /// Validated provider file permission mode (`inherit` or `task_files`).
    #[serde(default)]
    pub file_permissions: WorkerFilePermissions,
    /// True when `constructed_argv` is only a static base preview and the actual launch argv
    /// requires canonical working-directory resolution via [`WorkerSpec::build_argv_for_workdir`].
    #[serde(default)]
    pub workdir_argv_resolution_required: bool,
    /// Explicit note when final launch argv depends on resolving the canonical task working
    /// directory (`Some(...)` when `file_permissions == TaskFiles`, `None` when `Inherit`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub argv_resolution_note: Option<String>,
    /// Canonical executable path.
    pub canonical_executable: PathBuf,
    /// Canonical repository path.
    pub canonical_repository_path: PathBuf,
    /// Validated work area root path.
    pub work_area_root: PathBuf,
    /// Resolved model and effort/variant details.
    pub resolved_model: ResolvedModelTarget,
    /// Static base argv array (excluding `argv[0]`).
    ///
    /// When `file_permissions` is [`WorkerFilePermissions::Inherit`], this is the full launch argv.
    /// When `file_permissions` is [`WorkerFilePermissions::TaskFiles`], session-only
    /// `--settings <inline-json>` is omitted here and resolved against the canonical working
    /// directory by [`WorkerSpec::build_argv_for_workdir`].
    pub constructed_argv: Vec<String>,
    /// Validated repository-relative write allowlist.
    pub allowed_write_paths: BTreeSet<String>,
    /// Validated execution budgets.
    pub budgets: ExecutionBudgets,
}

impl WorkerSpec {
    /// Validates all static fields of the worker spec before launch and returns a [`SpecValidationReport`].
    pub fn validate(&self) -> Result<SpecValidationReport, WorkerError> {
        ensure_platform_supported()?;
        self.budgets.validate()?;

        let canonical_executable = validate_executable_file(&self.executable)?;
        let canonical_repository_path =
            validate_existing_canonical_dir(&self.repository_path, "repository_path")?;
        let work_area_root =
            validate_absolute_no_symlink_components(&self.work_area_root, "work_area_root")?;

        if work_area_root.starts_with(&canonical_repository_path) {
            return Err(WorkerError::InvalidSpec(format!(
                "work_area_root `{}` must reside outside repository_path `{}`",
                work_area_root.display(),
                canonical_repository_path.display()
            )));
        }

        validate_commit_ref(&self.base_commit)?;
        let resolved_model = self.resolve_model_target()?;

        if !self.allowed_models.is_empty()
            && !self.allowed_models.contains(&self.model)
            && !self.allowed_models.contains(&resolved_model.base_model)
            && !self.allowed_models.contains(&resolved_model.cli_model_arg)
        {
            return Err(WorkerError::DisallowedModel {
                model: resolved_model.cli_model_arg,
            });
        }

        match self.provider {
            WorkerProvider::Antigravity => {
                if self.session_id.is_some() {
                    return Err(WorkerError::InvalidSpec(
                        "`session_id` (`--session`) is only supported for provider `opencode_v2`"
                            .to_string(),
                    ));
                }
                if self.standalone {
                    return Err(WorkerError::InvalidSpec(
                        "`standalone` (`--standalone`) is only supported for provider `opencode_v2`"
                            .to_string(),
                    ));
                }
            }
            WorkerProvider::OpenCodeV2 => {
                if !self.standalone {
                    return Err(WorkerError::InvalidSpec(
                        "provider `opencode_v2` requires `standalone: true` (`--standalone`) so each worker runs an isolated private server rather than attaching to an unowned shared background server"
                            .to_string(),
                    ));
                }
                if let Some(ref session_id) = self.session_id {
                    validate_simple_identifier(session_id, "session_id")?;
                }
            }
            WorkerProvider::CodexCli => {
                if self.session_id.is_some() {
                    return Err(WorkerError::InvalidSpec(
                        "`session_id` (`--session`) is only supported for provider `opencode_v2`; `codex_cli` worker runs must use `--ephemeral`"
                            .to_string(),
                    ));
                }
                if self.standalone {
                    return Err(WorkerError::InvalidSpec(
                        "`standalone` (`--standalone`) is only supported for provider `opencode_v2`"
                            .to_string(),
                    ));
                }
                if self.file_permissions == WorkerFilePermissions::Inherit {
                    return Err(WorkerError::UnsupportedCapability {
                        provider: self.provider.as_str().to_string(),
                        capability: format!(
                            "worker_file_permissions:{}",
                            self.file_permissions.as_str()
                        ),
                        reason: "provider `codex_cli` refuses unrestricted `file_permissions: inherit`; mutable Codex worker invocations require `file_permissions: task_files` with explicit approved write scopes"
                            .to_string(),
                    });
                }
            }
        }

        if self.allowed_write_paths.is_empty() {
            return Err(WorkerError::InvalidSpec(
                "allowed_write_paths must contain at least one repository-relative path"
                    .to_string(),
            ));
        }
        if self.file_permissions == WorkerFilePermissions::ReadOnly {
            return Err(WorkerError::InvalidSpec(
                "`file_permissions: read_only` requires `AdvisoryWorkerSpec` with zero write scopes; `WorkerSpec` is only for mutable task worktrees (`inherit` or `task_files`)"
                    .to_string(),
            ));
        }

        self.file_permissions
            .validate_for_provider_and_paths(self.provider, &self.allowed_write_paths)?;
        if self.file_permissions.requires_workdir_argv() {
            let _ = validate_permission_pattern_path(&work_area_root, "work_area_root")?;
        }

        validate_prompt(&self.prompt)?;
        let constructed_argv = self.build_argv_from_resolved(&resolved_model, None);
        let workdir_argv_resolution_required = matches!(
            self.provider,
            WorkerProvider::Antigravity | WorkerProvider::CodexCli
        ) && self.file_permissions.requires_workdir_argv();
        let argv_resolution_note = workdir_argv_resolution_required.then(|| {
            match self.provider {
                WorkerProvider::CodexCli => format!(
                    "file_permissions is `{}`; `constructed_argv` is the static base preview without `-c permissions.{}=...`, which is resolved from the canonical task working directory and base commit via `WorkerSpec::build_argv_for_workdir`.",
                    self.file_permissions.as_str(),
                    CODEX_TASK_FILES_PERMISSION_PROFILE_NAME
                ),
                _ => format!(
                    "file_permissions is `{}`; `constructed_argv` is the static base preview without `--settings`, which is resolved from the canonical task working directory via `WorkerSpec::build_argv_for_workdir`.",
                    self.file_permissions.as_str()
                ),
            }
        });

        Ok(SpecValidationReport {
            provider: self.provider,
            file_permissions: self.file_permissions,
            workdir_argv_resolution_required,
            argv_resolution_note,
            canonical_executable,
            canonical_repository_path,
            work_area_root,
            resolved_model,
            constructed_argv,
            allowed_write_paths: self.allowed_write_paths.clone(),
            budgets: self.budgets.clone(),
        })
    }

    /// Validates the spec and enforces an external trusted [`WorkerPolicy`].
    pub fn validate_against_policy(
        &self,
        policy: &WorkerPolicy,
    ) -> Result<SpecValidationReport, WorkerError> {
        let report = self.validate()?;
        if !policy.allowed_executables.is_empty()
            && !policy
                .allowed_executables
                .contains(&report.canonical_executable)
            && !policy.allowed_executables.contains(&self.executable)
        {
            return Err(WorkerError::DisallowedExecutable {
                executable: report.canonical_executable,
            });
        }
        if !policy.allowed_models.is_empty()
            && !policy.allowed_models.contains(&self.model)
            && !policy
                .allowed_models
                .contains(&report.resolved_model.base_model)
            && !policy
                .allowed_models
                .contains(&report.resolved_model.cli_model_arg)
        {
            return Err(WorkerError::DisallowedModel {
                model: report.resolved_model.cli_model_arg,
            });
        }
        Ok(report)
    }

    /// Resolves and validates the provider-specific model and effort/variant identifiers.
    pub fn resolve_model_target(&self) -> Result<ResolvedModelTarget, WorkerError> {
        resolve_model_target(self.provider, &self.model, self.effort.as_deref())
    }

    /// Validates the spec and constructs the static CLI argument array (excluding `argv[0]`)
    /// when `file_permissions` is [`WorkerFilePermissions::Inherit`].
    ///
    /// Returns [`WorkerError::InvalidSpec`] when `file_permissions` is
    /// [`WorkerFilePermissions::TaskFiles`] or [`WorkerFilePermissions::ReadOnly`] on Antigravity
    /// because session-only `--settings <inline-json>` requires validating and interpolating the
    /// actual canonical working directory via [`WorkerSpec::build_argv_for_workdir`].
    ///
    /// Never uses shell interpolation, never invents unsupported flags (such as `--variant`),
    /// and never adds `--dangerously-skip-permissions` or `--auto`.
    pub fn build_argv(&self) -> Result<Vec<String>, WorkerError> {
        let report = self.validate()?;
        if report.workdir_argv_resolution_required {
            return Err(WorkerError::InvalidSpec(format!(
                "`file_permissions: {}` requires a canonical task working directory to construct session-only `--settings`; call `WorkerSpec::build_argv_for_workdir`",
                self.file_permissions.as_str()
            )));
        }
        Ok(report.constructed_argv)
    }

    /// Validates the spec and `working_directory` and constructs the real launch CLI argument
    /// array (excluding `argv[0]`) for `working_directory`.
    ///
    /// When `file_permissions` is [`WorkerFilePermissions::Inherit`], returns the base provider
    /// argument array (while still validating OpenCode V2 per-child configuration when the
    /// provider is [`WorkerProvider::OpenCodeV2`]). When `file_permissions` is
    /// [`WorkerFilePermissions::TaskFiles`], validates that the canonical `working_directory` and
    /// `allowed_write_paths` contain no unsafe glob/rule metacharacters or symlinks and, for
    /// [`WorkerProvider::Antigravity`], resolves `allowed_write_paths` against the immutable
    /// pinned `base_commit` tree via [`crate::git::resolve_allowed_write_scopes_at_base`],
    /// injects `--settings <inline-json>` scoped to `working_directory` (granting descendant `/**`
    /// rules only for [`WriteScopeKind::DirectorySubtree`] scopes and exact-only rules for
    /// [`WriteScopeKind::ExactFile`] scopes), and prepends a session-local `task_files` execution
    /// contract ahead of the verbatim task instruction in the `--print` argument.
    /// [`WorkerFilePermissions::ReadOnly`] is rejected on [`WorkerSpec`] and must use
    /// [`AdvisoryWorkerSpec::build_argv_for_workdir`].
    pub fn build_argv_for_workdir(
        &self,
        working_directory: &Path,
    ) -> Result<Vec<String>, WorkerError> {
        let report = self.validate()?;
        let canonical_workdir =
            validate_existing_canonical_dir(working_directory, "working_directory")?;
        let (settings_json, effective_prompt) = match (self.provider, self.file_permissions) {
            (WorkerProvider::Antigravity, WorkerFilePermissions::Inherit) => (None, None),
            (WorkerProvider::Antigravity, WorkerFilePermissions::TaskFiles) => {
                let resolved_write_scopes = crate::git::resolve_allowed_write_scopes_at_base(
                    &report.canonical_repository_path,
                    &self.base_commit,
                    &self.allowed_write_paths,
                    crate::git::DEFAULT_GIT_COMMAND_TIMEOUT_MS,
                )?;
                let settings_json = build_antigravity_task_files_settings_json_for_scopes(
                    &canonical_workdir,
                    &resolved_write_scopes,
                )?;
                let guarded_prompt = build_antigravity_task_files_prompt_for_scopes(
                    &canonical_workdir,
                    &resolved_write_scopes,
                    &self.prompt,
                );
                (Some(settings_json), Some(guarded_prompt))
            }
            (WorkerProvider::Antigravity, WorkerFilePermissions::ReadOnly) => {
                return Err(WorkerError::InvalidSpec(
                    "`file_permissions: read_only` requires `AdvisoryWorkerSpec` with zero write scopes; `WorkerSpec` is only for mutable task worktrees (`inherit` or `task_files`)"
                        .to_string(),
                ));
            }
            (WorkerProvider::OpenCodeV2, _) => {
                let _ = self.build_opencode_config_for_workdir(&canonical_workdir)?;
                (None, None)
            }
            (WorkerProvider::CodexCli, WorkerFilePermissions::TaskFiles) => {
                let resolved_write_scopes = crate::git::resolve_allowed_write_scopes_at_base(
                    &report.canonical_repository_path,
                    &self.base_commit,
                    &self.allowed_write_paths,
                    crate::git::DEFAULT_GIT_COMMAND_TIMEOUT_MS,
                )?;
                for entry in &self.allowed_write_paths {
                    let (clean, expected_kind) = match entry.strip_suffix('/') {
                        Some(clean) => (clean, WriteScopeKind::DirectorySubtree),
                        None => (entry.as_str(), WriteScopeKind::ExactFile),
                    };
                    if resolved_write_scopes.get(clean) != Some(&expected_kind) {
                        return Err(WorkerError::InvalidSpec(format!(
                            "Codex write scope `{entry}` changed kind at base_commit; directory scopes require an explicit trailing `/`"
                        )));
                    }
                }
                (
                    Some(build_codex_task_files_permission_profile_config_for_scopes(
                        &canonical_workdir,
                        &resolved_write_scopes,
                    )?),
                    None,
                )
            }
            (WorkerProvider::CodexCli, WorkerFilePermissions::Inherit) => {
                return Err(WorkerError::UnsupportedCapability {
                    provider: self.provider.as_str().to_string(),
                    capability: format!(
                        "worker_file_permissions:{}",
                        self.file_permissions.as_str()
                    ),
                    reason: "provider `codex_cli` refuses unrestricted `file_permissions: inherit`; mutable Codex worker invocations require `file_permissions: task_files` with explicit approved write scopes".to_string(),
                });
            }
            (WorkerProvider::CodexCli, WorkerFilePermissions::ReadOnly) => {
                return Err(WorkerError::InvalidSpec(
                    "`file_permissions: read_only` requires `AdvisoryWorkerSpec` with zero write scopes; `WorkerSpec` is only for mutable task worktrees (`inherit` or `task_files`)"
                        .to_string(),
                ));
            }
        };
        Ok(build_provider_argv_from_resolved(
            self.provider,
            &report.resolved_model,
            "accept-edits",
            self.standalone,
            self.session_id.as_deref(),
            settings_json,
            effective_prompt.as_deref().unwrap_or(&self.prompt),
        ))
    }

    /// Builds the structured OpenCode 2.2.0 V2 [`OpenCodeConfigContent`] for this spec and
    /// `working_directory`.
    ///
    /// Returns [`WorkerError::InvalidSpec`] if `self.provider` is not [`WorkerProvider::OpenCodeV2`].
    pub fn build_opencode_config_content_for_workdir(
        &self,
        working_directory: &Path,
    ) -> Result<OpenCodeConfigContent, WorkerError> {
        let report = self.validate()?;
        if self.provider != WorkerProvider::OpenCodeV2 {
            return Err(WorkerError::InvalidSpec(format!(
                "`build_opencode_config_content_for_workdir` is only supported for provider `opencode_v2` (got `{}`)",
                self.provider.as_str()
            )));
        }
        build_opencode_config_content(
            &report.resolved_model,
            self.file_permissions,
            working_directory,
            &self.allowed_write_paths,
        )
    }

    /// Builds and serializes the bounded per-child `OPENCODE_CONFIG_CONTENT` JSON string for this
    /// spec and `working_directory`, enforcing [`MAX_OPENCODE_CONFIG_BYTES`] (`65_536` bytes).
    ///
    /// Returns [`WorkerError::InvalidSpec`] if `self.provider` is not [`WorkerProvider::OpenCodeV2`].
    pub fn build_opencode_config_for_workdir(
        &self,
        working_directory: &Path,
    ) -> Result<String, WorkerError> {
        let report = self.validate()?;
        if self.provider != WorkerProvider::OpenCodeV2 {
            return Err(WorkerError::InvalidSpec(format!(
                "`build_opencode_config_for_workdir` is only supported for provider `opencode_v2` (got `{}`)",
                self.provider.as_str()
            )));
        }
        build_opencode_config_content_json(
            &report.resolved_model,
            self.file_permissions,
            working_directory,
            &self.allowed_write_paths,
        )
    }

    /// Builds the per-child environment variable overrides for spawning this worker inside
    /// `working_directory`.
    ///
    /// For [`WorkerProvider::Antigravity`], returns an empty vector so legacy Antigravity launch
    /// behavior is completely unchanged. For [`WorkerProvider::OpenCodeV2`], returns
    /// `[("OPENCODE_CONFIG_CONTENT", <json>)]` so the child process receives bounded model/agent/
    /// policy (and optional V2 `TaskFiles` or `ReadOnly` permission) configuration overriding any
    /// inherited `OPENCODE_CONFIG_CONTENT` in the child environment only.
    pub fn build_child_env_for_workdir(
        &self,
        working_directory: &Path,
    ) -> Result<Vec<(String, String)>, WorkerError> {
        let _report = self.validate()?;
        let canonical_workdir =
            validate_existing_canonical_dir(working_directory, "working_directory")?;
        match self.provider {
            WorkerProvider::Antigravity => Ok(Vec::new()),
            WorkerProvider::OpenCodeV2 => {
                let config_json = self.build_opencode_config_for_workdir(&canonical_workdir)?;
                Ok(vec![(OPENCODE_CONFIG_ENV_VAR.to_string(), config_json)])
            }
            WorkerProvider::CodexCli => {
                validate_codex_task_files_workdir(&canonical_workdir)?;
                Ok(Vec::new())
            }
        }
    }

    fn build_argv_from_resolved(
        &self,
        resolved: &ResolvedModelTarget,
        settings_json: Option<String>,
    ) -> Vec<String> {
        build_provider_argv_from_resolved(
            self.provider,
            resolved,
            "accept-edits",
            self.standalone,
            self.session_id.as_deref(),
            settings_json,
            &self.prompt,
        )
    }
}

const fn default_advisory_file_permissions() -> WorkerFilePermissions {
    WorkerFilePermissions::ReadOnly
}

/// Typed, pre-validated specification for launching a provider-configured read-only advisory
/// worker process inside an existing canonical non-root working directory.
///
/// Unlike [`WorkerSpec`], an advisory worker never creates a mutable Git worktree, never
/// accepts write scopes (`allowed_write_paths` must be empty), and fails closed with
/// [`WorkerError::UnsupportedCapability`] if `file_permissions` is not
/// [`WorkerFilePermissions::ReadOnly`]. For Antigravity, `AdvisoryWorkerSpec` constructs
/// `--mode plan` (never `accept-edits`, `auto`, or `--dangerously-skip-permissions`) along
/// with session-only `--settings <inline-json>`; for OpenCode v2, it constructs
/// `OPENCODE_CONFIG_CONTENT` with read-only `policies` and `permissions`.
///
/// Static provider/mode admission and generated policy fixtures are distinct from live
/// runtime enforcement inside the external CLI binary, and provider configuration is a
/// provider-level policy rather than an OS-level filesystem or process sandbox.
/// Credentials remain strictly provider-managed and are never embedded in `AdvisoryWorkerSpec`
/// or emitted to snapshots/output.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdvisoryWorkerSpec {
    /// Target worker CLI provider (`antigravity` or `opencode_v2`).
    pub provider: WorkerProvider,
    /// Absolute path to the non-symlink worker executable.
    pub executable: PathBuf,
    /// Exact requested model identifier (`gemini-4-argon-high` or `provider/model` / `provider/model#variant`).
    pub model: String,
    /// Exact requested effort or variant (`high` for Antigravity; `#variant` for OpenCode v2).
    #[serde(default, alias = "variant")]
    pub effort: Option<String>,
    /// Optional explicit trusted allowlist of permitted model identifiers.
    #[serde(default)]
    pub allowed_models: BTreeSet<String>,
    /// Optional session identifier (`--session <ID>`, supported only for `opencode_v2`).
    #[serde(default)]
    pub session_id: Option<String>,
    /// Whether to pass the global `--standalone` flag before `run` (required `true` for `opencode_v2`; must be `false` for `antigravity`).
    #[serde(default)]
    pub standalone: bool,
    /// Trusted canonical non-root advisory working directory (`cwd`).
    pub working_directory: PathBuf,
    /// Write scopes for the invocation; must always be empty (`0` paths) for advisory workers.
    #[serde(default)]
    pub allowed_write_paths: BTreeSet<String>,
    /// Untrusted advisory prompt passed as data in the argument array.
    pub prompt: String,
    /// Trusted session-scoped provider file permission mode (defaults to `read_only`;
    /// `inherit` and `task_files` are rejected with [`WorkerError::UnsupportedCapability`]).
    #[serde(default = "default_advisory_file_permissions")]
    pub file_permissions: WorkerFilePermissions,
    /// Finite execution timeout and output capture budgets.
    #[serde(default)]
    pub budgets: ExecutionBudgets,
}

/// Structured result of validating an [`AdvisoryWorkerSpec`] and resolving its read-only
/// launch argv and per-child environment overrides.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdvisorySpecValidationReport {
    /// Validated provider (`antigravity` or `opencode_v2`).
    pub provider: WorkerProvider,
    /// Validated file permission mode (always [`WorkerFilePermissions::ReadOnly`]).
    pub file_permissions: WorkerFilePermissions,
    /// Canonical executable path.
    pub canonical_executable: PathBuf,
    /// Canonical non-root advisory working directory.
    pub canonical_working_directory: PathBuf,
    /// Resolved model and effort/variant details.
    pub resolved_model: ResolvedModelTarget,
    /// Complete launch argv array (excluding `argv[0]`), including session-only `--settings`
    /// when `provider` is [`WorkerProvider::Antigravity`].
    pub constructed_argv: Vec<String>,
    /// Per-child environment variable overrides (`[]` for Antigravity;
    /// `[("OPENCODE_CONFIG_CONTENT", <json>)]` for OpenCode v2).
    pub child_env: Vec<(String, String)>,
    /// Validated (always empty) write-scope set.
    pub allowed_write_paths: BTreeSet<String>,
    /// Validated execution budgets.
    pub budgets: ExecutionBudgets,
}

impl AdvisoryWorkerSpec {
    /// Validates all fields of the advisory worker spec before launch and returns an
    /// [`AdvisorySpecValidationReport`] with the resolved read-only launch argv and child env.
    ///
    /// Fails closed with [`WorkerError::UnsupportedCapability`] if `file_permissions` is not
    /// [`WorkerFilePermissions::ReadOnly`], and with [`WorkerError::InvalidSpec`] if
    /// `allowed_write_paths` is non-empty or `working_directory` is root `/`, contains symlinks
    /// or glob metacharacters, or targets a protected control path.
    pub fn validate(&self) -> Result<AdvisorySpecValidationReport, WorkerError> {
        self.validate_for_workdir(&self.working_directory)
    }

    /// Validates the advisory spec and enforces an external trusted [`WorkerPolicy`].
    pub fn validate_against_policy(
        &self,
        policy: &WorkerPolicy,
    ) -> Result<AdvisorySpecValidationReport, WorkerError> {
        let report = self.validate()?;
        if !policy.allowed_executables.is_empty()
            && !policy
                .allowed_executables
                .contains(&report.canonical_executable)
            && !policy.allowed_executables.contains(&self.executable)
        {
            return Err(WorkerError::DisallowedExecutable {
                executable: report.canonical_executable,
            });
        }
        if !policy.allowed_models.is_empty()
            && !policy.allowed_models.contains(&self.model)
            && !policy
                .allowed_models
                .contains(&report.resolved_model.base_model)
            && !policy
                .allowed_models
                .contains(&report.resolved_model.cli_model_arg)
        {
            return Err(WorkerError::DisallowedModel {
                model: report.resolved_model.cli_model_arg,
            });
        }
        Ok(report)
    }

    /// Resolves and validates the provider-specific model and effort/variant identifiers.
    pub fn resolve_model_target(&self) -> Result<ResolvedModelTarget, WorkerError> {
        resolve_model_target(self.provider, &self.model, self.effort.as_deref())
    }

    /// Validates the spec and returns the complete launch CLI argument array (excluding `argv[0]`)
    /// for `self.working_directory`.
    pub fn build_argv(&self) -> Result<Vec<String>, WorkerError> {
        Ok(self.validate()?.constructed_argv)
    }

    /// Validates the spec and `working_directory` and returns the complete launch CLI argument
    /// array (excluding `argv[0]`) for `working_directory`.
    pub fn build_argv_for_workdir(
        &self,
        working_directory: &Path,
    ) -> Result<Vec<String>, WorkerError> {
        Ok(self
            .validate_for_workdir(working_directory)?
            .constructed_argv)
    }

    /// Validates the spec and returns the per-child environment variable overrides for
    /// `self.working_directory`.
    pub fn build_child_env(&self) -> Result<Vec<(String, String)>, WorkerError> {
        Ok(self.validate()?.child_env)
    }

    /// Validates the spec and `working_directory` and returns the per-child environment variable
    /// overrides for `working_directory`.
    pub fn build_child_env_for_workdir(
        &self,
        working_directory: &Path,
    ) -> Result<Vec<(String, String)>, WorkerError> {
        Ok(self.validate_for_workdir(working_directory)?.child_env)
    }

    /// Builds the structured session-only Antigravity [`AntigravitySessionSettings`] for this
    /// advisory spec.
    ///
    /// Returns [`WorkerError::InvalidSpec`] if `self.provider` is not
    /// [`WorkerProvider::Antigravity`].
    pub fn build_antigravity_settings(&self) -> Result<AntigravitySessionSettings, WorkerError> {
        let report = self.validate()?;
        if self.provider != WorkerProvider::Antigravity {
            return Err(WorkerError::InvalidSpec(format!(
                "`build_antigravity_settings` is only supported for provider `antigravity` (got `{}`)",
                self.provider.as_str()
            )));
        }
        build_antigravity_read_only_settings(&report.canonical_working_directory)
    }

    /// Builds and serializes the session-only Antigravity `--settings <inline-json>` string for
    /// this advisory spec.
    ///
    /// Returns [`WorkerError::InvalidSpec`] if `self.provider` is not
    /// [`WorkerProvider::Antigravity`].
    pub fn build_antigravity_settings_json(&self) -> Result<String, WorkerError> {
        let report = self.validate()?;
        if self.provider != WorkerProvider::Antigravity {
            return Err(WorkerError::InvalidSpec(format!(
                "`build_antigravity_settings_json` is only supported for provider `antigravity` (got `{}`)",
                self.provider.as_str()
            )));
        }
        build_antigravity_read_only_settings_json(&report.canonical_working_directory)
    }

    /// Builds the structured OpenCode 2.2.0 V2 [`OpenCodeConfigContent`] for this advisory spec.
    ///
    /// Returns [`WorkerError::InvalidSpec`] if `self.provider` is not
    /// [`WorkerProvider::OpenCodeV2`].
    pub fn build_opencode_config_content(&self) -> Result<OpenCodeConfigContent, WorkerError> {
        let report = self.validate()?;
        if self.provider != WorkerProvider::OpenCodeV2 {
            return Err(WorkerError::InvalidSpec(format!(
                "`build_opencode_config_content` is only supported for provider `opencode_v2` (got `{}`)",
                self.provider.as_str()
            )));
        }
        build_opencode_config_content(
            &report.resolved_model,
            self.file_permissions,
            &report.canonical_working_directory,
            &self.allowed_write_paths,
        )
    }

    /// Builds and serializes the bounded per-child `OPENCODE_CONFIG_CONTENT` JSON string for this
    /// advisory spec.
    ///
    /// Returns [`WorkerError::InvalidSpec`] if `self.provider` is not
    /// [`WorkerProvider::OpenCodeV2`].
    pub fn build_opencode_config_json(&self) -> Result<String, WorkerError> {
        let report = self.validate()?;
        if self.provider != WorkerProvider::OpenCodeV2 {
            return Err(WorkerError::InvalidSpec(format!(
                "`build_opencode_config_json` is only supported for provider `opencode_v2` (got `{}`)",
                self.provider.as_str()
            )));
        }
        build_opencode_config_content_json(
            &report.resolved_model,
            self.file_permissions,
            &report.canonical_working_directory,
            &self.allowed_write_paths,
        )
    }

    fn validate_for_workdir(
        &self,
        working_directory: &Path,
    ) -> Result<AdvisorySpecValidationReport, WorkerError> {
        ensure_platform_supported()?;
        self.budgets.validate()?;

        self.file_permissions
            .ensure_advisory_read_only_capability(self.provider)?;
        self.file_permissions
            .validate_for_provider_and_paths(self.provider, &self.allowed_write_paths)?;

        let canonical_executable = validate_executable_file(&self.executable)?;
        let canonical_working_directory = match self.provider {
            WorkerProvider::CodexCli => validate_codex_advisory_workdir(working_directory)?,
            WorkerProvider::Antigravity | WorkerProvider::OpenCodeV2 => {
                validate_advisory_read_only_workdir(working_directory)?
            }
        };
        let resolved_model = self.resolve_model_target()?;

        if !self.allowed_models.is_empty()
            && !self.allowed_models.contains(&self.model)
            && !self.allowed_models.contains(&resolved_model.base_model)
            && !self.allowed_models.contains(&resolved_model.cli_model_arg)
        {
            return Err(WorkerError::DisallowedModel {
                model: resolved_model.cli_model_arg,
            });
        }

        match self.provider {
            WorkerProvider::Antigravity => {
                if self.session_id.is_some() {
                    return Err(WorkerError::InvalidSpec(
                        "`session_id` (`--session`) is only supported for provider `opencode_v2`"
                            .to_string(),
                    ));
                }
                if self.standalone {
                    return Err(WorkerError::InvalidSpec(
                        "`standalone` (`--standalone`) is only supported for provider `opencode_v2`"
                            .to_string(),
                    ));
                }
            }
            WorkerProvider::OpenCodeV2 => {
                if !self.standalone {
                    return Err(WorkerError::InvalidSpec(
                        "provider `opencode_v2` requires `standalone: true` (`--standalone`) so each worker runs an isolated private server rather than attaching to an unowned shared background server"
                            .to_string(),
                    ));
                }
                if let Some(ref session_id) = self.session_id {
                    validate_simple_identifier(session_id, "session_id")?;
                }
            }
            WorkerProvider::CodexCli => {
                if self.session_id.is_some() {
                    return Err(WorkerError::InvalidSpec(
                        "`session_id` (`--session`) is only supported for provider `opencode_v2`; `codex_cli` advisory runs must use `--ephemeral`"
                            .to_string(),
                    ));
                }
                if self.standalone {
                    return Err(WorkerError::InvalidSpec(
                        "`standalone` (`--standalone`) is only supported for provider `opencode_v2`"
                            .to_string(),
                    ));
                }
            }
        }

        validate_prompt(&self.prompt)?;

        let (settings_json, child_env) = match self.provider {
            WorkerProvider::Antigravity => {
                let settings =
                    build_antigravity_read_only_settings_json(&canonical_working_directory)?;
                (Some(settings), Vec::new())
            }
            WorkerProvider::OpenCodeV2 => {
                let config_json = build_opencode_config_content_json(
                    &resolved_model,
                    self.file_permissions,
                    &canonical_working_directory,
                    &self.allowed_write_paths,
                )?;
                (
                    None,
                    vec![(OPENCODE_CONFIG_ENV_VAR.to_string(), config_json)],
                )
            }
            WorkerProvider::CodexCli => (None, Vec::new()),
        };

        let constructed_argv = build_provider_argv_from_resolved(
            self.provider,
            &resolved_model,
            "plan",
            self.standalone,
            self.session_id.as_deref(),
            settings_json,
            &self.prompt,
        );

        Ok(AdvisorySpecValidationReport {
            provider: self.provider,
            file_permissions: self.file_permissions,
            canonical_executable,
            canonical_working_directory,
            resolved_model,
            constructed_argv,
            child_env,
            allowed_write_paths: self.allowed_write_paths.clone(),
            budgets: self.budgets.clone(),
        })
    }
}

fn build_provider_argv_from_resolved(
    provider: WorkerProvider,
    resolved: &ResolvedModelTarget,
    antigravity_mode: &str,
    standalone: bool,
    session_id: Option<&str>,
    settings_json: Option<String>,
    prompt: &str,
) -> Vec<String> {
    match provider {
        WorkerProvider::Antigravity => {
            let effort = resolved
                .effort
                .as_deref()
                .expect("Antigravity effort is verified during model target resolution");
            let mut argv = Vec::with_capacity(if settings_json.is_some() { 14 } else { 12 });
            argv.push("--model".to_string());
            argv.push(resolved.cli_model_arg.clone());
            argv.push("--effort".to_string());
            argv.push(effort.to_string());
            argv.push("--mode".to_string());
            argv.push(antigravity_mode.to_string());
            argv.push("--output-format".to_string());
            argv.push("stream-json".to_string());
            argv.push("--print-timeout".to_string());
            argv.push("0".to_string());
            if let Some(settings) = settings_json {
                argv.push("--settings".to_string());
                argv.push(settings);
            }
            argv.push("--print".to_string());
            argv.push(prompt.to_string());
            argv
        }
        WorkerProvider::OpenCodeV2 => {
            let mut argv = Vec::with_capacity(10);
            if standalone {
                argv.push("--standalone".to_string());
            }
            argv.push("run".to_string());
            argv.push("--model".to_string());
            argv.push(resolved.cli_model_arg.clone());
            argv.push("--format".to_string());
            argv.push("json".to_string());
            if let Some(session_id) = session_id {
                argv.push("--session".to_string());
                argv.push(session_id.to_string());
            }
            argv.push("--".to_string());
            argv.push(prompt.to_string());
            argv
        }
        WorkerProvider::CodexCli => {
            let effort = resolved
                .effort
                .as_deref()
                .expect("CodexCli effort is verified during model target resolution");
            let mut argv = Vec::with_capacity(20 + CODEX_ADVISORY_DISABLED_FEATURES.len() * 2 + 2);
            argv.push("--no-daemon".to_string());
            argv.push("exec".to_string());
            argv.push("--json".to_string());
            argv.push("--ephemeral".to_string());
            argv.push("--ignore-user-config".to_string());
            if antigravity_mode != "plan" {
                argv.push("--ignore-rules".to_string());
            }
            argv.push("--model".to_string());
            argv.push(resolved.cli_model_arg.clone());
            argv.push("-c".to_string());
            argv.push(format!("model_reasoning_effort=\"{effort}\""));
            argv.push("-c".to_string());
            argv.push("approval_policy=\"never\"".to_string());
            argv.push("-c".to_string());
            argv.push("model_provider=\"openai\"".to_string());
            if let Some(profile_config) = settings_json {
                argv.push("-c".to_string());
                argv.push(format!(
                    "default_permissions=\"{CODEX_TASK_FILES_PERMISSION_PROFILE_NAME}\""
                ));
                argv.push("-c".to_string());
                argv.push(profile_config);
            } else if antigravity_mode == "plan" {
                argv.push("-c".to_string());
                argv.push(format!(
                    "default_permissions=\"{CODEX_ADVISORY_PERMISSION_PROFILE_NAME}\""
                ));
                argv.push("-c".to_string());
                argv.push(CODEX_ADVISORY_PERMISSION_PROFILE_CONFIG.to_string());
            } else {
                argv.push("-c".to_string());
                argv.push(format!(
                    "default_permissions=\"{CODEX_TASK_FILES_PERMISSION_PROFILE_NAME}\""
                ));
            }
            argv.push("-c".to_string());
            argv.push("web_search=\"disabled\"".to_string());
            for feature in CODEX_ADVISORY_DISABLED_FEATURES {
                argv.push("--disable".to_string());
                argv.push(feature.to_string());
            }
            argv.push("--".to_string());
            argv.push(prompt.to_string());
            argv
        }
    }
}

/// Returns `true` if `argv` matches the exact read-only `codex` advisory argument vector
/// pinned to `resolved` (`--no-daemon exec --json --ephemeral --ignore-user-config --model <model>`,
/// `-c` overrides for `model_reasoning_effort`, `approval_policy`, `model_provider`,
/// `default_permissions`, `permissions.threadlane-advisory`, `web_search`, all
/// [`CODEX_ADVISORY_DISABLED_FEATURES`] `--disable` pairs, `--`, and a non-empty prompt).
#[must_use]
pub fn is_pinned_codex_read_only_argv(argv: &[String], resolved: &ResolvedModelTarget) -> bool {
    let Some(effort) = resolved.effort.as_deref() else {
        return false;
    };
    if validate_simple_identifier(&resolved.base_model, "model").is_err()
        || validate_simple_identifier(effort, "effort").is_err()
        || resolved.cli_model_arg != resolved.base_model
    {
        return false;
    }
    let expected_len = 19 + CODEX_ADVISORY_DISABLED_FEATURES.len() * 2 + 2;
    if argv.len() != expected_len {
        return false;
    }
    let expected_effort_cfg = format!("model_reasoning_effort=\"{effort}\"");
    let expected_default_perms =
        format!("default_permissions=\"{CODEX_ADVISORY_PERMISSION_PROFILE_NAME}\"");
    if !(argv[0] == "--no-daemon"
        && argv[1] == "exec"
        && argv[2] == "--json"
        && argv[3] == "--ephemeral"
        && argv[4] == "--ignore-user-config"
        && argv[5] == "--model"
        && argv[6] == resolved.cli_model_arg
        && argv[7] == "-c"
        && argv[8] == expected_effort_cfg
        && argv[9] == "-c"
        && argv[10] == "approval_policy=\"never\""
        && argv[11] == "-c"
        && argv[12] == "model_provider=\"openai\""
        && argv[13] == "-c"
        && argv[14] == expected_default_perms
        && argv[15] == "-c"
        && argv[16] == CODEX_ADVISORY_PERMISSION_PROFILE_CONFIG
        && argv[17] == "-c"
        && argv[18] == "web_search=\"disabled\"")
    {
        return false;
    }
    for (idx, feature) in CODEX_ADVISORY_DISABLED_FEATURES.iter().enumerate() {
        let base = 19 + idx * 2;
        if argv[base] != "--disable" || argv[base + 1] != *feature {
            return false;
        }
    }
    let sep_idx = 19 + CODEX_ADVISORY_DISABLED_FEATURES.len() * 2;
    argv[sep_idx] == "--"
        && !argv[sep_idx + 1].trim().is_empty()
        && !argv[sep_idx + 1].contains('\0')
}

/// Validates and extracts the exact repository-relative write scopes from a launch-pinned
/// `codex` `TaskFiles` argument vector (`--ignore-rules`,
/// `default_permissions="threadlane-task-files"`, and a valid
/// `permissions.threadlane-task-files={...}` inline config).
///
/// Returns `None` if any element of `argv`, `resolved`, the disabled feature set, or the
/// inline permission profile config fails strict validation.
#[must_use]
pub fn extract_pinned_codex_task_files_write_scopes(
    argv: &[String],
    resolved: &ResolvedModelTarget,
) -> Option<BTreeMap<String, WriteScopeKind>> {
    let effort = resolved.effort.as_deref()?;
    if validate_simple_identifier(&resolved.base_model, "model").is_err()
        || validate_simple_identifier(effort, "effort").is_err()
        || resolved.cli_model_arg != resolved.base_model
    {
        return None;
    }
    let expected_len = 20 + CODEX_ADVISORY_DISABLED_FEATURES.len() * 2 + 2;
    if argv.len() != expected_len {
        return None;
    }
    let expected_effort_cfg = format!("model_reasoning_effort=\"{effort}\"");
    let expected_default_perms =
        format!("default_permissions=\"{CODEX_TASK_FILES_PERMISSION_PROFILE_NAME}\"");
    if !(argv[0] == "--no-daemon"
        && argv[1] == "exec"
        && argv[2] == "--json"
        && argv[3] == "--ephemeral"
        && argv[4] == "--ignore-user-config"
        && argv[5] == "--ignore-rules"
        && argv[6] == "--model"
        && argv[7] == resolved.cli_model_arg
        && argv[8] == "-c"
        && argv[9] == expected_effort_cfg
        && argv[10] == "-c"
        && argv[11] == "approval_policy=\"never\""
        && argv[12] == "-c"
        && argv[13] == "model_provider=\"openai\""
        && argv[14] == "-c"
        && argv[15] == expected_default_perms
        && argv[16] == "-c"
        && argv[18] == "-c"
        && argv[19] == "web_search=\"disabled\"")
    {
        return None;
    }
    let write_scopes =
        parse_and_validate_codex_task_files_permission_profile_config(&argv[17]).ok()?;
    for (idx, feature) in CODEX_ADVISORY_DISABLED_FEATURES.iter().enumerate() {
        let base = 20 + idx * 2;
        if argv[base] != "--disable" || argv[base + 1] != *feature {
            return None;
        }
    }
    let sep_idx = 20 + CODEX_ADVISORY_DISABLED_FEATURES.len() * 2;
    if argv[sep_idx] != "--"
        || argv[sep_idx + 1].trim().is_empty()
        || argv[sep_idx + 1].contains('\0')
    {
        return None;
    }
    Some(write_scopes)
}

/// Returns `true` if `argv` matches the exact mutable `codex` `TaskFiles` argument vector
/// pinned to `resolved` (`--no-daemon exec --json --ephemeral --ignore-user-config --ignore-rules --model <model>`,
/// `-c` overrides for `model_reasoning_effort`, `approval_policy="never"`, `model_provider="openai"`,
/// `default_permissions="threadlane-task-files"`, a validated `permissions.threadlane-task-files={...}`
/// inline profile with finite write scopes and all protected deny rules, `web_search="disabled"`,
/// all [`CODEX_ADVISORY_DISABLED_FEATURES`] `--disable` pairs, `--`, and a non-empty prompt).
#[must_use]
pub fn is_pinned_codex_task_files_argv(argv: &[String], resolved: &ResolvedModelTarget) -> bool {
    extract_pinned_codex_task_files_write_scopes(argv, resolved).is_some()
}

/// Resolves and validates a provider-specific model and effort/variant target from the three
/// explicit inputs used by [`WorkerSpec`] and [`AdvisoryWorkerSpec`]: provider, model argument,
/// and optional effort/variant.
///
/// This is the canonical pure target resolver for worker execution. It performs no filesystem,
/// process, provider, or network I/O and applies the same Antigravity/Codex explicit-effort and
/// OpenCode inline-variant rules as [`WorkerSpec::resolve_model_target`].
pub fn resolve_model_target(
    provider: WorkerProvider,
    model: &str,
    effort: Option<&str>,
) -> Result<ResolvedModelTarget, WorkerError> {
    match provider {
        WorkerProvider::Antigravity => {
            validate_simple_identifier(model, "model")?;
            let effort = effort.ok_or_else(|| {
                WorkerError::InvalidSpec(
                    "provider `antigravity` requires an explicit `effort` identifier".to_string(),
                )
            })?;
            validate_simple_identifier(effort, "effort")?;
            if let Some(catalog_effort) =
                crate::antigravity_models::antigravity_catalog_effort(model)
            {
                if effort != catalog_effort {
                    return Err(WorkerError::InvalidSpec(format!(
                        "Antigravity model `{model}` encodes catalog effort `{catalog_effort}` which conflicts with explicit effort `{effort}`"
                    )));
                }
            }
            Ok(ResolvedModelTarget {
                base_model: model.to_string(),
                effort: Some(effort.to_string()),
                cli_model_arg: model.to_string(),
            })
        }
        WorkerProvider::OpenCodeV2 => {
            if model.is_empty() || model.len() > MAX_IDENTIFIER_LEN || model.trim() != model {
                return Err(WorkerError::InvalidSpec(format!(
                    "invalid OpenCode model identifier `{model}`"
                )));
            }

            let (base_part, inline_variant) = match model.split_once('#') {
                Some((base, variant)) => {
                    if variant.contains('#') {
                        return Err(WorkerError::InvalidSpec(format!(
                            "OpenCode model `{model}` must not contain multiple `#` separators"
                        )));
                    }
                    validate_simple_identifier(variant, "OpenCode model variant")?;
                    (base, Some(variant))
                }
                None => (model, None),
            };

            validate_opencode_provider_model(base_part)?;

            let resolved_effort = match (inline_variant, effort) {
                (Some(inline_v), Some(explicit_v)) => {
                    validate_simple_identifier(explicit_v, "effort")?;
                    if inline_v != explicit_v {
                        return Err(WorkerError::InvalidSpec(format!(
                            "OpenCode model `{model}` inline variant `{inline_v}` conflicts with explicit effort `{explicit_v}`"
                        )));
                    }
                    Some(inline_v.to_string())
                }
                (Some(inline_v), None) => Some(inline_v.to_string()),
                (None, Some(explicit_v)) => {
                    validate_simple_identifier(explicit_v, "effort")?;
                    Some(explicit_v.to_string())
                }
                (None, None) => None,
            };

            let cli_model_arg = match resolved_effort {
                Some(ref v) => format!("{base_part}#{v}"),
                None => base_part.to_string(),
            };

            Ok(ResolvedModelTarget {
                base_model: base_part.to_string(),
                effort: resolved_effort,
                cli_model_arg,
            })
        }
        WorkerProvider::CodexCli => {
            validate_simple_identifier(model, "model")?;
            let effort = effort.ok_or_else(|| {
                WorkerError::InvalidSpec(
                    "provider `codex_cli` requires an explicit `effort` identifier".to_string(),
                )
            })?;
            validate_simple_identifier(effort, "effort")?;
            Ok(ResolvedModelTarget {
                base_model: model.to_string(),
                effort: Some(effort.to_string()),
                cli_model_arg: model.to_string(),
            })
        }
    }
}

/// Validates a simple identifier (model, effort, variant, session_id, run_id, task_id).
pub fn validate_simple_identifier(value: &str, label: &str) -> Result<(), WorkerError> {
    if value.is_empty() || value.len() > MAX_IDENTIFIER_LEN || value.trim() != value {
        return Err(WorkerError::InvalidSpec(format!(
            "{label} must be a non-empty identifier of at most {MAX_IDENTIFIER_LEN} bytes"
        )));
    }
    if value.starts_with('-') || value.starts_with('.') {
        return Err(WorkerError::InvalidSpec(format!(
            "{label} `{value}` must not start with `-` or `.`"
        )));
    }
    if !value
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '-'))
    {
        return Err(WorkerError::InvalidSpec(format!(
            "{label} `{value}` contains invalid characters (only ASCII alphanumeric, `.`, `_`, `-` are allowed)"
        )));
    }
    Ok(())
}

fn validate_opencode_provider_model(base_part: &str) -> Result<(), WorkerError> {
    if base_part.is_empty() || base_part.len() > MAX_IDENTIFIER_LEN {
        return Err(WorkerError::InvalidSpec(format!(
            "OpenCode model `{base_part}` must be between 1 and {MAX_IDENTIFIER_LEN} bytes"
        )));
    }
    let segments: Vec<&str> = base_part.split('/').collect();
    if segments.len() < 2 {
        return Err(WorkerError::InvalidSpec(format!(
            "OpenCode model `{base_part}` must use `provider/model` format"
        )));
    }
    for segment in segments {
        validate_simple_identifier(segment, "OpenCode model segment")?;
    }
    Ok(())
}

/// Validates a Git commit reference or 40-character hexadecimal commit hash.
pub fn validate_commit_ref(commit_ref: &str) -> Result<(), WorkerError> {
    if commit_ref.is_empty()
        || commit_ref.len() > MAX_IDENTIFIER_LEN
        || commit_ref.trim() != commit_ref
    {
        return Err(WorkerError::InvalidSpec(
            "base_commit must be a non-empty commit hash or ref".to_string(),
        ));
    }
    if commit_ref.starts_with('-') || commit_ref.contains("..") || commit_ref.contains("@{") {
        return Err(WorkerError::InvalidSpec(format!(
            "base_commit `{commit_ref}` contains disallowed revision syntax"
        )));
    }
    if !commit_ref
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '-' | '/'))
    {
        return Err(WorkerError::InvalidSpec(format!(
            "base_commit `{commit_ref}` contains invalid characters"
        )));
    }
    Ok(())
}

fn validate_prompt(prompt: &str) -> Result<(), WorkerError> {
    if prompt.trim().is_empty() {
        return Err(WorkerError::InvalidSpec(
            "prompt must not be empty".to_string(),
        ));
    }
    if prompt.len() > MAX_PROMPT_BYTES {
        return Err(WorkerError::InvalidSpec(format!(
            "prompt length ({} bytes) exceeds maximum of {MAX_PROMPT_BYTES} bytes",
            prompt.len()
        )));
    }
    if prompt.contains('\0') {
        return Err(WorkerError::InvalidSpec(
            "prompt must not contain NUL bytes".to_string(),
        ));
    }
    Ok(())
}

/// Validates that `rel_path` is a safe, normalized repository-relative path without
/// leading slashes, backslashes, NUL bytes, or `.` / `..` components.
///
/// Entries in `allowed_write_paths` (`label.contains("allowed_write_paths")`) may end with a
/// single trailing `/` to explicitly mark a directory scope (for example `"crates/foo/"`).
pub fn validate_repo_relative_path(rel_path: &str, label: &str) -> Result<(), WorkerError> {
    if rel_path.is_empty() || rel_path.len() > MAX_PATH_ENTRY_LEN {
        return Err(WorkerError::InvalidSpec(format!(
            "{label} must be between 1 and {MAX_PATH_ENTRY_LEN} bytes"
        )));
    }
    let allow_dir_trailing_slash = label.contains("allowed_write_paths");
    let stripped = if allow_dir_trailing_slash {
        rel_path.strip_suffix('/').unwrap_or(rel_path)
    } else {
        rel_path
    };
    if stripped.is_empty()
        || stripped.starts_with('/')
        || stripped.ends_with('/')
        || stripped.contains('\\')
        || stripped.contains('\0')
        || stripped.contains("//")
    {
        return Err(WorkerError::SecurityViolation(format!(
            "{label} `{rel_path}` must be a clean repository-relative path without leading/trailing slashes or backslashes"
        )));
    }

    let path = Path::new(stripped);
    if path.is_absolute() {
        return Err(WorkerError::SecurityViolation(format!(
            "{label} `{rel_path}` must be relative, not absolute"
        )));
    }

    let mut saw_normal = false;
    for comp in path.components() {
        match comp {
            Component::Normal(part) => {
                let s = part.to_string_lossy();
                if s.is_empty() || s.chars().any(char::is_control) {
                    return Err(WorkerError::SecurityViolation(format!(
                        "{label} `{rel_path}` contains control characters"
                    )));
                }
                saw_normal = true;
            }
            Component::CurDir
            | Component::ParentDir
            | Component::RootDir
            | Component::Prefix(_) => {
                return Err(WorkerError::SecurityViolation(format!(
                    "{label} `{rel_path}` must not contain `.` or `..` traversal components"
                )));
            }
        }
    }

    if !saw_normal {
        return Err(WorkerError::InvalidSpec(format!(
            "{label} `{rel_path}` must contain at least one path component"
        )));
    }

    Ok(())
}

/// Returns `true` if `rel_path` targets Git metadata, repository control files, or agent/toolchain configuration.
pub fn is_control_config_path(rel_path: &str) -> bool {
    let trimmed = rel_path.trim_matches('/');
    if trimmed.is_empty() {
        return true;
    }

    let path = Path::new(trimmed);
    let components: Vec<String> = path
        .components()
        .map(|c| c.as_os_str().to_string_lossy().to_ascii_lowercase())
        .collect();

    if components.is_empty() {
        return true;
    }

    // Any component matching `.git`, `.gitmodules`, `.gitattributes`, `.gitignore`, `agents.md`, `claude.md`, `opencode.json`, `opencode.jsonc`
    for comp in &components {
        if matches!(
            comp.as_str(),
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
        ) {
            return true;
        }
        if comp == ".env" || comp.starts_with(".env.") {
            return true;
        }
    }

    let first = components[0].as_str();
    if first == ".github" {
        return true;
    }
    if first == ".cargo"
        && components
            .get(1)
            .is_some_and(|second| second == "config" || second == "config.toml")
    {
        return true;
    }

    false
}

/// Effective write-scope matching mode for a repository-relative `allowed_write_paths` entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WriteScopeKind {
    /// Matches only the exact repository-relative file path (`candidate == scope`).
    ExactFile,
    /// Matches descendant files and directories under `scope` componentwise
    /// (`Path::new(candidate).starts_with(Path::new(scope))`).
    DirectorySubtree,
}

/// Checks whether a validated repository-relative path is covered by `resolved_write_scopes`
/// (where each key is a clean repository-relative path without trailing `/` and each value is
/// its resolved [`WriteScopeKind`]) and is not a protected control configuration path.
///
/// - [`WriteScopeKind::ExactFile`] entries match **only** when `rel_path == clean_scope`
///   (and never match descendant paths such as `clean_scope/child`).
/// - [`WriteScopeKind::DirectorySubtree`] entries match when `Path::new(rel_path)` starts with
///   `Path::new(clean_scope)` componentwise.
pub fn is_path_allowed_by_scopes(
    rel_path: &str,
    resolved_write_scopes: &BTreeMap<String, WriteScopeKind>,
) -> bool {
    if validate_repo_relative_path(rel_path, "changed path").is_err() {
        return false;
    }
    if is_control_config_path(rel_path) {
        return false;
    }
    let candidate = Path::new(rel_path);
    resolved_write_scopes.iter().any(|(clean_scope, kind)| {
        if clean_scope.is_empty()
            || clean_scope.ends_with('/')
            || validate_repo_relative_path(clean_scope, "allowed_write_paths entry").is_err()
            || is_control_config_path(clean_scope)
        {
            return false;
        }
        match kind {
            WriteScopeKind::ExactFile => rel_path == clean_scope.as_str(),
            WriteScopeKind::DirectorySubtree => candidate.starts_with(Path::new(clean_scope)),
        }
    })
}

/// Checks whether a validated repository-relative path is covered by `allowed_write_paths`
/// using pure syntactic scope conventions (trailing `/` means [`WriteScopeKind::DirectorySubtree`];
/// bare entries without trailing `/` mean [`WriteScopeKind::ExactFile`]) and is not a protected
/// control configuration path.
pub fn is_path_allowed_syntactic(rel_path: &str, allowed_write_paths: &BTreeSet<String>) -> bool {
    if validate_repo_relative_path(rel_path, "changed path").is_err() {
        return false;
    }
    if is_control_config_path(rel_path) {
        return false;
    }
    let candidate = Path::new(rel_path);
    allowed_write_paths.iter().any(|allowed| {
        if validate_repo_relative_path(allowed, "allowed_write_paths entry").is_err() {
            return false;
        }
        if let Some(clean_dir) = allowed.strip_suffix('/') {
            if is_control_config_path(clean_dir) {
                return false;
            }
            candidate.starts_with(Path::new(clean_dir))
        } else {
            if is_control_config_path(allowed) {
                return false;
            }
            rel_path == allowed.as_str()
        }
    })
}

/// Checks whether a validated repository-relative path is covered by `allowed_write_paths`
/// under legacy syntactic prefix matching and is not a protected control configuration path.
///
/// Note: This legacy syntactic helper does not inspect a Git commit tree and treats bare entries
/// as componentwise prefixes. Actual post-run diff gate acceptance in
/// [`crate::git::evaluate_diff_gate`] resolves `allowed_write_paths` against the immutable pinned
/// `base_commit` tree and uses [`is_path_allowed_by_scopes`] so bare file or absent paths are
/// strictly enforced as [`WriteScopeKind::ExactFile`].
pub fn is_path_allowed(rel_path: &str, allowed_write_paths: &BTreeSet<String>) -> bool {
    if validate_repo_relative_path(rel_path, "changed path").is_err() {
        return false;
    }
    if is_control_config_path(rel_path) {
        return false;
    }
    let candidate = Path::new(rel_path);
    allowed_write_paths.iter().any(|allowed| {
        let clean = allowed.strip_suffix('/').unwrap_or(allowed.as_str());
        candidate.starts_with(Path::new(clean))
    })
}
