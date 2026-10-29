use std::collections::BTreeSet;
use std::path::{Component, Path, PathBuf};
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::error::WorkerError;
use crate::fs_safety::{
    ensure_platform_supported, validate_absolute_no_symlink_components, validate_executable_file,
    validate_existing_canonical_dir,
};
pub use crate::permissions::WorkerFilePermissions;
use crate::permissions::{
    build_antigravity_task_files_settings_json, validate_permission_pattern_path,
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

/// Supported external worker CLI providers for Milestone 2.
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
}

impl WorkerProvider {
    /// Returns the canonical snake_case provider identifier.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Antigravity => "antigravity",
            Self::OpenCodeV2 => "opencode_v2",
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
        }

        self.file_permissions
            .validate_for_provider_and_paths(self.provider, &self.allowed_write_paths)?;
        if self.file_permissions == WorkerFilePermissions::TaskFiles {
            let _ = validate_permission_pattern_path(&work_area_root, "work_area_root")?;
        }

        validate_prompt(&self.prompt)?;
        let constructed_argv = self.build_argv_from_resolved(&resolved_model, None);
        let workdir_argv_resolution_required = self.file_permissions.requires_workdir_argv();
        let argv_resolution_note = workdir_argv_resolution_required.then(|| {
            "file_permissions is `task_files`; `constructed_argv` is the static base preview without `--settings`, which is resolved from the canonical task working directory via `WorkerSpec::build_argv_for_workdir`.".to_string()
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
        match self.provider {
            WorkerProvider::Antigravity => {
                validate_simple_identifier(&self.model, "model")?;
                let effort = self.effort.as_deref().ok_or_else(|| {
                    WorkerError::InvalidSpec(
                        "provider `antigravity` requires an explicit `effort` identifier"
                            .to_string(),
                    )
                })?;
                validate_simple_identifier(effort, "effort")?;
                Ok(ResolvedModelTarget {
                    base_model: self.model.clone(),
                    effort: Some(effort.to_string()),
                    cli_model_arg: self.model.clone(),
                })
            }
            WorkerProvider::OpenCodeV2 => {
                if self.model.is_empty()
                    || self.model.len() > MAX_IDENTIFIER_LEN
                    || self.model.trim() != self.model
                {
                    return Err(WorkerError::InvalidSpec(format!(
                        "invalid OpenCode model identifier `{}`",
                        self.model
                    )));
                }

                let (base_part, inline_variant) = match self.model.split_once('#') {
                    Some((base, variant)) => {
                        if variant.contains('#') {
                            return Err(WorkerError::InvalidSpec(format!(
                                "OpenCode model `{}` must not contain multiple `#` separators",
                                self.model
                            )));
                        }
                        validate_simple_identifier(variant, "OpenCode model variant")?;
                        (base, Some(variant))
                    }
                    None => (self.model.as_str(), None),
                };

                validate_opencode_provider_model(base_part)?;

                let resolved_effort = match (inline_variant, self.effort.as_deref()) {
                    (Some(inline_v), Some(explicit_v)) => {
                        validate_simple_identifier(explicit_v, "effort")?;
                        if inline_v != explicit_v {
                            return Err(WorkerError::InvalidSpec(format!(
                                "OpenCode model `{}` inline variant `{inline_v}` conflicts with explicit effort `{explicit_v}`",
                                self.model
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
        }
    }

    /// Validates the spec and constructs the static CLI argument array (excluding `argv[0]`)
    /// when `file_permissions` is [`WorkerFilePermissions::Inherit`].
    ///
    /// Returns [`WorkerError::InvalidSpec`] when `file_permissions` is
    /// [`WorkerFilePermissions::TaskFiles`] because session-only `--settings <inline-json>`
    /// requires validating and interpolating the actual canonical task working directory via
    /// [`WorkerSpec::build_argv_for_workdir`].
    ///
    /// Never uses shell interpolation, never invents unsupported flags (such as `--variant`),
    /// and never adds `--dangerously-skip-permissions` or `--auto`.
    pub fn build_argv(&self) -> Result<Vec<String>, WorkerError> {
        let report = self.validate()?;
        if report.workdir_argv_resolution_required {
            return Err(WorkerError::InvalidSpec(
                "`file_permissions: task_files` requires a canonical task working directory to construct session-only `--settings`; call `WorkerSpec::build_argv_for_workdir`"
                    .to_string(),
            ));
        }
        Ok(report.constructed_argv)
    }

    /// Validates the spec and `working_directory` and constructs the real launch CLI argument
    /// array (excluding `argv[0]`) for `working_directory`.
    ///
    /// When `file_permissions` is [`WorkerFilePermissions::Inherit`], returns the base provider
    /// argument array. When `file_permissions` is [`WorkerFilePermissions::TaskFiles`], validates
    /// that the canonical `working_directory` and `allowed_write_paths` contain no unsafe
    /// glob/rule metacharacters or symlinks and injects `--settings <inline-json>` scoped to
    /// `working_directory`.
    pub fn build_argv_for_workdir(
        &self,
        working_directory: &Path,
    ) -> Result<Vec<String>, WorkerError> {
        let report = self.validate()?;
        let canonical_workdir =
            validate_existing_canonical_dir(working_directory, "working_directory")?;
        let settings_json = match self.file_permissions {
            WorkerFilePermissions::Inherit => None,
            WorkerFilePermissions::TaskFiles => Some(build_antigravity_task_files_settings_json(
                &canonical_workdir,
                &self.allowed_write_paths,
            )?),
        };
        Ok(self.build_argv_from_resolved(&report.resolved_model, settings_json))
    }

    fn build_argv_from_resolved(
        &self,
        resolved: &ResolvedModelTarget,
        settings_json: Option<String>,
    ) -> Vec<String> {
        match self.provider {
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
                argv.push("accept-edits".to_string());
                argv.push("--output-format".to_string());
                argv.push("stream-json".to_string());
                argv.push("--print-timeout".to_string());
                argv.push("0".to_string());
                if let Some(settings) = settings_json {
                    argv.push("--settings".to_string());
                    argv.push(settings);
                }
                argv.push("--print".to_string());
                argv.push(self.prompt.clone());
                argv
            }
            WorkerProvider::OpenCodeV2 => {
                let mut argv = Vec::with_capacity(10);
                if self.standalone {
                    argv.push("--standalone".to_string());
                }
                argv.push("run".to_string());
                argv.push("--model".to_string());
                argv.push(resolved.cli_model_arg.clone());
                argv.push("--format".to_string());
                argv.push("json".to_string());
                if let Some(ref session_id) = self.session_id {
                    argv.push("--session".to_string());
                    argv.push(session_id.clone());
                }
                argv.push("--".to_string());
                argv.push(self.prompt.clone());
                argv
            }
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
pub fn validate_repo_relative_path(rel_path: &str, label: &str) -> Result<(), WorkerError> {
    if rel_path.is_empty() || rel_path.len() > MAX_PATH_ENTRY_LEN {
        return Err(WorkerError::InvalidSpec(format!(
            "{label} must be between 1 and {MAX_PATH_ENTRY_LEN} bytes"
        )));
    }
    if rel_path.starts_with('/')
        || rel_path.ends_with('/')
        || rel_path.contains('\\')
        || rel_path.contains('\0')
        || rel_path.contains("//")
    {
        return Err(WorkerError::SecurityViolation(format!(
            "{label} `{rel_path}` must be a clean repository-relative path without leading/trailing slashes or backslashes"
        )));
    }

    let path = Path::new(rel_path);
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

    // Any component matching `.git`, `.gitmodules`, `.gitattributes`, `.gitignore`, `agents.md`, `claude.md`
    for comp in &components {
        if matches!(
            comp.as_str(),
            ".git"
                | ".gitmodules"
                | ".gitattributes"
                | ".gitignore"
                | "agents.md"
                | "claude.md"
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

/// Checks whether a validated repository-relative path is covered by `allowed_write_paths`
/// and is not a protected control configuration path.
pub fn is_path_allowed(rel_path: &str, allowed_write_paths: &BTreeSet<String>) -> bool {
    if validate_repo_relative_path(rel_path, "changed path").is_err() {
        return false;
    }
    if is_control_config_path(rel_path) {
        return false;
    }
    let candidate = Path::new(rel_path);
    allowed_write_paths
        .iter()
        .any(|allowed| candidate.starts_with(Path::new(allowed)))
}
