use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Component, Path, PathBuf};

use serde::{Deserialize, Deserializer, Serialize};
use sha2::{Digest, Sha256};
use threadlane_core::{
    WorkerAcceptanceState, WorkerClaim, WorkerCompletionRecord, WorkerOutcomeSummary,
    WorkerProgressUpdate, WorkerStatus, WorkerWorktreeUpdate, MAX_ID_LEN,
    MAX_WORKER_CHANGED_PATHS_PREVIEW, MAX_WORKER_DIGEST_LEN, MAX_WORKER_PATH_BYTES,
    MAX_WORKER_PROFILE_ID_LEN, MAX_WORKER_SUMMARY_TEXT_BYTES, MAX_WORKER_VIOLATION_SUMMARIES,
};
use threadlane_workers::git::WORK_AREA_LOCK_FILENAME;
pub use threadlane_workers::WorkerFilePermissions;
use threadlane_workers::{
    execute_worker_task_with_observer, sanitize_and_redact,
    validate_absolute_no_symlink_components, validate_simple_identifier, ExecutionBudgets,
    FieldEvidence, NormalizedEventKind, NormalizedWorkerEvent, OutputOverflowKind,
    ResolvedModelTarget, TaskExecutionReport, WorkAcceptanceState, WorkerError,
    WorkerExecutionReport, WorkerProvider, WorkerRunStatus, WorkerSpec, WorktreeManager,
};
use tokio::sync::{mpsc, watch};

use crate::error::Error;

/// Maximum number of named worker profiles allowed in a startup configuration.
pub const MAX_WORKER_PROFILES: usize = 32;

/// Minimum allowed `max_parallel` worker concurrency limit.
pub const MIN_MAX_PARALLEL: usize = 1;

/// Maximum allowed `max_parallel` worker concurrency limit.
pub const MAX_MAX_PARALLEL: usize = 8;

/// Default `max_parallel` worker concurrency limit when omitted in startup configuration.
pub const DEFAULT_MAX_PARALLEL: usize = 2;

/// Maximum size in bytes of the private startup `--workers-config` JSON file (`64 KiB`).
pub const MAX_WORKERS_CONFIG_FILE_BYTES: usize = 64 * 1024;

/// Bounded capacity of the per-worker live progress observer channel.
pub const LIVE_OBSERVER_CHANNEL_CAPACITY: usize = 64;

/// Maximum size in bytes of a private local worker execution report artifact (`128 KiB`).
const MAX_LOCAL_REPORT_ARTIFACT_BYTES: usize = 128 * 1024;

const fn default_max_parallel() -> usize {
    DEFAULT_MAX_PARALLEL
}

/// Trusted worker specification template fixed by a startup profile.
///
/// Callers cannot override any field of this template over HTTP, CLI, or MCP;
/// they may only supply a `profile_id`, bounded `instruction`, and `command_key`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerSpecTemplate {
    /// Target worker CLI provider (`antigravity` or `opencode_v2`).
    pub provider: WorkerProvider,
    /// Absolute, non-symlink path to the provider executable.
    pub executable: PathBuf,
    /// Exact requested model identifier (`provider/model` for OpenCode, or model name for Antigravity).
    pub model: String,
    /// Requested reasoning effort or variant (`low`, `medium`, `high`, `max`, etc.).
    #[serde(default, alias = "variant")]
    pub effort: Option<String>,
    /// Explicit policy allowlist of permitted base model identifiers.
    #[serde(default)]
    pub allowed_models: BTreeSet<String>,
    /// Whether to pass `--standalone` (required `true` for OpenCode v2; `false` for Antigravity).
    #[serde(default)]
    pub standalone: bool,
    /// Optional session identifier when continuing a known session.
    #[serde(default)]
    pub session_id: Option<String>,
    /// Absolute canonical path to the source Git repository.
    pub repository_path: PathBuf,
    /// Exact 40-character hexadecimal Git commit SHA for isolated worktrees.
    ///
    /// Mutable symbolic refs (such as `HEAD` or branch names) are rejected so an immutable
    /// profile digest cannot silently change its target commit as the source repository moves.
    pub base_commit: String,
    /// Absolute non-symlink path to the private work area root directory (`0700` on Unix).
    pub work_area_root: PathBuf,
    /// Non-empty set of repository-relative paths or directory prefixes the worker may modify.
    pub allowed_write_paths: BTreeSet<String>,
    /// Scoped provider file permission policy (`inherit` by default, or `task_files` for
    /// Antigravity and OpenCode v2).
    #[serde(default)]
    pub file_permissions: WorkerFilePermissions,
    /// Explicit execution, stream, and output budgets.
    #[serde(default)]
    pub budgets: ExecutionBudgets,
}

impl WorkerSpecTemplate {
    /// Materializes a full [`WorkerSpec`] by combining this trusted template with a caller's
    /// bounded `instruction`.
    pub fn to_worker_spec(&self, instruction: String) -> WorkerSpec {
        WorkerSpec {
            provider: self.provider,
            executable: self.executable.clone(),
            model: self.model.clone(),
            effort: self.effort.clone(),
            allowed_models: self.allowed_models.clone(),
            prompt: instruction,
            standalone: self.standalone,
            session_id: self.session_id.clone(),
            repository_path: self.repository_path.clone(),
            base_commit: self.base_commit.to_ascii_lowercase(),
            work_area_root: self.work_area_root.clone(),
            allowed_write_paths: self.allowed_write_paths.clone(),
            file_permissions: self.file_permissions,
            budgets: self.budgets.clone(),
        }
    }
}

/// Named, binding-scoped worker profile defined in the private startup configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct WorkerProfileConfig {
    /// Unique startup profile identifier (`1..=64` ASCII identifier characters).
    pub profile_id: String,
    /// Non-empty set of binding IDs authorized to start workers with this profile.
    pub allowed_binding_ids: BTreeSet<String>,
    /// Trusted worker specification template fixed by this profile.
    pub spec: WorkerSpecTemplate,
}

impl<'de> Deserialize<'de> for WorkerProfileConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let mut map = serde_json::Map::<String, serde_json::Value>::deserialize(deserializer)?;

        let profile_id_val = map
            .remove("profile_id")
            .ok_or_else(|| serde::de::Error::missing_field("profile_id"))?;
        let profile_id: String =
            serde_json::from_value(profile_id_val).map_err(serde::de::Error::custom)?;

        let allowed_val = match (
            map.remove("allowed_binding_ids"),
            map.remove("allowed_bindings"),
            map.remove("binding_ids"),
        ) {
            (Some(v), None, None) | (None, Some(v), None) | (None, None, Some(v)) => v,
            (None, None, None) => {
                return Err(serde::de::Error::missing_field("allowed_binding_ids"));
            }
            _ => {
                return Err(serde::de::Error::custom(
                    "duplicate allowed_binding_ids / allowed_bindings / binding_ids field",
                ));
            }
        };
        let allowed_binding_ids: BTreeSet<String> =
            serde_json::from_value(allowed_val).map_err(serde::de::Error::custom)?;

        let spec: WorkerSpecTemplate = match (map.remove("spec"), map.remove("template")) {
            (Some(_), Some(_)) => {
                return Err(serde::de::Error::custom(
                    "cannot specify both `spec` and `template` in worker profile",
                ));
            }
            (Some(spec_val), None) | (None, Some(spec_val)) => {
                if let Some(unexpected_key) = map.keys().next() {
                    return Err(serde::de::Error::unknown_field(
                        unexpected_key,
                        &["profile_id", "allowed_binding_ids", "spec"],
                    ));
                }
                serde_json::from_value(spec_val).map_err(serde::de::Error::custom)?
            }
            (None, None) => serde_json::from_value(serde_json::Value::Object(map))
                .map_err(serde::de::Error::custom)?,
        };

        Ok(Self {
            profile_id,
            allowed_binding_ids,
            spec,
        })
    }
}

/// Optional private startup worker dispatch configuration.
///
/// Worker dispatch is disabled by default when [`SupervisorConfig::workers`](crate::supervisor::SupervisorConfig::workers)
/// is `None`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkersConfig {
    /// Maximum number of concurrent worker executions (`1..=8`, defaults to `2`).
    #[serde(default = "default_max_parallel")]
    pub max_parallel: usize,
    /// Fixed startup registry of named worker profiles (`1..=32`).
    pub profiles: Vec<WorkerProfileConfig>,
}

impl WorkersConfig {
    /// Loads and validates a [`WorkersConfig`] from a canonical, non-symlink, private (`0600`)
    /// JSON file on disk without performing any worktree, directory creation, permission change,
    /// or process side effects.
    pub fn load_from_path(path: &Path) -> Result<Self, Error> {
        let resolved = resolve_clean_path(path)?;
        validate_no_symlink_components(&resolved, "workers_config")?;

        let symlink_meta = fs::symlink_metadata(&resolved).map_err(|err| {
            Error::InvalidConfig(format!(
                "failed to stat workers_config `{}`: {err}",
                resolved.display()
            ))
        })?;
        if symlink_meta.file_type().is_symlink() {
            return Err(Error::InvalidConfig(format!(
                "workers_config `{}` must not be a symlink",
                resolved.display()
            )));
        }
        if !symlink_meta.file_type().is_file() {
            return Err(Error::InvalidConfig(format!(
                "workers_config `{}` must be a regular file",
                resolved.display()
            )));
        }

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = symlink_meta.permissions().mode() & 0o777;
            if mode != 0o600 {
                return Err(Error::InvalidConfig(format!(
                    "workers_config `{}` has permissions {:04o}; expected private 0600",
                    resolved.display(),
                    mode
                )));
            }
        }

        if symlink_meta.len() > MAX_WORKERS_CONFIG_FILE_BYTES as u64 {
            return Err(Error::InvalidConfig(format!(
                "workers_config `{}` size ({} bytes) exceeds maximum ({MAX_WORKERS_CONFIG_FILE_BYTES} bytes)",
                resolved.display(),
                symlink_meta.len()
            )));
        }

        let file = File::open(&resolved).map_err(|err| {
            Error::InvalidConfig(format!(
                "failed to open workers_config `{}`: {err}",
                resolved.display()
            ))
        })?;
        let open_meta = file.metadata().map_err(|err| {
            Error::InvalidConfig(format!(
                "failed to read metadata for open workers_config `{}`: {err}",
                resolved.display()
            ))
        })?;
        if !open_meta.file_type().is_file() {
            return Err(Error::InvalidConfig(format!(
                "workers_config `{}` is not a regular file",
                resolved.display()
            )));
        }

        #[cfg(unix)]
        {
            use std::os::unix::fs::{MetadataExt, PermissionsExt};
            if symlink_meta.dev() != open_meta.dev() || symlink_meta.ino() != open_meta.ino() {
                return Err(Error::InvalidConfig(format!(
                    "workers_config `{}` changed during open (inode mismatch)",
                    resolved.display()
                )));
            }
            let mode = open_meta.permissions().mode() & 0o777;
            if mode != 0o600 {
                return Err(Error::InvalidConfig(format!(
                    "workers_config `{}` has permissions {:04o}; expected private 0600",
                    resolved.display(),
                    mode
                )));
            }
        }

        if open_meta.len() > MAX_WORKERS_CONFIG_FILE_BYTES as u64 {
            return Err(Error::InvalidConfig(format!(
                "workers_config `{}` size ({} bytes) exceeds maximum ({MAX_WORKERS_CONFIG_FILE_BYTES} bytes)",
                resolved.display(),
                open_meta.len()
            )));
        }

        let mut bytes = Vec::with_capacity(
            usize::try_from(open_meta.len())
                .unwrap_or(1024)
                .min(MAX_WORKERS_CONFIG_FILE_BYTES + 1),
        );
        file.take((MAX_WORKERS_CONFIG_FILE_BYTES as u64) + 1)
            .read_to_end(&mut bytes)
            .map_err(|err| {
                Error::InvalidConfig(format!(
                    "failed to read workers_config `{}`: {err}",
                    resolved.display()
                ))
            })?;
        if bytes.len() > MAX_WORKERS_CONFIG_FILE_BYTES {
            return Err(Error::InvalidConfig(format!(
                "workers_config `{}` exceeds maximum size of {MAX_WORKERS_CONFIG_FILE_BYTES} bytes",
                resolved.display()
            )));
        }

        let raw_json: serde_json::Value = serde_json::from_slice(&bytes).map_err(|err| {
            Error::InvalidConfig(format!(
                "invalid JSON in workers_config `{}`: {err}",
                resolved.display()
            ))
        })?;
        reject_embedded_credentials(&raw_json, "workers_config")?;

        let config: Self = serde_json::from_slice(&bytes).map_err(|err| {
            Error::InvalidConfig(format!(
                "invalid workers_config `{}`: {err}",
                resolved.display()
            ))
        })?;
        let _ = config.validate()?;
        Ok(config)
    }

    /// Validates this configuration without any filesystem mutation, worktree creation, or
    /// process spawning, returning a [`ValidatedWorkersConfig`] with canonical profile digests.
    pub fn validate(&self) -> Result<ValidatedWorkersConfig, Error> {
        if !(MIN_MAX_PARALLEL..=MAX_MAX_PARALLEL).contains(&self.max_parallel) {
            return Err(Error::InvalidConfig(format!(
                "workers max_parallel must be in {MIN_MAX_PARALLEL}..={MAX_MAX_PARALLEL}, got {}",
                self.max_parallel
            )));
        }
        if self.profiles.is_empty() || self.profiles.len() > MAX_WORKER_PROFILES {
            return Err(Error::InvalidConfig(format!(
                "workers profiles count must be in 1..={MAX_WORKER_PROFILES}, got {}",
                self.profiles.len()
            )));
        }

        let mut profiles_by_id = BTreeMap::new();
        for profile in &self.profiles {
            let validated = validate_profile_side_effect_free(profile)?;
            if profiles_by_id
                .insert(validated.profile_id.clone(), validated)
                .is_some()
            {
                return Err(Error::InvalidConfig(format!(
                    "duplicate worker profile_id `{}`",
                    profile.profile_id
                )));
            }
        }

        Ok(ValidatedWorkersConfig {
            max_parallel: self.max_parallel,
            profiles: profiles_by_id,
        })
    }
}

/// Validated, immutable runtime worker profile with precomputed canonical SHA-256 digest.
#[derive(Debug, Clone)]
pub struct ValidatedWorkerProfile {
    /// Unique profile identifier.
    pub profile_id: String,
    /// Canonical 64-hex SHA-256 digest of the complete trusted execution profile.
    pub profile_digest: String,
    /// Non-empty set of binding IDs permitted to use this profile.
    pub allowed_binding_ids: BTreeSet<String>,
    /// Validated specification template (with normalized 40-hex `base_commit` and canonical paths).
    pub template: WorkerSpecTemplate,
    /// Resolved provider model and effort target.
    pub resolved_model: ResolvedModelTarget,
    /// Non-empty requested effort label recorded in `threadlane-core`.
    pub requested_effort: String,
}

impl ValidatedWorkerProfile {
    /// Returns the provider identifier string stored in `threadlane-core`.
    pub fn provider_str(&self) -> &'static str {
        self.template.provider.as_str()
    }

    /// Returns the requested base model identifier stored in `threadlane-core`.
    pub fn requested_model_str(&self) -> &str {
        &self.resolved_model.base_model
    }

    /// Builds a [`WorkerSpec`] for the given caller `instruction`.
    pub fn build_spec(&self, instruction: String) -> WorkerSpec {
        self.template.to_worker_spec(instruction)
    }
}

/// Validated startup worker configuration held by the supervisor.
#[derive(Debug, Clone)]
pub struct ValidatedWorkersConfig {
    /// Maximum number of concurrent active worker executions (`1..=8`).
    pub max_parallel: usize,
    /// Validated profiles keyed by `profile_id`.
    pub profiles: BTreeMap<String, ValidatedWorkerProfile>,
}

fn resolve_clean_path(path: &Path) -> Result<PathBuf, Error> {
    let abs = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()?.join(path)
    };
    let mut normalized = PathBuf::new();
    for component in abs.components() {
        match component {
            Component::RootDir => normalized.push(Component::RootDir.as_os_str()),
            Component::Prefix(prefix) => normalized.push(prefix.as_os_str()),
            Component::Normal(part) => normalized.push(part),
            Component::CurDir => {}
            Component::ParentDir => {
                return Err(Error::InvalidConfig(format!(
                    "path `{}` must not contain parent directory (`..`) traversal components",
                    path.display()
                )));
            }
        }
    }
    Ok(normalized)
}

fn validate_no_symlink_components(path: &Path, label: &str) -> Result<(), Error> {
    let mut current = PathBuf::new();
    for component in path.components() {
        match component {
            Component::RootDir => current.push(Component::RootDir.as_os_str()),
            Component::Prefix(prefix) => current.push(prefix.as_os_str()),
            Component::Normal(part) => {
                current.push(part);
                if let Ok(meta) = fs::symlink_metadata(&current) {
                    if meta.file_type().is_symlink() {
                        return Err(Error::InvalidConfig(format!(
                            "{label} path `{}` contains symlink component `{}`",
                            path.display(),
                            current.display()
                        )));
                    }
                }
            }
            Component::CurDir | Component::ParentDir => {
                return Err(Error::InvalidConfig(format!(
                    "{label} path `{}` contains non-canonical relative component",
                    path.display()
                )));
            }
        }
    }
    Ok(())
}

fn reject_embedded_credentials(value: &serde_json::Value, path: &str) -> Result<(), Error> {
    match value {
        serde_json::Value::Object(map) => {
            for (k, v) in map {
                let lower = k.to_ascii_lowercase();
                if lower.contains("api_key")
                    || lower.contains("apikey")
                    || lower.contains("secret")
                    || lower.contains("password")
                    || lower.contains("credential")
                    || lower.contains("auth_token")
                    || lower.contains("access_token")
                    || lower.contains("bearer")
                    || lower == "env"
                    || lower == "environment"
                {
                    return Err(Error::InvalidConfig(format!(
                        "{path}.{k}: credentials and environment overrides are forbidden in startup worker config"
                    )));
                }
                reject_embedded_credentials(v, &format!("{path}.{k}"))?;
            }
        }
        serde_json::Value::Array(items) => {
            for (idx, item) in items.iter().enumerate() {
                reject_embedded_credentials(item, &format!("{path}[{idx}]"))?;
            }
        }
        serde_json::Value::String(text) => {
            let redacted = sanitize_and_redact(text, text.len().max(64));
            if redacted.contains("[REDACTED]") {
                return Err(Error::InvalidConfig(format!(
                    "{path}: value appears to contain an embedded secret or credential"
                )));
            }
        }
        _ => {}
    }
    Ok(())
}

fn validate_identifier_chars(value: &str, field_name: &str, max_len: usize) -> Result<(), Error> {
    if value.is_empty() || value.len() > max_len {
        return Err(Error::InvalidConfig(format!(
            "{field_name} must be 1..={max_len} bytes"
        )));
    }
    if value != value.trim()
        || value
            .chars()
            .any(|ch| ch.is_whitespace() || ch.is_control())
    {
        return Err(Error::InvalidConfig(format!(
            "{field_name} `{value}` must not contain whitespace or control characters"
        )));
    }
    Ok(())
}

/// Validates an exact 40-character hexadecimal Git commit SHA.
///
/// Symbolic refs (such as `HEAD` or branch names) and abbreviated hashes are rejected.
pub fn validate_exact_commit_sha(commit: &str) -> Result<String, Error> {
    if commit.len() != 40 || !commit.chars().all(|ch| ch.is_ascii_hexdigit()) {
        return Err(Error::InvalidConfig(format!(
            "profile base_commit `{commit}` must be an exact 40-character hexadecimal commit SHA (mutable refs like HEAD are forbidden)"
        )));
    }
    Ok(commit.to_ascii_lowercase())
}

fn validate_work_area_side_effect_free(work_area_root: &Path) -> Result<PathBuf, Error> {
    let checked = validate_absolute_no_symlink_components(work_area_root, "work_area_root")
        .map_err(|err| Error::InvalidConfig(err.to_string()))?;

    if let Ok(meta) = fs::symlink_metadata(&checked) {
        if meta.file_type().is_symlink() {
            return Err(Error::InvalidConfig(format!(
                "work_area_root `{}` must not be a symlink",
                checked.display()
            )));
        }
        if !meta.file_type().is_dir() {
            return Err(Error::InvalidConfig(format!(
                "work_area_root `{}` must be a directory",
                checked.display()
            )));
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = meta.permissions().mode() & 0o777;
            if mode & 0o077 != 0 {
                return Err(Error::InvalidConfig(format!(
                    "existing work_area_root `{}` has non-private permissions {:04o} (expected 0700)",
                    checked.display(),
                    mode
                )));
            }
        }
        let mut entries = Vec::new();
        let read_dir = fs::read_dir(&checked).map_err(|err| {
            Error::InvalidConfig(format!(
                "failed to inspect existing work_area_root `{}`: {err}",
                checked.display()
            ))
        })?;
        for entry_res in read_dir {
            let entry = entry_res.map_err(|err| {
                Error::InvalidConfig(format!(
                    "failed to read directory entry in work_area_root `{}`: {err}",
                    checked.display()
                ))
            })?;
            let entry_name = entry.file_name().to_string_lossy().to_string();
            if entry_name == WORK_AREA_LOCK_FILENAME {
                threadlane_workers::validate_preexisting_private_file(
                    &entry.path(),
                    "work area ownership lock",
                )
                .map_err(|err| Error::InvalidConfig(err.to_string()))?;
                let lock_meta = fs::symlink_metadata(entry.path()).map_err(|err| {
                    Error::InvalidConfig(format!("cannot inspect work area ownership lock: {err}"))
                })?;
                if lock_meta.len() != 0 {
                    return Err(Error::InvalidConfig(
                        "work area ownership lock must be empty".to_string(),
                    ));
                }
            }
            entries.push(entry_name);
        }
        if !entries.is_empty() {
            let has_marker = entries.iter().any(|n| n == ".threadlane-work-area.json");
            let has_unexpected = entries.iter().any(|n| {
                !matches!(
                    n.as_str(),
                    ".threadlane-work-area.json"
                        | WORK_AREA_LOCK_FILENAME
                        | "manifests"
                        | "checkouts"
                )
            });
            if !has_marker || has_unexpected {
                return Err(Error::InvalidConfig(format!(
                    "existing non-empty work_area_root `{}` is not a valid Threadlane work area",
                    checked.display()
                )));
            }
        }
    }

    Ok(checked)
}

fn contains_unsafe_permission_pattern_char(value: &str) -> bool {
    value.chars().any(|ch| {
        matches!(ch, '*' | '?' | '[' | ']' | '{' | '}' | '(' | ')' | '\\') || ch.is_control()
    })
}

fn validate_task_files_profile_paths(
    profile_id: &str,
    provider: WorkerProvider,
    work_area_root: &Path,
    allowed_write_paths: &BTreeSet<String>,
) -> Result<(), Error> {
    if !matches!(
        provider,
        WorkerProvider::Antigravity | WorkerProvider::OpenCodeV2
    ) {
        return Err(Error::InvalidConfig(format!(
            "profile `{profile_id}`: `file_permissions: \"task_files\"` is only supported for providers `antigravity` and `opencode_v2`"
        )));
    }
    let work_area_str = work_area_root.to_str().ok_or_else(|| {
        Error::InvalidConfig(format!(
            "profile `{profile_id}`: `work_area_root` must be valid UTF-8 when `file_permissions` is `task_files`"
        ))
    })?;
    if contains_unsafe_permission_pattern_char(work_area_str) {
        return Err(Error::InvalidConfig(format!(
            "profile `{profile_id}`: `work_area_root` `{work_area_str}` contains unsafe permission-pattern metacharacters or control characters"
        )));
    }
    for path_entry in allowed_write_paths {
        if contains_unsafe_permission_pattern_char(path_entry) {
            return Err(Error::InvalidConfig(format!(
                "profile `{profile_id}`: `allowed_write_paths` entry `{path_entry}` contains unsafe permission-pattern metacharacters or control characters"
            )));
        }
    }
    Ok(())
}

fn validate_profile_side_effect_free(
    profile: &WorkerProfileConfig,
) -> Result<ValidatedWorkerProfile, Error> {
    validate_identifier_chars(&profile.profile_id, "profile_id", MAX_WORKER_PROFILE_ID_LEN)?;
    validate_simple_identifier(&profile.profile_id, "profile_id")
        .map_err(|err| Error::InvalidConfig(err.to_string()))?;

    if profile.allowed_binding_ids.is_empty() {
        return Err(Error::InvalidConfig(format!(
            "profile `{}` must specify at least one allowed binding_id",
            profile.profile_id
        )));
    }
    for binding_id in &profile.allowed_binding_ids {
        validate_identifier_chars(binding_id, "allowed_binding_ids entry", MAX_ID_LEN)?;
    }

    let normalized_base_commit = validate_exact_commit_sha(&profile.spec.base_commit)?;
    let checked_work_area = validate_work_area_side_effect_free(&profile.spec.work_area_root)?;

    let placeholder_spec = profile
        .spec
        .to_worker_spec("validation-placeholder".to_string());
    let validation = placeholder_spec
        .validate()
        .map_err(|err| Error::InvalidConfig(format!("profile `{}`: {err}", profile.profile_id)))?;

    if profile.spec.file_permissions == WorkerFilePermissions::TaskFiles {
        validate_task_files_profile_paths(
            &profile.profile_id,
            profile.spec.provider,
            &checked_work_area,
            &profile.spec.allowed_write_paths,
        )?;
    }

    let git_entry = validation.canonical_repository_path.join(".git");
    if fs::symlink_metadata(&git_entry).is_err() {
        return Err(Error::InvalidConfig(format!(
            "profile `{}` repository_path `{}` does not contain a `.git` entry",
            profile.profile_id,
            validation.canonical_repository_path.display()
        )));
    }

    let requested_effort = validation
        .resolved_model
        .effort
        .clone()
        .unwrap_or_else(|| "default".to_string());
    validate_identifier_chars(&requested_effort, "effort", MAX_ID_LEN)?;

    let mut canonical_template = profile.spec.clone();
    canonical_template.executable = validation.canonical_executable.clone();
    canonical_template.repository_path = validation.canonical_repository_path.clone();
    canonical_template.work_area_root = checked_work_area;
    canonical_template.base_commit = normalized_base_commit;
    canonical_template.file_permissions = validation.file_permissions;

    let profile_digest = compute_profile_digest(
        &profile.profile_id,
        &profile.allowed_binding_ids,
        &canonical_template,
        &validation.resolved_model,
        &requested_effort,
    )?;

    Ok(ValidatedWorkerProfile {
        profile_id: profile.profile_id.clone(),
        profile_digest,
        allowed_binding_ids: profile.allowed_binding_ids.clone(),
        template: canonical_template,
        resolved_model: validation.resolved_model,
        requested_effort,
    })
}

fn compute_profile_digest(
    profile_id: &str,
    allowed_binding_ids: &BTreeSet<String>,
    template: &WorkerSpecTemplate,
    resolved_model: &ResolvedModelTarget,
    requested_effort: &str,
) -> Result<String, Error> {
    let mut canonical = serde_json::json!({
        "profile_id": profile_id,
        "allowed_binding_ids": allowed_binding_ids,
        "provider": template.provider.as_str(),
        "executable": template.executable.to_string_lossy(),
        "base_model": resolved_model.base_model,
        "requested_effort": requested_effort,
        "cli_model_arg": resolved_model.cli_model_arg,
        "allowed_models": template.allowed_models,
        "standalone": template.standalone,
        "session_id": template.session_id,
        "repository_path": template.repository_path.to_string_lossy(),
        "base_commit": template.base_commit,
        "work_area_root": template.work_area_root.to_string_lossy(),
        "allowed_write_paths": template.allowed_write_paths,
        "budgets": {
            "timeout_ms": template.budgets.timeout_ms,
            "max_line_bytes": template.budgets.max_line_bytes,
            "max_stdout_bytes": template.budgets.max_stdout_bytes,
            "max_stderr_bytes": template.budgets.max_stderr_bytes,
            "max_events": template.budgets.max_events,
        }
    });
    if template.file_permissions == WorkerFilePermissions::TaskFiles {
        canonical["file_permissions"] = serde_json::json!("task_files");
    }
    let bytes = serde_json::to_vec(&canonical)?;
    let mut hasher = Sha256::new();
    hasher.update(&bytes);
    let digest = hasher.finalize();
    let mut hex = String::with_capacity(64);
    for byte in digest {
        use std::fmt::Write as _;
        let _ = write!(&mut hex, "{byte:02x}");
    }
    Ok(hex)
}

/// Derives a compact, deterministic worktree identifier tag (`14` chars, `<prefix>_<12hex>`)
/// from a `run_id` or `worker_id` using SHA-256 so composite worktree IDs always stay within
/// `threadlane-workers`'s 64-character identifier budget without prefix collisions.
pub(crate) fn compact_worktree_tag(id: &str, prefix: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(id.as_bytes());
    let digest = hasher.finalize();
    let mut hex = String::with_capacity(12);
    for byte in &digest[..6] {
        use std::fmt::Write as _;
        let _ = write!(&mut hex, "{byte:02x}");
    }
    format!("{prefix}_{hex}")
}

/// Creates the owned per-run integration worktree and per-task execution worktree for a
/// claimed worker attempt.
pub(crate) fn prepare_claim_worktrees(
    profile: &ValidatedWorkerProfile,
    claim: &WorkerClaim,
) -> Result<WorkerWorktreeUpdate, WorkerError> {
    let manager = WorktreeManager::new(
        &profile.template.repository_path,
        &profile.template.work_area_root,
        &claim.base_commit,
    )?;
    let wt_run_tag = compact_worktree_tag(&claim.run_id, "r");
    let wt_task_tag = compact_worktree_tag(&claim.worker_id, "w");

    let run_manifest = manager.create_run_worktree(&wt_run_tag)?;
    let task_manifest = manager.create_task_worktree(
        &wt_run_tag,
        &wt_task_tag,
        &profile.template.allowed_write_paths,
    )?;

    Ok(WorkerWorktreeUpdate {
        run_worktree_id: run_manifest.worktree_id,
        run_worktree_path: truncate_path_string(&run_manifest.checkout_path),
        run_branch_name: run_manifest.branch_name,
        task_worktree_id: task_manifest.worktree_id,
        task_worktree_path: truncate_path_string(&task_manifest.checkout_path),
        task_branch_name: task_manifest.branch_name,
        base_commit: manager.resolved_base_commit().to_string(),
    })
}

/// Executes the worker task inside its pre-created task worktree while streaming live normalized
/// events over a bounded channel.
pub(crate) async fn execute_claimed_worker(
    profile: &ValidatedWorkerProfile,
    claim: &WorkerClaim,
    task_worktree_id: &str,
    cancel_rx: &mut watch::Receiver<bool>,
    progress_tx: mpsc::Sender<NormalizedWorkerEvent>,
) -> Result<TaskExecutionReport, WorkerError> {
    let spec = profile.build_spec(claim.instruction.clone());
    let wt_run_tag = compact_worktree_tag(&claim.run_id, "r");
    let wt_task_tag = compact_worktree_tag(&claim.worker_id, "w");
    execute_worker_task_with_observer(
        &spec,
        &wt_run_tag,
        &wt_task_tag,
        Some(task_worktree_id),
        cancel_rx,
        Some(progress_tx),
    )
    .await
}

/// Maps a live [`NormalizedWorkerEvent`] from the worker observer channel to a compact,
/// sanitized [`WorkerProgressUpdate`].
pub(crate) fn map_live_event_to_progress(event: &NormalizedWorkerEvent) -> WorkerProgressUpdate {
    let event_kind = match event.kind {
        NormalizedEventKind::Init => "system_init".to_string(),
        NormalizedEventKind::Progress => "assistant_message".to_string(),
        NormalizedEventKind::Tool => {
            if event.raw_type.contains("result") {
                "tool_result".to_string()
            } else {
                "tool_use".to_string()
            }
        }
        NormalizedEventKind::Blocked => "blocked".to_string(),
        NormalizedEventKind::Error => "error".to_string(),
        NormalizedEventKind::Result => "result".to_string(),
        NormalizedEventKind::Other => "other".to_string(),
    };

    let raw_summary = event
        .summary
        .as_deref()
        .filter(|s| !s.trim().is_empty())
        .or(event.tool_name.as_deref())
        .unwrap_or(&event.raw_type);
    let sanitized = sanitize_and_redact(raw_summary, MAX_WORKER_SUMMARY_TEXT_BYTES);
    let summary = if sanitized.trim().is_empty() {
        event_kind.clone()
    } else {
        sanitized
    };

    WorkerProgressUpdate {
        event_kind,
        summary,
    }
}

/// Builds a [`WorkerCompletionRecord`] for a claimed worker that was cancelled by the caller
/// prior to spawning the child process.
pub(crate) fn build_pre_spawn_cancelled_completion(
    worktree_update: Option<&WorkerWorktreeUpdate>,
) -> WorkerCompletionRecord {
    WorkerCompletionRecord {
        status: WorkerStatus::Cancelled,
        observed_model: None,
        observed_effort: None,
        head_commit: worktree_update.map(|u| u.base_commit.clone()),
        dropped_live_events: 0,
        outcome: WorkerOutcomeSummary {
            exit_code: None,
            signal: None,
            duration_ms: Some(0),
            timed_out: false,
            cancelled: true,
            cancel_requested_before_completion: true,
            missing_model_evidence: false,
            missing_effort_evidence: false,
            provider_error: false,
            incomplete_cleanup: false,
            rejected_by_diff_gate: false,
            stdout_truncated: false,
            stderr_truncated: false,
            acceptance_state: WorkerAcceptanceState::ExecutionFailed,
            changed_paths_count: 0,
            changed_paths_preview: Vec::new(),
            violation_count: 0,
            violation_summaries: Vec::new(),
            final_summary: None,
            failure_reason: Some(
                "worker execution was cancelled before process launch".to_string(),
            ),
            report_artifact_id: None,
        },
    }
}

/// Builds a [`WorkerCompletionRecord`] from a [`TaskExecutionReport`] (or execution error) and
/// optionally writes a private, redacted finite-size local report artifact inside `state_dir`.
pub(crate) fn build_completion_record(
    state_dir: &Path,
    claim: &WorkerClaim,
    result: Result<TaskExecutionReport, WorkerError>,
    cancel_requested_before_completion: bool,
) -> WorkerCompletionRecord {
    match result {
        Ok(report) => build_completion_from_report(
            state_dir,
            claim,
            report,
            cancel_requested_before_completion,
        ),
        Err(err) => build_completion_from_error(err, cancel_requested_before_completion),
    }
}

fn has_complete_natural_completion_evidence(report: &WorkerExecutionReport) -> bool {
    report.exit_code == Some(0)
        && report.signal.is_none()
        && report.status != WorkerRunStatus::TimedOut
        && !report.cleanup_incomplete
        && report.overflow.is_none()
        && report.evidence.blocked_events.is_empty()
        && report.evidence.provider_errors.is_empty()
        && report.evidence.malformed_lines_count == 0
        && report.evidence.model_evidence.is_satisfied()
        && report.evidence.effort_evidence.is_satisfied()
        && report.evidence.session_evidence.is_satisfied()
        && report.evidence.saw_result_event
}

fn build_completion_from_report(
    state_dir: &Path,
    claim: &WorkerClaim,
    report: TaskExecutionReport,
    cancel_requested_flag: bool,
) -> WorkerCompletionRecord {
    let worker_report = &report.worker_report;
    let diff_gate = &report.diff_gate;

    let natural_success_evidence = has_complete_natural_completion_evidence(worker_report);
    let cancel_requested_before_completion =
        cancel_requested_flag || worker_report.status == WorkerRunStatus::Cancelled;

    // If cancellation raced with a process that had already emitted full valid completion
    // evidence and exited 0 without signal termination, retain the genuine completion/diff
    // result while preserving `cancel_requested_before_completion = true`.
    let proven_cancelled =
        worker_report.status == WorkerRunStatus::Cancelled && !natural_success_evidence;
    let timed_out = worker_report.status == WorkerRunStatus::TimedOut;

    let (status, acceptance_state) = if proven_cancelled {
        (
            WorkerStatus::Cancelled,
            WorkerAcceptanceState::ExecutionFailed,
        )
    } else if timed_out {
        (
            WorkerStatus::TimedOut,
            WorkerAcceptanceState::ExecutionFailed,
        )
    } else if worker_report.status == WorkerRunStatus::Succeeded || natural_success_evidence {
        if diff_gate.passed {
            (
                WorkerStatus::Completed,
                WorkerAcceptanceState::ReadyForReview,
            )
        } else {
            (
                WorkerStatus::Failed,
                WorkerAcceptanceState::RejectedByDiffGate,
            )
        }
    } else if !diff_gate.passed
        && matches!(
            report.overall_acceptance_state,
            WorkAcceptanceState::RejectedByDiffGate
        )
    {
        (
            WorkerStatus::Failed,
            WorkerAcceptanceState::RejectedByDiffGate,
        )
    } else {
        (WorkerStatus::Failed, WorkerAcceptanceState::ExecutionFailed)
    };

    let observed_model = extract_observed_field(&worker_report.evidence.model_evidence);
    let observed_effort = extract_observed_field(&worker_report.evidence.effort_evidence);

    let missing_model_evidence = !worker_report.evidence.model_evidence.is_satisfied()
        || matches!(
            worker_report.status,
            WorkerRunStatus::MissingModelEvidence | WorkerRunStatus::ModelMismatch
        );
    let missing_effort_evidence = !worker_report.evidence.effort_evidence.is_satisfied()
        || matches!(
            worker_report.status,
            WorkerRunStatus::MissingEffortEvidence | WorkerRunStatus::EffortMismatch
        );
    let provider_error = !worker_report.evidence.provider_errors.is_empty()
        || worker_report.status == WorkerRunStatus::ProviderError;
    let incomplete_cleanup = worker_report.cleanup_incomplete
        || worker_report.status == WorkerRunStatus::IncompleteCleanup;
    let rejected_by_diff_gate = !diff_gate.passed;

    let stdout_truncated = matches!(
        worker_report.overflow,
        Some(
            OutputOverflowKind::StdoutLineExceeded { .. }
                | OutputOverflowKind::StdoutTotalExceeded { .. }
        )
    );
    let stderr_truncated = matches!(
        worker_report.overflow,
        Some(OutputOverflowKind::StderrTotalExceeded { .. })
    );

    let changed_paths_count = u32::try_from(diff_gate.changed_paths.len()).unwrap_or(u32::MAX);
    let changed_paths_preview: Vec<String> = diff_gate
        .changed_paths
        .iter()
        .take(MAX_WORKER_CHANGED_PATHS_PREVIEW)
        .map(|entry| sanitize_and_redact(&entry.path, MAX_WORKER_PATH_BYTES))
        .filter(|path| !path.trim().is_empty())
        .collect();

    let violation_count = u32::try_from(diff_gate.violations.len()).unwrap_or(u32::MAX);
    let violation_summaries: Vec<String> = diff_gate
        .violations
        .iter()
        .take(MAX_WORKER_VIOLATION_SUMMARIES)
        .map(|v| {
            let raw = format!("{:?} on `{}`: {}", v.kind, v.path, v.detail);
            sanitize_and_redact(&raw, MAX_WORKER_SUMMARY_TEXT_BYTES)
        })
        .filter(|s| !s.trim().is_empty())
        .collect();

    let final_summary = extract_final_summary(worker_report);
    let failure_reason = if status == WorkerStatus::Completed {
        None
    } else {
        Some(build_failure_reason(
            status,
            worker_report,
            diff_gate.passed,
            &violation_summaries,
        ))
    };

    let head_commit = if diff_gate.head_commit.trim().is_empty() {
        None
    } else {
        Some(sanitize_and_redact(
            diff_gate.head_commit.trim(),
            MAX_WORKER_DIGEST_LEN,
        ))
    };

    let dropped_live_events = u32::try_from(worker_report.dropped_live_events).unwrap_or(u32::MAX);
    let report_artifact_id = write_private_report_artifact(state_dir, claim, &report);

    WorkerCompletionRecord {
        status,
        observed_model,
        observed_effort,
        head_commit,
        dropped_live_events,
        outcome: WorkerOutcomeSummary {
            exit_code: worker_report.exit_code,
            signal: worker_report.signal,
            duration_ms: Some(worker_report.duration_ms),
            timed_out,
            cancelled: proven_cancelled,
            cancel_requested_before_completion,
            missing_model_evidence,
            missing_effort_evidence,
            provider_error,
            incomplete_cleanup,
            rejected_by_diff_gate,
            stdout_truncated,
            stderr_truncated,
            acceptance_state,
            changed_paths_count,
            changed_paths_preview,
            violation_count,
            violation_summaries,
            final_summary,
            failure_reason,
            report_artifact_id,
        },
    }
}

fn build_completion_from_error(
    err: WorkerError,
    cancel_requested_before_completion: bool,
) -> WorkerCompletionRecord {
    let raw_reason = format!("worker execution failed before completion: {err}");
    let sanitized = sanitize_and_redact(&raw_reason, MAX_WORKER_SUMMARY_TEXT_BYTES);
    let failure_reason = if sanitized.trim().is_empty() {
        "worker execution failed".to_string()
    } else {
        sanitized
    };

    WorkerCompletionRecord {
        status: WorkerStatus::Failed,
        observed_model: None,
        observed_effort: None,
        head_commit: None,
        dropped_live_events: 0,
        outcome: WorkerOutcomeSummary {
            exit_code: None,
            signal: None,
            duration_ms: None,
            timed_out: false,
            cancelled: false,
            cancel_requested_before_completion,
            missing_model_evidence: false,
            missing_effort_evidence: false,
            provider_error: false,
            incomplete_cleanup: false,
            rejected_by_diff_gate: false,
            stdout_truncated: false,
            stderr_truncated: false,
            acceptance_state: WorkerAcceptanceState::ExecutionFailed,
            changed_paths_count: 0,
            changed_paths_preview: Vec::new(),
            violation_count: 0,
            violation_summaries: Vec::new(),
            final_summary: None,
            failure_reason: Some(failure_reason),
            report_artifact_id: None,
        },
    }
}

fn extract_observed_field(evidence: &FieldEvidence) -> Option<String> {
    let raw = match evidence {
        FieldEvidence::VerifiedMatch { value } => Some(value.as_str()),
        FieldEvidence::Mismatch { observed, .. } => Some(observed.as_str()),
        FieldEvidence::NotRequested { observed } => observed.as_deref(),
        FieldEvidence::Missing { .. } => None,
    }?;
    let cleaned = sanitize_and_redact(raw.trim(), MAX_ID_LEN);
    if cleaned.trim().is_empty() || cleaned.chars().any(char::is_whitespace) {
        None
    } else {
        Some(cleaned)
    }
}

fn extract_final_summary(worker_report: &WorkerExecutionReport) -> Option<String> {
    let candidate = worker_report
        .evidence
        .retained_events
        .iter()
        .rev()
        .find(|ev| ev.kind == NormalizedEventKind::Result && ev.summary.is_some())
        .or_else(|| {
            worker_report
                .evidence
                .retained_events
                .iter()
                .rev()
                .find(|ev| ev.summary.is_some())
        })
        .and_then(|ev| ev.summary.as_deref())?;
    let sanitized = sanitize_and_redact(candidate, MAX_WORKER_SUMMARY_TEXT_BYTES);
    if sanitized.trim().is_empty() {
        None
    } else {
        Some(sanitized)
    }
}

fn build_failure_reason(
    status: WorkerStatus,
    worker_report: &WorkerExecutionReport,
    diff_gate_passed: bool,
    violation_summaries: &[String],
) -> String {
    let raw = if status == WorkerStatus::Cancelled {
        "worker execution was cancelled and process group stopped".to_string()
    } else if status == WorkerStatus::TimedOut {
        "worker execution exceeded configured timeout budget".to_string()
    } else if let Some(err) = worker_report.evidence.provider_errors.first() {
        match &err.code {
            Some(code) => format!("provider error ({code}): {}", err.message),
            None => format!("provider error: {}", err.message),
        }
    } else if let Some(blocked) = worker_report.evidence.blocked_events.first() {
        format!("worker blocked ({:?}): {}", blocked.kind, blocked.summary)
    } else if matches!(
        worker_report.status,
        WorkerRunStatus::ModelMismatch | WorkerRunStatus::MissingModelEvidence
    ) {
        format!(
            "model verification failed ({}): {:?}",
            worker_report.status.as_str(),
            worker_report.evidence.model_evidence
        )
    } else if matches!(
        worker_report.status,
        WorkerRunStatus::EffortMismatch | WorkerRunStatus::MissingEffortEvidence
    ) {
        format!(
            "effort verification failed ({}): {:?}",
            worker_report.status.as_str(),
            worker_report.evidence.effort_evidence
        )
    } else if worker_report.cleanup_incomplete
        || worker_report.status == WorkerRunStatus::IncompleteCleanup
    {
        "worker process group or pipe drain cleanup remained incomplete".to_string()
    } else if let Some(overflow) = worker_report.overflow {
        format!("worker output exceeded configured budget ({overflow:?})")
    } else if worker_report.status == WorkerRunStatus::MalformedStream {
        "worker stdout stream contained malformed non-JSON lines".to_string()
    } else if !diff_gate_passed {
        if let Some(first_violation) = violation_summaries.first() {
            format!("post-run diff gate rejected changes: {first_violation}")
        } else {
            "post-run diff gate rejected worktree changes".to_string()
        }
    } else if let Some(stderr) = &worker_report.redacted_stderr_summary {
        format!(
            "worker failed ({}): {stderr}",
            worker_report.status.as_str()
        )
    } else {
        format!(
            "worker failed with status `{}` (exit_code={:?}, signal={:?})",
            worker_report.status.as_str(),
            worker_report.exit_code,
            worker_report.signal
        )
    };

    let sanitized = sanitize_and_redact(&raw, MAX_WORKER_SUMMARY_TEXT_BYTES);
    if sanitized.trim().is_empty() {
        "worker execution failed".to_string()
    } else {
        sanitized
    }
}

fn truncate_path_string(path: &Path) -> String {
    let lossy = path.to_string_lossy();
    sanitize_and_redact(&lossy, MAX_WORKER_PATH_BYTES)
}

fn write_private_report_artifact(
    state_dir: &Path,
    claim: &WorkerClaim,
    report: &TaskExecutionReport,
) -> Option<String> {
    let artifact_id = format!("art_{}", claim.worker_id);
    if artifact_id.len() > MAX_ID_LEN {
        return None;
    }

    // Never persist raw prompt argv in the local report artifact.
    let mut sanitized_report = report.clone();
    sanitized_report.worker_report.constructed_argv.clear();

    let payload = serde_json::to_vec(&sanitized_report).ok()?;
    if payload.len() > MAX_LOCAL_REPORT_ARTIFACT_BYTES {
        return None;
    }

    validate_no_symlink_components(state_dir, "worker report state directory").ok()?;
    let state_meta = fs::symlink_metadata(state_dir).ok()?;
    if state_meta.file_type().is_symlink() || !state_meta.file_type().is_dir() {
        return None;
    }

    let artifacts_dir = state_dir.join("worker_reports");
    validate_no_symlink_components(&artifacts_dir, "worker reports directory").ok()?;
    match fs::symlink_metadata(&artifacts_dir) {
        Ok(_) => {}
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            let mut builder = fs::DirBuilder::new();
            #[cfg(unix)]
            {
                use std::os::unix::fs::DirBuilderExt;
                builder.mode(0o700);
            }
            match builder.create(&artifacts_dir) {
                Ok(()) => {}
                Err(create_err) if create_err.kind() == std::io::ErrorKind::AlreadyExists => {}
                Err(_) => return None,
            }
        }
        Err(_) => return None,
    }

    validate_no_symlink_components(&artifacts_dir, "worker reports directory").ok()?;
    let artifacts_meta = fs::symlink_metadata(&artifacts_dir).ok()?;
    if artifacts_meta.file_type().is_symlink() || !artifacts_meta.file_type().is_dir() {
        return None;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if artifacts_meta.permissions().mode() & 0o777 != 0o700 {
            return None;
        }
    }

    let path = artifacts_dir.join(format!("{artifact_id}.json"));
    if path.parent() != Some(artifacts_dir.as_path()) {
        return None;
    }
    validate_no_symlink_components(&path, "worker report artifact").ok()?;

    let mut opts = OpenOptions::new();
    opts.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
    }
    let mut file = opts.open(&path).ok()?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        file.set_permissions(fs::Permissions::from_mode(0o600))
            .ok()?;
    }
    file.write_all(&payload).ok()?;
    file.sync_all().ok()?;
    Some(artifact_id)
}
