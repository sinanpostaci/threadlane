use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Component, Path, PathBuf};

use serde::{Deserialize, Deserializer, Serialize};
use sha2::{Digest, Sha256};
use threadlane_core::{
    WorkerAcceptanceState, WorkerClaim, WorkerCompletionRecord, WorkerExecutionBinding,
    WorkerOutcomeSummary, WorkerProgressUpdate, WorkerStatus, WorkerWorktreeUpdate, MAX_ID_LEN,
    MAX_WORKER_CHANGED_PATHS_PREVIEW, MAX_WORKER_DIGEST_LEN, MAX_WORKER_PATH_BYTES,
    MAX_WORKER_PROFILE_ID_LEN, MAX_WORKER_SUMMARY_TEXT_BYTES, MAX_WORKER_VIOLATION_SUMMARIES,
};
use threadlane_workers::git::{
    resolve_allowed_write_scopes_at_base, DEFAULT_GIT_COMMAND_TIMEOUT_MS, MANIFESTS_SUBDIR,
    WORK_AREA_LOCK_FILENAME,
};
pub use threadlane_workers::{
    dependency_input_lock_path, read_dependency_input, validate_dependency_input_policy,
    DependencyInputPolicy, DependencyInputSeal, DependencyPredecessorInput,
    VerifiedDependencyInput, WorkerFilePermissions, DEPENDENCIES_MANIFEST_SUBDIR,
    DEPENDENCY_INPUT_SCHEMA_VERSION, MAX_DEPENDENCY_INPUT_BYTES, MAX_DEPENDENCY_PREDECESSORS,
};
use threadlane_workers::{
    execute_worker_task_with_observer, sanitize_and_redact,
    validate_absolute_no_symlink_components, validate_simple_identifier, ExecutionBudgets,
    NormalizedEventKind, NormalizedWorkerEvent, OutputOverflowKind, ResolvedModelTarget,
    TaskExecutionReport, WorkAcceptanceState, WorkerError, WorkerExecutionReport, WorkerProvider,
    WorkerRunStatus, WorkerSpec, WorktreeManager,
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
    /// Optional startup dependency input policy for dependent tasks.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dependency_input: Option<DependencyInputPolicy>,
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

        let dependency_input: Option<DependencyInputPolicy> = match map.remove("dependency_input") {
            Some(v) => serde_json::from_value(v).map_err(serde::de::Error::custom)?,
            None => None,
        };

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
                        &[
                            "profile_id",
                            "allowed_binding_ids",
                            "spec",
                            "dependency_input",
                        ],
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
            dependency_input,
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
    /// Optional validated dependency input policy for dependent tasks.
    pub dependency_input: Option<DependencyInputPolicy>,
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
        WorkerProvider::Antigravity | WorkerProvider::OpenCodeV2 | WorkerProvider::CodexCli
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

    let canonical_dependency_input = match &profile.dependency_input {
        Some(policy) => {
            validate_dependency_input_policy(policy, &checked_work_area).map_err(|err| {
                Error::InvalidConfig(format!("profile `{}`: {err}", profile.profile_id))
            })?;
            let mut normalized = policy.clone();
            normalized.seal_path = checked_work_area
                .join(MANIFESTS_SUBDIR)
                .join(DEPENDENCIES_MANIFEST_SUBDIR)
                .join(format!("{}.json", policy.task_id));
            Some(normalized)
        }
        None => None,
    };

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
        canonical_dependency_input.as_ref(),
    )?;

    Ok(ValidatedWorkerProfile {
        profile_id: profile.profile_id.clone(),
        profile_digest,
        allowed_binding_ids: profile.allowed_binding_ids.clone(),
        template: canonical_template,
        resolved_model: validation.resolved_model,
        requested_effort,
        dependency_input: canonical_dependency_input,
    })
}

fn validate_frozen_canonical_path(
    path: &Path,
    field: &str,
    profile_id: &str,
) -> Result<PathBuf, Error> {
    let Some(raw) = path.to_str() else {
        return Err(Error::InvalidConfig(format!(
            "profile `{profile_id}`: `{field}` must be valid UTF-8"
        )));
    };
    if raw.is_empty()
        || raw.len() > MAX_WORKER_PATH_BYTES
        || raw.contains('\0')
        || !path.is_absolute()
        || raw.ends_with('/')
        || raw.contains("//")
    {
        return Err(Error::InvalidConfig(format!(
            "profile `{profile_id}`: `{field}` must be a normalized non-root absolute path"
        )));
    }
    let mut saw_root = false;
    let mut normal_count = 0usize;
    for comp in path.components() {
        match comp {
            Component::RootDir if !saw_root && normal_count == 0 => saw_root = true,
            Component::Normal(p) if saw_root && !p.is_empty() => normal_count += 1,
            _ => {
                return Err(Error::InvalidConfig(format!(
                    "profile `{profile_id}`: `{field}` has non-normal path component"
                )));
            }
        }
    }
    if !saw_root || normal_count == 0 {
        return Err(Error::InvalidConfig(format!(
            "profile `{profile_id}`: `{field}` must be a non-root absolute path"
        )));
    }
    Ok(path.to_path_buf())
}

/// Computes the canonical 64-hex SHA-256 identity digest for a frozen [`WorkerProfileConfig`]
/// without checking executable existence on disk.
///
/// This is a pure read-only historical identity helper and is **not** executable validation
/// or runtime execution admission authority.
pub fn compute_frozen_profile_identity_digest(
    profile: &WorkerProfileConfig,
) -> Result<String, Error> {
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
    let checked_executable = validate_frozen_canonical_path(
        &profile.spec.executable,
        "executable",
        &profile.profile_id,
    )?;
    let checked_repository_path = validate_frozen_canonical_path(
        &profile.spec.repository_path,
        "repository_path",
        &profile.profile_id,
    )?;
    let checked_work_area = validate_frozen_canonical_path(
        &profile.spec.work_area_root,
        "work_area_root",
        &profile.profile_id,
    )?;
    if checked_repository_path == checked_work_area
        || checked_executable == checked_repository_path
        || checked_executable == checked_work_area
    {
        return Err(Error::InvalidConfig(format!(
            "profile `{}`: executable, repository_path, and work_area_root must be distinct",
            profile.profile_id
        )));
    }
    if profile.spec.allowed_write_paths.is_empty() {
        return Err(Error::InvalidConfig(format!(
            "profile `{}`: `allowed_write_paths` must not be empty",
            profile.profile_id
        )));
    }
    for entry in &profile.spec.allowed_write_paths {
        let trimmed = entry.trim();
        let rel = entry.strip_suffix('/').unwrap_or(entry);
        if trimmed.is_empty()
            || trimmed != entry
            || rel.is_empty()
            || entry.len() > MAX_WORKER_PATH_BYTES
            || entry.starts_with('/')
            || entry.contains("//")
            || entry.chars().any(|c| {
                c.is_control()
                    || matches!(
                        c,
                        '\\' | '*'
                            | '?'
                            | '['
                            | ']'
                            | '{'
                            | '}'
                            | '$'
                            | '`'
                            | ';'
                            | '|'
                            | '&'
                            | '<'
                            | '>'
                    )
            })
            || rel
                .split('/')
                .any(|seg| seg.is_empty() || seg == "." || seg == "..")
        {
            return Err(Error::InvalidConfig(format!(
                "profile `{}`: invalid `allowed_write_paths` entry `{entry}`",
                profile.profile_id
            )));
        }
        let mut count = 0usize;
        for comp in Path::new(rel).components() {
            match comp {
                Component::Normal(p) if !p.is_empty() => count += 1,
                _ => {
                    return Err(Error::InvalidConfig(format!(
                        "profile `{}`: `allowed_write_paths` entry `{entry}` must be normalized relative",
                        profile.profile_id
                    )));
                }
            }
        }
        if count == 0 {
            return Err(Error::InvalidConfig(format!(
                "profile `{}`: empty `allowed_write_paths` entry",
                profile.profile_id
            )));
        }
    }
    match profile.spec.provider {
        WorkerProvider::OpenCodeV2 if !profile.spec.standalone => {
            return Err(Error::InvalidConfig(format!(
                "profile `{}`: OpenCode v2 requires `standalone: true`",
                profile.profile_id
            )));
        }
        WorkerProvider::Antigravity if profile.spec.standalone => {
            return Err(Error::InvalidConfig(format!(
                "profile `{}`: Antigravity requires `standalone: false`",
                profile.profile_id
            )));
        }
        _ => {}
    }
    if let Some(ref sid) = profile.spec.session_id {
        validate_identifier_chars(sid, "session_id", MAX_ID_LEN)?;
    }
    let b = &profile.spec.budgets;
    if b.timeout_ms == 0
        || b.max_line_bytes == 0
        || b.max_stdout_bytes == 0
        || b.max_stderr_bytes == 0
        || b.max_events == 0
    {
        return Err(Error::InvalidConfig(format!(
            "profile `{}`: budgets must be non-zero",
            profile.profile_id
        )));
    }
    let canonical_dependency_input = match &profile.dependency_input {
        Some(policy) => {
            validate_dependency_input_policy(policy, &checked_work_area).map_err(|err| {
                Error::InvalidConfig(format!("profile `{}`: {err}", profile.profile_id))
            })?;
            let mut normalized = policy.clone();
            normalized.seal_path = checked_work_area
                .join(MANIFESTS_SUBDIR)
                .join(DEPENDENCIES_MANIFEST_SUBDIR)
                .join(format!("{}.json", policy.task_id));
            Some(normalized)
        }
        None => None,
    };
    let resolved_model = profile
        .spec
        .to_worker_spec("validation-placeholder".to_string())
        .resolve_model_target()
        .map_err(|err| Error::InvalidConfig(format!("profile `{}`: {err}", profile.profile_id)))?;
    if !profile
        .spec
        .allowed_models
        .contains(&resolved_model.base_model)
    {
        return Err(Error::InvalidConfig(format!(
            "profile `{}`: model `{}` is not in `allowed_models`",
            profile.profile_id, resolved_model.base_model
        )));
    }
    if profile.spec.file_permissions == WorkerFilePermissions::TaskFiles {
        validate_task_files_profile_paths(
            &profile.profile_id,
            profile.spec.provider,
            &checked_work_area,
            &profile.spec.allowed_write_paths,
        )?;
    }
    let requested_effort = resolved_model
        .effort
        .clone()
        .unwrap_or_else(|| "default".to_string());
    validate_identifier_chars(&requested_effort, "effort", MAX_ID_LEN)?;
    let mut canonical_template = profile.spec.clone();
    canonical_template.executable = checked_executable;
    canonical_template.repository_path = checked_repository_path;
    canonical_template.work_area_root = checked_work_area;
    canonical_template.base_commit = normalized_base_commit;
    compute_profile_digest(
        &profile.profile_id,
        &profile.allowed_binding_ids,
        &canonical_template,
        &resolved_model,
        &requested_effort,
        canonical_dependency_input.as_ref(),
    )
}

fn compute_profile_digest(
    profile_id: &str,
    allowed_binding_ids: &BTreeSet<String>,
    template: &WorkerSpecTemplate,
    resolved_model: &ResolvedModelTarget,
    requested_effort: &str,
    dependency_input: Option<&DependencyInputPolicy>,
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
    if let Some(policy) = dependency_input {
        canonical["dependency_input"] = serde_json::to_value(policy)?;
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

/// Resolved worker execution specification and optional dependency execution binding.
///
/// When resolved for a dependent profile, this value privately retains the shared `fs2` lock
/// lease on the verified dependency seal until dropped.
#[derive(Debug)]
pub struct ResolvedWorkerExecution {
    /// Materialized worker specification with `base_commit` set to the verified execution base.
    pub spec: WorkerSpec,
    /// Durable dependency execution binding (`Some` for dependent profiles, `None` for root profiles).
    pub execution_binding: Option<WorkerExecutionBinding>,
    profile_id: String,
    profile_digest: String,
    policy: Option<DependencyInputPolicy>,
    _lease: Option<VerifiedDependencyInput>,
}

/// Resolves and verifies the execution specification and optional [`WorkerExecutionBinding`] for
/// a worker profile before admission or claim execution.
///
/// For root profiles (`profile.dependency_input.is_none()`), returns the unmodified template
/// specification with `execution_binding: None`.
///
/// For dependent profiles (`profile.dependency_input.is_some()`), acquires the shared dependency
/// seal lock lease, verifies the seal and Git object/ancestry invariants, verifies persisted
/// predecessor review/integration receipts, verifies that allowed write scope kinds at
/// `execution_base_commit` do not widen relative to the approved template `base_commit`, updates
/// `spec.base_commit` to `execution_base_commit`, and computes the [`WorkerExecutionBinding`]
/// bound to `profile.profile_digest`.
pub fn resolve_worker_execution(
    profile: &ValidatedWorkerProfile,
    command_key: &str,
    instruction: String,
) -> Result<ResolvedWorkerExecution, Error> {
    let Some(ref policy) = profile.dependency_input else {
        return Ok(ResolvedWorkerExecution {
            spec: profile.build_spec(instruction),
            execution_binding: None,
            profile_id: profile.profile_id.clone(),
            profile_digest: profile.profile_digest.clone(),
            policy: None,
            _lease: None,
        });
    };

    let verified = read_dependency_input(
        policy,
        &profile.template.repository_path,
        &profile.template.work_area_root,
        &profile.template.base_commit,
        &instruction,
        command_key,
    )
    .map_err(|err| {
        Error::InvalidConfig(format!(
            "profile `{}` dependency input verification failed: {err}",
            profile.profile_id
        ))
    })?;

    threadlane_operations::dependency::verify_dependency_predecessor_receipts(&verified.seal)
        .map_err(|err| {
            Error::InvalidConfig(format!(
                "profile `{}` dependency predecessor receipt verification failed: {err}",
                profile.profile_id
            ))
        })?;

    let approved_scopes = resolve_allowed_write_scopes_at_base(
        &profile.template.repository_path,
        &profile.template.base_commit,
        &profile.template.allowed_write_paths,
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .map_err(|err| {
        Error::InvalidConfig(format!(
            "profile `{}` failed to resolve allowed write scopes at approved base_commit `{}`: {err}",
            profile.profile_id, profile.template.base_commit
        ))
    })?;

    let execution_scopes = resolve_allowed_write_scopes_at_base(
        &profile.template.repository_path,
        &verified.seal.execution_base_commit,
        &profile.template.allowed_write_paths,
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .map_err(|err| {
        Error::InvalidConfig(format!(
            "profile `{}` failed to resolve allowed write scopes at execution_base_commit `{}`: {err}",
            profile.profile_id, verified.seal.execution_base_commit
        ))
    })?;

    for (path_entry, approved_kind) in &approved_scopes {
        let Some(execution_kind) = execution_scopes.get(path_entry) else {
            return Err(Error::InvalidConfig(format!(
                "profile `{}` allowed_write_paths entry `{path_entry}` missing in execution base scope resolution",
                profile.profile_id
            )));
        };
        if approved_kind != execution_kind {
            return Err(Error::InvalidConfig(format!(
                "profile `{}` allowed_write_paths entry `{path_entry}` changed write scope kind between approved base `{}` ({approved_kind:?}) and execution base `{}` ({execution_kind:?})",
                profile.profile_id,
                profile.template.base_commit,
                verified.seal.execution_base_commit
            )));
        }
    }
    if approved_scopes.len() != execution_scopes.len() {
        return Err(Error::InvalidConfig(format!(
            "profile `{}` allowed_write_paths scope count changed between approved base and execution base",
            profile.profile_id
        )));
    }

    let mut spec = profile.build_spec(instruction);
    spec.base_commit = verified.seal.execution_base_commit.clone();

    let binding = WorkerExecutionBinding::new(
        &profile.profile_digest,
        &spec.prompt,
        &verified.seal.seal_digest,
        &verified.seal_file_digest,
        &verified.seal.execution_base_commit,
    )?;

    Ok(ResolvedWorkerExecution {
        spec,
        execution_binding: Some(binding),
        profile_id: profile.profile_id.clone(),
        profile_digest: profile.profile_digest.clone(),
        policy: Some(policy.clone()),
        _lease: Some(verified),
    })
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
/// claimed worker attempt using the resolved execution [`WorkerSpec`].
pub(crate) fn prepare_claim_worktrees(
    spec: &WorkerSpec,
    claim: &WorkerClaim,
) -> Result<WorkerWorktreeUpdate, WorkerError> {
    if spec.base_commit != claim.base_commit {
        return Err(WorkerError::SecurityViolation(format!(
            "resolved worker spec base_commit `{}` does not match claim base_commit `{}`",
            spec.base_commit, claim.base_commit
        )));
    }
    let manager = WorktreeManager::new(
        &spec.repository_path,
        &spec.work_area_root,
        &spec.base_commit,
    )?;
    let wt_run_tag = compact_worktree_tag(&claim.run_id, "r");
    let wt_task_tag = compact_worktree_tag(&claim.worker_id, "w");

    let run_manifest = manager.create_run_worktree(&wt_run_tag)?;
    let task_manifest =
        manager.create_task_worktree(&wt_run_tag, &wt_task_tag, &spec.allowed_write_paths)?;

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
///
/// Immediately before first provider allocation/spawn (including OpenCode session allocation),
/// re-verifies the pinned [`WorkerExecutionBinding`], pinned [`DependencyInputSeal`], and
/// persisted predecessor receipts while retaining the original shared seal lock lease.
pub(crate) async fn execute_claimed_worker(
    resolved: &ResolvedWorkerExecution,
    claim: &WorkerClaim,
    task_worktree_id: &str,
    cancel_rx: &mut watch::Receiver<bool>,
    progress_tx: mpsc::Sender<NormalizedWorkerEvent>,
) -> Result<TaskExecutionReport, WorkerError> {
    if resolved.spec.base_commit != claim.base_commit
        || resolved.spec.prompt != claim.instruction
        || resolved.execution_binding != claim.execution_binding
    {
        return Err(WorkerError::SecurityViolation(format!(
            "resolved worker execution spec/binding does not match claim `{}`",
            claim.claim_id
        )));
    }

    match (&resolved.policy, &resolved._lease, &claim.execution_binding) {
        (None, None, None) => {}
        (Some(policy), Some(pinned_verified), Some(pinned_binding)) => {
            pinned_binding
                .verify_against(&resolved.profile_digest, &claim.instruction, &claim.base_commit)
                .map_err(|err| {
                    WorkerError::SecurityViolation(format!(
                        "profile `{}` claim execution_binding failed pre-provider verification: {err}",
                        resolved.profile_id
                    ))
                })?;
            if pinned_binding.input_digest != pinned_verified.seal.seal_digest
                || pinned_binding.seal_file_digest != pinned_verified.seal_file_digest
                || pinned_binding.execution_base_commit
                    != pinned_verified.seal.execution_base_commit
            {
                return Err(WorkerError::SecurityViolation(format!(
                    "profile `{}` pinned execution_binding does not match retained dependency seal lease",
                    resolved.profile_id
                )));
            }

            let rechecked = read_dependency_input(
                policy,
                &resolved.spec.repository_path,
                &resolved.spec.work_area_root,
                &pinned_verified.seal.approved_base_commit,
                &claim.instruction,
                &policy.command_key,
            )
            .map_err(|err| {
                WorkerError::SecurityViolation(format!(
                    "profile `{}` dependency input revalidation failed before provider execution: {err}",
                    resolved.profile_id
                ))
            })?;
            if rechecked.seal != pinned_verified.seal
                || rechecked.seal.seal_digest != pinned_binding.input_digest
                || rechecked.seal_file_digest != pinned_binding.seal_file_digest
                || rechecked.seal.execution_base_commit != pinned_binding.execution_base_commit
            {
                return Err(WorkerError::SecurityViolation(format!(
                    "profile `{}` dependency input seal changed before provider execution",
                    resolved.profile_id
                )));
            }

            threadlane_operations::dependency::verify_dependency_predecessor_receipts(
                &pinned_verified.seal,
            )
            .map_err(|err| {
                WorkerError::SecurityViolation(format!(
                    "profile `{}` dependency predecessor receipt verification failed before provider execution: {err}",
                    resolved.profile_id
                ))
            })?;
        }
        _ => {
            return Err(WorkerError::SecurityViolation(format!(
                "profile `{}` dependency execution binding state is inconsistent for claim `{}`",
                resolved.profile_id, claim.claim_id
            )));
        }
    }

    let wt_run_tag = compact_worktree_tag(&claim.run_id, "r");
    let wt_task_tag = compact_worktree_tag(&claim.worker_id, "w");
    execute_worker_task_with_observer(
        &resolved.spec,
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
        NormalizedEventKind::SupervisorAttention => "supervisor_attention".to_string(),
    };

    // Only the software-minted typed variant can carry supervision metadata. A provider
    // raw_type or prose that spells the marker remains an ordinary untrusted event.
    let watchdog_attention = if event.kind == NormalizedEventKind::SupervisorAttention
        && event.line_number == 0
        && event.raw_type == "threadlane.progress_watchdog"
    {
        event
            .summary
            .as_deref()
            .filter(|raw| raw.len() <= threadlane_core::MAX_WORKER_WATCHDOG_SNAPSHOT_BYTES)
            .and_then(|raw| {
                serde_json::from_str::<threadlane_workers::ProgressWatchdogSnapshot>(raw).ok()
            })
            .map(convert_watchdog_attention)
            .filter(|snapshot| snapshot.validate_attention().is_ok())
    } else {
        None
    };
    if event.kind == NormalizedEventKind::SupervisorAttention {
        return WorkerProgressUpdate {
            event_kind,
            summary: if watchdog_attention.is_some() {
                "Progress evidence is uncertain; inspect execution status.".to_string()
            } else {
                "Ignored malformed software-supervision attention.".to_string()
            },
            watchdog_attention,
        };
    }

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
        watchdog_attention: None,
    }
}

fn convert_watchdog_attention(
    snapshot: threadlane_workers::ProgressWatchdogSnapshot,
) -> threadlane_core::WorkerProgressWatchdogSnapshot {
    use threadlane_core::{
        WorkerProgressAttentionReason as Reason, WorkerProgressWatchdogStatus as Status,
    };
    use threadlane_workers::{
        ProgressAttentionReason as SourceReason, ProgressWatchdogStatus as SourceStatus,
    };
    threadlane_core::WorkerProgressWatchdogSnapshot {
        status: match snapshot.status {
            SourceStatus::Observing => Status::Observing,
            SourceStatus::SoftDeadlineExtended => Status::SoftDeadlineExtended,
            SourceStatus::AttentionRequired => Status::AttentionRequired,
            SourceStatus::HardLimitReached => Status::HardLimitReached,
        },
        attention_reason: snapshot.attention_reason.map(|reason| match reason {
            SourceReason::NoAdvancementHints => Reason::NoAdvancementHints,
            SourceReason::ReadOnlyActivity => Reason::ReadOnlyActivity,
            SourceReason::AdvancementNotRecent => Reason::AdvancementNotRecent,
        }),
        elapsed_ms: snapshot.elapsed_ms,
        soft_deadline_ms: snapshot.soft_deadline_ms,
        hard_timeout_ms: snapshot.hard_timeout_ms,
        last_advancement_ms: snapshot.last_advancement_ms,
        events_observed: snapshot.events_observed,
        advancement_hints: snapshot.advancement_hints,
        read_hints: snapshot.read_hints,
        output_hints: snapshot.output_hints,
        other_tool_hints: snapshot.other_tool_hints,
        extensions: snapshot.extensions,
        attention_emitted: snapshot.attention_emitted,
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
    report.has_complete_natural_completion_evidence()
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
    } else if natural_success_evidence {
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

    let observed_model = worker_report
        .effective_observed_model()
        .and_then(sanitize_observed_identifier);
    let observed_effort = worker_report
        .effective_observed_effort()
        .and_then(sanitize_observed_identifier);

    let missing_model_evidence = !worker_report.has_satisfied_model_evidence()
        || matches!(
            worker_report.status,
            WorkerRunStatus::MissingModelEvidence | WorkerRunStatus::ModelMismatch
        );
    let missing_effort_evidence = !worker_report.has_satisfied_effort_evidence()
        || matches!(
            worker_report.status,
            WorkerRunStatus::MissingEffortEvidence | WorkerRunStatus::EffortMismatch
        );
    let provider_error = !worker_report.evidence.provider_errors.is_empty()
        || worker_report.status == WorkerRunStatus::ProviderError
        || worker_report.opencode_metadata_error.as_deref() == Some("provider_error");
    let incomplete_cleanup = worker_report.cleanup_incomplete
        || worker_report.status == WorkerRunStatus::IncompleteCleanup
        || worker_report.opencode_metadata_error.as_deref() == Some("cleanup_incomplete");
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

fn sanitize_observed_identifier(raw: &str) -> Option<String> {
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
    } else if worker_report.cleanup_incomplete
        || worker_report.status == WorkerRunStatus::IncompleteCleanup
    {
        "worker process group or pipe drain cleanup remained incomplete".to_string()
    } else if let Some(overflow) = worker_report.overflow {
        format!("worker output exceeded configured budget ({overflow:?})")
    } else if worker_report.status == WorkerRunStatus::MalformedStream {
        "worker stdout stream contained malformed non-JSON lines".to_string()
    } else if let Some(meta_err) = worker_report.opencode_metadata_error.as_deref() {
        format!("OpenCode session metadata verification failed ({meta_err})")
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
    if report.worker_report.provider == WorkerProvider::CodexCli
        && report
            .worker_report
            .has_pinned_codex_task_files_launch_config()
    {
        // Retain the finite preventive permission/model pins, never the private prompt.
        if let Some(prompt) = sanitized_report.worker_report.constructed_argv.last_mut() {
            *prompt = "[redacted task instruction]".to_string();
        }
    } else {
        sanitized_report.worker_report.constructed_argv.clear();
    }

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

#[cfg(test)]
mod tests {
    use super::*;
    use threadlane_core::{Command, Core};
    use threadlane_workers::{
        DiffGateReport, FieldEvidence, IsolationLevel, OpenCodeMetadataEvidence,
        StreamEvidenceSummary, WorktreeKind, WorktreeManifest, MANIFEST_SCHEMA_VERSION,
    };

    fn sample_opencode_task_report(workdir: &Path) -> TaskExecutionReport {
        let base_commit = "0123456789abcdef0123456789abcdef01234567".to_string();
        let mut allowed_write_paths = BTreeSet::new();
        allowed_write_paths.insert("src/lib.rs".to_string());

        let manifest = WorktreeManifest {
            manifest_version: MANIFEST_SCHEMA_VERSION,
            worktree_id: "wt-task-r_01-w_01-tok".to_string(),
            kind: WorktreeKind::Task,
            run_id: "r_01".to_string(),
            task_id: Some("w_01".to_string()),
            repository_path: workdir.to_path_buf(),
            repository_common_git_dir: Some(workdir.join(".git")),
            work_area_root: workdir.join("work-area"),
            checkout_path: workdir.join("work-area/checkouts/wt-task-r_01-w_01-tok"),
            manifest_path: workdir.join("work-area/manifests/wt-task-r_01-w_01-tok.json"),
            branch_name: "threadlane/task/r_01/w_01/tok".to_string(),
            base_commit: base_commit.clone(),
            allowed_write_paths,
            job_active: false,
            active_job_pid: None,
            partial_cleanup_accepted_head: None,
        };

        let worker_report = WorkerExecutionReport {
            progress_watchdog: None,
            executed_executable: None,
            provider: WorkerProvider::OpenCodeV2,
            requested_model: ResolvedModelTarget {
                base_model: "opencode-go/deepseek-v4.1-flash".to_string(),
                effort: Some("max".to_string()),
                cli_model_arg: "opencode-go/deepseek-v4.1-flash".to_string(),
            },
            requested_session_id: None,
            allocated_session_id: Some("ses_owned_001".to_string()),
            constructed_argv: vec![
                "run".to_string(),
                "--format".to_string(),
                "json".to_string(),
                "--standalone".to_string(),
                "--model".to_string(),
                "opencode-go/deepseek-v4.1-flash".to_string(),
                "--session".to_string(),
                "ses_owned_001".to_string(),
            ],
            working_directory: manifest.checkout_path.clone(),
            status: WorkerRunStatus::Succeeded,
            exit_code: Some(0),
            signal: None,
            process_group_id: Some(4242),
            duration_ms: 45,
            stdout_bytes_captured: 128,
            stderr_bytes_captured: 0,
            overflow: None,
            descendants_terminated_after_exit: false,
            cleanup_incomplete: false,
            redacted_stderr_summary: None,
            dropped_live_events: 0,
            evidence: StreamEvidenceSummary {
                total_lines: 1,
                valid_json_events: 1,
                malformed_lines_count: 0,
                malformed_diagnostics: Vec::new(),
                saw_init_event: false,
                saw_result_event: false,
                model_evidence: FieldEvidence::Missing {
                    requested: "opencode-go/deepseek-v4.1-flash".to_string(),
                },
                effort_evidence: FieldEvidence::Missing {
                    requested: "max".to_string(),
                },
                effort_evidence_source: None,
                session_evidence: FieldEvidence::VerifiedMatch {
                    value: "ses_owned_001".to_string(),
                },
                observed_session_id: Some("ses_owned_001".to_string()),
                observed_usage: None,
                codex_turn_lifecycle: None,
                provider_errors: Vec::new(),
                recoverable_step_errors_count: 0,
                blocked_events: Vec::new(),
                retained_events: Vec::new(),
                dropped_events_count: 0,
            },
            opencode_metadata: Some(OpenCodeMetadataEvidence {
                source: "opencode_sanitized_session_export".to_string(),
                strength: "cli_reported_per_assistant".to_string(),
                session_id: "ses_owned_001".to_string(),
                observed_model: "opencode-go/deepseek-v4.1-flash".to_string(),
                observed_effort: Some("max".to_string()),
                assistant_message_count: 1,
                terminal_message_id: "msg_ast_001".to_string(),
            }),
            opencode_metadata_error: None,
        };

        let diff_gate = DiffGateReport {
            worktree_id: manifest.worktree_id.clone(),
            checkout_path: manifest.checkout_path.clone(),
            base_commit: base_commit.clone(),
            head_commit: base_commit,
            isolation_level: IsolationLevel::PostRunDiffEvidence,
            preventive_sandbox: false,
            isolation_note: "post-run diff gate".to_string(),
            evidence_complete: true,
            passed: true,
            acceptance_state: WorkAcceptanceState::DiffGatePassedPendingReview,
            changed_paths: Vec::new(),
            violations: Vec::new(),
        };

        TaskExecutionReport {
            manifest,
            worker_report,
            diff_gate,
            overall_acceptance_state: WorkAcceptanceState::DiffGatePassedPendingReview,
        }
    }

    #[test]
    fn opencode_metadata_mapping_yields_ready_for_review_and_rejects_negative_stream_conflict() {
        let temp = tempfile::tempdir().expect("tempdir");
        let state_dir = temp.path().join("state");
        crate::descriptor::prepare_state_dir(&state_dir).expect("prepare_state_dir");
        let db_path = state_dir.join(crate::descriptor::DATABASE_FILENAME);
        let mut core = Core::open(&db_path).expect("open core");

        let binding = core
            .create_binding("11111111-1111-4111-8111-111111111111", "opencode-runtime")
            .expect("create_binding");
        let profile_digest = "a".repeat(64);
        let base_commit = "0123456789abcdef0123456789abcdef01234567".to_string();

        // 1. Positive case: missing stream model/effort/result supplemented by validated metadata
        // maps to Completed + ReadyForReview with accepted: false and missing flags cleared.
        let queued = core
            .execute(
                &binding.binding_id,
                Command::StartWorker {
                    command_key: "wk-opencode-pos-1".to_string(),
                    profile_id: "prof_opencode".to_string(),
                    profile_digest: profile_digest.clone(),
                    provider: "opencode_v2".to_string(),
                    requested_model: "opencode-go/deepseek-v4.1-flash".to_string(),
                    requested_effort: "max".to_string(),
                    base_commit: base_commit.clone(),
                    instruction: "implement bounded change".to_string(),
                    execution_binding: None,
                    now_ms: 1_000,
                },
            )
            .expect("start_worker")
            .into_worker()
            .expect("worker snapshot");

        let claim = core
            .claim_worker(
                &binding.binding_id,
                &queued.run_id,
                queued.revision,
                &profile_digest,
                1_001,
            )
            .expect("claim_worker");

        let report = sample_opencode_task_report(temp.path());
        assert!(!report.worker_report.evidence.saw_result_event);
        assert!(report.worker_report.requested_session_id.is_none());

        let completion = build_completion_record(&state_dir, &claim, Ok(report.clone()), false);
        assert_eq!(completion.status, WorkerStatus::Completed);
        assert_eq!(
            completion.observed_model.as_deref(),
            Some("opencode-go/deepseek-v4.1-flash")
        );
        assert_eq!(completion.observed_effort.as_deref(), Some("max"));
        assert_eq!(
            completion.outcome.acceptance_state,
            WorkerAcceptanceState::ReadyForReview
        );
        assert!(!completion.outcome.missing_model_evidence);
        assert!(!completion.outcome.missing_effort_evidence);
        assert!(!completion.outcome.provider_error);
        assert!(!completion.outcome.incomplete_cleanup);
        assert!(!completion.outcome.rejected_by_diff_gate);
        assert!(completion.outcome.failure_reason.is_none());

        let snapshot = core
            .record_worker_outcome(&claim, completion, 1_002)
            .expect("record_worker_outcome");
        assert_eq!(snapshot.status, WorkerStatus::Completed);
        let serialized_snapshot = serde_json::to_value(&snapshot).expect("serialize snapshot");
        assert!(
            serialized_snapshot.get("accepted").is_none(),
            "WorkerSnapshot must not fabricate a top-level accepted field"
        );
        assert_eq!(
            snapshot.observed_model.as_deref(),
            Some("opencode-go/deepseek-v4.1-flash")
        );
        assert_eq!(snapshot.observed_effort.as_deref(), Some("max"));
        let outcome = snapshot.outcome.expect("outcome present");
        assert_eq!(
            outcome.acceptance_state,
            WorkerAcceptanceState::ReadyForReview
        );
        assert!(!outcome.missing_model_evidence);
        assert!(!outcome.missing_effort_evidence);

        // 2. Negative case: a stream conflict (e.g. model mismatch or effort mismatch) cannot be
        // rescued by serialized metadata even if status was set to Succeeded and diff_gate passed.
        let queued_conflict = core
            .execute(
                &binding.binding_id,
                Command::StartWorker {
                    command_key: "wk-opencode-neg-1".to_string(),
                    profile_id: "prof_opencode".to_string(),
                    profile_digest: profile_digest.clone(),
                    provider: "opencode_v2".to_string(),
                    requested_model: "opencode-go/deepseek-v4.1-flash".to_string(),
                    requested_effort: "max".to_string(),
                    base_commit,
                    instruction: "attempt with conflicting stream".to_string(),
                    execution_binding: None,
                    now_ms: 2_000,
                },
            )
            .expect("start_worker conflict")
            .into_worker()
            .expect("worker snapshot");

        let claim_conflict = core
            .claim_worker(
                &binding.binding_id,
                &queued_conflict.run_id,
                queued_conflict.revision,
                &profile_digest,
                2_001,
            )
            .expect("claim_worker conflict");

        let mut conflict_report = report;
        conflict_report.worker_report.evidence.model_evidence = FieldEvidence::Mismatch {
            requested: Some("opencode-go/deepseek-v4.1-flash".to_string()),
            observed: "opencode-go/rogue-model".to_string(),
            detail: "stream reported conflicting model".to_string(),
        };

        let conflict_completion =
            build_completion_record(&state_dir, &claim_conflict, Ok(conflict_report), false);
        assert_eq!(conflict_completion.status, WorkerStatus::Failed);
        assert_eq!(
            conflict_completion.observed_model.as_deref(),
            Some("opencode-go/rogue-model")
        );
        assert_eq!(
            conflict_completion.outcome.acceptance_state,
            WorkerAcceptanceState::ExecutionFailed
        );
        assert!(conflict_completion.outcome.missing_model_evidence);

        let conflict_snapshot = core
            .record_worker_outcome(&claim_conflict, conflict_completion, 2_002)
            .expect("record_worker_outcome conflict");
        assert_eq!(conflict_snapshot.status, WorkerStatus::Failed);
        let serialized_conflict =
            serde_json::to_value(&conflict_snapshot).expect("serialize conflict snapshot");
        assert!(serialized_conflict.get("accepted").is_none());
        assert_eq!(
            conflict_snapshot.outcome.expect("outcome").acceptance_state,
            WorkerAcceptanceState::ExecutionFailed
        );
    }

    #[test]
    fn opencode_metadata_errors_preserve_verified_stream_fields_and_report_finite_metadata_provenance(
    ) {
        let temp = tempfile::tempdir().expect("tempdir");
        let state_dir = temp.path().join("state");
        crate::descriptor::prepare_state_dir(&state_dir).expect("prepare_state_dir");
        let db_path = state_dir.join(crate::descriptor::DATABASE_FILENAME);
        let mut core = Core::open(&db_path).expect("open core");

        let binding = core
            .create_binding("22222222-2222-4222-8222-222222222222", "opencode-meta-err")
            .expect("create_binding");
        let profile_digest = "b".repeat(64);
        let base_commit = "0123456789abcdef0123456789abcdef01234567".to_string();

        // 1. Verified stream model + effort (`max`) + session with non-model/effort metadata
        // errors (`missing_completion`, `nonzero_exit`, `timed_out`) must fail execution while
        // preserving observed model/effort and keeping missing_model/effort_evidence false.
        let non_field_cases: &[(&str, WorkerRunStatus, WorkerStatus, &str)] = &[
            (
                "missing_completion",
                WorkerRunStatus::MissingResultEvidence,
                WorkerStatus::Failed,
                "OpenCode session metadata verification failed (missing_completion)",
            ),
            (
                "nonzero_exit",
                WorkerRunStatus::NonZeroExit,
                WorkerStatus::Failed,
                "OpenCode session metadata verification failed (nonzero_exit)",
            ),
            (
                "timed_out",
                WorkerRunStatus::TimedOut,
                WorkerStatus::TimedOut,
                "worker execution exceeded configured timeout budget",
            ),
        ];

        for (idx, &(err_code, run_status, expected_worker_status, expected_reason)) in
            non_field_cases.iter().enumerate()
        {
            let now_base = 3_000 + (idx as i64) * 100;
            let queued = core
                .execute(
                    &binding.binding_id,
                    Command::StartWorker {
                        command_key: format!("wk-meta-nonfield-{err_code}"),
                        profile_id: "prof_opencode".to_string(),
                        profile_digest: profile_digest.clone(),
                        provider: "opencode_v2".to_string(),
                        requested_model: "opencode-go/deepseek-v4.1-flash".to_string(),
                        requested_effort: "max".to_string(),
                        base_commit: base_commit.clone(),
                        instruction: "verify stream preservation on metadata failure".to_string(),
                        execution_binding: None,
                        now_ms: now_base,
                    },
                )
                .expect("start_worker")
                .into_worker()
                .expect("worker snapshot");

            let claim = core
                .claim_worker(
                    &binding.binding_id,
                    &queued.run_id,
                    queued.revision,
                    &profile_digest,
                    now_base + 1,
                )
                .expect("claim_worker");

            let mut report = sample_opencode_task_report(temp.path());
            report.overall_acceptance_state = WorkAcceptanceState::RejectedByWorkerOutcome;
            report.worker_report.status = run_status;
            report.worker_report.opencode_metadata = None;
            report.worker_report.opencode_metadata_error = Some(err_code.to_string());
            report.worker_report.evidence.saw_result_event = false;
            report.worker_report.evidence.model_evidence = FieldEvidence::VerifiedMatch {
                value: "opencode-go/deepseek-v4.1-flash".to_string(),
            };
            report.worker_report.evidence.effort_evidence = FieldEvidence::VerifiedMatch {
                value: "max".to_string(),
            };

            assert!(!report.worker_report.evidence.saw_result_event);
            assert!(!report
                .worker_report
                .has_complete_natural_completion_evidence());
            assert!(!report.worker_report.is_completed_pending_effort_evidence());
            assert!(report.worker_report.has_satisfied_model_evidence());
            assert!(report.worker_report.has_satisfied_effort_evidence());
            assert!(report.worker_report.has_satisfied_session_evidence());
            assert!(!report.worker_report.has_satisfied_result_evidence());

            let completion = build_completion_record(&state_dir, &claim, Ok(report), false);
            assert_eq!(completion.status, expected_worker_status);
            assert_eq!(
                completion.observed_model.as_deref(),
                Some("opencode-go/deepseek-v4.1-flash")
            );
            assert_eq!(completion.observed_effort.as_deref(), Some("max"));
            assert_eq!(
                completion.outcome.acceptance_state,
                WorkerAcceptanceState::ExecutionFailed
            );
            assert!(
                !completion.outcome.missing_model_evidence,
                "verified stream model must not be marked missing for `{err_code}`"
            );
            assert!(
                !completion.outcome.missing_effort_evidence,
                "verified stream effort must not be marked missing for `{err_code}`"
            );
            let reason = completion
                .outcome
                .failure_reason
                .as_deref()
                .expect("failure_reason present");
            assert_eq!(reason, expected_reason);
            assert!(
                !reason.contains("VerifiedMatch"),
                "failure_reason must not print misleading VerifiedMatch debug for `{err_code}`"
            );

            let snapshot = core
                .record_worker_outcome(&claim, completion, now_base + 2)
                .expect("record_worker_outcome");
            assert_eq!(snapshot.status, expected_worker_status);
            let outcome = snapshot.outcome.expect("outcome");
            assert_eq!(
                outcome.acceptance_state,
                WorkerAcceptanceState::ExecutionFailed
            );
            assert!(!outcome.missing_model_evidence);
            assert!(!outcome.missing_effort_evidence);
        }

        // 2. Metadata-specific model/effort errors (`model_mismatch`, `missing_model`,
        // `effort_mismatch`, `missing_effort`) reject the relevant dimension, do not supplement
        // absent fields, and report the finite metadata error code instead of VerifiedMatch debug.
        let field_cases: &[(&str, WorkerRunStatus, bool, bool)] = &[
            (
                "model_mismatch",
                WorkerRunStatus::ModelMismatch,
                true,
                false,
            ),
            (
                "missing_model",
                WorkerRunStatus::MissingModelEvidence,
                true,
                false,
            ),
            (
                "effort_mismatch",
                WorkerRunStatus::EffortMismatch,
                false,
                true,
            ),
            (
                "missing_effort",
                WorkerRunStatus::MissingEffortEvidence,
                false,
                true,
            ),
        ];

        for (idx, &(err_code, run_status, expect_missing_model, expect_missing_effort)) in
            field_cases.iter().enumerate()
        {
            let now_base = 4_000 + (idx as i64) * 100;
            let queued = core
                .execute(
                    &binding.binding_id,
                    Command::StartWorker {
                        command_key: format!("wk-meta-field-{err_code}"),
                        profile_id: "prof_opencode".to_string(),
                        profile_digest: profile_digest.clone(),
                        provider: "opencode_v2".to_string(),
                        requested_model: "opencode-go/deepseek-v4.1-flash".to_string(),
                        requested_effort: "max".to_string(),
                        base_commit: base_commit.clone(),
                        instruction: "verify metadata field error provenance".to_string(),
                        execution_binding: None,
                        now_ms: now_base,
                    },
                )
                .expect("start_worker")
                .into_worker()
                .expect("worker snapshot");

            let claim = core
                .claim_worker(
                    &binding.binding_id,
                    &queued.run_id,
                    queued.revision,
                    &profile_digest,
                    now_base + 1,
                )
                .expect("claim_worker");

            let mut report = sample_opencode_task_report(temp.path());
            report.overall_acceptance_state = WorkAcceptanceState::RejectedByWorkerOutcome;
            report.worker_report.status = run_status;
            report.worker_report.opencode_metadata = None;
            report.worker_report.opencode_metadata_error = Some(err_code.to_string());
            report.worker_report.evidence.model_evidence = FieldEvidence::VerifiedMatch {
                value: "opencode-go/deepseek-v4.1-flash".to_string(),
            };
            report.worker_report.evidence.effort_evidence = FieldEvidence::VerifiedMatch {
                value: "max".to_string(),
            };

            assert!(!report
                .worker_report
                .has_complete_natural_completion_evidence());
            assert!(!report.worker_report.is_completed_pending_effort_evidence());
            assert_eq!(
                !report.worker_report.has_satisfied_model_evidence(),
                expect_missing_model
            );
            assert_eq!(
                !report.worker_report.has_satisfied_effort_evidence(),
                expect_missing_effort
            );

            let completion = build_completion_record(&state_dir, &claim, Ok(report), false);
            assert_eq!(completion.status, WorkerStatus::Failed);
            assert_eq!(
                completion.outcome.acceptance_state,
                WorkerAcceptanceState::ExecutionFailed
            );
            assert_eq!(
                completion.outcome.missing_model_evidence,
                expect_missing_model
            );
            assert_eq!(
                completion.outcome.missing_effort_evidence,
                expect_missing_effort
            );
            let reason = completion
                .outcome
                .failure_reason
                .as_deref()
                .expect("failure_reason present");
            assert_eq!(
                reason,
                format!("OpenCode session metadata verification failed ({err_code})")
            );
            assert!(
                !reason.contains("VerifiedMatch"),
                "failure_reason must report finite metadata error rather than VerifiedMatch for `{err_code}`"
            );
        }
    }
    fn supervision_event() -> NormalizedWorkerEvent {
        NormalizedWorkerEvent {
            line_number: 0,
            kind: NormalizedEventKind::SupervisorAttention,
            raw_type: "threadlane.progress_watchdog".to_string(),
            session_id: None,
            model: None,
            effort: None,
            tool_name: None,
            summary: Some(
                serde_json::json!({
                    "status": "attention_required", "attention_reason": "read_only_activity",
                    "elapsed_ms": 750, "soft_deadline_ms": 750, "hard_timeout_ms": 1000,
                    "last_advancement_ms": null, "events_observed": 4, "advancement_hints": 0,
                    "read_hints": 3, "output_hints": 1, "other_tool_hints": 0, "extensions": 0,
                    "attention_emitted": true
                })
                .to_string(),
            ),
        }
    }

    #[test]
    fn watchdog_mapping_requires_typed_software_origin_and_bounded_validated_dto() {
        let event = supervision_event();
        let mapped = map_live_event_to_progress(&event);
        assert_eq!(mapped.event_kind, "supervisor_attention");
        let snapshot = mapped
            .watchdog_attention
            .expect("trusted software attention");
        assert_eq!(
            snapshot.attention_reason,
            Some(threadlane_core::WorkerProgressAttentionReason::ReadOnlyActivity)
        );
        assert_eq!(snapshot.hard_timeout_ms, 1000);
        assert!(snapshot.validate_attention().is_ok());
        // A provider-controlled marker or JSON summary never creates trusted supervision.
        for kind in [
            NormalizedEventKind::Other,
            NormalizedEventKind::Progress,
            NormalizedEventKind::Tool,
        ] {
            let mut forged = event.clone();
            forged.kind = kind;
            assert!(map_live_event_to_progress(&forged)
                .watchdog_attention
                .is_none());
        }
        let mut wrong_line = event.clone();
        wrong_line.line_number = 1;
        assert!(map_live_event_to_progress(&wrong_line)
            .watchdog_attention
            .is_none());
        let mut wrong_marker = event.clone();
        wrong_marker.raw_type = "provider.progress_watchdog".into();
        assert!(map_live_event_to_progress(&wrong_marker)
            .watchdog_attention
            .is_none());
        let mut malformed = event.clone();
        malformed.summary = Some("SECRET-provider-instruction".repeat(4096));
        let ignored = map_live_event_to_progress(&malformed);
        assert!(ignored.watchdog_attention.is_none());
        assert!(!ignored.summary.contains("SECRET"));
        let mut json: serde_json::Value =
            serde_json::from_str(event.summary.as_ref().unwrap()).unwrap();
        json["provider_instruction"] = "change model".into();
        malformed.summary = Some(json.to_string());
        assert!(map_live_event_to_progress(&malformed)
            .watchdog_attention
            .is_none());
        json.as_object_mut().unwrap().remove("provider_instruction");
        json["status"] = "hard_limit_reached".into();
        malformed.summary = Some(json.to_string());
        assert!(map_live_event_to_progress(&malformed)
            .watchdog_attention
            .is_none());
        json["status"] = "attention_required".into();
        json["read_hints"] = 999.into();
        malformed.summary = Some(json.to_string());
        assert!(map_live_event_to_progress(&malformed)
            .watchdog_attention
            .is_none());
    }

    #[test]
    fn frozen_profile_identity_digest_matches_validated_and_survives_executable_gc() {
        use std::os::unix::fs::PermissionsExt;
        let temp = tempfile::tempdir().expect("tempdir");
        let root = fs::canonicalize(temp.path()).expect("canonical root");
        let repo_path = root.join("repo");
        fs::create_dir_all(repo_path.join(".git")).expect("repo .git");
        fs::create_dir_all(repo_path.join("docs")).expect("repo docs");
        let work_area_root = root.join("work-area");
        crate::descriptor::prepare_state_dir(&work_area_root).expect("work_area");
        let exe_path = root.join("mock-opencode");
        fs::write(&exe_path, b"#!/bin/sh\nexit 0\n").expect("write exe");
        fs::set_permissions(&exe_path, fs::Permissions::from_mode(0o700)).expect("chmod");
        let base_commit = "0123456789abcdef0123456789abcdef01234567".to_string();
        let profile = WorkerProfileConfig {
            profile_id: "prof_hist_01".to_string(),
            allowed_binding_ids: BTreeSet::from(["bind_01".to_string()]),
            spec: WorkerSpecTemplate {
                provider: WorkerProvider::OpenCodeV2,
                executable: exe_path.clone(),
                model: "opencode-go/deepseek-v4.1-flash".to_string(),
                effort: Some("max".to_string()),
                allowed_models: BTreeSet::from(["opencode-go/deepseek-v4.1-flash".to_string()]),
                standalone: true,
                session_id: None,
                repository_path: repo_path,
                base_commit: base_commit.clone(),
                work_area_root,
                allowed_write_paths: BTreeSet::from(["src/lib.rs".to_string()]),
                file_permissions: WorkerFilePermissions::Inherit,
                budgets: ExecutionBudgets::default(),
            },
            dependency_input: None,
        };
        let validated = WorkersConfig {
            max_parallel: 1,
            profiles: vec![profile.clone()],
        }
        .validate()
        .expect("strict validate");
        let validated_digest = validated.profiles["prof_hist_01"].profile_digest.clone();
        let frozen_digest = compute_frozen_profile_identity_digest(&profile).expect("frozen");
        assert_eq!(frozen_digest, validated_digest);
        let mut dir_profile = profile.clone();
        dir_profile.spec.allowed_write_paths = BTreeSet::from(["docs/".to_string()]);
        let dir_validated = WorkersConfig {
            max_parallel: 1,
            profiles: vec![dir_profile.clone()],
        }
        .validate()
        .expect("strict validate dir scope");
        let dir_validated_digest = dir_validated.profiles["prof_hist_01"]
            .profile_digest
            .clone();
        let dir_frozen_digest =
            compute_frozen_profile_identity_digest(&dir_profile).expect("frozen dir scope");
        assert_eq!(dir_frozen_digest, dir_validated_digest);
        assert_ne!(dir_frozen_digest, validated_digest);
        let binding = WorkerExecutionBinding::new(
            &validated_digest,
            "implement task",
            &"a".repeat(64),
            &"b".repeat(64),
            &base_commit,
        )
        .expect("binding");
        fs::remove_file(&exe_path).expect("remove executable");
        assert!(WorkersConfig {
            max_parallel: 1,
            profiles: vec![profile.clone()],
        }
        .validate()
        .is_err());
        let post_gc_digest = compute_frozen_profile_identity_digest(&profile).expect("post gc");
        assert_eq!(post_gc_digest, validated_digest);
        let post_gc_dir_digest =
            compute_frozen_profile_identity_digest(&dir_profile).expect("post gc dir scope");
        assert_eq!(post_gc_dir_digest, dir_validated_digest);
        assert!(binding
            .verify_against(&post_gc_digest, "implement task", &base_commit)
            .is_ok());

        let mut bad_binding = profile.clone();
        bad_binding.allowed_binding_ids = BTreeSet::from(["bind_other".to_string()]);
        let d_bind = compute_frozen_profile_identity_digest(&bad_binding).expect("binding digest");
        assert_ne!(d_bind, validated_digest);
        assert!(binding
            .verify_against(&d_bind, "implement task", &base_commit)
            .is_err());

        let mut bad_model = profile.clone();
        bad_model.spec.model = "opencode-go/unlisted".to_string();
        assert!(compute_frozen_profile_identity_digest(&bad_model).is_err());
        bad_model
            .spec
            .allowed_models
            .insert("opencode-go/unlisted".to_string());
        let d_model = compute_frozen_profile_identity_digest(&bad_model).expect("model digest");
        assert_ne!(d_model, validated_digest);

        let mut bad_effort = profile.clone();
        bad_effort.spec.effort = Some("high".to_string());
        let d_effort = compute_frozen_profile_identity_digest(&bad_effort).expect("effort digest");
        assert_ne!(d_effort, validated_digest);

        let mut bad_scope = profile.clone();
        bad_scope.spec.allowed_write_paths = BTreeSet::from(["docs/README.md".to_string()]);
        let d_scope = compute_frozen_profile_identity_digest(&bad_scope).expect("scope digest");
        assert_ne!(d_scope, validated_digest);
        assert!(binding
            .verify_against(&d_scope, "implement task", &base_commit)
            .is_err());
        for invalid_scope in ["", "/", "docs//", "docs/..", "../docs/", "docs/*"] {
            let mut invalid_profile = profile.clone();
            invalid_profile.spec.allowed_write_paths = BTreeSet::from([invalid_scope.to_string()]);
            assert!(compute_frozen_profile_identity_digest(&invalid_profile).is_err());
        }
    }
}
