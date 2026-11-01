use std::ffi::{OsStr, OsString};
use std::fs;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use threadlane_registry::{ToolAdapter, ToolDefinition, MAX_EXECUTABLE_PATH_LEN};
use tokio::sync::Semaphore;

use crate::codex_installation::{resolve_codex_system_executable, CodexDiscoveryOptions};
use crate::credentials::validate_tool_id;
use crate::error::ConnectorError;
use crate::parsers::parse_cli_version;
use crate::probe::{
    check_resolved_file_executable, ensure_unix_platform, run_bounded_probe_command,
    validate_explicit_executable_path, AdapterKind, AVAILABILITY_API_ONLY, AVAILABILITY_FOUND,
    AVAILABILITY_NOT_FOUND,
};

/// Availability status when a tool is disabled in the registry (`enabled: false`).
pub const AVAILABILITY_DISABLED: &str = "disabled";

/// Selection mode when [`ToolDefinition::executable_path`] is `None` (`null`),
/// indicating automatic system installation resolution.
pub const SELECTION_MODE_SYSTEM: &str = "system";

/// Selection mode when [`ToolDefinition::executable_path`] is `Some`,
/// indicating a user-configured or pinned executable override.
pub const SELECTION_MODE_CUSTOM: &str = "custom";

/// Default wall-clock timeout per CLI version inspection process (3 seconds).
pub const DEFAULT_VERSION_INSPECTION_TIMEOUT_MS: u64 = 3_000;

/// Maximum number of concurrent CLI version inspection subprocesses.
pub const MAX_VERSION_INSPECTION_CONCURRENCY: usize = 4;

const MSG_DISABLED: &str = "Tool is disabled; version inspection was skipped.";
const MSG_API_ONLY: &str =
    "API adapter does not use a local CLI executable; version inspection is not applicable.";
const MSG_SYSTEM_NOT_FOUND: &str = "System CLI executable was not found on PATH.";
const MSG_SYSTEM_INSPECTED: &str = "System CLI executable version inspected.";
const MSG_CUSTOM_MATCHES_SYSTEM: &str =
    "Custom CLI executable version inspected; resolved path matches system executable.";
const MSG_CUSTOM_DIFFERS_SYSTEM: &str =
    "Custom CLI executable version inspected; resolved path differs from system executable.";
const MSG_CUSTOM_NO_SYSTEM: &str =
    "Custom CLI executable version inspected; no system executable was found on PATH.";
const MSG_CUSTOM_NOT_FOUND_WITH_SYSTEM: &str =
    "Configured custom CLI executable was not found; a system executable is available.";
const MSG_CUSTOM_NOT_FOUND: &str = "Configured custom CLI executable was not found.";
const MSG_TASK_FAILED: &str = "Tool version inspection task failed unexpectedly.";

/// Safe startup/background CLI version and executable resolution summary for a [`ToolDefinition`].
///
/// Serializes with `snake_case` fields and preserves `null` for `None` options.
/// Never contains raw CLI stdout/stderr, credentials, or unvalidated tokens.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolVersionInspection {
    /// Identifier of the inspected tool definition.
    pub tool_id: String,
    /// Executable availability (`found`, `not_found`, `api_only`, or `disabled`).
    pub availability: String,
    /// Executable selection mode (`system` or `custom`).
    pub selection_mode: String,
    /// Configured custom executable path when `selection_mode == "custom"`, or `None` (`null`).
    pub configured_path: Option<String>,
    /// Canonical filesystem path of the active executable if resolved and valid, or `None` (`null`).
    pub resolved_path: Option<String>,
    /// Canonical filesystem path of the automatic system executable if found, or `None` (`null`).
    pub system_path: Option<String>,
    /// Parsed CLI version token of the active executable, if available (`null` when `None`).
    pub version: Option<String>,
    /// Parsed CLI version token of the automatic system executable, if available (`null` when `None`).
    pub system_version: Option<String>,
    /// Whether a custom configuration differs from the automatic system resolution.
    pub system_path_differs: bool,
    /// Short, safe human-readable summary of the version inspection outcome.
    pub message: String,
    /// Stable `snake_case` connector error code when inspection encountered an error, or `None` (`null`).
    pub error_code: Option<String>,
}

/// Source for `PATH` resolution during tool version inspection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PathSource {
    /// Read `PATH` afresh from the current process environment (`std::env::var_os("PATH")`).
    AmbientEnv,
    /// Use an explicitly supplied `PATH` value (or `None` to simulate an unset `PATH`).
    Explicit(Option<OsString>),
}

/// Configurable options for [`inspect_tool_versions_with_options`] and
/// [`inspect_tool_version_with_options`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VersionInspectionOptions {
    /// Source used to resolve system executables on `PATH`.
    pub path_source: PathSource,
    /// Per-process wall-clock timeout (capped at [`DEFAULT_VERSION_INSPECTION_TIMEOUT_MS`]).
    pub per_process_timeout: Duration,
    /// Maximum concurrent subprocesses (clamped to `1..=MAX_VERSION_INSPECTION_CONCURRENCY`).
    pub max_concurrency: usize,
    /// Optional Codex desktop/system discovery configuration override.
    pub codex_discovery: Option<CodexDiscoveryOptions>,
}

impl Default for VersionInspectionOptions {
    fn default() -> Self {
        Self {
            path_source: PathSource::AmbientEnv,
            per_process_timeout: Duration::from_millis(DEFAULT_VERSION_INSPECTION_TIMEOUT_MS),
            max_concurrency: MAX_VERSION_INSPECTION_CONCURRENCY,
            codex_discovery: None,
        }
    }
}

impl VersionInspectionOptions {
    /// Creates default version inspection options (`AmbientEnv`, 3s timeout, max 4 concurrency).
    pub fn new() -> Self {
        Self::default()
    }

    /// Overrides the `PATH` value used for automatic system executable resolution.
    pub fn with_path_env(mut self, path_env: impl Into<OsString>) -> Self {
        self.path_source = PathSource::Explicit(Some(path_env.into()));
        self
    }

    /// Configures system executable resolution to run with an unset/empty `PATH`.
    pub fn with_empty_path(mut self) -> Self {
        self.path_source = PathSource::Explicit(None);
        self
    }

    /// Overrides the per-process timeout (capped at 3 seconds when executed).
    pub const fn with_timeout(mut self, timeout: Duration) -> Self {
        self.per_process_timeout = timeout;
        self
    }

    /// Overrides the maximum number of concurrent version subprocesses (clamped to `1..=4`).
    pub const fn with_max_concurrency(mut self, max_concurrency: usize) -> Self {
        self.max_concurrency = max_concurrency;
        self
    }

    /// Overrides the Codex desktop/system installation discovery configuration.
    pub fn with_codex_discovery(mut self, discovery: CodexDiscoveryOptions) -> Self {
        self.codex_discovery = Some(discovery);
        self
    }

    fn resolve_path_env(&self) -> Option<OsString> {
        match &self.path_source {
            PathSource::AmbientEnv => std::env::var_os("PATH"),
            PathSource::Explicit(val) => val.clone(),
        }
    }

    fn effective_codex_discovery(&self) -> CodexDiscoveryOptions {
        if let Some(ref explicit_discovery) = self.codex_discovery {
            let mut cfg = explicit_discovery.clone();
            if cfg.path_env.is_none() {
                if let PathSource::Explicit(ref explicit_path) = self.path_source {
                    cfg.path_env = explicit_path.clone();
                }
            }
            return cfg;
        }

        match &self.path_source {
            PathSource::AmbientEnv => CodexDiscoveryOptions::ambient(),
            PathSource::Explicit(explicit_path) => {
                CodexDiscoveryOptions::isolated().with_optional_path_env(explicit_path.clone())
            }
        }
    }

    fn effective_timeout(&self) -> Duration {
        let max_timeout = Duration::from_millis(DEFAULT_VERSION_INSPECTION_TIMEOUT_MS);
        if self.per_process_timeout > max_timeout {
            max_timeout
        } else {
            self.per_process_timeout
        }
    }

    fn effective_concurrency(&self) -> usize {
        self.max_concurrency
            .clamp(1, MAX_VERSION_INSPECTION_CONCURRENCY)
    }
}

const fn adapter_version_args(adapter: AdapterKind) -> Option<&'static [&'static str]> {
    match adapter {
        AdapterKind::CodexCli
        | AdapterKind::OpencodeV2
        | AdapterKind::Antigravity
        | AdapterKind::Git
        | AdapterKind::Codegraph => Some(&["--version"]),
        AdapterKind::Tmux => Some(&["-V"]),
        AdapterKind::JevApi | AdapterKind::OpenaiDecisionsApi => None,
    }
}

/// Returns the fixed, shell-free CLI argument template used for startup version inspection
/// of a [`ToolAdapter`], or `None` for API adapters that never spawn a CLI process.
///
/// Startup version inspection never invokes model catalog, login, installation, or inference subcommands:
/// - `codex_cli`, `opencode_v2`, `antigravity`, `git`, `codegraph`: `["--version"]`
/// - `tmux`: `["-V"]`
/// - `jev_api`, `openai_decisions_api`: `None`
pub fn version_command_template(adapter: &ToolAdapter) -> Option<&'static [&'static str]> {
    AdapterKind::from_registry_adapter(adapter)
        .ok()
        .and_then(adapter_version_args)
}

fn is_safe_search_dir(dir: &Path) -> bool {
    let raw = dir.as_os_str().to_string_lossy();
    if raw.is_empty() || raw.contains('\0') || raw.chars().any(char::is_control) {
        return false;
    }
    if !dir.is_absolute() {
        return false;
    }
    !dir.components()
        .any(|c| matches!(c, Component::CurDir | Component::ParentDir))
}

fn normal_components(path: &Path) -> Vec<&str> {
    path.components()
        .filter_map(|comp| match comp {
            Component::Normal(os) => os.to_str(),
            _ => None,
        })
        .collect()
}

/// Returns `true` if `dir` is a direct `/nix/store/...` directory entry rather than a
/// stable profile symlink directory.
pub(crate) fn is_direct_nix_store_path(dir: &Path) -> bool {
    let parts = normal_components(dir);
    parts.windows(2).any(|pair| pair == ["nix", "store"])
}

/// Returns `true` if `dir` is a stable Nix user or system profile directory entry
/// (e.g. `~/.nix-profile/bin`, `$XDG_STATE_HOME/nix/profile/bin`,
/// `/etc/profiles/per-user/<user>/bin`, `/nix/var/nix/profiles/.../bin`,
/// `/run/current-system/sw/bin`, `/run/wrappers/bin`).
///
/// Never uses hardcoded user identities.
pub(crate) fn is_stable_nix_profile_path(dir: &Path) -> bool {
    if is_direct_nix_store_path(dir) {
        return false;
    }
    let parts = normal_components(dir);
    if parts.is_empty() {
        return false;
    }

    if parts.iter().any(|&seg| {
        seg == ".nix-profile"
            || seg == "profile"
            || seg == "profiles"
            || seg.ends_with("-profile")
            || seg.ends_with("_profile")
    }) {
        return true;
    }

    parts.windows(2).any(|pair| {
        pair == ["etc", "profiles"]
            || pair == ["nix", "profile"]
            || pair == ["nix", "profiles"]
            || pair == ["current-system", "sw"]
            || pair == ["run", "current-system"]
            || pair == ["run", "wrappers"]
    })
}

/// Produces the ordered list of valid, absolute search directories from `path_var`.
///
/// Rejects empty or relative entries and entries containing `.` or `..` traversal
/// components or control characters. When direct `/nix/store/...` entries are present,
/// stable Nix user/system profile entries in `PATH` are ordered ahead of the direct
/// `/nix/store` entries while preserving ordinary `PATH` precedence otherwise.
pub fn ordered_system_search_dirs(path_var: Option<&OsStr>) -> Vec<PathBuf> {
    let Some(raw_path) = path_var else {
        return Vec::new();
    };

    let mut valid_dirs: Vec<PathBuf> = Vec::new();
    for dir in std::env::split_paths(raw_path) {
        if !is_safe_search_dir(&dir) {
            continue;
        }
        if !valid_dirs.iter().any(|existing| existing == &dir) {
            valid_dirs.push(dir);
        }
    }

    let Some(first_store_idx) = valid_dirs.iter().position(|d| is_direct_nix_store_path(d)) else {
        return valid_dirs;
    };

    let mut ordered = Vec::with_capacity(valid_dirs.len());
    ordered.extend_from_slice(&valid_dirs[..first_store_idx]);

    let tail = &valid_dirs[first_store_idx..];
    for dir in tail {
        if is_stable_nix_profile_path(dir) {
            ordered.push(dir.clone());
        }
    }
    for dir in tail {
        if !is_stable_nix_profile_path(dir) {
            ordered.push(dir.clone());
        }
    }

    ordered
}

/// Resolves the first valid, executable target for `binaries` across `path_var`,
/// preferring stable Nix user/system profile entries when present over stale direct
/// `/nix/store` `PATH` entries and canonicalizing the resolved target afresh.
pub fn resolve_system_executable_in_path(
    path_var: Option<&OsStr>,
    binaries: &[&str],
) -> Option<PathBuf> {
    for dir in ordered_system_search_dirs(path_var) {
        for bin in binaries {
            let candidate = dir.join(bin);
            if let Ok(canonical) = check_resolved_file_executable(&candidate) {
                return Some(canonical);
            }
        }
    }
    None
}

/// Resolves the system executable for `adapter` afresh using the current environment.
///
/// For `codex_cli`, applies [`resolve_codex_system_executable`] (preferring stable Nix profile
/// standalone `codex` and stable profile desktop bundles over stale inherited `/nix/store` `PATH`
/// entries). Returns `Ok(None)` for API adapters or when no matching executable is found.
pub fn resolve_tool_system_executable(
    adapter: &ToolAdapter,
) -> Result<Option<PathBuf>, ConnectorError> {
    resolve_tool_system_executable_with_options(adapter, &VersionInspectionOptions::default())
}

/// Resolves the system executable for `adapter` afresh using the supplied `path_var`.
///
/// Returns `Ok(None)` for API adapters or when no matching executable is found.
pub fn resolve_tool_system_executable_with_path(
    adapter: &ToolAdapter,
    path_var: Option<&OsStr>,
) -> Result<Option<PathBuf>, ConnectorError> {
    let options = match path_var {
        Some(p) => VersionInspectionOptions::new().with_path_env(p),
        None => VersionInspectionOptions::new().with_empty_path(),
    };
    resolve_tool_system_executable_with_options(adapter, &options)
}

/// Resolves the system executable for `adapter` afresh using caller-supplied
/// [`VersionInspectionOptions`].
pub fn resolve_tool_system_executable_with_options(
    adapter: &ToolAdapter,
    options: &VersionInspectionOptions,
) -> Result<Option<PathBuf>, ConnectorError> {
    let kind = AdapterKind::from_registry_adapter(adapter)?;
    if kind.is_api_adapter() {
        return Ok(None);
    }
    if kind == AdapterKind::CodexCli {
        let codex_discovery = options.effective_codex_discovery();
        return Ok(resolve_codex_system_executable(&codex_discovery));
    }
    let path_os = options.resolve_path_env();
    Ok(resolve_system_executable_in_path(
        path_os.as_deref(),
        kind.default_binaries(),
    ))
}

fn sanitize_configured_path(path: &Path) -> String {
    let raw = path.as_os_str().to_string_lossy();
    let cleaned: String = raw
        .chars()
        .filter(|ch| *ch != '\0' && !ch.is_control())
        .take(MAX_EXECUTABLE_PATH_LEN)
        .collect();
    cleaned.trim().to_string()
}

fn resolve_custom_executable(configured: &Path) -> Result<Option<PathBuf>, ConnectorError> {
    validate_explicit_executable_path(configured)?;
    match fs::symlink_metadata(configured) {
        Ok(_) => {
            let canonical = check_resolved_file_executable(configured)?;
            Ok(Some(canonical))
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(_) => Err(ConnectorError::InvalidExecutable(
            "Configured executable path could not be accessed.",
        )),
    }
}

async fn execute_version_command(
    executable: &Path,
    args: &[&str],
    timeout: Duration,
    semaphore: &Semaphore,
) -> Result<String, ConnectorError> {
    let _permit = semaphore.acquire().await.map_err(|_| {
        ConnectorError::ProbeFailed("Version inspection concurrency limiter closed.")
    })?;
    let out = run_bounded_probe_command(executable, args, timeout).await?;
    if out.exit_code != Some(0) {
        return Err(ConnectorError::ProbeFailed(
            "CLI version command exited with a non-zero status.",
        ));
    }
    parse_cli_version(&out.stdout, &out.stderr).ok_or(ConnectorError::ParseError(
        "CLI version command output did not contain a valid version token.",
    ))
}

/// Inspects a single [`ToolDefinition`] using default startup version inspection options.
pub async fn inspect_tool_version(tool: &ToolDefinition) -> ToolVersionInspection {
    inspect_tool_version_with_options(tool, &VersionInspectionOptions::default()).await
}

/// Inspects a single [`ToolDefinition`] using caller-specified [`VersionInspectionOptions`].
pub async fn inspect_tool_version_with_options(
    tool: &ToolDefinition,
    options: &VersionInspectionOptions,
) -> ToolVersionInspection {
    let path_env = options.resolve_path_env();
    let codex_discovery = options.effective_codex_discovery();
    let semaphore = Semaphore::new(options.effective_concurrency());
    inspect_single_tool(
        tool,
        path_env.as_deref(),
        &codex_discovery,
        options.effective_timeout(),
        options.per_process_timeout.is_zero(),
        &semaphore,
    )
    .await
}

/// Inspects a slice of [`ToolDefinition`] items in background/startup mode, preserving input order.
///
/// - Enabled local CLI tools execute only fixed version argv (`--version` or `tmux -V`) with a
///   3-second per-process timeout, 1 MiB stream cap, process-group cleanup, and at most 4
///   concurrent subprocesses.
/// - Disabled tools (`enabled: false`) and API adapters (`jev_api`, `openai_decisions_api`)
///   launch zero subprocesses.
/// - System installation (`executable_path: None`) resolves the command afresh on `PATH`,
///   preferring stable Nix user/system profile entries (and for `codex_cli`, stable profile
///   desktop bundles) over stale direct `/nix/store` entries.
/// - Custom installation (`executable_path: Some(...)`) compares against system resolution and
///   reports `system_path_differs` without mutating the tool or claiming the system version is newer.
pub async fn inspect_tool_versions(tools: &[ToolDefinition]) -> Vec<ToolVersionInspection> {
    inspect_tool_versions_with_options(tools, &VersionInspectionOptions::default()).await
}

/// Inspects a slice of [`ToolDefinition`] items with a caller-specified per-process timeout
/// (capped at [`DEFAULT_VERSION_INSPECTION_TIMEOUT_MS`]), preserving input order.
pub async fn inspect_tool_versions_with_timeout(
    tools: &[ToolDefinition],
    timeout: Duration,
) -> Vec<ToolVersionInspection> {
    let options = VersionInspectionOptions::default().with_timeout(timeout);
    inspect_tool_versions_with_options(tools, &options).await
}

/// Inspects a slice of [`ToolDefinition`] items with configurable [`VersionInspectionOptions`],
/// preserving input order and bounding concurrent subprocesses to at most 4.
pub async fn inspect_tool_versions_with_options(
    tools: &[ToolDefinition],
    options: &VersionInspectionOptions,
) -> Vec<ToolVersionInspection> {
    if tools.is_empty() {
        return Vec::new();
    }

    let path_env: Option<Arc<OsStr>> = options.resolve_path_env().map(Arc::from);
    let codex_discovery = Arc::new(options.effective_codex_discovery());
    let effective_timeout = options.effective_timeout();
    let timeout_is_zero = options.per_process_timeout.is_zero();
    let semaphore = Arc::new(Semaphore::new(options.effective_concurrency()));

    let mut handles = Vec::with_capacity(tools.len());
    for tool in tools {
        // Each spawned inspection owns its immutable registry definition.
        let tool = tool.clone();
        let path_clone = path_env.clone();
        let codex_clone = Arc::clone(&codex_discovery);
        let sem_clone = Arc::clone(&semaphore);
        handles.push(tokio::spawn(async move {
            inspect_single_tool(
                &tool,
                path_clone.as_deref(),
                &codex_clone,
                effective_timeout,
                timeout_is_zero,
                &sem_clone,
            )
            .await
        }));
    }

    let mut results = Vec::with_capacity(tools.len());
    for (tool, handle) in tools.iter().zip(handles) {
        match handle.await {
            Ok(inspection) => results.push(inspection),
            Err(_) => {
                let (selection_mode, configured_path) = selection_mode_and_path(tool);
                results.push(ToolVersionInspection {
                    tool_id: tool.id.clone(),
                    availability: AVAILABILITY_NOT_FOUND.to_string(),
                    selection_mode,
                    configured_path,
                    resolved_path: None,
                    system_path: None,
                    version: None,
                    system_version: None,
                    system_path_differs: false,
                    message: MSG_TASK_FAILED.to_string(),
                    error_code: Some("probe_failed".to_string()),
                });
            }
        }
    }

    results
}

fn selection_mode_and_path(tool: &ToolDefinition) -> (String, Option<String>) {
    match tool.executable_path.as_deref() {
        Some(path) => (
            SELECTION_MODE_CUSTOM.to_string(),
            Some(sanitize_configured_path(path)),
        ),
        None => (SELECTION_MODE_SYSTEM.to_string(), None),
    }
}

async fn inspect_single_tool(
    tool: &ToolDefinition,
    path_var: Option<&OsStr>,
    codex_discovery: &CodexDiscoveryOptions,
    timeout: Duration,
    timeout_is_zero: bool,
    semaphore: &Semaphore,
) -> ToolVersionInspection {
    let (selection_mode, configured_path) = selection_mode_and_path(tool);

    if let Err(err) = validate_tool_id(&tool.id) {
        return ToolVersionInspection {
            tool_id: tool.id.clone(),
            availability: AVAILABILITY_NOT_FOUND.to_string(),
            selection_mode,
            configured_path,
            resolved_path: None,
            system_path: None,
            version: None,
            system_version: None,
            system_path_differs: false,
            message: err.message().to_string(),
            error_code: Some(err.code().to_string()),
        };
    }

    let adapter = match AdapterKind::from_registry_adapter(&tool.adapter) {
        Ok(kind) => kind,
        Err(err) => {
            return ToolVersionInspection {
                tool_id: tool.id.clone(),
                availability: AVAILABILITY_NOT_FOUND.to_string(),
                selection_mode,
                configured_path,
                resolved_path: None,
                system_path: None,
                version: None,
                system_version: None,
                system_path_differs: false,
                message: err.message().to_string(),
                error_code: Some(err.code().to_string()),
            };
        }
    };

    if !tool.enabled {
        return ToolVersionInspection {
            tool_id: tool.id.clone(),
            availability: AVAILABILITY_DISABLED.to_string(),
            selection_mode,
            configured_path,
            resolved_path: None,
            system_path: None,
            version: None,
            system_version: None,
            system_path_differs: false,
            message: MSG_DISABLED.to_string(),
            error_code: None,
        };
    }

    if adapter.is_api_adapter() {
        return ToolVersionInspection {
            tool_id: tool.id.clone(),
            availability: AVAILABILITY_API_ONLY.to_string(),
            selection_mode,
            configured_path,
            resolved_path: None,
            system_path: None,
            version: None,
            system_version: None,
            system_path_differs: false,
            message: MSG_API_ONLY.to_string(),
            error_code: None,
        };
    }

    if timeout_is_zero {
        let err = ConnectorError::InvalidInput("Timeout must be greater than zero.");
        return ToolVersionInspection {
            tool_id: tool.id.clone(),
            availability: AVAILABILITY_NOT_FOUND.to_string(),
            selection_mode,
            configured_path,
            resolved_path: None,
            system_path: None,
            version: None,
            system_version: None,
            system_path_differs: false,
            message: err.message().to_string(),
            error_code: Some(err.code().to_string()),
        };
    }

    if let Err(err) = ensure_unix_platform() {
        return ToolVersionInspection {
            tool_id: tool.id.clone(),
            availability: AVAILABILITY_NOT_FOUND.to_string(),
            selection_mode,
            configured_path,
            resolved_path: None,
            system_path: None,
            version: None,
            system_version: None,
            system_path_differs: false,
            message: err.message().to_string(),
            error_code: Some(err.code().to_string()),
        };
    }

    let args = adapter_version_args(adapter).unwrap_or(&["--version"]);
    let system_canonical = if adapter == AdapterKind::CodexCli {
        resolve_codex_system_executable(codex_discovery)
    } else {
        resolve_system_executable_in_path(path_var, adapter.default_binaries())
    };
    let system_path = system_canonical
        .as_ref()
        .map(|p| p.to_string_lossy().into_owned());

    let Some(custom_raw_path) = tool.executable_path.as_deref() else {
        // System selection mode (automatic).
        let Some(ref sys_exec) = system_canonical else {
            return ToolVersionInspection {
                tool_id: tool.id.clone(),
                availability: AVAILABILITY_NOT_FOUND.to_string(),
                selection_mode,
                configured_path: None,
                resolved_path: None,
                system_path: None,
                version: None,
                system_version: None,
                system_path_differs: false,
                message: MSG_SYSTEM_NOT_FOUND.to_string(),
                error_code: None,
            };
        };

        return match execute_version_command(sys_exec, args, timeout, semaphore).await {
            Ok(ver) => ToolVersionInspection {
                tool_id: tool.id.clone(),
                availability: AVAILABILITY_FOUND.to_string(),
                selection_mode,
                configured_path: None,
                resolved_path: system_path.clone(),
                system_path,
                version: Some(ver.clone()),
                system_version: Some(ver),
                system_path_differs: false,
                message: MSG_SYSTEM_INSPECTED.to_string(),
                error_code: None,
            },
            Err(err) => ToolVersionInspection {
                tool_id: tool.id.clone(),
                availability: AVAILABILITY_FOUND.to_string(),
                selection_mode,
                configured_path: None,
                resolved_path: system_path.clone(),
                system_path,
                version: None,
                system_version: None,
                system_path_differs: false,
                message: err.message().to_string(),
                error_code: Some(err.code().to_string()),
            },
        };
    };

    // Custom selection mode (user-configured or pinned override).
    match resolve_custom_executable(custom_raw_path) {
        Ok(Some(custom_exec)) => {
            let resolved_path = Some(custom_exec.to_string_lossy().into_owned());
            let system_path_differs = resolved_path != system_path;

            if !system_path_differs {
                return match execute_version_command(&custom_exec, args, timeout, semaphore).await {
                    Ok(ver) => ToolVersionInspection {
                        tool_id: tool.id.clone(),
                        availability: AVAILABILITY_FOUND.to_string(),
                        selection_mode,
                        configured_path,
                        resolved_path,
                        system_path,
                        version: Some(ver.clone()),
                        system_version: Some(ver),
                        system_path_differs: false,
                        message: MSG_CUSTOM_MATCHES_SYSTEM.to_string(),
                        error_code: None,
                    },
                    Err(err) => ToolVersionInspection {
                        tool_id: tool.id.clone(),
                        availability: AVAILABILITY_FOUND.to_string(),
                        selection_mode,
                        configured_path,
                        resolved_path,
                        system_path,
                        version: None,
                        system_version: None,
                        system_path_differs: false,
                        message: err.message().to_string(),
                        error_code: Some(err.code().to_string()),
                    },
                };
            }

            let (custom_res, system_version) = match system_canonical.as_deref() {
                Some(sys_exec) => {
                    let (c_res, s_res) = tokio::join!(
                        execute_version_command(&custom_exec, args, timeout, semaphore),
                        execute_version_command(sys_exec, args, timeout, semaphore)
                    );
                    (c_res, s_res.ok())
                }
                None => {
                    let c_res =
                        execute_version_command(&custom_exec, args, timeout, semaphore).await;
                    (c_res, None)
                }
            };

            match custom_res {
                Ok(ver) => {
                    let message = if system_path.is_some() {
                        MSG_CUSTOM_DIFFERS_SYSTEM
                    } else {
                        MSG_CUSTOM_NO_SYSTEM
                    };
                    ToolVersionInspection {
                        tool_id: tool.id.clone(),
                        availability: AVAILABILITY_FOUND.to_string(),
                        selection_mode,
                        configured_path,
                        resolved_path,
                        system_path,
                        version: Some(ver),
                        system_version,
                        system_path_differs: true,
                        message: message.to_string(),
                        error_code: None,
                    }
                }
                Err(err) => ToolVersionInspection {
                    tool_id: tool.id.clone(),
                    availability: AVAILABILITY_FOUND.to_string(),
                    selection_mode,
                    configured_path,
                    resolved_path,
                    system_path,
                    version: None,
                    system_version,
                    system_path_differs: true,
                    message: err.message().to_string(),
                    error_code: Some(err.code().to_string()),
                },
            }
        }
        Ok(None) => {
            let system_version = match system_canonical.as_deref() {
                Some(sys_exec) => execute_version_command(sys_exec, args, timeout, semaphore)
                    .await
                    .ok(),
                None => None,
            };
            let system_path_differs = system_path.is_some();
            let message = if system_path.is_some() {
                MSG_CUSTOM_NOT_FOUND_WITH_SYSTEM
            } else {
                MSG_CUSTOM_NOT_FOUND
            };
            ToolVersionInspection {
                tool_id: tool.id.clone(),
                availability: AVAILABILITY_NOT_FOUND.to_string(),
                selection_mode,
                configured_path,
                resolved_path: None,
                system_path,
                version: None,
                system_version,
                system_path_differs,
                message: message.to_string(),
                error_code: None,
            }
        }
        Err(err) => {
            let system_version = match system_canonical.as_deref() {
                Some(sys_exec) => execute_version_command(sys_exec, args, timeout, semaphore)
                    .await
                    .ok(),
                None => None,
            };
            let system_path_differs = system_path.is_some();
            ToolVersionInspection {
                tool_id: tool.id.clone(),
                availability: AVAILABILITY_NOT_FOUND.to_string(),
                selection_mode,
                configured_path,
                resolved_path: None,
                system_path,
                version: None,
                system_version,
                system_path_differs,
                message: err.message().to_string(),
                error_code: Some(err.code().to_string()),
            }
        }
    }
}
