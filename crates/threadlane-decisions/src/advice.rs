use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use fs2::FileExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use threadlane_connectors::CredentialStore;
use threadlane_registry::{
    DecisionAdvisorMode, Difficulty, RegistrySnapshot, RegistryStore, ToolAdapter,
};

use crate::client::DecisionClient;
use crate::error::DecisionError;
use crate::schema::{
    is_supported_jev_model, JevCatalogEntry, EASY_DESCRIPTION, EXTREME_DESCRIPTION,
    HARD_DESCRIPTION, JEV_INSTRUCTIONS, MAX_IDENTIFIER_BYTES, MAX_TASK_SUMMARY_BYTES,
    MEDIUM_DESCRIPTION, QUESTION_NAME,
};
use crate::types::{
    DecisionAdvice, DecisionProvider, DecisionRequest, MAX_JS_SAFE_INTEGER,
    MIN_CONFIDENCE_THRESHOLD,
};

/// Schema version required in private shadow advice request files.
pub const ADVICE_REQUEST_SCHEMA_VERSION: u32 = 1;

/// Schema version persisted in private shadow advice cache files.
pub const ADVICE_CACHE_SCHEMA_VERSION: u32 = 1;

/// Version identifier for the Jev shadow difficulty prompt and criteria schema.
pub const ADVICE_PROMPT_VERSION: &str = "threadlane-jev-shadow-v1";

/// Shadow execution mode label reported in all advice outputs.
pub const ADVICE_MODE_SHADOW: &str = "shadow";

/// Default TTL in seconds for completed advice cache records (`3600` seconds = 1 hour).
pub const DEFAULT_CACHE_TTL_SECONDS: u64 = 3600;

/// Minimum allowed cache TTL in seconds.
pub const MIN_CACHE_TTL_SECONDS: u64 = 1;

/// Maximum allowed cache TTL in seconds (`86400` seconds = 24 hours).
pub const MAX_CACHE_TTL_SECONDS: u64 = 86_400;

/// Maximum size in bytes for a private advice request JSON file (16 KiB).
pub const MAX_REQUEST_FILE_BYTES: usize = 16 * 1024;

/// Maximum number of persisted advice cache records (`.json` files) in the cache directory.
pub const MAX_CACHE_RECORDS: usize = 256;

/// Maximum total size in bytes across all persisted advice cache records and temporary files (1 MiB).
pub const MAX_CACHE_TOTAL_BYTES: u64 = 1024 * 1024;

/// Maximum size in bytes for a single persisted advice cache record file (16 KiB).
pub const MAX_CACHE_RECORD_BYTES: usize = 16 * 1024;

/// Maximum number of directory entries inspected when scanning the cache directory.
pub const MAX_CACHE_DIR_ENTRIES: usize = 1024;

/// Maximum size in bytes for `registry.json` when verifying read-only registry snapshots (1 MiB).
const MAX_REGISTRY_FILE_BYTES: u64 = 1024 * 1024;

/// Fixed filename for the short-held cache-wide admission, pruning, and publication lock.
const GLOBAL_CACHE_LOCK_FILENAME: &str = ".cache.lock";

static ATTEMPT_SEQUENCE: AtomicU64 = AtomicU64::new(1);
static PROCESS_CACHE_MUTEX: Mutex<()> = Mutex::new(());

/// Sanitized, bounded error returned by shadow advice operations.
///
/// `AdviceError` uses static error codes and static user-safe messages so raw request
/// payloads, task summaries, secrets, filesystem paths, and upstream HTTP bodies are
/// never leaked in `Display`, `Debug`, or CLI error output.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdviceError {
    code: String,
    message: String,
}

impl AdviceError {
    /// Creates a new [`AdviceError`] from a static code and static safe message.
    pub fn new(code: &'static str, message: &'static str) -> Self {
        Self {
            code: code.to_string(),
            message: message.to_string(),
        }
    }

    /// Returns the machine-readable snake_case error code.
    pub fn code(&self) -> &str {
        &self.code
    }

    /// Returns the sanitized user-safe error message.
    pub fn message(&self) -> &str {
        &self.message
    }

    pub(crate) fn invalid_input(message: &'static str) -> Self {
        Self::new("invalid_input", message)
    }

    pub(crate) fn invalid_request(message: &'static str) -> Self {
        Self::new("invalid_request", message)
    }

    pub(crate) fn request_too_large() -> Self {
        Self::new(
            "request_too_large",
            "Advice request file exceeds the 16 KiB size limit.",
        )
    }

    pub(crate) fn security_violation(message: &'static str) -> Self {
        Self::new("security_violation", message)
    }

    #[allow(dead_code)]
    pub(crate) fn unsupported_platform() -> Self {
        Self::new(
            "unsupported_platform",
            "Private shadow advice cache requires Unix filesystem ownership and permission semantics.",
        )
    }

    pub(crate) fn registry_error() -> Self {
        Self::new(
            "registry_error",
            "Failed to read or validate the read-only registry snapshot.",
        )
    }

    pub(crate) fn stale_registry_revision() -> Self {
        Self::new(
            "stale_registry_revision",
            "Registry snapshot revision does not match expected_registry_revision.",
        )
    }

    pub(crate) fn tool_not_found() -> Self {
        Self::new(
            "tool_not_found",
            "Requested tool_id was not found in the registry snapshot.",
        )
    }

    pub(crate) fn tool_disabled() -> Self {
        Self::new(
            "tool_disabled",
            "Requested Jev tool is disabled in the registry snapshot.",
        )
    }

    pub(crate) fn unsupported_adapter() -> Self {
        Self::new(
            "unsupported_adapter",
            "Shadow advice supports enabled jev_api tools only.",
        )
    }

    pub(crate) fn model_not_found() -> Self {
        Self::new(
            "model_not_found",
            "Requested model_id was not found for the selected tool in the registry snapshot.",
        )
    }

    pub(crate) fn invalid_model() -> Self {
        Self::new(
            "invalid_model",
            "Selected registry model does not have a valid Jev model_arg.",
        )
    }

    pub(crate) fn profile_not_found() -> Self {
        Self::new(
            "profile_not_found",
            "Requested profile_id was not found in the registry snapshot.",
        )
    }

    pub(crate) fn decision_advisor_not_configured() -> Self {
        Self::new(
            "decision_advisor_not_configured",
            "Selected workflow profile does not have a shadow decision advisor configured.",
        )
    }

    pub(crate) fn profile_advisor_mismatch() -> Self {
        Self::new(
            "profile_advisor_mismatch",
            "Requested tool_id and model_id do not match the selected profile's decision advisor.",
        )
    }

    pub(crate) fn cache_busy() -> Self {
        Self::new(
            "cache_busy",
            "Another advice classification attempt for this cache key is already in progress.",
        )
    }

    pub(crate) fn pending_attempt_requires_refresh() -> Self {
        Self::new(
            "pending_attempt_requires_refresh",
            "An abandoned pending classification attempt exists for this cache key; pass --refresh to authorize a new provider request.",
        )
    }

    pub(crate) fn uncertain_attempt_requires_refresh() -> Self {
        Self::new(
            "uncertain_attempt_requires_refresh",
            "A prior classification attempt had an uncertain outcome; pass --refresh to authorize a new provider request.",
        )
    }

    pub(crate) fn cache_capacity_exceeded() -> Self {
        Self::new(
            "cache_capacity_exceeded",
            "Advice cache has reached its bounded record count or byte capacity limit.",
        )
    }

    pub(crate) fn corrupt_cache() -> Self {
        Self::new(
            "corrupt_cache",
            "Persisted advice cache record failed schema or scope integrity verification.",
        )
    }

    pub(crate) fn io_error() -> Self {
        Self::new(
            "io_error",
            "Local filesystem I/O failed during advice operation.",
        )
    }
}

impl std::fmt::Display for AdviceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}

impl std::error::Error for AdviceError {}

impl From<DecisionError> for AdviceError {
    fn from(err: DecisionError) -> Self {
        Self::new(err.code(), err.message())
    }
}

/// Scope and directory configuration for a `threadlane-advice` invocation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdviceScope {
    /// Existing private `0700` registry configuration directory.
    pub registry_dir: PathBuf,
    /// Private `0700` advice cache directory under an existing private `0700` parent.
    pub cache_dir: PathBuf,
    /// Existing canonical local project root directory.
    pub project_root: PathBuf,
    /// Explicit local correlation context identifier.
    pub context_id: String,
    /// Completed-result cache TTL in seconds (`1..=86400`).
    pub cache_ttl_seconds: u64,
    /// Optional workflow profile identifier whose configured shadow decision advisor must match the request.
    pub profile_id: Option<String>,
}

/// Private shadow advice classification/preview request deserialized from `--request`.
///
/// Unknown and duplicate JSON fields are strictly rejected. `Debug` redacts `task_summary`.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdviceRequest {
    /// Request schema version (must be `1`).
    pub schema_version: u32,
    /// Positive registry revision expected by the caller.
    pub expected_registry_revision: u64,
    /// Registry tool identifier for an enabled `jev_api` tool.
    pub tool_id: String,
    /// Registry model identifier belonging to `tool_id`.
    pub model_id: String,
    /// Bounded task identifier within the plan/workflow.
    pub task_id: String,
    /// Positive task revision number.
    pub task_revision: u64,
    /// Bounded operator-supplied task summary (`<= 8192` UTF-8 bytes).
    pub task_summary: String,
    /// Optional baseline difficulty to compare against the advisory classification.
    #[serde(default)]
    pub baseline_difficulty: Option<Difficulty>,
}

impl std::fmt::Debug for AdviceRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AdviceRequest")
            .field("schema_version", &self.schema_version)
            .field(
                "expected_registry_revision",
                &self.expected_registry_revision,
            )
            .field("tool_id", &self.tool_id)
            .field("model_id", &self.model_id)
            .field("task_id", &self.task_id)
            .field("task_revision", &self.task_revision)
            .field("task_summary", &"[REDACTED]")
            .field("baseline_difficulty", &self.baseline_difficulty)
            .finish()
    }
}

/// Origin of a shadow advice classification result.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AdviceSource {
    /// Newly classified via a single-attempt Jev System One API call.
    Provider,
    /// Served from a validated, non-expired completed cache record without network or vault access.
    Cache,
}

/// Output of `threadlane-advice preview`.
///
/// `preview` is the only successful advice command allowed to include the exact original
/// `task_summary` in its serialized JSON output. Its `Debug` implementation still redacts
/// `task_summary`.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdvicePreviewOutput {
    /// Marks output as untrusted external/operator data that is never treated as execution authority.
    pub untrusted_data: bool,
    /// Marks result as strictly advisory.
    pub advisory_only: bool,
    /// Execution mode (`"shadow"`).
    pub mode: String,
    /// Local context correlation identifier.
    pub context_id: String,
    /// Canonical project root path.
    pub project_root: String,
    /// Validated registry snapshot revision.
    pub registry_revision: u64,
    /// Optional validated workflow profile identifier when `--profile-id` was supplied.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub profile_id: Option<String>,
    /// Validated registry tool identifier.
    pub tool_id: String,
    /// Validated registry model identifier.
    pub model_id: String,
    /// Resolved decision provider (`"jev"`).
    pub provider: DecisionProvider,
    /// Exact `model_arg` resolved from the registry snapshot.
    pub requested_model: String,
    /// Validated task identifier.
    pub task_id: String,
    /// Validated positive task revision.
    pub task_revision: u64,
    /// Exact operator-supplied task summary text from the request file.
    pub task_summary: String,
    /// Lowercase hexadecimal SHA-256 digest of the normalized task summary.
    pub summary_sha256: String,
    /// Deterministic lowercase hexadecimal SHA-256 cache key.
    pub cache_key: String,
    /// Optional baseline difficulty from the request.
    pub baseline_difficulty: Option<Difficulty>,
    /// Always `false` for `preview`.
    pub provider_call_performed: bool,
}

impl std::fmt::Debug for AdvicePreviewOutput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AdvicePreviewOutput")
            .field("untrusted_data", &self.untrusted_data)
            .field("advisory_only", &self.advisory_only)
            .field("mode", &self.mode)
            .field("context_id", &self.context_id)
            .field("project_root", &self.project_root)
            .field("registry_revision", &self.registry_revision)
            .field("profile_id", &self.profile_id)
            .field("tool_id", &self.tool_id)
            .field("model_id", &self.model_id)
            .field("provider", &self.provider)
            .field("requested_model", &self.requested_model)
            .field("task_id", &self.task_id)
            .field("task_revision", &self.task_revision)
            .field("task_summary", &"[REDACTED]")
            .field("summary_sha256", &self.summary_sha256)
            .field("cache_key", &self.cache_key)
            .field("baseline_difficulty", &self.baseline_difficulty)
            .field("provider_call_performed", &self.provider_call_performed)
            .finish()
    }
}

/// Output of `threadlane-advice models`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdviceModelsOutput {
    /// Marks output as untrusted external provider metadata.
    pub untrusted_data: bool,
    /// Marks result as strictly advisory.
    pub advisory_only: bool,
    /// Execution mode (`"shadow"`).
    pub mode: String,
    /// Local context correlation identifier.
    pub context_id: String,
    /// Current read-only registry snapshot revision.
    pub registry_revision: u64,
    /// Validated registry tool identifier.
    pub tool_id: String,
    /// Resolved decision provider (`"jev"`).
    pub provider: DecisionProvider,
    /// Exact catalog models returned by the Jev server (`GET /v1/models`).
    pub models: Vec<JevCatalogEntry>,
}

/// Output of `threadlane-advice classify`.
///
/// Never includes raw `task_summary`, secret keys, or raw upstream HTTP payloads.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AdviceClassifyOutput {
    /// Marks output as untrusted external provider data that is never treated as approval.
    pub untrusted_data: bool,
    /// Marks result as strictly advisory.
    pub advisory_only: bool,
    /// Execution mode (`"shadow"`).
    pub mode: String,
    /// Whether the advice came from a live `"provider"` call or a `"cache"` hit.
    pub source: AdviceSource,
    /// Local context correlation identifier.
    pub context_id: String,
    /// Validated task identifier.
    pub task_id: String,
    /// Validated positive task revision.
    pub task_revision: u64,
    /// Validated registry snapshot revision.
    pub registry_revision: u64,
    /// Optional validated workflow profile identifier when `--profile-id` was supplied.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub profile_id: Option<String>,
    /// Validated registry tool identifier.
    pub tool_id: String,
    /// Validated registry model identifier.
    pub model_id: String,
    /// Exact `model_arg` resolved from the registry snapshot.
    pub requested_model: String,
    /// Lowercase hexadecimal SHA-256 digest of the normalized task summary.
    pub summary_sha256: String,
    /// Deterministic lowercase hexadecimal SHA-256 cache key.
    pub cache_key: String,
    /// Validated advisory classification result (including historical token usage on cache hits).
    pub advice: DecisionAdvice,
    /// Optional baseline difficulty supplied in the request.
    #[serde(default)]
    pub baseline_difficulty: Option<Difficulty>,
    /// Whether `advice.difficulty` matches `baseline_difficulty` (`None` when no baseline was supplied).
    #[serde(default)]
    pub agrees_with_baseline: Option<bool>,
    /// `true` when an outbound provider HTTP call was performed, `false` on a cache hit.
    pub provider_call_performed: bool,
    /// Age of the cached result in milliseconds when `source == AdviceSource::Cache`.
    #[serde(default)]
    pub cache_age_ms: Option<u64>,
    /// Total elapsed wall-clock time for this command in milliseconds.
    pub elapsed_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PersistedCacheRecord {
    schema_version: u32,
    prompt_digest: String,
    cache_key: String,
    project_root_sha256: String,
    context_id: String,
    task_id: String,
    task_revision: u64,
    summary_sha256: String,
    registry_revision: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    profile_id: Option<String>,
    tool_id: String,
    model_id: String,
    requested_model: String,
    state: PersistedAttemptState,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
enum PersistedAttemptState {
    Pending {
        attempt_id: String,
        started_at_ms: u64,
    },
    Uncertain {
        attempt_id: String,
        failed_at_ms: u64,
        error_code: String,
    },
    Completed {
        attempt_id: String,
        completed_at_ms: u64,
        advice: DecisionAdvice,
    },
}

/// Internal runtime hooks used for deterministic testing of UID checks, clock progression,
/// and crash simulation after sealing a pending cache record.
#[derive(Clone)]
pub(crate) struct AdviceRuntimeOptions {
    pub expected_uid: u32,
    pub now_ms_override: Option<u64>,
    pub abort_after_pending_seal: bool,
}

impl Default for AdviceRuntimeOptions {
    fn default() -> Self {
        Self {
            expected_uid: current_effective_uid(),
            now_ms_override: None,
            abort_after_pending_seal: false,
        }
    }
}

fn current_effective_uid() -> u32 {
    #[cfg(unix)]
    {
        rustix::process::geteuid().as_raw()
    }
    #[cfg(not(unix))]
    {
        0
    }
}

fn current_unix_ms(options: &AdviceRuntimeOptions) -> u64 {
    if let Some(override_ms) = options.now_ms_override {
        return override_ms.min(MAX_JS_SAFE_INTEGER);
    }
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|dur| (dur.as_millis() as u64).min(MAX_JS_SAFE_INTEGER))
        .unwrap_or(0)
}

fn next_attempt_id(now_ms: u64) -> String {
    let seq = ATTEMPT_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let pid = std::process::id();
    format!("att-{now_ms}-{pid}-{seq}")
}

/// Validates and renders a shadow advice preview without network, vault, or cache mutation.
pub fn preview_advice(
    scope: &AdviceScope,
    request_path: &Path,
) -> Result<AdvicePreviewOutput, AdviceError> {
    preview_advice_with_options(scope, request_path, &AdviceRuntimeOptions::default())
}

pub(crate) fn preview_advice_with_options(
    scope: &AdviceScope,
    request_path: &Path,
    options: &AdviceRuntimeOptions,
) -> Result<AdvicePreviewOutput, AdviceError> {
    let validated_scope = validate_scope(scope, false, options.expected_uid)?;
    let request = read_and_validate_request_file(request_path, options.expected_uid)?;
    let snapshot =
        read_readonly_registry_snapshot(&validated_scope.registry_dir, options.expected_uid)?;
    let resolved =
        resolve_jev_tool_and_model(&snapshot, &request, validated_scope.profile_id.as_deref())?;

    let summary_sha256 = sha256_hex(request.normalized_summary.as_bytes());
    let prompt_digest = compute_prompt_digest();
    let cache_key = compute_cache_key(
        &prompt_digest,
        &validated_scope.project_root_str,
        &validated_scope.context_id,
        &request.raw.task_id,
        request.raw.task_revision,
        &summary_sha256,
        request.raw.expected_registry_revision,
        &resolved.tool_id,
        &resolved.model_id,
        &resolved.requested_model,
        resolved.profile_id.as_deref(),
    );

    Ok(AdvicePreviewOutput {
        untrusted_data: true,
        advisory_only: true,
        mode: ADVICE_MODE_SHADOW.to_string(),
        context_id: validated_scope.context_id,
        project_root: validated_scope.project_root_str,
        registry_revision: snapshot.revision,
        profile_id: resolved.profile_id,
        tool_id: resolved.tool_id,
        model_id: resolved.model_id,
        provider: DecisionProvider::Jev,
        requested_model: resolved.requested_model,
        task_id: request.raw.task_id,
        task_revision: request.raw.task_revision,
        task_summary: request.raw.task_summary,
        summary_sha256,
        cache_key,
        baseline_difficulty: request.raw.baseline_difficulty,
        provider_call_performed: false,
    })
}

/// Queries Jev `/v1/models` catalog metadata for an enabled `jev_api` tool in the read-only
/// registry without mutating the registry or creating/touching the advice cache.
pub async fn list_advice_models(
    scope: &AdviceScope,
    tool_id: &str,
    client: &DecisionClient,
) -> Result<AdviceModelsOutput, AdviceError> {
    list_advice_models_with_options(scope, tool_id, client, &AdviceRuntimeOptions::default()).await
}

pub(crate) async fn list_advice_models_with_options(
    scope: &AdviceScope,
    tool_id: &str,
    client: &DecisionClient,
    options: &AdviceRuntimeOptions,
) -> Result<AdviceModelsOutput, AdviceError> {
    if scope.profile_id.is_some() {
        return Err(AdviceError::invalid_input(
            "--profile-id is not supported for the models command.",
        ));
    }
    let validated_scope = validate_scope(scope, false, options.expected_uid)?;
    let validated_tool_id = validate_strict_identifier(tool_id, "tool_id is invalid.")?;
    let snapshot =
        read_readonly_registry_snapshot(&validated_scope.registry_dir, options.expected_uid)?;
    validate_enabled_jev_tool(&snapshot, &validated_tool_id)?;

    let models = client.jev_catalog_entries(&validated_tool_id).await?;

    Ok(AdviceModelsOutput {
        untrusted_data: true,
        advisory_only: true,
        mode: ADVICE_MODE_SHADOW.to_string(),
        context_id: validated_scope.context_id,
        registry_revision: snapshot.revision,
        tool_id: validated_tool_id,
        provider: DecisionProvider::Jev,
        models,
    })
}

/// Executes a single-attempt shadow advice classification or returns a validated completed
/// cache hit.
pub async fn classify_advice(
    scope: &AdviceScope,
    request_path: &Path,
    refresh: bool,
    vault: Arc<dyn CredentialStore>,
    client: &DecisionClient,
) -> Result<AdviceClassifyOutput, AdviceError> {
    classify_advice_with_options(
        scope,
        request_path,
        refresh,
        vault,
        client,
        &AdviceRuntimeOptions::default(),
    )
    .await
}

pub(crate) async fn classify_advice_with_options(
    scope: &AdviceScope,
    request_path: &Path,
    refresh: bool,
    vault: Arc<dyn CredentialStore>,
    client: &DecisionClient,
    options: &AdviceRuntimeOptions,
) -> Result<AdviceClassifyOutput, AdviceError> {
    let started = Instant::now();

    // 1. Validate scope, request file, and read-only registry snapshot BEFORE creating
    //    the cache directory, reading credentials, or performing any network call.
    let preflight_scope = validate_scope(scope, false, options.expected_uid)?;
    let request = read_and_validate_request_file(request_path, options.expected_uid)?;
    let initial_snapshot =
        read_readonly_registry_snapshot(&preflight_scope.registry_dir, options.expected_uid)?;
    let resolved = resolve_jev_tool_and_model(
        &initial_snapshot,
        &request,
        preflight_scope.profile_id.as_deref(),
    )?;

    let summary_sha256 = sha256_hex(request.normalized_summary.as_bytes());
    let prompt_digest = compute_prompt_digest();
    let project_root_sha256 = sha256_hex(preflight_scope.project_root_str.as_bytes());
    let cache_key = compute_cache_key(
        &prompt_digest,
        &preflight_scope.project_root_str,
        &preflight_scope.context_id,
        &request.raw.task_id,
        request.raw.task_revision,
        &summary_sha256,
        request.raw.expected_registry_revision,
        &resolved.tool_id,
        &resolved.model_id,
        &resolved.requested_model,
        resolved.profile_id.as_deref(),
    );

    // 2. Ensure private 0700 cache directory exists (only `classify` may create it).
    let validated_scope = validate_scope(scope, true, options.expected_uid)?;
    let record_path = validated_scope.cache_dir.join(format!("{cache_key}.json"));
    let now_ms = current_unix_ms(options);
    let ttl_ms = validated_scope.cache_ttl_seconds.saturating_mul(1000);
    let attempt_id = next_attempt_id(now_ms);
    let pending_record = PersistedCacheRecord {
        schema_version: ADVICE_CACHE_SCHEMA_VERSION,
        prompt_digest: prompt_digest.clone(),
        cache_key: cache_key.clone(),
        project_root_sha256: project_root_sha256.clone(),
        context_id: validated_scope.context_id.clone(),
        task_id: request.raw.task_id.clone(),
        task_revision: request.raw.task_revision,
        summary_sha256: summary_sha256.clone(),
        registry_revision: request.raw.expected_registry_revision,
        profile_id: resolved.profile_id.clone(),
        tool_id: resolved.tool_id.clone(),
        model_id: resolved.model_id.clone(),
        requested_model: resolved.requested_model.clone(),
        state: PersistedAttemptState::Pending {
            attempt_id: attempt_id.clone(),
            started_at_ms: now_ms,
        },
    };

    // 3. Phase 1 (Global Admission & Pending Seal):
    //    Acquire the short-lived cache-wide lock `.cache.lock` to serialize directory
    //    scanning, cache hit inspection, capacity pruning, per-key lock creation, and
    //    atomic pending publication. `.cache.lock` is always dropped before any `.await`.
    let (key_lock, prior_record) = {
        let _global_lock =
            acquire_global_cache_lock(&validated_scope.cache_dir, options.expected_uid)?;

        let mut scan =
            scan_cache_directory(&validated_scope.cache_dir, &cache_key, options.expected_uid)?;

        // If a per-key lock file already exists on disk (e.g. an active in-flight attempt),
        // test non-blocking ownership immediately before creating or mutating any files.
        let mut existing_key_lock = try_acquire_existing_per_key_lock(
            &validated_scope.cache_dir,
            &cache_key,
            options.expected_uid,
        )?;

        let mut prior_existing: Option<PersistedCacheRecord> = None;
        if scan.current_key_exists {
            let existing = read_and_validate_cache_record(
                &record_path,
                &prompt_digest,
                &cache_key,
                &project_root_sha256,
                &validated_scope.context_id,
                &request.raw.task_id,
                request.raw.task_revision,
                &summary_sha256,
                request.raw.expected_registry_revision,
                resolved.profile_id.as_deref(),
                &resolved.tool_id,
                &resolved.model_id,
                &resolved.requested_model,
                options.expected_uid,
            )?;

            match &existing.state {
                PersistedAttemptState::Pending { .. } => {
                    if !refresh {
                        return Err(AdviceError::pending_attempt_requires_refresh());
                    }
                    prior_existing = Some(existing);
                }
                PersistedAttemptState::Uncertain { .. } => {
                    // Uncertain outcomes never expire into automatic replay; explicit --refresh is required.
                    if !refresh {
                        return Err(AdviceError::uncertain_attempt_requires_refresh());
                    }
                    prior_existing = Some(existing);
                }
                PersistedAttemptState::Completed {
                    completed_at_ms,
                    advice,
                    ..
                } => {
                    let cache_age_ms = now_ms.saturating_sub(*completed_at_ms);
                    let is_expired = cache_age_ms > ttl_ms;
                    if !is_expired && !refresh {
                        let agrees_with_baseline = request
                            .raw
                            .baseline_difficulty
                            .map(|baseline| advice.difficulty == Some(baseline));
                        let elapsed_ms =
                            (started.elapsed().as_millis() as u64).min(MAX_JS_SAFE_INTEGER);
                        return Ok(AdviceClassifyOutput {
                            untrusted_data: true,
                            advisory_only: true,
                            mode: ADVICE_MODE_SHADOW.to_string(),
                            source: AdviceSource::Cache,
                            context_id: validated_scope.context_id,
                            task_id: request.raw.task_id,
                            task_revision: request.raw.task_revision,
                            registry_revision: initial_snapshot.revision,
                            profile_id: resolved.profile_id,
                            tool_id: resolved.tool_id,
                            model_id: resolved.model_id,
                            requested_model: resolved.requested_model,
                            summary_sha256,
                            cache_key,
                            advice: advice.clone(),
                            baseline_difficulty: request.raw.baseline_difficulty,
                            agrees_with_baseline,
                            provider_call_performed: false,
                            cache_age_ms: Some(cache_age_ms),
                            elapsed_ms,
                        });
                    }
                    prior_existing = Some(existing);
                }
            }
        }

        let serialized_pending = serialize_cache_record(&pending_record)?;

        // Enforce directory/record/byte budget BEFORE creating a new per-key lock file.
        ensure_cache_capacity_for_write(
            &validated_scope.cache_dir,
            &cache_key,
            serialized_pending.len(),
            &mut scan,
            now_ms,
            ttl_ms,
            options.expected_uid,
        )?;

        let acquired_key_lock = match existing_key_lock.take() {
            Some(lock) => lock,
            None => acquire_or_create_per_key_lock(
                &validated_scope.cache_dir,
                &cache_key,
                options.expected_uid,
            )?,
        };

        write_serialized_cache_record_atomic(
            &validated_scope.cache_dir,
            &record_path,
            &serialized_pending,
            scan.total_disk_bytes(),
            options.expected_uid,
        )?;

        (acquired_key_lock, prior_existing)
    };

    if options.abort_after_pending_seal {
        return Err(AdviceError::io_error());
    }

    // 4. Verify credential availability before making an outbound API call.
    //    Known credential refusal before API effect restores any prior record
    //    (including Pending, Uncertain, or Completed) or removes our owned pending
    //    marker only when no previous record existed.
    if let Err(pre_api_err) = precheck_tool_credential(Arc::clone(&vault), &resolved.tool_id).await
    {
        cleanup_owned_pending_before_api(
            &validated_scope.cache_dir,
            &cache_key,
            &record_path,
            &attempt_id,
            prior_record.as_ref(),
            now_ms,
            ttl_ms,
            options.expected_uid,
            key_lock,
        );
        return Err(AdviceError::from(pre_api_err));
    }

    // 5. Execute single-attempt Jev classification while holding only `key_lock` (never `.cache.lock`).
    //    Any error at or after this point may have incurred a paid API call and must remain
    //    `Pending` or `Uncertain` until explicit `--refresh`.
    let classify_res = client
        .classify(DecisionRequest {
            tool_id: resolved.tool_id.clone(),
            provider: DecisionProvider::Jev,
            model: resolved.requested_model.clone(),
            task_summary: request.normalized_summary.clone(),
        })
        .await;

    let advice = match classify_res {
        Ok(advice) => advice,
        Err(provider_err) => {
            let failed_at_ms = current_unix_ms(options);
            let uncertain_record = PersistedCacheRecord {
                state: PersistedAttemptState::Uncertain {
                    attempt_id,
                    failed_at_ms,
                    error_code: provider_err.code().to_string(),
                },
                ..pending_record
            };
            seal_post_api_record(
                &validated_scope.cache_dir,
                &cache_key,
                &record_path,
                &uncertain_record,
                failed_at_ms,
                ttl_ms,
                options.expected_uid,
                key_lock,
            );
            return Err(AdviceError::from(provider_err));
        }
    };

    // 6. Recheck registry revision and tool/model resolution after the API call before
    //    treating advice as current.
    let post_check = (|| -> Result<(), AdviceError> {
        let post_snapshot =
            read_readonly_registry_snapshot(&validated_scope.registry_dir, options.expected_uid)?;
        let post_resolved = resolve_jev_tool_and_model(
            &post_snapshot,
            &request,
            validated_scope.profile_id.as_deref(),
        )?;
        if post_resolved.profile_id != resolved.profile_id
            || post_resolved.tool_id != resolved.tool_id
            || post_resolved.model_id != resolved.model_id
            || post_resolved.requested_model != resolved.requested_model
        {
            return Err(AdviceError::stale_registry_revision());
        }
        Ok(())
    })();

    if let Err(post_err) = post_check {
        let failed_at_ms = current_unix_ms(options);
        let uncertain_record = PersistedCacheRecord {
            state: PersistedAttemptState::Uncertain {
                attempt_id,
                failed_at_ms,
                error_code: post_err.code().to_string(),
            },
            ..pending_record
        };
        seal_post_api_record(
            &validated_scope.cache_dir,
            &cache_key,
            &record_path,
            &uncertain_record,
            failed_at_ms,
            ttl_ms,
            options.expected_uid,
            key_lock,
        );
        return Err(post_err);
    }

    // 7. Phase 2 (Global Publication):
    //    Re-acquire `.cache.lock` after the provider `.await`, perform a fresh bounded scan
    //    to account for any concurrent cache writes or temporary files, and atomically publish
    //    the `Completed` record. If capacity is exceeded at completion, the sealed `Pending`
    //    record remains on disk so the paid call is never automatically replayed.
    let completed_at_ms = current_unix_ms(options);
    let completed_record = PersistedCacheRecord {
        state: PersistedAttemptState::Completed {
            attempt_id,
            completed_at_ms,
            advice: advice.clone(),
        },
        ..pending_record
    };
    publish_completed_record(
        &validated_scope.cache_dir,
        &cache_key,
        &record_path,
        &completed_record,
        completed_at_ms,
        ttl_ms,
        options.expected_uid,
        key_lock,
    )?;

    let agrees_with_baseline = request
        .raw
        .baseline_difficulty
        .map(|baseline| advice.difficulty == Some(baseline));
    let elapsed_ms = (started.elapsed().as_millis() as u64).min(MAX_JS_SAFE_INTEGER);

    Ok(AdviceClassifyOutput {
        untrusted_data: true,
        advisory_only: true,
        mode: ADVICE_MODE_SHADOW.to_string(),
        source: AdviceSource::Provider,
        context_id: validated_scope.context_id,
        task_id: request.raw.task_id,
        task_revision: request.raw.task_revision,
        registry_revision: request.raw.expected_registry_revision,
        profile_id: resolved.profile_id,
        tool_id: resolved.tool_id,
        model_id: resolved.model_id,
        requested_model: resolved.requested_model,
        summary_sha256,
        cache_key,
        advice,
        baseline_difficulty: request.raw.baseline_difficulty,
        agrees_with_baseline,
        provider_call_performed: true,
        cache_age_ms: None,
        elapsed_ms,
    })
}

async fn precheck_tool_credential(
    vault: Arc<dyn CredentialStore>,
    tool_id: &str,
) -> Result<(), DecisionError> {
    let lookup_id = tool_id.to_string();
    let secret = tokio::task::spawn_blocking(move || vault.get(&lookup_id))
        .await
        .map_err(|_| DecisionError::VaultUnavailable)?
        .map_err(|_| DecisionError::VaultUnavailable)?;
    match secret {
        Some(raw) => {
            let trimmed = raw.trim();
            if trimmed.is_empty() {
                Err(DecisionError::NotConfigured)
            } else if trimmed.len() > 4096 || trimmed.chars().any(|ch| ch.is_control()) {
                Err(DecisionError::InvalidRequest)
            } else {
                Ok(())
            }
        }
        None => Err(DecisionError::NotConfigured),
    }
}

#[allow(clippy::too_many_arguments)]
fn cleanup_owned_pending_before_api(
    cache_dir: &Path,
    cache_key: &str,
    record_path: &Path,
    expected_attempt_id: &str,
    prior_record: Option<&PersistedCacheRecord>,
    now_ms: u64,
    ttl_ms: u64,
    expected_uid: u32,
    key_lock: PerKeyLockGuard,
) {
    let Ok(_global_lock) = acquire_global_cache_lock(cache_dir, expected_uid) else {
        return;
    };
    let Ok(mut scan) = scan_cache_directory(cache_dir, cache_key, expected_uid) else {
        return;
    };
    let Ok(bytes) = read_bounded_private_file(record_path, MAX_CACHE_RECORD_BYTES, expected_uid)
    else {
        return;
    };
    let Ok(current) = serde_json::from_slice::<PersistedCacheRecord>(&bytes) else {
        return;
    };
    let is_owned_pending = matches!(
        &current.state,
        PersistedAttemptState::Pending { attempt_id, .. } if attempt_id == expected_attempt_id
    );
    if !is_owned_pending {
        return;
    }

    if let Some(prior) = prior_record {
        if let Ok(serialized_prior) = serialize_cache_record(prior) {
            let _ = ensure_cache_capacity_for_write(
                cache_dir,
                cache_key,
                serialized_prior.len(),
                &mut scan,
                now_ms,
                ttl_ms,
                expected_uid,
            );
            let _ = write_serialized_cache_record_atomic(
                cache_dir,
                record_path,
                &serialized_prior,
                scan.total_disk_bytes(),
                expected_uid,
            );
        }
    } else {
        let _ = fs::remove_file(record_path);
    }
    remove_per_key_lock_under_global(cache_dir, cache_key, key_lock);
}

#[allow(clippy::too_many_arguments)]
fn seal_post_api_record(
    cache_dir: &Path,
    cache_key: &str,
    record_path: &Path,
    uncertain_record: &PersistedCacheRecord,
    now_ms: u64,
    ttl_ms: u64,
    expected_uid: u32,
    key_lock: PerKeyLockGuard,
) {
    if let Ok(_global_lock) = acquire_global_cache_lock(cache_dir, expected_uid) {
        if let Ok(serialized) = serialize_cache_record(uncertain_record) {
            if let Ok(mut scan) = scan_cache_directory(cache_dir, cache_key, expected_uid) {
                let _ = ensure_cache_capacity_for_write(
                    cache_dir,
                    cache_key,
                    serialized.len(),
                    &mut scan,
                    now_ms,
                    ttl_ms,
                    expected_uid,
                );
                let _ = write_serialized_cache_record_atomic(
                    cache_dir,
                    record_path,
                    &serialized,
                    scan.total_disk_bytes(),
                    expected_uid,
                );
            }
        }
        remove_per_key_lock_under_global(cache_dir, cache_key, key_lock);
    }
}

#[allow(clippy::too_many_arguments)]
fn publish_completed_record(
    cache_dir: &Path,
    cache_key: &str,
    record_path: &Path,
    completed_record: &PersistedCacheRecord,
    now_ms: u64,
    ttl_ms: u64,
    expected_uid: u32,
    key_lock: PerKeyLockGuard,
) -> Result<(), AdviceError> {
    let _global_lock = acquire_global_cache_lock(cache_dir, expected_uid)?;
    let serialized = serialize_cache_record(completed_record)?;
    let mut scan = match scan_cache_directory(cache_dir, cache_key, expected_uid) {
        Ok(scan) => scan,
        Err(err) => {
            remove_per_key_lock_under_global(cache_dir, cache_key, key_lock);
            return Err(err);
        }
    };

    if let Err(err) = ensure_cache_capacity_for_write(
        cache_dir,
        cache_key,
        serialized.len(),
        &mut scan,
        now_ms,
        ttl_ms,
        expected_uid,
    ) {
        remove_per_key_lock_under_global(cache_dir, cache_key, key_lock);
        return Err(err);
    }

    let write_res = write_serialized_cache_record_atomic(
        cache_dir,
        record_path,
        &serialized,
        scan.total_disk_bytes(),
        expected_uid,
    );
    remove_per_key_lock_under_global(cache_dir, cache_key, key_lock);
    write_res
}

fn remove_per_key_lock_under_global(cache_dir: &Path, cache_key: &str, key_lock: PerKeyLockGuard) {
    let lock_path = cache_dir.join(format!("{cache_key}.lock"));
    let _ = fs::remove_file(&lock_path);
    drop(key_lock);
    let _ = sync_directory(cache_dir);
}

#[derive(Debug, Clone)]
struct ValidatedScope {
    registry_dir: PathBuf,
    cache_dir: PathBuf,
    project_root_str: String,
    context_id: String,
    cache_ttl_seconds: u64,
    profile_id: Option<String>,
}

#[derive(Clone)]
struct ValidatedAdviceRequest {
    raw: AdviceRequest,
    normalized_summary: String,
}

#[derive(Debug, Clone)]
struct ResolvedJevTarget {
    profile_id: Option<String>,
    tool_id: String,
    model_id: String,
    requested_model: String,
}

fn validate_strict_identifier(
    value: &str,
    err_message: &'static str,
) -> Result<String, AdviceError> {
    let trimmed = value.trim();
    if trimmed.is_empty()
        || trimmed != value
        || value.len() > MAX_IDENTIFIER_BYTES
        || value
            .chars()
            .any(|ch| ch.is_control() || matches!(ch, '/' | '\\'))
        || value == "."
        || value == ".."
    {
        return Err(AdviceError::invalid_input(err_message));
    }
    Ok(value.to_string())
}

fn validate_request_identifier(
    value: &str,
    err_message: &'static str,
) -> Result<String, AdviceError> {
    let trimmed = value.trim();
    if trimmed.is_empty()
        || trimmed != value
        || value.len() > MAX_IDENTIFIER_BYTES
        || value
            .chars()
            .any(|ch| ch.is_control() || matches!(ch, '/' | '\\'))
        || value == "."
        || value == ".."
    {
        return Err(AdviceError::invalid_request(err_message));
    }
    Ok(value.to_string())
}

fn normalize_and_validate_task_summary(raw: &str) -> Result<String, AdviceError> {
    if raw.len() > MAX_TASK_SUMMARY_BYTES {
        return Err(AdviceError::invalid_request(
            "task_summary exceeds the 8192 UTF-8 byte limit.",
        ));
    }
    if raw
        .chars()
        .any(|ch| ch == '\0' || (ch.is_control() && !matches!(ch, '\n' | '\r' | '\t')))
    {
        return Err(AdviceError::invalid_request(
            "task_summary contains disallowed control characters.",
        ));
    }
    let normalized = raw.replace("\r\n", "\n").replace('\r', "\n");
    let trimmed = normalized.trim();
    if trimmed.is_empty() || trimmed.len() > MAX_TASK_SUMMARY_BYTES {
        return Err(AdviceError::invalid_request(
            "task_summary must not be empty.",
        ));
    }
    Ok(trimmed.to_string())
}

fn validate_scope(
    scope: &AdviceScope,
    create_cache_dir: bool,
    expected_uid: u32,
) -> Result<ValidatedScope, AdviceError> {
    #[cfg(not(unix))]
    {
        let _ = (scope, create_cache_dir, expected_uid);
        return Err(AdviceError::unsupported_platform());
    }

    #[cfg(unix)]
    {
        if !(MIN_CACHE_TTL_SECONDS..=MAX_CACHE_TTL_SECONDS).contains(&scope.cache_ttl_seconds) {
            return Err(AdviceError::invalid_input(
                "cache_ttl_seconds must be between 1 and 86400.",
            ));
        }

        let context_id = validate_strict_identifier(
            &scope.context_id,
            "context_id must be a non-empty bounded identifier.",
        )?;

        let profile_id = match scope.profile_id.as_deref() {
            Some(raw_profile_id) => Some(validate_strict_identifier(
                raw_profile_id,
                "profile_id must be a non-empty bounded identifier.",
            )?),
            None => None,
        };

        validate_normalized_absolute_path(
            &scope.project_root,
            "project_root must be a normalized absolute path.",
        )?;
        validate_normalized_absolute_path(
            &scope.registry_dir,
            "registry_dir must be a normalized absolute path.",
        )?;
        validate_normalized_absolute_path(
            &scope.cache_dir,
            "cache_dir must be a normalized absolute path.",
        )?;

        if paths_overlap(&scope.project_root, &scope.registry_dir)
            || paths_overlap(&scope.project_root, &scope.cache_dir)
            || paths_overlap(&scope.registry_dir, &scope.cache_dir)
        {
            return Err(AdviceError::security_violation(
                "project_root, registry_dir, and cache_dir must be distinct non-overlapping paths.",
            ));
        }

        let canonical_project_root =
            validate_existing_project_root(&scope.project_root, expected_uid)?;
        let project_root_str = canonical_project_root
            .to_str()
            .ok_or_else(|| AdviceError::invalid_input("project_root must be valid UTF-8."))?
            .to_string();

        validate_existing_private_dir(
            &scope.registry_dir,
            expected_uid,
            "registry_dir must be an existing private 0700 directory owned by the current user.",
        )?;

        validate_or_prepare_cache_dir(&scope.cache_dir, create_cache_dir, expected_uid)?;

        Ok(ValidatedScope {
            registry_dir: scope.registry_dir.clone(),
            cache_dir: scope.cache_dir.clone(),
            project_root_str,
            context_id,
            cache_ttl_seconds: scope.cache_ttl_seconds,
            profile_id,
        })
    }
}

fn paths_overlap(a: &Path, b: &Path) -> bool {
    a == b || a.starts_with(b) || b.starts_with(a)
}

fn validate_normalized_absolute_path(
    path: &Path,
    invalid_message: &'static str,
) -> Result<(), AdviceError> {
    let Some(raw) = path.to_str() else {
        return Err(AdviceError::invalid_input(invalid_message));
    };
    if raw.is_empty()
        || raw == "/"
        || !raw.starts_with('/')
        || raw.ends_with('/')
        || raw.contains("//")
        || raw.chars().any(|ch| ch.is_control())
    {
        return Err(AdviceError::invalid_input(invalid_message));
    }

    let mut normal_count = 0usize;
    for component in path.components() {
        match component {
            Component::RootDir => {}
            Component::Normal(part) => {
                if part.is_empty() {
                    return Err(AdviceError::invalid_input(invalid_message));
                }
                normal_count += 1;
            }
            Component::CurDir | Component::ParentDir | Component::Prefix(_) => {
                return Err(AdviceError::invalid_input(invalid_message));
            }
        }
    }

    if normal_count == 0 {
        return Err(AdviceError::invalid_input(invalid_message));
    }

    // Reject symlink components anywhere in the existing ancestor chain.
    let mut cursor = PathBuf::from("/");
    for component in path.components() {
        if let Component::Normal(part) = component {
            cursor.push(part);
            if let Ok(meta) = fs::symlink_metadata(&cursor) {
                if meta.file_type().is_symlink() {
                    return Err(AdviceError::security_violation(
                        "Symlink path components are not permitted.",
                    ));
                }
            }
        }
    }

    Ok(())
}

#[cfg(unix)]
fn open_nofollow_rdonly(path: &Path) -> Result<File, AdviceError> {
    use std::os::unix::fs::OpenOptionsExt;
    let flags = (rustix::fs::OFlags::NOFOLLOW | rustix::fs::OFlags::NONBLOCK).bits() as i32;
    OpenOptions::new()
        .read(true)
        .custom_flags(flags)
        .open(path)
        .map_err(|_| {
            AdviceError::security_violation("Failed to open path safely without symlinks.")
        })
}

#[cfg(unix)]
fn validate_existing_project_root(
    project_root: &Path,
    expected_uid: u32,
) -> Result<PathBuf, AdviceError> {
    use std::os::unix::fs::MetadataExt;

    let lstat = fs::symlink_metadata(project_root)
        .map_err(|_| AdviceError::invalid_input("project_root directory does not exist."))?;
    if lstat.file_type().is_symlink() || !lstat.file_type().is_dir() {
        return Err(AdviceError::security_violation(
            "project_root must be a regular non-symlink directory.",
        ));
    }
    if lstat.uid() != expected_uid {
        return Err(AdviceError::security_violation(
            "project_root must be owned by the current effective UID.",
        ));
    }

    let handle = open_nofollow_rdonly(project_root)?;
    let fstat = handle
        .metadata()
        .map_err(|_| AdviceError::security_violation("Failed to inspect project_root metadata."))?;
    if !fstat.is_dir()
        || fstat.dev() != lstat.dev()
        || fstat.ino() != lstat.ino()
        || fstat.uid() != expected_uid
    {
        return Err(AdviceError::security_violation(
            "project_root failed before/after identity verification.",
        ));
    }

    let canonical = fs::canonicalize(project_root)
        .map_err(|_| AdviceError::security_violation("Failed to canonicalize project_root."))?;
    if canonical != project_root {
        return Err(AdviceError::security_violation(
            "project_root must match its canonical path without symlinks.",
        ));
    }
    Ok(canonical)
}

#[cfg(unix)]
fn validate_existing_private_dir(
    dir: &Path,
    expected_uid: u32,
    err_message: &'static str,
) -> Result<(), AdviceError> {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    let lstat =
        fs::symlink_metadata(dir).map_err(|_| AdviceError::security_violation(err_message))?;
    if lstat.file_type().is_symlink() || !lstat.file_type().is_dir() {
        return Err(AdviceError::security_violation(err_message));
    }
    if lstat.uid() != expected_uid {
        return Err(AdviceError::security_violation(err_message));
    }
    let mode = lstat.permissions().mode() & 0o777;
    if mode != 0o700 {
        return Err(AdviceError::security_violation(err_message));
    }

    let handle = open_nofollow_rdonly(dir)?;
    let fstat = handle
        .metadata()
        .map_err(|_| AdviceError::security_violation(err_message))?;
    if !fstat.is_dir()
        || fstat.dev() != lstat.dev()
        || fstat.ino() != lstat.ino()
        || fstat.uid() != expected_uid
        || (fstat.permissions().mode() & 0o777) != 0o700
    {
        return Err(AdviceError::security_violation(err_message));
    }

    Ok(())
}

#[cfg(unix)]
fn validate_or_prepare_cache_dir(
    cache_dir: &Path,
    create_if_missing: bool,
    expected_uid: u32,
) -> Result<(), AdviceError> {
    use std::os::unix::fs::DirBuilderExt;

    let parent = cache_dir.parent().ok_or_else(|| {
        AdviceError::invalid_input("cache_dir must have an existing private parent directory.")
    })?;
    validate_existing_private_dir(
        parent,
        expected_uid,
        "cache_dir parent must be an existing private 0700 directory owned by the current user.",
    )?;

    match fs::symlink_metadata(cache_dir) {
        Ok(_) => validate_existing_private_dir(
            cache_dir,
            expected_uid,
            "cache_dir must be a private 0700 non-symlink directory owned by the current user.",
        ),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            if !create_if_missing {
                return Ok(());
            }
            let mut builder = fs::DirBuilder::new();
            builder.mode(0o700);
            match builder.create(cache_dir) {
                Ok(()) => {}
                Err(create_err) if create_err.kind() == std::io::ErrorKind::AlreadyExists => {}
                Err(_) => return Err(AdviceError::io_error()),
            }
            validate_existing_private_dir(
                cache_dir,
                expected_uid,
                "Created cache_dir failed private 0700 ownership and identity verification.",
            )
        }
        Err(_) => Err(AdviceError::security_violation(
            "Unable to inspect cache_dir metadata.",
        )),
    }
}

#[cfg(unix)]
fn read_bounded_private_file(
    path: &Path,
    max_bytes: usize,
    expected_uid: u32,
) -> Result<Vec<u8>, AdviceError> {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    let lstat = fs::symlink_metadata(path)
        .map_err(|_| AdviceError::invalid_request("Target file does not exist."))?;
    if lstat.file_type().is_symlink() || !lstat.file_type().is_file() {
        return Err(AdviceError::security_violation(
            "File must be a regular non-symlink file.",
        ));
    }
    if lstat.uid() != expected_uid {
        return Err(AdviceError::security_violation(
            "File must be owned by the current effective UID.",
        ));
    }
    let mode = lstat.permissions().mode() & 0o777;
    if mode != 0o600 || lstat.nlink() != 1 {
        return Err(AdviceError::security_violation(
            "File must have exact 0600 permissions and a single link.",
        ));
    }
    if lstat.len() > max_bytes as u64 {
        return Err(AdviceError::request_too_large());
    }

    let handle = open_nofollow_rdonly(path)?;
    let fstat_before = handle
        .metadata()
        .map_err(|_| AdviceError::security_violation("Failed to read file metadata."))?;
    if !fstat_before.is_file()
        || fstat_before.dev() != lstat.dev()
        || fstat_before.ino() != lstat.ino()
        || fstat_before.uid() != expected_uid
        || (fstat_before.permissions().mode() & 0o777) != 0o600
        || fstat_before.nlink() != 1
        || fstat_before.len() != lstat.len()
    {
        return Err(AdviceError::security_violation(
            "File failed pre-read identity check.",
        ));
    }

    let mut bytes = Vec::with_capacity(fstat_before.len() as usize);
    let mut reader = (&handle).take((max_bytes as u64).saturating_add(1));
    reader
        .read_to_end(&mut bytes)
        .map_err(|_| AdviceError::io_error())?;

    if bytes.len() > max_bytes {
        return Err(AdviceError::request_too_large());
    }

    let fstat_after = handle
        .metadata()
        .map_err(|_| AdviceError::security_violation("Failed to recheck file metadata."))?;
    if !fstat_after.is_file()
        || fstat_after.dev() != fstat_before.dev()
        || fstat_after.ino() != fstat_before.ino()
        || fstat_after.uid() != expected_uid
        || (fstat_after.permissions().mode() & 0o777) != 0o600
        || fstat_after.nlink() != 1
        || fstat_after.len() != fstat_before.len()
        || fstat_after.mtime() != fstat_before.mtime()
        || fstat_after.mtime_nsec() != fstat_before.mtime_nsec()
        || fstat_after.ctime() != fstat_before.ctime()
        || fstat_after.ctime_nsec() != fstat_before.ctime_nsec()
    {
        return Err(AdviceError::security_violation(
            "File changed during read; before/after identity check failed.",
        ));
    }

    Ok(bytes)
}

#[cfg(not(unix))]
fn read_bounded_private_file(
    _path: &Path,
    _max_bytes: usize,
    _expected_uid: u32,
) -> Result<Vec<u8>, AdviceError> {
    Err(AdviceError::unsupported_platform())
}

fn read_and_validate_request_file(
    request_path: &Path,
    expected_uid: u32,
) -> Result<ValidatedAdviceRequest, AdviceError> {
    validate_normalized_absolute_path(
        request_path,
        "request path must be a normalized absolute path.",
    )?;

    let bytes = read_bounded_private_file(request_path, MAX_REQUEST_FILE_BYTES, expected_uid)?;
    if bytes.is_empty() {
        return Err(AdviceError::invalid_request(
            "Advice request file must not be empty.",
        ));
    }

    let raw: AdviceRequest = serde_json::from_slice(&bytes).map_err(|_| {
        AdviceError::invalid_request(
            "Advice request JSON is malformed or contains unknown/duplicate fields.",
        )
    })?;

    if raw.schema_version != ADVICE_REQUEST_SCHEMA_VERSION {
        return Err(AdviceError::invalid_request(
            "Advice request schema_version must be 1.",
        ));
    }

    if raw.expected_registry_revision == 0 || raw.expected_registry_revision > MAX_JS_SAFE_INTEGER {
        return Err(AdviceError::invalid_request(
            "expected_registry_revision must be a positive safe integer.",
        ));
    }

    if raw.task_revision == 0 || raw.task_revision > MAX_JS_SAFE_INTEGER {
        return Err(AdviceError::invalid_request(
            "task_revision must be a positive safe integer.",
        ));
    }

    let _ = validate_request_identifier(&raw.tool_id, "tool_id in request is invalid.")?;
    let _ = validate_request_identifier(&raw.model_id, "model_id in request is invalid.")?;
    let _ = validate_request_identifier(&raw.task_id, "task_id in request is invalid.")?;
    let normalized_summary = normalize_and_validate_task_summary(&raw.task_summary)?;

    Ok(ValidatedAdviceRequest {
        raw,
        normalized_summary,
    })
}

fn read_readonly_registry_snapshot(
    registry_dir: &Path,
    expected_uid: u32,
) -> Result<RegistrySnapshot, AdviceError> {
    let registry_file = registry_dir.join("registry.json");
    if fs::symlink_metadata(&registry_file).is_ok() {
        let _ = read_bounded_private_file(
            &registry_file,
            MAX_REGISTRY_FILE_BYTES as usize,
            expected_uid,
        )
        .map_err(|err| {
            if err.code() == "security_violation" {
                err
            } else {
                AdviceError::registry_error()
            }
        })?;
    }

    RegistryStore::read_snapshot_readonly_at(registry_dir).map_err(|err| {
        if err.code() == "security_violation" {
            AdviceError::security_violation(
                "Registry directory or file failed security validation.",
            )
        } else {
            AdviceError::registry_error()
        }
    })
}

fn validate_enabled_jev_tool(
    snapshot: &RegistrySnapshot,
    tool_id: &str,
) -> Result<(), AdviceError> {
    let tool = snapshot
        .tools
        .iter()
        .find(|candidate| candidate.id == tool_id)
        .ok_or_else(AdviceError::tool_not_found)?;

    if tool.adapter != ToolAdapter::JevApi {
        return Err(AdviceError::unsupported_adapter());
    }

    if !tool.enabled {
        return Err(AdviceError::tool_disabled());
    }

    Ok(())
}

fn resolve_jev_tool_and_model(
    snapshot: &RegistrySnapshot,
    request: &ValidatedAdviceRequest,
    profile_id: Option<&str>,
) -> Result<ResolvedJevTarget, AdviceError> {
    if snapshot.revision != request.raw.expected_registry_revision {
        return Err(AdviceError::stale_registry_revision());
    }

    let resolved_profile_id = if let Some(expected_profile_id) = profile_id {
        let profile = snapshot
            .profiles
            .iter()
            .find(|candidate| candidate.id == expected_profile_id)
            .ok_or_else(AdviceError::profile_not_found)?;

        let advisor = profile
            .decision_advisor
            .as_ref()
            .ok_or_else(AdviceError::decision_advisor_not_configured)?;

        if advisor.mode != DecisionAdvisorMode::Shadow {
            return Err(AdviceError::invalid_request(
                "Selected workflow profile decision advisor must use shadow mode.",
            ));
        }

        let advisor_model = snapshot
            .models
            .iter()
            .find(|candidate| candidate.id == advisor.model_id)
            .ok_or_else(AdviceError::model_not_found)?;

        let advisor_tool = snapshot
            .tools
            .iter()
            .find(|candidate| candidate.id == advisor_model.tool_id)
            .ok_or_else(AdviceError::tool_not_found)?;

        if advisor_tool.adapter != ToolAdapter::JevApi {
            return Err(AdviceError::unsupported_adapter());
        }

        if !advisor_tool.enabled {
            return Err(AdviceError::tool_disabled());
        }

        if !advisor_model.allow_no_effort {
            return Err(AdviceError::invalid_model());
        }

        validate_enabled_jev_tool(snapshot, &request.raw.tool_id)?;

        if advisor_model.tool_id != request.raw.tool_id || advisor.model_id != request.raw.model_id
        {
            return Err(AdviceError::profile_advisor_mismatch());
        }

        Some(profile.id.clone())
    } else {
        validate_enabled_jev_tool(snapshot, &request.raw.tool_id)?;
        None
    };

    let model = snapshot
        .models
        .iter()
        .find(|candidate| {
            candidate.id == request.raw.model_id && candidate.tool_id == request.raw.tool_id
        })
        .ok_or_else(AdviceError::model_not_found)?;

    let requested_model = model.model_arg.trim();
    if requested_model != model.model_arg || !is_supported_jev_model(requested_model) {
        return Err(AdviceError::invalid_model());
    }

    Ok(ResolvedJevTarget {
        profile_id: resolved_profile_id,
        tool_id: request.raw.tool_id.clone(),
        model_id: model.id.clone(),
        requested_model: requested_model.to_string(),
    })
}

/// Computes the lowercase hexadecimal SHA-256 digest of `bytes`.
pub fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut out = String::with_capacity(64);
    for byte in digest {
        use std::fmt::Write as _;
        let _ = write!(&mut out, "{byte:02x}");
    }
    out
}

fn hash_len_prefixed(hasher: &mut Sha256, field: &[u8]) {
    let len = field.len() as u64;
    hasher.update(len.to_le_bytes());
    hasher.update(field);
}

fn compute_prompt_digest() -> String {
    let mut hasher = Sha256::new();
    hash_len_prefixed(&mut hasher, ADVICE_PROMPT_VERSION.as_bytes());
    hash_len_prefixed(&mut hasher, QUESTION_NAME.as_bytes());
    hash_len_prefixed(&mut hasher, JEV_INSTRUCTIONS.as_bytes());
    hash_len_prefixed(&mut hasher, EASY_DESCRIPTION.as_bytes());
    hash_len_prefixed(&mut hasher, MEDIUM_DESCRIPTION.as_bytes());
    hash_len_prefixed(&mut hasher, HARD_DESCRIPTION.as_bytes());
    hash_len_prefixed(&mut hasher, EXTREME_DESCRIPTION.as_bytes());
    let digest = hasher.finalize();
    let mut out = String::with_capacity(64);
    for byte in digest {
        use std::fmt::Write as _;
        let _ = write!(&mut out, "{byte:02x}");
    }
    out
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn compute_cache_key(
    prompt_digest: &str,
    canonical_project_root: &str,
    context_id: &str,
    task_id: &str,
    task_revision: u64,
    summary_sha256: &str,
    registry_revision: u64,
    tool_id: &str,
    model_id: &str,
    requested_model: &str,
    profile_id: Option<&str>,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(ADVICE_CACHE_SCHEMA_VERSION.to_le_bytes());
    hash_len_prefixed(&mut hasher, prompt_digest.as_bytes());
    hash_len_prefixed(&mut hasher, canonical_project_root.as_bytes());
    hash_len_prefixed(&mut hasher, context_id.as_bytes());
    hash_len_prefixed(&mut hasher, task_id.as_bytes());
    hasher.update(task_revision.to_le_bytes());
    hash_len_prefixed(&mut hasher, summary_sha256.as_bytes());
    hasher.update(registry_revision.to_le_bytes());
    hash_len_prefixed(&mut hasher, tool_id.as_bytes());
    hash_len_prefixed(&mut hasher, model_id.as_bytes());
    hash_len_prefixed(&mut hasher, requested_model.as_bytes());
    if let Some(profile_id) = profile_id {
        hash_len_prefixed(&mut hasher, b"profile_id");
        hash_len_prefixed(&mut hasher, profile_id.as_bytes());
    }
    let digest = hasher.finalize();
    let mut out = String::with_capacity(64);
    for byte in digest {
        use std::fmt::Write as _;
        let _ = write!(&mut out, "{byte:02x}");
    }
    out
}

struct GlobalCacheLockGuard {
    file: File,
    _process_guard: MutexGuard<'static, ()>,
}

impl Drop for GlobalCacheLockGuard {
    fn drop(&mut self) {
        let _ = self.file.unlock();
    }
}

struct PerKeyLockGuard {
    file: File,
}

impl Drop for PerKeyLockGuard {
    fn drop(&mut self) {
        let _ = self.file.unlock();
    }
}

#[cfg(unix)]
fn validate_and_verify_lock_file_after_open(
    lock_path: &Path,
    file: &File,
    pre_open_lstat: Option<&fs::Metadata>,
    expected_uid: u32,
) -> Result<(), AdviceError> {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    let fstat = file
        .metadata()
        .map_err(|_| AdviceError::security_violation("Failed to inspect cache lock metadata."))?;
    if !fstat.is_file()
        || fstat.uid() != expected_uid
        || (fstat.permissions().mode() & 0o777) != 0o600
        || fstat.len() != 0
        || fstat.nlink() != 1
    {
        return Err(AdviceError::security_violation(
            "Cache lock file failed exact 0600, UID, single-link, or size verification.",
        ));
    }

    if let Some(lstat) = pre_open_lstat {
        if fstat.dev() != lstat.dev() || fstat.ino() != lstat.ino() {
            return Err(AdviceError::security_violation(
                "Cache lock file failed pre/post open identity check.",
            ));
        }
    }

    let post_lstat = fs::symlink_metadata(lock_path).map_err(|_| {
        AdviceError::security_violation("Cache lock file disappeared during identity check.")
    })?;
    if post_lstat.file_type().is_symlink()
        || !post_lstat.file_type().is_file()
        || post_lstat.dev() != fstat.dev()
        || post_lstat.ino() != fstat.ino()
        || post_lstat.uid() != expected_uid
        || (post_lstat.permissions().mode() & 0o777) != 0o600
        || post_lstat.nlink() != 1
        || post_lstat.len() != 0
    {
        return Err(AdviceError::security_violation(
            "Cache lock file failed post-lock identity verification.",
        ));
    }

    Ok(())
}

#[cfg(unix)]
fn check_existing_lock_lstat(
    lock_path: &Path,
    expected_uid: u32,
) -> Result<Option<fs::Metadata>, AdviceError> {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    match fs::symlink_metadata(lock_path) {
        Ok(lstat) => {
            if lstat.file_type().is_symlink() || !lstat.file_type().is_file() {
                return Err(AdviceError::security_violation(
                    "Cache lock file must be a regular non-symlink file.",
                ));
            }
            if lstat.uid() != expected_uid
                || (lstat.permissions().mode() & 0o777) != 0o600
                || lstat.nlink() != 1
                || lstat.len() != 0
            {
                return Err(AdviceError::security_violation(
                    "Cache lock file failed ownership, exact 0600 permission, single-link, or size validation.",
                ));
            }
            Ok(Some(lstat))
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(_) => Err(AdviceError::security_violation(
            "Unable to inspect cache lock metadata.",
        )),
    }
}

#[cfg(unix)]
fn acquire_global_cache_lock(
    cache_dir: &Path,
    expected_uid: u32,
) -> Result<GlobalCacheLockGuard, AdviceError> {
    use std::os::unix::fs::OpenOptionsExt;

    let process_guard = PROCESS_CACHE_MUTEX
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());

    validate_existing_private_dir(
        cache_dir,
        expected_uid,
        "cache_dir failed private 0700 validation before acquiring global cache lock.",
    )?;

    let lock_path = cache_dir.join(GLOBAL_CACHE_LOCK_FILENAME);
    let pre_lstat = check_existing_lock_lstat(&lock_path, expected_uid)?;

    let flags = (rustix::fs::OFlags::NOFOLLOW | rustix::fs::OFlags::NONBLOCK).bits() as i32;
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .custom_flags(flags)
        .open(&lock_path)
        .map_err(|_| AdviceError::security_violation("Failed to open global cache lock safely."))?;

    validate_and_verify_lock_file_after_open(&lock_path, &file, pre_lstat.as_ref(), expected_uid)?;

    file.lock_exclusive().map_err(|_| AdviceError::io_error())?;

    validate_and_verify_lock_file_after_open(&lock_path, &file, None, expected_uid)?;

    Ok(GlobalCacheLockGuard {
        file,
        _process_guard: process_guard,
    })
}

#[cfg(not(unix))]
fn acquire_global_cache_lock(
    _cache_dir: &Path,
    _expected_uid: u32,
) -> Result<GlobalCacheLockGuard, AdviceError> {
    Err(AdviceError::unsupported_platform())
}

#[cfg(unix)]
fn try_acquire_existing_per_key_lock(
    cache_dir: &Path,
    cache_key: &str,
    expected_uid: u32,
) -> Result<Option<PerKeyLockGuard>, AdviceError> {
    use std::os::unix::fs::OpenOptionsExt;

    let lock_path = cache_dir.join(format!("{cache_key}.lock"));
    let Some(pre_lstat) = check_existing_lock_lstat(&lock_path, expected_uid)? else {
        return Ok(None);
    };

    let flags = (rustix::fs::OFlags::NOFOLLOW | rustix::fs::OFlags::NONBLOCK).bits() as i32;
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .custom_flags(flags)
        .open(&lock_path)
        .map_err(|_| {
            AdviceError::security_violation("Failed to open existing per-key cache lock safely.")
        })?;

    validate_and_verify_lock_file_after_open(&lock_path, &file, Some(&pre_lstat), expected_uid)?;

    file.try_lock_exclusive()
        .map_err(|_| AdviceError::cache_busy())?;

    validate_and_verify_lock_file_after_open(&lock_path, &file, Some(&pre_lstat), expected_uid)?;

    Ok(Some(PerKeyLockGuard { file }))
}

#[cfg(not(unix))]
fn try_acquire_existing_per_key_lock(
    _cache_dir: &Path,
    _cache_key: &str,
    _expected_uid: u32,
) -> Result<Option<PerKeyLockGuard>, AdviceError> {
    Err(AdviceError::unsupported_platform())
}

#[cfg(unix)]
fn acquire_or_create_per_key_lock(
    cache_dir: &Path,
    cache_key: &str,
    expected_uid: u32,
) -> Result<PerKeyLockGuard, AdviceError> {
    use std::os::unix::fs::OpenOptionsExt;

    let lock_path = cache_dir.join(format!("{cache_key}.lock"));
    let pre_lstat = check_existing_lock_lstat(&lock_path, expected_uid)?;

    let flags = (rustix::fs::OFlags::NOFOLLOW | rustix::fs::OFlags::NONBLOCK).bits() as i32;
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .custom_flags(flags)
        .open(&lock_path)
        .map_err(|_| {
            AdviceError::security_violation("Failed to open per-key cache lock safely.")
        })?;

    validate_and_verify_lock_file_after_open(&lock_path, &file, pre_lstat.as_ref(), expected_uid)?;

    file.try_lock_exclusive()
        .map_err(|_| AdviceError::cache_busy())?;

    validate_and_verify_lock_file_after_open(&lock_path, &file, None, expected_uid)?;

    Ok(PerKeyLockGuard { file })
}

#[cfg(not(unix))]
fn acquire_or_create_per_key_lock(
    _cache_dir: &Path,
    _cache_key: &str,
    _expected_uid: u32,
) -> Result<PerKeyLockGuard, AdviceError> {
    Err(AdviceError::unsupported_platform())
}

#[derive(Debug, Default)]
struct CacheScanSummary {
    record_keys: Vec<(String, u64)>,
    lock_keys: Vec<String>,
    other_disk_bytes: u64,
    current_record_bytes: u64,
    current_key_exists: bool,
    total_entries: usize,
}

impl CacheScanSummary {
    fn total_disk_bytes(&self) -> u64 {
        self.other_disk_bytes
            .saturating_add(self.current_record_bytes)
    }
}

fn is_lower_hex_64(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

#[cfg(unix)]
fn scan_cache_directory(
    cache_dir: &Path,
    current_key: &str,
    expected_uid: u32,
) -> Result<CacheScanSummary, AdviceError> {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    validate_existing_private_dir(
        cache_dir,
        expected_uid,
        "cache_dir failed private 0700 validation before directory scan.",
    )?;

    let entries = fs::read_dir(cache_dir).map_err(|_| AdviceError::io_error())?;
    let mut summary = CacheScanSummary::default();

    for entry_res in entries {
        summary.total_entries += 1;
        if summary.total_entries > MAX_CACHE_DIR_ENTRIES {
            return Err(AdviceError::cache_capacity_exceeded());
        }

        let entry = entry_res.map_err(|_| AdviceError::io_error())?;
        let file_name_os = entry.file_name();
        let Some(file_name) = file_name_os.to_str() else {
            return Err(AdviceError::security_violation(
                "Cache directory contains non-UTF-8 entry name.",
            ));
        };

        let path = entry.path();
        let lstat = fs::symlink_metadata(&path).map_err(|_| {
            AdviceError::security_violation("Failed to inspect cache directory entry metadata.")
        })?;
        if lstat.file_type().is_symlink() || !lstat.file_type().is_file() {
            return Err(AdviceError::security_violation(
                "Cache directory must contain only regular non-symlink files.",
            ));
        }
        if lstat.uid() != expected_uid
            || (lstat.permissions().mode() & 0o777) != 0o600
            || lstat.nlink() != 1
        {
            return Err(AdviceError::security_violation(
                "Cache directory entry failed UID, exact 0600 permission, or single-link validation.",
            ));
        }

        let handle = open_nofollow_rdonly(&path)?;
        let fstat = handle.metadata().map_err(|_| {
            AdviceError::security_violation("Failed to verify cache directory entry identity.")
        })?;
        if !fstat.is_file()
            || fstat.dev() != lstat.dev()
            || fstat.ino() != lstat.ino()
            || fstat.uid() != expected_uid
            || (fstat.permissions().mode() & 0o777) != 0o600
            || fstat.nlink() != 1
            || fstat.len() != lstat.len()
        {
            return Err(AdviceError::security_violation(
                "Cache directory entry failed pre/post open identity verification.",
            ));
        }

        if file_name == GLOBAL_CACHE_LOCK_FILENAME {
            if fstat.len() != 0 {
                return Err(AdviceError::security_violation(
                    "Global cache lock file must be 0 bytes.",
                ));
            }
        } else if let Some(stem) = file_name.strip_suffix(".json") {
            if !is_lower_hex_64(stem) {
                return Err(AdviceError::security_violation(
                    "Cache record filename must be a 64-character lowercase hex SHA-256 key.",
                ));
            }
            let len = fstat.len();
            if len == 0 || len > MAX_CACHE_RECORD_BYTES as u64 {
                return Err(AdviceError::security_violation(
                    "Cache record file size is out of bounds.",
                ));
            }
            if stem == current_key {
                summary.current_key_exists = true;
                summary.current_record_bytes = len;
            } else {
                summary.other_disk_bytes = summary.other_disk_bytes.saturating_add(len);
                summary.record_keys.push((stem.to_string(), len));
            }
        } else if let Some(stem) = file_name.strip_suffix(".lock") {
            if !is_lower_hex_64(stem) || fstat.len() != 0 {
                return Err(AdviceError::security_violation(
                    "Cache lock file is invalid or non-empty.",
                ));
            }
            summary.lock_keys.push(stem.to_string());
        } else if file_name.starts_with(".tmp-advice-") {
            let len = fstat.len();
            if len > MAX_CACHE_RECORD_BYTES as u64 {
                return Err(AdviceError::security_violation(
                    "Temporary cache file exceeds size limit.",
                ));
            }
            // Temporary entries count toward the 1 MiB filesystem bound so they cannot
            // permit an uncounted second MiB.
            summary.other_disk_bytes = summary.other_disk_bytes.saturating_add(len);
        } else {
            return Err(AdviceError::security_violation(
                "Unexpected file present in advice cache directory.",
            ));
        }
    }

    let total_records = summary.record_keys.len() + usize::from(summary.current_key_exists);
    if total_records > MAX_CACHE_RECORDS || summary.total_disk_bytes() > MAX_CACHE_TOTAL_BYTES {
        return Err(AdviceError::cache_capacity_exceeded());
    }

    Ok(summary)
}

#[cfg(not(unix))]
fn scan_cache_directory(
    _cache_dir: &Path,
    _current_key: &str,
    _expected_uid: u32,
) -> Result<CacheScanSummary, AdviceError> {
    Err(AdviceError::unsupported_platform())
}

fn ensure_cache_capacity_for_write(
    cache_dir: &Path,
    current_key: &str,
    new_record_len: usize,
    scan: &mut CacheScanSummary,
    now_ms: u64,
    ttl_ms: u64,
    expected_uid: u32,
) -> Result<(), AdviceError> {
    if new_record_len == 0 || new_record_len > MAX_CACHE_RECORD_BYTES {
        return Err(AdviceError::cache_capacity_exceeded());
    }

    // Because atomic publication writes `.tmp-advice-*` alongside any existing `<current_key>.json`
    // before `rename`, total existing disk bytes (`other_disk_bytes + current_record_bytes`) plus
    // `new_record_len` must fit within `MAX_CACHE_TOTAL_BYTES`.
    let needs_pruning = (!scan.current_key_exists && scan.record_keys.len() >= MAX_CACHE_RECORDS)
        || scan
            .total_disk_bytes()
            .saturating_add(new_record_len as u64)
            > MAX_CACHE_TOTAL_BYTES
        || scan.total_entries.saturating_add(2) > MAX_CACHE_DIR_ENTRIES;

    if needs_pruning {
        prune_expired_completed_records(cache_dir, current_key, now_ms, ttl_ms, expected_uid)?;
        *scan = scan_cache_directory(cache_dir, current_key, expected_uid)?;
    }

    if !scan.current_key_exists && scan.record_keys.len() >= MAX_CACHE_RECORDS {
        return Err(AdviceError::cache_capacity_exceeded());
    }

    if scan
        .total_disk_bytes()
        .saturating_add(new_record_len as u64)
        > MAX_CACHE_TOTAL_BYTES
    {
        return Err(AdviceError::cache_capacity_exceeded());
    }

    if scan.total_entries.saturating_add(2) > MAX_CACHE_DIR_ENTRIES {
        return Err(AdviceError::cache_capacity_exceeded());
    }

    Ok(())
}

fn prune_expired_completed_records(
    cache_dir: &Path,
    current_key: &str,
    now_ms: u64,
    ttl_ms: u64,
    expected_uid: u32,
) -> Result<(), AdviceError> {
    let scan = scan_cache_directory(cache_dir, current_key, expected_uid)?;
    let mut removed_any = false;

    // 1. Reclaim any unlocked orphan `<key>.lock` files that have no `.json` record.
    for lock_key in &scan.lock_keys {
        if lock_key == current_key {
            continue;
        }
        let has_record = scan.record_keys.iter().any(|(k, _)| k == lock_key);
        if !has_record {
            if let Ok(Some(guard)) =
                try_acquire_existing_per_key_lock(cache_dir, lock_key, expected_uid)
            {
                let lock_path = cache_dir.join(format!("{lock_key}.lock"));
                if fs::remove_file(&lock_path).is_ok() {
                    removed_any = true;
                }
                drop(guard);
            }
        }
    }

    // 2. Evict only expired `Completed` records whose per-key lock is not held.
    //    Active `Pending` and `Uncertain` records always survive pruning.
    for (key, _) in scan.record_keys {
        if key == current_key {
            continue;
        }
        let maybe_lock = match try_acquire_existing_per_key_lock(cache_dir, &key, expected_uid) {
            Ok(lock) => lock,
            Err(_) => continue,
        };
        let record_path = cache_dir.join(format!("{key}.json"));
        let Ok(bytes) =
            read_bounded_private_file(&record_path, MAX_CACHE_RECORD_BYTES, expected_uid)
        else {
            drop(maybe_lock);
            continue;
        };
        let Ok(record) = serde_json::from_slice::<PersistedCacheRecord>(&bytes) else {
            drop(maybe_lock);
            continue;
        };
        if let PersistedAttemptState::Completed {
            completed_at_ms, ..
        } = record.state
        {
            if now_ms.saturating_sub(completed_at_ms) > ttl_ms
                && fs::remove_file(&record_path).is_ok()
            {
                let lock_path = cache_dir.join(format!("{key}.lock"));
                let _ = fs::remove_file(&lock_path);
                removed_any = true;
            }
        }
        drop(maybe_lock);
    }

    if removed_any {
        sync_directory(cache_dir)?;
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn read_and_validate_cache_record(
    record_path: &Path,
    expected_prompt_digest: &str,
    expected_cache_key: &str,
    expected_project_root_sha256: &str,
    expected_context_id: &str,
    expected_task_id: &str,
    expected_task_revision: u64,
    expected_summary_sha256: &str,
    expected_registry_revision: u64,
    expected_profile_id: Option<&str>,
    expected_tool_id: &str,
    expected_model_id: &str,
    expected_requested_model: &str,
    expected_uid: u32,
) -> Result<PersistedCacheRecord, AdviceError> {
    let bytes = read_bounded_private_file(record_path, MAX_CACHE_RECORD_BYTES, expected_uid)
        .map_err(|err| {
            if err.code() == "security_violation" {
                err
            } else {
                AdviceError::corrupt_cache()
            }
        })?;

    let record: PersistedCacheRecord =
        serde_json::from_slice(&bytes).map_err(|_| AdviceError::corrupt_cache())?;

    if record.schema_version != ADVICE_CACHE_SCHEMA_VERSION
        || record.prompt_digest != expected_prompt_digest
        || record.cache_key != expected_cache_key
        || record.project_root_sha256 != expected_project_root_sha256
        || record.context_id != expected_context_id
        || record.task_id != expected_task_id
        || record.task_revision != expected_task_revision
        || record.summary_sha256 != expected_summary_sha256
        || record.registry_revision != expected_registry_revision
        || record.profile_id.as_deref() != expected_profile_id
        || record.tool_id != expected_tool_id
        || record.model_id != expected_model_id
        || record.requested_model != expected_requested_model
    {
        return Err(AdviceError::corrupt_cache());
    }

    match &record.state {
        PersistedAttemptState::Pending {
            attempt_id,
            started_at_ms,
        } => {
            if attempt_id.trim().is_empty() || *started_at_ms > MAX_JS_SAFE_INTEGER {
                return Err(AdviceError::corrupt_cache());
            }
        }
        PersistedAttemptState::Uncertain {
            attempt_id,
            failed_at_ms,
            error_code,
        } => {
            if attempt_id.trim().is_empty()
                || *failed_at_ms > MAX_JS_SAFE_INTEGER
                || error_code.trim().is_empty()
            {
                return Err(AdviceError::corrupt_cache());
            }
        }
        PersistedAttemptState::Completed {
            attempt_id,
            completed_at_ms,
            advice,
        } => {
            if attempt_id.trim().is_empty() || *completed_at_ms > MAX_JS_SAFE_INTEGER {
                return Err(AdviceError::corrupt_cache());
            }
            validate_persisted_advice(advice)?;
        }
    }

    Ok(record)
}

fn validate_persisted_advice(advice: &DecisionAdvice) -> Result<(), AdviceError> {
    if advice.provider != DecisionProvider::Jev {
        return Err(AdviceError::corrupt_cache());
    }
    if !is_supported_jev_model(advice.model.trim()) || advice.model.trim() != advice.model {
        return Err(AdviceError::corrupt_cache());
    }
    if advice.difficulty.is_none() {
        return Err(AdviceError::corrupt_cache());
    }
    let Some(confidence) = advice.confidence else {
        return Err(AdviceError::corrupt_cache());
    };
    if !confidence.is_finite() || !(0.0..=1.0).contains(&confidence) {
        return Err(AdviceError::corrupt_cache());
    }
    let expected_needs_review = confidence < MIN_CONFIDENCE_THRESHOLD;
    let expected_reason_code = if expected_needs_review {
        "low_confidence"
    } else {
        "classified"
    };
    if advice.needs_review != expected_needs_review || advice.reason_code != expected_reason_code {
        return Err(AdviceError::corrupt_cache());
    }
    if let Some(inp) = advice.input_tokens {
        if inp > MAX_JS_SAFE_INTEGER {
            return Err(AdviceError::corrupt_cache());
        }
    }
    if let Some(out) = advice.output_tokens {
        if out > MAX_JS_SAFE_INTEGER {
            return Err(AdviceError::corrupt_cache());
        }
    }
    Ok(())
}

fn serialize_cache_record(record: &PersistedCacheRecord) -> Result<Vec<u8>, AdviceError> {
    let serialized = serde_json::to_vec_pretty(record).map_err(|_| AdviceError::io_error())?;
    if serialized.is_empty() || serialized.len() > MAX_CACHE_RECORD_BYTES {
        return Err(AdviceError::cache_capacity_exceeded());
    }
    Ok(serialized)
}

#[cfg(unix)]
struct ScopedTempFile {
    path: PathBuf,
    persisted: bool,
}

#[cfg(unix)]
impl Drop for ScopedTempFile {
    fn drop(&mut self) {
        if !self.persisted {
            let _ = fs::remove_file(&self.path);
        }
    }
}

#[cfg(unix)]
fn write_serialized_cache_record_atomic(
    cache_dir: &Path,
    record_path: &Path,
    serialized: &[u8],
    existing_disk_bytes: u64,
    expected_uid: u32,
) -> Result<(), AdviceError> {
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};

    validate_existing_private_dir(
        cache_dir,
        expected_uid,
        "cache_dir failed private 0700 verification before write.",
    )?;

    if serialized.is_empty() || serialized.len() > MAX_CACHE_RECORD_BYTES {
        return Err(AdviceError::cache_capacity_exceeded());
    }
    if existing_disk_bytes.saturating_add(serialized.len() as u64) > MAX_CACHE_TOTAL_BYTES {
        return Err(AdviceError::cache_capacity_exceeded());
    }

    let seq = ATTEMPT_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let temp_path = cache_dir.join(format!(".tmp-advice-{}-{seq}", std::process::id()));
    let flags = (rustix::fs::OFlags::NOFOLLOW | rustix::fs::OFlags::NONBLOCK).bits() as i32;
    let mut temp_file = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(flags)
        .open(&temp_path)
        .map_err(|_| AdviceError::io_error())?;

    let mut temp_guard = ScopedTempFile {
        path: temp_path,
        persisted: false,
    };

    let fstat_before = temp_file
        .metadata()
        .map_err(|_| AdviceError::security_violation("Failed to inspect temporary cache file."))?;
    if !fstat_before.is_file()
        || fstat_before.uid() != expected_uid
        || (fstat_before.permissions().mode() & 0o777) != 0o600
        || fstat_before.nlink() != 1
        || fstat_before.len() != 0
    {
        return Err(AdviceError::security_violation(
            "Temporary cache file failed UID, exact 0600 permission, or single-link check.",
        ));
    }

    temp_file
        .write_all(serialized)
        .map_err(|_| AdviceError::io_error())?;
    temp_file.flush().map_err(|_| AdviceError::io_error())?;
    temp_file.sync_all().map_err(|_| AdviceError::io_error())?;

    let fstat_after = temp_file
        .metadata()
        .map_err(|_| AdviceError::security_violation("Failed to recheck temporary cache file."))?;
    if !fstat_after.is_file()
        || fstat_after.dev() != fstat_before.dev()
        || fstat_after.ino() != fstat_before.ino()
        || fstat_after.uid() != expected_uid
        || (fstat_after.permissions().mode() & 0o777) != 0o600
        || fstat_after.nlink() != 1
        || fstat_after.len() != serialized.len() as u64
    {
        return Err(AdviceError::security_violation(
            "Temporary cache file failed post-write identity check.",
        ));
    }

    fs::rename(&temp_guard.path, record_path).map_err(|_| AdviceError::io_error())?;
    temp_guard.persisted = true;

    let post_lstat = fs::symlink_metadata(record_path).map_err(|_| {
        AdviceError::security_violation("Published cache record failed post-rename inspection.")
    })?;
    if post_lstat.file_type().is_symlink()
        || !post_lstat.file_type().is_file()
        || post_lstat.dev() != fstat_after.dev()
        || post_lstat.ino() != fstat_after.ino()
        || post_lstat.uid() != expected_uid
        || (post_lstat.permissions().mode() & 0o777) != 0o600
        || post_lstat.nlink() != 1
        || post_lstat.len() != serialized.len() as u64
    {
        return Err(AdviceError::security_violation(
            "Published cache record failed post-rename identity check.",
        ));
    }

    sync_directory(cache_dir)?;

    Ok(())
}

#[cfg(not(unix))]
fn write_serialized_cache_record_atomic(
    _cache_dir: &Path,
    _record_path: &Path,
    _serialized: &[u8],
    _existing_disk_bytes: u64,
    _expected_uid: u32,
) -> Result<(), AdviceError> {
    Err(AdviceError::unsupported_platform())
}

#[cfg(unix)]
fn sync_directory(dir: &Path) -> Result<(), AdviceError> {
    let handle = open_nofollow_rdonly(dir)?;
    handle.sync_all().map_err(|_| AdviceError::io_error())
}

#[cfg(not(unix))]
fn sync_directory(_dir: &Path) -> Result<(), AdviceError> {
    Err(AdviceError::unsupported_platform())
}
