//! Bounded before-dispatch shadow difficulty forecast hook for `prepare-task-exact`.
//!
//! Persists one immutable private (`0700`/`0600`) forecast record per actual preview
//! `subject_digest` under `<plans_dir>/ordinary-forecasts/<subject_digest>/record.json`,
//! storing only scope identity, baseline difficulty (`task.difficulty`), bounded complexity
//! facts, and `summary_sha256` (never raw summary text).
//!
//! When triggered (`uncertain || coupled || component_count > 1`), the selected profile
//! has an enabled shadow decision advisor in the already-opened validated registry snapshot,
//! and at least two usable effective Worker targets across `Difficulty::ALL` differ in tool,
//! `model_arg`, or effort, acquires a durable exclusive claim before launching the canonical
//! sibling `threadlane-advice` binary nonblocking with `--profile-id <ID> classify` and no
//! `--refresh`. Advisory failures never block a valid preview, never modify the preview
//! authority digest, and never retry.

use std::fmt;
#[cfg(unix)]
use std::fs::{self, File, OpenOptions};
#[cfg(unix)]
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
#[cfg(unix)]
use std::process::Stdio;
#[cfg(unix)]
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use threadlane_authority::validation::validate_sha256_hex;
use threadlane_authority::OperatorApprovalPreview;
#[cfg(unix)]
use threadlane_native::fs_safety::validate_absolute_no_symlink_components;
#[cfg(unix)]
use threadlane_workflow::{resolve_choice, DecisionAdvisorMode, WorkflowRole};
use threadlane_workflow::{Difficulty, RegistrySnapshot, ToolAdapter};
#[cfg(unix)]
use uuid::Uuid;

use crate::CliError;

#[cfg(unix)]
const ORDINARY_FORECASTS_DIR_NAME: &str = "ordinary-forecasts";
#[cfg(unix)]
const ADVICE_BINARY_NAME: &str = "threadlane-advice";
#[cfg(unix)]
const ADVICE_CACHE_DIR_NAME: &str = "advice-cache";

#[cfg(unix)]
const FORECAST_RECORD_SCHEMA_VERSION: u32 = 1;
#[cfg(unix)]
const FORECAST_CLAIM_SCHEMA_VERSION: u32 = 1;
#[cfg(unix)]
const ADVICE_REQUEST_SCHEMA_VERSION: u32 = 1;
#[cfg(unix)]
const FORECAST_MODE_SHADOW: &str = "shadow";

#[cfg(unix)]
const RECORD_FILE_NAME: &str = "record.json";
#[cfg(unix)]
const CLAIM_FILE_NAME: &str = "claim.json";
#[cfg(unix)]
const REQUEST_FILE_NAME: &str = "request.json";
#[cfg(unix)]
const ADVICE_FILE_NAME: &str = "advice.json";

const MIN_FORECAST_COMPONENT_COUNT: u32 = 1;
const MAX_FORECAST_COMPONENT_COUNT: u32 = 32;
const MAX_FORECAST_SUMMARY_BYTES: usize = 512;
#[cfg(unix)]
const MAX_FORECAST_RECORD_BYTES: usize = 16 * 1024;
#[cfg(unix)]
const MAX_ADVICE_REQUEST_BYTES: usize = 16 * 1024;
#[cfg(unix)]
const MAX_IDENTIFIER_BYTES: usize = 128;
#[cfg(unix)]
const MAX_JS_SAFE_INTEGER: u64 = 9_007_199_254_740_991;
#[cfg(unix)]
const DEFAULT_OPENAI_DECISION_MODEL: &str = "gpt-6-luna";
#[cfg(unix)]
const MODEL_DIFFICULTY_RUBRIC: &str =
    "Rubric: Use dependencies, coupling, and uncertainty to choose among the four difficulty grades; component or file count alone does not determine difficulty.";

/// Strict opt-in `forecast` object accepted only on `prepare-task-exact`.
///
/// `Debug` redacts `summary` so caller-supplied prose is never leaked into diagnostics.
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ForecastInput {
    component_count: u32,
    coupled: bool,
    uncertain: bool,
    summary: String,
}

impl fmt::Debug for ForecastInput {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ForecastInput")
            .field("component_count", &self.component_count)
            .field("coupled", &self.coupled)
            .field("uncertain", &self.uncertain)
            .field("summary", &"[REDACTED]")
            .finish()
    }
}

/// Upfront-validated shareable forecast input with precomputed summary SHA-256 digest.
#[derive(Clone)]
pub(super) struct ValidatedForecast {
    component_count: u32,
    coupled: bool,
    uncertain: bool,
    summary: String,
    summary_sha256: String,
}

impl fmt::Debug for ValidatedForecast {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ValidatedForecast")
            .field("component_count", &self.component_count)
            .field("coupled", &self.coupled)
            .field("uncertain", &self.uncertain)
            .field("summary", &"[REDACTED]")
            .field("summary_sha256", &self.summary_sha256)
            .finish()
    }
}

impl ValidatedForecast {
    /// Validates bounds and sanitization invariants upfront before any workflow mutation.
    pub(super) fn validate(input: ForecastInput, canonical_repo: &Path) -> Result<Self, CliError> {
        if !(MIN_FORECAST_COMPONENT_COUNT..=MAX_FORECAST_COMPONENT_COUNT)
            .contains(&input.component_count)
        {
            return Err(CliError::invalid_input(
                "forecast component_count must be an integer between 1 and 32",
            ));
        }
        if input.summary.len() > MAX_FORECAST_SUMMARY_BYTES {
            return Err(CliError::invalid_input(
                "forecast summary must not exceed 512 UTF-8 bytes",
            ));
        }
        if input.summary.chars().any(char::is_control) {
            return Err(CliError::invalid_input(
                "forecast summary must not contain control characters",
            ));
        }
        let trimmed = input.summary.trim();
        if trimmed.is_empty() {
            return Err(CliError::invalid_input(
                "forecast summary must not be empty",
            ));
        }
        if canonical_repo
            .to_str()
            .is_some_and(|repo_str| !repo_str.is_empty() && trimmed.contains(repo_str))
        {
            return Err(CliError::invalid_input(
                "forecast summary must not include local repository paths",
            ));
        }

        let summary_sha256 = sha256_hex(trimmed.as_bytes());
        Ok(Self {
            component_count: input.component_count,
            coupled: input.coupled,
            uncertain: input.uncertain,
            summary: trimmed.to_string(),
            summary_sha256,
        })
    }

    /// Returns `true` when the complexity signals trigger shadow difficulty classification.
    pub(super) const fn should_trigger(&self) -> bool {
        self.uncertain || self.coupled || self.component_count > 1
    }

    #[cfg(unix)]
    const fn complexity(&self) -> ForecastComplexity {
        ForecastComplexity {
            component_count: self.component_count,
            coupled: self.coupled,
            uncertain: self.uncertain,
        }
    }

    /// Builds the bounded shareable text sent to the decision model: strictly the intentionally
    /// supplied summary, structured complexity facts, and a short stable rubric, never task
    /// instructions, paths, approvals, secrets, or Coworker's baseline difficulty guess.
    #[cfg(unix)]
    fn build_model_task_summary(&self) -> String {
        format!(
            "Task summary: {}\nComplexity facts: component_count={}, coupled={}, uncertain={}\n{MODEL_DIFFICULTY_RUBRIC}",
            self.summary, self.component_count, self.coupled, self.uncertain
        )
    }
}

/// Bounded complexity signals persisted inside `record.json`.
#[cfg(unix)]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ForecastComplexity {
    component_count: u32,
    coupled: bool,
    uncertain: bool,
}

/// Outcome status of the before-dispatch shadow forecast hook.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum ShadowForecastStatus {
    /// Simple task (`component_count == 1 && !coupled && !uncertain`) or fixed Worker route
    /// across `Difficulty::ALL`; advisor skipped.
    Skipped,
    /// Exclusive claim acquired and canonical sibling `threadlane-advice` spawned nonblocking.
    Launched,
    /// Advisor unconfigured, disabled, invalid, or canonical sibling unavailable before spawn.
    Unavailable,
    /// Ambiguous, interrupted, post-claim, or storage verification error; never retried.
    Unknown,
}

/// Resolved shadow advisor selection metadata from the validated registry snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ShadowAdvisorSelection {
    tool_id: String,
    model_id: String,
    model_arg: String,
    adapter: ToolAdapter,
}

/// Bounded stdout summary appended only when `prepare-task-exact` receives `forecast`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(super) struct ShadowForecastReceipt {
    advisory_only: bool,
    status: ShadowForecastStatus,
    reason: String,
    triggered: bool,
    reused: bool,
    launched: bool,
    subject_digest: String,
    baseline_difficulty: Difficulty,
    summary_sha256: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    advisor: Option<ShadowAdvisorSelection>,
    #[serde(skip_serializing_if = "Option::is_none")]
    record_path: Option<PathBuf>,
    #[serde(skip_serializing_if = "Option::is_none")]
    advice_path: Option<PathBuf>,
}

/// Authoritative context passed to the before-dispatch shadow forecast hook.
pub(super) struct BeforeDispatchForecastContext<'a> {
    pub(super) registry_dir: &'a Path,
    pub(super) plans_dir: &'a Path,
    pub(super) canonical_repo: &'a Path,
    pub(super) registry_snapshot: &'a RegistrySnapshot,
    pub(super) profile_id: &'a str,
    pub(super) profile_revision: u64,
    pub(super) task_id: &'a str,
    pub(super) baseline_difficulty: Difficulty,
    pub(super) preview: &'a OperatorApprovalPreview,
    pub(super) forecast: &'a ValidatedForecast,
}

/// Immutable persisted forecast record written to
/// `<plans_dir>/ordinary-forecasts/<subject_digest>/record.json`.
///
/// Intentionally stores `summary_sha256` and never raw `forecast.summary`.
#[cfg(unix)]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PersistedForecastRecord {
    schema_version: u32,
    advisory_only: bool,
    mode: String,
    binding_id: String,
    thread_uuid: String,
    plan_id: String,
    plan_revision: u64,
    task_id: String,
    profile_id: String,
    profile_revision: u64,
    base_commit: String,
    subject_digest: String,
    baseline_difficulty: Difficulty,
    complexity: ForecastComplexity,
    summary_sha256: String,
    triggered: bool,
    status: ShadowForecastStatus,
    reason: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    advisor: Option<ShadowAdvisorSelection>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    request_summary_sha256: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    request_file: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    advice_file: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pid_hint: Option<u32>,
    recorded_at_ms: u64,
}

#[cfg(unix)]
#[derive(Serialize)]
struct PersistedClaimMarker<'a> {
    schema_version: u32,
    subject_digest: &'a str,
    pid: u32,
    claimed_at_ms: u64,
}

/// Private version-1 request payload matching `threadlane-advice`'s `AdviceRequest`.
#[cfg(unix)]
#[derive(Serialize)]
struct ShadowAdviceRequest<'a> {
    schema_version: u32,
    expected_registry_revision: u64,
    tool_id: &'a str,
    model_id: &'a str,
    task_id: &'a str,
    task_revision: u64,
    task_summary: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    baseline_difficulty: Option<Difficulty>,
}

/// Runs the bounded before-dispatch shadow forecast hook after a valid private preview has been
/// written. Infallible by design: advisory errors are reported in the returned receipt and never
/// block or alter the valid preview.
pub(super) fn run_before_dispatch_forecast(
    ctx: &BeforeDispatchForecastContext<'_>,
) -> ShadowForecastReceipt {
    if validate_sha256_hex("subject_digest", &ctx.preview.subject_digest).is_err() {
        return receipt_failure(ctx, ShadowForecastStatus::Unknown, "invalid_subject_digest");
    }

    #[cfg(not(unix))]
    {
        receipt_failure(
            ctx,
            ShadowForecastStatus::Unavailable,
            "unsupported_platform",
        )
    }

    #[cfg(unix)]
    {
        run_before_dispatch_forecast_unix(ctx)
    }
}

#[cfg(unix)]
fn run_before_dispatch_forecast_unix(
    ctx: &BeforeDispatchForecastContext<'_>,
) -> ShadowForecastReceipt {
    let expected_uid = current_effective_uid();
    if validate_existing_private_dir(ctx.plans_dir, expected_uid).is_err() {
        return receipt_failure(ctx, ShadowForecastStatus::Unknown, "plans_dir_invalid");
    }

    let forecasts_root = ctx.plans_dir.join(ORDINARY_FORECASTS_DIR_NAME);
    if ensure_private_dir(&forecasts_root, expected_uid).is_err() {
        return receipt_failure(ctx, ShadowForecastStatus::Unknown, "forecasts_dir_invalid");
    }

    let subject_dir = forecasts_root.join(&ctx.preview.subject_digest);
    if let Err(reason) = prepare_subject_dir(&subject_dir, expected_uid) {
        return receipt_failure(ctx, ShadowForecastStatus::Unknown, reason);
    }

    let claim_path = subject_dir.join(CLAIM_FILE_NAME);
    let record_path = subject_dir.join(RECORD_FILE_NAME);

    match acquire_exclusive_claim(
        &claim_path,
        &subject_dir,
        &ctx.preview.subject_digest,
        expected_uid,
    ) {
        ClaimOutcome::Acquired => {}
        ClaimOutcome::AlreadyClaimed => {
            return inspect_existing_record_after_claim(
                ctx,
                &subject_dir,
                &record_path,
                expected_uid,
            );
        }
        ClaimOutcome::Error(reason) => {
            return receipt_failure(ctx, ShadowForecastStatus::Unknown, reason);
        }
    }

    // Never overwrite a pre-existing record.json even if claim.json was absent.
    match fs::symlink_metadata(&record_path) {
        Err(err) if err.kind() == io::ErrorKind::NotFound => {}
        Ok(_) => {
            return inspect_existing_record_after_claim(
                ctx,
                &subject_dir,
                &record_path,
                expected_uid,
            );
        }
        Err(_) => {
            return receipt_failure(ctx, ShadowForecastStatus::Unknown, "record_stat_failed");
        }
    }

    if !ctx.forecast.should_trigger() {
        return persist_non_launched_record(
            ctx,
            &subject_dir,
            &record_path,
            ShadowForecastStatus::Skipped,
            "simple_task",
            None,
            expected_uid,
        );
    }

    let advisor = match resolve_profile_shadow_advisor(
        ctx.registry_snapshot,
        ctx.profile_id,
        ctx.profile_revision,
    ) {
        Ok(advisor) => advisor,
        Err(reason) => {
            return persist_non_launched_record(
                ctx,
                &subject_dir,
                &record_path,
                ShadowForecastStatus::Unavailable,
                reason,
                None,
                expected_uid,
            );
        }
    };

    if !profile_has_variable_worker_route(ctx.registry_snapshot, ctx.profile_id) {
        return persist_non_launched_record(
            ctx,
            &subject_dir,
            &record_path,
            ShadowForecastStatus::Skipped,
            "fixed_worker_route",
            Some(advisor),
            expected_uid,
        );
    }

    let advice_exe = match resolve_canonical_sibling_advice_executable() {
        Ok(exe) => exe,
        Err(reason) => {
            return persist_non_launched_record(
                ctx,
                &subject_dir,
                &record_path,
                ShadowForecastStatus::Unavailable,
                reason,
                Some(advisor),
                expected_uid,
            );
        }
    };

    let cache_dir = ctx.plans_dir.join(ADVICE_CACHE_DIR_NAME);
    let request_path = subject_dir.join(REQUEST_FILE_NAME);
    let advice_path = subject_dir.join(ADVICE_FILE_NAME);
    if ctx.registry_dir.to_str().is_none()
        || cache_dir.to_str().is_none()
        || ctx.canonical_repo.to_str().is_none()
        || request_path.to_str().is_none()
        || advice_path.to_str().is_none()
    {
        return persist_non_launched_record(
            ctx,
            &subject_dir,
            &record_path,
            ShadowForecastStatus::Unavailable,
            "invalid_scope_path",
            Some(advisor),
            expected_uid,
        );
    }

    let model_task_summary = ctx.forecast.build_model_task_summary();
    let request_summary_sha256 = sha256_hex(model_task_summary.as_bytes());
    let advice_request = ShadowAdviceRequest {
        schema_version: ADVICE_REQUEST_SCHEMA_VERSION,
        expected_registry_revision: ctx.profile_revision,
        tool_id: &advisor.tool_id,
        model_id: &advisor.model_id,
        task_id: ctx.task_id,
        task_revision: ctx.preview.request.plan_revision,
        task_summary: &model_task_summary,
        baseline_difficulty: None,
    };
    let mut request_bytes = match serde_json::to_vec(&advice_request) {
        Ok(bytes) => bytes,
        Err(_) => {
            return persist_non_launched_record(
                ctx,
                &subject_dir,
                &record_path,
                ShadowForecastStatus::Unknown,
                "request_serialize_failed",
                Some(advisor),
                expected_uid,
            );
        }
    };
    request_bytes.push(b'\n');
    if request_bytes.len() > MAX_ADVICE_REQUEST_BYTES {
        return persist_non_launched_record(
            ctx,
            &subject_dir,
            &record_path,
            ShadowForecastStatus::Unavailable,
            "request_too_large",
            Some(advisor),
            expected_uid,
        );
    }

    if create_new_private_file(&request_path, &request_bytes, expected_uid).is_err() {
        return persist_non_launched_record(
            ctx,
            &subject_dir,
            &record_path,
            ShadowForecastStatus::Unknown,
            "request_write_failed",
            Some(advisor),
            expected_uid,
        );
    }

    let advice_file = match create_new_empty_private_file(&advice_path, expected_uid) {
        Ok(file) => file,
        Err(_) => {
            let _ = fs::remove_file(&request_path);
            return persist_non_launched_record(
                ctx,
                &subject_dir,
                &record_path,
                ShadowForecastStatus::Unknown,
                "advice_file_create_failed",
                Some(advisor),
                expected_uid,
            );
        }
    };
    let _ = sync_directory(&subject_dir);

    match spawn_advice_classify_nonblocking(
        &advice_exe,
        ctx,
        &cache_dir,
        &request_path,
        advice_file,
    ) {
        Ok(pid) => {
            let record = build_record(
                ctx,
                ShadowForecastStatus::Launched,
                "shadow_classify_launched",
                Some(advisor.clone()),
                Some(request_summary_sha256),
                Some(REQUEST_FILE_NAME.to_string()),
                Some(ADVICE_FILE_NAME.to_string()),
                Some(pid),
            );
            match write_record_no_overwrite(&subject_dir, &record_path, &record, expected_uid) {
                Ok(()) => ShadowForecastReceipt {
                    advisory_only: true,
                    status: ShadowForecastStatus::Launched,
                    reason: "shadow_classify_launched".to_string(),
                    triggered: true,
                    reused: false,
                    launched: true,
                    subject_digest: ctx.preview.subject_digest.clone(),
                    baseline_difficulty: ctx.baseline_difficulty,
                    summary_sha256: ctx.forecast.summary_sha256.clone(),
                    advisor: Some(advisor),
                    record_path: Some(record_path),
                    advice_path: Some(advice_path),
                },
                Err(_) => ShadowForecastReceipt {
                    advisory_only: true,
                    status: ShadowForecastStatus::Unknown,
                    reason: "record_persist_failed".to_string(),
                    triggered: true,
                    reused: false,
                    launched: true,
                    subject_digest: ctx.preview.subject_digest.clone(),
                    baseline_difficulty: ctx.baseline_difficulty,
                    summary_sha256: ctx.forecast.summary_sha256.clone(),
                    advisor: Some(advisor),
                    record_path: None,
                    advice_path: Some(advice_path),
                },
            }
        }
        Err((status, reason)) => {
            let _ = fs::remove_file(&advice_path);
            let _ = fs::remove_file(&request_path);
            persist_non_launched_record(
                ctx,
                &subject_dir,
                &record_path,
                status,
                reason,
                Some(advisor),
                expected_uid,
            )
        }
    }
}

#[cfg(unix)]
fn resolve_profile_shadow_advisor(
    snapshot: &RegistrySnapshot,
    profile_id: &str,
    expected_revision: u64,
) -> Result<ShadowAdvisorSelection, &'static str> {
    if expected_revision == 0
        || expected_revision > MAX_JS_SAFE_INTEGER
        || snapshot.revision != expected_revision
    {
        return Err("stale_registry_revision");
    }

    let profile = snapshot
        .profiles
        .iter()
        .find(|candidate| candidate.id == profile_id)
        .ok_or("profile_not_found")?;

    let advisor = profile
        .decision_advisor
        .as_ref()
        .ok_or("advisor_not_configured")?;

    if advisor.mode != DecisionAdvisorMode::Shadow {
        return Err("advisor_unsupported_mode");
    }

    let model = snapshot
        .models
        .iter()
        .find(|candidate| candidate.id == advisor.model_id)
        .ok_or("advisor_model_not_found")?;

    if !model.allow_no_effort {
        return Err("advisor_invalid_model");
    }

    let tool = snapshot
        .tools
        .iter()
        .find(|candidate| candidate.id == model.tool_id)
        .ok_or("advisor_tool_not_found")?;

    if !matches!(
        tool.adapter,
        ToolAdapter::JevApi | ToolAdapter::OpenaiDecisionsApi
    ) {
        return Err("advisor_unsupported_adapter");
    }

    if !tool.enabled {
        return Err("advisor_tool_disabled");
    }

    if !is_supported_model_for_adapter(tool.adapter, &model.model_arg) {
        return Err("advisor_invalid_model");
    }

    Ok(ShadowAdvisorSelection {
        tool_id: tool.id.clone(),
        model_id: model.id.clone(),
        model_arg: model.model_arg.clone(),
        adapter: tool.adapter,
    })
}

#[cfg(unix)]
fn profile_has_variable_worker_route(snapshot: &RegistrySnapshot, profile_id: &str) -> bool {
    use threadlane_workers::{spec::resolve_model_target, WorkerProvider};

    let mut targets = Difficulty::ALL.into_iter().filter_map(|difficulty| {
        let choice =
            resolve_choice(snapshot, profile_id, WorkflowRole::Coder, Some(difficulty)).ok()?;
        let model = snapshot
            .models
            .iter()
            .find(|model| model.id == choice.model_id)?;
        let tool = snapshot
            .tools
            .iter()
            .find(|tool| tool.enabled && tool.id == model.tool_id)?;
        let provider = match tool.adapter {
            ToolAdapter::Antigravity => WorkerProvider::Antigravity,
            ToolAdapter::OpencodeV2 => WorkerProvider::OpenCodeV2,
            ToolAdapter::CodexCli => WorkerProvider::CodexCli,
            _ => return None,
        };
        let target =
            resolve_model_target(provider, &model.model_arg, choice.effort.as_deref()).ok()?;
        Some((&tool.id, target))
    });
    let Some(first) = targets.next() else {
        return false;
    };
    targets.any(|target| target != first)
}

#[cfg(unix)]
fn is_valid_model_token(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_IDENTIFIER_BYTES
        && value
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.' | ':'))
}

#[cfg(unix)]
fn is_supported_model_for_adapter(adapter: ToolAdapter, model_arg: &str) -> bool {
    if model_arg.trim() != model_arg || !is_valid_model_token(model_arg) {
        return false;
    }
    match adapter {
        ToolAdapter::JevApi => model_arg.starts_with("jev-") && model_arg.len() > 4,
        ToolAdapter::OpenaiDecisionsApi => {
            model_arg == DEFAULT_OPENAI_DECISION_MODEL || model_arg.starts_with("gpt-6-luna-")
        }
        _ => false,
    }
}

#[cfg(unix)]
fn resolve_canonical_sibling_advice_executable() -> Result<PathBuf, &'static str> {
    let current_exe = std::env::current_exe().map_err(|_| "advice_binary_unavailable")?;
    resolve_canonical_sibling_advice_executable_from(&current_exe)
}

#[cfg(unix)]
fn resolve_canonical_sibling_advice_executable_from(
    current_exe: &Path,
) -> Result<PathBuf, &'static str> {
    if current_exe.as_os_str().is_empty() || !current_exe.is_absolute() {
        return Err("advice_binary_unavailable");
    }
    let canonical_self = fs::canonicalize(current_exe).map_err(|_| "advice_binary_unavailable")?;
    let clean_self = validate_absolute_no_symlink_components(&canonical_self, "current_exe")
        .map_err(|_| "advice_binary_unavailable")?;
    let self_meta = fs::symlink_metadata(&clean_self).map_err(|_| "advice_binary_unavailable")?;
    if self_meta.file_type().is_symlink() || !self_meta.file_type().is_file() {
        return Err("advice_binary_unavailable");
    }
    let bin_dir = clean_self.parent().ok_or("advice_binary_unavailable")?;
    let candidate = bin_dir.join(ADVICE_BINARY_NAME);
    validate_canonical_sibling_binary(&candidate, bin_dir)
}

#[cfg(unix)]
fn validate_canonical_sibling_binary(
    candidate: &Path,
    expected_parent: &Path,
) -> Result<PathBuf, &'static str> {
    use std::os::unix::fs::PermissionsExt;

    let meta = fs::symlink_metadata(candidate).map_err(|_| "advice_binary_unavailable")?;
    if meta.file_type().is_symlink() || !meta.file_type().is_file() {
        return Err("advice_binary_unsafe");
    }
    let mode = meta.permissions().mode();
    if (mode & 0o022) != 0 || (mode & 0o111) == 0 {
        return Err("advice_binary_unsafe");
    }
    let clean = validate_absolute_no_symlink_components(candidate, ADVICE_BINARY_NAME)
        .map_err(|_| "advice_binary_unsafe")?;
    let canonical = fs::canonicalize(&clean).map_err(|_| "advice_binary_unsafe")?;
    if canonical != clean || canonical.parent() != Some(expected_parent) {
        return Err("advice_binary_unsafe");
    }
    Ok(canonical)
}

#[cfg(unix)]
fn spawn_advice_classify_nonblocking(
    advice_exe: &Path,
    ctx: &BeforeDispatchForecastContext<'_>,
    cache_dir: &Path,
    request_path: &Path,
    advice_file: File,
) -> Result<u32, (ShadowForecastStatus, &'static str)> {
    use std::os::unix::process::CommandExt;

    let mut cmd = std::process::Command::new(advice_exe);
    cmd.arg("--registry-dir")
        .arg(ctx.registry_dir)
        .arg("--cache-dir")
        .arg(cache_dir)
        .arg("--project-root")
        .arg(ctx.canonical_repo)
        .arg("--context-id")
        .arg(&ctx.preview.request.thread_uuid)
        .arg("--profile-id")
        .arg(ctx.profile_id)
        .arg("classify")
        .arg("--request")
        .arg(request_path)
        .stdin(Stdio::null())
        .stdout(Stdio::from(advice_file))
        .stderr(Stdio::null())
        .process_group(0);

    match cmd.spawn() {
        Ok(child) => {
            let pid = child.id();
            drop(child);
            Ok(pid)
        }
        Err(err) => {
            let status = match err.kind() {
                io::ErrorKind::NotFound | io::ErrorKind::PermissionDenied => {
                    ShadowForecastStatus::Unavailable
                }
                _ => ShadowForecastStatus::Unknown,
            };
            Err((status, "advice_spawn_failed"))
        }
    }
}

#[cfg(unix)]
#[allow(clippy::too_many_arguments)]
fn build_record(
    ctx: &BeforeDispatchForecastContext<'_>,
    status: ShadowForecastStatus,
    reason: &'static str,
    advisor: Option<ShadowAdvisorSelection>,
    request_summary_sha256: Option<String>,
    request_file: Option<String>,
    advice_file: Option<String>,
    pid_hint: Option<u32>,
) -> PersistedForecastRecord {
    PersistedForecastRecord {
        schema_version: FORECAST_RECORD_SCHEMA_VERSION,
        advisory_only: true,
        mode: FORECAST_MODE_SHADOW.to_string(),
        binding_id: ctx.preview.request.binding_id.clone(),
        thread_uuid: ctx.preview.request.thread_uuid.clone(),
        plan_id: ctx.preview.request.plan_id.clone(),
        plan_revision: ctx.preview.request.plan_revision,
        task_id: ctx.task_id.to_string(),
        profile_id: ctx.profile_id.to_string(),
        profile_revision: ctx.profile_revision,
        base_commit: ctx.preview.request.base_commit.clone(),
        subject_digest: ctx.preview.subject_digest.clone(),
        baseline_difficulty: ctx.baseline_difficulty,
        complexity: ctx.forecast.complexity(),
        summary_sha256: ctx.forecast.summary_sha256.clone(),
        triggered: ctx.forecast.should_trigger(),
        status,
        reason: reason.to_string(),
        advisor,
        request_summary_sha256,
        request_file,
        advice_file,
        pid_hint,
        recorded_at_ms: current_unix_ms(),
    }
}

#[cfg(unix)]
fn persist_non_launched_record(
    ctx: &BeforeDispatchForecastContext<'_>,
    subject_dir: &Path,
    record_path: &Path,
    status: ShadowForecastStatus,
    reason: &'static str,
    advisor: Option<ShadowAdvisorSelection>,
    expected_uid: u32,
) -> ShadowForecastReceipt {
    let record = build_record(ctx, status, reason, advisor.clone(), None, None, None, None);
    match write_record_no_overwrite(subject_dir, record_path, &record, expected_uid) {
        Ok(()) => ShadowForecastReceipt {
            advisory_only: true,
            status,
            reason: reason.to_string(),
            triggered: ctx.forecast.should_trigger(),
            reused: false,
            launched: false,
            subject_digest: ctx.preview.subject_digest.clone(),
            baseline_difficulty: ctx.baseline_difficulty,
            summary_sha256: ctx.forecast.summary_sha256.clone(),
            advisor,
            record_path: Some(record_path.to_path_buf()),
            advice_path: None,
        },
        Err(_) => ShadowForecastReceipt {
            advisory_only: true,
            status: ShadowForecastStatus::Unknown,
            reason: "record_persist_failed".to_string(),
            triggered: ctx.forecast.should_trigger(),
            reused: false,
            launched: false,
            subject_digest: ctx.preview.subject_digest.clone(),
            baseline_difficulty: ctx.baseline_difficulty,
            summary_sha256: ctx.forecast.summary_sha256.clone(),
            advisor,
            record_path: None,
            advice_path: None,
        },
    }
}

fn receipt_failure(
    ctx: &BeforeDispatchForecastContext<'_>,
    status: ShadowForecastStatus,
    reason: &'static str,
) -> ShadowForecastReceipt {
    ShadowForecastReceipt {
        advisory_only: true,
        status,
        reason: reason.to_string(),
        triggered: ctx.forecast.should_trigger(),
        reused: false,
        launched: false,
        subject_digest: ctx.preview.subject_digest.clone(),
        baseline_difficulty: ctx.baseline_difficulty,
        summary_sha256: ctx.forecast.summary_sha256.clone(),
        advisor: None,
        record_path: None,
        advice_path: None,
    }
}

#[cfg(unix)]
enum ClaimOutcome {
    Acquired,
    AlreadyClaimed,
    Error(&'static str),
}

#[cfg(unix)]
fn acquire_exclusive_claim(
    claim_path: &Path,
    subject_dir: &Path,
    subject_digest: &str,
    expected_uid: u32,
) -> ClaimOutcome {
    use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};

    let marker = PersistedClaimMarker {
        schema_version: FORECAST_CLAIM_SCHEMA_VERSION,
        subject_digest,
        pid: std::process::id(),
        claimed_at_ms: current_unix_ms(),
    };
    let Ok(mut bytes) = serde_json::to_vec(&marker) else {
        return ClaimOutcome::Error("claim_serialize_failed");
    };
    bytes.push(b'\n');

    let mut file = match OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(claim_path)
    {
        Ok(file) => file,
        Err(err) if err.kind() == io::ErrorKind::AlreadyExists => {
            return ClaimOutcome::AlreadyClaimed;
        }
        Err(_) => return ClaimOutcome::Error("claim_create_failed"),
    };

    if file
        .set_permissions(fs::Permissions::from_mode(0o600))
        .is_err()
        || file.write_all(&bytes).is_err()
        || file.sync_all().is_err()
    {
        return ClaimOutcome::Error("claim_write_failed");
    }
    drop(file);

    if sync_directory(subject_dir).is_err()
        || verify_private_file_after_write(claim_path, expected_uid).is_err()
    {
        return ClaimOutcome::Error("claim_verify_failed");
    }

    ClaimOutcome::Acquired
}

#[cfg(unix)]
fn inspect_existing_record_after_claim(
    ctx: &BeforeDispatchForecastContext<'_>,
    subject_dir: &Path,
    record_path: &Path,
    expected_uid: u32,
) -> ShadowForecastReceipt {
    match read_and_validate_existing_record(ctx, record_path, expected_uid) {
        Ok(Some(existing)) => {
            let advice_path = existing
                .advice_file
                .as_deref()
                .filter(|name| *name == ADVICE_FILE_NAME)
                .map(|name| subject_dir.join(name));
            ShadowForecastReceipt {
                advisory_only: true,
                status: existing.status,
                reason: existing.reason,
                triggered: existing.triggered,
                reused: true,
                launched: false,
                subject_digest: existing.subject_digest,
                baseline_difficulty: existing.baseline_difficulty,
                summary_sha256: existing.summary_sha256,
                advisor: existing.advisor,
                record_path: Some(record_path.to_path_buf()),
                advice_path,
            }
        }
        Ok(None) => ShadowForecastReceipt {
            advisory_only: true,
            status: ShadowForecastStatus::Unknown,
            reason: "claim_exists".to_string(),
            triggered: ctx.forecast.should_trigger(),
            reused: true,
            launched: false,
            subject_digest: ctx.preview.subject_digest.clone(),
            baseline_difficulty: ctx.baseline_difficulty,
            summary_sha256: ctx.forecast.summary_sha256.clone(),
            advisor: None,
            record_path: None,
            advice_path: None,
        },
        Err(reason) => ShadowForecastReceipt {
            advisory_only: true,
            status: ShadowForecastStatus::Unknown,
            reason: reason.to_string(),
            triggered: ctx.forecast.should_trigger(),
            reused: true,
            launched: false,
            subject_digest: ctx.preview.subject_digest.clone(),
            baseline_difficulty: ctx.baseline_difficulty,
            summary_sha256: ctx.forecast.summary_sha256.clone(),
            advisor: None,
            record_path: None,
            advice_path: None,
        },
    }
}

#[cfg(unix)]
fn read_and_validate_existing_record(
    ctx: &BeforeDispatchForecastContext<'_>,
    record_path: &Path,
    expected_uid: u32,
) -> Result<Option<PersistedForecastRecord>, &'static str> {
    match fs::symlink_metadata(record_path) {
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err("record_unreadable"),
        Ok(_) => {}
    }

    let bytes =
        read_bounded_private_file_exact(record_path, MAX_FORECAST_RECORD_BYTES, expected_uid)?;
    let record: PersistedForecastRecord =
        serde_json::from_slice(&bytes).map_err(|_| "record_corrupt")?;

    let scope_matches = record.schema_version == FORECAST_RECORD_SCHEMA_VERSION
        && record.advisory_only
        && record.mode == FORECAST_MODE_SHADOW
        && record.binding_id == ctx.preview.request.binding_id
        && record.thread_uuid == ctx.preview.request.thread_uuid
        && record.plan_id == ctx.preview.request.plan_id
        && record.plan_revision == ctx.preview.request.plan_revision
        && record.task_id == ctx.task_id
        && record.profile_id == ctx.profile_id
        && record.profile_revision == ctx.profile_revision
        && record.base_commit == ctx.preview.request.base_commit
        && record.subject_digest == ctx.preview.subject_digest
        && record.baseline_difficulty == ctx.baseline_difficulty
        && (MIN_FORECAST_COMPONENT_COUNT..=MAX_FORECAST_COMPONENT_COUNT)
            .contains(&record.complexity.component_count)
        && validate_sha256_hex("summary_sha256", &record.summary_sha256).is_ok();

    if !scope_matches {
        return Err("record_scope_mismatch");
    }

    Ok(Some(record))
}

#[cfg(unix)]
fn write_record_no_overwrite(
    subject_dir: &Path,
    record_path: &Path,
    record: &PersistedForecastRecord,
    expected_uid: u32,
) -> Result<(), &'static str> {
    use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};

    let mut bytes = serde_json::to_vec(record).map_err(|_| "record_serialize_failed")?;
    bytes.push(b'\n');
    if bytes.len() > MAX_FORECAST_RECORD_BYTES {
        return Err("record_too_large");
    }

    let tmp_name = format!(".{RECORD_FILE_NAME}.tmp.{}", Uuid::new_v4().simple());
    let tmp_path = subject_dir.join(tmp_name);
    let link_res = (|| -> Result<(), &'static str> {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(&tmp_path)
            .map_err(|_| "record_create_failed")?;
        file.set_permissions(fs::Permissions::from_mode(0o600))
            .map_err(|_| "record_chmod_failed")?;
        file.write_all(&bytes).map_err(|_| "record_write_failed")?;
        file.sync_all().map_err(|_| "record_sync_failed")?;
        drop(file);

        fs::hard_link(&tmp_path, record_path).map_err(|err| {
            if err.kind() == io::ErrorKind::AlreadyExists {
                "record_already_exists"
            } else {
                "record_link_failed"
            }
        })?;
        Ok(())
    })();
    let _ = fs::remove_file(&tmp_path);
    link_res?;
    sync_directory(subject_dir).map_err(|_| "record_dir_sync_failed")?;
    verify_private_file_after_write(record_path, expected_uid)
}

#[cfg(unix)]
fn create_new_private_file(
    path: &Path,
    bytes: &[u8],
    expected_uid: u32,
) -> Result<(), &'static str> {
    use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};

    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(path)
        .map_err(|_| "file_create_failed")?;
    file.set_permissions(fs::Permissions::from_mode(0o600))
        .map_err(|_| "file_chmod_failed")?;
    file.write_all(bytes).map_err(|_| "file_write_failed")?;
    file.sync_all().map_err(|_| "file_sync_failed")?;
    drop(file);
    verify_private_file_after_write(path, expected_uid)
}

#[cfg(unix)]
fn create_new_empty_private_file(path: &Path, expected_uid: u32) -> Result<File, &'static str> {
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};

    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(path)
        .map_err(|_| "file_create_failed")?;
    file.set_permissions(fs::Permissions::from_mode(0o600))
        .map_err(|_| "file_chmod_failed")?;
    let meta = file.metadata().map_err(|_| "file_stat_failed")?;
    if !meta.file_type().is_file()
        || meta.uid() != expected_uid
        || (meta.permissions().mode() & 0o777) != 0o600
        || meta.nlink() != 1
    {
        return Err("file_verify_failed");
    }
    Ok(file)
}

#[cfg(unix)]
fn verify_private_file_after_write(path: &Path, expected_uid: u32) -> Result<(), &'static str> {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    let meta = fs::symlink_metadata(path).map_err(|_| "file_stat_failed")?;
    if meta.file_type().is_symlink()
        || !meta.file_type().is_file()
        || meta.uid() != expected_uid
        || (meta.permissions().mode() & 0o777) != 0o600
        || meta.nlink() != 1
    {
        return Err("file_verify_failed");
    }
    Ok(())
}

#[cfg(unix)]
fn read_bounded_private_file_exact(
    path: &Path,
    max_bytes: usize,
    expected_uid: u32,
) -> Result<Vec<u8>, &'static str> {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    let lstat = fs::symlink_metadata(path).map_err(|_| "record_unreadable")?;
    if lstat.file_type().is_symlink()
        || !lstat.file_type().is_file()
        || lstat.uid() != expected_uid
        || (lstat.permissions().mode() & 0o777) != 0o600
        || lstat.nlink() != 1
    {
        return Err("record_security_violation");
    }
    if lstat.len() > max_bytes as u64 {
        return Err("record_too_large");
    }

    let handle = open_nofollow_rdonly(path).map_err(|_| "record_security_violation")?;
    let before = handle.metadata().map_err(|_| "record_unreadable")?;
    if !before.is_file()
        || before.dev() != lstat.dev()
        || before.ino() != lstat.ino()
        || before.uid() != expected_uid
        || (before.permissions().mode() & 0o777) != 0o600
        || before.nlink() != 1
        || before.len() != lstat.len()
    {
        return Err("record_security_violation");
    }

    let mut bytes = Vec::with_capacity(before.len() as usize);
    (&handle)
        .take((max_bytes as u64).saturating_add(1))
        .read_to_end(&mut bytes)
        .map_err(|_| "record_unreadable")?;
    if bytes.len() > max_bytes {
        return Err("record_too_large");
    }

    let after = handle.metadata().map_err(|_| "record_unreadable")?;
    if !after.is_file()
        || after.dev() != before.dev()
        || after.ino() != before.ino()
        || after.uid() != expected_uid
        || (after.permissions().mode() & 0o777) != 0o600
        || after.nlink() != 1
        || after.len() != before.len()
        || after.mtime() != before.mtime()
        || after.mtime_nsec() != before.mtime_nsec()
        || after.ctime() != before.ctime()
        || after.ctime_nsec() != before.ctime_nsec()
    {
        return Err("record_security_violation");
    }

    Ok(bytes)
}

#[cfg(unix)]
fn prepare_subject_dir(subject_dir: &Path, expected_uid: u32) -> Result<(), &'static str> {
    use std::os::unix::fs::DirBuilderExt;

    match fs::symlink_metadata(subject_dir) {
        Ok(_) => validate_existing_private_dir(subject_dir, expected_uid)
            .map_err(|_| "subject_dir_invalid"),
        Err(err) if err.kind() == io::ErrorKind::NotFound => {
            let mut builder = fs::DirBuilder::new();
            builder.mode(0o700);
            match builder.create(subject_dir) {
                Err(create_err) if create_err.kind() != io::ErrorKind::AlreadyExists => {
                    return Err("subject_dir_create_failed");
                }
                Ok(()) | Err(_) => {}
            }
            validate_existing_private_dir(subject_dir, expected_uid)
                .map_err(|_| "subject_dir_invalid")
        }
        Err(_) => Err("subject_dir_invalid"),
    }
}

#[cfg(unix)]
fn ensure_private_dir(dir: &Path, expected_uid: u32) -> Result<(), &'static str> {
    use std::os::unix::fs::DirBuilderExt;

    match fs::symlink_metadata(dir) {
        Ok(_) => validate_existing_private_dir(dir, expected_uid),
        Err(err) if err.kind() == io::ErrorKind::NotFound => {
            let mut builder = fs::DirBuilder::new();
            builder.mode(0o700);
            match builder.create(dir) {
                Err(create_err) if create_err.kind() != io::ErrorKind::AlreadyExists => {
                    return Err("dir_create_failed");
                }
                Ok(()) | Err(_) => {}
            }
            validate_existing_private_dir(dir, expected_uid)
        }
        Err(_) => Err("dir_stat_failed"),
    }
}

#[cfg(unix)]
fn validate_existing_private_dir(dir: &Path, expected_uid: u32) -> Result<(), &'static str> {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    let clean = validate_absolute_no_symlink_components(dir, "ordinary forecast directory")
        .map_err(|_| "dir_symlink_violation")?;
    let lstat = fs::symlink_metadata(&clean).map_err(|_| "dir_stat_failed")?;
    if lstat.file_type().is_symlink()
        || !lstat.file_type().is_dir()
        || lstat.uid() != expected_uid
        || (lstat.permissions().mode() & 0o777) != 0o700
    {
        return Err("dir_security_violation");
    }

    let handle = open_nofollow_rdonly(&clean).map_err(|_| "dir_security_violation")?;
    let fstat = handle.metadata().map_err(|_| "dir_stat_failed")?;
    if !fstat.is_dir()
        || fstat.dev() != lstat.dev()
        || fstat.ino() != lstat.ino()
        || fstat.uid() != expected_uid
        || (fstat.permissions().mode() & 0o777) != 0o700
    {
        return Err("dir_security_violation");
    }
    Ok(())
}

#[cfg(unix)]
fn open_nofollow_rdonly(path: &Path) -> io::Result<File> {
    use std::os::unix::fs::OpenOptionsExt;

    OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK | libc::O_CLOEXEC)
        .open(path)
}

#[cfg(unix)]
fn sync_directory(dir: &Path) -> io::Result<()> {
    let handle = open_nofollow_rdonly(dir)?;
    handle.sync_all()
}

#[cfg(unix)]
fn current_effective_uid() -> u32 {
    rustix::process::geteuid().as_raw()
}

#[cfg(unix)]
fn current_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |dur| {
            u64::try_from(dur.as_millis())
                .unwrap_or(u64::MAX)
                .min(MAX_JS_SAFE_INTEGER)
        })
}

fn sha256_hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;

    let digest = Sha256::digest(bytes);
    let mut out = String::with_capacity(64);
    for byte in digest {
        let _ = write!(&mut out, "{byte:02x}");
    }
    out
}

#[cfg(all(test, unix))]
mod tests {
    use std::os::unix::fs::{symlink, PermissionsExt};

    use tempfile::TempDir;
    use threadlane_authority::OperatorApprovalRequest;
    use threadlane_workers::ExecutionBudgets;
    use threadlane_workflow::{
        DecisionAdvisor, ModelChoice, ModelDefinition, RoleAssignment, ToolDefinition,
        WorkflowProfile,
    };

    use super::*;

    fn sample_input(
        component_count: u32,
        coupled: bool,
        uncertain: bool,
        summary: &str,
    ) -> ForecastInput {
        ForecastInput {
            component_count,
            coupled,
            uncertain,
            summary: summary.to_string(),
        }
    }

    #[test]
    fn forecast_validation_enforces_bounds_sanitization_and_redacts_debug() {
        let repo = Path::new("/tmp/private-checkout/my-repo");

        let exact_512 = "é".repeat(256);
        assert_eq!(exact_512.len(), 512);
        let ok = ValidatedForecast::validate(sample_input(1, false, false, &exact_512), repo)
            .expect("exact 512 UTF-8 byte summary must succeed");
        assert!(!ok.should_trigger());
        assert_eq!(ok.summary_sha256, sha256_hex(exact_512.as_bytes()));
        assert!(!format!("{ok:?}").contains(&exact_512));

        let max_components = ValidatedForecast::validate(
            sample_input(32, false, false, "Cross-module coordination"),
            repo,
        )
        .expect("component_count 32 must succeed");
        assert!(max_components.should_trigger());
        let model_prompt = max_components.build_model_task_summary();
        assert!(model_prompt.contains("Cross-module coordination"));
        assert!(model_prompt.contains("component_count=32, coupled=false, uncertain=false"));

        for bad in [
            sample_input(0, false, false, "Too few components"),
            sample_input(33, false, false, "Too many components"),
            sample_input(1, false, false, ""),
            sample_input(1, false, false, "   "),
            sample_input(1, false, false, &"a".repeat(513)),
            sample_input(1, false, false, &"é".repeat(257)),
            sample_input(1, false, false, "has\nnewline"),
            sample_input(1, false, false, "has\0nul"),
            sample_input(1, false, false, "has\x1bescape"),
            sample_input(
                1,
                false,
                false,
                "Mentions /tmp/private-checkout/my-repo directly",
            ),
        ] {
            let err = ValidatedForecast::validate(bad, repo)
                .expect_err("invalid forecast input must fail upfront");
            assert_eq!(err.code, "invalid_input");
        }
    }

    #[test]
    fn forecast_trigger_matrix_matches_contract() {
        let repo = Path::new("/tmp/repo");
        for (components, coupled, uncertain, expected) in [
            (1, false, false, false),
            (2, false, false, true),
            (1, true, false, true),
            (1, false, true, true),
            (3, true, true, true),
        ] {
            let validated = ValidatedForecast::validate(
                sample_input(components, coupled, uncertain, "Bounded summary"),
                repo,
            )
            .expect("valid forecast");
            assert_eq!(
                validated.should_trigger(),
                expected,
                "trigger mismatch for ({components}, {coupled}, {uncertain})"
            );
        }
    }

    #[test]
    fn resolve_profile_shadow_advisor_is_exact_and_never_falls_back() {
        let base_tools = vec![
            ToolDefinition {
                id: "jev-api".to_string(),
                label: "Jev API".to_string(),
                adapter: ToolAdapter::JevApi,
                executable_path: None,
                enabled: true,
            },
            ToolDefinition {
                id: "openai-decisions".to_string(),
                label: "OpenAI Decisions".to_string(),
                adapter: ToolAdapter::OpenaiDecisionsApi,
                executable_path: None,
                enabled: true,
            },
            ToolDefinition {
                id: "jev-disabled".to_string(),
                label: "Disabled Jev".to_string(),
                adapter: ToolAdapter::JevApi,
                executable_path: None,
                enabled: false,
            },
        ];
        let base_models = vec![
            ModelDefinition {
                id: "jev-api:jev-latest".to_string(),
                tool_id: "jev-api".to_string(),
                label: "jev-latest".to_string(),
                model_arg: "jev-latest".to_string(),
                efforts: Vec::new(),
                allow_no_effort: true,
            },
            ModelDefinition {
                id: "openai-decisions:gpt-6-luna".to_string(),
                tool_id: "openai-decisions".to_string(),
                label: "gpt-6-luna".to_string(),
                model_arg: "gpt-6-luna".to_string(),
                efforts: Vec::new(),
                allow_no_effort: true,
            },
            ModelDefinition {
                id: "jev-disabled:jev-latest".to_string(),
                tool_id: "jev-disabled".to_string(),
                label: "jev-latest".to_string(),
                model_arg: "jev-latest".to_string(),
                efforts: Vec::new(),
                allow_no_effort: true,
            },
            ModelDefinition {
                id: "openai-decisions:gpt-5-nano".to_string(),
                tool_id: "openai-decisions".to_string(),
                label: "gpt-5-nano".to_string(),
                model_arg: "gpt-5-nano".to_string(),
                efforts: Vec::new(),
                allow_no_effort: true,
            },
            ModelDefinition {
                id: "jev-api:effort-required".to_string(),
                tool_id: "jev-api".to_string(),
                label: "jev-1.0".to_string(),
                model_arg: "jev-1.0".to_string(),
                efforts: vec!["high".to_string()],
                allow_no_effort: false,
            },
        ];
        let snapshot = RegistrySnapshot {
            schema_version: RegistrySnapshot::empty().schema_version,
            revision: 7,
            tools: base_tools,
            models: base_models,
            profiles: vec![
                WorkflowProfile {
                    id: "p-jev".to_string(),
                    name: "Jev Profile".to_string(),
                    assignments: Vec::new(),
                    decision_advisor: Some(DecisionAdvisor {
                        model_id: "jev-api:jev-latest".to_string(),
                        mode: DecisionAdvisorMode::Shadow,
                    }),
                },
                WorkflowProfile {
                    id: "p-openai".to_string(),
                    name: "OpenAI Profile".to_string(),
                    assignments: Vec::new(),
                    decision_advisor: Some(DecisionAdvisor {
                        model_id: "openai-decisions:gpt-6-luna".to_string(),
                        mode: DecisionAdvisorMode::Shadow,
                    }),
                },
                WorkflowProfile {
                    id: "p-off".to_string(),
                    name: "Off Profile".to_string(),
                    assignments: Vec::new(),
                    decision_advisor: None,
                },
                WorkflowProfile {
                    id: "p-disabled".to_string(),
                    name: "Disabled Tool Profile".to_string(),
                    assignments: Vec::new(),
                    decision_advisor: Some(DecisionAdvisor {
                        model_id: "jev-disabled:jev-latest".to_string(),
                        mode: DecisionAdvisorMode::Shadow,
                    }),
                },
                WorkflowProfile {
                    id: "p-bad-model".to_string(),
                    name: "Unsupported Model Profile".to_string(),
                    assignments: Vec::new(),
                    decision_advisor: Some(DecisionAdvisor {
                        model_id: "openai-decisions:gpt-5-nano".to_string(),
                        mode: DecisionAdvisorMode::Shadow,
                    }),
                },
                WorkflowProfile {
                    id: "p-effort".to_string(),
                    name: "Effort Model Profile".to_string(),
                    assignments: Vec::new(),
                    decision_advisor: Some(DecisionAdvisor {
                        model_id: "jev-api:effort-required".to_string(),
                        mode: DecisionAdvisorMode::Shadow,
                    }),
                },
            ],
            default_profile_id: Some("p-jev".to_string()),
        };

        let jev = resolve_profile_shadow_advisor(&snapshot, "p-jev", 7).expect("Jev advisor");
        assert_eq!(jev.tool_id, "jev-api");
        assert_eq!(jev.model_id, "jev-api:jev-latest");
        assert_eq!(jev.adapter, ToolAdapter::JevApi);

        let openai =
            resolve_profile_shadow_advisor(&snapshot, "p-openai", 7).expect("OpenAI advisor");
        assert_eq!(openai.tool_id, "openai-decisions");
        assert_eq!(openai.model_id, "openai-decisions:gpt-6-luna");
        assert_eq!(openai.adapter, ToolAdapter::OpenaiDecisionsApi);

        assert_eq!(
            resolve_profile_shadow_advisor(&snapshot, "p-off", 7),
            Err("advisor_not_configured")
        );
        assert_eq!(
            resolve_profile_shadow_advisor(&snapshot, "p-disabled", 7),
            Err("advisor_tool_disabled")
        );
        assert_eq!(
            resolve_profile_shadow_advisor(&snapshot, "p-bad-model", 7),
            Err("advisor_invalid_model")
        );
        assert_eq!(
            resolve_profile_shadow_advisor(&snapshot, "p-effort", 7),
            Err("advisor_invalid_model")
        );
        assert_eq!(
            resolve_profile_shadow_advisor(&snapshot, "p-jev", 6),
            Err("stale_registry_revision")
        );
        assert_eq!(
            resolve_profile_shadow_advisor(&snapshot, "p-missing", 7),
            Err("profile_not_found")
        );
    }

    #[test]
    fn canonical_sibling_advice_binary_rejects_missing_symlink_non_exec_and_writable() {
        let tmp = TempDir::new().expect("tempdir");
        let root = fs::canonicalize(tmp.path()).expect("canonicalize tempdir");
        let bin = root.join("bin");
        fs::create_dir(&bin).expect("create bin");
        let fake_approve = bin.join("threadlane-approve");
        fs::write(&fake_approve, b"#!/bin/sh\nexit 0\n").expect("write approve");
        fs::set_permissions(&fake_approve, fs::Permissions::from_mode(0o755))
            .expect("chmod approve");

        assert_eq!(
            resolve_canonical_sibling_advice_executable_from(&fake_approve),
            Err("advice_binary_unavailable")
        );

        let advice = bin.join(ADVICE_BINARY_NAME);
        let outside = root.join("outside-advice");
        fs::write(&outside, b"#!/bin/sh\nexit 0\n").expect("write outside");
        fs::set_permissions(&outside, fs::Permissions::from_mode(0o755)).expect("chmod outside");

        symlink(&outside, &advice).expect("symlink advice");
        assert_eq!(
            resolve_canonical_sibling_advice_executable_from(&fake_approve),
            Err("advice_binary_unsafe")
        );
        fs::remove_file(&advice).expect("remove symlink");

        fs::write(&advice, b"#!/bin/sh\nexit 0\n").expect("write non-exec advice");
        fs::set_permissions(&advice, fs::Permissions::from_mode(0o600)).expect("chmod 0600");
        assert_eq!(
            resolve_canonical_sibling_advice_executable_from(&fake_approve),
            Err("advice_binary_unsafe")
        );

        fs::set_permissions(&advice, fs::Permissions::from_mode(0o775)).expect("chmod 0775");
        assert_eq!(
            resolve_canonical_sibling_advice_executable_from(&fake_approve),
            Err("advice_binary_unsafe")
        );

        fs::set_permissions(&advice, fs::Permissions::from_mode(0o555)).expect("chmod 0555");
        assert_eq!(
            resolve_canonical_sibling_advice_executable_from(&fake_approve),
            Ok(advice)
        );
    }

    #[test]
    fn model_task_summary_includes_stable_rubric_and_distinguishes_coupling_and_uncertainty() {
        let repo = Path::new("/tmp/repo");
        let summary = "Coordinate updates across three modules";
        let cases = [
            (3, false, false),
            (3, true, false),
            (3, false, true),
            (3, true, true),
        ];

        let mut normalized_inputs = std::collections::HashSet::new();
        let mut digests = std::collections::HashSet::new();
        for (component_count, coupled, uncertain) in cases {
            let validated = ValidatedForecast::validate(
                sample_input(component_count, coupled, uncertain, summary),
                repo,
            )
            .expect("valid forecast");
            let model_summary = validated.build_model_task_summary();
            let normalized = model_summary
                .replace("\r\n", "\n")
                .replace('\r', "\n")
                .trim()
                .to_string();
            assert_eq!(model_summary, normalized);
            assert!(normalized.contains(summary));
            assert!(normalized.contains(&format!(
                "component_count={component_count}, coupled={coupled}, uncertain={uncertain}"
            )));
            assert!(normalized.contains(MODEL_DIFFICULTY_RUBRIC));
            for grade in ["easy", "medium", "hard", "extreme"] {
                assert!(
                    !normalized.to_ascii_lowercase().contains(grade),
                    "model task_summary must not leak difficulty label `{grade}`: {normalized}"
                );
            }
            assert!(digests.insert(sha256_hex(normalized.as_bytes())));
            assert!(normalized_inputs.insert(normalized));
        }
        assert_eq!(normalized_inputs.len(), cases.len());
        assert_eq!(digests.len(), cases.len());

        let max_summary = "é".repeat(256);
        let max_validated =
            ValidatedForecast::validate(sample_input(32, true, true, &max_summary), repo)
                .expect("max-length summary must succeed");
        let max_prompt = max_validated.build_model_task_summary();
        assert!(max_prompt.len() <= 8 * 1024);

        let request = ShadowAdviceRequest {
            schema_version: ADVICE_REQUEST_SCHEMA_VERSION,
            expected_registry_revision: 7,
            tool_id: "jev-api",
            model_id: "jev-api:jev-latest",
            task_id: "task-1",
            task_revision: 1,
            task_summary: &max_prompt,
            baseline_difficulty: None,
        };
        let encoded = serde_json::to_value(request).expect("serialize shadow advice request");
        assert!(
            encoded.get("baseline_difficulty").is_none(),
            "outgoing shadow advice request must omit baseline_difficulty"
        );
    }

    #[test]
    fn prepare_subject_dir_allows_more_than_256_historical_subject_dirs_without_deleting_history() {
        let tmp = TempDir::new().expect("tempdir");
        let root = fs::canonicalize(tmp.path()).expect("canonicalize tempdir");
        fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).expect("chmod tempdir 0700");

        let forecasts_root = root.join(ORDINARY_FORECASTS_DIR_NAME);
        let expected_uid = current_effective_uid();
        ensure_private_dir(&forecasts_root, expected_uid).expect("create private forecasts root");

        let historical_count = 260usize;
        let mut historical_records = Vec::with_capacity(historical_count);
        for idx in 0..historical_count {
            let dir = forecasts_root.join(format!("{idx:064x}"));
            prepare_subject_dir(&dir, expected_uid).expect("create historical subject dir");
            let record_path = dir.join(RECORD_FILE_NAME);
            let sentinel = format!("{{\"historical\":{idx}}}\n");
            create_new_private_file(&record_path, sentinel.as_bytes(), expected_uid)
                .expect("write historical sentinel record");
            historical_records.push((dir, record_path, sentinel));
        }

        let new_subject_dir = forecasts_root.join("f".repeat(64));
        prepare_subject_dir(&new_subject_dir, expected_uid)
            .expect("261st subject dir must succeed without global 256 cap");
        prepare_subject_dir(&new_subject_dir, expected_uid)
            .expect("existing subject dir must remain idempotent");

        let mode = fs::symlink_metadata(new_subject_dir)
            .expect("stat new subject dir")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o700);

        for (dir, record_path, sentinel) in historical_records {
            assert!(dir.is_dir(), "historical dir must never be deleted");
            assert_eq!(
                fs::read_to_string(record_path).expect("read preserved historical record"),
                sentinel
            );
        }

        let insecure_dir = forecasts_root.join("e".repeat(64));
        fs::create_dir(&insecure_dir).expect("create insecure subject dir");
        fs::set_permissions(&insecure_dir, fs::Permissions::from_mode(0o755))
            .expect("chmod insecure subject dir 0755");
        assert_eq!(
            prepare_subject_dir(&insecure_dir, expected_uid),
            Err("subject_dir_invalid")
        );

        let outside = root.join("outside-subject");
        fs::create_dir(&outside).expect("create outside dir");
        fs::set_permissions(&outside, fs::Permissions::from_mode(0o700))
            .expect("chmod outside 0700");
        let symlink_dir = forecasts_root.join("d".repeat(64));
        symlink(&outside, &symlink_dir).expect("symlink subject dir");
        assert_eq!(
            prepare_subject_dir(&symlink_dir, expected_uid),
            Err("subject_dir_invalid")
        );
    }

    fn routing_snapshot(assignments: Vec<RoleAssignment>) -> RegistrySnapshot {
        RegistrySnapshot {
            schema_version: RegistrySnapshot::empty().schema_version,
            revision: 7,
            tools: vec![
                ToolDefinition {
                    id: "tool-oc".to_string(),
                    label: "OpenCode".to_string(),
                    adapter: ToolAdapter::OpencodeV2,
                    executable_path: Some(PathBuf::from("/usr/bin/opencode")),
                    enabled: true,
                },
                ToolDefinition {
                    id: "tool-agy".to_string(),
                    label: "Antigravity".to_string(),
                    adapter: ToolAdapter::Antigravity,
                    executable_path: Some(PathBuf::from("/usr/bin/agy")),
                    enabled: true,
                },
                ToolDefinition {
                    id: "tool-codex".to_string(),
                    label: "Codex".to_string(),
                    adapter: ToolAdapter::CodexCli,
                    executable_path: Some(PathBuf::from("/usr/bin/codex")),
                    enabled: true,
                },
                ToolDefinition {
                    id: "tool-oc-disabled".to_string(),
                    label: "Disabled OpenCode".to_string(),
                    adapter: ToolAdapter::OpencodeV2,
                    executable_path: Some(PathBuf::from("/usr/bin/opencode")),
                    enabled: false,
                },
                ToolDefinition {
                    id: "jev-api".to_string(),
                    label: "Jev API".to_string(),
                    adapter: ToolAdapter::JevApi,
                    executable_path: None,
                    enabled: true,
                },
                ToolDefinition {
                    id: "git-cli".to_string(),
                    label: "Git".to_string(),
                    adapter: ToolAdapter::Git,
                    executable_path: Some(PathBuf::from("/usr/bin/git")),
                    enabled: true,
                },
            ],
            models: vec![
                ModelDefinition {
                    id: "oc-flash".to_string(),
                    tool_id: "tool-oc".to_string(),
                    label: "Flash".to_string(),
                    model_arg: "opencode-go/deepseek-v4.1-flash".to_string(),
                    efforts: vec!["medium".to_string(), "max".to_string()],
                    allow_no_effort: false,
                },
                ModelDefinition {
                    id: "oc-flash-alias".to_string(),
                    tool_id: "tool-oc".to_string(),
                    label: "Flash Alias".to_string(),
                    model_arg: "opencode-go/deepseek-v4.1-flash".to_string(),
                    efforts: vec!["max".to_string()],
                    allow_no_effort: false,
                },
                ModelDefinition {
                    id: "oc-flash-inline-alias".to_string(),
                    tool_id: "tool-oc".to_string(),
                    label: "Flash Inline".to_string(),
                    model_arg: "opencode-go/deepseek-v4.1-flash#max".to_string(),
                    efforts: Vec::new(),
                    allow_no_effort: true,
                },
                ModelDefinition {
                    id: "oc-pro".to_string(),
                    tool_id: "tool-oc".to_string(),
                    label: "Pro".to_string(),
                    model_arg: "opencode-go/deepseek-v4.1-pro".to_string(),
                    efforts: vec!["max".to_string()],
                    allow_no_effort: false,
                },
                ModelDefinition {
                    id: "agy-argon".to_string(),
                    tool_id: "tool-agy".to_string(),
                    label: "Argon".to_string(),
                    model_arg: "gemini-4-argon-high".to_string(),
                    efforts: vec!["medium".to_string(), "high".to_string()],
                    allow_no_effort: false,
                },
                ModelDefinition {
                    id: "codex-worker".to_string(),
                    tool_id: "tool-codex".to_string(),
                    label: "Codex".to_string(),
                    model_arg: "gpt-6-astra".to_string(),
                    efforts: vec!["high".to_string()],
                    allow_no_effort: false,
                },
                ModelDefinition {
                    id: "oc-disabled-model".to_string(),
                    tool_id: "tool-oc-disabled".to_string(),
                    label: "Disabled".to_string(),
                    model_arg: "opencode-go/deepseek-v4.1-pro".to_string(),
                    efforts: vec!["max".to_string()],
                    allow_no_effort: false,
                },
                ModelDefinition {
                    id: "jev-api:jev-latest".to_string(),
                    tool_id: "jev-api".to_string(),
                    label: "Jev".to_string(),
                    model_arg: "jev-latest".to_string(),
                    efforts: Vec::new(),
                    allow_no_effort: true,
                },
                ModelDefinition {
                    id: "git-helper".to_string(),
                    tool_id: "git-cli".to_string(),
                    label: "Git".to_string(),
                    model_arg: "git".to_string(),
                    efforts: Vec::new(),
                    allow_no_effort: true,
                },
            ],
            profiles: vec![WorkflowProfile {
                id: "p-test".to_string(),
                name: "Test Profile".to_string(),
                assignments,
                decision_advisor: Some(DecisionAdvisor {
                    model_id: "jev-api:jev-latest".to_string(),
                    mode: DecisionAdvisorMode::Shadow,
                }),
            }],
            default_profile_id: Some("p-test".to_string()),
        }
    }

    fn coder_assignment(
        difficulty: Option<Difficulty>,
        model_id: &str,
        effort: Option<&str>,
    ) -> RoleAssignment {
        RoleAssignment {
            role: WorkflowRole::Coder,
            difficulty,
            choice: ModelChoice {
                model_id: model_id.to_string(),
                effort: effort.map(str::to_string),
            },
        }
    }

    #[test]
    fn worker_routing_variation_handles_none_default_aliases_other_roles_and_effort_diffs() {
        // 1. None: empty assignments, only non-Coder roles (even varying), or unusable Coder tools.
        assert!(!profile_has_variable_worker_route(
            &routing_snapshot(Vec::new()),
            "p-test"
        ));
        assert!(!profile_has_variable_worker_route(
            &routing_snapshot(vec![
                RoleAssignment {
                    role: WorkflowRole::Reviewer,
                    difficulty: Some(Difficulty::Easy),
                    choice: ModelChoice {
                        model_id: "oc-flash".to_string(),
                        effort: Some("max".to_string()),
                    },
                },
                RoleAssignment {
                    role: WorkflowRole::Reviewer,
                    difficulty: Some(Difficulty::Hard),
                    choice: ModelChoice {
                        model_id: "agy-argon".to_string(),
                        effort: Some("high".to_string()),
                    },
                },
                RoleAssignment {
                    role: WorkflowRole::Advisor,
                    difficulty: Some(Difficulty::Easy),
                    choice: ModelChoice {
                        model_id: "oc-pro".to_string(),
                        effort: Some("max".to_string()),
                    },
                },
                RoleAssignment {
                    role: WorkflowRole::Closure,
                    difficulty: Some(Difficulty::Extreme),
                    choice: ModelChoice {
                        model_id: "git-helper".to_string(),
                        effort: None,
                    },
                },
            ]),
            "p-test"
        ));
        assert!(!profile_has_variable_worker_route(
            &routing_snapshot(vec![
                coder_assignment(Some(Difficulty::Easy), "oc-disabled-model", Some("max")),
                coder_assignment(Some(Difficulty::Hard), "jev-api:jev-latest", None),
                coder_assignment(Some(Difficulty::Extreme), "git-helper", None),
            ]),
            "p-test"
        ));

        // 2. Default only: Coder default alone (even with varying non-Coder roles) must not trigger.
        assert!(!profile_has_variable_worker_route(
            &routing_snapshot(vec![
                coder_assignment(None, "oc-flash", Some("max")),
                RoleAssignment {
                    role: WorkflowRole::Reviewer,
                    difficulty: Some(Difficulty::Easy),
                    choice: ModelChoice {
                        model_id: "oc-flash".to_string(),
                        effort: Some("max".to_string()),
                    },
                },
                RoleAssignment {
                    role: WorkflowRole::Reviewer,
                    difficulty: Some(Difficulty::Hard),
                    choice: ModelChoice {
                        model_id: "agy-argon".to_string(),
                        effort: Some("high".to_string()),
                    },
                },
            ]),
            "p-test"
        ));

        // 3. Single exact tier only (or one usable + one disabled/non-worker tier) must not trigger.
        assert!(!profile_has_variable_worker_route(
            &routing_snapshot(vec![coder_assignment(
                Some(Difficulty::Easy),
                "oc-flash",
                Some("max")
            )]),
            "p-test"
        ));
        assert!(!profile_has_variable_worker_route(
            &routing_snapshot(vec![
                coder_assignment(Some(Difficulty::Easy), "oc-flash", Some("max")),
                coder_assignment(Some(Difficulty::Hard), "oc-disabled-model", Some("max")),
                coder_assignment(Some(Difficulty::Extreme), "jev-api:jev-latest", None),
            ]),
            "p-test"
        ));

        // 4. Identical tiers and aliases selecting identical execution targets must not trigger.
        assert!(!profile_has_variable_worker_route(
            &routing_snapshot(vec![
                coder_assignment(Some(Difficulty::Easy), "oc-flash", Some("max")),
                coder_assignment(Some(Difficulty::Medium), "oc-flash", Some("max")),
                coder_assignment(Some(Difficulty::Hard), "oc-flash", Some("max")),
                coder_assignment(Some(Difficulty::Extreme), "oc-flash", Some("max")),
            ]),
            "p-test"
        ));
        assert!(!profile_has_variable_worker_route(
            &routing_snapshot(vec![
                coder_assignment(Some(Difficulty::Easy), "oc-flash", Some("max")),
                coder_assignment(Some(Difficulty::Medium), "oc-flash-alias", Some("max")),
                coder_assignment(Some(Difficulty::Hard), "oc-flash-inline-alias", None),
                coder_assignment(None, "oc-flash-alias", Some("max")),
            ]),
            "p-test"
        ));

        // 5. Real differences across Difficulty::ALL trigger: tool, model_arg, effort-only, and fallback+override.
        assert!(profile_has_variable_worker_route(
            &routing_snapshot(vec![
                coder_assignment(Some(Difficulty::Easy), "oc-flash", Some("max")),
                coder_assignment(Some(Difficulty::Hard), "agy-argon", Some("high")),
            ]),
            "p-test"
        ));
        assert!(profile_has_variable_worker_route(
            &routing_snapshot(vec![
                coder_assignment(Some(Difficulty::Easy), "oc-flash", Some("max")),
                coder_assignment(Some(Difficulty::Hard), "oc-pro", Some("max")),
            ]),
            "p-test"
        ));
        // The High-only AGY variant cannot execute the Medium assignment.
        assert!(!profile_has_variable_worker_route(
            &routing_snapshot(vec![
                coder_assignment(Some(Difficulty::Easy), "agy-argon", Some("medium")),
                coder_assignment(Some(Difficulty::Hard), "agy-argon", Some("high")),
            ]),
            "p-test"
        ));
        assert!(profile_has_variable_worker_route(
            &routing_snapshot(vec![
                coder_assignment(None, "oc-flash", Some("medium")),
                coder_assignment(Some(Difficulty::Extreme), "oc-flash", Some("max")),
            ]),
            "p-test"
        ));
        assert!(profile_has_variable_worker_route(
            &routing_snapshot(vec![
                coder_assignment(Some(Difficulty::Easy), "oc-flash", Some("max")),
                coder_assignment(Some(Difficulty::Hard), "codex-worker", Some("high")),
            ]),
            "p-test"
        ));
    }

    #[test]
    fn fixed_worker_route_skips_with_truthful_triggered_and_preserves_advisor_validation() {
        let tmp = TempDir::new().expect("tempdir");
        let root = fs::canonicalize(tmp.path()).expect("canonicalize tempdir");
        fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).expect("chmod root 0700");
        let registry_dir = root.join("registry");
        let plans_dir = root.join("plans");
        let repo = root.join("repo");
        fs::create_dir(&registry_dir).expect("mkdir registry");
        fs::create_dir(&plans_dir).expect("mkdir plans");
        fs::create_dir(&repo).expect("mkdir repo");
        fs::set_permissions(&plans_dir, fs::Permissions::from_mode(0o700))
            .expect("chmod plans 0700");

        let subject_fixed = "a".repeat(64);
        let subject_unconf = "b".repeat(64);
        let preview_for = |subject: &str| OperatorApprovalPreview {
            request: OperatorApprovalRequest {
                binding_id: "bnd-1".to_string(),
                thread_uuid: "550e8400-e29b-41d4-a716-446655440000".to_string(),
                plan_id: "plan_1".to_string(),
                plan_revision: 1,
                expected_registry_revision: 7,
                expected_plan_digest: "c".repeat(64),
                repository_path: repo.clone(),
                base_commit: "d".repeat(40),
                approved_task_ids: vec!["task-1".to_string()],
                approved_write_paths: vec!["src/lib.rs".to_string()],
                max_parallel: 1,
                budgets: ExecutionBudgets::default(),
            },
            subject_digest: subject.to_string(),
            tasks: Vec::new(),
        };

        let forecast = ValidatedForecast::validate(
            sample_input(2, true, false, "Coupled two-component change"),
            &repo,
        )
        .expect("valid triggered forecast");

        // Even with no Coder assignments ("none"), selected advisor validation still runs first.
        let mut no_advisor_snap = routing_snapshot(Vec::new());
        no_advisor_snap.profiles[0].decision_advisor = None;
        let preview_unconf = preview_for(&subject_unconf);
        let unconf_receipt = run_before_dispatch_forecast(&BeforeDispatchForecastContext {
            registry_dir: &registry_dir,
            plans_dir: &plans_dir,
            canonical_repo: &repo,
            registry_snapshot: &no_advisor_snap,
            profile_id: "p-test",
            profile_revision: 7,
            task_id: "task-1",
            baseline_difficulty: Difficulty::Hard,
            preview: &preview_unconf,
            forecast: &forecast,
        });
        assert_eq!(unconf_receipt.status, ShadowForecastStatus::Unavailable);
        assert_eq!(unconf_receipt.reason, "advisor_not_configured");
        assert!(unconf_receipt.triggered);
        assert!(!unconf_receipt.launched);

        // With a valid exact advisor and fixed Coder route, skip before launching advice while
        // preserving truthful `triggered: true` in both receipt and persisted record.json.
        let fixed_snap = routing_snapshot(vec![coder_assignment(None, "oc-flash", Some("max"))]);
        let preview_fixed = preview_for(&subject_fixed);
        let ctx_fixed = BeforeDispatchForecastContext {
            registry_dir: &registry_dir,
            plans_dir: &plans_dir,
            canonical_repo: &repo,
            registry_snapshot: &fixed_snap,
            profile_id: "p-test",
            profile_revision: 7,
            task_id: "task-1",
            baseline_difficulty: Difficulty::Hard,
            preview: &preview_fixed,
            forecast: &forecast,
        };
        let first = run_before_dispatch_forecast(&ctx_fixed);
        assert_eq!(first.status, ShadowForecastStatus::Skipped);
        assert_eq!(first.reason, "fixed_worker_route");
        assert!(first.triggered);
        assert!(!first.reused);
        assert!(!first.launched);
        assert!(first.advice_path.is_none());
        let record_path = first.record_path.clone().expect("record_path");
        let record_bytes = fs::read(&record_path).expect("read persisted record.json");
        let persisted: PersistedForecastRecord =
            serde_json::from_slice(&record_bytes).expect("parse persisted record.json");
        assert_eq!(persisted.status, ShadowForecastStatus::Skipped);
        assert_eq!(persisted.reason, "fixed_worker_route");
        assert!(persisted.triggered);
        assert!(persisted.request_file.is_none());
        assert!(persisted.advice_file.is_none());
        assert!(!plans_dir.join(ADVICE_CACHE_DIR_NAME).exists());

        // Replay must reuse the skipped record without overwriting or launching.
        let replay = run_before_dispatch_forecast(&ctx_fixed);
        assert_eq!(replay.status, ShadowForecastStatus::Skipped);
        assert_eq!(replay.reason, "fixed_worker_route");
        assert!(replay.triggered);
        assert!(replay.reused);
        assert!(!replay.launched);
        assert_eq!(
            fs::read(&record_path).expect("reread record.json"),
            record_bytes
        );
    }
}
