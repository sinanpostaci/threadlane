//! Read-only verification of scoped Forecast A (`record.json`) and shadow advice (`advice.json`)
//! artifacts, plus compact [`WorkspaceDecisionBadge`] projection for verified desktop chats.
//!
//! All reads are strictly read-only, bounded, symlink-resistant, and owner/permission verified
//! (`0700` directories, `0600` regular files, `O_NOFOLLOW | O_NONBLOCK`, inode stability). This
//! module never calls any external provider, never acquires or creates lock files, never mutates
//! stores or caches, and never projects raw prompts, summaries, paths, or secrets.

use std::fmt;
use std::fs::{self, Metadata, OpenOptions};
use std::io::{self, Read};
use std::path::{Component, Path, PathBuf};

use serde::Deserialize;
use sha2::{Digest, Sha256};
use threadlane_authority::validation::{
    validate_canonical_thread_uuid, validate_identifier, validate_sha256_hex,
};
use threadlane_authority::{ApprovalRecord, ApprovedTaskRecord, AuthorityStore};
use threadlane_core::observation::{
    WorkspaceDecisionAdviceSource, WorkspaceDecisionAdvisorProvider, WorkspaceDecisionBadge,
    WorkspaceDecisionBadgeStatus, WorkspaceDecisionDifficulty,
};
use threadlane_core::WorkerBacklogAuthority;
use threadlane_workflow::{Difficulty, ToolAdapter};

use crate::fs_safety::constant_time_str_eq;

/// Subdirectory under `<plans_dir>` holding per-`subject_digest` Forecast A records and advice.
pub const FORECAST_SUBDIR: &str = "ordinary-forecasts";

/// Subdirectory under `<plans_dir>` holding private shadow advice cache files.
pub const ADVICE_CACHE_SUBDIR: &str = "advice-cache";

/// Private Forecast A record filename (`record.json`).
pub const FORECAST_RECORD_FILENAME: &str = "record.json";

/// Private shadow advice request filename (`request.json`).
pub const FORECAST_REQUEST_FILENAME: &str = "request.json";

/// Private shadow advice output filename (`advice.json`).
pub const FORECAST_ADVICE_FILENAME: &str = "advice.json";

/// Required schema version for Forecast A `record.json` (`1`).
pub const FORECAST_SCHEMA_VERSION: u32 = 1;

/// Minimum allowed Forecast A component count (`1`).
pub const MIN_COMPONENT_COUNT: u8 = 1;

/// Maximum allowed Forecast A component count (`32`).
pub const MAX_COMPONENT_COUNT: u8 = 32;

/// Maximum bounded file size (`64 KiB`) read from Forecast A / shadow advice JSON files.
pub const MAX_FORECAST_FILE_BYTES: usize = 65_536;

/// Maximum bounded file size (`16 KiB`) read from a single persisted advice cache record.
pub const MAX_ADVICE_CACHE_RECORD_BYTES: usize = 16 * 1024;

/// Maximum IEEE-754 safe integer (`2^53 - 1`) accepted in JSON numeric metadata.
pub const MAX_JS_SAFE_INTEGER: u64 = 9_007_199_254_740_991;

/// Shadow mode marker required on both Forecast A records and shadow advice outputs.
pub const ADVICE_SHADOW_MODE: &str = "shadow";

const ADVICE_CACHE_SCHEMA_VERSION: u32 = 1;
const ADVICE_PROMPT_VERSION: &str = "threadlane-jev-shadow-v1";
const OPENAI_ADVICE_PROMPT_VERSION: &str = "threadlane-openai-shadow-v1";
const QUESTION_NAME: &str = "difficulty";
const JEV_INSTRUCTIONS: &str = "Select implementation difficulty from the supplied task evidence.";
const OPENAI_ADVICE_INSTRUCTIONS: &str =
    "Select implementation difficulty from the supplied evidence.";
const EASY_DESCRIPTION: &str = "Small isolated behavior-preserving edit.";
const MEDIUM_DESCRIPTION: &str = "Bounded multi-step change within known module boundaries.";
const HARD_DESCRIPTION: &str =
    "Substantial uncertainty, concurrency, or cross-module integration complexity.";
const EXTREME_DESCRIPTION: &str =
    "High-risk architectural, security-critical, or multi-system redesign.";

/// Structured, sanitized error produced while validating Forecast A or shadow advice artifacts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ForecastArtifactError {
    /// Machine-readable snake_case error code.
    pub code: &'static str,
    /// Static user-safe error summary (never carries paths, prompts, or secrets).
    pub message: &'static str,
}

impl ForecastArtifactError {
    /// Creates a new [`ForecastArtifactError`] with a static code and message.
    pub const fn new(code: &'static str, message: &'static str) -> Self {
        Self { code, message }
    }
}

impl fmt::Display for ForecastArtifactError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}

impl std::error::Error for ForecastArtifactError {}

/// Complexity markers recorded in Forecast A (`record.json`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ForecastComplexity {
    /// Estimated component count (`1..=32`).
    pub component_count: u8,
    /// Whether components are tightly coupled.
    pub coupled: bool,
    /// Whether the scope carries meaningful uncertainty.
    pub uncertain: bool,
}

/// Launch disposition recorded in Forecast A (`record.json`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ShadowForecastStatus {
    /// Consultation was intentionally skipped (simple task or fixed Worker route).
    Skipped,
    /// Consultation was durably launched via `threadlane-advice`.
    Launched,
    /// Advisor was unconfigured, disabled, invalid, or binary unavailable.
    Unavailable,
    /// Launch or storage state was ambiguous/unknown.
    Unknown,
}

/// Selected shadow decision advisor recorded in Forecast A (`record.json`).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ShadowAdvisorSelection {
    /// Registry tool identifier.
    pub tool_id: String,
    /// Registry model identifier.
    pub model_id: String,
    /// Exact requested model argument passed to the decision provider.
    pub model_arg: String,
    /// Decision provider adapter (`jev_api` or `openai_decisions_api`).
    pub adapter: ToolAdapter,
}

/// Verified Forecast A durable record (`<plans_dir>/ordinary-forecasts/<subject_digest>/record.json`).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ForecastRecordFile {
    /// Record schema version (must be `1`).
    pub schema_version: u32,
    /// Must be `true` (strictly advisory).
    pub advisory_only: bool,
    /// Must be `"shadow"`.
    pub mode: String,
    /// Control-plane binding identifier.
    pub binding_id: String,
    /// Canonical lowercase hyphenated non-nil thread UUID.
    pub thread_uuid: String,
    /// Published workflow plan identifier.
    pub plan_id: String,
    /// Published workflow plan revision.
    pub plan_revision: u64,
    /// Frozen approved task identifier.
    pub task_id: String,
    /// Workflow profile identifier.
    pub profile_id: String,
    /// Workflow registry/profile revision.
    pub profile_revision: u64,
    /// Frozen repository base commit SHA.
    pub base_commit: String,
    /// Frozen 64-character lowercase hex SHA-256 approval subject digest.
    pub subject_digest: String,
    /// Frozen initial Coworker baseline difficulty.
    pub baseline_difficulty: Difficulty,
    /// Bounded complexity markers.
    pub complexity: ForecastComplexity,
    /// SHA-256 digest of the operator-supplied forecast summary.
    pub summary_sha256: String,
    /// Whether complexity markers triggered shadow consultation.
    pub triggered: bool,
    /// Launch disposition (`skipped`, `launched`, `unavailable`, or `unknown`).
    pub status: ShadowForecastStatus,
    /// Safe machine reason string.
    pub reason: String,
    /// Chosen shadow decision advisor metadata, when resolved.
    #[serde(default)]
    pub advisor: Option<ShadowAdvisorSelection>,
    /// SHA-256 digest of the shadow advice request summary, when launched.
    #[serde(default)]
    pub request_summary_sha256: Option<String>,
    /// Fixed relative request filename (`"request.json"`), when launched.
    #[serde(default)]
    pub request_file: Option<String>,
    /// Fixed relative advice filename (`"advice.json"`), when launched.
    #[serde(default)]
    pub advice_file: Option<String>,
    /// Non-zero child PID hint recorded at launch, when available.
    #[serde(default)]
    pub pid_hint: Option<u32>,
    /// Recording timestamp in milliseconds since Unix epoch.
    pub recorded_at_ms: u64,
}

/// Verified state of optional shadow advice (`advice.json`) relative to Forecast A (`record.json`).
#[derive(Debug, Clone, PartialEq)]
pub enum AdviceLoadState {
    /// No `advice.json` exists and consultation was not launched.
    Absent,
    /// Forecast A was launched and `advice.json` is absent or empty/whitespace-only.
    Pending,
    /// `advice.json` ended with an unexpected JSON EOF (partial or interrupted write).
    Unknown,
    /// `advice.json` parsed and passed every schema and scope invariant.
    Loaded(Box<AdviceClassifyFile>),
}

/// Verified shadow advice output (`<plans_dir>/ordinary-forecasts/<subject_digest>/advice.json`).
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdviceClassifyFile {
    /// Must be `true` (untrusted external data).
    pub untrusted_data: bool,
    /// Must be `true` (strictly advisory).
    pub advisory_only: bool,
    /// Must be `"shadow"`.
    pub mode: String,
    /// Response provenance (`"provider"` or `"cache"`).
    pub source: String,
    /// Correlation context identifier (matched against `thread_uuid`).
    pub context_id: String,
    /// Frozen task identifier.
    pub task_id: String,
    /// Frozen plan/task revision.
    pub task_revision: u64,
    /// Frozen registry revision.
    pub registry_revision: u64,
    /// Frozen workflow profile identifier.
    #[serde(default)]
    pub profile_id: Option<String>,
    /// Selected advisor tool identifier.
    pub tool_id: String,
    /// Selected advisor model identifier.
    pub model_id: String,
    /// Exact requested model argument.
    pub requested_model: String,
    /// SHA-256 digest of the normalized task summary.
    pub summary_sha256: String,
    /// Deterministic SHA-256 cache key.
    pub cache_key: String,
    /// Advisor classification payload.
    pub advice: AdvicePayload,
    /// Optional echoed baseline difficulty.
    #[serde(default)]
    pub baseline_difficulty: Option<Difficulty>,
    /// Optional agreement marker.
    #[serde(default)]
    pub agrees_with_baseline: Option<bool>,
    /// `true` for live `"provider"` calls, `false` for `"cache"` hits.
    pub provider_call_performed: bool,
    /// Cache age in milliseconds when `source == "cache"`.
    #[serde(default)]
    pub cache_age_ms: Option<u64>,
    /// Total command elapsed time in milliseconds.
    pub elapsed_ms: u64,
}

/// Inner classification payload inside [`AdviceClassifyFile`].
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdvicePayload {
    /// Provider token (`"jev"` or `"openai"`).
    pub provider: String,
    /// Echoed provider model identifier.
    pub model: String,
    /// Validated classified difficulty, or `None` on refusal.
    pub difficulty: Option<Difficulty>,
    /// Optional finite provider confidence in `[0.0, 1.0]`.
    pub confidence: Option<f64>,
    /// Whether the advisor flagged the classification for review (required when `difficulty` is `None`).
    pub needs_review: bool,
    /// Safe machine reason code (for example `"classified"`, `"low_confidence"`, or `"refusal"`).
    pub reason_code: String,
    /// Optional input token count.
    pub input_tokens: Option<u64>,
    /// Optional output token count.
    pub output_tokens: Option<u64>,
}

/// Scoped read-only lookup inputs required to project one [`WorkspaceDecisionBadge`].
#[derive(Debug, Clone, Copy)]
pub struct DecisionBadgeLookupScope<'a> {
    /// Canonical existing private (`0700`) plans directory holding `ordinary-forecasts`.
    pub plans_dir: &'a Path,
    /// Canonical existing private (`0700`) authority store directory.
    pub authority_dir: &'a Path,
    /// Verified control-plane binding identifier.
    pub binding_id: &'a str,
    /// Verified canonical lowercase hyphenated non-nil thread UUID.
    pub thread_uuid: &'a str,
    /// Verified canonical project root path for the owning chat.
    pub canonical_project_scope: &'a str,
    /// Durable Worker backlog item identifier.
    pub item_id: &'a str,
    /// Exact current guarded Worker execution reference on `item_id`.
    pub execution_ref: &'a str,
    /// Frozen execution authority tuple from the current backlog execution row.
    pub authority: &'a WorkerBacklogAuthority,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct PersistedAdviceCacheRecord {
    schema_version: u32,
    prompt_digest: String,
    cache_key: String,
    project_root_sha256: String,
    context_id: String,
    task_id: String,
    task_revision: u64,
    summary_sha256: String,
    registry_revision: u64,
    #[serde(default)]
    profile_id: Option<String>,
    #[serde(default)]
    provider: Option<String>,
    tool_id: String,
    model_id: String,
    requested_model: String,
    state: PersistedAdviceCacheState,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
enum PersistedAdviceCacheState {
    Pending {
        #[allow(dead_code)]
        attempt_id: String,
        #[allow(dead_code)]
        started_at_ms: u64,
    },
    Uncertain {
        #[allow(dead_code)]
        attempt_id: String,
        failed_at_ms: u64,
        error_code: String,
    },
    Completed {
        #[allow(dead_code)]
        attempt_id: String,
        #[allow(dead_code)]
        completed_at_ms: u64,
        #[allow(dead_code)]
        advice: AdvicePayload,
    },
}

/// Projects one compact, verified [`WorkspaceDecisionBadge`] strictly read-only for the exact
/// current Worker execution scope.
///
/// Returns `None` (fail-closed, no icon and no placeholder) whenever:
/// - scope identifiers, authority records, canonical project path, or digests do not match;
/// - Forecast A is absent, skipped, unavailable, unknown, or fails strict validation;
/// - shadow advice is truncated/unproven, corrupt, scope-mismatched, or fails private file checks.
pub fn project_verified_decision_badge_readonly(
    scope: DecisionBadgeLookupScope<'_>,
) -> Option<WorkspaceDecisionBadge> {
    let binding_id = validate_identifier("binding_id", scope.binding_id).ok()?;
    let thread_uuid = validate_canonical_thread_uuid("thread_uuid", scope.thread_uuid).ok()?;
    let item_id = validate_identifier("item_id", scope.item_id).ok()?;
    let execution_ref = validate_identifier("execution_ref", scope.execution_ref).ok()?;
    let approval_id = validate_identifier("approval_id", &scope.authority.approval_id).ok()?;
    let task_id = validate_identifier("task_id", &scope.authority.task_id).ok()?;
    validate_sha256_hex("subject_digest", &scope.authority.subject_digest).ok()?;
    validate_sha256_hex("task_digest", &scope.authority.task_digest).ok()?;

    if scope.canonical_project_scope.trim().is_empty()
        || !Path::new(scope.canonical_project_scope).is_absolute()
    {
        return None;
    }

    let approval =
        AuthorityStore::get_for_operator_readonly_at(scope.authority_dir, &approval_id).ok()?;
    if approval.binding_id != binding_id
        || approval.thread_uuid != thread_uuid
        || approval.repository_path != Path::new(scope.canonical_project_scope)
        || !constant_time_str_eq(&approval.subject_digest, &scope.authority.subject_digest)
    {
        return None;
    }
    validate_identifier("profile_id", &approval.profile_id).ok()?;

    let mut matching_tasks = approval.tasks.iter().filter(|task| task.task_id == task_id);
    let approved_task = matching_tasks.next()?;
    if matching_tasks.next().is_some()
        || !constant_time_str_eq(&approved_task.task_digest, &scope.authority.task_digest)
    {
        return None;
    }

    let (forecast_record, advice_state) = read_verified_forecast_artifacts(
        scope.plans_dir,
        &binding_id,
        &thread_uuid,
        &approval,
        approved_task,
    )
    .ok()?;

    let record = forecast_record?;
    if record.status != ShadowForecastStatus::Launched {
        return None;
    }
    let advisor = record.advisor.as_ref()?;
    let provider = match advisor.adapter {
        ToolAdapter::JevApi => WorkspaceDecisionAdvisorProvider::Jev,
        ToolAdapter::OpenaiDecisionsApi => WorkspaceDecisionAdvisorProvider::Openai,
        _ => return None,
    };

    let coworker_difficulty = Some(map_workflow_difficulty(approved_task.difficulty));

    let badge = match advice_state {
        AdviceLoadState::Loaded(file) => {
            let advice_source = match file.source.as_str() {
                "provider" => WorkspaceDecisionAdviceSource::Fresh,
                "cache" => WorkspaceDecisionAdviceSource::Cached,
                _ => return None,
            };
            let cache_age_ms = if advice_source == WorkspaceDecisionAdviceSource::Cached {
                file.cache_age_ms
            } else {
                None
            };
            if let Some(recommended) = file.advice.difficulty {
                WorkspaceDecisionBadge {
                    item_id,
                    execution_ref,
                    approval_id,
                    task_id,
                    subject_digest: approval.subject_digest.clone(),
                    provider,
                    status: WorkspaceDecisionBadgeStatus::Result,
                    recommended_difficulty: Some(map_workflow_difficulty(recommended)),
                    coworker_difficulty,
                    advice_source: Some(advice_source),
                    advisor_model: Some(file.advice.model),
                    cache_age_ms,
                    failure_reason_code: None,
                    agrees_with_coworker: Some(recommended == approved_task.difficulty),
                }
            } else {
                WorkspaceDecisionBadge {
                    item_id,
                    execution_ref,
                    approval_id,
                    task_id,
                    subject_digest: approval.subject_digest.clone(),
                    provider,
                    status: WorkspaceDecisionBadgeStatus::Failed,
                    recommended_difficulty: None,
                    coworker_difficulty,
                    advice_source: Some(advice_source),
                    advisor_model: Some(file.advice.model),
                    cache_age_ms,
                    failure_reason_code: Some(file.advice.reason_code),
                    agrees_with_coworker: None,
                }
            }
        }
        AdviceLoadState::Pending => {
            let cache_failure = read_verified_uncertain_cache_failure(
                scope.plans_dir,
                scope.canonical_project_scope,
                &thread_uuid,
                &approval,
                approved_task,
                &record,
                advisor,
                provider,
            )
            .ok()?;
            match cache_failure {
                Some(error_code) => WorkspaceDecisionBadge {
                    item_id,
                    execution_ref,
                    approval_id,
                    task_id,
                    subject_digest: approval.subject_digest.clone(),
                    provider,
                    status: WorkspaceDecisionBadgeStatus::Failed,
                    recommended_difficulty: None,
                    coworker_difficulty,
                    advice_source: None,
                    advisor_model: Some(advisor.model_arg.clone()),
                    cache_age_ms: None,
                    failure_reason_code: Some(error_code),
                    agrees_with_coworker: None,
                },
                None => WorkspaceDecisionBadge {
                    item_id,
                    execution_ref,
                    approval_id,
                    task_id,
                    subject_digest: approval.subject_digest.clone(),
                    provider,
                    status: WorkspaceDecisionBadgeStatus::Pending,
                    recommended_difficulty: None,
                    coworker_difficulty,
                    advice_source: None,
                    advisor_model: Some(advisor.model_arg.clone()),
                    cache_age_ms: None,
                    failure_reason_code: None,
                    agrees_with_coworker: None,
                },
            }
        }
        AdviceLoadState::Unknown | AdviceLoadState::Absent => return None,
    };

    badge.normalized_safe()
}

/// Computes the deterministic SHA-256 prompt digest matching `threadlane-decisions` shadow advice.
pub fn compute_shadow_advice_prompt_digest(provider: WorkspaceDecisionAdvisorProvider) -> String {
    let mut hasher = Sha256::new();
    match provider {
        WorkspaceDecisionAdvisorProvider::Jev => {
            hash_len_prefixed(&mut hasher, ADVICE_PROMPT_VERSION.as_bytes());
            hash_len_prefixed(&mut hasher, QUESTION_NAME.as_bytes());
            hash_len_prefixed(&mut hasher, JEV_INSTRUCTIONS.as_bytes());
            hash_len_prefixed(&mut hasher, EASY_DESCRIPTION.as_bytes());
            hash_len_prefixed(&mut hasher, MEDIUM_DESCRIPTION.as_bytes());
            hash_len_prefixed(&mut hasher, HARD_DESCRIPTION.as_bytes());
            hash_len_prefixed(&mut hasher, EXTREME_DESCRIPTION.as_bytes());
        }
        WorkspaceDecisionAdvisorProvider::Openai => {
            hash_len_prefixed(&mut hasher, OPENAI_ADVICE_PROMPT_VERSION.as_bytes());
            hash_len_prefixed(&mut hasher, b"openai");
            hash_len_prefixed(&mut hasher, QUESTION_NAME.as_bytes());
            hash_len_prefixed(&mut hasher, OPENAI_ADVICE_INSTRUCTIONS.as_bytes());
            hash_len_prefixed(&mut hasher, EASY_DESCRIPTION.as_bytes());
            hash_len_prefixed(&mut hasher, MEDIUM_DESCRIPTION.as_bytes());
            hash_len_prefixed(&mut hasher, HARD_DESCRIPTION.as_bytes());
            hash_len_prefixed(&mut hasher, EXTREME_DESCRIPTION.as_bytes());
        }
    }
    finalize_sha256_hex(hasher)
}

/// Computes the deterministic SHA-256 advice cache key matching `threadlane-decisions`.
#[allow(clippy::too_many_arguments)]
pub fn compute_shadow_advice_cache_key(
    canonical_project_root: &str,
    context_id: &str,
    task_id: &str,
    task_revision: u64,
    summary_sha256: &str,
    registry_revision: u64,
    provider: WorkspaceDecisionAdvisorProvider,
    tool_id: &str,
    model_id: &str,
    requested_model: &str,
    profile_id: Option<&str>,
) -> String {
    let prompt_digest = compute_shadow_advice_prompt_digest(provider);
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
    if provider != WorkspaceDecisionAdvisorProvider::Jev {
        hash_len_prefixed(&mut hasher, b"provider");
        hash_len_prefixed(&mut hasher, provider.as_str().as_bytes());
    }
    finalize_sha256_hex(hasher)
}

#[allow(clippy::too_many_arguments)]
fn read_verified_uncertain_cache_failure(
    plans_dir: &Path,
    canonical_project_scope: &str,
    thread_uuid: &str,
    approval: &ApprovalRecord,
    approved_task: &ApprovedTaskRecord,
    forecast_record: &ForecastRecordFile,
    advisor: &ShadowAdvisorSelection,
    provider: WorkspaceDecisionAdvisorProvider,
) -> Result<Option<String>, ForecastArtifactError> {
    let plans_meta = validate_existing_private_dir_meta(plans_dir)?;
    let cache_dir = plans_dir.join(ADVICE_CACHE_SUBDIR);
    let Some(cache_dir_meta) = validate_optional_private_child_dir(&cache_dir, &plans_meta)? else {
        return Ok(None);
    };

    let Some(request_summary_sha) = forecast_record.request_summary_sha256.as_deref() else {
        return Err(ForecastArtifactError::new(
            "invalid_forecast",
            "Forecast A record.json is missing request_summary_sha256 for launched advice.",
        ));
    };

    let prompt_digest = compute_shadow_advice_prompt_digest(provider);
    let cache_key = compute_shadow_advice_cache_key(
        canonical_project_scope,
        thread_uuid,
        &approved_task.task_id,
        approval.plan_revision,
        request_summary_sha,
        approval.registry_revision,
        provider,
        &advisor.tool_id,
        &advisor.model_id,
        &advisor.model_arg,
        Some(approval.profile_id.as_str()),
    );
    let project_root_sha256 = sha256_hex(canonical_project_scope.as_bytes());

    let cache_path = cache_dir.join(format!("{cache_key}.json"));
    let Some(bytes) =
        read_private_file_optional(&cache_path, &cache_dir_meta, MAX_ADVICE_CACHE_RECORD_BYTES)?
    else {
        return Ok(None);
    };

    let parsed: PersistedAdviceCacheRecord = serde_json::from_slice(&bytes).map_err(|_| {
        ForecastArtifactError::new(
            "invalid_advice",
            "Advice cache record failed strict JSON schema validation.",
        )
    })?;

    let provider_matches = match provider {
        WorkspaceDecisionAdvisorProvider::Jev => {
            parsed.provider.is_none() || parsed.provider.as_deref() == Some("jev")
        }
        WorkspaceDecisionAdvisorProvider::Openai => parsed.provider.as_deref() == Some("openai"),
    };

    if parsed.schema_version != ADVICE_CACHE_SCHEMA_VERSION
        || !constant_time_str_eq(&parsed.prompt_digest, &prompt_digest)
        || !constant_time_str_eq(&parsed.cache_key, &cache_key)
        || !constant_time_str_eq(&parsed.project_root_sha256, &project_root_sha256)
        || parsed.context_id != thread_uuid
        || parsed.task_id != approved_task.task_id
        || parsed.task_revision != approval.plan_revision
        || !constant_time_str_eq(&parsed.summary_sha256, request_summary_sha)
        || parsed.registry_revision != approval.registry_revision
        || parsed.profile_id.as_deref() != Some(approval.profile_id.as_str())
        || !provider_matches
        || parsed.tool_id != advisor.tool_id
        || parsed.model_id != advisor.model_id
        || parsed.requested_model != advisor.model_arg
    {
        return Err(ForecastArtifactError::new(
            "scope_mismatch",
            "Advice cache record scope does not match the launched Forecast A consultation.",
        ));
    }

    match parsed.state {
        PersistedAdviceCacheState::Uncertain {
            failed_at_ms,
            error_code,
            ..
        } => {
            if failed_at_ms > MAX_JS_SAFE_INTEGER
                || error_code.trim().is_empty()
                || error_code.chars().any(char::is_control)
            {
                return Err(ForecastArtifactError::new(
                    "invalid_advice",
                    "Advice cache uncertain failure metadata is invalid.",
                ));
            }
            Ok(Some(error_code))
        }
        PersistedAdviceCacheState::Pending { .. } | PersistedAdviceCacheState::Completed { .. } => {
            Ok(None)
        }
    }
}

/// Reads and strictly validates optional scoped Forecast A (`record.json`) and shadow advice
/// (`advice.json`) artifacts under `<plans_dir>/ordinary-forecasts/<subject_digest>/`.
pub fn read_verified_forecast_artifacts(
    plans_dir: &Path,
    binding_id: &str,
    thread_uuid: &str,
    approval: &ApprovalRecord,
    approved_task: &ApprovedTaskRecord,
) -> Result<(Option<ForecastRecordFile>, AdviceLoadState), ForecastArtifactError> {
    let plans_meta = validate_existing_private_dir_meta(plans_dir)?;
    validate_sha256_hex("subject_digest", &approval.subject_digest).map_err(|_| {
        ForecastArtifactError::new(
            "corrupt_store",
            "Approval subject_digest failed SHA-256 validation.",
        )
    })?;

    let forecasts_root = plans_dir.join(FORECAST_SUBDIR);
    let Some(forecasts_root_meta) =
        validate_optional_private_child_dir(&forecasts_root, &plans_meta)?
    else {
        return Ok((None, AdviceLoadState::Absent));
    };

    let subject_dir = forecasts_root.join(&approval.subject_digest);
    let Some(subject_dir_meta) =
        validate_optional_private_child_dir(&subject_dir, &forecasts_root_meta)?
    else {
        return Ok((None, AdviceLoadState::Absent));
    };

    let record_bytes = read_private_file_optional(
        &subject_dir.join(FORECAST_RECORD_FILENAME),
        &subject_dir_meta,
        MAX_FORECAST_FILE_BYTES,
    )?;
    let record = match record_bytes {
        Some(bytes) => {
            let parsed: ForecastRecordFile = serde_json::from_slice(&bytes).map_err(|_| {
                ForecastArtifactError::new(
                    "invalid_forecast",
                    "Forecast A record.json failed strict JSON schema validation.",
                )
            })?;
            validate_forecast_record(&parsed, binding_id, thread_uuid, approval, approved_task)?;
            Some(parsed)
        }
        None => None,
    };

    let advice_bytes = read_private_file_optional(
        &subject_dir.join(FORECAST_ADVICE_FILENAME),
        &subject_dir_meta,
        MAX_FORECAST_FILE_BYTES,
    )?;
    let advice_state = match (record.as_ref(), advice_bytes) {
        (None, None) => AdviceLoadState::Absent,
        (None, Some(_)) => {
            return Err(ForecastArtifactError::new(
                "invalid_advice",
                "Provider advice.json cannot be verified without a matching launched Forecast A record.json.",
            ));
        }
        (Some(rec), None) if rec.status == ShadowForecastStatus::Launched => {
            AdviceLoadState::Pending
        }
        (Some(_), None) => AdviceLoadState::Absent,
        (Some(rec), Some(_)) if rec.status != ShadowForecastStatus::Launched => {
            return Err(ForecastArtifactError::new(
                "invalid_advice",
                "Provider advice.json is unexpected for a non-launched Forecast A record.json.",
            ));
        }
        (Some(rec), Some(bytes)) => {
            if bytes.iter().all(u8::is_ascii_whitespace) {
                AdviceLoadState::Pending
            } else {
                match serde_json::from_slice::<AdviceClassifyFile>(&bytes) {
                    Ok(parsed) => {
                        validate_advice_file(&parsed, thread_uuid, approval, approved_task, rec)?;
                        AdviceLoadState::Loaded(Box::new(parsed))
                    }
                    Err(err) if err.is_eof() => AdviceLoadState::Unknown,
                    Err(_) => {
                        return Err(ForecastArtifactError::new(
                            "invalid_advice",
                            "Provider advice.json failed strict JSON schema validation.",
                        ));
                    }
                }
            }
        }
    };

    Ok((record, advice_state))
}

fn validate_shadow_advisor(advisor: &ShadowAdvisorSelection) -> Result<(), ForecastArtifactError> {
    if !matches!(
        advisor.adapter,
        ToolAdapter::JevApi | ToolAdapter::OpenaiDecisionsApi
    ) {
        return Err(ForecastArtifactError::new(
            "invalid_forecast",
            "Forecast A record.json advisor adapter must be jev_api or openai_decisions_api.",
        ));
    }
    if advisor.tool_id.trim().is_empty()
        || advisor.tool_id.trim() != advisor.tool_id
        || advisor.tool_id.chars().any(char::is_control)
        || advisor.model_id.trim().is_empty()
        || advisor.model_id.trim() != advisor.model_id
        || advisor.model_id.chars().any(char::is_control)
        || advisor.model_arg.trim().is_empty()
        || advisor.model_arg.trim() != advisor.model_arg
        || advisor.model_arg.chars().any(char::is_control)
    {
        return Err(ForecastArtifactError::new(
            "invalid_forecast",
            "Forecast A record.json advisor metadata is missing or invalid.",
        ));
    }
    Ok(())
}

fn validate_forecast_record(
    record: &ForecastRecordFile,
    binding_id: &str,
    thread_uuid: &str,
    approval: &ApprovalRecord,
    approved_task: &ApprovedTaskRecord,
) -> Result<(), ForecastArtifactError> {
    if record.schema_version != FORECAST_SCHEMA_VERSION {
        return Err(ForecastArtifactError::new(
            "invalid_forecast",
            "Forecast A record.json has an unsupported schema_version.",
        ));
    }
    if !record.advisory_only || record.mode != ADVICE_SHADOW_MODE {
        return Err(ForecastArtifactError::new(
            "invalid_forecast",
            "Forecast A record.json failed advisory shadow mode invariants.",
        ));
    }
    if record.binding_id != binding_id || record.thread_uuid != thread_uuid {
        return Err(ForecastArtifactError::new(
            "scope_mismatch",
            "Forecast A record.json does not belong to the requested binding and thread scope.",
        ));
    }
    if !constant_time_str_eq(&record.subject_digest, &approval.subject_digest) {
        return Err(ForecastArtifactError::new(
            "subject_mismatch",
            "Forecast A record.json subject_digest does not match the approval.",
        ));
    }
    if record.plan_id != approval.plan_id
        || record.plan_revision != approval.plan_revision
        || record.profile_id != approval.profile_id
        || record.profile_revision != approval.registry_revision
        || record.task_id != approved_task.task_id
        || record.base_commit != approval.base_commit
    {
        return Err(ForecastArtifactError::new(
            "scope_mismatch",
            "Forecast A record.json plan, profile, task, or base_commit does not match the approval.",
        ));
    }
    if record.baseline_difficulty != approved_task.difficulty {
        return Err(ForecastArtifactError::new(
            "scope_mismatch",
            "Forecast A record.json baseline_difficulty does not match frozen task difficulty.",
        ));
    }
    if !(MIN_COMPONENT_COUNT..=MAX_COMPONENT_COUNT).contains(&record.complexity.component_count) {
        return Err(ForecastArtifactError::new(
            "invalid_forecast",
            "Forecast A record.json complexity.component_count must be between 1 and 32.",
        ));
    }
    validate_sha256_hex("summary_sha256", &record.summary_sha256).map_err(|_| {
        ForecastArtifactError::new(
            "invalid_forecast",
            "Forecast A record.json summary_sha256 is not a valid SHA-256 hex digest.",
        )
    })?;
    if record.reason.trim().is_empty() || record.reason.chars().any(char::is_control) {
        return Err(ForecastArtifactError::new(
            "invalid_forecast",
            "Forecast A record.json reason must be non-empty without control characters.",
        ));
    }
    if record.recorded_at_ms > MAX_JS_SAFE_INTEGER {
        return Err(ForecastArtifactError::new(
            "invalid_forecast",
            "Forecast A record.json recorded_at_ms exceeds safe integer bounds.",
        ));
    }

    let expected_triggered = record.complexity.uncertain
        || record.complexity.coupled
        || record.complexity.component_count > 1;
    if record.triggered != expected_triggered {
        return Err(ForecastArtifactError::new(
            "invalid_forecast",
            "Forecast A record.json triggered flag contradicts complexity markers.",
        ));
    }

    if let Some(advisor) = record.advisor.as_ref() {
        validate_shadow_advisor(advisor)?;
    }

    match record.status {
        ShadowForecastStatus::Skipped => {
            let valid_skip = match record.reason.as_str() {
                "fixed_worker_route" => record.triggered,
                _ => !record.triggered && record.advisor.is_none(),
            };
            if !valid_skip
                || record.request_summary_sha256.is_some()
                || record.request_file.is_some()
                || record.advice_file.is_some()
                || record.pid_hint.is_some()
            {
                return Err(ForecastArtifactError::new(
                    "invalid_forecast",
                    "Forecast A record.json skipped status has incompatible trigger or launch fields.",
                ));
            }
        }
        ShadowForecastStatus::Unavailable | ShadowForecastStatus::Unknown => {
            if !record.triggered
                || record.request_summary_sha256.is_some()
                || record.request_file.is_some()
                || record.advice_file.is_some()
                || record.pid_hint.is_some()
            {
                return Err(ForecastArtifactError::new(
                    "invalid_forecast",
                    "Forecast A record.json non-launched status has incompatible trigger or launch fields.",
                ));
            }
        }
        ShadowForecastStatus::Launched => {
            if !record.triggered || record.advisor.is_none() {
                return Err(ForecastArtifactError::new(
                    "invalid_forecast",
                    "Forecast A record.json launched status requires triggered=true and advisor metadata.",
                ));
            }
            let Some(request_sha) = record.request_summary_sha256.as_deref() else {
                return Err(ForecastArtifactError::new(
                    "invalid_forecast",
                    "Forecast A record.json launched status requires request_summary_sha256.",
                ));
            };
            validate_sha256_hex("request_summary_sha256", request_sha).map_err(|_| {
                ForecastArtifactError::new(
                    "invalid_forecast",
                    "Forecast A record.json request_summary_sha256 is not a valid SHA-256 hex digest.",
                )
            })?;
            if record.request_file.as_deref() != Some(FORECAST_REQUEST_FILENAME)
                || record.advice_file.as_deref() != Some(FORECAST_ADVICE_FILENAME)
            {
                return Err(ForecastArtifactError::new(
                    "invalid_forecast",
                    "Forecast A record.json launched status has invalid request_file or advice_file.",
                ));
            }
            if record.pid_hint == Some(0) {
                return Err(ForecastArtifactError::new(
                    "invalid_forecast",
                    "Forecast A record.json pid_hint must be non-zero when present.",
                ));
            }
        }
    }

    Ok(())
}

fn validate_advice_file(
    file: &AdviceClassifyFile,
    thread_uuid: &str,
    approval: &ApprovalRecord,
    approved_task: &ApprovedTaskRecord,
    forecast_record: &ForecastRecordFile,
) -> Result<(), ForecastArtifactError> {
    if !file.untrusted_data || !file.advisory_only || file.mode != ADVICE_SHADOW_MODE {
        return Err(ForecastArtifactError::new(
            "invalid_advice",
            "Provider advice.json failed advisory shadow mode invariants.",
        ));
    }
    if !matches!(file.source.as_str(), "provider" | "cache") {
        return Err(ForecastArtifactError::new(
            "invalid_advice",
            "Provider advice.json carries an unrecognized source value.",
        ));
    }
    let expected_provider_call = file.source == "provider";
    if file.provider_call_performed != expected_provider_call {
        return Err(ForecastArtifactError::new(
            "invalid_advice",
            "Provider advice.json source contradicts provider_call_performed.",
        ));
    }
    if expected_provider_call && file.cache_age_ms.is_some() {
        return Err(ForecastArtifactError::new(
            "invalid_advice",
            "Provider advice.json must not include cache_age_ms for live provider output.",
        ));
    }
    if !expected_provider_call && file.cache_age_ms.is_none() {
        return Err(ForecastArtifactError::new(
            "invalid_advice",
            "Provider advice.json cached output must include cache_age_ms.",
        ));
    }
    if file.elapsed_ms > MAX_JS_SAFE_INTEGER
        || file
            .cache_age_ms
            .is_some_and(|age| age > MAX_JS_SAFE_INTEGER)
        || file
            .advice
            .input_tokens
            .is_some_and(|tokens| tokens > MAX_JS_SAFE_INTEGER)
        || file
            .advice
            .output_tokens
            .is_some_and(|tokens| tokens > MAX_JS_SAFE_INTEGER)
    {
        return Err(ForecastArtifactError::new(
            "invalid_advice",
            "Provider advice.json numeric metadata exceeds safe integer bounds.",
        ));
    }
    if file.advice.difficulty.is_none() && !file.advice.needs_review {
        return Err(ForecastArtifactError::new(
            "invalid_advice",
            "Provider advice.json refusal without difficulty must mark needs_review.",
        ));
    }
    if file.context_id != thread_uuid
        || file.task_id != approved_task.task_id
        || file.task_revision != approval.plan_revision
        || file.registry_revision != approval.registry_revision
        || file.profile_id.as_deref() != Some(approval.profile_id.as_str())
    {
        return Err(ForecastArtifactError::new(
            "scope_mismatch",
            "Provider advice.json scope does not match thread, task, revision, registry, or profile.",
        ));
    }
    validate_sha256_hex("summary_sha256", &file.summary_sha256).map_err(|_| {
        ForecastArtifactError::new(
            "invalid_advice",
            "Provider advice.json summary_sha256 is not a valid SHA-256 hex digest.",
        )
    })?;
    validate_sha256_hex("cache_key", &file.cache_key).map_err(|_| {
        ForecastArtifactError::new(
            "invalid_advice",
            "Provider advice.json cache_key is not a valid SHA-256 hex digest.",
        )
    })?;
    let expected_request_summary_sha = forecast_record
        .request_summary_sha256
        .as_deref()
        .ok_or_else(|| {
            ForecastArtifactError::new(
                "invalid_forecast",
                "Forecast A record.json is missing request_summary_sha256 for launched advice.",
            )
        })?;
    if !constant_time_str_eq(&file.summary_sha256, expected_request_summary_sha) {
        return Err(ForecastArtifactError::new(
            "scope_mismatch",
            "Provider advice.json summary_sha256 does not match Forecast A request_summary_sha256.",
        ));
    }
    let advisor = forecast_record.advisor.as_ref().ok_or_else(|| {
        ForecastArtifactError::new(
            "invalid_forecast",
            "Forecast A record.json is missing chosen advisor metadata for launched advice.",
        )
    })?;
    let expected_provider = match advisor.adapter {
        ToolAdapter::JevApi => "jev",
        ToolAdapter::OpenaiDecisionsApi => "openai",
        _ => {
            return Err(ForecastArtifactError::new(
                "invalid_forecast",
                "Forecast A record.json carries an unsupported advisor adapter.",
            ));
        }
    };
    if file.tool_id != advisor.tool_id
        || file.model_id != advisor.model_id
        || file.requested_model != advisor.model_arg
        || file.advice.provider != expected_provider
    {
        return Err(ForecastArtifactError::new(
            "scope_mismatch",
            "Provider advice.json advisor tool, model, or provider does not match Forecast A.",
        ));
    }
    if let Some(baseline) = file.baseline_difficulty {
        if baseline != approved_task.difficulty {
            return Err(ForecastArtifactError::new(
                "scope_mismatch",
                "Provider advice.json baseline_difficulty does not match frozen task difficulty.",
            ));
        }
    }
    if file.advice.model.trim().is_empty() || file.advice.reason_code.trim().is_empty() {
        return Err(ForecastArtifactError::new(
            "invalid_advice",
            "Provider advice.json is missing required classifier metadata.",
        ));
    }
    if let Some(confidence) = file.advice.confidence {
        if !confidence.is_finite() || !(0.0..=1.0).contains(&confidence) {
            return Err(ForecastArtifactError::new(
                "invalid_advice",
                "Provider advice.json confidence must be finite and within [0.0, 1.0].",
            ));
        }
    }
    let expected_agrees = match (file.baseline_difficulty, file.advice.difficulty) {
        (Some(baseline), Some(provider_diff)) => Some(provider_diff == baseline),
        _ => None,
    };
    if file.agrees_with_baseline != expected_agrees {
        return Err(ForecastArtifactError::new(
            "invalid_advice",
            "Provider advice.json agrees_with_baseline contradicts difficulty fields.",
        ));
    }
    Ok(())
}

/// Validates that `dir` is an existing absolute non-symlink private (`0700`) directory without
/// relative traversal or symlink ancestor components, returning its metadata.
pub fn validate_existing_private_dir_meta(dir: &Path) -> Result<Metadata, ForecastArtifactError> {
    if !dir.is_absolute() {
        return Err(ForecastArtifactError::new(
            "security_violation",
            "Private store directory path must be absolute.",
        ));
    }

    let mut current = PathBuf::new();
    for component in dir.components() {
        match component {
            Component::Prefix(prefix) => current.push(prefix.as_os_str()),
            Component::RootDir => current.push(component.as_os_str()),
            Component::CurDir | Component::ParentDir => {
                return Err(ForecastArtifactError::new(
                    "security_violation",
                    "Private store directory path must not contain relative traversal components.",
                ));
            }
            Component::Normal(part) => {
                current.push(part);
                let meta = fs::symlink_metadata(&current).map_err(|err| {
                    if err.kind() == io::ErrorKind::NotFound {
                        ForecastArtifactError::new(
                            "not_found",
                            "Required store directory is missing.",
                        )
                    } else {
                        ForecastArtifactError::new(
                            "io_error",
                            "Failed to inspect private store directory metadata.",
                        )
                    }
                })?;
                if meta.file_type().is_symlink() || !meta.file_type().is_dir() {
                    return Err(ForecastArtifactError::new(
                        "security_violation",
                        "Private store directory path must not traverse symlinks.",
                    ));
                }
            }
        }
    }

    let meta = fs::symlink_metadata(dir).map_err(|_| {
        ForecastArtifactError::new("io_error", "Failed to inspect private store directory.")
    })?;
    verify_private_dir_permissions(&meta, None)?;
    Ok(meta)
}

/// Validates an optional child directory under an already-verified private parent directory.
pub fn validate_optional_private_child_dir(
    child_dir: &Path,
    parent_meta: &Metadata,
) -> Result<Option<Metadata>, ForecastArtifactError> {
    let meta = match fs::symlink_metadata(child_dir) {
        Ok(meta) => meta,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(_) => {
            return Err(ForecastArtifactError::new(
                "io_error",
                "Failed to inspect forecast directory metadata.",
            ));
        }
    };
    if meta.file_type().is_symlink() || !meta.file_type().is_dir() {
        return Err(ForecastArtifactError::new(
            "security_violation",
            "Forecast directory path must be a real non-symlink directory.",
        ));
    }
    verify_private_dir_permissions(&meta, Some(parent_meta))?;
    Ok(Some(meta))
}

/// Verifies that `meta` describes a real private directory with no group/world bits and matching
/// owner UID when `parent_meta` is supplied.
pub fn verify_private_dir_permissions(
    meta: &Metadata,
    parent_meta: Option<&Metadata>,
) -> Result<(), ForecastArtifactError> {
    if meta.file_type().is_symlink() || !meta.file_type().is_dir() {
        return Err(ForecastArtifactError::new(
            "security_violation",
            "Directory path must be a real non-symlink directory.",
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        if (meta.permissions().mode() & 0o077) != 0 {
            return Err(ForecastArtifactError::new(
                "security_violation",
                "Directory permissions must not grant group or world access.",
            ));
        }
        if let Some(parent) = parent_meta {
            if meta.uid() != parent.uid() {
                return Err(ForecastArtifactError::new(
                    "security_violation",
                    "Directory owner does not match parent private directory owner.",
                ));
            }
        }
        Ok(())
    }
    #[cfg(not(unix))]
    {
        let _ = parent_meta;
        Err(ForecastArtifactError::new(
            "unsupported_platform",
            "Trusted local forecast inspection requires Unix private permissions.",
        ))
    }
}

/// Reads an optional private regular file bounded to `max_bytes`, rejecting symlinks, non-private
/// permissions, owner mismatches, and pre/post open inode changes.
pub fn read_private_file_optional(
    file_path: &Path,
    parent_meta: &Metadata,
    max_bytes: usize,
) -> Result<Option<Vec<u8>>, ForecastArtifactError> {
    let pre_meta = match fs::symlink_metadata(file_path) {
        Ok(meta) => meta,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(_) => {
            return Err(ForecastArtifactError::new(
                "io_error",
                "Failed to inspect private forecast file metadata.",
            ));
        }
    };
    verify_private_regular_file_metadata(&pre_meta, parent_meta, max_bytes)?;

    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }

    let file = options.open(file_path).map_err(map_nofollow_open_error)?;
    let post_meta = file.metadata().map_err(|_| {
        ForecastArtifactError::new("io_error", "Failed to stat opened private forecast file.")
    })?;
    verify_private_regular_file_metadata(&post_meta, parent_meta, max_bytes)?;
    verify_same_file_identity(&pre_meta, &post_meta)?;

    let mut bytes = Vec::with_capacity(
        usize::try_from(post_meta.len())
            .unwrap_or(max_bytes)
            .min(max_bytes),
    );
    file.take(max_bytes as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| {
            ForecastArtifactError::new("io_error", "Failed to read private forecast file bytes.")
        })?;
    if bytes.len() > max_bytes {
        return Err(ForecastArtifactError::new(
            "output_overflow",
            "Private forecast file exceeds maximum bounded size.",
        ));
    }

    Ok(Some(bytes))
}

fn verify_private_regular_file_metadata(
    meta: &Metadata,
    parent_meta: &Metadata,
    max_bytes: usize,
) -> Result<(), ForecastArtifactError> {
    if meta.file_type().is_symlink() || !meta.file_type().is_file() {
        return Err(ForecastArtifactError::new(
            "security_violation",
            "Forecast or advice path must be a non-symlink regular file.",
        ));
    }
    if meta.len() > max_bytes as u64 {
        return Err(ForecastArtifactError::new(
            "output_overflow",
            "Private forecast file exceeds maximum bounded size.",
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        if (meta.permissions().mode() & 0o077) != 0 {
            return Err(ForecastArtifactError::new(
                "security_violation",
                "Private forecast file must not grant group or world permissions.",
            ));
        }
        if meta.uid() != parent_meta.uid() {
            return Err(ForecastArtifactError::new(
                "security_violation",
                "Private forecast file owner does not match parent directory owner.",
            ));
        }
        Ok(())
    }
    #[cfg(not(unix))]
    {
        let _ = parent_meta;
        Err(ForecastArtifactError::new(
            "unsupported_platform",
            "Trusted local forecast inspection requires Unix private permissions.",
        ))
    }
}

/// Verifies that pre-open and post-open metadata refer to the exact same Unix device and inode.
pub fn verify_same_file_identity(
    pre: &Metadata,
    post: &Metadata,
) -> Result<(), ForecastArtifactError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if pre.dev() != post.dev() || pre.ino() != post.ino() {
            return Err(ForecastArtifactError::new(
                "security_violation",
                "File identity changed while opening private state file.",
            ));
        }
    }
    #[cfg(not(unix))]
    {
        let _ = (pre, post);
    }
    Ok(())
}

/// Maps an `O_NOFOLLOW` open failure into a sanitized [`ForecastArtifactError`].
pub fn map_nofollow_open_error(err: io::Error) -> ForecastArtifactError {
    #[cfg(unix)]
    if err.raw_os_error() == Some(libc::ELOOP) {
        return ForecastArtifactError::new(
            "security_violation",
            "Refusing to follow symlink while reading private state.",
        );
    }
    let _ = err;
    ForecastArtifactError::new("io_error", "Failed to open private state file read-only.")
}

fn map_workflow_difficulty(difficulty: Difficulty) -> WorkspaceDecisionDifficulty {
    match difficulty {
        Difficulty::Easy => WorkspaceDecisionDifficulty::Easy,
        Difficulty::Medium => WorkspaceDecisionDifficulty::Medium,
        Difficulty::Hard => WorkspaceDecisionDifficulty::Hard,
        Difficulty::Extreme => WorkspaceDecisionDifficulty::Extreme,
    }
}

fn hash_len_prefixed(hasher: &mut Sha256, field: &[u8]) {
    let len = field.len() as u64;
    hasher.update(len.to_le_bytes());
    hasher.update(field);
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    finalize_sha256_hex(hasher)
}

fn finalize_sha256_hex(hasher: Sha256) -> String {
    let digest = hasher.finalize();
    let mut out = String::with_capacity(64);
    for byte in digest {
        use std::fmt::Write as _;
        let _ = write!(&mut out, "{byte:02x}");
    }
    out
}

#[cfg(all(test, unix))]
mod tests {
    use std::fs;
    use std::os::unix::fs::{symlink, DirBuilderExt, OpenOptionsExt, PermissionsExt};
    use std::path::{Path, PathBuf};

    use serde_json::json;
    use tempfile::TempDir;
    use threadlane_authority::{
        compute_authority_core_digest, compute_record_subject_digest, derive_approval_id,
        derive_command_key, derive_profile_id, ApprovalRecord, ApprovedTaskRecord,
        APPROVALS_FILE_NAME, AUTHORITY_POLICY_VERSION, AUTHORITY_SCHEMA_VERSION,
    };
    use threadlane_workers::{WorkerFilePermissions, WorkerProvider};
    use threadlane_workflow::{Difficulty, ToolAdapter, WorkflowRole};

    use super::*;

    fn make_private_dir(path: &Path) {
        let mut builder = fs::DirBuilder::new();
        builder.mode(0o700);
        builder.create(path).expect("create private 0700 dir");
        fs::set_permissions(path, fs::Permissions::from_mode(0o700)).expect("chmod 0700");
    }

    fn write_private_json(path: &Path, value: &serde_json::Value) {
        let bytes = serde_json::to_vec_pretty(value).expect("serialize JSON");
        write_private_bytes(path, &bytes, 0o600);
    }

    fn write_private_bytes(path: &Path, bytes: &[u8], mode: u32) {
        use std::io::Write as _;
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(mode)
            .open(path)
            .expect("open private file");
        file.write_all(bytes).expect("write private bytes");
        file.sync_all().expect("sync private bytes");
        drop(file);
        fs::set_permissions(path, fs::Permissions::from_mode(mode)).expect("chmod file");
    }

    struct BadgeFixture {
        _tmp: TempDir,
        plans_dir: PathBuf,
        authority_dir: PathBuf,
        project_dir: PathBuf,
        binding_id: String,
        thread_uuid: String,
        approval: ApprovalRecord,
        authority: WorkerBacklogAuthority,
    }

    impl BadgeFixture {
        fn new(difficulty: Difficulty) -> Self {
            let tmp = TempDir::new().expect("tempdir");
            fs::set_permissions(tmp.path(), fs::Permissions::from_mode(0o700))
                .expect("chmod root 0700");
            let plans_dir = tmp.path().join("plans");
            let authority_dir = tmp.path().join("authority");
            let project_dir = tmp.path().join("repo");
            make_private_dir(&plans_dir);
            make_private_dir(&authority_dir);
            make_private_dir(&project_dir);

            let binding_id = "bnd_badge_unit_01".to_string();
            let thread_uuid = "0196ed00-0000-7000-8000-000000000b01".to_string();
            let task_digest = "2".repeat(64);
            let mut approval = ApprovalRecord {
                policy_version: AUTHORITY_POLICY_VERSION,
                approval_id: String::new(),
                subject_digest: String::new(),
                binding_id: binding_id.clone(),
                thread_uuid: thread_uuid.clone(),
                plan_id: "plan_badge_unit_01".to_string(),
                plan_revision: 3,
                plan_digest: "1".repeat(64),
                repository_path: project_dir.clone(),
                base_commit: "a".repeat(40),
                profile_id: "profile-main".to_string(),
                registry_revision: 4,
                approved_task_ids: vec!["task-badge-1".into()],
                approved_write_paths: vec!["src/lib.rs".into()],
                max_parallel: 1,
                created_at_ms: 1_700_000_000_000,
                budgets: Default::default(),
                tasks: vec![ApprovedTaskRecord {
                    task_id: "task-badge-1".to_string(),
                    task_digest: task_digest.clone(),
                    title: "Implement decision badge".to_string(),
                    instruction: "Secret instruction that must never be projected".to_string(),
                    role: WorkflowRole::Coder,
                    difficulty,
                    depends_on: Vec::new(),
                    allowed_write_paths: vec!["src/lib.rs".to_string()],
                    required_changed_paths: None,
                    instruction_digest: sha256_hex(
                        b"Secret instruction that must never be projected",
                    ),
                    provider: Some(WorkerProvider::Antigravity),
                    model_arg: "fake-legacy-model".into(),
                    cli_model_arg: Some("fake-legacy-model".into()),
                    standalone: Some(false),
                    file_permissions: Some(WorkerFilePermissions::TaskFiles),
                    tool_id: "tool-agy".to_string(),
                    model_id: "model-codex".to_string(),
                    adapter: ToolAdapter::Antigravity,
                    base_model: "fake-legacy-model".to_string(),
                    effort: Some("high".to_string()),
                    canonical_executable: Some(PathBuf::from("/opt/test/fake-antigravity")),
                    profile_id: None,
                    command_key: None,
                }],
            };
            let core_digest = compute_authority_core_digest(&approval).expect("core digest");
            for task in &mut approval.tasks {
                task.profile_id = Some(derive_profile_id(
                    &core_digest,
                    &task.task_id,
                    &task.task_digest,
                ));
                task.command_key = Some(derive_command_key(
                    &core_digest,
                    &task.task_id,
                    &task.task_digest,
                ));
            }
            approval.subject_digest =
                compute_record_subject_digest(&approval).expect("subject digest");
            approval.approval_id = derive_approval_id(&approval.subject_digest);

            threadlane_authority::verify_approval_record_integrity(
                &approval,
                &authority_dir.join(APPROVALS_FILE_NAME),
            )
            .expect("fixture approval integrity");
            let ledger = json!({
                "schema_version": AUTHORITY_SCHEMA_VERSION,
                "approvals": [approval],
            });
            write_private_json(&authority_dir.join(APPROVALS_FILE_NAME), &ledger);

            let authority = WorkerBacklogAuthority {
                approval_id: approval.approval_id.clone(),
                subject_digest: approval.subject_digest.clone(),
                task_id: "task-badge-1".to_string(),
                task_digest,
            };

            Self {
                _tmp: tmp,
                plans_dir,
                authority_dir,
                project_dir,
                binding_id,
                thread_uuid,
                approval,
                authority,
            }
        }

        fn subject_dir(&self) -> PathBuf {
            let root = self.plans_dir.join(FORECAST_SUBDIR);
            if !root.exists() {
                make_private_dir(&root);
            }
            let dir = root.join(&self.approval.subject_digest);
            if !dir.exists() {
                make_private_dir(&dir);
            }
            dir
        }

        fn scope(&self) -> DecisionBadgeLookupScope<'_> {
            DecisionBadgeLookupScope {
                plans_dir: &self.plans_dir,
                authority_dir: &self.authority_dir,
                binding_id: &self.binding_id,
                thread_uuid: &self.thread_uuid,
                canonical_project_scope: self.project_dir.to_str().expect("utf8 project path"),
                item_id: "blg_badge_unit_01",
                execution_ref: "wbe_00000000000000000000000000000001",
                authority: &self.authority,
            }
        }

        fn write_launched_record(
            &self,
            adapter: &str,
            tool_id: &str,
            model_id: &str,
            model_arg: &str,
            request_summary_sha: &str,
        ) {
            let subject_dir = self.subject_dir();
            write_private_json(
                &subject_dir.join(FORECAST_RECORD_FILENAME),
                &json!({
                    "schema_version": 1,
                    "advisory_only": true,
                    "mode": "shadow",
                    "binding_id": self.binding_id,
                    "thread_uuid": self.thread_uuid,
                    "plan_id": self.approval.plan_id,
                    "plan_revision": self.approval.plan_revision,
                    "task_id": "task-badge-1",
                    "profile_id": self.approval.profile_id,
                    "profile_revision": self.approval.registry_revision,
                    "base_commit": self.approval.base_commit,
                    "subject_digest": self.approval.subject_digest,
                    "baseline_difficulty": "medium",
                    "complexity": {
                        "component_count": 3,
                        "coupled": true,
                        "uncertain": false
                    },
                    "summary_sha256": "3".repeat(64),
                    "triggered": true,
                    "status": "launched",
                    "reason": "shadow_classify_launched",
                    "advisor": {
                        "tool_id": tool_id,
                        "model_id": model_id,
                        "model_arg": model_arg,
                        "adapter": adapter
                    },
                    "request_summary_sha256": request_summary_sha,
                    "request_file": "request.json",
                    "advice_file": "advice.json",
                    "pid_hint": 4242,
                    "recorded_at_ms": 1_700_000_000_100u64
                }),
            );
        }
    }

    #[test]
    fn launched_pending_transitions_to_fresh_and_cached_results_for_jev_and_openai() {
        let req_sha = "4".repeat(64);
        let fixture = BadgeFixture::new(Difficulty::Medium);
        fixture.write_launched_record("jev_api", "tool-jev", "model-jev", "jev-2.5", &req_sha);
        write_private_bytes(
            &fixture.subject_dir().join(FORECAST_ADVICE_FILENAME),
            b"",
            0o600,
        );

        let pending = project_verified_decision_badge_readonly(fixture.scope())
            .expect("confirmed launched consultation should project pending");
        assert_eq!(pending.provider, WorkspaceDecisionAdvisorProvider::Jev);
        assert_eq!(pending.status, WorkspaceDecisionBadgeStatus::Pending);
        assert_eq!(pending.recommended_difficulty, None);
        assert_eq!(
            pending.coworker_difficulty,
            Some(WorkspaceDecisionDifficulty::Medium)
        );
        assert_eq!(pending.advice_source, None);
        assert_eq!(pending.advisor_model.as_deref(), Some("jev-2.5"));

        // Write fresh provider result -> Hard (disagrees with coworker Medium).
        write_private_json(
            &fixture.subject_dir().join(FORECAST_ADVICE_FILENAME),
            &json!({
                "untrusted_data": true,
                "advisory_only": true,
                "mode": "shadow",
                "source": "provider",
                "context_id": fixture.thread_uuid,
                "task_id": "task-badge-1",
                "task_revision": fixture.approval.plan_revision,
                "registry_revision": fixture.approval.registry_revision,
                "profile_id": fixture.approval.profile_id,
                "tool_id": "tool-jev",
                "model_id": "model-jev",
                "requested_model": "jev-2.5",
                "summary_sha256": req_sha,
                "cache_key": "5".repeat(64),
                "advice": {
                    "provider": "jev",
                    "model": "jev-2.5-pro",
                    "difficulty": "hard",
                    "confidence": 0.91,
                    "needs_review": false,
                    "reason_code": "classified",
                    "input_tokens": 210,
                    "output_tokens": 12
                },
                "baseline_difficulty": "medium",
                "agrees_with_baseline": false,
                "provider_call_performed": true,
                "cache_age_ms": null,
                "elapsed_ms": 480
            }),
        );

        let fresh = project_verified_decision_badge_readonly(fixture.scope())
            .expect("validated provider advice should project result");
        assert_eq!(fresh.provider, WorkspaceDecisionAdvisorProvider::Jev);
        assert_eq!(fresh.status, WorkspaceDecisionBadgeStatus::Result);
        assert_eq!(
            fresh.recommended_difficulty,
            Some(WorkspaceDecisionDifficulty::Hard)
        );
        assert_eq!(
            fresh.coworker_difficulty,
            Some(WorkspaceDecisionDifficulty::Medium)
        );
        assert_eq!(
            fresh.advice_source,
            Some(WorkspaceDecisionAdviceSource::Fresh)
        );
        assert_eq!(fresh.cache_age_ms, None);
        assert_eq!(fresh.advisor_model.as_deref(), Some("jev-2.5-pro"));
        assert_eq!(fresh.agrees_with_coworker, Some(false));

        // OpenAI cached result -> Extreme.
        let openai_fixture = BadgeFixture::new(Difficulty::Medium);
        openai_fixture.write_launched_record(
            "openai_decisions_api",
            "tool-openai",
            "model-openai",
            "gpt-6-luna",
            &req_sha,
        );
        write_private_json(
            &openai_fixture.subject_dir().join(FORECAST_ADVICE_FILENAME),
            &json!({
                "untrusted_data": true,
                "advisory_only": true,
                "mode": "shadow",
                "source": "cache",
                "context_id": openai_fixture.thread_uuid,
                "task_id": "task-badge-1",
                "task_revision": openai_fixture.approval.plan_revision,
                "registry_revision": openai_fixture.approval.registry_revision,
                "profile_id": openai_fixture.approval.profile_id,
                "tool_id": "tool-openai",
                "model_id": "model-openai",
                "requested_model": "gpt-6-luna",
                "summary_sha256": req_sha,
                "cache_key": "6".repeat(64),
                "advice": {
                    "provider": "openai",
                    "model": "gpt-6-luna-2026-08-01",
                    "difficulty": "extreme",
                    "confidence": 0.87,
                    "needs_review": false,
                    "reason_code": "classified",
                    "input_tokens": 180,
                    "output_tokens": 9
                },
                "baseline_difficulty": "medium",
                "agrees_with_baseline": false,
                "provider_call_performed": false,
                "cache_age_ms": 24_500,
                "elapsed_ms": 4
            }),
        );
        let cached = project_verified_decision_badge_readonly(openai_fixture.scope())
            .expect("validated cached OpenAI advice should project result");
        assert_eq!(cached.provider, WorkspaceDecisionAdvisorProvider::Openai);
        assert_eq!(cached.status, WorkspaceDecisionBadgeStatus::Result);
        assert_eq!(
            cached.recommended_difficulty,
            Some(WorkspaceDecisionDifficulty::Extreme)
        );
        assert_eq!(
            cached.advice_source,
            Some(WorkspaceDecisionAdviceSource::Cached)
        );
        assert_eq!(cached.cache_age_ms, Some(24_500));
        assert_eq!(
            cached.advisor_model.as_deref(),
            Some("gpt-6-luna-2026-08-01")
        );
    }

    #[test]
    fn distinguishes_genuine_refusal_and_uncertain_cache_failure_while_hiding_skipped_and_tampered()
    {
        let req_sha = "7".repeat(64);

        // 1. Genuine refusal recorded in advice.json -> Failed with Fresh provenance and refusal code.
        let refusal_fixture = BadgeFixture::new(Difficulty::Medium);
        refusal_fixture.write_launched_record(
            "openai_decisions_api",
            "tool-openai",
            "model-openai",
            "gpt-6-luna",
            &req_sha,
        );
        write_private_json(
            &refusal_fixture.subject_dir().join(FORECAST_ADVICE_FILENAME),
            &json!({
                "untrusted_data": true,
                "advisory_only": true,
                "mode": "shadow",
                "source": "provider",
                "context_id": refusal_fixture.thread_uuid,
                "task_id": "task-badge-1",
                "task_revision": refusal_fixture.approval.plan_revision,
                "registry_revision": refusal_fixture.approval.registry_revision,
                "profile_id": refusal_fixture.approval.profile_id,
                "tool_id": "tool-openai",
                "model_id": "model-openai",
                "requested_model": "gpt-6-luna",
                "summary_sha256": req_sha,
                "cache_key": "8".repeat(64),
                "advice": {
                    "provider": "openai",
                    "model": "gpt-6-luna",
                    "difficulty": null,
                    "confidence": null,
                    "needs_review": true,
                    "reason_code": "refusal",
                    "input_tokens": 110,
                    "output_tokens": 0
                },
                "baseline_difficulty": "medium",
                "agrees_with_baseline": null,
                "provider_call_performed": true,
                "cache_age_ms": null,
                "elapsed_ms": 220
            }),
        );
        let refused = project_verified_decision_badge_readonly(refusal_fixture.scope())
            .expect("genuine refusal should project failed badge");
        assert_eq!(refused.status, WorkspaceDecisionBadgeStatus::Failed);
        assert_eq!(refused.recommended_difficulty, None);
        assert_eq!(
            refused.advice_source,
            Some(WorkspaceDecisionAdviceSource::Fresh)
        );
        assert_eq!(refused.failure_reason_code.as_deref(), Some("refusal"));

        // 2. Genuine post-launch transport/timeout failure recorded in advice-cache/<key>.json while advice.json is empty.
        let cache_fail_fixture = BadgeFixture::new(Difficulty::Medium);
        cache_fail_fixture.write_launched_record(
            "jev_api",
            "tool-jev",
            "model-jev",
            "jev-2.5",
            &req_sha,
        );
        write_private_bytes(
            &cache_fail_fixture
                .subject_dir()
                .join(FORECAST_ADVICE_FILENAME),
            b"",
            0o600,
        );
        let cache_dir = cache_fail_fixture.plans_dir.join(ADVICE_CACHE_SUBDIR);
        make_private_dir(&cache_dir);
        let project_str = cache_fail_fixture
            .project_dir
            .to_str()
            .expect("utf8 project");
        let prompt_digest =
            compute_shadow_advice_prompt_digest(WorkspaceDecisionAdvisorProvider::Jev);
        let cache_key = compute_shadow_advice_cache_key(
            project_str,
            &cache_fail_fixture.thread_uuid,
            "task-badge-1",
            cache_fail_fixture.approval.plan_revision,
            &req_sha,
            cache_fail_fixture.approval.registry_revision,
            WorkspaceDecisionAdvisorProvider::Jev,
            "tool-jev",
            "model-jev",
            "jev-2.5",
            Some(cache_fail_fixture.approval.profile_id.as_str()),
        );
        write_private_json(
            &cache_dir.join(format!("{cache_key}.json")),
            &json!({
                "schema_version": 1,
                "prompt_digest": prompt_digest,
                "cache_key": cache_key,
                "project_root_sha256": sha256_hex(project_str.as_bytes()),
                "context_id": cache_fail_fixture.thread_uuid,
                "task_id": "task-badge-1",
                "task_revision": cache_fail_fixture.approval.plan_revision,
                "summary_sha256": req_sha,
                "registry_revision": cache_fail_fixture.approval.registry_revision,
                "profile_id": cache_fail_fixture.approval.profile_id,
                "tool_id": "tool-jev",
                "model_id": "model-jev",
                "requested_model": "jev-2.5",
                "state": {
                    "status": "uncertain",
                    "attempt_id": "att-1-1-1",
                    "failed_at_ms": 1_700_000_000_500u64,
                    "error_code": "timeout"
                }
            }),
        );
        let failed = project_verified_decision_badge_readonly(cache_fail_fixture.scope())
            .expect("verified uncertain cache failure should project failed badge");
        assert_eq!(failed.provider, WorkspaceDecisionAdvisorProvider::Jev);
        assert_eq!(failed.status, WorkspaceDecisionBadgeStatus::Failed);
        assert_eq!(failed.advice_source, None);
        assert_eq!(failed.failure_reason_code.as_deref(), Some("timeout"));

        // 3. Skipped Forecast A -> None (no icon / no placeholder).
        let skipped_fixture = BadgeFixture::new(Difficulty::Easy);
        write_private_json(
            &skipped_fixture.subject_dir().join(FORECAST_RECORD_FILENAME),
            &json!({
                "schema_version": 1,
                "advisory_only": true,
                "mode": "shadow",
                "binding_id": skipped_fixture.binding_id,
                "thread_uuid": skipped_fixture.thread_uuid,
                "plan_id": skipped_fixture.approval.plan_id,
                "plan_revision": skipped_fixture.approval.plan_revision,
                "task_id": "task-badge-1",
                "profile_id": skipped_fixture.approval.profile_id,
                "profile_revision": skipped_fixture.approval.registry_revision,
                "base_commit": skipped_fixture.approval.base_commit,
                "subject_digest": skipped_fixture.approval.subject_digest,
                "baseline_difficulty": "easy",
                "complexity": {
                    "component_count": 1,
                    "coupled": false,
                    "uncertain": false
                },
                "summary_sha256": "3".repeat(64),
                "triggered": false,
                "status": "skipped",
                "reason": "simple_task",
                "recorded_at_ms": 1_700_000_000_100u64
            }),
        );
        assert!(project_verified_decision_badge_readonly(skipped_fixture.scope()).is_none());

        // 4. Truncated EOF advice.json (unproven) -> None.
        let truncated_fixture = BadgeFixture::new(Difficulty::Medium);
        truncated_fixture.write_launched_record(
            "jev_api",
            "tool-jev",
            "model-jev",
            "jev-2.5",
            &req_sha,
        );
        write_private_bytes(
            &truncated_fixture
                .subject_dir()
                .join(FORECAST_ADVICE_FILENAME),
            b"{\"untrusted_data\":true",
            0o600,
        );
        assert!(project_verified_decision_badge_readonly(truncated_fixture.scope()).is_none());

        // 5. Symlinked advice.json or group-readable record.json -> None.
        let symlink_fixture = BadgeFixture::new(Difficulty::Medium);
        symlink_fixture.write_launched_record(
            "jev_api",
            "tool-jev",
            "model-jev",
            "jev-2.5",
            &req_sha,
        );
        let outside = symlink_fixture._tmp.path().join("outside.json");
        write_private_bytes(&outside, b"", 0o600);
        symlink(
            &outside,
            symlink_fixture.subject_dir().join(FORECAST_ADVICE_FILENAME),
        )
        .expect("create advice symlink");
        assert!(project_verified_decision_badge_readonly(symlink_fixture.scope()).is_none());

        fs::remove_file(symlink_fixture.subject_dir().join(FORECAST_ADVICE_FILENAME))
            .expect("remove symlink");
        fs::set_permissions(
            symlink_fixture.subject_dir().join(FORECAST_RECORD_FILENAME),
            fs::Permissions::from_mode(0o644),
        )
        .expect("chmod 0644 record");
        assert!(project_verified_decision_badge_readonly(symlink_fixture.scope()).is_none());
    }
}
