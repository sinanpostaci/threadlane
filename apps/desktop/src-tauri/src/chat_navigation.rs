#[path = "codex_navigation_linux.rs"]
mod codex_navigation_linux;

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use tauri::{AppHandle, State, Window};
use threadlane_core::observation::{
    validate_correlation_id, ObservationSnapshotRequest, ObservationSource,
    ObservationSourceAvailability, WorkspaceChatIdentity,
    WorkspaceSnapshot as CoreWorkspaceSnapshot,
};
use tokio::sync::watch;
use uuid::Uuid;

use crate::observation::{ObservationService, MAX_REQUEST_PAGE_LIMIT};
use crate::tray::MAIN_WINDOW_LABEL;
use crate::AppError;

/// Canonical deep-link URI scheme and path prefix for opening a native Codex conversation thread.
pub(crate) const CODEX_THREAD_URI_PREFIX: &str = "codex://threads/";

/// Exact byte length of a canonical lowercase hyphenated UUID (`8-4-4-4-12`).
pub(crate) const CANONICAL_UUID_LEN: usize = 36;

/// Maximum byte length allowed for `source_id` or `job_id` navigation identifiers.
pub(crate) const MAX_NAVIGATION_ID_LEN: usize = 128;

/// Per-request page limit passed when querying [`ObservationService::workspace_snapshot`].
pub(crate) const CHAT_NAVIGATION_PAGE_LIMIT: usize = MAX_REQUEST_PAGE_LIMIT;

/// Short post-submission coalescing window for rapid duplicate clicks on the exact same chat.
pub(crate) const DEFAULT_SUBMISSION_COALESCE_WINDOW: Duration = Duration::from_millis(750);

/// Maximum number of recent clean `Submitted` timestamps retained for rapid-click coalescing.
pub(crate) const DEFAULT_MAX_RECENT_SUBMISSIONS: usize = 128;

/// Maximum number of concurrent in-flight chat activations tracked by a controller instance.
pub(crate) const DEFAULT_MAX_IN_FLIGHT_ACTIVATIONS: usize = 64;

/// Maximum number of uncertain (`Unknown`) chat targets latched in-process without eviction.
pub(crate) const DEFAULT_MAX_UNKNOWN_TARGETS: usize = 512;

/// Frozen frontend request payload for `desktop_open_codex_chat`.
///
/// Accepts only scoped identity keys (`source_id`, `thread_uuid`, and optional `job_id`).
/// Unknown fields (such as raw URLs, executables, shell argv, filesystem paths, socket paths,
/// environment overrides, approval flags, or model selectors) are rejected at deserialization time.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct OpenCodexChatRequest {
    pub(crate) source_id: String,
    pub(crate) thread_uuid: String,
    #[serde(default)]
    pub(crate) job_id: Option<String>,
}

/// Pre-validated and normalized request parameters prior to any snapshot read or activation effect.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ValidatedCodexChatRequest {
    pub(crate) source_id: String,
    pub(crate) thread_uuid: String,
    pub(crate) job_id: Option<String>,
}

/// Identifies which verified [`CoreWorkspaceSnapshot`] surface established the target chat identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum VerifiedChatMatchOrigin {
    /// Matched a verified [`threadlane_core::observation::WorkspaceChatSummary`] (including jobless chats).
    Chat,
    /// Matched a verified [`threadlane_core::observation::WorkspaceJobHistory`] entry scoped to the same chat.
    Job { job_id: String },
}

/// Verified target resolution outcome containing the full four-part [`WorkspaceChatIdentity`] and
/// canonical `codex://threads/<uuid>` URI.
///
/// Submitting this target to an existing Codex instance is a local transport activation attempt
/// only; it never records an operator approval, dispatches a worker, sends a conversation message,
/// or proves window viewing, delivery, or human reading.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ResolvedCodexChatTarget {
    pub(crate) chat_identity: WorkspaceChatIdentity,
    pub(crate) source_id: String,
    pub(crate) thread_uuid: String,
    pub(crate) job_id: Option<String>,
    pub(crate) canonical_uri: String,
    pub(crate) match_origin: VerifiedChatMatchOrigin,
}

/// Verified activation payload passed to a [`CodexActivationPort`] after main-window and
/// [`CoreWorkspaceSnapshot`] full-chat-scope verification succeed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CodexActivationRequest {
    pub(crate) chat_identity: WorkspaceChatIdentity,
    pub(crate) canonical_uri: String,
}

/// Transport- and compositor-level outcome of an existing-instance Codex activation attempt.
///
/// [`Self::Submitted`] means only that the verified local singleton socket accepted the `START`
/// frame with `ACK` and the supported local compositor confirmed focus on the unique window owned
/// by that same Codex PID; it never proves internal conversation rendering, message delivery,
/// operator approval, or human reading.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ActivationPortOutcome {
    /// Verified existing Codex instance accepted `START`/`ACK` and verified compositor focus succeeded.
    Submitted,
    /// Pre-send verification, focus preflight, or connection failed before any URI or focus bytes could be written.
    RejectedBeforeSend(AppError),
    /// Transport or compositor focus failed, timed out, or mismatched after URI or focus bytes may have been sent.
    UnknownAfterPossibleSend(AppError),
}

/// Outcome returned by [`ChatNavigationController`] for a validated activation request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum NavigationSubmissionOutcome {
    /// Exactly one existing-instance transport and focus activation completed.
    Submitted,
    /// Rapid or concurrent duplicate request for the exact same chat was coalesced without re-sending.
    CoalescedDuplicate,
}

pub(crate) type BoxActivationFuture<'a> =
    Pin<Box<dyn Future<Output = ActivationPortOutcome> + Send + 'a>>;

/// Injected existing-instance Codex activation boundary.
pub(crate) trait CodexActivationPort: Send + Sync {
    fn activate<'a>(&'a self, request: &'a CodexActivationRequest) -> BoxActivationFuture<'a>;
}

/// Production platform activation adapter.
///
/// Uses the qualified existing-instance Owl shell singleton transport paired with verified
/// local compositor focus (`Hyprland` or `Sway`) on Linux, keeps macOS explicitly gated until an
/// existing-instance activation API is qualified, and adds no Windows implementation or OS URL
/// opener / process-launch fallback.
#[derive(Debug, Clone, Default)]
pub(crate) struct PlatformCodexActivator {
    #[cfg(target_os = "linux")]
    linux: codex_navigation_linux::LinuxOwlCodexActivator,
}

impl PlatformCodexActivator {
    pub(crate) fn new() -> Self {
        Self {
            #[cfg(target_os = "linux")]
            linux: codex_navigation_linux::LinuxOwlCodexActivator::new(),
        }
    }
}

impl CodexActivationPort for PlatformCodexActivator {
    fn activate<'a>(&'a self, request: &'a CodexActivationRequest) -> BoxActivationFuture<'a> {
        #[cfg(target_os = "linux")]
        {
            Box::pin(self.linux.activate(request))
        }
        #[cfg(target_os = "macos")]
        {
            let _ = request;
            Box::pin(async {
                ActivationPortOutcome::RejectedBeforeSend(AppError::new(
                    "codex_navigation_unsupported",
                    "Existing-instance Codex chat activation is not yet qualified on macOS; refusing to launch a new application instance.",
                ))
            })
        }
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        {
            let _ = request;
            Box::pin(async {
                ActivationPortOutcome::RejectedBeforeSend(AppError::new(
                    "codex_navigation_unsupported",
                    "Existing-instance Codex chat activation is unsupported on this platform.",
                ))
            })
        }
    }
}

/// Enforces that chat navigation requests originate exclusively from the primary `main` window.
pub(crate) fn ensure_main_window_caller(window_label: &str) -> Result<(), AppError> {
    if window_label == MAIN_WINDOW_LABEL {
        Ok(())
    } else {
        Err(AppError::new(
            "forbidden_surface",
            "Opening a Codex chat is only permitted from the main Threadlane window.",
        ))
    }
}

/// Validates that `raw` is a canonical lowercase hyphenated non-nil UUID (`8-4-4-4-12`).
///
/// Rejects leading/trailing whitespace, control characters, uppercase hex, unhyphenated or braced
/// representations, nil UUIDs, query strings, fragments, path segments, and oversized inputs.
pub(crate) fn validate_canonical_codex_thread_uuid(raw: &str) -> Result<String, AppError> {
    if raw.is_empty()
        || raw != raw.trim()
        || raw.len() != CANONICAL_UUID_LEN
        || raw
            .bytes()
            .any(|b| b.is_ascii_whitespace() || b.is_ascii_control())
        || raw.chars().any(|ch| {
            ch.is_whitespace()
                || ch.is_control()
                || matches!(ch, '?' | '#' | '/' | '\\' | '%' | ':' | '@')
        })
    {
        return Err(AppError::new(
            "invalid_thread_uuid",
            "Thread UUID must be a canonical lowercase hyphenated non-nil UUID.",
        ));
    }

    let parsed = Uuid::parse_str(raw).map_err(|_| {
        AppError::new(
            "invalid_thread_uuid",
            "Thread UUID must be a canonical lowercase hyphenated non-nil UUID.",
        )
    })?;

    if parsed.is_nil() {
        return Err(AppError::new(
            "invalid_thread_uuid",
            "Thread UUID must be a canonical lowercase hyphenated non-nil UUID.",
        ));
    }

    let canonical = parsed.hyphenated().to_string();
    if raw != canonical {
        return Err(AppError::new(
            "invalid_thread_uuid",
            "Thread UUID must be a canonical lowercase hyphenated non-nil UUID.",
        ));
    }

    Ok(canonical)
}

/// Builds the canonical `codex://threads/<canonicalUuid>` deep link after verifying the UUID.
pub(crate) fn build_codex_thread_uri(canonical_thread_uuid: &str) -> Result<String, AppError> {
    let validated_uuid = validate_canonical_codex_thread_uuid(canonical_thread_uuid)?;
    Ok(format!("{CODEX_THREAD_URI_PREFIX}{validated_uuid}"))
}

/// Verifies that a URI string is strictly `codex://threads/<canonicalUuid>` before transport send.
pub(crate) fn verify_canonical_codex_thread_uri(uri: &str) -> Result<String, AppError> {
    let uuid_part = uri.strip_prefix(CODEX_THREAD_URI_PREFIX).ok_or_else(|| {
        AppError::new(
            "invalid_thread_uuid",
            "Target URI must use the canonical codex://threads/<uuid> format.",
        )
    })?;
    build_codex_thread_uri(uuid_part)
}

fn validate_strict_navigation_id(
    raw: &str,
    field_name: &'static str,
    allow_colon: bool,
) -> Result<String, AppError> {
    let invalid_err = || {
        AppError::new(
            "invalid_input",
            format!("Chat navigation {field_name} must be a valid canonical identifier."),
        )
    };

    if raw.is_empty()
        || raw != raw.trim()
        || raw.len() > MAX_NAVIGATION_ID_LEN
        || raw == "."
        || raw == ".."
        || raw.contains("..")
        || raw.starts_with('.')
        || raw.ends_with('.')
        || raw.starts_with(':')
        || raw.ends_with(':')
        || raw
            .bytes()
            .any(|b| b.is_ascii_whitespace() || b.is_ascii_control())
        || raw.chars().any(|ch| {
            ch.is_whitespace()
                || ch.is_control()
                || matches!(ch, '?' | '#' | '/' | '\\' | '%' | '@' | '&' | '=')
        })
    {
        return Err(invalid_err());
    }

    if !allow_colon && raw.contains(':') {
        return Err(invalid_err());
    }

    match validate_correlation_id(raw) {
        Some(validated) if validated == raw => Ok(validated),
        _ => Err(invalid_err()),
    }
}

/// Validates an [`OpenCodexChatRequest`] before any snapshot read or activation effect occurs.
pub(crate) fn validate_open_codex_chat_request(
    request: &OpenCodexChatRequest,
) -> Result<ValidatedCodexChatRequest, AppError> {
    let source_id = validate_strict_navigation_id(&request.source_id, "source_id", false)?;
    let thread_uuid = validate_canonical_codex_thread_uuid(&request.thread_uuid)?;
    let job_id = match request.job_id.as_deref() {
        Some(raw_job_id) => Some(validate_strict_navigation_id(raw_job_id, "job_id", true)?),
        None => None,
    };

    Ok(ValidatedCodexChatRequest {
        source_id,
        thread_uuid,
        job_id,
    })
}

fn target_scope_unverified_error() -> AppError {
    AppError::new(
        "target_scope_unverified",
        "Requested Codex chat does not have a verified binding and project scope. Refresh and try again.",
    )
}

fn target_scope_mismatch_error() -> AppError {
    AppError::new(
        "target_scope_mismatch",
        "Observed chat or job scope did not match the verified binding and project scope. Refresh and try again.",
    )
}

fn ambiguous_chat_target_error() -> AppError {
    AppError::new(
        "ambiguous_chat_target",
        "Multiple bindings or project scopes matched the requested Codex chat thread; refusing ambiguous activation.",
    )
}

fn target_not_found_error() -> AppError {
    AppError::new(
        "target_not_found",
        "Requested Codex chat target was not found in the current workspace snapshot. Refresh and try again.",
    )
}

fn target_thread_mismatch_error() -> AppError {
    AppError::new(
        "target_thread_mismatch",
        "Observed job does not match the requested Codex chat thread. Refresh and try again.",
    )
}

fn verify_source_available_in_workspace(
    snapshot: &CoreWorkspaceSnapshot,
    source_id: &str,
) -> Result<ObservationSource, AppError> {
    let source = snapshot
        .observation
        .sources
        .iter()
        .find(|candidate| candidate.id == source_id)
        .ok_or_else(|| {
            AppError::new(
                "source_not_found",
                "Requested observation source is not available in the current view. Refresh and try again.",
            )
        })?;

    if source.availability != ObservationSourceAvailability::Available {
        return Err(AppError::new(
            "source_unavailable",
            "Requested observation source is currently unavailable. Refresh and try again.",
        ));
    }

    Ok(source.clone())
}

fn verify_identity_against_source(
    identity: &WorkspaceChatIdentity,
    expected_source: &ObservationSource,
    expected_thread_uuid: &str,
) -> Result<(), AppError> {
    if identity.source_id != expected_source.id || identity.thread_uuid != expected_thread_uuid {
        return Err(target_scope_mismatch_error());
    }
    if identity.validate().is_err() {
        return Err(target_scope_unverified_error());
    }
    if let Some(ref src_project_path) = expected_source.project_path {
        if &identity.canonical_project_scope != src_project_path {
            return Err(target_scope_mismatch_error());
        }
    }
    Ok(())
}

struct CollectedSourceThreadIdentities {
    chat_summary_identities: BTreeSet<WorkspaceChatIdentity>,
    all_identities: BTreeSet<WorkspaceChatIdentity>,
    first_matching_job_id: Option<String>,
}

fn collect_verified_thread_identities(
    snapshot: &CoreWorkspaceSnapshot,
    source: &ObservationSource,
    validated: &ValidatedCodexChatRequest,
) -> Result<CollectedSourceThreadIdentities, AppError> {
    let mut chat_summary_identities = BTreeSet::new();
    let mut all_identities = BTreeSet::new();
    let mut first_matching_job_id: Option<String> = None;

    for chat in &snapshot.chats {
        if chat.identity.source_id == validated.source_id
            && chat.identity.thread_uuid == validated.thread_uuid
        {
            verify_identity_against_source(&chat.identity, source, &validated.thread_uuid)?;
            chat_summary_identities.insert(chat.identity.clone());
            all_identities.insert(chat.identity.clone());
        }
    }

    for history in &snapshot.job_histories {
        if history.job.source_id != validated.source_id {
            continue;
        }
        if let Some(ref ci) = history.job.chat_identity {
            if ci.thread_uuid == validated.thread_uuid {
                verify_identity_against_source(ci, source, &validated.thread_uuid)?;
                if let Some(ref jb) = history.job.binding_id {
                    if jb != &ci.binding_id {
                        return Err(target_scope_mismatch_error());
                    }
                }
                if first_matching_job_id.is_none()
                    && validate_strict_navigation_id(&history.job.job_id, "job_id", true).is_ok()
                {
                    first_matching_job_id = Some(history.job.job_id.clone());
                }
                all_identities.insert(ci.clone());
            }
        }
    }

    Ok(CollectedSourceThreadIdentities {
        chat_summary_identities,
        all_identities,
        first_matching_job_id,
    })
}

fn has_unverified_legacy_thread_reference(
    snapshot: &CoreWorkspaceSnapshot,
    validated: &ValidatedCodexChatRequest,
) -> bool {
    let in_obs_jobs = snapshot.observation.jobs.iter().any(|job| {
        job.source_id == validated.source_id
            && job.thread_uuid.as_deref() == Some(validated.thread_uuid.as_str())
    });
    let in_obs_requests = snapshot.observation.requests.iter().any(|req| {
        req.source_id == validated.source_id
            && req.thread_uuid.as_deref() == Some(validated.thread_uuid.as_str())
    });
    in_obs_jobs || in_obs_requests
}

fn resolve_validated_in_workspace_snapshot(
    snapshot: &CoreWorkspaceSnapshot,
    validated: &ValidatedCodexChatRequest,
    prior_thread_identities: &BTreeSet<WorkspaceChatIdentity>,
) -> Result<ResolvedCodexChatTarget, AppError> {
    let source = verify_source_available_in_workspace(snapshot, &validated.source_id)?;
    let collected = collect_verified_thread_identities(snapshot, &source, validated)?;

    let mut combined_identities = prior_thread_identities.clone();
    combined_identities.extend(collected.all_identities.iter().cloned());

    let canonical_uri = build_codex_thread_uri(&validated.thread_uuid)?;

    if let Some(ref expected_job_id) = validated.job_id {
        let matching_histories: Vec<_> = snapshot
            .job_histories
            .iter()
            .filter(|jh| {
                jh.job.source_id == validated.source_id && &jh.job.job_id == expected_job_id
            })
            .collect();

        if matching_histories.is_empty() {
            if let Some(legacy_job) = snapshot
                .observation
                .jobs
                .iter()
                .find(|j| j.source_id == validated.source_id && &j.id == expected_job_id)
            {
                if legacy_job.thread_uuid.as_deref() == Some(validated.thread_uuid.as_str()) {
                    return Err(target_scope_unverified_error());
                }
                return Err(target_thread_mismatch_error());
            }
            return Err(target_not_found_error());
        }

        let mut matched_job_identities = BTreeSet::new();
        for history in &matching_histories {
            let Some(ref job_ci) = history.job.chat_identity else {
                let legacy_matches_thread = snapshot.observation.jobs.iter().any(|j| {
                    j.source_id == validated.source_id
                        && &j.id == expected_job_id
                        && j.thread_uuid.as_deref() == Some(validated.thread_uuid.as_str())
                });
                if legacy_matches_thread {
                    return Err(target_scope_unverified_error());
                }
                return Err(target_thread_mismatch_error());
            };

            if job_ci.source_id != validated.source_id {
                return Err(target_scope_mismatch_error());
            }
            if job_ci.thread_uuid != validated.thread_uuid {
                return Err(target_thread_mismatch_error());
            }
            verify_identity_against_source(job_ci, &source, &validated.thread_uuid)?;
            if let Some(ref jb) = history.job.binding_id {
                if jb != &job_ci.binding_id {
                    return Err(target_scope_mismatch_error());
                }
            }
            matched_job_identities.insert(job_ci.clone());
        }

        if matched_job_identities.len() > 1 {
            return Err(ambiguous_chat_target_error());
        }

        let Some(job_identity) = matched_job_identities.into_iter().next() else {
            return Err(target_not_found_error());
        };

        if collected.chat_summary_identities.len() > 1 {
            return Err(ambiguous_chat_target_error());
        }
        if let Some(chat_ci) = collected.chat_summary_identities.iter().next() {
            if chat_ci != &job_identity {
                return Err(target_scope_mismatch_error());
            }
        }
        if combined_identities.len() > 1
            || (!combined_identities.is_empty() && !combined_identities.contains(&job_identity))
        {
            return Err(ambiguous_chat_target_error());
        }

        return Ok(ResolvedCodexChatTarget {
            chat_identity: job_identity,
            source_id: validated.source_id.clone(),
            thread_uuid: validated.thread_uuid.clone(),
            job_id: Some(expected_job_id.clone()),
            canonical_uri,
            match_origin: VerifiedChatMatchOrigin::Job {
                job_id: expected_job_id.clone(),
            },
        });
    }

    if combined_identities.len() > 1 {
        return Err(ambiguous_chat_target_error());
    }

    let Some(unique_identity) = combined_identities.into_iter().next() else {
        if has_unverified_legacy_thread_reference(snapshot, validated) {
            return Err(target_scope_unverified_error());
        }
        return Err(target_not_found_error());
    };

    let match_origin = if collected.chat_summary_identities.contains(&unique_identity) {
        VerifiedChatMatchOrigin::Chat
    } else if let Some(job_id) = collected.first_matching_job_id {
        VerifiedChatMatchOrigin::Job { job_id }
    } else {
        VerifiedChatMatchOrigin::Chat
    };

    Ok(ResolvedCodexChatTarget {
        chat_identity: unique_identity,
        source_id: validated.source_id.clone(),
        thread_uuid: validated.thread_uuid.clone(),
        job_id: None,
        canonical_uri,
        match_origin,
    })
}

/// Resolves a Codex chat target using a bounded [`CoreWorkspaceSnapshot`] fetcher.
///
/// Enforces [`ensure_main_window_caller`] and [`validate_open_codex_chat_request`] before invoking
/// `fetch_workspace_snapshot`. Performs one source-scoped snapshot read (plus at most one targeted
/// `job_id` read when `job_id` is supplied and job histories were capped), preserving source-wide
/// chat identity uniqueness across both reads.
pub(crate) fn resolve_codex_chat_target_with_fetcher<F>(
    window_label: &str,
    request: &OpenCodexChatRequest,
    mut fetch_workspace_snapshot: F,
) -> Result<ResolvedCodexChatTarget, AppError>
where
    F: FnMut(Option<ObservationSnapshotRequest>) -> Result<CoreWorkspaceSnapshot, AppError>,
{
    ensure_main_window_caller(window_label)?;
    let validated = validate_open_codex_chat_request(request)?;

    let initial_snapshot = fetch_workspace_snapshot(Some(ObservationSnapshotRequest {
        source_id: Some(validated.source_id.clone()),
        job_id: None,
        cursor: None,
        limit: Some(CHAT_NAVIGATION_PAGE_LIMIT),
    }))?;

    let empty_prior = BTreeSet::new();
    match resolve_validated_in_workspace_snapshot(&initial_snapshot, &validated, &empty_prior) {
        Ok(resolved) => Ok(resolved),
        Err(err)
            if err.code == "target_not_found"
                && validated.job_id.is_some()
                && (initial_snapshot.bounds.job_histories_omitted > 0
                    || initial_snapshot.bounds.jobs_capped
                    || initial_snapshot.observation.jobs_has_more) =>
        {
            let source =
                verify_source_available_in_workspace(&initial_snapshot, &validated.source_id)?;
            let prior = collect_verified_thread_identities(&initial_snapshot, &source, &validated)?;
            if prior.all_identities.len() > 1 {
                return Err(ambiguous_chat_target_error());
            }

            let targeted_snapshot = fetch_workspace_snapshot(Some(ObservationSnapshotRequest {
                source_id: Some(validated.source_id.clone()),
                job_id: validated.job_id.clone(),
                cursor: None,
                limit: Some(CHAT_NAVIGATION_PAGE_LIMIT),
            }))?;

            resolve_validated_in_workspace_snapshot(
                &targeted_snapshot,
                &validated,
                &prior.all_identities,
            )
        }
        Err(err) => Err(err),
    }
}

/// Resolves a Codex chat target against a live read-only [`ObservationService`] via
/// `ObservationService::workspace_snapshot`.
pub(crate) fn resolve_codex_chat_target(
    window_label: &str,
    observation: &ObservationService,
    request: &OpenCodexChatRequest,
) -> Result<ResolvedCodexChatTarget, AppError> {
    resolve_codex_chat_target_with_fetcher(window_label, request, |snapshot_request| {
        observation.workspace_snapshot(snapshot_request)
    })
}

fn canonical_safe_navigation_code(raw_code: &str) -> &'static str {
    match raw_code {
        "codex_app_unavailable" => "codex_app_unavailable",
        "codex_runtime_unsupported" => "codex_runtime_unsupported",
        "codex_navigation_unsupported" => "codex_navigation_unsupported",
        "codex_profile_ambiguous" => "codex_profile_ambiguous",
        "codex_window_ambiguous" => "codex_window_ambiguous",
        "codex_focus_unavailable" => "codex_focus_unavailable",
        "codex_activation_unknown" => "codex_activation_unknown",
        "codex_activation_invalid_frame" => "codex_activation_invalid_frame",
        "codex_activation_frame_too_large" => "codex_activation_frame_too_large",
        "codex_activation_capacity_exhausted" => "codex_activation_capacity_exhausted",
        "target_scope_unverified" => "target_scope_unverified",
        "target_scope_mismatch" => "target_scope_mismatch",
        "invalid_thread_uuid" => "invalid_thread_uuid",
        "internal_task_error" => "internal_task_error",
        _ => "open_codex_chat_failed",
    }
}

fn fallback_message_for_safe_code(safe_code: &'static str) -> &'static str {
    match safe_code {
        "codex_app_unavailable" => {
            "No verified running Codex desktop instance is available for chat activation."
        }
        "codex_runtime_unsupported" => {
            "Running Codex instance is not using the qualified immutable Owl shell runtime."
        }
        "codex_navigation_unsupported" => {
            "Existing-instance Codex chat activation is unsupported on this platform or runtime."
        }
        "codex_profile_ambiguous" => {
            "Multiple candidate Codex desktop profiles were detected; refusing to guess which instance to activate."
        }
        "codex_window_ambiguous" => {
            "Multiple eligible Codex desktop windows were detected; refusing to guess which window to focus."
        }
        "codex_focus_unavailable" => {
            "Verified Codex desktop window could not be prepared for focus in the active compositor."
        }
        "codex_activation_unknown" => {
            "Codex chat activation outcome is uncertain after transport or focus submission; automatic retry is disabled."
        }
        "codex_activation_invalid_frame" => {
            "Codex chat activation request failed transport framing validation."
        }
        "codex_activation_frame_too_large" => {
            "Codex chat activation frame exceeded the 4 KiB transport bound."
        }
        "codex_activation_capacity_exhausted" => {
            "Codex chat activation controller reached its uncertain-outcome safety limit."
        }
        "target_scope_unverified" => {
            "Requested Codex chat does not have a verified binding and project scope."
        }
        "target_scope_mismatch" => {
            "Observed chat target did not match the verified binding and project scope."
        }
        "invalid_thread_uuid" => {
            "Target URI must use the canonical codex://threads/<uuid> format."
        }
        "internal_task_error" => "Background desktop worker task failed.",
        _ => "Unable to activate the requested chat in an existing verified Codex desktop instance.",
    }
}

/// Sanitizes an activation failure so that raw filesystem paths, socket paths, cookies, window
/// addresses, or OS environment details are never exposed in public diagnostics.
pub(crate) fn sanitize_navigation_error(err: AppError) -> AppError {
    let safe_code = canonical_safe_navigation_code(&err.code);
    let msg = err.message.trim();
    let msg_without_uri_placeholder = msg.replace("codex://threads/<uuid>", "");
    let lower = msg_without_uri_placeholder.to_ascii_lowercase();

    let contains_sensitive_detail = msg.is_empty()
        || msg_without_uri_placeholder.contains('/')
        || msg_without_uri_placeholder.contains('\\')
        || lower.contains("nix/store")
        || lower.contains("singletoncookie")
        || lower.contains("singletonsocket")
        || lower.contains("singletonlock")
        || lower.contains("xdg_config_home")
        || lower.contains("xdg_runtime_dir")
        || lower.contains("hyprland_instance_signature")
        || lower.contains("swaysock")
        || lower.contains(".socket.sock")
        || lower.contains(".socket2.sock")
        || lower.contains("sway-ipc")
        || lower.contains("0x")
        || lower.contains("con_id")
        || lower.contains("home")
        || lower.contains("users")
        || lower.contains("cookie=");

    if contains_sensitive_detail {
        AppError::new(safe_code, fallback_message_for_safe_code(safe_code))
    } else {
        AppError::new(safe_code, msg)
    }
}

/// Verifies consistency of a [`ResolvedCodexChatTarget`] against the [`ValidatedCodexChatRequest`]
/// and constructs the exact [`CodexActivationRequest`] for the activation port.
pub(crate) fn build_verified_activation_request(
    resolved: &ResolvedCodexChatTarget,
    validated: &ValidatedCodexChatRequest,
) -> Result<(String, CodexActivationRequest), AppError> {
    if resolved.chat_identity.validate().is_err() {
        return Err(target_scope_unverified_error());
    }
    let Some(chat_key) = resolved.chat_identity.canonical_key() else {
        return Err(target_scope_unverified_error());
    };

    if resolved.source_id != validated.source_id
        || resolved.thread_uuid != validated.thread_uuid
        || resolved.job_id != validated.job_id
        || resolved.chat_identity.source_id != validated.source_id
        || resolved.chat_identity.thread_uuid != validated.thread_uuid
    {
        return Err(target_thread_mismatch_error());
    }

    match &resolved.match_origin {
        VerifiedChatMatchOrigin::Job { job_id } => {
            if job_id.trim().is_empty()
                || validated
                    .job_id
                    .as_ref()
                    .is_some_and(|expected| expected != job_id)
            {
                return Err(target_thread_mismatch_error());
            }
        }
        VerifiedChatMatchOrigin::Chat => {
            if validated.job_id.is_some() {
                return Err(target_thread_mismatch_error());
            }
        }
    }

    let expected_uri = build_codex_thread_uri(&validated.thread_uuid)?;
    let validated_uri = verify_canonical_codex_thread_uri(&resolved.canonical_uri)?;
    if validated_uri != expected_uri {
        return Err(AppError::new(
            "invalid_thread_uuid",
            "Target URI must use the canonical codex://threads/<uuid> format.",
        ));
    }

    Ok((
        chat_key,
        CodexActivationRequest {
            chat_identity: resolved.chat_identity.clone(),
            canonical_uri: validated_uri,
        },
    ))
}

/// Configuration bounds for [`ChatNavigationController`].
#[derive(Debug, Clone, Copy)]
pub(crate) struct ChatNavigationControllerConfig {
    pub(crate) coalesce_window: Duration,
    pub(crate) max_recent_submissions: usize,
    pub(crate) max_in_flight: usize,
    pub(crate) max_unknown_targets: usize,
}

impl Default for ChatNavigationControllerConfig {
    fn default() -> Self {
        Self {
            coalesce_window: DEFAULT_SUBMISSION_COALESCE_WINDOW,
            max_recent_submissions: DEFAULT_MAX_RECENT_SUBMISSIONS,
            max_in_flight: DEFAULT_MAX_IN_FLIGHT_ACTIVATIONS,
            max_unknown_targets: DEFAULT_MAX_UNKNOWN_TARGETS,
        }
    }
}

#[derive(Debug, Clone)]
enum InFlightResolution {
    Submitted,
    Failed(AppError),
}

struct NavigationControllerState {
    in_flight: BTreeMap<String, watch::Receiver<Option<InFlightResolution>>>,
    recent_submissions: VecDeque<(String, Instant)>,
    unknown_latches: BTreeMap<String, AppError>,
}

impl NavigationControllerState {
    fn new() -> Self {
        Self {
            in_flight: BTreeMap::new(),
            recent_submissions: VecDeque::new(),
            unknown_latches: BTreeMap::new(),
        }
    }

    fn prune_expired_submissions(&mut self, now: Instant, window: Duration) {
        if window.is_zero() {
            self.recent_submissions.clear();
            return;
        }
        while let Some(&(_, submitted_at)) = self.recent_submissions.front() {
            if now.saturating_duration_since(submitted_at) >= window {
                self.recent_submissions.pop_front();
            } else {
                break;
            }
        }
    }
}

enum DispatchDecision {
    AlreadyUnknown(AppError),
    CoalescedRecent,
    JoinInFlight(watch::Receiver<Option<InFlightResolution>>),
    ExecuteLeader(watch::Sender<Option<InFlightResolution>>),
}

struct LeaderInFlightGuard<'a> {
    state: &'a Mutex<NavigationControllerState>,
    chat_key: String,
    tx: Option<watch::Sender<Option<InFlightResolution>>>,
    entered_port: bool,
}

impl<'a> LeaderInFlightGuard<'a> {
    fn finish(
        mut self,
        outcome: ActivationPortOutcome,
        config: ChatNavigationControllerConfig,
    ) -> Result<NavigationSubmissionOutcome, AppError> {
        let tx = self.tx.take();
        let mut guard = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        guard.in_flight.remove(&self.chat_key);

        match outcome {
            ActivationPortOutcome::Submitted => {
                if !config.coalesce_window.is_zero() && config.max_recent_submissions > 0 {
                    let now = Instant::now();
                    guard.prune_expired_submissions(now, config.coalesce_window);
                    guard
                        .recent_submissions
                        .retain(|(existing_key, _)| existing_key != &self.chat_key);
                    while guard.recent_submissions.len() >= config.max_recent_submissions {
                        guard.recent_submissions.pop_front();
                    }
                    guard
                        .recent_submissions
                        .push_back((self.chat_key.clone(), now));
                }
                drop(guard);
                if let Some(sender) = tx {
                    let _ = sender.send(Some(InFlightResolution::Submitted));
                }
                Ok(NavigationSubmissionOutcome::Submitted)
            }
            ActivationPortOutcome::RejectedBeforeSend(raw_err) => {
                let sanitized = sanitize_navigation_error(raw_err);
                drop(guard);
                if let Some(sender) = tx {
                    let _ = sender.send(Some(InFlightResolution::Failed(sanitized.clone())));
                }
                Err(sanitized)
            }
            ActivationPortOutcome::UnknownAfterPossibleSend(raw_err) => {
                let sanitized_raw = sanitize_navigation_error(raw_err);
                let latched = AppError::new(
                    "codex_activation_unknown",
                    if sanitized_raw.code == "codex_activation_unknown" {
                        sanitized_raw.message
                    } else {
                        fallback_message_for_safe_code("codex_activation_unknown").to_string()
                    },
                );
                guard
                    .recent_submissions
                    .retain(|(existing_key, _)| existing_key != &self.chat_key);
                guard
                    .unknown_latches
                    .insert(self.chat_key.clone(), latched.clone());
                drop(guard);
                if let Some(sender) = tx {
                    let _ = sender.send(Some(InFlightResolution::Failed(latched.clone())));
                }
                Err(latched)
            }
        }
    }
}

impl Drop for LeaderInFlightGuard<'_> {
    fn drop(&mut self) {
        let Some(sender) = self.tx.take() else {
            return;
        };
        let mut guard = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        guard.in_flight.remove(&self.chat_key);

        let abort_err = if self.entered_port {
            let unknown_err = AppError::new(
                "codex_activation_unknown",
                fallback_message_for_safe_code("codex_activation_unknown"),
            );
            guard
                .recent_submissions
                .retain(|(existing_key, _)| existing_key != &self.chat_key);
            guard
                .unknown_latches
                .insert(self.chat_key.clone(), unknown_err.clone());
            unknown_err
        } else {
            AppError::internal_join_error()
        };
        drop(guard);
        let _ = sender.send(Some(InFlightResolution::Failed(abort_err)));
    }
}

/// Bounded in-process controller coordinating existing-instance Codex chat activations.
///
/// Coalesces concurrent and rapid duplicate clicks per full four-part [`WorkspaceChatIdentity`],
/// latches uncertain post-send outcomes (`Unknown`) without eviction so they are never replayed,
/// and stores zero conversation/viewing/reading receipts.
pub(crate) struct ChatNavigationController<P> {
    port: P,
    config: ChatNavigationControllerConfig,
    state: Arc<Mutex<NavigationControllerState>>,
}

impl<P: CodexActivationPort> ChatNavigationController<P> {
    pub(crate) fn new(port: P) -> Self {
        Self::with_config(port, ChatNavigationControllerConfig::default())
    }

    pub(crate) fn with_config(port: P, config: ChatNavigationControllerConfig) -> Self {
        Self {
            port,
            config,
            state: Arc::new(Mutex::new(NavigationControllerState::new())),
        }
    }

    /// Validates consistency between `resolved` and `validated` and submits or coalesces activation
    /// through the injected [`CodexActivationPort`].
    pub(crate) async fn activate_resolved(
        &self,
        resolved: &ResolvedCodexChatTarget,
        validated: &ValidatedCodexChatRequest,
    ) -> Result<NavigationSubmissionOutcome, AppError> {
        let (chat_key, activation_request) =
            build_verified_activation_request(resolved, validated)?;

        let decision = {
            let mut guard = self
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());

            if let Some(_existing_unknown) = guard.unknown_latches.get(&chat_key) {
                DispatchDecision::AlreadyUnknown(AppError::new(
                    "codex_activation_unknown",
                    "Previous Codex chat activation for this conversation had an uncertain outcome; automatic or duplicate re-send is blocked.",
                ))
            } else if let Some(rx) = guard.in_flight.get(&chat_key) {
                DispatchDecision::JoinInFlight(rx.clone())
            } else {
                let now = Instant::now();
                guard.prune_expired_submissions(now, self.config.coalesce_window);
                if guard
                    .recent_submissions
                    .iter()
                    .any(|(recent_key, _)| recent_key == &chat_key)
                {
                    DispatchDecision::CoalescedRecent
                } else if guard.in_flight.len() >= self.config.max_in_flight
                    || guard
                        .unknown_latches
                        .len()
                        .saturating_add(guard.in_flight.len())
                        >= self.config.max_unknown_targets
                {
                    return Err(AppError::new(
                        "codex_activation_capacity_exhausted",
                        fallback_message_for_safe_code("codex_activation_capacity_exhausted"),
                    ));
                } else {
                    let (tx, rx) = watch::channel(None);
                    guard.in_flight.insert(chat_key.clone(), rx);
                    DispatchDecision::ExecuteLeader(tx)
                }
            }
        };

        match decision {
            DispatchDecision::AlreadyUnknown(err) => Err(err),
            DispatchDecision::CoalescedRecent => {
                Ok(NavigationSubmissionOutcome::CoalescedDuplicate)
            }
            DispatchDecision::JoinInFlight(mut rx) => loop {
                if let Some(resolution) = rx.borrow().clone() {
                    return match resolution {
                        InFlightResolution::Submitted => {
                            Ok(NavigationSubmissionOutcome::CoalescedDuplicate)
                        }
                        InFlightResolution::Failed(err) => Err(err),
                    };
                }
                if rx.changed().await.is_err() {
                    if let Some(resolution) = rx.borrow().clone() {
                        return match resolution {
                            InFlightResolution::Submitted => {
                                Ok(NavigationSubmissionOutcome::CoalescedDuplicate)
                            }
                            InFlightResolution::Failed(err) => Err(err),
                        };
                    }
                    return Err(AppError::internal_join_error());
                }
            },
            DispatchDecision::ExecuteLeader(tx) => {
                let mut leader_guard = LeaderInFlightGuard {
                    state: &self.state,
                    chat_key,
                    tx: Some(tx),
                    entered_port: true,
                };
                let outcome = self.port.activate(&activation_request).await;
                leader_guard.entered_port = false;
                leader_guard.finish(outcome, self.config)
            }
        }
    }

    /// End-to-end helper parameterized over a synchronous [`CoreWorkspaceSnapshot`] fetcher and
    /// this controller's injected [`CodexActivationPort`].
    #[cfg(test)]
    pub(crate) async fn open_codex_chat_with_fetcher<F>(
        &self,
        window_label: &str,
        request: &OpenCodexChatRequest,
        fetch_workspace_snapshot: F,
    ) -> Result<(ResolvedCodexChatTarget, NavigationSubmissionOutcome), AppError>
    where
        F: FnMut(Option<ObservationSnapshotRequest>) -> Result<CoreWorkspaceSnapshot, AppError>,
    {
        ensure_main_window_caller(window_label)?;
        let validated = validate_open_codex_chat_request(request)?;
        let resolved = resolve_codex_chat_target_with_fetcher(
            window_label,
            request,
            fetch_workspace_snapshot,
        )?;
        let submission = self.activate_resolved(&resolved, &validated).await?;
        Ok((resolved, submission))
    }

    #[cfg(test)]
    pub(crate) fn unknown_latch_count(&self) -> usize {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .unknown_latches
            .len()
    }
}

fn production_navigation_controller() -> &'static ChatNavigationController<PlatformCodexActivator> {
    static CONTROLLER: OnceLock<ChatNavigationController<PlatformCodexActivator>> = OnceLock::new();
    CONTROLLER.get_or_init(|| ChatNavigationController::new(PlatformCodexActivator::new()))
}

/// Validates and resolves an observed `(source_id, thread_uuid, job_id)` target against
/// [`ObservationService::workspace_snapshot`] and activates the conversation in an existing
/// verified Codex desktop instance from the primary `main` window.
///
/// Never launches a new Codex process or OS URL opener, never mutates desktop environment/protocol
/// handlers, and never records conversation viewing, delivery, or human reading.
#[tauri::command]
pub(crate) async fn desktop_open_codex_chat(
    app: AppHandle,
    window: Window,
    observation: State<'_, ObservationService>,
    request: OpenCodexChatRequest,
) -> Result<(), AppError> {
    let _ = app;
    ensure_main_window_caller(window.label())?;
    let validated = validate_open_codex_chat_request(&request)?;
    let window_label = window.label().to_string();
    let service = observation.inner().clone();
    let request_for_worker = request.clone();

    let resolved = tauri::async_runtime::spawn_blocking(move || {
        resolve_codex_chat_target(&window_label, &service, &request_for_worker)
    })
    .await
    .map_err(|_| AppError::internal_join_error())??;

    let _submission = production_navigation_controller()
        .activate_resolved(&resolved, &validated)
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque as StdVecDeque;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use threadlane_core::observation::{
        ObservationActor, ObservationCallState, ObservationJobState, ObservationRequest,
        ObservationSnapshot, WorkspaceAvailabilityState, WorkspaceChatSummary,
        WorkspaceConfiguredSelection, WorkspaceJobHistory, WorkspaceJobSummary,
        WorkspaceSnapshotBounds, OBSERVATION_SCHEMA_VERSION,
    };
    use tokio::sync::Notify;

    const VALID_UUID_1: &str = "550e8400-e29b-41d4-a716-446655440000";
    const VALID_UUID_2: &str = "6ba7b810-9dad-11d1-80b4-00c04fd430c8";
    const VALID_UUID_3: &str = "f47ac10b-58cc-4372-a567-0e02b2c3d479";

    #[derive(Clone)]
    struct FakeActivationPort {
        calls: Arc<AtomicUsize>,
        recorded_requests: Arc<Mutex<Vec<CodexActivationRequest>>>,
        scripted_outcomes: Arc<Mutex<StdVecDeque<ActivationPortOutcome>>>,
        default_outcome: ActivationPortOutcome,
        gate: Option<(Arc<Notify>, Arc<Notify>)>,
    }

    impl FakeActivationPort {
        fn new(default_outcome: ActivationPortOutcome) -> Self {
            Self {
                calls: Arc::new(AtomicUsize::new(0)),
                recorded_requests: Arc::new(Mutex::new(Vec::new())),
                scripted_outcomes: Arc::new(Mutex::new(StdVecDeque::new())),
                default_outcome,
                gate: None,
            }
        }

        fn scripted(
            outcomes: impl IntoIterator<Item = ActivationPortOutcome>,
            fallback: ActivationPortOutcome,
        ) -> Self {
            Self {
                calls: Arc::new(AtomicUsize::new(0)),
                recorded_requests: Arc::new(Mutex::new(Vec::new())),
                scripted_outcomes: Arc::new(Mutex::new(outcomes.into_iter().collect())),
                default_outcome: fallback,
                gate: None,
            }
        }

        fn gated(default_outcome: ActivationPortOutcome) -> (Self, Arc<Notify>, Arc<Notify>) {
            let entered = Arc::new(Notify::new());
            let release = Arc::new(Notify::new());
            let port = Self {
                calls: Arc::new(AtomicUsize::new(0)),
                recorded_requests: Arc::new(Mutex::new(Vec::new())),
                scripted_outcomes: Arc::new(Mutex::new(StdVecDeque::new())),
                default_outcome,
                gate: Some((Arc::clone(&entered), Arc::clone(&release))),
            };
            (port, entered, release)
        }

        fn call_count(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }

        fn requests(&self) -> Vec<CodexActivationRequest> {
            self.recorded_requests
                .lock()
                .expect("lock requests")
                .clone()
        }
    }

    impl CodexActivationPort for FakeActivationPort {
        fn activate<'a>(&'a self, request: &'a CodexActivationRequest) -> BoxActivationFuture<'a> {
            Box::pin(async move {
                self.calls.fetch_add(1, Ordering::SeqCst);
                self.recorded_requests
                    .lock()
                    .expect("lock requests")
                    .push(request.clone());

                if let Some((ref entered, ref release)) = self.gate {
                    entered.notify_waiters();
                    release.notified().await;
                }

                self.scripted_outcomes
                    .lock()
                    .expect("lock scripted")
                    .pop_front()
                    .unwrap_or_else(|| self.default_outcome.clone())
            })
        }
    }

    fn fixture_source(
        id: &str,
        availability: ObservationSourceAvailability,
        project_path: Option<&str>,
    ) -> ObservationSource {
        ObservationSource {
            id: id.to_string(),
            label: format!("Source {id}"),
            project_path: project_path.map(str::to_string),
            availability,
            message: None,
        }
    }

    fn fixture_chat_identity(
        source_id: &str,
        binding_id: &str,
        thread_uuid: &str,
        project_scope: &str,
    ) -> WorkspaceChatIdentity {
        WorkspaceChatIdentity::new(source_id, binding_id, thread_uuid, project_scope)
            .expect("valid fixture WorkspaceChatIdentity")
    }

    fn fixture_chat_summary(identity: WorkspaceChatIdentity) -> WorkspaceChatSummary {
        WorkspaceChatSummary {
            identity,
            thread_label: "Main Chat".to_string(),
            project_label: "Threadlane".to_string(),
            availability: WorkspaceAvailabilityState::Available,
            configured_selection: WorkspaceConfiguredSelection::unavailable(),
            ..WorkspaceChatSummary::default()
        }
    }

    fn fixture_job_history(
        source_id: &str,
        job_id: &str,
        binding_id: Option<&str>,
        chat_identity: Option<WorkspaceChatIdentity>,
        title: &str,
        summary: Option<&str>,
    ) -> WorkspaceJobHistory {
        WorkspaceJobHistory {
            job: WorkspaceJobSummary {
                source_id: source_id.to_string(),
                job_id: job_id.to_string(),
                binding_id: binding_id.map(str::to_string),
                chat_identity,
                title: title.to_string(),
                state: ObservationJobState::Ready,
                approval_status: "approved".to_string(),
                execution_status: "not_started".to_string(),
                updated_at_ms: Some(1_700_000_000_000),
                summary: summary.map(str::to_string),
                ..WorkspaceJobSummary::default()
            },
            ..WorkspaceJobHistory::default()
        }
    }

    fn fixture_workspace_snapshot(
        sources: Vec<ObservationSource>,
        chats: Vec<WorkspaceChatSummary>,
        job_histories: Vec<WorkspaceJobHistory>,
    ) -> CoreWorkspaceSnapshot {
        let obs = ObservationSnapshot {
            schema_version: OBSERVATION_SCHEMA_VERSION,
            generated_at_ms: 1_700_000_010_000,
            sources,
            jobs: Vec::new(),
            requests: Vec::new(),
            next_cursor: None,
            has_more: false,
            jobs_has_more: false,
            warnings: Vec::new(),
            warning_contexts: Vec::new(),
        };
        let mut ws = CoreWorkspaceSnapshot::new(1_700_000_010_000, obs);
        ws.chats = chats;
        ws.job_histories = job_histories;
        ws
    }

    #[test]
    fn request_serde_denies_unknown_fields_and_defaults_nullable_job_id() {
        let with_null: OpenCodexChatRequest = serde_json::from_str(&format!(
            r#"{{"source_id":"src_a","thread_uuid":"{VALID_UUID_1}","job_id":null}}"#
        ))
        .expect("deserialize with null job_id");
        assert_eq!(with_null.source_id, "src_a");
        assert_eq!(with_null.thread_uuid, VALID_UUID_1);
        assert_eq!(with_null.job_id, None);

        let omitted_job: OpenCodexChatRequest = serde_json::from_str(&format!(
            r#"{{"source_id":"src_a","thread_uuid":"{VALID_UUID_1}"}}"#
        ))
        .expect("deserialize with omitted job_id");
        assert_eq!(omitted_job.job_id, None);

        let with_job: OpenCodexChatRequest = serde_json::from_str(&format!(
            r#"{{"source_id":"src_a","thread_uuid":"{VALID_UUID_1}","job_id":"plan-1"}}"#
        ))
        .expect("deserialize with job_id");
        assert_eq!(with_job.job_id.as_deref(), Some("plan-1"));

        for forbidden_payload in [
            format!(
                r#"{{"source_id":"src_a","thread_uuid":"{VALID_UUID_1}","url":"codex://threads/{VALID_UUID_1}"}}"#
            ),
            format!(
                r#"{{"source_id":"src_a","thread_uuid":"{VALID_UUID_1}","uri":"codex://threads/{VALID_UUID_1}"}}"#
            ),
            format!(
                r#"{{"source_id":"src_a","thread_uuid":"{VALID_UUID_1}","executable":"/bin/sh"}}"#
            ),
            format!(
                r#"{{"source_id":"src_a","thread_uuid":"{VALID_UUID_1}","argv":["--danger"]}}"#
            ),
            format!(
                r#"{{"source_id":"src_a","thread_uuid":"{VALID_UUID_1}","project_path":"/tmp"}}"#
            ),
            format!(
                r#"{{"source_id":"src_a","thread_uuid":"{VALID_UUID_1}","binding_id":"bnd_1"}}"#
            ),
            format!(
                r#"{{"source_id":"src_a","thread_uuid":"{VALID_UUID_1}","canonical_project_scope":"/repo"}}"#
            ),
            format!(
                r#"{{"source_id":"src_a","thread_uuid":"{VALID_UUID_1}","socket_path":"/tmp/sock"}}"#
            ),
            format!(
                r#"{{"source_id":"src_a","thread_uuid":"{VALID_UUID_1}","env":{{"HOME":"/tmp"}}}}"#
            ),
            format!(r#"{{"source_id":"src_a","thread_uuid":"{VALID_UUID_1}","approved":true}}"#),
            format!(r#"{{"source_id":"src_a","thread_uuid":"{VALID_UUID_1}","model":"gpt-5"}}"#),
        ] {
            assert!(
                serde_json::from_str::<OpenCodexChatRequest>(&forbidden_payload).is_err(),
                "expected unknown field payload to be rejected: {forbidden_payload}"
            );
        }
    }

    #[tokio::test]
    async fn invalid_and_noncanonical_ids_or_uris_are_rejected_pre_effect() {
        let invalid_uuids = [
            "",
            "   ",
            "00000000-0000-0000-0000-000000000000",
            "550E8400-E29B-41D4-A716-446655440000",
            "550e8400-E29B-41d4-a716-446655440000",
            "550e8400e29b41d4a716446655440000",
            "{550e8400-e29b-41d4-a716-446655440000}",
            "urn:uuid:550e8400-e29b-41d4-a716-446655440000",
            "codex://threads/550e8400-e29b-41d4-a716-446655440000",
            " 550e8400-e29b-41d4-a716-446655440000",
            "550e8400-e29b-41d4-a716-446655440000 ",
            "550e8400-e29b-41d4-a716-446655440000\n",
            "550e8400-e29b-41d4-a716-446655440000\0",
            "550e8400-e29b-41d4-a716-446655440000?approve=1",
            "550e8400-e29b-41d4-a716-446655440000#frag",
            "550e8400-e29b-41d4-a716-446655440000/../etc",
            "550e8400-e29b-41d4-a716-446655440000%00",
        ];

        for bad_uuid in invalid_uuids {
            let port = FakeActivationPort::new(ActivationPortOutcome::Submitted);
            let controller = ChatNavigationController::new(port.clone());
            let fetch_calls = AtomicUsize::new(0);
            let req = OpenCodexChatRequest {
                source_id: "src_a".to_string(),
                thread_uuid: bad_uuid.to_string(),
                job_id: Some("plan-1".to_string()),
            };

            let err = controller
                .open_codex_chat_with_fetcher(MAIN_WINDOW_LABEL, &req, |_q| {
                    fetch_calls.fetch_add(1, Ordering::SeqCst);
                    unreachable!("snapshot must not be queried when thread_uuid is invalid");
                })
                .await
                .expect_err("invalid thread_uuid must be rejected");

            assert_eq!(err.code, "invalid_thread_uuid", "failed on {bad_uuid:?}");
            assert_eq!(fetch_calls.load(Ordering::SeqCst), 0);
            assert_eq!(port.call_count(), 0);
        }

        for bad_uri in [
            "",
            "https://openai.com/threads/550e8400-e29b-41d4-a716-446655440000",
            "file:///etc/passwd",
            "codex://threads/",
            "codex://threads/00000000-0000-0000-0000-000000000000",
            "codex://threads/550E8400-E29B-41D4-A716-446655440000",
            "codex://threads/550e8400-e29b-41d4-a716-446655440000/extra",
            "codex://threads/550e8400-e29b-41d4-a716-446655440000?cmd=sh",
        ] {
            assert_eq!(
                verify_canonical_codex_thread_uri(bad_uri)
                    .expect_err("non-canonical URI must fail")
                    .code,
                "invalid_thread_uuid"
            );
        }

        let oversized = "a".repeat(MAX_NAVIGATION_ID_LEN + 1);
        for bad_source in [
            "",
            "   ",
            " src_a",
            "src_a ",
            "src\na",
            "src\0a",
            "src?query=1",
            "src#frag",
            "src/path",
            "src\\path",
            "src%20a",
            "..",
            ".",
            "a..b",
            "codex:threads",
            "sk-secret-12345",
            oversized.as_str(),
        ] {
            let port = FakeActivationPort::new(ActivationPortOutcome::Submitted);
            let controller = ChatNavigationController::new(port.clone());
            let fetch_calls = AtomicUsize::new(0);
            let req = OpenCodexChatRequest {
                source_id: bad_source.to_string(),
                thread_uuid: VALID_UUID_1.to_string(),
                job_id: None,
            };

            let err = controller
                .open_codex_chat_with_fetcher(MAIN_WINDOW_LABEL, &req, |_q| {
                    fetch_calls.fetch_add(1, Ordering::SeqCst);
                    unreachable!("snapshot must not be queried when source_id is invalid");
                })
                .await
                .expect_err("invalid source_id must be rejected");

            assert_eq!(err.code, "invalid_input", "failed on {bad_source:?}");
            assert_eq!(fetch_calls.load(Ordering::SeqCst), 0);
            assert_eq!(port.call_count(), 0);
        }

        for bad_job in [
            "",
            "   ",
            " plan-1",
            "plan-1 ",
            "plan\n1",
            "plan?x=1",
            "plan#frag",
            "plan/1",
            "..",
            "sk-secret-12345",
            oversized.as_str(),
        ] {
            let port = FakeActivationPort::new(ActivationPortOutcome::Submitted);
            let controller = ChatNavigationController::new(port.clone());
            let fetch_calls = AtomicUsize::new(0);
            let req = OpenCodexChatRequest {
                source_id: "src_a".to_string(),
                thread_uuid: VALID_UUID_1.to_string(),
                job_id: Some(bad_job.to_string()),
            };

            let err = controller
                .open_codex_chat_with_fetcher(MAIN_WINDOW_LABEL, &req, |_q| {
                    fetch_calls.fetch_add(1, Ordering::SeqCst);
                    unreachable!("snapshot must not be queried when job_id is invalid");
                })
                .await
                .expect_err("invalid job_id must be rejected");

            assert_eq!(err.code, "invalid_input", "failed on {bad_job:?}");
            assert_eq!(fetch_calls.load(Ordering::SeqCst), 0);
            assert_eq!(port.call_count(), 0);
        }
    }

    #[tokio::test]
    async fn wrong_window_label_is_rejected_pre_effect_with_zero_reads_or_sends() {
        for bad_window in ["tray", "", " main", "main ", "other", "popover"] {
            let port = FakeActivationPort::new(ActivationPortOutcome::Submitted);
            let controller = ChatNavigationController::new(port.clone());
            let fetch_calls = AtomicUsize::new(0);
            let req = OpenCodexChatRequest {
                source_id: "src_a".to_string(),
                thread_uuid: VALID_UUID_1.to_string(),
                job_id: None,
            };

            let err = controller
                .open_codex_chat_with_fetcher(bad_window, &req, |_q| {
                    fetch_calls.fetch_add(1, Ordering::SeqCst);
                    unreachable!("wrong window must not query workspace_snapshot");
                })
                .await
                .expect_err("non-main window must be rejected");

            assert_eq!(err.code, "forbidden_surface");
            assert_eq!(fetch_calls.load(Ordering::SeqCst), 0);
            assert_eq!(port.call_count(), 0);
        }
    }

    #[tokio::test]
    async fn verified_jobless_chat_and_matching_job_chat_activate_with_exact_canonical_uri() {
        let jobless_identity =
            fixture_chat_identity("src_a", "bnd_jobless", VALID_UUID_1, "/workspace/repo-a");
        let job_chat_identity =
            fixture_chat_identity("src_a", "bnd_with_job", VALID_UUID_2, "/workspace/repo-a");

        let snap = fixture_workspace_snapshot(
            vec![fixture_source(
                "src_a",
                ObservationSourceAvailability::Available,
                Some("/workspace/repo-a"),
            )],
            vec![
                fixture_chat_summary(jobless_identity.clone()),
                fixture_chat_summary(job_chat_identity.clone()),
            ],
            vec![fixture_job_history(
                "src_a",
                "plan-42",
                Some("bnd_with_job"),
                Some(job_chat_identity.clone()),
                "Job 42",
                Some("Verified job"),
            )],
        );

        let port = FakeActivationPort::new(ActivationPortOutcome::Submitted);
        let controller = ChatNavigationController::new(port.clone());

        // 1. Verified jobless chat (`job_id: None`, zero jobs for VALID_UUID_1).
        let (resolved_jobless, sub_jobless) = controller
            .open_codex_chat_with_fetcher(
                MAIN_WINDOW_LABEL,
                &OpenCodexChatRequest {
                    source_id: "src_a".to_string(),
                    thread_uuid: VALID_UUID_1.to_string(),
                    job_id: None,
                },
                |_q| Ok(snap.clone()),
            )
            .await
            .expect("verified jobless chat must succeed");

        assert_eq!(sub_jobless, NavigationSubmissionOutcome::Submitted);
        assert_eq!(resolved_jobless.chat_identity, jobless_identity);
        assert_eq!(resolved_jobless.match_origin, VerifiedChatMatchOrigin::Chat);
        assert_eq!(
            resolved_jobless.canonical_uri,
            format!("codex://threads/{VALID_UUID_1}")
        );

        // 2. Verified job-scoped chat (`job_id: Some("plan-42")`).
        let (resolved_job, sub_job) = controller
            .open_codex_chat_with_fetcher(
                MAIN_WINDOW_LABEL,
                &OpenCodexChatRequest {
                    source_id: "src_a".to_string(),
                    thread_uuid: VALID_UUID_2.to_string(),
                    job_id: Some("plan-42".to_string()),
                },
                |_q| Ok(snap.clone()),
            )
            .await
            .expect("verified job-scoped chat must succeed");

        assert_eq!(sub_job, NavigationSubmissionOutcome::Submitted);
        assert_eq!(resolved_job.chat_identity, job_chat_identity);
        assert_eq!(
            resolved_job.match_origin,
            VerifiedChatMatchOrigin::Job {
                job_id: "plan-42".to_string()
            }
        );
        assert_eq!(
            resolved_job.canonical_uri,
            format!("codex://threads/{VALID_UUID_2}")
        );

        assert_eq!(port.call_count(), 2);
        let sent = port.requests();
        assert_eq!(
            sent[0],
            CodexActivationRequest {
                chat_identity: jobless_identity,
                canonical_uri: format!("codex://threads/{VALID_UUID_1}"),
            }
        );
        assert_eq!(
            sent[1],
            CodexActivationRequest {
                chat_identity: job_chat_identity,
                canonical_uri: format!("codex://threads/{VALID_UUID_2}"),
            }
        );
    }

    #[tokio::test]
    async fn wrong_source_binding_project_job_unverified_legacy_and_ambiguous_chats_fail_with_zero_sends(
    ) {
        let port = FakeActivationPort::new(ActivationPortOutcome::Submitted);
        let controller = ChatNavigationController::new(port.clone());

        // 1. Missing source and unavailable source.
        let unavail_snap = fixture_workspace_snapshot(
            vec![fixture_source(
                "src_down",
                ObservationSourceAvailability::Unavailable,
                None,
            )],
            Vec::new(),
            Vec::new(),
        );
        assert_eq!(
            controller
                .open_codex_chat_with_fetcher(
                    MAIN_WINDOW_LABEL,
                    &OpenCodexChatRequest {
                        source_id: "src_missing".to_string(),
                        thread_uuid: VALID_UUID_1.to_string(),
                        job_id: None,
                    },
                    |_q| Ok(unavail_snap.clone()),
                )
                .await
                .expect_err("missing source must fail")
                .code,
            "source_not_found"
        );
        assert_eq!(
            controller
                .open_codex_chat_with_fetcher(
                    MAIN_WINDOW_LABEL,
                    &OpenCodexChatRequest {
                        source_id: "src_down".to_string(),
                        thread_uuid: VALID_UUID_1.to_string(),
                        job_id: None,
                    },
                    |_q| Ok(unavail_snap.clone()),
                )
                .await
                .expect_err("unavailable source must fail")
                .code,
            "source_unavailable"
        );

        // 2. Source-only legacy observation request/job without verified WorkspaceChatIdentity -> target_scope_unverified (no fallback).
        let mut legacy_only_snap = fixture_workspace_snapshot(
            vec![fixture_source(
                "src_a",
                ObservationSourceAvailability::Available,
                None,
            )],
            Vec::new(),
            Vec::new(),
        );
        legacy_only_snap
            .observation
            .requests
            .push(ObservationRequest {
                source_id: "src_a".to_string(),
                id: "obsreq_legacy".to_string(),
                job_id: None,
                parent_request_id: None,
                title: "Legacy request".to_string(),
                operation: "mcp.workflow.status".to_string(),
                project_label: "Local".to_string(),
                binding_id: Some("bnd_1".to_string()),
                thread_uuid: Some(VALID_UUID_1.to_string()),
                thread_label: Some("Main".to_string()),
                state: ObservationCallState::Completed,
                started_at_ms: Some(1_700_000_000_000),
                finished_at_ms: Some(1_700_000_000_025),
                updated_at_ms: 1_700_000_000_025,
                duration_ms: Some(25),
                actor: ObservationActor::paired_coworker("Main"),
                executor: None,
                usage: None,
                delivery: Vec::new(),
                children: Vec::new(),
                children_count: 0,
                summary: Some("Completed".to_string()),
            });
        assert_eq!(
            controller
                .open_codex_chat_with_fetcher(
                    MAIN_WINDOW_LABEL,
                    &OpenCodexChatRequest {
                        source_id: "src_a".to_string(),
                        thread_uuid: VALID_UUID_1.to_string(),
                        job_id: None,
                    },
                    |_q| Ok(legacy_only_snap.clone()),
                )
                .await
                .expect_err("source-only legacy request without verified chat scope must fail")
                .code,
            "target_scope_unverified"
        );

        // 3. Ambiguous chat: two distinct bindings for same (source_id, thread_uuid).
        let ambig_binding_snap = fixture_workspace_snapshot(
            vec![fixture_source(
                "src_a",
                ObservationSourceAvailability::Available,
                None,
            )],
            vec![
                fixture_chat_summary(fixture_chat_identity(
                    "src_a",
                    "bnd_one",
                    VALID_UUID_1,
                    "/workspace/repo-a",
                )),
                fixture_chat_summary(fixture_chat_identity(
                    "src_a",
                    "bnd_two",
                    VALID_UUID_1,
                    "/workspace/repo-a",
                )),
            ],
            Vec::new(),
        );
        assert_eq!(
            controller
                .open_codex_chat_with_fetcher(
                    MAIN_WINDOW_LABEL,
                    &OpenCodexChatRequest {
                        source_id: "src_a".to_string(),
                        thread_uuid: VALID_UUID_1.to_string(),
                        job_id: None,
                    },
                    |_q| Ok(ambig_binding_snap.clone()),
                )
                .await
                .expect_err("ambiguous binding must fail")
                .code,
            "ambiguous_chat_target"
        );

        // 4. Ambiguous chat: two distinct canonical_project_scope values for same (source_id, thread_uuid).
        let ambig_project_snap = fixture_workspace_snapshot(
            vec![fixture_source(
                "src_a",
                ObservationSourceAvailability::Available,
                None,
            )],
            vec![
                fixture_chat_summary(fixture_chat_identity(
                    "src_a",
                    "bnd_same",
                    VALID_UUID_1,
                    "/workspace/repo-a",
                )),
                fixture_chat_summary(fixture_chat_identity(
                    "src_a",
                    "bnd_same",
                    VALID_UUID_1,
                    "/workspace/repo-b",
                )),
            ],
            Vec::new(),
        );
        assert_eq!(
            controller
                .open_codex_chat_with_fetcher(
                    MAIN_WINDOW_LABEL,
                    &OpenCodexChatRequest {
                        source_id: "src_a".to_string(),
                        thread_uuid: VALID_UUID_1.to_string(),
                        job_id: None,
                    },
                    |_q| Ok(ambig_project_snap.clone()),
                )
                .await
                .expect_err("ambiguous project scope must fail")
                .code,
            "ambiguous_chat_target"
        );

        // 5. Source project_path disagrees with chat identity canonical_project_scope -> target_scope_mismatch.
        let wrong_src_project_snap = fixture_workspace_snapshot(
            vec![fixture_source(
                "src_ctx",
                ObservationSourceAvailability::Available,
                Some("/workspace/expected-repo"),
            )],
            vec![fixture_chat_summary(fixture_chat_identity(
                "src_ctx",
                "bnd_1",
                VALID_UUID_1,
                "/workspace/other-repo",
            ))],
            Vec::new(),
        );
        assert_eq!(
            controller
                .open_codex_chat_with_fetcher(
                    MAIN_WINDOW_LABEL,
                    &OpenCodexChatRequest {
                        source_id: "src_ctx".to_string(),
                        thread_uuid: VALID_UUID_1.to_string(),
                        job_id: None,
                    },
                    |_q| Ok(wrong_src_project_snap.clone()),
                )
                .await
                .expect_err("mismatched source project_path must fail")
                .code,
            "target_scope_mismatch"
        );

        // 6. Job scope / thread / prose mismatches:
        //    - `plan-other-thread` belongs to VALID_UUID_2
        //    - `plan-prose` has chat_identity: None and mentions VALID_UUID_1 in title/summary
        //    - `plan-wrong-binding` has binding `bnd_other` conflicting with chat `bnd_main`
        let main_chat_ci =
            fixture_chat_identity("src_a", "bnd_main", VALID_UUID_1, "/workspace/repo-a");
        let other_thread_ci =
            fixture_chat_identity("src_a", "bnd_main", VALID_UUID_2, "/workspace/repo-a");
        let conflicting_job_ci =
            fixture_chat_identity("src_a", "bnd_other", VALID_UUID_3, "/workspace/repo-a");
        let chat_uuid3_main_ci =
            fixture_chat_identity("src_a", "bnd_main", VALID_UUID_3, "/workspace/repo-a");

        let job_mismatch_snap = fixture_workspace_snapshot(
            vec![fixture_source(
                "src_a",
                ObservationSourceAvailability::Available,
                None,
            )],
            vec![
                fixture_chat_summary(main_chat_ci),
                fixture_chat_summary(chat_uuid3_main_ci),
            ],
            vec![
                fixture_job_history(
                    "src_a",
                    "plan-other-thread",
                    Some("bnd_main"),
                    Some(other_thread_ci),
                    "Other Thread Job",
                    None,
                ),
                fixture_job_history(
                    "src_a",
                    "plan-prose",
                    Some("bnd_main"),
                    None,
                    &format!("Chat {VALID_UUID_1}"),
                    Some(&format!("Mentions {VALID_UUID_1} in summary prose")),
                ),
                fixture_job_history(
                    "src_a",
                    "plan-wrong-binding",
                    Some("bnd_other"),
                    Some(conflicting_job_ci),
                    "Conflicting Binding Job",
                    None,
                ),
            ],
        );

        assert_eq!(
            controller
                .open_codex_chat_with_fetcher(
                    MAIN_WINDOW_LABEL,
                    &OpenCodexChatRequest {
                        source_id: "src_a".to_string(),
                        thread_uuid: VALID_UUID_1.to_string(),
                        job_id: Some("plan-other-thread".to_string()),
                    },
                    |_q| Ok(job_mismatch_snap.clone()),
                )
                .await
                .expect_err("job on different thread must fail")
                .code,
            "target_thread_mismatch"
        );
        assert_eq!(
            controller
                .open_codex_chat_with_fetcher(
                    MAIN_WINDOW_LABEL,
                    &OpenCodexChatRequest {
                        source_id: "src_a".to_string(),
                        thread_uuid: VALID_UUID_1.to_string(),
                        job_id: Some("plan-prose".to_string()),
                    },
                    |_q| Ok(job_mismatch_snap.clone()),
                )
                .await
                .expect_err("prose thread mention must fail")
                .code,
            "target_thread_mismatch"
        );
        assert_eq!(
            controller
                .open_codex_chat_with_fetcher(
                    MAIN_WINDOW_LABEL,
                    &OpenCodexChatRequest {
                        source_id: "src_a".to_string(),
                        thread_uuid: VALID_UUID_1.to_string(),
                        job_id: Some("plan-missing".to_string()),
                    },
                    |_q| Ok(job_mismatch_snap.clone()),
                )
                .await
                .expect_err("missing job must not fall back to verified chat")
                .code,
            "target_not_found"
        );
        assert_eq!(
            controller
                .open_codex_chat_with_fetcher(
                    MAIN_WINDOW_LABEL,
                    &OpenCodexChatRequest {
                        source_id: "src_a".to_string(),
                        thread_uuid: VALID_UUID_3.to_string(),
                        job_id: Some("plan-wrong-binding".to_string()),
                    },
                    |_q| Ok(job_mismatch_snap.clone()),
                )
                .await
                .expect_err("job conflicting with chat binding must fail")
                .code,
            "target_scope_mismatch"
        );

        assert_eq!(port.call_count(), 0);
    }

    #[tokio::test]
    async fn capped_job_history_performs_at_most_one_targeted_followup_while_preserving_scope_uniqueness(
    ) {
        let chat_ci = fixture_chat_identity("src_a", "bnd_1", VALID_UUID_1, "/workspace/repo-a");
        let mut initial_snap = fixture_workspace_snapshot(
            vec![fixture_source(
                "src_a",
                ObservationSourceAvailability::Available,
                Some("/workspace/repo-a"),
            )],
            vec![fixture_chat_summary(chat_ci.clone())],
            Vec::new(),
        );
        initial_snap.bounds = WorkspaceSnapshotBounds {
            job_histories_omitted: 3,
            ..WorkspaceSnapshotBounds::default()
        };

        let targeted_snap = fixture_workspace_snapshot(
            vec![fixture_source(
                "src_a",
                ObservationSourceAvailability::Available,
                Some("/workspace/repo-a"),
            )],
            vec![fixture_chat_summary(chat_ci.clone())],
            vec![fixture_job_history(
                "src_a",
                "plan-archived",
                Some("bnd_1"),
                Some(chat_ci.clone()),
                "Older Job",
                None,
            )],
        );

        let queries = Mutex::new(Vec::<Option<ObservationSnapshotRequest>>::new());
        let port = FakeActivationPort::new(ActivationPortOutcome::Submitted);
        let controller = ChatNavigationController::new(port.clone());

        let (resolved, sub) = controller
            .open_codex_chat_with_fetcher(
                MAIN_WINDOW_LABEL,
                &OpenCodexChatRequest {
                    source_id: "src_a".to_string(),
                    thread_uuid: VALID_UUID_1.to_string(),
                    job_id: Some("plan-archived".to_string()),
                },
                |q| {
                    let mut log = queries.lock().expect("lock queries");
                    log.push(q.clone());
                    if log.len() == 1 {
                        Ok(initial_snap.clone())
                    } else {
                        Ok(targeted_snap.clone())
                    }
                },
            )
            .await
            .expect("capped job history follow-up should resolve matching scope");

        assert_eq!(sub, NavigationSubmissionOutcome::Submitted);
        assert_eq!(resolved.chat_identity, chat_ci);
        assert_eq!(queries.lock().expect("lock").len(), 2);
        assert_eq!(port.call_count(), 1);

        // Conflicting binding discovered in the capped follow-up snapshot must fail closed.
        let conflicting_ci =
            fixture_chat_identity("src_a", "bnd_conflict", VALID_UUID_1, "/workspace/repo-a");
        let conflicting_targeted_snap = fixture_workspace_snapshot(
            vec![fixture_source(
                "src_a",
                ObservationSourceAvailability::Available,
                Some("/workspace/repo-a"),
            )],
            Vec::new(),
            vec![fixture_job_history(
                "src_a",
                "plan-conflict",
                Some("bnd_conflict"),
                Some(conflicting_ci),
                "Conflicting Archived Job",
                None,
            )],
        );
        let conflict_port = FakeActivationPort::new(ActivationPortOutcome::Submitted);
        let conflict_controller = ChatNavigationController::new(conflict_port.clone());
        let conflict_calls = AtomicUsize::new(0);
        let conflict_err = conflict_controller
            .open_codex_chat_with_fetcher(
                MAIN_WINDOW_LABEL,
                &OpenCodexChatRequest {
                    source_id: "src_a".to_string(),
                    thread_uuid: VALID_UUID_1.to_string(),
                    job_id: Some("plan-conflict".to_string()),
                },
                |_q| {
                    let n = conflict_calls.fetch_add(1, Ordering::SeqCst);
                    if n == 0 {
                        Ok(initial_snap.clone())
                    } else {
                        Ok(conflicting_targeted_snap.clone())
                    }
                },
            )
            .await
            .expect_err("conflicting follow-up chat scope must be rejected");
        assert_eq!(conflict_err.code, "ambiguous_chat_target");
        assert_eq!(conflict_calls.load(Ordering::SeqCst), 2);
        assert_eq!(conflict_port.call_count(), 0);
    }

    #[tokio::test]
    async fn cancelled_in_flight_leader_latches_unknown_and_blocks_replay() {
        let chat_ci = fixture_chat_identity("src_a", "bnd_1", VALID_UUID_1, "/workspace/repo-a");
        let snap = fixture_workspace_snapshot(
            vec![fixture_source(
                "src_a",
                ObservationSourceAvailability::Available,
                None,
            )],
            vec![fixture_chat_summary(chat_ci)],
            Vec::new(),
        );
        let (gated_port, entered, _release) =
            FakeActivationPort::gated(ActivationPortOutcome::Submitted);
        let controller = Arc::new(ChatNavigationController::new(gated_port.clone()));

        let req = OpenCodexChatRequest {
            source_id: "src_a".to_string(),
            thread_uuid: VALID_UUID_1.to_string(),
            job_id: None,
        };

        let c1 = Arc::clone(&controller);
        let snap1 = snap.clone();
        let req1 = req.clone();
        let leader = tokio::spawn(async move {
            c1.open_codex_chat_with_fetcher(MAIN_WINDOW_LABEL, &req1, |_q| Ok(snap1.clone()))
                .await
        });

        tokio::time::timeout(Duration::from_secs(2), entered.notified())
            .await
            .expect("timed out waiting for leader to enter activation port");

        let c2 = Arc::clone(&controller);
        let snap2 = snap.clone();
        let req2 = req.clone();
        let follower = tokio::spawn(async move {
            tokio::task::yield_now().await;
            c2.open_codex_chat_with_fetcher(MAIN_WINDOW_LABEL, &req2, |_q| Ok(snap2.clone()))
                .await
        });

        tokio::time::sleep(Duration::from_millis(15)).await;
        leader.abort();
        let leader_join = tokio::time::timeout(Duration::from_secs(2), leader)
            .await
            .expect("timed out waiting for aborted leader task to terminate");
        let leader_err = leader_join.expect_err("leader must be cancelled");
        assert!(leader_err.is_cancelled());

        let follower_join = tokio::time::timeout(Duration::from_secs(2), follower)
            .await
            .expect("timed out waiting for follower task after leader abort")
            .expect("join follower");
        let follower_err = follower_join
            .expect_err("aborted leader after entering port must fail follower as unknown");
        assert_eq!(follower_err.code, "codex_activation_unknown");
        assert_eq!(controller.unknown_latch_count(), 1);
        assert_eq!(gated_port.call_count(), 1);

        let s3 = snap.clone();
        let replay_fut =
            controller.open_codex_chat_with_fetcher(MAIN_WINDOW_LABEL, &req, |_q| Ok(s3.clone()));
        let replay_err = tokio::time::timeout(Duration::from_secs(2), replay_fut)
            .await
            .expect("timed out awaiting unknown-latch replay rejection")
            .expect_err("aborted in-flight target must remain latched as unknown");
        assert_eq!(replay_err.code, "codex_activation_unknown");
        assert_eq!(gated_port.call_count(), 1);
    }

    #[tokio::test]
    async fn concurrent_in_flight_and_rapid_post_ack_double_clicks_coalesce_to_single_send() {
        let chat_a = fixture_chat_identity("src_a", "bnd_1", VALID_UUID_1, "/workspace/repo-a");
        let chat_b = fixture_chat_identity("src_a", "bnd_2", VALID_UUID_2, "/workspace/repo-a");
        let snap = fixture_workspace_snapshot(
            vec![fixture_source(
                "src_a",
                ObservationSourceAvailability::Available,
                None,
            )],
            vec![
                fixture_chat_summary(chat_a.clone()),
                fixture_chat_summary(chat_b.clone()),
            ],
            Vec::new(),
        );

        // 1. Concurrent in-flight duplicate clicks on exact same full chat identity -> 1 port call.
        let (gated_port, entered, release) =
            FakeActivationPort::gated(ActivationPortOutcome::Submitted);
        let controller = Arc::new(ChatNavigationController::with_config(
            gated_port.clone(),
            ChatNavigationControllerConfig {
                coalesce_window: Duration::from_secs(5),
                max_recent_submissions: 16,
                max_in_flight: 16,
                max_unknown_targets: 16,
            },
        ));

        let req_a = OpenCodexChatRequest {
            source_id: "src_a".to_string(),
            thread_uuid: VALID_UUID_1.to_string(),
            job_id: None,
        };

        let c1 = Arc::clone(&controller);
        let snap1 = snap.clone();
        let req1 = req_a.clone();
        let t1 = tokio::spawn(async move {
            c1.open_codex_chat_with_fetcher(MAIN_WINDOW_LABEL, &req1, |_q| Ok(snap1.clone()))
                .await
        });

        entered.notified().await;

        let c2 = Arc::clone(&controller);
        let snap2 = snap.clone();
        let req2 = req_a.clone();
        let t2 = tokio::spawn(async move {
            // Yield once so t2 joins `in_flight` while t1 is blocked on `release`.
            tokio::task::yield_now().await;
            c2.open_codex_chat_with_fetcher(MAIN_WINDOW_LABEL, &req2, |_q| Ok(snap2.clone()))
                .await
        });

        tokio::time::sleep(Duration::from_millis(15)).await;
        release.notify_waiters();

        let (_, sub1) = t1.await.expect("join t1").expect("t1 ok");
        let (_, sub2) = t2.await.expect("join t2").expect("t2 ok");
        assert_eq!(sub1, NavigationSubmissionOutcome::Submitted);
        assert_eq!(sub2, NavigationSubmissionOutcome::CoalescedDuplicate);
        assert_eq!(gated_port.call_count(), 1);

        // 2. Rapid sequential post-ACK double-click within `coalesce_window` -> still 1 port call!
        let (_, sub3) = controller
            .open_codex_chat_with_fetcher(MAIN_WINDOW_LABEL, &req_a, |_q| Ok(snap.clone()))
            .await
            .expect("rapid post-ACK click should coalesce cleanly");
        assert_eq!(sub3, NavigationSubmissionOutcome::CoalescedDuplicate);
        assert_eq!(gated_port.call_count(), 1);

        // 3. Distinct chat identity (`chat_b`) is NOT suppressed by `chat_a`'s coalesce window.
        let (fast_port, fast_entered, fast_release) =
            FakeActivationPort::gated(ActivationPortOutcome::Submitted);
        let _ = (fast_entered, fast_release);
        let fast_port_simple = FakeActivationPort::new(ActivationPortOutcome::Submitted);
        let controller_multi = ChatNavigationController::new(fast_port_simple.clone());
        controller_multi
            .open_codex_chat_with_fetcher(MAIN_WINDOW_LABEL, &req_a, |_q| Ok(snap.clone()))
            .await
            .expect("open chat_a");
        controller_multi
            .open_codex_chat_with_fetcher(
                MAIN_WINDOW_LABEL,
                &OpenCodexChatRequest {
                    source_id: "src_a".to_string(),
                    thread_uuid: VALID_UUID_2.to_string(),
                    job_id: None,
                },
                |_q| Ok(snap.clone()),
            )
            .await
            .expect("open chat_b");
        assert_eq!(fast_port_simple.call_count(), 2);
        let _ = fast_port;
    }

    #[tokio::test]
    async fn unavailable_app_pre_send_fails_explicitly_without_latching_unknown() {
        let chat_ci = fixture_chat_identity("src_a", "bnd_1", VALID_UUID_1, "/workspace/repo-a");
        let snap = fixture_workspace_snapshot(
            vec![fixture_source(
                "src_a",
                ObservationSourceAvailability::Available,
                None,
            )],
            vec![fixture_chat_summary(chat_ci)],
            Vec::new(),
        );
        let req = OpenCodexChatRequest {
            source_id: "src_a".to_string(),
            thread_uuid: VALID_UUID_1.to_string(),
            job_id: None,
        };

        let port = FakeActivationPort::scripted(
            [
                ActivationPortOutcome::RejectedBeforeSend(AppError::new(
                    "codex_app_unavailable",
                    "No verified running Codex desktop instance is available for chat activation.",
                )),
                ActivationPortOutcome::RejectedBeforeSend(AppError::new(
                    "codex_runtime_unsupported",
                    "Running Codex instance is not using the qualified immutable Owl shell runtime.",
                )),
                ActivationPortOutcome::Submitted,
            ],
            ActivationPortOutcome::Submitted,
        );
        let controller = ChatNavigationController::new(port.clone());

        let err1 = controller
            .open_codex_chat_with_fetcher(MAIN_WINDOW_LABEL, &req, |_q| Ok(snap.clone()))
            .await
            .expect_err("missing app must fail explicitly");
        assert_eq!(err1.code, "codex_app_unavailable");
        assert_eq!(controller.unknown_latch_count(), 0);

        let err2 = controller
            .open_codex_chat_with_fetcher(MAIN_WINDOW_LABEL, &req, |_q| Ok(snap.clone()))
            .await
            .expect_err("unqualified runtime must fail explicitly");
        assert_eq!(err2.code, "codex_runtime_unsupported");
        assert_eq!(controller.unknown_latch_count(), 0);

        // Once user starts the qualified Codex app, a subsequent click succeeds because prior
        // failures happened strictly BEFORE send.
        let (_resolved, sub3) = controller
            .open_codex_chat_with_fetcher(MAIN_WINDOW_LABEL, &req, |_q| Ok(snap.clone()))
            .await
            .expect("subsequent activation after pre-send rejection must succeed");
        assert_eq!(sub3, NavigationSubmissionOutcome::Submitted);
        assert_eq!(port.call_count(), 3);
    }

    #[tokio::test]
    async fn unknown_after_possible_send_never_retries_or_evicts_unknown_latch() {
        let chat_1 = fixture_chat_identity("src_a", "bnd_1", VALID_UUID_1, "/workspace/repo-a");
        let chat_2 = fixture_chat_identity("src_a", "bnd_2", VALID_UUID_2, "/workspace/repo-a");
        let chat_3 = fixture_chat_identity("src_a", "bnd_3", VALID_UUID_3, "/workspace/repo-a");
        let snap = fixture_workspace_snapshot(
            vec![fixture_source(
                "src_a",
                ObservationSourceAvailability::Available,
                None,
            )],
            vec![
                fixture_chat_summary(chat_1),
                fixture_chat_summary(chat_2),
                fixture_chat_summary(chat_3),
            ],
            Vec::new(),
        );

        let port = FakeActivationPort::scripted(
            [
                ActivationPortOutcome::UnknownAfterPossibleSend(AppError::new(
                    "codex_activation_unknown",
                    "Timed out waiting for ACK after writing frame.",
                )),
                ActivationPortOutcome::UnknownAfterPossibleSend(AppError::new(
                    "codex_activation_unknown",
                    "Malformed ACK payload.",
                )),
            ],
            ActivationPortOutcome::Submitted,
        );

        let controller = ChatNavigationController::with_config(
            port.clone(),
            ChatNavigationControllerConfig {
                coalesce_window: Duration::ZERO,
                max_recent_submissions: 8,
                max_in_flight: 8,
                max_unknown_targets: 2,
            },
        );

        let req_1 = OpenCodexChatRequest {
            source_id: "src_a".to_string(),
            thread_uuid: VALID_UUID_1.to_string(),
            job_id: None,
        };
        let err_first = controller
            .open_codex_chat_with_fetcher(MAIN_WINDOW_LABEL, &req_1, |_q| Ok(snap.clone()))
            .await
            .expect_err("timeout after possible send must fail as unknown");
        assert_eq!(err_first.code, "codex_activation_unknown");
        assert_eq!(port.call_count(), 1);
        assert_eq!(controller.unknown_latch_count(), 1);

        // Repeated calls for `chat_1` NEVER re-send to the port.
        let err_replay = controller
            .open_codex_chat_with_fetcher(MAIN_WINDOW_LABEL, &req_1, |_q| Ok(snap.clone()))
            .await
            .expect_err("latched unknown target must never replay");
        assert_eq!(err_replay.code, "codex_activation_unknown");
        assert_eq!(port.call_count(), 1);

        // Fill second `Unknown` slot (`max_unknown_targets == 2`).
        let req_2 = OpenCodexChatRequest {
            source_id: "src_a".to_string(),
            thread_uuid: VALID_UUID_2.to_string(),
            job_id: None,
        };
        let err_second = controller
            .open_codex_chat_with_fetcher(MAIN_WINDOW_LABEL, &req_2, |_q| Ok(snap.clone()))
            .await
            .expect_err("malformed ACK must fail as unknown");
        assert_eq!(err_second.code, "codex_activation_unknown");
        assert_eq!(port.call_count(), 2);
        assert_eq!(controller.unknown_latch_count(), 2);

        // Third distinct target hits `max_unknown_targets` BEFORE send without evicting `chat_1` or `chat_2`.
        let req_3 = OpenCodexChatRequest {
            source_id: "src_a".to_string(),
            thread_uuid: VALID_UUID_3.to_string(),
            job_id: None,
        };
        let err_cap = controller
            .open_codex_chat_with_fetcher(MAIN_WINDOW_LABEL, &req_3, |_q| Ok(snap.clone()))
            .await
            .expect_err("saturated unknown latch capacity must refuse dispatch");
        assert_eq!(err_cap.code, "codex_activation_capacity_exhausted");
        assert_eq!(port.call_count(), 2);
        assert_eq!(controller.unknown_latch_count(), 2);

        // `chat_1` remains permanently latched (not evicted!).
        let err_still_latched = controller
            .open_codex_chat_with_fetcher(MAIN_WINDOW_LABEL, &req_1, |_q| Ok(snap.clone()))
            .await
            .expect_err("original unknown target must remain latched");
        assert_eq!(err_still_latched.code, "codex_activation_unknown");
        assert_eq!(port.call_count(), 2);
    }

    #[tokio::test]
    async fn focus_preflight_ambiguity_does_not_latch_while_post_uri_focus_failure_latches_unknown()
    {
        let chat_ci = fixture_chat_identity("src_a", "bnd_1", VALID_UUID_1, "/workspace/repo-a");
        let snap = fixture_workspace_snapshot(
            vec![fixture_source(
                "src_a",
                ObservationSourceAvailability::Available,
                None,
            )],
            vec![fixture_chat_summary(chat_ci)],
            Vec::new(),
        );
        let req = OpenCodexChatRequest {
            source_id: "src_a".to_string(),
            thread_uuid: VALID_UUID_1.to_string(),
            job_id: None,
        };

        let port = FakeActivationPort::scripted(
            [
                ActivationPortOutcome::RejectedBeforeSend(AppError::new(
                    "codex_window_ambiguous",
                    "Multiple eligible Codex desktop windows were detected; refusing to guess which window to focus.",
                )),
                ActivationPortOutcome::RejectedBeforeSend(AppError::new(
                    "codex_focus_unavailable",
                    "Verified Codex desktop window could not be prepared for focus in the active compositor.",
                )),
                ActivationPortOutcome::UnknownAfterPossibleSend(AppError::new(
                    "codex_focus_unavailable",
                    "Failed to focus address:0x55a1b2c3d4e0 via /run/user/1000/hypr/sig/.socket.sock",
                )),
            ],
            ActivationPortOutcome::Submitted,
        );
        let controller = ChatNavigationController::with_config(
            port.clone(),
            ChatNavigationControllerConfig {
                coalesce_window: Duration::ZERO,
                max_recent_submissions: 8,
                max_in_flight: 8,
                max_unknown_targets: 8,
            },
        );

        let err_ambig = controller
            .open_codex_chat_with_fetcher(MAIN_WINDOW_LABEL, &req, |_q| Ok(snap.clone()))
            .await
            .expect_err("window ambiguity preflight must reject before send");
        assert_eq!(err_ambig.code, "codex_window_ambiguous");
        assert_eq!(controller.unknown_latch_count(), 0);

        let err_unavail = controller
            .open_codex_chat_with_fetcher(MAIN_WINDOW_LABEL, &req, |_q| Ok(snap.clone()))
            .await
            .expect_err("missing window preflight must reject before send");
        assert_eq!(err_unavail.code, "codex_focus_unavailable");
        assert_eq!(controller.unknown_latch_count(), 0);

        let err_post_uri_focus = controller
            .open_codex_chat_with_fetcher(MAIN_WINDOW_LABEL, &req, |_q| Ok(snap.clone()))
            .await
            .expect_err("post-URI focus failure must latch as unknown");
        assert_eq!(err_post_uri_focus.code, "codex_activation_unknown");
        assert!(!err_post_uri_focus.message.contains("0x55a1b2c3d4e0"));
        assert!(!err_post_uri_focus.message.contains(".socket.sock"));
        assert_eq!(controller.unknown_latch_count(), 1);
        assert_eq!(port.call_count(), 3);

        let err_blocked = controller
            .open_codex_chat_with_fetcher(MAIN_WINDOW_LABEL, &req, |_q| Ok(snap.clone()))
            .await
            .expect_err(
                "uncertain post-URI/focus outcome must block automatic or duplicate resend",
            );
        assert_eq!(err_blocked.code, "codex_activation_unknown");
        assert_eq!(port.call_count(), 3);
    }

    #[test]
    fn navigation_error_sanitizer_strips_paths_cookies_and_runtime_locations() {
        for raw in [
            AppError::new(
                "raw_os_error",
                "Failed to connect /tmp/.org.chromium.Chromium.abc/SingletonSocket",
            ),
            AppError::new(
                "codex_app_unavailable",
                "Mismatch for SingletonCookie=987654321 in /home/secret-user/.config/Codex",
            ),
            AppError::new(
                "codex_runtime_unsupported",
                "Missing marker in /nix/store/0123456789-codex/libexec/owl-shell",
            ),
            AppError::new(
                "codex_focus_unavailable",
                "Failed on XDG_RUNTIME_DIR=/run/user/1000/hypr/secret_his/.socket.sock for address:0x55a1b2c3d4e0",
            ),
            AppError::new(
                "codex_window_ambiguous",
                "SWAYSOCK=sway-ipc.1000.42.sock matched [con_id=42] and [con_id=99]",
            ),
            AppError::new(
                "invalid_thread_uuid",
                "Expected codex://threads/<uuid> in /home/secret-user/private",
            ),
        ] {
            let clean = sanitize_navigation_error(raw);
            assert!(!clean.message.contains("/tmp"));
            assert!(!clean.message.contains("/home/secret-user"));
            assert!(!clean.message.contains("/nix/store"));
            assert!(!clean.message.contains("/run/user"));
            assert!(!clean.message.contains("SingletonSocket"));
            assert!(!clean.message.contains("SingletonCookie"));
            assert!(!clean.message.contains("987654321"));
            assert!(!clean.message.contains(".socket.sock"));
            assert!(!clean.message.contains("secret_his"));
            assert!(!clean.message.contains("0x55a1b2c3d4e0"));
            assert!(!clean.message.contains("sway-ipc"));
            assert!(!clean.message.contains("con_id"));
        }
    }

    #[cfg(unix)]
    #[test]
    fn observation_service_workspace_resolution_rejects_unverified_source_only_records_and_stays_readonly(
    ) {
        use crate::observation::ObservationSourceConfig;
        use std::fs::{self, DirBuilder, OpenOptions};
        use std::io::Write;
        use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
        use tempfile::TempDir;
        use threadlane_core::observation::{
            append_journal_record, ObservationJournalRecord, ObservationRecordPhase,
        };

        let tmp = TempDir::new().expect("tempdir");
        let root = fs::canonicalize(tmp.path()).expect("canonicalize temp root");
        let mk_private_dir = |name: &str| {
            let p = root.join(name);
            DirBuilder::new()
                .mode(0o700)
                .create(&p)
                .expect("create 0700 dir");
            p
        };

        let registry_dir = mk_private_dir("registry");
        let plans_dir = mk_private_dir("plans");
        let authority_dir = mk_private_dir("authority");
        let sessions_dir = mk_private_dir("sessions");

        let plans_path = plans_dir.join("plans.json");
        let mut f = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&plans_path)
            .expect("create plans.json");
        f.write_all(b"{\"schema_version\":1,\"revisions\":[]}\n")
            .expect("write plans.json");
        f.sync_all().expect("sync plans.json");
        drop(f);

        // Journal record has `project_path: None` (no verified canonical_project_scope).
        let record = ObservationJournalRecord {
            record_id: "obsrec_11111111111111111111111111111111".to_string(),
            request_id: "obsreq_22222222222222222222222222222222".to_string(),
            parent_request_id: None,
            call_id: None,
            phase: ObservationRecordPhase::Finish,
            at_ms: 1_700_000_020_000,
            pid: None,
            process_instance: None,
            project_label: Some("Threadlane local".to_string()),
            project_path: None,
            binding_id: Some("bnd_live_1".to_string()),
            thread_uuid: Some(VALID_UUID_1.to_string()),
            thread_label: Some("Main Chat".to_string()),
            job_id: None,
            plan_id: None,
            task_id: None,
            approval_id: None,
            operation: "mcp.workflow.status".to_string(),
            title: "workflow status".to_string(),
            state: ObservationCallState::Completed,
            actor: Some(ObservationActor::paired_coworker("Main Chat")),
            executor: None,
            usage: None,
            delivery: Vec::new(),
            summary: Some("Completed".to_string()),
        };
        append_journal_record(&plans_dir, &record).expect("append journal record");

        let service = ObservationService::new(ObservationSourceConfig {
            runtime_state_dir: None,
            registry_dir: registry_dir.clone(),
            plans_dir: plans_dir.clone(),
            authority_dir: authority_dir.clone(),
            sessions_dir: sessions_dir.clone(),
            app_data_dir: registry_dir.clone(),
        });

        let sources = service.sources();
        assert_eq!(sources.len(), 1);
        let source_id = sources[0].id.clone();

        let err = resolve_codex_chat_target(
            MAIN_WINDOW_LABEL,
            &service,
            &OpenCodexChatRequest {
                source_id,
                thread_uuid: VALID_UUID_1.to_string(),
                job_id: None,
            },
        )
        .expect_err("source-only record without verified project scope must fail closed");
        assert!(
            matches!(
                err.code.as_str(),
                "target_scope_unverified" | "target_not_found"
            ),
            "unexpected error code: {}",
            err.code
        );

        assert!(!plans_dir.join("plans.lock").exists());
        assert!(!authority_dir.join("approvals.json").exists());
    }
}
