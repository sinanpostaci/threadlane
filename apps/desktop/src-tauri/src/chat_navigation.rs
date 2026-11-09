use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use tauri::{AppHandle, State, Window};
use tauri_plugin_opener::OpenerExt;
use threadlane_core::observation::{
    validate_correlation_id, ObservationSnapshot, ObservationSnapshotRequest,
    ObservationSourceAvailability,
};
use uuid::Uuid;

use crate::observation::{ObservationService, MAX_CURSOR_BYTES, MAX_REQUEST_PAGE_LIMIT};
use crate::tray::MAIN_WINDOW_LABEL;
use crate::AppError;

/// Canonical deep-link URI scheme and path prefix for opening a native Codex conversation thread.
pub(crate) const CODEX_THREAD_URI_PREFIX: &str = "codex://threads/";

/// Exact byte length of a canonical lowercase hyphenated UUID (`8-4-4-4-12`).
pub(crate) const CANONICAL_UUID_LEN: usize = 36;

/// Maximum byte length allowed for `source_id` or `job_id` navigation identifiers.
pub(crate) const MAX_NAVIGATION_ID_LEN: usize = 128;

/// Maximum number of observation request pages scanned when resolving a thread without `job_id`.
pub(crate) const MAX_CHAT_NAVIGATION_PAGES: usize = 4;

/// Per-page request limit used when scanning observation snapshots for chat navigation.
pub(crate) const CHAT_NAVIGATION_PAGE_LIMIT: usize = MAX_REQUEST_PAGE_LIMIT;

/// Frozen frontend request payload for `desktop_open_codex_chat`.
///
/// Accepts only scoped identity keys (`source_id`, `thread_uuid`, and optional `job_id`).
/// Unknown fields (such as raw URLs, executables, shell argv, filesystem paths, approval flags,
/// or model selectors) are rejected at deserialization time.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct OpenCodexChatRequest {
    pub(crate) source_id: String,
    pub(crate) thread_uuid: String,
    #[serde(default)]
    pub(crate) job_id: Option<String>,
}

/// Pre-validated and normalized request parameters prior to any observation read or OS open effect.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ValidatedCodexChatRequest {
    pub(crate) source_id: String,
    pub(crate) thread_uuid: String,
    pub(crate) job_id: Option<String>,
}

/// Identifies which verified record in the observation feed established the target thread identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum VerifiedChatMatchOrigin {
    /// Matched an [`threadlane_core::observation::ObservationJob`] record by `(source_id, job_id, thread_uuid)`.
    Job { job_id: String },
    /// Matched an [`threadlane_core::observation::ObservationRequest`] record by `(source_id, request_id, thread_uuid)`.
    Request { request_id: String },
}

/// Verified target resolution outcome containing the canonical `codex://threads/<uuid>` URI.
///
/// Submitting this URI to the OS opener is a local launch submission only; it never records an
/// operator approval, dispatches a worker, sends a message, or proves human reading.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ResolvedCodexChatTarget {
    pub(crate) source_id: String,
    pub(crate) thread_uuid: String,
    pub(crate) job_id: Option<String>,
    pub(crate) canonical_uri: String,
    pub(crate) match_origin: VerifiedChatMatchOrigin,
}

/// Enforces that chat launch requests originate exclusively from the primary `main` window.
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

/// Verifies that a URI string is strictly `codex://threads/<canonicalUuid>` before OS submission.
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

/// Validates an [`OpenCodexChatRequest`] before any observation read or OS open effect occurs.
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

fn verify_source_available_in_snapshot(
    snapshot: &ObservationSnapshot,
    source_id: &str,
) -> Result<(), AppError> {
    let source = snapshot
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

    Ok(())
}

fn resolve_job_scoped_target_in_snapshot(
    snapshot: &ObservationSnapshot,
    validated: &ValidatedCodexChatRequest,
    expected_job_id: &str,
) -> Result<ResolvedCodexChatTarget, AppError> {
    verify_source_available_in_snapshot(snapshot, &validated.source_id)?;

    let matching_jobs: Vec<_> = snapshot
        .jobs
        .iter()
        .filter(|job| job.source_id == validated.source_id && job.id == expected_job_id)
        .collect();

    if matching_jobs.is_empty() {
        return Err(AppError::new(
            "target_not_found",
            "Requested job was not found in the current observation source. Refresh and try again.",
        ));
    }

    for job in &matching_jobs {
        let Some(ref observed_uuid) = job.thread_uuid else {
            return Err(AppError::new(
                "target_thread_mismatch",
                "Observed job does not match the requested Codex chat thread. Refresh and try again.",
            ));
        };
        let canonical_observed = validate_canonical_codex_thread_uuid(observed_uuid).map_err(|_| {
            AppError::new(
                "target_thread_mismatch",
                "Observed job does not match the requested Codex chat thread. Refresh and try again.",
            )
        })?;
        if canonical_observed != validated.thread_uuid {
            return Err(AppError::new(
                "target_thread_mismatch",
                "Observed job does not match the requested Codex chat thread. Refresh and try again.",
            ));
        }
    }

    let canonical_uri = build_codex_thread_uri(&validated.thread_uuid)?;
    Ok(ResolvedCodexChatTarget {
        source_id: validated.source_id.clone(),
        thread_uuid: validated.thread_uuid.clone(),
        job_id: Some(expected_job_id.to_string()),
        canonical_uri,
        match_origin: VerifiedChatMatchOrigin::Job {
            job_id: expected_job_id.to_string(),
        },
    })
}

fn find_unlinked_thread_match_in_snapshot(
    snapshot: &ObservationSnapshot,
    validated: &ValidatedCodexChatRequest,
) -> Result<Option<ResolvedCodexChatTarget>, AppError> {
    verify_source_available_in_snapshot(snapshot, &validated.source_id)?;

    for job in &snapshot.jobs {
        if job.source_id != validated.source_id {
            continue;
        }
        if let Some(ref observed_uuid) = job.thread_uuid {
            if observed_uuid == &validated.thread_uuid
                && validate_canonical_codex_thread_uuid(observed_uuid).is_ok()
            {
                let canonical_uri = build_codex_thread_uri(&validated.thread_uuid)?;
                return Ok(Some(ResolvedCodexChatTarget {
                    source_id: validated.source_id.clone(),
                    thread_uuid: validated.thread_uuid.clone(),
                    job_id: None,
                    canonical_uri,
                    match_origin: VerifiedChatMatchOrigin::Job {
                        job_id: job.id.clone(),
                    },
                }));
            }
        }
    }

    for req in &snapshot.requests {
        if req.source_id != validated.source_id {
            continue;
        }
        if let Some(ref observed_uuid) = req.thread_uuid {
            if observed_uuid == &validated.thread_uuid
                && validate_canonical_codex_thread_uuid(observed_uuid).is_ok()
            {
                let canonical_uri = build_codex_thread_uri(&validated.thread_uuid)?;
                return Ok(Some(ResolvedCodexChatTarget {
                    source_id: validated.source_id.clone(),
                    thread_uuid: validated.thread_uuid.clone(),
                    job_id: None,
                    canonical_uri,
                    match_origin: VerifiedChatMatchOrigin::Request {
                        request_id: req.id.clone(),
                    },
                }));
            }
        }
    }

    Ok(None)
}

/// Pure single-snapshot target resolver for fixture inspection and unit tests.
pub(crate) fn resolve_codex_chat_target_in_snapshot(
    snapshot: &ObservationSnapshot,
    request: &OpenCodexChatRequest,
) -> Result<ResolvedCodexChatTarget, AppError> {
    let validated = validate_open_codex_chat_request(request)?;
    match validated.job_id.as_deref() {
        Some(expected_job_id) => {
            resolve_job_scoped_target_in_snapshot(snapshot, &validated, expected_job_id)
        }
        None => find_unlinked_thread_match_in_snapshot(snapshot, &validated)?.ok_or_else(|| {
            AppError::new(
                "target_not_found",
                "Requested Codex chat thread was not found in recent observation records. Refresh and try again.",
            )
        }),
    }
}

/// Resolves a Codex chat target using a bounded snapshot fetcher (`<= MAX_CHAT_NAVIGATION_PAGES`).
///
/// Validates both `window_label` and `request` before calling `fetch_snapshot`.
pub(crate) fn resolve_codex_chat_target_with_fetcher<F>(
    window_label: &str,
    request: &OpenCodexChatRequest,
    mut fetch_snapshot: F,
) -> Result<ResolvedCodexChatTarget, AppError>
where
    F: FnMut(ObservationSnapshotRequest) -> Result<ObservationSnapshot, AppError>,
{
    ensure_main_window_caller(window_label)?;
    let validated = validate_open_codex_chat_request(request)?;

    if let Some(ref expected_job_id) = validated.job_id {
        let snapshot = fetch_snapshot(ObservationSnapshotRequest {
            source_id: Some(validated.source_id.clone()),
            job_id: Some(expected_job_id.clone()),
            cursor: None,
            limit: Some(CHAT_NAVIGATION_PAGE_LIMIT),
        })?;
        return resolve_codex_chat_target_in_snapshot(&snapshot, request);
    }

    let mut cursor: Option<String> = None;
    let mut seen_cursors = BTreeSet::new();

    for page_idx in 0..MAX_CHAT_NAVIGATION_PAGES {
        let snapshot = fetch_snapshot(ObservationSnapshotRequest {
            source_id: Some(validated.source_id.clone()),
            job_id: None,
            cursor: cursor.clone(),
            limit: Some(CHAT_NAVIGATION_PAGE_LIMIT),
        })?;

        match resolve_codex_chat_target_in_snapshot(&snapshot, request) {
            Ok(resolved) => return Ok(resolved),
            Err(err) if err.code != "target_not_found" => return Err(err),
            Err(_) => {}
        }

        if page_idx + 1 >= MAX_CHAT_NAVIGATION_PAGES || !snapshot.has_more {
            break;
        }

        let Some(next_cursor) = snapshot.next_cursor else {
            break;
        };

        if next_cursor.is_empty()
            || next_cursor != next_cursor.trim()
            || next_cursor.len() > MAX_CURSOR_BYTES
            || next_cursor
                .bytes()
                .any(|b| b.is_ascii_whitespace() || b.is_ascii_control())
            || !seen_cursors.insert(next_cursor.clone())
        {
            break;
        }

        cursor = Some(next_cursor);
    }

    Err(AppError::new(
        "target_not_found",
        "Requested Codex chat thread was not found in recent observation records. Refresh and try again.",
    ))
}

/// Resolves a Codex chat target across a pre-supplied slice of fixture [`ObservationSnapshot`] pages.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn resolve_codex_chat_target_in_pages(
    window_label: &str,
    pages: &[ObservationSnapshot],
    request: &OpenCodexChatRequest,
) -> Result<ResolvedCodexChatTarget, AppError> {
    let mut index = 0usize;
    resolve_codex_chat_target_with_fetcher(window_label, request, |_req| {
        let page = pages.get(index).cloned().ok_or_else(|| {
            AppError::new(
                "target_not_found",
                "Requested Codex chat thread was not found in recent observation records. Refresh and try again.",
            )
        })?;
        index += 1;
        Ok(page)
    })
}

/// Resolves a Codex chat target against a live read-only [`ObservationService`].
pub(crate) fn resolve_codex_chat_target(
    window_label: &str,
    observation: &ObservationService,
    request: &OpenCodexChatRequest,
) -> Result<ResolvedCodexChatTarget, AppError> {
    resolve_codex_chat_target_with_fetcher(window_label, request, |snapshot_request| {
        observation.snapshot(Some(snapshot_request))
    })
}

/// Sanitizes an opener failure so that raw filesystem paths or OS details are never exposed.
pub(crate) fn sanitize_opener_error(err: AppError) -> AppError {
    let msg = err.message.trim();
    let lower = msg.to_ascii_lowercase();
    if msg.is_empty()
        || msg.contains('/')
        || msg.contains('\\')
        || lower.contains("path")
        || lower.contains("home")
        || lower.contains("nix/store")
        || lower.contains("users")
    {
        AppError::new(
            "open_codex_chat_failed",
            "Unable to submit Codex chat launch request to the operating system. Verify that Codex is installed and registered for codex:// links.",
        )
    } else {
        AppError::new("open_codex_chat_failed", msg)
    }
}

/// Verifies consistency of a [`ResolvedCodexChatTarget`] against the [`ValidatedCodexChatRequest`]
/// and invokes `open_uri` with the verified canonical `codex://threads/<uuid>` URI.
pub(crate) fn submit_resolved_codex_chat_target<G>(
    resolved: &ResolvedCodexChatTarget,
    validated: &ValidatedCodexChatRequest,
    open_uri: G,
) -> Result<(), AppError>
where
    G: FnOnce(&str) -> Result<(), AppError>,
{
    if resolved.source_id != validated.source_id
        || resolved.thread_uuid != validated.thread_uuid
        || resolved.job_id != validated.job_id
    {
        return Err(AppError::new(
            "target_thread_mismatch",
            "Observed chat target did not match the requested identity. Refresh and try again.",
        ));
    }

    match &resolved.match_origin {
        VerifiedChatMatchOrigin::Job { job_id } => {
            if job_id.trim().is_empty()
                || validated
                    .job_id
                    .as_ref()
                    .is_some_and(|expected| expected != job_id)
            {
                return Err(AppError::new(
                    "target_thread_mismatch",
                    "Observed job target did not match the requested identity. Refresh and try again.",
                ));
            }
        }
        VerifiedChatMatchOrigin::Request { request_id } => {
            if request_id.trim().is_empty() || validated.job_id.is_some() {
                return Err(AppError::new(
                    "target_thread_mismatch",
                    "Observed request target did not match the requested identity. Refresh and try again.",
                ));
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

    open_uri(&validated_uri).map_err(sanitize_opener_error)
}

/// Pure resolution-and-open boundary parameterized over both snapshot fetching and URI opening.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn open_codex_chat_with_opener<F, G>(
    window_label: &str,
    request: &OpenCodexChatRequest,
    fetch_snapshot: F,
    open_uri: G,
) -> Result<ResolvedCodexChatTarget, AppError>
where
    F: FnMut(ObservationSnapshotRequest) -> Result<ObservationSnapshot, AppError>,
    G: FnOnce(&str) -> Result<(), AppError>,
{
    ensure_main_window_caller(window_label)?;
    let validated = validate_open_codex_chat_request(request)?;
    let resolved = resolve_codex_chat_target_with_fetcher(window_label, request, fetch_snapshot)?;
    submit_resolved_codex_chat_target(&resolved, &validated, open_uri)?;
    Ok(resolved)
}

/// Pure resolution-and-open boundary over a slice of fixture [`ObservationSnapshot`] pages.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn open_codex_chat_in_pages_with_opener<G>(
    window_label: &str,
    pages: &[ObservationSnapshot],
    request: &OpenCodexChatRequest,
    open_uri: G,
) -> Result<ResolvedCodexChatTarget, AppError>
where
    G: FnOnce(&str) -> Result<(), AppError>,
{
    let mut index = 0usize;
    open_codex_chat_with_opener(
        window_label,
        request,
        |_req| {
            let page = pages.get(index).cloned().ok_or_else(|| {
                AppError::new(
                    "target_not_found",
                    "Requested Codex chat thread was not found in recent observation records. Refresh and try again.",
                )
            })?;
            index += 1;
            Ok(page)
        },
        open_uri,
    )
}

fn open_canonical_uri_via_tauri(app: &AppHandle, canonical_uri: &str) -> Result<(), AppError> {
    app.opener()
        .open_url(canonical_uri, None::<&str>)
        .map_err(|_| {
            AppError::new(
                "open_codex_chat_failed",
                "Unable to submit Codex chat launch request to the operating system. Verify that Codex is installed and registered for codex:// links.",
            )
        })
}

/// Validates and resolves an observed `(source_id, thread_uuid, job_id)` target and submits
/// `codex://threads/<canonicalUuid>` to the OS URL opener from the primary `main` window.
#[tauri::command]
pub(crate) async fn desktop_open_codex_chat(
    app: AppHandle,
    window: Window,
    observation: State<'_, ObservationService>,
    request: OpenCodexChatRequest,
) -> Result<(), AppError> {
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

    submit_resolved_codex_chat_target(&resolved, &validated, |uri| {
        open_canonical_uri_via_tauri(&app, uri)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;
    use threadlane_core::observation::{
        ObservationActor, ObservationCallState, ObservationJob, ObservationJobState,
        ObservationRequest, ObservationSource, OBSERVATION_SCHEMA_VERSION,
    };

    const VALID_UUID_1: &str = "550e8400-e29b-41d4-a716-446655440000";
    const VALID_UUID_2: &str = "6ba7b810-9dad-11d1-80b4-00c04fd430c8";

    fn fixture_source(id: &str, availability: ObservationSourceAvailability) -> ObservationSource {
        ObservationSource {
            id: id.to_string(),
            label: format!("Source {id}"),
            project_path: None,
            availability,
            message: None,
        }
    }

    fn fixture_job(
        source_id: &str,
        job_id: &str,
        thread_uuid: Option<&str>,
        title: &str,
    ) -> ObservationJob {
        ObservationJob {
            source_id: source_id.to_string(),
            id: job_id.to_string(),
            project_label: "Threadlane local".to_string(),
            project_path: None,
            title: title.to_string(),
            thread_uuid: thread_uuid.map(str::to_string),
            thread_label: Some("Main Chat".to_string()),
            binding_id: Some("bnd_1".to_string()),
            plan_id: Some(job_id.to_string()),
            plan_revision: Some(1),
            state: ObservationJobState::Ready,
            approval_status: "approved".to_string(),
            execution_status: "not_started".to_string(),
            review_state: None,
            updated_at_ms: Some(1_700_000_000_000),
            active_call_count: 0,
            tasks: Vec::new(),
            summary: Some(format!("Summary for {job_id}")),
        }
    }

    fn fixture_request(
        source_id: &str,
        request_id: &str,
        job_id: Option<&str>,
        thread_uuid: Option<&str>,
        updated_at_ms: i64,
    ) -> ObservationRequest {
        ObservationRequest {
            source_id: source_id.to_string(),
            id: request_id.to_string(),
            job_id: job_id.map(str::to_string),
            parent_request_id: None,
            title: format!("Request {request_id}"),
            operation: "mcp.workflow.status".to_string(),
            project_label: "Threadlane local".to_string(),
            binding_id: Some("bnd_1".to_string()),
            thread_uuid: thread_uuid.map(str::to_string),
            thread_label: Some("Main Chat".to_string()),
            state: ObservationCallState::Completed,
            started_at_ms: Some(updated_at_ms - 25),
            finished_at_ms: Some(updated_at_ms),
            updated_at_ms,
            duration_ms: Some(25),
            actor: ObservationActor::paired_coworker("Main Chat"),
            executor: None,
            usage: None,
            delivery: Vec::new(),
            children: Vec::new(),
            children_count: 0,
            summary: Some("Completed".to_string()),
        }
    }

    fn fixture_snapshot(
        sources: Vec<ObservationSource>,
        jobs: Vec<ObservationJob>,
        requests: Vec<ObservationRequest>,
        next_cursor: Option<String>,
        has_more: bool,
    ) -> ObservationSnapshot {
        ObservationSnapshot {
            schema_version: OBSERVATION_SCHEMA_VERSION,
            generated_at_ms: 1_700_000_010_000,
            sources,
            jobs,
            requests,
            next_cursor,
            has_more,
            jobs_has_more: false,
            warnings: Vec::new(),
            warning_contexts: Vec::new(),
        }
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
                r#"{{"source_id":"src_a","thread_uuid":"{VALID_UUID_1}","executable":"/bin/sh"}}"#
            ),
            format!(
                r#"{{"source_id":"src_a","thread_uuid":"{VALID_UUID_1}","argv":["--danger"]}}"#
            ),
            format!(
                r#"{{"source_id":"src_a","thread_uuid":"{VALID_UUID_1}","project_path":"/tmp"}}"#
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

    #[test]
    fn invalid_and_noncanonical_ids_or_controls_are_rejected_pre_effect() {
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
            let fetch_calls = RefCell::new(0usize);
            let open_calls = RefCell::new(0usize);
            let req = OpenCodexChatRequest {
                source_id: "src_a".to_string(),
                thread_uuid: bad_uuid.to_string(),
                job_id: Some("plan-1".to_string()),
            };

            let err = open_codex_chat_with_opener(
                MAIN_WINDOW_LABEL,
                &req,
                |_q| {
                    *fetch_calls.borrow_mut() += 1;
                    unreachable!("snapshot must not be queried when thread_uuid is invalid");
                },
                |_uri| {
                    *open_calls.borrow_mut() += 1;
                    Ok(())
                },
            )
            .expect_err("invalid thread_uuid must be rejected");

            assert_eq!(err.code, "invalid_thread_uuid", "failed on {bad_uuid:?}");
            assert_eq!(*fetch_calls.borrow(), 0);
            assert_eq!(*open_calls.borrow(), 0);
        }

        let oversized = "a".repeat(MAX_NAVIGATION_ID_LEN + 1);
        let invalid_ids = [
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
        ];

        for bad_source in invalid_ids {
            let fetch_calls = RefCell::new(0usize);
            let open_calls = RefCell::new(0usize);
            let req = OpenCodexChatRequest {
                source_id: bad_source.to_string(),
                thread_uuid: VALID_UUID_1.to_string(),
                job_id: None,
            };

            let err = open_codex_chat_with_opener(
                MAIN_WINDOW_LABEL,
                &req,
                |_q| {
                    *fetch_calls.borrow_mut() += 1;
                    unreachable!("snapshot must not be queried when source_id is invalid");
                },
                |_uri| {
                    *open_calls.borrow_mut() += 1;
                    Ok(())
                },
            )
            .expect_err("invalid source_id must be rejected");

            assert_eq!(err.code, "invalid_input", "failed on {bad_source:?}");
            assert_eq!(*fetch_calls.borrow(), 0);
            assert_eq!(*open_calls.borrow(), 0);
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
            let fetch_calls = RefCell::new(0usize);
            let open_calls = RefCell::new(0usize);
            let req = OpenCodexChatRequest {
                source_id: "src_a".to_string(),
                thread_uuid: VALID_UUID_1.to_string(),
                job_id: Some(bad_job.to_string()),
            };

            let err = open_codex_chat_with_opener(
                MAIN_WINDOW_LABEL,
                &req,
                |_q| {
                    *fetch_calls.borrow_mut() += 1;
                    unreachable!("snapshot must not be queried when job_id is invalid");
                },
                |_uri| {
                    *open_calls.borrow_mut() += 1;
                    Ok(())
                },
            )
            .expect_err("invalid job_id must be rejected");

            assert_eq!(err.code, "invalid_input", "failed on {bad_job:?}");
            assert_eq!(*fetch_calls.borrow(), 0);
            assert_eq!(*open_calls.borrow(), 0);
        }
    }

    #[test]
    fn wrong_window_label_is_rejected_pre_effect_with_zero_opens() {
        for bad_window in ["tray", "", " main", "main ", "other", "popover"] {
            let fetch_calls = RefCell::new(0usize);
            let open_calls = RefCell::new(0usize);
            let req = OpenCodexChatRequest {
                source_id: "src_a".to_string(),
                thread_uuid: VALID_UUID_1.to_string(),
                job_id: Some("plan-1".to_string()),
            };

            let err = open_codex_chat_with_opener(
                bad_window,
                &req,
                |_q| {
                    *fetch_calls.borrow_mut() += 1;
                    unreachable!("wrong window must not query observation snapshot");
                },
                |_uri| {
                    *open_calls.borrow_mut() += 1;
                    Ok(())
                },
            )
            .expect_err("non-main window must be rejected");

            assert_eq!(err.code, "forbidden_surface");
            assert_eq!(*fetch_calls.borrow(), 0);
            assert_eq!(*open_calls.borrow(), 0);
        }
    }

    #[test]
    fn wrong_source_and_unavailable_source_cause_zero_opens() {
        let available_other = fixture_source("src_other", ObservationSourceAvailability::Available);
        let unavailable_target =
            fixture_source("src_unavail", ObservationSourceAvailability::Unavailable);

        let snap = fixture_snapshot(
            vec![available_other, unavailable_target],
            vec![
                fixture_job("src_other", "plan-1", Some(VALID_UUID_1), "Job 1"),
                fixture_job("src_unavail", "plan-1", Some(VALID_UUID_1), "Job 1"),
            ],
            vec![fixture_request(
                "src_other",
                "obsreq_1",
                Some("plan-1"),
                Some(VALID_UUID_1),
                1_700_000_000_000,
            )],
            None,
            false,
        );

        let open_calls = RefCell::new(0usize);

        // 1. Unknown source_id not in snapshot.sources -> source_not_found, 0 opens.
        let missing_source_req = OpenCodexChatRequest {
            source_id: "src_missing".to_string(),
            thread_uuid: VALID_UUID_1.to_string(),
            job_id: Some("plan-1".to_string()),
        };
        let err = open_codex_chat_in_pages_with_opener(
            MAIN_WINDOW_LABEL,
            std::slice::from_ref(&snap),
            &missing_source_req,
            |_uri| {
                *open_calls.borrow_mut() += 1;
                Ok(())
            },
        )
        .expect_err("unknown source must fail");
        assert_eq!(err.code, "source_not_found");
        assert!(err.message.contains("Refresh"));
        assert_eq!(*open_calls.borrow(), 0);

        // 2. Unavailable source_id -> source_unavailable, 0 opens.
        let unavail_source_req = OpenCodexChatRequest {
            source_id: "src_unavail".to_string(),
            thread_uuid: VALID_UUID_1.to_string(),
            job_id: Some("plan-1".to_string()),
        };
        let err = open_codex_chat_in_pages_with_opener(
            MAIN_WINDOW_LABEL,
            std::slice::from_ref(&snap),
            &unavail_source_req,
            |_uri| {
                *open_calls.borrow_mut() += 1;
                Ok(())
            },
        )
        .expect_err("unavailable source must fail");
        assert_eq!(err.code, "source_unavailable");
        assert!(err.message.contains("Refresh"));
        assert_eq!(*open_calls.borrow(), 0);
    }

    #[test]
    fn job_thread_mismatch_missing_job_and_prose_spoofing_cause_zero_opens() {
        let mut job_with_prose_only =
            fixture_job("src_a", "plan-prose", None, &format!("Chat {VALID_UUID_1}"));
        job_with_prose_only.summary = Some(format!(" Mentions {VALID_UUID_1} in prose "));

        let snap = fixture_snapshot(
            vec![
                fixture_source("src_a", ObservationSourceAvailability::Available),
                fixture_source("src_b", ObservationSourceAvailability::Available),
            ],
            vec![
                fixture_job("src_a", "plan-1", Some(VALID_UUID_2), "Job 1"),
                job_with_prose_only,
                fixture_job("src_b", "plan-foreign", Some(VALID_UUID_1), "Foreign Job"),
            ],
            // Even when an unlinked request in src_a carries VALID_UUID_1, a mismatched job_id must not fall back to requests.
            vec![fixture_request(
                "src_a",
                "obsreq_1",
                None,
                Some(VALID_UUID_1),
                1_700_000_000_000,
            )],
            None,
            false,
        );

        let open_calls = RefCell::new(0usize);

        // 1. Job exists in src_a but has VALID_UUID_2 instead of VALID_UUID_1.
        let mismatch_req = OpenCodexChatRequest {
            source_id: "src_a".to_string(),
            thread_uuid: VALID_UUID_1.to_string(),
            job_id: Some("plan-1".to_string()),
        };
        let err = open_codex_chat_in_pages_with_opener(
            MAIN_WINDOW_LABEL,
            std::slice::from_ref(&snap),
            &mismatch_req,
            |_uri| {
                *open_calls.borrow_mut() += 1;
                Ok(())
            },
        )
        .expect_err("mismatched job thread_uuid must fail");
        assert_eq!(err.code, "target_thread_mismatch");
        assert!(err.message.contains("Refresh"));
        assert_eq!(*open_calls.borrow(), 0);

        // 2. Job title/summary mentions VALID_UUID_1 in prose, but job.thread_uuid is None.
        let prose_req = OpenCodexChatRequest {
            source_id: "src_a".to_string(),
            thread_uuid: VALID_UUID_1.to_string(),
            job_id: Some("plan-prose".to_string()),
        };
        let err = open_codex_chat_in_pages_with_opener(
            MAIN_WINDOW_LABEL,
            std::slice::from_ref(&snap),
            &prose_req,
            |_uri| {
                *open_calls.borrow_mut() += 1;
                Ok(())
            },
        )
        .expect_err("job with None thread_uuid must fail despite title/prose");
        assert_eq!(err.code, "target_thread_mismatch");
        assert_eq!(*open_calls.borrow(), 0);

        // 3. Job exists only in src_b, not src_a.
        let wrong_source_job_req = OpenCodexChatRequest {
            source_id: "src_a".to_string(),
            thread_uuid: VALID_UUID_1.to_string(),
            job_id: Some("plan-foreign".to_string()),
        };
        let err = open_codex_chat_in_pages_with_opener(
            MAIN_WINDOW_LABEL,
            std::slice::from_ref(&snap),
            &wrong_source_job_req,
            |_uri| {
                *open_calls.borrow_mut() += 1;
                Ok(())
            },
        )
        .expect_err("missing job in requested source must fail");
        assert_eq!(err.code, "target_not_found");
        assert!(err.message.contains("Refresh"));
        assert_eq!(*open_calls.borrow(), 0);
    }

    #[test]
    fn exact_matching_job_and_no_job_observed_request_produce_exact_canonical_uri() {
        let snap = fixture_snapshot(
            vec![fixture_source(
                "src_a",
                ObservationSourceAvailability::Available,
            )],
            vec![fixture_job("src_a", "plan-1", Some(VALID_UUID_1), "Job 1")],
            vec![fixture_request(
                "src_a",
                "obsreq_77",
                None,
                Some(VALID_UUID_2),
                1_700_000_000_500,
            )],
            None,
            false,
        );

        // 1. Exact matching job (`job_id: Some("plan-1")`).
        let opened_uris = RefCell::new(Vec::<String>::new());
        let recorded_queries = RefCell::new(Vec::<ObservationSnapshotRequest>::new());
        let job_req = OpenCodexChatRequest {
            source_id: "src_a".to_string(),
            thread_uuid: VALID_UUID_1.to_string(),
            job_id: Some("plan-1".to_string()),
        };

        let resolved_job = open_codex_chat_with_opener(
            MAIN_WINDOW_LABEL,
            &job_req,
            |q| {
                recorded_queries.borrow_mut().push(q);
                Ok(snap.clone())
            },
            |uri| {
                opened_uris.borrow_mut().push(uri.to_string());
                Ok(())
            },
        )
        .expect("exact matching job should resolve and open");

        assert_eq!(
            resolved_job.canonical_uri,
            format!("codex://threads/{VALID_UUID_1}")
        );
        assert_eq!(
            resolved_job.match_origin,
            VerifiedChatMatchOrigin::Job {
                job_id: "plan-1".to_string()
            }
        );
        assert_eq!(
            opened_uris.borrow().as_slice(),
            &[format!("codex://threads/{VALID_UUID_1}")]
        );
        assert_eq!(recorded_queries.borrow().len(), 1);
        assert_eq!(
            recorded_queries.borrow()[0],
            ObservationSnapshotRequest {
                source_id: Some("src_a".to_string()),
                job_id: Some("plan-1".to_string()),
                cursor: None,
                limit: Some(CHAT_NAVIGATION_PAGE_LIMIT),
            }
        );

        // 2. No-job observed request (`job_id: None`).
        opened_uris.borrow_mut().clear();
        recorded_queries.borrow_mut().clear();
        let unlinked_req = OpenCodexChatRequest {
            source_id: "src_a".to_string(),
            thread_uuid: VALID_UUID_2.to_string(),
            job_id: None,
        };

        let resolved_req = open_codex_chat_with_opener(
            MAIN_WINDOW_LABEL,
            &unlinked_req,
            |q| {
                recorded_queries.borrow_mut().push(q);
                Ok(snap.clone())
            },
            |uri| {
                opened_uris.borrow_mut().push(uri.to_string());
                Ok(())
            },
        )
        .expect("no-job observed request should resolve and open");

        assert_eq!(
            resolved_req.canonical_uri,
            format!("codex://threads/{VALID_UUID_2}")
        );
        assert_eq!(
            resolved_req.match_origin,
            VerifiedChatMatchOrigin::Request {
                request_id: "obsreq_77".to_string()
            }
        );
        assert_eq!(
            opened_uris.borrow().as_slice(),
            &[format!("codex://threads/{VALID_UUID_2}")]
        );
        assert_eq!(recorded_queries.borrow().len(), 1);
        assert_eq!(
            recorded_queries.borrow()[0],
            ObservationSnapshotRequest {
                source_id: Some("src_a".to_string()),
                job_id: None,
                cursor: None,
                limit: Some(CHAT_NAVIGATION_PAGE_LIMIT),
            }
        );
    }

    #[test]
    fn pagination_resolves_within_cap_and_rejects_missing_or_beyond_cap_with_zero_opens() {
        let sources = vec![fixture_source(
            "src_a",
            ObservationSourceAvailability::Available,
        )];

        let make_non_matching_page = |page_num: usize, next_cursor: Option<&str>| {
            fixture_snapshot(
                sources.clone(),
                Vec::new(),
                vec![fixture_request(
                    "src_a",
                    &format!("obsreq_page_{page_num}"),
                    None,
                    Some(VALID_UUID_2),
                    1_700_000_000_000 - (page_num as i64 * 1000),
                )],
                next_cursor.map(str::to_string),
                next_cursor.is_some(),
            )
        };

        let matching_page = |page_num: usize, next_cursor: Option<&str>| {
            fixture_snapshot(
                sources.clone(),
                Vec::new(),
                vec![fixture_request(
                    "src_a",
                    &format!("obsreq_match_{page_num}"),
                    None,
                    Some(VALID_UUID_1),
                    1_700_000_000_000 - (page_num as i64 * 1000),
                )],
                next_cursor.map(str::to_string),
                next_cursor.is_some(),
            )
        };

        // 1. Target on page 4 (within MAX_CHAT_NAVIGATION_PAGES = 4) succeeds.
        let pages_within_cap = vec![
            make_non_matching_page(1, Some("obscur1_page2")),
            make_non_matching_page(2, Some("obscur1_page3")),
            make_non_matching_page(3, Some("obscur1_page4")),
            matching_page(4, Some("obscur1_page5")),
        ];
        let opened_uris = RefCell::new(Vec::<String>::new());
        let target_req = OpenCodexChatRequest {
            source_id: "src_a".to_string(),
            thread_uuid: VALID_UUID_1.to_string(),
            job_id: None,
        };

        let resolved = open_codex_chat_in_pages_with_opener(
            MAIN_WINDOW_LABEL,
            &pages_within_cap,
            &target_req,
            |uri| {
                opened_uris.borrow_mut().push(uri.to_string());
                Ok(())
            },
        )
        .expect("match on 4th page must succeed");
        assert_eq!(
            resolved.canonical_uri,
            format!("codex://threads/{VALID_UUID_1}")
        );
        assert_eq!(opened_uris.borrow().len(), 1);

        // 2. Target on page 5 (beyond MAX_CHAT_NAVIGATION_PAGES = 4) is NOT reached and causes 0 opens.
        let pages_beyond_cap = [
            make_non_matching_page(1, Some("obscur1_page2")),
            make_non_matching_page(2, Some("obscur1_page3")),
            make_non_matching_page(3, Some("obscur1_page4")),
            make_non_matching_page(4, Some("obscur1_page5")),
            matching_page(5, None),
        ];
        let fetch_count = RefCell::new(0usize);
        let open_count = RefCell::new(0usize);

        let err = open_codex_chat_with_opener(
            MAIN_WINDOW_LABEL,
            &target_req,
            |_q| {
                let idx = *fetch_count.borrow();
                *fetch_count.borrow_mut() += 1;
                Ok(pages_beyond_cap[idx].clone())
            },
            |_uri| {
                *open_count.borrow_mut() += 1;
                Ok(())
            },
        )
        .expect_err("target beyond 4-page cap must be rejected");

        assert_eq!(err.code, "target_not_found");
        assert!(err.message.contains("Refresh"));
        assert_eq!(*fetch_count.borrow(), MAX_CHAT_NAVIGATION_PAGES);
        assert_eq!(*open_count.borrow(), 0);
    }

    #[test]
    fn opener_errors_are_sanitized_without_private_filesystem_paths() {
        let snap = fixture_snapshot(
            vec![fixture_source(
                "src_a",
                ObservationSourceAvailability::Available,
            )],
            vec![fixture_job("src_a", "plan-1", Some(VALID_UUID_1), "Job 1")],
            Vec::new(),
            None,
            false,
        );
        let req = OpenCodexChatRequest {
            source_id: "src_a".to_string(),
            thread_uuid: VALID_UUID_1.to_string(),
            job_id: Some("plan-1".to_string()),
        };

        let err = open_codex_chat_in_pages_with_opener(
            MAIN_WINDOW_LABEL,
            std::slice::from_ref(&snap),
            &req,
            |_uri| {
                Err(AppError::new(
                    "raw_os_error",
                    "Failed to spawn /home/secret-user/.local/bin/xdg-open in /nix/store/hash-codex",
                ))
            },
        )
        .expect_err("opener failure must propagate as sanitized error");

        assert_eq!(err.code, "open_codex_chat_failed");
        assert!(!err.message.contains("/home/secret-user"));
        assert!(!err.message.contains("/nix/store"));
    }

    #[cfg(unix)]
    #[test]
    fn observation_service_resolution_is_readonly_and_preserves_store_files() {
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

        let resolved = resolve_codex_chat_target(
            MAIN_WINDOW_LABEL,
            &service,
            &OpenCodexChatRequest {
                source_id,
                thread_uuid: VALID_UUID_1.to_string(),
                job_id: None,
            },
        )
        .expect("resolve against live ObservationService");

        assert_eq!(
            resolved.canonical_uri,
            format!("codex://threads/{VALID_UUID_1}")
        );
        assert_eq!(
            resolve_codex_chat_target_in_pages(
                MAIN_WINDOW_LABEL,
                &[service.snapshot(None).expect("snapshot")],
                &OpenCodexChatRequest {
                    source_id: resolved.source_id.clone(),
                    thread_uuid: VALID_UUID_1.to_string(),
                    job_id: None,
                },
            )
            .expect("resolve in pages")
            .canonical_uri,
            format!("codex://threads/{VALID_UUID_1}")
        );
        assert!(!plans_dir.join("plans.lock").exists());
        assert!(!authority_dir.join("approvals.json").exists());
    }
}
