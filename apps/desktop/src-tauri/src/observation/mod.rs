pub(crate) mod coworker_jobs;
pub(crate) mod jobs;
pub(crate) mod requests;
pub(crate) mod sources;

use std::cmp::Reverse;
use std::collections::BTreeSet;
#[cfg(unix)]
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use threadlane_core::observation::{
    current_observation_time_ms, read_journal_records_readonly, sanitize_observation_summary,
    validate_correlation_id, ObservationJob, ObservationJournalRecord, ObservationRequest,
    ObservationSnapshot, ObservationSnapshotRequest, ObservationSource, OBSERVATION_SCHEMA_VERSION,
};

use crate::AppError;

pub(crate) use sources::{
    ObservationSourceConfig, ObservationSourceError, ObservationSourceRegistry, SourceSpec,
};

/// Default number of requests returned per page when `limit` is omitted.
pub(crate) const DEFAULT_REQUEST_PAGE_LIMIT: usize = 50;

/// Maximum number of requests allowed per page.
pub(crate) const MAX_REQUEST_PAGE_LIMIT: usize = 200;

/// Global maximum number of jobs returned in a single observation snapshot.
pub(crate) const MAX_OBSERVATION_JOBS: usize = 500;

/// Maximum number of sanitized warnings returned in a single observation snapshot.
pub(crate) const MAX_OBSERVATION_WARNINGS: usize = 32;

/// Maximum encoded string length permitted for a request pagination cursor.
pub(crate) const MAX_CURSOR_BYTES: usize = 2048;

const CURSOR_VERSION: u32 = 1;
const CURSOR_PREFIX: &str = "obscur1_";

/// Cloneable read-only observation service backed by [`ObservationSourceRegistry`].
#[derive(Debug, Clone)]
pub(crate) struct ObservationService {
    registry: ObservationSourceRegistry,
}

impl ObservationService {
    /// Creates a new [`ObservationService`] from the initial source configuration.
    pub(crate) fn new(config: ObservationSourceConfig) -> Self {
        Self {
            registry: ObservationSourceRegistry::new(config),
        }
    }

    /// Updates the optional attached runtime `state_dir` on the underlying source registry.
    pub(crate) fn set_runtime_state_dir(&self, runtime_state_dir: Option<PathBuf>) {
        self.registry.set_runtime_state_dir(runtime_state_dir);
    }

    /// Returns the current inventory of configured and linked observation sources.
    pub(crate) fn sources(&self) -> Vec<ObservationSource> {
        let registry = self.registry.clone();
        registry.inventory().sources
    }

    /// Explicitly links a validated private Threadlane context file into the observation registry.
    pub(crate) fn link_context(&self, path: &Path) -> Result<ObservationSource, AppError> {
        let raw = path.to_string_lossy();
        if raw.is_empty() || raw.as_ref() != raw.trim() {
            return Err(map_source_error(ObservationSourceError::InvalidContextPath));
        }
        let registry = self.registry.clone();
        registry.link_context(path).map_err(map_source_error)
    }

    /// Projects a point-in-time read-only [`ObservationSnapshot`] across available sources.
    pub(crate) fn snapshot(
        &self,
        request: Option<ObservationSnapshotRequest>,
    ) -> Result<ObservationSnapshot, AppError> {
        let query = validate_snapshot_request(request.as_ref())?;
        let registry = self.registry.clone();
        let inventory = registry.inventory();

        let mut raw_warnings = inventory.warnings;
        let mut all_jobs = Vec::new();
        let mut all_requests = Vec::new();
        let mut upstream_jobs_has_more = false;

        let mut seen_source_ids = BTreeSet::new();
        let mut seen_physical_stores = BTreeSet::new();
        #[cfg(unix)]
        let mut seen_store_inodes = BTreeSet::new();

        for spec in &inventory.specs {
            if let Some(ref target_source_id) = query.source_id {
                if &spec.source.id != target_source_id {
                    continue;
                }
            }
            if !seen_source_ids.insert(spec.source.id.clone()) {
                continue;
            }
            if !seen_physical_stores.insert((
                spec.plans_dir.clone(),
                spec.authority_dir.clone(),
                spec.sessions_dir.clone(),
            )) {
                continue;
            }
            #[cfg(unix)]
            if let Some(inode_key) = physical_store_inode_key(spec) {
                if !seen_store_inodes.insert(inode_key) {
                    continue;
                }
            }

            let mut job_projection = jobs::project_jobs(spec);
            raw_warnings.extend(job_projection.warnings);
            if job_projection.jobs_has_more {
                upstream_jobs_has_more = true;
            }

            let (journal_records, journal_warnings) = read_source_journal_records(spec);
            raw_warnings.extend(journal_warnings);

            let prepared_journal = coworker_jobs::prepare_journal_records_for_projection(
                &journal_records,
                &job_projection.coworker_scopes,
            );
            raw_warnings.extend(prepared_journal.warnings.clone());

            let mut request_projection = requests::project_requests(
                &spec.source.id,
                &job_projection.jobs,
                &prepared_journal.records,
                &job_projection.core_states,
                &job_projection.worker_links,
            );
            coworker_jobs::finalize_projected_coworker_requests(
                &mut job_projection.jobs,
                &mut request_projection,
                &job_projection.coworker_scopes,
                &prepared_journal,
            );
            raw_warnings.extend(request_projection.warnings);

            all_jobs.extend(job_projection.jobs);
            all_requests.extend(request_projection.requests);
        }

        let jobs_page = filter_and_cap_jobs(all_jobs, &query, upstream_jobs_has_more);
        if let Some(cap_warning) = jobs_page.cap_warning {
            raw_warnings.push(cap_warning);
        }

        let requests_page = filter_sort_and_page_requests(all_requests, &query);
        let warnings = sanitize_and_bound_warnings(raw_warnings);

        Ok(ObservationSnapshot {
            schema_version: OBSERVATION_SCHEMA_VERSION,
            generated_at_ms: current_observation_time_ms(),
            sources: inventory.sources,
            jobs: jobs_page.jobs,
            requests: requests_page.requests,
            next_cursor: requests_page.next_cursor,
            has_more: requests_page.has_more,
            jobs_has_more: jobs_page.jobs_has_more,
            warnings,
        })
    }
}

#[cfg(unix)]
type StoreTriadInodeKey = ((u64, u64), (u64, u64), (u64, u64));

#[cfg(unix)]
fn physical_store_inode_key(spec: &SourceSpec) -> Option<StoreTriadInodeKey> {
    let plans_meta = std::fs::symlink_metadata(&spec.plans_dir).ok()?;
    let authority_meta = std::fs::symlink_metadata(&spec.authority_dir).ok()?;
    let sessions_meta = std::fs::symlink_metadata(&spec.sessions_dir).ok()?;
    if plans_meta.file_type().is_symlink()
        || authority_meta.file_type().is_symlink()
        || sessions_meta.file_type().is_symlink()
    {
        return None;
    }
    Some((
        (plans_meta.dev(), plans_meta.ino()),
        (authority_meta.dev(), authority_meta.ino()),
        (sessions_meta.dev(), sessions_meta.ino()),
    ))
}

fn read_source_journal_records(spec: &SourceSpec) -> (Vec<ObservationJournalRecord>, Vec<String>) {
    let mut records = Vec::new();
    let mut warnings = Vec::new();
    let mut seen_record_ids = BTreeSet::new();
    let mut seen_paths = BTreeSet::new();
    #[cfg(unix)]
    let mut seen_dir_inodes = BTreeSet::new();

    let candidate_dirs = std::iter::once(("plans", spec.plans_dir.as_path())).chain(
        spec.runtime_state_dirs
            .iter()
            .map(|dir| ("runtime", dir.as_path())),
    );

    for (kind_label, dir) in candidate_dirs {
        if !seen_paths.insert(dir.to_path_buf()) {
            continue;
        }
        #[cfg(unix)]
        if let Ok(meta) = std::fs::symlink_metadata(dir) {
            if !meta.file_type().is_symlink() && !seen_dir_inodes.insert((meta.dev(), meta.ino())) {
                continue;
            }
        }

        match read_journal_records_readonly(dir) {
            Ok((dir_records, dir_warnings)) => {
                for warning in dir_warnings {
                    warnings.push(format!(
                        "Source {} ({kind_label} journal): {}",
                        spec.source.id,
                        sanitize_observation_summary(&warning)
                    ));
                }
                for record in dir_records {
                    let record_id = record.record_id.trim();
                    if record_id.is_empty() {
                        warnings.push(format!(
                            "Source {} ({kind_label} journal): skipped journal record with empty record_id.",
                            spec.source.id
                        ));
                        continue;
                    }
                    if seen_record_ids.insert(record_id.to_string()) {
                        records.push(record);
                    }
                }
            }
            Err(_) => {
                warnings.push(format!(
                    "Source {}: failed to read {kind_label} observation journal in read-only mode.",
                    spec.source.id
                ));
            }
        }
    }

    (records, warnings)
}

fn map_source_error(err: ObservationSourceError) -> AppError {
    let code = match err {
        #[cfg(not(unix))]
        ObservationSourceError::UnsupportedPlatform => "unsupported_platform",
        ObservationSourceError::InvalidContextPath => "invalid_context_path",
        ObservationSourceError::InaccessibleContextFile => "inaccessible_context_file",
        ObservationSourceError::InvalidContextFormat => "invalid_context_format",
        ObservationSourceError::InvalidSchemaVersion => "invalid_schema_version",
        ObservationSourceError::ForbiddenContextAlias => "forbidden_context_alias",
        ObservationSourceError::InvalidContextRoute => "invalid_context_route",
        ObservationSourceError::InvalidContextIdentityFlag => "invalid_context_identity_flag",
        ObservationSourceError::InvalidBindingId => "invalid_binding_id",
        ObservationSourceError::InvalidThreadUuid => "invalid_thread_uuid",
        ObservationSourceError::InvalidProjectRoot => "invalid_project_root",
        ObservationSourceError::InvalidStoreDirectory => "invalid_store_directory",
        ObservationSourceError::OverlappingStoreDirectories => "overlapping_store_directories",
        ObservationSourceError::ProjectRootOverlapsStore => "project_root_overlaps_store",
        ObservationSourceError::InvalidAppDataDirectory => "invalid_app_data_directory",
        ObservationSourceError::InvalidLinkRegistry => "invalid_link_registry",
        ObservationSourceError::LinkRegistryLimitReached => "link_registry_limit_reached",
        ObservationSourceError::LinkRegistryWriteFailed => "link_registry_write_failed",
    };
    AppError::new(code, err.message())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CursorPosition {
    pub(crate) updated_at_ms: i64,
    pub(crate) source_id: String,
    pub(crate) request_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ValidatedSnapshotQuery {
    pub(crate) source_id: Option<String>,
    pub(crate) job_id: Option<String>,
    pub(crate) cursor: Option<CursorPosition>,
    pub(crate) limit: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct EncodedCursorPayload {
    v: u32,
    scope_source_id: Option<String>,
    scope_job_id: Option<String>,
    updated_at_ms: i64,
    source_id: String,
    request_id: String,
}

fn validate_strict_filter_id(raw: &str, field_name: &'static str) -> Result<String, AppError> {
    if raw.is_empty() || raw != raw.trim() {
        return Err(AppError::new(
            "invalid_input",
            format!("Observation {field_name} filter must be a non-empty trimmed identifier."),
        ));
    }
    validate_correlation_id(raw).ok_or_else(|| {
        AppError::new(
            "invalid_input",
            format!("Observation {field_name} filter is invalid or exceeds size bounds."),
        )
    })
}

pub(crate) fn validate_snapshot_request(
    request: Option<&ObservationSnapshotRequest>,
) -> Result<ValidatedSnapshotQuery, AppError> {
    let Some(req) = request else {
        return Ok(ValidatedSnapshotQuery {
            source_id: None,
            job_id: None,
            cursor: None,
            limit: DEFAULT_REQUEST_PAGE_LIMIT,
        });
    };

    let source_id = match req.source_id.as_deref() {
        Some(raw) => Some(validate_strict_filter_id(raw, "source_id")?),
        None => None,
    };

    let job_id = match req.job_id.as_deref() {
        Some(raw) => Some(validate_strict_filter_id(raw, "job_id")?),
        None => None,
    };

    if job_id.is_some() && source_id.is_none() {
        return Err(AppError::new(
            "invalid_input",
            "Observation job_id filter requires a non-empty source_id filter.",
        ));
    }

    let limit = match req.limit {
        None => DEFAULT_REQUEST_PAGE_LIMIT,
        Some(0) => {
            return Err(AppError::new(
                "invalid_input",
                "Observation page limit must be between 1 and 200.",
            ));
        }
        Some(n) if n > MAX_REQUEST_PAGE_LIMIT => {
            return Err(AppError::new(
                "invalid_input",
                "Observation page limit must be between 1 and 200.",
            ));
        }
        Some(n) => n,
    };

    let cursor = match req.cursor.as_deref() {
        Some(raw_cursor) => Some(decode_and_validate_cursor(
            raw_cursor,
            source_id.as_deref(),
            job_id.as_deref(),
        )?),
        None => None,
    };

    Ok(ValidatedSnapshotQuery {
        source_id,
        job_id,
        cursor,
        limit,
    })
}

pub(crate) fn encode_request_cursor(
    scope_source_id: Option<&str>,
    scope_job_id: Option<&str>,
    last_request: &ObservationRequest,
) -> String {
    let payload = EncodedCursorPayload {
        v: CURSOR_VERSION,
        scope_source_id: scope_source_id.map(str::to_string),
        scope_job_id: scope_job_id.map(str::to_string),
        updated_at_ms: last_request.updated_at_ms,
        source_id: last_request.source_id.clone(),
        request_id: last_request.id.clone(),
    };
    let json_bytes =
        serde_json::to_vec(&payload).expect("cursor payload serialization cannot fail");
    format!("{CURSOR_PREFIX}{}", encode_hex_lower(&json_bytes))
}

pub(crate) fn decode_and_validate_cursor(
    raw_cursor: &str,
    expected_source_id: Option<&str>,
    expected_job_id: Option<&str>,
) -> Result<CursorPosition, AppError> {
    if raw_cursor.is_empty()
        || raw_cursor != raw_cursor.trim()
        || raw_cursor.len() > MAX_CURSOR_BYTES
        || raw_cursor
            .bytes()
            .any(|b| b.is_ascii_whitespace() || b.is_ascii_control())
    {
        return Err(AppError::new(
            "invalid_cursor",
            "Observation pagination cursor is malformed or exceeds size bounds.",
        ));
    }

    let hex_body = raw_cursor.strip_prefix(CURSOR_PREFIX).ok_or_else(|| {
        AppError::new(
            "invalid_cursor",
            "Observation pagination cursor has an unsupported version or format.",
        )
    })?;

    let json_bytes = decode_hex_lower(hex_body).ok_or_else(|| {
        AppError::new(
            "invalid_cursor",
            "Observation pagination cursor encoding is malformed.",
        )
    })?;

    let payload: EncodedCursorPayload = serde_json::from_slice(&json_bytes).map_err(|_| {
        AppError::new(
            "invalid_cursor",
            "Observation pagination cursor payload is malformed.",
        )
    })?;

    if payload.v != CURSOR_VERSION {
        return Err(AppError::new(
            "invalid_cursor",
            "Observation pagination cursor version is unsupported.",
        ));
    }

    let validated_source_id = validate_strict_cursor_id(&payload.source_id)?;
    let validated_request_id = validate_strict_cursor_id(&payload.request_id)?;
    if expected_source_id.is_some_and(|source| source != validated_source_id) {
        return Err(AppError::new(
            "cursor_scope_mismatch",
            "Observation pagination cursor position does not match the active source.",
        ));
    }

    let cursor_scope_source = match payload.scope_source_id.as_deref() {
        Some(raw) => Some(validate_strict_cursor_id(raw)?),
        None => None,
    };
    let cursor_scope_job = match payload.scope_job_id.as_deref() {
        Some(raw) => Some(validate_strict_cursor_id(raw)?),
        None => None,
    };

    if cursor_scope_job.is_some() && cursor_scope_source.is_none() {
        return Err(AppError::new(
            "invalid_cursor",
            "Observation pagination cursor contains an invalid scope.",
        ));
    }

    if cursor_scope_source.as_deref() != expected_source_id
        || cursor_scope_job.as_deref() != expected_job_id
    {
        return Err(AppError::new(
            "cursor_scope_mismatch",
            "Observation pagination cursor scope does not match the active filter.",
        ));
    }

    Ok(CursorPosition {
        updated_at_ms: payload.updated_at_ms,
        source_id: validated_source_id,
        request_id: validated_request_id,
    })
}

fn validate_strict_cursor_id(raw: &str) -> Result<String, AppError> {
    if raw.is_empty() || raw != raw.trim() {
        return Err(AppError::new(
            "invalid_cursor",
            "Observation pagination cursor contains an invalid identifier.",
        ));
    }
    validate_correlation_id(raw).ok_or_else(|| {
        AppError::new(
            "invalid_cursor",
            "Observation pagination cursor contains an invalid identifier.",
        )
    })
}

fn encode_hex_lower(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

fn decode_hex_lower(hex: &str) -> Option<Vec<u8>> {
    let bytes = hex.as_bytes();
    if bytes.is_empty() || !bytes.len().is_multiple_of(2) {
        return None;
    }
    let mut out = Vec::with_capacity(bytes.len() / 2);
    for chunk in bytes.as_chunks::<2>().0 {
        let hi = decode_hex_nibble(chunk[0])?;
        let lo = decode_hex_nibble(chunk[1])?;
        out.push((hi << 4) | lo);
    }
    Some(out)
}

const fn decode_hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        _ => None,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ProjectedJobsPage {
    pub(crate) jobs: Vec<ObservationJob>,
    pub(crate) jobs_has_more: bool,
    pub(crate) cap_warning: Option<String>,
}

pub(crate) fn filter_and_cap_jobs(
    jobs: Vec<ObservationJob>,
    query: &ValidatedSnapshotQuery,
    upstream_jobs_has_more: bool,
) -> ProjectedJobsPage {
    let mut filtered: Vec<ObservationJob> = jobs
        .into_iter()
        .filter(|job| {
            if let Some(ref target_source) = query.source_id {
                if &job.source_id != target_source {
                    return false;
                }
            }
            if let Some(ref target_job) = query.job_id {
                if &job.id != target_job {
                    return false;
                }
            }
            true
        })
        .collect();

    filtered.sort_by(|a, b| {
        b.updated_at_ms
            .unwrap_or(i64::MIN)
            .cmp(&a.updated_at_ms.unwrap_or(i64::MIN))
            .then_with(|| a.source_id.cmp(&b.source_id))
            .then_with(|| a.id.cmp(&b.id))
    });

    let mut seen_keys = BTreeSet::new();
    filtered.retain(|job| seen_keys.insert((job.source_id.clone(), job.id.clone())));

    if filtered.len() > MAX_OBSERVATION_JOBS {
        let omitted = filtered.len() - MAX_OBSERVATION_JOBS;
        filtered.truncate(MAX_OBSERVATION_JOBS);
        ProjectedJobsPage {
            jobs: filtered,
            jobs_has_more: true,
            cap_warning: Some(format!(
                "Observation job list reached the maximum display cap ({MAX_OBSERVATION_JOBS} jobs); {omitted} older job(s) were omitted."
            )),
        }
    } else {
        ProjectedJobsPage {
            jobs: filtered,
            jobs_has_more: upstream_jobs_has_more,
            cap_warning: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ProjectedRequestsPage {
    pub(crate) requests: Vec<ObservationRequest>,
    pub(crate) next_cursor: Option<String>,
    pub(crate) has_more: bool,
}

fn request_order_key(req: &ObservationRequest) -> (Reverse<i64>, &str, &str) {
    (
        Reverse(req.updated_at_ms),
        req.source_id.as_str(),
        req.id.as_str(),
    )
}

pub(crate) fn filter_sort_and_page_requests(
    requests: Vec<ObservationRequest>,
    query: &ValidatedSnapshotQuery,
) -> ProjectedRequestsPage {
    let mut filtered: Vec<ObservationRequest> = requests
        .into_iter()
        .filter(|req| {
            if let Some(ref target_source) = query.source_id {
                if &req.source_id != target_source {
                    return false;
                }
            }
            if let Some(ref target_job) = query.job_id {
                if req.job_id.as_deref() != Some(target_job.as_str()) {
                    return false;
                }
            }
            true
        })
        .collect();

    filtered.sort_by(|a, b| request_order_key(a).cmp(&request_order_key(b)));

    let mut seen_keys = BTreeSet::new();
    filtered.retain(|req| seen_keys.insert((req.source_id.clone(), req.id.clone())));

    if let Some(ref cursor) = query.cursor {
        let cursor_key = (
            Reverse(cursor.updated_at_ms),
            cursor.source_id.as_str(),
            cursor.request_id.as_str(),
        );
        filtered.retain(|req| request_order_key(req) > cursor_key);
    }

    if filtered.len() > query.limit {
        filtered.truncate(query.limit);
        let next_cursor = filtered.last().map(|last| {
            encode_request_cursor(query.source_id.as_deref(), query.job_id.as_deref(), last)
        });
        ProjectedRequestsPage {
            requests: filtered,
            next_cursor,
            has_more: true,
        }
    } else {
        ProjectedRequestsPage {
            requests: filtered,
            next_cursor: None,
            has_more: false,
        }
    }
}

pub(crate) fn sanitize_and_bound_warnings(
    raw_warnings: impl IntoIterator<Item = String>,
) -> Vec<String> {
    let mut unique = Vec::new();
    for raw in raw_warnings {
        let sanitized = sanitize_observation_summary(&raw);
        let trimmed = sanitized.trim();
        if trimmed.is_empty() {
            continue;
        }
        let clean = trimmed.to_string();
        if !unique.contains(&clean) {
            unique.push(clean);
        }
    }

    if unique.len() > MAX_OBSERVATION_WARNINGS {
        let kept = MAX_OBSERVATION_WARNINGS - 1;
        let omitted = unique.len() - kept;
        unique.truncate(kept);
        unique.push(format!(
            "Additional observation warnings ({omitted} more) were omitted."
        ));
    }

    unique
}

#[cfg(test)]
mod tests {
    use super::*;
    use threadlane_core::observation::{
        ObservationActor, ObservationCallState, ObservationJobState,
    };

    fn sample_request(
        source_id: &str,
        id: &str,
        job_id: Option<&str>,
        updated_at_ms: i64,
    ) -> ObservationRequest {
        ObservationRequest {
            source_id: source_id.to_string(),
            id: id.to_string(),
            job_id: job_id.map(str::to_string),
            parent_request_id: None,
            title: format!("Request {id}"),
            operation: "context.workflow.status".to_string(),
            project_label: "Threadlane local".to_string(),
            binding_id: Some("bnd_1".to_string()),
            thread_uuid: Some("550e8400-e29b-41d4-a716-446655440000".to_string()),
            thread_label: Some("Main Thread".to_string()),
            state: ObservationCallState::Completed,
            started_at_ms: Some(updated_at_ms - 50),
            finished_at_ms: Some(updated_at_ms),
            updated_at_ms,
            duration_ms: Some(50),
            actor: ObservationActor::unknown(Some("trusted_local_operator".to_string())),
            executor: None,
            usage: None,
            delivery: Vec::new(),
            children: Vec::new(),
            children_count: 0,
            summary: Some("Completed status check".to_string()),
        }
    }

    fn sample_job(source_id: &str, id: &str, updated_at_ms: Option<i64>) -> ObservationJob {
        ObservationJob {
            source_id: source_id.to_string(),
            id: id.to_string(),
            project_label: "Threadlane local".to_string(),
            project_path: None,
            title: format!("Job {id}"),
            thread_uuid: None,
            thread_label: None,
            binding_id: Some("bnd_1".to_string()),
            plan_id: Some(id.to_string()),
            plan_revision: Some(1),
            state: ObservationJobState::Ready,
            approval_status: "approved".to_string(),
            execution_status: "not_started".to_string(),
            review_state: None,
            updated_at_ms,
            active_call_count: 0,
            tasks: Vec::new(),
            summary: None,
        }
    }

    #[test]
    fn request_validation_rejects_invalid_limits_filters_and_orphaned_job_id() {
        let default_q = validate_snapshot_request(None).expect("default query should succeed");
        assert_eq!(default_q.limit, DEFAULT_REQUEST_PAGE_LIMIT);
        assert!(default_q.source_id.is_none());
        assert!(default_q.job_id.is_none());

        // Limit 0 and > 200 must fail.
        let zero_limit = ObservationSnapshotRequest {
            limit: Some(0),
            ..Default::default()
        };
        assert_eq!(
            validate_snapshot_request(Some(&zero_limit))
                .unwrap_err()
                .code,
            "invalid_input"
        );

        let over_limit = ObservationSnapshotRequest {
            limit: Some(MAX_REQUEST_PAGE_LIMIT + 1),
            ..Default::default()
        };
        assert_eq!(
            validate_snapshot_request(Some(&over_limit))
                .unwrap_err()
                .code,
            "invalid_input"
        );

        // job_id without source_id must fail.
        let orphan_job = ObservationSnapshotRequest {
            source_id: None,
            job_id: Some("plan_1".to_string()),
            ..Default::default()
        };
        assert_eq!(
            validate_snapshot_request(Some(&orphan_job))
                .unwrap_err()
                .code,
            "invalid_input"
        );

        // Whitespace or oversized filters must fail, never fake empty.
        for bad_filter in ["", "   ", " src_1", "src_1 ", "src with space"] {
            let req = ObservationSnapshotRequest {
                source_id: Some(bad_filter.to_string()),
                ..Default::default()
            };
            assert_eq!(
                validate_snapshot_request(Some(&req)).unwrap_err().code,
                "invalid_input"
            );
        }

        let oversized_filter = ObservationSnapshotRequest {
            source_id: Some("a".repeat(129)),
            ..Default::default()
        };
        assert_eq!(
            validate_snapshot_request(Some(&oversized_filter))
                .unwrap_err()
                .code,
            "invalid_input"
        );
    }

    #[test]
    fn cursor_validation_rejects_malformed_oversized_and_mismatched_scope_cursors() {
        let req_item = sample_request("src_a", "obsreq_10", Some("plan_1"), 1_700_000_000_000);
        let valid_cursor = encode_request_cursor(Some("src_a"), Some("plan_1"), &req_item);

        let foreign_position =
            sample_request("src_z", "obsreq_10", Some("plan_1"), 1_700_000_000_000);
        let edited_cursor = encode_request_cursor(Some("src_a"), Some("plan_1"), &foreign_position);
        assert_eq!(
            decode_and_validate_cursor(&edited_cursor, Some("src_a"), Some("plan_1"))
                .unwrap_err()
                .code,
            "cursor_scope_mismatch"
        );

        // Matching scope succeeds.
        let matching_req = ObservationSnapshotRequest {
            source_id: Some("src_a".to_string()),
            job_id: Some("plan_1".to_string()),
            cursor: Some(valid_cursor.clone()),
            limit: Some(25),
        };
        let validated =
            validate_snapshot_request(Some(&matching_req)).expect("matching cursor must validate");
        assert_eq!(
            validated.cursor,
            Some(CursorPosition {
                updated_at_ms: 1_700_000_000_000,
                source_id: "src_a".to_string(),
                request_id: "obsreq_10".to_string(),
            })
        );

        // Mismatched source_id scope fails with cursor_scope_mismatch.
        let wrong_source_req = ObservationSnapshotRequest {
            source_id: Some("src_b".to_string()),
            job_id: Some("plan_1".to_string()),
            cursor: Some(valid_cursor.clone()),
            limit: Some(25),
        };
        assert_eq!(
            validate_snapshot_request(Some(&wrong_source_req))
                .unwrap_err()
                .code,
            "cursor_scope_mismatch"
        );

        // Mismatched job_id scope fails with cursor_scope_mismatch.
        let wrong_job_req = ObservationSnapshotRequest {
            source_id: Some("src_a".to_string()),
            job_id: Some("plan_2".to_string()),
            cursor: Some(valid_cursor.clone()),
            limit: Some(25),
        };
        assert_eq!(
            validate_snapshot_request(Some(&wrong_job_req))
                .unwrap_err()
                .code,
            "cursor_scope_mismatch"
        );

        // Unfiltered request with scoped cursor fails with cursor_scope_mismatch.
        let unfiltered_req = ObservationSnapshotRequest {
            source_id: None,
            job_id: None,
            cursor: Some(valid_cursor),
            limit: Some(25),
        };
        assert_eq!(
            validate_snapshot_request(Some(&unfiltered_req))
                .unwrap_err()
                .code,
            "cursor_scope_mismatch"
        );

        // Malformed and oversized cursors fail with invalid_cursor.
        for bad_cursor in [
            "".to_string(),
            "   ".to_string(),
            "obscur1_not_hex!".to_string(),
            "obscur1_abc".to_string(),
            "obscur2_7b7d".to_string(),
            "obscur1_7b7d".to_string(),
            format!("obscur1_{}", "a".repeat(MAX_CURSOR_BYTES)),
        ] {
            let req = ObservationSnapshotRequest {
                cursor: Some(bad_cursor),
                ..Default::default()
            };
            assert_eq!(
                validate_snapshot_request(Some(&req)).unwrap_err().code,
                "invalid_cursor"
            );
        }
    }

    #[test]
    fn request_paging_is_deterministic_across_same_timestamps_and_multi_source_ids() {
        let t_same = 1_700_000_005_000;
        let t_older = 1_700_000_001_000;

        // Intentionally supply out-of-order requests with identical timestamps and overlapping IDs across sources.
        let all = vec![
            sample_request("src_b", "obsreq_2", Some("plan_shared"), t_same),
            sample_request("src_a", "obsreq_2", Some("plan_shared"), t_same),
            sample_request("src_b", "obsreq_1", Some("plan_shared"), t_same),
            sample_request("src_a", "obsreq_1", Some("plan_shared"), t_same),
            sample_request("src_a", "obsreq_0", Some("plan_shared"), t_older),
        ];

        // Page 1 (limit 2).
        let q1 = validate_snapshot_request(Some(&ObservationSnapshotRequest {
            limit: Some(2),
            ..Default::default()
        }))
        .unwrap();
        let page1 = filter_sort_and_page_requests(all.clone(), &q1);
        assert_eq!(page1.requests.len(), 2);
        assert_eq!(
            (&page1.requests[0].source_id, &page1.requests[0].id),
            (&"src_a".to_string(), &"obsreq_1".to_string())
        );
        assert_eq!(
            (&page1.requests[1].source_id, &page1.requests[1].id),
            (&"src_a".to_string(), &"obsreq_2".to_string())
        );
        assert!(page1.has_more);
        assert!(page1.next_cursor.is_some());

        // Page 2 (limit 2).
        let q2 = validate_snapshot_request(Some(&ObservationSnapshotRequest {
            cursor: page1.next_cursor.clone(),
            limit: Some(2),
            ..Default::default()
        }))
        .unwrap();
        let page2 = filter_sort_and_page_requests(all.clone(), &q2);
        assert_eq!(page2.requests.len(), 2);
        assert_eq!(
            (&page2.requests[0].source_id, &page2.requests[0].id),
            (&"src_b".to_string(), &"obsreq_1".to_string())
        );
        assert_eq!(
            (&page2.requests[1].source_id, &page2.requests[1].id),
            (&"src_b".to_string(), &"obsreq_2".to_string())
        );
        assert!(page2.has_more);
        assert!(page2.next_cursor.is_some());

        // Page 3 (limit 2) reaches the final item.
        let q3 = validate_snapshot_request(Some(&ObservationSnapshotRequest {
            cursor: page2.next_cursor,
            limit: Some(2),
            ..Default::default()
        }))
        .unwrap();
        let page3 = filter_sort_and_page_requests(all.clone(), &q3);
        assert_eq!(page3.requests.len(), 1);
        assert_eq!(
            (&page3.requests[0].source_id, &page3.requests[0].id),
            (&"src_a".to_string(), &"obsreq_0".to_string())
        );
        assert!(!page3.has_more);
        assert!(page3.next_cursor.is_none());

        // Filtering by source_id computes has_more/next_cursor strictly after filter.
        let filtered_src_b = validate_snapshot_request(Some(&ObservationSnapshotRequest {
            source_id: Some("src_b".to_string()),
            limit: Some(2),
            ..Default::default()
        }))
        .unwrap();
        let src_b_page = filter_sort_and_page_requests(all, &filtered_src_b);
        assert_eq!(src_b_page.requests.len(), 2);
        assert!(!src_b_page.has_more);
        assert!(src_b_page.next_cursor.is_none());
    }

    #[test]
    fn request_paging_handles_new_head_insertions_and_updated_requests_moving_above_cursor() {
        let initial = vec![
            sample_request("src_a", "obsreq_3", Some("plan_1"), 300),
            sample_request("src_a", "obsreq_2", Some("plan_1"), 200),
            sample_request("src_a", "obsreq_1", Some("plan_1"), 100),
        ];

        let q1 = validate_snapshot_request(Some(&ObservationSnapshotRequest {
            source_id: Some("src_a".to_string()),
            job_id: Some("plan_1".to_string()),
            cursor: None,
            limit: Some(2),
        }))
        .unwrap();
        let page1 = filter_sort_and_page_requests(initial, &q1);
        assert_eq!(page1.requests.len(), 2);
        assert_eq!(page1.requests[0].id, "obsreq_3");
        assert_eq!(page1.requests[1].id, "obsreq_2");
        assert!(page1.has_more);

        // Before page 2 is queried:
        // 1. A brand-new request `obsreq_4` arrives at the head (updated_at_ms = 400).
        // 2. `obsreq_2` receives a finish update and moves above the cursor (updated_at_ms = 350).
        let updated_feed = vec![
            sample_request("src_a", "obsreq_4", Some("plan_1"), 400),
            sample_request("src_a", "obsreq_2", Some("plan_1"), 350),
            sample_request("src_a", "obsreq_3", Some("plan_1"), 300),
            sample_request("src_a", "obsreq_1", Some("plan_1"), 100),
        ];

        let q2 = validate_snapshot_request(Some(&ObservationSnapshotRequest {
            source_id: Some("src_a".to_string()),
            job_id: Some("plan_1".to_string()),
            cursor: page1.next_cursor,
            limit: Some(2),
        }))
        .unwrap();
        let page2 = filter_sort_and_page_requests(updated_feed, &q2);
        assert_eq!(page2.requests.len(), 1);
        assert_eq!(page2.requests[0].id, "obsreq_1");
        assert!(!page2.has_more);
        assert!(page2.next_cursor.is_none());
    }

    #[test]
    fn jobs_filtering_and_global_cap_propagate_jobs_has_more_and_warning() {
        let mut many_jobs = Vec::with_capacity(MAX_OBSERVATION_JOBS + 5);
        for idx in 0..(MAX_OBSERVATION_JOBS + 5) {
            many_jobs.push(sample_job(
                "src_a",
                &format!("job_{idx:04}"),
                Some(1_700_000_000_000 + idx as i64),
            ));
        }

        let unfiltered_query = validate_snapshot_request(None).unwrap();
        let capped = filter_and_cap_jobs(many_jobs, &unfiltered_query, false);
        assert_eq!(capped.jobs.len(), MAX_OBSERVATION_JOBS);
        assert!(capped.jobs_has_more);
        assert!(capped.cap_warning.is_some());
        // Newest job should be first.
        assert_eq!(
            capped.jobs[0].id,
            format!("job_{:04}", MAX_OBSERVATION_JOBS + 4)
        );

        // Upstream jobs_has_more propagates even when total jobs < MAX_OBSERVATION_JOBS.
        let small_jobs = vec![sample_job("src_a", "job_1", Some(100))];
        let propagated = filter_and_cap_jobs(small_jobs, &unfiltered_query, true);
        assert_eq!(propagated.jobs.len(), 1);
        assert!(propagated.jobs_has_more);
        assert!(propagated.cap_warning.is_none());
    }

    #[test]
    fn warnings_are_sanitized_deduplicated_and_bounded_with_truncation_notice() {
        let mut raw = vec![
            "Duplicate warning".to_string(),
            "Duplicate warning".to_string(),
            "Authorization: Bearer super-secret-token".to_string(),
        ];
        for i in 0..(MAX_OBSERVATION_WARNINGS + 10) {
            raw.push(format!("Source src_a: warning number {i}"));
        }

        let bounded = sanitize_and_bound_warnings(raw);
        assert_eq!(bounded.len(), MAX_OBSERVATION_WARNINGS);
        assert_eq!(bounded[0], "Duplicate warning");
        assert_eq!(bounded[1], "[redacted sensitive metadata]");
        let last = bounded.last().unwrap();
        assert!(last.contains("Additional observation warnings"));
        assert!(last.contains("were omitted."));
    }

    #[cfg(unix)]
    mod unix_fixture_tests {
        use super::*;
        use std::fs::{self, DirBuilder, OpenOptions};
        use std::io::Write;
        use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt};
        use std::sync::atomic::{AtomicU64, Ordering};
        use std::time::{SystemTime, UNIX_EPOCH};
        use threadlane_core::observation::{
            append_journal_record, ObservationRecordPhase, ObservationSourceAvailability,
        };

        static FIXTURE_COUNTER: AtomicU64 = AtomicU64::new(1);

        struct ScopedFixtureRoot {
            root: PathBuf,
        }

        impl ScopedFixtureRoot {
            fn new(label: &str) -> Self {
                let base =
                    fs::canonicalize(std::env::temp_dir()).unwrap_or_else(|_| std::env::temp_dir());
                let pid = std::process::id();
                for attempt in 0..1024u32 {
                    let seq = FIXTURE_COUNTER.fetch_add(1, Ordering::Relaxed);
                    let nanos = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .map(|d| d.as_nanos())
                        .unwrap_or(0);
                    let candidate = base.join(format!(
                        "threadlane-obs-fixture-{label}-{pid}-{seq}-{nanos}-{attempt}"
                    ));
                    match DirBuilder::new().mode(0o700).create(&candidate) {
                        Ok(()) => {
                            let root = fs::canonicalize(&candidate).unwrap_or(candidate);
                            return Self { root };
                        }
                        Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => continue,
                        Err(err) => panic!("failed to create unique fixture root: {err}"),
                    }
                }
                panic!("exhausted collision retries creating unique fixture root");
            }

            fn create_private_subdir(&self, name: &str) -> PathBuf {
                let dir = self.root.join(name);
                DirBuilder::new()
                    .mode(0o700)
                    .create(&dir)
                    .expect("create private 0700 subdirectory");
                dir
            }
        }

        impl Drop for ScopedFixtureRoot {
            fn drop(&mut self) {
                let _ = fs::remove_dir_all(&self.root);
            }
        }

        #[derive(Debug, Clone, PartialEq, Eq)]
        struct TreeEntryFingerprint {
            rel_path: PathBuf,
            is_dir: bool,
            mode: u32,
            len: u64,
            mtime: i64,
            mtime_nsec: i64,
            bytes: Vec<u8>,
        }

        fn capture_tree_fingerprint(root: &Path) -> Vec<TreeEntryFingerprint> {
            let mut entries = Vec::new();
            collect_tree_entries(root, root, &mut entries);
            entries.sort_by(|a, b| a.rel_path.cmp(&b.rel_path));
            entries
        }

        fn collect_tree_entries(root: &Path, current: &Path, out: &mut Vec<TreeEntryFingerprint>) {
            let meta = fs::symlink_metadata(current).expect("stat fixture entry");
            let rel_path = current
                .strip_prefix(root)
                .unwrap_or(Path::new(""))
                .to_path_buf();
            if meta.is_dir() {
                out.push(TreeEntryFingerprint {
                    rel_path,
                    is_dir: true,
                    mode: meta.mode() & 0o7777,
                    len: 0,
                    mtime: meta.mtime(),
                    mtime_nsec: meta.mtime_nsec(),
                    bytes: Vec::new(),
                });
                let mut child_paths: Vec<PathBuf> = fs::read_dir(current)
                    .expect("read fixture dir")
                    .map(|entry| entry.expect("dir entry").path())
                    .collect();
                child_paths.sort();
                for child in child_paths {
                    collect_tree_entries(root, &child, out);
                }
            } else {
                let bytes = fs::read(current).expect("read fixture file bytes");
                out.push(TreeEntryFingerprint {
                    rel_path,
                    is_dir: false,
                    mode: meta.mode() & 0o7777,
                    len: meta.len(),
                    mtime: meta.mtime(),
                    mtime_nsec: meta.mtime_nsec(),
                    bytes,
                });
            }
        }

        #[test]
        fn snapshot_projects_journal_lifecycle_readonly_without_mutating_stores_or_creating_sidecars(
        ) {
            let fixture = ScopedFixtureRoot::new("readonly-journal");
            let registry_dir = fixture.create_private_subdir("registry");
            let plans_dir = fixture.create_private_subdir("plans");
            let authority_dir = fixture.create_private_subdir("authority");
            let sessions_dir = fixture.create_private_subdir("sessions");

            // Write minimal valid empty PlanStore ledger (schema_version 1, revisions empty) with 0600 mode.
            let plans_ledger_path = plans_dir.join("plans.json");
            let mut ledger_file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(&plans_ledger_path)
                .expect("create private plans.json");
            ledger_file
                .write_all(b"{\"schema_version\":1,\"revisions\":[]}\n")
                .expect("write empty plans.json");
            ledger_file.sync_all().expect("sync plans.json");
            drop(ledger_file);

            let request_id = "obsreq_11111111111111111111111111111111".to_string();
            let entry_record = ObservationJournalRecord {
                record_id: "obsrec_aaaa1111111111111111111111111111".to_string(),
                request_id: request_id.clone(),
                parent_request_id: None,
                call_id: None,
                phase: ObservationRecordPhase::Entry,
                at_ms: 1_700_000_010_000,
                pid: None,
                process_instance: None,
                project_label: Some("Threadlane local".to_string()),
                project_path: None,
                binding_id: Some("bnd_fixture_1".to_string()),
                thread_uuid: Some("550e8400-e29b-41d4-a716-446655440000".to_string()),
                thread_label: Some("Fixture Thread".to_string()),
                job_id: None,
                plan_id: None,
                task_id: None,
                approval_id: None,
                operation: "context.workflow.status".to_string(),
                title: "workflow status".to_string(),
                state: ObservationCallState::Running,
                actor: None,
                executor: None,
                usage: None,
                delivery: Vec::new(),
                summary: Some("Operation context.workflow.status".to_string()),
            };
            append_journal_record(&plans_dir, &entry_record).expect("append entry journal record");

            let finish_record = ObservationJournalRecord {
                record_id: "obsrec_bbbb2222222222222222222222222222".to_string(),
                phase: ObservationRecordPhase::Finish,
                at_ms: 1_700_000_010_150,
                state: ObservationCallState::Completed,
                ..entry_record
            };
            append_journal_record(&plans_dir, &finish_record)
                .expect("append finish journal record");

            let service = ObservationService::new(ObservationSourceConfig {
                runtime_state_dir: None,
                registry_dir: registry_dir.clone(),
                plans_dir: plans_dir.clone(),
                authority_dir: authority_dir.clone(),
                sessions_dir: sessions_dir.clone(),
                app_data_dir: registry_dir.clone(),
            });

            let fingerprint_before = capture_tree_fingerprint(&fixture.root);

            let snap1 = service.snapshot(None).expect("first observation snapshot");
            let snap2 = service.snapshot(None).expect("second observation snapshot");

            assert_eq!(snap1.schema_version, OBSERVATION_SCHEMA_VERSION);
            assert_eq!(snap1.sources.len(), 1);
            assert_eq!(snap2.sources.len(), 1);
            assert_eq!(
                snap1.sources[0].availability,
                ObservationSourceAvailability::Available
            );
            assert!(snap1.sources[0].id.starts_with("src_"));
            assert_eq!(snap1.sources[0].id, snap2.sources[0].id);

            assert!(snap1.jobs.is_empty());
            assert!(snap2.jobs.is_empty());
            assert!(!snap1.jobs_has_more);
            assert!(!snap1.has_more);
            assert!(snap1.next_cursor.is_none());

            assert_eq!(snap1.requests.len(), 1);
            assert_eq!(snap1.requests, snap2.requests);

            let projected = &snap1.requests[0];
            assert_eq!(projected.id, request_id);
            assert_eq!(projected.source_id, snap1.sources[0].id);
            assert_eq!(projected.state, ObservationCallState::Completed);
            assert_eq!(projected.operation, "context.workflow.status");
            assert!(projected.job_id.is_none());
            assert!(projected.usage.is_none());
            assert!(projected.children.is_empty());
            assert_eq!(projected.children_count, 0);

            // Verify no lock files, SQLite DBs, WAL/SHM sidecars, supervisor state, or link dirs were created.
            for forbidden in [
                plans_dir.join("plans.lock"),
                registry_dir.join("registry.lock"),
                registry_dir.join("observations"),
                authority_dir.join("authority.lock"),
                authority_dir.join("approvals.json"),
                sessions_dir.join("supervisor.json"),
                fixture.root.join("state.db"),
                fixture.root.join("state.db-wal"),
                fixture.root.join("state.db-shm"),
            ] {
                assert!(
                    !forbidden.exists(),
                    "observation snapshot must not create {}",
                    forbidden.display()
                );
            }

            let fingerprint_after = capture_tree_fingerprint(&fixture.root);
            assert_eq!(fingerprint_before, fingerprint_after);
        }

        #[test]
        fn missing_default_source_directories_report_unavailable_and_never_auto_create_dirs() {
            let fixture = ScopedFixtureRoot::new("missing-stores");
            let registry_dir = fixture.root.join("missing-registry");
            let plans_dir = fixture.root.join("missing-plans");
            let authority_dir = fixture.root.join("missing-authority");
            let sessions_dir = fixture.root.join("missing-sessions");

            let fingerprint_before = capture_tree_fingerprint(&fixture.root);

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
            assert_eq!(
                sources[0].availability,
                ObservationSourceAvailability::Unavailable
            );
            assert!(sources[0].id.starts_with("src_"));
            assert!(sources[0].message.is_some());

            let snap = service.snapshot(None).expect(
                "snapshot with missing default stores must succeed with unavailable source",
            );
            assert_eq!(snap.sources, sources);
            assert!(snap
                .sources
                .iter()
                .all(|s| s.availability == ObservationSourceAvailability::Unavailable));
            assert!(!snap.warnings.is_empty());
            assert!(snap.jobs.is_empty());
            assert!(snap.requests.is_empty());
            assert!(!snap.has_more);
            assert!(!snap.jobs_has_more);

            for missing_dir in [&registry_dir, &plans_dir, &authority_dir, &sessions_dir] {
                assert!(
                    !missing_dir.exists(),
                    "observation must not auto-create missing store directory {}",
                    missing_dir.display()
                );
            }

            let fingerprint_after = capture_tree_fingerprint(&fixture.root);
            assert_eq!(fingerprint_before, fingerprint_after);
        }
    }
}
