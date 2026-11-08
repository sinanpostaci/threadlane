use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, OpenOptions};
use std::io::Read;
use std::path::{Component, Path, PathBuf};

use serde::Deserialize;
use threadlane_authority::{
    verify_approval_record_integrity, ApprovalRecord, ApprovedTaskRecord, APPROVALS_FILE_NAME,
    AUTHORITY_SCHEMA_VERSION, MAX_APPROVALS_IN_STORE, MAX_AUTHORITY_STORE_BYTES,
};
use threadlane_core::observation::{
    read_core_state_readonly, sanitize_observation_summary, validate_existing_private_dir_readonly,
    validate_existing_private_file_readonly, ObservationJob, ObservationJobState, ObservationTask,
    ReadonlyCoreStateSnapshot, ReadonlyRunRecord, ReadonlyWorkerRecord,
};
use threadlane_core::{WorkerAcceptanceState, WorkerSnapshot, WorkerStatus};
use threadlane_host::{
    is_host_lock_held, project_task_lifecycle, project_task_lifecycle_recorded_history_readonly,
    read_host_manifest_optional, read_session_task_ledger_readonly,
    read_session_task_ledger_recorded_history_readonly, HostPhase, HostSessionManifest,
    TaskDispatchIntent, TaskExecutionStatus, TaskIntegrationLifecycleStatus,
    TaskLifecycleProjection, TaskReviewLifecycleStatus, TaskRunReceipt, RUNTIME_STATE_SUBDIR,
    TASK_RECEIPTS_FILENAME, WORK_AREA_SUBDIR,
};
use threadlane_plans::{
    validate_plan_revision, PlanRevision, PlanStatus, PlanTask, MAX_PLAN_STORE_BYTES,
    MAX_REVISIONS_PER_PLAN, PLAN_STORE_SCHEMA_VERSION,
};

use super::coworker_jobs::{project_coworker_jobs_and_dedupe_plans, VerifiedCoworkerJobScope};
use super::requests::WorkerObservationLink;
use super::sources::SourceSpec;

/// Maximum number of projected jobs returned per observation source.
pub(crate) const MAX_PROJECTED_JOBS: usize = 500;

const PLANS_LEDGER_FILE_NAME: &str = "plans.json";
const CORE_STATE_DB_FILE_NAME: &str = "state.db";
const MAX_RAW_PLAN_REVISION_ENTRIES: usize = 4_096;
const GENERIC_PROJECT_LABEL: &str = "Unbound Project";
pub(super) const FALLBACK_VERIFIED_PROJECT_LABEL: &str = "Project";
const REDACTED_SUMMARY_MARKER: &str = "[redacted sensitive metadata]";

const WARN_PLAN_LEDGER_INVALID: &str =
    "Skipped unreadable or invalid plan ledger in observation source.";
const WARN_PLAN_ENTRY_INVALID: &str =
    "Skipped malformed or invalid plan revision entry in observation source.";
const WARN_APPROVAL_LEDGER_INVALID: &str =
    "Skipped unreadable or invalid approval ledger in observation source.";
const WARN_APPROVAL_ENTRY_INVALID: &str =
    "Skipped invalid or conflicting approval record in observation source.";
const WARN_RUNTIME_STATE_INVALID: &str =
    "Skipped unreadable or invalid runtime state database in observation source.";
const WARN_SESSION_STATE_INVALID: &str =
    "Skipped unreadable or invalid host session state in observation source.";
const WARN_WORKER_JOIN_INVALID: &str =
    "Runtime worker evidence did not match frozen approval, manifest, or receipt identity.";

/// Source-local job projection result produced by [`project_jobs`].
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct JobProjection {
    pub(crate) jobs: Vec<ObservationJob>,
    pub(crate) worker_links: Vec<WorkerObservationLink>,
    pub(crate) core_states: Vec<ReadonlyCoreStateSnapshot>,
    pub(crate) coworker_scopes: BTreeMap<String, VerifiedCoworkerJobScope>,
    pub(crate) warnings: Vec<String>,
    pub(crate) jobs_has_more: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawPlanLedgerEnvelope {
    schema_version: u32,
    revisions: Vec<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawApprovalLedgerEnvelope {
    schema_version: u32,
    approvals: Vec<serde_json::Value>,
}

#[derive(Debug, Clone)]
enum ApprovalLookup {
    NoneFound,
    Unique(Box<ApprovalRecord>),
    ConflictingOrInvalid,
}

#[derive(Debug)]
struct VerifiedSessionLedger {
    manifest: HostSessionManifest,
    session_binding_id: String,
    pending_intents: Vec<TaskDispatchIntent>,
    receipts: Vec<TaskRunReceipt>,
    extra_core_snapshot: Option<ReadonlyCoreStateSnapshot>,
}

#[derive(Debug)]
enum SessionInspection {
    NoManifest,
    CorruptOrUnresolved,
    VerifiedLedger(Box<VerifiedSessionLedger>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TaskReviewOutcome {
    None,
    PendingReview,
    ReviewPassed,
    Invalidated,
    Unknown,
}

#[derive(Debug, Clone)]
struct ProjectedTaskOutcome {
    observation_task: ObservationTask,
    is_approved: bool,
    is_worker_task: bool,
    is_native_route: bool,
    live_running: bool,
    review_outcome: TaskReviewOutcome,
}

/// Projects durable jobs, verified worker observation links, and source-local read-only Core
/// state snapshots for a single [`SourceSpec`].
///
/// Never reads `spec.registry_dir`, never trusts `project_root_hint`, `binding_id_hint`, or
/// `thread_uuid_hint` as authoritative evidence, and never creates directories, files, or locks.
pub(crate) fn project_jobs(spec: &SourceSpec) -> JobProjection {
    let mut projection = JobProjection::default();
    let mut seen_runtime_dirs = BTreeSet::new();

    let mut seen_initial_dirs = BTreeSet::new();
    for state_dir in &spec.runtime_state_dirs {
        let normalized_key = normalize_path_key(state_dir);
        if !seen_initial_dirs.insert(normalized_key.clone()) {
            continue;
        }
        let db_path = state_dir.join(CORE_STATE_DB_FILE_NAME);
        match read_core_state_readonly(&db_path) {
            Ok(snapshot) => {
                seen_runtime_dirs.insert(normalized_key);
                projection.core_states.push(snapshot);
            }
            Err(_) => push_unique_warning(&mut projection.warnings, WARN_RUNTIME_STATE_INVALID),
        }
    }

    let latest_plans = load_latest_plan_revisions(&spec.plans_dir, &mut projection.warnings);
    let approvals_state = load_verified_approvals(&spec.authority_dir, &mut projection.warnings);

    let mut all_jobs = Vec::with_capacity(latest_plans.len());
    let mut candidate_links = Vec::new();
    let mut worker_id_owners: BTreeMap<String, (String, String, String)> = BTreeMap::new();
    let mut conflicted_worker_ids: BTreeSet<String> = BTreeSet::new();

    for latest_plan in latest_plans.values() {
        let (job, job_links) = project_single_plan_job(
            spec,
            latest_plan,
            &approvals_state,
            &mut projection.core_states,
            &mut seen_runtime_dirs,
            &mut worker_id_owners,
            &mut conflicted_worker_ids,
            &mut projection.warnings,
        );
        all_jobs.push(job);
        candidate_links.extend(job_links);
    }

    if !conflicted_worker_ids.is_empty() {
        candidate_links.retain(|link| !conflicted_worker_ids.contains(&link.worker_id));
    }

    let mut deduped_links = Vec::with_capacity(candidate_links.len());
    let mut seen_worker_ids = BTreeSet::new();
    for link in candidate_links {
        if seen_worker_ids.insert(link.worker_id.clone()) {
            deduped_links.push(link);
        }
    }

    project_coworker_jobs_and_dedupe_plans(
        spec,
        &latest_plans,
        &approvals_state,
        &projection.core_states,
        &mut all_jobs,
        &mut deduped_links,
        &mut projection.coworker_scopes,
        &mut projection.warnings,
    );

    if all_jobs.is_empty() {
        return projection;
    }

    all_jobs.sort_by(|left, right| {
        left.source_id
            .cmp(&right.source_id)
            .then_with(|| left.id.cmp(&right.id))
    });

    if all_jobs.len() > MAX_PROJECTED_JOBS {
        all_jobs.truncate(MAX_PROJECTED_JOBS);
        projection.jobs_has_more = true;
    }

    projection.jobs = all_jobs;
    projection.worker_links = deduped_links;
    projection
}

pub(super) fn push_unique_warning(warnings: &mut Vec<String>, message: &'static str) {
    if !warnings.iter().any(|existing| existing == message) {
        warnings.push(message.to_string());
    }
}

fn normalize_path_key(path: &Path) -> PathBuf {
    fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf())
}

#[cfg(unix)]
const fn libc_o_nofollow() -> i32 {
    #[cfg(target_os = "linux")]
    {
        0o400000
    }
    #[cfg(target_os = "macos")]
    {
        0x0100
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        0
    }
}

fn verify_same_file_identity(pre_meta: &fs::Metadata, post_meta: &fs::Metadata) -> bool {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        pre_meta.dev() == post_meta.dev() && pre_meta.ino() == post_meta.ino()
    }
    #[cfg(not(unix))]
    {
        let _ = (pre_meta, post_meta);
        true
    }
}

pub(super) fn read_bounded_private_file_readonly(
    dir: &Path,
    file_name: &str,
    max_bytes: usize,
) -> Result<Option<Vec<u8>>, ()> {
    match fs::symlink_metadata(dir) {
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err(()),
        Ok(_) => {}
    }

    validate_existing_private_dir_readonly(dir).map_err(|_| ())?;

    let file_path = dir.join(file_name);
    let pre_meta = match fs::symlink_metadata(&file_path) {
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err(()),
        Ok(meta) => meta,
    };

    let validated_meta =
        validate_existing_private_file_readonly(&file_path, max_bytes as u64).map_err(|_| ())?;
    if !verify_same_file_identity(&pre_meta, &validated_meta) {
        return Err(());
    }

    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc_o_nofollow());
    }

    let file = options.open(&file_path).map_err(|_| ())?;
    let post_meta = file.metadata().map_err(|_| ())?;
    if !post_meta.file_type().is_file()
        || post_meta.file_type().is_symlink()
        || post_meta.len() > max_bytes as u64
        || !verify_same_file_identity(&validated_meta, &post_meta)
    {
        return Err(());
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if (post_meta.permissions().mode() & 0o777) != 0o600 {
            return Err(());
        }
    }

    let mut bytes = Vec::with_capacity(post_meta.len() as usize);
    file.take((max_bytes as u64).saturating_add(1))
        .read_to_end(&mut bytes)
        .map_err(|_| ())?;
    if bytes.len() > max_bytes {
        return Err(());
    }

    Ok(Some(bytes))
}

fn extract_raw_string_field(value: &serde_json::Value, key: &str) -> Option<String> {
    let raw = value.get(key)?.as_str()?;
    threadlane_plans::validate_identifier("id", raw).ok()
}

fn extract_raw_u64_field(value: &serde_json::Value, key: &str) -> Option<u64> {
    let rev = value.get(key)?.as_u64()?;
    if rev == 0 {
        return None;
    }
    threadlane_plans::validate_js_safe_revision("revision", rev).ok()
}

fn load_latest_plan_revisions(
    plans_dir: &Path,
    warnings: &mut Vec<String>,
) -> BTreeMap<String, PlanRevision> {
    let bytes = match read_bounded_private_file_readonly(
        plans_dir,
        PLANS_LEDGER_FILE_NAME,
        MAX_PLAN_STORE_BYTES,
    ) {
        Ok(Some(bytes)) => bytes,
        Ok(None) => return BTreeMap::new(),
        Err(()) => {
            push_unique_warning(warnings, WARN_PLAN_LEDGER_INVALID);
            return BTreeMap::new();
        }
    };

    let envelope: RawPlanLedgerEnvelope = match serde_json::from_slice(&bytes) {
        Ok(env) => env,
        Err(_) => {
            push_unique_warning(warnings, WARN_PLAN_LEDGER_INVALID);
            return BTreeMap::new();
        }
    };

    if envelope.schema_version != PLAN_STORE_SCHEMA_VERSION
        || envelope.revisions.len() > MAX_RAW_PLAN_REVISION_ENTRIES
    {
        push_unique_warning(warnings, WARN_PLAN_LEDGER_INVALID);
        return BTreeMap::new();
    }

    let mut by_plan: BTreeMap<String, Vec<PlanRevision>> = BTreeMap::new();
    let mut corrupt_plan_ids: BTreeSet<String> = BTreeSet::new();

    for raw_rev in envelope.revisions {
        let hinted_plan_id = extract_raw_string_field(&raw_rev, "plan_id");
        let parsed: PlanRevision = match serde_json::from_value(raw_rev) {
            Ok(rev) => rev,
            Err(_) => {
                push_unique_warning(warnings, WARN_PLAN_ENTRY_INVALID);
                if let Some(pid) = hinted_plan_id {
                    corrupt_plan_ids.insert(pid);
                }
                continue;
            }
        };

        if validate_plan_revision(&parsed).is_err() {
            push_unique_warning(warnings, WARN_PLAN_ENTRY_INVALID);
            corrupt_plan_ids.insert(parsed.plan_id);
            continue;
        }

        by_plan
            .entry(parsed.plan_id.clone())
            .or_default()
            .push(parsed);
    }

    let mut latest_by_plan = BTreeMap::new();
    for (plan_id, mut revisions) in by_plan {
        if corrupt_plan_ids.contains(&plan_id) {
            continue;
        }
        if revisions.is_empty() || revisions.len() > MAX_REVISIONS_PER_PLAN {
            push_unique_warning(warnings, WARN_PLAN_ENTRY_INVALID);
            continue;
        }

        revisions.sort_by_key(|rev| rev.revision);

        let expected_binding = &revisions[0].binding_id;
        let mut valid_plan_history = true;
        let mut seen_revisions = BTreeSet::new();

        for rev in &revisions {
            if !seen_revisions.insert(rev.revision) || &rev.binding_id != expected_binding {
                valid_plan_history = false;
                break;
            }
        }

        if revisions
            .last()
            .is_some_and(|latest| latest.status == PlanStatus::Superseded)
        {
            valid_plan_history = false;
        }

        if !valid_plan_history {
            push_unique_warning(warnings, WARN_PLAN_ENTRY_INVALID);
            continue;
        }

        if let Some(latest) = revisions.pop() {
            latest_by_plan.insert(plan_id, latest);
        }
    }

    latest_by_plan
}

pub(super) struct LoadedApprovals {
    pub(super) ledger_corrupt: bool,
    pub(super) valid_by_target: BTreeMap<(String, u64), Vec<ApprovalRecord>>,
    pub(super) corrupt_targets: BTreeSet<(String, u64)>,
    pub(super) corrupt_plan_ids: BTreeSet<String>,
}

fn load_verified_approvals(authority_dir: &Path, warnings: &mut Vec<String>) -> LoadedApprovals {
    let bytes = match read_bounded_private_file_readonly(
        authority_dir,
        APPROVALS_FILE_NAME,
        MAX_AUTHORITY_STORE_BYTES,
    ) {
        Ok(Some(bytes)) => bytes,
        Ok(None) => {
            return LoadedApprovals {
                ledger_corrupt: false,
                valid_by_target: BTreeMap::new(),
                corrupt_targets: BTreeSet::new(),
                corrupt_plan_ids: BTreeSet::new(),
            };
        }
        Err(()) => {
            push_unique_warning(warnings, WARN_APPROVAL_LEDGER_INVALID);
            return LoadedApprovals {
                ledger_corrupt: true,
                valid_by_target: BTreeMap::new(),
                corrupt_targets: BTreeSet::new(),
                corrupt_plan_ids: BTreeSet::new(),
            };
        }
    };

    let envelope: RawApprovalLedgerEnvelope = match serde_json::from_slice(&bytes) {
        Ok(env) => env,
        Err(_) => {
            push_unique_warning(warnings, WARN_APPROVAL_LEDGER_INVALID);
            return LoadedApprovals {
                ledger_corrupt: true,
                valid_by_target: BTreeMap::new(),
                corrupt_targets: BTreeSet::new(),
                corrupt_plan_ids: BTreeSet::new(),
            };
        }
    };

    if envelope.schema_version != AUTHORITY_SCHEMA_VERSION
        || envelope.approvals.len() > MAX_APPROVALS_IN_STORE
    {
        push_unique_warning(warnings, WARN_APPROVAL_LEDGER_INVALID);
        return LoadedApprovals {
            ledger_corrupt: true,
            valid_by_target: BTreeMap::new(),
            corrupt_targets: BTreeSet::new(),
            corrupt_plan_ids: BTreeSet::new(),
        };
    }

    let approvals_path = authority_dir.join(APPROVALS_FILE_NAME);
    let mut valid_by_target: BTreeMap<(String, u64), Vec<ApprovalRecord>> = BTreeMap::new();
    let mut corrupt_targets: BTreeSet<(String, u64)> = BTreeSet::new();
    let mut corrupt_plan_ids: BTreeSet<String> = BTreeSet::new();
    let mut seen_approval_ids: BTreeMap<String, (String, u64)> = BTreeMap::new();
    let mut seen_subject_digests: BTreeMap<String, (String, u64)> = BTreeMap::new();

    for raw_approval in envelope.approvals {
        let hinted_plan_id = extract_raw_string_field(&raw_approval, "plan_id");
        let hinted_rev = extract_raw_u64_field(&raw_approval, "plan_revision");

        let record: ApprovalRecord = match serde_json::from_value(raw_approval) {
            Ok(rec) => rec,
            Err(_) => {
                push_unique_warning(warnings, WARN_APPROVAL_ENTRY_INVALID);
                match (hinted_plan_id, hinted_rev) {
                    (Some(pid), Some(rev)) => {
                        corrupt_targets.insert((pid, rev));
                    }
                    (Some(pid), None) => {
                        corrupt_plan_ids.insert(pid);
                    }
                    _ => {}
                }
                continue;
            }
        };

        let target = (record.plan_id.clone(), record.plan_revision);
        if verify_approval_record_integrity(&record, &approvals_path).is_err()
            || !is_safe_repository_path(&record.repository_path)
        {
            push_unique_warning(warnings, WARN_APPROVAL_ENTRY_INVALID);
            corrupt_targets.insert(target);
            continue;
        }

        if let Some(prev_target) =
            seen_approval_ids.insert(record.approval_id.clone(), target.clone())
        {
            push_unique_warning(warnings, WARN_APPROVAL_ENTRY_INVALID);
            corrupt_targets.insert(prev_target);
            corrupt_targets.insert(target.clone());
            continue;
        }

        if let Some(prev_target) =
            seen_subject_digests.insert(record.subject_digest.clone(), target.clone())
        {
            push_unique_warning(warnings, WARN_APPROVAL_ENTRY_INVALID);
            corrupt_targets.insert(prev_target);
            corrupt_targets.insert(target.clone());
            continue;
        }

        valid_by_target.entry(target).or_default().push(record);
    }

    LoadedApprovals {
        ledger_corrupt: false,
        valid_by_target,
        corrupt_targets,
        corrupt_plan_ids,
    }
}

pub(super) fn is_safe_repository_path(path: &Path) -> bool {
    if !path.is_absolute() || path.as_os_str().is_empty() {
        return false;
    }

    let mut acc = PathBuf::new();
    let mut saw_normal = false;
    for component in path.components() {
        match component {
            Component::RootDir => acc.push(Component::RootDir.as_os_str()),
            Component::Normal(part) => {
                let text = part.to_string_lossy();
                if text.is_empty() || text.chars().any(char::is_control) {
                    return false;
                }
                saw_normal = true;
                acc.push(part);
                if let Ok(meta) = fs::symlink_metadata(&acc) {
                    if meta.file_type().is_symlink() {
                        return false;
                    }
                }
            }
            Component::Prefix(_) | Component::CurDir | Component::ParentDir => return false,
        }
    }
    saw_normal
}

fn approval_matches_latest_plan(record: &ApprovalRecord, plan: &PlanRevision) -> bool {
    if !plan.status.is_active_published() {
        return false;
    }
    if record.plan_id != plan.plan_id
        || record.plan_revision != plan.revision
        || record.binding_id != plan.binding_id
        || record.profile_id != plan.profile_id
        || record.registry_revision != plan.profile_revision
    {
        return false;
    }
    if record.tasks.is_empty() || record.approved_task_ids.len() != record.tasks.len() {
        return false;
    }

    let mut plan_task_iter = plan.tasks.iter();
    let approved_set: BTreeSet<&str> = record
        .approved_task_ids
        .iter()
        .map(String::as_str)
        .collect();
    if approved_set.len() != record.approved_task_ids.len() {
        return false;
    }

    for (expected_id, approved_task) in record.approved_task_ids.iter().zip(record.tasks.iter()) {
        if expected_id != &approved_task.task_id {
            return false;
        }
        let Some(matched_plan_task) =
            plan_task_iter.find(|candidate| candidate.id == approved_task.task_id)
        else {
            return false;
        };
        if !approved_task_matches_plan_task(approved_task, matched_plan_task) {
            return false;
        }
    }

    true
}

fn approved_task_matches_plan_task(approved: &ApprovedTaskRecord, plan_task: &PlanTask) -> bool {
    approved.task_id == plan_task.id
        && approved.title == plan_task.title
        && approved.instruction == plan_task.instruction
        && approved.role == plan_task.role
        && approved.difficulty == plan_task.difficulty
        && approved.depends_on == plan_task.depends_on
        && approved.allowed_write_paths == plan_task.allowed_write_paths
}

fn resolve_plan_approval(
    plan: &PlanRevision,
    approvals_state: &LoadedApprovals,
    warnings: &mut Vec<String>,
) -> ApprovalLookup {
    if approvals_state.ledger_corrupt {
        return ApprovalLookup::ConflictingOrInvalid;
    }

    let target = (plan.plan_id.clone(), plan.revision);
    if approvals_state.corrupt_targets.contains(&target)
        || approvals_state.corrupt_plan_ids.contains(&plan.plan_id)
    {
        return ApprovalLookup::ConflictingOrInvalid;
    }

    let Some(candidates) = approvals_state.valid_by_target.get(&target) else {
        return ApprovalLookup::NoneFound;
    };

    if candidates.len() != 1 {
        push_unique_warning(warnings, WARN_APPROVAL_ENTRY_INVALID);
        return ApprovalLookup::ConflictingOrInvalid;
    }

    let candidate = &candidates[0];
    if !approval_matches_latest_plan(candidate, plan) {
        push_unique_warning(warnings, WARN_APPROVAL_ENTRY_INVALID);
        return ApprovalLookup::ConflictingOrInvalid;
    }

    ApprovalLookup::Unique(Box::new(candidate.clone()))
}

pub(super) fn sanitize_label_or_fallback(raw: &str, fallback: &str) -> String {
    let sanitized = sanitize_observation_summary(raw);
    if sanitized.is_empty() || sanitized == REDACTED_SUMMARY_MARKER {
        fallback.to_string()
    } else {
        sanitized
    }
}

fn derive_verified_project_metadata(record: &ApprovalRecord) -> (String, Option<String>) {
    let raw_label = record
        .repository_path
        .file_name()
        .and_then(|os| os.to_str())
        .unwrap_or(FALLBACK_VERIFIED_PROJECT_LABEL);
    let label = sanitize_label_or_fallback(raw_label, FALLBACK_VERIFIED_PROJECT_LABEL);
    let path = record.repository_path.to_string_lossy().into_owned();
    (label, Some(path))
}

pub(super) fn resolve_thread_label(
    binding_id: &str,
    thread_uuid: &str,
    core_states: &[ReadonlyCoreStateSnapshot],
    extra_snapshot: Option<&ReadonlyCoreStateSnapshot>,
) -> Option<String> {
    let mut labels = BTreeSet::new();
    for snapshot in core_states.iter().chain(extra_snapshot) {
        for binding in &snapshot.bindings {
            if binding.binding_id == binding_id && binding.thread_uuid == thread_uuid {
                let sanitized = sanitize_observation_summary(&binding.label);
                if !sanitized.is_empty() && sanitized != REDACTED_SUMMARY_MARKER {
                    labels.insert(sanitized);
                }
            }
        }
    }
    if labels.len() == 1 {
        labels.into_iter().next()
    } else {
        None
    }
}

fn inspect_approval_session(
    spec: &SourceSpec,
    record: &ApprovalRecord,
    seen_runtime_dirs: &mut BTreeSet<PathBuf>,
    warnings: &mut Vec<String>,
) -> (SessionInspection, PathBuf) {
    let session_dir = spec.sessions_dir.join(&record.approval_id);

    match fs::symlink_metadata(&spec.sessions_dir) {
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            return (SessionInspection::NoManifest, session_dir);
        }
        Err(_) => {
            push_unique_warning(warnings, WARN_SESSION_STATE_INVALID);
            return (SessionInspection::CorruptOrUnresolved, session_dir);
        }
        Ok(_) => {}
    }

    if validate_existing_private_dir_readonly(&spec.sessions_dir).is_err() {
        push_unique_warning(warnings, WARN_SESSION_STATE_INVALID);
        return (SessionInspection::CorruptOrUnresolved, session_dir);
    }

    match fs::symlink_metadata(&session_dir) {
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            return (SessionInspection::NoManifest, session_dir);
        }
        Err(_) => {
            push_unique_warning(warnings, WARN_SESSION_STATE_INVALID);
            return (SessionInspection::CorruptOrUnresolved, session_dir);
        }
        Ok(_) => {}
    }

    if validate_existing_private_dir_readonly(&session_dir).is_err() {
        push_unique_warning(warnings, WARN_SESSION_STATE_INVALID);
        return (SessionInspection::CorruptOrUnresolved, session_dir);
    }

    let manifest = match read_host_manifest_optional(&session_dir, record) {
        Ok(Some(manifest)) => manifest,
        Ok(None) => {
            if fs::symlink_metadata(session_dir.join(TASK_RECEIPTS_FILENAME)).is_ok()
                || fs::symlink_metadata(session_dir.join(RUNTIME_STATE_SUBDIR)).is_ok()
                || fs::symlink_metadata(session_dir.join(WORK_AREA_SUBDIR)).is_ok()
            {
                push_unique_warning(warnings, WARN_SESSION_STATE_INVALID);
                return (SessionInspection::CorruptOrUnresolved, session_dir);
            }
            return (SessionInspection::NoManifest, session_dir);
        }
        Err(_) => {
            push_unique_warning(warnings, WARN_SESSION_STATE_INVALID);
            return (SessionInspection::CorruptOrUnresolved, session_dir);
        }
    };

    let Some(ref session_binding_id) = manifest.session_binding_id else {
        return (SessionInspection::CorruptOrUnresolved, session_dir);
    };

    let host_lock_held = match is_host_lock_held(&session_dir) {
        Ok(held) => held,
        Err(_) => {
            push_unique_warning(warnings, WARN_SESSION_STATE_INVALID);
            return (SessionInspection::CorruptOrUnresolved, session_dir);
        }
    };
    let ledger_res = if manifest.phase == HostPhase::Stopped || !host_lock_held {
        read_session_task_ledger_recorded_history_readonly(&session_dir, record, session_binding_id)
    } else {
        read_session_task_ledger_readonly(&session_dir, record, session_binding_id)
    };
    let (pending_intents, receipts) = match ledger_res {
        Ok(Some(ledger)) => ledger,
        Ok(None) => {
            return (SessionInspection::CorruptOrUnresolved, session_dir);
        }
        Err(_) => {
            push_unique_warning(warnings, WARN_SESSION_STATE_INVALID);
            return (SessionInspection::CorruptOrUnresolved, session_dir);
        }
    };

    let session_runtime_dir = session_dir.join(RUNTIME_STATE_SUBDIR);
    let extra_core_snapshot = match fs::symlink_metadata(&session_runtime_dir) {
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => None,
        Err(_) => {
            push_unique_warning(warnings, WARN_RUNTIME_STATE_INVALID);
            return (SessionInspection::CorruptOrUnresolved, session_dir);
        }
        Ok(_) => {
            if validate_existing_private_dir_readonly(&session_runtime_dir).is_err() {
                push_unique_warning(warnings, WARN_RUNTIME_STATE_INVALID);
                return (SessionInspection::CorruptOrUnresolved, session_dir);
            }
            let normalized_session_rt = normalize_path_key(&session_runtime_dir);
            if seen_runtime_dirs.contains(&normalized_session_rt) {
                None
            } else {
                let session_db_path = session_runtime_dir.join(CORE_STATE_DB_FILE_NAME);
                match read_core_state_readonly(&session_db_path) {
                    Ok(snap) => {
                        seen_runtime_dirs.insert(normalized_session_rt);
                        Some(snap)
                    }
                    Err(_) => {
                        push_unique_warning(warnings, WARN_RUNTIME_STATE_INVALID);
                        return (SessionInspection::CorruptOrUnresolved, session_dir);
                    }
                }
            }
        }
    };

    (
        SessionInspection::VerifiedLedger(Box::new(VerifiedSessionLedger {
            session_binding_id: session_binding_id.clone(),
            manifest,
            pending_intents,
            receipts,
            extra_core_snapshot,
        })),
        session_dir,
    )
}

#[allow(clippy::too_many_arguments)]
fn project_single_plan_job(
    spec: &SourceSpec,
    plan: &PlanRevision,
    approvals_state: &LoadedApprovals,
    core_states: &mut Vec<ReadonlyCoreStateSnapshot>,
    seen_runtime_dirs: &mut BTreeSet<PathBuf>,
    worker_id_owners: &mut BTreeMap<String, (String, String, String)>,
    conflicted_worker_ids: &mut BTreeSet<String>,
    warnings: &mut Vec<String>,
) -> (ObservationJob, Vec<WorkerObservationLink>) {
    let title = sanitize_label_or_fallback(&plan.summary, &plan.plan_id);
    let summary_text = sanitize_observation_summary(&plan.summary);
    let summary = (!summary_text.is_empty()).then_some(summary_text);

    let approval_lookup = resolve_plan_approval(plan, approvals_state, warnings);

    match approval_lookup {
        ApprovalLookup::NoneFound => {
            let approval_status = if plan.status == PlanStatus::Draft {
                "draft".to_string()
            } else {
                "unapproved".to_string()
            };
            let tasks = plan
                .tasks
                .iter()
                .map(|task| ObservationTask {
                    id: task.id.clone(),
                    title: sanitize_label_or_fallback(&task.title, &task.id),
                    status: "not_approved".to_string(),
                    depends_on: task.depends_on.clone(),
                })
                .collect();

            let job = ObservationJob {
                source_id: spec.source.id.clone(),
                id: plan.plan_id.clone(),
                project_label: GENERIC_PROJECT_LABEL.to_string(),
                project_path: None,
                title,
                thread_uuid: None,
                thread_label: None,
                binding_id: Some(plan.binding_id.clone()),
                plan_id: Some(plan.plan_id.clone()),
                plan_revision: Some(plan.revision),
                state: ObservationJobState::Planning,
                approval_status,
                execution_status: "not_started".to_string(),
                review_state: None,
                updated_at_ms: Some(plan.created_at_ms),
                active_call_count: 0,
                tasks,
                summary,
            };
            (job, Vec::new())
        }
        ApprovalLookup::ConflictingOrInvalid => {
            let tasks = plan
                .tasks
                .iter()
                .map(|task| ObservationTask {
                    id: task.id.clone(),
                    title: sanitize_label_or_fallback(&task.title, &task.id),
                    status: "unknown".to_string(),
                    depends_on: task.depends_on.clone(),
                })
                .collect();

            let job = ObservationJob {
                source_id: spec.source.id.clone(),
                id: plan.plan_id.clone(),
                project_label: GENERIC_PROJECT_LABEL.to_string(),
                project_path: None,
                title,
                thread_uuid: None,
                thread_label: None,
                binding_id: Some(plan.binding_id.clone()),
                plan_id: Some(plan.plan_id.clone()),
                plan_revision: Some(plan.revision),
                state: ObservationJobState::Unknown,
                approval_status: "unknown".to_string(),
                execution_status: "unknown".to_string(),
                review_state: None,
                updated_at_ms: Some(plan.created_at_ms),
                active_call_count: 0,
                tasks,
                summary,
            };
            (job, Vec::new())
        }
        ApprovalLookup::Unique(record) => project_approved_plan_job(
            spec,
            plan,
            record.as_ref(),
            title,
            summary,
            core_states,
            seen_runtime_dirs,
            worker_id_owners,
            conflicted_worker_ids,
            warnings,
        ),
    }
}

#[allow(clippy::too_many_arguments)]
fn project_approved_plan_job(
    spec: &SourceSpec,
    plan: &PlanRevision,
    record: &ApprovalRecord,
    title: String,
    summary: Option<String>,
    core_states: &mut Vec<ReadonlyCoreStateSnapshot>,
    seen_runtime_dirs: &mut BTreeSet<PathBuf>,
    worker_id_owners: &mut BTreeMap<String, (String, String, String)>,
    conflicted_worker_ids: &mut BTreeSet<String>,
    warnings: &mut Vec<String>,
) -> (ObservationJob, Vec<WorkerObservationLink>) {
    let (project_label, project_path) = derive_verified_project_metadata(record);
    let mut max_updated_at_ms = plan.created_at_ms.max(record.created_at_ms);

    let (mut session_inspection, session_dir) =
        inspect_approval_session(spec, record, seen_runtime_dirs, warnings);

    if let SessionInspection::VerifiedLedger(ref mut ledger) = session_inspection {
        max_updated_at_ms = max_updated_at_ms
            .max(ledger.manifest.created_at_ms)
            .max(ledger.manifest.updated_at_ms);
        if let Some(snapshot) = ledger.extra_core_snapshot.take() {
            core_states.push(snapshot);
        }
    }

    let thread_label =
        resolve_thread_label(&record.binding_id, &record.thread_uuid, core_states, None);

    let approved_by_id: BTreeMap<&str, &ApprovedTaskRecord> = record
        .tasks
        .iter()
        .map(|task| (task.task_id.as_str(), task))
        .collect();

    let host_lock_held = match &session_inspection {
        SessionInspection::VerifiedLedger(_) => {
            matches!(is_host_lock_held(&session_dir), Ok(true))
        }
        _ => false,
    };

    let mut task_outcomes = Vec::with_capacity(plan.tasks.len());
    let mut worker_links = Vec::new();
    let mut active_call_count = 0usize;

    for plan_task in &plan.tasks {
        let task_title = sanitize_label_or_fallback(&plan_task.title, &plan_task.id);
        let Some(&approved_task) = approved_by_id.get(plan_task.id.as_str()) else {
            task_outcomes.push(ProjectedTaskOutcome {
                observation_task: ObservationTask {
                    id: plan_task.id.clone(),
                    title: task_title,
                    status: "not_approved".to_string(),
                    depends_on: plan_task.depends_on.clone(),
                },
                is_approved: false,
                is_worker_task: false,
                is_native_route: false,
                live_running: false,
                review_outcome: TaskReviewOutcome::None,
            });
            continue;
        };

        if approved_task.provider.is_none() {
            let status = match &session_inspection {
                SessionInspection::CorruptOrUnresolved => "unknown".to_string(),
                SessionInspection::NoManifest | SessionInspection::VerifiedLedger(_) => {
                    "native_route_required".to_string()
                }
            };
            let review_outcome = if status == "unknown" {
                TaskReviewOutcome::Unknown
            } else {
                TaskReviewOutcome::None
            };
            task_outcomes.push(ProjectedTaskOutcome {
                observation_task: ObservationTask {
                    id: plan_task.id.clone(),
                    title: task_title,
                    status,
                    depends_on: plan_task.depends_on.clone(),
                },
                is_approved: true,
                is_worker_task: false,
                is_native_route: true,
                live_running: false,
                review_outcome,
            });
            continue;
        }

        match &session_inspection {
            SessionInspection::NoManifest => {
                task_outcomes.push(ProjectedTaskOutcome {
                    observation_task: ObservationTask {
                        id: plan_task.id.clone(),
                        title: task_title,
                        status: "not_started".to_string(),
                        depends_on: plan_task.depends_on.clone(),
                    },
                    is_approved: true,
                    is_worker_task: true,
                    is_native_route: false,
                    live_running: false,
                    review_outcome: TaskReviewOutcome::None,
                });
            }
            SessionInspection::CorruptOrUnresolved => {
                task_outcomes.push(ProjectedTaskOutcome {
                    observation_task: ObservationTask {
                        id: plan_task.id.clone(),
                        title: task_title,
                        status: "unknown".to_string(),
                        depends_on: plan_task.depends_on.clone(),
                    },
                    is_approved: true,
                    is_worker_task: true,
                    is_native_route: false,
                    live_running: false,
                    review_outcome: TaskReviewOutcome::Unknown,
                });
            }
            SessionInspection::VerifiedLedger(ledger) => {
                let outcome = evaluate_approved_worker_task(
                    plan,
                    record,
                    approved_task,
                    task_title,
                    &ledger.manifest,
                    &ledger.session_binding_id,
                    &session_dir,
                    host_lock_held,
                    &ledger.pending_intents,
                    &ledger.receipts,
                    core_states,
                    ledger.extra_core_snapshot.as_ref(),
                    worker_id_owners,
                    conflicted_worker_ids,
                    &mut worker_links,
                    &mut max_updated_at_ms,
                    warnings,
                );
                if outcome.live_running {
                    active_call_count += 1;
                }
                task_outcomes.push(outcome);
            }
        }
    }

    let manifest_phase = match &session_inspection {
        SessionInspection::VerifiedLedger(ledger) => Some(ledger.manifest.phase),
        _ => None,
    };

    let (state, execution_status, review_state) = aggregate_job_lifecycle(
        &task_outcomes,
        active_call_count,
        manifest_phase,
        host_lock_held,
    );

    let tasks = task_outcomes
        .into_iter()
        .map(|outcome| outcome.observation_task)
        .collect();

    let job = ObservationJob {
        source_id: spec.source.id.clone(),
        id: plan.plan_id.clone(),
        project_label,
        project_path,
        title,
        thread_uuid: Some(record.thread_uuid.clone()),
        thread_label,
        binding_id: Some(record.binding_id.clone()),
        plan_id: Some(plan.plan_id.clone()),
        plan_revision: Some(plan.revision),
        state,
        approval_status: "approved".to_string(),
        execution_status,
        review_state,
        updated_at_ms: Some(max_updated_at_ms),
        active_call_count,
        tasks,
        summary,
    };

    (job, worker_links)
}

#[allow(clippy::too_many_arguments)]
fn evaluate_approved_worker_task(
    plan: &PlanRevision,
    record: &ApprovalRecord,
    approved_task: &ApprovedTaskRecord,
    task_title: String,
    manifest: &HostSessionManifest,
    session_binding_id: &str,
    session_dir: &Path,
    host_lock_held: bool,
    pending_intents: &[TaskDispatchIntent],
    receipts: &[TaskRunReceipt],
    core_states: &[ReadonlyCoreStateSnapshot],
    extra_core_snapshot: Option<&ReadonlyCoreStateSnapshot>,
    worker_id_owners: &mut BTreeMap<String, (String, String, String)>,
    conflicted_worker_ids: &mut BTreeSet<String>,
    worker_links: &mut Vec<WorkerObservationLink>,
    max_updated_at_ms: &mut i64,
    warnings: &mut Vec<String>,
) -> ProjectedTaskOutcome {
    let matched_intent = pending_intents
        .iter()
        .find(|intent| intent.task_id == approved_task.task_id);
    let matched_receipt = receipts
        .iter()
        .find(|receipt| receipt.task_id == approved_task.task_id);

    if let Some(intent) = matched_intent {
        if verify_intent_matches_task(intent, approved_task) {
            *max_updated_at_ms = (*max_updated_at_ms).max(intent.recorded_at_ms);
        } else {
            push_unique_warning(warnings, WARN_SESSION_STATE_INVALID);
            return unknown_worker_task_outcome(approved_task, task_title);
        }
    }

    let Some(receipt) = matched_receipt else {
        if matched_intent.is_some() || manifest.phase == HostPhase::Failed {
            return unknown_worker_task_outcome(approved_task, task_title);
        }
        return ProjectedTaskOutcome {
            observation_task: ObservationTask {
                id: approved_task.task_id.clone(),
                title: task_title,
                status: "not_started".to_string(),
                depends_on: approved_task.depends_on.clone(),
            },
            is_approved: true,
            is_worker_task: true,
            is_native_route: false,
            live_running: false,
            review_outcome: TaskReviewOutcome::None,
        };
    };

    if !verify_receipt_matches_task(receipt, approved_task) {
        push_unique_warning(warnings, WARN_WORKER_JOIN_INVALID);
        return unknown_worker_task_outcome(approved_task, task_title);
    }

    *max_updated_at_ms = (*max_updated_at_ms).max(receipt.recorded_at_ms);

    let Some((worker, matched_run)) = find_exact_matching_core_worker(
        record,
        approved_task,
        session_binding_id,
        receipt,
        core_states,
        extra_core_snapshot,
    ) else {
        push_unique_warning(warnings, WARN_WORKER_JOIN_INVALID);
        return unknown_worker_task_outcome(approved_task, task_title);
    };

    let owner_tuple = (
        plan.plan_id.clone(),
        record.approval_id.clone(),
        approved_task.task_id.clone(),
    );
    if let Some(prev_owner) = worker_id_owners.get(&worker.worker_id) {
        if prev_owner != &owner_tuple {
            conflicted_worker_ids.insert(worker.worker_id.clone());
            push_unique_warning(warnings, WARN_WORKER_JOIN_INVALID);
            return unknown_worker_task_outcome(approved_task, task_title);
        }
    } else {
        worker_id_owners.insert(worker.worker_id.clone(), owner_tuple);
    }

    *max_updated_at_ms = (*max_updated_at_ms)
        .max(worker.created_at_ms)
        .max(worker.updated_at_ms);
    if let Some(started_ms) = worker.started_at_ms {
        *max_updated_at_ms = (*max_updated_at_ms).max(started_ms);
    }
    if let Some(completed_ms) = worker.completed_at_ms {
        *max_updated_at_ms = (*max_updated_at_ms).max(completed_ms);
    }
    if let Some(cancel_ms) = worker.cancel_requested_at_ms {
        *max_updated_at_ms = (*max_updated_at_ms).max(cancel_ms);
    }
    if let Some(run) = matched_run {
        *max_updated_at_ms = (*max_updated_at_ms)
            .max(run.started_at_ms)
            .max(run.updated_at_ms);
        if let Some(completed_ms) = run.completed_at_ms {
            *max_updated_at_ms = (*max_updated_at_ms).max(completed_ms);
        }
        if let Some(cancelled_ms) = run.cancelled_at_ms {
            *max_updated_at_ms = (*max_updated_at_ms).max(cancelled_ms);
        }
    }

    let has_claim_id = worker
        .claim_id
        .as_ref()
        .is_some_and(|claim_id| !claim_id.trim().is_empty());
    let manifest_allows_live = matches!(manifest.phase, HostPhase::Ready | HostPhase::Stopping);
    let is_live = worker.status == WorkerStatus::Running
        && has_claim_id
        && manifest_allows_live
        && host_lock_held;

    worker_links.push(WorkerObservationLink {
        worker_id: worker.worker_id.clone(),
        job_id: plan.plan_id.clone(),
        approval_id: record.approval_id.clone(),
        task_id: approved_task.task_id.clone(),
        title: task_title.clone(),
        control_binding_id: record.binding_id.clone(),
        session_binding_id: session_binding_id.to_string(),
        thread_uuid: record.thread_uuid.clone(),
        role: Some(approved_task.role.as_str().to_string()),
        live: is_live,
    });

    let exec_status = TaskExecutionStatus::from_worker_snapshot(&worker);
    let lifecycle = if host_lock_held && manifest_allows_live {
        project_task_lifecycle(
            record,
            session_dir,
            approved_task,
            exec_status,
            Some(receipt),
            Some(&worker),
        )
    } else {
        project_task_lifecycle_recorded_history_readonly(
            record,
            session_dir,
            approved_task,
            exec_status,
            Some(receipt),
            Some(&worker),
        )
    };

    let (status_str, review_outcome) = apply_task_lifecycle_projection(
        &worker,
        approved_task,
        record,
        receipt,
        exec_status,
        &lifecycle,
        has_claim_id,
        is_live,
        max_updated_at_ms,
    );

    ProjectedTaskOutcome {
        observation_task: ObservationTask {
            id: approved_task.task_id.clone(),
            title: task_title,
            status: status_str,
            depends_on: approved_task.depends_on.clone(),
        },
        is_approved: true,
        is_worker_task: true,
        is_native_route: false,
        live_running: is_live,
        review_outcome,
    }
}

#[allow(clippy::too_many_arguments)]
fn apply_task_lifecycle_projection(
    worker: &WorkerSnapshot,
    approved_task: &ApprovedTaskRecord,
    record: &ApprovalRecord,
    receipt: &TaskRunReceipt,
    exec_status: TaskExecutionStatus,
    lifecycle: &TaskLifecycleProjection,
    has_claim_id: bool,
    is_live: bool,
    max_updated_at_ms: &mut i64,
) -> (String, TaskReviewOutcome) {
    if matches!(
        lifecycle.review_status,
        TaskReviewLifecycleStatus::Accepted | TaskReviewLifecycleStatus::HistoricalAccepted
    ) {
        if let Some(ref rev_receipt) = lifecycle.review_receipt {
            *max_updated_at_ms = (*max_updated_at_ms).max(rev_receipt.approved_at_ms);
        }
        if matches!(
            lifecycle.integration_status,
            TaskIntegrationLifecycleStatus::Integrated
                | TaskIntegrationLifecycleStatus::HistoricalIntegrated
        ) {
            if let Some(ref int_receipt) = lifecycle.integration_receipt {
                *max_updated_at_ms = (*max_updated_at_ms).max(int_receipt.integrated_at_ms);
            }
        }
    }

    match exec_status {
        TaskExecutionStatus::Queued => ("queued".to_string(), TaskReviewOutcome::None),
        TaskExecutionStatus::Claimed => {
            if has_claim_id {
                ("claimed".to_string(), TaskReviewOutcome::None)
            } else {
                ("unknown".to_string(), TaskReviewOutcome::Unknown)
            }
        }
        TaskExecutionStatus::Running => {
            if is_live {
                ("running".to_string(), TaskReviewOutcome::None)
            } else {
                ("unknown".to_string(), TaskReviewOutcome::Unknown)
            }
        }
        TaskExecutionStatus::CancelRequested => ("unknown".to_string(), TaskReviewOutcome::Unknown),
        TaskExecutionStatus::ReadyForReview => match lifecycle.review_status {
            TaskReviewLifecycleStatus::Accepted | TaskReviewLifecycleStatus::HistoricalAccepted => {
                ("accepted".to_string(), TaskReviewOutcome::ReviewPassed)
            }
            TaskReviewLifecycleStatus::PendingReview => (
                "ready_for_review".to_string(),
                TaskReviewOutcome::PendingReview,
            ),
            TaskReviewLifecycleStatus::Invalidated => {
                if !worker_satisfies_ready_for_review_contract(
                    worker,
                    approved_task,
                    record,
                    receipt,
                ) {
                    ("failed".to_string(), TaskReviewOutcome::Invalidated)
                } else {
                    (
                        "ready_for_review".to_string(),
                        TaskReviewOutcome::Invalidated,
                    )
                }
            }
            TaskReviewLifecycleStatus::NotReady => {
                ("unknown".to_string(), TaskReviewOutcome::Unknown)
            }
        },
        TaskExecutionStatus::Failed => ("failed".to_string(), TaskReviewOutcome::None),
        TaskExecutionStatus::Cancelled => ("cancelled".to_string(), TaskReviewOutcome::None),
        TaskExecutionStatus::TimedOut => ("timed_out".to_string(), TaskReviewOutcome::None),
        TaskExecutionStatus::NotStarted
        | TaskExecutionStatus::DependencyInputsUnverified
        | TaskExecutionStatus::DependencyInputsReady
        | TaskExecutionStatus::NativeRouteRequired
        | TaskExecutionStatus::Unknown => ("unknown".to_string(), TaskReviewOutcome::Unknown),
    }
}

fn unknown_worker_task_outcome(
    approved_task: &ApprovedTaskRecord,
    task_title: String,
) -> ProjectedTaskOutcome {
    ProjectedTaskOutcome {
        observation_task: ObservationTask {
            id: approved_task.task_id.clone(),
            title: task_title,
            status: "unknown".to_string(),
            depends_on: approved_task.depends_on.clone(),
        },
        is_approved: true,
        is_worker_task: true,
        is_native_route: false,
        live_running: false,
        review_outcome: TaskReviewOutcome::Unknown,
    }
}

fn verify_intent_matches_task(
    intent: &TaskDispatchIntent,
    approved_task: &ApprovedTaskRecord,
) -> bool {
    if intent.task_id != approved_task.task_id
        || intent.task_digest != approved_task.task_digest
        || Some(intent.profile_id.as_str()) != approved_task.profile_id.as_deref()
        || Some(intent.command_key.as_str()) != approved_task.command_key.as_deref()
    {
        return false;
    }
    if approved_task.depends_on.is_empty() {
        intent.execution_binding.is_none()
    } else {
        intent.execution_binding.is_some()
    }
}

fn verify_receipt_matches_task(
    receipt: &TaskRunReceipt,
    approved_task: &ApprovedTaskRecord,
) -> bool {
    if receipt.task_id != approved_task.task_id
        || receipt.task_digest != approved_task.task_digest
        || Some(receipt.profile_id.as_str()) != approved_task.profile_id.as_deref()
        || Some(receipt.command_key.as_str()) != approved_task.command_key.as_deref()
    {
        return false;
    }
    if approved_task.depends_on.is_empty() {
        receipt.execution_binding.is_none()
    } else {
        receipt.execution_binding.is_some()
    }
}

fn find_exact_matching_core_worker(
    record: &ApprovalRecord,
    approved_task: &ApprovedTaskRecord,
    session_binding_id: &str,
    receipt: &TaskRunReceipt,
    core_states: &[ReadonlyCoreStateSnapshot],
    extra_core_snapshot: Option<&ReadonlyCoreStateSnapshot>,
) -> Option<(WorkerSnapshot, Option<ReadonlyRunRecord>)> {
    let mut matched_workers: Vec<(WorkerSnapshot, Option<ReadonlyRunRecord>)> = Vec::new();

    for snapshot in core_states.iter().chain(extra_core_snapshot) {
        for raw_worker in &snapshot.workers {
            let matches_worker_id = raw_worker.worker_id == receipt.worker_id;
            let matches_run_id = raw_worker.run_id == receipt.run_id;
            if !matches_worker_id && !matches_run_id {
                continue;
            }
            if !matches_worker_id || !matches_run_id {
                return None;
            }

            let worker_snapshot = raw_worker.to_worker_snapshot()?;
            if !worker_matches_frozen_identity(
                &worker_snapshot,
                raw_worker,
                record,
                approved_task,
                session_binding_id,
                receipt,
                snapshot,
            ) {
                return None;
            }

            let matched_run = snapshot
                .runs
                .iter()
                .find(|run| run.run_id == worker_snapshot.run_id)
                .cloned();
            matched_workers.push((worker_snapshot, matched_run));
        }
    }

    if matched_workers.is_empty() {
        return None;
    }

    let (first_worker, first_run) = matched_workers.remove(0);
    for (other_worker, other_run) in matched_workers {
        if other_worker != first_worker || other_run != first_run {
            return None;
        }
    }

    Some((first_worker, first_run))
}

fn worker_matches_frozen_identity(
    worker: &WorkerSnapshot,
    _raw_worker: &ReadonlyWorkerRecord,
    record: &ApprovalRecord,
    approved_task: &ApprovedTaskRecord,
    session_binding_id: &str,
    receipt: &TaskRunReceipt,
    snapshot: &ReadonlyCoreStateSnapshot,
) -> bool {
    let Some(expected_provider) = approved_task.provider else {
        return false;
    };

    if worker.worker_id != receipt.worker_id
        || worker.run_id != receipt.run_id
        || worker.binding_id != session_binding_id
        || worker.profile_id != receipt.profile_id
        || Some(worker.profile_id.as_str()) != approved_task.profile_id.as_deref()
        || worker.provider != expected_provider.as_str()
        || worker.requested_model != approved_task.base_model
        || worker.instruction != approved_task.instruction
    {
        return false;
    }

    match &approved_task.effort {
        Some(expected_effort) => {
            if &worker.requested_effort != expected_effort {
                return false;
            }
        }
        None => {
            if !worker.requested_effort.is_empty() {
                return false;
            }
        }
    }

    if approved_task.depends_on.is_empty() {
        if receipt.execution_binding.is_some() || worker.execution_binding.is_some() {
            return false;
        }
        if worker.worktree.base_commit != record.base_commit {
            return false;
        }
    } else {
        let (Some(ref receipt_binding), Some(ref worker_binding)) =
            (&receipt.execution_binding, &worker.execution_binding)
        else {
            return false;
        };
        if receipt_binding != worker_binding {
            return false;
        }
        if worker.worktree.base_commit != receipt_binding.execution_base_commit {
            return false;
        }
    }

    for run in snapshot.runs.iter().filter(|r| r.run_id == worker.run_id) {
        if run.binding_id != session_binding_id {
            return false;
        }
    }

    for binding in snapshot
        .bindings
        .iter()
        .filter(|b| b.binding_id == session_binding_id)
    {
        if binding.thread_uuid != record.thread_uuid {
            return false;
        }
    }

    true
}

fn worker_satisfies_ready_for_review_contract(
    worker: &WorkerSnapshot,
    approved_task: &ApprovedTaskRecord,
    record: &ApprovalRecord,
    receipt: &TaskRunReceipt,
) -> bool {
    if worker.status != WorkerStatus::Completed
        || worker.observed_model.as_deref() != Some(approved_task.base_model.as_str())
    {
        return false;
    }

    if let Some(ref expected_effort) = approved_task.effort {
        if worker.observed_effort.as_deref() != Some(expected_effort.as_str()) {
            return false;
        }
    }

    let Some(ref outcome) = worker.outcome else {
        return false;
    };

    if outcome.acceptance_state != WorkerAcceptanceState::ReadyForReview
        || outcome.missing_model_evidence
        || outcome.missing_effort_evidence
        || outcome.provider_error
        || outcome.incomplete_cleanup
        || outcome.rejected_by_diff_gate
        || outcome.timed_out
        || outcome.cancelled
        || outcome.exit_code != Some(0)
    {
        return false;
    }

    if worker.worktree.task_worktree_id.is_none() || worker.worktree.run_worktree_id.is_none() {
        return false;
    }

    let expected_base = if approved_task.depends_on.is_empty() {
        &record.base_commit
    } else {
        let Some(ref binding) = receipt.execution_binding else {
            return false;
        };
        &binding.execution_base_commit
    };

    worker.worktree.base_commit == *expected_base
}

fn aggregate_job_lifecycle(
    task_outcomes: &[ProjectedTaskOutcome],
    active_call_count: usize,
    manifest_phase: Option<HostPhase>,
    host_lock_held: bool,
) -> (ObservationJobState, String, Option<String>) {
    let approved_outcomes: Vec<&ProjectedTaskOutcome> =
        task_outcomes.iter().filter(|t| t.is_approved).collect();
    let worker_outcomes: Vec<&ProjectedTaskOutcome> = approved_outcomes
        .iter()
        .copied()
        .filter(|t| t.is_worker_task)
        .collect();
    let native_outcomes: Vec<&ProjectedTaskOutcome> = approved_outcomes
        .iter()
        .copied()
        .filter(|t| t.is_native_route)
        .collect();

    let any_unknown = approved_outcomes
        .iter()
        .any(|t| t.observation_task.status == "unknown");
    let any_invalidated = approved_outcomes
        .iter()
        .any(|t| t.review_outcome == TaskReviewOutcome::Invalidated);
    let any_pending_review = approved_outcomes
        .iter()
        .any(|t| t.review_outcome == TaskReviewOutcome::PendingReview);
    let any_review_passed = approved_outcomes
        .iter()
        .any(|t| t.review_outcome == TaskReviewOutcome::ReviewPassed);
    let all_workers_accepted = !worker_outcomes.is_empty()
        && worker_outcomes
            .iter()
            .all(|t| t.observation_task.status == "accepted");

    let review_state = if any_invalidated {
        Some("invalidated".to_string())
    } else if any_unknown {
        Some("unknown".to_string())
    } else if any_pending_review {
        Some("pending_review".to_string())
    } else if all_workers_accepted {
        Some("review_passed".to_string())
    } else if any_review_passed {
        Some("pending_review".to_string())
    } else {
        None
    };

    if active_call_count > 0 {
        return (
            ObservationJobState::Running,
            "running".to_string(),
            review_state,
        );
    }

    if any_unknown {
        return (
            ObservationJobState::Unknown,
            "unknown".to_string(),
            review_state,
        );
    }

    let any_failed = worker_outcomes
        .iter()
        .any(|t| matches!(t.observation_task.status.as_str(), "failed" | "timed_out"))
        || manifest_phase == Some(HostPhase::Failed);

    if any_failed {
        return (
            ObservationJobState::Failed,
            "failed".to_string(),
            review_state,
        );
    }

    let any_cancelled = worker_outcomes
        .iter()
        .any(|t| t.observation_task.status == "cancelled");
    if any_cancelled {
        return (
            ObservationJobState::Failed,
            "stopped".to_string(),
            review_state,
        );
    }

    let any_ready_for_review = worker_outcomes
        .iter()
        .any(|t| t.observation_task.status == "ready_for_review");
    if any_ready_for_review {
        return (
            ObservationJobState::Reviewing,
            "ready_for_review".to_string(),
            review_state,
        );
    }

    let any_claimed = worker_outcomes
        .iter()
        .any(|t| t.observation_task.status == "claimed");
    let any_queued = worker_outcomes
        .iter()
        .any(|t| t.observation_task.status == "queued");
    if any_claimed || any_queued {
        let exec_status = if any_claimed {
            "claimed".to_string()
        } else {
            "queued".to_string()
        };
        let state = if any_claimed {
            if host_lock_held
                && matches!(manifest_phase, Some(HostPhase::Ready | HostPhase::Stopping))
            {
                ObservationJobState::Ready
            } else {
                ObservationJobState::Unknown
            }
        } else {
            ObservationJobState::Ready
        };
        return (state, exec_status, review_state);
    }

    if all_workers_accepted && native_outcomes.is_empty() {
        return (
            ObservationJobState::Completed,
            "accepted".to_string(),
            Some("review_passed".to_string()),
        );
    }

    if !native_outcomes.is_empty() {
        let accepted_ids: BTreeSet<&str> = worker_outcomes
            .iter()
            .filter(|t| t.observation_task.status == "accepted")
            .map(|t| t.observation_task.id.as_str())
            .collect();
        let native_ready = worker_outcomes.is_empty()
            || all_workers_accepted
            || native_outcomes.iter().any(|native| {
                native
                    .observation_task
                    .depends_on
                    .iter()
                    .all(|dep| accepted_ids.contains(dep.as_str()))
            });
        if native_ready {
            let exec_status = if all_workers_accepted {
                "accepted".to_string()
            } else {
                "not_started".to_string()
            };
            return (ObservationJobState::NeedsUser, exec_status, review_state);
        }
    }

    let all_workers_not_started = !worker_outcomes.is_empty()
        && worker_outcomes.iter().all(|t| {
            t.observation_task.status == "not_started" || t.observation_task.status == "accepted"
        });

    if all_workers_not_started {
        return (
            ObservationJobState::Ready,
            "not_started".to_string(),
            review_state,
        );
    }

    (
        ObservationJobState::Unknown,
        "unknown".to_string(),
        review_state,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    use serde_json::json;
    use sha2::Digest;
    use threadlane_authority::{
        compute_authority_core_digest, compute_record_subject_digest, derive_approval_id,
        derive_command_key, derive_profile_id, AUTHORITY_POLICY_VERSION,
    };
    use threadlane_core::observation::{ObservationSource, ObservationSourceAvailability};
    use threadlane_core::{
        Command, CommandOutput, Core, WorkerCompletionRecord, WorkerOutcomeSummary,
        WorkerWorktreeUpdate,
    };
    use threadlane_host::{write_host_manifest, HostSessionLock, TASK_RECEIPTS_SCHEMA_VERSION};
    use threadlane_registry::{Difficulty, ToolAdapter, WorkflowRole};
    use threadlane_workers::{ExecutionBudgets, WorkerFilePermissions, WorkerProvider};

    fn sha256_hex(bytes: &[u8]) -> String {
        format!("{:x}", sha2::Sha256::digest(bytes))
    }

    static TEST_DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

    struct TempFixture {
        root: PathBuf,
    }

    impl TempFixture {
        fn new(prefix: &str) -> Self {
            let base = fs::canonicalize(std::env::temp_dir()).expect("canonicalize temp_dir");
            let nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0);
            let seq = TEST_DIR_COUNTER.fetch_add(1, Ordering::Relaxed);
            let root = base.join(format!(
                "threadlane-jobs-test-{prefix}-{}-{nanos}-{seq}",
                std::process::id()
            ));
            create_private_dir(&root);
            Self { root }
        }

        fn path(&self) -> &Path {
            &self.root
        }

        fn make_source_spec(&self, source_id: &str) -> SourceSpec {
            let registry_dir = self.root.join(format!("{source_id}-registry"));
            let plans_dir = self.root.join(format!("{source_id}-plans"));
            let authority_dir = self.root.join(format!("{source_id}-authority"));
            let sessions_dir = self.root.join(format!("{source_id}-sessions"));
            let runtime_dir = self.root.join(format!("{source_id}-runtime"));

            create_private_dir(&registry_dir);
            create_private_dir(&plans_dir);
            create_private_dir(&authority_dir);
            create_private_dir(&sessions_dir);
            create_private_dir(&runtime_dir);

            let hint_root = self.root.join("untrusted-hint-project");
            create_private_dir(&hint_root);

            SourceSpec {
                source: ObservationSource {
                    id: source_id.to_string(),
                    label: "Hinted Source Label".to_string(),
                    project_path: Some(hint_root.to_string_lossy().into_owned()),
                    availability: ObservationSourceAvailability::Available,
                    message: None,
                },
                registry_dir,
                plans_dir,
                authority_dir,
                sessions_dir,
                runtime_state_dirs: vec![runtime_dir],
                project_root_hint: Some(hint_root),
                binding_id_hint: Some("bnd_untrusted_hint".to_string()),
                thread_uuid_hint: Some("0196ed00-0000-7000-8000-999999999999".to_string()),
            }
        }
    }

    impl Drop for TempFixture {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.root);
        }
    }

    fn create_private_dir(path: &Path) {
        fs::create_dir_all(path).expect("create test dir");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(path, fs::Permissions::from_mode(0o700))
                .expect("chmod 0700 test dir");
        }
    }

    fn write_private_file(path: &Path, bytes: &[u8]) {
        if let Some(parent) = path.parent() {
            create_private_dir(parent);
        }
        fs::write(path, bytes).expect("write private test file");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(path, fs::Permissions::from_mode(0o600))
                .expect("chmod 0600 test file");
        }
    }

    fn sample_plan_task(
        id: &str,
        title: &str,
        role: WorkflowRole,
        depends_on: Vec<&str>,
    ) -> PlanTask {
        PlanTask {
            id: id.to_string(),
            title: title.to_string(),
            instruction: format!("Implement task {id} carefully"),
            role,
            difficulty: Difficulty::Hard,
            depends_on: depends_on.into_iter().map(String::from).collect(),
            allowed_write_paths: vec![format!("src/{id}.rs")],
        }
    }

    fn sample_plan_revision(
        plan_id: &str,
        binding_id: &str,
        revision: u64,
        status: PlanStatus,
        summary: &str,
        tasks: Vec<PlanTask>,
        created_at_ms: i64,
    ) -> PlanRevision {
        PlanRevision {
            plan_id: plan_id.to_string(),
            binding_id: binding_id.to_string(),
            revision,
            profile_id: "default_profile".to_string(),
            profile_revision: 1,
            summary: summary.to_string(),
            tasks,
            status,
            created_at_ms,
        }
    }

    fn write_plans_ledger(plans_dir: &Path, revisions: &[PlanRevision]) {
        let payload = json!({
            "schema_version": PLAN_STORE_SCHEMA_VERSION,
            "revisions": revisions,
        });
        let bytes = serde_json::to_vec_pretty(&payload).expect("serialize plans.json");
        write_private_file(&plans_dir.join(PLANS_LEDGER_FILE_NAME), &bytes);
    }

    fn build_signed_approval_record(
        plan: &PlanRevision,
        thread_uuid: &str,
        repo_path: &Path,
        base_commit: &str,
        selected_tasks: &[(&PlanTask, ToolAdapter, Option<WorkerProvider>)],
        created_at_ms: i64,
    ) -> ApprovalRecord {
        let approved_task_ids: Vec<String> = selected_tasks
            .iter()
            .map(|(t, _, _)| t.id.clone())
            .collect();
        let approved_write_paths: Vec<String> = selected_tasks
            .iter()
            .flat_map(|(t, _, _)| t.allowed_write_paths.iter().cloned())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();

        let exec_path = PathBuf::from("/bin/sh");
        let mut tasks = Vec::with_capacity(selected_tasks.len());
        for (plan_task, adapter, provider) in selected_tasks {
            let instruction_digest = sha256_hex(plan_task.instruction.as_bytes());
            let task_digest = sha256_hex(
                format!(
                    "task-digest:{}:{}:{}:{}",
                    plan.plan_id, plan.revision, plan_task.id, instruction_digest
                )
                .as_bytes(),
            );
            let is_worker = provider.is_some();
            tasks.push(ApprovedTaskRecord {
                task_id: plan_task.id.clone(),
                title: plan_task.title.clone(),
                instruction: plan_task.instruction.clone(),
                instruction_digest,
                role: plan_task.role,
                difficulty: plan_task.difficulty,
                tool_id: if is_worker {
                    "antigravity_cli".to_string()
                } else {
                    "codex_native".to_string()
                },
                adapter: *adapter,
                provider: *provider,
                canonical_executable: is_worker.then(|| exec_path.clone()),
                model_id: "mdl_argon".to_string(),
                model_arg: "gemini-4-argon-high".to_string(),
                base_model: "gemini-4-argon-high".to_string(),
                effort: Some("high".to_string()),
                cli_model_arg: is_worker.then(|| "gemini-4-argon-high".to_string()),
                standalone: is_worker.then_some(false),
                file_permissions: is_worker.then_some(WorkerFilePermissions::TaskFiles),
                depends_on: plan_task.depends_on.clone(),
                allowed_write_paths: plan_task.allowed_write_paths.clone(),
                task_digest,
                profile_id: None,
                command_key: None,
            });
        }

        let plan_digest =
            sha256_hex(format!("plan-digest:{}:{}", plan.plan_id, plan.revision).as_bytes());
        let budgets = ExecutionBudgets::default();

        let mut record = ApprovalRecord {
            approval_id: String::new(),
            subject_digest: String::new(),
            policy_version: AUTHORITY_POLICY_VERSION,
            binding_id: plan.binding_id.clone(),
            thread_uuid: thread_uuid.to_string(),
            plan_id: plan.plan_id.clone(),
            plan_revision: plan.revision,
            profile_id: plan.profile_id.clone(),
            registry_revision: plan.profile_revision,
            plan_digest,
            repository_path: repo_path.to_path_buf(),
            base_commit: base_commit.to_string(),
            approved_task_ids,
            approved_write_paths,
            max_parallel: 2,
            budgets,
            tasks,
            created_at_ms,
        };

        let core_digest =
            compute_authority_core_digest(&record).expect("compute authority core digest");
        for task in &mut record.tasks {
            if task.provider.is_some() {
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
        }

        let subject_digest =
            compute_record_subject_digest(&record).expect("compute record subject digest");
        record.approval_id = derive_approval_id(&subject_digest);
        record.subject_digest = subject_digest;

        verify_approval_record_integrity(&record, Path::new("/tmp/approvals.json"))
            .expect("generated test ApprovalRecord must pass integrity verification");
        record
    }

    fn write_approvals_ledger(authority_dir: &Path, approvals: &[ApprovalRecord]) {
        let payload = json!({
            "schema_version": AUTHORITY_SCHEMA_VERSION,
            "approvals": approvals,
        });
        let bytes = serde_json::to_vec_pretty(&payload).expect("serialize approvals.json");
        write_private_file(&authority_dir.join(APPROVALS_FILE_NAME), &bytes);
    }

    fn write_task_receipts_fixture(
        session_dir: &Path,
        record: &ApprovalRecord,
        session_binding_id: &str,
        pending_intents: &[TaskDispatchIntent],
        receipts: &[TaskRunReceipt],
    ) {
        let payload = json!({
            "schema_version": TASK_RECEIPTS_SCHEMA_VERSION,
            "approval_id": record.approval_id,
            "subject_digest": record.subject_digest,
            "session_binding_id": session_binding_id,
            "pending_intents": pending_intents,
            "receipts": receipts,
        });
        let bytes = serde_json::to_vec_pretty(&payload).expect("serialize task-receipts.json");
        write_private_file(&session_dir.join(TASK_RECEIPTS_FILENAME), &bytes);
    }

    #[test]
    fn missing_source_files_create_no_files_or_locks_and_return_empty_projection() {
        let fixture = TempFixture::new("missing-files");
        let missing_base = fixture.path().join("nonexistent");
        let spec = SourceSpec {
            source: ObservationSource {
                id: "default".to_string(),
                label: "Local".to_string(),
                project_path: None,
                availability: ObservationSourceAvailability::Available,
                message: None,
            },
            registry_dir: missing_base.join("registry"),
            plans_dir: missing_base.join("plans"),
            authority_dir: missing_base.join("authority"),
            sessions_dir: missing_base.join("sessions"),
            runtime_state_dirs: vec![missing_base.join("runtime")],
            project_root_hint: None,
            binding_id_hint: None,
            thread_uuid_hint: None,
        };

        let projection = project_jobs(&spec);
        assert!(projection.jobs.is_empty());
        assert!(projection.worker_links.is_empty());
        assert!(projection.warnings.is_empty(), "{:?}", projection.warnings);
        assert!(!projection.jobs_has_more);
        assert_eq!(projection.core_states.len(), 1);
        assert_eq!(
            projection.core_states[0],
            ReadonlyCoreStateSnapshot::default()
        );
        assert!(!missing_base.exists());
    }

    #[test]
    fn latest_revision_only_and_source_project_hint_isolation() {
        let fixture = TempFixture::new("latest-rev-and-hints");
        let spec = fixture.make_source_spec("ctx_1");

        let real_repo = fixture.path().join("verified-alpha-repo");
        create_private_dir(&real_repo);
        let base_commit = "a".repeat(40);
        let verified_thread = "0196ed00-0000-7000-8000-000000000011";

        let task_v1 = sample_plan_task("task_1", "Rev1 Task", WorkflowRole::Coder, vec![]);
        let rev1 = sample_plan_revision(
            "plan_alpha",
            "bnd_alpha",
            1,
            PlanStatus::Superseded,
            "Alpha v1 summary",
            vec![task_v1.clone()],
            1_700_000_001_000,
        );

        // Revision 1 had an approval, but Revision 2 is a newer Draft without approval.
        let old_approval = build_signed_approval_record(
            &rev1,
            verified_thread,
            &real_repo,
            &base_commit,
            &[(
                &task_v1,
                ToolAdapter::Antigravity,
                Some(WorkerProvider::Antigravity),
            )],
            1_700_000_002_000,
        );

        let task_v2 = sample_plan_task(
            "task_2",
            "Rev2 Task With Control",
            WorkflowRole::Coder,
            vec![],
        );
        let rev2 = sample_plan_revision(
            "plan_alpha",
            "bnd_alpha",
            2,
            PlanStatus::Draft,
            "Alpha v2\nwith sk-secret-value",
            vec![task_v2],
            1_700_000_005_000,
        );

        // Second plan in the same project/binding that IS approved at its latest revision.
        let beta_task_1 = sample_plan_task("b_1", "Beta Root", WorkflowRole::Coder, vec![]);
        let beta_task_2 =
            sample_plan_task("b_2", "Beta Extra", WorkflowRole::Reviewer, vec!["b_1"]);
        let beta_rev1 = sample_plan_revision(
            "plan_beta",
            "bnd_alpha",
            1,
            PlanStatus::Published,
            "Beta published summary",
            vec![beta_task_1.clone(), beta_task_2.clone()],
            1_700_000_010_000,
        );
        let beta_approval = build_signed_approval_record(
            &beta_rev1,
            verified_thread,
            &real_repo,
            &base_commit,
            &[(
                &beta_task_1,
                ToolAdapter::Antigravity,
                Some(WorkerProvider::Antigravity),
            )],
            1_700_000_020_000,
        );

        write_plans_ledger(&spec.plans_dir, &[rev1, rev2, beta_rev1]);
        write_approvals_ledger(&spec.authority_dir, &[old_approval, beta_approval]);

        let projection = project_jobs(&spec);
        assert!(projection.warnings.is_empty(), "{:?}", projection.warnings);
        assert_eq!(projection.jobs.len(), 2);

        // Job 1: plan_alpha uses latest revision (2) only; old approval for rev 1 is not attached.
        // Source hints (`project_root_hint`, `binding_id_hint`, `thread_uuid_hint`) are never used.
        let alpha = &projection.jobs[0];
        assert_eq!(alpha.source_id, "ctx_1");
        assert_eq!(alpha.id, "plan_alpha");
        assert_eq!(alpha.plan_id.as_deref(), Some("plan_alpha"));
        assert_eq!(alpha.plan_revision, Some(2));
        assert_eq!(alpha.state, ObservationJobState::Planning);
        assert_eq!(alpha.approval_status, "draft");
        assert_eq!(alpha.execution_status, "not_started");
        assert_eq!(alpha.review_state, None);
        assert_eq!(alpha.project_path, None);
        assert_eq!(alpha.project_label, GENERIC_PROJECT_LABEL);
        assert_eq!(alpha.binding_id.as_deref(), Some("bnd_alpha"));
        assert_eq!(alpha.thread_uuid, None);
        assert_eq!(alpha.updated_at_ms, Some(1_700_000_005_000));
        assert_eq!(alpha.summary.as_deref(), Some(REDACTED_SUMMARY_MARKER));
        assert_eq!(alpha.tasks.len(), 1);
        assert_eq!(alpha.tasks[0].id, "task_2");
        assert_eq!(alpha.tasks[0].title, "Rev2 Task With Control");
        assert_eq!(alpha.tasks[0].status, "not_approved");

        // Job 2: plan_beta has a unique verified approval at latest revision (1).
        let beta = &projection.jobs[1];
        assert_eq!(beta.id, "plan_beta");
        assert_eq!(beta.plan_revision, Some(1));
        assert_eq!(beta.state, ObservationJobState::Ready);
        assert_eq!(beta.approval_status, "approved");
        assert_eq!(beta.execution_status, "not_started");
        assert_eq!(beta.review_state, None);
        assert_eq!(
            beta.project_path.as_deref(),
            Some(real_repo.to_string_lossy().as_ref())
        );
        assert_eq!(beta.project_label, "verified-alpha-repo");
        assert_eq!(beta.binding_id.as_deref(), Some("bnd_alpha"));
        assert_eq!(beta.thread_uuid.as_deref(), Some(verified_thread));
        assert_eq!(beta.updated_at_ms, Some(1_700_000_020_000));
        assert_eq!(beta.tasks.len(), 2);
        assert_eq!(beta.tasks[0].id, "b_1");
        assert_eq!(beta.tasks[0].status, "not_started");
        assert_eq!(beta.tasks[1].id, "b_2");
        assert_eq!(beta.tasks[1].status, "not_approved");
        assert_eq!(beta.tasks[1].depends_on, vec!["b_1".to_string()]);
    }

    #[test]
    fn bad_oversize_and_symlink_ledgers_fail_closed_with_generic_warnings() {
        let fixture = TempFixture::new("bad-ledgers");
        let spec = fixture.make_source_spec("default");

        // 1. Malformed JSON with sensitive path/secret text must emit only a generic path-free warning.
        write_private_file(
            &spec.plans_dir.join(PLANS_LEDGER_FILE_NAME),
            b"{bad json /secret/path sk-live-12345",
        );
        let proj_bad = project_jobs(&spec);
        assert!(proj_bad.jobs.is_empty());
        assert_eq!(proj_bad.warnings.len(), 1);
        assert!(!proj_bad.warnings[0].contains("/secret/path"));
        assert!(!proj_bad.warnings[0].contains("sk-live"));
        assert!(!proj_bad.warnings[0].contains("plans.json"));

        // 2. Oversize ledger (> 1 MiB) fails closed.
        let oversize = vec![b' '; MAX_PLAN_STORE_BYTES + 16];
        write_private_file(&spec.plans_dir.join(PLANS_LEDGER_FILE_NAME), &oversize);
        let proj_oversize = project_jobs(&spec);
        assert!(proj_oversize.jobs.is_empty());
        assert_eq!(proj_oversize.warnings.len(), 1);

        // 3. Symlink ledger fails closed.
        #[cfg(unix)]
        {
            let _ = fs::remove_file(spec.plans_dir.join(PLANS_LEDGER_FILE_NAME));
            let target = fixture.path().join("outside-plans.json");
            write_private_file(&target, b"{\"schema_version\":1,\"revisions\":[]}");
            std::os::unix::fs::symlink(&target, spec.plans_dir.join(PLANS_LEDGER_FILE_NAME))
                .expect("create symlink plans.json");
            let proj_symlink = project_jobs(&spec);
            assert!(proj_symlink.jobs.is_empty());
            assert_eq!(proj_symlink.warnings.len(), 1);
            assert!(!proj_symlink.warnings[0].contains("outside-plans"));
        }
    }

    #[test]
    fn corrupt_plan_approval_and_db_preserve_unrelated_healthy_jobs_and_snapshots() {
        let fixture = TempFixture::new("isolation-corrupt");
        let mut spec = fixture.make_source_spec("default");
        let bad_runtime_dir = fixture.path().join("bad-runtime");
        create_private_dir(&bad_runtime_dir);
        write_private_file(
            &bad_runtime_dir.join("state.db"),
            b"not a valid sqlite database",
        );
        spec.runtime_state_dirs.push(bad_runtime_dir);

        let healthy_task = sample_plan_task("t_ok", "Healthy Task", WorkflowRole::Coder, vec![]);
        let healthy_plan = sample_plan_revision(
            "plan_healthy",
            "bnd_ok",
            1,
            PlanStatus::Published,
            "Healthy plan summary",
            vec![healthy_task],
            1_700_000_000_000,
        );

        let raw_plans = json!({
            "schema_version": 1,
            "revisions": [
                healthy_plan,
                {
                    "plan_id": "plan_corrupt",
                    "binding_id": "bnd_bad",
                    "revision": 1,
                    "profile_id": "default_profile",
                    "profile_revision": 1,
                    "summary": "",
                    "tasks": [],
                    "status": "draft",
                    "created_at_ms": 1700000000000i64
                }
            ]
        });
        write_private_file(
            &spec.plans_dir.join(PLANS_LEDGER_FILE_NAME),
            &serde_json::to_vec(&raw_plans).expect("serialize raw plans"),
        );

        let projection = project_jobs(&spec);
        assert_eq!(projection.jobs.len(), 1);
        assert_eq!(projection.jobs[0].id, "plan_healthy");
        assert_eq!(projection.jobs[0].approval_status, "unapproved");
        assert_eq!(projection.jobs[0].state, ObservationJobState::Planning);
        assert_eq!(projection.core_states.len(), 1);
        assert_eq!(projection.warnings.len(), 2);
    }

    #[test]
    fn deterministic_ordering_and_500_job_cap() {
        let fixture = TempFixture::new("cap-500");
        let spec = fixture.make_source_spec("default");

        let mut revisions = Vec::with_capacity(505);
        for idx in (0..505).rev() {
            let plan_id = format!("plan_{idx:04}");
            let task = sample_plan_task("t1", "Task", WorkflowRole::Coder, vec![]);
            revisions.push(sample_plan_revision(
                &plan_id,
                "bnd_1",
                1,
                PlanStatus::Draft,
                "Summary",
                vec![task],
                1_700_000_000_000 + i64::from(idx),
            ));
        }

        write_plans_ledger(&spec.plans_dir, &revisions);

        let projection = project_jobs(&spec);
        assert!(projection.warnings.is_empty());
        assert_eq!(projection.jobs.len(), MAX_PROJECTED_JOBS);
        assert!(projection.jobs_has_more);
        assert_eq!(projection.jobs[0].id, "plan_0000");
        assert_eq!(projection.jobs[499].id, "plan_0499");
    }

    #[test]
    fn live_worker_requires_held_host_lock_and_claim_id_while_completed_requires_verified_review() {
        let fixture = TempFixture::new("worker-lifecycle");
        let spec = fixture.make_source_spec("default");
        let repo_path = fixture.path().join("repo");
        create_private_dir(&repo_path);
        let base_commit = "b".repeat(40);
        let thread_uuid = "0196ed00-0000-7000-8000-000000000022";

        let task_1 = sample_plan_task("t1", "Worker Task", WorkflowRole::Coder, vec![]);
        let plan = sample_plan_revision(
            "plan_exec",
            "bnd_ctrl",
            1,
            PlanStatus::Applied,
            "Execution plan",
            vec![task_1.clone()],
            1_700_000_100_000,
        );
        let approval = build_signed_approval_record(
            &plan,
            thread_uuid,
            &repo_path,
            &base_commit,
            &[(
                &task_1,
                ToolAdapter::Antigravity,
                Some(WorkerProvider::Antigravity),
            )],
            1_700_000_200_000,
        );
        let approved_task = &approval.tasks[0];

        write_plans_ledger(&spec.plans_dir, &[plan]);
        write_approvals_ledger(&spec.authority_dir, std::slice::from_ref(&approval));

        let session_dir = spec.sessions_dir.join(&approval.approval_id);
        create_private_dir(&session_dir);

        // Seed Core runtime DB with a session binding and a running worker.
        let db_path = spec.runtime_state_dirs[0].join(CORE_STATE_DB_FILE_NAME);
        let mut core = Core::open(&db_path).expect("open Core DB");
        let session_binding = core
            .create_binding(thread_uuid, "Session Thread Label")
            .expect("create session binding");
        let session_binding_id = session_binding.binding_id.clone();

        let manifest = HostSessionManifest::from_approval_record(
            &approval,
            Some(session_binding_id.clone()),
            HostPhase::Ready,
            Some(std::process::id()),
            None,
            1_700_000_300_000,
            1_700_000_350_000,
        );
        write_host_manifest(&session_dir, &manifest).expect("write host manifest");

        // 1. Missing task ledger after manifest exists must project Unknown, never NotStarted.
        let proj_missing_ledger = project_jobs(&spec);
        assert_eq!(proj_missing_ledger.jobs.len(), 1);
        assert_eq!(
            proj_missing_ledger.jobs[0].state,
            ObservationJobState::Unknown
        );
        assert_eq!(proj_missing_ledger.jobs[0].execution_status, "unknown");
        assert_eq!(proj_missing_ledger.jobs[0].tasks[0].status, "unknown");

        // 2. Dispatch intent without sealed receipt must project Unknown, never NotStarted or Running.
        let intent = TaskDispatchIntent {
            task_id: approved_task.task_id.clone(),
            task_digest: approved_task.task_digest.clone(),
            profile_id: approved_task.profile_id.clone().expect("profile_id"),
            command_key: approved_task.command_key.clone().expect("command_key"),
            execution_binding: None,
            recorded_at_ms: 1_700_000_400_000,
        };
        write_task_receipts_fixture(
            &session_dir,
            &approval,
            &session_binding_id,
            std::slice::from_ref(&intent),
            &[],
        );
        let proj_intent_only = project_jobs(&spec);
        assert_eq!(proj_intent_only.jobs[0].state, ObservationJobState::Unknown);
        assert_eq!(proj_intent_only.jobs[0].tasks[0].status, "unknown");
        assert!(proj_intent_only.worker_links.is_empty());

        // 3. Start and claim worker in Core DB and seal receipt.
        let profile_digest = "c".repeat(64);
        let started = match core
            .execute(
                &session_binding_id,
                Command::StartWorker {
                    command_key: approved_task.command_key.clone().expect("command_key"),
                    profile_id: approved_task.profile_id.clone().expect("profile_id"),
                    profile_digest: profile_digest.clone(),
                    provider: "antigravity".to_string(),
                    requested_model: approved_task.base_model.clone(),
                    requested_effort: approved_task.effort.clone().unwrap_or_default(),
                    base_commit: base_commit.clone(),
                    execution_binding: None,
                    instruction: approved_task.instruction.clone(),
                    now_ms: 1_700_000_500_000,
                },
            )
            .expect("start worker")
        {
            CommandOutput::Worker(w) => w,
            other => panic!("unexpected output: {other:?}"),
        };

        let receipt = TaskRunReceipt {
            task_id: approved_task.task_id.clone(),
            task_digest: approved_task.task_digest.clone(),
            profile_id: approved_task.profile_id.clone().expect("profile_id"),
            command_key: approved_task.command_key.clone().expect("command_key"),
            run_id: started.run_id.clone(),
            worker_id: started.worker_id.clone(),
            execution_binding: None,
            recorded_at_ms: 1_700_000_510_000,
        };
        write_task_receipts_fixture(
            &session_dir,
            &approval,
            &session_binding_id,
            std::slice::from_ref(&intent),
            std::slice::from_ref(&receipt),
        );

        let claim = core
            .claim_worker(
                &session_binding_id,
                &started.run_id,
                started.revision,
                &profile_digest,
                1_700_000_520_000,
            )
            .expect("claim worker");
        core.record_worker_running(
            &claim,
            WorkerWorktreeUpdate {
                run_worktree_id: "wt-run-1".to_string(),
                run_worktree_path: session_dir
                    .join("work-area/runs/wt-run-1")
                    .to_string_lossy()
                    .into_owned(),
                run_branch_name: "tl/run/wt-run-1".to_string(),
                task_worktree_id: "wt-task-1".to_string(),
                task_worktree_path: session_dir
                    .join("work-area/tasks/wt-task-1")
                    .to_string_lossy()
                    .into_owned(),
                task_branch_name: "tl/task/wt-task-1".to_string(),
                base_commit: base_commit.clone(),
            },
            1_700_000_530_000,
        )
        .expect("record worker running");

        // Without held host.lock, Running status in DB must project Unknown and live=false!
        let proj_stale_running = project_jobs(&spec);
        assert_eq!(
            proj_stale_running.jobs[0].state,
            ObservationJobState::Unknown
        );
        assert_eq!(proj_stale_running.jobs[0].execution_status, "unknown");
        assert_eq!(proj_stale_running.jobs[0].active_call_count, 0);
        assert_eq!(proj_stale_running.jobs[0].tasks[0].status, "unknown");
        assert_eq!(proj_stale_running.worker_links.len(), 1);
        assert!(!proj_stale_running.worker_links[0].live);
        assert_eq!(
            proj_stale_running.worker_links[0].approval_id,
            approval.approval_id
        );
        assert_eq!(
            proj_stale_running.worker_links[0].role.as_deref(),
            Some("coder")
        );

        // With held host.lock, Running worker projects Running, live=true, and active_call_count=1.
        let host_lock =
            HostSessionLock::acquire(&session_dir, &approval.approval_id).expect("acquire lock");
        let proj_live = project_jobs(&spec);
        assert_eq!(proj_live.jobs[0].state, ObservationJobState::Running);
        assert_eq!(proj_live.jobs[0].execution_status, "running");
        assert_eq!(proj_live.jobs[0].active_call_count, 1);
        assert_eq!(proj_live.jobs[0].tasks[0].status, "running");
        assert_eq!(proj_live.worker_links.len(), 1);
        assert!(proj_live.worker_links[0].live);
        drop(host_lock);

        // 4. Complete worker -> ReadyForReview. Even with PlanStatus::Applied, never Completed without review receipt.
        core.record_worker_outcome(
            &claim,
            WorkerCompletionRecord {
                status: WorkerStatus::Completed,
                observed_model: Some(approved_task.base_model.clone()),
                observed_effort: approved_task.effort.clone(),
                head_commit: Some("d".repeat(40)),
                dropped_live_events: 0,
                outcome: WorkerOutcomeSummary {
                    exit_code: Some(0),
                    signal: None,
                    duration_ms: Some(70_000),
                    timed_out: false,
                    cancelled: false,
                    cancel_requested_before_completion: false,
                    missing_model_evidence: false,
                    missing_effort_evidence: false,
                    provider_error: false,
                    incomplete_cleanup: false,
                    rejected_by_diff_gate: false,
                    stdout_truncated: false,
                    stderr_truncated: false,
                    acceptance_state: WorkerAcceptanceState::ReadyForReview,
                    changed_paths_count: 1,
                    changed_paths_preview: vec!["src/t1.rs".to_string()],
                    violation_count: 0,
                    violation_summaries: vec![],
                    final_summary: Some("Completed implementation".to_string()),
                    failure_reason: None,
                    report_artifact_id: None,
                },
            },
            1_700_000_600_000,
        )
        .expect("complete worker");
        drop(core);

        let proj_completed = project_jobs(&spec);
        assert_eq!(proj_completed.jobs[0].state, ObservationJobState::Reviewing);
        assert_eq!(proj_completed.jobs[0].execution_status, "ready_for_review");
        assert_eq!(
            proj_completed.jobs[0].review_state.as_deref(),
            Some("invalidated")
        );
        assert_eq!(proj_completed.jobs[0].tasks[0].status, "ready_for_review");
        assert_eq!(
            proj_completed.jobs[0].updated_at_ms,
            Some(1_700_000_600_000)
        );
        assert_eq!(proj_completed.worker_links.len(), 1);
        assert!(!proj_completed.worker_links[0].live);

        // 5. Verify that only Accepted / HistoricalAccepted from the lifecycle verifier yields review_passed / Completed.
        let completed_worker = proj_completed.core_states[0].workers[0]
            .to_worker_snapshot()
            .expect("worker snapshot");
        let mut max_ts = 1_700_000_600_000;
        let pending_proj = TaskLifecycleProjection {
            review_status: TaskReviewLifecycleStatus::PendingReview,
            integration_status: TaskIntegrationLifecycleStatus::NotIntegrated,
            evidence_status: threadlane_host::TaskEvidenceVerificationStatus::PendingReview,
            task_checkout_present: true,
            integration_checkout_present: false,
            review_receipt: None,
            integration_receipt: None,
            verification_detail: None,
        };
        let (pending_task_status, pending_rev_outcome) = apply_task_lifecycle_projection(
            &completed_worker,
            approved_task,
            &approval,
            &receipt,
            TaskExecutionStatus::ReadyForReview,
            &pending_proj,
            false,
            false,
            &mut max_ts,
        );
        assert_eq!(pending_task_status, "ready_for_review");
        assert_eq!(pending_rev_outcome, TaskReviewOutcome::PendingReview);
        let (p_state, p_exec, p_rev) = aggregate_job_lifecycle(
            &[ProjectedTaskOutcome {
                observation_task: ObservationTask {
                    id: approved_task.task_id.clone(),
                    title: "Worker Task".to_string(),
                    status: pending_task_status,
                    depends_on: vec![],
                },
                is_approved: true,
                is_worker_task: true,
                is_native_route: false,
                live_running: false,
                review_outcome: pending_rev_outcome,
            }],
            0,
            Some(HostPhase::Ready),
            false,
        );
        assert_eq!(p_state, ObservationJobState::Reviewing);
        assert_eq!(p_exec, "ready_for_review");
        assert_eq!(p_rev.as_deref(), Some("pending_review"));

        let accepted_proj = TaskLifecycleProjection {
            review_status: TaskReviewLifecycleStatus::Accepted,
            integration_status: TaskIntegrationLifecycleStatus::ReadyToIntegrate,
            evidence_status: threadlane_host::TaskEvidenceVerificationStatus::VerifiedLive,
            task_checkout_present: true,
            integration_checkout_present: false,
            review_receipt: Some(threadlane_host::VerifiedReviewReceiptSummary {
                receipt_id: "rev_1".to_string(),
                subject_digest: "f".repeat(64),
                receipt_digest: "a".repeat(64),
                task_worktree_id: "wt-task-1".to_string(),
                base_commit: base_commit.clone(),
                task_head_commit: "d".repeat(40),
                task_tree_sha: "e".repeat(40),
                diff_digest: "b".repeat(64),
                reviewer_id: "reviewer_1".to_string(),
                approved_at_ms: 1_700_000_700_000,
            }),
            integration_receipt: None,
            verification_detail: None,
        };
        let (acc_task_status, acc_rev_outcome) = apply_task_lifecycle_projection(
            &completed_worker,
            approved_task,
            &approval,
            &receipt,
            TaskExecutionStatus::ReadyForReview,
            &accepted_proj,
            false,
            false,
            &mut max_ts,
        );
        assert_eq!(acc_task_status, "accepted");
        assert_eq!(acc_rev_outcome, TaskReviewOutcome::ReviewPassed);
        assert_eq!(max_ts, 1_700_000_700_000);
        let (a_state, a_exec, a_rev) = aggregate_job_lifecycle(
            &[ProjectedTaskOutcome {
                observation_task: ObservationTask {
                    id: approved_task.task_id.clone(),
                    title: "Worker Task".to_string(),
                    status: acc_task_status,
                    depends_on: vec![],
                },
                is_approved: true,
                is_worker_task: true,
                is_native_route: false,
                live_running: false,
                review_outcome: acc_rev_outcome,
            }],
            0,
            Some(HostPhase::Ready),
            false,
        );
        assert_eq!(a_state, ObservationJobState::Completed);
        assert_eq!(a_exec, "accepted");
        assert_eq!(a_rev.as_deref(), Some("review_passed"));
    }

    #[test]
    fn native_route_approval_projects_needs_user_and_mismatched_worker_never_emits_link() {
        let fixture = TempFixture::new("native-and-mismatch");
        let spec = fixture.make_source_spec("default");
        let repo_path = fixture.path().join("repo-native");
        create_private_dir(&repo_path);
        let base_commit = "e".repeat(40);
        let thread_uuid = "0196ed00-0000-7000-8000-000000000033";

        // 1. Plan with approved manual native route task -> NeedsUser.
        let native_task = sample_plan_task(
            "t_native",
            "Manual Codex Step",
            WorkflowRole::Orchestrator,
            vec![],
        );
        let native_plan = sample_plan_revision(
            "plan_native",
            "bnd_native",
            1,
            PlanStatus::Published,
            "Native route plan",
            vec![native_task.clone()],
            1_700_000_000_000,
        );
        let native_approval = build_signed_approval_record(
            &native_plan,
            thread_uuid,
            &repo_path,
            &base_commit,
            &[(&native_task, ToolAdapter::CodexCli, None)],
            1_700_000_010_000,
        );

        // 2. Plan with a receipt pointing to a non-existent/mismatched Core worker -> Unknown, no worker link.
        let worker_task = sample_plan_task(
            "t_mismatch",
            "Mismatched Worker",
            WorkflowRole::Coder,
            vec![],
        );
        let mismatch_plan = sample_plan_revision(
            "plan_mismatch",
            "bnd_native",
            1,
            PlanStatus::Published,
            "Mismatch plan",
            vec![worker_task.clone()],
            1_700_000_020_000,
        );
        let mismatch_approval = build_signed_approval_record(
            &mismatch_plan,
            thread_uuid,
            &repo_path,
            &base_commit,
            &[(
                &worker_task,
                ToolAdapter::Antigravity,
                Some(WorkerProvider::Antigravity),
            )],
            1_700_000_030_000,
        );

        let session_dir = spec.sessions_dir.join(&mismatch_approval.approval_id);
        create_private_dir(&session_dir);
        let manifest = HostSessionManifest::from_approval_record(
            &mismatch_approval,
            Some("bnd_session_1".to_string()),
            HostPhase::Ready,
            None,
            None,
            1_700_000_040_000,
            1_700_000_040_000,
        );
        write_host_manifest(&session_dir, &manifest).expect("write manifest");

        let approved_wt = &mismatch_approval.tasks[0];
        let orphan_receipt = TaskRunReceipt {
            task_id: approved_wt.task_id.clone(),
            task_digest: approved_wt.task_digest.clone(),
            profile_id: approved_wt.profile_id.clone().expect("profile_id"),
            command_key: approved_wt.command_key.clone().expect("command_key"),
            run_id: "run_0196ed00000070008000000000000999".to_string(),
            worker_id: "wrk_0196ed00000070008000000000000999".to_string(),
            execution_binding: None,
            recorded_at_ms: 1_700_000_050_000,
        };
        write_task_receipts_fixture(
            &session_dir,
            &mismatch_approval,
            "bnd_session_1",
            &[TaskDispatchIntent {
                task_id: approved_wt.task_id.clone(),
                task_digest: approved_wt.task_digest.clone(),
                profile_id: approved_wt.profile_id.clone().expect("profile_id"),
                command_key: approved_wt.command_key.clone().expect("command_key"),
                execution_binding: None,
                recorded_at_ms: 1_700_000_045_000,
            }],
            &[orphan_receipt],
        );

        write_plans_ledger(&spec.plans_dir, &[native_plan, mismatch_plan]);
        write_approvals_ledger(&spec.authority_dir, &[native_approval, mismatch_approval]);

        let projection = project_jobs(&spec);
        assert_eq!(projection.jobs.len(), 2);
        assert!(projection.worker_links.is_empty());

        let mismatch_job = &projection.jobs[0];
        assert_eq!(mismatch_job.id, "plan_mismatch");
        assert_eq!(mismatch_job.state, ObservationJobState::Unknown);
        assert_eq!(mismatch_job.execution_status, "unknown");
        assert_eq!(mismatch_job.review_state.as_deref(), Some("unknown"));
        assert_eq!(mismatch_job.tasks[0].status, "unknown");

        let native_job = &projection.jobs[1];
        assert_eq!(native_job.id, "plan_native");
        assert_eq!(native_job.state, ObservationJobState::NeedsUser);
        assert_eq!(native_job.approval_status, "approved");
        assert_eq!(native_job.execution_status, "not_started");
        assert_eq!(native_job.review_state, None);
        assert_eq!(native_job.tasks[0].status, "native_route_required");
    }

    #[test]
    fn malformed_or_unreadable_approvals_ledger_projects_unknown_without_disclosing_paths() {
        let fixture = TempFixture::new("corrupt-approvals-ledger");
        let spec = fixture.make_source_spec("default");

        let task_1 = sample_plan_task("t1", "Published Task 1", WorkflowRole::Coder, vec![]);
        let task_2 = sample_plan_task("t2", "Published Task 2", WorkflowRole::Reviewer, vec!["t1"]);
        let plan = sample_plan_revision(
            "plan_pub",
            "bnd_pub",
            1,
            PlanStatus::Published,
            "Valid published plan",
            vec![task_1, task_2],
            1_700_000_000_000,
        );
        write_plans_ledger(&spec.plans_dir, &[plan]);

        // 1. Malformed approvals ledger with sensitive path and secret token.
        write_private_file(
            &spec.authority_dir.join(APPROVALS_FILE_NAME),
            b"{corrupt approvals /secret/approval/path sk-live-approval-999",
        );

        let proj_malformed = project_jobs(&spec);
        assert_eq!(proj_malformed.jobs.len(), 1);
        assert_eq!(proj_malformed.jobs[0].id, "plan_pub");
        assert_eq!(proj_malformed.jobs[0].state, ObservationJobState::Unknown);
        assert_eq!(proj_malformed.jobs[0].approval_status, "unknown");
        assert_eq!(proj_malformed.jobs[0].execution_status, "unknown");
        assert_eq!(proj_malformed.jobs[0].tasks.len(), 2);
        assert!(proj_malformed.jobs[0]
            .tasks
            .iter()
            .all(|t| t.status == "unknown"));
        assert!(proj_malformed.worker_links.is_empty());
        assert_eq!(
            proj_malformed.warnings,
            vec![WARN_APPROVAL_LEDGER_INVALID.to_string()]
        );
        assert!(!proj_malformed.warnings[0].contains("/secret/approval/path"));
        assert!(!proj_malformed.warnings[0].contains("sk-live-approval"));
        assert!(!proj_malformed.warnings[0].contains(APPROVALS_FILE_NAME));

        // 2. Oversize approvals ledger fails closed as Unknown.
        let oversize = vec![b' '; MAX_AUTHORITY_STORE_BYTES + 16];
        write_private_file(&spec.authority_dir.join(APPROVALS_FILE_NAME), &oversize);

        let proj_oversize = project_jobs(&spec);
        assert_eq!(proj_oversize.jobs.len(), 1);
        assert_eq!(proj_oversize.jobs[0].state, ObservationJobState::Unknown);
        assert_eq!(proj_oversize.jobs[0].approval_status, "unknown");
        assert_eq!(proj_oversize.jobs[0].execution_status, "unknown");
        assert!(proj_oversize.jobs[0]
            .tasks
            .iter()
            .all(|t| t.status == "unknown"));
        assert!(proj_oversize.worker_links.is_empty());
        assert_eq!(
            proj_oversize.warnings,
            vec![WARN_APPROVAL_LEDGER_INVALID.to_string()]
        );

        // 3. Unreadable / symlinked approvals ledger fails closed as Unknown without path disclosure.
        #[cfg(unix)]
        {
            let _ = fs::remove_file(spec.authority_dir.join(APPROVALS_FILE_NAME));
            let target = fixture.path().join("outside-approvals.json");
            write_private_file(&target, b"{\"schema_version\":1,\"approvals\":[]}");
            std::os::unix::fs::symlink(&target, spec.authority_dir.join(APPROVALS_FILE_NAME))
                .expect("create symlink approvals.json");

            let proj_symlink = project_jobs(&spec);
            assert_eq!(proj_symlink.jobs.len(), 1);
            assert_eq!(proj_symlink.jobs[0].state, ObservationJobState::Unknown);
            assert_eq!(proj_symlink.jobs[0].approval_status, "unknown");
            assert_eq!(proj_symlink.jobs[0].execution_status, "unknown");
            assert!(proj_symlink.jobs[0]
                .tasks
                .iter()
                .all(|t| t.status == "unknown"));
            assert!(proj_symlink.worker_links.is_empty());
            assert_eq!(
                proj_symlink.warnings,
                vec![WARN_APPROVAL_LEDGER_INVALID.to_string()]
            );
            assert!(!proj_symlink.warnings[0].contains("outside-approvals"));
        }
    }

    #[test]
    fn tampered_or_conflicting_approval_for_exact_target_projects_unknown_approval_status() {
        let fixture = TempFixture::new("tampered-approval-target");
        let spec = fixture.make_source_spec("default");
        let repo_path = fixture.path().join("repo-tampered");
        create_private_dir(&repo_path);
        let base_commit = "a".repeat(40);
        let thread_uuid = "0196ed00-0000-7000-8000-000000000055";

        // 1. Validly parsed ApprovalRecord whose signature/integrity is tampered for exact target.
        let task_tampered =
            sample_plan_task("t_tamp", "Tampered Task", WorkflowRole::Coder, vec![]);
        let plan_tampered = sample_plan_revision(
            "plan_1_tampered",
            "bnd_1",
            1,
            PlanStatus::Published,
            "Tampered approval plan",
            vec![task_tampered.clone()],
            1_700_000_010_000,
        );
        let mut tampered_approval = build_signed_approval_record(
            &plan_tampered,
            thread_uuid,
            &repo_path,
            &base_commit,
            &[(
                &task_tampered,
                ToolAdapter::Antigravity,
                Some(WorkerProvider::Antigravity),
            )],
            1_700_000_015_000,
        );
        tampered_approval.base_commit = "f".repeat(40);

        // 2. Validly signed ApprovalRecord for exact (plan_id, revision) that mismatches plan metadata.
        let task_mismatch = sample_plan_task("t_mis", "Mismatch Task", WorkflowRole::Coder, vec![]);
        let plan_mismatched = sample_plan_revision(
            "plan_2_mismatched",
            "bnd_1",
            1,
            PlanStatus::Published,
            "Mismatched approval plan",
            vec![task_mismatch.clone()],
            1_700_000_020_000,
        );
        let mut altered_plan_for_signing = plan_mismatched.clone();
        altered_plan_for_signing.profile_revision = 2;
        let mismatched_approval = build_signed_approval_record(
            &altered_plan_for_signing,
            thread_uuid,
            &repo_path,
            &base_commit,
            &[(
                &task_mismatch,
                ToolAdapter::Antigravity,
                Some(WorkerProvider::Antigravity),
            )],
            1_700_000_025_000,
        );

        // 3. Draft plan that has conflicting approval evidence for its exact (plan_id, revision).
        let task_draft = sample_plan_task("t_draft", "Draft Task", WorkflowRole::Coder, vec![]);
        let plan_draft_conflict = sample_plan_revision(
            "plan_3_draft_conflict",
            "bnd_1",
            1,
            PlanStatus::Draft,
            "Draft conflict plan",
            vec![task_draft.clone()],
            1_700_000_030_000,
        );
        let mut draft_as_published = plan_draft_conflict.clone();
        draft_as_published.status = PlanStatus::Published;
        let draft_conflict_approval = build_signed_approval_record(
            &draft_as_published,
            thread_uuid,
            &repo_path,
            &base_commit,
            &[(
                &task_draft,
                ToolAdapter::Antigravity,
                Some(WorkerProvider::Antigravity),
            )],
            1_700_000_035_000,
        );

        // 4. Unrelated published plan with no approval target in the ledger remains Planning / unapproved.
        let task_clean = sample_plan_task("t_clean", "Clean Task", WorkflowRole::Coder, vec![]);
        let plan_clean = sample_plan_revision(
            "plan_4_clean",
            "bnd_1",
            1,
            PlanStatus::Published,
            "Clean unapproved plan",
            vec![task_clean],
            1_700_000_040_000,
        );

        write_plans_ledger(
            &spec.plans_dir,
            &[
                plan_tampered,
                plan_mismatched,
                plan_draft_conflict,
                plan_clean,
            ],
        );
        write_approvals_ledger(
            &spec.authority_dir,
            &[
                tampered_approval,
                mismatched_approval,
                draft_conflict_approval,
            ],
        );

        let projection = project_jobs(&spec);
        assert_eq!(projection.jobs.len(), 4);
        assert!(projection.worker_links.is_empty());
        assert_eq!(
            projection.warnings,
            vec![WARN_APPROVAL_ENTRY_INVALID.to_string()]
        );

        for job in &projection.jobs[0..3] {
            assert_eq!(job.state, ObservationJobState::Unknown);
            assert_eq!(job.approval_status, "unknown");
            assert_eq!(job.execution_status, "unknown");
            assert_eq!(job.tasks.len(), 1);
            assert_eq!(job.tasks[0].status, "unknown");
        }

        let clean_job = &projection.jobs[3];
        assert_eq!(clean_job.id, "plan_4_clean");
        assert_eq!(clean_job.state, ObservationJobState::Planning);
        assert_eq!(clean_job.approval_status, "unapproved");
        assert_eq!(clean_job.execution_status, "not_started");
        assert_eq!(clean_job.tasks[0].status, "not_approved");
    }

    #[test]
    fn approved_session_private_runtime_db_populates_core_states_and_distinct_binding_links() {
        let fixture = TempFixture::new("session-private-core");
        let mut spec = fixture.make_source_spec("default");

        let repo_path = fixture.path().join("repo-private-rt");
        create_private_dir(&repo_path);
        let base_commit = "c".repeat(40);
        let thread_uuid = "0196ed00-0000-7000-8000-000000000044";

        let source_db_path = spec.runtime_state_dirs[0].join(CORE_STATE_DB_FILE_NAME);
        let mut source_core = Core::open(&source_db_path).expect("open source Core DB");
        let control_binding = source_core
            .create_binding(thread_uuid, "Control B1 Thread")
            .expect("create source control binding");
        let control_binding_id = control_binding.binding_id.clone();
        drop(source_core);

        let task_1 = sample_plan_task(
            "t_priv",
            "Session Private Worker",
            WorkflowRole::Coder,
            vec![],
        );
        let plan = sample_plan_revision(
            "plan_priv",
            &control_binding_id,
            1,
            PlanStatus::Published,
            "Private session plan",
            vec![task_1.clone()],
            1_700_000_100_000,
        );
        let approval = build_signed_approval_record(
            &plan,
            thread_uuid,
            &repo_path,
            &base_commit,
            &[(
                &task_1,
                ToolAdapter::Antigravity,
                Some(WorkerProvider::Antigravity),
            )],
            1_700_000_200_000,
        );
        let approved_task = &approval.tasks[0];

        write_plans_ledger(&spec.plans_dir, &[plan]);
        write_approvals_ledger(&spec.authority_dir, std::slice::from_ref(&approval));

        let session_dir = spec.sessions_dir.join(&approval.approval_id);
        let session_runtime_dir = session_dir.join(RUNTIME_STATE_SUBDIR);
        create_private_dir(&session_dir);
        create_private_dir(&session_runtime_dir);

        let session_db_path = session_runtime_dir.join(CORE_STATE_DB_FILE_NAME);
        let mut core = Core::open(&session_db_path).expect("open session-private Core DB");
        let session_binding = core
            .create_binding(thread_uuid, "Session B2 Thread")
            .expect("create session binding");
        let session_binding_id = session_binding.binding_id.clone();
        assert_ne!(session_binding_id, control_binding_id);
        assert_ne!(session_binding_id, approval.binding_id);

        let profile_digest = "d".repeat(64);
        let started = match core
            .execute(
                &session_binding_id,
                Command::StartWorker {
                    command_key: approved_task.command_key.clone().expect("command_key"),
                    profile_id: approved_task.profile_id.clone().expect("profile_id"),
                    profile_digest: profile_digest.clone(),
                    provider: "antigravity".to_string(),
                    requested_model: approved_task.base_model.clone(),
                    requested_effort: approved_task.effort.clone().unwrap_or_default(),
                    base_commit: base_commit.clone(),
                    execution_binding: None,
                    instruction: approved_task.instruction.clone(),
                    now_ms: 1_700_000_300_000,
                },
            )
            .expect("start worker in session-private DB")
        {
            CommandOutput::Worker(w) => w,
            other => panic!("unexpected output: {other:?}"),
        };

        let claim = core
            .claim_worker(
                &session_binding_id,
                &started.run_id,
                started.revision,
                &profile_digest,
                1_700_000_310_000,
            )
            .expect("claim worker");
        core.record_worker_running(
            &claim,
            WorkerWorktreeUpdate {
                run_worktree_id: "wt-run-priv-1".to_string(),
                run_worktree_path: session_dir
                    .join("work-area/runs/wt-run-priv-1")
                    .to_string_lossy()
                    .into_owned(),
                run_branch_name: "tl/run/wt-run-priv-1".to_string(),
                task_worktree_id: "wt-task-priv-1".to_string(),
                task_worktree_path: session_dir
                    .join("work-area/tasks/wt-task-priv-1")
                    .to_string_lossy()
                    .into_owned(),
                task_branch_name: "tl/task/wt-task-priv-1".to_string(),
                base_commit: base_commit.clone(),
            },
            1_700_000_320_000,
        )
        .expect("record worker running");
        drop(core);

        let manifest = HostSessionManifest::from_approval_record(
            &approval,
            Some(session_binding_id.clone()),
            HostPhase::Ready,
            Some(std::process::id()),
            None,
            1_700_000_250_000,
            1_700_000_260_000,
        );
        write_host_manifest(&session_dir, &manifest).expect("write host manifest");

        let intent = TaskDispatchIntent {
            task_id: approved_task.task_id.clone(),
            task_digest: approved_task.task_digest.clone(),
            profile_id: approved_task.profile_id.clone().expect("profile_id"),
            command_key: approved_task.command_key.clone().expect("command_key"),
            execution_binding: None,
            recorded_at_ms: 1_700_000_290_000,
        };
        let receipt = TaskRunReceipt {
            task_id: approved_task.task_id.clone(),
            task_digest: approved_task.task_digest.clone(),
            profile_id: approved_task.profile_id.clone().expect("profile_id"),
            command_key: approved_task.command_key.clone().expect("command_key"),
            run_id: started.run_id.clone(),
            worker_id: started.worker_id.clone(),
            execution_binding: None,
            recorded_at_ms: 1_700_000_305_000,
        };
        write_task_receipts_fixture(
            &session_dir,
            &approval,
            &session_binding_id,
            &[intent],
            &[receipt],
        );

        let _host_lock =
            HostSessionLock::acquire(&session_dir, &approval.approval_id).expect("acquire lock");

        let projection = project_jobs(&spec);
        assert!(projection.warnings.is_empty());
        assert_eq!(projection.core_states.len(), 2);
        assert_eq!(projection.core_states[0].bindings.len(), 1);
        assert_eq!(
            projection.core_states[0].bindings[0].binding_id,
            control_binding_id
        );
        assert_eq!(
            projection.core_states[0].bindings[0].thread_uuid,
            thread_uuid
        );
        assert!(projection.core_states[0].workers.is_empty());

        assert_eq!(projection.core_states[1].bindings.len(), 1);
        assert_eq!(
            projection.core_states[1].bindings[0].binding_id,
            session_binding_id
        );
        assert_eq!(
            projection.core_states[1].bindings[0].thread_uuid,
            thread_uuid
        );
        assert_eq!(projection.core_states[1].workers.len(), 1);
        assert_eq!(
            projection.core_states[1].workers[0].worker_id,
            started.worker_id
        );
        assert_eq!(
            projection.core_states[1].workers[0].binding_id,
            session_binding_id
        );

        assert_eq!(projection.jobs.len(), 1);
        assert_eq!(projection.jobs[0].id, "plan_priv");
        assert_eq!(projection.jobs[0].state, ObservationJobState::Running);
        assert_eq!(projection.jobs[0].approval_status, "approved");
        assert_eq!(projection.jobs[0].execution_status, "running");
        assert_eq!(
            projection.jobs[0].binding_id.as_deref(),
            Some(control_binding_id.as_str())
        );
        assert_eq!(projection.jobs[0].thread_uuid.as_deref(), Some(thread_uuid));
        assert_eq!(
            projection.jobs[0].thread_label.as_deref(),
            Some("Control B1 Thread")
        );

        assert_eq!(projection.worker_links.len(), 1);
        let link = &projection.worker_links[0];
        assert_eq!(link.worker_id, started.worker_id);
        assert_eq!(link.job_id, "plan_priv");
        assert_eq!(link.approval_id, approval.approval_id);
        assert_eq!(link.task_id, "t_priv");
        assert_eq!(link.title, "Session Private Worker");
        assert_eq!(link.control_binding_id, control_binding_id);
        assert_eq!(link.session_binding_id, session_binding_id);
        assert_ne!(link.control_binding_id, link.session_binding_id);
        assert_eq!(link.thread_uuid, thread_uuid);
        assert_eq!(link.role.as_deref(), Some("coder"));
        assert!(link.live);

        // Including the same physical runtime dir in source runtime_state_dirs must deduplicate.
        spec.runtime_state_dirs.push(session_runtime_dir);
        let projection_dedup = project_jobs(&spec);
        assert!(projection_dedup.warnings.is_empty());
        assert_eq!(projection_dedup.core_states.len(), 2);
        assert_eq!(
            projection_dedup.core_states[0].bindings[0].binding_id,
            control_binding_id
        );
        assert_eq!(
            projection_dedup.core_states[1].bindings[0].binding_id,
            session_binding_id
        );
        assert_eq!(projection_dedup.worker_links.len(), 1);
    }
}
