use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path;

use threadlane_core::observation::{
    sanitize_observation_summary, validate_existing_private_dir_readonly, ObservationCall,
    ObservationCallState, ObservationJob, ObservationJobState, ObservationJournalRecord,
    ObservationRecordPhase, ObservationTask, ReadonlyCoreStateSnapshot,
};
use threadlane_native::coworker_observation::is_coworker_workflow_operation;
use threadlane_native::coworker_workflow::{
    CombinedCandidateStatus, ControllerJobRuntimeState, ControllerPolicyStore,
    CoworkerBindingLedger, CoworkerJobLockState, CoworkerJobOwnershipKey,
    CoworkerJobOwnershipStore, CoworkerJobPhase, CoworkerJobRecord, CoworkerTaskRecord,
    CoworkerTaskStage, CoworkerWorkflowStore, CONTROLLER_POLICY_SCHEMA_VERSION,
    COWORKER_WORKFLOW_SCHEMA_VERSION, MAX_COWORKER_LEDGER_BYTES,
};
use threadlane_plans::PlanRevision;

use super::jobs::{
    is_safe_repository_path, push_unique_warning, read_bounded_private_file_readonly,
    resolve_thread_label, sanitize_label_or_fallback, LoadedApprovals,
    FALLBACK_VERIFIED_PROJECT_LABEL,
};
use super::requests::{RequestProjection, WorkerObservationLink};
use super::sources::SourceSpec;

const COWORKER_WORKFLOW_FILE_PREFIX: &str = "coworker-workflow-";
const COWORKER_CONTROLLER_FILE_PREFIX: &str = "coworker-controller-";
const COWORKER_LEDGER_FILE_SUFFIX: &str = ".json";
const MAX_COWORKER_LEDGER_FILES: usize = 128;

const WARN_COWORKER_LEDGER_INVALID: &str =
    "Skipped unreadable or invalid coworker workflow ledger in observation source.";
const WARN_COWORKER_ENTRY_INVALID: &str =
    "Skipped invalid or conflicting coworker job record in observation source.";
const WARN_COWORKER_CONTROLLER_INVALID: &str =
    "Skipped unreadable or invalid coworker controller runtime ledger in observation source.";
const WARN_UNVERIFIED_CORRELATION_HINT: &str =
    "Ignored unverified job, plan, task, or approval hint on journal record.";
const WARN_CONFLICTING_REQUEST_PHASES: &str =
    "Detected conflicting scope fields across journal phases for a request.";

/// Verified scope metadata for a projected [`CoworkerJobRecord`], used to validate and attach
/// `coworker.*` journal calls and deduplicated legacy plan/worker records.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct VerifiedCoworkerJobScope {
    pub(crate) job_id: String,
    pub(crate) binding_id: String,
    pub(crate) thread_uuid: String,
    pub(crate) thread_label: Option<String>,
    pub(crate) project_label: String,
    pub(crate) project_path: Option<String>,
    pub(crate) plan_id: Option<String>,
    pub(crate) plan_revision: Option<u64>,
    pub(crate) approval_id: Option<String>,
    pub(crate) task_ids: BTreeSet<String>,
    pub(crate) allows_live_in_flight_calls: bool,
    pub(crate) has_proven_live_execution: bool,
}

/// Verified metadata extracted from `coworker.*` (and deduplicated plan) journal records prior to
/// [`super::requests::project_requests`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct VerifiedCoworkerRequestMeta {
    pub(crate) is_coworker_op: bool,
    pub(crate) verified_job_id: Option<String>,
    pub(crate) call_id: Option<String>,
    pub(crate) entry_has_live_process_instance: bool,
    pub(crate) has_finish: bool,
}

/// Pre-processed journal records and verified Coworker request attachments for a source.
#[derive(Debug, Clone, Default)]
pub(crate) struct PreparedJournalProjection {
    pub(crate) records: Vec<ObservationJournalRecord>,
    pub(crate) warnings: Vec<String>,
    pub(crate) coworker_requests: BTreeMap<String, VerifiedCoworkerRequestMeta>,
}

/// Enumerates private `coworker-workflow-<tag>.json` ledgers in `spec.plans_dir`, projects
/// truthful [`ObservationJob`] entries for each [`CoworkerJobRecord`], and deduplicates legacy
/// plan-projected jobs only when exact verified identity (`binding_id`, `plan_id`, `plan_revision`,
/// and when approved `approval_id`, `approval_subject_digest`, `thread_uuid`, `repository_path`,
/// `base_commit`) matches.
#[allow(clippy::too_many_arguments)]
pub(super) fn project_coworker_jobs_and_dedupe_plans(
    spec: &SourceSpec,
    latest_plans: &BTreeMap<String, PlanRevision>,
    approvals_state: &LoadedApprovals,
    core_states: &[ReadonlyCoreStateSnapshot],
    legacy_jobs: &mut Vec<ObservationJob>,
    worker_links: &mut [WorkerObservationLink],
    coworker_scopes: &mut BTreeMap<String, VerifiedCoworkerJobScope>,
    warnings: &mut Vec<String>,
) {
    let ledgers = load_verified_coworker_ledgers(&spec.plans_dir, warnings);
    if ledgers.is_empty() {
        return;
    }

    let policy_store = ControllerPolicyStore::new(&spec.plans_dir).ok();
    let mut seen_job_ids = BTreeSet::new();
    let mut conflicted_job_ids = BTreeSet::new();
    let mut projected_coworker_jobs = Vec::new();
    let mut deduped_legacy_plan_ids = BTreeSet::new();

    for ledger in &ledgers {
        for job in &ledger.jobs {
            if !seen_job_ids.insert(job.job_id.clone()) {
                push_unique_warning(warnings, WARN_COWORKER_ENTRY_INVALID);
                conflicted_job_ids.insert(job.job_id.clone());
            }
        }
    }

    for ledger in &ledgers {
        let controller_ledger_corrupt = inspect_controller_ledger_corrupt(
            &spec.plans_dir,
            policy_store.as_ref(),
            &ledger.binding_id,
            warnings,
        );

        for job in &ledger.jobs {
            if conflicted_job_ids.contains(&job.job_id) {
                continue;
            }
            let runtime_state = if controller_ledger_corrupt {
                Err(())
            } else if let Some(ref ps) = policy_store {
                match ps.read_job_runtime(&ledger.binding_id, &job.job_id) {
                    Ok(opt) => Ok(opt),
                    Err(_) => {
                        push_unique_warning(warnings, WARN_COWORKER_CONTROLLER_INVALID);
                        Err(())
                    }
                }
            } else {
                Ok(None)
            };

            let outcome = project_single_coworker_job(
                spec,
                ledger,
                job,
                runtime_state,
                latest_plans,
                approvals_state,
                core_states,
                legacy_jobs,
                worker_links,
                warnings,
            );

            if let Some(plan_id) = outcome.deduped_legacy_plan_id {
                deduped_legacy_plan_ids.insert(plan_id);
            }
            if let Some(scope) = outcome.verified_scope {
                coworker_scopes.insert(scope.job_id.clone(), scope);
            }
            projected_coworker_jobs.push(outcome.job);
        }
    }

    if !deduped_legacy_plan_ids.is_empty() {
        legacy_jobs.retain(|job| !deduped_legacy_plan_ids.contains(&job.id));
    }

    legacy_jobs.extend(projected_coworker_jobs);
}

struct ProjectedCoworkerJobOutcome {
    job: ObservationJob,
    verified_scope: Option<VerifiedCoworkerJobScope>,
    deduped_legacy_plan_id: Option<String>,
}

fn load_verified_coworker_ledgers(
    plans_dir: &Path,
    warnings: &mut Vec<String>,
) -> Vec<CoworkerBindingLedger> {
    match fs::symlink_metadata(plans_dir) {
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Vec::new(),
        Err(_) => {
            push_unique_warning(warnings, WARN_COWORKER_LEDGER_INVALID);
            return Vec::new();
        }
        Ok(_) => {}
    }

    if validate_existing_private_dir_readonly(plans_dir).is_err() {
        push_unique_warning(warnings, WARN_COWORKER_LEDGER_INVALID);
        return Vec::new();
    }

    let Ok(store) = CoworkerWorkflowStore::new(plans_dir) else {
        push_unique_warning(warnings, WARN_COWORKER_LEDGER_INVALID);
        return Vec::new();
    };

    let Ok(entries) = fs::read_dir(plans_dir) else {
        push_unique_warning(warnings, WARN_COWORKER_LEDGER_INVALID);
        return Vec::new();
    };

    let mut candidate_filenames = Vec::new();
    for entry_res in entries {
        let Ok(entry) = entry_res else {
            push_unique_warning(warnings, WARN_COWORKER_LEDGER_INVALID);
            continue;
        };
        let file_name_os = entry.file_name();
        let Some(file_name) = file_name_os.to_str() else {
            continue;
        };
        if file_name.starts_with(COWORKER_WORKFLOW_FILE_PREFIX) {
            if !is_valid_tagged_ledger_filename(file_name, COWORKER_WORKFLOW_FILE_PREFIX) {
                push_unique_warning(warnings, WARN_COWORKER_LEDGER_INVALID);
                continue;
            }
            candidate_filenames.push(file_name.to_string());
        } else if file_name.starts_with(COWORKER_CONTROLLER_FILE_PREFIX)
            && !is_valid_tagged_ledger_filename(file_name, COWORKER_CONTROLLER_FILE_PREFIX)
        {
            push_unique_warning(warnings, WARN_COWORKER_CONTROLLER_INVALID);
        }
    }

    candidate_filenames.sort();
    if candidate_filenames.len() > MAX_COWORKER_LEDGER_FILES {
        push_unique_warning(warnings, WARN_COWORKER_LEDGER_INVALID);
        candidate_filenames.truncate(MAX_COWORKER_LEDGER_FILES);
    }

    let mut ledgers = Vec::new();
    let mut seen_bindings = BTreeSet::new();

    for file_name in candidate_filenames {
        let bytes = match read_bounded_private_file_readonly(
            plans_dir,
            &file_name,
            MAX_COWORKER_LEDGER_BYTES,
        ) {
            Ok(Some(b)) => b,
            Ok(None) => continue,
            Err(()) => {
                push_unique_warning(warnings, WARN_COWORKER_LEDGER_INVALID);
                continue;
            }
        };

        let raw_val: serde_json::Value = match serde_json::from_slice(&bytes) {
            Ok(v) => v,
            Err(_) => {
                push_unique_warning(warnings, WARN_COWORKER_LEDGER_INVALID);
                continue;
            }
        };

        let Some(schema_version) = raw_val.get("schema_version").and_then(|v| v.as_u64()) else {
            push_unique_warning(warnings, WARN_COWORKER_LEDGER_INVALID);
            continue;
        };
        if schema_version != u64::from(COWORKER_WORKFLOW_SCHEMA_VERSION) {
            push_unique_warning(warnings, WARN_COWORKER_LEDGER_INVALID);
            continue;
        }

        let Some(binding_id) = raw_val
            .get("binding_id")
            .and_then(|v| v.as_str())
            .map(str::to_string)
        else {
            push_unique_warning(warnings, WARN_COWORKER_LEDGER_INVALID);
            continue;
        };

        let Ok(expected_path) = store.ledger_path_for_binding(&binding_id) else {
            push_unique_warning(warnings, WARN_COWORKER_LEDGER_INVALID);
            continue;
        };
        if expected_path.file_name().and_then(|n| n.to_str()) != Some(file_name.as_str()) {
            push_unique_warning(warnings, WARN_COWORKER_LEDGER_INVALID);
            continue;
        }

        if !seen_bindings.insert(binding_id.clone()) {
            push_unique_warning(warnings, WARN_COWORKER_LEDGER_INVALID);
            continue;
        }

        match store.read_binding_ledger(&binding_id) {
            Ok(ledger) if ledger.ledger_revision > 0 => {
                ledgers.push(ledger);
            }
            _ => {
                push_unique_warning(warnings, WARN_COWORKER_LEDGER_INVALID);
            }
        }
    }

    ledgers
}

fn is_valid_tagged_ledger_filename(file_name: &str, prefix: &str) -> bool {
    let Some(rest) = file_name.strip_prefix(prefix) else {
        return false;
    };
    let Some(tag) = rest.strip_suffix(COWORKER_LEDGER_FILE_SUFFIX) else {
        return false;
    };
    tag.len() == 32
        && tag
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

fn inspect_controller_ledger_corrupt(
    plans_dir: &Path,
    policy_store: Option<&ControllerPolicyStore>,
    binding_id: &str,
    warnings: &mut Vec<String>,
) -> bool {
    let Some(ps) = policy_store else {
        return false;
    };
    let Ok(expected_path) = ps.ledger_path_for_binding(binding_id) else {
        return false;
    };
    let Some(file_name) = expected_path.file_name().and_then(|n| n.to_str()) else {
        return false;
    };
    match read_bounded_private_file_readonly(plans_dir, file_name, MAX_COWORKER_LEDGER_BYTES) {
        Ok(None) => false,
        Ok(Some(bytes)) => {
            let Ok(raw_val) = serde_json::from_slice::<serde_json::Value>(&bytes) else {
                push_unique_warning(warnings, WARN_COWORKER_CONTROLLER_INVALID);
                return true;
            };
            let schema_ok = raw_val.get("schema_version").and_then(|v| v.as_u64())
                == Some(u64::from(CONTROLLER_POLICY_SCHEMA_VERSION));
            let binding_ok = raw_val.get("binding_id").and_then(|v| v.as_str()) == Some(binding_id);
            if !schema_ok || !binding_ok {
                push_unique_warning(warnings, WARN_COWORKER_CONTROLLER_INVALID);
                return true;
            }
            false
        }
        Err(()) => {
            push_unique_warning(warnings, WARN_COWORKER_CONTROLLER_INVALID);
            true
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn project_single_coworker_job(
    spec: &SourceSpec,
    ledger: &CoworkerBindingLedger,
    job: &CoworkerJobRecord,
    runtime_state_res: Result<Option<ControllerJobRuntimeState>, ()>,
    latest_plans: &BTreeMap<String, PlanRevision>,
    approvals_state: &LoadedApprovals,
    core_states: &[ReadonlyCoreStateSnapshot],
    legacy_jobs: &[ObservationJob],
    worker_links: &mut [WorkerObservationLink],
    warnings: &mut Vec<String>,
) -> ProjectedCoworkerJobOutcome {
    let title = sanitize_label_or_fallback(&job.work_brief.summary, &job.job_id);
    let summary_text = sanitize_observation_summary(&job.work_brief.summary);
    let summary = (!summary_text.is_empty()).then_some(summary_text);

    let repo_path_ref = Path::new(&job.work_brief.project_base.repository_path);
    let repo_safe = is_safe_repository_path(repo_path_ref);
    let (project_label, project_path) = if repo_safe {
        let raw_label = repo_path_ref
            .file_name()
            .and_then(|os| os.to_str())
            .unwrap_or(FALLBACK_VERIFIED_PROJECT_LABEL);
        (
            sanitize_label_or_fallback(raw_label, FALLBACK_VERIFIED_PROJECT_LABEL),
            Some(job.work_brief.project_base.repository_path.clone()),
        )
    } else {
        (FALLBACK_VERIFIED_PROJECT_LABEL.to_string(), None)
    };

    let thread_label = resolve_thread_label(&job.binding_id, &job.thread_uuid, core_states, None);

    if !repo_safe
        || job.binding_id != ledger.binding_id
        || ledger.thread_uuid.as_deref() != Some(job.thread_uuid.as_str())
        || job.validate().is_err()
        || runtime_state_res.is_err()
    {
        push_unique_warning(warnings, WARN_COWORKER_ENTRY_INVALID);
        return unknown_coworker_job_outcome(
            spec,
            job,
            title,
            summary,
            project_label,
            project_path,
            thread_label,
        );
    }

    let runtime_state = runtime_state_res.ok().flatten();
    if let Some(ref rt) = runtime_state {
        let Some(ref linkage) = job.approval_linkage else {
            push_unique_warning(warnings, WARN_COWORKER_CONTROLLER_INVALID);
            return unknown_coworker_job_outcome(
                spec,
                job,
                title,
                summary,
                project_label,
                project_path,
                thread_label,
            );
        };
        if rt.job_id != job.job_id
            || rt.envelope.verify_matches_job(job).is_err()
            || rt.envelope.approval_id != linkage.approval_id
        {
            push_unique_warning(warnings, WARN_COWORKER_CONTROLLER_INVALID);
            return unknown_coworker_job_outcome(
                spec,
                job,
                title,
                summary,
                project_label,
                project_path,
                thread_label,
            );
        }
    }

    let plan_ref = job
        .approval_linkage
        .as_ref()
        .map(|l| (l.plan_id.as_str(), l.plan_revision, l.plan_digest.as_str()))
        .or_else(|| {
            job.planning_result.as_ref().map(|pr| {
                (
                    pr.plan_id.as_str(),
                    pr.plan_revision,
                    pr.plan_digest.as_str(),
                )
            })
        });

    let mut matched_plan: Option<&PlanRevision> = None;
    let mut deduped_legacy_plan_id: Option<String> = None;

    if let Some((plan_id, plan_rev, _plan_digest)) = plan_ref {
        if let Some(latest_plan) = latest_plans.get(plan_id) {
            if latest_plan.binding_id != job.binding_id {
                push_unique_warning(warnings, WARN_COWORKER_ENTRY_INVALID);
                return unknown_coworker_job_outcome(
                    spec,
                    job,
                    title,
                    summary,
                    project_label,
                    project_path,
                    thread_label,
                );
            }
            if latest_plan.revision == plan_rev {
                matched_plan = Some(latest_plan);
            }
        }
    }

    if let Some(ref linkage) = job.approval_linkage {
        if approvals_state.ledger_corrupt {
            return unknown_coworker_job_outcome(
                spec,
                job,
                title,
                summary,
                project_label,
                project_path,
                thread_label,
            );
        }
        let target = (linkage.plan_id.clone(), linkage.plan_revision);
        if approvals_state.corrupt_targets.contains(&target)
            || approvals_state.corrupt_plan_ids.contains(&linkage.plan_id)
        {
            return unknown_coworker_job_outcome(
                spec,
                job,
                title,
                summary,
                project_label,
                project_path,
                thread_label,
            );
        }
        if let Some(candidates) = approvals_state.valid_by_target.get(&target) {
            if candidates.len() != 1 {
                push_unique_warning(warnings, WARN_COWORKER_ENTRY_INVALID);
                return unknown_coworker_job_outcome(
                    spec,
                    job,
                    title,
                    summary,
                    project_label,
                    project_path,
                    thread_label,
                );
            }
            let record = &candidates[0];
            if record.approval_id != linkage.approval_id
                || record.subject_digest != linkage.approval_subject_digest
                || record.binding_id != job.binding_id
                || record.thread_uuid != job.thread_uuid
                || record.repository_path != repo_path_ref
                || record.base_commit != linkage.project_base.base_commit
                || record.profile_id != linkage.profile_id
                || record.registry_revision != linkage.registry_revision
                || record.approved_task_ids != linkage.approved_task_ids
            {
                push_unique_warning(warnings, WARN_COWORKER_ENTRY_INVALID);
                return unknown_coworker_job_outcome(
                    spec,
                    job,
                    title,
                    summary,
                    project_label,
                    project_path,
                    thread_label,
                );
            }
        }
    }

    let mut max_updated_at_ms: Option<i64> = None;
    let mut legacy_active_calls = 0usize;
    let mut legacy_task_titles: BTreeMap<String, String> = BTreeMap::new();
    let mut legacy_live_running_tasks: BTreeSet<String> = BTreeSet::new();

    if let Some(plan) = matched_plan {
        for pt in &plan.tasks {
            legacy_task_titles.insert(pt.id.clone(), sanitize_label_or_fallback(&pt.title, &pt.id));
        }
        max_updated_at_ms = Some(
            max_updated_at_ms
                .unwrap_or(plan.created_at_ms)
                .max(plan.created_at_ms),
        );

        if let Some(legacy_job) = legacy_jobs
            .iter()
            .find(|lj| lj.id == plan.plan_id && lj.binding_id.as_deref() == Some(&job.binding_id))
        {
            let can_dedupe = match &job.approval_linkage {
                Some(_linkage) => legacy_job.approval_status == "approved",
                None => legacy_job.approval_status != "unknown",
            };
            if can_dedupe {
                deduped_legacy_plan_id = Some(plan.plan_id.clone());
                legacy_active_calls = legacy_job.active_call_count;
                if let Some(ts) = legacy_job.updated_at_ms {
                    max_updated_at_ms = Some(max_updated_at_ms.unwrap_or(ts).max(ts));
                }
                for lt in &legacy_job.tasks {
                    if lt.status == "running" {
                        legacy_live_running_tasks.insert(lt.id.clone());
                    }
                }
                if let Some(ref linkage) = job.approval_linkage {
                    for link in worker_links.iter_mut() {
                        if link.job_id == plan.plan_id
                            && link.control_binding_id == job.binding_id
                            && link.approval_id == linkage.approval_id
                        {
                            link.job_id = job.job_id.clone();
                        }
                    }
                }
            }
        }
    }

    update_job_timestamp_from_ledger(ledger, job, &mut max_updated_at_ms);

    let (has_active_job_lock, has_stale_unlocked_job_lock) = inspect_job_ownership_locks(spec, job);

    let task_list = build_observation_tasks(
        job,
        matched_plan,
        &legacy_task_titles,
        &legacy_live_running_tasks,
        has_stale_unlocked_job_lock,
    );

    let (mut state, approval_status, mut execution_status, review_state, allows_live_calls) =
        evaluate_coworker_job_lifecycle(
            job,
            runtime_state.as_ref(),
            has_active_job_lock,
            has_stale_unlocked_job_lock,
            legacy_active_calls > 0,
            warnings,
        );

    if legacy_active_calls > 0 && state == ObservationJobState::Ready {
        state = ObservationJobState::Running;
        execution_status = "running".to_string();
    }

    let mut task_ids: BTreeSet<String> = job.tasks.iter().map(|t| t.task_id.clone()).collect();
    if let Some(plan) = matched_plan {
        for pt in &plan.tasks {
            task_ids.insert(pt.id.clone());
        }
    }

    let plan_id = plan_ref.map(|(pid, _, _)| pid.to_string());
    let plan_revision = plan_ref.map(|(_, rev, _)| rev);
    let approval_id = job.approval_linkage.as_ref().map(|l| l.approval_id.clone());

    let verified_scope = VerifiedCoworkerJobScope {
        job_id: job.job_id.clone(),
        binding_id: job.binding_id.clone(),
        thread_uuid: job.thread_uuid.clone(),
        thread_label: thread_label.clone(),
        project_label: project_label.clone(),
        project_path: project_path.clone(),
        plan_id: plan_id.clone(),
        plan_revision,
        approval_id,
        task_ids,
        allows_live_in_flight_calls: allows_live_calls,
        has_proven_live_execution: has_active_job_lock || legacy_active_calls > 0,
    };

    let obs_job = ObservationJob {
        source_id: spec.source.id.clone(),
        id: job.job_id.clone(),
        project_label,
        project_path,
        title,
        thread_uuid: Some(job.thread_uuid.clone()),
        thread_label,
        binding_id: Some(job.binding_id.clone()),
        plan_id,
        plan_revision,
        state,
        approval_status,
        execution_status,
        review_state,
        updated_at_ms: max_updated_at_ms,
        active_call_count: legacy_active_calls,
        tasks: task_list,
        summary,
    };

    ProjectedCoworkerJobOutcome {
        job: obs_job,
        verified_scope: Some(verified_scope),
        deduped_legacy_plan_id,
    }
}

fn unknown_coworker_job_outcome(
    spec: &SourceSpec,
    job: &CoworkerJobRecord,
    title: String,
    summary: Option<String>,
    project_label: String,
    project_path: Option<String>,
    thread_label: Option<String>,
) -> ProjectedCoworkerJobOutcome {
    let tasks = job
        .tasks
        .iter()
        .map(|t| ObservationTask {
            id: t.task_id.clone(),
            title: t.task_id.clone(),
            status: "unknown".to_string(),
            depends_on: t.depends_on.clone(),
        })
        .collect();
    let plan_id = job
        .approval_linkage
        .as_ref()
        .map(|l| l.plan_id.clone())
        .or_else(|| job.planning_result.as_ref().map(|pr| pr.plan_id.clone()));
    let plan_revision = job
        .approval_linkage
        .as_ref()
        .map(|l| l.plan_revision)
        .or_else(|| job.planning_result.as_ref().map(|pr| pr.plan_revision));

    ProjectedCoworkerJobOutcome {
        job: ObservationJob {
            source_id: spec.source.id.clone(),
            id: job.job_id.clone(),
            project_label,
            project_path,
            title,
            thread_uuid: Some(job.thread_uuid.clone()),
            thread_label,
            binding_id: Some(job.binding_id.clone()),
            plan_id,
            plan_revision,
            state: ObservationJobState::Unknown,
            approval_status: "unknown".to_string(),
            execution_status: "unknown".to_string(),
            review_state: Some("unknown".to_string()),
            updated_at_ms: None,
            active_call_count: 0,
            tasks,
            summary,
        },
        verified_scope: None,
        deduped_legacy_plan_id: None,
    }
}

fn update_job_timestamp_from_ledger(
    ledger: &CoworkerBindingLedger,
    job: &CoworkerJobRecord,
    max_updated_at_ms: &mut Option<i64>,
) {
    let mut bump = |ms_u64: u64| {
        if let Ok(ms) = i64::try_from(ms_u64) {
            *max_updated_at_ms = Some(max_updated_at_ms.unwrap_or(ms).max(ms));
        }
    };

    if let Some(ref pr) = job.planning_result {
        bump(pr.recorded_at_ms);
        for grant in &ledger.planning_grants {
            if grant.grant_id == pr.grant_id {
                bump(grant.created_at_ms);
                for res in &grant.reservations {
                    if res.reservation_id == pr.reservation_id {
                        bump(res.reserved_at_ms);
                        bump(res.updated_at_ms);
                    }
                }
            }
        }
        for advice_id in &pr.architect_advice_ids {
            if let Some(adv) = ledger
                .architect_advice
                .iter()
                .find(|a| &a.advice_id == advice_id)
            {
                bump(adv.created_at_ms);
            }
        }
    } else if matches!(
        job.phase,
        CoworkerJobPhase::Briefing | CoworkerJobPhase::Planning
    ) {
        for grant in &ledger.planning_grants {
            if grant.binding_id == job.binding_id
                && grant.thread_uuid == job.thread_uuid
                && grant.project_base.repository_path == job.work_brief.project_base.repository_path
            {
                bump(grant.created_at_ms);
                for res in &grant.reservations {
                    bump(res.reserved_at_ms);
                    bump(res.updated_at_ms);
                }
            }
        }
    }
}

fn inspect_job_ownership_locks(spec: &SourceSpec, job: &CoworkerJobRecord) -> (bool, bool) {
    let Ok(key) = CoworkerJobOwnershipKey::new(&job.binding_id, &job.thread_uuid, &job.job_id)
    else {
        return (false, false);
    };

    let candidate_dirs = [
        spec.plans_dir.clone(),
        spec.sessions_dir.clone(),
        spec.plans_dir.join("locks"),
        spec.sessions_dir.join("locks"),
    ];

    let mut active = false;
    let mut stale_unlocked = false;

    for dir in &candidate_dirs {
        if validate_existing_private_dir_readonly(dir).is_err() {
            continue;
        }
        let Ok(store) = CoworkerJobOwnershipStore::new(dir) else {
            continue;
        };
        match store.inspect_job_lock(&key) {
            Ok(CoworkerJobLockState::HeldByActiveProcess { .. }) => {
                active = true;
            }
            Ok(CoworkerJobLockState::UnlockedStaleFile { .. }) => {
                stale_unlocked = true;
            }
            Ok(CoworkerJobLockState::Absent) => {}
            Err(_) => {
                stale_unlocked = true;
            }
        }
    }

    (active, stale_unlocked && !active)
}

fn build_observation_tasks(
    job: &CoworkerJobRecord,
    matched_plan: Option<&PlanRevision>,
    legacy_task_titles: &BTreeMap<String, String>,
    legacy_live_running_tasks: &BTreeSet<String>,
    has_stale_unlocked_job_lock: bool,
) -> Vec<ObservationTask> {
    if !job.tasks.is_empty() {
        return job
            .tasks
            .iter()
            .map(|task| {
                let title = legacy_task_titles
                    .get(&task.task_id)
                    .cloned()
                    .unwrap_or_else(|| task.task_id.clone());
                let status = map_coworker_task_status(
                    job.phase,
                    task,
                    legacy_live_running_tasks.contains(&task.task_id),
                    has_stale_unlocked_job_lock,
                );
                ObservationTask {
                    id: task.task_id.clone(),
                    title,
                    status,
                    depends_on: task.depends_on.clone(),
                }
            })
            .collect();
    }

    if let Some(plan) = matched_plan {
        let default_status = match job.phase {
            CoworkerJobPhase::Briefing
            | CoworkerJobPhase::Planning
            | CoworkerJobPhase::AwaitingApproval => "not_approved",
            CoworkerJobPhase::Held => "held",
            CoworkerJobPhase::Cancelled => "cancelled",
            _ => "not_started",
        };
        return plan
            .tasks
            .iter()
            .map(|pt| ObservationTask {
                id: pt.id.clone(),
                title: sanitize_label_or_fallback(&pt.title, &pt.id),
                status: default_status.to_string(),
                depends_on: pt.depends_on.clone(),
            })
            .collect();
    }

    Vec::new()
}

fn map_coworker_task_status(
    job_phase: CoworkerJobPhase,
    task: &CoworkerTaskRecord,
    legacy_live_running: bool,
    has_stale_unlocked_job_lock: bool,
) -> String {
    if task.unresolved_outbox_id.is_some() && has_stale_unlocked_job_lock {
        return "unknown".to_string();
    }
    match task.stage {
        CoworkerTaskStage::PendingDependencies | CoworkerTaskStage::ReadyToDispatch => {
            if legacy_live_running {
                "running".to_string()
            } else {
                "not_started".to_string()
            }
        }
        CoworkerTaskStage::DispatchReserved | CoworkerTaskStage::Running => {
            if has_stale_unlocked_job_lock {
                "unknown".to_string()
            } else {
                "running".to_string()
            }
        }
        CoworkerTaskStage::ReadyForReview => "ready_for_review".to_string(),
        CoworkerTaskStage::ReviewReserved | CoworkerTaskStage::UnderReview => {
            if has_stale_unlocked_job_lock {
                "unknown".to_string()
            } else {
                "ready_for_review".to_string()
            }
        }
        CoworkerTaskStage::CorrectInScopeRequired => "changes_requested".to_string(),
        CoworkerTaskStage::Accepted => "accepted".to_string(),
        CoworkerTaskStage::IntegrationReserved => {
            if has_stale_unlocked_job_lock {
                "unknown".to_string()
            } else {
                "running".to_string()
            }
        }
        CoworkerTaskStage::Integrated => {
            if job_phase == CoworkerJobPhase::Completed {
                "completed".to_string()
            } else {
                "accepted".to_string()
            }
        }
        CoworkerTaskStage::Held => "held".to_string(),
        CoworkerTaskStage::Blocked | CoworkerTaskStage::Failed => "failed".to_string(),
        CoworkerTaskStage::EffectUnknown => "unknown".to_string(),
        CoworkerTaskStage::Cancelled => "cancelled".to_string(),
    }
}

fn evaluate_coworker_job_lifecycle(
    job: &CoworkerJobRecord,
    runtime_state: Option<&ControllerJobRuntimeState>,
    has_active_job_lock: bool,
    has_stale_unlocked_job_lock: bool,
    has_legacy_live_calls: bool,
    warnings: &mut Vec<String>,
) -> (ObservationJobState, String, String, Option<String>, bool) {
    let approval_status = if job.approval_linkage.is_some() {
        "approved".to_string()
    } else if job.phase == CoworkerJobPhase::AwaitingApproval || job.planning_result.is_some() {
        "pending_approval".to_string()
    } else {
        "draft".to_string()
    };

    let any_active_hold = job.holds.iter().any(|h| h.active)
        || job
            .tasks
            .iter()
            .any(|t| t.stage == CoworkerTaskStage::Held || !t.active_hold_ids.is_empty());

    let any_effect_unknown = job
        .tasks
        .iter()
        .any(|t| t.stage == CoworkerTaskStage::EffectUnknown)
        || job.outbox.iter().any(|o| {
            o.status == threadlane_native::coworker_workflow::OutboxDispatchStatus::EffectUnknown
        });

    let has_orphaned_pending_effects =
        runtime_state.is_some_and(|rt| !rt.pending_effects.is_empty()) && !has_active_job_lock;

    let review_state = derive_coworker_review_state(job, runtime_state);

    match job.phase {
        CoworkerJobPhase::Briefing | CoworkerJobPhase::Planning => {
            if any_effect_unknown || has_stale_unlocked_job_lock {
                return (
                    ObservationJobState::Unknown,
                    approval_status,
                    "unknown".to_string(),
                    review_state,
                    false,
                );
            }
            if any_active_hold {
                return (
                    ObservationJobState::NeedsUser,
                    approval_status,
                    "held".to_string(),
                    review_state,
                    false,
                );
            }
            (
                ObservationJobState::Planning,
                approval_status,
                "not_started".to_string(),
                review_state,
                true,
            )
        }
        CoworkerJobPhase::AwaitingApproval => (
            ObservationJobState::NeedsUser,
            "pending_approval".to_string(),
            "not_started".to_string(),
            review_state,
            false,
        ),
        CoworkerJobPhase::Held => {
            if any_effect_unknown {
                (
                    ObservationJobState::Unknown,
                    approval_status,
                    "unknown".to_string(),
                    Some("unknown".to_string()),
                    false,
                )
            } else {
                (
                    ObservationJobState::NeedsUser,
                    approval_status,
                    "held".to_string(),
                    review_state,
                    false,
                )
            }
        }
        CoworkerJobPhase::Cancelled => (
            ObservationJobState::Failed,
            approval_status,
            "stopped".to_string(),
            review_state,
            false,
        ),
        CoworkerJobPhase::Completed => {
            if verify_job_closure_completion_proof(job, runtime_state) {
                (
                    ObservationJobState::Completed,
                    "approved".to_string(),
                    "completed".to_string(),
                    Some("review_passed".to_string()),
                    false,
                )
            } else {
                push_unique_warning(warnings, WARN_COWORKER_ENTRY_INVALID);
                (
                    ObservationJobState::Unknown,
                    "unknown".to_string(),
                    "unknown".to_string(),
                    Some("unknown".to_string()),
                    false,
                )
            }
        }
        CoworkerJobPhase::AssemblingCandidate => {
            if any_effect_unknown || has_stale_unlocked_job_lock || has_orphaned_pending_effects {
                return (
                    ObservationJobState::Unknown,
                    approval_status,
                    "unknown".to_string(),
                    Some("unknown".to_string()),
                    false,
                );
            }
            if any_active_hold {
                return (
                    ObservationJobState::NeedsUser,
                    approval_status,
                    "held".to_string(),
                    review_state,
                    false,
                );
            }
            (
                ObservationJobState::Running,
                approval_status,
                "running".to_string(),
                review_state,
                true,
            )
        }
        CoworkerJobPhase::ClosureReview => {
            if any_effect_unknown || has_stale_unlocked_job_lock || has_orphaned_pending_effects {
                return (
                    ObservationJobState::Unknown,
                    approval_status,
                    "unknown".to_string(),
                    Some("unknown".to_string()),
                    false,
                );
            }
            if any_active_hold {
                return (
                    ObservationJobState::NeedsUser,
                    approval_status,
                    "held".to_string(),
                    review_state,
                    false,
                );
            }
            if job
                .combined_candidates
                .iter()
                .any(|c| c.status == CombinedCandidateStatus::Rejected)
            {
                return (
                    ObservationJobState::Failed,
                    approval_status,
                    "failed".to_string(),
                    Some("changes_requested".to_string()),
                    false,
                );
            }
            (
                ObservationJobState::Reviewing,
                approval_status,
                "ready_for_review".to_string(),
                Some(review_state.unwrap_or_else(|| "pending_review".to_string())),
                true,
            )
        }
        CoworkerJobPhase::Executing => {
            if any_effect_unknown || has_stale_unlocked_job_lock || has_orphaned_pending_effects {
                return (
                    ObservationJobState::Unknown,
                    approval_status,
                    "unknown".to_string(),
                    Some("unknown".to_string()),
                    false,
                );
            }
            if runtime_state.is_some_and(|rt| rt.stop_requested) {
                return (
                    ObservationJobState::Failed,
                    approval_status,
                    "stopped".to_string(),
                    review_state,
                    false,
                );
            }

            let any_failed = job.tasks.iter().any(|t| {
                matches!(
                    t.stage,
                    CoworkerTaskStage::Failed | CoworkerTaskStage::Blocked
                )
            });
            if any_failed {
                return (
                    ObservationJobState::Failed,
                    approval_status,
                    "failed".to_string(),
                    review_state,
                    false,
                );
            }

            let any_running = has_legacy_live_calls
                || job.tasks.iter().any(|t| {
                    matches!(
                        t.stage,
                        CoworkerTaskStage::DispatchReserved
                            | CoworkerTaskStage::Running
                            | CoworkerTaskStage::IntegrationReserved
                    )
                });
            if any_running {
                return (
                    ObservationJobState::Running,
                    approval_status,
                    "running".to_string(),
                    review_state,
                    true,
                );
            }

            let any_reviewing = job.tasks.iter().any(|t| {
                matches!(
                    t.stage,
                    CoworkerTaskStage::ReadyForReview
                        | CoworkerTaskStage::ReviewReserved
                        | CoworkerTaskStage::UnderReview
                        | CoworkerTaskStage::CorrectInScopeRequired
                        | CoworkerTaskStage::Accepted
                )
            });
            if any_reviewing {
                return (
                    ObservationJobState::Reviewing,
                    approval_status,
                    "ready_for_review".to_string(),
                    Some(review_state.unwrap_or_else(|| "pending_review".to_string())),
                    true,
                );
            }

            if any_active_hold {
                return (
                    ObservationJobState::NeedsUser,
                    approval_status,
                    "held".to_string(),
                    review_state,
                    false,
                );
            }

            if !job.tasks.is_empty()
                && job
                    .tasks
                    .iter()
                    .all(|t| t.stage == CoworkerTaskStage::Integrated)
            {
                // All tasks integrated is NOT job completion without Closure acceptance.
                return (
                    ObservationJobState::Reviewing,
                    approval_status,
                    "ready_for_review".to_string(),
                    Some("pending_review".to_string()),
                    true,
                );
            }

            (
                ObservationJobState::Ready,
                approval_status,
                "not_started".to_string(),
                review_state,
                true,
            )
        }
    }
}

fn derive_coworker_review_state(
    job: &CoworkerJobRecord,
    runtime_state: Option<&ControllerJobRuntimeState>,
) -> Option<String> {
    if job.phase == CoworkerJobPhase::Completed
        && verify_job_closure_completion_proof(job, runtime_state)
    {
        return Some("review_passed".to_string());
    }
    if job
        .tasks
        .iter()
        .any(|t| t.stage == CoworkerTaskStage::CorrectInScopeRequired)
        || job
            .combined_candidates
            .iter()
            .any(|c| c.status == CombinedCandidateStatus::Rejected)
    {
        return Some("changes_requested".to_string());
    }
    if job.phase == CoworkerJobPhase::ClosureReview
        || job.tasks.iter().any(|t| {
            matches!(
                t.stage,
                CoworkerTaskStage::ReadyForReview
                    | CoworkerTaskStage::ReviewReserved
                    | CoworkerTaskStage::UnderReview
            )
        })
    {
        return Some("pending_review".to_string());
    }
    if !job.tasks.is_empty()
        && job.tasks.iter().all(|t| {
            matches!(
                t.stage,
                CoworkerTaskStage::Accepted | CoworkerTaskStage::Integrated
            )
        })
    {
        return Some("pending_review".to_string());
    }
    None
}

fn verify_job_closure_completion_proof(
    job: &CoworkerJobRecord,
    runtime_state: Option<&ControllerJobRuntimeState>,
) -> bool {
    if job.phase != CoworkerJobPhase::Completed
        || job.approval_linkage.is_none()
        || job.tasks.is_empty()
        || !job
            .tasks
            .iter()
            .all(|t| t.stage == CoworkerTaskStage::Integrated && t.accepted_candidate_id.is_some())
    {
        return false;
    }

    let Some(accepted_candidate) = job.combined_candidates.iter().find(|c| {
        c.status == CombinedCandidateStatus::ClosureAccepted
            && c.closure_reservation_id.is_some()
            && c.validate().is_ok()
    }) else {
        return false;
    };

    if let Some(rt) = runtime_state {
        if rt.closure_verdict_id.is_none() {
            return false;
        }
        let Some(ref attempt_id) = rt.closure_attempt_id else {
            return false;
        };
        let Some(closure_art) = rt.closure_artifacts.get(attempt_id) else {
            return false;
        };
        if closure_art.fenced_from_promotion
            || closure_art.candidate_tree_sha != accepted_candidate.final_tree_sha
        {
            return false;
        }
    }

    true
}

/// Validates `coworker.*` and deduplicated plan-result journal records against
/// `coworker_scopes` before [`super::requests::project_requests`] runs.
pub(crate) fn prepare_journal_records_for_projection(
    journal_records: &[ObservationJournalRecord],
    coworker_scopes: &BTreeMap<String, VerifiedCoworkerJobScope>,
) -> PreparedJournalProjection {
    if coworker_scopes.is_empty()
        && !journal_records
            .iter()
            .any(|r| is_coworker_workflow_operation(&r.operation))
    {
        return PreparedJournalProjection {
            records: journal_records.to_vec(),
            warnings: Vec::new(),
            coworker_requests: BTreeMap::new(),
        };
    }

    let mut warnings = Vec::new();
    let mut coworker_by_req: BTreeMap<String, Vec<&ObservationJournalRecord>> = BTreeMap::new();
    for rec in journal_records {
        if is_coworker_workflow_operation(&rec.operation) && !rec.request_id.trim().is_empty() {
            coworker_by_req
                .entry(rec.request_id.trim().to_string())
                .or_default()
                .push(rec);
        }
    }

    let mut coworker_requests = BTreeMap::new();
    for (req_id, group) in coworker_by_req {
        let meta = evaluate_coworker_request_group(&group, coworker_scopes, &mut warnings);
        coworker_requests.insert(req_id, meta);
    }

    let mut prepared_records = Vec::with_capacity(journal_records.len());
    for rec in journal_records {
        let req_id = rec.request_id.trim();
        if is_coworker_workflow_operation(&rec.operation) {
            let mut clean = rec.clone();
            clean.call_id = None;
            clean.job_id = None;
            clean.plan_id = None;
            clean.task_id = None;
            clean.approval_id = None;
            prepared_records.push(clean);
            continue;
        }

        let mut adjusted = rec.clone();
        if let Some(raw_job_id) = rec.job_id.as_deref() {
            if !coworker_scopes.contains_key(raw_job_id) {
                if let Some(scope) = coworker_scopes.values().find(|s| {
                    s.plan_id.as_deref() == Some(raw_job_id)
                        && rec.binding_id.as_deref() == Some(s.binding_id.as_str())
                        && rec.thread_uuid.as_deref() == Some(s.thread_uuid.as_str())
                        && (rec.approval_id.is_none() || rec.approval_id == s.approval_id)
                }) {
                    adjusted.job_id = Some(scope.job_id.clone());
                }
            }
        } else if is_plan_result_operation(&rec.operation) {
            if let Some(raw_plan_id) = rec.plan_id.as_deref() {
                if let Some(scope) = coworker_scopes.values().find(|s| {
                    s.plan_id.as_deref() == Some(raw_plan_id)
                        && rec.binding_id.as_deref() == Some(s.binding_id.as_str())
                        && rec
                            .thread_uuid
                            .as_deref()
                            .is_none_or(|tu| tu == s.thread_uuid.as_str())
                }) {
                    coworker_requests.entry(req_id.to_string()).or_insert(
                        VerifiedCoworkerRequestMeta {
                            is_coworker_op: false,
                            verified_job_id: Some(scope.job_id.clone()),
                            call_id: None,
                            entry_has_live_process_instance: true,
                            has_finish: true,
                        },
                    );
                    adjusted.plan_id = None;
                    adjusted.job_id = None;
                }
            }
        }

        prepared_records.push(adjusted);
    }

    PreparedJournalProjection {
        records: prepared_records,
        warnings,
        coworker_requests,
    }
}

fn is_plan_result_operation(op: &str) -> bool {
    matches!(
        op.trim(),
        "workflow.save-draft"
            | "workflow.publish"
            | "workflow.get-plan"
            | "context.workflow.propose"
            | "context.workflow.save-draft"
            | "context.workflow.publish"
            | "context.workflow.get-plan"
            | "mcp.save_draft_plan"
            | "mcp.publish_plan"
            | "mcp.get_plan"
    )
}

fn evaluate_coworker_request_group(
    group: &[&ObservationJournalRecord],
    coworker_scopes: &BTreeMap<String, VerifiedCoworkerJobScope>,
    warnings: &mut Vec<String>,
) -> VerifiedCoworkerRequestMeta {
    let first = group[0];
    let mut call_id = first.call_id.clone();
    let mut binding_id = first.binding_id.clone();
    let mut thread_uuid = first.thread_uuid.clone();
    let mut job_id = first.job_id.clone();
    let mut plan_id = first.plan_id.clone();
    let mut task_id = first.task_id.clone();
    let mut approval_id = first.approval_id.clone();
    let mut project_path = first.project_path.clone();
    let mut has_conflict = false;
    let mut entry_has_live_process_instance = false;
    let mut has_finish = false;

    for rec in group {
        if rec.operation != first.operation {
            has_conflict = true;
        }
        if !merge_optional_scope_field(&mut call_id, &rec.call_id)
            || !merge_optional_scope_field(&mut binding_id, &rec.binding_id)
            || !merge_optional_scope_field(&mut thread_uuid, &rec.thread_uuid)
            || !merge_optional_scope_field(&mut job_id, &rec.job_id)
            || !merge_optional_scope_field(&mut plan_id, &rec.plan_id)
            || !merge_optional_scope_field(&mut task_id, &rec.task_id)
            || !merge_optional_scope_field(&mut approval_id, &rec.approval_id)
            || !merge_optional_scope_field(&mut project_path, &rec.project_path)
        {
            has_conflict = true;
        }
        if rec.phase == ObservationRecordPhase::Entry && rec.has_live_process_instance() {
            entry_has_live_process_instance = true;
        }
        if rec.phase == ObservationRecordPhase::Finish {
            has_finish = true;
        }
    }

    if has_conflict {
        push_unique_warning(warnings, WARN_CONFLICTING_REQUEST_PHASES);
        return VerifiedCoworkerRequestMeta {
            is_coworker_op: true,
            verified_job_id: None,
            call_id,
            entry_has_live_process_instance,
            has_finish,
        };
    }

    let matched_scope = if let Some(ref jid) = job_id {
        coworker_scopes.get(jid)
    } else if let (Some(ref bid), Some(ref tu)) = (&binding_id, &thread_uuid) {
        let candidates: Vec<&VerifiedCoworkerJobScope> = coworker_scopes
            .values()
            .filter(|s| {
                &s.binding_id == bid
                    && &s.thread_uuid == tu
                    && (plan_id.is_none() || s.plan_id == plan_id)
            })
            .collect();
        if candidates.len() == 1 {
            Some(candidates[0])
        } else {
            None
        }
    } else {
        None
    };

    let verified_job_id = match matched_scope {
        Some(scope)
            if binding_id.as_deref() == Some(scope.binding_id.as_str())
                && thread_uuid.as_deref() == Some(scope.thread_uuid.as_str())
                && plan_id
                    .as_deref()
                    .is_none_or(|pid| scope.plan_id.as_deref() == Some(pid))
                && approval_id
                    .as_deref()
                    .is_none_or(|aid| scope.approval_id.as_deref() == Some(aid))
                && task_id
                    .as_deref()
                    .is_none_or(|tid| scope.task_ids.contains(tid))
                && project_path
                    .as_deref()
                    .is_none_or(|pp| scope.project_path.as_deref() == Some(pp)) =>
        {
            Some(scope.job_id.clone())
        }
        _ => {
            if job_id.is_some() || plan_id.is_some() || task_id.is_some() || approval_id.is_some() {
                push_unique_warning(warnings, WARN_UNVERIFIED_CORRELATION_HINT);
            }
            None
        }
    };

    VerifiedCoworkerRequestMeta {
        is_coworker_op: true,
        verified_job_id,
        call_id,
        entry_has_live_process_instance,
        has_finish,
    }
}

fn merge_optional_scope_field(slot: &mut Option<String>, incoming: &Option<String>) -> bool {
    match (&*slot, incoming) {
        (None, Some(v)) => {
            *slot = Some(v.clone());
            true
        }
        (Some(existing), Some(v)) => existing == v,
        _ => true,
    }
}

/// Attaches verified `job_id`, child [`ObservationCall`] entries, live process-instance + stage
/// checks, and stable timestamps to projected Coworker requests and jobs.
pub(crate) fn finalize_projected_coworker_requests(
    jobs: &mut [ObservationJob],
    request_projection: &mut RequestProjection,
    coworker_scopes: &BTreeMap<String, VerifiedCoworkerJobScope>,
    prepared: &PreparedJournalProjection,
) {
    let mut active_calls_by_job: BTreeMap<String, usize> = BTreeMap::new();
    let mut dead_in_flight_by_job: BTreeSet<String> = BTreeSet::new();
    let mut max_ts_by_job: BTreeMap<String, i64> = BTreeMap::new();

    for req in &mut request_projection.requests {
        if let Some(meta) = prepared.coworker_requests.get(&req.id) {
            req.job_id = meta.verified_job_id.clone();
            if let Some(ref jid) = meta.verified_job_id {
                if let Some(scope) = coworker_scopes.get(jid) {
                    req.project_label = scope.project_label.clone();
                    if req.thread_label.is_none() {
                        req.thread_label = scope.thread_label.clone();
                    }
                }
            }

            if meta.is_coworker_op {
                if !meta.has_finish
                    && (req.state.is_active() || req.state == ObservationCallState::Unknown)
                {
                    let stage_allows_live = meta
                        .verified_job_id
                        .as_ref()
                        .and_then(|jid| coworker_scopes.get(jid))
                        .is_some_and(|s| s.allows_live_in_flight_calls);
                    if !meta.entry_has_live_process_instance
                        || !stage_allows_live
                        || req.state == ObservationCallState::Unknown
                    {
                        req.state = ObservationCallState::Unknown;
                        if let Some(ref jid) = meta.verified_job_id {
                            dead_in_flight_by_job.insert(jid.clone());
                        }
                    }
                }

                if let Some(ref call_id) = meta.call_id {
                    let call = ObservationCall {
                        id: call_id.clone(),
                        title: req.title.clone(),
                        state: req.state,
                        executor: req.executor.clone(),
                        started_at_ms: req.started_at_ms,
                        finished_at_ms: req.finished_at_ms,
                        duration_ms: req.duration_ms,
                        usage: req.usage.clone(),
                        summary: req.summary.clone(),
                    };
                    req.children = vec![call];
                    req.children_count = 1;
                }
            }
        }

        if let Some(ref jid) = req.job_id {
            if coworker_scopes.contains_key(jid) {
                if req.state.is_active() {
                    *active_calls_by_job.entry(jid.clone()).or_insert(0) += 1;
                }
                let entry = max_ts_by_job
                    .entry(jid.clone())
                    .or_insert(req.updated_at_ms);
                *entry = (*entry).max(req.updated_at_ms);
            }
        }
    }

    for job in jobs.iter_mut() {
        let Some(scope) = coworker_scopes.get(&job.id) else {
            continue;
        };
        let active_count = active_calls_by_job.get(&job.id).copied().unwrap_or(0);
        job.active_call_count = active_count;
        let has_live_execution_proof = active_count > 0 || scope.has_proven_live_execution;
        if (active_count > 0
            || (scope.has_proven_live_execution && dead_in_flight_by_job.contains(&job.id)))
            && matches!(
                job.state,
                ObservationJobState::Ready | ObservationJobState::Reviewing
            )
        {
            job.state = ObservationJobState::Running;
            job.execution_status = "running".to_string();
        }

        if dead_in_flight_by_job.contains(&job.id)
            && !has_live_execution_proof
            && matches!(
                job.state,
                ObservationJobState::Planning
                    | ObservationJobState::Ready
                    | ObservationJobState::Running
                    | ObservationJobState::Reviewing
            )
        {
            job.state = ObservationJobState::Unknown;
            job.execution_status = "unknown".to_string();
        }

        if let Some(&max_req_ts) = max_ts_by_job.get(&job.id) {
            job.updated_at_ms = Some(job.updated_at_ms.unwrap_or(max_req_ts).max(max_req_ts));
        }
    }
}
