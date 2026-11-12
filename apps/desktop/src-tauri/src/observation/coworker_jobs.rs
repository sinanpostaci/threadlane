use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path;

use threadlane_core::observation::{
    sanitize_observation_summary, validate_existing_private_dir_readonly, ObservationCall,
    ObservationCallState, ObservationJob, ObservationJobState, ObservationJournalRecord,
    ObservationRecordPhase, ObservationTask, ReadonlyCoreStateSnapshot, WorkspaceAcceptanceStatus,
    WorkspaceChatIdentity,
};
use threadlane_native::coworker_observation::{
    is_coworker_workflow_operation, read_scoped_workspace_job_histories, WorkspaceHistoryStores,
    WorkspaceScopedHistoryRead,
};
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
                match ps.get_job_runtime_readonly(&ledger.binding_id, &job.job_id) {
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

pub(super) fn load_verified_coworker_ledgers(
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

pub(super) fn inspect_controller_ledger_corrupt(
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
    let job_completion_verified = job.phase == CoworkerJobPhase::Completed
        && verify_job_closure_completion_proof(spec, job, runtime_state.as_ref());

    let task_list = build_observation_tasks(
        job,
        matched_plan,
        &legacy_task_titles,
        &legacy_live_running_tasks,
        has_stale_unlocked_job_lock,
        job_completion_verified,
    );

    let (mut state, approval_status, mut execution_status, review_state, allows_live_calls) =
        evaluate_coworker_job_lifecycle(
            job,
            runtime_state.as_ref(),
            has_active_job_lock,
            has_stale_unlocked_job_lock,
            legacy_active_calls > 0,
            job_completion_verified,
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
    job_completion_verified: bool,
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
                    job_completion_verified,
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
    job_completion_verified: bool,
) -> String {
    if job_phase == CoworkerJobPhase::Completed && !job_completion_verified {
        return "unknown".to_string();
    }
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
            if job_phase == CoworkerJobPhase::Completed && job_completion_verified {
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
    job_completion_verified: bool,
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

    let review_state = derive_coworker_review_state(job, job_completion_verified);

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
            if job_completion_verified {
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
    job_completion_verified: bool,
) -> Option<String> {
    if job.phase == CoworkerJobPhase::Completed {
        return if job_completion_verified {
            Some("review_passed".to_string())
        } else {
            Some("unknown".to_string())
        };
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
    spec: &SourceSpec,
    job: &CoworkerJobRecord,
    runtime_state: Option<&ControllerJobRuntimeState>,
) -> bool {
    let Some(rt) = runtime_state else {
        return false;
    };
    if !has_structural_closure_completion_preconditions(job, rt) {
        return false;
    }

    let Ok(chat_identity) = WorkspaceChatIdentity::new(
        &spec.source.id,
        &job.binding_id,
        &job.thread_uuid,
        &job.work_brief.project_base.repository_path,
    ) else {
        return false;
    };

    let stores = WorkspaceHistoryStores::new(&spec.plans_dir)
        .with_authority_dir(&spec.authority_dir)
        .with_sessions_dir(&spec.sessions_dir);
    let Ok(scoped_read) = read_scoped_workspace_job_histories(&chat_identity, &stores) else {
        return false;
    };

    has_verified_scoped_job_completion_proof(&chat_identity, job, Some(rt), &scoped_read)
}

fn has_structural_closure_completion_preconditions(
    job: &CoworkerJobRecord,
    rt: &ControllerJobRuntimeState,
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

    if rt.closure_verdict_id.is_none() {
        return false;
    }
    let Some(ref attempt_id) = rt.closure_attempt_id else {
        return false;
    };
    let Some(closure_art) = rt.closure_artifacts.get(attempt_id) else {
        return false;
    };
    !closure_art.fenced_from_promotion
        && closure_art.candidate_tree_sha == accepted_candidate.final_tree_sha
}

fn has_verified_scoped_job_completion_proof(
    chat_identity: &WorkspaceChatIdentity,
    job: &CoworkerJobRecord,
    runtime_state: Option<&ControllerJobRuntimeState>,
    scoped_read: &WorkspaceScopedHistoryRead,
) -> bool {
    let Some(rt) = runtime_state else {
        return false;
    };
    if !has_structural_closure_completion_preconditions(job, rt) {
        return false;
    }
    if scoped_read.degraded || &scoped_read.chat_identity != chat_identity {
        return false;
    }

    let Some(accepted_candidate) = job.combined_candidates.iter().find(|c| {
        c.status == CombinedCandidateStatus::ClosureAccepted
            && c.closure_reservation_id.is_some()
            && c.validate().is_ok()
    }) else {
        return false;
    };

    let Some(projected_job) = scoped_read
        .jobs
        .iter()
        .find(|entry| entry.job.job_id == job.job_id)
    else {
        return false;
    };

    if projected_job.job.chat_identity.as_ref() != Some(chat_identity)
        || projected_job.job.state != ObservationJobState::Completed
        || projected_job.tasks.is_empty()
        || projected_job.truncation.tasks_omitted > 0
    {
        return false;
    }

    let job_proof = projected_job.acceptance_proof.normalized_for_job();
    if job_proof.status != WorkspaceAcceptanceStatus::VerifiedAccepted
        || job_proof.combined_candidate_id.as_deref()
            != Some(accepted_candidate.combined_candidate_id.as_str())
        || job_proof.closure_verdict_id.as_deref() != rt.closure_verdict_id.as_deref()
    {
        return false;
    }
    if let Some(ref proof_res_id) = job_proof.closure_reservation_id {
        if Some(proof_res_id.as_str()) != accepted_candidate.closure_reservation_id.as_deref() {
            return false;
        }
    }

    job.tasks.iter().all(|raw_task| {
        projected_job
            .tasks
            .iter()
            .find(|pt| pt.task_id == raw_task.task_id)
            .is_some_and(|pt| {
                let task_proof = pt.acceptance_proof.normalized_for_task();
                task_proof.status == WorkspaceAcceptanceStatus::VerifiedAccepted
                    && task_proof.accepted_candidate_id.as_deref()
                        == raw_task.accepted_candidate_id.as_deref()
            })
    })
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

#[cfg(all(test, unix))]
mod completion_proof_tests {
    use super::*;
    use std::fs::DirBuilder;
    use std::os::unix::fs::DirBuilderExt;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};
    use threadlane_core::observation::{
        ObservationSource, ObservationSourceAvailability, WorkspaceAcceptanceProof,
        WorkspaceJobHistory, WorkspaceJobHistoryTruncation, WorkspaceJobSummary,
        WorkspaceRecordLinks, WorkspaceTaskHistory,
    };
    use threadlane_native::coworker_workflow::{
        AcceptedPredecessorReceiptRef, CandidateCheckEvidence, CanonicalProjectBaseRef,
        CombinedCandidateIdentity, FrozenApprovalLinkage, WorkBrief, WorkBriefKind,
    };
    use threadlane_native::profile_flow::{RoleExecutionRoute, SelectedRoleChoice};
    use threadlane_plans::{Difficulty, WorkflowRole};

    static PROOF_FIXTURE_COUNTER: AtomicU64 = AtomicU64::new(1);

    struct ProofFixtureDir {
        root: PathBuf,
    }

    impl ProofFixtureDir {
        fn new(label: &str) -> Self {
            let base =
                fs::canonicalize(std::env::temp_dir()).unwrap_or_else(|_| std::env::temp_dir());
            let pid = std::process::id();
            for attempt in 0..1024u32 {
                let seq = PROOF_FIXTURE_COUNTER.fetch_add(1, Ordering::Relaxed);
                let nanos = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map(|d| d.as_nanos())
                    .unwrap_or(0);
                let candidate =
                    base.join(format!("tl-cw-proof-{label}-{pid}-{seq}-{nanos}-{attempt}"));
                match DirBuilder::new().mode(0o700).create(&candidate) {
                    Ok(()) => {
                        let root = fs::canonicalize(&candidate).unwrap_or(candidate);
                        return Self { root };
                    }
                    Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => continue,
                    Err(err) => panic!("failed to create proof fixture dir: {err}"),
                }
            }
            panic!("exhausted proof fixture retries");
        }

        fn subdir(&self, name: &str) -> PathBuf {
            let dir = self.root.join(name);
            DirBuilder::new()
                .mode(0o700)
                .create(&dir)
                .expect("create private 0700 subdir");
            dir
        }
    }

    impl Drop for ProofFixtureDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.root);
        }
    }

    fn sample_completed_coworker_job(
        repo_path: &Path,
        binding_id: &str,
        thread_uuid: &str,
    ) -> (CoworkerJobRecord, String, String, String) {
        let project_base = CanonicalProjectBaseRef {
            repository_path: repo_path.to_string_lossy().into_owned(),
            base_commit: "a".repeat(40),
            base_tree_sha: "b".repeat(40),
        };
        let work_brief = WorkBrief {
            brief_id: "brf_00000000000000000000000000000001".to_string(),
            binding_id: binding_id.to_string(),
            thread_uuid: thread_uuid.to_string(),
            project_base: project_base.clone(),
            kind: WorkBriefKind::NewJob,
            summary: "Implement scoped workspace snapshot".to_string(),
            confirmed_requirements: vec!["Preserve read-only invariant".to_string()],
            proposed_additions: Vec::new(),
            assumptions: Vec::new(),
            open_questions: Vec::new(),
            accepted_architect_decisions: Vec::new(),
        };
        let work_brief_digest = work_brief.digest().expect("brief digest");

        let cand_id = "cnd_00000000000000000000000000000001".to_string();
        let combined_id = "cmb_00000000000000000000000000000001".to_string();
        let closure_res_id = "cls_00000000000000000000000000000001".to_string();
        let final_tree_sha = "e".repeat(40);

        let mut combined = CombinedCandidateIdentity {
            combined_candidate_id: combined_id.clone(),
            request_key: "req_cmb_1".to_string(),
            job_id: "job_00000000000000000000000000000001".to_string(),
            job_revision: 4,
            project_base: project_base.clone(),
            final_commit_sha: Some("f".repeat(40)),
            final_tree_sha: final_tree_sha.clone(),
            plan_id: "plan_1".to_string(),
            plan_revision: 1,
            plan_digest: "1".repeat(64),
            approval_id: "apr_00000000000000000000000000000001".to_string(),
            approval_subject_digest: "2".repeat(64),
            scope_policy_revision: 1,
            policy_id: "pol_1".to_string(),
            policy_digest: "3".repeat(64),
            accepted_predecessor_receipts: vec![AcceptedPredecessorReceiptRef {
                task_id: "task_1".to_string(),
                candidate_id: cand_id.clone(),
                head_commit: "c".repeat(40),
                tree_sha: "d".repeat(40),
                review_subject_digest: "4".repeat(64),
                review_receipt_id: "rev_00000000000000000000000000000001".to_string(),
                integration_receipt_id: "int_00000000000000000000000000000001".to_string(),
            }],
            check_evidence: vec![CandidateCheckEvidence {
                check_id: "cargo_test".to_string(),
                command_summary: "cargo test".to_string(),
                passed: true,
                exit_code: 0,
                evidence_digest: "5".repeat(64),
            }],
            candidate_digest: String::new(),
            closure_reservation_id: Some(closure_res_id.clone()),
            status: CombinedCandidateStatus::ClosureAccepted,
        };
        combined.candidate_digest = combined
            .compute_candidate_digest()
            .expect("compute combined candidate digest");

        let job = CoworkerJobRecord {
            job_id: "job_00000000000000000000000000000001".to_string(),
            job_revision: 5,
            create_request_key: "req_create_1".to_string(),
            binding_id: binding_id.to_string(),
            thread_uuid: thread_uuid.to_string(),
            phase: CoworkerJobPhase::Completed,
            work_brief,
            work_brief_digest,
            supersedes_job_id: None,
            planning_result: None,
            approval_linkage: Some(FrozenApprovalLinkage {
                approval_id: "apr_00000000000000000000000000000001".to_string(),
                approval_subject_digest: "2".repeat(64),
                plan_id: "plan_1".to_string(),
                plan_revision: 1,
                plan_digest: "1".repeat(64),
                project_base,
                profile_id: "profile_1".to_string(),
                registry_revision: 1,
                policy_id: "pol_1".to_string(),
                policy_digest: "3".repeat(64),
                approved_task_ids: vec!["task_1".to_string()],
                approved_write_paths: vec!["src/lib.rs".to_string()],
                controller_envelope: None,
            }),
            tasks: vec![CoworkerTaskRecord {
                task_id: "task_1".to_string(),
                task_digest: "6".repeat(64),
                role: WorkflowRole::Coder,
                difficulty: Difficulty::Hard,
                selected_choice: SelectedRoleChoice {
                    matched_difficulty: Some(Difficulty::Hard),
                    tool_id: "tool_agy".to_string(),
                    adapter: threadlane_registry::ToolAdapter::Antigravity,
                    route: RoleExecutionRoute::HeadlessWorker,
                    model_id: "model_argon".to_string(),
                    model_arg: "gemini-4-argon-high".to_string(),
                    effort: Some("high".to_string()),
                },
                depends_on: Vec::new(),
                write_paths: vec!["src/lib.rs".to_string()],
                owned_interfaces: Vec::new(),
                consumed_interfaces: Vec::new(),
                stage: CoworkerTaskStage::Integrated,
                stage_before_hold: None,
                current_attempt_no: 1,
                active_candidate_id: Some(cand_id.clone()),
                accepted_candidate_id: Some(cand_id.clone()),
                dependency_input_commit: None,
                dependency_input_tree_sha: None,
                accepted_predecessor_receipts: Vec::new(),
                attempts: Vec::new(),
                candidates: Vec::new(),
                active_hold_ids: Vec::new(),
                unresolved_outbox_id: None,
            }],
            holds: Vec::new(),
            outbox: Vec::new(),
            combined_candidates: vec![combined],
        };

        (job, cand_id, combined_id, closure_res_id)
    }

    fn forged_runtime_state(
        job_id: &str,
        final_tree_sha: &str,
        closure_verdict_id: &str,
    ) -> ControllerJobRuntimeState {
        let choice = SelectedRoleChoice {
            matched_difficulty: None,
            tool_id: "tool_agy".to_string(),
            adapter: threadlane_registry::ToolAdapter::Antigravity,
            route: RoleExecutionRoute::HeadlessWorker,
            model_id: "model_argon".to_string(),
            model_arg: "gemini-4-argon-high".to_string(),
            effort: Some("high".to_string()),
        };
        let mut closure_artifacts = BTreeMap::new();
        closure_artifacts.insert(
            "cat_00000000000000000000000000000001".to_string(),
            threadlane_native::coworker_workflow::ClosureExecutionArtifactRecord {
                attempt_id: "cat_00000000000000000000000000000001".to_string(),
                command_key: "cmd_cls_1".to_string(),
                subject_digest: "a".repeat(64),
                candidate_tree_sha: final_tree_sha.to_string(),
                artifact_ref: None,
                execution_report: None,
                advisory_response: None,
                response_error: None,
                observed_model: None,
                observed_effort: None,
                fenced_from_promotion: false,
                diagnostic: None,
                recorded_at_ms: 1_700_000_005_000,
            },
        );
        ControllerJobRuntimeState {
            job_id: job_id.to_string(),
            runtime_revision: 3,
            envelope: threadlane_native::coworker_workflow::FrozenControllerExecutionEnvelope {
                schema_version: CONTROLLER_POLICY_SCHEMA_VERSION,
                envelope_id: "env_00000000000000000000000000000001".to_string(),
                binding_id: "bnd_proof_1".to_string(),
                thread_uuid: "0196ed00-0000-7000-8000-000000000099".to_string(),
                job_id: job_id.to_string(),
                work_brief_id: "brf_00000000000000000000000000000001".to_string(),
                work_brief_digest: "8".repeat(64),
                approval_id: "apr_00000000000000000000000000000001".to_string(),
                approval_subject_digest: "2".repeat(64),
                plan_id: "plan_1".to_string(),
                plan_revision: 1,
                plan_digest: "1".repeat(64),
                profile_id: "profile_1".to_string(),
                registry_revision: 1,
                project_base: CanonicalProjectBaseRef {
                    repository_path: "/tmp/repo".to_string(),
                    base_commit: "a".repeat(40),
                    base_tree_sha: "b".repeat(40),
                },
                policy: threadlane_native::profile_flow::ExecutionPolicy {
                    schema_version: 1,
                    policy_id: "pol_1".to_string(),
                    allow_rules: Vec::new(),
                },
                policy_digest: "3".repeat(64),
                task_coder_choices: BTreeMap::new(),
                reviewer_choice: choice.clone(),
                closure_choice: choice,
                role_budgets: BTreeMap::new(),
                max_in_scope_corrections_per_task: 0,
                max_total_corrections_per_job: 0,
                max_total_task_attempts_per_job: 4,
                check_commands: BTreeMap::new(),
                allow_task_worktree_commits: true,
                allow_run_integration_execution: true,
                allow_check_failure_corrections: false,
                default_integration_strategy: serde_json::from_str("\"fast_forward_only\"")
                    .expect("deserialize fast_forward_only IntegrationStrategy"),
                independence_completeness_receipt: None,
                shadow_jev_policy: None,
                created_at_ms: 1_700_000_000_000,
                operator_attested: true,
                native_per_call_identity_verified: false,
                capability_envelope_digest: "9".repeat(64),
                envelope_digest: "7".repeat(64),
            },
            lease: None,
            role_usage: BTreeMap::new(),
            total_task_attempts_reserved: 1,
            total_corrections_reserved: 0,
            derived_attempts: Vec::new(),
            attempt_artifacts: BTreeMap::new(),
            pending_effects: BTreeMap::new(),
            provider_health: BTreeMap::new(),
            original_worker_provider_health: BTreeMap::new(),
            candidate_check_evidence: Vec::new(),
            final_candidate_subject: None,
            closure_attempt_id: Some("cat_00000000000000000000000000000001".to_string()),
            closure_verdict_id: Some(closure_verdict_id.to_string()),
            closure_artifacts,
            shadow_jev_observations: Vec::new(),
            synced_change_revision: 0,
            stop_requested: false,
            stop_or_block_reason: None,
        }
    }

    #[test]
    fn missing_runtime_or_unverified_ids_never_produce_completed_done_state() {
        let fixture = ProofFixtureDir::new("missing-runtime");
        let repo_dir = fixture.subdir("repo");
        let plans_dir = fixture.subdir("plans");
        let authority_dir = fixture.subdir("authority");
        let sessions_dir = fixture.subdir("sessions");
        let registry_dir = fixture.subdir("registry");

        let source_id = threadlane_native::native_chat_observation::compute_observation_source_id(
            &plans_dir,
            &authority_dir,
            &sessions_dir,
        )
        .expect("compute source_id");

        let spec = SourceSpec {
            source: ObservationSource {
                id: source_id.clone(),
                label: "Proof Source".to_string(),
                project_path: None,
                availability: ObservationSourceAvailability::Available,
                message: None,
            },
            registry_dir,
            plans_dir,
            authority_dir,
            sessions_dir,
            runtime_state_dirs: Vec::new(),
            project_root_hint: None,
            binding_id_hint: None,
            thread_uuid_hint: None,
        };

        let thread_uuid = "0196ed00-0000-7000-8000-000000000099";
        let (job, cand_id, combined_id, closure_res_id) =
            sample_completed_coworker_job(&repo_dir, "bnd_proof_1", thread_uuid);

        // 1. Missing runtime state (None) MUST fail completion proof and degrade job/task to Unknown.
        assert!(!verify_job_closure_completion_proof(&spec, &job, None));
        let mut warnings = Vec::new();
        let (state_none, approval_none, exec_none, rev_none, _) =
            evaluate_coworker_job_lifecycle(&job, None, false, false, false, false, &mut warnings);
        assert_eq!(state_none, ObservationJobState::Unknown);
        assert_eq!(approval_none, "unknown");
        assert_eq!(exec_none, "unknown");
        assert_eq!(rev_none.as_deref(), Some("unknown"));
        assert!(!warnings.is_empty());
        assert_eq!(
            map_coworker_task_status(
                CoworkerJobPhase::Completed,
                &job.tasks[0],
                false,
                false,
                false,
            ),
            "unknown"
        );

        // 2. Runtime state with plausible IDs on disk/memory, but without verified scoped receipt proof,
        // MUST also fail completion proof.
        let rt = forged_runtime_state(&job.job_id, &"e".repeat(40), "cvd_forged_1");
        assert!(has_structural_closure_completion_preconditions(&job, &rt));
        assert!(!verify_job_closure_completion_proof(&spec, &job, Some(&rt)));

        let chat_identity = WorkspaceChatIdentity::new(
            &source_id,
            &job.binding_id,
            &job.thread_uuid,
            repo_dir.to_string_lossy().as_ref(),
        )
        .expect("chat identity");

        let base_projected_job = WorkspaceJobHistory {
            job: WorkspaceJobSummary {
                source_id: source_id.clone(),
                job_id: job.job_id.clone(),
                binding_id: Some(job.binding_id.clone()),
                chat_identity: Some(chat_identity.clone()),
                title: "Scoped snapshot".to_string(),
                job_revision: Some(job.job_revision),
                state: ObservationJobState::Unknown,
                phase: Some("completed".to_string()),
                approval_status: "approved".to_string(),
                execution_status: "unknown".to_string(),
                review_state: Some("unknown".to_string()),
                created_at_ms: None,
                updated_at_ms: Some(1_700_000_005_000),
                summary: None,
            },
            tasks: vec![WorkspaceTaskHistory {
                task_id: "task_1".to_string(),
                title: "Task 1".to_string(),
                role: Some("coder".to_string()),
                difficulty: Some("hard".to_string()),
                stage: Some("integrated".to_string()),
                status: "unknown".to_string(),
                depends_on: Vec::new(),
                current_attempt_no: 1,
                attempts: Vec::new(),
                candidates: Vec::new(),
                active_candidate_id: Some(cand_id.clone()),
                accepted_candidate_id: Some(cand_id.clone()),
                historical_failure: None,
                current_attention: None,
                acceptance_proof: WorkspaceAcceptanceProof::unknown(),
            }],
            runs: Vec::new(),
            outbox: Vec::new(),
            combined_candidates: Vec::new(),
            result: None,
            links: WorkspaceRecordLinks::default(),
            historical_failure: None,
            current_attention: None,
            acceptance_proof: WorkspaceAcceptanceProof::unknown(),
            external_resolutions: Vec::new(),
            truncation: WorkspaceJobHistoryTruncation::default(),
        };

        let unknown_scoped = WorkspaceScopedHistoryRead {
            chat_identity: chat_identity.clone(),
            jobs: vec![base_projected_job.clone()],
            jobs_omitted: 0,
            rejected_scope_count: 0,
            degraded: false,
            diagnostics: Vec::new(),
        };
        assert!(!has_verified_scoped_job_completion_proof(
            &chat_identity,
            &job,
            Some(&rt),
            &unknown_scoped,
        ));

        // 3. VerifiedAccepted with matching job + task receipt correlation succeeds, EXCEPT when
        // degraded, truncated, or mismatched IDs are present.
        let mut verified_job_hist = base_projected_job;
        verified_job_hist.job.state = ObservationJobState::Completed;
        verified_job_hist.job.execution_status = "completed".to_string();
        verified_job_hist.acceptance_proof = WorkspaceAcceptanceProof {
            status: WorkspaceAcceptanceStatus::VerifiedAccepted,
            accepted_candidate_id: None,
            combined_candidate_id: Some(combined_id),
            review_verdict_id: None,
            review_receipt_id: None,
            integration_receipt_id: None,
            closure_reservation_id: Some(closure_res_id),
            closure_verdict_id: Some("cvd_forged_1".to_string()),
            verified_at_ms: Some(1_700_000_006_000),
        };
        verified_job_hist.tasks[0].status = "completed".to_string();
        verified_job_hist.tasks[0].acceptance_proof = WorkspaceAcceptanceProof {
            status: WorkspaceAcceptanceStatus::VerifiedAccepted,
            accepted_candidate_id: Some(cand_id),
            combined_candidate_id: None,
            review_verdict_id: Some("vrd_1".to_string()),
            review_receipt_id: Some("rev_00000000000000000000000000000001".to_string()),
            integration_receipt_id: Some("int_00000000000000000000000000000001".to_string()),
            closure_reservation_id: None,
            closure_verdict_id: None,
            verified_at_ms: Some(1_700_000_005_500),
        };

        let verified_scoped = WorkspaceScopedHistoryRead {
            chat_identity: chat_identity.clone(),
            jobs: vec![verified_job_hist.clone()],
            jobs_omitted: 0,
            rejected_scope_count: 0,
            degraded: false,
            diagnostics: Vec::new(),
        };
        assert!(has_verified_scoped_job_completion_proof(
            &chat_identity,
            &job,
            Some(&rt),
            &verified_scoped,
        ));

        // Degraded read rejects completion even if proof fields look populated.
        let mut degraded_scoped = verified_scoped.clone();
        degraded_scoped.degraded = true;
        assert!(!has_verified_scoped_job_completion_proof(
            &chat_identity,
            &job,
            Some(&rt),
            &degraded_scoped,
        ));

        // Truncated tasks reject job-wide completion proof.
        let mut truncated_scoped = verified_scoped.clone();
        truncated_scoped.jobs[0].truncation.tasks_omitted = 1;
        assert!(!has_verified_scoped_job_completion_proof(
            &chat_identity,
            &job,
            Some(&rt),
            &truncated_scoped,
        ));

        // Mismatched closure_verdict_id rejects completion proof.
        let mut wrong_verdict_scoped = verified_scoped;
        wrong_verdict_scoped.jobs[0]
            .acceptance_proof
            .closure_verdict_id = Some("cvd_other".to_string());
        assert!(!has_verified_scoped_job_completion_proof(
            &chat_identity,
            &job,
            Some(&rt),
            &wrong_verdict_scoped,
        ));
    }
}
