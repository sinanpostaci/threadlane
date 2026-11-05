use std::collections::{BTreeMap, BTreeSet};

use threadlane_core::observation::{
    sanitize_observation_selection_token, sanitize_observation_summary, ObservationActor,
    ObservationCall, ObservationCallState, ObservationDelivery, ObservationDeliveryStage,
    ObservationExecutor, ObservationJob, ObservationJournalRecord, ObservationRecordPhase,
    ObservationRequest, ObservationUsage, ReadonlyBindingRecord, ReadonlyCoreStateSnapshot,
    ReadonlyEventBatchRecord, ReadonlyEventRecord, ReadonlyRunRecord, ReadonlyWakeupRecord,
};

const MAX_REQUEST_CHILDREN: usize = 64;
const UNSCOPED_PROJECT_LABEL: &str = "Unscoped";

/// Verified linkage between a durable worker execution and an approved job task scope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WorkerObservationLink {
    pub(crate) worker_id: String,
    pub(crate) job_id: String,
    pub(crate) task_id: String,
    pub(crate) approval_id: String,
    pub(crate) title: String,
    pub(crate) control_binding_id: String,
    pub(crate) session_binding_id: String,
    pub(crate) thread_uuid: String,
    pub(crate) role: Option<String>,
    pub(crate) live: bool,
}

/// Pure projection result containing newest-first reduced requests and sanitized warnings.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct RequestProjection {
    pub(crate) requests: Vec<ObservationRequest>,
    pub(crate) warnings: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct WorkerRecordView {
    worker_id: String,
    run_id: String,
    binding_id: String,
    revision: u64,
    status: String,
    provider: String,
    requested_model: String,
    requested_effort: String,
    observed_model: Option<String>,
    observed_effort: Option<String>,
    created_at_ms: i64,
    updated_at_ms: i64,
    started_at_ms: Option<i64>,
    completed_at_ms: Option<i64>,
}

#[derive(Debug, Clone, Default)]
struct CoreStateView {
    bindings: Vec<ReadonlyBindingRecord>,
    runs: Vec<ReadonlyRunRecord>,
    workers: Vec<WorkerRecordView>,
    events: Vec<ReadonlyEventRecord>,
    event_batches: Vec<ReadonlyEventBatchRecord>,
    wakeups: Vec<ReadonlyWakeupRecord>,
}

impl CoreStateView {
    fn from_snapshot(snapshot: &ReadonlyCoreStateSnapshot) -> Self {
        let workers = snapshot
            .workers
            .iter()
            .map(|w| WorkerRecordView {
                worker_id: w.worker_id.clone(),
                run_id: w.run_id.clone(),
                binding_id: w.binding_id.clone(),
                revision: w.revision,
                status: w.status.clone(),
                provider: w.provider.clone(),
                requested_model: w.requested_model.clone(),
                requested_effort: w.requested_effort.clone(),
                observed_model: w.observed_model.clone(),
                observed_effort: w.observed_effort.clone(),
                created_at_ms: w.created_at_ms,
                updated_at_ms: w.updated_at_ms,
                started_at_ms: w.started_at_ms,
                completed_at_ms: w.completed_at_ms,
            })
            .collect();

        Self {
            bindings: snapshot.bindings.clone(),
            runs: snapshot.runs.clone(),
            workers,
            events: snapshot.events.clone(),
            event_batches: snapshot.event_batches.clone(),
            wakeups: snapshot.wakeups.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ScopedField<T> {
    value: Option<T>,
    conflicted: bool,
}

impl<T: PartialEq + Clone> ScopedField<T> {
    fn new() -> Self {
        Self {
            value: None,
            conflicted: false,
        }
    }

    fn observe(&mut self, candidate: Option<T>) {
        if self.conflicted {
            return;
        }
        if let Some(next) = candidate {
            match &self.value {
                None => self.value = Some(next),
                Some(existing) if *existing == next => {}
                Some(_) => {
                    self.value = None;
                    self.conflicted = true;
                }
            }
        }
    }

    fn resolved(&self) -> Option<&T> {
        if self.conflicted {
            None
        } else {
            self.value.as_ref()
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct VerifiedWorkerLink {
    worker_id: String,
    job_id: String,
    task_id: String,
    approval_id: String,
    title: String,
    control_binding_id: String,
    session_binding_id: String,
    thread_uuid: String,
    role: Option<String>,
    live: bool,
    job_project_label: String,
    job_thread_label: Option<String>,
    job_plan_id: Option<String>,
    task_status: String,
    job_review_state: Option<String>,
}

#[derive(Debug, Clone)]
struct CollapsedJournalGroup<'a> {
    request_id: String,
    call_id: Option<String>,
    parent_request_id: ScopedField<String>,
    binding_id: ScopedField<String>,
    thread_uuid: ScopedField<String>,
    raw_job_id: ScopedField<String>,
    finish_job_id: ScopedField<String>,
    raw_plan_id: ScopedField<String>,
    raw_task_id: ScopedField<String>,
    raw_approval_id: ScopedField<String>,
    verified_job_id: Option<String>,
    verified_task_id: Option<String>,
    verified_approval_id: Option<String>,
    has_verified_worker_link_scope: bool,
    operation: String,
    title: String,
    project_label: Option<String>,
    thread_label: Option<String>,
    summary: Option<String>,
    actor: Option<ObservationActor>,
    executor: Option<ObservationExecutor>,
    usage: Option<ObservationUsage>,
    delivery: Vec<ObservationDelivery>,
    earliest_at_ms: i64,
    min_entry_at_ms: Option<i64>,
    max_finish_at_ms: Option<i64>,
    updated_at_ms: i64,
    has_entry: bool,
    has_finish: bool,
    all_finishes_completed: bool,
    latest_finish_state: Option<ObservationCallState>,
    in_flight_probe_record: &'a ObservationJournalRecord,
    folded_children: Vec<ObservationCall>,
    has_active_verified_worker_receipt: bool,
}

impl<'a> CollapsedJournalGroup<'a> {
    fn has_scope_conflict(&self) -> bool {
        self.parent_request_id.conflicted
            || self.binding_id.conflicted
            || self.thread_uuid.conflicted
            || self.raw_job_id.conflicted
            || self.finish_job_id.conflicted
            || self.raw_plan_id.conflicted
            || self.raw_task_id.conflicted
            || self.raw_approval_id.conflicted
    }

    fn has_raw_job_hints(&self) -> bool {
        self.raw_job_id.value.is_some()
            || self.finish_job_id.value.is_some()
            || self.raw_plan_id.value.is_some()
            || self.raw_task_id.value.is_some()
            || self.raw_approval_id.value.is_some()
            || self.raw_job_id.conflicted
            || self.finish_job_id.conflicted
            || self.raw_plan_id.conflicted
            || self.raw_task_id.conflicted
            || self.raw_approval_id.conflicted
    }
}

/// Pure request-history reducer that collapses journal records, verifies job/worker links,
/// and projects newest-first [`ObservationRequest`] rows.
pub(crate) fn project_requests(
    source_id: &str,
    jobs: &[ObservationJob],
    records: &[ObservationJournalRecord],
    core_states: &[ReadonlyCoreStateSnapshot],
    worker_links: &[WorkerObservationLink],
) -> RequestProjection {
    let core_views: Vec<CoreStateView> = core_states
        .iter()
        .map(CoreStateView::from_snapshot)
        .collect();
    project_requests_with_views(source_id, jobs, records, &core_views, worker_links)
}

fn project_requests_with_views(
    source_id: &str,
    jobs: &[ObservationJob],
    records: &[ObservationJournalRecord],
    core_states: &[CoreStateView],
    worker_links: &[WorkerObservationLink],
) -> RequestProjection {
    let mut warnings = Vec::new();
    let trimmed_source_id = source_id.trim();
    let effective_source_id = if trimmed_source_id.is_empty() {
        push_warning(
            &mut warnings,
            "Request projection received an empty source identifier.",
        );
        "unknown"
    } else {
        trimmed_source_id
    };

    // 1. Index unambiguous jobs in this source_id.
    let jobs_by_id = index_source_jobs(effective_source_id, jobs, &mut warnings);

    // 2. Verify WorkerObservationLink entries against approved jobs in this source_id.
    let verified_links = verify_worker_links(&jobs_by_id, worker_links, core_states, &mut warnings);

    // 3. Collapse journal records by (source_id, request_id, call_id).
    let mut collapsed_groups = collapse_journal_records(records, &mut warnings);

    // 4. Resolve verified job membership for each collapsed group.
    for group in collapsed_groups.values_mut() {
        resolve_group_job_scope(
            group,
            &jobs_by_id,
            &verified_links,
            core_states,
            &mut warnings,
        );
    }

    // 5. Partition into root requests and fold verified child calls / wrapper operations.
    let mut root_groups = fold_journal_groups(collapsed_groups, &mut warnings);

    // 6. Index structural ReadonlyWorkerRecord views and run-to-worker mappings.
    let (workers_by_id, conflicted_worker_ids, runs_to_workers) =
        index_core_workers(core_states, &verified_links, &mut warnings);

    // Check whether an unfinished allowlisted dispatch root has an active verified worker receipt.
    for root in root_groups.values_mut() {
        if !root.has_finish
            && is_allowlisted_dispatch_op(&root.operation)
            && !root.has_scope_conflict()
        {
            let start_bound_ms = root.min_entry_at_ms.unwrap_or(root.earliest_at_ms);
            let has_live_receipt = verified_links.iter().any(|vlink| {
                vlink.live
                    && root_matches_link_scope(root, vlink)
                    && !conflicted_worker_ids.contains(&vlink.worker_id)
                    && workers_by_id.get(&vlink.worker_id).is_some_and(|wv| {
                        wv.binding_id == vlink.session_binding_id
                            && wv.created_at_ms >= start_bound_ms
                            && resolve_worker_state(vlink, Some(wv)).is_active()
                            && has_unique_core_binding_thread(
                                core_states,
                                &vlink.session_binding_id,
                                &vlink.thread_uuid,
                            )
                    })
            });
            if has_live_receipt {
                root.has_active_verified_worker_receipt = true;
            }
        }
    }

    // 7. Match verified workers to completed dispatch roots or create standalone Historical worker rows.
    let mut historical_worker_requests = Vec::new();
    let mut emitted_worker_ids = BTreeSet::new();

    for vlink in &verified_links {
        if !emitted_worker_ids.insert(vlink.worker_id.clone()) {
            continue;
        }

        let valid_worker_view = if conflicted_worker_ids.contains(&vlink.worker_id) {
            None
        } else {
            workers_by_id.get(&vlink.worker_id).and_then(|wv| {
                if wv.binding_id != vlink.session_binding_id {
                    push_warning(
                        &mut warnings,
                        "Rejected core worker record with mismatched binding identifier.",
                    );
                    None
                } else if !has_unique_core_binding_thread(
                    core_states,
                    &vlink.session_binding_id,
                    &vlink.thread_uuid,
                ) {
                    push_warning(
                        &mut warnings,
                        "Rejected core worker record with missing or mismatched session binding thread UUID.",
                    );
                    None
                } else {
                    Some(wv)
                }
            })
        };

        let matching_root_ids: Vec<String> = if let Some(wv) = valid_worker_view {
            root_groups
                .iter()
                .filter(|(_, root)| root_matches_completed_dispatch(root, vlink, wv))
                .map(|(req_id, _)| req_id.clone())
                .collect()
        } else {
            Vec::new()
        };

        if matching_root_ids.len() == 1 {
            let root_id = &matching_root_ids[0];
            if let (Some(root), Some(wv)) = (root_groups.get_mut(root_id), valid_worker_view) {
                let worker_state = resolve_worker_state(vlink, Some(wv));
                if worker_state.is_active() {
                    root.has_active_verified_worker_receipt = true;
                }
                let call_title =
                    sanitize_non_empty(&vlink.title).unwrap_or_else(|| vlink.worker_id.clone());
                let worker_call = ObservationCall {
                    id: vlink.worker_id.clone(),
                    title: call_title,
                    state: worker_state,
                    executor: build_worker_executor(vlink.role.as_deref(), Some(wv)),
                    started_at_ms: wv.started_at_ms,
                    finished_at_ms: wv.completed_at_ms,
                    duration_ms: compute_duration(wv.started_at_ms, wv.completed_at_ms),
                    usage: None,
                    summary: None,
                };
                root.folded_children.push(worker_call);

                let core_deliveries = collect_core_deliveries(
                    core_states,
                    vlink,
                    wv,
                    &runs_to_workers,
                    &mut warnings,
                );
                if let Some(latest_ts) = max_worker_and_delivery_timestamp(
                    Some(root.updated_at_ms),
                    Some(wv),
                    &core_deliveries,
                ) {
                    root.updated_at_ms = latest_ts;
                }
                root.delivery.extend(core_deliveries);
                continue;
            }
        }

        if matching_root_ids.len() > 1 {
            push_warning(
                &mut warnings,
                "Worker matched multiple distinct dispatch roots; emitted as standalone historical worker.",
            );
        }

        let worker_state = resolve_worker_state(vlink, valid_worker_view);
        let clean_link_title =
            sanitize_non_empty(&vlink.title).unwrap_or_else(|| vlink.worker_id.clone());
        let historical_title =
            sanitize_observation_summary(&format!("Historical worker · {clean_link_title}"));

        let mut delivery = if let Some(wv) = valid_worker_view {
            collect_core_deliveries(core_states, vlink, wv, &runs_to_workers, &mut warnings)
        } else {
            Vec::new()
        };
        dedup_and_sort_deliveries(&mut delivery);
        let historical_updated_at_ms =
            max_worker_and_delivery_timestamp(None, valid_worker_view, &delivery).unwrap_or(0);

        historical_worker_requests.push(ObservationRequest {
            source_id: effective_source_id.to_string(),
            id: vlink.worker_id.clone(),
            job_id: Some(vlink.job_id.clone()),
            parent_request_id: None,
            title: historical_title,
            operation: "worker.historical".to_string(),
            project_label: vlink.job_project_label.clone(),
            binding_id: Some(vlink.session_binding_id.clone()),
            thread_uuid: Some(vlink.thread_uuid.clone()),
            thread_label: vlink.job_thread_label.clone(),
            state: worker_state,
            started_at_ms: valid_worker_view.and_then(|wv| wv.started_at_ms),
            finished_at_ms: valid_worker_view.and_then(|wv| wv.completed_at_ms),
            updated_at_ms: historical_updated_at_ms,
            duration_ms: valid_worker_view
                .and_then(|wv| compute_duration(wv.started_at_ms, wv.completed_at_ms)),
            actor: ObservationActor::unknown(None),
            executor: build_worker_executor(vlink.role.as_deref(), valid_worker_view),
            usage: None,
            delivery,
            children: Vec::new(),
            children_count: 0,
            summary: None,
        });
    }

    // 8. Finalize root journal requests and combine with standalone historical worker rows.
    let mut requests = Vec::with_capacity(root_groups.len() + historical_worker_requests.len());
    for (_, mut root) in root_groups {
        if emitted_worker_ids.contains(&root.request_id) {
            push_warning(
                &mut warnings,
                "Skipped journal request whose identifier collided with a verified worker ID.",
            );
            continue;
        }

        let state = resolve_journal_group_state(&root, root.has_active_verified_worker_receipt);
        let started_at_ms = root.min_entry_at_ms;
        let finished_at_ms = root.max_finish_at_ms;
        let duration_ms = compute_duration(started_at_ms, finished_at_ms);

        let project_label = root
            .project_label
            .clone()
            .or_else(|| {
                root.verified_job_id
                    .as_ref()
                    .and_then(|jid| jobs_by_id.get(jid))
                    .and_then(|job| sanitize_non_empty(&job.project_label))
            })
            .unwrap_or_else(|| UNSCOPED_PROJECT_LABEL.to_string());

        let thread_label = root.thread_label.clone().or_else(|| {
            root.verified_job_id
                .as_ref()
                .and_then(|jid| jobs_by_id.get(jid))
                .and_then(|job| sanitize_opt(job.thread_label.as_deref()))
        });

        dedup_and_sort_deliveries(&mut root.delivery);
        if let Some(latest_ts) =
            max_worker_and_delivery_timestamp(Some(root.updated_at_ms), None, &root.delivery)
        {
            root.updated_at_ms = latest_ts;
        }

        let (children, children_count) = finalize_children(root.folded_children, &mut warnings);

        requests.push(ObservationRequest {
            source_id: effective_source_id.to_string(),
            id: root.request_id,
            job_id: root.verified_job_id,
            parent_request_id: None,
            title: root.title,
            operation: root.operation,
            project_label,
            binding_id: root.binding_id.resolved().cloned(),
            thread_uuid: root.thread_uuid.resolved().cloned(),
            thread_label,
            state,
            started_at_ms,
            finished_at_ms,
            updated_at_ms: root.updated_at_ms,
            duration_ms,
            actor: root
                .actor
                .unwrap_or_else(|| ObservationActor::unknown(None)),
            executor: root.executor,
            usage: root.usage,
            delivery: root.delivery,
            children,
            children_count,
            summary: root.summary,
        });
    }

    requests.extend(historical_worker_requests);

    // Sort newest meaningful activity first (descending updated_at_ms, deterministic ID tie-break).
    requests.sort_by(|a, b| {
        b.updated_at_ms
            .cmp(&a.updated_at_ms)
            .then_with(|| a.id.cmp(&b.id))
    });

    RequestProjection { requests, warnings }
}

fn index_source_jobs<'a>(
    source_id: &str,
    jobs: &'a [ObservationJob],
    warnings: &mut Vec<String>,
) -> BTreeMap<String, &'a ObservationJob> {
    let mut jobs_by_id = BTreeMap::new();
    let mut ambiguous_ids = BTreeSet::new();

    for job in jobs {
        if job.source_id.trim() != source_id {
            continue;
        }
        let Some(job_id) = clean_id(&job.id) else {
            push_warning(warnings, "Skipped observation job with invalid identifier.");
            continue;
        };
        if ambiguous_ids.contains(&job_id) {
            continue;
        }
        if jobs_by_id.contains_key(&job_id) {
            jobs_by_id.remove(&job_id);
            ambiguous_ids.insert(job_id);
            push_warning(
                warnings,
                "Rejected duplicate observation job identifiers within the same source.",
            );
            continue;
        }
        jobs_by_id.insert(job_id, job);
    }

    jobs_by_id
}

fn verify_worker_links(
    jobs_by_id: &BTreeMap<String, &ObservationJob>,
    worker_links: &[WorkerObservationLink],
    core_states: &[CoreStateView],
    warnings: &mut Vec<String>,
) -> Vec<VerifiedWorkerLink> {
    let mut unique_links: BTreeMap<String, &WorkerObservationLink> = BTreeMap::new();
    let mut conflicted_workers = BTreeSet::new();

    for link in worker_links {
        let Some(worker_id) = clean_id(&link.worker_id) else {
            push_warning(
                warnings,
                "Skipped worker observation link with missing worker identifier.",
            );
            continue;
        };
        if conflicted_workers.contains(&worker_id) {
            continue;
        }
        if let Some(existing) = unique_links.get(&worker_id) {
            if *existing != link {
                unique_links.remove(&worker_id);
                conflicted_workers.insert(worker_id);
                push_warning(
                    warnings,
                    "Skipped worker observation link with conflicting duplicate worker ID metadata.",
                );
            }
            continue;
        }
        unique_links.insert(worker_id, link);
    }

    let mut verified = Vec::new();
    for (worker_id, link) in unique_links {
        let (
            Some(job_id),
            Some(task_id),
            Some(approval_id),
            Some(control_binding_id),
            Some(session_binding_id),
            Some(thread_uuid),
        ) = (
            clean_id(&link.job_id),
            clean_id(&link.task_id),
            clean_id(&link.approval_id),
            clean_id(&link.control_binding_id),
            clean_id(&link.session_binding_id),
            clean_id(&link.thread_uuid),
        )
        else {
            push_warning(
                warnings,
                "Rejected worker observation link with missing scope identifiers.",
            );
            continue;
        };

        let Some(job) = jobs_by_id.get(&job_id) else {
            push_warning(
                warnings,
                "Rejected worker observation link referencing an unknown job in this source.",
            );
            continue;
        };

        if job.approval_status.trim() != "approved" {
            push_warning(
                warnings,
                "Rejected worker observation link for an unapproved job.",
            );
            continue;
        }

        let job_binding = clean_opt_id(job.binding_id.as_deref());
        let job_thread = clean_opt_id(job.thread_uuid.as_deref());
        if job_binding.as_deref() != Some(control_binding_id.as_str())
            || job_thread.as_deref() != Some(thread_uuid.as_str())
        {
            push_warning(
                warnings,
                "Rejected worker observation link due to job binding or thread mismatch.",
            );
            continue;
        }

        if binding_thread_conflicts(core_states, &session_binding_id, &thread_uuid) {
            push_warning(
                warnings,
                "Rejected worker observation link due to conflicting core session binding thread.",
            );
            continue;
        }

        let Some(matched_task) = job
            .tasks
            .iter()
            .find(|t| clean_id(&t.id).as_deref() == Some(task_id.as_str()))
        else {
            push_warning(
                warnings,
                "Rejected worker observation link referencing a task not present on the job.",
            );
            continue;
        };

        let job_project_label = sanitize_non_empty(&job.project_label)
            .unwrap_or_else(|| UNSCOPED_PROJECT_LABEL.to_string());
        let job_thread_label = sanitize_opt(job.thread_label.as_deref());
        let job_plan_id = clean_opt_id(job.plan_id.as_deref());
        let task_status = matched_task.status.trim().to_string();
        let job_review_state = job
            .review_state
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(ToOwned::to_owned);

        verified.push(VerifiedWorkerLink {
            worker_id,
            job_id,
            task_id,
            approval_id,
            title: sanitize_non_empty(&link.title)
                .unwrap_or_else(|| link.worker_id.trim().to_string()),
            control_binding_id,
            session_binding_id,
            thread_uuid,
            role: sanitize_opt(link.role.as_deref()),
            live: link.live,
            job_project_label,
            job_thread_label,
            job_plan_id,
            task_status,
            job_review_state,
        });
    }

    verified
}

fn collapse_journal_records<'a>(
    records: &'a [ObservationJournalRecord],
    warnings: &mut Vec<String>,
) -> BTreeMap<(String, Option<String>), CollapsedJournalGroup<'a>> {
    let mut grouped: BTreeMap<(String, Option<String>), Vec<&'a ObservationJournalRecord>> =
        BTreeMap::new();

    for record in records {
        let Some(request_id) = clean_id(&record.request_id) else {
            push_warning(
                warnings,
                "Skipped journal record with missing or invalid request ID.",
            );
            continue;
        };
        let call_id = clean_opt_id(record.call_id.as_deref());
        grouped
            .entry((request_id, call_id))
            .or_default()
            .push(record);
    }

    let mut collapsed = BTreeMap::new();
    for ((request_id, call_id), mut group_records) in grouped {
        group_records.sort_by(|a, b| {
            a.at_ms
                .cmp(&b.at_ms)
                .then_with(|| phase_rank(&a.phase).cmp(&phase_rank(&b.phase)))
                .then_with(|| a.record_id.cmp(&b.record_id))
        });

        let first_record = group_records[0];
        let mut parent_request_id = ScopedField::new();
        let mut binding_id = ScopedField::new();
        let mut thread_uuid = ScopedField::new();
        let mut raw_job_id = ScopedField::new();
        let mut finish_job_id = ScopedField::new();
        let mut raw_plan_id = ScopedField::new();
        let mut raw_task_id = ScopedField::new();
        let mut raw_approval_id = ScopedField::new();

        let mut operation = None;
        let mut title = None;
        let mut project_label = None;
        let mut thread_label = None;
        let mut summary = None;
        let mut actor: Option<ObservationActor> = None;
        let mut executor: Option<ObservationExecutor> = None;
        let mut usage: Option<ObservationUsage> = None;
        let mut delivery = Vec::new();

        let earliest_at_ms = first_record.at_ms;
        let mut min_entry_at_ms: Option<i64> = None;
        let mut max_finish_at_ms: Option<i64> = None;
        let mut updated_at_ms = first_record.at_ms;
        let mut has_entry = false;
        let mut has_finish = false;
        let mut all_finishes_completed = true;
        let mut latest_finish_state = None;
        let mut in_flight_probe_record = first_record;

        for record in group_records {
            if record.at_ms > updated_at_ms {
                updated_at_ms = record.at_ms;
            }
            if record.process_instance.is_some()
                || in_flight_probe_record.process_instance.is_none()
            {
                in_flight_probe_record = record;
            }

            let record_op = sanitize_non_empty(&record.operation);

            match record.phase {
                ObservationRecordPhase::Entry => {
                    has_entry = true;
                    min_entry_at_ms =
                        Some(min_entry_at_ms.map_or(record.at_ms, |prev| prev.min(record.at_ms)));
                }
                ObservationRecordPhase::Progress => {}
                ObservationRecordPhase::Finish => {
                    has_finish = true;
                    max_finish_at_ms =
                        Some(max_finish_at_ms.map_or(record.at_ms, |prev| prev.max(record.at_ms)));
                    if record.state == ObservationCallState::Completed {
                        if record_op
                            .as_deref()
                            .is_some_and(is_allowlisted_plan_result_op)
                        {
                            finish_job_id.observe(clean_opt_id(record.job_id.as_deref()));
                        }
                    } else {
                        all_finishes_completed = false;
                    }
                    latest_finish_state = Some(record.state);
                }
            }

            let normalized_parent_id =
                clean_opt_id(record.parent_request_id.as_deref()).filter(|pid| pid != &request_id);
            parent_request_id.observe(normalized_parent_id);
            binding_id.observe(clean_opt_id(record.binding_id.as_deref()));
            thread_uuid.observe(clean_opt_id(record.thread_uuid.as_deref()));
            raw_job_id.observe(clean_opt_id(record.job_id.as_deref()));
            raw_plan_id.observe(clean_opt_id(record.plan_id.as_deref()));
            raw_task_id.observe(clean_opt_id(record.task_id.as_deref()));
            raw_approval_id.observe(clean_opt_id(record.approval_id.as_deref()));

            if let Some(op) = record_op {
                operation = Some(op);
            }
            if let Some(t) = sanitize_non_empty(&record.title) {
                title = Some(t);
            }
            if let Some(pl) = sanitize_opt(record.project_label.as_deref()) {
                project_label = Some(pl);
            }
            if let Some(tl) = sanitize_opt(record.thread_label.as_deref()) {
                thread_label = Some(tl);
            }
            if let Some(s) = sanitize_opt(record.summary.as_deref()) {
                summary = Some(s);
            }

            if let Some(ref rec_actor) = record.actor {
                actor = Some(merge_actor(actor.take(), rec_actor));
            }
            if let Some(ref rec_exec) = record.executor {
                executor = Some(merge_executor(executor.take(), rec_exec));
            }
            if let Some(ref rec_usage) = record.usage {
                usage = Some(sanitize_usage(rec_usage));
            }

            for d in &record.delivery {
                if let Some(d_ms) = d.at_ms {
                    if d_ms > updated_at_ms {
                        updated_at_ms = d_ms;
                    }
                }
                delivery.push(sanitize_delivery(d));
            }
        }

        let resolved_operation = operation.unwrap_or_else(|| "unknown".to_string());
        let resolved_title = title.unwrap_or_else(|| resolved_operation.clone());

        let group = CollapsedJournalGroup {
            request_id: request_id.clone(),
            call_id: call_id.clone(),
            parent_request_id,
            binding_id,
            thread_uuid,
            raw_job_id,
            finish_job_id,
            raw_plan_id,
            raw_task_id,
            raw_approval_id,
            verified_job_id: None,
            verified_task_id: None,
            verified_approval_id: None,
            has_verified_worker_link_scope: false,
            operation: resolved_operation,
            title: resolved_title,
            project_label,
            thread_label,
            summary,
            actor,
            executor,
            usage,
            delivery,
            earliest_at_ms,
            min_entry_at_ms,
            max_finish_at_ms,
            updated_at_ms,
            has_entry,
            has_finish,
            all_finishes_completed: has_finish && all_finishes_completed,
            latest_finish_state,
            in_flight_probe_record,
            folded_children: Vec::new(),
            has_active_verified_worker_receipt: false,
        };

        if group.has_scope_conflict() {
            push_warning(
                warnings,
                "Detected conflicting scope fields across journal phases for a request.",
            );
        }

        collapsed.insert((request_id, call_id), group);
    }

    collapsed
}

fn resolve_group_job_scope(
    group: &mut CollapsedJournalGroup<'_>,
    jobs_by_id: &BTreeMap<String, &ObservationJob>,
    verified_links: &[VerifiedWorkerLink],
    core_states: &[CoreStateView],
    warnings: &mut Vec<String>,
) {
    let has_hints = group.has_raw_job_hints();
    if group.has_scope_conflict() {
        if has_hints {
            push_warning(
                warnings,
                "Ignored unverified job, plan, task, or approval hint on journal record.",
            );
        }
        return;
    }

    if let (Some(binding_id), Some(thread_uuid), Some(approval_id)) = (
        group.binding_id.resolved(),
        group.thread_uuid.resolved(),
        group.raw_approval_id.resolved(),
    ) {
        let mut plan_mismatch = false;
        let mut matching_links = Vec::new();

        for vlink in verified_links {
            if vlink.control_binding_id != *binding_id
                || vlink.thread_uuid != *thread_uuid
                || vlink.approval_id != *approval_id
            {
                continue;
            }
            if let Some(raw_jid) = group.raw_job_id.resolved() {
                if *raw_jid != vlink.job_id {
                    continue;
                }
            }
            if let Some(raw_tid) = group.raw_task_id.resolved() {
                if *raw_tid != vlink.task_id {
                    continue;
                }
            }
            if let (Some(job_plan), Some(rec_plan)) =
                (vlink.job_plan_id.as_ref(), group.raw_plan_id.resolved())
            {
                if job_plan != rec_plan {
                    plan_mismatch = true;
                    continue;
                }
            }
            matching_links.push(vlink);
        }

        if plan_mismatch {
            push_warning(
                warnings,
                "Rejected job link due to mismatched plan ID between job and journal record.",
            );
        }

        if !matching_links.is_empty() {
            let first_job_id = &matching_links[0].job_id;
            if matching_links.iter().all(|l| &l.job_id == first_job_id) {
                group.verified_job_id = Some(first_job_id.clone());
                group.verified_approval_id = Some(approval_id.clone());
                group.has_verified_worker_link_scope = true;

                let first_task_id = &matching_links[0].task_id;
                if matching_links.iter().all(|l| &l.task_id == first_task_id) {
                    group.verified_task_id = Some(first_task_id.clone());
                }
                return;
            }
            if has_hints {
                push_warning(
                    warnings,
                    "Ignored ambiguous job hint matching multiple jobs.",
                );
            }
            return;
        }
    }

    if try_resolve_plan_result_job_scope(group, jobs_by_id, core_states, warnings) {
        return;
    }

    if has_hints {
        push_warning(
            warnings,
            "Ignored unverified job, plan, task, or approval hint on journal record.",
        );
    }
}

fn try_resolve_plan_result_job_scope(
    group: &mut CollapsedJournalGroup<'_>,
    jobs_by_id: &BTreeMap<String, &ObservationJob>,
    core_states: &[CoreStateView],
    warnings: &mut Vec<String>,
) -> bool {
    if !is_allowlisted_plan_result_op(&group.operation) {
        return false;
    }
    if !group.has_finish
        || !group.all_finishes_completed
        || group.latest_finish_state != Some(ObservationCallState::Completed)
    {
        return false;
    }
    if group.raw_task_id.resolved().is_some() || group.raw_approval_id.resolved().is_some() {
        return false;
    }

    let Some(finish_job_id) = group.finish_job_id.resolved() else {
        return false;
    };
    if group.raw_job_id.resolved() != Some(finish_job_id) {
        return false;
    }

    if let Some(rec_plan_id) = group.raw_plan_id.resolved() {
        if rec_plan_id != finish_job_id {
            push_warning(
                warnings,
                "Rejected plan request link due to mismatched plan ID between record and finish result.",
            );
            return false;
        }
    }

    let (Some(binding_id), Some(thread_uuid)) =
        (group.binding_id.resolved(), group.thread_uuid.resolved())
    else {
        return false;
    };

    let Some(job) = jobs_by_id.get(finish_job_id) else {
        return false;
    };

    let Some(job_plan_id) = clean_opt_id(job.plan_id.as_deref()) else {
        return false;
    };
    if &job_plan_id != finish_job_id {
        push_warning(
            warnings,
            "Rejected plan request link because job ID and job plan ID do not match.",
        );
        return false;
    }

    let plan_match_count = jobs_by_id
        .values()
        .filter(|candidate| {
            clean_opt_id(candidate.plan_id.as_deref()).as_deref() == Some(finish_job_id.as_str())
        })
        .count();
    if plan_match_count != 1 {
        push_warning(
            warnings,
            "Rejected plan request link due to ambiguous jobs sharing the same plan ID.",
        );
        return false;
    }

    if clean_opt_id(job.binding_id.as_deref()).as_deref() != Some(binding_id.as_str()) {
        return false;
    }

    if let Some(job_thread) = clean_opt_id(job.thread_uuid.as_deref()) {
        if &job_thread != thread_uuid
            || binding_thread_conflicts(core_states, binding_id, thread_uuid)
        {
            return false;
        }
    } else if !has_unique_core_binding_thread(core_states, binding_id, thread_uuid) {
        return false;
    }

    group.verified_job_id = Some(finish_job_id.clone());
    true
}

fn fold_journal_groups<'a>(
    collapsed: BTreeMap<(String, Option<String>), CollapsedJournalGroup<'a>>,
    warnings: &mut Vec<String>,
) -> BTreeMap<String, CollapsedJournalGroup<'a>> {
    let mut root_groups: BTreeMap<String, CollapsedJournalGroup<'a>> = BTreeMap::new();
    let mut candidate_children: Vec<CollapsedJournalGroup<'a>> = Vec::new();

    for ((request_id, call_id), group) in collapsed {
        if call_id.is_none()
            && !group.parent_request_id.conflicted
            && group.parent_request_id.resolved().is_none()
        {
            root_groups.insert(request_id, group);
        } else {
            candidate_children.push(group);
        }
    }

    let approved_plan_wrapper_children =
        select_unique_workflow_plan_wrapper_children(&root_groups, &candidate_children, warnings);

    for (child_idx, mut child) in candidate_children.into_iter().enumerate() {
        let target_parent_id = if child.call_id.is_some() {
            if child.parent_request_id.conflicted {
                None
            } else {
                match child.parent_request_id.resolved() {
                    Some(hint) if hint != &child.request_id => Some(hint.clone()),
                    Some(_) | None => Some(child.request_id.clone()),
                }
            }
        } else if child.parent_request_id.conflicted {
            None
        } else {
            child
                .parent_request_id
                .resolved()
                .filter(|hint| *hint != &child.request_id)
                .cloned()
        };

        let is_approved_plan_wrapper_child = target_parent_id.as_ref().is_some_and(|pid| {
            approved_plan_wrapper_children
                .get(&child_idx)
                .is_some_and(|expected_pid| expected_pid == pid)
        });

        let can_fold = is_approved_plan_wrapper_child
            || target_parent_id
                .as_ref()
                .and_then(|pid| root_groups.get(pid))
                .is_some_and(|parent| can_fold_child_into_parent(parent, &child));

        if can_fold {
            if let Some(pid) = target_parent_id {
                if let Some(parent) = root_groups.get_mut(&pid) {
                    if is_approved_plan_wrapper_child {
                        parent.verified_job_id = child.verified_job_id.clone();
                    }
                    if is_same_root_dispatch_wrapper(parent, &child) {
                        merge_dispatch_wrapper_into_root(parent, child);
                    } else {
                        if child.updated_at_ms > parent.updated_at_ms {
                            parent.updated_at_ms = child.updated_at_ms;
                        }
                        if let Some(latest_ts) = max_worker_and_delivery_timestamp(
                            Some(parent.updated_at_ms),
                            None,
                            &child.delivery,
                        ) {
                            parent.updated_at_ms = latest_ts;
                        }
                        let call_id = child
                            .call_id
                            .clone()
                            .unwrap_or_else(|| child.request_id.clone());
                        let call_state = resolve_journal_group_state(&child, false);
                        let call = ObservationCall {
                            id: call_id,
                            title: child.title,
                            state: call_state,
                            executor: child.executor,
                            started_at_ms: child.min_entry_at_ms,
                            finished_at_ms: child.max_finish_at_ms,
                            duration_ms: compute_duration(
                                child.min_entry_at_ms,
                                child.max_finish_at_ms,
                            ),
                            usage: child.usage,
                            summary: child.summary,
                        };
                        parent.folded_children.push(call);
                    }
                    continue;
                }
            }
        }

        // Folding rejected: warn without exposing the raw parent_request_id hint and keep standalone.
        push_warning(
            warnings,
            "Rejected unverified or mismatched parent request hint; kept request standalone.",
        );
        child.parent_request_id = ScopedField::new();
        let standalone_id = allocate_standalone_id(&child, &root_groups);
        child.request_id = standalone_id.clone();
        child.call_id = None;
        root_groups.insert(standalone_id, child);
    }

    root_groups
}

fn select_unique_workflow_plan_wrapper_children(
    root_groups: &BTreeMap<String, CollapsedJournalGroup<'_>>,
    candidate_children: &[CollapsedJournalGroup<'_>],
    warnings: &mut Vec<String>,
) -> BTreeMap<usize, String> {
    let mut approved = BTreeMap::new();

    for (parent_request_id, parent) in root_groups {
        if !is_valid_context_workflow_plan_parent(parent) {
            continue;
        }

        let (Some(p_bind), Some(p_thread)) =
            (parent.binding_id.resolved(), parent.thread_uuid.resolved())
        else {
            continue;
        };

        let matching_indices: Vec<usize> = candidate_children
            .iter()
            .enumerate()
            .filter_map(|(idx, child)| {
                if child.request_id == *parent_request_id
                    && is_context_workflow_plan_pair(&parent.operation, &child.operation)
                    && child.binding_id.resolved() == Some(p_bind)
                    && child.thread_uuid.resolved() == Some(p_thread)
                    && is_verified_workflow_plan_child(child)
                {
                    Some(idx)
                } else {
                    None
                }
            })
            .collect();

        if matching_indices.len() > 1 {
            push_warning(
                warnings,
                "Rejected ambiguous workflow plan calls sharing a context wrapper request; kept rows standalone.",
            );
            continue;
        }

        if let Some(&child_idx) = matching_indices.first() {
            let child = &candidate_children[child_idx];
            if can_fold_workflow_plan_child_into_context_parent(parent, child) {
                approved.insert(child_idx, parent_request_id.clone());
            }
        }
    }

    approved
}

fn is_context_workflow_plan_pair(parent_op: &str, child_op: &str) -> bool {
    matches!(
        (parent_op, child_op),
        ("context.workflow.get-plan", "workflow.get-plan")
            | ("context.workflow.save-draft", "workflow.save-draft")
            | ("context.workflow.publish", "workflow.publish")
    )
}

fn is_context_workflow_plan_op(operation: &str) -> bool {
    matches!(
        operation,
        "context.workflow.get-plan" | "context.workflow.save-draft" | "context.workflow.publish"
    )
}

fn is_workflow_plan_cli_op(operation: &str) -> bool {
    matches!(
        operation,
        "workflow.get-plan" | "workflow.save-draft" | "workflow.publish"
    )
}

fn is_valid_context_workflow_plan_parent(parent: &CollapsedJournalGroup<'_>) -> bool {
    is_context_workflow_plan_op(&parent.operation)
        && !parent.request_id.is_empty()
        && parent.call_id.is_none()
        && !parent.has_scope_conflict()
        && parent.parent_request_id.resolved().is_none()
        && parent.binding_id.resolved().is_some()
        && parent.thread_uuid.resolved().is_some()
        && parent.has_entry
        && parent.has_finish
        && parent.all_finishes_completed
        && parent.latest_finish_state == Some(ObservationCallState::Completed)
        && !parent.has_verified_worker_link_scope
        && parent.verified_job_id.is_none()
        && parent.verified_task_id.is_none()
        && parent.verified_approval_id.is_none()
        && parent.raw_job_id.resolved().is_none()
        && parent.finish_job_id.resolved().is_none()
        && parent.raw_task_id.resolved().is_none()
        && parent.raw_approval_id.resolved().is_none()
}

fn is_verified_workflow_plan_child(child: &CollapsedJournalGroup<'_>) -> bool {
    if !is_workflow_plan_cli_op(&child.operation)
        || child.request_id.is_empty()
        || child.call_id.is_none()
        || child.has_scope_conflict()
        || child.parent_request_id.resolved().is_some()
        || child.binding_id.resolved().is_none()
        || child.thread_uuid.resolved().is_none()
        || !child.has_entry
        || !child.has_finish
        || !child.all_finishes_completed
        || child.latest_finish_state != Some(ObservationCallState::Completed)
        || child.has_verified_worker_link_scope
        || child.verified_task_id.is_some()
        || child.verified_approval_id.is_some()
        || child.raw_task_id.resolved().is_some()
        || child.raw_approval_id.resolved().is_some()
    {
        return false;
    }

    let Some(verified_job_id) = child.verified_job_id.as_ref() else {
        return false;
    };
    child.finish_job_id.resolved() == Some(verified_job_id)
        && child.raw_job_id.resolved() == Some(verified_job_id)
}

fn can_fold_workflow_plan_child_into_context_parent(
    parent: &CollapsedJournalGroup<'_>,
    child: &CollapsedJournalGroup<'_>,
) -> bool {
    if !is_valid_context_workflow_plan_parent(parent)
        || !is_verified_workflow_plan_child(child)
        || !is_context_workflow_plan_pair(&parent.operation, &child.operation)
        || parent.request_id != child.request_id
        || parent.binding_id.resolved() != child.binding_id.resolved()
        || parent.thread_uuid.resolved() != child.thread_uuid.resolved()
    {
        return false;
    }

    let Some(child_job_id) = child.verified_job_id.as_ref() else {
        return false;
    };

    if let Some(parent_plan_id) = parent.raw_plan_id.resolved() {
        if parent_plan_id != child_job_id {
            return false;
        }
    }

    let (Some(p_start), Some(p_finish), Some(c_start), Some(c_finish)) = (
        parent.min_entry_at_ms,
        parent.max_finish_at_ms,
        child.min_entry_at_ms,
        child.max_finish_at_ms,
    ) else {
        return false;
    };

    p_start <= p_finish && c_start <= c_finish && c_start >= p_start && c_finish <= p_finish
}

fn allocate_standalone_id(
    child: &CollapsedJournalGroup<'_>,
    existing_roots: &BTreeMap<String, CollapsedJournalGroup<'_>>,
) -> String {
    let base = match &child.call_id {
        Some(cid) if existing_roots.contains_key(&child.request_id) => {
            format!("{}:{cid}", child.request_id)
        }
        _ => child.request_id.clone(),
    };
    if !existing_roots.contains_key(&base) {
        return base;
    }
    let mut suffix = 2usize;
    loop {
        let candidate = format!("{base}:{suffix}");
        if !existing_roots.contains_key(&candidate) {
            return candidate;
        }
        suffix += 1;
    }
}

fn can_fold_child_into_parent(
    parent: &CollapsedJournalGroup<'_>,
    child: &CollapsedJournalGroup<'_>,
) -> bool {
    if is_context_workflow_plan_op(&parent.operation) && is_workflow_plan_cli_op(&child.operation) {
        return false;
    }

    if parent.has_scope_conflict() || child.has_scope_conflict() {
        return false;
    }

    let (Some(p_bind), Some(c_bind), Some(p_thread), Some(c_thread)) = (
        parent.binding_id.resolved(),
        child.binding_id.resolved(),
        parent.thread_uuid.resolved(),
        child.thread_uuid.resolved(),
    ) else {
        return false;
    };

    if p_bind != c_bind || p_thread != c_thread {
        return false;
    }

    if parent.verified_job_id != child.verified_job_id
        || parent.verified_task_id != child.verified_task_id
        || parent.verified_approval_id != child.verified_approval_id
    {
        return false;
    }

    if parent.raw_job_id.resolved() != child.raw_job_id.resolved()
        || parent.raw_task_id.resolved() != child.raw_task_id.resolved()
        || parent.raw_approval_id.resolved() != child.raw_approval_id.resolved()
        || parent.raw_plan_id.resolved() != child.raw_plan_id.resolved()
    {
        return false;
    }

    true
}

fn is_same_root_dispatch_wrapper(
    parent: &CollapsedJournalGroup<'_>,
    child: &CollapsedJournalGroup<'_>,
) -> bool {
    is_allowlisted_dispatch_op(&parent.operation)
        && is_allowlisted_dispatch_op(&child.operation)
        && parent.has_verified_worker_link_scope
        && child.has_verified_worker_link_scope
        && parent.verified_job_id.is_some()
        && parent.verified_task_id.is_some()
        && parent.verified_approval_id.is_some()
}

fn merge_dispatch_wrapper_into_root<'a>(
    parent: &mut CollapsedJournalGroup<'a>,
    child: CollapsedJournalGroup<'a>,
) {
    if child.earliest_at_ms < parent.earliest_at_ms {
        parent.earliest_at_ms = child.earliest_at_ms;
    }
    if child.has_entry {
        parent.has_entry = true;
        if let Some(child_entry) = child.min_entry_at_ms {
            parent.min_entry_at_ms = Some(
                parent
                    .min_entry_at_ms
                    .map_or(child_entry, |prev| prev.min(child_entry)),
            );
        }
    }
    if child.has_finish {
        if !parent.has_finish {
            parent.has_finish = true;
            parent.all_finishes_completed = child.all_finishes_completed;
            parent.latest_finish_state = child.latest_finish_state;
        } else {
            parent.all_finishes_completed =
                parent.all_finishes_completed && child.all_finishes_completed;
        }
        if let Some(child_finish) = child.max_finish_at_ms {
            parent.max_finish_at_ms = Some(
                parent
                    .max_finish_at_ms
                    .map_or(child_finish, |prev| prev.max(child_finish)),
            );
        }
    }
    if child.updated_at_ms > parent.updated_at_ms {
        parent.updated_at_ms = child.updated_at_ms;
    }
    if let Some(latest_ts) =
        max_worker_and_delivery_timestamp(Some(parent.updated_at_ms), None, &child.delivery)
    {
        parent.updated_at_ms = latest_ts;
    }
    parent.delivery.extend(child.delivery);
}

type WorkerIndex = (
    BTreeMap<String, WorkerRecordView>,
    BTreeSet<String>,
    BTreeMap<String, BTreeSet<String>>,
);

fn index_core_workers(
    core_states: &[CoreStateView],
    verified_links: &[VerifiedWorkerLink],
    warnings: &mut Vec<String>,
) -> WorkerIndex {
    let mut workers_by_id: BTreeMap<String, WorkerRecordView> = BTreeMap::new();
    let mut conflicted_worker_ids: BTreeSet<String> = BTreeSet::new();
    let mut runs_to_workers: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();

    for state in core_states {
        for worker in &state.workers {
            let (Some(worker_id), Some(run_id), Some(binding_id)) = (
                clean_id(&worker.worker_id),
                clean_id(&worker.run_id),
                clean_id(&worker.binding_id),
            ) else {
                push_warning(
                    warnings,
                    "Skipped core worker record with invalid identifier fields.",
                );
                continue;
            };

            runs_to_workers
                .entry(run_id.clone())
                .or_default()
                .insert(worker_id.clone());

            if conflicted_worker_ids.contains(&worker_id) {
                continue;
            }

            let normalized = WorkerRecordView {
                worker_id: worker_id.clone(),
                run_id,
                binding_id,
                revision: worker.revision,
                status: worker.status.trim().to_string(),
                provider: worker.provider.clone(),
                requested_model: worker.requested_model.clone(),
                requested_effort: worker.requested_effort.clone(),
                observed_model: worker.observed_model.clone(),
                observed_effort: worker.observed_effort.clone(),
                created_at_ms: worker.created_at_ms,
                updated_at_ms: worker.updated_at_ms,
                started_at_ms: worker.started_at_ms,
                completed_at_ms: worker.completed_at_ms,
            };

            if let Some(existing) = workers_by_id.get(&worker_id) {
                if existing.binding_id != normalized.binding_id
                    || existing.run_id != normalized.run_id
                    || existing.created_at_ms != normalized.created_at_ms
                {
                    workers_by_id.remove(&worker_id);
                    conflicted_worker_ids.insert(worker_id);
                    push_warning(
                        warnings,
                        "Rejected core worker record with conflicting snapshot identity.",
                    );
                    continue;
                }
                if (normalized.updated_at_ms, normalized.revision)
                    >= (existing.updated_at_ms, existing.revision)
                {
                    workers_by_id.insert(worker_id, normalized);
                }
            } else {
                workers_by_id.insert(worker_id, normalized);
            }
        }
    }

    for vlink in verified_links {
        if let Some(wv) = workers_by_id.get(&vlink.worker_id) {
            runs_to_workers
                .entry(wv.run_id.clone())
                .or_default()
                .insert(vlink.worker_id.clone());
        }
    }

    (workers_by_id, conflicted_worker_ids, runs_to_workers)
}

fn binding_thread_conflicts(
    core_states: &[CoreStateView],
    binding_id: &str,
    expected_thread_uuid: &str,
) -> bool {
    for state in core_states {
        for binding in &state.bindings {
            if clean_id(&binding.binding_id).as_deref() == Some(binding_id)
                && clean_id(&binding.thread_uuid).as_deref() != Some(expected_thread_uuid)
            {
                return true;
            }
        }
    }
    false
}

fn has_unique_core_binding_thread(
    core_states: &[CoreStateView],
    binding_id: &str,
    expected_thread_uuid: &str,
) -> bool {
    let mut observed_threads = BTreeSet::new();
    for state in core_states {
        for binding in &state.bindings {
            if clean_id(&binding.binding_id).as_deref() == Some(binding_id) {
                let Some(thread_uuid) = clean_id(&binding.thread_uuid) else {
                    return false;
                };
                observed_threads.insert(thread_uuid);
            }
        }
    }
    observed_threads.len() == 1 && observed_threads.contains(expected_thread_uuid)
}

fn root_matches_link_scope(root: &CollapsedJournalGroup<'_>, vlink: &VerifiedWorkerLink) -> bool {
    if root.has_scope_conflict() || !root.has_verified_worker_link_scope {
        return false;
    }
    if root.verified_job_id.as_deref() != Some(vlink.job_id.as_str()) {
        return false;
    }
    if root.raw_job_id.resolved().map(String::as_str) != Some(vlink.job_id.as_str())
        || root.raw_task_id.resolved().map(String::as_str) != Some(vlink.task_id.as_str())
        || root.raw_approval_id.resolved().map(String::as_str) != Some(vlink.approval_id.as_str())
        || root.binding_id.resolved().map(String::as_str) != Some(vlink.control_binding_id.as_str())
        || root.thread_uuid.resolved().map(String::as_str) != Some(vlink.thread_uuid.as_str())
    {
        return false;
    }
    if let (Some(job_plan), Some(rec_plan)) =
        (vlink.job_plan_id.as_ref(), root.raw_plan_id.resolved())
    {
        if job_plan != rec_plan {
            return false;
        }
    }
    true
}

fn root_matches_completed_dispatch(
    root: &CollapsedJournalGroup<'_>,
    vlink: &VerifiedWorkerLink,
    worker: &WorkerRecordView,
) -> bool {
    if !is_allowlisted_dispatch_op(&root.operation) {
        return false;
    }
    if !root_matches_link_scope(root, vlink) {
        return false;
    }
    if !root.has_entry
        || !root.has_finish
        || !root.all_finishes_completed
        || root.latest_finish_state != Some(ObservationCallState::Completed)
    {
        return false;
    }
    let (Some(entry_ms), Some(finish_ms)) = (root.min_entry_at_ms, root.max_finish_at_ms) else {
        return false;
    };
    if entry_ms > finish_ms {
        return false;
    }
    worker.created_at_ms >= entry_ms && worker.created_at_ms <= finish_ms
}

fn is_allowlisted_dispatch_op(operation: &str) -> bool {
    matches!(
        operation,
        "session.dispatch"
            | "context.session.dispatch"
            | "flow.handoff"
            | "context.flow.handoff"
            | "mcp.paired.start_approved_task"
            | "mcp.scoped.start_worker"
    )
}

fn is_allowlisted_plan_result_op(operation: &str) -> bool {
    matches!(
        operation,
        "workflow.get-plan"
            | "workflow.save-draft"
            | "workflow.publish"
            | "mcp.paired.get_plan"
            | "mcp.paired.save_draft"
            | "mcp.paired.publish"
    )
}

fn resolve_worker_state(
    vlink: &VerifiedWorkerLink,
    worker: Option<&WorkerRecordView>,
) -> ObservationCallState {
    if let Some(w) = worker {
        match w.status.as_str() {
            "completed" => {
                if vlink.task_status == "ready_for_review"
                    || vlink.job_review_state.as_deref() == Some("pending_review")
                {
                    return ObservationCallState::ReadyForReview;
                }
                return ObservationCallState::Completed;
            }
            "ready_for_review" => return ObservationCallState::ReadyForReview,
            "failed" | "timed_out" | "rejected_by_diff_gate" | "execution_failed" => {
                return ObservationCallState::Failed;
            }
            "cancelled" => return ObservationCallState::Cancelled,
            "unknown" => return ObservationCallState::Unknown,
            _ => {}
        }
    }
    if vlink.live {
        ObservationCallState::Running
    } else {
        ObservationCallState::Unknown
    }
}

fn build_worker_executor(
    role: Option<&str>,
    worker: Option<&WorkerRecordView>,
) -> Option<ObservationExecutor> {
    let clean_role = sanitize_opt(role);
    let (provider, requested_model, observed_model, requested_effort, observed_effort) =
        if let Some(w) = worker {
            (
                sanitize_non_empty(&w.provider),
                sanitize_non_empty(&w.requested_model),
                sanitize_opt(w.observed_model.as_deref()),
                sanitize_non_empty(&w.requested_effort),
                sanitize_opt(w.observed_effort.as_deref()),
            )
        } else {
            (None, None, None, None, None)
        };

    if clean_role.is_none()
        && provider.is_none()
        && requested_model.is_none()
        && observed_model.is_none()
        && requested_effort.is_none()
        && observed_effort.is_none()
    {
        None
    } else {
        Some(ObservationExecutor {
            tool: None,
            provider,
            role: clean_role,
            requested_model,
            observed_model,
            requested_effort,
            observed_effort,
        })
    }
}

fn resolve_journal_group_state(
    group: &CollapsedJournalGroup<'_>,
    active_verified_worker_receipt: bool,
) -> ObservationCallState {
    if group.has_finish {
        if let Some(ref finish_state) = group.latest_finish_state {
            if !finish_state.is_active() {
                return *finish_state;
            }
        }
    }

    if group.operation.starts_with("mcp.") {
        if active_verified_worker_receipt {
            return group.in_flight_probe_record.resolve_in_flight_state(true);
        }
        return ObservationCallState::Unknown;
    }

    let has_live_instance = group.in_flight_probe_record.has_live_process_instance();
    if !active_verified_worker_receipt && !has_live_instance {
        return ObservationCallState::Unknown;
    }

    group
        .in_flight_probe_record
        .resolve_in_flight_state(active_verified_worker_receipt)
}

fn collect_core_deliveries(
    core_states: &[CoreStateView],
    vlink: &VerifiedWorkerLink,
    worker: &WorkerRecordView,
    runs_to_workers: &BTreeMap<String, BTreeSet<String>>,
    warnings: &mut Vec<String>,
) -> Vec<ObservationDelivery> {
    let is_shared_run = runs_to_workers
        .get(&worker.run_id)
        .is_some_and(|set| set.len() > 1);

    if is_shared_run {
        let has_run_events = core_states.iter().any(|s| {
            s.events.iter().any(|ev| {
                clean_id(&ev.binding_id).as_deref() == Some(vlink.session_binding_id.as_str())
                    && clean_id(&ev.run_id).as_deref() == Some(worker.run_id.as_str())
            })
        });
        if has_run_events {
            push_warning(
                warnings,
                "Skipped ambiguous core delivery events for run shared by multiple workers.",
            );
        }
        return Vec::new();
    }

    let mut deliveries = Vec::new();

    for state in core_states {
        let matching_bindings: Vec<&ReadonlyBindingRecord> = state
            .bindings
            .iter()
            .filter(|b| {
                clean_id(&b.binding_id).as_deref() == Some(vlink.session_binding_id.as_str())
            })
            .collect();
        if matching_bindings.len() != 1
            || clean_id(&matching_bindings[0].thread_uuid).as_deref()
                != Some(vlink.thread_uuid.as_str())
        {
            continue;
        }

        let has_matching_worker = state.workers.iter().any(|w| {
            clean_id(&w.worker_id).as_deref() == Some(vlink.worker_id.as_str())
                && clean_id(&w.binding_id).as_deref() == Some(vlink.session_binding_id.as_str())
                && clean_id(&w.run_id).as_deref() == Some(worker.run_id.as_str())
        });
        if !has_matching_worker {
            continue;
        }

        let has_matching_run = state.runs.iter().any(|r| {
            clean_id(&r.run_id).as_deref() == Some(worker.run_id.as_str())
                && clean_id(&r.binding_id).as_deref() == Some(vlink.session_binding_id.as_str())
        });
        if !has_matching_run {
            continue;
        }

        for ev in &state.events {
            if clean_id(&ev.binding_id).as_deref() != Some(vlink.session_binding_id.as_str())
                || clean_id(&ev.run_id).as_deref() != Some(worker.run_id.as_str())
                || ev.created_at_ms < worker.created_at_ms
            {
                continue;
            }

            let kind = ev.kind.trim();
            if matches!(kind, "worker_failed" | "worker_timed_out") {
                deliveries.push(ObservationDelivery {
                    stage: ObservationDeliveryStage::Failed,
                    at_ms: Some(ev.created_at_ms),
                    summary: sanitize_observation_summary(&format!(
                        "Core event seq {} ({kind}) recorded for run; not a read receipt.",
                        ev.seq
                    )),
                    recipient: Some(vlink.session_binding_id.clone()),
                });
            } else if kind == "worker_unknown" {
                deliveries.push(ObservationDelivery {
                    stage: ObservationDeliveryStage::Unknown,
                    at_ms: Some(ev.created_at_ms),
                    summary: sanitize_observation_summary(&format!(
                        "Core event seq {} ({kind}) recorded for run; not a read receipt.",
                        ev.seq
                    )),
                    recipient: Some(vlink.session_binding_id.clone()),
                });
            } else if ev.important
                || matches!(
                    kind,
                    "worker_completed"
                        | "run_completed"
                        | "question_asked"
                        | "worker_cancelled"
                        | "run_cancelled"
                )
            {
                deliveries.push(ObservationDelivery {
                    stage: ObservationDeliveryStage::ResultReady,
                    at_ms: Some(ev.created_at_ms),
                    summary: sanitize_observation_summary(&format!(
                        "Core result event seq {} ({kind}) ready; not a read receipt.",
                        ev.seq
                    )),
                    recipient: Some(vlink.session_binding_id.clone()),
                });
            }

            for batch in &state.event_batches {
                if clean_id(&batch.binding_id).as_deref() != Some(vlink.session_binding_id.as_str())
                    || batch.event_count == 0
                    || batch.from_seq > batch.to_seq
                    || ev.seq < batch.from_seq
                    || ev.seq > batch.to_seq
                {
                    continue;
                }

                let batch_id =
                    sanitize_non_empty(&batch.batch_id).unwrap_or_else(|| "batch".to_string());
                deliveries.push(ObservationDelivery {
                    stage: ObservationDeliveryStage::BatchDelivered,
                    at_ms: Some(batch.created_at_ms),
                    summary: sanitize_observation_summary(&format!(
                        "Event batch {batch_id} delivered covering seq {}; batch delivery only, not a read receipt.",
                        ev.seq
                    )),
                    recipient: Some(vlink.session_binding_id.clone()),
                });

                if let Some(acked_at_ms) = batch.acked_at_ms {
                    deliveries.push(ObservationDelivery {
                        stage: ObservationDeliveryStage::BatchAcknowledged,
                        at_ms: Some(acked_at_ms),
                        summary: sanitize_observation_summary(&format!(
                            "Event batch {batch_id} acknowledged covering seq {}; batch ACK only, not a read receipt.",
                            ev.seq
                        )),
                        recipient: Some(vlink.session_binding_id.clone()),
                    });
                }
            }

            if ev.important && ev.seq > 0 {
                for wakeup in &state.wakeups {
                    if clean_id(&wakeup.binding_id).as_deref()
                        != Some(vlink.session_binding_id.as_str())
                        || clean_id(&wakeup.thread_uuid).as_deref()
                            != Some(vlink.thread_uuid.as_str())
                    {
                        continue;
                    }

                    let covered = wakeup.covered_up_to_seq >= ev.seq
                        && wakeup.latest_important_seq >= ev.seq
                        && wakeup
                            .pending_since_seq
                            .map_or(ev.seq == wakeup.covered_up_to_seq, |since| ev.seq >= since);
                    if !covered {
                        continue;
                    }

                    let status = wakeup.status.trim();
                    // "idle", "pending", and "satisfied_by_ack" do not represent a submitted notification.
                    let stage = match status {
                        "succeeded" => Some(ObservationDeliveryStage::NotificationSubmitted),
                        "failed" => Some(ObservationDeliveryStage::Failed),
                        "unknown" | "in_flight" => Some(ObservationDeliveryStage::Unknown),
                        _ => None,
                    };

                    if let Some(stage) = stage {
                        deliveries.push(ObservationDelivery {
                            stage,
                            at_ms: wakeup.resolved_at_ms.or(wakeup.claimed_at_ms),
                            summary: sanitize_observation_summary(&format!(
                                "Wakeup status {status} covering important event seq {}; notification state only, not a read receipt.",
                                ev.seq
                            )),
                            recipient: Some(vlink.session_binding_id.clone()),
                        });
                    }
                }
            }
        }
    }

    deliveries
}

fn finalize_children(
    children: Vec<ObservationCall>,
    warnings: &mut Vec<String>,
) -> (Vec<ObservationCall>, usize) {
    let mut unique_by_id: BTreeMap<String, ObservationCall> = BTreeMap::new();
    for call in children {
        unique_by_id.insert(call.id.clone(), call);
    }

    let mut deduped: Vec<ObservationCall> = unique_by_id.into_values().collect();
    deduped.sort_by(|a, b| {
        a.started_at_ms
            .unwrap_or(i64::MAX)
            .cmp(&b.started_at_ms.unwrap_or(i64::MAX))
            .then_with(|| {
                a.finished_at_ms
                    .unwrap_or(i64::MAX)
                    .cmp(&b.finished_at_ms.unwrap_or(i64::MAX))
            })
            .then_with(|| a.id.cmp(&b.id))
    });

    let total_unique = deduped.len();
    if total_unique > MAX_REQUEST_CHILDREN {
        deduped.truncate(MAX_REQUEST_CHILDREN);
        push_warning(
            warnings,
            "Truncated request child calls to 64 entries; children_count reflects the full unique count.",
        );
    }

    (deduped, total_unique)
}

fn dedup_and_sort_deliveries(deliveries: &mut Vec<ObservationDelivery>) {
    deliveries.sort_by(|a, b| {
        a.at_ms
            .unwrap_or(i64::MAX)
            .cmp(&b.at_ms.unwrap_or(i64::MAX))
            .then_with(|| delivery_stage_rank(&a.stage).cmp(&delivery_stage_rank(&b.stage)))
            .then_with(|| a.recipient.cmp(&b.recipient))
            .then_with(|| a.summary.cmp(&b.summary))
    });
    deliveries.dedup();
}

fn merge_actor(previous: Option<ObservationActor>, next: &ObservationActor) -> ObservationActor {
    let prev = previous.unwrap_or_else(|| ObservationActor::unknown(None));
    ObservationActor {
        label: sanitize_opt(next.label.as_deref()).or(prev.label),
        role: sanitize_opt(next.role.as_deref()).or(prev.role),
        source: sanitize_opt(next.source.as_deref()).or(prev.source),
        model: sanitize_opt(next.model.as_deref()).or(prev.model),
        effort: sanitize_opt(next.effort.as_deref()).or(prev.effort),
        selected_model: sanitize_observation_selection_token(next.selected_model.as_deref())
            .or(prev.selected_model),
        selected_effort: sanitize_observation_selection_token(next.selected_effort.as_deref())
            .or(prev.selected_effort),
        evidence: next.evidence,
    }
}

fn merge_executor(
    previous: Option<ObservationExecutor>,
    next: &ObservationExecutor,
) -> ObservationExecutor {
    let prev = previous.unwrap_or(ObservationExecutor {
        tool: None,
        provider: None,
        role: None,
        requested_model: None,
        observed_model: None,
        requested_effort: None,
        observed_effort: None,
    });
    ObservationExecutor {
        tool: sanitize_opt(next.tool.as_deref()).or(prev.tool),
        provider: sanitize_opt(next.provider.as_deref()).or(prev.provider),
        role: sanitize_opt(next.role.as_deref()).or(prev.role),
        requested_model: sanitize_opt(next.requested_model.as_deref()).or(prev.requested_model),
        observed_model: sanitize_opt(next.observed_model.as_deref()).or(prev.observed_model),
        requested_effort: sanitize_opt(next.requested_effort.as_deref()).or(prev.requested_effort),
        observed_effort: sanitize_opt(next.observed_effort.as_deref()).or(prev.observed_effort),
    }
}

fn sanitize_usage(usage: &ObservationUsage) -> ObservationUsage {
    ObservationUsage {
        input_tokens: usage.input_tokens,
        output_tokens: usage.output_tokens,
        cache_read_tokens: usage.cache_read_tokens,
        total_tokens: usage.total_tokens,
        provenance: sanitize_non_empty(&usage.provenance).unwrap_or_else(|| "unknown".to_string()),
    }
}

fn sanitize_delivery(delivery: &ObservationDelivery) -> ObservationDelivery {
    ObservationDelivery {
        stage: delivery.stage,
        at_ms: delivery.at_ms,
        summary: sanitize_observation_summary(&delivery.summary),
        recipient: sanitize_opt(delivery.recipient.as_deref()),
    }
}

fn compute_duration(started_at_ms: Option<i64>, finished_at_ms: Option<i64>) -> Option<i64> {
    match (started_at_ms, finished_at_ms) {
        (Some(start), Some(finish)) if finish >= start => Some(finish - start),
        _ => None,
    }
}

fn max_worker_and_delivery_timestamp(
    initial: Option<i64>,
    worker: Option<&WorkerRecordView>,
    deliveries: &[ObservationDelivery],
) -> Option<i64> {
    let mut latest = initial;
    let mut observe = |candidate: Option<i64>| {
        if let Some(ts) = candidate {
            latest = Some(latest.map_or(ts, |prev| prev.max(ts)));
        }
    };
    if let Some(wv) = worker {
        observe(Some(wv.created_at_ms));
        observe(Some(wv.updated_at_ms));
        observe(wv.started_at_ms);
        observe(wv.completed_at_ms);
    }
    for d in deliveries {
        observe(d.at_ms);
    }
    latest
}

fn phase_rank(phase: &ObservationRecordPhase) -> u8 {
    match phase {
        ObservationRecordPhase::Entry => 0,
        ObservationRecordPhase::Progress => 1,
        ObservationRecordPhase::Finish => 2,
    }
}

fn delivery_stage_rank(stage: &ObservationDeliveryStage) -> u8 {
    match stage {
        ObservationDeliveryStage::ResultReady => 0,
        ObservationDeliveryStage::NotificationSubmitted => 1,
        ObservationDeliveryStage::BatchDelivered => 2,
        ObservationDeliveryStage::BatchAcknowledged => 3,
        ObservationDeliveryStage::ResponseReturned => 4,
        ObservationDeliveryStage::Unknown => 5,
        ObservationDeliveryStage::Failed => 6,
    }
}

fn clean_id(value: &str) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() || trimmed.chars().any(char::is_control) {
        None
    } else {
        Some(trimmed.to_string())
    }
}

fn clean_opt_id(value: Option<&str>) -> Option<String> {
    value.and_then(clean_id)
}

fn sanitize_non_empty(value: &str) -> Option<String> {
    let sanitized = sanitize_observation_summary(value.trim());
    let trimmed = sanitized.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

fn sanitize_opt(value: Option<&str>) -> Option<String> {
    value.and_then(sanitize_non_empty)
}

fn push_warning(warnings: &mut Vec<String>, message: &str) {
    let sanitized = sanitize_observation_summary(message);
    if !sanitized.is_empty() && !warnings.contains(&sanitized) {
        warnings.push(sanitized);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use threadlane_core::observation::{
        ObservationActorEvidence, ObservationJobState, ObservationProcessInstance, ObservationTask,
    };

    const SOURCE_ID: &str = "src-main";
    const THREAD_UUID_A: &str = "11111111-1111-4111-8111-111111111111";
    const THREAD_UUID_B: &str = "22222222-2222-4222-8222-222222222222";

    fn sample_job(
        id: &str,
        binding_id: &str,
        thread_uuid: &str,
        plan_id: &str,
        task_id: &str,
    ) -> ObservationJob {
        ObservationJob {
            source_id: SOURCE_ID.to_string(),
            id: id.to_string(),
            project_label: "Threadlane".to_string(),
            project_path: Some("/workspace/threadlane".to_string()),
            title: format!("Job {id}"),
            thread_uuid: Some(thread_uuid.to_string()),
            thread_label: Some("Main Chat".to_string()),
            binding_id: Some(binding_id.to_string()),
            plan_id: Some(plan_id.to_string()),
            plan_revision: Some(1),
            state: ObservationJobState::Running,
            approval_status: "approved".to_string(),
            execution_status: "running".to_string(),
            review_state: None,
            updated_at_ms: Some(1_000),
            active_call_count: 0,
            tasks: vec![ObservationTask {
                id: task_id.to_string(),
                title: format!("Task {task_id}"),
                status: "running".to_string(),
                depends_on: Vec::new(),
            }],
            summary: None,
        }
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "Keep control and session binding identities explicit in isolation fixtures"
    )]
    fn sample_link(
        worker_id: &str,
        job_id: &str,
        task_id: &str,
        approval_id: &str,
        control_binding_id: &str,
        session_binding_id: &str,
        thread_uuid: &str,
        live: bool,
    ) -> WorkerObservationLink {
        WorkerObservationLink {
            worker_id: worker_id.to_string(),
            job_id: job_id.to_string(),
            task_id: task_id.to_string(),
            approval_id: approval_id.to_string(),
            title: format!("Worker {worker_id}"),
            control_binding_id: control_binding_id.to_string(),
            session_binding_id: session_binding_id.to_string(),
            thread_uuid: thread_uuid.to_string(),
            role: Some("implementer".to_string()),
            live,
        }
    }

    fn base_journal_record(
        record_id: &str,
        request_id: &str,
        phase: ObservationRecordPhase,
        at_ms: i64,
        operation: &str,
        state: ObservationCallState,
    ) -> ObservationJournalRecord {
        ObservationJournalRecord {
            record_id: record_id.to_string(),
            request_id: request_id.to_string(),
            parent_request_id: None,
            call_id: None,
            phase,
            at_ms,
            pid: None,
            process_instance: None,
            project_label: Some("Threadlane".to_string()),
            project_path: None,
            binding_id: Some("bnd-1".to_string()),
            thread_uuid: Some(THREAD_UUID_A.to_string()),
            thread_label: Some("Main Chat".to_string()),
            job_id: None,
            plan_id: None,
            task_id: None,
            approval_id: None,
            operation: operation.to_string(),
            title: format!("Title {request_id}"),
            state,
            actor: None,
            executor: None,
            usage: None,
            delivery: Vec::new(),
            summary: None,
        }
    }

    // (a) Two approved jobs in one source/chat stay separate and Entry/Progress/Finish collapse
    // to unique newest-first rows.
    #[test]
    fn two_approved_jobs_in_same_chat_stay_separate_and_collapse_newest_first() {
        let job_1 = sample_job("job-1", "bnd-1", THREAD_UUID_A, "plan-1", "task-1");
        let job_2 = sample_job("job-2", "bnd-1", THREAD_UUID_A, "plan-2", "task-2");
        let link_1 = sample_link(
            "wrk-1",
            "job-1",
            "task-1",
            "apr-1",
            "bnd-1",
            "bnd-sess-1",
            THREAD_UUID_A,
            false,
        );
        let link_2 = sample_link(
            "wrk-2",
            "job-2",
            "task-2",
            "apr-2",
            "bnd-1",
            "bnd-sess-2",
            THREAD_UUID_A,
            false,
        );

        let mut req_1_entry = base_journal_record(
            "rec-1a",
            "req-1",
            ObservationRecordPhase::Entry,
            1_000,
            "session.dispatch",
            ObservationCallState::Running,
        );
        req_1_entry.job_id = Some("job-1".to_string());
        req_1_entry.plan_id = Some("plan-1".to_string());
        req_1_entry.task_id = Some("task-1".to_string());
        req_1_entry.approval_id = Some("apr-1".to_string());

        let mut req_1_progress = req_1_entry.clone();
        req_1_progress.record_id = "rec-1b".to_string();
        req_1_progress.phase = ObservationRecordPhase::Progress;
        req_1_progress.at_ms = 1_200;
        req_1_progress.summary = Some("Progress on job 1".to_string());

        let mut req_1_finish = req_1_progress.clone();
        req_1_finish.record_id = "rec-1c".to_string();
        req_1_finish.phase = ObservationRecordPhase::Finish;
        req_1_finish.at_ms = 1_500;
        req_1_finish.state = ObservationCallState::Completed;

        let mut req_2_entry = base_journal_record(
            "rec-2a",
            "req-2",
            ObservationRecordPhase::Entry,
            1_100,
            "session.dispatch",
            ObservationCallState::Running,
        );
        req_2_entry.job_id = Some("job-2".to_string());
        req_2_entry.plan_id = Some("plan-2".to_string());
        req_2_entry.task_id = Some("task-2".to_string());
        req_2_entry.approval_id = Some("apr-2".to_string());
        // Attempt to fold job-2 into job-1's request inside the same chat must be rejected.
        req_2_entry.parent_request_id = Some("req-1".to_string());

        let mut req_2_progress = req_2_entry.clone();
        req_2_progress.record_id = "rec-2b".to_string();
        req_2_progress.phase = ObservationRecordPhase::Progress;
        req_2_progress.at_ms = 1_800;

        let mut req_2_finish = req_2_progress.clone();
        req_2_finish.record_id = "rec-2c".to_string();
        req_2_finish.phase = ObservationRecordPhase::Finish;
        req_2_finish.at_ms = 2_000;
        req_2_finish.state = ObservationCallState::Completed;

        let projection = project_requests(
            SOURCE_ID,
            &[job_1, job_2],
            &[
                req_1_entry,
                req_1_progress,
                req_1_finish,
                req_2_entry,
                req_2_progress,
                req_2_finish,
            ],
            &[ReadonlyCoreStateSnapshot::default()],
            &[link_1, link_2],
        );

        let journal_requests: Vec<&ObservationRequest> = projection
            .requests
            .iter()
            .filter(|r| r.id == "req-1" || r.id == "req-2")
            .collect();
        assert_eq!(journal_requests.len(), 2);

        // Newest meaningful activity first: req-2 (updated 2000) before req-1 (updated 1500).
        assert_eq!(journal_requests[0].id, "req-2");
        assert_eq!(journal_requests[0].job_id.as_deref(), Some("job-2"));
        assert_eq!(journal_requests[0].parent_request_id, None);
        assert_eq!(journal_requests[0].started_at_ms, Some(1_100));
        assert_eq!(journal_requests[0].finished_at_ms, Some(2_000));
        assert_eq!(journal_requests[0].updated_at_ms, 2_000);
        assert_eq!(journal_requests[0].duration_ms, Some(900));
        assert_eq!(journal_requests[0].state, ObservationCallState::Completed);

        assert_eq!(journal_requests[1].id, "req-1");
        assert_eq!(journal_requests[1].job_id.as_deref(), Some("job-1"));
        assert_eq!(journal_requests[1].started_at_ms, Some(1_000));
        assert_eq!(journal_requests[1].finished_at_ms, Some(1_500));
        assert_eq!(journal_requests[1].updated_at_ms, 1_500);
        assert_eq!(journal_requests[1].duration_ms, Some(500));
        assert!(journal_requests[1].children.is_empty());
    }

    // (b) Cross-binding/thread env parent/job/task hints do not join or assign job.
    #[test]
    fn cross_binding_or_thread_hints_do_not_fold_or_assign_job() {
        let job_1 = sample_job("job-1", "bnd-1", THREAD_UUID_A, "plan-1", "task-1");
        let link_1 = sample_link(
            "wrk-1",
            "job-1",
            "task-1",
            "apr-1",
            "bnd-1",
            "bnd-sess-1",
            THREAD_UUID_A,
            false,
        );

        let mut parent = base_journal_record(
            "rec-p",
            "req-parent-secret-77",
            ObservationRecordPhase::Finish,
            1_000,
            "mcp.paired.status",
            ObservationCallState::Completed,
        );
        parent.binding_id = Some("bnd-1".to_string());
        parent.thread_uuid = Some(THREAD_UUID_A.to_string());

        let mut cross_child = base_journal_record(
            "rec-c",
            "req-child",
            ObservationRecordPhase::Finish,
            1_500,
            "session.status",
            ObservationCallState::Completed,
        );
        cross_child.parent_request_id = Some("req-parent-secret-77".to_string());
        cross_child.binding_id = Some("bnd-2".to_string());
        cross_child.thread_uuid = Some(THREAD_UUID_B.to_string());
        cross_child.job_id = Some("job-1".to_string());
        cross_child.plan_id = Some("plan-1".to_string());
        cross_child.task_id = Some("task-1".to_string());
        cross_child.approval_id = Some("apr-1".to_string());

        let projection = project_requests(
            SOURCE_ID,
            &[job_1],
            &[parent, cross_child],
            &[ReadonlyCoreStateSnapshot::default()],
            &[link_1],
        );

        let child_row = projection
            .requests
            .iter()
            .find(|r| r.id == "req-child")
            .expect("child should remain a standalone request");
        assert_eq!(child_row.parent_request_id, None);
        assert_eq!(child_row.job_id, None);

        let parent_row = projection
            .requests
            .iter()
            .find(|r| r.id == "req-parent-secret-77")
            .expect("parent request should exist");
        assert!(parent_row.children.is_empty());

        // Warnings must be present and must never leak the raw parent_request_id hint.
        assert!(!projection.warnings.is_empty());
        for warning in &projection.warnings {
            assert!(!warning.contains("req-parent-secret-77"));
        }
    }

    // (c) In-flight dead/bare PID and unfinished `mcp.*` resolve Unknown.
    #[test]
    fn in_flight_bare_or_dead_pid_and_unfinished_mcp_resolve_unknown() {
        let mut bare_pid_cli = base_journal_record(
            "rec-bare",
            "req-bare-pid",
            ObservationRecordPhase::Entry,
            1_000,
            "context.session.dispatch",
            ObservationCallState::Running,
        );
        bare_pid_cli.pid = Some(999_999);
        bare_pid_cli.process_instance = None;

        let mut dead_pid_cli = base_journal_record(
            "rec-dead",
            "req-dead-pid",
            ObservationRecordPhase::Entry,
            1_100,
            "session.dispatch",
            ObservationCallState::Running,
        );
        dead_pid_cli.pid = Some(u32::MAX);
        dead_pid_cli.process_instance = Some(ObservationProcessInstance {
            pid: u32::MAX,
            boot_id: "00000000-0000-0000-0000-000000000000".to_string(),
            start_ticks: u64::MAX,
        });

        let mut unfinished_mcp = base_journal_record(
            "rec-mcp",
            "req-unfinished-mcp",
            ObservationRecordPhase::Progress,
            1_200,
            "mcp.paired.start_approved_task",
            ObservationCallState::Running,
        );
        unfinished_mcp.pid = Some(42_424);

        let projection = project_requests(
            SOURCE_ID,
            &[],
            &[bare_pid_cli, dead_pid_cli, unfinished_mcp],
            &[ReadonlyCoreStateSnapshot::default()],
            &[],
        );

        assert_eq!(projection.requests.len(), 3);
        for req in &projection.requests {
            assert_eq!(
                req.state,
                ObservationCallState::Unknown,
                "request {} should resolve to Unknown",
                req.id
            );
        }
    }

    // (d) Synchronous `ResponseReturned` remains separate and generic binding-only event batch ACK
    // is not attached/read.
    #[test]
    fn synchronous_response_returned_preserved_and_generic_binding_ack_not_attached() {
        let mut sync_record = base_journal_record(
            "rec-sync",
            "req-sync",
            ObservationRecordPhase::Finish,
            2_000,
            "mcp.paired.status",
            ObservationCallState::Completed,
        );
        sync_record.delivery = vec![ObservationDelivery {
            stage: ObservationDeliveryStage::ResponseReturned,
            at_ms: Some(2_000),
            summary: "Synchronous MCP response returned.".to_string(),
            recipient: Some("bnd-1".to_string()),
        }];

        let snapshot = ReadonlyCoreStateSnapshot {
            bindings: vec![ReadonlyBindingRecord {
                binding_id: "bnd-1".to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                label: "Main Chat".to_string(),
                delivered_seq: 10,
                acked_seq: 10,
            }],
            runs: Vec::new(),
            workers: Vec::new(),
            events: Vec::new(),
            event_batches: vec![ReadonlyEventBatchRecord {
                batch_id: "bat-generic".to_string(),
                binding_id: "bnd-1".to_string(),
                from_seq: 1,
                to_seq: 10,
                event_count: 10,
                created_at_ms: 2_010,
                acked_at_ms: Some(2_020),
            }],
            wakeups: vec![ReadonlyWakeupRecord {
                binding_id: "bnd-1".to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                status: "succeeded".to_string(),
                claim_id: Some("wcl-1".to_string()),
                pending_since_seq: Some(1),
                latest_important_seq: 10,
                covered_up_to_seq: 10,
                claimed_at_ms: Some(2_005),
                resolved_at_ms: Some(2_006),
            }],
        };

        let projection = project_requests(SOURCE_ID, &[], &[sync_record], &[snapshot], &[]);
        assert_eq!(projection.requests.len(), 1);
        let req = &projection.requests[0];
        assert_eq!(req.delivery.len(), 1);
        assert_eq!(
            req.delivery[0].stage,
            ObservationDeliveryStage::ResponseReturned
        );
        assert_eq!(req.delivery[0].at_ms, Some(2_000));
    }

    // (e) Unmatched verified worker is an explicit Historical worker row with unknown actor and
    // no invented Luna parent.
    #[test]
    fn unmatched_verified_worker_becomes_historical_worker_row_with_unknown_actor() {
        let job = sample_job("job-1", "bnd-1", THREAD_UUID_A, "plan-1", "task-1");
        let mut link = sample_link(
            "wrk-hist-1",
            "job-1",
            "task-1",
            "apr-1",
            "bnd-1",
            "bnd-sess-hist-1",
            THREAD_UUID_A,
            false,
        );
        link.title = "Implement reducer".to_string();

        let projection = project_requests(
            SOURCE_ID,
            &[job],
            &[],
            &[ReadonlyCoreStateSnapshot::default()],
            &[link],
        );

        assert_eq!(projection.requests.len(), 1);
        let row = &projection.requests[0];
        assert_eq!(row.id, "wrk-hist-1");
        assert_eq!(row.job_id.as_deref(), Some("job-1"));
        assert_eq!(row.binding_id.as_deref(), Some("bnd-sess-hist-1"));
        assert_eq!(row.parent_request_id, None);
        assert_eq!(row.title, "Historical worker · Implement reducer");
        assert_eq!(row.state, ObservationCallState::Unknown);
        assert_eq!(row.actor, ObservationActor::unknown(None));
        assert_eq!(row.actor.evidence, ObservationActorEvidence::Unknown);
        assert_eq!(row.usage, None);
        assert!(row.children.is_empty());
    }

    #[test]
    fn wrapper_dispatch_deduplicates_root_and_attaches_worker_and_core_delivery() {
        let job = sample_job("job-1", "bnd-1", THREAD_UUID_A, "plan-1", "task-1");
        let link = sample_link(
            "wrk-1",
            "job-1",
            "task-1",
            "apr-1",
            "bnd-1",
            "bnd-sess-1",
            THREAD_UUID_A,
            true,
        );
        // Second link whose Core worker row only exists under B1 ("bnd-1") instead of its B2
        // ("bnd-sess-2") must not substitute B1 Core data for B2 session data.
        let wrong_binding_link = sample_link(
            "wrk-b1-only",
            "job-1",
            "task-1",
            "apr-1",
            "bnd-1",
            "bnd-sess-2",
            THREAD_UUID_A,
            true,
        );

        let mut outer_entry = base_journal_record(
            "rec-oe",
            "req-root",
            ObservationRecordPhase::Entry,
            1_000,
            "context.session.dispatch",
            ObservationCallState::Running,
        );
        outer_entry.job_id = Some("job-1".to_string());
        outer_entry.plan_id = Some("plan-1".to_string());
        outer_entry.task_id = Some("task-1".to_string());
        outer_entry.approval_id = Some("apr-1".to_string());
        outer_entry.actor = Some(ObservationActor {
            label: Some("Luna".to_string()),
            role: Some("coordinator".to_string()),
            source: Some("journal".to_string()),
            model: Some("gpt-5.4".to_string()),
            effort: Some("high".to_string()),
            selected_model: None,
            selected_effort: None,
            evidence: ObservationActorEvidence::Unknown,
        });

        let mut inner_finish = outer_entry.clone();
        inner_finish.record_id = "rec-if".to_string();
        inner_finish.request_id = "req-inner".to_string();
        inner_finish.parent_request_id = Some("req-root".to_string());
        inner_finish.phase = ObservationRecordPhase::Finish;
        inner_finish.at_ms = 1_900;
        inner_finish.operation = "session.dispatch".to_string();
        inner_finish.state = ObservationCallState::Completed;

        let mut outer_finish = outer_entry.clone();
        outer_finish.record_id = "rec-of".to_string();
        outer_finish.phase = ObservationRecordPhase::Finish;
        outer_finish.at_ms = 2_000;
        outer_finish.state = ObservationCallState::Completed;

        let core_view = CoreStateView {
            bindings: vec![
                ReadonlyBindingRecord {
                    binding_id: "bnd-1".to_string(),
                    thread_uuid: THREAD_UUID_A.to_string(),
                    label: "Main Chat".to_string(),
                    delivered_seq: 99,
                    acked_seq: 99,
                },
                ReadonlyBindingRecord {
                    binding_id: "bnd-sess-1".to_string(),
                    thread_uuid: THREAD_UUID_A.to_string(),
                    label: "Worker Session".to_string(),
                    delivered_seq: 5,
                    acked_seq: 5,
                },
                ReadonlyBindingRecord {
                    binding_id: "bnd-sess-2".to_string(),
                    thread_uuid: THREAD_UUID_A.to_string(),
                    label: "Other Worker Session".to_string(),
                    delivered_seq: 0,
                    acked_seq: 0,
                },
            ],
            runs: vec![
                ReadonlyRunRecord {
                    run_id: "run-1".to_string(),
                    binding_id: "bnd-sess-1".to_string(),
                    revision: 1,
                    status: "running".to_string(),
                    active_question_id: None,
                    started_at_ms: 1_500,
                    updated_at_ms: 2_500,
                    completed_at_ms: None,
                    cancelled_at_ms: None,
                },
                ReadonlyRunRecord {
                    run_id: "run-b1".to_string(),
                    binding_id: "bnd-1".to_string(),
                    revision: 1,
                    status: "running".to_string(),
                    active_question_id: None,
                    started_at_ms: 1_500,
                    updated_at_ms: 9_999,
                    completed_at_ms: None,
                    cancelled_at_ms: None,
                },
            ],
            workers: vec![
                WorkerRecordView {
                    worker_id: "wrk-1".to_string(),
                    run_id: "run-1".to_string(),
                    binding_id: "bnd-sess-1".to_string(),
                    revision: 1,
                    status: "running".to_string(),
                    provider: "agy".to_string(),
                    requested_model: "gemini-4-argon-high".to_string(),
                    requested_effort: "high".to_string(),
                    observed_model: Some("gemini-4-argon-high".to_string()),
                    observed_effort: Some("high".to_string()),
                    created_at_ms: 1_500,
                    updated_at_ms: 2_500,
                    started_at_ms: Some(1_510),
                    completed_at_ms: None,
                },
                WorkerRecordView {
                    worker_id: "wrk-b1-only".to_string(),
                    run_id: "run-b1".to_string(),
                    binding_id: "bnd-1".to_string(),
                    revision: 1,
                    status: "completed".to_string(),
                    provider: "agy".to_string(),
                    requested_model: "gemini-4-argon-high".to_string(),
                    requested_effort: "high".to_string(),
                    observed_model: None,
                    observed_effort: None,
                    created_at_ms: 1_500,
                    updated_at_ms: 9_999,
                    started_at_ms: Some(1_510),
                    completed_at_ms: Some(9_999),
                },
            ],
            events: vec![
                ReadonlyEventRecord {
                    event_id: "evt-1".to_string(),
                    binding_id: "bnd-sess-1".to_string(),
                    seq: 5,
                    run_id: "run-1".to_string(),
                    revision: 1,
                    kind: "worker_completed".to_string(),
                    important: true,
                    created_at_ms: 2_400,
                },
                ReadonlyEventRecord {
                    event_id: "evt-b1".to_string(),
                    binding_id: "bnd-1".to_string(),
                    seq: 99,
                    run_id: "run-1".to_string(),
                    revision: 1,
                    kind: "worker_completed".to_string(),
                    important: true,
                    created_at_ms: 9_999,
                },
            ],
            event_batches: vec![
                ReadonlyEventBatchRecord {
                    batch_id: "bat-5".to_string(),
                    binding_id: "bnd-sess-1".to_string(),
                    from_seq: 5,
                    to_seq: 5,
                    event_count: 1,
                    created_at_ms: 2_510,
                    acked_at_ms: Some(2_620),
                },
                ReadonlyEventBatchRecord {
                    batch_id: "bat-b1".to_string(),
                    binding_id: "bnd-1".to_string(),
                    from_seq: 1,
                    to_seq: 99,
                    event_count: 99,
                    created_at_ms: 9_990,
                    acked_at_ms: Some(9_999),
                },
            ],
            wakeups: vec![
                ReadonlyWakeupRecord {
                    binding_id: "bnd-sess-1".to_string(),
                    thread_uuid: THREAD_UUID_A.to_string(),
                    status: "succeeded".to_string(),
                    claim_id: Some("wcl-5".to_string()),
                    pending_since_seq: Some(5),
                    latest_important_seq: 5,
                    covered_up_to_seq: 5,
                    claimed_at_ms: Some(2_402),
                    resolved_at_ms: Some(2_405),
                },
                ReadonlyWakeupRecord {
                    binding_id: "bnd-1".to_string(),
                    thread_uuid: THREAD_UUID_A.to_string(),
                    status: "succeeded".to_string(),
                    claim_id: Some("wcl-b1".to_string()),
                    pending_since_seq: Some(1),
                    latest_important_seq: 99,
                    covered_up_to_seq: 99,
                    claimed_at_ms: Some(9_995),
                    resolved_at_ms: Some(9_999),
                },
            ],
        };

        let projection = project_requests_with_views(
            SOURCE_ID,
            &[job],
            &[outer_entry, inner_finish, outer_finish],
            &[core_view],
            &[link, wrong_binding_link],
        );

        let root = projection
            .requests
            .iter()
            .find(|r| r.id == "req-root")
            .expect("B1 dispatch root should exist");
        assert_eq!(root.binding_id.as_deref(), Some("bnd-1"));
        assert_eq!(root.state, ObservationCallState::Completed);
        assert_eq!(root.started_at_ms, Some(1_000));
        assert_eq!(root.finished_at_ms, Some(2_000));
        assert_eq!(root.duration_ms, Some(1_000));
        assert_eq!(root.updated_at_ms, 2_620);
        assert_eq!(root.children_count, 1);
        assert_eq!(root.children.len(), 1);
        assert_eq!(root.children[0].id, "wrk-1");
        assert_eq!(root.children[0].state, ObservationCallState::Running);
        assert!(root
            .delivery
            .iter()
            .all(|d| d.recipient.as_deref() == Some("bnd-sess-1")));
        assert!(root.delivery.iter().any(|d| d.stage
            == ObservationDeliveryStage::NotificationSubmitted
            && d.summary.contains("not a read receipt")));
        assert!(root
            .delivery
            .iter()
            .any(|d| d.stage == ObservationDeliveryStage::BatchAcknowledged
                && d.summary.contains("not a read receipt")));

        let rejected_hist = projection
            .requests
            .iter()
            .find(|r| r.id == "wrk-b1-only")
            .expect("worker whose Core row used B1 instead of B2 should not attach to root");
        assert_eq!(rejected_hist.binding_id.as_deref(), Some("bnd-sess-2"));
        assert_eq!(rejected_hist.started_at_ms, None);
        assert!(rejected_hist.delivery.is_empty());
    }

    #[test]
    fn children_capped_at_64_with_full_unique_children_count() {
        let mut records = Vec::new();
        let parent = base_journal_record(
            "rec-root",
            "req-many",
            ObservationRecordPhase::Finish,
            5_000,
            "mcp.paired.status",
            ObservationCallState::Completed,
        );
        records.push(parent);

        for idx in 0..70 {
            let mut sub = base_journal_record(
                &format!("rec-sub-{idx}"),
                "req-many",
                ObservationRecordPhase::Finish,
                5_000 + idx,
                "sub.call",
                ObservationCallState::Completed,
            );
            sub.call_id = Some(format!("call-{idx:03}"));
            records.push(sub);
        }

        let projection = project_requests(
            SOURCE_ID,
            &[],
            &records,
            &[ReadonlyCoreStateSnapshot::default()],
            &[],
        );

        assert_eq!(projection.requests.len(), 1);
        assert_eq!(projection.requests[0].children.len(), 64);
        assert_eq!(projection.requests[0].children_count, 70);
        assert!(projection
            .warnings
            .iter()
            .any(|w| w.contains("Truncated request child calls to 64")));
    }

    #[test]
    fn plan_result_associates_pending_plan_without_job_thread_only_via_unique_core_binding() {
        let mut pending_plan_job =
            sample_job("plan-100", "bnd-1", THREAD_UUID_A, "plan-100", "task-1");
        pending_plan_job.thread_uuid = None;
        pending_plan_job.approval_status = "pending".to_string();
        pending_plan_job.execution_status = "not_started".to_string();
        pending_plan_job.tasks.clear();

        let mut wf_entry = base_journal_record(
            "rec-wf-entry",
            "req-wf-save",
            ObservationRecordPhase::Entry,
            1_000,
            "workflow.save-draft",
            ObservationCallState::Running,
        );
        wf_entry.plan_id = Some("plan-100".to_string());

        let mut wf_finish = wf_entry.clone();
        wf_finish.record_id = "rec-wf-finish".to_string();
        wf_finish.phase = ObservationRecordPhase::Finish;
        wf_finish.at_ms = 1_100;
        wf_finish.state = ObservationCallState::Completed;
        wf_finish.job_id = Some("plan-100".to_string());

        let mcp_entry = base_journal_record(
            "rec-mcp-entry",
            "req-mcp-publish",
            ObservationRecordPhase::Entry,
            1_200,
            "mcp.paired.publish",
            ObservationCallState::Running,
        );
        let mut mcp_finish = mcp_entry.clone();
        mcp_finish.record_id = "rec-mcp-finish".to_string();
        mcp_finish.phase = ObservationRecordPhase::Finish;
        mcp_finish.at_ms = 1_300;
        mcp_finish.state = ObservationCallState::Completed;
        mcp_finish.job_id = Some("plan-100".to_string());
        mcp_finish.plan_id = Some("plan-100".to_string());

        let matching_binding_snapshot = ReadonlyCoreStateSnapshot {
            bindings: vec![ReadonlyBindingRecord {
                binding_id: "bnd-1".to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                label: "Main Chat".to_string(),
                delivered_seq: 0,
                acked_seq: 0,
            }],
            ..ReadonlyCoreStateSnapshot::default()
        };

        let linked_projection = project_requests(
            SOURCE_ID,
            std::slice::from_ref(&pending_plan_job),
            &[
                wf_entry.clone(),
                wf_finish.clone(),
                mcp_entry.clone(),
                mcp_finish.clone(),
            ],
            &[
                matching_binding_snapshot.clone(),
                matching_binding_snapshot.clone(),
            ],
            &[],
        );

        assert_eq!(linked_projection.requests.len(), 2);
        for req in &linked_projection.requests {
            assert_eq!(req.job_id.as_deref(), Some("plan-100"));
        }

        // Without a Core binding proving B/T when job.thread_uuid is None, stay unlinked.
        let unlinked_missing = project_requests(
            SOURCE_ID,
            std::slice::from_ref(&pending_plan_job),
            &[
                wf_entry.clone(),
                wf_finish.clone(),
                mcp_entry.clone(),
                mcp_finish.clone(),
            ],
            &[ReadonlyCoreStateSnapshot::default()],
            &[],
        );
        for req in &unlinked_missing.requests {
            assert_eq!(req.job_id, None);
        }

        // Conflicting Core binding thread mappings for bnd-1 must also reject the link.
        let conflicting_snapshot = ReadonlyCoreStateSnapshot {
            bindings: vec![ReadonlyBindingRecord {
                binding_id: "bnd-1".to_string(),
                thread_uuid: THREAD_UUID_B.to_string(),
                label: "Other Chat".to_string(),
                delivered_seq: 0,
                acked_seq: 0,
            }],
            ..ReadonlyCoreStateSnapshot::default()
        };
        let unlinked_conflict = project_requests(
            SOURCE_ID,
            &[pending_plan_job],
            &[wf_entry, wf_finish, mcp_entry, mcp_finish],
            &[matching_binding_snapshot, conflicting_snapshot],
            &[],
        );
        for req in &unlinked_conflict.requests {
            assert_eq!(req.job_id, None);
        }
    }

    #[test]
    fn plan_result_with_wrong_plan_or_binding_or_entry_only_hint_stays_unlinked() {
        let mut plan_job = sample_job("plan-100", "bnd-1", THREAD_UUID_A, "plan-100", "task-1");
        plan_job.approval_status = "pending".to_string();

        // 1. Mismatched record.plan_id vs finish job_id.
        let mut wrong_plan = base_journal_record(
            "rec-wp",
            "req-wrong-plan",
            ObservationRecordPhase::Finish,
            1_000,
            "workflow.save-draft",
            ObservationCallState::Completed,
        );
        wrong_plan.job_id = Some("plan-100".to_string());
        wrong_plan.plan_id = Some("plan-999".to_string());

        // 2. Wrong binding/thread.
        let mut wrong_thread = base_journal_record(
            "rec-wt",
            "req-wrong-thread",
            ObservationRecordPhase::Finish,
            1_100,
            "mcp.paired.get_plan",
            ObservationCallState::Completed,
        );
        wrong_thread.job_id = Some("plan-100".to_string());
        wrong_thread.plan_id = Some("plan-100".to_string());
        wrong_thread.thread_uuid = Some(THREAD_UUID_B.to_string());

        // 3. Environment-only plan_id and Entry-only job_id with no verified Finish job_id.
        let mut env_only_entry = base_journal_record(
            "rec-env-e",
            "req-env-only",
            ObservationRecordPhase::Entry,
            1_200,
            "workflow.publish",
            ObservationCallState::Running,
        );
        env_only_entry.job_id = Some("plan-100".to_string());
        env_only_entry.plan_id = Some("plan-100".to_string());

        let mut env_only_finish = base_journal_record(
            "rec-env-f",
            "req-env-only",
            ObservationRecordPhase::Finish,
            1_300,
            "workflow.publish",
            ObservationCallState::Completed,
        );
        env_only_finish.job_id = None;
        env_only_finish.plan_id = Some("plan-100".to_string());

        let projection = project_requests(
            SOURCE_ID,
            &[plan_job],
            &[wrong_plan, wrong_thread, env_only_entry, env_only_finish],
            &[ReadonlyCoreStateSnapshot::default()],
            &[],
        );

        assert_eq!(projection.requests.len(), 3);
        for req in &projection.requests {
            assert_eq!(req.job_id, None, "request {} must stay unlinked", req.id);
        }
        assert!(!projection.warnings.is_empty());
    }

    #[test]
    fn advice_classify_with_missing_binding_and_env_hints_stays_standalone_preserving_actor() {
        let job = sample_job("job-1", "bnd-1", THREAD_UUID_A, "plan-1", "task-1");
        let parent = base_journal_record(
            "rec-parent",
            "req-parent",
            ObservationRecordPhase::Finish,
            1_000,
            "mcp.paired.status",
            ObservationCallState::Completed,
        );

        let expected_actor = ObservationActor {
            label: Some("Advisor".to_string()),
            role: Some("classifier".to_string()),
            source: Some("journal".to_string()),
            model: Some("gpt-5.4-mini".to_string()),
            effort: Some("low".to_string()),
            selected_model: None,
            selected_effort: None,
            evidence: ObservationActorEvidence::Unknown,
        };

        let mut classify = base_journal_record(
            "rec-classify",
            "req-classify",
            ObservationRecordPhase::Finish,
            1_500,
            "advice.classify",
            ObservationCallState::Completed,
        );
        classify.binding_id = None;
        classify.thread_uuid = Some(THREAD_UUID_A.to_string());
        classify.parent_request_id = Some("req-parent".to_string());
        classify.job_id = Some("job-1".to_string());
        classify.plan_id = Some("plan-1".to_string());
        classify.task_id = Some("task-1".to_string());
        classify.approval_id = Some("apr-1".to_string());
        classify.actor = Some(expected_actor.clone());

        let projection = project_requests(
            SOURCE_ID,
            &[job],
            &[parent, classify],
            &[ReadonlyCoreStateSnapshot::default()],
            &[],
        );

        let classify_row = projection
            .requests
            .iter()
            .find(|r| r.id == "req-classify")
            .expect("advice.classify should remain standalone");
        assert_eq!(classify_row.parent_request_id, None);
        assert_eq!(classify_row.job_id, None);
        assert_eq!(classify_row.binding_id, None);
        assert_eq!(classify_row.thread_uuid.as_deref(), Some(THREAD_UUID_A));
        assert_eq!(classify_row.actor, expected_actor);

        let parent_row = projection
            .requests
            .iter()
            .find(|r| r.id == "req-parent")
            .expect("parent request should exist");
        assert!(parent_row.children.is_empty());
    }

    #[test]
    fn selected_actor_metadata_survives_entry_finish_merge_without_becoming_observed() {
        let mut selected_entry = base_journal_record(
            "rec-sel-e",
            "req-sel",
            ObservationRecordPhase::Entry,
            1_000,
            "mcp.paired.status",
            ObservationCallState::Running,
        );
        let mut selected_actor = ObservationActor::paired_coworker("Main Chat");
        selected_actor.selected_model = Some("  gpt-6-astra  ".to_string());
        selected_actor.selected_effort = Some(" high ".to_string());
        selected_entry.actor = Some(selected_actor);

        let mut selected_finish = base_journal_record(
            "rec-sel-f",
            "req-sel",
            ObservationRecordPhase::Finish,
            1_500,
            "mcp.paired.status",
            ObservationCallState::Completed,
        );
        selected_finish.actor = Some(ObservationActor::unknown(Some("paired_mcp".to_string())));

        let mut plain_finish = base_journal_record(
            "rec-plain-f",
            "req-plain",
            ObservationRecordPhase::Finish,
            1_600,
            "workflow.save-draft",
            ObservationCallState::Completed,
        );
        plain_finish.actor = Some(ObservationActor::unknown(Some(
            "trusted_local_operator".to_string(),
        )));

        let projection = project_requests(
            SOURCE_ID,
            &[],
            &[selected_entry, selected_finish, plain_finish],
            &[ReadonlyCoreStateSnapshot::default()],
            &[],
        );

        let selected_row = projection
            .requests
            .iter()
            .find(|r| r.id == "req-sel")
            .expect("request with selected metadata should exist");
        assert_eq!(
            selected_row.actor.selected_model.as_deref(),
            Some("gpt-6-astra")
        );
        assert_eq!(selected_row.actor.selected_effort.as_deref(), Some("high"));
        assert_eq!(selected_row.actor.model, None);
        assert_eq!(selected_row.actor.effort, None);

        let plain_row = projection
            .requests
            .iter()
            .find(|r| r.id == "req-plain")
            .expect("request without selected metadata should exist");
        assert_eq!(plain_row.actor.selected_model, None);
        assert_eq!(plain_row.actor.selected_effort, None);
        assert_eq!(plain_row.actor.model, None);
        assert_eq!(plain_row.actor.effort, None);
    }

    #[test]
    fn mcp_completed_and_in_flight_metadata_and_live_worker_receipt_behavior() {
        let job = sample_job("job-1", "bnd-1", THREAD_UUID_A, "plan-1", "task-1");
        let live_link = sample_link(
            "wrk-live-1",
            "job-1",
            "task-1",
            "apr-1",
            "bnd-1",
            "bnd-sess-live-1",
            THREAD_UUID_A,
            true,
        );

        let expected_actor = ObservationActor {
            label: Some("Luna".to_string()),
            role: Some("coordinator".to_string()),
            source: Some("mcp.journal".to_string()),
            model: Some("gpt-5.4".to_string()),
            effort: Some("high".to_string()),
            selected_model: None,
            selected_effort: None,
            evidence: ObservationActorEvidence::Unknown,
        };
        let expected_usage = ObservationUsage {
            input_tokens: Some(120),
            output_tokens: Some(30),
            cache_read_tokens: Some(80),
            total_tokens: Some(150),
            provenance: "mcp.journal".to_string(),
        };

        let mut completed_status = base_journal_record(
            "rec-mcp-done",
            "req-mcp-done",
            ObservationRecordPhase::Finish,
            1_000,
            "mcp.paired.status",
            ObservationCallState::Completed,
        );
        completed_status.actor = Some(expected_actor.clone());
        completed_status.usage = Some(expected_usage.clone());
        completed_status.delivery = vec![ObservationDelivery {
            stage: ObservationDeliveryStage::ResponseReturned,
            at_ms: Some(1_000),
            summary: "Synchronous status response returned.".to_string(),
            recipient: Some("bnd-1".to_string()),
        }];

        let mut stale_mcp = base_journal_record(
            "rec-mcp-stale",
            "req-mcp-stale",
            ObservationRecordPhase::Entry,
            2_000,
            "mcp.paired.status",
            ObservationCallState::Running,
        );
        stale_mcp.pid = Some(77_777);
        stale_mcp.process_instance = Some(ObservationProcessInstance {
            pid: u32::MAX,
            boot_id: "00000000-0000-0000-0000-000000000000".to_string(),
            start_ticks: u64::MAX,
        });

        let mut live_mcp_dispatch = base_journal_record(
            "rec-mcp-live",
            "req-mcp-live",
            ObservationRecordPhase::Entry,
            3_000,
            "mcp.paired.start_approved_task",
            ObservationCallState::Running,
        );
        live_mcp_dispatch.job_id = Some("job-1".to_string());
        live_mcp_dispatch.plan_id = Some("plan-1".to_string());
        live_mcp_dispatch.task_id = Some("task-1".to_string());
        live_mcp_dispatch.approval_id = Some("apr-1".to_string());

        let core_view = CoreStateView {
            bindings: vec![ReadonlyBindingRecord {
                binding_id: "bnd-sess-live-1".to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                label: "Live Worker Session".to_string(),
                delivered_seq: 0,
                acked_seq: 0,
            }],
            runs: Vec::new(),
            workers: vec![WorkerRecordView {
                worker_id: "wrk-live-1".to_string(),
                run_id: "run-live-1".to_string(),
                binding_id: "bnd-sess-live-1".to_string(),
                revision: 1,
                status: "running".to_string(),
                provider: "agy".to_string(),
                requested_model: "gemini-4-argon-high".to_string(),
                requested_effort: "high".to_string(),
                observed_model: None,
                observed_effort: None,
                created_at_ms: 3_050,
                updated_at_ms: 3_060,
                started_at_ms: Some(3_055),
                completed_at_ms: None,
            }],
            events: Vec::new(),
            event_batches: Vec::new(),
            wakeups: Vec::new(),
        };

        let projection = project_requests_with_views(
            SOURCE_ID,
            &[job],
            &[completed_status, stale_mcp, live_mcp_dispatch],
            &[core_view],
            &[live_link],
        );

        let done_row = projection
            .requests
            .iter()
            .find(|r| r.id == "req-mcp-done")
            .expect("completed mcp.paired.status should exist");
        assert_eq!(done_row.state, ObservationCallState::Completed);
        assert_eq!(done_row.actor, expected_actor);
        assert_eq!(done_row.usage, Some(expected_usage));
        assert_eq!(done_row.delivery.len(), 1);
        assert_eq!(
            done_row.delivery[0].stage,
            ObservationDeliveryStage::ResponseReturned
        );

        let stale_row = projection
            .requests
            .iter()
            .find(|r| r.id == "req-mcp-stale")
            .expect("unfinished stale mcp.paired.status should exist");
        assert_eq!(stale_row.state, ObservationCallState::Unknown);

        let live_row = projection
            .requests
            .iter()
            .find(|r| r.id == "req-mcp-live")
            .expect("unfinished mcp.paired.start_approved_task with live receipt should exist");
        assert_eq!(live_row.state, ObservationCallState::Running);
        assert_eq!(live_row.job_id.as_deref(), Some("job-1"));
        assert_eq!(live_row.binding_id.as_deref(), Some("bnd-1"));
    }

    #[test]
    fn pending_and_idle_wakeups_do_not_create_notification_submitted_delivery() {
        let job = sample_job("job-1", "bnd-1", THREAD_UUID_A, "plan-1", "task-1");
        let link = sample_link(
            "wrk-1",
            "job-1",
            "task-1",
            "apr-1",
            "bnd-1",
            "bnd-sess-1",
            THREAD_UUID_A,
            false,
        );

        let mut dispatch_entry = base_journal_record(
            "rec-de",
            "req-disp",
            ObservationRecordPhase::Entry,
            1_000,
            "session.dispatch",
            ObservationCallState::Running,
        );
        dispatch_entry.job_id = Some("job-1".to_string());
        dispatch_entry.plan_id = Some("plan-1".to_string());
        dispatch_entry.task_id = Some("task-1".to_string());
        dispatch_entry.approval_id = Some("apr-1".to_string());

        let mut dispatch_finish = dispatch_entry.clone();
        dispatch_finish.record_id = "rec-df".to_string();
        dispatch_finish.phase = ObservationRecordPhase::Finish;
        dispatch_finish.at_ms = 2_000;
        dispatch_finish.state = ObservationCallState::Completed;

        let core_view = CoreStateView {
            bindings: vec![ReadonlyBindingRecord {
                binding_id: "bnd-sess-1".to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                label: "Worker Session".to_string(),
                delivered_seq: 5,
                acked_seq: 5,
            }],
            runs: vec![ReadonlyRunRecord {
                run_id: "run-1".to_string(),
                binding_id: "bnd-sess-1".to_string(),
                revision: 1,
                status: "completed".to_string(),
                active_question_id: None,
                started_at_ms: 1_500,
                updated_at_ms: 2_500,
                completed_at_ms: Some(2_500),
                cancelled_at_ms: None,
            }],
            workers: vec![WorkerRecordView {
                worker_id: "wrk-1".to_string(),
                run_id: "run-1".to_string(),
                binding_id: "bnd-sess-1".to_string(),
                revision: 1,
                status: "completed".to_string(),
                provider: "agy".to_string(),
                requested_model: "gemini-4-argon-high".to_string(),
                requested_effort: "high".to_string(),
                observed_model: Some("gemini-4-argon-high".to_string()),
                observed_effort: Some("high".to_string()),
                created_at_ms: 1_500,
                updated_at_ms: 2_500,
                started_at_ms: Some(1_510),
                completed_at_ms: Some(2_500),
            }],
            events: vec![ReadonlyEventRecord {
                event_id: "evt-1".to_string(),
                binding_id: "bnd-sess-1".to_string(),
                seq: 5,
                run_id: "run-1".to_string(),
                revision: 1,
                kind: "worker_completed".to_string(),
                important: true,
                created_at_ms: 2_400,
            }],
            event_batches: Vec::new(),
            wakeups: vec![
                ReadonlyWakeupRecord {
                    binding_id: "bnd-sess-1".to_string(),
                    thread_uuid: THREAD_UUID_A.to_string(),
                    status: "pending".to_string(),
                    claim_id: None,
                    pending_since_seq: Some(5),
                    latest_important_seq: 5,
                    covered_up_to_seq: 5,
                    claimed_at_ms: None,
                    resolved_at_ms: None,
                },
                ReadonlyWakeupRecord {
                    binding_id: "bnd-sess-1".to_string(),
                    thread_uuid: THREAD_UUID_A.to_string(),
                    status: "idle".to_string(),
                    claim_id: None,
                    pending_since_seq: Some(5),
                    latest_important_seq: 5,
                    covered_up_to_seq: 5,
                    claimed_at_ms: None,
                    resolved_at_ms: None,
                },
                ReadonlyWakeupRecord {
                    binding_id: "bnd-sess-1".to_string(),
                    thread_uuid: THREAD_UUID_A.to_string(),
                    status: "satisfied_by_ack".to_string(),
                    claim_id: None,
                    pending_since_seq: Some(5),
                    latest_important_seq: 5,
                    covered_up_to_seq: 5,
                    claimed_at_ms: None,
                    resolved_at_ms: Some(2_450),
                },
            ],
        };

        let projection = project_requests_with_views(
            SOURCE_ID,
            &[job],
            &[dispatch_entry, dispatch_finish],
            &[core_view],
            &[link],
        );

        assert_eq!(projection.requests.len(), 1);
        let row = &projection.requests[0];
        assert_eq!(row.binding_id.as_deref(), Some("bnd-1"));
        let stages: Vec<&ObservationDeliveryStage> =
            row.delivery.iter().map(|d| &d.stage).collect();
        assert_eq!(stages, vec![&ObservationDeliveryStage::ResultReady]);
        assert_eq!(row.delivery[0].recipient.as_deref(), Some("bnd-sess-1"));
        assert!(
            !stages.contains(&&ObservationDeliveryStage::NotificationSubmitted),
            "pending, idle, and satisfied_by_ack wakeups must not produce NotificationSubmitted"
        );
    }

    #[test]
    fn completed_worker_projects_ready_for_review_or_completed_from_verified_task_and_job_review_state(
    ) {
        let mut review_job =
            sample_job("job-review", "bnd-1", THREAD_UUID_A, "plan-rev", "task-rev");
        review_job.review_state = Some("pending_review".to_string());
        review_job.tasks[0].status = "ready_for_review".to_string();

        let mut accepted_job =
            sample_job("job-acc", "bnd-1", THREAD_UUID_A, "plan-acc", "task-acc");
        accepted_job.review_state = Some("review_passed".to_string());
        accepted_job.tasks[0].status = "accepted".to_string();

        let review_link = sample_link(
            "wrk-rev",
            "job-review",
            "task-rev",
            "apr-rev",
            "bnd-1",
            "bnd-sess-rev",
            THREAD_UUID_A,
            false,
        );
        let hist_review_link = sample_link(
            "wrk-hist-rev",
            "job-review",
            "task-rev",
            "apr-rev-hist",
            "bnd-1",
            "bnd-sess-hist-rev",
            THREAD_UUID_A,
            false,
        );
        let accepted_link = sample_link(
            "wrk-acc",
            "job-acc",
            "task-acc",
            "apr-acc",
            "bnd-1",
            "bnd-sess-acc",
            THREAD_UUID_A,
            false,
        );

        let mut rev_entry = base_journal_record(
            "rec-rev-e",
            "req-disp-rev",
            ObservationRecordPhase::Entry,
            1_000,
            "session.dispatch",
            ObservationCallState::Running,
        );
        rev_entry.job_id = Some("job-review".to_string());
        rev_entry.plan_id = Some("plan-rev".to_string());
        rev_entry.task_id = Some("task-rev".to_string());
        rev_entry.approval_id = Some("apr-rev".to_string());

        let mut rev_finish = rev_entry.clone();
        rev_finish.record_id = "rec-rev-f".to_string();
        rev_finish.phase = ObservationRecordPhase::Finish;
        rev_finish.at_ms = 2_000;
        rev_finish.state = ObservationCallState::Completed;

        let mut acc_entry = base_journal_record(
            "rec-acc-e",
            "req-disp-acc",
            ObservationRecordPhase::Entry,
            3_000,
            "session.dispatch",
            ObservationCallState::Running,
        );
        acc_entry.job_id = Some("job-acc".to_string());
        acc_entry.plan_id = Some("plan-acc".to_string());
        acc_entry.task_id = Some("task-acc".to_string());
        acc_entry.approval_id = Some("apr-acc".to_string());

        let mut acc_finish = acc_entry.clone();
        acc_finish.record_id = "rec-acc-f".to_string();
        acc_finish.phase = ObservationRecordPhase::Finish;
        acc_finish.at_ms = 4_000;
        acc_finish.state = ObservationCallState::Completed;

        let core_view = CoreStateView {
            bindings: vec![
                ReadonlyBindingRecord {
                    binding_id: "bnd-sess-rev".to_string(),
                    thread_uuid: THREAD_UUID_A.to_string(),
                    label: "Review Worker Session".to_string(),
                    delivered_seq: 0,
                    acked_seq: 0,
                },
                ReadonlyBindingRecord {
                    binding_id: "bnd-sess-hist-rev".to_string(),
                    thread_uuid: THREAD_UUID_A.to_string(),
                    label: "Historical Review Worker Session".to_string(),
                    delivered_seq: 0,
                    acked_seq: 0,
                },
                ReadonlyBindingRecord {
                    binding_id: "bnd-sess-acc".to_string(),
                    thread_uuid: THREAD_UUID_A.to_string(),
                    label: "Accepted Worker Session".to_string(),
                    delivered_seq: 0,
                    acked_seq: 0,
                },
            ],
            runs: Vec::new(),
            workers: vec![
                WorkerRecordView {
                    worker_id: "wrk-rev".to_string(),
                    run_id: "run-rev".to_string(),
                    binding_id: "bnd-sess-rev".to_string(),
                    revision: 2,
                    status: "completed".to_string(),
                    provider: "agy".to_string(),
                    requested_model: "gemini-4-argon-high".to_string(),
                    requested_effort: "high".to_string(),
                    observed_model: Some("gemini-4-argon-high".to_string()),
                    observed_effort: Some("high".to_string()),
                    created_at_ms: 1_500,
                    updated_at_ms: 2_400,
                    started_at_ms: Some(1_510),
                    completed_at_ms: Some(2_400),
                },
                WorkerRecordView {
                    worker_id: "wrk-hist-rev".to_string(),
                    run_id: "run-hist-rev".to_string(),
                    binding_id: "bnd-sess-hist-rev".to_string(),
                    revision: 2,
                    status: "completed".to_string(),
                    provider: "agy".to_string(),
                    requested_model: "gemini-4-argon-high".to_string(),
                    requested_effort: "high".to_string(),
                    observed_model: None,
                    observed_effort: None,
                    created_at_ms: 1_200,
                    updated_at_ms: 1_800,
                    started_at_ms: Some(1_210),
                    completed_at_ms: Some(1_800),
                },
                WorkerRecordView {
                    worker_id: "wrk-acc".to_string(),
                    run_id: "run-acc".to_string(),
                    binding_id: "bnd-sess-acc".to_string(),
                    revision: 2,
                    status: "completed".to_string(),
                    provider: "agy".to_string(),
                    requested_model: "gemini-4-argon-high".to_string(),
                    requested_effort: "high".to_string(),
                    observed_model: Some("gemini-4-argon-high".to_string()),
                    observed_effort: Some("high".to_string()),
                    created_at_ms: 3_500,
                    updated_at_ms: 4_400,
                    started_at_ms: Some(3_510),
                    completed_at_ms: Some(4_400),
                },
            ],
            events: Vec::new(),
            event_batches: Vec::new(),
            wakeups: Vec::new(),
        };

        let projection = project_requests_with_views(
            SOURCE_ID,
            &[review_job, accepted_job],
            &[rev_entry, rev_finish, acc_entry, acc_finish],
            &[core_view],
            &[review_link, hist_review_link, accepted_link],
        );

        let rev_root = projection
            .requests
            .iter()
            .find(|r| r.id == "req-disp-rev")
            .expect("review dispatch root should exist");
        assert_eq!(rev_root.state, ObservationCallState::Completed);
        assert_eq!(rev_root.started_at_ms, Some(1_000));
        assert_eq!(rev_root.finished_at_ms, Some(2_000));
        assert_eq!(rev_root.duration_ms, Some(1_000));
        assert_eq!(rev_root.updated_at_ms, 2_400);
        assert_eq!(rev_root.children.len(), 1);
        assert_eq!(rev_root.children[0].id, "wrk-rev");
        assert_eq!(
            rev_root.children[0].state,
            ObservationCallState::ReadyForReview
        );
        assert_eq!(rev_root.children[0].summary, None);

        let hist_rev_row = projection
            .requests
            .iter()
            .find(|r| r.id == "wrk-hist-rev")
            .expect("historical review worker row should exist");
        assert_eq!(hist_rev_row.state, ObservationCallState::ReadyForReview);
        assert_eq!(
            hist_rev_row.binding_id.as_deref(),
            Some("bnd-sess-hist-rev")
        );
        assert_eq!(hist_rev_row.summary, None);

        let acc_root = projection
            .requests
            .iter()
            .find(|r| r.id == "req-disp-acc")
            .expect("accepted dispatch root should exist");
        assert_eq!(acc_root.state, ObservationCallState::Completed);
        assert_eq!(acc_root.children.len(), 1);
        assert_eq!(acc_root.children[0].id, "wrk-acc");
        assert_eq!(acc_root.children[0].state, ObservationCallState::Completed);
        assert_eq!(acc_root.children[0].summary, None);
    }

    #[test]
    fn context_workflow_plan_wrapper_folds_verified_inner_workflow_call_and_exposes_plan_job_id() {
        let mut plan_job = sample_job("plan-200", "bnd-1", THREAD_UUID_A, "plan-200", "task-1");
        plan_job.thread_uuid = None;
        plan_job.approval_status = "pending".to_string();
        plan_job.execution_status = "not_started".to_string();
        plan_job.tasks.clear();

        let outer_actor = ObservationActor {
            label: Some("Luna".to_string()),
            role: Some("coordinator".to_string()),
            source: Some("cli.context".to_string()),
            model: Some("gpt-5.4".to_string()),
            effort: Some("high".to_string()),
            selected_model: None,
            selected_effort: None,
            evidence: ObservationActorEvidence::Unknown,
        };

        let mut outer_entry = base_journal_record(
            "rec-ctx-wf-e",
            "req-ctx-wf",
            ObservationRecordPhase::Entry,
            1_000,
            "context.workflow.save-draft",
            ObservationCallState::Running,
        );
        outer_entry.plan_id = Some("plan-200".to_string());
        outer_entry.actor = Some(outer_actor.clone());

        let mut outer_finish = outer_entry.clone();
        outer_finish.record_id = "rec-ctx-wf-f".to_string();
        outer_finish.phase = ObservationRecordPhase::Finish;
        outer_finish.at_ms = 1_500;
        outer_finish.state = ObservationCallState::Completed;

        let mut inner_entry = base_journal_record(
            "rec-inner-wf-e",
            "req-ctx-wf",
            ObservationRecordPhase::Entry,
            1_100,
            "workflow.save-draft",
            ObservationCallState::Running,
        );
        inner_entry.call_id = Some("call-wf-1".to_string());
        inner_entry.parent_request_id = None;
        inner_entry.plan_id = Some("plan-200".to_string());

        let mut inner_finish = inner_entry.clone();
        inner_finish.record_id = "rec-inner-wf-f".to_string();
        inner_finish.phase = ObservationRecordPhase::Finish;
        inner_finish.at_ms = 1_400;
        inner_finish.state = ObservationCallState::Completed;
        inner_finish.job_id = Some("plan-200".to_string());
        inner_finish.plan_id = Some("plan-200".to_string());
        inner_finish.summary = Some("Saved draft revision 2.".to_string());

        let snapshot = ReadonlyCoreStateSnapshot {
            bindings: vec![ReadonlyBindingRecord {
                binding_id: "bnd-1".to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                label: "Main Chat".to_string(),
                delivered_seq: 0,
                acked_seq: 0,
            }],
            ..ReadonlyCoreStateSnapshot::default()
        };

        let projection = project_requests(
            SOURCE_ID,
            &[plan_job],
            &[outer_entry, inner_entry, inner_finish, outer_finish],
            &[snapshot],
            &[],
        );

        assert_eq!(projection.requests.len(), 1);
        let root = &projection.requests[0];
        assert_eq!(root.id, "req-ctx-wf");
        assert_eq!(root.operation, "context.workflow.save-draft");
        assert_eq!(root.job_id.as_deref(), Some("plan-200"));
        assert_eq!(root.actor, outer_actor);
        assert_eq!(root.started_at_ms, Some(1_000));
        assert_eq!(root.finished_at_ms, Some(1_500));
        assert_eq!(root.duration_ms, Some(500));
        assert_eq!(root.updated_at_ms, 1_500);
        assert_eq!(root.children_count, 1);
        assert_eq!(root.children.len(), 1);
        assert_eq!(root.children[0].id, "call-wf-1");
        assert_eq!(root.children[0].state, ObservationCallState::Completed);
        assert_eq!(root.children[0].started_at_ms, Some(1_100));
        assert_eq!(root.children[0].finished_at_ms, Some(1_400));
        assert_eq!(root.children[0].duration_ms, Some(300));
        assert_eq!(
            root.children[0].summary.as_deref(),
            Some("Saved draft revision 2.")
        );
    }

    #[test]
    fn context_workflow_plan_wrapper_rejects_contradictory_plan_hint_or_foreign_child_binding_thread(
    ) {
        let mut plan_job = sample_job("plan-200", "bnd-1", THREAD_UUID_A, "plan-200", "task-1");
        plan_job.approval_status = "pending".to_string();

        // 1. Contradictory outer plan hint ("plan-999" vs child verified "plan-200").
        let mut contrad_outer_e = base_journal_record(
            "rec-co-e",
            "req-contrad",
            ObservationRecordPhase::Entry,
            1_000,
            "context.workflow.save-draft",
            ObservationCallState::Running,
        );
        contrad_outer_e.plan_id = Some("plan-999".to_string());

        let mut contrad_outer_f = contrad_outer_e.clone();
        contrad_outer_f.record_id = "rec-co-f".to_string();
        contrad_outer_f.phase = ObservationRecordPhase::Finish;
        contrad_outer_f.at_ms = 1_500;
        contrad_outer_f.state = ObservationCallState::Completed;

        let mut contrad_inner_e = base_journal_record(
            "rec-ci-e",
            "req-contrad",
            ObservationRecordPhase::Entry,
            1_100,
            "workflow.save-draft",
            ObservationCallState::Running,
        );
        contrad_inner_e.call_id = Some("call-ci".to_string());
        contrad_inner_e.plan_id = Some("plan-200".to_string());

        let mut contrad_inner_f = contrad_inner_e.clone();
        contrad_inner_f.record_id = "rec-ci-f".to_string();
        contrad_inner_f.phase = ObservationRecordPhase::Finish;
        contrad_inner_f.at_ms = 1_400;
        contrad_inner_f.state = ObservationCallState::Completed;
        contrad_inner_f.job_id = Some("plan-200".to_string());
        contrad_inner_f.plan_id = Some("plan-200".to_string());

        // 2. Foreign child binding/thread ("bnd-2" / THREAD_UUID_B).
        let mut foreign_outer_e = base_journal_record(
            "rec-fo-e",
            "req-foreign",
            ObservationRecordPhase::Entry,
            2_000,
            "context.workflow.save-draft",
            ObservationCallState::Running,
        );
        foreign_outer_e.plan_id = Some("plan-200".to_string());

        let mut foreign_outer_f = foreign_outer_e.clone();
        foreign_outer_f.record_id = "rec-fo-f".to_string();
        foreign_outer_f.phase = ObservationRecordPhase::Finish;
        foreign_outer_f.at_ms = 2_500;
        foreign_outer_f.state = ObservationCallState::Completed;

        let mut foreign_inner_e = base_journal_record(
            "rec-fi-e",
            "req-foreign",
            ObservationRecordPhase::Entry,
            2_100,
            "workflow.save-draft",
            ObservationCallState::Running,
        );
        foreign_inner_e.call_id = Some("call-fi".to_string());
        foreign_inner_e.binding_id = Some("bnd-2".to_string());
        foreign_inner_e.thread_uuid = Some(THREAD_UUID_B.to_string());
        foreign_inner_e.plan_id = Some("plan-200".to_string());

        let mut foreign_inner_f = foreign_inner_e.clone();
        foreign_inner_f.record_id = "rec-fi-f".to_string();
        foreign_inner_f.phase = ObservationRecordPhase::Finish;
        foreign_inner_f.at_ms = 2_400;
        foreign_inner_f.state = ObservationCallState::Completed;
        foreign_inner_f.job_id = Some("plan-200".to_string());
        foreign_inner_f.plan_id = Some("plan-200".to_string());

        let projection = project_requests(
            SOURCE_ID,
            &[plan_job],
            &[
                contrad_outer_e,
                contrad_outer_f,
                contrad_inner_e,
                contrad_inner_f,
                foreign_outer_e,
                foreign_outer_f,
                foreign_inner_e,
                foreign_inner_f,
            ],
            &[ReadonlyCoreStateSnapshot::default()],
            &[],
        );

        assert_eq!(projection.requests.len(), 4);

        let contrad_root = projection
            .requests
            .iter()
            .find(|r| r.id == "req-contrad")
            .expect("contradictory outer context request should remain standalone");
        assert_eq!(contrad_root.job_id, None);
        assert!(contrad_root.children.is_empty());

        let foreign_root = projection
            .requests
            .iter()
            .find(|r| r.id == "req-foreign")
            .expect("foreign-child outer context request should remain standalone");
        assert_eq!(foreign_root.job_id, None);
        assert!(foreign_root.children.is_empty());
    }

    #[test]
    fn context_workflow_plan_wrapper_rejects_ambiguous_multiple_verified_plan_children() {
        let mut plan_job_1 = sample_job("plan-201", "bnd-1", THREAD_UUID_A, "plan-201", "task-1");
        plan_job_1.approval_status = "pending".to_string();
        let mut plan_job_2 = sample_job("plan-202", "bnd-1", THREAD_UUID_A, "plan-202", "task-2");
        plan_job_2.approval_status = "pending".to_string();

        let outer_e = base_journal_record(
            "rec-amb-oe",
            "req-ambig",
            ObservationRecordPhase::Entry,
            1_000,
            "context.workflow.save-draft",
            ObservationCallState::Running,
        );
        let mut outer_f = outer_e.clone();
        outer_f.record_id = "rec-amb-of".to_string();
        outer_f.phase = ObservationRecordPhase::Finish;
        outer_f.at_ms = 2_000;
        outer_f.state = ObservationCallState::Completed;

        let mut child_1_e = base_journal_record(
            "rec-amb-c1e",
            "req-ambig",
            ObservationRecordPhase::Entry,
            1_100,
            "workflow.save-draft",
            ObservationCallState::Running,
        );
        child_1_e.call_id = Some("call-1".to_string());
        child_1_e.plan_id = Some("plan-201".to_string());

        let mut child_1_f = child_1_e.clone();
        child_1_f.record_id = "rec-amb-c1f".to_string();
        child_1_f.phase = ObservationRecordPhase::Finish;
        child_1_f.at_ms = 1_300;
        child_1_f.state = ObservationCallState::Completed;
        child_1_f.job_id = Some("plan-201".to_string());
        child_1_f.plan_id = Some("plan-201".to_string());

        let mut child_2_e = base_journal_record(
            "rec-amb-c2e",
            "req-ambig",
            ObservationRecordPhase::Entry,
            1_400,
            "workflow.save-draft",
            ObservationCallState::Running,
        );
        child_2_e.call_id = Some("call-2".to_string());
        child_2_e.plan_id = Some("plan-202".to_string());

        let mut child_2_f = child_2_e.clone();
        child_2_f.record_id = "rec-amb-c2f".to_string();
        child_2_f.phase = ObservationRecordPhase::Finish;
        child_2_f.at_ms = 1_600;
        child_2_f.state = ObservationCallState::Completed;
        child_2_f.job_id = Some("plan-202".to_string());
        child_2_f.plan_id = Some("plan-202".to_string());

        let projection = project_requests(
            SOURCE_ID,
            &[plan_job_1, plan_job_2],
            &[outer_e, child_1_e, child_1_f, child_2_e, child_2_f, outer_f],
            &[ReadonlyCoreStateSnapshot::default()],
            &[],
        );

        assert_eq!(projection.requests.len(), 3);
        let wrapper_row = projection
            .requests
            .iter()
            .find(|r| r.id == "req-ambig")
            .expect("ambiguous outer context wrapper should remain standalone");
        assert_eq!(wrapper_row.job_id, None);
        assert!(wrapper_row.children.is_empty());
    }
}
