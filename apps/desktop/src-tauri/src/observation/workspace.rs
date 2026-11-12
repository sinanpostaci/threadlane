use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::ErrorKind;
#[cfg(unix)]
use std::os::unix::fs::MetadataExt;
use std::path::Path;
use std::sync::{Arc, Mutex};

use threadlane_core::observation::{
    bound_workspace_job_histories, bound_workspace_native_samples_per_source,
    project_safe_workspace_messages, read_core_state_readonly, reconcile_workspace_native_sample,
    sanitize_observation_label, sanitize_observation_summary, validate_correlation_id,
    ObservationJob, ObservationJobState, ObservationRequest, ObservationSnapshotRequest,
    ReadonlyCoreStateSnapshot, WorkspaceAcceptanceProof, WorkspaceAcceptanceStatus,
    WorkspaceActivityState, WorkspaceAttentionKind, WorkspaceAvailabilityState,
    WorkspaceChatIdentity, WorkspaceChatSummary, WorkspaceConfiguredSelection,
    WorkspaceExternalResolutionScope, WorkspaceJobHistory, WorkspaceJobSummary,
    WorkspaceMessageCandidateInput, WorkspaceMessageItem,
    WorkspaceMessageScope, WorkspaceMessageSourceOrder, WorkspaceMessagesWindow,
    WorkspaceNativeSample, WorkspaceRecordLinks, WorkspaceSampleReconcileDecision,
    WorkspaceSamplingPolicy, WorkspaceSnapshot, WorkspaceSnapshotBounds, WorkspaceTaskHistory,
    MAX_OBSERVATION_JOBS, MAX_WORKSPACE_NATIVE_SAMPLES_PER_SOURCE,
    WORKSPACE_SNAPSHOT_SCHEMA_VERSION,
};
use threadlane_core::MAX_WORKER_PATH_BYTES;
use threadlane_native::coworker_observation::{
    read_legacy_plan_external_resolutions_readonly, read_scoped_workspace_job_histories,
    LegacyPlanObservationScope, VerifiedLegacyPlanExternalResolutions,
    WorkspaceHistoryDiagnostic, WorkspaceHistoryDiagnosticKind, WorkspaceHistoryStores,
};
use threadlane_native::coworker_workflow::{CoworkerBindingLedger, CoworkerJobRecord};
use threadlane_native::native_chat_observation::read_workspace_native_samples_readonly;
use threadlane_plans::{PlanRevision, PlanStore};

use crate::AppError;

use super::coworker_jobs::load_verified_coworker_ledgers;
use super::diagnostics::{
    ObservationWarningCollector, WARNING_ORIGIN_JOBS, WARNING_ORIGIN_SNAPSHOT,
};
use super::jobs::{is_safe_repository_path, resolve_thread_label, FALLBACK_VERIFIED_PROJECT_LABEL};
use super::sources::SourceSpec;
use super::{
    physical_store_inode_key, validate_snapshot_request, ObservationService,
    ValidatedSnapshotQuery, MAX_REQUEST_PAGE_LIMIT,
};

const CORE_STATE_DB_FILE_NAME: &str = "state.db";
const MAX_CACHED_NATIVE_SAMPLES_TOTAL: usize = 1_024;

const WARN_NATIVE_SAMPLES_READ_FAILED: &str =
    "Skipped unreadable or invalid native chat samples directory in observation source.";
const WARN_NATIVE_SAMPLE_SCOPE_REJECTED: &str =
    "Rejected native chat sample with non-canonical or overlapping project scope.";
const WARN_NATIVE_SAMPLE_REGRESSED: &str =
    "Ignored stale or regressed native chat sample update in observation source.";
const WARN_COWORKER_SCOPE_REJECTED: &str =
    "Rejected coworker job record whose scope did not match its binding ledger or project root.";

/// Bounded in-memory native sample reconciliation cache attached to [`ObservationService`].
///
/// Preserves monotonic `(producer_epoch, backend_process_instance, sequence)` ordering across
/// snapshot refreshes without ever writing files, acquiring locks, or mutating sample timestamps.
#[derive(Debug, Clone, Default)]
pub(crate) struct BoundedSampleCache {
    inner: Arc<Mutex<BTreeMap<String, WorkspaceNativeSample>>>,
}

impl BoundedSampleCache {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    fn reconcile_source_samples(
        &self,
        source_id: &str,
        disk_samples_by_key: BTreeMap<String, WorkspaceNativeSample>,
        warnings: &mut Vec<String>,
    ) -> Vec<WorkspaceNativeSample> {
        let mut guard = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        // Prune cached entries for this source whose sidecar is no longer present on disk.
        guard.retain(|key, existing| {
            existing.chat_identity.source_id != source_id || disk_samples_by_key.contains_key(key)
        });

        let mut reconciled = Vec::with_capacity(disk_samples_by_key.len());
        for (chat_key, candidate) in disk_samples_by_key {
            let chosen = match guard.get(&chat_key) {
                None => {
                    guard.insert(chat_key.clone(), candidate.clone());
                    candidate
                }
                Some(existing) => {
                    match reconcile_workspace_native_sample(Some(existing), &candidate) {
                        WorkspaceSampleReconcileDecision::AcceptedInitial
                        | WorkspaceSampleReconcileDecision::AcceptedNextSequence
                        | WorkspaceSampleReconcileDecision::AcceptedNewEpoch => {
                            guard.insert(chat_key.clone(), candidate.clone());
                            candidate
                        }
                        WorkspaceSampleReconcileDecision::RejectedDuplicateSequence => {
                            existing.clone()
                        }
                        WorkspaceSampleReconcileDecision::RejectedStaleSequence
                        | WorkspaceSampleReconcileDecision::RejectedStaleEpoch
                        | WorkspaceSampleReconcileDecision::RejectedEpochProcessMismatch => {
                            push_unique_string_warning(warnings, WARN_NATIVE_SAMPLE_REGRESSED);
                            existing.clone()
                        }
                        WorkspaceSampleReconcileDecision::RejectedInvalidSample
                        | WorkspaceSampleReconcileDecision::RejectedMismatchedIdentity => {
                            push_unique_string_warning(warnings, WARN_NATIVE_SAMPLE_REGRESSED);
                            continue;
                        }
                    }
                }
            };
            reconciled.push(chosen);
        }

        enforce_cache_bounds(&mut guard);
        reconciled
    }
}

fn enforce_cache_bounds(cache: &mut BTreeMap<String, WorkspaceNativeSample>) {
    let mut keys_by_source: BTreeMap<String, Vec<(Option<i64>, u64, String)>> = BTreeMap::new();
    for (key, sample) in cache.iter() {
        keys_by_source
            .entry(sample.chat_identity.source_id.clone())
            .or_default()
            .push((sample.observed_at_ms, sample.sequence, key.clone()));
    }

    for entries in keys_by_source.values_mut() {
        if entries.len() > MAX_WORKSPACE_NATIVE_SAMPLES_PER_SOURCE {
            entries.sort_by(|left, right| {
                right
                    .0
                    .unwrap_or(i64::MIN)
                    .cmp(&left.0.unwrap_or(i64::MIN))
                    .then_with(|| right.1.cmp(&left.1))
                    .then_with(|| left.2.cmp(&right.2))
            });
            for (_, _, evicted_key) in entries.drain(MAX_WORKSPACE_NATIVE_SAMPLES_PER_SOURCE..) {
                cache.remove(&evicted_key);
            }
        }
    }

    if cache.len() > MAX_CACHED_NATIVE_SAMPLES_TOTAL {
        let mut all_entries: Vec<(Option<i64>, u64, String)> = cache
            .iter()
            .map(|(k, s)| (s.observed_at_ms, s.sequence, k.clone()))
            .collect();
        all_entries.sort_by(|left, right| {
            right
                .0
                .unwrap_or(i64::MIN)
                .cmp(&left.0.unwrap_or(i64::MIN))
                .then_with(|| right.1.cmp(&left.1))
                .then_with(|| left.2.cmp(&right.2))
        });
        for (_, _, evicted_key) in all_entries.drain(MAX_CACHED_NATIVE_SAMPLES_TOTAL..) {
            cache.remove(&evicted_key);
        }
    }
}

impl ObservationService {
    /// Projects a bounded, read-only [`WorkspaceSnapshot`] across configured and linked sources.
    ///
    /// Preserves the embedded legacy [`ObservationSnapshot`] contract while populating canonical
    /// `chats`, `native_samples`, `job_histories`, safe `messages`, and truncation `bounds`
    /// without creating directories, lockfiles, WAL sidecars, or mutating sample timestamps.
    pub(crate) fn workspace_snapshot(
        &self,
        request: Option<ObservationSnapshotRequest>,
    ) -> Result<WorkspaceSnapshot, AppError> {
        let query = validate_snapshot_request(request.as_ref())?;
        let mut observation = self.snapshot(request)?;
        let generated_at_ms = observation.generated_at_ms;

        let registry = self.registry.clone();
        let inventory = registry.inventory();

        let mut warning_collector = ObservationWarningCollector::new();
        seed_warning_collector_from_observation(&mut warning_collector, &observation);
        warning_collector.extend_inventory(inventory.warnings);

        let mut all_native_samples: Vec<WorkspaceNativeSample> = Vec::new();
        let mut native_samples_omitted = 0usize;
        let mut all_chats: BTreeMap<String, WorkspaceChatSummary> = BTreeMap::new();
        let mut all_job_histories: Vec<WorkspaceJobHistory> = Vec::new();
        let mut job_histories_omitted = 0usize;
        let mut message_candidates: Vec<WorkspaceMessageCandidateInput> = Vec::new();
        let mut invalid_messages_omitted = 0usize;

        // Track verified chat scopes by `(source_id, job_id)` and `(source_id, binding_id, thread_uuid)`
        // for strictly validated request/journal message correlation.
        let mut chat_by_source_job: BTreeMap<(String, String), WorkspaceChatIdentity> =
            BTreeMap::new();
        let mut chats_by_source_binding_thread: BTreeMap<
            (String, String, String),
            Vec<WorkspaceChatIdentity>,
        > = BTreeMap::new();

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

            project_single_source_workspace(
                spec,
                &query,
                generated_at_ms,
                &observation.jobs,
                &observation.requests,
                &self.sample_cache,
                &mut warning_collector,
                &mut all_native_samples,
                &mut native_samples_omitted,
                &mut all_chats,
                &mut all_job_histories,
                &mut job_histories_omitted,
                &mut message_candidates,
                &mut chat_by_source_job,
                &mut chats_by_source_binding_thread,
            );
        }

        // Append safe journal summaries from already-validated projected requests.
        collect_request_journal_message_candidates(
            &observation.requests,
            &query,
            &chat_by_source_job,
            &chats_by_source_binding_thread,
            &mut message_candidates,
        );

        reconcile_observation_jobs_with_external_resolutions(
            &mut observation.jobs,
            &all_job_histories,
        );

        let (bounded_samples, extra_samples_omitted) =
            bound_workspace_native_samples_per_source(all_native_samples);
        native_samples_omitted = native_samples_omitted.saturating_add(extra_samples_omitted);

        let (bounded_histories, extra_histories_omitted) =
            bound_workspace_job_histories(all_job_histories);
        job_histories_omitted = job_histories_omitted.saturating_add(extra_histories_omitted);

        // If `query.job_id` is active, scope chats and native samples to the target job's verified chat.
        let (retained_chats, retained_samples) = if query.job_id.is_some() {
            let allowed_chat_keys: BTreeSet<String> = bounded_histories
                .iter()
                .filter_map(|h| h.job.chat_identity.as_ref()?.canonical_key())
                .collect();
            let chats = all_chats
                .into_iter()
                .filter(|(k, _)| allowed_chat_keys.contains(k))
                .map(|(_, v)| v)
                .collect();
            let samples = bounded_samples
                .into_iter()
                .filter(|s| {
                    s.chat_identity
                        .canonical_key()
                        .is_some_and(|k| allowed_chat_keys.contains(&k))
                })
                .collect();
            (chats, samples)
        } else {
            (all_chats.into_values().collect(), bounded_samples)
        };

        let messages = build_safe_messages_window(
            message_candidates,
            &mut invalid_messages_omitted,
            &mut warning_collector,
        );

        let (warnings, warning_contexts) = warning_collector.finish();
        observation.warnings = warnings;
        observation.warning_contexts = warning_contexts;

        let jobs_capped =
            observation.jobs_has_more || observation.jobs.len() > MAX_OBSERVATION_JOBS;
        let requests_capped = (observation.has_more && query.limit >= MAX_REQUEST_PAGE_LIMIT)
            || observation.requests.len() > MAX_REQUEST_PAGE_LIMIT;

        let snapshot = WorkspaceSnapshot {
            schema_version: WORKSPACE_SNAPSHOT_SCHEMA_VERSION,
            generated_at_ms,
            sampling_policy: WorkspaceSamplingPolicy::default(),
            observation,
            chats: retained_chats,
            native_samples: retained_samples,
            job_histories: bounded_histories,
            bounds: WorkspaceSnapshotBounds {
                native_samples_omitted,
                job_histories_omitted,
                messages_omitted: messages
                    .omitted_count
                    .saturating_add(invalid_messages_omitted),
                requests_capped,
                jobs_capped,
            },
            messages,
        };

        Ok(snapshot.normalized())
    }
}

#[allow(clippy::too_many_arguments)]
fn project_single_source_workspace(
    spec: &SourceSpec,
    query: &ValidatedSnapshotQuery,
    generated_at_ms: i64,
    projected_jobs: &[ObservationJob],
    projected_requests: &[ObservationRequest],
    sample_cache: &BoundedSampleCache,
    warning_collector: &mut ObservationWarningCollector,
    all_native_samples: &mut Vec<WorkspaceNativeSample>,
    native_samples_omitted: &mut usize,
    all_chats: &mut BTreeMap<String, WorkspaceChatSummary>,
    all_job_histories: &mut Vec<WorkspaceJobHistory>,
    job_histories_omitted: &mut usize,
    message_candidates: &mut Vec<WorkspaceMessageCandidateInput>,
    chat_by_source_job: &mut BTreeMap<(String, String), WorkspaceChatIdentity>,
    chats_by_source_binding_thread: &mut BTreeMap<
        (String, String, String),
        Vec<WorkspaceChatIdentity>,
    >,
) {
    let source_id = &spec.source.id;
    let core_states = load_source_core_states_readonly(spec);

    // 1. Read native samples across deduplicated runtime_state_dirs in strictly read-only mode.
    let (source_samples, source_samples_omitted, sample_warnings) =
        read_and_reconcile_source_native_samples(spec, generated_at_ms, sample_cache);
    *native_samples_omitted = native_samples_omitted.saturating_add(source_samples_omitted);
    warning_collector.extend_source(source_id, WARNING_ORIGIN_SNAPSHOT, sample_warnings);

    // 2. Discover candidate four-part chat identities strictly from:
    //    (a) verified full native sample identities,
    //    (b) trusted SourceSpec hints when ALL components exist,
    //    (c) verified coworker ledgers or already-projected scoped jobs with verified binding,
    //        thread_uuid, and canonical project scope.
    let mut candidate_chats: BTreeMap<String, WorkspaceChatIdentity> = BTreeMap::new();

    for sample in &source_samples {
        if let Some(key) = sample.chat_identity.canonical_key() {
            candidate_chats.insert(key, sample.chat_identity.clone());
        }
    }

    if let (Some(project_root), Some(binding_id), Some(thread_uuid)) = (
        spec.project_root_hint.as_ref(),
        spec.binding_id_hint.as_deref(),
        spec.thread_uuid_hint.as_deref(),
    ) {
        if let Some(identity) = project_root
            .to_str()
            .and_then(|p| verified_chat_identity_for_spec(spec, binding_id, thread_uuid, p))
        {
            if let Some(key) = identity.canonical_key() {
                candidate_chats.insert(key, identity);
            }
        }
    }

    let mut coworker_warnings = Vec::new();
    let coworker_ledgers = load_verified_coworker_ledgers(&spec.plans_dir, &mut coworker_warnings);
    warning_collector.extend_source(source_id, WARNING_ORIGIN_JOBS, coworker_warnings);

    let mut coworker_ledger_job_ids: BTreeSet<String> = BTreeSet::new();
    let mut coworker_jobs_by_id: BTreeMap<String, CoworkerJobRecord> = BTreeMap::new();
    collect_coworker_ledger_identities(
        spec,
        &coworker_ledgers,
        &mut candidate_chats,
        &mut coworker_ledger_job_ids,
        &mut coworker_jobs_by_id,
        warning_collector,
    );

    // Legacy-only projected jobs (excluding any coworker ledger jobs so tampered/unknown coworker
    // records never bypass ledger scope checks).
    for job in projected_jobs
        .iter()
        .filter(|j| &j.source_id == source_id && !coworker_ledger_job_ids.contains(&j.id))
    {
        if let (Some(binding_id), Some(thread_uuid), Some(project_path)) = (
            job.binding_id.as_deref(),
            job.thread_uuid.as_deref(),
            job.project_path.as_deref(),
        ) {
            if let Some(identity) =
                verified_chat_identity_for_spec(spec, binding_id, thread_uuid, project_path)
            {
                if let Some(key) = identity.canonical_key() {
                    candidate_chats.insert(key, identity);
                }
            }
        }
    }

    all_native_samples.extend(source_samples.iter().cloned());

    let samples_by_chat_key: BTreeMap<String, &WorkspaceNativeSample> = source_samples
        .iter()
        .filter_map(|s| Some((s.chat_identity.canonical_key()?, s)))
        .collect();

    // Load frozen plan revisions read-only (without creating `plans.lock`) for safe plan summaries.
    let plan_revisions_by_key = load_plan_revisions_map_readonly(&spec.plans_dir);

    let stores = WorkspaceHistoryStores::new(&spec.plans_dir)
        .with_authority_dir(&spec.authority_dir)
        .with_sessions_dir(&spec.sessions_dir);

    let mut native_projected_job_ids: BTreeSet<String> = BTreeSet::new();
    let mut deduped_plan_ids: BTreeSet<String> = BTreeSet::new();
    let mut chat_job_stats: BTreeMap<String, ChatJobStats> = BTreeMap::new();

    // 3. Read scoped coworker job histories for every verified chat identity.
    for (chat_key, identity) in &candidate_chats {
        register_verified_chat_lookup(identity, chats_by_source_binding_thread);

        match read_scoped_workspace_job_histories(identity, &stores) {
            Ok(scoped_read) => {
                *job_histories_omitted =
                    job_histories_omitted.saturating_add(scoped_read.jobs_omitted);
                record_history_diagnostics(source_id, &scoped_read.diagnostics, warning_collector);

                let stats = chat_job_stats.entry(chat_key.clone()).or_default();
                for mut history in scoped_read.jobs {
                    let job_id = history.job.job_id.clone();
                    stats.observe_history(&history);
                    chat_by_source_job
                        .insert((source_id.clone(), job_id.clone()), identity.clone());

                    if let Some(ref plan_id) = history.links.source_plan_id {
                        deduped_plan_ids.insert(plan_id.clone());
                    }

                    attach_correlated_request_ids(&mut history, projected_requests);

                    if query
                        .job_id
                        .as_ref()
                        .is_none_or(|target_job_id| target_job_id == &job_id)
                    {
                        collect_coworker_job_message_candidates(
                            identity,
                            &history,
                            coworker_jobs_by_id.get(&job_id),
                            &plan_revisions_by_key,
                            message_candidates,
                        );
                        if native_projected_job_ids.insert(job_id) {
                            all_job_histories.push(history);
                        }
                    } else {
                        native_projected_job_ids.insert(job_id);
                    }
                }
            }
            Err(diag) => {
                record_history_diagnostics(source_id, &[diag], warning_collector);
            }
        }
    }

    // 4. Seed fallback histories for genuine legacy-only plan/host jobs that were not projected
    //    or deduplicated by native coworker job history. Legacy completion NEVER claims verified
    //    acceptance proof.
    for legacy_job in projected_jobs.iter().filter(|j| &j.source_id == source_id) {
        if coworker_ledger_job_ids.contains(&legacy_job.id)
            || native_projected_job_ids.contains(&legacy_job.id)
            || deduped_plan_ids.contains(&legacy_job.id)
        {
            continue;
        }

        let verified_chat = match (
            legacy_job.binding_id.as_deref(),
            legacy_job.thread_uuid.as_deref(),
            legacy_job.project_path.as_deref(),
        ) {
            (Some(b), Some(t), Some(p)) => verified_chat_identity_for_spec(spec, b, t, p),
            _ => None,
        };

        let legacy_task_ids: Vec<String> =
            legacy_job.tasks.iter().map(|task| task.id.clone()).collect();
        let verified_external_resolutions = match (
            verified_chat.as_ref(),
            legacy_job.binding_id.as_deref(),
            legacy_job.thread_uuid.as_deref(),
            legacy_job.project_path.as_deref(),
            legacy_job.plan_id.as_deref(),
            legacy_job.plan_revision,
        ) {
            (
                Some(chat_identity),
                Some(binding_id),
                Some(thread_uuid),
                Some(project_scope),
                Some(plan_id),
                Some(plan_revision),
            ) => {
                let scope = LegacyPlanObservationScope {
                    binding_id,
                    thread_uuid,
                    canonical_project_scope: project_scope,
                    plan_id,
                    plan_revision,
                    task_ids: &legacy_task_ids,
                };
                match read_legacy_plan_external_resolutions_readonly(
                    chat_identity, &stores, &scope,
                ) {
                    Ok(verified) => {
                        record_history_diagnostics(
                            source_id,
                            verified.diagnostics(),
                            warning_collector,
                        );
                        Some(verified)
                    }
                    Err(diag) => {
                        record_history_diagnostics(source_id, &[diag], warning_collector);
                        None
                    }
                }
            }
            _ => None,
        };
        let mut legacy_history = build_legacy_fallback_job_history(
            legacy_job,
            verified_chat,
            verified_external_resolutions.as_ref(),
        );
        attach_correlated_request_ids(&mut legacy_history, projected_requests);

        if let Some(ref chat_id) = legacy_history.job.chat_identity {
            if let Some(chat_key) = chat_id.canonical_key() {
                chat_job_stats
                    .entry(chat_key)
                    .or_default()
                    .observe_history(&legacy_history);
                chat_by_source_job
                    .insert((source_id.clone(), legacy_job.id.clone()), chat_id.clone());
            }
        }

        if query
            .job_id
            .as_ref()
            .is_none_or(|target_job_id| target_job_id == &legacy_job.id)
        {
            collect_legacy_job_message_candidates(legacy_job, &legacy_history, message_candidates);
            all_job_histories.push(legacy_history);
        }
    }

    // 5. Assemble canonical WorkspaceChatSummary entries (including verified jobless native chats).
    for (chat_key, identity) in candidate_chats {
        let stats = chat_job_stats.remove(&chat_key).unwrap_or_default();
        let native_sample = samples_by_chat_key.get(&chat_key).copied();

        let thread_label = resolve_verified_chat_thread_label(
            &identity,
            &core_states,
            projected_jobs,
            projected_requests,
        );
        let project_label = derive_project_label(&identity.canonical_project_scope);

        let mut updated_at_ms = stats.max_updated_at_ms;
        if let Some(sample_obs_ms) = native_sample.and_then(|s| s.observed_at_ms) {
            if updated_at_ms.is_none_or(|prev| sample_obs_ms > prev) {
                updated_at_ms = Some(sample_obs_ms);
            }
        }

        let (availability, activity, configured_selection) = match native_sample {
            Some(sample) => {
                let eval = sample.evaluate_at(
                    generated_at_ms,
                    WorkspaceSamplingPolicy::default().stale_threshold_ms,
                );
                (
                    eval.effective_availability,
                    eval.effective_activity,
                    eval.effective_configured_selection,
                )
            }
            None => (
                WorkspaceAvailabilityState::Unavailable,
                WorkspaceActivityState::Unknown,
                WorkspaceConfiguredSelection::unavailable(),
            ),
        };

        let summary = WorkspaceChatSummary {
            identity,
            thread_label,
            project_label,
            availability,
            activity,
            configured_selection,
            active_job_count: stats.active_job_count,
            attention_count: stats.attention_count,
            updated_at_ms,
        }
        .normalized();

        all_chats.insert(chat_key, summary);
    }
}

#[derive(Debug, Default)]
struct ChatJobStats {
    active_job_count: usize,
    attention_count: usize,
    max_updated_at_ms: Option<i64>,
}

impl ChatJobStats {
    fn observe_history(&mut self, history: &WorkspaceJobHistory) {
        let is_active = matches!(
            history.job.state,
            ObservationJobState::Planning
                | ObservationJobState::Ready
                | ObservationJobState::Running
                | ObservationJobState::Reviewing
                | ObservationJobState::NeedsUser
        );
        if is_active {
            self.active_job_count += 1;
        }
        let has_explicit_attention = history.current_attention.as_ref().is_some_and(|attn| {
            !matches!(
                attn.kind,
                WorkspaceAttentionKind::None | WorkspaceAttentionKind::Unknown
            )
        });
        let has_task_attention = history.tasks.iter().any(|task| {
            task.current_attention.as_ref().is_some_and(|attn| {
                !matches!(
                    attn.kind,
                    WorkspaceAttentionKind::None | WorkspaceAttentionKind::Unknown
                )
            })
        });
        let covered_by_full_external_resolution = history.external_resolutions.iter().any(|res| {
            res.applicability.is_applicable()
                && res.scope == WorkspaceExternalResolutionScope::Job
        });
        let needs_attention = has_explicit_attention
            || has_task_attention
            || (history.job.state == ObservationJobState::NeedsUser
                && !covered_by_full_external_resolution);
        if needs_attention {
            self.attention_count += 1;
        }
        if let Some(ts) = history.job.updated_at_ms {
            if self.max_updated_at_ms.is_none_or(|prev| ts > prev) {
                self.max_updated_at_ms = Some(ts);
            }
        }
    }
}

/// Applies the same store-verified external-resolution result to legacy `ObservationJob` reads.
///
/// Only an applicable full-job receipt that leaves no current attention replaces the projection
/// state with `unknown` plus the separate logical `resolved_externally` execution token. It never
/// fabricates `completed`, `verified_accepted`, worker outcomes, task stages, or delivery/ACK state.
fn reconcile_observation_jobs_with_external_resolutions(
    observation_jobs: &mut [ObservationJob],
    histories: &[WorkspaceJobHistory],
) {
    for observation_job in observation_jobs.iter_mut() {
        if observation_job.state == ObservationJobState::Completed {
            continue;
        }
        let Some(history) = histories.iter().find(|history| {
            history.job.source_id == observation_job.source_id
                && history.job.job_id == observation_job.id
        }) else {
            continue;
        };
        let has_applicable_full_receipt = history.external_resolutions.iter().any(|resolution| {
            resolution.applicability.is_applicable()
                && resolution.scope == WorkspaceExternalResolutionScope::Job
        });
        if !has_applicable_full_receipt
            || history.current_attention.is_some()
            || history
                .tasks
                .iter()
                .any(|task| task.current_attention.is_some())
        {
            continue;
        }
        observation_job.state = ObservationJobState::Unknown;
        observation_job.execution_status = "resolved_externally".to_string();
    }
}

fn read_and_reconcile_source_native_samples(
    spec: &SourceSpec,
    generated_at_ms: i64,
    sample_cache: &BoundedSampleCache,
) -> (Vec<WorkspaceNativeSample>, usize, Vec<String>) {
    let mut disk_samples_by_key: BTreeMap<String, WorkspaceNativeSample> = BTreeMap::new();
    let mut omitted = 0usize;
    let mut warnings = Vec::new();

    let mut seen_paths = BTreeSet::new();
    #[cfg(unix)]
    let mut seen_inodes = BTreeSet::new();

    for state_dir in &spec.runtime_state_dirs {
        let normalized_dir = fs::canonicalize(state_dir).unwrap_or_else(|_| state_dir.clone());
        if !seen_paths.insert(normalized_dir) {
            continue;
        }
        match fs::symlink_metadata(state_dir) {
            Err(err) if err.kind() == ErrorKind::NotFound => continue,
            Err(_) => {
                push_unique_string_warning(&mut warnings, WARN_NATIVE_SAMPLES_READ_FAILED);
                continue;
            }
            Ok(meta) => {
                #[cfg(unix)]
                if !meta.file_type().is_symlink() && !seen_inodes.insert((meta.dev(), meta.ino())) {
                    continue;
                }
                #[cfg(not(unix))]
                let _ = meta;
            }
        }

        match read_workspace_native_samples_readonly(
            state_dir,
            Some(&spec.source.id),
            generated_at_ms,
        ) {
            Ok(report) => {
                omitted = omitted.saturating_add(report.omitted_count);
                for raw_warning in report.warnings {
                    let clean = sanitize_observation_summary(&raw_warning);
                    if !clean.is_empty() {
                        push_unique_string_warning(
                            &mut warnings,
                            &format!("Source {}: {clean}", spec.source.id),
                        );
                    }
                }

                for entry in report.entries {
                    let sample = entry.sample;
                    if sample.chat_identity.source_id != spec.source.id
                        || sample.validate().is_err()
                        || !is_verified_project_scope_for_spec(
                            spec,
                            &sample.chat_identity.canonical_project_scope,
                        )
                    {
                        omitted = omitted.saturating_add(1);
                        push_unique_string_warning(
                            &mut warnings,
                            WARN_NATIVE_SAMPLE_SCOPE_REJECTED,
                        );
                        continue;
                    }
                    let Some(chat_key) = sample.chat_identity.canonical_key() else {
                        omitted = omitted.saturating_add(1);
                        continue;
                    };

                    match disk_samples_by_key.get(&chat_key) {
                        None => {
                            disk_samples_by_key.insert(chat_key, sample);
                        }
                        Some(existing) => {
                            let decision =
                                reconcile_workspace_native_sample(Some(existing), &sample);
                            if decision.is_accepted() {
                                disk_samples_by_key.insert(chat_key, sample);
                            } else if !matches!(
                                decision,
                                WorkspaceSampleReconcileDecision::RejectedDuplicateSequence
                            ) {
                                omitted = omitted.saturating_add(1);
                                push_unique_string_warning(
                                    &mut warnings,
                                    WARN_NATIVE_SAMPLE_REGRESSED,
                                );
                            }
                        }
                    }
                }
            }
            Err(_) => {
                push_unique_string_warning(&mut warnings, WARN_NATIVE_SAMPLES_READ_FAILED);
            }
        }
    }

    let reconciled =
        sample_cache.reconcile_source_samples(&spec.source.id, disk_samples_by_key, &mut warnings);
    let (bounded, cap_omitted) = bound_workspace_native_samples_per_source(reconciled);
    (bounded, omitted.saturating_add(cap_omitted), warnings)
}

fn collect_coworker_ledger_identities(
    spec: &SourceSpec,
    ledgers: &[CoworkerBindingLedger],
    candidate_chats: &mut BTreeMap<String, WorkspaceChatIdentity>,
    coworker_ledger_job_ids: &mut BTreeSet<String>,
    coworker_jobs_by_id: &mut BTreeMap<String, CoworkerJobRecord>,
    warning_collector: &mut ObservationWarningCollector,
) {
    let mut scope_warnings = Vec::new();
    let mut seen_ids = BTreeSet::new();
    let mut conflicted_ids = BTreeSet::new();

    for ledger in ledgers {
        for job in &ledger.jobs {
            coworker_ledger_job_ids.insert(job.job_id.clone());
            if !seen_ids.insert(job.job_id.clone()) {
                conflicted_ids.insert(job.job_id.clone());
            }
        }
    }

    for ledger in ledgers {
        let Some(ref ledger_thread_uuid) = ledger.thread_uuid else {
            if !ledger.jobs.is_empty() {
                push_unique_string_warning(&mut scope_warnings, WARN_COWORKER_SCOPE_REJECTED);
            }
            continue;
        };

        for grant in &ledger.planning_grants {
            if grant.binding_id == ledger.binding_id && &grant.thread_uuid == ledger_thread_uuid {
                if let Some(identity) = verified_chat_identity_for_spec(
                    spec,
                    &grant.binding_id,
                    &grant.thread_uuid,
                    &grant.project_base.repository_path,
                ) {
                    if let Some(key) = identity.canonical_key() {
                        candidate_chats.insert(key, identity);
                    }
                }
            }
        }

        for job in &ledger.jobs {
            if conflicted_ids.contains(&job.job_id) {
                continue;
            }
            let repo_path = &job.work_brief.project_base.repository_path;
            let verified_identity = if job.binding_id == ledger.binding_id
                && &job.thread_uuid == ledger_thread_uuid
                && job.work_brief.binding_id == job.binding_id
                && job.work_brief.thread_uuid == job.thread_uuid
                && job.validate().is_ok()
            {
                verified_chat_identity_for_spec(spec, &job.binding_id, &job.thread_uuid, repo_path)
            } else {
                None
            };

            let Some(identity) = verified_identity else {
                push_unique_string_warning(&mut scope_warnings, WARN_COWORKER_SCOPE_REJECTED);
                continue;
            };

            if let Some(key) = identity.canonical_key() {
                candidate_chats.insert(key, identity);
                coworker_jobs_by_id.insert(job.job_id.clone(), job.clone());
            }
        }
    }

    warning_collector.extend_source(&spec.source.id, WARNING_ORIGIN_JOBS, scope_warnings);
}

fn verified_chat_identity_for_spec(
    spec: &SourceSpec,
    binding_id: &str,
    thread_uuid: &str,
    raw_project_path: &str,
) -> Option<WorkspaceChatIdentity> {
    let identity =
        WorkspaceChatIdentity::new(&spec.source.id, binding_id, thread_uuid, raw_project_path)
            .ok()?;
    is_verified_project_scope_for_spec(spec, &identity.canonical_project_scope).then_some(identity)
}

fn is_verified_project_scope_for_spec(spec: &SourceSpec, raw_path: &str) -> bool {
    if raw_path.is_empty()
        || raw_path.len() > MAX_WORKER_PATH_BYTES
        || raw_path.trim() != raw_path
        || !raw_path.starts_with('/')
        || raw_path == "/"
        || raw_path.chars().any(char::is_control)
        || raw_path.contains("//")
        || raw_path == "/."
        || raw_path == "/.."
        || raw_path.contains("/./")
        || raw_path.contains("/../")
        || raw_path.ends_with("/.")
        || raw_path.ends_with("/..")
        || raw_path.ends_with('/')
    {
        return false;
    }
    let path = Path::new(raw_path);
    if !is_safe_repository_path(path) {
        return false;
    }
    let Ok(meta) = fs::symlink_metadata(path) else {
        return false;
    };
    if meta.file_type().is_symlink() || !meta.is_dir() {
        return false;
    }
    let Ok(canonical) = fs::canonicalize(path) else {
        return false;
    };
    if canonical != path || canonical.to_str() != Some(raw_path) {
        return false;
    }

    let store_dirs = [
        spec.registry_dir.as_path(),
        spec.plans_dir.as_path(),
        spec.authority_dir.as_path(),
        spec.sessions_dir.as_path(),
    ];
    for store_dir in store_dirs {
        let clean_store = fs::canonicalize(store_dir).unwrap_or_else(|_| store_dir.to_path_buf());
        if paths_overlap(&canonical, &clean_store) {
            return false;
        }
    }
    for rt_dir in &spec.runtime_state_dirs {
        let clean_rt = fs::canonicalize(rt_dir).unwrap_or_else(|_| rt_dir.clone());
        if paths_overlap(&canonical, &clean_rt) {
            return false;
        }
    }
    true
}

fn paths_overlap(left: &Path, right: &Path) -> bool {
    left.starts_with(right) || right.starts_with(left)
}

fn load_source_core_states_readonly(spec: &SourceSpec) -> Vec<ReadonlyCoreStateSnapshot> {
    let mut snapshots = Vec::new();
    let mut seen_dirs = BTreeSet::new();
    for state_dir in &spec.runtime_state_dirs {
        let key = fs::canonicalize(state_dir).unwrap_or_else(|_| state_dir.clone());
        if !seen_dirs.insert(key) {
            continue;
        }
        let db_path = state_dir.join(CORE_STATE_DB_FILE_NAME);
        if let Ok(snapshot) = read_core_state_readonly(&db_path) {
            snapshots.push(snapshot);
        }
    }
    snapshots
}

fn load_plan_revisions_map_readonly(plans_dir: &Path) -> BTreeMap<(String, u64), PlanRevision> {
    match fs::symlink_metadata(plans_dir) {
        Ok(_) => {}
        Err(_) => return BTreeMap::new(),
    }
    let Ok(revisions) = PlanStore::read_snapshot_readonly_at(plans_dir) else {
        return BTreeMap::new();
    };
    revisions
        .into_iter()
        .map(|rev| ((rev.plan_id.clone(), rev.revision), rev))
        .collect()
}

fn resolve_verified_chat_thread_label(
    identity: &WorkspaceChatIdentity,
    core_states: &[ReadonlyCoreStateSnapshot],
    projected_jobs: &[ObservationJob],
    projected_requests: &[ObservationRequest],
) -> String {
    if let Some(label) = resolve_thread_label(
        &identity.binding_id,
        &identity.thread_uuid,
        core_states,
        None,
    ) {
        return sanitize_observation_label(&label, "Codex chat");
    }

    for job in projected_jobs {
        if job.source_id == identity.source_id
            && job.binding_id.as_deref() == Some(identity.binding_id.as_str())
            && job.thread_uuid.as_deref() == Some(identity.thread_uuid.as_str())
        {
            if let Some(ref label) = job.thread_label {
                let clean = sanitize_observation_label(label, "");
                if !clean.is_empty() {
                    return clean;
                }
            }
        }
    }

    for req in projected_requests {
        if req.source_id == identity.source_id
            && req.binding_id.as_deref() == Some(identity.binding_id.as_str())
            && req.thread_uuid.as_deref() == Some(identity.thread_uuid.as_str())
        {
            if let Some(ref label) = req.thread_label {
                let clean = sanitize_observation_label(label, "");
                if !clean.is_empty() {
                    return clean;
                }
            }
        }
    }

    "Codex chat".to_string()
}

fn derive_project_label(canonical_project_scope: &str) -> String {
    let raw = Path::new(canonical_project_scope)
        .file_name()
        .and_then(|os| os.to_str())
        .unwrap_or(FALLBACK_VERIFIED_PROJECT_LABEL);
    sanitize_observation_label(raw, FALLBACK_VERIFIED_PROJECT_LABEL)
}

fn build_legacy_fallback_job_history(
    job: &ObservationJob,
    verified_chat: Option<WorkspaceChatIdentity>,
    verified_external_resolutions: Option<&VerifiedLegacyPlanExternalResolutions>,
) -> WorkspaceJobHistory {
    let tasks = job
        .tasks
        .iter()
        .map(|t| WorkspaceTaskHistory {
            task_id: t.id.clone(),
            title: t.title.clone(),
            status: t.status.clone(),
            depends_on: t.depends_on.clone(),
            acceptance_proof: WorkspaceAcceptanceProof::unknown(),
            ..WorkspaceTaskHistory::default()
        })
        .collect();

    let full_external_resolution =
        verified_external_resolutions.is_some_and(|verified| verified.has_applicable_full_job());
    let mut history = WorkspaceJobHistory {
        job: WorkspaceJobSummary {
            source_id: job.source_id.clone(),
            job_id: job.id.clone(),
            binding_id: job.binding_id.clone(),
            chat_identity: verified_chat,
            title: job.title.clone(),
            job_revision: None,
            state: job.state,
            phase: None,
            approval_status: job.approval_status.clone(),
            execution_status: job.execution_status.clone(),
            review_state: job.review_state.clone(),
            created_at_ms: None,
            updated_at_ms: job.updated_at_ms,
            summary: job.summary.clone(),
        },
        tasks,
        links: WorkspaceRecordLinks {
            source_plan_id: job.plan_id.clone(),
            source_plan_revision: job.plan_revision,
            ..WorkspaceRecordLinks::default()
        },
        acceptance_proof: WorkspaceAcceptanceProof::unknown(),
        external_resolutions: verified_external_resolutions
            .map(|verified| verified.resolutions().to_vec())
            .unwrap_or_default(),
        ..WorkspaceJobHistory::default()
    };
    if full_external_resolution {
        // External resolution is a separate logical token: preserve the old fallback identity,
        // stored phase/outcomes, tasks, and acceptance proof without fabricating completion.
        history.job.state = ObservationJobState::Unknown;
        history.job.execution_status = "resolved_externally".to_string();
    }
    history.normalized()
}

fn attach_correlated_request_ids(
    history: &mut WorkspaceJobHistory,
    projected_requests: &[ObservationRequest],
) {
    for req in projected_requests {
        if req.source_id == history.job.source_id
            && req.job_id.as_deref() == Some(history.job.job_id.as_str())
            && !history.links.source_request_ids.contains(&req.id)
        {
            history.links.source_request_ids.push(req.id.clone());
        }
    }
}

fn register_verified_chat_lookup(
    identity: &WorkspaceChatIdentity,
    lookup: &mut BTreeMap<(String, String, String), Vec<WorkspaceChatIdentity>>,
) {
    let key = (
        identity.source_id.clone(),
        identity.binding_id.clone(),
        identity.thread_uuid.clone(),
    );
    let list = lookup.entry(key).or_default();
    if !list.contains(identity) {
        list.push(identity.clone());
    }
}

fn record_history_diagnostics(
    source_id: &str,
    diagnostics: &[WorkspaceHistoryDiagnostic],
    warning_collector: &mut ObservationWarningCollector,
) {
    let mut formatted = Vec::new();
    for diag in diagnostics {
        // Missing optional plans_dir on a fresh jobless source is expected when no jobs exist yet;
        // surface all scope-rejection, degraded-store, approval, runtime, and acceptance diagnostics.
        if diag.kind == WorkspaceHistoryDiagnosticKind::MissingStore && diag.job_id.is_none() {
            continue;
        }
        let msg = match &diag.job_id {
            Some(job_id) => format!("Source {source_id} (job {job_id}): {}", diag.message),
            None => format!("Source {source_id}: {}", diag.message),
        };
        push_unique_string_warning(&mut formatted, &msg);
    }
    warning_collector.extend_source(source_id, WARNING_ORIGIN_JOBS, formatted);
}

fn collect_coworker_job_message_candidates(
    identity: &WorkspaceChatIdentity,
    history: &WorkspaceJobHistory,
    raw_job: Option<&CoworkerJobRecord>,
    plan_revisions_by_key: &BTreeMap<(String, u64), PlanRevision>,
    out: &mut Vec<WorkspaceMessageCandidateInput>,
) {
    let job_scope = WorkspaceMessageScope {
        source_id: identity.source_id.clone(),
        binding_id: Some(identity.binding_id.clone()),
        thread_uuid: Some(identity.thread_uuid.clone()),
        canonical_project_scope: Some(identity.canonical_project_scope.clone()),
        chat_identity: Some(identity.clone()),
        job_id: Some(history.job.job_id.clone()),
        task_id: None,
        run_id: None,
        attempt_no: None,
    };

    // 1. Work brief summary (preserves exact recorded language without reading instructions/transcripts).
    if let Some(job_rec) = raw_job {
        if !job_rec.work_brief.summary.trim().is_empty() {
            out.push(WorkspaceMessageCandidateInput {
                source_kind: "work_brief_summary".to_string(),
                source_record_id: job_rec.work_brief.brief_id.clone(),
                source_order: WorkspaceMessageSourceOrder {
                    recorded_at_ms: history.job.created_at_ms,
                    source_seq: 1,
                    sub_index: 0,
                },
                scope: job_scope.clone(),
                actor_role: Some("coworker".to_string()),
                raw_summary: job_rec.work_brief.summary.clone(),
                upstream_truncated: false,
                incomplete_reason: None,
            });
        }
    }

    // 2. Verified frozen plan revision summary (`PlanRevision.summary` only; never task instructions).
    if let (Some(ref plan_id), Some(plan_rev_no)) = (
        &history.links.source_plan_id,
        history.links.source_plan_revision,
    ) {
        if let Some(plan_rev) = plan_revisions_by_key.get(&(plan_id.clone(), plan_rev_no)) {
            if plan_rev.binding_id == identity.binding_id && !plan_rev.summary.trim().is_empty() {
                let plan_recorded_ms =
                    (plan_rev.created_at_ms > 0).then_some(plan_rev.created_at_ms);
                out.push(WorkspaceMessageCandidateInput {
                    source_kind: "plan_summary".to_string(),
                    source_record_id: format!("{plan_id}_r{plan_rev_no}"),
                    source_order: WorkspaceMessageSourceOrder {
                        recorded_at_ms: plan_recorded_ms,
                        source_seq: plan_rev_no,
                        sub_index: 0,
                    },
                    scope: job_scope.clone(),
                    actor_role: Some("orchestrator".to_string()),
                    raw_summary: plan_rev.summary.clone(),
                    upstream_truncated: false,
                    incomplete_reason: None,
                });
            }
        }
    }

    // 3. Task attempt result summaries and structured Reviewer verdict summaries.
    for task in &history.tasks {
        let candidate_verdict_summaries: BTreeSet<(u32, &str)> = task
            .candidates
            .iter()
            .filter_map(|c| {
                let _ = c.review_verdict_id.as_ref()?;
                Some((c.attempt_no, c.summary.as_deref()?))
            })
            .collect();

        for attempt in &task.attempts {
            if let Some(ref summary) = attempt.summary {
                if !summary.trim().is_empty()
                    && !candidate_verdict_summaries
                        .contains(&(attempt.attempt_no, summary.as_str()))
                {
                    let record_id = attempt
                        .outbox_id
                        .clone()
                        .or_else(|| attempt.run_id.clone())
                        .unwrap_or_else(|| {
                            format!(
                                "{}_{}_att_{}",
                                history.job.job_id, task.task_id, attempt.attempt_no
                            )
                        });
                    let mut task_scope = job_scope.clone();
                    task_scope.task_id = Some(task.task_id.clone());
                    task_scope.run_id = attempt.run_id.clone();
                    task_scope.attempt_no = Some(attempt.attempt_no);

                    out.push(WorkspaceMessageCandidateInput {
                        source_kind: "task_result_summary".to_string(),
                        source_record_id: record_id,
                        source_order: WorkspaceMessageSourceOrder {
                            recorded_at_ms: attempt.finished_at_ms.or(attempt.started_at_ms),
                            source_seq: u64::from(attempt.attempt_no),
                            sub_index: 0,
                        },
                        scope: task_scope,
                        actor_role: attempt
                            .executor
                            .as_ref()
                            .and_then(|e| e.role.clone())
                            .or_else(|| task.role.clone()),
                        raw_summary: summary.clone(),
                        upstream_truncated: false,
                        incomplete_reason: None,
                    });
                }
            }
        }

        for cand in &task.candidates {
            if let (Some(ref verdict_id), Some(ref summary)) =
                (&cand.review_verdict_id, &cand.summary)
            {
                if !summary.trim().is_empty() {
                    let mut cand_scope = job_scope.clone();
                    cand_scope.task_id = Some(task.task_id.clone());
                    cand_scope.attempt_no = Some(cand.attempt_no);

                    out.push(WorkspaceMessageCandidateInput {
                        source_kind: "review_verdict_summary".to_string(),
                        source_record_id: verdict_id.clone(),
                        source_order: WorkspaceMessageSourceOrder {
                            recorded_at_ms: cand.produced_at_ms,
                            source_seq: u64::from(cand.attempt_no),
                            sub_index: 1,
                        },
                        scope: cand_scope,
                        actor_role: Some("reviewer".to_string()),
                        raw_summary: summary.clone(),
                        upstream_truncated: false,
                        incomplete_reason: None,
                    });
                }
            }
        }
    }

    // 4. Verified Closure summary vs. general job result summary.
    if let Some(ref res) = history.result {
        if !res.summary.trim().is_empty() {
            let job_seq = history.job.job_revision.unwrap_or(0);
            if let Some(ref closure_verdict_id) = res.closure_verdict_id {
                if history.acceptance_proof.status == WorkspaceAcceptanceStatus::VerifiedAccepted {
                    out.push(WorkspaceMessageCandidateInput {
                        source_kind: "closure_summary".to_string(),
                        source_record_id: closure_verdict_id.clone(),
                        source_order: WorkspaceMessageSourceOrder {
                            recorded_at_ms: res
                                .produced_at_ms
                                .or(history.acceptance_proof.verified_at_ms),
                            source_seq: job_seq,
                            sub_index: 0,
                        },
                        scope: job_scope.clone(),
                        actor_role: Some("closure".to_string()),
                        raw_summary: res.summary.clone(),
                        upstream_truncated: false,
                        incomplete_reason: None,
                    });
                }
            } else {
                let incomplete_reason = (history.job.phase.as_deref() == Some("completed")
                    && history.acceptance_proof.status
                        != WorkspaceAcceptanceStatus::VerifiedAccepted)
                    .then(|| "Closure acceptance proof is missing or unverified.".to_string());
                out.push(WorkspaceMessageCandidateInput {
                    source_kind: "job_result_summary".to_string(),
                    source_record_id: format!("res_{}", history.job.job_id),
                    source_order: WorkspaceMessageSourceOrder {
                        recorded_at_ms: res.produced_at_ms.or(history.job.updated_at_ms),
                        source_seq: job_seq,
                        sub_index: 0,
                    },
                    scope: job_scope.clone(),
                    actor_role: Some("coworker".to_string()),
                    raw_summary: res.summary.clone(),
                    upstream_truncated: false,
                    incomplete_reason,
                });
            }
        }
    }

    // 5. Store-verified external resolution receipts. The message identity is the real receipt ID,
    // so Messages and Activity can link the same receipt without requiring a journal record.
    for (idx, resolution) in history.external_resolutions.iter().enumerate() {
        if resolution.summary.trim().is_empty() {
            continue;
        }
        let completed = resolution.applicability.is_applicable();
        out.push(WorkspaceMessageCandidateInput {
            source_kind: "attention_summary".to_string(),
            source_record_id: resolution.receipt_id.clone(),
            source_order: WorkspaceMessageSourceOrder {
                recorded_at_ms: resolution.recorded_at_ms.or(history.job.updated_at_ms),
                source_seq: history.job.job_revision.unwrap_or(0),
                sub_index: idx as u32,
            },
            scope: job_scope.clone(),
            actor_role: Some("operator".to_string()),
            raw_summary: resolution.summary.clone(),
            upstream_truncated: false,
            incomplete_reason: (!completed).then_some(
                "External resolution receipt is stale or unverified; it does not close blockers."
                    .to_string(),
            ),
        });
    }

    // 6. Live attention or pending user question summary.
    if let Some(ref attn) = history.current_attention {
        if !matches!(
            attn.kind,
            WorkspaceAttentionKind::None | WorkspaceAttentionKind::Unknown
        ) && !attn.reason.trim().is_empty()
        {
            let job_seq = history.job.job_revision.unwrap_or(0);
            if attn.kind == WorkspaceAttentionKind::NeedsUserInput {
                if let Some(ref question_id) = attn.question_id {
                    out.push(WorkspaceMessageCandidateInput {
                        source_kind: "question_prompt".to_string(),
                        source_record_id: question_id.clone(),
                        source_order: WorkspaceMessageSourceOrder {
                            recorded_at_ms: attn.updated_at_ms,
                            source_seq: job_seq,
                            sub_index: 0,
                        },
                        scope: job_scope,
                        actor_role: Some("coworker".to_string()),
                        raw_summary: attn.reason.clone(),
                        upstream_truncated: false,
                        incomplete_reason: None,
                    });
                    return;
                }
            }

            let record_id = attn
                .blocking_hold_ids
                .first()
                .cloned()
                .or_else(|| attn.blocking_outbox_ids.first().cloned())
                .unwrap_or_else(|| format!("attn_{}", history.job.job_id));
            out.push(WorkspaceMessageCandidateInput {
                source_kind: "attention_summary".to_string(),
                source_record_id: record_id,
                source_order: WorkspaceMessageSourceOrder {
                    recorded_at_ms: attn.updated_at_ms,
                    source_seq: job_seq,
                    sub_index: 0,
                },
                scope: job_scope,
                actor_role: Some("coworker".to_string()),
                raw_summary: attn.reason.clone(),
                upstream_truncated: false,
                incomplete_reason: None,
            });
        }
    }
}

fn collect_legacy_job_message_candidates(
    legacy_job: &ObservationJob,
    legacy_history: &WorkspaceJobHistory,
    out: &mut Vec<WorkspaceMessageCandidateInput>,
) {
    let chat_id = legacy_history.job.chat_identity.clone();

    let scope = WorkspaceMessageScope {
        source_id: legacy_job.source_id.clone(),
        binding_id: legacy_job.binding_id.clone(),
        thread_uuid: legacy_job.thread_uuid.clone(),
        canonical_project_scope: chat_id.as_ref().map(|c| c.canonical_project_scope.clone()),
        chat_identity: chat_id,
        job_id: Some(legacy_job.id.clone()),
        task_id: None,
        run_id: None,
        attempt_no: None,
    };

    if let (Some(summary), Some(plan_id)) = (&legacy_job.summary, &legacy_job.plan_id) {
        if !summary.trim().is_empty() {
            let plan_rev = legacy_job.plan_revision.unwrap_or(0);
            out.push(WorkspaceMessageCandidateInput {
                source_kind: "plan_summary".to_string(),
                source_record_id: format!("{plan_id}_r{plan_rev}"),
                source_order: WorkspaceMessageSourceOrder {
                    recorded_at_ms: legacy_job.updated_at_ms,
                    source_seq: plan_rev,
                    sub_index: 0,
                },
                scope: scope.clone(),
                actor_role: Some("operator".to_string()),
                raw_summary: summary.clone(),
                upstream_truncated: false,
                incomplete_reason: None,
            });
        }
    }

    // Store-verified legacy-plan external resolution receipts keep the actual receipt ID as the
    // message identity so Jobs, Messages, and Activity can link the same row without a journal.
    for (idx, resolution) in legacy_history.external_resolutions.iter().enumerate() {
        if resolution.summary.trim().is_empty() {
            continue;
        }
        let completed = resolution.applicability.is_applicable();
        out.push(WorkspaceMessageCandidateInput {
            source_kind: "attention_summary".to_string(),
            source_record_id: resolution.receipt_id.clone(),
            source_order: WorkspaceMessageSourceOrder {
                recorded_at_ms: resolution.recorded_at_ms.or(legacy_job.updated_at_ms),
                source_seq: legacy_job.plan_revision.unwrap_or(0),
                sub_index: idx as u32,
            },
            scope: scope.clone(),
            actor_role: Some("operator".to_string()),
            raw_summary: resolution.summary.clone(),
            upstream_truncated: false,
            incomplete_reason: (!completed).then_some(
                "External resolution receipt is stale or unverified; it does not close blockers."
                    .to_string(),
            ),
        });
    }
}

fn collect_request_journal_message_candidates(
    projected_requests: &[ObservationRequest],
    query: &ValidatedSnapshotQuery,
    chat_by_source_job: &BTreeMap<(String, String), WorkspaceChatIdentity>,
    chats_by_source_binding_thread: &BTreeMap<(String, String, String), Vec<WorkspaceChatIdentity>>,
    out: &mut Vec<WorkspaceMessageCandidateInput>,
) {
    for req in projected_requests {
        if let Some(ref target_source) = query.source_id {
            if &req.source_id != target_source {
                continue;
            }
        }
        if let Some(ref target_job) = query.job_id {
            if req.job_id.as_deref() != Some(target_job.as_str()) {
                continue;
            }
        }
        let Some(ref summary) = req.summary else {
            continue;
        };
        if summary.trim().is_empty() {
            continue;
        }

        let matched_chat = req
            .job_id
            .as_ref()
            .and_then(|jid| {
                chat_by_source_job
                    .get(&(req.source_id.clone(), jid.clone()))
                    .cloned()
            })
            .or_else(|| {
                let b = req.binding_id.as_ref()?;
                let t = req.thread_uuid.as_ref()?;
                let candidates = chats_by_source_binding_thread.get(&(
                    req.source_id.clone(),
                    b.clone(),
                    t.clone(),
                ))?;
                (candidates.len() == 1).then(|| candidates[0].clone())
            });

        let scope = WorkspaceMessageScope {
            source_id: req.source_id.clone(),
            binding_id: matched_chat
                .as_ref()
                .map(|c| c.binding_id.clone())
                .or_else(|| req.binding_id.clone()),
            thread_uuid: matched_chat
                .as_ref()
                .map(|c| c.thread_uuid.clone())
                .or_else(|| req.thread_uuid.clone()),
            canonical_project_scope: matched_chat
                .as_ref()
                .map(|c| c.canonical_project_scope.clone()),
            chat_identity: matched_chat,
            job_id: req.job_id.clone(),
            task_id: None,
            run_id: None,
            attempt_no: None,
        };

        let actor_role = req
            .executor
            .as_ref()
            .and_then(|e| e.role.as_deref())
            .and_then(validate_correlation_id)
            .or_else(|| req.actor.role.as_deref().and_then(validate_correlation_id));

        out.push(WorkspaceMessageCandidateInput {
            source_kind: "journal_summary".to_string(),
            source_record_id: req.id.clone(),
            source_order: WorkspaceMessageSourceOrder {
                recorded_at_ms: (req.updated_at_ms > 0).then_some(req.updated_at_ms),
                source_seq: 0,
                sub_index: 0,
            },
            scope,
            actor_role,
            raw_summary: summary.clone(),
            upstream_truncated: false,
            incomplete_reason: None,
        });
    }
}

fn build_safe_messages_window(
    candidates: Vec<WorkspaceMessageCandidateInput>,
    invalid_messages_omitted: &mut usize,
    warning_collector: &mut ObservationWarningCollector,
) -> WorkspaceMessagesWindow {
    let mut valid_inputs = Vec::with_capacity(candidates.len());
    for candidate in candidates {
        if WorkspaceMessageItem::from_safe_input(candidate.clone()).is_ok() {
            valid_inputs.push(candidate);
        } else {
            *invalid_messages_omitted = invalid_messages_omitted.saturating_add(1);
            warning_collector.push_snapshot(
                "Omitted workspace message candidate with invalid safe source or identifier."
                    .to_string(),
            );
        }
    }

    match project_safe_workspace_messages(valid_inputs) {
        Ok(window) => window,
        Err(_) => {
            warning_collector.push_snapshot(
                "Workspace messages projection degraded to an empty safe window.".to_string(),
            );
            WorkspaceMessagesWindow {
                availability: WorkspaceAvailabilityState::Available,
                ..WorkspaceMessagesWindow::default()
            }
            .normalized()
        }
    }
}

fn seed_warning_collector_from_observation(
    collector: &mut ObservationWarningCollector,
    observation: &threadlane_core::observation::ObservationSnapshot,
) {
    let mut contextualized: BTreeSet<&str> = BTreeSet::new();
    for ctx in &observation.warning_contexts {
        contextualized.insert(ctx.warning.as_str());
        if let Some(ref source_id) = ctx.source_id {
            collector.extend_source(source_id, &ctx.origin, std::iter::once(ctx.warning.clone()));
        } else if ctx.origin == "registry" {
            collector.extend_inventory(std::iter::once(ctx.warning.clone()));
        } else {
            collector.push_snapshot(ctx.warning.clone());
        }
    }
    for warning in &observation.warnings {
        if !contextualized.contains(warning.as_str()) {
            collector.push_snapshot(warning.clone());
        }
    }
}

fn push_unique_string_warning(warnings: &mut Vec<String>, message: &str) {
    if !warnings.iter().any(|existing| existing == message) {
        warnings.push(message.to_string());
    }
}

#[cfg(test)]
#[path = "workspace_tests.rs"]
mod workspace_tests;
