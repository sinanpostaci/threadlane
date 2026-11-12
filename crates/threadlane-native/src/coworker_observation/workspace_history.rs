//! Scoped read-only workspace job history projection and historical acceptance verification.
//!
//! ## Core Invariants
//!
//! - **Reuses `threadlane_core::observation` contracts**: Projects [`CoworkerJobRecord`] and
//!   optional [`ControllerJobRuntimeState`] records directly into [`WorkspaceJobHistory`],
//!   [`WorkspaceTaskHistory`], [`WorkspaceTaskAttempt`], [`WorkspaceTaskCandidate`],
//!   [`WorkspaceRunRecord`], [`WorkspaceOutboxRecord`], [`WorkspaceCombinedCandidate`],
//!   [`WorkspaceJobResult`], [`WorkspaceRecordLinks`], [`WorkspaceHistoricalFailure`], and
//!   [`WorkspaceAttentionState`], enforcing bounds via [`WorkspaceJobHistory::normalized`] and
//!   [`bound_workspace_job_histories`].
//! - **Pure read-only access**: Opens [`CoworkerWorkflowStore`], [`ControllerPolicyStore`],
//!   [`FinalCandidateClosureStore`], and optional [`PlanStore`], [`AuthorityStore`], and
//!   `sessions_dir` strictly through read-only entry points. Never creates directories, lockfiles,
//!   caches, reservations, reconciliations, or repair writes, and never reads provider transcripts,
//!   raw worker stdout/stderr logs, or task instruction prompts.
//! - **Strict four-part chat and immutable frozen scope verification**: Every projected job must
//!   match the caller's validated [`WorkspaceChatIdentity`] (`source_id`, `binding_id`,
//!   `thread_uuid`, and `canonical_project_scope`) as well as internal brief, plan, approval, and
//!   runtime envelope relationships against the job's original frozen revisions. Project scope is
//!   never inferred from chat titles or thread metadata alone.
//! - **Separated historical failure and live attention**: Earlier failed or superseded attempts
//!   remain visible in [`WorkspaceHistoricalFailure`] (`superseded_or_resolved = true`) after a
//!   subsequent retry advances or completes, without fabricating live
//!   [`WorkspaceAttentionKind::NeedsApproval`]. When genuine historical acceptance is verified,
//!   only `current_attention` is cleared while earlier failed attempts and historical failure
//!   summaries remain intact.
//! - **Store-backed historical acceptance verification**: [`WorkspaceAcceptanceStatus::VerifiedAccepted`]
//!   and green [`ObservationJobState::Completed`] are derived exclusively when
//!   [`read_scoped_workspace_job_histories`] verifies an exact 1-to-1 chain across the immutable
//!   persisted [`FinalCandidateSubject`], terminal `Accepted` [`ClosureVerdict`] in
//!   [`FinalCandidateClosureStore`], passing candidate check evidence/reports, unfenced Closure
//!   artifacts, and per-task persisted `ReviewReceipt` and `IntegrationReceipt` records. Pure
//!   in-memory projection helpers without store-backed receipt verification always remain
//!   [`WorkspaceAcceptanceProof::unknown`]. Historical observation verifies immutable persisted
//!   digests, commits, trees, normalized frozen command-path data, and original frozen revisions
//!   rather than requiring current live Git `HEAD`, active plan status, latest registry revision
//!   equality, or continued on-disk availability of past worker/check executables.
//! - **Scope-first causal lineage validation**: Verified in-scope amendment `follow_up_job_ids`
//!   are preserved in deterministic `(target_job_revision, job_id)` order, while conflicting
//!   admitted successors, cycles, or missing/unverified targets in a connected local lineage
//!   component conservatively clear canonical `superseded_by_job_id` and refuse obsolete-planning
//!   attention retirement regardless of ledger vector order. Scope-rejected foreign records never
//!   mint proof or poison verified local components.
//! - **Store-backed awaiting-plan publication metadata**: [`WorkspaceAttentionKind::NeedsApproval`],
//!   [`ObservationJobState::NeedsUser`], and `approval_status = "pending_approval"` for an
//!   unapproved [`CoworkerJobPhase::AwaitingApproval`] job are emitted only when
//!   [`read_scoped_workspace_job_histories`] verifies a current active-published [`PlanRevision`]
//!   head and matching profile/grant/reservation metadata from the same read-only [`PlanStore`]
//!   snapshot, using bounded wording that describes recorded plan/approval metadata without
//!   claiming authenticated human intent or exact artifact content. Missing, unreadable, stale, or
//!   pure caller-supplied plan context projects [`WorkspaceAttentionKind::Unknown`],
//!   [`ObservationJobState::Unknown`], and `approval_status = "unknown"` without clearing
//!   attention or triggering raw `NeedsUser` fallback resurrection. A caller-supplied
//!   `superseded_by_job_id` link alone never mints `approval_status = "superseded"` for that
//!   unproved awaiting-approval state.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};

use threadlane_authority::{
    is_task_scope_contained_in_operator_allowlist, verify_approval_record_integrity,
    ApprovalRecord, AuthorityStore,
};
use threadlane_core::observation::{
    bound_workspace_job_histories, sanitize_observation_label, sanitize_workspace_summary,
    validate_correlation_id, ObservationDeliveryStage, ObservationExecutor, ObservationJobState,
    WorkspaceAcceptanceProof, WorkspaceAcceptanceStatus, WorkspaceAttentionKind,
    WorkspaceAttentionState, WorkspaceChatIdentity, WorkspaceCombinedCandidate,
    WorkspaceCoveredAttention, WorkspaceExternalResolution, WorkspaceExternalResolutionApplicability,
    WorkspaceExternalResolutionOutcome, WorkspaceExternalResolutionScope,
    WorkspaceHistoricalFailure, WorkspaceJobHistory, WorkspaceJobHistoryTruncation,
    WorkspaceJobResult, WorkspaceJobSummary, WorkspaceOutboxRecord, WorkspaceRecordLinks,
    WorkspaceRunRecord, WorkspaceTaskAttempt, WorkspaceTaskCandidate, WorkspaceTaskHistory,
};
use threadlane_host::{
    read_host_launch_intent_optional, read_host_manifest_optional, verify_existing_private_dir,
    verify_existing_session_dir, HostLaunchIntentStatus, HostPhase, WORK_AREA_SUBDIR,
};
use threadlane_operations::{
    inspect_job_integration_intent_readonly, load_integration_receipt, load_review_receipt,
    verify_legacy_base_closure_insufficient, ClosureAttemptState, ClosureVerdictKind,
    FinalCandidateClosureStore, FinalCandidateSubject, JobIntegrationIntentState,
};
use threadlane_workers::{resolve_model_target, WorkerProvider, WorkerRunStatus};
use threadlane_workflow::{Difficulty, PlanRevision, PlanStore, ToolAdapter, WorkflowRole};

use crate::coworker_driver::coworker_driver_dir;
use crate::coworker_observation::CoworkerObservationExecutorSpec;
use crate::coworker_workflow::{
    read_persisted_candidate_check_report, read_persisted_job_integration_report,
    CombinedCandidateIdentity, CombinedCandidateStatus, ControllerJobRuntimeState,
    ControllerPolicyStore, CoworkerBindingLedger, CoworkerJobLockState, CoworkerJobOwnershipKey,
    CoworkerJobOwnershipStore, CoworkerJobPhase, CoworkerJobRecord, CoworkerTaskRecord,
    CoworkerTaskStage, CoworkerWorkflowStore, FrozenCheckExecutionStatus, FrozenCheckSubject,
    FrozenCheckTargetKind, OutboxDispatchKind, OutboxDispatchRecord, OutboxDispatchStatus,
    PendingControllerEffect, PendingControllerEffectKind, PendingControllerEffectStatus,
    PlanningReservationStatus, ReviewVerdictOutcome, TaskAttemptExecutionArtifacts, TaskAttemptRef,
    TaskCandidateRef, WorkBriefKind, WorkflowHoldReason, WorkflowHoldRecord,
};
use crate::external_resolution::{
    evaluate_receipt_applicability, ExternalResolutionApplicability, ExternalResolutionReceipt,
    ExternalResolutionScope, ExternalResolutionTarget,
};
use crate::fs_safety::constant_time_str_eq;

/// Classification of safe read-only diagnostics emitted while projecting workspace job history.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkspaceHistoryDiagnosticKind {
    /// Requested identity or record failed `source_id`, `binding_id`, `thread_uuid`, or canonical repository scope checks.
    ScopeRejected,
    /// A configured store directory does not exist on disk; read degraded without creating directories or locks.
    MissingStore,
    /// Expected [`ControllerJobRuntimeState`] for an active or executed job was absent; projection degraded honestly.
    MissingRuntime,
    /// Store file or directory failed read-only validation or permission/integrity checks.
    DegradedStore,
    /// Bound approval linkage failed scope or digest verification against [`AuthorityStore`].
    ApprovalVerificationFailed,
    /// Optional [`PlanStore`] read-only lookup failed or returned mismatched plan metadata.
    PlanLookupDegraded,
    /// Optional host session manifest verification failed or was unreadable.
    SessionLookupDegraded,
    /// Persisted review, integration, check, or Closure receipt chain failed read-only verification; acceptance remained unknown.
    AcceptanceVerificationFailed,
    /// A persisted external resolution receipt could not be independently linked to current scope,
    /// revision, authority, plan, runtime, ownership, or host evidence; applicability remained
    /// unverified and covered attention was not retired.
    ExternalResolutionVerificationFailed,
}

/// Sanitized read-only diagnostic emitted during workspace job history projection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceHistoryDiagnostic {
    /// Diagnostic category.
    pub kind: WorkspaceHistoryDiagnosticKind,
    /// Validated `job_id` associated with the diagnostic when applicable.
    pub job_id: Option<String>,
    /// Redacted bounded summary message (`<= 240` chars, free of raw filesystem paths or secrets).
    pub message: String,
}

impl WorkspaceHistoryDiagnostic {
    /// Constructs a sanitized [`WorkspaceHistoryDiagnostic`].
    #[must_use]
    pub fn new(kind: WorkspaceHistoryDiagnosticKind, job_id: Option<&str>, message: &str) -> Self {
        let clean_msg = sanitize_workspace_summary(message);
        Self {
            kind,
            job_id: job_id.and_then(validate_correlation_id),
            message: if clean_msg.is_empty() {
                "Workspace history read diagnostic".to_string()
            } else {
                clean_msg
            },
        }
    }
}

/// Validated read-only store handles and existing private (`0700`) store paths used to read
/// scoped [`WorkspaceJobHistory`] records without side effects.
#[derive(Debug, Clone, Copy)]
pub struct WorkspaceHistoryStores<'a> {
    /// Existing private (`0700`) workflow/plans directory holding `coworker-workflow-*.json` and `coworker-controller-*.json`.
    pub plans_dir: &'a Path,
    /// Optional already-opened [`PlanStore`] handle (inspected via `PlanStore::read_snapshot_readonly_at` without creating `plans.lock`).
    pub plan_store: Option<&'a PlanStore>,
    /// Optional already-opened [`AuthorityStore`] handle (inspected via `AuthorityStore::snapshot_readonly` without creating `authority.lock`).
    pub authority_store: Option<&'a AuthorityStore>,
    /// Optional existing private (`0700`) authority directory when no [`AuthorityStore`] handle is held.
    pub authority_dir: Option<&'a Path>,
    /// Optional existing private (`0700`) host sessions root directory (`<sessions_dir>/<approval_id>`).
    pub sessions_dir: Option<&'a Path>,
}

impl<'a> WorkspaceHistoryStores<'a> {
    /// Constructs read-only store inputs rooted at `plans_dir`.
    #[must_use]
    pub const fn new(plans_dir: &'a Path) -> Self {
        Self {
            plans_dir,
            plan_store: None,
            authority_store: None,
            authority_dir: None,
            sessions_dir: None,
        }
    }

    /// Attaches an optional [`PlanStore`] reference for task-title and plan-scope verification.
    #[must_use]
    pub const fn with_plan_store(mut self, plan_store: &'a PlanStore) -> Self {
        self.plan_store = Some(plan_store);
        self
    }

    /// Attaches an optional [`AuthorityStore`] reference for read-only approval relationship verification.
    #[must_use]
    pub const fn with_authority_store(mut self, authority_store: &'a AuthorityStore) -> Self {
        self.authority_store = Some(authority_store);
        self
    }

    /// Attaches an optional private (`0700`) authority directory path for read-only approval verification.
    #[must_use]
    pub const fn with_authority_dir(mut self, authority_dir: &'a Path) -> Self {
        self.authority_dir = Some(authority_dir);
        self
    }

    /// Attaches an optional private (`0700`) host sessions root directory for read-only manifest checks.
    #[must_use]
    pub const fn with_sessions_dir(mut self, sessions_dir: &'a Path) -> Self {
        self.sessions_dir = Some(sessions_dir);
        self
    }
}

/// Optional verified lineage and companion store records supplied to [`project_coworker_job_history`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WorkspaceJobLineageContext {
    /// Newer same-scope `job_id` that directly superseded this job, when present in the binding ledger.
    pub superseded_by_job_id: Option<String>,
    /// Optional upstream parent `job_id` when distinct from `supersedes_job_id`.
    pub parent_job_id: Option<String>,
    /// Ordered follow-up/amendment `job_id`s in the same verified chat scope referencing this job.
    pub follow_up_job_ids: Vec<String>,
    /// Optional read-only [`PlanRevision`] matching the job's planned or approved `(plan_id, plan_revision)`.
    pub plan_revision: Option<PlanRevision>,
    /// Optional read-only [`ApprovalRecord`] matching the job's `approval_linkage`.
    pub approval_record: Option<ApprovalRecord>,
    /// Marks that expected controller runtime state was missing or unreadable on disk.
    pub runtime_missing_or_degraded: bool,
}

/// Bounded result of reading scoped [`WorkspaceJobHistory`] projections for a single [`WorkspaceChatIdentity`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceScopedHistoryRead {
    /// Verified four-part chat identity owning all returned `jobs`.
    pub chat_identity: WorkspaceChatIdentity,
    /// Bounded, normalized [`WorkspaceJobHistory`] projections belonging exclusively to `chat_identity`.
    pub jobs: Vec<WorkspaceJobHistory>,
    /// Count of valid in-scope jobs omitted due to [`bound_workspace_job_histories`] caps.
    pub jobs_omitted: usize,
    /// Count of records rejected and omitted due to mismatched thread, binding, repository, or approval scope.
    pub rejected_scope_count: usize,
    /// `true` when any store or controller runtime record was missing or degraded during the read.
    pub degraded: bool,
    /// Safe bounded diagnostics collected during the read.
    pub diagnostics: Vec<WorkspaceHistoryDiagnostic>,
}

/// Reads and projects bounded [`WorkspaceJobHistory`] records exclusively for `chat_identity`
/// using pure read-only store accessors.
///
/// - Rejects invalid `chat_identity` components or non-canonical/symlink `canonical_project_scope` paths.
/// - Never creates directories, lockfiles (`coworker-workflow-*.lock`, `plans.lock`, `authority.lock`, `host.lock`), or caches.
/// - Degrades honestly when `plans_dir` or `ControllerJobRuntimeState` is missing, without fabricating acceptance.
pub fn read_scoped_workspace_job_histories(
    chat_identity: &WorkspaceChatIdentity,
    stores: &WorkspaceHistoryStores<'_>,
) -> Result<WorkspaceScopedHistoryRead, WorkspaceHistoryDiagnostic> {
    validate_chat_identity_and_canonical_repo_on_disk(chat_identity)?;

    let mut diagnostics = Vec::new();
    let mut degraded = false;

    match fs::symlink_metadata(stores.plans_dir) {
        Ok(_) => {}
        Err(err) if err.kind() == ErrorKind::NotFound => {
            diagnostics.push(WorkspaceHistoryDiagnostic::new(
                WorkspaceHistoryDiagnosticKind::MissingStore,
                None,
                "Workflow plans store directory is absent; returning empty degraded history.",
            ));
            return Ok(WorkspaceScopedHistoryRead {
                chat_identity: chat_identity.clone(),
                jobs: Vec::new(),
                jobs_omitted: 0,
                rejected_scope_count: 0,
                degraded: true,
                diagnostics,
            });
        }
        Err(_) => {
            return Err(WorkspaceHistoryDiagnostic::new(
                WorkspaceHistoryDiagnosticKind::DegradedStore,
                None,
                "Workflow plans store directory metadata could not be verified.",
            ));
        }
    }

    let workflow_store = CoworkerWorkflowStore::new(stores.plans_dir).map_err(|_| {
        WorkspaceHistoryDiagnostic::new(
            WorkspaceHistoryDiagnosticKind::DegradedStore,
            None,
            "Workflow plans store directory failed private non-symlink validation.",
        )
    })?;

    let ledger = workflow_store
        .read_binding_ledger(&chat_identity.binding_id)
        .map_err(|_| {
            WorkspaceHistoryDiagnostic::new(
                WorkspaceHistoryDiagnosticKind::DegradedStore,
                None,
                "Binding workflow ledger failed read-only integrity validation.",
            )
        })?;

    if let Some(ref pinned_thread) = ledger.thread_uuid {
        if pinned_thread != &chat_identity.thread_uuid {
            let rejected = ledger.jobs.len();
            diagnostics.push(WorkspaceHistoryDiagnostic::new(
                WorkspaceHistoryDiagnosticKind::ScopeRejected,
                None,
                "Binding workflow ledger is pinned to a different native thread UUID.",
            ));
            return Ok(WorkspaceScopedHistoryRead {
                chat_identity: chat_identity.clone(),
                jobs: Vec::new(),
                jobs_omitted: 0,
                rejected_scope_count: rejected,
                degraded,
                diagnostics,
            });
        }
    }

    let policy_store = match ControllerPolicyStore::new(stores.plans_dir) {
        Ok(store) => Some(store),
        Err(_) => {
            degraded = true;
            diagnostics.push(WorkspaceHistoryDiagnostic::new(
                WorkspaceHistoryDiagnosticKind::DegradedStore,
                None,
                "Controller runtime store handle could not be validated read-only.",
            ));
            None
        }
    };

    let mut plan_lookup_degraded = false;
    let plan_snapshot = load_plan_revisions_readonly(
        chat_identity,
        stores,
        &mut degraded,
        &mut plan_lookup_degraded,
        &mut diagnostics,
    );
    let authority_records_by_id =
        load_authority_records_readonly(stores, &mut degraded, &mut diagnostics);
    let authority_store_configured =
        stores.authority_store.is_some() || stores.authority_dir.is_some();
    let authority_store_degraded =
        authority_store_configured && authority_records_by_id.is_none();
    let verified_sessions_root =
        verify_sessions_root_readonly(stores.sessions_dir, &mut degraded, &mut diagnostics);
    let all_approval_records: Vec<ApprovalRecord> = authority_records_by_id
        .as_ref()
        .map(|records| records.values().cloned().collect())
        .unwrap_or_default();
    let all_plan_revisions: Vec<PlanRevision> =
        plan_snapshot.revisions_by_key.values().cloned().collect();

    // Pre-validate every job and its companion records against the verified 4-part scope before
    // building lineage maps or minting store-backed obsolete-planning retirement evidence.
    let mut admitted_jobs: Vec<PrevalidatedScopedJob<'_>> = Vec::with_capacity(ledger.jobs.len());
    let mut rejected_scope_count = 0usize;

    for job in &ledger.jobs {
        let mut runtime_missing_or_degraded = policy_store.is_none();
        let runtime_opt = if let Some(ref pstore) = policy_store {
            match pstore.get_job_runtime_readonly(&chat_identity.binding_id, &job.job_id) {
                Ok(Some(rt)) => Some(rt),
                Ok(None) => {
                    if job_expects_controller_runtime(job) {
                        degraded = true;
                        runtime_missing_or_degraded = true;
                        diagnostics.push(WorkspaceHistoryDiagnostic::new(
                            WorkspaceHistoryDiagnosticKind::MissingRuntime,
                            Some(&job.job_id),
                            "Controller runtime state is missing for executed job; degraded to unverified state.",
                        ));
                    }
                    None
                }
                Err(_) => {
                    degraded = true;
                    runtime_missing_or_degraded = true;
                    diagnostics.push(WorkspaceHistoryDiagnostic::new(
                        WorkspaceHistoryDiagnosticKind::DegradedStore,
                        Some(&job.job_id),
                        "Controller runtime state failed read-only validation; degraded to unverified state.",
                    ));
                    None
                }
            }
        } else {
            None
        };

        let target_plan_key = job
            .approval_linkage
            .as_ref()
            .map(|l| (l.plan_id.as_str(), l.plan_revision))
            .or_else(|| {
                job.planning_result
                    .as_ref()
                    .map(|pr| (pr.plan_id.as_str(), pr.plan_revision))
            });
        let plan_revision = target_plan_key.and_then(|(pid, rev)| {
            plan_snapshot
                .revisions_by_key
                .get(&(pid.to_string(), rev))
                .cloned()
        });

        let mut approval_record = None;
        if let (Some(linkage), Some(records_map)) = (
            job.approval_linkage.as_ref(),
            authority_records_by_id.as_ref(),
        ) {
            let Some(found) = records_map.get(&linkage.approval_id) else {
                rejected_scope_count += 1;
                diagnostics.push(WorkspaceHistoryDiagnostic::new(
                    WorkspaceHistoryDiagnosticKind::ApprovalVerificationFailed,
                    Some(&job.job_id),
                    "Job approval_id was not found in the configured read-only authority store.",
                ));
                continue;
            };
            approval_record = Some(found.clone());
        }

        if let (Some(sroot), Some(ref approval)) = (verified_sessions_root, &approval_record) {
            inspect_session_manifest_readonly(
                sroot,
                approval,
                &job.job_id,
                &mut degraded,
                &mut diagnostics,
            );
        }

        let prelim_context = WorkspaceJobLineageContext {
            superseded_by_job_id: None,
            parent_job_id: None,
            follow_up_job_ids: Vec::new(),
            plan_revision: plan_revision.clone(),
            approval_record: approval_record.clone(),
            runtime_missing_or_degraded,
        };

        if let Err(diag) = validate_job_and_companion_scopes(
            chat_identity,
            job,
            runtime_opt.as_ref(),
            Some(&prelim_context),
        ) {
            rejected_scope_count += 1;
            diagnostics.push(diag);
            continue;
        }
        if job.validate().is_err() {
            rejected_scope_count += 1;
            diagnostics.push(WorkspaceHistoryDiagnostic::new(
                WorkspaceHistoryDiagnosticKind::ScopeRejected,
                Some(&job.job_id),
                "Job record failed typed workflow contract validation.",
            ));
            continue;
        }

        let external_resolutions = verify_scoped_external_resolutions_readonly(
            chat_identity,
            job,
            runtime_opt.as_ref(),
            &ledger,
            &plan_snapshot,
            &all_approval_records,
            &all_plan_revisions,
            authority_records_by_id.as_ref(),
            authority_store_configured,
            policy_store.as_ref(),
            stores,
            verified_sessions_root,
            &mut diagnostics,
        );

        admitted_jobs.push(PrevalidatedScopedJob {
            job,
            runtime_opt,
            plan_revision,
            approval_record,
            runtime_missing_or_degraded,
            external_resolutions,
        });
    }

    let mut admitted_by_id: BTreeMap<&str, &PrevalidatedScopedJob<'_>> = BTreeMap::new();
    for entry in &admitted_jobs {
        admitted_by_id.insert(entry.job.job_id.as_str(), entry);
    }

    let lineage = build_validated_scoped_lineage(&admitted_jobs, &admitted_by_id);

    let mut projected_jobs = Vec::with_capacity(admitted_jobs.len());

    for entry in &admitted_jobs {
        let job = entry.job;
        let context = WorkspaceJobLineageContext {
            superseded_by_job_id: lineage
                .superseded_by_target
                .get(job.job_id.as_str())
                .cloned(),
            parent_job_id: None,
            follow_up_job_ids: lineage
                .follow_ups_by_target
                .get(job.job_id.as_str())
                .cloned()
                .unwrap_or_default(),
            plan_revision: entry.plan_revision.clone(),
            approval_record: entry.approval_record.clone(),
            runtime_missing_or_degraded: entry.runtime_missing_or_degraded,
        };

        let verified_obsolete_planning = verify_obsolete_planning_supersession_readonly(
            &ledger,
            entry,
            &admitted_by_id,
            &lineage,
        );

        let verified_current_published_plan = if verified_obsolete_planning.is_none() {
            verify_current_published_plan_metadata_readonly(
                chat_identity,
                &ledger,
                entry,
                &lineage,
                &plan_snapshot,
                plan_lookup_degraded,
            )
        } else {
            None
        };

        let verified_acceptance = if job.phase == CoworkerJobPhase::Completed {
            let store_ctx = HistoricalStoreVerificationContext {
                stores,
                policy_store: policy_store.as_ref(),
                verified_sessions_root,
                plan_lookup_degraded,
                authority_store_degraded,
            };
            match verify_historical_job_acceptance_readonly(
                chat_identity,
                job,
                entry.runtime_opt.as_ref(),
                &context,
                &store_ctx,
            ) {
                Ok(verified) => Some(verified),
                Err(diag) => {
                    degraded = true;
                    diagnostics.push(diag);
                    None
                }
            }
        } else {
            None
        };

        match project_coworker_job_history_internal(
            chat_identity,
            job,
            entry.runtime_opt.as_ref(),
            Some(&context),
            verified_acceptance.as_ref(),
            verified_obsolete_planning.as_ref(),
            verified_current_published_plan.as_ref(),
            Some(&entry.external_resolutions),
        ) {
            Ok(history) => projected_jobs.push(history),
            Err(diag) => {
                rejected_scope_count += 1;
                diagnostics.push(diag);
            }
        }
    }

    let (bounded_jobs, jobs_omitted) = bound_workspace_job_histories(projected_jobs);

    Ok(WorkspaceScopedHistoryRead {
        chat_identity: chat_identity.clone(),
        jobs: bounded_jobs,
        jobs_omitted,
        rejected_scope_count,
        degraded,
        diagnostics,
    })
}

/// Pure helper projecting a single typed [`CoworkerJobRecord`] and optional
/// [`ControllerJobRuntimeState`] into a normalized [`WorkspaceJobHistory`] for `chat_identity`.
///
/// Without store-backed receipt, Closure, and [`PlanStore`] head verification, `acceptance_proof`
/// always remains [`WorkspaceAcceptanceProof::unknown`] and unapproved awaiting-approval records
/// project [`WorkspaceAttentionKind::Unknown`] (`ObservationJobState::Unknown` / `approval_status = "unknown"`).
pub fn project_coworker_job_record(
    chat_identity: &WorkspaceChatIdentity,
    job: &CoworkerJobRecord,
    runtime: Option<&ControllerJobRuntimeState>,
) -> Result<WorkspaceJobHistory, WorkspaceHistoryDiagnostic> {
    project_coworker_job_history(chat_identity, job, runtime, None)
}

/// Pure helper projecting a single typed [`CoworkerJobRecord`], optional
/// [`ControllerJobRuntimeState`], and optional [`WorkspaceJobLineageContext`] into a normalized
/// [`WorkspaceJobHistory`].
///
/// Validates `source_id`, `binding_id`, `thread_uuid`, `canonical_project_scope`, brief/planning
/// base consistency, approval linkage, and runtime envelope identity before projecting. Without
/// store-backed receipt, Closure, and [`PlanStore`] head verification, `acceptance_proof` always
/// remains [`WorkspaceAcceptanceProof::unknown`], and caller-supplied
/// [`WorkspaceJobLineageContext`] neither suppresses live planning attention nor mints a current
/// published-plan witness or an `approval_status = "superseded"` claim for unproved
/// awaiting-approval `Unknown` attention.
pub fn project_coworker_job_history(
    chat_identity: &WorkspaceChatIdentity,
    job: &CoworkerJobRecord,
    runtime: Option<&ControllerJobRuntimeState>,
    context: Option<&WorkspaceJobLineageContext>,
) -> Result<WorkspaceJobHistory, WorkspaceHistoryDiagnostic> {
    project_coworker_job_history_internal(
        chat_identity, job, runtime, context, None, None, None, None,
    )
}

struct PrevalidatedScopedJob<'a> {
    job: &'a CoworkerJobRecord,
    runtime_opt: Option<ControllerJobRuntimeState>,
    plan_revision: Option<PlanRevision>,
    approval_record: Option<ApprovalRecord>,
    runtime_missing_or_degraded: bool,
    external_resolutions: VerifiedExternalResolutionHistory,
}

#[derive(Debug, Clone, Copy)]
struct VerifiedObsoletePlanningSupersession;

#[derive(Debug, Clone, Copy)]
struct VerifiedCurrentPublishedPlanMetadata;

/// Private store-backed external resolution proof. Only
/// [`read_scoped_workspace_job_histories`] constructs this value; pure caller-supplied lineage or
/// public history contexts can never mint it.
#[derive(Debug, Clone, Default)]
struct VerifiedExternalResolutionHistory {
    resolutions: Vec<WorkspaceExternalResolution>,
}

#[derive(Debug, Clone)]
struct VerifiedHistoricalTaskAcceptance {
    accepted_candidate_id: String,
    review_verdict_id: String,
    review_receipt_id: String,
    integration_receipt_id: String,
    verified_at_ms: Option<i64>,
}

#[derive(Debug, Clone)]
struct VerifiedHistoricalJobAcceptance {
    combined_candidate_id: String,
    closure_reservation_id: String,
    closure_verdict_id: String,
    closure_summary: String,
    verified_at_ms: Option<i64>,
    tasks: BTreeMap<String, VerifiedHistoricalTaskAcceptance>,
}

struct HistoricalStoreVerificationContext<'a, 'b> {
    stores: &'a WorkspaceHistoryStores<'b>,
    policy_store: Option<&'a ControllerPolicyStore>,
    verified_sessions_root: Option<&'b Path>,
    plan_lookup_degraded: bool,
    authority_store_degraded: bool,
}

#[allow(clippy::too_many_arguments)]
fn project_coworker_job_history_internal(
    chat_identity: &WorkspaceChatIdentity,
    job: &CoworkerJobRecord,
    runtime: Option<&ControllerJobRuntimeState>,
    context: Option<&WorkspaceJobLineageContext>,
    verified_acceptance: Option<&VerifiedHistoricalJobAcceptance>,
    verified_obsolete_planning: Option<&VerifiedObsoletePlanningSupersession>,
    verified_current_published_plan: Option<&VerifiedCurrentPublishedPlanMetadata>,
    verified_external_resolutions: Option<&VerifiedExternalResolutionHistory>,
) -> Result<WorkspaceJobHistory, WorkspaceHistoryDiagnostic> {
    chat_identity.validate().map_err(|_| {
        WorkspaceHistoryDiagnostic::new(
            WorkspaceHistoryDiagnosticKind::ScopeRejected,
            Some(&job.job_id),
            "Requested WorkspaceChatIdentity failed four-part validation.",
        )
    })?;

    validate_job_and_companion_scopes(chat_identity, job, runtime, context)?;

    let runtime_missing = (runtime.is_none() && job_expects_controller_runtime(job))
        || context.is_some_and(|c| c.runtime_missing_or_degraded);

    let task_titles = collect_verified_task_titles(context);

    let mut projected_tasks = Vec::with_capacity(job.tasks.len());
    for task in &job.tasks {
        let verified_task =
            verified_acceptance.and_then(|vjob| vjob.tasks.get(&task.task_id).map(|vt| (vjob, vt)));
        projected_tasks.push(project_task_history(
            job,
            task,
            runtime,
            runtime_missing,
            task_titles.get(task.task_id.as_str()).map(String::as_str),
            verified_task,
        ));
    }

    let runs = project_job_runs(job, runtime, &projected_tasks);
    let outbox = project_job_outbox(job, runtime);
    let combined_candidates: Vec<WorkspaceCombinedCandidate> = job
        .combined_candidates
        .iter()
        .map(|c| project_combined_candidate(c, verified_acceptance))
        .collect();

    let historical_failure =
        derive_job_historical_failure(job, runtime, &projected_tasks, verified_acceptance);
    let mut current_attention = if verified_acceptance.is_some() {
        None
    } else {
        derive_job_current_attention(
            job,
            runtime,
            &projected_tasks,
            verified_obsolete_planning,
            verified_current_published_plan,
        )
    };
    let (external_resolutions, full_job_externally_resolved) =
        if verified_acceptance.is_some() {
            (
                verified_external_resolutions
                    .map(|verified| verified.resolutions.clone())
                    .unwrap_or_default(),
                false,
            )
        } else {
            apply_external_resolution_coverage(
                job,
                runtime,
                verified_obsolete_planning,
                verified_current_published_plan,
                &mut projected_tasks,
                &mut current_attention,
                verified_external_resolutions,
            )
        };
    let links = project_record_links(job, context);
    let result = project_job_result(job, runtime, runtime_missing, verified_acceptance);
    let mut job_summary = project_job_summary(
        chat_identity,
        job,
        runtime,
        context,
        runtime_missing,
        &projected_tasks,
        &runs,
        current_attention.as_ref(),
        verified_acceptance,
        verified_obsolete_planning,
        verified_current_published_plan,
    );
    if full_job_externally_resolved && verified_acceptance.is_none() {
        job_summary.state = ObservationJobState::Unknown;
        job_summary.execution_status = "resolved_externally".to_string();
    }

    let acceptance_proof = match verified_acceptance {
        Some(vjob) => WorkspaceAcceptanceProof {
            status: WorkspaceAcceptanceStatus::VerifiedAccepted,
            accepted_candidate_id: None,
            combined_candidate_id: Some(vjob.combined_candidate_id.clone()),
            review_verdict_id: None,
            review_receipt_id: None,
            integration_receipt_id: None,
            closure_reservation_id: Some(vjob.closure_reservation_id.clone()),
            closure_verdict_id: Some(vjob.closure_verdict_id.clone()),
            verified_at_ms: vjob.verified_at_ms,
        },
        None => WorkspaceAcceptanceProof::unknown(),
    };

    let raw_history = WorkspaceJobHistory {
        job: job_summary,
        tasks: projected_tasks,
        runs,
        outbox,
        combined_candidates,
        result,
        links,
        historical_failure,
        current_attention,
        acceptance_proof,
        external_resolutions,
        truncation: WorkspaceJobHistoryTruncation::default(),
    };

    Ok(raw_history.normalized())
}

fn attention_is_externally_coverable(attention: &WorkspaceAttentionState) -> bool {
    !matches!(
        attention.kind,
        WorkspaceAttentionKind::None
            | WorkspaceAttentionKind::Unknown
            | WorkspaceAttentionKind::UnresolvedEffect
    )
}

fn collect_covered_attention_ids(
    attention: &WorkspaceAttentionState,
    covered_hold_ids: &mut BTreeSet<String>,
    covered_outbox_ids: &mut BTreeSet<String>,
) {
    covered_hold_ids.extend(attention.blocking_hold_ids.iter().cloned());
    covered_outbox_ids.extend(attention.blocking_outbox_ids.iter().cloned());
}

fn apply_external_resolution_coverage(
    job: &CoworkerJobRecord,
    runtime: Option<&ControllerJobRuntimeState>,
    verified_obsolete_planning: Option<&VerifiedObsoletePlanningSupersession>,
    verified_current_published_plan: Option<&VerifiedCurrentPublishedPlanMetadata>,
    projected_tasks: &mut [WorkspaceTaskHistory],
    current_attention: &mut Option<WorkspaceAttentionState>,
    verified: Option<&VerifiedExternalResolutionHistory>,
) -> (Vec<WorkspaceExternalResolution>, bool) {
    let Some(verified) = verified else {
        return (Vec::new(), false);
    };
    if verified.resolutions.is_empty() {
        return (Vec::new(), false);
    }

    let mut resolutions = verified.resolutions.clone();
    let mut any_full_job = false;
    let mut full_job_attention_covered = false;
    let mut covered_task_ids = BTreeSet::new();
    let mut covered_hold_ids = BTreeSet::new();
    let mut covered_outbox_ids = BTreeSet::new();

    for resolution in &mut resolutions {
        if !resolution.applicability.is_applicable() {
            continue;
        }
        let mut covered_rows = Vec::new();
        match resolution.scope {
            WorkspaceExternalResolutionScope::Job => {
                any_full_job = true;
                if let Some(attention) = current_attention
                    .as_ref()
                    .filter(|attention| attention_is_externally_coverable(attention))
                {
                    covered_rows.push(WorkspaceCoveredAttention {
                        task_id: None,
                        attention: attention.clone(),
                    });
                    collect_covered_attention_ids(
                        attention,
                        &mut covered_hold_ids,
                        &mut covered_outbox_ids,
                    );
                    full_job_attention_covered = true;
                }
                for task in projected_tasks.iter() {
                    let Some(attention) = task
                        .current_attention
                        .as_ref()
                        .filter(|attention| attention_is_externally_coverable(attention))
                    else {
                        continue;
                    };
                    covered_task_ids.insert(task.task_id.clone());
                    collect_covered_attention_ids(
                        attention,
                        &mut covered_hold_ids,
                        &mut covered_outbox_ids,
                    );
                    covered_rows.push(WorkspaceCoveredAttention {
                        task_id: Some(task.task_id.clone()),
                        attention: attention.clone(),
                    });
                }
            }
            WorkspaceExternalResolutionScope::Tasks => {
                for task_id in &resolution.task_ids {
                    let Some(task) = projected_tasks
                        .iter()
                        .find(|task| &task.task_id == task_id)
                    else {
                        continue;
                    };
                    let Some(attention) = task
                        .current_attention
                        .as_ref()
                        .filter(|attention| attention_is_externally_coverable(attention))
                    else {
                        continue;
                    };
                    covered_task_ids.insert(task.task_id.clone());
                    collect_covered_attention_ids(
                        attention,
                        &mut covered_hold_ids,
                        &mut covered_outbox_ids,
                    );
                    covered_rows.push(WorkspaceCoveredAttention {
                        task_id: Some(task.task_id.clone()),
                        attention: attention.clone(),
                    });
                }
            }
            WorkspaceExternalResolutionScope::Unknown => {}
        }
        resolution.covered_attention = covered_rows;
    }

    if any_full_job && full_job_attention_covered {
        *current_attention = None;
    }
    for task in projected_tasks.iter_mut() {
        if covered_task_ids.contains(&task.task_id)
            && task
                .current_attention
                .as_ref()
                .is_some_and(attention_is_externally_coverable)
        {
            task.current_attention = None;
        }
    }

    if any_full_job && full_job_attention_covered {
        // The full receipt already covered all explicit job-level attention. Re-derive only from
        // remaining unresolved/unknown blockers instead of resurrecting a phase-level need.
        let mut residual = Vec::new();
        for task in projected_tasks.iter() {
            if let Some(attention) = task.current_attention.as_ref() {
                residual.push(attention.clone());
            }
        }
        if let Some(first) = residual.first() {
            *current_attention = Some(first.clone());
        }
    } else {
        let (filtered_job, filtered_runtime) = filter_job_for_covered_task_attention(
            job,
            runtime,
            &covered_task_ids,
            &covered_hold_ids,
            &covered_outbox_ids,
        );
        *current_attention = derive_job_current_attention(
            &filtered_job,
            filtered_runtime.as_ref(),
            projected_tasks,
            verified_obsolete_planning,
            verified_current_published_plan,
        );
    }

    let all_attention_clear = current_attention.is_none()
        && projected_tasks
            .iter()
            .all(|task| task.current_attention.is_none());
    (resolutions, any_full_job && all_attention_clear)
}

fn filter_job_for_covered_task_attention(
    job: &CoworkerJobRecord,
    runtime: Option<&ControllerJobRuntimeState>,
    covered_task_ids: &BTreeSet<String>,
    covered_hold_ids: &BTreeSet<String>,
    covered_outbox_ids: &BTreeSet<String>,
) -> (CoworkerJobRecord, Option<ControllerJobRuntimeState>) {
    let mut filtered = job.clone();
    filtered.holds.retain(|hold| {
        if !covered_hold_ids.contains(&hold.hold_id) {
            return true;
        }
        hold.holds_entire_job
            || hold
                .directly_affected_task_ids
                .iter()
                .any(|task_id| !covered_task_ids.contains(task_id))
    });
    for task in &mut filtered.tasks {
        if !covered_task_ids.contains(&task.task_id) {
            continue;
        }
        task.active_hold_ids
            .retain(|hold_id| !covered_hold_ids.contains(hold_id));
        if task
            .unresolved_outbox_id
            .as_ref()
            .is_some_and(|outbox_id| covered_outbox_ids.contains(outbox_id))
        {
            task.unresolved_outbox_id = None;
        }
    }
    filtered.outbox.retain(|outbox| {
        !outbox
            .task_id
            .as_ref()
            .is_some_and(|task_id| covered_task_ids.contains(task_id))
    });
    let filtered_runtime = runtime.map(|runtime| {
        let mut filtered_runtime = runtime.clone();
        filtered_runtime.pending_effects.retain(|effect_id, effect| {
            !covered_outbox_ids.contains(effect_id)
                && !effect
                    .task_id
                    .as_ref()
                    .is_some_and(|task_id| covered_task_ids.contains(task_id))
        });
        filtered_runtime
    });
    (filtered, filtered_runtime)
}

#[allow(clippy::too_many_arguments)]
fn verify_scoped_external_resolutions_readonly(
    chat_identity: &WorkspaceChatIdentity,
    job: &CoworkerJobRecord,
    runtime: Option<&ControllerJobRuntimeState>,
    verified_ledger: &CoworkerBindingLedger,
    plan_snapshot: &ReadOnlyPlanSnapshot,
    approval_records: &[ApprovalRecord],
    plan_revisions: &[PlanRevision],
    authority_records_by_id: Option<&BTreeMap<String, ApprovalRecord>>,
    authority_store_configured: bool,
    policy_store: Option<&ControllerPolicyStore>,
    stores: &WorkspaceHistoryStores<'_>,
    verified_sessions_root: Option<&Path>,
    diagnostics: &mut Vec<WorkspaceHistoryDiagnostic>,
) -> VerifiedExternalResolutionHistory {
    let mut resolutions = Vec::new();
    for receipt in verified_ledger.external_resolutions.iter() {
        let ExternalResolutionTarget::Job { job_id } = &receipt.target else {
            // Legacy-plan receipts cannot be attached to a Coworker job history without inventing
            // a job identity. Their stored history remains untouched in the binding ledger.
            continue;
        };
        if job_id != &job.job_id {
            continue;
        }

        let mut projected = project_external_resolution_receipt(receipt);
        let (applicability, verification_error) = classify_external_resolution_applicability_readonly(
            chat_identity,
            job,
            runtime,
            receipt,
            verified_ledger,
            plan_snapshot,
            approval_records,
            plan_revisions,
            authority_records_by_id,
            authority_store_configured,
            policy_store,
            stores,
            verified_sessions_root,
        );
        projected.applicability = applicability;
        if let Some(message) = verification_error {
            diagnostics.push(WorkspaceHistoryDiagnostic::new(
                WorkspaceHistoryDiagnosticKind::ExternalResolutionVerificationFailed,
                Some(&job.job_id),
                message,
            ));
        }
        resolutions.push(projected);
    }
    VerifiedExternalResolutionHistory { resolutions }
}

fn project_external_resolution_receipt(
    receipt: &ExternalResolutionReceipt,
) -> WorkspaceExternalResolution {
    let scope = match &receipt.scope {
        ExternalResolutionScope::FullJob => WorkspaceExternalResolutionScope::Job,
        ExternalResolutionScope::Tasks { .. } => WorkspaceExternalResolutionScope::Tasks,
    };
    let evidence_refs = receipt
        .evidence_refs
        .iter()
        .map(|evidence| match evidence {
            crate::external_resolution::ExternalResolutionEvidenceRef::CommitSha { commit_sha } => {
                commit_sha.clone()
            }
            crate::external_resolution::ExternalResolutionEvidenceRef::ArtifactRef {
                reference,
            } => reference.clone(),
            crate::external_resolution::ExternalResolutionEvidenceRef::ProviderText { text } => {
                text.clone()
            }
            crate::external_resolution::ExternalResolutionEvidenceRef::JsonSelfAttestation {
                claim,
            } => claim.clone(),
        })
        .collect();
    WorkspaceExternalResolution {
        receipt_id: receipt.receipt_id.clone(),
        request_key: Some(receipt.request_key.clone()),
        preview_digest: Some(receipt.preview_digest.clone()),
        outcome: WorkspaceExternalResolutionOutcome::ResolvedExternally,
        scope,
        task_ids: receipt.resolved_task_ids.clone(),
        applicability: WorkspaceExternalResolutionApplicability::Unverified,
        summary: receipt.explanation.clone(),
        evidence_refs,
        recorded_at_ms: i64::try_from(receipt.created_at_ms)
            .ok()
            .filter(|value| *value > 0),
        covered_attention: Vec::new(),
    }
}

#[allow(clippy::too_many_arguments)]
fn classify_external_resolution_applicability_readonly(
    chat_identity: &WorkspaceChatIdentity,
    job: &CoworkerJobRecord,
    runtime: Option<&ControllerJobRuntimeState>,
    receipt: &ExternalResolutionReceipt,
    ledger: &CoworkerBindingLedger,
    plan_snapshot: &ReadOnlyPlanSnapshot,
    approval_records: &[ApprovalRecord],
    plan_revisions: &[PlanRevision],
    authority_records_by_id: Option<&BTreeMap<String, ApprovalRecord>>,
    authority_store_configured: bool,
    policy_store: Option<&ControllerPolicyStore>,
    stores: &WorkspaceHistoryStores<'_>,
    verified_sessions_root: Option<&Path>,
) -> (
    WorkspaceExternalResolutionApplicability,
    Option<&'static str>,
) {
    if let Err(message) = verify_receipt_identity_and_project(receipt, chat_identity, job) {
        return (
            WorkspaceExternalResolutionApplicability::Unverified,
            Some(message),
        );
    }
    let Some(source_job_revision) = receipt.source_job_revision else {
        return (
            WorkspaceExternalResolutionApplicability::Unverified,
            Some("External resolution receipt for a job is missing source_job_revision."),
        );
    };
    if source_job_revision > job.job_revision {
        return (
            WorkspaceExternalResolutionApplicability::Unverified,
            Some("External resolution receipt source_job_revision is ahead of the current job."),
        );
    }

    if source_job_revision == job.job_revision {
        if let Err(message) = verify_exact_current_job_receipt_links(
            receipt,
            job,
            plan_snapshot,
            authority_records_by_id,
            authority_store_configured,
        ) {
            return (
                WorkspaceExternalResolutionApplicability::Unverified,
                Some(message),
            );
        }
    }

    if let Some(policy_store) = policy_store {
        match policy_store.get_binding_ledger_revision_readonly(&job.binding_id) {
            Ok(current_controller_revision)
                if current_controller_revision < receipt.source_controller_ledger_revision =>
            {
                return (
                    WorkspaceExternalResolutionApplicability::Unverified,
                    Some(
                        "External resolution receipt controller ledger revision is ahead of current controller state.",
                    ),
                );
            }
            Ok(_) => {}
            Err(_) => {
                return (
                    WorkspaceExternalResolutionApplicability::Unverified,
                    Some(
                        "Controller ledger revision could not be verified for external resolution applicability.",
                    ),
                );
            }
        }
    } else if receipt.source_controller_ledger_revision > 0 {
        return (
            WorkspaceExternalResolutionApplicability::Unverified,
            Some(
                "Controller ledger evidence is missing for external resolution applicability.",
            ),
        );
    }

    let applicability = match evaluate_receipt_applicability(
        receipt,
        ledger,
        approval_records,
        plan_revisions,
        runtime,
        stores.plans_dir,
    ) {
        Ok(ExternalResolutionApplicability::Stale { .. }) => {
            return (
                WorkspaceExternalResolutionApplicability::Stale,
                None,
            );
        }
        Ok(ExternalResolutionApplicability::Applicable) => {
            WorkspaceExternalResolutionApplicability::Applicable
        }
        Err(_) => {
            return (
                WorkspaceExternalResolutionApplicability::Unverified,
                Some("External resolution applicability evaluation failed read-only validation."),
            );
        }
    };

    if !verify_current_runtime_evidence(job, runtime, receipt) {
        return (
            WorkspaceExternalResolutionApplicability::Unverified,
            Some("Current controller runtime evidence does not match the external resolution receipt."),
        );
    }
    if !verify_no_active_job_owner_readonly(stores.plans_dir, chat_identity, job) {
        return (
            WorkspaceExternalResolutionApplicability::Unverified,
            Some("Current job ownership evidence does not permit external resolution applicability."),
        );
    }
    if receipt.approval_id.is_some() {
        let Some(approval) = receipt.approval_id.as_deref().and_then(|approval_id| {
            authority_records_by_id.and_then(|records| records.get(approval_id))
        }) else {
            return (
                WorkspaceExternalResolutionApplicability::Unverified,
                Some("Bound approval evidence is missing for external resolution applicability."),
            );
        };
        if !verify_receipt_host_quiescent_readonly(
            stores.sessions_dir,
            verified_sessions_root,
            approval,
        ) {
            return (
                WorkspaceExternalResolutionApplicability::Unverified,
                Some("Current host session evidence is missing or not quiescent."),
            );
        }
    }

    (applicability, None)
}

fn verify_receipt_identity_and_project(
    receipt: &ExternalResolutionReceipt,
    chat_identity: &WorkspaceChatIdentity,
    job: &CoworkerJobRecord,
) -> Result<(), &'static str> {
    receipt
        .validate_integrity()
        .map_err(|_| "External resolution receipt failed immutable digest verification.")?;
    if receipt.binding_id != chat_identity.binding_id
        || receipt.thread_uuid != chat_identity.thread_uuid
        || job.binding_id != chat_identity.binding_id
        || job.thread_uuid != chat_identity.thread_uuid
    {
        return Err("External resolution receipt binding or thread scope does not match the job.");
    }
    let ExternalResolutionTarget::Job { job_id } = &receipt.target else {
        return Err("External resolution receipt does not target a Coworker job.");
    };
    if job_id != &job.job_id {
        return Err("External resolution receipt targets a different Coworker job.");
    }
    let chat_project = Path::new(&chat_identity.canonical_project_scope);
    let job_project = Path::new(&job.work_brief.project_base.repository_path);
    if receipt.project.canonical_repo_root.as_path() != chat_project
        || receipt.project.canonical_repo_root.as_path() != job_project
        || receipt.project.base_commit != job.work_brief.project_base.base_commit
        || receipt.project.base_tree_sha.as_deref()
            != Some(job.work_brief.project_base.base_tree_sha.as_str())
    {
        return Err(
            "External resolution receipt canonical project/base does not match the job and chat scope.",
        );
    }
    Ok(())
}

fn verify_exact_current_job_receipt_links(
    receipt: &ExternalResolutionReceipt,
    job: &CoworkerJobRecord,
    plan_snapshot: &ReadOnlyPlanSnapshot,
    authority_records_by_id: Option<&BTreeMap<String, ApprovalRecord>>,
    authority_store_configured: bool,
) -> Result<(), &'static str> {
    let recomputed_brief_digest = job
        .work_brief
        .digest()
        .map_err(|_| "Current job work brief digest could not be recomputed.")?;
    if recomputed_brief_digest != job.work_brief_digest
        || receipt.work_brief_digest.as_deref() != Some(job.work_brief_digest.as_str())
    {
        return Err("External resolution work brief digest does not match the current job.");
    }

    let mut current_task_ids: Vec<String> =
        job.tasks.iter().map(|task| task.task_id.clone()).collect();
    current_task_ids.sort();
    match &receipt.scope {
        ExternalResolutionScope::FullJob => {
            if receipt.resolved_task_ids != current_task_ids {
                return Err(
                    "Full-job external resolution receipt does not cover the exact current task set.",
                );
            }
        }
        ExternalResolutionScope::Tasks { task_ids } => {
            if task_ids != &receipt.resolved_task_ids
                || task_ids
                    .iter()
                    .any(|task_id| !current_task_ids.contains(task_id))
            {
                return Err(
                    "Task-scoped external resolution receipt references a task absent from the current job.",
                );
            }
        }
    }

    match (&receipt.plan_id, receipt.plan_revision) {
        (Some(plan_id), Some(plan_revision)) => {
            if !plan_snapshot.loaded {
                return Err("Plan evidence is missing for external resolution applicability.");
            }
            let plan = plan_snapshot
                .revisions_by_key
                .get(&(plan_id.clone(), plan_revision))
                .ok_or("External resolution plan revision is missing from the current PlanStore.")?;
            if plan.binding_id != receipt.binding_id {
                return Err("External resolution plan revision belongs to a different binding.");
            }
            let mut linked = false;
            if let Some(planning_result) = job.planning_result.as_ref() {
                linked = true;
                if &planning_result.plan_id != plan_id
                    || planning_result.plan_revision != plan_revision
                    || receipt.plan_digest.as_deref()
                        != Some(planning_result.plan_digest.as_str())
                {
                    return Err(
                        "External resolution plan link drifted from the job planning result.",
                    );
                }
            }
            if let Some(approval_linkage) = job.approval_linkage.as_ref() {
                linked = true;
                if &approval_linkage.plan_id != plan_id
                    || approval_linkage.plan_revision != plan_revision
                    || receipt.plan_digest.as_deref()
                        != Some(approval_linkage.plan_digest.as_str())
                {
                    return Err(
                        "External resolution plan link drifted from the job approval linkage.",
                    );
                }
            }
            if !linked {
                return Err("External resolution plan link has no job planning/approval owner.");
            }
        }
        (None, None) => {
            if job.planning_result.is_some() || job.approval_linkage.is_some() {
                return Err(
                    "External resolution receipt omits plan links recorded on the current job.",
                );
            }
        }
        _ => {
            return Err("External resolution receipt has incomplete plan/revision linkage.");
        }
    }

    match (&receipt.approval_id, &receipt.approval_subject_digest) {
        (Some(approval_id), Some(subject_digest)) => {
            if !authority_store_configured {
                return Err(
                    "Authority evidence is missing for external resolution applicability.",
                );
            }
            let approval = authority_records_by_id
                .and_then(|records| records.get(approval_id))
                .ok_or("External resolution approval is missing from the current authority store.")?;
            if approval.binding_id != receipt.binding_id
                || approval.thread_uuid != receipt.thread_uuid
                || approval.repository_path != receipt.project.canonical_repo_root
                || approval.base_commit != receipt.project.base_commit
                || approval.subject_digest != *subject_digest
                || receipt.plan_digest.as_deref() != Some(approval.plan_digest.as_str())
            {
                return Err(
                    "External resolution approval linkage does not match immutable receipt scope.",
                );
            }
            let Some(linkage) = job.approval_linkage.as_ref() else {
                return Err("External resolution approval has no current job approval linkage.");
            };
            if &linkage.approval_id != approval_id
                || linkage.approval_subject_digest != *subject_digest
            {
                return Err("External resolution approval drifted from the current job linkage.");
            }
            if linkage.verify_matches_approval(approval).is_err() {
                return Err(
                    "External resolution approval failed independent linkage verification.",
                );
            }
            if let Some(ref plan_id) = receipt.plan_id {
                if &approval.plan_id != plan_id || receipt.plan_revision != Some(approval.plan_revision)
                {
                    return Err("External resolution approval plan/revision link drifted.");
                }
            }
            if receipt
                .resolved_task_ids
                .iter()
                .any(|task_id| !approval.approved_task_ids.contains(task_id))
            {
                return Err(
                    "External resolution task scope is not contained in the approved task set.",
                );
            }
        }
        (None, None) => {
            if job.approval_linkage.is_some() {
                return Err(
                    "External resolution receipt omits approval linkage recorded on the current job.",
                );
            }
        }
        _ => {
            return Err(
                "External resolution receipt has incomplete approval/subject digest linkage.",
            );
        }
    }
    Ok(())
}

fn verify_current_runtime_evidence(
    job: &CoworkerJobRecord,
    runtime: Option<&ControllerJobRuntimeState>,
    receipt: &ExternalResolutionReceipt,
) -> bool {
    match (receipt.source_runtime_revision, runtime) {
        (None, None) => !job_expects_controller_runtime(job),
        (Some(expected_revision), Some(runtime)) => {
            runtime.runtime_revision == expected_revision
                && receipt.envelope_digest.as_deref()
                    == Some(runtime.envelope.envelope_digest.as_str())
                && runtime.envelope.verify_matches_job(job).is_ok()
        }
        _ => false,
    }
}

fn verify_no_active_job_owner_readonly(
    plans_dir: &Path,
    chat_identity: &WorkspaceChatIdentity,
    job: &CoworkerJobRecord,
) -> bool {
    let ownership_dir = coworker_driver_dir(plans_dir);
    match fs::symlink_metadata(&ownership_dir) {
        Err(err) if err.kind() == ErrorKind::NotFound => return true,
        Err(_) => return false,
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => return false,
        Ok(_) => {}
    }
    let Ok(store) = CoworkerJobOwnershipStore::new(&ownership_dir) else {
        return false;
    };
    let Ok(key) = CoworkerJobOwnershipKey::new(
        chat_identity.binding_id.as_str(),
        chat_identity.thread_uuid.as_str(),
        job.job_id.as_str(),
    ) else {
        return false;
    };
    match store.inspect_job_lock(&key) {
        Ok(CoworkerJobLockState::Absent | CoworkerJobLockState::UnlockedStaleFile { .. }) => true,
        Ok(CoworkerJobLockState::HeldByActiveProcess { .. }) | Err(_) => false,
    }
}

fn verify_receipt_host_quiescent_readonly(
    sessions_dir: Option<&Path>,
    verified_sessions_root: Option<&Path>,
    approval: &ApprovalRecord,
) -> bool {
    if sessions_dir.is_none() {
        return false;
    }
    let Some(sessions_root) = verified_sessions_root else {
        return false;
    };
    match read_host_launch_intent_optional(sessions_root, approval) {
        Ok(None) => {}
        Ok(Some(intent)) if intent.status == HostLaunchIntentStatus::ReconciledReady => {}
        Ok(Some(_)) | Err(_) => return false,
    }
    let session_dir = sessions_root.join(&approval.approval_id);
    match fs::symlink_metadata(&session_dir) {
        Err(err) if err.kind() == ErrorKind::NotFound => true,
        Err(_) => false,
        Ok(_) => {
            let Ok(session_dir) = verify_existing_session_dir(sessions_root, &approval.approval_id)
            else {
                return false;
            };
            matches!(
                read_host_manifest_optional(&session_dir, approval),
                Ok(Some(manifest)) if manifest.phase == HostPhase::Stopped
            )
        }
    }
}

fn validate_chat_identity_and_canonical_repo_on_disk(
    chat_identity: &WorkspaceChatIdentity,
) -> Result<(), WorkspaceHistoryDiagnostic> {
    chat_identity.validate().map_err(|_| {
        WorkspaceHistoryDiagnostic::new(
            WorkspaceHistoryDiagnosticKind::ScopeRejected,
            None,
            "WorkspaceChatIdentity failed source, binding, thread UUID, or canonical project scope validation.",
        )
    })?;

    let repo_path = Path::new(&chat_identity.canonical_project_scope);
    if !repo_path.is_absolute() {
        return Err(WorkspaceHistoryDiagnostic::new(
            WorkspaceHistoryDiagnosticKind::ScopeRejected,
            None,
            "Requested canonical_project_scope must be an absolute path.",
        ));
    }

    let meta = fs::symlink_metadata(repo_path).map_err(|_| {
        WorkspaceHistoryDiagnostic::new(
            WorkspaceHistoryDiagnosticKind::ScopeRejected,
            None,
            "Requested canonical_project_scope could not be verified on disk.",
        )
    })?;
    if meta.file_type().is_symlink() || !meta.is_dir() {
        return Err(WorkspaceHistoryDiagnostic::new(
            WorkspaceHistoryDiagnosticKind::ScopeRejected,
            None,
            "Requested canonical_project_scope must be a real non-symlink directory.",
        ));
    }
    let canonical = fs::canonicalize(repo_path).map_err(|_| {
        WorkspaceHistoryDiagnostic::new(
            WorkspaceHistoryDiagnosticKind::ScopeRejected,
            None,
            "Requested canonical_project_scope failed canonicalization.",
        )
    })?;
    if canonical != repo_path {
        return Err(WorkspaceHistoryDiagnostic::new(
            WorkspaceHistoryDiagnosticKind::ScopeRejected,
            None,
            "Requested canonical_project_scope does not match its canonical filesystem path.",
        ));
    }
    Ok(())
}

struct ValidatedScopedLineage<'l> {
    follow_ups_by_target: BTreeMap<&'l str, Vec<String>>,
    superseded_by_target: BTreeMap<&'l str, String>,
    tainted_job_ids: BTreeSet<&'l str>,
}

fn build_validated_scoped_lineage<'a, 'l>(
    admitted_jobs: &'a [PrevalidatedScopedJob<'l>],
    admitted_by_id: &BTreeMap<&'l str, &'a PrevalidatedScopedJob<'l>>,
) -> ValidatedScopedLineage<'l> {
    let mut raw_follow_ups_by_target: BTreeMap<&'l str, Vec<(u64, &'l str)>> = BTreeMap::new();
    let mut undirected_neighbors: BTreeMap<&'l str, BTreeSet<&'l str>> = BTreeMap::new();
    let mut direct_tainted: BTreeSet<&'l str> = BTreeSet::new();

    if admitted_jobs.len() != admitted_by_id.len() {
        for entry in admitted_jobs {
            direct_tainted.insert(entry.job.job_id.as_str());
        }
    }

    {
        let mut link_undirected = |left: &'l str, right: &'l str| {
            if left != right {
                undirected_neighbors.entry(left).or_default().insert(right);
                undirected_neighbors.entry(right).or_default().insert(left);
            }
        };

        for entry in admitted_jobs {
            let job = entry.job;
            let succ_id = job.job_id.as_str();
            match (&job.supersedes_job_id, &job.work_brief.kind) {
                (None, WorkBriefKind::NewJob) => {}
                (
                    Some(sup_target_id),
                    WorkBriefKind::Amendment {
                        target_job_id,
                        target_job_revision,
                        ..
                    },
                ) => {
                    let target_id = sup_target_id.as_str();
                    let target_rev_ok = match admitted_by_id.get(target_id) {
                        Some(t) => *target_job_revision <= t.job.job_revision,
                        None => false,
                    };
                    let valid_in_scope_edge = sup_target_id == target_job_id
                        && *target_job_revision >= 1
                        && target_id != succ_id
                        && target_rev_ok;
                    if valid_in_scope_edge {
                        raw_follow_ups_by_target
                            .entry(target_id)
                            .or_default()
                            .push((*target_job_revision, succ_id));
                        link_undirected(target_id, succ_id);
                    } else {
                        direct_tainted.insert(succ_id);
                        if admitted_by_id.contains_key(target_id) {
                            link_undirected(target_id, succ_id);
                        }
                        let brief_target = target_job_id.as_str();
                        if brief_target != target_id && admitted_by_id.contains_key(brief_target) {
                            link_undirected(brief_target, succ_id);
                        }
                    }
                }
                (sup_opt, kind) => {
                    direct_tainted.insert(succ_id);
                    if let Some(sup_target_id) = sup_opt {
                        let target_id = sup_target_id.as_str();
                        if admitted_by_id.contains_key(target_id) {
                            link_undirected(target_id, succ_id);
                        }
                    }
                    if let WorkBriefKind::Amendment { target_job_id, .. } = kind {
                        let brief_target = target_job_id.as_str();
                        if admitted_by_id.contains_key(brief_target) {
                            link_undirected(brief_target, succ_id);
                        }
                    }
                }
            }
        }
    }

    let mut directed_children: BTreeMap<&'l str, Vec<&'l str>> = BTreeMap::new();
    let mut in_degree: BTreeMap<&'l str, usize> =
        admitted_by_id.keys().map(|&id| (id, 0usize)).collect();
    let mut follow_ups_by_target: BTreeMap<&'l str, Vec<String>> = BTreeMap::new();

    for (target_id, mut edges) in raw_follow_ups_by_target {
        edges.sort_unstable();
        if edges.len() > 1 {
            direct_tainted.insert(target_id);
        }
        let mut children = Vec::with_capacity(edges.len());
        let mut owned_ids = Vec::with_capacity(edges.len());
        for (_, succ_id) in edges {
            if children.last().copied() != Some(succ_id) {
                children.push(succ_id);
                owned_ids.push(succ_id.to_string());
                if let Some(deg) = in_degree.get_mut(succ_id) {
                    *deg += 1;
                }
            } else {
                direct_tainted.insert(target_id);
            }
        }
        directed_children.insert(target_id, children);
        follow_ups_by_target.insert(target_id, owned_ids);
    }

    for entry in admitted_jobs {
        let job = entry.job;
        let job_id = job.job_id.as_str();
        let follow_ups = follow_ups_by_target.get(job_id).map(Vec::as_slice);
        for hold in &job.holds {
            if !amendment_hold_matches_followup(job, hold, follow_ups, admitted_by_id) {
                direct_tainted.insert(job_id);
                break;
            }
        }
    }

    let mut zero_in_queue: VecDeque<&'l str> = in_degree
        .iter()
        .filter_map(|(&id, &deg)| (deg == 0).then_some(id))
        .collect();
    while let Some(node) = zero_in_queue.pop_front() {
        if let Some(children) = directed_children.get(node) {
            for &child in children {
                if let Some(deg) = in_degree.get_mut(child) {
                    *deg = deg.saturating_sub(1);
                    if *deg == 0 {
                        zero_in_queue.push_back(child);
                    }
                }
            }
        }
    }
    for (&id, &remaining_deg) in &in_degree {
        if remaining_deg > 0 {
            direct_tainted.insert(id);
        }
    }

    let mut tainted_job_ids: BTreeSet<&'l str> = BTreeSet::new();
    let mut bfs_queue: VecDeque<&'l str> = VecDeque::new();
    for &seed in &direct_tainted {
        if tainted_job_ids.insert(seed) {
            bfs_queue.push_back(seed);
        }
    }
    while let Some(curr) = bfs_queue.pop_front() {
        if let Some(neighbors) = undirected_neighbors.get(curr) {
            for &next in neighbors {
                if tainted_job_ids.insert(next) {
                    bfs_queue.push_back(next);
                }
            }
        }
    }

    let mut superseded_by_target: BTreeMap<&'l str, String> = BTreeMap::new();
    for (&target_id, follow_ups) in &follow_ups_by_target {
        if !tainted_job_ids.contains(target_id) && follow_ups.len() == 1 {
            superseded_by_target.insert(target_id, follow_ups[0].clone());
        }
    }

    ValidatedScopedLineage {
        follow_ups_by_target,
        superseded_by_target,
        tainted_job_ids,
    }
}

fn successor_amendment_hold_revision(
    old_job: &CoworkerJobRecord,
    succ_job: &CoworkerJobRecord,
) -> Option<u64> {
    if succ_job.job_id == old_job.job_id {
        return None;
    }
    if succ_job.supersedes_job_id.as_deref() != Some(old_job.job_id.as_str()) {
        return None;
    }
    match &succ_job.work_brief.kind {
        WorkBriefKind::Amendment {
            target_job_id,
            target_job_revision,
            ..
        } if target_job_id == &old_job.job_id && *target_job_revision >= 1 => {
            let expected_hold_rev = target_job_revision.checked_add(1)?;
            (expected_hold_rev <= old_job.job_revision).then_some(expected_hold_rev)
        }
        _ => None,
    }
}

fn amendment_hold_matches_followup<'l>(
    job: &CoworkerJobRecord,
    hold: &WorkflowHoldRecord,
    follow_ups: Option<&[String]>,
    admitted_by_id: &BTreeMap<&'l str, &PrevalidatedScopedJob<'l>>,
) -> bool {
    if hold.reason != WorkflowHoldReason::BriefAmendmentPending {
        return true;
    }
    let Some(expected_req_key) = hold.request_key.strip_prefix("amend_") else {
        return true;
    };
    let Some(succ_ids) = follow_ups else {
        return false;
    };
    for succ_id in succ_ids {
        if let Some(succ_entry) = admitted_by_id.get(succ_id.as_str()) {
            let expected_rev = successor_amendment_hold_revision(job, succ_entry.job);
            if succ_entry.job.create_request_key == expected_req_key
                && expected_rev == Some(hold.created_at_job_revision)
            {
                return true;
            }
        }
    }
    false
}

fn job_hold_matches_successor(
    hold: &WorkflowHoldRecord,
    succ_job: &CoworkerJobRecord,
    expected_hold_rev: u64,
) -> bool {
    hold.reason == WorkflowHoldReason::BriefAmendmentPending
        && hold.created_at_job_revision == expected_hold_rev
        && hold
            .request_key
            .strip_prefix("amend_")
            .is_some_and(|rk| rk == succ_job.create_request_key)
}

fn verify_obsolete_planning_supersession_readonly<'l>(
    ledger: &CoworkerBindingLedger,
    old_entry: &PrevalidatedScopedJob<'l>,
    admitted_by_id: &BTreeMap<&'l str, &PrevalidatedScopedJob<'l>>,
    lineage: &ValidatedScopedLineage<'l>,
) -> Option<VerifiedObsoletePlanningSupersession> {
    let old_job = old_entry.job;
    if lineage.tainted_job_ids.contains(old_job.job_id.as_str()) {
        return None;
    }
    let canonical_succ_id = lineage
        .superseded_by_target
        .get(old_job.job_id.as_str())?
        .as_str();
    let succ_entry = admitted_by_id.get(canonical_succ_id)?;

    if !job_is_effect_free_obsolete_planning(ledger, old_entry) {
        return None;
    }

    let succ_job = succ_entry.job;
    let hold_rev = successor_amendment_hold_revision(old_job, succ_job)?;

    let has_causal_amendment_hold = old_job
        .holds
        .iter()
        .any(|h| job_hold_matches_successor(h, succ_job, hold_rev));
    if !has_causal_amendment_hold {
        return None;
    }

    let all_active_holds_causally_verified = old_job
        .holds
        .iter()
        .filter(|h| h.active)
        .all(|h| job_hold_matches_successor(h, succ_job, hold_rev));

    all_active_holds_causally_verified.then_some(VerifiedObsoletePlanningSupersession)
}

fn job_is_effect_free_obsolete_planning(
    ledger: &CoworkerBindingLedger,
    old_entry: &PrevalidatedScopedJob<'_>,
) -> bool {
    let job = old_entry.job;
    if !matches!(
        job.phase,
        CoworkerJobPhase::AwaitingApproval | CoworkerJobPhase::Held
    ) {
        return false;
    }
    if job.approval_linkage.is_some()
        || !job.tasks.is_empty()
        || !job.outbox.is_empty()
        || !job.combined_candidates.is_empty()
    {
        return false;
    }
    if old_entry.runtime_missing_or_degraded || old_entry.runtime_opt.is_some() {
        return false;
    }

    let active_holds: Vec<&WorkflowHoldRecord> = job.holds.iter().filter(|h| h.active).collect();
    if job.phase == CoworkerJobPhase::Held && active_holds.is_empty() {
        return false;
    }
    if active_holds
        .iter()
        .any(|h| h.reason != WorkflowHoldReason::BriefAmendmentPending)
    {
        return false;
    }

    !job_has_ambiguous_planning_reservation(ledger, job)
}

fn job_has_ambiguous_planning_reservation(
    ledger: &CoworkerBindingLedger,
    job: &CoworkerJobRecord,
) -> bool {
    for grant in &ledger.planning_grants {
        for res in &grant.reservations {
            if !matches!(
                res.status,
                PlanningReservationStatus::ReservedBeforeEffect
                    | PlanningReservationStatus::EffectUnknown
            ) {
                continue;
            }
            let linked_via_planning_result = job.planning_result.as_ref().is_some_and(|pr| {
                pr.grant_id == grant.grant_id
                    && (pr.reservation_id == res.reservation_id
                        || pr.request_key == res.request_key)
            });
            let linked_via_advice = ledger.architect_advice.iter().any(|adv| {
                adv.grant_id == grant.grant_id
                    && adv.reservation_id == res.reservation_id
                    && adv.context_dependencies.work_brief_digest == job.work_brief_digest
            });
            if linked_via_planning_result || linked_via_advice {
                return true;
            }

            if grant.binding_id != job.binding_id
                || grant.thread_uuid != job.thread_uuid
                || grant.project_base.repository_path != job.work_brief.project_base.repository_path
            {
                continue;
            }
            if let Some(ref call_base) = res.call_project_base {
                if call_base != &job.work_brief.project_base {
                    continue;
                }
            }

            // Orchestrator and Architect context_digest values hash composite tuples whose dynamic
            // call inputs (plan_id, expected_plan_revision, advice refs, or question summary) are
            // not stored on an unfinalized reservation. Any same-scoped unresolved reservation not
            // positively proven to target another scope/base conservatively blocks retirement.
            return true;
        }
    }
    false
}

fn verify_current_published_plan_metadata_readonly<'l>(
    chat_identity: &WorkspaceChatIdentity,
    ledger: &CoworkerBindingLedger,
    entry: &PrevalidatedScopedJob<'l>,
    lineage: &ValidatedScopedLineage<'l>,
    plan_snapshot: &ReadOnlyPlanSnapshot,
    plan_lookup_degraded: bool,
) -> Option<VerifiedCurrentPublishedPlanMetadata> {
    let job = entry.job;
    if job.phase != CoworkerJobPhase::AwaitingApproval
        || job.approval_linkage.is_some()
        || !job.tasks.is_empty()
        || !job.outbox.is_empty()
        || !job.combined_candidates.is_empty()
        || job.holds.iter().any(|h| h.active)
        || entry.runtime_missing_or_degraded
        || entry.runtime_opt.is_some()
    {
        return None;
    }
    let job_id = job.job_id.as_str();
    if lineage.tainted_job_ids.contains(job_id)
        || lineage.superseded_by_target.contains_key(job_id)
        || lineage.follow_ups_by_target.contains_key(job_id)
    {
        return None;
    }

    let pr = job.planning_result.as_ref()?;
    pr.validate().ok()?;
    if pr.project_base != job.work_brief.project_base
        || pr.work_brief_digest != job.work_brief_digest
        || !pr.plan_status.is_active_published()
    {
        return None;
    }

    let grant = ledger
        .planning_grants
        .iter()
        .find(|g| g.grant_id == pr.grant_id)?;
    if grant.binding_id != chat_identity.binding_id
        || grant.thread_uuid != chat_identity.thread_uuid
        || grant.project_base.repository_path != chat_identity.canonical_project_scope
        || grant.profile_id != pr.profile_id
        || grant.registry_revision != pr.registry_revision
        || grant.policy_digest != pr.policy_digest
    {
        return None;
    }

    let reservation = grant
        .reservations
        .iter()
        .find(|r| r.reservation_id == pr.reservation_id)?;
    if reservation.role != WorkflowRole::Orchestrator
        || reservation.status != PlanningReservationStatus::Completed
        || reservation.request_key != pr.request_key
        || reservation.selected_choice != pr.selected_choice
        || reservation.run_id.as_deref() != Some(pr.run_id.as_str())
        || reservation.report_artifact_id.as_deref() != Some(pr.report_artifact_id.as_str())
        || reservation.output_digest.as_deref() != Some(pr.output_digest.as_str())
    {
        return None;
    }
    if let Some(ref call_base) = reservation.call_project_base {
        if call_base != &pr.project_base {
            return None;
        }
    }

    for adv_id in &pr.architect_advice_ids {
        let adv = ledger
            .architect_advice
            .iter()
            .find(|a| &a.advice_id == adv_id)?;
        if adv.binding_id != chat_identity.binding_id
            || adv.thread_uuid != chat_identity.thread_uuid
            || adv.context_dependencies.project_base.repository_path
                != chat_identity.canonical_project_scope
        {
            return None;
        }
    }

    for g in &ledger.planning_grants {
        for res in &g.reservations {
            if !matches!(
                res.status,
                PlanningReservationStatus::ReservedBeforeEffect
                    | PlanningReservationStatus::EffectUnknown
            ) {
                continue;
            }
            let linked_via_result = g.grant_id == pr.grant_id
                && (res.reservation_id == pr.reservation_id || res.request_key == pr.request_key);
            let linked_via_advice = ledger.architect_advice.iter().any(|adv| {
                adv.grant_id == g.grant_id
                    && adv.reservation_id == res.reservation_id
                    && adv.context_dependencies.work_brief_digest == job.work_brief_digest
            });
            if linked_via_result || linked_via_advice {
                return None;
            }
        }
    }

    if plan_lookup_degraded || !plan_snapshot.loaded {
        return None;
    }
    let plan_rev = entry.plan_revision.as_ref()?;
    if plan_rev.binding_id != chat_identity.binding_id
        || plan_rev.plan_id != pr.plan_id
        || plan_rev.revision != pr.plan_revision
        || plan_rev.profile_id != pr.profile_id
        || plan_rev.profile_revision != pr.registry_revision
        || !plan_rev.status.is_active_published()
    {
        return None;
    }
    let plan_id = pr.plan_id.as_str();
    if plan_snapshot.latest_revision_by_plan.get(plan_id) != Some(&pr.plan_revision) {
        return None;
    }
    if plan_rev.tasks.is_empty() {
        return None;
    }
    for tid in pr.task_interfaces.keys() {
        if !plan_rev.tasks.iter().any(|t| &t.id == tid) {
            return None;
        }
    }

    Some(VerifiedCurrentPublishedPlanMetadata)
}

fn validate_job_and_companion_scopes(
    chat_identity: &WorkspaceChatIdentity,
    job: &CoworkerJobRecord,
    runtime: Option<&ControllerJobRuntimeState>,
    context: Option<&WorkspaceJobLineageContext>,
) -> Result<(), WorkspaceHistoryDiagnostic> {
    if job.binding_id != chat_identity.binding_id
        || job.work_brief.binding_id != chat_identity.binding_id
    {
        return Err(WorkspaceHistoryDiagnostic::new(
            WorkspaceHistoryDiagnosticKind::ScopeRejected,
            Some(&job.job_id),
            "Job record binding_id does not match requested WorkspaceChatIdentity.",
        ));
    }
    if job.thread_uuid != chat_identity.thread_uuid
        || job.work_brief.thread_uuid != chat_identity.thread_uuid
    {
        return Err(WorkspaceHistoryDiagnostic::new(
            WorkspaceHistoryDiagnosticKind::ScopeRejected,
            Some(&job.job_id),
            "Job record thread_uuid does not match requested WorkspaceChatIdentity.",
        ));
    }
    if job.work_brief.project_base.repository_path != chat_identity.canonical_project_scope {
        return Err(WorkspaceHistoryDiagnostic::new(
            WorkspaceHistoryDiagnosticKind::ScopeRejected,
            Some(&job.job_id),
            "Job project_base.repository_path does not match requested canonical_project_scope.",
        ));
    }

    if let Some(ref pr) = job.planning_result {
        if pr.project_base != job.work_brief.project_base
            || pr.work_brief_digest != job.work_brief_digest
        {
            return Err(WorkspaceHistoryDiagnostic::new(
                WorkspaceHistoryDiagnosticKind::ScopeRejected,
                Some(&job.job_id),
                "Job planning_result project_base or work_brief_digest drifted from work_brief.",
            ));
        }
    }

    if let Some(ref linkage) = job.approval_linkage {
        if linkage.project_base != job.work_brief.project_base {
            return Err(WorkspaceHistoryDiagnostic::new(
                WorkspaceHistoryDiagnosticKind::ApprovalVerificationFailed,
                Some(&job.job_id),
                "Job approval_linkage.project_base does not match work_brief.project_base.",
            ));
        }
        if let Some(ref pr) = job.planning_result {
            if linkage.plan_id != pr.plan_id
                || linkage.plan_revision != pr.plan_revision
                || linkage.plan_digest != pr.plan_digest
                || linkage.profile_id != pr.profile_id
                || linkage.registry_revision != pr.registry_revision
            {
                return Err(WorkspaceHistoryDiagnostic::new(
                    WorkspaceHistoryDiagnosticKind::ApprovalVerificationFailed,
                    Some(&job.job_id),
                    "Job approval_linkage plan/profile tuple does not match planning_result.",
                ));
            }
        }
        for task in &job.tasks {
            if !linkage
                .approved_task_ids
                .iter()
                .any(|id| id == &task.task_id)
            {
                return Err(WorkspaceHistoryDiagnostic::new(
                    WorkspaceHistoryDiagnosticKind::ApprovalVerificationFailed,
                    Some(&job.job_id),
                    "Job contains a task_id outside FrozenApprovalLinkage.approved_task_ids.",
                ));
            }
        }
    } else if !job.tasks.is_empty() || !job.outbox.is_empty() || !job.combined_candidates.is_empty()
    {
        return Err(WorkspaceHistoryDiagnostic::new(
            WorkspaceHistoryDiagnosticKind::ApprovalVerificationFailed,
            Some(&job.job_id),
            "Unapproved job cannot project executable tasks, outbox dispatches, or combined candidates.",
        ));
    }

    for comb in &job.combined_candidates {
        if comb.job_id != job.job_id || comb.project_base != job.work_brief.project_base {
            return Err(WorkspaceHistoryDiagnostic::new(
                WorkspaceHistoryDiagnosticKind::ScopeRejected,
                Some(&job.job_id),
                "CombinedCandidateIdentity job_id or project_base does not match owning job.",
            ));
        }
    }

    if let Some(ctx) = context {
        if let Some(ref plan_rev) = ctx.plan_revision {
            let expected_plan = job
                .approval_linkage
                .as_ref()
                .map(|l| (l.plan_id.as_str(), l.plan_revision))
                .or_else(|| {
                    job.planning_result
                        .as_ref()
                        .map(|pr| (pr.plan_id.as_str(), pr.plan_revision))
                });
            if plan_rev.binding_id != chat_identity.binding_id
                || expected_plan != Some((plan_rev.plan_id.as_str(), plan_rev.revision))
            {
                return Err(WorkspaceHistoryDiagnostic::new(
                    WorkspaceHistoryDiagnosticKind::ScopeRejected,
                    Some(&job.job_id),
                    "Supplied PlanRevision does not match job binding_id or plan revision tuple.",
                ));
            }
        }

        if let Some(ref approval) = ctx.approval_record {
            let Some(ref linkage) = job.approval_linkage else {
                return Err(WorkspaceHistoryDiagnostic::new(
                    WorkspaceHistoryDiagnosticKind::ApprovalVerificationFailed,
                    Some(&job.job_id),
                    "ApprovalRecord supplied for a job without FrozenApprovalLinkage.",
                ));
            };
            if approval.approval_id != linkage.approval_id
                || approval.subject_digest != linkage.approval_subject_digest
                || approval.binding_id != chat_identity.binding_id
                || approval.thread_uuid != chat_identity.thread_uuid
                || approval.repository_path != Path::new(&chat_identity.canonical_project_scope)
                || approval.base_commit != job.work_brief.project_base.base_commit
                || approval.plan_id != linkage.plan_id
                || approval.plan_revision != linkage.plan_revision
                || approval.plan_digest != linkage.plan_digest
                || approval.profile_id != linkage.profile_id
                || approval.registry_revision != linkage.registry_revision
            {
                return Err(WorkspaceHistoryDiagnostic::new(
                    WorkspaceHistoryDiagnosticKind::ApprovalVerificationFailed,
                    Some(&job.job_id),
                    "ApprovalRecord failed binding, thread, repository, base commit, or digest verification.",
                ));
            }
        }
    }

    if let Some(rt) = runtime {
        if rt.job_id != job.job_id
            || rt.envelope.job_id != job.job_id
            || rt.envelope.binding_id != chat_identity.binding_id
            || rt.envelope.thread_uuid != chat_identity.thread_uuid
            || rt.envelope.project_base != job.work_brief.project_base
            || rt.envelope.work_brief_id != job.work_brief.brief_id
            || rt.envelope.work_brief_digest != job.work_brief_digest
        {
            return Err(WorkspaceHistoryDiagnostic::new(
                WorkspaceHistoryDiagnosticKind::ScopeRejected,
                Some(&job.job_id),
                "ControllerJobRuntimeState scope, job_id, or project_base does not match job.",
            ));
        }
        if let Some(ref linkage) = job.approval_linkage {
            if rt.envelope.approval_id != linkage.approval_id
                || rt.envelope.approval_subject_digest != linkage.approval_subject_digest
                || rt.envelope.plan_id != linkage.plan_id
                || rt.envelope.plan_revision != linkage.plan_revision
                || rt.envelope.plan_digest != linkage.plan_digest
                || rt.envelope.policy_digest != linkage.policy_digest
            {
                return Err(WorkspaceHistoryDiagnostic::new(
                    WorkspaceHistoryDiagnosticKind::ApprovalVerificationFailed,
                    Some(&job.job_id),
                    "ControllerJobRuntimeState approval or policy linkage drifted from job.",
                ));
            }
        } else {
            return Err(WorkspaceHistoryDiagnostic::new(
                WorkspaceHistoryDiagnosticKind::ApprovalVerificationFailed,
                Some(&job.job_id),
                "ControllerJobRuntimeState cannot attach to an unapproved job.",
            ));
        }
    }

    Ok(())
}

fn job_expects_controller_runtime(job: &CoworkerJobRecord) -> bool {
    if job.approval_linkage.is_none() {
        return false;
    }
    !job.outbox.is_empty()
        || !job.combined_candidates.is_empty()
        || job
            .tasks
            .iter()
            .any(|t| t.current_attempt_no > 0 || !t.attempts.is_empty() || !t.candidates.is_empty())
        || matches!(
            job.phase,
            CoworkerJobPhase::AssemblingCandidate
                | CoworkerJobPhase::ClosureReview
                | CoworkerJobPhase::Completed
        )
}

fn collect_verified_task_titles(
    context: Option<&WorkspaceJobLineageContext>,
) -> BTreeMap<&str, String> {
    let mut titles = BTreeMap::new();
    let Some(ctx) = context else {
        return titles;
    };
    if let Some(ref plan_rev) = ctx.plan_revision {
        for task in &plan_rev.tasks {
            titles.insert(task.id.as_str(), task.title.clone());
        }
    }
    if let Some(ref approval) = ctx.approval_record {
        for task in &approval.tasks {
            titles.insert(task.task_id.as_str(), task.title.clone());
        }
    }
    titles
}

fn project_task_history(
    job: &CoworkerJobRecord,
    task: &CoworkerTaskRecord,
    runtime: Option<&ControllerJobRuntimeState>,
    runtime_missing: bool,
    verified_title: Option<&str>,
    verified_task: Option<(
        &VerifiedHistoricalJobAcceptance,
        &VerifiedHistoricalTaskAcceptance,
    )>,
) -> WorkspaceTaskHistory {
    let raw_title = verified_title.unwrap_or(&task.task_id);
    let title = sanitize_observation_label(raw_title, "Task");

    let attempts = project_task_attempts(job, task, runtime, runtime_missing);
    let candidates = project_task_candidates(task, runtime, verified_task.map(|(_, vt)| vt));
    let historical_failure = derive_task_historical_failure(job, task, runtime);
    let current_attention = if verified_task.is_some() {
        None
    } else {
        derive_task_current_attention(job, task, runtime)
    };

    let (stage, status) = if verified_task.is_some() {
        ("integrated", "completed")
    } else {
        map_task_stage_and_status(task, runtime_missing)
    };

    let (active_candidate_id, accepted_candidate_id, acceptance_proof) = match verified_task {
        Some((vjob, vtask)) => (
            task.active_candidate_id.clone(),
            Some(vtask.accepted_candidate_id.clone()),
            WorkspaceAcceptanceProof {
                status: WorkspaceAcceptanceStatus::VerifiedAccepted,
                accepted_candidate_id: Some(vtask.accepted_candidate_id.clone()),
                combined_candidate_id: Some(vjob.combined_candidate_id.clone()),
                review_verdict_id: Some(vtask.review_verdict_id.clone()),
                review_receipt_id: Some(vtask.review_receipt_id.clone()),
                integration_receipt_id: Some(vtask.integration_receipt_id.clone()),
                closure_reservation_id: Some(vjob.closure_reservation_id.clone()),
                closure_verdict_id: Some(vjob.closure_verdict_id.clone()),
                verified_at_ms: vtask.verified_at_ms.or(vjob.verified_at_ms),
            },
        ),
        None => (
            task.active_candidate_id
                .clone()
                .or_else(|| task.accepted_candidate_id.clone()),
            None,
            WorkspaceAcceptanceProof::unknown(),
        ),
    };

    WorkspaceTaskHistory {
        task_id: task.task_id.clone(),
        title,
        role: Some(workflow_role_name(task.role).to_string()),
        difficulty: Some(difficulty_name(task.difficulty).to_string()),
        stage: Some(stage.to_string()),
        status: status.to_string(),
        depends_on: task.depends_on.clone(),
        current_attempt_no: task.current_attempt_no,
        attempts,
        candidates,
        active_candidate_id,
        accepted_candidate_id,
        historical_failure,
        current_attention,
        acceptance_proof,
    }
}

fn project_task_attempts(
    job: &CoworkerJobRecord,
    task: &CoworkerTaskRecord,
    runtime: Option<&ControllerJobRuntimeState>,
    runtime_missing: bool,
) -> Vec<WorkspaceTaskAttempt> {
    let mut out = Vec::with_capacity(task.attempts.len() + 1);
    let mut seen_outbox_ids = BTreeSet::new();

    for att in &task.attempts {
        seen_outbox_ids.insert(att.outbox_id.as_str());
        let outbox = job.outbox.iter().find(|o| o.outbox_id == att.outbox_id);
        let art = (att.role == WorkflowRole::Coder)
            .then(|| runtime.and_then(|rt| rt.get_attempt_artifacts(&task.task_id, att.attempt_no)))
            .flatten();
        let effect =
            find_matching_controller_effect(runtime, &task.task_id, att.attempt_no, att.role);
        let cand = task
            .candidates
            .iter()
            .find(|c| c.attempt_no == att.attempt_no);

        let run_id = att
            .run_id
            .clone()
            .or_else(|| outbox.and_then(|o| o.run_id.clone()))
            .or_else(|| art.map(|a| a.run_id.clone()));
        let worker_id = att
            .worker_id
            .clone()
            .or_else(|| outbox.and_then(|o| o.worker_id.clone()))
            .or_else(|| art.map(|a| a.worker_id.clone()));

        let executor = build_attempt_executor(task, att, art, runtime);
        let status = derive_attempt_status(
            job,
            task,
            att,
            outbox,
            art,
            effect,
            cand,
            runtime,
            runtime_missing,
        );
        let (started_at_ms, finished_at_ms) = derive_attempt_timestamps(effect, &status);
        let summary = derive_attempt_summary(att, outbox, effect, cand);

        out.push(WorkspaceTaskAttempt {
            attempt_no: att.attempt_no,
            status,
            run_id,
            worker_id,
            outbox_id: Some(att.outbox_id.clone()),
            executor,
            started_at_ms,
            finished_at_ms,
            summary,
        });
    }

    // Surface task-attempt outbox reservations that were refused/unknown/reserved before a TaskAttemptRef existed.
    for outbox in &job.outbox {
        if outbox.task_id == task.task_id
            && outbox.kind == OutboxDispatchKind::TaskAttemptDispatch
            && !seen_outbox_ids.contains(outbox.outbox_id.as_str())
        {
            let status = match outbox.status {
                OutboxDispatchStatus::ReservedBeforeEffect => "queued",
                OutboxDispatchStatus::RefusedBeforeEffect => "failed",
                OutboxDispatchStatus::DispatchedEffectConfirmed => {
                    if runtime_missing {
                        "unknown"
                    } else {
                        "running"
                    }
                }
                OutboxDispatchStatus::EffectUnknown => "unknown",
                OutboxDispatchStatus::ReconciledConfirmed => "completed",
                OutboxDispatchStatus::ReconciledNotExecuted => "cancelled",
            };
            let spec = CoworkerObservationExecutorSpec::from_selected_role_choice(
                outbox.role,
                &task.selected_choice,
            );
            out.push(WorkspaceTaskAttempt {
                attempt_no: outbox.attempt_no,
                status: status.to_string(),
                run_id: outbox.run_id.clone(),
                worker_id: outbox.worker_id.clone(),
                outbox_id: Some(outbox.outbox_id.clone()),
                executor: Some(spec.to_observation_executor(None, None)),
                started_at_ms: None,
                finished_at_ms: None,
                summary: outbox.diagnostic.clone(),
            });
        }
    }

    out.sort_by(|a, b| {
        a.attempt_no
            .cmp(&b.attempt_no)
            .then_with(|| a.outbox_id.cmp(&b.outbox_id))
    });
    out
}

#[allow(clippy::too_many_arguments)]
fn derive_attempt_status(
    job: &CoworkerJobRecord,
    task: &CoworkerTaskRecord,
    att: &TaskAttemptRef,
    outbox: Option<&OutboxDispatchRecord>,
    art: Option<&TaskAttemptExecutionArtifacts>,
    effect: Option<&PendingControllerEffect>,
    cand: Option<&TaskCandidateRef>,
    runtime: Option<&ControllerJobRuntimeState>,
    runtime_missing: bool,
) -> String {
    if let Some(ob) = outbox {
        match ob.status {
            OutboxDispatchStatus::RefusedBeforeEffect => return "failed".to_string(),
            OutboxDispatchStatus::ReconciledNotExecuted => return "cancelled".to_string(),
            OutboxDispatchStatus::EffectUnknown => return "unknown".to_string(),
            OutboxDispatchStatus::ReservedBeforeEffect
                if att.run_id.is_none() && ob.run_id.is_none() && art.is_none() =>
            {
                return "queued".to_string();
            }
            _ => {}
        }
    }

    if let Some(eff) = effect {
        match eff.status {
            PendingControllerEffectStatus::EffectFailedTerminal
            | PendingControllerEffectStatus::RefusedBeforeEffect => return "failed".to_string(),
            PendingControllerEffectStatus::EffectUnknown => return "unknown".to_string(),
            _ => {}
        }
    }

    if let Some(a) = art {
        if !a.check_evidence.is_empty()
            && a.check_evidence
                .iter()
                .any(|c| !c.passed || c.exit_code != 0)
        {
            return "failed".to_string();
        }
    }

    if let Some(rt) = runtime {
        if rt.derived_attempts.iter().any(|d| {
            d.task_id == task.task_id
                && d.attempt_no == att.attempt_no.saturating_add(1)
                && (d.prior_check_failure_basis.is_some() || d.prior_review_verdict_id.is_some())
        }) {
            return "failed".to_string();
        }
    }

    if att.role == WorkflowRole::Coder {
        if let Some(c) = cand {
            if let Some(ref verdict) = c.review_verdict {
                match verdict.outcome {
                    ReviewVerdictOutcome::CorrectInScope
                    | ReviewVerdictOutcome::ScopeConflict
                    | ReviewVerdictOutcome::Blocked => return "failed".to_string(),
                    ReviewVerdictOutcome::Unclear => return "unknown".to_string(),
                    ReviewVerdictOutcome::Accept => return "completed".to_string(),
                }
            }
        }
    }

    if att.attempt_no < task.current_attempt_no {
        if cand.is_some() || att.head_commit.is_some() {
            return "completed".to_string();
        }
        return "failed".to_string();
    }

    if att.attempt_no == task.current_attempt_no {
        match task.stage {
            CoworkerTaskStage::Failed | CoworkerTaskStage::Blocked => return "failed".to_string(),
            CoworkerTaskStage::Cancelled => return "cancelled".to_string(),
            CoworkerTaskStage::EffectUnknown => return "unknown".to_string(),
            CoworkerTaskStage::DispatchReserved if att.role == WorkflowRole::Coder => {
                return "queued".to_string();
            }
            CoworkerTaskStage::Running if att.role == WorkflowRole::Coder => {
                return if runtime_missing {
                    "unknown".to_string()
                } else {
                    "running".to_string()
                };
            }
            CoworkerTaskStage::ReviewReserved if att.role == WorkflowRole::Reviewer => {
                return "queued".to_string();
            }
            CoworkerTaskStage::UnderReview if att.role == WorkflowRole::Reviewer => {
                return if runtime_missing {
                    "unknown".to_string()
                } else {
                    "running".to_string()
                };
            }
            CoworkerTaskStage::CorrectInScopeRequired if att.role == WorkflowRole::Coder => {
                return "failed".to_string();
            }
            _ => {}
        }
    }

    if job.phase == CoworkerJobPhase::Cancelled {
        return "cancelled".to_string();
    }

    if cand.is_some()
        || att.head_commit.is_some()
        || att.report_artifact_id.is_some()
        || outbox.is_some_and(|o| {
            matches!(
                o.status,
                OutboxDispatchStatus::DispatchedEffectConfirmed
                    | OutboxDispatchStatus::ReconciledConfirmed
            )
        })
    {
        return "completed".to_string();
    }

    "unknown".to_string()
}

fn build_attempt_executor(
    task: &CoworkerTaskRecord,
    att: &TaskAttemptRef,
    art: Option<&TaskAttemptExecutionArtifacts>,
    runtime: Option<&ControllerJobRuntimeState>,
) -> Option<ObservationExecutor> {
    let choice = match att.role {
        WorkflowRole::Coder => Some(&task.selected_choice),
        WorkflowRole::Reviewer => runtime.map(|rt| &rt.envelope.reviewer_choice),
        WorkflowRole::Closure => runtime.map(|rt| &rt.envelope.closure_choice),
        _ => None,
    };
    let observed_model = att
        .observed_model
        .as_deref()
        .or_else(|| art.and_then(|a| a.observed_model.as_deref()));
    let observed_effort = att
        .observed_effort
        .as_deref()
        .or_else(|| art.and_then(|a| a.observed_effort.as_deref()));

    if let Some(selected) = choice {
        let spec = CoworkerObservationExecutorSpec::from_selected_role_choice(att.role, selected);
        Some(spec.to_observation_executor(observed_model, observed_effort))
    } else {
        Some(ObservationExecutor {
            tool: None,
            provider: art.map(|a| a.observed_provider.clone()),
            role: Some(workflow_role_name(att.role).to_string()),
            requested_model: None,
            observed_model: observed_model.map(str::to_string),
            requested_effort: None,
            observed_effort: observed_effort.map(str::to_string),
        })
    }
}

fn derive_attempt_timestamps(
    effect: Option<&PendingControllerEffect>,
    status: &str,
) -> (Option<i64>, Option<i64>) {
    let Some(eff) = effect else {
        return (None, None);
    };
    let started = i64::try_from(eff.created_at_ms).ok().filter(|&v| v > 0);
    let is_terminal = matches!(status, "completed" | "failed" | "cancelled");
    let finished = if is_terminal {
        i64::try_from(eff.updated_at_ms).ok().filter(|&v| v > 0)
    } else {
        None
    };
    (started, finished)
}

fn derive_attempt_summary(
    att: &TaskAttemptRef,
    outbox: Option<&OutboxDispatchRecord>,
    effect: Option<&PendingControllerEffect>,
    cand: Option<&TaskCandidateRef>,
) -> Option<String> {
    if let Some(diag) = outbox.and_then(|o| o.diagnostic.as_deref()) {
        return Some(diag.to_string());
    }
    if let Some(diag) = effect.and_then(|e| e.diagnostic.as_deref()) {
        return Some(diag.to_string());
    }
    if att.role == WorkflowRole::Coder {
        if let Some(verdict) = cand.and_then(|c| c.review_verdict.as_ref()) {
            if let Some(ref prose) = verdict.prose_summary {
                return Some(prose.clone());
            }
            if let Some(first_fix) = verdict.required_in_scope_fixes.first() {
                return Some(first_fix.clone());
            }
            if let Some(first_finding) = verdict.findings.first() {
                return Some(first_finding.clone());
            }
        }
    }
    None
}

fn project_task_candidates(
    task: &CoworkerTaskRecord,
    runtime: Option<&ControllerJobRuntimeState>,
    verified_task: Option<&VerifiedHistoricalTaskAcceptance>,
) -> Vec<WorkspaceTaskCandidate> {
    let mut out = Vec::with_capacity(task.candidates.len());
    for cand in &task.candidates {
        let is_verified_accepted =
            verified_task.is_some_and(|vt| vt.accepted_candidate_id == cand.candidate_id);

        let coder_att = task
            .attempts
            .iter()
            .find(|a| a.attempt_no == cand.attempt_no && a.role == WorkflowRole::Coder);
        let art = runtime.and_then(|rt| rt.get_attempt_artifacts(&task.task_id, cand.attempt_no));

        let worker_id = coder_att
            .and_then(|a| a.worker_id.clone())
            .or_else(|| art.map(|a| a.worker_id.clone()));

        let verification_passed = if is_verified_accepted {
            Some(true)
        } else {
            art.and_then(|a| {
                (!a.check_evidence.is_empty()).then(|| {
                    a.check_evidence
                        .iter()
                        .all(|c| c.passed && c.exit_code == 0)
                })
            })
        };

        let is_superseded_attempt = cand.attempt_no < task.current_attempt_no
            && task.active_candidate_id.as_deref() != Some(cand.candidate_id.as_str())
            && task.accepted_candidate_id.as_deref() != Some(cand.candidate_id.as_str());

        let review_status = if is_verified_accepted {
            "accepted"
        } else if let Some(ref verdict) = cand.review_verdict {
            match verdict.outcome {
                ReviewVerdictOutcome::CorrectInScope => "changes_requested",
                ReviewVerdictOutcome::ScopeConflict | ReviewVerdictOutcome::Blocked => "rejected",
                ReviewVerdictOutcome::Unclear => "unknown",
                // Positive verdict/receipt IDs remain "unknown" unless verified by receipt correlation.
                ReviewVerdictOutcome::Accept => {
                    if is_superseded_attempt {
                        "invalidated"
                    } else {
                        "unknown"
                    }
                }
            }
        } else if is_superseded_attempt {
            "invalidated"
        } else if matches!(
            task.stage,
            CoworkerTaskStage::ReadyForReview
                | CoworkerTaskStage::ReviewReserved
                | CoworkerTaskStage::UnderReview
        ) && task.active_candidate_id.as_deref() == Some(cand.candidate_id.as_str())
        {
            "pending"
        } else {
            "unknown"
        };

        let summary = cand.review_verdict.as_ref().and_then(|v| {
            v.prose_summary
                .clone()
                .or_else(|| v.required_in_scope_fixes.first().cloned())
                .or_else(|| v.findings.first().cloned())
        });

        let produced_at_ms = if is_verified_accepted {
            verified_task.and_then(|vt| vt.verified_at_ms)
        } else {
            None
        };

        out.push(WorkspaceTaskCandidate {
            candidate_id: cand.candidate_id.clone(),
            attempt_no: cand.attempt_no,
            worker_id,
            head_commit_sha: Some(cand.head_commit.clone()),
            tree_sha: Some(cand.tree_sha.clone()),
            verification_passed,
            review_status: review_status.to_string(),
            review_verdict_id: cand.review_verdict.as_ref().map(|v| v.verdict_id.clone()),
            review_receipt_id: cand.review_receipt_id.clone(),
            produced_at_ms,
            summary,
        });
    }
    out
}

fn map_task_stage_and_status(
    task: &CoworkerTaskRecord,
    runtime_missing: bool,
) -> (&'static str, &'static str) {
    let unstarted = task.current_attempt_no == 0 && task.attempts.is_empty();
    match task.stage {
        CoworkerTaskStage::PendingDependencies => (
            "pending_dependencies",
            if unstarted { "not_started" } else { "pending" },
        ),
        CoworkerTaskStage::ReadyToDispatch => (
            "ready_to_dispatch",
            if unstarted { "not_started" } else { "pending" },
        ),
        CoworkerTaskStage::DispatchReserved => ("dispatch_reserved", "queued"),
        CoworkerTaskStage::Running => (
            "running",
            if runtime_missing {
                "unknown"
            } else {
                "running"
            },
        ),
        CoworkerTaskStage::ReadyForReview => ("ready_for_review", "ready_for_review"),
        CoworkerTaskStage::ReviewReserved => ("review_reserved", "ready_for_review"),
        CoworkerTaskStage::UnderReview => (
            "under_review",
            if runtime_missing {
                "unknown"
            } else {
                "ready_for_review"
            },
        ),
        CoworkerTaskStage::CorrectInScopeRequired => {
            ("correct_in_scope_required", "changes_requested")
        }
        // Phase 1 invariant: never report "accepted" before receipt verification lands in Phase 2.
        CoworkerTaskStage::Accepted => (
            "accepted_unverified",
            if runtime_missing {
                "unknown"
            } else {
                "completed"
            },
        ),
        CoworkerTaskStage::IntegrationReserved => (
            "integration_reserved",
            if runtime_missing {
                "unknown"
            } else {
                "running"
            },
        ),
        CoworkerTaskStage::Integrated => (
            "integrated_unverified",
            if runtime_missing {
                "unknown"
            } else {
                "completed"
            },
        ),
        CoworkerTaskStage::Held => ("held", "on_hold"),
        CoworkerTaskStage::Blocked => ("blocked", "blocked"),
        CoworkerTaskStage::EffectUnknown => ("effect_unknown", "unknown"),
        CoworkerTaskStage::Failed => ("failed", "failed"),
        CoworkerTaskStage::Cancelled => ("cancelled", "cancelled"),
    }
}

fn derive_task_historical_failure(
    job: &CoworkerJobRecord,
    task: &CoworkerTaskRecord,
    runtime: Option<&ControllerJobRuntimeState>,
) -> Option<WorkspaceHistoricalFailure> {
    let mut best: Option<WorkspaceHistoricalFailure> = None;

    let task_currently_failed_at = |attempt_no: u32| -> bool {
        attempt_no == task.current_attempt_no
            && matches!(
                task.stage,
                CoworkerTaskStage::Failed
                    | CoworkerTaskStage::Blocked
                    | CoworkerTaskStage::CorrectInScopeRequired
            )
    };

    // 1. Outbox dispatch refusals for this task.
    for ob in job.outbox.iter().filter(|o| o.task_id == task.task_id) {
        if ob.status == OutboxDispatchStatus::RefusedBeforeEffect {
            let superseded = !task_currently_failed_at(ob.attempt_no)
                && (ob.attempt_no < task.current_attempt_no
                    || !matches!(
                        task.stage,
                        CoworkerTaskStage::Failed | CoworkerTaskStage::Blocked
                    ));
            best = Some(WorkspaceHistoricalFailure {
                failure_id: Some(ob.outbox_id.clone()),
                stage: "dispatch".to_string(),
                failed_at_ms: None,
                task_id: Some(task.task_id.clone()),
                attempt_no: Some(ob.attempt_no),
                run_id: ob.run_id.clone(),
                outbox_id: Some(ob.outbox_id.clone()),
                reason: ob
                    .diagnostic
                    .clone()
                    .unwrap_or_else(|| "Task dispatch refused before effect".to_string()),
                superseded_or_resolved: superseded,
            });
        }
    }

    // 2. Prior coder attempts without a candidate that were superseded by a later attempt.
    for att in &task.attempts {
        if att.attempt_no < task.current_attempt_no
            && att.role == WorkflowRole::Coder
            && !task
                .candidates
                .iter()
                .any(|c| c.attempt_no == att.attempt_no)
        {
            best = Some(WorkspaceHistoricalFailure {
                failure_id: att.report_artifact_id.clone(),
                stage: "worker_execution".to_string(),
                failed_at_ms: None,
                task_id: Some(task.task_id.clone()),
                attempt_no: Some(att.attempt_no),
                run_id: att.run_id.clone(),
                outbox_id: Some(att.outbox_id.clone()),
                reason: format!(
                    "Task attempt {} failed before producing a candidate and was superseded",
                    att.attempt_no
                ),
                superseded_or_resolved: true,
            });
        }
    }

    // 3. Runtime check failures, derived-attempt failure bases, and terminal failed controller effects.
    if let Some(rt) = runtime {
        for art in rt
            .attempt_artifacts
            .values()
            .filter(|a| a.task_id == task.task_id)
        {
            if let Some(failed_check) = art
                .check_evidence
                .iter()
                .find(|c| !c.passed || c.exit_code != 0)
            {
                let superseded = art.attempt_no < task.current_attempt_no
                    || !task_currently_failed_at(art.attempt_no);
                best = Some(WorkspaceHistoricalFailure {
                    failure_id: None,
                    stage: "verification".to_string(),
                    failed_at_ms: None,
                    task_id: Some(task.task_id.clone()),
                    attempt_no: Some(art.attempt_no),
                    run_id: Some(art.run_id.clone()),
                    outbox_id: None,
                    reason: format!(
                        "Verification check `{}` failed with exit code {}",
                        failed_check.check_id, failed_check.exit_code
                    ),
                    superseded_or_resolved: superseded,
                });
            }
        }

        for derived in rt
            .derived_attempts
            .iter()
            .filter(|d| d.task_id == task.task_id && d.attempt_no >= 2)
        {
            let failed_attempt = derived.attempt_no - 1;
            let stage = if derived.prior_check_failure_basis.is_some() {
                "verification"
            } else {
                "review"
            };
            let reason = derived
                .required_in_scope_fixes
                .first()
                .cloned()
                .unwrap_or_else(|| {
                    format!("Attempt {failed_attempt} required in-scope correction")
                });
            best = Some(WorkspaceHistoricalFailure {
                failure_id: derived
                    .prior_review_verdict_id
                    .clone()
                    .or_else(|| Some(derived.receipt_id.clone())),
                stage: stage.to_string(),
                failed_at_ms: None,
                task_id: Some(task.task_id.clone()),
                attempt_no: Some(failed_attempt),
                run_id: None,
                outbox_id: None,
                reason,
                superseded_or_resolved: true,
            });
        }

        for eff in rt
            .pending_effects
            .values()
            .filter(|e| e.task_id.as_deref() == Some(task.task_id.as_str()))
        {
            if matches!(
                eff.status,
                PendingControllerEffectStatus::EffectFailedTerminal
                    | PendingControllerEffectStatus::RefusedBeforeEffect
            ) {
                let att_no = eff.attempt_no.unwrap_or(task.current_attempt_no);
                let superseded = !task_currently_failed_at(att_no)
                    && (att_no < task.current_attempt_no
                        || !matches!(
                            task.stage,
                            CoworkerTaskStage::Failed | CoworkerTaskStage::Blocked
                        ));
                best = Some(WorkspaceHistoricalFailure {
                    failure_id: Some(eff.effect_id.clone()),
                    stage: effect_kind_failure_stage(eff.kind).to_string(),
                    failed_at_ms: i64::try_from(eff.updated_at_ms).ok(),
                    task_id: Some(task.task_id.clone()),
                    attempt_no: eff.attempt_no,
                    run_id: eff
                        .external_correlation_id
                        .as_deref()
                        .filter(|id| id.starts_with("run_"))
                        .map(str::to_string),
                    outbox_id: None,
                    reason: eff
                        .diagnostic
                        .clone()
                        .unwrap_or_else(|| "Controller effect failed".to_string()),
                    superseded_or_resolved: superseded,
                });
            }
        }
    }

    // 4. Non-accepted candidate review verdicts (preserved even after a newer attempt succeeds).
    for cand in &task.candidates {
        if let Some(ref verdict) = cand.review_verdict {
            if verdict.outcome != ReviewVerdictOutcome::Accept {
                let superseded = cand.attempt_no < task.current_attempt_no
                    || !task_currently_failed_at(cand.attempt_no);
                let reason = verdict
                    .required_in_scope_fixes
                    .first()
                    .or_else(|| verdict.findings.first())
                    .or(verdict.prose_summary.as_ref())
                    .cloned()
                    .unwrap_or_else(|| {
                        format!("Review outcome `{}` recorded", verdict.outcome.as_str())
                    });
                let coder_att = task
                    .attempts
                    .iter()
                    .find(|a| a.attempt_no == cand.attempt_no && a.role == WorkflowRole::Coder);
                best = Some(WorkspaceHistoricalFailure {
                    failure_id: Some(verdict.verdict_id.clone()),
                    stage: "review".to_string(),
                    failed_at_ms: None,
                    task_id: Some(task.task_id.clone()),
                    attempt_no: Some(cand.attempt_no),
                    run_id: coder_att.and_then(|a| a.run_id.clone()),
                    outbox_id: coder_att.map(|a| a.outbox_id.clone()),
                    reason,
                    superseded_or_resolved: superseded,
                });
            }
        }
    }

    // 5. Terminal Failed or Blocked task stage fallback when no structured child record existed.
    if best.is_none()
        && matches!(
            task.stage,
            CoworkerTaskStage::Failed | CoworkerTaskStage::Blocked
        )
    {
        let last_att = task.attempts.last();
        best = Some(WorkspaceHistoricalFailure {
            failure_id: None,
            stage: "worker_execution".to_string(),
            failed_at_ms: None,
            task_id: Some(task.task_id.clone()),
            attempt_no: (task.current_attempt_no > 0).then_some(task.current_attempt_no),
            run_id: last_att.and_then(|a| a.run_id.clone()),
            outbox_id: last_att.map(|a| a.outbox_id.clone()),
            reason: format!(
                "Task entered `{}` stage",
                map_task_stage_and_status(task, false).0
            ),
            superseded_or_resolved: false,
        });
    }

    best
}

fn derive_task_current_attention(
    job: &CoworkerJobRecord,
    task: &CoworkerTaskRecord,
    runtime: Option<&ControllerJobRuntimeState>,
) -> Option<WorkspaceAttentionState> {
    let mut blocking_outbox_ids = Vec::new();
    if let Some(ref ob_id) = task.unresolved_outbox_id {
        blocking_outbox_ids.push(ob_id.clone());
    }
    for ob in job.outbox.iter().filter(|o| o.task_id == task.task_id) {
        if ob.status == OutboxDispatchStatus::EffectUnknown
            && !blocking_outbox_ids.contains(&ob.outbox_id)
        {
            blocking_outbox_ids.push(ob.outbox_id.clone());
        }
    }
    if let Some(rt) = runtime {
        for eff in rt
            .pending_effects
            .values()
            .filter(|e| e.task_id.as_deref() == Some(task.task_id.as_str()))
        {
            if eff.status == PendingControllerEffectStatus::EffectUnknown
                && !blocking_outbox_ids.contains(&eff.effect_id)
            {
                blocking_outbox_ids.push(eff.effect_id.clone());
            }
        }
    }

    let active_holds: Vec<&WorkflowHoldRecord> = job
        .holds
        .iter()
        .filter(|h| {
            h.active
                && (h.holds_entire_job
                    || h.directly_affected_task_ids.contains(&task.task_id)
                    || task.active_hold_ids.contains(&h.hold_id))
        })
        .collect();
    let mut blocking_hold_ids: Vec<String> =
        active_holds.iter().map(|h| h.hold_id.clone()).collect();
    for hid in &task.active_hold_ids {
        if !blocking_hold_ids.contains(hid) {
            blocking_hold_ids.push(hid.clone());
        }
    }

    if !blocking_outbox_ids.is_empty() || task.stage == CoworkerTaskStage::EffectUnknown {
        return Some(WorkspaceAttentionState {
            kind: WorkspaceAttentionKind::UnresolvedEffect,
            reason: "Task has an unresolved dispatch effect requiring receipt reconciliation."
                .to_string(),
            next_step: Some("Reconcile task receipt before replay.".to_string()),
            blocking_hold_ids,
            blocking_outbox_ids,
            question_id: None,
            updated_at_ms: None,
        });
    }

    if !blocking_hold_ids.is_empty() || task.stage == CoworkerTaskStage::Held {
        let (kind, reason) = classify_holds_attention(&active_holds);
        return Some(WorkspaceAttentionState {
            kind,
            reason,
            next_step: None,
            blocking_hold_ids,
            blocking_outbox_ids,
            question_id: None,
            updated_at_ms: None,
        });
    }

    // Historical failure alone never creates live attention or NeedsApproval; only active terminal block/failure does.
    match task.stage {
        CoworkerTaskStage::Failed | CoworkerTaskStage::Blocked => Some(WorkspaceAttentionState {
            kind: WorkspaceAttentionKind::BlockedFailure,
            reason: "Task is stopped at an active failure fence.".to_string(),
            next_step: None,
            blocking_hold_ids: Vec::new(),
            blocking_outbox_ids: Vec::new(),
            question_id: None,
            updated_at_ms: None,
        }),
        CoworkerTaskStage::ReadyForReview
        | CoworkerTaskStage::ReviewReserved
        | CoworkerTaskStage::UnderReview => Some(WorkspaceAttentionState {
            kind: WorkspaceAttentionKind::ReviewRequired,
            reason: "Task candidate is awaiting structured review verification.".to_string(),
            next_step: None,
            blocking_hold_ids: Vec::new(),
            blocking_outbox_ids: Vec::new(),
            question_id: None,
            updated_at_ms: None,
        }),
        _ => None,
    }
}

fn derive_job_historical_failure(
    job: &CoworkerJobRecord,
    runtime: Option<&ControllerJobRuntimeState>,
    projected_tasks: &[WorkspaceTaskHistory],
    verified_acceptance: Option<&VerifiedHistoricalJobAcceptance>,
) -> Option<WorkspaceHistoricalFailure> {
    for comb in &job.combined_candidates {
        if comb.status == CombinedCandidateStatus::Rejected {
            let superseded = verified_acceptance.is_some()
                || job.combined_candidates.last().is_some_and(|latest| {
                    latest.combined_candidate_id != comb.combined_candidate_id
                });
            return Some(WorkspaceHistoricalFailure {
                failure_id: Some(comb.combined_candidate_id.clone()),
                stage: "closure".to_string(),
                failed_at_ms: None,
                task_id: None,
                attempt_no: None,
                run_id: None,
                outbox_id: None,
                reason: "Combined candidate was rejected during closure review".to_string(),
                superseded_or_resolved: superseded,
            });
        }
    }

    if let Some(rt) = runtime {
        for eff in rt.pending_effects.values().filter(|e| e.task_id.is_none()) {
            if matches!(
                eff.status,
                PendingControllerEffectStatus::EffectFailedTerminal
                    | PendingControllerEffectStatus::RefusedBeforeEffect
            ) {
                return Some(WorkspaceHistoricalFailure {
                    failure_id: Some(eff.effect_id.clone()),
                    stage: effect_kind_failure_stage(eff.kind).to_string(),
                    failed_at_ms: i64::try_from(eff.updated_at_ms).ok(),
                    task_id: None,
                    attempt_no: None,
                    run_id: eff
                        .external_correlation_id
                        .as_deref()
                        .filter(|id| id.starts_with("run_"))
                        .map(str::to_string),
                    outbox_id: None,
                    reason: eff
                        .diagnostic
                        .clone()
                        .unwrap_or_else(|| "Job-level controller effect failed".to_string()),
                    superseded_or_resolved: verified_acceptance.is_some(),
                });
            }
        }
    }

    projected_tasks
        .iter()
        .filter_map(|t| t.historical_failure.as_ref())
        .find(|f| !f.superseded_or_resolved)
        .or_else(|| {
            projected_tasks
                .iter()
                .filter_map(|t| t.historical_failure.as_ref())
                .next_back()
        })
        .cloned()
}

const VERIFIED_PENDING_PLAN_ATTENTION_REASON: &str =
    "The latest recorded published plan referenced by this job has no approval linked to the job.";
const UNVERIFIED_PENDING_PLAN_ATTENTION_REASON: &str =
    "This job records an awaiting-approval state, but its current published plan could not be verified.";
const PENDING_PLAN_ATTENTION_NEXT_STEP: &str =
    "Inspect the current plan and approval evidence before deciding whether approval or a revised plan is needed.";

fn derive_job_current_attention(
    job: &CoworkerJobRecord,
    runtime: Option<&ControllerJobRuntimeState>,
    projected_tasks: &[WorkspaceTaskHistory],
    verified_obsolete_planning: Option<&VerifiedObsoletePlanningSupersession>,
    verified_current_published_plan: Option<&VerifiedCurrentPublishedPlanMetadata>,
) -> Option<WorkspaceAttentionState> {
    let mut blocking_outbox_ids: Vec<String> = job
        .outbox
        .iter()
        .filter(|o| o.status == OutboxDispatchStatus::EffectUnknown)
        .map(|o| o.outbox_id.clone())
        .collect();

    if let Some(rt) = runtime {
        for eff in rt.pending_effects.values() {
            if eff.status == PendingControllerEffectStatus::EffectUnknown
                && !blocking_outbox_ids.contains(&eff.effect_id)
            {
                blocking_outbox_ids.push(eff.effect_id.clone());
            }
        }
    }

    let active_holds: Vec<&WorkflowHoldRecord> = job.holds.iter().filter(|h| h.active).collect();
    let mut blocking_hold_ids: Vec<String> =
        active_holds.iter().map(|h| h.hold_id.clone()).collect();

    let mut has_task_unresolved_effect = false;
    for task in projected_tasks {
        if let Some(attn) = task
            .current_attention
            .as_ref()
            .filter(|a| a.kind == WorkspaceAttentionKind::UnresolvedEffect)
        {
            has_task_unresolved_effect = true;
            for hold_id in &attn.blocking_hold_ids {
                if !blocking_hold_ids.contains(hold_id) {
                    blocking_hold_ids.push(hold_id.clone());
                }
            }
            for outbox_id in &attn.blocking_outbox_ids {
                if !blocking_outbox_ids.contains(outbox_id) {
                    blocking_outbox_ids.push(outbox_id.clone());
                }
            }
        }
    }

    if !blocking_outbox_ids.is_empty()
        || has_task_unresolved_effect
        || active_holds
            .iter()
            .any(|h| h.reason == WorkflowHoldReason::UnknownEffectReconciliationRequired)
    {
        return Some(WorkspaceAttentionState {
            kind: WorkspaceAttentionKind::UnresolvedEffect,
            reason: "Job has unresolved external effects requiring receipt reconciliation."
                .to_string(),
            next_step: Some("Reconcile pending effect receipts before continuing.".to_string()),
            blocking_hold_ids,
            blocking_outbox_ids,
            question_id: None,
            updated_at_ms: None,
        });
    }

    if job.phase == CoworkerJobPhase::Cancelled {
        return None;
    }

    let suppressed_obsolete_amendment_hold = verified_obsolete_planning.is_some()
        && !active_holds.is_empty()
        && active_holds
            .iter()
            .all(|h| h.reason == WorkflowHoldReason::BriefAmendmentPending);

    if (!active_holds.is_empty() || job.phase == CoworkerJobPhase::Held)
        && !suppressed_obsolete_amendment_hold
    {
        let (kind, reason) = classify_holds_attention(&active_holds);
        return Some(WorkspaceAttentionState {
            kind,
            reason,
            next_step: None,
            blocking_hold_ids,
            blocking_outbox_ids: Vec::new(),
            question_id: None,
            updated_at_ms: None,
        });
    }

    if job.phase == CoworkerJobPhase::AwaitingApproval
        && job.approval_linkage.is_none()
        && verified_obsolete_planning.is_none()
    {
        let (kind, reason) = if verified_current_published_plan.is_some() {
            (
                WorkspaceAttentionKind::NeedsApproval,
                VERIFIED_PENDING_PLAN_ATTENTION_REASON.to_string(),
            )
        } else {
            (
                WorkspaceAttentionKind::Unknown,
                UNVERIFIED_PENDING_PLAN_ATTENTION_REASON.to_string(),
            )
        };
        return Some(WorkspaceAttentionState {
            kind,
            reason,
            next_step: Some(PENDING_PLAN_ATTENTION_NEXT_STEP.to_string()),
            blocking_hold_ids,
            blocking_outbox_ids: Vec::new(),
            question_id: None,
            updated_at_ms: None,
        });
    }

    if job.phase == CoworkerJobPhase::Briefing && !job.work_brief.open_questions.is_empty() {
        return Some(WorkspaceAttentionState {
            kind: WorkspaceAttentionKind::NeedsUserInput,
            reason: job.work_brief.open_questions[0].clone(),
            next_step: Some("Clarify open work brief questions in native chat.".to_string()),
            blocking_hold_ids: Vec::new(),
            blocking_outbox_ids: Vec::new(),
            question_id: None,
            updated_at_ms: None,
        });
    }

    if let Some(rt) = runtime {
        if rt.stop_requested || rt.stop_or_block_reason.is_some() {
            return Some(WorkspaceAttentionState {
                kind: WorkspaceAttentionKind::BlockedFailure,
                reason: rt
                    .stop_or_block_reason
                    .clone()
                    .unwrap_or_else(|| "Controller execution stop requested.".to_string()),
                next_step: None,
                blocking_hold_ids: Vec::new(),
                blocking_outbox_ids: Vec::new(),
                question_id: None,
                updated_at_ms: None,
            });
        }
    }

    if projected_tasks.iter().any(|t| {
        t.current_attention
            .as_ref()
            .is_some_and(|a| a.kind == WorkspaceAttentionKind::BlockedFailure)
    }) {
        return Some(WorkspaceAttentionState {
            kind: WorkspaceAttentionKind::BlockedFailure,
            reason: "One or more tasks are blocked by an active failure fence.".to_string(),
            next_step: None,
            blocking_hold_ids: Vec::new(),
            blocking_outbox_ids: Vec::new(),
            question_id: None,
            updated_at_ms: None,
        });
    }

    if matches!(job.phase, CoworkerJobPhase::ClosureReview)
        || projected_tasks.iter().any(|t| {
            t.current_attention
                .as_ref()
                .is_some_and(|a| a.kind == WorkspaceAttentionKind::ReviewRequired)
        })
    {
        return Some(WorkspaceAttentionState {
            kind: WorkspaceAttentionKind::ReviewRequired,
            reason: "Candidate output is awaiting review or closure verification.".to_string(),
            next_step: None,
            blocking_hold_ids: Vec::new(),
            blocking_outbox_ids: Vec::new(),
            question_id: None,
            updated_at_ms: None,
        });
    }

    if job.phase == CoworkerJobPhase::AwaitingApproval && verified_obsolete_planning.is_none() {
        return Some(WorkspaceAttentionState {
            kind: WorkspaceAttentionKind::Unknown,
            reason: UNVERIFIED_PENDING_PLAN_ATTENTION_REASON.to_string(),
            next_step: Some(PENDING_PLAN_ATTENTION_NEXT_STEP.to_string()),
            blocking_hold_ids,
            blocking_outbox_ids: Vec::new(),
            question_id: None,
            updated_at_ms: None,
        });
    }

    None
}

fn classify_holds_attention(
    active_holds: &[&WorkflowHoldRecord],
) -> (WorkspaceAttentionKind, String) {
    if let Some(clarify) = active_holds
        .iter()
        .find(|h| h.reason == WorkflowHoldReason::ConversationClarificationPending)
    {
        return (
            WorkspaceAttentionKind::NeedsUserInput,
            clarify.detail.clone(),
        );
    }
    if let Some(unknown_eff) = active_holds
        .iter()
        .find(|h| h.reason == WorkflowHoldReason::UnknownEffectReconciliationRequired)
    {
        return (
            WorkspaceAttentionKind::UnresolvedEffect,
            unknown_eff.detail.clone(),
        );
    }
    let detail = active_holds
        .first()
        .map(|h| h.detail.clone())
        .unwrap_or_else(|| "Workflow advancement is paused by an active hold.".to_string());
    (WorkspaceAttentionKind::OnHold, detail)
}

fn project_job_runs(
    job: &CoworkerJobRecord,
    runtime: Option<&ControllerJobRuntimeState>,
    projected_tasks: &[WorkspaceTaskHistory],
) -> Vec<WorkspaceRunRecord> {
    let mut runs = Vec::new();
    let mut seen_run_ids = BTreeSet::new();

    if let Some(ref pr) = job.planning_result {
        if seen_run_ids.insert(pr.run_id.clone()) {
            let spec = CoworkerObservationExecutorSpec::from_selected_role_choice(
                WorkflowRole::Orchestrator,
                &pr.selected_choice,
            );
            let ts = i64::try_from(pr.recorded_at_ms).ok().filter(|&v| v > 0);
            runs.push(WorkspaceRunRecord {
                run_id: pr.run_id.clone(),
                worker_id: None,
                binding_id: job.binding_id.clone(),
                task_id: None,
                attempt_no: None,
                role: Some("orchestrator".to_string()),
                status: "completed".to_string(),
                executor: Some(spec.to_observation_executor(None, None)),
                started_at_ms: None,
                finished_at_ms: ts,
                updated_at_ms: ts,
            });
        }
    }

    for task in projected_tasks {
        for att in &task.attempts {
            let Some(ref rid) = att.run_id else {
                continue;
            };
            if !seen_run_ids.insert(rid.clone()) {
                continue;
            }
            let role = att
                .executor
                .as_ref()
                .and_then(|e| e.role.clone())
                .or_else(|| task.role.clone());
            runs.push(WorkspaceRunRecord {
                run_id: rid.clone(),
                worker_id: att.worker_id.clone(),
                binding_id: job.binding_id.clone(),
                task_id: Some(task.task_id.clone()),
                attempt_no: Some(att.attempt_no),
                role,
                status: att.status.clone(),
                executor: att.executor.clone(),
                started_at_ms: att.started_at_ms,
                finished_at_ms: att.finished_at_ms,
                updated_at_ms: att.finished_at_ms.or(att.started_at_ms),
            });
        }
    }

    for ob in &job.outbox {
        let Some(ref rid) = ob.run_id else {
            continue;
        };
        if !seen_run_ids.insert(rid.clone()) {
            continue;
        }
        let status = match ob.status {
            OutboxDispatchStatus::ReservedBeforeEffect => "queued",
            OutboxDispatchStatus::RefusedBeforeEffect => "failed",
            OutboxDispatchStatus::DispatchedEffectConfirmed => "completed",
            OutboxDispatchStatus::EffectUnknown => "unknown",
            OutboxDispatchStatus::ReconciledConfirmed => "completed",
            OutboxDispatchStatus::ReconciledNotExecuted => "cancelled",
        };
        runs.push(WorkspaceRunRecord {
            run_id: rid.clone(),
            worker_id: ob.worker_id.clone(),
            binding_id: job.binding_id.clone(),
            task_id: Some(ob.task_id.clone()),
            attempt_no: Some(ob.attempt_no),
            role: Some(workflow_role_name(ob.role).to_string()),
            status: status.to_string(),
            executor: None,
            started_at_ms: None,
            finished_at_ms: None,
            updated_at_ms: None,
        });
    }

    if let Some(rt) = runtime {
        for art in rt.attempt_artifacts.values() {
            if !seen_run_ids.insert(art.run_id.clone()) {
                continue;
            }
            runs.push(WorkspaceRunRecord {
                run_id: art.run_id.clone(),
                worker_id: Some(art.worker_id.clone()),
                binding_id: job.binding_id.clone(),
                task_id: Some(art.task_id.clone()),
                attempt_no: Some(art.attempt_no),
                role: Some("coder".to_string()),
                status: if art.committed_head_commit.is_some() {
                    "completed".to_string()
                } else {
                    "unknown".to_string()
                },
                executor: Some(ObservationExecutor {
                    tool: None,
                    provider: Some(art.observed_provider.clone()),
                    role: Some("coder".to_string()),
                    requested_model: None,
                    observed_model: art.observed_model.clone(),
                    requested_effort: None,
                    observed_effort: art.observed_effort.clone(),
                }),
                started_at_ms: None,
                finished_at_ms: None,
                updated_at_ms: None,
            });
        }
    }

    runs
}

fn project_job_outbox(
    job: &CoworkerJobRecord,
    runtime: Option<&ControllerJobRuntimeState>,
) -> Vec<WorkspaceOutboxRecord> {
    let mut records = Vec::with_capacity(job.outbox.len());
    let mut seen_keys = BTreeSet::new();

    for rec in &job.outbox {
        seen_keys.insert(rec.request_key.as_str());
        let kind = match rec.kind {
            OutboxDispatchKind::TaskAttemptDispatch => "coder_dispatch",
            OutboxDispatchKind::TaskReviewDispatch => "reviewer_dispatch",
            OutboxDispatchKind::DependencyPreparation => "dependency_preparation",
            OutboxDispatchKind::TaskIntegration => "task_integration",
            OutboxDispatchKind::ClosureReviewDispatch => "closure_dispatch",
        };
        let status = match rec.status {
            OutboxDispatchStatus::ReservedBeforeEffect => "reserved",
            OutboxDispatchStatus::RefusedBeforeEffect => "failed",
            OutboxDispatchStatus::DispatchedEffectConfirmed => "dispatched",
            OutboxDispatchStatus::EffectUnknown => "unknown",
            OutboxDispatchStatus::ReconciledConfirmed => "reconciled",
            OutboxDispatchStatus::ReconciledNotExecuted => "cancelled",
        };
        records.push(WorkspaceOutboxRecord {
            outbox_id: rec.outbox_id.clone(),
            request_key: Some(rec.request_key.clone()),
            kind: kind.to_string(),
            status: status.to_string(),
            task_id: Some(rec.task_id.clone()),
            attempt_no: Some(rec.attempt_no),
            role: Some(workflow_role_name(rec.role).to_string()),
            run_id: rec.run_id.clone(),
            worker_id: rec.worker_id.clone(),
            receipt_id: rec.receipt_id.clone(),
            reserved_at_job_revision: Some(rec.reserved_at_job_revision),
            updated_at_job_revision: Some(rec.updated_at_job_revision),
            diagnostic: rec.diagnostic.clone(),
        });
    }

    if let Some(rt) = runtime {
        for eff in rt.pending_effects.values() {
            if seen_keys.contains(eff.request_key.as_str()) {
                continue;
            }
            let status = match eff.status {
                PendingControllerEffectStatus::ReservedBeforeEffect => "reserved",
                PendingControllerEffectStatus::EffectConfirmed => "completed",
                PendingControllerEffectStatus::EffectFailedTerminal
                | PendingControllerEffectStatus::RefusedBeforeEffect => "failed",
                PendingControllerEffectStatus::EffectUnknown => "unknown",
            };
            let run_id = eff
                .external_correlation_id
                .as_deref()
                .filter(|id| id.starts_with("run_"))
                .map(str::to_string);
            let receipt_id = eff
                .external_correlation_id
                .as_deref()
                .filter(|id| !id.starts_with("run_"))
                .map(str::to_string);
            records.push(WorkspaceOutboxRecord {
                outbox_id: eff.effect_id.clone(),
                request_key: Some(eff.request_key.clone()),
                kind: eff.kind.as_str().to_string(),
                status: status.to_string(),
                task_id: eff.task_id.clone(),
                attempt_no: eff.attempt_no,
                role: effect_kind_role(eff.kind).map(str::to_string),
                run_id,
                worker_id: None,
                receipt_id,
                reserved_at_job_revision: None,
                updated_at_job_revision: None,
                diagnostic: eff.diagnostic.clone(),
            });
        }
    }

    records
}

fn project_combined_candidate(
    comb: &CombinedCandidateIdentity,
    verified_acceptance: Option<&VerifiedHistoricalJobAcceptance>,
) -> WorkspaceCombinedCandidate {
    let is_verified_accepted =
        verified_acceptance.is_some_and(|v| v.combined_candidate_id == comb.combined_candidate_id);
    let status = match comb.status {
        CombinedCandidateStatus::ChecksVerified => "ready_for_closure",
        CombinedCandidateStatus::ClosureReserved => "closure_reserved",
        CombinedCandidateStatus::ClosureAccepted => {
            if is_verified_accepted {
                "accepted"
            } else {
                "unknown"
            }
        }
        CombinedCandidateStatus::Superseded => "invalidated",
        CombinedCandidateStatus::Rejected => "failed",
    };
    let produced_at_ms = if is_verified_accepted {
        verified_acceptance.and_then(|v| v.verified_at_ms)
    } else {
        None
    };
    WorkspaceCombinedCandidate {
        combined_candidate_id: comb.combined_candidate_id.clone(),
        job_revision: Some(comb.job_revision),
        status: status.to_string(),
        final_commit_sha: comb.final_commit_sha.clone(),
        final_tree_sha: Some(comb.final_tree_sha.clone()),
        candidate_digest: Some(comb.candidate_digest.clone()),
        closure_reservation_id: comb.closure_reservation_id.clone(),
        check_count: comb.check_evidence.len(),
        produced_at_ms,
    }
}

fn project_record_links(
    job: &CoworkerJobRecord,
    context: Option<&WorkspaceJobLineageContext>,
) -> WorkspaceRecordLinks {
    let source_plan_id = job
        .approval_linkage
        .as_ref()
        .map(|l| l.plan_id.clone())
        .or_else(|| job.planning_result.as_ref().map(|pr| pr.plan_id.clone()));
    let source_plan_revision = job
        .approval_linkage
        .as_ref()
        .map(|l| l.plan_revision)
        .or_else(|| job.planning_result.as_ref().map(|pr| pr.plan_revision));
    let source_approval_id = job.approval_linkage.as_ref().map(|l| l.approval_id.clone());

    let mut source_request_ids = vec![job.create_request_key.clone()];
    if let Some(ref pr) = job.planning_result {
        if !source_request_ids.contains(&pr.request_key) {
            source_request_ids.push(pr.request_key.clone());
        }
    }

    WorkspaceRecordLinks {
        source_plan_id,
        source_plan_revision,
        source_approval_id,
        source_work_brief_id: Some(job.work_brief.brief_id.clone()),
        planning_grant_id: job.planning_result.as_ref().map(|pr| pr.grant_id.clone()),
        planning_reservation_id: job
            .planning_result
            .as_ref()
            .map(|pr| pr.reservation_id.clone()),
        supersedes_job_id: job.supersedes_job_id.clone(),
        superseded_by_job_id: context.and_then(|c| c.superseded_by_job_id.clone()),
        parent_job_id: context.and_then(|c| c.parent_job_id.clone()),
        follow_up_job_ids: context
            .map(|c| c.follow_up_job_ids.clone())
            .unwrap_or_default(),
        architect_advice_ids: job
            .planning_result
            .as_ref()
            .map(|pr| pr.architect_advice_ids.clone())
            .unwrap_or_default(),
        source_request_ids,
    }
}

fn project_job_result(
    job: &CoworkerJobRecord,
    runtime: Option<&ControllerJobRuntimeState>,
    runtime_missing: bool,
    verified_acceptance: Option<&VerifiedHistoricalJobAcceptance>,
) -> Option<WorkspaceJobResult> {
    let closure_verdict_id = verified_acceptance
        .map(|v| v.closure_verdict_id.clone())
        .or_else(|| runtime.and_then(|rt| rt.closure_verdict_id.clone()));
    match job.phase {
        CoworkerJobPhase::ClosureReview => Some(WorkspaceJobResult {
            outcome: if runtime_missing {
                "unknown".to_string()
            } else {
                "ready_for_review".to_string()
            },
            summary: sanitize_workspace_summary(&job.work_brief.summary),
            produced_at_ms: None,
            delivery_stage: ObservationDeliveryStage::Unknown,
            closure_verdict_id,
        }),
        CoworkerJobPhase::Completed => match verified_acceptance {
            Some(vjob) => {
                let raw_summary = if !vjob.closure_summary.trim().is_empty() {
                    &vjob.closure_summary
                } else {
                    &job.work_brief.summary
                };
                Some(WorkspaceJobResult {
                    outcome: "completed".to_string(),
                    summary: sanitize_workspace_summary(raw_summary),
                    produced_at_ms: vjob.verified_at_ms,
                    delivery_stage: ObservationDeliveryStage::Unknown,
                    closure_verdict_id: Some(vjob.closure_verdict_id.clone()),
                })
            }
            None => Some(WorkspaceJobResult {
                outcome: "unknown".to_string(),
                summary: sanitize_workspace_summary(&job.work_brief.summary),
                produced_at_ms: None,
                delivery_stage: ObservationDeliveryStage::Unknown,
                closure_verdict_id,
            }),
        },
        CoworkerJobPhase::Cancelled => Some(WorkspaceJobResult {
            outcome: "stopped".to_string(),
            summary: "Job was cancelled or superseded.".to_string(),
            produced_at_ms: None,
            delivery_stage: ObservationDeliveryStage::Unknown,
            closure_verdict_id: None,
        }),
        _ => None,
    }
}

#[allow(clippy::too_many_arguments)]
fn project_job_summary(
    chat_identity: &WorkspaceChatIdentity,
    job: &CoworkerJobRecord,
    runtime: Option<&ControllerJobRuntimeState>,
    context: Option<&WorkspaceJobLineageContext>,
    runtime_missing: bool,
    projected_tasks: &[WorkspaceTaskHistory],
    runs: &[WorkspaceRunRecord],
    current_attention: Option<&WorkspaceAttentionState>,
    verified_acceptance: Option<&VerifiedHistoricalJobAcceptance>,
    verified_obsolete_planning: Option<&VerifiedObsoletePlanningSupersession>,
    verified_current_published_plan: Option<&VerifiedCurrentPublishedPlanMetadata>,
) -> WorkspaceJobSummary {
    let phase_str = job_phase_name(job.phase);
    // A supersession link alone is not private proof of obsolete-planning retirement.
    // Unproved awaiting-approval Unknown attention keeps its truthful unknown approval status.
    let unproved_awaiting_approval_unknown = job.phase == CoworkerJobPhase::AwaitingApproval
        && job.approval_linkage.is_none()
        && verified_obsolete_planning.is_none()
        && verified_current_published_plan.is_none()
        && current_attention.is_some_and(|a| a.kind == WorkspaceAttentionKind::Unknown);
    let approval_status = if unproved_awaiting_approval_unknown {
        "unknown"
    } else if context.is_some_and(|c| c.superseded_by_job_id.is_some()) {
        "superseded"
    } else if job.approval_linkage.is_some() {
        "approved"
    } else if verified_current_published_plan.is_some() {
        "pending_approval"
    } else if matches!(job.phase, CoworkerJobPhase::AwaitingApproval)
        || job.planning_result.is_some()
    {
        "unknown"
    } else {
        "draft"
    };

    let any_task_started =
        job.tasks.iter().any(|t| {
            t.current_attempt_no > 0 || !t.attempts.is_empty() || !t.candidates.is_empty()
        }) || !job.outbox.is_empty();

    let has_unresolved_effect =
        current_attention.is_some_and(|a| a.kind == WorkspaceAttentionKind::UnresolvedEffect);

    let (state, execution_status, review_state) = match job.phase {
        CoworkerJobPhase::Briefing | CoworkerJobPhase::Planning => {
            if has_unresolved_effect {
                (
                    ObservationJobState::Unknown,
                    "unknown",
                    Some("unresolved".to_string()),
                )
            } else {
                (ObservationJobState::Planning, "not_started", None)
            }
        }
        CoworkerJobPhase::AwaitingApproval => {
            if has_unresolved_effect {
                (
                    ObservationJobState::Unknown,
                    "unknown",
                    Some("unresolved".to_string()),
                )
            } else if verified_current_published_plan.is_some()
                || current_attention.is_some_and(|a| {
                    matches!(
                        a.kind,
                        WorkspaceAttentionKind::NeedsApproval
                            | WorkspaceAttentionKind::NeedsUserInput
                            | WorkspaceAttentionKind::OnHold
                    )
                })
            {
                (ObservationJobState::NeedsUser, "not_started", None)
            } else {
                (ObservationJobState::Unknown, "not_started", None)
            }
        }
        CoworkerJobPhase::Held => {
            if has_unresolved_effect {
                (
                    ObservationJobState::Unknown,
                    "unknown",
                    Some("unresolved".to_string()),
                )
            } else if verified_obsolete_planning.is_some() && current_attention.is_none() {
                (ObservationJobState::Unknown, "not_started", None)
            } else {
                (
                    ObservationJobState::NeedsUser,
                    if any_task_started {
                        "stopped"
                    } else {
                        "not_started"
                    },
                    None,
                )
            }
        }
        CoworkerJobPhase::Cancelled => {
            if has_unresolved_effect {
                (
                    ObservationJobState::Unknown,
                    "unknown",
                    Some("unresolved".to_string()),
                )
            } else {
                (ObservationJobState::Failed, "stopped", None)
            }
        }
        CoworkerJobPhase::ClosureReview => (
            if runtime_missing {
                ObservationJobState::Unknown
            } else {
                ObservationJobState::Reviewing
            },
            if runtime_missing {
                "unknown"
            } else {
                "ready_for_review"
            },
            Some("pending_review".to_string()),
        ),
        CoworkerJobPhase::Completed => {
            if verified_acceptance.is_some() {
                (
                    ObservationJobState::Completed,
                    "completed",
                    Some("accepted".to_string()),
                )
            } else {
                (
                    ObservationJobState::Unknown,
                    "unknown",
                    Some("unknown".to_string()),
                )
            }
        }
        CoworkerJobPhase::AssemblingCandidate => (
            if runtime_missing {
                ObservationJobState::Unknown
            } else {
                ObservationJobState::Running
            },
            if runtime_missing {
                "unknown"
            } else {
                "running"
            },
            None,
        ),
        CoworkerJobPhase::Executing => derive_executing_job_state(
            runtime_missing,
            any_task_started,
            projected_tasks,
            current_attention,
        ),
    };

    let (created_at_ms, updated_at_ms) =
        derive_job_timestamps(job, runtime, context, runs, verified_acceptance);

    let title = sanitize_observation_label(&job.work_brief.summary, &job.job_id);
    let summary =
        Some(sanitize_workspace_summary(&job.work_brief.summary)).filter(|s| !s.is_empty());

    WorkspaceJobSummary {
        source_id: chat_identity.source_id.clone(),
        job_id: job.job_id.clone(),
        binding_id: Some(chat_identity.binding_id.clone()),
        chat_identity: Some(chat_identity.clone()),
        title,
        job_revision: Some(job.job_revision),
        state,
        phase: Some(phase_str.to_string()),
        approval_status: approval_status.to_string(),
        execution_status: execution_status.to_string(),
        review_state,
        created_at_ms,
        updated_at_ms,
        summary,
    }
}

fn derive_executing_job_state(
    runtime_missing: bool,
    any_task_started: bool,
    projected_tasks: &[WorkspaceTaskHistory],
    current_attention: Option<&WorkspaceAttentionState>,
) -> (ObservationJobState, &'static str, Option<String>) {
    if let Some(attn) = current_attention {
        match attn.kind {
            WorkspaceAttentionKind::UnresolvedEffect => {
                return (
                    ObservationJobState::Unknown,
                    "unknown",
                    Some("unresolved".to_string()),
                );
            }
            WorkspaceAttentionKind::NeedsUserInput | WorkspaceAttentionKind::OnHold => {
                return (
                    ObservationJobState::NeedsUser,
                    if any_task_started {
                        "stopped"
                    } else {
                        "not_started"
                    },
                    None,
                );
            }
            WorkspaceAttentionKind::BlockedFailure => {
                return (ObservationJobState::Failed, "failed", None);
            }
            _ => {}
        }
    }

    if !any_task_started {
        return (ObservationJobState::Ready, "not_started", None);
    }
    if runtime_missing {
        return (ObservationJobState::Unknown, "unknown", None);
    }

    let any_running = projected_tasks
        .iter()
        .any(|t| matches!(t.status.as_str(), "running" | "queued"));
    let any_review = projected_tasks
        .iter()
        .any(|t| t.status == "ready_for_review");
    let any_changes_req = projected_tasks
        .iter()
        .any(|t| t.status == "changes_requested");

    if any_running {
        (ObservationJobState::Running, "running", None)
    } else if any_review {
        (
            ObservationJobState::Reviewing,
            "ready_for_review",
            Some("pending_review".to_string()),
        )
    } else if any_changes_req {
        (
            ObservationJobState::Running,
            "running",
            Some("changes_requested".to_string()),
        )
    } else {
        (ObservationJobState::Running, "running", None)
    }
}

fn derive_job_timestamps(
    job: &CoworkerJobRecord,
    runtime: Option<&ControllerJobRuntimeState>,
    context: Option<&WorkspaceJobLineageContext>,
    runs: &[WorkspaceRunRecord],
    verified_acceptance: Option<&VerifiedHistoricalJobAcceptance>,
) -> (Option<i64>, Option<i64>) {
    let mut created_candidates = Vec::new();
    let mut updated_candidates = Vec::new();

    if let Some(ctx) = context {
        if let Some(ref plan_rev) = ctx.plan_revision {
            if plan_rev.created_at_ms > 0 {
                created_candidates.push(plan_rev.created_at_ms);
                updated_candidates.push(plan_rev.created_at_ms);
            }
        }
        if let Some(ref approval) = ctx.approval_record {
            if approval.created_at_ms > 0 {
                created_candidates.push(approval.created_at_ms);
                updated_candidates.push(approval.created_at_ms);
            }
        }
    }

    if let Some(ref pr) = job.planning_result {
        if let Ok(ts) = i64::try_from(pr.recorded_at_ms) {
            if ts > 0 {
                created_candidates.push(ts);
                updated_candidates.push(ts);
            }
        }
    }

    if let Some(rt) = runtime {
        if let Ok(env_ts) = i64::try_from(rt.envelope.created_at_ms) {
            if env_ts > 0 {
                created_candidates.push(env_ts);
                updated_candidates.push(env_ts);
            }
        }
        for eff in rt.pending_effects.values() {
            if let Ok(cts) = i64::try_from(eff.created_at_ms) {
                if cts > 0 {
                    updated_candidates.push(cts);
                }
            }
            if let Ok(uts) = i64::try_from(eff.updated_at_ms) {
                if uts > 0 {
                    updated_candidates.push(uts);
                }
            }
        }
    }

    for run in runs {
        if let Some(ts) = run.updated_at_ms {
            if ts > 0 {
                updated_candidates.push(ts);
            }
        }
    }

    if let Some(ts) = verified_acceptance.and_then(|v| v.verified_at_ms) {
        if ts > 0 {
            updated_candidates.push(ts);
        }
    }

    let created_at_ms = created_candidates.into_iter().min();
    let updated_at_ms = updated_candidates.into_iter().max().or(created_at_ms);
    (created_at_ms, updated_at_ms)
}

fn find_matching_controller_effect<'a>(
    runtime: Option<&'a ControllerJobRuntimeState>,
    task_id: &str,
    attempt_no: u32,
    role: WorkflowRole,
) -> Option<&'a PendingControllerEffect> {
    let rt = runtime?;
    let expected_kind = match role {
        WorkflowRole::Coder => PendingControllerEffectKind::CoderDispatch,
        WorkflowRole::Reviewer => PendingControllerEffectKind::ReviewerInvocation,
        WorkflowRole::Closure => PendingControllerEffectKind::ClosureInvocation,
        _ => return None,
    };
    rt.pending_effects.values().find(|eff| {
        eff.task_id.as_deref() == Some(task_id)
            && eff.attempt_no == Some(attempt_no)
            && eff.kind == expected_kind
    })
}

#[derive(Default)]
struct ReadOnlyPlanSnapshot {
    loaded: bool,
    revisions_by_key: BTreeMap<(String, u64), PlanRevision>,
    latest_revision_by_plan: BTreeMap<String, u64>,
}

fn load_plan_revisions_readonly(
    chat_identity: &WorkspaceChatIdentity,
    stores: &WorkspaceHistoryStores<'_>,
    degraded: &mut bool,
    plan_lookup_degraded: &mut bool,
    diagnostics: &mut Vec<WorkspaceHistoryDiagnostic>,
) -> ReadOnlyPlanSnapshot {
    let mut snapshot = ReadOnlyPlanSnapshot::default();
    let plan_dir = stores.plan_store.map_or(stores.plans_dir, PlanStore::dir);
    if stores.plan_store.is_none()
        && matches!(
            fs::symlink_metadata(plan_dir.join("plans.json")),
            Err(ref err) if err.kind() == ErrorKind::NotFound
        )
    {
        return snapshot;
    }

    match PlanStore::read_snapshot_readonly_at(plan_dir) {
        Ok(revisions) => {
            snapshot.loaded = true;
            for rev in revisions {
                let max_rev = snapshot
                    .latest_revision_by_plan
                    .entry(rev.plan_id.clone())
                    .or_insert(rev.revision);
                *max_rev = (*max_rev).max(rev.revision);
                if rev.binding_id == chat_identity.binding_id {
                    let key = (rev.plan_id.clone(), rev.revision);
                    snapshot.revisions_by_key.insert(key, rev);
                }
            }
        }
        Err(_) => {
            *degraded = true;
            *plan_lookup_degraded = true;
            diagnostics.push(WorkspaceHistoryDiagnostic::new(
                WorkspaceHistoryDiagnosticKind::PlanLookupDegraded,
                None,
                "PlanStore read-only snapshot failed validation; continuing without plan titles.",
            ));
        }
    }
    snapshot
}

fn load_authority_records_readonly(
    stores: &WorkspaceHistoryStores<'_>,
    degraded: &mut bool,
    diagnostics: &mut Vec<WorkspaceHistoryDiagnostic>,
) -> Option<BTreeMap<String, ApprovalRecord>> {
    let records_res = if let Some(auth_store) = stores.authority_store {
        if !auth_store.approvals_path().exists() {
            return Some(BTreeMap::new());
        }
        Some(auth_store.snapshot_readonly())
    } else if let Some(auth_dir) = stores.authority_dir {
        match fs::symlink_metadata(auth_dir) {
            Ok(_) => {
                if !auth_dir.join("approvals.json").exists()
                    && verify_existing_private_dir(auth_dir).is_ok()
                {
                    return Some(BTreeMap::new());
                }
                Some(AuthorityStore::read_snapshot_readonly_at(auth_dir))
            }
            Err(err) if err.kind() == ErrorKind::NotFound => {
                *degraded = true;
                diagnostics.push(WorkspaceHistoryDiagnostic::new(
                    WorkspaceHistoryDiagnosticKind::MissingStore,
                    None,
                    "Configured authority store directory is missing on disk.",
                ));
                return None;
            }
            Err(_) => {
                *degraded = true;
                diagnostics.push(WorkspaceHistoryDiagnostic::new(
                    WorkspaceHistoryDiagnosticKind::DegradedStore,
                    None,
                    "Configured authority store directory could not be inspected read-only.",
                ));
                return None;
            }
        }
    } else {
        None
    };

    match records_res? {
        Ok(records) => {
            let mut by_id = BTreeMap::new();
            for rec in records {
                by_id.insert(rec.approval_id.clone(), rec);
            }
            Some(by_id)
        }
        Err(_) => {
            *degraded = true;
            diagnostics.push(WorkspaceHistoryDiagnostic::new(
                WorkspaceHistoryDiagnosticKind::DegradedStore,
                None,
                "Authority store read-only snapshot failed integrity validation.",
            ));
            Some(BTreeMap::new())
        }
    }
}

fn verify_sessions_root_readonly<'a>(
    sessions_dir: Option<&'a Path>,
    degraded: &mut bool,
    diagnostics: &mut Vec<WorkspaceHistoryDiagnostic>,
) -> Option<&'a Path> {
    let sdir = sessions_dir?;
    match fs::symlink_metadata(sdir) {
        Ok(_) => match verify_existing_private_dir(sdir) {
            Ok(_) => Some(sdir),
            Err(_) => {
                *degraded = true;
                diagnostics.push(WorkspaceHistoryDiagnostic::new(
                    WorkspaceHistoryDiagnosticKind::SessionLookupDegraded,
                    None,
                    "Configured host sessions directory failed private non-symlink verification.",
                ));
                None
            }
        },
        Err(err) if err.kind() == ErrorKind::NotFound => {
            *degraded = true;
            diagnostics.push(WorkspaceHistoryDiagnostic::new(
                WorkspaceHistoryDiagnosticKind::MissingStore,
                None,
                "Configured host sessions directory does not exist on disk.",
            ));
            None
        }
        Err(_) => {
            *degraded = true;
            diagnostics.push(WorkspaceHistoryDiagnostic::new(
                WorkspaceHistoryDiagnosticKind::SessionLookupDegraded,
                None,
                "Configured host sessions directory metadata could not be read.",
            ));
            None
        }
    }
}

fn inspect_session_manifest_readonly(
    sessions_root: &Path,
    approval: &ApprovalRecord,
    job_id: &str,
    degraded: &mut bool,
    diagnostics: &mut Vec<WorkspaceHistoryDiagnostic>,
) {
    let candidate_dir = sessions_root.join(&approval.approval_id);
    if fs::symlink_metadata(&candidate_dir).is_err() {
        return;
    }
    let Ok(session_dir) = verify_existing_session_dir(sessions_root, &approval.approval_id) else {
        *degraded = true;
        diagnostics.push(WorkspaceHistoryDiagnostic::new(
            WorkspaceHistoryDiagnosticKind::SessionLookupDegraded,
            Some(job_id),
            "Host session directory failed read-only structure or permission verification.",
        ));
        return;
    };
    if read_host_manifest_optional(&session_dir, approval).is_err() {
        *degraded = true;
        diagnostics.push(WorkspaceHistoryDiagnostic::new(
            WorkspaceHistoryDiagnosticKind::SessionLookupDegraded,
            Some(job_id),
            "Host session manifest failed read-only integrity verification against approval.",
        ));
    }
}

const fn workflow_role_name(role: WorkflowRole) -> &'static str {
    match role {
        WorkflowRole::Follower => "follower",
        WorkflowRole::Orchestrator => "orchestrator",
        WorkflowRole::Architect => "architect",
        WorkflowRole::Coder => "coder",
        WorkflowRole::Reviewer => "reviewer",
        WorkflowRole::Closure => "closure",
    }
}

const fn difficulty_name(difficulty: Difficulty) -> &'static str {
    match difficulty {
        Difficulty::Easy => "easy",
        Difficulty::Medium => "medium",
        Difficulty::Hard => "hard",
        Difficulty::Extreme => "extreme",
    }
}

const fn job_phase_name(phase: CoworkerJobPhase) -> &'static str {
    match phase {
        CoworkerJobPhase::Briefing => "briefing",
        CoworkerJobPhase::Planning => "planning",
        CoworkerJobPhase::AwaitingApproval => "awaiting_approval",
        CoworkerJobPhase::Executing => "executing",
        CoworkerJobPhase::AssemblingCandidate => "assembling_candidate",
        CoworkerJobPhase::ClosureReview => "closure_review",
        CoworkerJobPhase::Held => "held",
        CoworkerJobPhase::Completed => "completed",
        CoworkerJobPhase::Cancelled => "cancelled",
    }
}

const fn effect_kind_failure_stage(kind: PendingControllerEffectKind) -> &'static str {
    match kind {
        PendingControllerEffectKind::CoderDispatch | PendingControllerEffectKind::TaskCommit => {
            "worker_execution"
        }
        PendingControllerEffectKind::CheckExecution => "verification",
        PendingControllerEffectKind::ReviewerInvocation => "review",
        PendingControllerEffectKind::TaskIntegration
        | PendingControllerEffectKind::JobAggregation => "integration",
        PendingControllerEffectKind::ClosureInvocation => "closure",
    }
}

const fn effect_kind_role(kind: PendingControllerEffectKind) -> Option<&'static str> {
    match kind {
        PendingControllerEffectKind::CoderDispatch | PendingControllerEffectKind::TaskCommit => {
            Some("coder")
        }
        PendingControllerEffectKind::CheckExecution => Some("verification"),
        PendingControllerEffectKind::ReviewerInvocation => Some("reviewer"),
        PendingControllerEffectKind::TaskIntegration
        | PendingControllerEffectKind::JobAggregation => Some("integration"),
        PendingControllerEffectKind::ClosureInvocation => Some("closure"),
    }
}

fn acceptance_failed(job_id: &str, message: &str) -> WorkspaceHistoryDiagnostic {
    WorkspaceHistoryDiagnostic::new(
        WorkspaceHistoryDiagnosticKind::AcceptanceVerificationFailed,
        Some(job_id),
        message,
    )
}

fn adapter_to_worker_provider(
    job_id: &str,
    adapter: ToolAdapter,
) -> Result<WorkerProvider, WorkspaceHistoryDiagnostic> {
    match adapter {
        ToolAdapter::Antigravity => Ok(WorkerProvider::Antigravity),
        ToolAdapter::OpencodeV2 => Ok(WorkerProvider::OpenCodeV2),
        ToolAdapter::CodexCli => Ok(WorkerProvider::CodexCli),
        _ => Err(acceptance_failed(
            job_id,
            "Frozen role choice uses an unsupported tool adapter.",
        )),
    }
}

fn verify_historical_job_acceptance_readonly(
    chat_identity: &WorkspaceChatIdentity,
    job: &CoworkerJobRecord,
    runtime: Option<&ControllerJobRuntimeState>,
    context: &WorkspaceJobLineageContext,
    store_ctx: &HistoricalStoreVerificationContext<'_, '_>,
) -> Result<VerifiedHistoricalJobAcceptance, WorkspaceHistoryDiagnostic> {
    if job.phase != CoworkerJobPhase::Completed || job.tasks.is_empty() {
        return Err(acceptance_failed(
            &job.job_id,
            "Completed job acceptance requires Completed phase and at least one approved task.",
        ));
    }

    let Some(ref linkage) = job.approval_linkage else {
        return Err(acceptance_failed(
            &job.job_id,
            "Completed job is missing FrozenApprovalLinkage.",
        ));
    };

    if store_ctx.authority_store_degraded
        || ((store_ctx.stores.authority_store.is_some()
            || store_ctx.stores.authority_dir.is_some())
            && context.approval_record.is_none())
    {
        return Err(acceptance_failed(
            &job.job_id,
            "Configured authority store could not verify the job's frozen approval record.",
        ));
    }

    if store_ctx.plan_lookup_degraded
        || (store_ctx.stores.plan_store.is_some() && context.plan_revision.is_none())
    {
        return Err(acceptance_failed(
            &job.job_id,
            "Configured PlanStore could not verify the job's original frozen plan revision.",
        ));
    }

    if store_ctx.stores.sessions_dir.is_some() && store_ctx.verified_sessions_root.is_none() {
        return Err(acceptance_failed(
            &job.job_id,
            "Configured host sessions store could not be verified read-only.",
        ));
    }

    if job.holds.iter().any(|h| h.active)
        || job
            .tasks
            .iter()
            .any(|t| !t.active_hold_ids.is_empty() || t.unresolved_outbox_id.is_some())
    {
        return Err(acceptance_failed(
            &job.job_id,
            "Completed job still carries an active workflow hold or unresolved task outbox reference.",
        ));
    }

    let mut seen_outbox_ids = BTreeSet::new();
    let mut seen_outbox_keys = BTreeSet::new();
    for ob in &job.outbox {
        if !seen_outbox_ids.insert(ob.outbox_id.as_str())
            || !seen_outbox_keys.insert(ob.request_key.as_str())
        {
            return Err(acceptance_failed(
                &job.job_id,
                "Job outbox contains duplicate outbox_id or request_key references.",
            ));
        }
        if matches!(
            ob.status,
            OutboxDispatchStatus::ReservedBeforeEffect | OutboxDispatchStatus::EffectUnknown
        ) {
            return Err(acceptance_failed(
                &job.job_id,
                "Completed job still has an unresolved or reserved outbox dispatch.",
            ));
        }
    }

    let Some(rt) = runtime else {
        return Err(acceptance_failed(
            &job.job_id,
            "Completed job is missing ControllerJobRuntimeState; acceptance remains unknown.",
        ));
    };

    if rt.stop_requested || rt.stop_or_block_reason.is_some() {
        return Err(acceptance_failed(
            &job.job_id,
            "Controller runtime state recorded a stop or block fence.",
        ));
    }

    rt.envelope
        .verify_matches_job_readonly_historical(job)
        .map_err(|_| {
            acceptance_failed(
                &job.job_id,
                "FrozenControllerExecutionEnvelope failed integrity or job linkage verification.",
            )
        })?;
    let recomputed_env_digest = rt
        .envelope
        .compute_digest_readonly_historical()
        .map_err(|_| {
            acceptance_failed(
                &job.job_id,
                "FrozenControllerExecutionEnvelope digest could not be recomputed.",
            )
        })?;
    if !constant_time_str_eq(&recomputed_env_digest, &rt.envelope.envelope_digest) {
        return Err(acceptance_failed(
            &job.job_id,
            "FrozenControllerExecutionEnvelope digest mismatch.",
        ));
    }

    let computed_policy_digest = rt.envelope.policy.digest().map_err(|_| {
        acceptance_failed(
            &job.job_id,
            "Frozen ExecutionPolicy digest computation failed.",
        )
    })?;
    if rt.envelope.policy.policy_id != linkage.policy_id
        || computed_policy_digest != linkage.policy_digest
    {
        return Err(acceptance_failed(
            &job.job_id,
            "Frozen ExecutionPolicy does not match FrozenApprovalLinkage policy_id or policy_digest.",
        ));
    }

    for eff in rt.pending_effects.values() {
        if eff.job_id != job.job_id {
            return Err(acceptance_failed(
                &job.job_id,
                "PendingControllerEffect references a foreign job_id.",
            ));
        }
        if matches!(
            eff.status,
            PendingControllerEffectStatus::ReservedBeforeEffect
                | PendingControllerEffectStatus::EffectUnknown
        ) {
            return Err(acceptance_failed(
                &job.job_id,
                "Controller runtime contains an unresolved or reserved pending effect.",
            ));
        }
    }

    verify_companion_approval_and_plan_frozen_records(
        &job.job_id,
        job,
        linkage,
        context,
        store_ctx,
    )?;

    let comb = verify_unique_accepted_combined_candidate(&job.job_id, job, linkage)?;

    let Some(ref subject) = rt.final_candidate_subject else {
        return Err(acceptance_failed(
            &job.job_id,
            "ControllerJobRuntimeState is missing persisted FinalCandidateSubject.",
        ));
    };

    verify_final_subject_against_frozen_scope(
        chat_identity,
        job,
        linkage,
        rt,
        comb,
        subject,
        context,
        store_ctx,
    )?;

    verify_candidate_checks_and_reports(
        chat_identity,
        &job.job_id,
        rt,
        comb,
        subject,
        store_ctx.stores.plans_dir,
    )?;

    let (closure_verdict_id, closure_summary, verified_at_ms) =
        verify_closure_ledger_and_artifacts(chat_identity, job, rt, comb, subject, store_ctx)?;

    let tasks = verify_task_review_and_integration_chain(
        job,
        linkage,
        rt,
        comb,
        subject,
        store_ctx.stores.plans_dir,
    )?;

    let closure_reservation_id = comb.closure_reservation_id.clone().ok_or_else(|| {
        acceptance_failed(
            &job.job_id,
            "Accepted CombinedCandidateIdentity is missing closure_reservation_id.",
        )
    })?;

    Ok(VerifiedHistoricalJobAcceptance {
        combined_candidate_id: comb.combined_candidate_id.clone(),
        closure_reservation_id,
        closure_verdict_id,
        closure_summary,
        verified_at_ms,
        tasks,
    })
}

fn verify_companion_approval_and_plan_frozen_records(
    job_id: &str,
    job: &CoworkerJobRecord,
    linkage: &crate::coworker_workflow::FrozenApprovalLinkage,
    context: &WorkspaceJobLineageContext,
    store_ctx: &HistoricalStoreVerificationContext<'_, '_>,
) -> Result<(), WorkspaceHistoryDiagnostic> {
    if let Some(ref approval) = context.approval_record {
        let auth_store_path = if let Some(auth_store) = store_ctx.stores.authority_store {
            PathBuf::from(auth_store.approvals_path())
        } else if let Some(auth_dir) = store_ctx.stores.authority_dir {
            auth_dir.join("approvals.json")
        } else {
            store_ctx.stores.plans_dir.join("approvals.json")
        };
        verify_approval_record_integrity(approval, &auth_store_path).map_err(|_| {
            acceptance_failed(
                job_id,
                "Persisted ApprovalRecord failed cryptographic subject_digest verification.",
            )
        })?;
        if approval.approved_task_ids != linkage.approved_task_ids
            || approval.approved_write_paths != linkage.approved_write_paths
            || approval.tasks.len() != job.tasks.len()
        {
            return Err(acceptance_failed(
                job_id,
                "ApprovalRecord task list or approved_write_paths drifted from job.",
            ));
        }
        let mut seen_approved_tasks = BTreeSet::new();
        for task in &job.tasks {
            if !seen_approved_tasks.insert(task.task_id.as_str()) {
                return Err(acceptance_failed(
                    job_id,
                    "Job contains duplicate task_id entries.",
                ));
            }
            let Some(approved_task) = approval.tasks.iter().find(|t| t.task_id == task.task_id)
            else {
                return Err(acceptance_failed(
                    job_id,
                    "Job task was not found in frozen ApprovalRecord.tasks.",
                ));
            };
            let approved_paths: BTreeSet<&str> = approved_task
                .allowed_write_paths
                .iter()
                .map(String::as_str)
                .collect();
            let job_paths: BTreeSet<&str> = task.write_paths.iter().map(String::as_str).collect();
            if approved_task.task_digest != task.task_digest
                || approved_task.role != task.role
                || approved_task.difficulty != task.difficulty
                || approved_task.depends_on != task.depends_on
                || approved_paths != job_paths
                || approved_task.tool_id != task.selected_choice.tool_id
                || approved_task.adapter != task.selected_choice.adapter
                || approved_task.model_id != task.selected_choice.model_id
                || approved_task.model_arg != task.selected_choice.model_arg
                || approved_task.effort != task.selected_choice.effort
            {
                return Err(acceptance_failed(
                    job_id,
                    "Job task specification drifted from frozen ApprovedTaskRecord.",
                ));
            }
        }
    }

    // Historical invariant: verify the exact frozen plan_revision snapshot without requiring it
    // to remain the latest/active published revision after later plan edits.
    if let Some(ref plan_rev) = context.plan_revision {
        if plan_rev.plan_id != linkage.plan_id
            || plan_rev.revision != linkage.plan_revision
            || plan_rev.profile_id != linkage.profile_id
            || plan_rev.profile_revision != linkage.registry_revision
        {
            return Err(acceptance_failed(
                job_id,
                "Persisted PlanRevision metadata does not match frozen approval linkage.",
            ));
        }
        for task in &job.tasks {
            let Some(plan_task) = plan_rev.tasks.iter().find(|pt| pt.id == task.task_id) else {
                return Err(acceptance_failed(
                    job_id,
                    "Job task_id is absent from the original frozen PlanRevision.",
                ));
            };
            let plan_paths: BTreeSet<&str> = plan_task
                .allowed_write_paths
                .iter()
                .map(String::as_str)
                .collect();
            let job_paths: BTreeSet<&str> = task.write_paths.iter().map(String::as_str).collect();
            if plan_task.role != task.role
                || plan_task.difficulty != task.difficulty
                || plan_task.depends_on != task.depends_on
                || plan_paths != job_paths
            {
                return Err(acceptance_failed(
                    job_id,
                    "Job task specification drifted from the original frozen PlanRevision.",
                ));
            }
        }
    }

    Ok(())
}

fn verify_unique_accepted_combined_candidate<'a>(
    job_id: &str,
    job: &'a CoworkerJobRecord,
    linkage: &crate::coworker_workflow::FrozenApprovalLinkage,
) -> Result<&'a CombinedCandidateIdentity, WorkspaceHistoryDiagnostic> {
    let mut seen_comb_ids = BTreeSet::new();
    let mut seen_res_ids = BTreeSet::new();
    let mut accepted_candidates = Vec::new();

    for comb in &job.combined_candidates {
        if !seen_comb_ids.insert(comb.combined_candidate_id.as_str()) {
            return Err(acceptance_failed(
                job_id,
                "Job contains duplicate combined_candidate_id entries.",
            ));
        }
        if let Some(ref res_id) = comb.closure_reservation_id {
            if !seen_res_ids.insert(res_id.as_str()) {
                return Err(acceptance_failed(
                    job_id,
                    "Job combined candidates contain duplicate closure_reservation_id references.",
                ));
            }
        }
        match comb.status {
            CombinedCandidateStatus::ClosureAccepted => accepted_candidates.push(comb),
            CombinedCandidateStatus::Superseded | CombinedCandidateStatus::Rejected => {}
            CombinedCandidateStatus::ChecksVerified | CombinedCandidateStatus::ClosureReserved => {
                return Err(acceptance_failed(
                    job_id,
                    "Completed job still carries an unfinalized combined candidate.",
                ));
            }
        }
    }

    if accepted_candidates.len() != 1 {
        return Err(acceptance_failed(
            job_id,
            "Completed job must have exactly one ClosureAccepted CombinedCandidateIdentity.",
        ));
    }

    let comb = accepted_candidates[0];
    let recomputed_digest = comb.compute_candidate_digest().map_err(|_| {
        acceptance_failed(
            job_id,
            "CombinedCandidateIdentity digest could not be recomputed.",
        )
    })?;
    if recomputed_digest != comb.candidate_digest {
        return Err(acceptance_failed(
            job_id,
            "CombinedCandidateIdentity candidate_digest mismatch.",
        ));
    }

    if comb.job_id != job.job_id
        || comb.project_base != linkage.project_base
        || comb.plan_id != linkage.plan_id
        || comb.plan_revision != linkage.plan_revision
        || comb.plan_digest != linkage.plan_digest
        || comb.approval_id != linkage.approval_id
        || comb.approval_subject_digest != linkage.approval_subject_digest
        || comb.scope_policy_revision != linkage.registry_revision
        || comb.policy_id != linkage.policy_id
        || comb.policy_digest != linkage.policy_digest
        || comb.final_commit_sha.is_none()
        || comb.closure_reservation_id.is_none()
    {
        return Err(acceptance_failed(
            job_id,
            "Accepted CombinedCandidateIdentity does not match frozen approval/plan/policy revisions.",
        ));
    }

    Ok(comb)
}

#[allow(clippy::too_many_arguments)]
fn verify_final_subject_against_frozen_scope(
    chat_identity: &WorkspaceChatIdentity,
    job: &CoworkerJobRecord,
    linkage: &crate::coworker_workflow::FrozenApprovalLinkage,
    rt: &ControllerJobRuntimeState,
    comb: &CombinedCandidateIdentity,
    subject: &FinalCandidateSubject,
    context: &WorkspaceJobLineageContext,
    store_ctx: &HistoricalStoreVerificationContext<'_, '_>,
) -> Result<(), WorkspaceHistoryDiagnostic> {
    subject.verify_integrity().map_err(|_| {
        acceptance_failed(
            &job.job_id,
            "Persisted FinalCandidateSubject failed cryptographic integrity verification.",
        )
    })?;

    verify_legacy_base_closure_insufficient(&linkage.project_base.base_tree_sha, subject)
        .and_then(|()| {
            verify_legacy_base_closure_insufficient(&linkage.approval_subject_digest, subject)
        })
        .map_err(|_| {
            acceptance_failed(
                &job.job_id,
                "FinalCandidateSubject failed base-vs-candidate distinctness verification.",
            )
        })?;

    if subject.repository_path != Path::new(&chat_identity.canonical_project_scope) {
        return Err(acceptance_failed(
            &job.job_id,
            "FinalCandidateSubject repository_path does not match canonical_project_scope.",
        ));
    }

    if let Some(sroot) = store_ctx.verified_sessions_root {
        let session_dir =
            verify_existing_session_dir(sroot, &linkage.approval_id).map_err(|_| {
                acceptance_failed(
                    &job.job_id,
                    "Host session directory for approval_id failed read-only verification.",
                )
            })?;
        if let Some(ref approval) = context.approval_record {
            read_host_manifest_optional(&session_dir, approval).map_err(|_| {
                acceptance_failed(
                    &job.job_id,
                    "Host session manifest failed read-only verification against approval.",
                )
            })?;
        }
        if subject.work_area_root != session_dir.join(WORK_AREA_SUBDIR) {
            return Err(acceptance_failed(
                &job.job_id,
                "FinalCandidateSubject work_area_root does not match the approval's session work-area path.",
            ));
        }
    }

    let closure_provider =
        adapter_to_worker_provider(&job.job_id, rt.envelope.closure_choice.adapter)?;
    let resolved_closure = resolve_model_target(
        closure_provider,
        &rt.envelope.closure_choice.model_arg,
        rt.envelope.closure_choice.effort.as_deref(),
    )
    .map_err(|_| {
        acceptance_failed(
            &job.job_id,
            "Frozen closure_choice model target failed validation.",
        )
    })?;

    if subject.approved_base_commit != linkage.project_base.base_commit
        || subject.approved_base_tree_sha != linkage.project_base.base_tree_sha
        || Some(subject.candidate_head_commit.as_str()) != comb.final_commit_sha.as_deref()
        || subject.candidate_tree_sha != comb.final_tree_sha
        || subject.policy.policy_id != linkage.policy_id
        || subject.policy.policy_digest != linkage.policy_digest
        || subject.policy.scope_revision != linkage.plan_revision
        || subject.policy.registry_revision != linkage.registry_revision
        || subject.policy.approval_id != linkage.approval_id
        || subject.policy.approval_subject_digest != linkage.approval_subject_digest
        || subject.policy.requested_provider != closure_provider.as_str()
        || subject.policy.requested_model != resolved_closure.base_model
        || subject.policy.requested_effort != resolved_closure.effort
        || subject.policy.require_effort_evidence != resolved_closure.effort.is_some()
        || !subject.policy.require_passing_checks
    {
        return Err(acceptance_failed(
            &job.job_id,
            "FinalCandidateSubject base/head/tree or frozen policy binding drifted from job.",
        ));
    }

    let expected_agg_write_paths: BTreeSet<String> = job
        .tasks
        .iter()
        .flat_map(|t| t.write_paths.iter().cloned())
        .collect();
    if subject.aggregate_allowed_write_paths != expected_agg_write_paths
        || !subject.aggregate_allowed_write_paths.iter().all(|scope| {
            is_task_scope_contained_in_operator_allowlist(scope, &linkage.approved_write_paths)
        })
    {
        return Err(acceptance_failed(
            &job.job_id,
            "FinalCandidateSubject aggregate_allowed_write_paths drifted from approved task scopes.",
        ));
    }

    Ok(())
}

fn verify_candidate_checks_and_reports(
    chat_identity: &WorkspaceChatIdentity,
    job_id: &str,
    rt: &ControllerJobRuntimeState,
    comb: &CombinedCandidateIdentity,
    subject: &FinalCandidateSubject,
    plans_dir: &Path,
) -> Result<(), WorkspaceHistoryDiagnostic> {
    if comb.check_evidence.is_empty() || comb.check_evidence != rt.candidate_check_evidence {
        return Err(acceptance_failed(
            job_id,
            "Candidate check_evidence is missing or mismatched between workflow and runtime ledgers.",
        ));
    }

    let mut seen_check_ids = BTreeSet::new();
    for ev in &comb.check_evidence {
        if !seen_check_ids.insert(ev.check_id.as_str())
            || !ev.passed
            || ev.exit_code != 0
            || ev.validate_passed().is_err()
        {
            return Err(acceptance_failed(
                job_id,
                "Candidate check_evidence contains duplicate or failing check entries.",
            ));
        }
    }

    if subject.check_evidence.validate(true).is_err()
        || !subject.check_evidence.all_checks_passed()
        || subject.check_evidence.failed_test_count != 0
    {
        return Err(acceptance_failed(
            job_id,
            "FinalCandidateSubject check_evidence summary is not passing.",
        ));
    }

    let expected_check_subject = FrozenCheckSubject {
        binding_id: chat_identity.binding_id.clone(),
        thread_uuid: chat_identity.thread_uuid.clone(),
        job_id: job_id.to_string(),
        target_kind: FrozenCheckTargetKind::RunIntegrationWorktree,
        repository_path: subject.repository_path.clone(),
        work_area_root: subject.work_area_root.clone(),
        worktree_id: subject.integration_worktree_id.clone(),
        expected_run_id: subject.run_id.clone(),
        expected_task_id: None,
        expected_base_commit: subject.approved_base_commit.clone(),
        expected_head_commit: subject.candidate_head_commit.clone(),
        expected_tree_sha: subject.candidate_tree_sha.clone(),
    };

    let mut verified_report_check_ids = BTreeSet::new();
    for eff in rt
        .pending_effects
        .values()
        .filter(|e| e.task_id.is_none() && e.kind == PendingControllerEffectKind::CheckExecution)
    {
        if eff.status == PendingControllerEffectStatus::EffectConfirmed {
            let report = read_persisted_candidate_check_report(plans_dir, &eff.effect_id)
                .map_err(|_| {
                    acceptance_failed(
                        job_id,
                        "Persisted candidate FrozenCheckExecutionReport failed read-only validation.",
                    )
                })?
                .ok_or_else(|| {
                    acceptance_failed(
                        job_id,
                        "Confirmed candidate CheckExecution effect is missing its persisted report.",
                    )
                })?;
            report.validate_integrity().map_err(|_| {
                acceptance_failed(
                    job_id,
                    "Persisted candidate FrozenCheckExecutionReport failed digest integrity check.",
                )
            })?;
            if report.subject.expected_head_commit == subject.candidate_head_commit
                && report.subject.expected_tree_sha == subject.candidate_tree_sha
            {
                if !report.passed()
                    || report.status != FrozenCheckExecutionStatus::Passed
                    || report.exit_code != Some(0)
                {
                    return Err(acceptance_failed(
                        job_id,
                        "Persisted candidate check report for final candidate did not pass.",
                    ));
                }
                let Some(spec) = rt.envelope.check_commands.get(&report.check_id) else {
                    return Err(acceptance_failed(
                        job_id,
                        "Persisted candidate check report references an unknown check_id.",
                    ));
                };
                spec.verify_historical_execution_report_readonly(&report, &expected_check_subject)
                    .map_err(|_| {
                        acceptance_failed(
                            job_id,
                            "Persisted candidate check report failed subject/spec context verification.",
                        )
                    })?;
                let Some(matching_ev) = comb.check_evidence.iter().find(|ev| {
                    ev.check_id == report.check_id && ev.evidence_digest == report.evidence_digest
                }) else {
                    return Err(acceptance_failed(
                        job_id,
                        "Persisted candidate check report digest is missing from CandidateCheckEvidence.",
                    ));
                };
                spec.verify_historical_candidate_check_evidence_readonly(
                    matching_ev,
                    &report,
                    &expected_check_subject,
                )
                .map_err(|_| {
                    acceptance_failed(
                        job_id,
                        "CandidateCheckEvidence failed full verification against persisted check report.",
                    )
                })?;
                verified_report_check_ids.insert(report.check_id.clone());
            }
        }
    }

    if verified_report_check_ids.is_empty()
        || !rt
            .envelope
            .check_commands
            .values()
            .any(|s| s.required_for_candidate_closure)
        || !seen_check_ids
            .iter()
            .all(|id| verified_report_check_ids.contains(*id))
    {
        return Err(acceptance_failed(
            job_id,
            "Accepted candidate check_evidence is not backed by verified persisted candidate check reports.",
        ));
    }

    for spec in rt
        .envelope
        .check_commands
        .values()
        .filter(|s| s.required_for_candidate_closure)
    {
        if !seen_check_ids.contains(spec.check_id.as_str())
            || !verified_report_check_ids.contains(spec.check_id.as_str())
        {
            return Err(acceptance_failed(
                job_id,
                "A required_for_candidate_closure check command lacks a verified passing check report.",
            ));
        }
    }

    Ok(())
}

fn verify_closure_ledger_and_artifacts(
    chat_identity: &WorkspaceChatIdentity,
    job: &CoworkerJobRecord,
    rt: &ControllerJobRuntimeState,
    comb: &CombinedCandidateIdentity,
    subject: &FinalCandidateSubject,
    store_ctx: &HistoricalStoreVerificationContext<'_, '_>,
) -> Result<(String, String, Option<i64>), WorkspaceHistoryDiagnostic> {
    let closure_store =
        FinalCandidateClosureStore::open(&subject.repository_path, &subject.work_area_root)
            .map_err(|_| {
                acceptance_failed(
                    &job.job_id,
                    "FinalCandidateClosureStore work area failed canonical ownership validation.",
                )
            })?;
    let closure_ledger = closure_store.inspect_ledger_readonly().map_err(|_| {
        acceptance_failed(
            &job.job_id,
            "FinalCandidateClosureLedger is missing or failed read-only validation.",
        )
    })?;

    let mut seen_attempt_ids = BTreeSet::new();
    let mut seen_command_keys = BTreeSet::new();
    let mut seen_verdict_ids = BTreeSet::new();
    let mut matching_accepted = Vec::new();

    for attempt in &closure_ledger.attempts {
        if !seen_attempt_ids.insert(attempt.attempt_id.as_str())
            || !seen_command_keys.insert(attempt.command_key.as_str())
        {
            return Err(acceptance_failed(
                &job.job_id,
                "FinalCandidateClosureLedger contains duplicate attempt_id or command_key entries.",
            ));
        }
        if let Some(ref v) = attempt.verdict {
            if !seen_verdict_ids.insert(v.verdict_id.as_str()) {
                return Err(acceptance_failed(
                    &job.job_id,
                    "FinalCandidateClosureLedger contains duplicate verdict_id entries.",
                ));
            }
        }
        if attempt.state != ClosureAttemptState::Terminal
            && (attempt.verdict.is_some() || attempt.terminal_failure.is_some())
        {
            return Err(acceptance_failed(
                &job.job_id,
                "FinalCandidateClosureLedger has an inconsistent non-terminal attempt record.",
            ));
        }
        if attempt.subject_digest == subject.subject_digest {
            if attempt.state != ClosureAttemptState::Terminal {
                return Err(acceptance_failed(
                    &job.job_id,
                    "FinalCandidateClosureLedger has an unresolved attempt for the final candidate subject.",
                ));
            }
            if attempt
                .verdict
                .as_ref()
                .is_some_and(|v| v.kind == ClosureVerdictKind::Accepted)
            {
                matching_accepted.push(attempt);
            }
        } else if rt.closure_artifacts.contains_key(&attempt.attempt_id)
            && attempt
                .verdict
                .as_ref()
                .is_some_and(|v| v.kind == ClosureVerdictKind::Accepted)
        {
            return Err(acceptance_failed(
                &job.job_id,
                "Multiple accepted Closure attempts exist across job closure artifacts.",
            ));
        }
    }

    if matching_accepted.len() != 1 {
        return Err(acceptance_failed(
            &job.job_id,
            "FinalCandidateClosureLedger must contain exactly one terminal Accepted attempt for subject.",
        ));
    }

    let selected_attempt = matching_accepted[0];
    let Some(ref verdict) = selected_attempt.verdict else {
        return Err(acceptance_failed(
            &job.job_id,
            "Selected Closure attempt is missing its ClosureVerdict.",
        ));
    };

    if rt.closure_attempt_id.as_deref() != Some(selected_attempt.attempt_id.as_str())
        || comb.closure_reservation_id.as_deref() != Some(selected_attempt.attempt_id.as_str())
        || rt.closure_verdict_id.as_deref() != Some(verdict.verdict_id.as_str())
        || selected_attempt.approved_base_commit != subject.approved_base_commit
        || selected_attempt.candidate_head_commit != subject.candidate_head_commit
        || selected_attempt.candidate_tree_sha != subject.candidate_tree_sha
        || selected_attempt.policy_digest != subject.policy.policy_digest
        || selected_attempt.scope_revision != subject.policy.scope_revision
        || selected_attempt.registry_revision != subject.policy.registry_revision
        || selected_attempt.uncertainty_reason.is_some()
        || selected_attempt.terminal_failure.is_some()
        || verdict.attempt_id != selected_attempt.attempt_id
        || verdict.kind != ClosureVerdictKind::Accepted
        || verdict.execution_evidence.producer_run_id != subject.run_id
    {
        return Err(acceptance_failed(
            &job.job_id,
            "Selected Closure attempt/verdict pointers or bound revisions do not match job and subject.",
        ));
    }

    verdict.verify_against_subject(subject).map_err(|_| {
        acceptance_failed(
            &job.job_id,
            "ClosureVerdict failed cryptographic digest or FinalCandidateSubject correlation.",
        )
    })?;

    for art in rt.closure_artifacts.values() {
        if (art.attempt_id == selected_attempt.attempt_id
            || art.subject_digest == subject.subject_digest)
            && (art.fenced_from_promotion || art.response_error.is_some())
        {
            return Err(acceptance_failed(
                &job.job_id,
                "ClosureExecutionArtifactRecord is fenced from promotion or carries a response error.",
            ));
        }
    }

    if !rt.closure_artifacts.is_empty()
        && !rt
            .closure_artifacts
            .contains_key(&selected_attempt.attempt_id)
    {
        return Err(acceptance_failed(
            &job.job_id,
            "Controller closure_artifacts is missing the selected Closure attempt record.",
        ));
    }

    if let Some(art_rec) = rt.closure_artifacts.get(&selected_attempt.attempt_id) {
        if art_rec.attempt_id != selected_attempt.attempt_id
            || art_rec.command_key != selected_attempt.command_key
            || art_rec.subject_digest != subject.subject_digest
            || art_rec.candidate_tree_sha != subject.candidate_tree_sha
            || art_rec.fenced_from_promotion
            || art_rec.response_error.is_some()
        {
            return Err(acceptance_failed(
                &job.job_id,
                "ClosureExecutionArtifactRecord metadata conflicts with selected Closure attempt.",
            ));
        }
        let Some(pstore) = store_ctx.policy_store else {
            return Err(acceptance_failed(
                &job.job_id,
                "ControllerPolicyStore unavailable to verify ClosureExecutionArtifactRecord.",
            ));
        };
        let loaded = pstore
            .load_closure_execution_artifact_for_record(
                &chat_identity.binding_id,
                &chat_identity.thread_uuid,
                &job.job_id,
                art_rec,
            )
            .map_err(|_| {
                acceptance_failed(
                    &job.job_id,
                    "ClosureExecutionArtifactRecord sidecar/inline payload failed read-only verification.",
                )
            })?;
        if loaded.fenced_from_promotion
            || loaded.response_error.is_some()
            || loaded.execution_report.status != WorkerRunStatus::Succeeded
            || loaded
                .advisory_response
                .as_ref()
                .map(|r| r.sha256_digest.as_str())
                != Some(
                    verdict
                        .execution_evidence
                        .transcript_or_event_digest
                        .as_str(),
                )
        {
            return Err(acceptance_failed(
                &job.job_id,
                "Loaded Closure execution artifact status or response digest does not match ClosureVerdict.",
            ));
        }
    }

    let verified_at_ms = (verdict.recorded_at_ms > 0).then_some(verdict.recorded_at_ms);
    Ok((
        verdict.verdict_id.clone(),
        verdict.summary.clone(),
        verified_at_ms,
    ))
}

fn verify_task_review_and_integration_chain(
    job: &CoworkerJobRecord,
    linkage: &crate::coworker_workflow::FrozenApprovalLinkage,
    rt: &ControllerJobRuntimeState,
    comb: &CombinedCandidateIdentity,
    subject: &FinalCandidateSubject,
    plans_dir: &Path,
) -> Result<BTreeMap<String, VerifiedHistoricalTaskAcceptance>, WorkspaceHistoryDiagnostic> {
    if comb.accepted_predecessor_receipts.len() != job.tasks.len()
        || subject.predecessors.len() != job.tasks.len()
    {
        return Err(acceptance_failed(
            &job.job_id,
            "Predecessor count in CombinedCandidateIdentity or FinalCandidateSubject does not match job.tasks.",
        ));
    }

    let mut comb_pred_tasks = BTreeSet::new();
    let mut comb_pred_cands = BTreeSet::new();
    let mut comb_pred_revs = BTreeSet::new();
    let mut comb_pred_ints = BTreeSet::new();
    for pred in &comb.accepted_predecessor_receipts {
        if !comb_pred_tasks.insert(pred.task_id.as_str())
            || !comb_pred_cands.insert(pred.candidate_id.as_str())
            || !comb_pred_revs.insert(pred.review_receipt_id.as_str())
            || !comb_pred_ints.insert(pred.integration_receipt_id.as_str())
        {
            return Err(acceptance_failed(
                &job.job_id,
                "CombinedCandidateIdentity contains duplicate predecessor task/candidate/receipt references.",
            ));
        }
    }

    let mut subj_pred_revs = BTreeSet::new();
    let mut subj_pred_ints = BTreeSet::new();
    for pred in &subject.predecessors {
        if !subj_pred_revs.insert(pred.review_receipt_id.as_str())
            || !subj_pred_ints.insert(pred.integration_receipt_id.as_str())
        {
            return Err(acceptance_failed(
                &job.job_id,
                "FinalCandidateSubject contains duplicate predecessor receipt references.",
            ));
        }
    }

    let confirmed_agg_effects: Vec<&PendingControllerEffect> = rt
        .pending_effects
        .values()
        .filter(|e| {
            e.kind == PendingControllerEffectKind::JobAggregation
                && e.status == PendingControllerEffectStatus::EffectConfirmed
        })
        .collect();
    if confirmed_agg_effects.len() > 1 {
        return Err(acceptance_failed(
            &job.job_id,
            "Controller runtime contains multiple confirmed JobAggregation effects.",
        ));
    }

    let disk_intent = inspect_job_integration_intent_readonly(
        &subject.work_area_root,
        &subject.integration_worktree_id,
    )
    .map_err(|_| {
        acceptance_failed(
            &job.job_id,
            "Job integration intent failed read-only inspection.",
        )
    })?;
    if let Some(ref intent) = disk_intent {
        intent.verify_integrity().map_err(|_| {
            acceptance_failed(
                &job.job_id,
                "On-disk JobIntegrationIntent failed digest integrity verification.",
            )
        })?;
        if intent.state != JobIntegrationIntentState::Completed || confirmed_agg_effects.is_empty()
        {
            return Err(acceptance_failed(
                &job.job_id,
                "On-disk JobIntegrationIntent is incomplete or unassociated with a confirmed JobAggregation effect.",
            ));
        }
    }

    let verified_agg_report = if let Some(agg_eff) = confirmed_agg_effects.first() {
        let report = read_persisted_job_integration_report(plans_dir, &agg_eff.effect_id)
            .map_err(|_| {
                acceptance_failed(
                    &job.job_id,
                    "Persisted CoworkerJobIntegrationReport failed read-only validation.",
                )
            })?
            .ok_or_else(|| {
                acceptance_failed(
                    &job.job_id,
                    "Confirmed JobAggregation effect is missing its persisted CoworkerJobIntegrationReport.",
                )
            })?;
        if disk_intent.as_ref() != Some(&report.intent)
            || report.job_scope.validate().is_err()
            || report.intent.job_scope != report.job_scope
            || report.job_scope.job_id != job.job_id
            || report.job_scope.approval_id != linkage.approval_id
            || report.job_scope.approval_subject_digest != linkage.approval_subject_digest
            || report.job_scope.target_integration_run_id != subject.run_id
            || report.job_scope.aggregate_allowed_write_paths
                != subject.aggregate_allowed_write_paths
            || report.repository_path != subject.repository_path
            || report.work_area_root != subject.work_area_root
            || report.integration_worktree_id != subject.integration_worktree_id
            || report.approved_base_commit != subject.approved_base_commit
            || report.final_integrated_target_commit != subject.candidate_head_commit
            || report.final_integrated_tree_sha != subject.candidate_tree_sha
            || report.receipts.len() != job.tasks.len()
        {
            return Err(acceptance_failed(
                &job.job_id,
                "Persisted CoworkerJobIntegrationReport does not match job, approval, or final candidate subject.",
            ));
        }
        Some(report)
    } else {
        None
    };

    let mut verified_tasks = BTreeMap::new();
    let mut seen_verdict_ids = BTreeSet::new();
    let mut seen_review_receipt_ids = BTreeSet::new();
    let mut seen_orig_int_ids = BTreeSet::new();
    let mut any_single_run_reached_final_head = false;

    for task in &job.tasks {
        if task.stage != CoworkerTaskStage::Integrated || task.current_attempt_no == 0 {
            return Err(acceptance_failed(
                &job.job_id,
                "All tasks in an accepted Completed job must be in the Integrated stage.",
            ));
        }
        let Some(ref accepted_cand_id) = task.accepted_candidate_id else {
            return Err(acceptance_failed(
                &job.job_id,
                "Integrated task is missing accepted_candidate_id.",
            ));
        };

        let mut task_cand_ids = BTreeSet::new();
        for c in &task.candidates {
            if !task_cand_ids.insert(c.candidate_id.as_str()) {
                return Err(acceptance_failed(
                    &job.job_id,
                    "Task contains duplicate candidate_id entries.",
                ));
            }
        }

        let Some(cand) = task
            .candidates
            .iter()
            .find(|c| &c.candidate_id == accepted_cand_id)
        else {
            return Err(acceptance_failed(
                &job.job_id,
                "Task accepted_candidate_id was not found in task.candidates.",
            ));
        };
        if cand.task_id != task.task_id || cand.attempt_no != task.current_attempt_no {
            return Err(acceptance_failed(
                &job.job_id,
                "Accepted task candidate does not match task_id or current_attempt_no.",
            ));
        }

        let (
            Some(ref structured_verdict),
            Some(ref review_receipt_id),
            Some(ref orig_int_receipt_id),
            Some(ref cand_rev_subj_digest),
        ) = (
            &cand.review_verdict,
            &cand.review_receipt_id,
            &cand.integration_receipt_id,
            &cand.review_subject_digest,
        )
        else {
            return Err(acceptance_failed(
                &job.job_id,
                "Accepted task candidate is missing review_verdict, review_subject_digest, or receipt IDs.",
            ));
        };

        if !structured_verdict.allows_acceptance()
            || structured_verdict.validate().is_err()
            || structured_verdict.task_id != task.task_id
            || structured_verdict.attempt_no != cand.attempt_no
            || structured_verdict.candidate_id != cand.candidate_id
            || &structured_verdict.review_subject_digest != cand_rev_subj_digest
            || structured_verdict.base_commit != cand.base_commit
            || structured_verdict.task_head_commit != cand.head_commit
            || structured_verdict.task_tree_sha != cand.tree_sha
            || structured_verdict.diff_digest != cand.diff_digest
            || structured_verdict.policy_digest != linkage.policy_digest
        {
            return Err(acceptance_failed(
                &job.job_id,
                "Task StructuredReviewVerdict is not a valid candidate-bound Accept verdict.",
            ));
        }

        if !seen_verdict_ids.insert(structured_verdict.verdict_id.as_str())
            || !seen_review_receipt_ids.insert(review_receipt_id.as_str())
            || !seen_orig_int_ids.insert(orig_int_receipt_id.as_str())
        {
            return Err(acceptance_failed(
                &job.job_id,
                "Duplicate review verdict or receipt reference detected across tasks.",
            ));
        }

        let Some(coder_att) = task
            .attempts
            .iter()
            .find(|a| a.attempt_no == cand.attempt_no && a.role == WorkflowRole::Coder)
        else {
            return Err(acceptance_failed(
                &job.job_id,
                "Accepted task candidate has no matching Coder TaskAttemptRef.",
            ));
        };
        if coder_att.base_commit != cand.base_commit
            || coder_att.head_commit.as_deref() != Some(cand.head_commit.as_str())
            || coder_att.tree_sha.as_deref() != Some(cand.tree_sha.as_str())
        {
            return Err(acceptance_failed(
                &job.job_id,
                "Coder TaskAttemptRef commit/tree pointers conflict with accepted TaskCandidateRef.",
            ));
        }

        let Some(comb_pred) = comb
            .accepted_predecessor_receipts
            .iter()
            .find(|p| p.task_id == task.task_id)
        else {
            return Err(acceptance_failed(
                &job.job_id,
                "Task is missing from CombinedCandidateIdentity.accepted_predecessor_receipts.",
            ));
        };
        if comb_pred.candidate_id != cand.candidate_id
            || comb_pred.head_commit != cand.head_commit
            || comb_pred.tree_sha != cand.tree_sha
            || &comb_pred.review_subject_digest != cand_rev_subj_digest
            || &comb_pred.review_receipt_id != review_receipt_id
            || &comb_pred.integration_receipt_id != orig_int_receipt_id
        {
            return Err(acceptance_failed(
                &job.job_id,
                "CombinedCandidateIdentity predecessor reference drifted from accepted task candidate.",
            ));
        }

        let disk_review = load_review_receipt(&subject.work_area_root, review_receipt_id)
            .and_then(|r| {
                r.verify_integrity()?;
                Ok(r)
            })
            .map_err(|_| {
                acceptance_failed(
                    &job.job_id,
                    "On-disk ReviewReceipt is missing or failed cryptographic integrity verification.",
                )
            })?;

        let coder_provider = adapter_to_worker_provider(&job.job_id, task.selected_choice.adapter)?;
        let resolved_coder = resolve_model_target(
            coder_provider,
            &task.selected_choice.model_arg,
            task.selected_choice.effort.as_deref(),
        )
        .map_err(|_| {
            acceptance_failed(
                &job.job_id,
                "Frozen task Coder model target failed validation.",
            )
        })?;
        let expected_task_paths: BTreeSet<String> = task.write_paths.iter().cloned().collect();

        if &disk_review.receipt_id != review_receipt_id
            || &disk_review.subject_digest != cand_rev_subj_digest
            || disk_review.repository_path != subject.repository_path
            || disk_review.work_area_root != subject.work_area_root
            || disk_review.base_commit != cand.base_commit
            || disk_review.task_head_commit != cand.head_commit
            || disk_review.task_tree_sha != cand.tree_sha
            || disk_review.diff_digest != cand.diff_digest
            || disk_review.allowed_write_paths != expected_task_paths
            || disk_review.policy.validate(true).is_err()
            || disk_review.policy.policy_id != linkage.policy_id
            || disk_review.policy.requested_provider.as_deref() != Some(coder_provider.as_str())
            || disk_review.policy.requested_model.as_deref()
                != Some(resolved_coder.base_model.as_str())
            || disk_review.policy.requested_effort != resolved_coder.effort
            || disk_review.policy.require_effort_evidence != resolved_coder.effort.is_some()
            || !disk_review.policy.require_passing_tests
            || disk_review.test_evidence.validate(true).is_err()
            || !disk_review.test_evidence.all_checks_passed()
            || disk_review.test_evidence.failed_test_count != 0
        {
            return Err(acceptance_failed(
                &job.job_id,
                "On-disk ReviewReceipt commits, write scope, test evidence, or frozen policy drifted from task.",
            ));
        }

        let disk_orig_int = load_integration_receipt(&subject.work_area_root, orig_int_receipt_id)
            .and_then(|r| {
                r.verify_integrity()?;
                Ok(r)
            })
            .map_err(|_| {
                acceptance_failed(
                    &job.job_id,
                    "On-disk original IntegrationReceipt is missing or failed integrity verification.",
                )
            })?;

        if &disk_orig_int.receipt_id != orig_int_receipt_id
            || disk_orig_int.job_aggregation.is_some()
            || disk_orig_int.repository_path != disk_review.repository_path
            || disk_orig_int.work_area_root != disk_review.work_area_root
            || disk_orig_int.run_id != disk_review.run_id
            || disk_orig_int.task_id != disk_review.task_id
            || disk_orig_int.task_worktree_id != disk_review.task_worktree_id
            || disk_orig_int.review_receipt_id != disk_review.receipt_id
            || disk_orig_int.review_receipt_digest != disk_review.receipt_digest
            || disk_orig_int.review_subject_digest != disk_review.subject_digest
            || disk_orig_int.base_commit != cand.base_commit
            || disk_orig_int.accepted_task_head_commit != cand.head_commit
        {
            return Err(acceptance_failed(
                &job.job_id,
                "On-disk original IntegrationReceipt does not match ReviewReceipt or accepted task commit.",
            ));
        }

        if !rt.attempt_artifacts.is_empty()
            && rt
                .get_attempt_artifacts(&task.task_id, cand.attempt_no)
                .is_none()
        {
            return Err(acceptance_failed(
                &job.job_id,
                "Controller attempt_artifacts is missing artifacts for the accepted task attempt.",
            ));
        }
        if let Some(art) = rt.get_attempt_artifacts(&task.task_id, cand.attempt_no) {
            if art.committed_head_commit.as_deref() != Some(cand.head_commit.as_str())
                || art.committed_tree_sha.as_deref() != Some(cand.tree_sha.as_str())
                || art.task_worktree_id != disk_review.task_worktree_id
                || art.check_evidence.is_empty()
                || art
                    .check_evidence
                    .iter()
                    .any(|c| !c.passed || c.exit_code != 0)
                || art
                    .review_receipt
                    .as_ref()
                    .is_some_and(|r| r != &disk_review)
                || art
                    .integration_receipt
                    .as_ref()
                    .is_some_and(|ir| ir != &disk_orig_int)
                || art.review_subject.as_ref().is_some_and(|rs| {
                    rs.verify_integrity().is_err()
                        || rs.subject_digest != disk_review.subject_digest
                })
            {
                return Err(acceptance_failed(
                    &job.job_id,
                    "TaskAttemptExecutionArtifacts conflicts with verified ReviewReceipt/IntegrationReceipt.",
                ));
            }
        }

        let Some(subj_pred) = subject
            .predecessors
            .iter()
            .find(|p| p.review_receipt_id == disk_review.receipt_id)
        else {
            return Err(acceptance_failed(
                &job.job_id,
                "FinalCandidateSubject.predecessors is missing verified task ReviewReceipt.",
            ));
        };

        if subj_pred.task_digest != task.task_digest
            || subj_pred.task_worktree_id != disk_review.task_worktree_id
            || subj_pred.review_receipt_digest != disk_review.receipt_digest
            || subj_pred.review_subject_digest != disk_review.subject_digest
            || subj_pred.accepted_task_head_commit != cand.head_commit
            || subj_pred.accepted_task_tree_sha != cand.tree_sha
            || subj_pred.allowed_write_paths != expected_task_paths
        {
            return Err(acceptance_failed(
                &job.job_id,
                "FinalCandidateSubject predecessor evidence drifted from task ReviewReceipt.",
            ));
        }

        let effective_int_receipt_id = if let Some(ref agg_report) = verified_agg_report {
            if subj_pred.task_id != task.task_id {
                return Err(acceptance_failed(
                    &job.job_id,
                    "Aggregated FinalCandidateSubject predecessor task_id does not match logical task_id.",
                ));
            }
            let disk_agg_int =
                load_integration_receipt(&subject.work_area_root, &subj_pred.integration_receipt_id)
                    .and_then(|r| {
                        r.verify_integrity()?;
                        Ok(r)
                    })
                    .map_err(|_| {
                        acceptance_failed(
                            &job.job_id,
                            "On-disk aggregated IntegrationReceipt is missing or failed integrity verification.",
                        )
                    })?;
            let Some(ref agg_prov) = disk_agg_int.job_aggregation else {
                return Err(acceptance_failed(
                    &job.job_id,
                    "Aggregated IntegrationReceipt is missing JobAggregationProvenance.",
                ));
            };
            if !agg_report.receipts.iter().any(|r| r == &disk_agg_int)
                || disk_agg_int.integration_worktree_id != subject.integration_worktree_id
                || disk_agg_int.review_receipt_id != disk_review.receipt_id
                || disk_agg_int.review_receipt_digest != disk_review.receipt_digest
                || disk_agg_int.accepted_task_head_commit != cand.head_commit
                || subj_pred.integration_receipt_digest != disk_agg_int.receipt_digest
                || agg_prov.job_id != job.job_id
                || agg_prov.approval_id != linkage.approval_id
                || agg_prov.approval_subject_digest != linkage.approval_subject_digest
                || agg_prov.logical_task_id != task.task_id
                || agg_prov.task_digest != task.task_digest
                || agg_prov.accepted_task_tree_sha != cand.tree_sha
                || agg_prov.accepted_task_diff_digest != cand.diff_digest
            {
                return Err(acceptance_failed(
                    &job.job_id,
                    "Aggregated IntegrationReceipt provenance does not match job, approval, or ReviewReceipt.",
                ));
            }
            disk_agg_int.receipt_id
        } else {
            if subj_pred.task_id != disk_review.task_id
                || subj_pred.integration_receipt_id != disk_orig_int.receipt_id
                || subj_pred.integration_receipt_digest != disk_orig_int.receipt_digest
                || disk_orig_int.integration_worktree_id != subject.integration_worktree_id
            {
                return Err(acceptance_failed(
                    &job.job_id,
                    "Single-run FinalCandidateSubject predecessor does not match original IntegrationReceipt.",
                ));
            }
            if disk_orig_int.integrated_target_commit == subject.candidate_head_commit
                && disk_orig_int.integrated_tree_sha == subject.candidate_tree_sha
            {
                any_single_run_reached_final_head = true;
            }
            disk_orig_int.receipt_id
        };

        let task_verified_ts =
            (disk_review.approved_at_ms > 0).then_some(disk_review.approved_at_ms);
        verified_tasks.insert(
            task.task_id.clone(),
            VerifiedHistoricalTaskAcceptance {
                accepted_candidate_id: cand.candidate_id.clone(),
                review_verdict_id: structured_verdict.verdict_id.clone(),
                review_receipt_id: disk_review.receipt_id,
                integration_receipt_id: effective_int_receipt_id,
                verified_at_ms: task_verified_ts,
            },
        );
    }

    if verified_agg_report.is_none() && !any_single_run_reached_final_head {
        return Err(acceptance_failed(
            &job.job_id,
            "No verified single-run IntegrationReceipt produced the FinalCandidateSubject head/tree.",
        ));
    }

    Ok(verified_tasks)
}
