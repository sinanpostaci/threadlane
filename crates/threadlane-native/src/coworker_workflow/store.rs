//! Private per-binding Coworker workflow ledger store and compare-and-swap transaction helpers.
//!
//! Reuses [`crate::fs_safety`] private (`0700`/`0600`) atomic file persistence and cross-process
//! locking (`fs2`). Preserves corrupt or unsupported-schema ledger files untouched on disk without
//! silent migration or data loss.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use threadlane_authority::validation::validate_canonical_thread_uuid;
use threadlane_authority::{AuthorityError, AuthorityStore};
use threadlane_workers::{validate_exact_40_hex_sha, validate_sha256_hex_digest};
use threadlane_workflow::{Difficulty, ScopedProfileDependencyProof, WorkflowRole};

use crate::coworker_workflow::contracts::{
    deterministic_prefixed_id, prove_task_independence, validate_bounded_text,
    validate_non_native_worker_choice, validate_unique_id_list, validate_unique_text_list,
    AcceptedPredecessorReceiptRef, ArchitectAdviceRecord, ArchitectContextDependencies,
    ArchitectDecisionStatus, CandidateCheckEvidence, CanonicalProjectBaseRef,
    CombinedCandidateIdentity, CombinedCandidateStatus, CoworkerJobPhase, CoworkerJobRecord,
    CoworkerTaskRecord, CoworkerTaskStage, FrozenApprovalLinkage,
    FrozenControllerCapabilityEnvelope, OutboxDispatchKind, OutboxDispatchRecord,
    OutboxDispatchStatus, PlanningAdviceGrant, PlanningCallBudget, PlanningCallReservation,
    PlanningGrantStatus, PlanningGrantUsage, PlanningReadOnlyCapabilities,
    PlanningReservationStatus, PlanningResultRef, ReviewVerdictOutcome, StructuredReviewVerdict,
    TaskAttemptRef, TaskCandidateRef, TaskInterfaceOwnershipSpec, WorkBrief, WorkBriefKind,
    WorkflowHoldReason, WorkflowHoldRecord, COWORKER_WORKFLOW_SCHEMA_VERSION,
    MAX_ARCHITECT_ADVICE_RECORDS_PER_BINDING, MAX_ATTEMPTS_PER_TASK, MAX_CANDIDATES_PER_TASK,
    MAX_COMBINED_CANDIDATES_PER_JOB, MAX_COWORKER_ID_LEN, MAX_COWORKER_LEDGER_BYTES,
    MAX_GRANT_RESERVATIONS, MAX_HOLDS_PER_JOB, MAX_JOBS_PER_BINDING, MAX_OUTBOX_RECORDS_PER_JOB,
    MAX_PLANNING_GRANTS_PER_BINDING, MAX_WORK_BRIEF_ITEMS,
};
use crate::coworker_workflow::controller_policy::{
    CheckFailureCorrectionBasis, ControllerPolicyStore, PendingControllerEffectKind,
    PendingControllerEffectStatus, MAX_IN_SCOPE_CORRECTION_ATTEMPTS,
};
use crate::coworker_workflow::task_verification::compute_task_candidate_id;
use crate::error::{NativeError, NativeErrorCode};
use crate::fs_safety::{
    acquire_cross_process_lock, constant_time_str_eq, ensure_platform_supported,
    read_private_file_bounded, sanitize_diagnostic_text, validate_absolute_no_symlink_components,
    validate_bounded_identifier, write_private_file_atomic,
};
use crate::profile_flow::{
    preflight, ExecutionPolicy, PreflightRequest, RolePreflightRequest, SelectedRoleChoice,
};

/// Persisted per-binding Coworker workflow ledger.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CoworkerBindingLedger {
    /// Wire and storage schema version (`1`).
    pub schema_version: u32,
    /// Control-plane chat binding identifier.
    pub binding_id: String,
    /// Pinned canonical native Codex thread UUID for this binding (`None` when revision is `0`).
    pub thread_uuid: Option<String>,
    /// Monotonically increasing ledger revision (`0` when uninitialized).
    pub ledger_revision: u64,
    /// Scoped read-only planning and architectural advice grants for this binding.
    #[serde(default)]
    pub planning_grants: Vec<PlanningAdviceGrant>,
    /// Dependency-bound architectural advice records for this binding.
    #[serde(default)]
    pub architect_advice: Vec<ArchitectAdviceRecord>,
    /// Monotonically versioned Coworker workflow jobs for this binding.
    #[serde(default)]
    pub jobs: Vec<CoworkerJobRecord>,
    /// Immutable automatic admission subjects, written atomically with approval linkage.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub admission_markers: BTreeMap<String, crate::coworker_admission::AdmissionMarker>,
}

impl CoworkerBindingLedger {
    /// Constructs an empty revision-0 [`CoworkerBindingLedger`] for `binding_id`.
    pub fn empty(binding_id: impl Into<String>) -> Result<Self, NativeError> {
        let binding_id = binding_id.into();
        validate_bounded_identifier(&binding_id, "binding_id", MAX_COWORKER_ID_LEN)?;
        Ok(Self {
            schema_version: COWORKER_WORKFLOW_SCHEMA_VERSION,
            binding_id,
            thread_uuid: None,
            ledger_revision: 0,
            planning_grants: Vec::new(),
            architect_advice: Vec::new(),
            jobs: Vec::new(),
            admission_markers: BTreeMap::new(),
        })
    }

    /// Validates the entire ledger against `expected_binding_id`, schema version, capacity limits,
    /// thread continuity, and internal referential integrity.
    pub fn validate(&self, expected_binding_id: &str) -> Result<(), NativeError> {
        if self.schema_version != COWORKER_WORKFLOW_SCHEMA_VERSION {
            return Err(NativeError::new(
                NativeErrorCode::ProtocolError,
                format!(
                    "unsupported coworker workflow ledger schema_version {} (expected {COWORKER_WORKFLOW_SCHEMA_VERSION})",
                    self.schema_version
                ),
            ));
        }
        validate_bounded_identifier(&self.binding_id, "ledger.binding_id", MAX_COWORKER_ID_LEN)?;
        if self.binding_id != expected_binding_id {
            return Err(NativeError::new(
                NativeErrorCode::ProtocolError,
                "coworker workflow ledger binding_id does not match expected binding file",
            ));
        }
        if let Some(ref tid) = self.thread_uuid {
            validate_canonical_thread_uuid("ledger.thread_uuid", tid).map_err(|_| {
                NativeError::new(
                    NativeErrorCode::ProtocolError,
                    "ledger.thread_uuid is not a canonical lowercase hyphenated non-nil UUID",
                )
            })?;
        }

        if self.admission_markers.len() > MAX_JOBS_PER_BINDING {
            return Err(NativeError::new(
                NativeErrorCode::ProtocolError,
                "Too many admission markers",
            ));
        }
        for (job_id, marker) in &self.admission_markers {
            let job = self
                .jobs
                .iter()
                .find(|j| &j.job_id == job_id)
                .ok_or_else(|| {
                    NativeError::new(
                        NativeErrorCode::ProtocolError,
                        "Admission marker has no job",
                    )
                })?;
            marker.validate_for_job(job)?;
        }
        if self.ledger_revision == 0 {
            if self.thread_uuid.is_some()
                || !self.planning_grants.is_empty()
                || !self.architect_advice.is_empty()
                || !self.jobs.is_empty()
                || !self.admission_markers.is_empty()
            {
                return Err(NativeError::new(
                    NativeErrorCode::ProtocolError,
                    "revision-0 coworker workflow ledger must not contain grants, advice, or jobs",
                ));
            }
            return Ok(());
        }

        let pinned_thread = self.thread_uuid.as_deref().ok_or_else(|| {
            NativeError::new(
                NativeErrorCode::ProtocolError,
                "non-empty coworker workflow ledger must pin a canonical thread_uuid",
            )
        })?;

        if self.planning_grants.len() > MAX_PLANNING_GRANTS_PER_BINDING
            || self.architect_advice.len() > MAX_ARCHITECT_ADVICE_RECORDS_PER_BINDING
            || self.jobs.len() > MAX_JOBS_PER_BINDING
        {
            return Err(NativeError::new(
                NativeErrorCode::ProtocolError,
                "coworker workflow ledger collection exceeds maximum capacity",
            ));
        }

        let mut grant_ids = BTreeSet::new();
        let mut grant_keys = BTreeSet::new();
        for grant in &self.planning_grants {
            grant.validate()?;
            if grant.binding_id != expected_binding_id || grant.thread_uuid != pinned_thread {
                return Err(NativeError::new(
                    NativeErrorCode::ProtocolError,
                    "PlanningAdviceGrant binding_id or thread_uuid does not match ledger scope",
                ));
            }
            if !grant_ids.insert(grant.grant_id.as_str())
                || !grant_keys.insert(grant.create_request_key.as_str())
            {
                return Err(NativeError::new(
                    NativeErrorCode::ProtocolError,
                    "duplicate grant_id or create_request_key in planning_grants",
                ));
            }
        }

        let mut advice_ids = BTreeSet::new();
        let mut advice_keys = BTreeSet::new();
        for advice in &self.architect_advice {
            advice.validate()?;
            if advice.binding_id != expected_binding_id || advice.thread_uuid != pinned_thread {
                return Err(NativeError::new(
                    NativeErrorCode::ProtocolError,
                    "ArchitectAdviceRecord binding_id or thread_uuid does not match ledger scope",
                ));
            }
            if !advice_ids.insert(advice.advice_id.as_str())
                || !advice_keys.insert(advice.request_key.as_str())
            {
                return Err(NativeError::new(
                    NativeErrorCode::ProtocolError,
                    "duplicate advice_id or request_key in architect_advice",
                ));
            }
            let grant = self
                .planning_grants
                .iter()
                .find(|g| g.grant_id == advice.grant_id)
                .ok_or_else(|| {
                    NativeError::new(
                        NativeErrorCode::ProtocolError,
                        "ArchitectAdviceRecord references a missing grant_id",
                    )
                })?;
            let reservation = grant
                .reservations
                .iter()
                .find(|r| r.reservation_id == advice.reservation_id)
                .ok_or_else(|| {
                    NativeError::new(
                        NativeErrorCode::ProtocolError,
                        "ArchitectAdviceRecord references a missing reservation_id",
                    )
                })?;
            if reservation.role != WorkflowRole::Architect
                || reservation.status != PlanningReservationStatus::Completed
                || reservation.context_digest != advice.context_digest
            {
                return Err(NativeError::new(
                    NativeErrorCode::ProtocolError,
                    "ArchitectAdviceRecord must bind a completed Architect reservation with matching context_digest",
                ));
            }
            if advice.context_dependencies.project_base.repository_path
                != grant.project_base.repository_path
            {
                return Err(NativeError::new(
                    NativeErrorCode::SecurityViolation,
                    "ArchitectAdviceRecord project_base.repository_path does not match the standing grant's canonical repository_path",
                ));
            }
            if advice.context_dependencies.policy_digest != grant.policy_digest {
                return Err(NativeError::new(
                    NativeErrorCode::ProtocolError,
                    "ArchitectAdviceRecord policy_digest does not match the standing grant's policy_digest",
                ));
            }
            if let Some(ref call_base) = reservation.call_project_base {
                if call_base != &advice.context_dependencies.project_base {
                    return Err(NativeError::new(
                        NativeErrorCode::ProtocolError,
                        "ArchitectAdviceRecord project_base does not match the reservation's call_project_base",
                    ));
                }
            }
        }

        let mut job_ids = BTreeSet::new();
        let mut job_keys = BTreeSet::new();
        let mut planned_plan_owners = BTreeMap::<&str, &str>::new();
        for job in &self.jobs {
            job.validate()?;
            if job.binding_id != expected_binding_id || job.thread_uuid != pinned_thread {
                return Err(NativeError::new(
                    NativeErrorCode::ProtocolError,
                    "CoworkerJobRecord binding_id or thread_uuid does not match ledger scope",
                ));
            }
            if !job_ids.insert(job.job_id.as_str())
                || !job_keys.insert(job.create_request_key.as_str())
            {
                return Err(NativeError::new(
                    NativeErrorCode::ProtocolError,
                    "duplicate job_id or create_request_key in jobs",
                ));
            }
            if let Some(ref pr) = job.planning_result {
                if let Some(existing_owner) =
                    planned_plan_owners.insert(pr.plan_id.as_str(), job.job_id.as_str())
                {
                    if existing_owner != job.job_id {
                        return Err(NativeError::new(
                            NativeErrorCode::ProtocolError,
                            "multiple jobs in the same binding ledger reference the same planned plan_id",
                        ));
                    }
                }
                let grant = self
                    .planning_grants
                    .iter()
                    .find(|g| g.grant_id == pr.grant_id)
                    .ok_or_else(|| {
                        NativeError::new(
                            NativeErrorCode::ProtocolError,
                            "PlanningResultRef references a missing grant_id",
                        )
                    })?;
                if grant.project_base.repository_path != pr.project_base.repository_path {
                    return Err(NativeError::new(
                        NativeErrorCode::SecurityViolation,
                        "PlanningResultRef project_base.repository_path does not match the standing grant's canonical repository_path",
                    ));
                }
                if grant.profile_id != pr.profile_id
                    || grant.registry_revision != pr.registry_revision
                    || grant.policy_digest != pr.policy_digest
                {
                    return Err(NativeError::new(
                        NativeErrorCode::ProtocolError,
                        "PlanningResultRef profile_id, registry_revision, or policy_digest does not match the referenced grant",
                    ));
                }
                let reservation = grant
                    .reservations
                    .iter()
                    .find(|r| r.reservation_id == pr.reservation_id)
                    .ok_or_else(|| {
                        NativeError::new(
                            NativeErrorCode::ProtocolError,
                            "PlanningResultRef references a missing reservation_id on the grant",
                        )
                    })?;
                if reservation.role != WorkflowRole::Orchestrator
                    || reservation.status != PlanningReservationStatus::Completed
                    || reservation.request_key != pr.request_key
                    || reservation.selected_choice != pr.selected_choice
                    || reservation.run_id.as_deref() != Some(pr.run_id.as_str())
                    || reservation.report_artifact_id.as_deref()
                        != Some(pr.report_artifact_id.as_str())
                    || reservation.output_digest.as_deref() != Some(pr.output_digest.as_str())
                {
                    return Err(NativeError::new(
                        NativeErrorCode::ProtocolError,
                        "PlanningResultRef must bind a completed Orchestrator reservation with matching run_id, report_artifact_id, and output_digest",
                    ));
                }
                if let Some(ref call_base) = reservation.call_project_base {
                    if call_base != &pr.project_base {
                        return Err(NativeError::new(
                            NativeErrorCode::ProtocolError,
                            "PlanningResultRef project_base does not match the reservation's call_project_base",
                        ));
                    }
                }
                for adv_id in &pr.architect_advice_ids {
                    if !self.architect_advice.iter().any(|a| &a.advice_id == adv_id) {
                        return Err(NativeError::new(
                            NativeErrorCode::ProtocolError,
                            "PlanningResultRef references an unknown architect_advice_id",
                        ));
                    }
                }
            }
        }
        for job in &self.jobs {
            if let Some(ref linkage) = job.approval_linkage {
                if let Some(&owner_job_id) = planned_plan_owners.get(linkage.plan_id.as_str()) {
                    if owner_job_id != job.job_id {
                        return Err(NativeError::new(
                            NativeErrorCode::ProtocolError,
                            "FrozenApprovalLinkage references a plan_id produced for a different job in this binding",
                        ));
                    }
                }
            }
        }
        Ok(())
    }

    pub(crate) fn ensure_thread_scope(&mut self, thread_uuid: &str) -> Result<(), NativeError> {
        validate_canonical_thread_uuid("thread_uuid", thread_uuid).map_err(|_| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                "thread_uuid must be a canonical lowercase hyphenated non-nil UUID",
            )
        })?;
        match &self.thread_uuid {
            Some(existing) if existing != thread_uuid => Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "thread_uuid does not match the thread pinned to this binding ledger",
            )),
            Some(_) => Ok(()),
            None => {
                self.thread_uuid = Some(thread_uuid.to_string());
                Ok(())
            }
        }
    }
}

/// Outcome returned by a [`CoworkerWorkflowStore::transact_binding_ledger`] closure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CoworkerLedgerMutationOutcome<T> {
    /// Request matched an existing idempotent record; do not increment `ledger_revision` or write to disk.
    IdempotentExisting(T),
    /// Ledger was mutated; validate, check `expected_ledger_revision`, increment `ledger_revision`, and persist atomically.
    Mutated(T),
}

/// Outcome of acquiring a [`PlanningCallReservation`] under the per-binding cross-process lock.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlanningCallReservationAcquisition {
    /// A new [`PlanningReservationStatus::ReservedBeforeEffect`] reservation was durably created by this call.
    FreshlyReserved(PlanningCallReservation),
    /// An existing terminal reservation (`Completed`, `FailedAfterEffect`, or `RefusedBeforeEffect`)
    /// with the same `request_key` and identical parameters already exists on the grant.
    ExistingTerminal(PlanningCallReservation),
}

impl PlanningCallReservationAcquisition {
    /// Returns the underlying [`PlanningCallReservation`] regardless of whether it was newly
    /// reserved or replayed from a terminal record.
    pub fn into_reservation(self) -> PlanningCallReservation {
        match self {
            Self::FreshlyReserved(res) | Self::ExistingTerminal(res) => res,
        }
    }
}

/// Request to create a new read-only [`PlanningAdviceGrant`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreatePlanningGrantRequest {
    /// Control-plane chat binding identifier.
    pub binding_id: String,
    /// Canonical lowercase hyphenated non-nil native Codex thread UUID.
    pub thread_uuid: String,
    /// Idempotent caller request key.
    pub request_key: String,
    /// Optional expected current ledger revision (CAS check).
    pub expected_ledger_revision: Option<u64>,
    /// Canonical project repository path and base commit/tree SHA.
    pub project_base: CanonicalProjectBaseRef,
    /// Pinned workflow profile identifier.
    pub profile_id: String,
    /// Pinned workflow registry revision (`>= 1`).
    pub registry_revision: u64,
    /// Deny-by-default execution policy restricted to `Orchestrator` and/or `Architect`.
    pub policy: ExecutionPolicy,
    /// Strictly read-only capability envelope.
    pub capabilities: PlanningReadOnlyCapabilities,
    /// Finite call and per-call execution budgets.
    pub budget: PlanningCallBudget,
    /// Creation timestamp (ms since Unix epoch).
    pub created_at_ms: u64,
    /// Expiry timestamp (ms since Unix epoch; must be `> created_at_ms`).
    pub expires_at_ms: u64,
    /// Explicit trusted-local human-confirmation attestation (`true` required).
    pub operator_attested: bool,
}

/// Request to durably reserve call budget on a [`PlanningAdviceGrant`] before effect.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReservePlanningCallRequest {
    /// Control-plane chat binding identifier.
    pub binding_id: String,
    /// Canonical lowercase hyphenated non-nil native Codex thread UUID.
    pub thread_uuid: String,
    /// Target [`PlanningAdviceGrant::grant_id`].
    pub grant_id: String,
    /// Idempotent caller request key.
    pub request_key: String,
    /// Optional expected current ledger revision (CAS check).
    pub expected_ledger_revision: Option<u64>,
    /// Target role (`Orchestrator` or `Architect`).
    pub role: WorkflowRole,
    /// Optional difficulty tier used to resolve `selected_choice`.
    pub difficulty: Option<Difficulty>,
    /// Resolved headless worker choice to reserve.
    pub selected_choice: SelectedRoleChoice,
    /// 64-character lowercase hex SHA-256 digest of the call input/context dependencies.
    pub context_digest: String,
    /// Optional canonical project repository and exact immutable base commit/tree SHA verified for
    /// this specific planning/advice call.
    ///
    /// When supplied, `reserve_planning_call` verifies the live repository and enforces that
    /// `repository_path` matches the standing grant's canonical repository while allowing
    /// `base_commit` and `base_tree_sha` to advance across normal commits in that repository
    /// without forcing a new human grant.
    #[serde(default)]
    pub call_project_base: Option<CanonicalProjectBaseRef>,
    /// Current wall-clock timestamp (ms since Unix epoch) used to enforce `expires_at_ms`.
    pub current_time_ms: u64,
}

/// Request to finalize or reconcile a [`PlanningCallReservation`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FinalizePlanningCallRequest {
    /// Control-plane chat binding identifier.
    pub binding_id: String,
    /// Canonical lowercase hyphenated non-nil native Codex thread UUID.
    pub thread_uuid: String,
    /// Target [`PlanningAdviceGrant::grant_id`].
    pub grant_id: String,
    /// Target [`PlanningCallReservation::reservation_id`].
    pub reservation_id: String,
    /// Optional expected current ledger revision (CAS check).
    pub expected_ledger_revision: Option<u64>,
    /// Target reservation status (`Completed`, `EffectUnknown`, or `RefusedBeforeEffect`).
    pub status: PlanningReservationStatus,
    /// Current wall-clock timestamp (ms since Unix epoch).
    pub updated_at_ms: u64,
    /// Optional runtime `run_id` observed.
    pub run_id: Option<String>,
    /// Optional local report artifact identifier.
    pub report_artifact_id: Option<String>,
    /// Optional 64-character lowercase hex SHA-256 digest of the structured output (required for `Completed`).
    pub output_digest: Option<String>,
    /// Optional sanitized diagnostic message.
    pub diagnostic: Option<String>,
}

/// Request to revoke a [`PlanningAdviceGrant`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RevokePlanningGrantRequest {
    /// Control-plane chat binding identifier.
    pub binding_id: String,
    /// Canonical lowercase hyphenated non-nil native Codex thread UUID.
    pub thread_uuid: String,
    /// Target [`PlanningAdviceGrant::grant_id`].
    pub grant_id: String,
    /// Optional expected current ledger revision (CAS check).
    pub expected_ledger_revision: Option<u64>,
    /// Revocation timestamp (ms since Unix epoch).
    pub revoked_at_ms: u64,
    /// Bounded human-readable revocation reason.
    pub reason: String,
}

/// Request to record a dependency-bound [`ArchitectAdviceRecord`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecordArchitectAdviceRequest {
    /// Control-plane chat binding identifier.
    pub binding_id: String,
    /// Canonical lowercase hyphenated non-nil native Codex thread UUID.
    pub thread_uuid: String,
    /// Idempotent caller request key.
    pub request_key: String,
    /// Optional expected current ledger revision (CAS check).
    pub expected_ledger_revision: Option<u64>,
    /// Owning [`PlanningAdviceGrant::grant_id`].
    pub grant_id: String,
    /// Completed [`PlanningCallReservation::reservation_id`] for `WorkflowRole::Architect`.
    pub reservation_id: String,
    /// Real context dependencies bound when the advice was produced.
    pub context_dependencies: ArchitectContextDependencies,
    /// Bounded architectural decision summary.
    pub decision_summary: String,
    /// 64-character lowercase hex SHA-256 digest of the architectural decision payload.
    pub decision_digest: String,
    /// Optional report artifact identifier.
    pub report_artifact_id: Option<String>,
    /// Initial decision status (`Proposed` or `AcceptedIntoBrief`).
    pub status: ArchitectDecisionStatus,
    /// Creation timestamp (ms since Unix epoch).
    pub created_at_ms: u64,
}

/// Request to create a new [`CoworkerJobRecord`] or amend an existing job revision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateCoworkerJobRequest {
    /// Control-plane chat binding identifier.
    pub binding_id: String,
    /// Canonical lowercase hyphenated non-nil native Codex thread UUID.
    pub thread_uuid: String,
    /// Idempotent caller request key.
    pub request_key: String,
    /// Optional expected current ledger revision (CAS check).
    pub expected_ledger_revision: Option<u64>,
    /// Structured [`WorkBrief`] (`NewJob` or `Amendment`).
    pub work_brief: WorkBrief,
    /// When `work_brief.kind` is `Amendment`, optional list of task IDs on the target job directly affected by the amendment.
    #[serde(default)]
    pub amended_target_task_ids: Vec<String>,
    /// When `work_brief.kind` is `Amendment`, optional list of write paths on the target job affected by the amendment.
    #[serde(default)]
    pub amended_write_paths: Vec<String>,
    /// When `work_brief.kind` is `Amendment`, optional list of interfaces on the target job affected by the amendment.
    #[serde(default)]
    pub amended_interfaces: Vec<String>,
}

/// Request to record a verified [`PlanningResultRef`] on a [`CoworkerJobRecord`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecordJobPlanningResultRequest {
    /// Control-plane chat binding identifier.
    pub binding_id: String,
    /// Canonical lowercase hyphenated non-nil native Codex thread UUID.
    pub thread_uuid: String,
    /// Target [`CoworkerJobRecord::job_id`].
    pub job_id: String,
    /// Optional expected current `job_revision` of `job_id` (`>= 1`).
    pub expected_job_revision: Option<u64>,
    /// Optional expected current ledger revision (CAS check).
    pub expected_ledger_revision: Option<u64>,
    /// Durable planning linkage tying the published plan revision to the verified Orchestrator call.
    pub planning_result: PlanningResultRef,
}

/// Request to bind an immutable operator [`ApprovalRecord`] and [`ExecutionPolicy`] to a [`CoworkerJobRecord`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BindJobApprovalRequest {
    /// Control-plane chat binding identifier.
    pub binding_id: String,
    /// Canonical lowercase hyphenated non-nil native Codex thread UUID.
    pub thread_uuid: String,
    /// Target [`CoworkerJobRecord::job_id`].
    pub job_id: String,
    /// Expected current `job_revision` of `job_id` (`>= 1`).
    pub expected_job_revision: u64,
    /// Optional expected current ledger revision (CAS check).
    pub expected_ledger_revision: Option<u64>,
    /// Persisted operator approval identifier (`apr_<32hex>`) in [`AuthorityStore`].
    pub approval_id: String,
    /// Governing [`ExecutionPolicy`] to freeze into the job.
    pub policy: ExecutionPolicy,
    /// Optional explicit frozen controller capability envelope covering Reviewer, Closure,
    /// in-scope corrections, verification check commands, and local Git integration.
    ///
    /// Defaults to `None` so existing coder-only approvals are not retrofitted and
    /// `policy_digest` or a saved profile alone never implies controller authority.
    #[serde(default)]
    pub controller_envelope: Option<FrozenControllerCapabilityEnvelope>,
    /// Optional per-task interface ownership declarations (`task_id -> TaskInterfaceOwnershipSpec`).
    #[serde(default)]
    pub task_interfaces: BTreeMap<String, TaskInterfaceOwnershipSpec>,
}

/// Request to reserve a pre-effect [`OutboxDispatchRecord`] for a task in a [`CoworkerJobRecord`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReserveTaskOutboxRequest {
    /// Control-plane chat binding identifier.
    pub binding_id: String,
    /// Canonical lowercase hyphenated non-nil native Codex thread UUID.
    pub thread_uuid: String,
    /// Target [`CoworkerJobRecord::job_id`].
    pub job_id: String,
    /// Expected current `job_revision` (`>= 1`).
    pub expected_job_revision: u64,
    /// Optional expected current ledger revision (CAS check).
    pub expected_ledger_revision: Option<u64>,
    /// Idempotent caller request key.
    pub request_key: String,
    /// Category of external dispatch or mutation.
    pub kind: OutboxDispatchKind,
    /// Target task identifier within the job.
    pub task_id: String,
    /// Workflow role for this dispatch.
    pub role: WorkflowRole,
    /// 64-character lowercase hex SHA-256 digest of the dispatch subject.
    pub subject_digest: String,
    /// Optional prepared dependency input commit SHA (when `kind == DependencyPreparation` or child dispatch).
    pub dependency_input_commit: Option<String>,
    /// Optional prepared dependency input Git tree SHA.
    pub dependency_input_tree_sha: Option<String>,
    /// Optional accepted predecessor receipts bound for a dependent task.
    #[serde(default)]
    pub accepted_predecessor_receipts: Vec<AcceptedPredecessorReceiptRef>,
}

/// Request to finalize or reconcile a previously reserved [`OutboxDispatchRecord`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FinalizeTaskOutboxRequest {
    /// Control-plane chat binding identifier.
    pub binding_id: String,
    /// Canonical lowercase hyphenated non-nil native Codex thread UUID.
    pub thread_uuid: String,
    /// Target [`CoworkerJobRecord::job_id`].
    pub job_id: String,
    /// Expected current `job_revision` (`>= 1`).
    pub expected_job_revision: u64,
    /// Optional expected current ledger revision (CAS check).
    pub expected_ledger_revision: Option<u64>,
    /// Target [`OutboxDispatchRecord::outbox_id`].
    pub outbox_id: String,
    /// Resulting outbox status (`DispatchedEffectConfirmed`, `RefusedBeforeEffect`, `EffectUnknown`, `ReconciledConfirmed`, or `ReconciledNotExecuted`).
    pub status: OutboxDispatchStatus,
    /// Target [`CoworkerTaskStage`] after finalization/reconciliation.
    pub next_task_stage: CoworkerTaskStage,
    /// Optional sealed runtime `run_id`.
    pub run_id: Option<String>,
    /// Optional sealed runtime `worker_id`.
    pub worker_id: Option<String>,
    /// Optional task worktree identifier.
    pub task_worktree_id: Option<String>,
    /// Optional committed head SHA produced by the attempt.
    pub head_commit: Option<String>,
    /// Optional Git tree SHA produced by the attempt.
    pub tree_sha: Option<String>,
    /// Optional 64-hex SHA-256 diff digest.
    pub diff_digest: Option<String>,
    /// Optional local report artifact identifier.
    pub report_artifact_id: Option<String>,
    /// Optional observed provider model.
    pub observed_model: Option<String>,
    /// Optional observed provider effort.
    pub observed_effort: Option<String>,
    /// Optional sealed receipt identifier (`rev_<hex>` or `int_<hex>`).
    pub receipt_id: Option<String>,
    /// Optional sanitized diagnostic message.
    pub diagnostic: Option<String>,
}

/// Request to record a committed [`TaskCandidateRef`] and its [`StructuredReviewVerdict`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecordTaskReviewVerdictRequest {
    /// Control-plane chat binding identifier.
    pub binding_id: String,
    /// Canonical lowercase hyphenated non-nil native Codex thread UUID.
    pub thread_uuid: String,
    /// Target [`CoworkerJobRecord::job_id`].
    pub job_id: String,
    /// Expected current `job_revision` (`>= 1`).
    pub expected_job_revision: u64,
    /// Optional expected current ledger revision (CAS check).
    pub expected_ledger_revision: Option<u64>,
    /// Committed per-task candidate and structured review verdict.
    pub candidate: TaskCandidateRef,
}

/// Request to record an unreviewed [`TaskCandidateRef`] (`review_verdict = None`) and transition
/// a task from [`CoworkerTaskStage::ReadyForReview`] to
/// [`CoworkerTaskStage::CorrectInScopeRequired`] based on a verified
/// [`CheckFailureCorrectionBasis`].
///
/// This is an internal trusted ledger primitive and is not standalone execution permission;
/// [`crate::coworker_workflow::CoworkerTaskCorrectionService`] owns full persisted check-report
/// and worktree artifact verification before invoking this transition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecordTaskCheckFailureCandidateRequest {
    /// Control-plane chat binding identifier.
    pub binding_id: String,
    /// Canonical lowercase hyphenated non-nil native Codex thread UUID.
    pub thread_uuid: String,
    /// Target [`CoworkerJobRecord::job_id`].
    pub job_id: String,
    /// Expected current `job_revision` (`>= 1`).
    pub expected_job_revision: u64,
    /// Optional expected current ledger revision (CAS check).
    pub expected_ledger_revision: Option<u64>,
    /// Committed unreviewed per-task candidate (`review_verdict = None`, `review_receipt_id = None`).
    pub candidate: TaskCandidateRef,
    /// Verified check-failure correction basis (`CheckFailureCorrectionBasis`).
    pub basis: CheckFailureCorrectionBasis,
}

/// Type alias for [`RecordTaskCheckFailureCandidateRequest`].
pub type RecordTaskCheckFailureRequest = RecordTaskCheckFailureCandidateRequest;

/// Request to record a [`WorkflowHoldRecord`] and hold all non-independent tasks in a job.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApplyWorkflowHoldRequest {
    /// Control-plane chat binding identifier.
    pub binding_id: String,
    /// Canonical lowercase hyphenated non-nil native Codex thread UUID.
    pub thread_uuid: String,
    /// Target [`CoworkerJobRecord::job_id`].
    pub job_id: String,
    /// Expected current `job_revision` (`>= 1`).
    pub expected_job_revision: u64,
    /// Optional expected current ledger revision (CAS check).
    pub expected_ledger_revision: Option<u64>,
    /// Idempotent caller request key.
    pub request_key: String,
    /// Structured hold reason.
    pub reason: WorkflowHoldReason,
    /// Bounded human-readable hold description.
    pub detail: String,
    /// Task identifiers directly affected by the change or conflict.
    #[serde(default)]
    pub directly_affected_task_ids: Vec<String>,
    /// Repository-relative write paths affected by the change or conflict.
    #[serde(default)]
    pub affected_write_paths: Vec<String>,
    /// Logical interface identifiers affected by the change or conflict.
    #[serde(default)]
    pub affected_interfaces: Vec<String>,
    /// Whether this hold applies to the entire job.
    pub holds_entire_job: bool,
}

/// Request to resolve an active [`WorkflowHoldRecord`] and release held tasks that are now proven
/// independent of any remaining holds.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResolveWorkflowHoldRequest {
    /// Control-plane chat binding identifier.
    pub binding_id: String,
    /// Canonical lowercase hyphenated non-nil native Codex thread UUID.
    pub thread_uuid: String,
    /// Target [`CoworkerJobRecord::job_id`].
    pub job_id: String,
    /// Expected current `job_revision` (`>= 1`).
    pub expected_job_revision: u64,
    /// Optional expected current ledger revision (CAS check).
    pub expected_ledger_revision: Option<u64>,
    /// Target [`WorkflowHoldRecord::hold_id`].
    pub hold_id: String,
    /// Bounded human-readable resolution note.
    pub resolution_note: String,
}

/// Request to record a verified [`CombinedCandidateIdentity`] on a [`CoworkerJobRecord`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecordCombinedCandidateRequest {
    /// Control-plane chat binding identifier.
    pub binding_id: String,
    /// Canonical lowercase hyphenated non-nil native Codex thread UUID.
    pub thread_uuid: String,
    /// Target [`CoworkerJobRecord::job_id`].
    pub job_id: String,
    /// Expected current `job_revision` (`>= 1`).
    pub expected_job_revision: u64,
    /// Optional expected current ledger revision (CAS check).
    pub expected_ledger_revision: Option<u64>,
    /// Idempotent caller request key.
    pub request_key: String,
    /// Optional 40-character lowercase hex final integration commit SHA.
    pub final_commit_sha: Option<String>,
    /// Exact 40-character lowercase hex final Git tree SHA.
    pub final_tree_sha: String,
    /// Accepted and integrated predecessor receipts composing the final tree.
    pub accepted_predecessor_receipts: Vec<AcceptedPredecessorReceiptRef>,
    /// Passing check evidence entries for the combined tree.
    pub check_evidence: Vec<CandidateCheckEvidence>,
    /// Optional bound `PackageClosureReservation::reservation_id` (`cls_<32hex>`).
    pub closure_reservation_id: Option<String>,
}

/// Durable per-binding Coworker workflow ledger store backed by private (`0600`) files inside an
/// existing private (`0700`) `plans_dir`.
#[derive(Debug, Clone)]
pub struct CoworkerWorkflowStore {
    plans_dir: PathBuf,
}

impl CoworkerWorkflowStore {
    /// Opens a [`CoworkerWorkflowStore`] handle over an existing private (`0700`) non-symlink
    /// `plans_dir` without creating any directory, state file, or lockfile.
    pub fn new(plans_dir: impl AsRef<Path>) -> Result<Self, NativeError> {
        ensure_platform_supported()?;
        let clean = validate_existing_private_store_dir(plans_dir.as_ref(), "plans_dir")?;
        Ok(Self { plans_dir: clean })
    }

    /// Returns the validated `plans_dir` path.
    pub fn plans_dir(&self) -> &Path {
        &self.plans_dir
    }

    /// Returns the deterministic private ledger file path (`coworker-workflow-<binding_tag>.json`)
    /// for `binding_id`.
    pub fn ledger_path_for_binding(&self, binding_id: &str) -> Result<PathBuf, NativeError> {
        validate_bounded_identifier(binding_id, "binding_id", MAX_COWORKER_ID_LEN)?;
        Ok(self.plans_dir.join(ledger_filename_for_binding(binding_id)))
    }

    /// Returns the deterministic cross-process lockfile path (`coworker-workflow-<binding_tag>.lock`)
    /// for `binding_id`.
    pub fn lock_path_for_binding(&self, binding_id: &str) -> Result<PathBuf, NativeError> {
        validate_bounded_identifier(binding_id, "binding_id", MAX_COWORKER_ID_LEN)?;
        Ok(self.plans_dir.join(lock_filename_for_binding(binding_id)))
    }

    /// Pure read-only snapshot of `binding_id`'s [`CoworkerBindingLedger`].
    ///
    /// Never creates a directory, state file, or lockfile. Returns an empty revision-0 ledger when
    /// no file exists yet. Fails closed with [`NativeErrorCode::ProtocolError`] while preserving
    /// the on-disk file untouched if the file is corrupt or has an unsupported `schema_version`.
    pub fn read_binding_ledger(
        &self,
        binding_id: &str,
    ) -> Result<CoworkerBindingLedger, NativeError> {
        validate_bounded_identifier(binding_id, "binding_id", MAX_COWORKER_ID_LEN)?;
        let _ = validate_existing_private_store_dir(&self.plans_dir, "plans_dir")?;
        let ledger_path = self.ledger_path_for_binding(binding_id)?;
        match self.read_ledger_unlocked(binding_id, &ledger_path)? {
            Some(ledger) => Ok(ledger),
            None => CoworkerBindingLedger::empty(binding_id),
        }
    }

    /// Executes an atomic compare-and-swap transaction over `binding_id`'s [`CoworkerBindingLedger`]
    /// under the per-binding cross-process lock (`coworker-workflow-<binding_tag>.lock`).
    ///
    /// - If `f` returns [`CoworkerLedgerMutationOutcome::IdempotentExisting`], the store verifies
    ///   that the ledger was not modified and returns without writing to disk or incrementing
    ///   `ledger_revision`.
    /// - If `f` returns [`CoworkerLedgerMutationOutcome::Mutated`], the store verifies
    ///   `expected_ledger_revision` (when `Some`), increments `ledger_revision` by `1`, validates
    ///   the updated [`CoworkerBindingLedger`], and persists it atomically (`0600`).
    pub fn transact_binding_ledger<T, F>(
        &self,
        binding_id: &str,
        expected_ledger_revision: Option<u64>,
        f: F,
    ) -> Result<(CoworkerBindingLedger, T), NativeError>
    where
        F: FnOnce(
            &mut CoworkerBindingLedger,
        ) -> Result<CoworkerLedgerMutationOutcome<T>, NativeError>,
    {
        validate_bounded_identifier(binding_id, "binding_id", MAX_COWORKER_ID_LEN)?;
        let _ = validate_existing_private_store_dir(&self.plans_dir, "plans_dir")?;
        let lock_filename = lock_filename_for_binding(binding_id);
        let _guard = acquire_cross_process_lock(
            &self.plans_dir,
            &lock_filename,
            "coworker_workflow_ledger_lock",
        )?;
        let ledger_path = self.ledger_path_for_binding(binding_id)?;
        let original = match self.read_ledger_unlocked(binding_id, &ledger_path)? {
            Some(existing) => existing,
            None => CoworkerBindingLedger::empty(binding_id)?,
        };
        let mut working = original.clone();

        match f(&mut working)? {
            CoworkerLedgerMutationOutcome::IdempotentExisting(value) => {
                if working != original {
                    return Err(NativeError::new(
                        NativeErrorCode::ProtocolError,
                        "transaction returned IdempotentExisting after mutating working ledger",
                    ));
                }
                Ok((original, value))
            }
            CoworkerLedgerMutationOutcome::Mutated(value) => {
                for (job_id, marker) in &original.admission_markers {
                    if working.admission_markers.get(job_id) != Some(marker) {
                        return Err(NativeError::new(
                            NativeErrorCode::ValidationFailed,
                            "Automatic admission markers are immutable and cannot be removed",
                        ));
                    }
                }
                if let Some(expected_rev) = expected_ledger_revision {
                    if expected_rev != original.ledger_revision {
                        return Err(NativeError::new(
                            NativeErrorCode::ValidationFailed,
                            format!(
                                "expected_ledger_revision {expected_rev} does not match current coworker workflow ledger_revision {}",
                                original.ledger_revision
                            ),
                        ));
                    }
                }
                let next_rev = original.ledger_revision.checked_add(1).ok_or_else(|| {
                    NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "coworker workflow ledger_revision overflow",
                    )
                })?;
                working.ledger_revision = next_rev;
                self.write_ledger_unlocked(binding_id, &working)?;
                Ok((working, value))
            }
        }
    }

    /// Creates and persists a scoped read-only [`PlanningAdviceGrant`] after verifying live
    /// repository/base commit state, operator attestation, read-only capabilities, and policy bounds.
    pub fn create_planning_grant(
        &self,
        request: &CreatePlanningGrantRequest,
    ) -> Result<PlanningAdviceGrant, NativeError> {
        self.create_planning_grant_with_scoped_proof(request, None)
    }

    /// Creates and persists a scoped read-only [`PlanningAdviceGrant`] with an optional canonical
    /// [`ScopedProfileDependencyProof`] bound to its identity and idempotency checks.
    pub fn create_planning_grant_with_scoped_proof(
        &self,
        request: &CreatePlanningGrantRequest,
        scoped_profile_dependency_proof: Option<ScopedProfileDependencyProof>,
    ) -> Result<PlanningAdviceGrant, NativeError> {
        validate_bounded_identifier(&request.binding_id, "binding_id", MAX_COWORKER_ID_LEN)?;
        validate_bounded_identifier(&request.request_key, "request_key", MAX_COWORKER_ID_LEN)?;
        validate_bounded_identifier(&request.profile_id, "profile_id", MAX_COWORKER_ID_LEN)?;
        if request.registry_revision == 0 {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "CreatePlanningGrantRequest.registry_revision must be >= 1",
            ));
        }
        request.project_base.verify_live_repository()?;
        let policy_digest = request.policy.digest()?;
        let grant_id = match &scoped_profile_dependency_proof {
            Some(proof) => deterministic_prefixed_id(
                "threadlane-native:coworker-planning-grant:v2",
                "pgr",
                &[
                    &request.binding_id,
                    &request.thread_uuid,
                    &request.request_key,
                    &policy_digest,
                    &proof.dependency_digest,
                ],
            ),
            None => deterministic_prefixed_id(
                "threadlane-native:coworker-planning-grant:v1",
                "pgr",
                &[
                    &request.binding_id,
                    &request.thread_uuid,
                    &request.request_key,
                    &policy_digest,
                ],
            ),
        };

        let candidate_grant = PlanningAdviceGrant {
            grant_id,
            create_request_key: request.request_key.clone(),
            binding_id: request.binding_id.clone(),
            thread_uuid: request.thread_uuid.clone(),
            project_base: request.project_base.clone(),
            profile_id: request.profile_id.clone(),
            registry_revision: request.registry_revision,
            policy: request.policy.clone(),
            policy_digest,
            capabilities: request.capabilities.clone(),
            budget: request.budget.clone(),
            usage: PlanningGrantUsage::default(),
            status: PlanningGrantStatus::Active,
            created_at_ms: request.created_at_ms,
            expires_at_ms: request.expires_at_ms,
            revoked_at_ms: None,
            revoked_reason: None,
            operator_attested: request.operator_attested,
            native_per_call_identity_verified: false,
            scoped_profile_dependency_proof,
            reservations: Vec::new(),
        };
        candidate_grant.validate()?;

        let (_, grant) = self.transact_binding_ledger(
            &request.binding_id,
            request.expected_ledger_revision,
            |ledger| {
                if let Some(existing) = ledger
                    .planning_grants
                    .iter()
                    .find(|g| g.create_request_key == request.request_key)
                {
                    if existing.binding_id == candidate_grant.binding_id
                        && existing.thread_uuid == candidate_grant.thread_uuid
                        && existing.project_base == candidate_grant.project_base
                        && existing.profile_id == candidate_grant.profile_id
                        && existing.registry_revision == candidate_grant.registry_revision
                        && existing.policy_digest == candidate_grant.policy_digest
                        && existing.policy == candidate_grant.policy
                        && existing.capabilities == candidate_grant.capabilities
                        && existing.budget == candidate_grant.budget
                        && existing.created_at_ms == candidate_grant.created_at_ms
                        && existing.expires_at_ms == candidate_grant.expires_at_ms
                        && existing.scoped_profile_dependency_proof
                            == candidate_grant.scoped_profile_dependency_proof
                    {
                        return Ok(CoworkerLedgerMutationOutcome::IdempotentExisting(
                            existing.clone(),
                        ));
                    }
                    return Err(NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "planning grant request_key was already used with different parameters",
                    ));
                }

                ledger.ensure_thread_scope(&request.thread_uuid)?;
                if ledger.planning_grants.len() >= MAX_PLANNING_GRANTS_PER_BINDING {
                    return Err(NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "maximum planning grants reached for this binding",
                    ));
                }
                ledger.planning_grants.push(candidate_grant.clone());
                Ok(CoworkerLedgerMutationOutcome::Mutated(candidate_grant))
            },
        )?;
        Ok(grant)
    }

    /// Reserves a single planning/advice call budget durably before effect.
    pub fn reserve_planning_call(
        &self,
        request: &ReservePlanningCallRequest,
    ) -> Result<PlanningCallReservation, NativeError> {
        Ok(self
            .acquire_planning_call_reservation(request)?
            .into_reservation())
    }

    /// Acquires a single planning/advice call reservation under the cross-process ledger lock,
    /// distinguishing a newly created [`PlanningReservationStatus::ReservedBeforeEffect`]
    /// reservation from an existing terminal reservation with the same `request_key`.
    pub fn acquire_planning_call_reservation(
        &self,
        request: &ReservePlanningCallRequest,
    ) -> Result<PlanningCallReservationAcquisition, NativeError> {
        validate_bounded_identifier(&request.binding_id, "binding_id", MAX_COWORKER_ID_LEN)?;
        validate_bounded_identifier(&request.grant_id, "grant_id", MAX_COWORKER_ID_LEN)?;
        validate_bounded_identifier(&request.request_key, "request_key", MAX_COWORKER_ID_LEN)?;
        validate_sha256_hex_digest(&request.context_digest, "context_digest").map_err(|_| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                "context_digest must be a 64-character lowercase hex SHA-256",
            )
        })?;
        if !matches!(
            request.role,
            WorkflowRole::Orchestrator | WorkflowRole::Architect
        ) {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "ReservePlanningCallRequest.role must be Orchestrator or Architect",
            ));
        }
        validate_non_native_worker_choice(request.role, &request.selected_choice)?;
        if let Some(ref call_base) = request.call_project_base {
            call_base.verify_live_repository()?;
        }

        let (_, acquisition) = self.transact_binding_ledger(
            &request.binding_id,
            request.expected_ledger_revision,
            |ledger| {
                ledger.ensure_thread_scope(&request.thread_uuid)?;
                let grant_idx = ledger
                    .planning_grants
                    .iter()
                    .position(|g| g.grant_id == request.grant_id)
                    .ok_or_else(|| {
                        NativeError::new(
                            NativeErrorCode::ValidationFailed,
                            "requested grant_id was not found in this binding",
                        )
                    })?;

                if let Some(ref call_base) = request.call_project_base {
                    if call_base.repository_path
                        != ledger.planning_grants[grant_idx].project_base.repository_path
                    {
                        return Err(NativeError::new(
                            NativeErrorCode::SecurityViolation,
                            "ReservePlanningCallRequest.call_project_base.repository_path does not match the standing grant's canonical repository_path",
                        ));
                    }
                }

                if let Some(existing) = ledger.planning_grants[grant_idx]
                    .reservations
                    .iter()
                    .find(|r| r.request_key == request.request_key)
                {
                    if existing.role != request.role
                        || existing.difficulty != request.difficulty
                        || existing.selected_choice != request.selected_choice
                        || existing.context_digest != request.context_digest
                        || existing.call_project_base != request.call_project_base
                    {
                        return Err(NativeError::new(
                            NativeErrorCode::ValidationFailed,
                            "planning reservation request_key was already used with different parameters",
                        ));
                    }
                    if existing.status.is_ambiguous() {
                        return Err(NativeError::new(
                            NativeErrorCode::AmbiguousWakeupPendingReconciliation,
                            "planning call reservation has an uncertain outcome; reconcile before retrying",
                        ));
                    }
                    return Ok(CoworkerLedgerMutationOutcome::IdempotentExisting(
                        PlanningCallReservationAcquisition::ExistingTerminal(existing.clone()),
                    ));
                }

                if ledger.planning_grants.iter().any(|g| {
                    g.reservations.iter().any(|r| {
                        r.role == request.role
                            && r.context_digest == request.context_digest
                            && r.status.is_ambiguous()
                    })
                }) {
                    return Err(NativeError::new(
                        NativeErrorCode::AmbiguousWakeupPendingReconciliation,
                        "another planning call for this role and context_digest has an uncertain outcome pending reconciliation",
                    ));
                }

                let grant = &mut ledger.planning_grants[grant_idx];

                if grant.status == PlanningGrantStatus::Revoked {
                    return Err(NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "planning grant is revoked and cannot reserve calls",
                    ));
                }
                if request.current_time_ms >= grant.expires_at_ms
                    || grant.status == PlanningGrantStatus::Expired
                {
                    return Err(NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "planning grant has expired and cannot reserve calls",
                    ));
                }
                if grant.status == PlanningGrantStatus::Exhausted
                    || grant.usage.reserved_total_calls >= grant.budget.max_total_calls
                {
                    return Err(NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "planning grant call budget is exhausted",
                    ));
                }
                match request.role {
                    WorkflowRole::Orchestrator => {
                        if grant.usage.reserved_orchestrator_calls
                            >= grant.budget.max_orchestrator_calls
                        {
                            return Err(NativeError::new(
                                NativeErrorCode::ValidationFailed,
                                "planning grant orchestrator call budget is exhausted",
                            ));
                        }
                    }
                    WorkflowRole::Architect => {
                        if grant.usage.reserved_architect_calls >= grant.budget.max_architect_calls
                        {
                            return Err(NativeError::new(
                                NativeErrorCode::ValidationFailed,
                                "planning grant architect call budget is exhausted",
                            ));
                        }
                    }
                    _ => unreachable!(),
                }

                if !grant.policy.allows(
                    request.role,
                    &request.selected_choice.tool_id,
                    &request.selected_choice.model_id,
                    request.selected_choice.effort.as_deref(),
                )? {
                    return Err(NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "selected_choice is not permitted by the planning grant ExecutionPolicy",
                    ));
                }
                if grant.reservations.len() >= MAX_GRANT_RESERVATIONS {
                    return Err(NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "planning grant reached maximum reservation capacity",
                    ));
                }

                let reservation_id = deterministic_prefixed_id(
                    "threadlane-native:coworker-planning-reservation:v1",
                    "prs",
                    &[&grant.grant_id, &request.request_key, &request.context_digest],
                );
                let reservation = PlanningCallReservation {
                    reservation_id,
                    request_key: request.request_key.clone(),
                    role: request.role,
                    difficulty: request.difficulty,
                    selected_choice: request.selected_choice.clone(),
                    context_digest: request.context_digest.clone(),
                    call_project_base: request.call_project_base.clone(),
                    status: PlanningReservationStatus::ReservedBeforeEffect,
                    reserved_at_ms: request.current_time_ms,
                    updated_at_ms: request.current_time_ms,
                    run_id: None,
                    report_artifact_id: None,
                    output_digest: None,
                    diagnostic: None,
                };

                grant.usage.reserved_total_calls += 1;
                if request.role == WorkflowRole::Orchestrator {
                    grant.usage.reserved_orchestrator_calls += 1;
                } else {
                    grant.usage.reserved_architect_calls += 1;
                }
                if grant.usage.reserved_total_calls >= grant.budget.max_total_calls {
                    grant.status = PlanningGrantStatus::Exhausted;
                }
                grant.reservations.push(reservation.clone());
                Ok(CoworkerLedgerMutationOutcome::Mutated(
                    PlanningCallReservationAcquisition::FreshlyReserved(reservation),
                ))
            },
        )?;
        Ok(acquisition)
    }

    /// Finalizes or reconciles a [`PlanningCallReservation`] after provider execution or refusal.
    pub fn finalize_planning_call(
        &self,
        request: &FinalizePlanningCallRequest,
    ) -> Result<PlanningCallReservation, NativeError> {
        validate_bounded_identifier(&request.binding_id, "binding_id", MAX_COWORKER_ID_LEN)?;
        validate_bounded_identifier(&request.grant_id, "grant_id", MAX_COWORKER_ID_LEN)?;
        validate_bounded_identifier(
            &request.reservation_id,
            "reservation_id",
            MAX_COWORKER_ID_LEN,
        )?;
        if request.status == PlanningReservationStatus::ReservedBeforeEffect {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "FinalizePlanningCallRequest.status cannot be ReservedBeforeEffect",
            ));
        }
        if request.status == PlanningReservationStatus::Completed && request.output_digest.is_none()
        {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "Completed planning call reservation requires an output_digest",
            ));
        }

        let (_, updated) = self.transact_binding_ledger(
            &request.binding_id,
            request.expected_ledger_revision,
            |ledger| {
                ledger.ensure_thread_scope(&request.thread_uuid)?;
                let grant = ledger
                    .planning_grants
                    .iter_mut()
                    .find(|g| g.grant_id == request.grant_id)
                    .ok_or_else(|| {
                        NativeError::new(
                            NativeErrorCode::ValidationFailed,
                            "requested grant_id was not found in this binding",
                        )
                    })?;
                let res = grant
                    .reservations
                    .iter_mut()
                    .find(|r| r.reservation_id == request.reservation_id)
                    .ok_or_else(|| {
                        NativeError::new(
                            NativeErrorCode::ValidationFailed,
                            "requested reservation_id was not found on this grant",
                        )
                    })?;

                if res.status == request.status
                    && res.run_id == request.run_id
                    && res.report_artifact_id == request.report_artifact_id
                    && res.output_digest == request.output_digest
                {
                    return Ok(CoworkerLedgerMutationOutcome::IdempotentExisting(
                        res.clone(),
                    ));
                }

                match (res.status, request.status) {
                    (
                        PlanningReservationStatus::ReservedBeforeEffect,
                        PlanningReservationStatus::Completed,
                    ) => {
                        grant.usage.completed_calls += 1;
                    }
                    (
                        PlanningReservationStatus::ReservedBeforeEffect,
                        PlanningReservationStatus::FailedAfterEffect,
                    ) => {
                        grant.usage.failed_calls += 1;
                    }
                    (
                        PlanningReservationStatus::ReservedBeforeEffect,
                        PlanningReservationStatus::EffectUnknown,
                    ) => {
                        grant.usage.unknown_effect_calls += 1;
                    }
                    (
                        PlanningReservationStatus::ReservedBeforeEffect,
                        PlanningReservationStatus::RefusedBeforeEffect,
                    ) => {
                        if request.run_id.is_some() || res.run_id.is_some() {
                            return Err(NativeError::new(
                                NativeErrorCode::ValidationFailed,
                                "RefusedBeforeEffect is forbidden when a run_id exists",
                            ));
                        }
                        grant.usage.reserved_total_calls =
                            grant.usage.reserved_total_calls.saturating_sub(1);
                        if res.role == WorkflowRole::Orchestrator {
                            grant.usage.reserved_orchestrator_calls =
                                grant.usage.reserved_orchestrator_calls.saturating_sub(1);
                        } else {
                            grant.usage.reserved_architect_calls =
                                grant.usage.reserved_architect_calls.saturating_sub(1);
                        }
                        grant.usage.refused_before_effect_calls += 1;
                        if grant.status == PlanningGrantStatus::Exhausted
                            && request.updated_at_ms < grant.expires_at_ms
                        {
                            grant.status = PlanningGrantStatus::Active;
                        }
                    }
                    (
                        PlanningReservationStatus::EffectUnknown,
                        PlanningReservationStatus::Completed,
                    ) => {
                        grant.usage.unknown_effect_calls =
                            grant.usage.unknown_effect_calls.saturating_sub(1);
                        grant.usage.completed_calls += 1;
                    }
                    (
                        PlanningReservationStatus::EffectUnknown,
                        PlanningReservationStatus::FailedAfterEffect,
                    ) => {
                        grant.usage.unknown_effect_calls =
                            grant.usage.unknown_effect_calls.saturating_sub(1);
                        grant.usage.failed_calls += 1;
                    }
                    _ => {
                        return Err(NativeError::new(
                            NativeErrorCode::ValidationFailed,
                            "invalid PlanningCallReservation status transition",
                        ));
                    }
                }

                res.status = request.status;
                res.updated_at_ms = request.updated_at_ms;
                res.run_id = request.run_id.clone();
                res.report_artifact_id = request.report_artifact_id.clone();
                res.output_digest = request.output_digest.clone();
                res.diagnostic = request.diagnostic.as_deref().map(sanitize_diagnostic_text);

                Ok(CoworkerLedgerMutationOutcome::Mutated(res.clone()))
            },
        )?;
        Ok(updated)
    }

    /// Explicitly revokes a [`PlanningAdviceGrant`] so no further calls can be reserved.
    pub fn revoke_planning_grant(
        &self,
        request: &RevokePlanningGrantRequest,
    ) -> Result<PlanningAdviceGrant, NativeError> {
        validate_bounded_identifier(&request.binding_id, "binding_id", MAX_COWORKER_ID_LEN)?;
        validate_bounded_identifier(&request.grant_id, "grant_id", MAX_COWORKER_ID_LEN)?;
        validate_bounded_text(&request.reason, "reason")?;

        let (_, grant) = self.transact_binding_ledger(
            &request.binding_id,
            request.expected_ledger_revision,
            |ledger| {
                ledger.ensure_thread_scope(&request.thread_uuid)?;
                let grant = ledger
                    .planning_grants
                    .iter_mut()
                    .find(|g| g.grant_id == request.grant_id)
                    .ok_or_else(|| {
                        NativeError::new(
                            NativeErrorCode::ValidationFailed,
                            "requested grant_id was not found in this binding",
                        )
                    })?;
                if grant.status == PlanningGrantStatus::Revoked
                    && grant.revoked_reason.as_deref() == Some(request.reason.as_str())
                {
                    return Ok(CoworkerLedgerMutationOutcome::IdempotentExisting(
                        grant.clone(),
                    ));
                }
                grant.status = PlanningGrantStatus::Revoked;
                grant.revoked_at_ms = Some(request.revoked_at_ms);
                grant.revoked_reason = Some(request.reason.clone());
                Ok(CoworkerLedgerMutationOutcome::Mutated(grant.clone()))
            },
        )?;
        Ok(grant)
    }

    /// Records a dependency-bound [`ArchitectAdviceRecord`] linked to a completed architect call
    /// reservation.
    pub fn record_architect_advice(
        &self,
        request: &RecordArchitectAdviceRequest,
    ) -> Result<ArchitectAdviceRecord, NativeError> {
        request
            .context_dependencies
            .project_base
            .verify_live_repository()?;
        let context_digest = request.context_dependencies.context_digest()?;
        let advice_id = deterministic_prefixed_id(
            "threadlane-native:coworker-architect-advice:v1",
            "adv",
            &[
                &request.binding_id,
                &request.grant_id,
                &request.reservation_id,
                &request.request_key,
            ],
        );

        let (_, advice) = self.transact_binding_ledger(
            &request.binding_id,
            request.expected_ledger_revision,
            |ledger| {
                ledger.ensure_thread_scope(&request.thread_uuid)?;
                if let Some(existing) = ledger
                    .architect_advice
                    .iter()
                    .find(|a| a.request_key == request.request_key)
                {
                    if existing.grant_id == request.grant_id
                        && existing.reservation_id == request.reservation_id
                        && existing.context_digest == context_digest
                        && existing.decision_digest == request.decision_digest
                        && existing.decision_summary == request.decision_summary
                    {
                        return Ok(CoworkerLedgerMutationOutcome::IdempotentExisting(
                            existing.clone(),
                        ));
                    }
                    return Err(NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "architect advice request_key was already used with different parameters",
                    ));
                }

                let grant = ledger
                    .planning_grants
                    .iter()
                    .find(|g| g.grant_id == request.grant_id)
                    .ok_or_else(|| {
                        NativeError::new(
                            NativeErrorCode::ValidationFailed,
                            "referenced grant_id was not found in this binding",
                        )
                    })?;
                if request.context_dependencies.project_base.repository_path
                    != grant.project_base.repository_path
                {
                    return Err(NativeError::new(
                        NativeErrorCode::SecurityViolation,
                        "ArchitectAdviceRecord project_base.repository_path does not match the standing grant's canonical repository_path",
                    ));
                }
                if request.context_dependencies.policy_digest != grant.policy_digest {
                    return Err(NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "ArchitectAdviceRecord policy_digest does not match the standing grant's policy_digest",
                    ));
                }
                let reservation = grant
                    .reservations
                    .iter()
                    .find(|r| r.reservation_id == request.reservation_id)
                    .ok_or_else(|| {
                        NativeError::new(
                            NativeErrorCode::ValidationFailed,
                            "referenced reservation_id was not found on the grant",
                        )
                    })?;
                if reservation.role != WorkflowRole::Architect
                    || reservation.status != PlanningReservationStatus::Completed
                {
                    return Err(NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "ArchitectAdviceRecord requires a Completed reservation for WorkflowRole::Architect",
                    ));
                }
                if reservation.context_digest != context_digest {
                    return Err(NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "ArchitectAdviceRecord context_digest does not match the reservation's context_digest",
                    ));
                }
                if let Some(ref call_base) = reservation.call_project_base {
                    if call_base != &request.context_dependencies.project_base {
                        return Err(NativeError::new(
                            NativeErrorCode::ValidationFailed,
                            "ArchitectAdviceRecord project_base does not match the reservation's call_project_base",
                        ));
                    }
                }
                if ledger.architect_advice.len() >= MAX_ARCHITECT_ADVICE_RECORDS_PER_BINDING {
                    return Err(NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "maximum architect advice records reached for this binding",
                    ));
                }

                let record = ArchitectAdviceRecord {
                    advice_id,
                    request_key: request.request_key.clone(),
                    grant_id: request.grant_id.clone(),
                    reservation_id: request.reservation_id.clone(),
                    binding_id: request.binding_id.clone(),
                    thread_uuid: request.thread_uuid.clone(),
                    selected_choice: reservation.selected_choice.clone(),
                    context_dependencies: request.context_dependencies.clone(),
                    context_digest,
                    decision_summary: request.decision_summary.clone(),
                    decision_digest: request.decision_digest.clone(),
                    report_artifact_id: request
                        .report_artifact_id
                        .clone()
                        .or_else(|| reservation.report_artifact_id.clone()),
                    status: request.status,
                    created_at_ms: request.created_at_ms,
                };
                record.validate()?;
                ledger.architect_advice.push(record.clone());
                Ok(CoworkerLedgerMutationOutcome::Mutated(record))
            },
        )?;
        Ok(advice)
    }

    /// Pure read-only lookup of a reusable [`ArchitectAdviceRecord`] whose real
    /// [`ArchitectContextDependencies`] match `current_deps` exactly.
    pub fn find_reusable_architect_advice(
        &self,
        binding_id: &str,
        thread_uuid: &str,
        current_deps: &ArchitectContextDependencies,
    ) -> Result<Option<ArchitectAdviceRecord>, NativeError> {
        validate_canonical_thread_uuid("thread_uuid", thread_uuid).map_err(|_| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                "thread_uuid must be a canonical lowercase hyphenated non-nil UUID",
            )
        })?;
        let _ = current_deps.context_digest()?;
        let ledger = self.read_binding_ledger(binding_id)?;
        if let Some(ref pinned_thread) = ledger.thread_uuid {
            if pinned_thread != thread_uuid {
                return Err(NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "thread_uuid does not match the thread pinned to this binding ledger",
                ));
            }
        }
        for record in ledger.architect_advice.iter().rev() {
            if record.can_reuse_for(current_deps)? {
                return Ok(Some(record.clone()));
            }
        }
        Ok(None)
    }

    /// Creates a new [`CoworkerJobRecord`] (`WorkBriefKind::NewJob`) or records an amendment job
    /// (`WorkBriefKind::Amendment`) while placing affected tasks on the superseded target job on hold.
    pub fn create_or_amend_job(
        &self,
        request: &CreateCoworkerJobRequest,
    ) -> Result<CoworkerJobRecord, NativeError> {
        validate_bounded_identifier(&request.binding_id, "binding_id", MAX_COWORKER_ID_LEN)?;
        validate_bounded_identifier(&request.request_key, "request_key", MAX_COWORKER_ID_LEN)?;
        if request.work_brief.binding_id != request.binding_id
            || request.work_brief.thread_uuid != request.thread_uuid
        {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "WorkBrief binding_id and thread_uuid must match CreateCoworkerJobRequest",
            ));
        }
        request.work_brief.project_base.verify_live_repository()?;
        let brief_digest = request.work_brief.digest()?;
        let job_id = deterministic_prefixed_id(
            "threadlane-native:coworker-job:v1",
            "job",
            &[
                &request.binding_id,
                &request.thread_uuid,
                &request.request_key,
                &brief_digest,
            ],
        );
        let supersedes_job_id = match &request.work_brief.kind {
            WorkBriefKind::NewJob => None,
            WorkBriefKind::Amendment { target_job_id, .. } => Some(target_job_id.clone()),
        };

        let (_, created) = self.transact_binding_ledger(
            &request.binding_id,
            request.expected_ledger_revision,
            |ledger| {
                if let Some(existing) = ledger
                    .jobs
                    .iter()
                    .find(|j| j.create_request_key == request.request_key)
                {
                    if existing.work_brief_digest == brief_digest
                        && existing.work_brief == request.work_brief
                    {
                        return Ok(CoworkerLedgerMutationOutcome::IdempotentExisting(
                            existing.clone(),
                        ));
                    }
                    return Err(NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "job request_key was already used with a different WorkBrief",
                    ));
                }

                ledger.ensure_thread_scope(&request.thread_uuid)?;
                if ledger.jobs.len() >= MAX_JOBS_PER_BINDING {
                    return Err(NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "maximum jobs reached for this binding",
                    ));
                }

                if let WorkBriefKind::Amendment {
                    ref target_job_id,
                    target_job_revision,
                    ref amendment_reason,
                } = request.work_brief.kind
                {
                    let target_job = ledger
                        .jobs
                        .iter_mut()
                        .find(|j| &j.job_id == target_job_id)
                        .ok_or_else(|| {
                            NativeError::new(
                                NativeErrorCode::ValidationFailed,
                                "amendment target_job_id was not found in this binding ledger",
                            )
                        })?;
                    if target_job.job_revision != target_job_revision {
                        return Err(NativeError::new(
                            NativeErrorCode::ValidationFailed,
                            "amendment target_job_revision does not match current target job_revision",
                        ));
                    }
                    let next_target_rev = target_job.job_revision.checked_add(1).ok_or_else(|| {
                        NativeError::new(
                            NativeErrorCode::ValidationFailed,
                            "target job_revision overflow",
                        )
                    })?;
                    let holds_entire_job = request.amended_target_task_ids.is_empty()
                        && request.amended_write_paths.is_empty()
                        && request.amended_interfaces.is_empty();
                    let hold_id = deterministic_prefixed_id(
                        "threadlane-native:coworker-hold:v1",
                        "hld",
                        &[target_job_id, &request.request_key, "amendment"],
                    );
                    let hold = WorkflowHoldRecord {
                        hold_id,
                        request_key: format!("amend_{}", request.request_key),
                        reason: WorkflowHoldReason::BriefAmendmentPending,
                        detail: amendment_reason.clone(),
                        directly_affected_task_ids: request.amended_target_task_ids.clone(),
                        affected_write_paths: request.amended_write_paths.clone(),
                        affected_interfaces: request.amended_interfaces.clone(),
                        holds_entire_job,
                        active: true,
                        created_at_job_revision: next_target_rev,
                        resolved_at_job_revision: None,
                        resolution_note: None,
                    };
                    apply_hold_to_job_tasks(target_job, &hold, next_target_rev)?;
                }

                let initial_phase = if request.work_brief.is_ready_for_execution_planning() {
                    CoworkerJobPhase::Planning
                } else {
                    CoworkerJobPhase::Briefing
                };

                let job = CoworkerJobRecord {
                    job_id,
                    job_revision: 1,
                    create_request_key: request.request_key.clone(),
                    binding_id: request.binding_id.clone(),
                    thread_uuid: request.thread_uuid.clone(),
                    phase: initial_phase,
                    work_brief: request.work_brief.clone(),
                    work_brief_digest: brief_digest,
                    supersedes_job_id,
                    planning_result: None,
                    approval_linkage: None,
                    tasks: Vec::new(),
                    holds: Vec::new(),
                    outbox: Vec::new(),
                    combined_candidates: Vec::new(),
                };
                job.validate()?;
                ledger.jobs.push(job.clone());
                Ok(CoworkerLedgerMutationOutcome::Mutated(job))
            },
        )?;
        Ok(created)
    }

    /// Records or reconciles a verified [`PlanningResultRef`] on a [`CoworkerJobRecord`] and
    /// transitions the job to [`CoworkerJobPhase::AwaitingApproval`] without granting execution
    /// approval.
    pub fn record_job_planning_result(
        &self,
        request: &RecordJobPlanningResultRequest,
    ) -> Result<CoworkerJobRecord, NativeError> {
        validate_bounded_identifier(&request.binding_id, "binding_id", MAX_COWORKER_ID_LEN)?;
        validate_bounded_identifier(&request.job_id, "job_id", MAX_COWORKER_ID_LEN)?;
        if let Some(exp_rev) = request.expected_job_revision {
            if exp_rev == 0 {
                return Err(NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "expected_job_revision must be >= 1",
                ));
            }
        }
        request.planning_result.validate()?;
        request
            .planning_result
            .project_base
            .verify_live_repository()?;

        let (_, updated) = self.transact_binding_ledger(
            &request.binding_id,
            request.expected_ledger_revision,
            |ledger| {
                ledger.ensure_thread_scope(&request.thread_uuid)?;
                if ledger.jobs.iter().any(|j| {
                    j.job_id != request.job_id
                        && j.planning_result
                            .as_ref()
                            .map(|pr| pr.plan_id == request.planning_result.plan_id)
                            .unwrap_or(false)
                }) {
                    return Err(NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "PlanningResultRef.plan_id is already bound to another job in this binding",
                    ));
                }

                if ledger
                    .jobs
                    .iter()
                    .any(|j| j.supersedes_job_id.as_deref() == Some(request.job_id.as_str()))
                {
                    return Err(NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "job has been superseded by an amendment job and cannot record a planning result",
                    ));
                }

                let job = ledger
                    .jobs
                    .iter_mut()
                    .find(|j| j.job_id == request.job_id)
                    .ok_or_else(|| {
                        NativeError::new(
                            NativeErrorCode::ValidationFailed,
                            "requested job_id was not found in this binding ledger",
                        )
                    })?;

                if job.holds.iter().any(|h| h.active) {
                    return Err(NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "job has an active hold and cannot record a planning result until resolved",
                    ));
                }
                if job.work_brief_digest != request.planning_result.work_brief_digest
                    || job.work_brief.project_base != request.planning_result.project_base
                {
                    return Err(NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "PlanningResultRef work_brief_digest or project_base does not match the target job",
                    ));
                }

                if let Some(ref existing) = job.planning_result {
                    if existing == &request.planning_result {
                        return Ok(CoworkerLedgerMutationOutcome::IdempotentExisting(
                            job.clone(),
                        ));
                    }
                    if existing.request_key == request.planning_result.request_key {
                        return Err(NativeError::new(
                            NativeErrorCode::ValidationFailed,
                            "planning_result request_key was already recorded on this job with different parameters",
                        ));
                    }
                }

                if let Some(exp_rev) = request.expected_job_revision {
                    if job.job_revision != exp_rev {
                        return Err(NativeError::new(
                            NativeErrorCode::ValidationFailed,
                            "expected_job_revision does not match current job_revision",
                        ));
                    }
                }
                if job.approval_linkage.is_some() {
                    return Err(NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "cannot overwrite planning_result after FrozenApprovalLinkage is bound",
                    ));
                }
                if !matches!(
                    job.phase,
                    CoworkerJobPhase::Briefing
                        | CoworkerJobPhase::Planning
                        | CoworkerJobPhase::AwaitingApproval
                ) {
                    return Err(NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "job must be in Briefing, Planning, or AwaitingApproval phase to record a planning result",
                    ));
                }

                let next_rev = job.job_revision.checked_add(1).ok_or_else(|| {
                    NativeError::new(NativeErrorCode::ValidationFailed, "job_revision overflow")
                })?;
                job.job_revision = next_rev;
                job.phase = CoworkerJobPhase::AwaitingApproval;
                job.planning_result = Some(request.planning_result.clone());
                job.validate()?;
                Ok(CoworkerLedgerMutationOutcome::Mutated(job.clone()))
            },
        )?;
        Ok(updated)
    }

    /// Binds an immutable operator [`ApprovalRecord`] and [`ExecutionPolicy`] to a [`CoworkerJobRecord`],
    /// initializing per-task records and verifying A1 preflight admission.
    ///
    /// This is the shared store-level manual approval-binding API (verifying [`PlanningResultRef`]
    /// consistency when present on the job while remaining compatible with explicitly approved
    /// manual jobs that have `planning_result == None`). Automatic controller admission must
    /// separately require a current-job [`PlanningResultRef`].
    ///
    /// Once bound, `approval_linkage` is immutable; any attempt to bind a different approval,
    /// plan revision, base commit, write scope, or policy on the same job fails closed.
    pub fn bind_job_approval(
        &self,
        request: &BindJobApprovalRequest,
        authority: &AuthorityStore,
    ) -> Result<CoworkerJobRecord, NativeError> {
        let approval = authority
            .get_for_operator(&request.approval_id)
            .map_err(map_authority_error)?;
        let registry = authority.registry().snapshot_readonly().map_err(|_| {
            NativeError::new(
                NativeErrorCode::ProtocolError,
                "Failed to read registry snapshot",
            )
        })?;
        let plans =
            threadlane_workflow::PlanStore::read_snapshot_readonly_at(authority.plans().dir())
                .map_err(|_| {
                    NativeError::new(
                        NativeErrorCode::ProtocolError,
                        "Failed to read plans snapshot",
                    )
                })?;
        let (_, job) = self.transact_binding_ledger(
            &request.binding_id,
            request.expected_ledger_revision,
            |ledger| prepare_job_approval_in_ledger(ledger, request, &approval, &registry, &plans),
        )?;
        Ok(job)
    }

    /// Reserves an [`OutboxDispatchRecord`] in [`OutboxDispatchStatus::ReservedBeforeEffect`]
    /// before any external task dispatch, review, dependency preparation, or integration effect.
    pub fn reserve_task_outbox(
        &self,
        request: &ReserveTaskOutboxRequest,
    ) -> Result<(CoworkerJobRecord, OutboxDispatchRecord), NativeError> {
        validate_bounded_identifier(&request.binding_id, "binding_id", MAX_COWORKER_ID_LEN)?;
        validate_bounded_identifier(&request.job_id, "job_id", MAX_COWORKER_ID_LEN)?;
        validate_bounded_identifier(&request.request_key, "request_key", MAX_COWORKER_ID_LEN)?;
        validate_bounded_identifier(&request.task_id, "task_id", MAX_COWORKER_ID_LEN)?;
        validate_sha256_hex_digest(&request.subject_digest, "subject_digest").map_err(|_| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                "subject_digest must be a 64-character lowercase hex SHA-256",
            )
        })?;
        if request.expected_job_revision == 0 {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "expected_job_revision must be >= 1",
            ));
        }

        let (_, pair) = self.transact_binding_ledger(
            &request.binding_id,
            request.expected_ledger_revision,
            |ledger| {
                ledger.ensure_thread_scope(&request.thread_uuid)?;
                let job = ledger
                    .jobs
                    .iter_mut()
                    .find(|j| j.job_id == request.job_id)
                    .ok_or_else(|| {
                        NativeError::new(
                            NativeErrorCode::ValidationFailed,
                            "requested job_id was not found in this binding ledger",
                        )
                    })?;

                if let Some(existing) = job
                    .outbox
                    .iter()
                    .find(|o| o.request_key == request.request_key)
                    .cloned()
                {
                    if existing.kind != request.kind
                        || existing.task_id != request.task_id
                        || existing.role != request.role
                        || existing.subject_digest != request.subject_digest
                    {
                        return Err(NativeError::new(
                            NativeErrorCode::ValidationFailed,
                            "outbox request_key was already used with different parameters",
                        ));
                    }
                    if existing.status.blocks_replay_and_new_dispatch() {
                        return Err(NativeError::new(
                            NativeErrorCode::AmbiguousWakeupPendingReconciliation,
                            "outbox record is in ReservedBeforeEffect or EffectUnknown state; automatic replay is forbidden",
                        ));
                    }
                    return Ok(CoworkerLedgerMutationOutcome::IdempotentExisting((
                        job.clone(),
                        existing,
                    )));
                }

                if job.job_revision != request.expected_job_revision {
                    return Err(NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "expected_job_revision does not match current job_revision",
                    ));
                }
                if job.outbox.len() >= MAX_OUTBOX_RECORDS_PER_JOB {
                    return Err(NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "job outbox reached maximum record capacity",
                    ));
                }
                if job
                    .outbox
                    .iter()
                    .any(|o| o.task_id == request.task_id && o.status.blocks_replay_and_new_dispatch())
                {
                    return Err(NativeError::new(
                        NativeErrorCode::AmbiguousWakeupPendingReconciliation,
                        "target task already has an unresolved outbox record pending reconciliation",
                    ));
                }

                let task_idx = job
                    .tasks
                    .iter()
                    .position(|t| t.task_id == request.task_id)
                    .ok_or_else(|| {
                        NativeError::new(
                            NativeErrorCode::ValidationFailed,
                            "target task_id was not found in job.tasks",
                        )
                    })?;

                // Verify independence and absence of active holds on the target task.
                let independence = prove_task_independence(
                    &job.tasks[task_idx],
                    &job.tasks,
                    &job.holds,
                )?;
                if !independence.independent
                    || !job.tasks[task_idx].active_hold_ids.is_empty()
                    || job.tasks[task_idx].stage == CoworkerTaskStage::Held
                {
                    return Err(NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "target task is held or not proven independent of active holds/uncertain tasks",
                    ));
                }

                let base_commit = job
                    .approval_linkage
                    .as_ref()
                    .map(|l| l.project_base.base_commit.clone())
                    .ok_or_else(|| {
                        NativeError::new(
                            NativeErrorCode::ValidationFailed,
                            "job has no FrozenApprovalLinkage",
                        )
                    })?;

                let task = &mut job.tasks[task_idx];
                if task.stage == CoworkerTaskStage::EffectUnknown || task.unresolved_outbox_id.is_some() {
                    return Err(NativeError::new(
                        NativeErrorCode::AmbiguousWakeupPendingReconciliation,
                        "target task is in EffectUnknown state and requires explicit reconciliation",
                    ));
                }

                let (next_stage, attempt_no, attempt_base_commit) = match request.kind {
                    OutboxDispatchKind::DependencyPreparation => {
                        if task.depends_on.is_empty() {
                            return Err(NativeError::new(
                                NativeErrorCode::ValidationFailed,
                                "root task with empty depends_on does not use DependencyPreparation",
                            ));
                        }
                        let dep_commit = request.dependency_input_commit.as_ref().ok_or_else(|| {
                            NativeError::new(
                                NativeErrorCode::ValidationFailed,
                                "DependencyPreparation requires dependency_input_commit",
                            )
                        })?;
                        let dep_tree = request.dependency_input_tree_sha.as_ref().ok_or_else(|| {
                            NativeError::new(
                                NativeErrorCode::ValidationFailed,
                                "DependencyPreparation requires dependency_input_tree_sha",
                            )
                        })?;
                        validate_exact_40_hex_sha(dep_commit, "dependency_input_commit").map_err(|_| {
                            NativeError::new(
                                NativeErrorCode::ValidationFailed,
                                "dependency_input_commit must be a 40-character lowercase hex SHA-1",
                            )
                        })?;
                        validate_exact_40_hex_sha(dep_tree, "dependency_input_tree_sha").map_err(|_| {
                            NativeError::new(
                                NativeErrorCode::ValidationFailed,
                                "dependency_input_tree_sha must be a 40-character lowercase hex SHA-1",
                            )
                        })?;
                        verify_direct_predecessor_receipts(
                            &task.depends_on,
                            &request.accepted_predecessor_receipts,
                        )?;
                        task.dependency_input_commit = Some(dep_commit.clone());
                        task.dependency_input_tree_sha = Some(dep_tree.clone());
                        task.accepted_predecessor_receipts =
                            request.accepted_predecessor_receipts.clone();
                        (
                            CoworkerTaskStage::ReadyToDispatch,
                            task.current_attempt_no.max(1),
                            dep_commit.clone(),
                        )
                    }
                    OutboxDispatchKind::TaskAttemptDispatch => {
                        if !task.stage.can_transition_to(CoworkerTaskStage::DispatchReserved) {
                            return Err(NativeError::new(
                                NativeErrorCode::ValidationFailed,
                                format!("illegal task transition from `{}` to `dispatch_reserved`", task.stage),
                            ));
                        }
                        let effective_base = if task.depends_on.is_empty() {
                            base_commit
                        } else {
                            verify_direct_predecessor_receipts(
                                &task.depends_on,
                                &task.accepted_predecessor_receipts,
                            )?;
                            task.dependency_input_commit.clone().ok_or_else(|| {
                                NativeError::new(
                                    NativeErrorCode::ValidationFailed,
                                    "dependent task requires prepared dependency_input_commit before TaskAttemptDispatch",
                                )
                            })?
                        };
                        let next_attempt = task.current_attempt_no.checked_add(1).ok_or_else(|| {
                            NativeError::new(NativeErrorCode::ValidationFailed, "attempt_no overflow")
                        })?;
                        if (next_attempt as usize) > MAX_ATTEMPTS_PER_TASK {
                            return Err(NativeError::new(
                                NativeErrorCode::ValidationFailed,
                                "task exceeded maximum attempts per task",
                            ));
                        }
                        task.current_attempt_no = next_attempt;
                        (
                            CoworkerTaskStage::DispatchReserved,
                            next_attempt,
                            effective_base,
                        )
                    }
                    OutboxDispatchKind::TaskReviewDispatch | OutboxDispatchKind::ClosureReviewDispatch => {
                        if !task.stage.can_transition_to(CoworkerTaskStage::ReviewReserved) {
                            return Err(NativeError::new(
                                NativeErrorCode::ValidationFailed,
                                format!("illegal task transition from `{}` to `review_reserved`", task.stage),
                            ));
                        }
                        (
                            CoworkerTaskStage::ReviewReserved,
                            task.current_attempt_no.max(1),
                            task.dependency_input_commit.clone().unwrap_or(base_commit),
                        )
                    }
                    OutboxDispatchKind::TaskIntegration => {
                        if !task.stage.can_transition_to(CoworkerTaskStage::IntegrationReserved)
                            || task.accepted_candidate_id.is_none()
                        {
                            return Err(NativeError::new(
                                NativeErrorCode::ValidationFailed,
                                "TaskIntegration requires an Accepted task with an accepted_candidate_id",
                            ));
                        }
                        (
                            CoworkerTaskStage::IntegrationReserved,
                            task.current_attempt_no.max(1),
                            task.dependency_input_commit.clone().unwrap_or(base_commit),
                        )
                    }
                };

                let next_job_rev = job.job_revision.checked_add(1).ok_or_else(|| {
                    NativeError::new(NativeErrorCode::ValidationFailed, "job_revision overflow")
                })?;
                let outbox_id = deterministic_prefixed_id(
                    "threadlane-native:coworker-outbox:v1",
                    "obx",
                    &[&job.job_id, &task.task_id, &request.request_key],
                );

                if request.kind == OutboxDispatchKind::TaskAttemptDispatch {
                    task.attempts.push(TaskAttemptRef {
                        attempt_no,
                        role: request.role,
                        request_key: request.request_key.clone(),
                        outbox_id: outbox_id.clone(),
                        run_id: None,
                        worker_id: None,
                        task_worktree_id: None,
                        base_commit: attempt_base_commit,
                        head_commit: None,
                        tree_sha: None,
                        diff_digest: None,
                        report_artifact_id: None,
                        observed_model: None,
                        observed_effort: None,
                    });
                }

                task.stage = next_stage;
                if request.kind != OutboxDispatchKind::DependencyPreparation {
                    task.unresolved_outbox_id = Some(outbox_id.clone());
                }

                let outbox_status = if request.kind == OutboxDispatchKind::DependencyPreparation {
                    OutboxDispatchStatus::DispatchedEffectConfirmed
                } else {
                    OutboxDispatchStatus::ReservedBeforeEffect
                };
                let outbox_rec = OutboxDispatchRecord {
                    outbox_id,
                    request_key: request.request_key.clone(),
                    kind: request.kind,
                    task_id: task.task_id.clone(),
                    attempt_no,
                    role: request.role,
                    subject_digest: request.subject_digest.clone(),
                    status: outbox_status,
                    reserved_at_job_revision: next_job_rev,
                    updated_at_job_revision: next_job_rev,
                    run_id: None,
                    worker_id: None,
                    receipt_id: None,
                    diagnostic: None,
                };

                job.job_revision = next_job_rev;
                job.outbox.push(outbox_rec.clone());
                Ok(CoworkerLedgerMutationOutcome::Mutated((
                    job.clone(),
                    outbox_rec,
                )))
            },
        )?;
        Ok(pair)
    }

    /// Finalizes or reconciles a reserved or uncertain [`OutboxDispatchRecord`] and transitions
    /// the bound [`CoworkerTaskRecord`].
    pub fn reconcile_or_finalize_outbox(
        &self,
        request: &FinalizeTaskOutboxRequest,
    ) -> Result<(CoworkerJobRecord, OutboxDispatchRecord), NativeError> {
        validate_bounded_identifier(&request.binding_id, "binding_id", MAX_COWORKER_ID_LEN)?;
        validate_bounded_identifier(&request.job_id, "job_id", MAX_COWORKER_ID_LEN)?;
        validate_bounded_identifier(&request.outbox_id, "outbox_id", MAX_COWORKER_ID_LEN)?;
        if request.expected_job_revision == 0 {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "expected_job_revision must be >= 1",
            ));
        }
        if request.status == OutboxDispatchStatus::ReservedBeforeEffect {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "FinalizeTaskOutboxRequest.status cannot be ReservedBeforeEffect",
            ));
        }

        let (_, pair) = self.transact_binding_ledger(
            &request.binding_id,
            request.expected_ledger_revision,
            |ledger| {
                ledger.ensure_thread_scope(&request.thread_uuid)?;
                let job = ledger
                    .jobs
                    .iter_mut()
                    .find(|j| j.job_id == request.job_id)
                    .ok_or_else(|| {
                        NativeError::new(
                            NativeErrorCode::ValidationFailed,
                            "requested job_id was not found in this binding ledger",
                        )
                    })?;

                let outbox_idx = job
                    .outbox
                    .iter()
                    .position(|o| o.outbox_id == request.outbox_id)
                    .ok_or_else(|| {
                        NativeError::new(
                            NativeErrorCode::ValidationFailed,
                            "requested outbox_id was not found in job.outbox",
                        )
                    })?;

                let existing_outbox = &job.outbox[outbox_idx];
                let task_idx = job
                    .tasks
                    .iter()
                    .position(|t| t.task_id == existing_outbox.task_id)
                    .ok_or_else(|| {
                        NativeError::new(
                            NativeErrorCode::ProtocolError,
                            "outbox record references a missing task_id",
                        )
                    })?;

                if existing_outbox.status == request.status
                    && job.tasks[task_idx].stage == request.next_task_stage
                    && existing_outbox.run_id == request.run_id
                    && existing_outbox.worker_id == request.worker_id
                    && existing_outbox.receipt_id == request.receipt_id
                {
                    return Ok(CoworkerLedgerMutationOutcome::IdempotentExisting((
                        job.clone(),
                        existing_outbox.clone(),
                    )));
                }

                if job.job_revision != request.expected_job_revision {
                    return Err(NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "expected_job_revision does not match current job_revision",
                    ));
                }

                let outbox_kind = job.outbox[outbox_idx].kind;
                let has_active_job_wide_hold =
                    job.holds.iter().any(|h| h.active && h.holds_entire_job);
                let task = &mut job.tasks[task_idx];

                let fast_completed_attempt_to_review = task.stage
                    == CoworkerTaskStage::DispatchReserved
                    && request.next_task_stage == CoworkerTaskStage::ReadyForReview
                    && outbox_kind == OutboxDispatchKind::TaskAttemptDispatch
                    && matches!(
                        request.status,
                        OutboxDispatchStatus::DispatchedEffectConfirmed
                            | OutboxDispatchStatus::ReconciledConfirmed
                    );

                if !task.stage.can_transition_to(request.next_task_stage)
                    && task.stage != request.next_task_stage
                    && !fast_completed_attempt_to_review
                {
                    return Err(NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        format!(
                            "illegal task stage transition from `{}` to `{}`",
                            task.stage, request.next_task_stage
                        ),
                    ));
                }

                if request.next_task_stage == CoworkerTaskStage::ReadyForReview {
                    if !task.active_hold_ids.is_empty() || has_active_job_wide_hold {
                        return Err(NativeError::new(
                            NativeErrorCode::ValidationFailed,
                            "task with an active workflow hold cannot transition to ready_for_review",
                        ));
                    }
                    match outbox_kind {
                        OutboxDispatchKind::TaskAttemptDispatch => {
                            if !matches!(
                                request.status,
                                OutboxDispatchStatus::DispatchedEffectConfirmed
                                    | OutboxDispatchStatus::ReconciledConfirmed
                            ) {
                                return Err(NativeError::new(
                                    NativeErrorCode::ValidationFailed,
                                    "TaskAttemptDispatch transition to ready_for_review requires DispatchedEffectConfirmed or ReconciledConfirmed",
                                ));
                            }
                            let (
                                Some(ref run_id),
                                Some(ref worker_id),
                                Some(ref task_worktree_id),
                                Some(ref report_artifact_id),
                                Some(ref head_commit),
                                Some(ref tree_sha),
                                Some(ref diff_digest),
                            ) = (
                                &request.run_id,
                                &request.worker_id,
                                &request.task_worktree_id,
                                &request.report_artifact_id,
                                &request.head_commit,
                                &request.tree_sha,
                                &request.diff_digest,
                            )
                            else {
                                return Err(NativeError::new(
                                    NativeErrorCode::ValidationFailed,
                                    "transitioning a task attempt to ready_for_review requires complete run_id, worker_id, task_worktree_id, report_artifact_id, head_commit, tree_sha, and diff_digest evidence",
                                ));
                            };
                            validate_bounded_identifier(run_id, "run_id", MAX_COWORKER_ID_LEN)?;
                            validate_bounded_identifier(
                                worker_id,
                                "worker_id",
                                MAX_COWORKER_ID_LEN,
                            )?;
                            validate_bounded_identifier(
                                task_worktree_id,
                                "task_worktree_id",
                                MAX_COWORKER_ID_LEN,
                            )?;
                            validate_bounded_identifier(
                                report_artifact_id,
                                "report_artifact_id",
                                MAX_COWORKER_ID_LEN,
                            )?;
                            validate_exact_40_hex_sha(head_commit, "head_commit").map_err(|_| {
                                NativeError::new(
                                    NativeErrorCode::ValidationFailed,
                                    "head_commit must be a 40-character lowercase hex SHA-1",
                                )
                            })?;
                            validate_exact_40_hex_sha(tree_sha, "tree_sha").map_err(|_| {
                                NativeError::new(
                                    NativeErrorCode::ValidationFailed,
                                    "tree_sha must be a 40-character lowercase hex SHA-1",
                                )
                            })?;
                            validate_sha256_hex_digest(diff_digest, "diff_digest").map_err(
                                |_| {
                                    NativeError::new(
                                        NativeErrorCode::ValidationFailed,
                                        "diff_digest must be a 64-character lowercase hex SHA-256",
                                    )
                                },
                            )?;
                        }
                        OutboxDispatchKind::TaskReviewDispatch => {
                            if !matches!(
                                request.status,
                                OutboxDispatchStatus::RefusedBeforeEffect
                                    | OutboxDispatchStatus::ReconciledNotExecuted
                            ) {
                                return Err(NativeError::new(
                                    NativeErrorCode::ValidationFailed,
                                    "TaskReviewDispatch may only return to ready_for_review on pre-effect refusal or not-executed reconciliation",
                                ));
                            }
                        }
                        _ => {
                            return Err(NativeError::new(
                                NativeErrorCode::ValidationFailed,
                                "outbox kind cannot transition task to ready_for_review",
                            ));
                        }
                    }
                }

                match request.status {
                    OutboxDispatchStatus::EffectUnknown => {
                        if request.next_task_stage != CoworkerTaskStage::EffectUnknown {
                            return Err(NativeError::new(
                                NativeErrorCode::ValidationFailed,
                                "OutboxDispatchStatus::EffectUnknown requires next_task_stage = EffectUnknown",
                            ));
                        }
                        task.unresolved_outbox_id = Some(request.outbox_id.clone());
                    }
                    OutboxDispatchStatus::RefusedBeforeEffect
                    | OutboxDispatchStatus::ReconciledNotExecuted => {
                        if request.run_id.is_some()
                            || request.worker_id.is_some()
                            || request.receipt_id.is_some()
                        {
                            return Err(NativeError::new(
                                NativeErrorCode::ValidationFailed,
                                "pre-effect refusal or not-executed reconciliation cannot carry a sealed run_id, worker_id, or receipt_id",
                            ));
                        }
                        task.unresolved_outbox_id = None;
                    }
                    OutboxDispatchStatus::DispatchedEffectConfirmed
                    | OutboxDispatchStatus::ReconciledConfirmed => {
                        if outbox_kind == OutboxDispatchKind::TaskAttemptDispatch
                            && (request.run_id.is_none() || request.worker_id.is_none())
                        {
                            return Err(NativeError::new(
                                NativeErrorCode::ValidationFailed,
                                "confirmed task attempt dispatch requires sealed run_id and worker_id",
                            ));
                        }
                        if outbox_kind == OutboxDispatchKind::TaskIntegration
                            && request.receipt_id.is_none()
                        {
                            return Err(NativeError::new(
                                NativeErrorCode::ValidationFailed,
                                "confirmed task integration requires a sealed integration receipt_id",
                            ));
                        }
                        task.unresolved_outbox_id = None;
                    }
                    OutboxDispatchStatus::ReservedBeforeEffect => unreachable!(),
                }

                if let Some(attempt) = task
                    .attempts
                    .iter_mut()
                    .find(|a| a.outbox_id == request.outbox_id)
                {
                    if let Some(ref rid) = request.run_id {
                        attempt.run_id = Some(rid.clone());
                    }
                    if let Some(ref wid) = request.worker_id {
                        attempt.worker_id = Some(wid.clone());
                    }
                    if let Some(ref twid) = request.task_worktree_id {
                        attempt.task_worktree_id = Some(twid.clone());
                    }
                    if let Some(ref head) = request.head_commit {
                        attempt.head_commit = Some(head.clone());
                    }
                    if let Some(ref tree) = request.tree_sha {
                        attempt.tree_sha = Some(tree.clone());
                    }
                    if let Some(ref diff) = request.diff_digest {
                        attempt.diff_digest = Some(diff.clone());
                    }
                    if let Some(ref art) = request.report_artifact_id {
                        attempt.report_artifact_id = Some(art.clone());
                    }
                    if let Some(ref obs_m) = request.observed_model {
                        attempt.observed_model = Some(obs_m.clone());
                    }
                    if let Some(ref obs_e) = request.observed_effort {
                        attempt.observed_effort = Some(obs_e.clone());
                    }
                }

                if job.outbox[outbox_idx].kind == OutboxDispatchKind::TaskIntegration
                    && request.next_task_stage == CoworkerTaskStage::Integrated
                {
                    if let (Some(ref acc_id), Some(ref int_receipt)) =
                        (&task.accepted_candidate_id, &request.receipt_id)
                    {
                        if let Some(cand) = task
                            .candidates
                            .iter_mut()
                            .find(|c| &c.candidate_id == acc_id)
                        {
                            cand.integration_receipt_id = Some(int_receipt.clone());
                        }
                    }
                }

                task.stage = request.next_task_stage;
                let next_job_rev = job.job_revision.checked_add(1).ok_or_else(|| {
                    NativeError::new(NativeErrorCode::ValidationFailed, "job_revision overflow")
                })?;
                let outbox_mut = &mut job.outbox[outbox_idx];
                outbox_mut.status = request.status;
                outbox_mut.updated_at_job_revision = next_job_rev;
                outbox_mut.run_id = request.run_id.clone();
                outbox_mut.worker_id = request.worker_id.clone();
                outbox_mut.receipt_id = request.receipt_id.clone();
                outbox_mut.diagnostic = request
                    .diagnostic
                    .as_deref()
                    .map(sanitize_diagnostic_text);

                let updated_outbox = outbox_mut.clone();
                job.job_revision = next_job_rev;
                Ok(CoworkerLedgerMutationOutcome::Mutated((
                    job.clone(),
                    updated_outbox,
                )))
            },
        )?;
        Ok(pair)
    }

    /// Records a committed [`TaskCandidateRef`] and its [`StructuredReviewVerdict`], advancing the
    /// task strictly by the structured verdict (`Accept`, `CorrectInScope`, `ScopeConflict`,
    /// `Blocked`, or `Unclear`) and never by reviewer prose.
    pub fn record_task_candidate_and_verdict(
        &self,
        request: &RecordTaskReviewVerdictRequest,
    ) -> Result<(CoworkerJobRecord, TaskCandidateRef), NativeError> {
        validate_bounded_identifier(&request.binding_id, "binding_id", MAX_COWORKER_ID_LEN)?;
        validate_bounded_identifier(&request.job_id, "job_id", MAX_COWORKER_ID_LEN)?;
        if request.expected_job_revision == 0 {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "expected_job_revision must be >= 1",
            ));
        }
        request.candidate.validate()?;
        let verdict: &StructuredReviewVerdict =
            request.candidate.review_verdict.as_ref().ok_or_else(|| {
                NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "RecordTaskReviewVerdictRequest.candidate requires a StructuredReviewVerdict",
                )
            })?;

        let (_, pair) = self.transact_binding_ledger(
            &request.binding_id,
            request.expected_ledger_revision,
            |ledger| {
                ledger.ensure_thread_scope(&request.thread_uuid)?;
                let job = ledger
                    .jobs
                    .iter_mut()
                    .find(|j| j.job_id == request.job_id)
                    .ok_or_else(|| {
                        NativeError::new(
                            NativeErrorCode::ValidationFailed,
                            "requested job_id was not found in this binding ledger",
                        )
                    })?;

                let policy_digest = job
                    .approval_linkage
                    .as_ref()
                    .map(|l| l.policy_digest.clone())
                    .ok_or_else(|| {
                        NativeError::new(
                            NativeErrorCode::ValidationFailed,
                            "job has no FrozenApprovalLinkage",
                        )
                    })?;
                if !constant_time_str_eq(&verdict.policy_digest, &policy_digest) {
                    return Err(NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "StructuredReviewVerdict.policy_digest does not match job's frozen policy_digest",
                    ));
                }

                let task_idx = job
                    .tasks
                    .iter()
                    .position(|t| t.task_id == request.candidate.task_id)
                    .ok_or_else(|| {
                        NativeError::new(
                            NativeErrorCode::ValidationFailed,
                            "candidate.task_id was not found in job.tasks",
                        )
                    })?;

                if let Some(existing_cand) = job.tasks[task_idx]
                    .candidates
                    .iter()
                    .find(|c| c.candidate_id == request.candidate.candidate_id)
                    .cloned()
                {
                    if existing_cand == request.candidate {
                        return Ok(CoworkerLedgerMutationOutcome::IdempotentExisting((
                            job.clone(),
                            existing_cand,
                        )));
                    }
                    return Err(NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "candidate_id was already recorded with different candidate or verdict fields",
                    ));
                }

                if job.job_revision != request.expected_job_revision {
                    return Err(NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "expected_job_revision does not match current job_revision",
                    ));
                }

                let task = &mut job.tasks[task_idx];
                if task.stage != CoworkerTaskStage::UnderReview {
                    return Err(NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        format!(
                            "task `{}` must be in `under_review` stage to record a review verdict (current: `{}`)",
                            task.task_id, task.stage
                        ),
                    ));
                }
                if task.unresolved_outbox_id.is_some() || !task.active_hold_ids.is_empty() {
                    return Err(NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "cannot record review verdict while task has an unresolved outbox record or active hold",
                    ));
                }
                if task.candidates.len() >= MAX_CANDIDATES_PER_TASK {
                    return Err(NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "task reached maximum candidate capacity",
                    ));
                }

                let target_stage = verdict.target_task_stage();
                if target_stage == CoworkerTaskStage::Accepted {
                    if !verdict.allows_acceptance() || request.candidate.review_receipt_id.is_none()
                    {
                        return Err(NativeError::new(
                            NativeErrorCode::ValidationFailed,
                            "accepting a task candidate requires ReviewVerdictOutcome::Accept and a sealed review_receipt_id",
                        ));
                    }
                    task.accepted_candidate_id = Some(request.candidate.candidate_id.clone());
                } else {
                    task.accepted_candidate_id = None;
                }

                task.active_candidate_id = Some(request.candidate.candidate_id.clone());
                task.candidates.push(request.candidate.clone());
                task.stage = target_stage;

                let next_job_rev = job.job_revision.checked_add(1).ok_or_else(|| {
                    NativeError::new(NativeErrorCode::ValidationFailed, "job_revision overflow")
                })?;

                if verdict.outcome == ReviewVerdictOutcome::ScopeConflict {
                    let hold_id = deterministic_prefixed_id(
                        "threadlane-native:coworker-hold:v1",
                        "hld",
                        &[
                            &job.job_id,
                            &request.candidate.task_id,
                            &verdict.verdict_id,
                        ],
                    );
                    let hold = WorkflowHoldRecord {
                        hold_id,
                        request_key: format!("verdict_hold_{}", verdict.verdict_id),
                        reason: WorkflowHoldReason::ReviewScopeConflict,
                        detail: format!(
                            "scope conflict on candidate `{}`",
                            request.candidate.candidate_id
                        ),
                        directly_affected_task_ids: vec![request.candidate.task_id.clone()],
                        affected_write_paths: verdict.conflicting_paths_or_interfaces.clone(),
                        affected_interfaces: verdict.conflicting_paths_or_interfaces.clone(),
                        holds_entire_job: false,
                        active: true,
                        created_at_job_revision: next_job_rev,
                        resolved_at_job_revision: None,
                        resolution_note: None,
                    };
                    apply_hold_to_job_tasks(job, &hold, next_job_rev)?;
                } else {
                    job.job_revision = next_job_rev;
                }

                Ok(CoworkerLedgerMutationOutcome::Mutated((
                    job.clone(),
                    request.candidate.clone(),
                )))
            },
        )?;
        Ok(pair)
    }

    /// Records an unreviewed [`TaskCandidateRef`] (`review_verdict = None`) and transitions the
    /// task from [`CoworkerTaskStage::ReadyForReview`] to
    /// [`CoworkerTaskStage::CorrectInScopeRequired`] based on a verified
    /// [`CheckFailureCorrectionBasis`].
    ///
    /// Never mints a fake [`StructuredReviewVerdict`] or [`threadlane_operations::ReviewReceipt`].
    /// Duplicate submissions for the same recorded candidate and stage return idempotently without
    /// incrementing `job_revision` or `ledger_revision`.
    pub fn record_task_check_failure_candidate(
        &self,
        request: &RecordTaskCheckFailureCandidateRequest,
    ) -> Result<(CoworkerJobRecord, TaskCandidateRef), NativeError> {
        validate_bounded_identifier(&request.binding_id, "binding_id", MAX_COWORKER_ID_LEN)?;
        validate_bounded_identifier(&request.job_id, "job_id", MAX_COWORKER_ID_LEN)?;
        if request.expected_job_revision == 0 {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "expected_job_revision must be >= 1",
            ));
        }
        request.candidate.validate()?;
        if request.candidate.review_verdict.is_some()
            || request.candidate.review_receipt_id.is_some()
            || request.candidate.integration_receipt_id.is_some()
        {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "RecordTaskCheckFailureCandidateRequest.candidate must be unreviewed (`review_verdict = None`, `review_receipt_id = None`, `integration_receipt_id = None`)",
            ));
        }
        let cand_subject_digest = request
            .candidate
            .review_subject_digest
            .as_ref()
            .ok_or_else(|| {
                NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "RecordTaskCheckFailureCandidateRequest.candidate requires a bound review_subject_digest",
                )
            })?;
        if request.candidate.task_id != request.basis.task_id
            || request.candidate.attempt_no != request.basis.prior_attempt_no
            || request.candidate.candidate_id != request.basis.prior_candidate_id
            || request.candidate.base_commit != request.basis.execution_base_commit
            || request.candidate.head_commit != request.basis.committed_head_commit
            || request.candidate.tree_sha != request.basis.committed_tree_sha
            || cand_subject_digest != &request.basis.prior_review_subject_digest
        {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "RecordTaskCheckFailureCandidateRequest.candidate does not match CheckFailureCorrectionBasis task, attempt, candidate_id, commits, or review_subject_digest",
            ));
        }
        if request.candidate.attempt_no == 0
            || request.candidate.attempt_no > MAX_IN_SCOPE_CORRECTION_ATTEMPTS
        {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!(
                    "check-failure candidate attempt_no {} exceeds maximum correction eligibility {}",
                    request.candidate.attempt_no, MAX_IN_SCOPE_CORRECTION_ATTEMPTS
                ),
            ));
        }

        let policy_store = ControllerPolicyStore::new(&self.plans_dir)?;
        let runtime_state = policy_store
            .get_job_runtime_readonly(&request.binding_id, &request.job_id)?
            .ok_or_else(|| {
                NativeError::new(
                    NativeErrorCode::NotConfigured,
                    "job has no FrozenControllerExecutionEnvelope in ControllerPolicyStore",
                )
            })?;

        let (_, pair) = self.transact_binding_ledger(
            &request.binding_id,
            request.expected_ledger_revision,
            |ledger| {
                ledger.ensure_thread_scope(&request.thread_uuid)?;
                if ledger
                    .jobs
                    .iter()
                    .any(|j| j.supersedes_job_id.as_deref() == Some(request.job_id.as_str()))
                {
                    return Err(NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "cannot record check-failure candidate on a superseded job",
                    ));
                }
                let job = ledger
                    .jobs
                    .iter_mut()
                    .find(|j| j.job_id == request.job_id)
                    .ok_or_else(|| {
                        NativeError::new(
                            NativeErrorCode::ValidationFailed,
                            "requested job_id was not found in this binding ledger",
                        )
                    })?;
                if job.phase != CoworkerJobPhase::Executing {
                    return Err(NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        format!(
                            "job `{}` must be in `executing` phase to record a check-failure candidate (current: `{:?}`)",
                            job.job_id, job.phase
                        ),
                    ));
                }
                if job.holds.iter().any(|h| h.active && h.holds_entire_job) {
                    return Err(NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "cannot record check-failure candidate while an active job-wide hold exists",
                    ));
                }

                runtime_state.envelope.verify_matches_job(job)?;
                let linkage = job.approval_linkage.as_ref().ok_or_else(|| {
                    NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "job has no FrozenApprovalLinkage",
                    )
                })?;
                let cap_envelope = linkage.controller_envelope.as_ref().ok_or_else(|| {
                    NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "job has no FrozenControllerCapabilityEnvelope",
                    )
                })?;
                if !cap_envelope.allow_check_failure_corrections
                    || !runtime_state.envelope.allow_check_failure_corrections
                {
                    return Err(NativeError::new(
                        NativeErrorCode::UnsupportedCapability,
                        "job FrozenControllerCapabilityEnvelope / FrozenControllerExecutionEnvelope does not authorize automatic check-failure corrections (allow_check_failure_corrections == false)",
                    ));
                }
                if request.candidate.attempt_no > cap_envelope.max_in_scope_correction_attempts
                    || request.candidate.attempt_no
                        > runtime_state.envelope.max_in_scope_corrections_per_task
                {
                    return Err(NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "check-failure candidate attempt_no exceeds envelope max_in_scope_corrections_per_task",
                    ));
                }

                let task_idx = job
                    .tasks
                    .iter()
                    .position(|t| t.task_id == request.candidate.task_id)
                    .ok_or_else(|| {
                        NativeError::new(
                            NativeErrorCode::ValidationFailed,
                            "candidate.task_id was not found in job.tasks",
                        )
                    })?;

                let expected_base_commit = if job.tasks[task_idx].depends_on.is_empty() {
                    linkage.project_base.base_commit.clone()
                } else {
                    job.tasks[task_idx]
                        .dependency_input_commit
                        .clone()
                        .ok_or_else(|| {
                            NativeError::new(
                                NativeErrorCode::ValidationFailed,
                                "dependent task is missing dependency_input_commit",
                            )
                        })?
                };
                if request.candidate.base_commit != expected_base_commit
                    || request.basis.execution_base_commit != expected_base_commit
                {
                    return Err(NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "check-failure candidate or basis execution_base_commit does not match task's frozen base commit",
                    ));
                }

                let cand_write_paths: BTreeSet<&str> = request
                    .candidate
                    .write_paths
                    .iter()
                    .map(String::as_str)
                    .collect();
                let task_write_paths: BTreeSet<&str> = job.tasks[task_idx]
                    .write_paths
                    .iter()
                    .map(String::as_str)
                    .collect();
                if cand_write_paths != task_write_paths {
                    return Err(NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "check-failure candidate write_paths do not match task's frozen write_paths",
                    ));
                }

                let prior_artifacts = runtime_state
                    .get_attempt_artifacts(&request.candidate.task_id, request.candidate.attempt_no)
                    .ok_or_else(|| {
                        NativeError::new(
                            NativeErrorCode::ValidationFailed,
                            "missing TaskAttemptExecutionArtifacts in ControllerJobRuntimeState for check-failure candidate",
                        )
                    })?;
                let expected_candidate_id = compute_task_candidate_id(
                    &job.job_id,
                    &request.candidate.task_id,
                    request.candidate.attempt_no,
                    &request.basis.committed_head_commit,
                    &request.basis.committed_tree_sha,
                );
                if prior_artifacts.task_id != request.candidate.task_id
                    || prior_artifacts.attempt_no != request.candidate.attempt_no
                    || prior_artifacts.execution_base_commit != expected_base_commit
                    || prior_artifacts.committed_head_commit.as_deref()
                        != Some(request.basis.committed_head_commit.as_str())
                    || prior_artifacts.committed_tree_sha.as_deref()
                        != Some(request.basis.committed_tree_sha.as_str())
                    || prior_artifacts.candidate_id.as_deref()
                        != Some(request.candidate.candidate_id.as_str())
                    || request.candidate.candidate_id != expected_candidate_id
                {
                    return Err(NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "check-failure candidate or basis does not match sealed TaskAttemptExecutionArtifacts in ControllerJobRuntimeState",
                    ));
                }
                let has_confirmed_task_commit =
                    runtime_state.pending_effects.values().any(|eff| {
                        eff.kind == PendingControllerEffectKind::TaskCommit
                            && eff.job_id == job.job_id
                            && eff.task_id.as_deref() == Some(request.candidate.task_id.as_str())
                            && eff.attempt_no == Some(request.candidate.attempt_no)
                            && eff.status == PendingControllerEffectStatus::EffectConfirmed
                            && eff.external_correlation_id.as_deref()
                                == Some(request.basis.committed_head_commit.as_str())
                    });
                if !has_confirmed_task_commit {
                    return Err(NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "missing confirmed TaskCommit PendingControllerEffect for check-failure candidate head_commit",
                    ));
                }

                let prior_attempt = job.tasks[task_idx]
                    .attempts
                    .iter()
                    .find(|a| {
                        a.attempt_no == request.candidate.attempt_no
                            && a.role == WorkflowRole::Coder
                    })
                    .ok_or_else(|| {
                        NativeError::new(
                            NativeErrorCode::ValidationFailed,
                            "check-failure candidate references an attempt_no not present in task.attempts",
                        )
                    })?;
                if prior_attempt.base_commit != expected_base_commit
                    || prior_attempt.run_id.as_deref() != Some(prior_artifacts.run_id.as_str())
                    || prior_attempt.worker_id.as_deref()
                        != Some(prior_artifacts.worker_id.as_str())
                    || prior_attempt.task_worktree_id.as_deref()
                        != Some(prior_artifacts.task_worktree_id.as_str())
                {
                    return Err(NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "prior TaskAttemptRef for check-failure candidate is missing sealed run_id, worker_id, task_worktree_id, or matching base_commit",
                    ));
                }
                let (Some(ref att_head), Some(ref att_tree)) =
                    (&prior_attempt.head_commit, &prior_attempt.tree_sha)
                else {
                    return Err(NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "prior TaskAttemptRef for check-failure candidate is missing sealed head_commit or tree_sha",
                    ));
                };
                let attempt_head_valid = (att_head == &request.basis.committed_head_commit
                    && att_tree == &request.basis.committed_tree_sha)
                    || (request.candidate.attempt_no == 1 && att_head == &expected_base_commit)
                    || (request.candidate.attempt_no >= 2
                        && job.tasks[task_idx].candidates.iter().any(|c| {
                            c.attempt_no < request.candidate.attempt_no
                                && &c.head_commit == att_head
                                && &c.tree_sha == att_tree
                        }));
                if !attempt_head_valid {
                    return Err(NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "prior TaskAttemptRef head_commit/tree_sha does not match either pre-TaskCommit worker execution head or confirmed TaskCommit head",
                    ));
                }
                let attempt_outbox = job
                    .outbox
                    .iter()
                    .find(|o| o.outbox_id == prior_attempt.outbox_id)
                    .ok_or_else(|| {
                        NativeError::new(
                            NativeErrorCode::ValidationFailed,
                            "prior TaskAttemptRef outbox_id was not found in job.outbox",
                        )
                    })?;
                if attempt_outbox.task_id != request.candidate.task_id
                    || attempt_outbox.attempt_no != request.candidate.attempt_no
                    || attempt_outbox.kind != OutboxDispatchKind::TaskAttemptDispatch
                    || attempt_outbox.role != WorkflowRole::Coder
                    || attempt_outbox.request_key != prior_attempt.request_key
                    || !matches!(
                        attempt_outbox.status,
                        OutboxDispatchStatus::DispatchedEffectConfirmed
                            | OutboxDispatchStatus::ReconciledConfirmed
                    )
                {
                    return Err(NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "prior TaskAttemptDispatch outbox record does not match TaskAttemptRef or is unconfirmed",
                    ));
                }

                if let Some(existing_cand) = job.tasks[task_idx]
                    .candidates
                    .iter()
                    .find(|c| c.candidate_id == request.candidate.candidate_id)
                    .cloned()
                {
                    let task_ref = &job.tasks[task_idx];
                    if existing_cand == request.candidate
                        && task_ref.stage == CoworkerTaskStage::CorrectInScopeRequired
                        && task_ref.current_attempt_no == request.candidate.attempt_no
                        && task_ref.active_candidate_id.as_deref()
                            == Some(request.candidate.candidate_id.as_str())
                        && task_ref.accepted_candidate_id.is_none()
                        && prior_attempt.head_commit.as_deref()
                            == Some(request.basis.committed_head_commit.as_str())
                        && prior_attempt.tree_sha.as_deref()
                            == Some(request.basis.committed_tree_sha.as_str())
                    {
                        request
                            .basis
                            .validate_against_task_and_envelope(task_ref, &runtime_state.envelope)?;
                        return Ok(CoworkerLedgerMutationOutcome::IdempotentExisting((
                            job.clone(),
                            existing_cand,
                        )));
                    }
                    return Err(NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "candidate_id was already recorded with different candidate fields or task stage",
                    ));
                }

                if job.job_revision != request.expected_job_revision {
                    return Err(NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "expected_job_revision does not match current job_revision",
                    ));
                }

                let task = &mut job.tasks[task_idx];
                if task.current_attempt_no != request.candidate.attempt_no {
                    return Err(NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        format!(
                            "task `{}` current_attempt_no ({}) does not match check-failure candidate attempt_no ({})",
                            task.task_id, task.current_attempt_no, request.candidate.attempt_no
                        ),
                    ));
                }
                if task.stage != CoworkerTaskStage::ReadyForReview
                    || !task
                        .stage
                        .can_transition_to(CoworkerTaskStage::CorrectInScopeRequired)
                {
                    return Err(NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        format!(
                            "task `{}` must be in `ready_for_review` stage to record a check-failure candidate (current: `{}`)",
                            task.task_id, task.stage
                        ),
                    ));
                }
                if task.unresolved_outbox_id.is_some() || !task.active_hold_ids.is_empty() {
                    return Err(NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "cannot record check-failure candidate while task has an unresolved outbox record or active hold",
                    ));
                }
                if task.candidates.len() >= MAX_CANDIDATES_PER_TASK {
                    return Err(NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "task reached maximum candidate capacity",
                    ));
                }

                if let Some(att_mut) = task.attempts.iter_mut().find(|a| {
                    a.attempt_no == request.candidate.attempt_no && a.role == WorkflowRole::Coder
                }) {
                    att_mut.head_commit = Some(request.basis.committed_head_commit.clone());
                    att_mut.tree_sha = Some(request.basis.committed_tree_sha.clone());
                }
                request
                    .basis
                    .validate_against_task_and_envelope(task, &runtime_state.envelope)?;

                task.accepted_candidate_id = None;
                task.active_candidate_id = Some(request.candidate.candidate_id.clone());
                task.candidates.push(request.candidate.clone());
                task.stage = CoworkerTaskStage::CorrectInScopeRequired;

                let next_job_rev = job.job_revision.checked_add(1).ok_or_else(|| {
                    NativeError::new(NativeErrorCode::ValidationFailed, "job_revision overflow")
                })?;
                job.job_revision = next_job_rev;

                Ok(CoworkerLedgerMutationOutcome::Mutated((
                    job.clone(),
                    request.candidate.clone(),
                )))
            },
        )?;
        Ok(pair)
    }

    /// Alias for [`Self::record_task_check_failure_candidate`].
    pub fn record_task_check_failure(
        &self,
        request: &RecordTaskCheckFailureCandidateRequest,
    ) -> Result<(CoworkerJobRecord, TaskCandidateRef), NativeError> {
        self.record_task_check_failure_candidate(request)
    }

    /// Records a [`WorkflowHoldRecord`] and places all tasks not proven independent via
    /// [`prove_task_independence`] into [`CoworkerTaskStage::Held`].
    pub fn apply_workflow_hold(
        &self,
        request: &ApplyWorkflowHoldRequest,
    ) -> Result<(CoworkerJobRecord, WorkflowHoldRecord), NativeError> {
        validate_bounded_identifier(&request.binding_id, "binding_id", MAX_COWORKER_ID_LEN)?;
        validate_bounded_identifier(&request.job_id, "job_id", MAX_COWORKER_ID_LEN)?;
        validate_bounded_identifier(&request.request_key, "request_key", MAX_COWORKER_ID_LEN)?;
        validate_bounded_text(&request.detail, "detail")?;
        validate_unique_id_list(
            &request.directly_affected_task_ids,
            "directly_affected_task_ids",
            MAX_WORK_BRIEF_ITEMS,
        )?;
        validate_unique_text_list(
            &request.affected_write_paths,
            "affected_write_paths",
            MAX_WORK_BRIEF_ITEMS,
        )?;
        validate_unique_id_list(
            &request.affected_interfaces,
            "affected_interfaces",
            MAX_WORK_BRIEF_ITEMS,
        )?;
        if request.expected_job_revision == 0 {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "expected_job_revision must be >= 1",
            ));
        }

        let (_, pair) = self.transact_binding_ledger(
            &request.binding_id,
            request.expected_ledger_revision,
            |ledger| {
                ledger.ensure_thread_scope(&request.thread_uuid)?;
                let job = ledger
                    .jobs
                    .iter_mut()
                    .find(|j| j.job_id == request.job_id)
                    .ok_or_else(|| {
                        NativeError::new(
                            NativeErrorCode::ValidationFailed,
                            "requested job_id was not found in this binding ledger",
                        )
                    })?;

                if let Some(existing) = job
                    .holds
                    .iter()
                    .find(|h| h.request_key == request.request_key)
                    .cloned()
                {
                    if existing.reason == request.reason
                        && existing.detail == request.detail
                        && existing.directly_affected_task_ids == request.directly_affected_task_ids
                        && existing.affected_write_paths == request.affected_write_paths
                        && existing.affected_interfaces == request.affected_interfaces
                        && existing.holds_entire_job == request.holds_entire_job
                    {
                        return Ok(CoworkerLedgerMutationOutcome::IdempotentExisting((
                            job.clone(),
                            existing,
                        )));
                    }
                    return Err(NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "hold request_key was already used with different parameters",
                    ));
                }

                if job.job_revision != request.expected_job_revision {
                    return Err(NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "expected_job_revision does not match current job_revision",
                    ));
                }

                let next_job_rev = job.job_revision.checked_add(1).ok_or_else(|| {
                    NativeError::new(NativeErrorCode::ValidationFailed, "job_revision overflow")
                })?;
                let hold_id = deterministic_prefixed_id(
                    "threadlane-native:coworker-hold:v1",
                    "hld",
                    &[&job.job_id, &request.request_key],
                );
                let hold = WorkflowHoldRecord {
                    hold_id,
                    request_key: request.request_key.clone(),
                    reason: request.reason,
                    detail: request.detail.clone(),
                    directly_affected_task_ids: request.directly_affected_task_ids.clone(),
                    affected_write_paths: request.affected_write_paths.clone(),
                    affected_interfaces: request.affected_interfaces.clone(),
                    holds_entire_job: request.holds_entire_job,
                    active: true,
                    created_at_job_revision: next_job_rev,
                    resolved_at_job_revision: None,
                    resolution_note: None,
                };

                apply_hold_to_job_tasks(job, &hold, next_job_rev)?;
                Ok(CoworkerLedgerMutationOutcome::Mutated((job.clone(), hold)))
            },
        )?;
        Ok(pair)
    }

    /// Resolves an active [`WorkflowHoldRecord`] and restores held tasks that are proven
    /// independent of any remaining active holds back to their prior legal stage.
    pub fn resolve_workflow_hold(
        &self,
        request: &ResolveWorkflowHoldRequest,
    ) -> Result<(CoworkerJobRecord, WorkflowHoldRecord), NativeError> {
        validate_bounded_identifier(&request.binding_id, "binding_id", MAX_COWORKER_ID_LEN)?;
        validate_bounded_identifier(&request.job_id, "job_id", MAX_COWORKER_ID_LEN)?;
        validate_bounded_identifier(&request.hold_id, "hold_id", MAX_COWORKER_ID_LEN)?;
        validate_bounded_text(&request.resolution_note, "resolution_note")?;
        if request.expected_job_revision == 0 {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "expected_job_revision must be >= 1",
            ));
        }

        let (_, pair) = self.transact_binding_ledger(
            &request.binding_id,
            request.expected_ledger_revision,
            |ledger| {
                ledger.ensure_thread_scope(&request.thread_uuid)?;
                let job = ledger
                    .jobs
                    .iter_mut()
                    .find(|j| j.job_id == request.job_id)
                    .ok_or_else(|| {
                        NativeError::new(
                            NativeErrorCode::ValidationFailed,
                            "requested job_id was not found in this binding ledger",
                        )
                    })?;

                let hold_idx = job
                    .holds
                    .iter()
                    .position(|h| h.hold_id == request.hold_id)
                    .ok_or_else(|| {
                        NativeError::new(
                            NativeErrorCode::ValidationFailed,
                            "requested hold_id was not found in job.holds",
                        )
                    })?;

                if !job.holds[hold_idx].active {
                    if job.holds[hold_idx].resolution_note.as_deref()
                        == Some(request.resolution_note.as_str())
                    {
                        let existing = job.holds[hold_idx].clone();
                        return Ok(CoworkerLedgerMutationOutcome::IdempotentExisting((
                            job.clone(),
                            existing,
                        )));
                    }
                    return Err(NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "hold is already resolved with a different resolution_note",
                    ));
                }

                if job.job_revision != request.expected_job_revision {
                    return Err(NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "expected_job_revision does not match current job_revision",
                    ));
                }

                let next_job_rev = job.job_revision.checked_add(1).ok_or_else(|| {
                    NativeError::new(NativeErrorCode::ValidationFailed, "job_revision overflow")
                })?;

                job.holds[hold_idx].active = false;
                job.holds[hold_idx].resolved_at_job_revision = Some(next_job_rev);
                job.holds[hold_idx].resolution_note = Some(request.resolution_note.clone());
                let resolved_hold = job.holds[hold_idx].clone();

                for task in &mut job.tasks {
                    task.active_hold_ids.retain(|id| id != &request.hold_id);
                }

                // Restore tasks whose active_hold_ids are now empty and that are proven independent
                // of any remaining active holds.
                let linkage_policy_digest = job
                    .approval_linkage
                    .as_ref()
                    .map(|l| l.policy_digest.clone());
                let job_outbox = job.outbox.clone();
                let mut snapshot_tasks = job.tasks.clone();
                for candidate in &mut snapshot_tasks {
                    if candidate.stage == CoworkerTaskStage::Held
                        && candidate.active_hold_ids.is_empty()
                    {
                        if let Some(prev_stage) = candidate.stage_before_hold {
                            if candidate.stage.can_transition_to(prev_stage)
                                || can_restore_held_integrated_task(
                                    candidate,
                                    &job_outbox,
                                    linkage_policy_digest.as_deref(),
                                )
                            {
                                candidate.stage = prev_stage;
                            }
                        }
                    }
                }
                let snapshot_holds = job.holds.clone();
                for task in &mut job.tasks {
                    if task.stage == CoworkerTaskStage::Held && task.active_hold_ids.is_empty() {
                        let proof =
                            prove_task_independence(task, &snapshot_tasks, &snapshot_holds)?;
                        if proof.independent {
                            let prev = task.stage_before_hold.unwrap_or_else(|| {
                                if task.depends_on.is_empty()
                                    || task.dependency_input_commit.is_some()
                                {
                                    CoworkerTaskStage::ReadyToDispatch
                                } else {
                                    CoworkerTaskStage::PendingDependencies
                                }
                            });
                            if task.stage.can_transition_to(prev)
                                || can_restore_held_integrated_task(
                                    task,
                                    &job_outbox,
                                    linkage_policy_digest.as_deref(),
                                )
                            {
                                task.stage = prev;
                                task.stage_before_hold = None;
                            }
                        } else {
                            for active_hold in snapshot_holds.iter().filter(|h| h.active) {
                                if !task.active_hold_ids.contains(&active_hold.hold_id) {
                                    task.active_hold_ids.push(active_hold.hold_id.clone());
                                }
                            }
                        }
                    }
                }

                if job.phase == CoworkerJobPhase::Held
                    && !job.holds.iter().any(|h| h.active && h.holds_entire_job)
                {
                    job.phase = if job.approval_linkage.is_some() {
                        CoworkerJobPhase::Executing
                    } else if job.work_brief.is_ready_for_execution_planning() {
                        CoworkerJobPhase::Planning
                    } else {
                        CoworkerJobPhase::Briefing
                    };
                }
                job.job_revision = next_job_rev;
                Ok(CoworkerLedgerMutationOutcome::Mutated((
                    job.clone(),
                    resolved_hold,
                )))
            },
        )?;
        Ok(pair)
    }

    /// Assembles, validates, and records a [`CombinedCandidateIdentity`] once all approved tasks
    /// in the job have been accepted and integrated and all check evidence has passed.
    pub fn record_combined_candidate(
        &self,
        request: &RecordCombinedCandidateRequest,
    ) -> Result<(CoworkerJobRecord, CombinedCandidateIdentity), NativeError> {
        validate_bounded_identifier(&request.binding_id, "binding_id", MAX_COWORKER_ID_LEN)?;
        validate_bounded_identifier(&request.job_id, "job_id", MAX_COWORKER_ID_LEN)?;
        validate_bounded_identifier(&request.request_key, "request_key", MAX_COWORKER_ID_LEN)?;
        if request.expected_job_revision == 0 {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "expected_job_revision must be >= 1",
            ));
        }

        let (_, pair) = self.transact_binding_ledger(
            &request.binding_id,
            request.expected_ledger_revision,
            |ledger| {
                ledger.ensure_thread_scope(&request.thread_uuid)?;
                let job = ledger
                    .jobs
                    .iter_mut()
                    .find(|j| j.job_id == request.job_id)
                    .ok_or_else(|| {
                        NativeError::new(
                            NativeErrorCode::ValidationFailed,
                            "requested job_id was not found in this binding ledger",
                        )
                    })?;

                if let Some(existing) = job
                    .combined_candidates
                    .iter()
                    .find(|c| c.request_key == request.request_key)
                    .cloned()
                {
                    if existing.final_commit_sha == request.final_commit_sha
                        && existing.final_tree_sha == request.final_tree_sha
                        && existing.accepted_predecessor_receipts
                            == request.accepted_predecessor_receipts
                        && existing.check_evidence == request.check_evidence
                        && existing.closure_reservation_id == request.closure_reservation_id
                    {
                        return Ok(CoworkerLedgerMutationOutcome::IdempotentExisting((
                            job.clone(),
                            existing,
                        )));
                    }
                    return Err(NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "combined candidate request_key was already used with different parameters",
                    ));
                }

                if job.job_revision != request.expected_job_revision {
                    return Err(NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "expected_job_revision does not match current job_revision",
                    ));
                }
                if job.combined_candidates.len() >= MAX_COMBINED_CANDIDATES_PER_JOB {
                    return Err(NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "job reached maximum combined candidates capacity",
                    ));
                }
                if job.holds.iter().any(|h| h.active) {
                    return Err(NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "cannot record a combined candidate while the job has an active hold",
                    ));
                }

                let linkage = job.approval_linkage.as_ref().ok_or_else(|| {
                    NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "job has no FrozenApprovalLinkage",
                    )
                })?;

                // Verify every approved task in the job is Integrated and matches `accepted_predecessor_receipts`.
                if request.accepted_predecessor_receipts.len() != job.tasks.len() {
                    return Err(NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "CombinedCandidateIdentity.accepted_predecessor_receipts must cover every approved task in the job",
                    ));
                }
                for task in &job.tasks {
                    if task.stage != CoworkerTaskStage::Integrated {
                        return Err(NativeError::new(
                            NativeErrorCode::ValidationFailed,
                            format!(
                                "task `{}` must be in `integrated` stage before recording a combined candidate (current: `{}`)",
                                task.task_id, task.stage
                            ),
                        ));
                    }
                    let acc_id = task.accepted_candidate_id.as_ref().ok_or_else(|| {
                        NativeError::new(
                            NativeErrorCode::ValidationFailed,
                            "integrated task is missing accepted_candidate_id",
                        )
                    })?;
                    let cand = task
                        .candidates
                        .iter()
                        .find(|c| &c.candidate_id == acc_id)
                        .ok_or_else(|| {
                            NativeError::new(
                                NativeErrorCode::ValidationFailed,
                                "integrated task accepted_candidate_id not found in task.candidates",
                            )
                        })?;
                    let receipt_ref = request
                        .accepted_predecessor_receipts
                        .iter()
                        .find(|r| r.task_id == task.task_id)
                        .ok_or_else(|| {
                            NativeError::new(
                                NativeErrorCode::ValidationFailed,
                                format!(
                                    "missing accepted_predecessor_receipts entry for task `{}`",
                                    task.task_id
                                ),
                            )
                        })?;
                    if receipt_ref.candidate_id != cand.candidate_id
                        || receipt_ref.head_commit != cand.head_commit
                        || receipt_ref.tree_sha != cand.tree_sha
                        || cand.review_subject_digest.as_deref()
                            != Some(receipt_ref.review_subject_digest.as_str())
                        || cand.review_receipt_id.as_deref()
                            != Some(receipt_ref.review_receipt_id.as_str())
                        || cand.integration_receipt_id.as_deref()
                            != Some(receipt_ref.integration_receipt_id.as_str())
                    {
                        return Err(NativeError::new(
                            NativeErrorCode::ValidationFailed,
                            format!(
                                "accepted_predecessor_receipts entry for task `{}` does not match its sealed candidate and receipts",
                                task.task_id
                            ),
                        ));
                    }
                }

                let next_job_rev = job.job_revision.checked_add(1).ok_or_else(|| {
                    NativeError::new(NativeErrorCode::ValidationFailed, "job_revision overflow")
                })?;
                let combined_candidate_id = deterministic_prefixed_id(
                    "threadlane-native:coworker-combined-candidate:v1",
                    "cmb",
                    &[&job.job_id, &request.final_tree_sha, &request.request_key],
                );
                let status = if request.closure_reservation_id.is_some() {
                    CombinedCandidateStatus::ClosureReserved
                } else {
                    CombinedCandidateStatus::ChecksVerified
                };

                let mut combined = CombinedCandidateIdentity {
                    combined_candidate_id,
                    request_key: request.request_key.clone(),
                    job_id: job.job_id.clone(),
                    job_revision: next_job_rev,
                    project_base: linkage.project_base.clone(),
                    final_commit_sha: request.final_commit_sha.clone(),
                    final_tree_sha: request.final_tree_sha.clone(),
                    plan_id: linkage.plan_id.clone(),
                    plan_revision: linkage.plan_revision,
                    plan_digest: linkage.plan_digest.clone(),
                    approval_id: linkage.approval_id.clone(),
                    approval_subject_digest: linkage.approval_subject_digest.clone(),
                    scope_policy_revision: linkage.registry_revision,
                    policy_id: linkage.policy_id.clone(),
                    policy_digest: linkage.policy_digest.clone(),
                    accepted_predecessor_receipts: request.accepted_predecessor_receipts.clone(),
                    check_evidence: request.check_evidence.clone(),
                    candidate_digest: String::new(),
                    closure_reservation_id: request.closure_reservation_id.clone(),
                    status,
                };
                combined.candidate_digest = combined.compute_candidate_digest()?;
                combined.validate()?;

                job.job_revision = next_job_rev;
                job.phase = if combined.status == CombinedCandidateStatus::ClosureReserved {
                    CoworkerJobPhase::ClosureReview
                } else {
                    CoworkerJobPhase::AssemblingCandidate
                };
                job.combined_candidates.push(combined.clone());
                Ok(CoworkerLedgerMutationOutcome::Mutated((
                    job.clone(),
                    combined,
                )))
            },
        )?;
        Ok(pair)
    }

    fn read_ledger_unlocked(
        &self,
        expected_binding_id: &str,
        ledger_path: &Path,
    ) -> Result<Option<CoworkerBindingLedger>, NativeError> {
        let Some(bytes) = read_private_file_bounded(
            ledger_path,
            MAX_COWORKER_LEDGER_BYTES,
            "coworker_workflow_ledger",
        )?
        else {
            return Ok(None);
        };
        let ledger: CoworkerBindingLedger = serde_json::from_slice(&bytes).map_err(|_| {
            NativeError::new(
                NativeErrorCode::ProtocolError,
                "failed to parse coworker workflow ledger JSON; existing file preserved untouched",
            )
        })?;
        ledger.validate(expected_binding_id)?;
        Ok(Some(ledger))
    }

    fn write_ledger_unlocked(
        &self,
        binding_id: &str,
        ledger: &CoworkerBindingLedger,
    ) -> Result<(), NativeError> {
        ledger.validate(binding_id)?;
        let bytes = serde_json::to_vec_pretty(ledger).map_err(|_| {
            NativeError::new(
                NativeErrorCode::ProtocolError,
                "failed to serialize coworker workflow ledger JSON",
            )
        })?;
        if bytes.len() > MAX_COWORKER_LEDGER_BYTES {
            return Err(NativeError::new(
                NativeErrorCode::OutputOverflow,
                "coworker workflow ledger exceeds maximum 1 MiB file budget",
            ));
        }
        let filename = ledger_filename_for_binding(binding_id);
        write_private_file_atomic(
            &self.plans_dir,
            &filename,
            &bytes,
            "coworker_workflow_ledger",
        )
        .map(|_| ())
    }
}

fn apply_hold_to_job_tasks(
    job: &mut CoworkerJobRecord,
    hold: &WorkflowHoldRecord,
    next_job_rev: u64,
) -> Result<(), NativeError> {
    if job.holds.len() >= MAX_HOLDS_PER_JOB {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "job reached maximum workflow hold capacity",
        ));
    }
    for affected_tid in &hold.directly_affected_task_ids {
        if !job.tasks.is_empty() && !job.tasks.iter().any(|t| &t.task_id == affected_tid) {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!("hold references unknown task_id `{affected_tid}`"),
            ));
        }
    }

    job.holds.push(hold.clone());
    let snapshot_tasks = job.tasks.clone();
    let snapshot_holds = job.holds.clone();

    for task in &mut job.tasks {
        let proof = prove_task_independence(task, &snapshot_tasks, &snapshot_holds)?;
        if !proof.independent {
            if !task.active_hold_ids.contains(&hold.hold_id) {
                task.active_hold_ids.push(hold.hold_id.clone());
            }
            if task.stage != CoworkerTaskStage::Held
                && task.stage != CoworkerTaskStage::Cancelled
                && task.stage != CoworkerTaskStage::EffectUnknown
                && task.stage.can_transition_to(CoworkerTaskStage::Held)
            {
                task.stage_before_hold = Some(task.stage);
                task.stage = CoworkerTaskStage::Held;
            }
        }
    }

    if hold.holds_entire_job {
        job.phase = CoworkerJobPhase::Held;
    }
    job.job_revision = next_job_rev;
    Ok(())
}

fn can_restore_held_integrated_task(
    task: &CoworkerTaskRecord,
    outbox: &[OutboxDispatchRecord],
    expected_policy_digest: Option<&str>,
) -> bool {
    if task.stage != CoworkerTaskStage::Held
        || task.stage_before_hold != Some(CoworkerTaskStage::Integrated)
        || !task.active_hold_ids.is_empty()
        || task.unresolved_outbox_id.is_some()
        || task.current_attempt_no == 0
    {
        return false;
    }
    if outbox
        .iter()
        .any(|o| o.task_id == task.task_id && o.status.blocks_replay_and_new_dispatch())
    {
        return false;
    }
    let Some(ref accepted_id) = task.accepted_candidate_id else {
        return false;
    };
    if task.active_candidate_id.as_ref() != Some(accepted_id) {
        return false;
    }
    let Some(cand) = task
        .candidates
        .iter()
        .find(|c| &c.candidate_id == accepted_id)
    else {
        return false;
    };
    if cand.validate().is_err()
        || cand.task_id != task.task_id
        || cand.attempt_no != task.current_attempt_no
    {
        return false;
    }
    let Some(ref verdict) = cand.review_verdict else {
        return false;
    };
    if !verdict.allows_acceptance() {
        return false;
    }
    let Some(policy_digest) = expected_policy_digest else {
        return false;
    };
    if !constant_time_str_eq(&verdict.policy_digest, policy_digest) {
        return false;
    }
    let (Some(ref rev_receipt_id), Some(ref rev_subj_digest), Some(ref int_receipt_id)) = (
        &cand.review_receipt_id,
        &cand.review_subject_digest,
        &cand.integration_receipt_id,
    ) else {
        return false;
    };
    if rev_receipt_id.trim().is_empty()
        || rev_subj_digest.trim().is_empty()
        || int_receipt_id.trim().is_empty()
        || !constant_time_str_eq(&verdict.review_subject_digest, rev_subj_digest)
    {
        return false;
    }
    if !task.depends_on.is_empty()
        && (task.dependency_input_commit.is_none()
            || task.dependency_input_tree_sha.is_none()
            || verify_direct_predecessor_receipts(
                &task.depends_on,
                &task.accepted_predecessor_receipts,
            )
            .is_err())
    {
        return false;
    }
    outbox.iter().any(|o| {
        o.task_id == task.task_id
            && o.attempt_no == cand.attempt_no
            && o.kind == OutboxDispatchKind::TaskIntegration
            && matches!(
                o.status,
                OutboxDispatchStatus::DispatchedEffectConfirmed
                    | OutboxDispatchStatus::ReconciledConfirmed
            )
            && o.receipt_id.as_deref() == Some(int_receipt_id.as_str())
    })
}

fn verify_direct_predecessor_receipts(
    depends_on: &[String],
    receipts: &[AcceptedPredecessorReceiptRef],
) -> Result<(), NativeError> {
    if receipts.len() != depends_on.len() {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "accepted_predecessor_receipts count must match task.depends_on count",
        ));
    }
    for dep_id in depends_on {
        let found = receipts
            .iter()
            .find(|r| &r.task_id == dep_id)
            .ok_or_else(|| {
                NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    format!("missing accepted predecessor receipt for dependency `{dep_id}`"),
                )
            })?;
        found.validate()?;
    }
    Ok(())
}

fn validate_existing_private_store_dir(dir: &Path, label: &str) -> Result<PathBuf, NativeError> {
    if dir.as_os_str().is_empty() {
        return Err(NativeError::new(
            NativeErrorCode::InvalidConfig,
            format!("{label} path must not be empty"),
        ));
    }
    let resolved = if dir.is_absolute() {
        dir.to_path_buf()
    } else {
        let cwd = std::env::current_dir().map_err(|_| {
            NativeError::new(
                NativeErrorCode::IoError,
                format!("failed to resolve current working directory for {label}"),
            )
        })?;
        cwd.join(dir)
    };
    let clean = validate_absolute_no_symlink_components(&resolved, label)?;
    let meta = fs::symlink_metadata(&clean).map_err(|err| {
        if err.kind() == std::io::ErrorKind::NotFound {
            NativeError::new(
                NativeErrorCode::NotConfigured,
                format!("required {label} directory does not exist"),
            )
        } else {
            NativeError::new(
                NativeErrorCode::IoError,
                format!("failed to inspect {label} directory metadata"),
            )
        }
    })?;
    if meta.file_type().is_symlink() || !meta.file_type().is_dir() {
        return Err(NativeError::new(
            NativeErrorCode::SecurityViolation,
            format!("{label} must be an existing private non-symlink directory"),
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if (meta.permissions().mode() & 0o077) != 0 {
            return Err(NativeError::new(
                NativeErrorCode::SecurityViolation,
                format!("{label} directory has insecure permissions (expected 0700)"),
            ));
        }
    }
    Ok(clean)
}

fn map_authority_error(err: AuthorityError) -> NativeError {
    match err.code() {
        "security_violation" => NativeError::new(
            NativeErrorCode::SecurityViolation,
            "AuthorityStore reported a security violation",
        ),
        "corrupt_store" => NativeError::new(
            NativeErrorCode::ProtocolError,
            "AuthorityStore reported corrupt state",
        ),
        "io_error" => NativeError::new(
            NativeErrorCode::IoError,
            "AuthorityStore reported an I/O failure",
        ),
        "unsupported_platform" => NativeError::new(
            NativeErrorCode::UnsupportedPlatform,
            "AuthorityStore reported an unsupported platform",
        ),
        code => NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!("AuthorityStore validation failed ({code})"),
        ),
    }
}

fn binding_ledger_tag(binding_id: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"threadlane-native:coworker-workflow-ledger:v1\0");
    hasher.update((binding_id.len() as u64).to_le_bytes());
    hasher.update(binding_id.as_bytes());
    let digest = hasher.finalize();
    let mut out = String::with_capacity(32);
    for b in &digest[..16] {
        let _ = write!(&mut out, "{b:02x}");
    }
    out
}

fn ledger_filename_for_binding(binding_id: &str) -> String {
    let tag = binding_ledger_tag(binding_id);
    format!("coworker-workflow-{tag}.json")
}

fn lock_filename_for_binding(binding_id: &str) -> String {
    let tag = binding_ledger_tag(binding_id);
    format!("coworker-workflow-{tag}.lock")
}

/// Shared in-memory approval candidate construction. No persistence or store locks.
pub(crate) fn prepare_job_approval_in_ledger(
    ledger: &mut CoworkerBindingLedger,
    request: &BindJobApprovalRequest,
    approval: &threadlane_authority::ApprovalRecord,
    reg_snapshot: &threadlane_workflow::RegistrySnapshot,
    plans: &[threadlane_workflow::PlanRevision],
) -> Result<CoworkerLedgerMutationOutcome<CoworkerJobRecord>, NativeError> {
    validate_bounded_identifier(&request.binding_id, "binding_id", MAX_COWORKER_ID_LEN)?;
    validate_bounded_identifier(&request.job_id, "job_id", MAX_COWORKER_ID_LEN)?;
    validate_bounded_identifier(&request.approval_id, "approval_id", MAX_COWORKER_ID_LEN)?;
    if request.expected_job_revision == 0 {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "expected_job_revision must be >= 1",
        ));
    }

    if approval.binding_id != request.binding_id || approval.thread_uuid != request.thread_uuid {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "ApprovalRecord does not belong to the requested binding_id and thread_uuid",
        ));
    }

    let plan_rev = plans
        .iter()
        .find(|p| p.plan_id == approval.plan_id && p.revision == approval.plan_revision)
        .ok_or_else(|| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                "Referenced plan revision is absent",
            )
        })?;
    let latest_plan = plans
        .iter()
        .filter(|p| p.plan_id == approval.plan_id)
        .max_by_key(|p| p.revision)
        .ok_or_else(|| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                "Referenced plan is absent",
            )
        })?;
    if !plan_rev.status.is_active_published() || latest_plan.revision != approval.plan_revision {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "ApprovalRecord plan_revision is not the active published revision",
        ));
    }

    if reg_snapshot.revision != approval.registry_revision {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "current registry revision drifted from the frozen approval registry_revision",
        ));
    }

    let live_base = CanonicalProjectBaseRef::resolve_and_validate(
        &approval.repository_path,
        &approval.base_commit,
    )?;
    let mut linkage = FrozenApprovalLinkage::from_approval_and_policy(
        approval,
        live_base.base_tree_sha.clone(),
        &request.policy,
    )?;
    if let Some(ref envelope) = request.controller_envelope {
        linkage = linkage.with_controller_envelope(envelope.clone(), &request.policy)?;
    }

    let mut unique_roles = BTreeSet::new();
    let mut role_reqs = Vec::new();
    for task in &approval.tasks {
        if unique_roles.insert((task.role, task.difficulty)) {
            role_reqs.push(RolePreflightRequest::new(task.role, Some(task.difficulty)));
        }
    }
    let report = preflight(
        reg_snapshot,
        &PreflightRequest {
            profile_id: Some(approval.profile_id.clone()),
            roles: role_reqs,
        },
        &request.policy,
    )?;
    if !report.ready {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "A1 preflight blocked one or more approved tasks under the ExecutionPolicy",
        ));
    }

    let mut task_records = Vec::with_capacity(approval.tasks.len());
    for approved_task in &approval.tasks {
        let outcome = report
            .roles
            .iter()
            .find(|o| {
                o.role == approved_task.role
                    && o.requested_difficulty == Some(approved_task.difficulty)
            })
            .and_then(|o| o.selected.as_ref())
            .ok_or_else(|| {
                NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "missing A1 preflight selected choice for approved task",
                )
            })?;
        if !approved_task.is_frozen_headless_worker() {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "task lacks frozen headless worker authority",
            ));
        }
        let outcome = crate::profile_flow::normalize_coder_choice(outcome)?;
        validate_non_native_worker_choice(approved_task.role, &outcome)?;
        if outcome.tool_id != approved_task.tool_id
            || outcome.adapter != approved_task.adapter
            || outcome.model_id != approved_task.model_id
            || outcome.model_arg != approved_task.model_arg
            || outcome.effort != approved_task.effort
        {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "approved task model/tool/effort pin drifted from A1 preflight resolution",
            ));
        }
        let iface_spec = request
            .task_interfaces
            .get(&approved_task.task_id)
            .cloned()
            .unwrap_or_default();
        validate_unique_id_list(
            &iface_spec.owned_interfaces,
            "owned_interfaces",
            MAX_WORK_BRIEF_ITEMS,
        )?;
        validate_unique_id_list(
            &iface_spec.consumed_interfaces,
            "consumed_interfaces",
            MAX_WORK_BRIEF_ITEMS,
        )?;

        let initial_stage = if approved_task.depends_on.is_empty() {
            CoworkerTaskStage::ReadyToDispatch
        } else {
            CoworkerTaskStage::PendingDependencies
        };
        task_records.push(CoworkerTaskRecord {
            task_id: approved_task.task_id.clone(),
            task_digest: approved_task.task_digest.clone(),
            role: approved_task.role,
            difficulty: approved_task.difficulty,
            selected_choice: outcome.clone(),
            depends_on: approved_task.depends_on.clone(),
            write_paths: approved_task.allowed_write_paths.clone(),
            owned_interfaces: iface_spec.owned_interfaces,
            consumed_interfaces: iface_spec.consumed_interfaces,
            stage: initial_stage,
            stage_before_hold: None,
            current_attempt_no: 0,
            active_candidate_id: None,
            accepted_candidate_id: None,
            dependency_input_commit: None,
            dependency_input_tree_sha: None,
            accepted_predecessor_receipts: Vec::new(),
            attempts: Vec::new(),
            candidates: Vec::new(),
            active_hold_ids: Vec::new(),
            unresolved_outbox_id: None,
        });
    }

    ledger.ensure_thread_scope(&request.thread_uuid)?;
    if ledger.jobs.iter().any(|other| {
        other.job_id != request.job_id
            && (other
                .planning_result
                .as_ref()
                .map(|pr| pr.plan_id == approval.plan_id)
                .unwrap_or(false)
                || other
                    .approval_linkage
                    .as_ref()
                    .map(|al| al.plan_id == approval.plan_id)
                    .unwrap_or(false))
    }) {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "ApprovalRecord plan_id belongs to a different job in this binding",
        ));
    }

    if ledger
        .jobs
        .iter()
        .any(|other| other.supersedes_job_id.as_deref() == Some(request.job_id.as_str()))
    {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "job has been superseded by an amendment job and cannot bind execution approval",
        ));
    }

    let job = ledger
        .jobs
        .iter_mut()
        .find(|j| j.job_id == request.job_id)
        .ok_or_else(|| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                "requested job_id was not found in this binding ledger",
            )
        })?;

    if let Some(ref existing_linkage) = job.approval_linkage {
        if existing_linkage == &linkage {
            return Ok(CoworkerLedgerMutationOutcome::IdempotentExisting(
                job.clone(),
            ));
        }
        return Err(NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "CoworkerJobRecord.approval_linkage is immutable and cannot be rebound to a different approval or policy",
                    ));
    }

    if job.job_revision != request.expected_job_revision {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "expected_job_revision does not match current job_revision",
        ));
    }
    if !job
        .work_brief
        .is_ready_for_execution_approval_with_planning(job.planning_result.as_ref())
    {
        return Err(NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "WorkBrief has open_questions or unconfirmed proposed_additions and cannot bind execution approval",
                    ));
    }
    if job.work_brief.project_base != live_base {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "ApprovalRecord repository or base_commit does not match WorkBrief.project_base",
        ));
    }
    if job.holds.iter().any(|h| h.active) {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "job has an active hold and cannot bind execution approval until resolved",
        ));
    }

    let mut resolved_tasks = task_records.clone();
    if let Some(ref planning_result) = job.planning_result {
        if approval.plan_id != planning_result.plan_id
            || approval.plan_revision != planning_result.plan_revision
            || approval.plan_digest != planning_result.plan_digest
            || approval.profile_id != planning_result.profile_id
            || approval.registry_revision != planning_result.registry_revision
            || live_base != planning_result.project_base
            || job.work_brief_digest != planning_result.work_brief_digest
        {
            return Err(NativeError::new(
                            NativeErrorCode::ValidationFailed,
                            "ApprovalRecord does not match the job's recorded PlanningResultRef plan id, revision, digest, profile, brief digest, or base",
                        ));
        }
        if !request.task_interfaces.is_empty()
            && !planning_result.task_interfaces.is_empty()
            && request.task_interfaces != planning_result.task_interfaces
        {
            return Err(NativeError::new(
                            NativeErrorCode::ValidationFailed,
                            "BindJobApprovalRequest.task_interfaces drifted from the job's recorded PlanningResultRef.task_interfaces",
                        ));
        }
        if request.task_interfaces.is_empty() && !planning_result.task_interfaces.is_empty() {
            for task_rec in &mut resolved_tasks {
                if let Some(iface) = planning_result.task_interfaces.get(&task_rec.task_id) {
                    task_rec.owned_interfaces = iface.owned_interfaces.clone();
                    task_rec.consumed_interfaces = iface.consumed_interfaces.clone();
                }
            }
        }
    }

    let next_job_rev = job.job_revision.checked_add(1).ok_or_else(|| {
        NativeError::new(NativeErrorCode::ValidationFailed, "job_revision overflow")
    })?;
    job.job_revision = next_job_rev;
    job.phase = CoworkerJobPhase::Executing;
    job.approval_linkage = Some(linkage);
    job.tasks = resolved_tasks;
    Ok(CoworkerLedgerMutationOutcome::Mutated(job.clone()))
}
