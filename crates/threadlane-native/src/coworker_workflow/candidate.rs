//! Verified combined-candidate assembly, check evidence verification, and single-closure
//! reservation/execution over [`threadlane_operations`] and [`CoworkerWorkflowStore`].

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use threadlane_authority::AuthorityStore;
use threadlane_operations::{
    inspect_final_candidate_readonly, load_integration_receipt, load_review_receipt,
    verify_final_candidate_subject_readonly, verify_legacy_base_closure_insufficient,
    ClosureAttemptRecord, ClosureExecutionEvidence, ClosureReservationOutcome,
    ClosureTerminalFailure, ClosureVerdict, ClosureVerdictKind, FinalCandidateClosureStore,
    FinalCandidatePolicyBinding, FinalCandidateSubject, FinalCandidateTaskInput,
    InspectFinalCandidateRequest, MarkClosureSubmittedRequest, MarkClosureUnknownRequest,
    OperationsError, ReconcileClosureAttemptRequest, RecordClosureFailureRequest,
    RecordClosureVerdictRequest, ReserveClosureAttemptRequest, TestEvidenceSummary,
};
use threadlane_workers::{
    resolve_exact_commit_sha, resolve_exact_tree_sha, resolve_model_target,
    run_advisory_worker_with_structured_result, validate_exact_40_hex_sha,
    validate_executable_file, validate_sha256_hex_digest, AdvisoryResponseBudget,
    AdvisoryStructuredExecution, AdvisoryWorkerSpec, ExecutionBudgets, WorkerFilePermissions,
    WorkerProvider, WorkerRunStatus, DEFAULT_GIT_COMMAND_TIMEOUT_MS,
};
use threadlane_workflow::{ToolAdapter, WorkflowRole};

use crate::coworker_workflow::contracts::{
    deterministic_prefixed_id, validate_non_native_worker_choice, AcceptedPredecessorReceiptRef,
    CandidateCheckEvidence, CombinedCandidateIdentity, CombinedCandidateStatus, CoworkerJobPhase,
    CoworkerJobRecord, CoworkerTaskStage, MAX_COWORKER_ID_LEN,
};
use crate::coworker_workflow::controller_policy::{
    verify_live_approval_and_registry, ClosureExecutionArtifactRecord, ControllerPolicyStore,
    FrozenControllerExecutionEnvelope, PendingControllerEffect, PendingControllerEffectKind,
    PendingControllerEffectStatus,
};
use crate::coworker_workflow::store::{
    CoworkerLedgerMutationOutcome, CoworkerWorkflowStore, RecordCombinedCandidateRequest,
};
use crate::error::{NativeError, NativeErrorCode};
use crate::fs_safety::{
    constant_time_str_eq, sanitize_diagnostic_text, validate_bounded_identifier,
};
use crate::profile_flow::SelectedRoleChoice;
use crate::role_flow::{
    read_registry_snapshot_readonly, ClosureSubjectSource, PackageClosureReservation,
    ReservePackageClosureRequest, RoleFlowStore,
};

/// Schema version for structured closure model JSON responses.
pub const STRUCTURED_CLOSURE_RESPONSE_SCHEMA_VERSION: u32 = 1;

/// Request to assemble a verified [`CombinedCandidateIdentity`] and optionally reserve single-closure review.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AssembleVerifiedCandidateRequest {
    /// Control-plane chat binding identifier.
    pub binding_id: String,
    /// Canonical lowercase hyphenated non-nil native Codex thread UUID.
    pub thread_uuid: String,
    /// Target job identifier (`job_<32hex>`).
    pub job_id: String,
    /// Expected current `job_revision` for compare-and-swap.
    pub expected_job_revision: u64,
    /// Expected current binding ledger revision (`None` to skip ledger-level CAS check).
    pub expected_ledger_revision: Option<u64>,
    /// Caller-supplied idempotency key.
    pub request_key: String,
    /// Optional expected 40-character lowercase hex final commit SHA.
    pub final_commit_sha: Option<String>,
    /// Required 40-character lowercase hex final Git tree SHA.
    pub final_tree_sha: String,
    /// Non-empty list of passing candidate check evidence items.
    pub check_evidence: Vec<CandidateCheckEvidence>,
    /// Whether to reserve single-closure review for this candidate.
    pub reserve_package_closure: bool,
    /// Optional role-flow cycle identifier (`flw_<32hex>`) when reserving through [`RoleFlowStore`].
    pub role_flow_cycle_id: Option<String>,
    /// Optional closure task identifier within the cycle or policy.
    pub closure_task_id: Option<String>,
    /// Optional human-readable closure summary or package label.
    pub closure_summary: Option<String>,
}

/// Result of assembling a verified [`CombinedCandidateIdentity`] and optional [`PackageClosureReservation`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AssembleVerifiedCandidateOutcome {
    /// Updated [`CoworkerJobRecord`] after recording the combined candidate.
    pub job: CoworkerJobRecord,
    /// Newly recorded or idempotently returned [`CombinedCandidateIdentity`].
    pub combined_candidate: CombinedCandidateIdentity,
    /// Optional [`PackageClosureReservation`] if reserved via [`RoleFlowStore`].
    pub closure_reservation: Option<PackageClosureReservation>,
}

/// Request to inspect and assemble a verified combined candidate from an owned
/// `WorktreeKind::RunIntegration` checkout using [`threadlane_operations::inspect_final_candidate_readonly`]
/// and [`FinalCandidateClosureStore`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AssembleOperationsFinalCandidateRequest {
    /// Control-plane chat binding identifier.
    pub binding_id: String,
    /// Canonical lowercase hyphenated non-nil native Codex thread UUID.
    pub thread_uuid: String,
    /// Target job identifier (`job_<32hex>`).
    pub job_id: String,
    /// Expected current `job_revision` for compare-and-swap.
    pub expected_job_revision: u64,
    /// Optional expected binding ledger revision.
    pub expected_ledger_revision: Option<u64>,
    /// Idempotency key for candidate assembly and optional closure reservation.
    pub request_key: String,
    /// Canonical private work-area root directory owning the task and integration worktrees.
    pub work_area_root: PathBuf,
    /// Run identifier owning the final `WorktreeKind::RunIntegration` checkout.
    pub run_id: String,
    /// Owned `WorktreeKind::RunIntegration` worktree identifier containing the integrated candidate.
    pub integration_worktree_id: String,
    /// Non-empty list of candidate-level check evidence items.
    pub check_evidence: Vec<CandidateCheckEvidence>,
    /// Logical task identifier for the final closure step.
    pub closure_task_id: String,
    /// Whether to reserve a closure attempt in [`FinalCandidateClosureStore`] immediately.
    pub reserve_closure_attempt: bool,
    /// Wall-clock timestamp (milliseconds since Unix epoch).
    pub reserved_at_ms: i64,
}

/// Outcome of assembling and inspecting a final candidate via [`threadlane_operations`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AssembleOperationsFinalCandidateOutcome {
    /// Updated [`CoworkerJobRecord`].
    pub job: CoworkerJobRecord,
    /// Recorded [`CombinedCandidateIdentity`] in the coworker workflow ledger.
    pub combined_candidate: CombinedCandidateIdentity,
    /// Cryptographically bound [`FinalCandidateSubject`] inspected from the live `RunIntegration` worktree.
    pub final_subject: FinalCandidateSubject,
    /// Optional closure reservation or cache-hit outcome from [`FinalCandidateClosureStore`].
    pub closure_reservation: Option<ClosureReservationOutcome>,
}

/// Structured JSON payload required from a read-only Closure advisory worker.
///
/// The controller never parses 256-byte display summaries or treats positive prose as acceptance.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StructuredClosureModelResponse {
    /// Schema version (`1`).
    pub schema_version: u32,
    /// Exact 64-hex [`FinalCandidateSubject::subject_digest`] evaluated by the closure model.
    pub subject_digest: String,
    /// Exact 40-hex [`FinalCandidateSubject::candidate_tree_sha`] evaluated by the closure model.
    pub candidate_tree_sha: String,
    /// Structured closure verdict classification (`accepted`, `rejected`, `blocked`, `unclear`).
    pub outcome: ClosureVerdictKind,
    /// Bounded diagnostic/summary note from the closure reviewer.
    pub summary: String,
    /// Optional list of concrete findings when `outcome != Accepted`.
    #[serde(default)]
    pub findings: Vec<String>,
}

impl StructuredClosureModelResponse {
    /// Parses and validates a [`StructuredClosureModelResponse`] from a complete
    /// `BoundedAdvisoryResponse` text payload against the expected [`FinalCandidateSubject`].
    pub fn parse_and_verify(
        raw_text: &str,
        expected_subject: &FinalCandidateSubject,
    ) -> Result<Self, NativeError> {
        let trimmed = extract_json_payload(raw_text);
        let parsed: Self = serde_json::from_str(trimmed).map_err(|err| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!(
                    "Closure worker output is not valid StructuredClosureModelResponse JSON: {err}"
                ),
            )
        })?;
        if parsed.schema_version != STRUCTURED_CLOSURE_RESPONSE_SCHEMA_VERSION {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!(
                    "unsupported StructuredClosureModelResponse schema_version {} (expected {STRUCTURED_CLOSURE_RESPONSE_SCHEMA_VERSION})",
                    parsed.schema_version
                ),
            ));
        }
        if !constant_time_str_eq(&parsed.subject_digest, &expected_subject.subject_digest) {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "StructuredClosureModelResponse.subject_digest does not match FinalCandidateSubject.subject_digest",
            ));
        }
        if !constant_time_str_eq(
            &parsed.candidate_tree_sha,
            &expected_subject.candidate_tree_sha,
        ) {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "StructuredClosureModelResponse.candidate_tree_sha does not match FinalCandidateSubject.candidate_tree_sha",
            ));
        }
        if parsed.summary.trim().is_empty() {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "StructuredClosureModelResponse.summary must not be empty",
            ));
        }
        Ok(parsed)
    }
}

/// Request to execute or reuse a single candidate-bound Closure evaluation over a verified
/// [`FinalCandidateSubject`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecuteFinalCandidateClosureRequest {
    /// Control-plane chat binding identifier.
    pub binding_id: String,
    /// Canonical lowercase hyphenated non-nil native Codex thread UUID.
    pub thread_uuid: String,
    /// Target job identifier (`job_<32hex>`).
    pub job_id: String,
    /// Expected current `job_revision` for compare-and-swap.
    pub expected_job_revision: u64,
    /// Optional expected binding ledger revision.
    pub expected_ledger_revision: Option<u64>,
    /// Idempotency command key for the closure attempt.
    pub command_key: String,
    /// Target [` CombinedCandidateIdentity::combined_candidate_id`] on the job.
    pub combined_candidate_id: String,
    /// Verified [`FinalCandidateSubject`] inspected from the owned `RunIntegration` checkout.
    pub final_subject: FinalCandidateSubject,
    /// Wall-clock timestamp (milliseconds since Unix epoch).
    pub current_time_ms: i64,
}

/// Result of executing or reusing a candidate-bound Closure evaluation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecuteFinalCandidateClosureOutcome {
    /// Updated [`CoworkerJobRecord`].
    pub job: CoworkerJobRecord,
    /// Current revision of the [`FinalCandidateClosureStore`] ledger.
    pub closure_ledger_revision: u64,
    /// Persisted [`ClosureAttemptRecord`].
    pub attempt: ClosureAttemptRecord,
    /// Terminal [`ClosureVerdict`] when the closure evaluation completed or hit cache.
    pub verdict: Option<ClosureVerdict>,
    /// `true` when an existing verified `Accepted` closure verdict for the identical subject was reused without invoking a worker.
    pub reused_cached_verdict: bool,
}

/// Service assembling verified combined candidates from integrated task receipts and binding
/// single-closure package review reservations and executions.
#[derive(Debug, Clone)]
pub struct CoworkerCandidateService {
    store: CoworkerWorkflowStore,
    role_flow: RoleFlowStore,
    registry_dir: PathBuf,
}

impl CoworkerCandidateService {
    /// Creates a new [`CoworkerCandidateService`] bound to `store`, `role_flow`, and `registry_dir`.
    pub fn new(
        store: CoworkerWorkflowStore,
        role_flow: RoleFlowStore,
        registry_dir: impl Into<PathBuf>,
    ) -> Result<Self, NativeError> {
        let registry_dir = registry_dir.into();
        let _ = read_registry_snapshot_readonly(&registry_dir)?;
        Ok(Self {
            store,
            role_flow,
            registry_dir,
        })
    }

    /// Returns a reference to the underlying [`CoworkerWorkflowStore`].
    pub fn store(&self) -> &CoworkerWorkflowStore {
        &self.store
    }

    /// Returns a reference to the underlying [`RoleFlowStore`].
    pub fn role_flow(&self) -> &RoleFlowStore {
        &self.role_flow
    }

    /// Returns the path to the profile registry directory.
    pub fn registry_dir(&self) -> &Path {
        &self.registry_dir
    }

    /// Verifies that all approved tasks on `job` are in [`CoworkerTaskStage::Integrated`] with
    /// sealed `review_receipt_id` and `integration_receipt_id`, returning their sorted
    /// [`AcceptedPredecessorReceiptRef`] records.
    pub fn verify_task_candidates_for_job(
        &self,
        job: &CoworkerJobRecord,
    ) -> Result<Vec<AcceptedPredecessorReceiptRef>, NativeError> {
        if job.tasks.is_empty() {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "cannot assemble a combined candidate for a job with no tasks",
            ));
        }
        if job.holds.iter().any(|h| h.active) {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "cannot assemble a combined candidate while the job has an active workflow hold",
            ));
        }

        let mut receipts = Vec::with_capacity(job.tasks.len());
        for task in &job.tasks {
            if task.stage != CoworkerTaskStage::Integrated {
                return Err(NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    format!(
                        "task `{}` is in stage `{}` (expected `integrated` before candidate assembly)",
                        task.task_id, task.stage
                    ),
                ));
            }
            let accepted_id = task.accepted_candidate_id.as_ref().ok_or_else(|| {
                NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    format!(
                        "integrated task `{}` is missing accepted_candidate_id",
                        task.task_id
                    ),
                )
            })?;
            let cand = task
                .candidates
                .iter()
                .find(|c| &c.candidate_id == accepted_id)
                .ok_or_else(|| {
                    NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        format!(
                            "integrated task `{}` accepted_candidate_id `{accepted_id}` was not found in task.candidates",
                            task.task_id
                        ),
                    )
                })?;
            let review_subject_digest = cand.review_subject_digest.as_ref().ok_or_else(|| {
                NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    format!(
                        "integrated task `{}` candidate `{accepted_id}` is missing review_subject_digest",
                        task.task_id
                    ),
                )
            })?;
            let review_receipt_id = cand.review_receipt_id.as_ref().ok_or_else(|| {
                NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    format!(
                        "integrated task `{}` candidate `{accepted_id}` is missing review_receipt_id",
                        task.task_id
                    ),
                )
            })?;
            let integration_receipt_id = cand.integration_receipt_id.as_ref().ok_or_else(|| {
                NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    format!(
                        "integrated task `{}` candidate `{accepted_id}` is missing integration_receipt_id",
                        task.task_id
                    ),
                )
            })?;

            let r = AcceptedPredecessorReceiptRef {
                task_id: task.task_id.clone(),
                candidate_id: cand.candidate_id.clone(),
                head_commit: cand.head_commit.clone(),
                tree_sha: cand.tree_sha.clone(),
                review_subject_digest: review_subject_digest.clone(),
                review_receipt_id: review_receipt_id.clone(),
                integration_receipt_id: integration_receipt_id.clone(),
            };
            r.validate()?;
            receipts.push(r);
        }

        receipts.sort_by(|a, b| a.task_id.cmp(&b.task_id));
        Ok(receipts)
    }

    /// Verifies that `check_evidence` is non-empty, structurally valid, and that every check
    /// passed (`passed == true` and `exit_code == 0`).
    pub fn verify_check_evidence_for_candidate(
        &self,
        check_evidence: &[CandidateCheckEvidence],
    ) -> Result<(), NativeError> {
        if check_evidence.is_empty() {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "combined candidate requires at least one CandidateCheckEvidence record",
            ));
        }
        let mut seen = BTreeSet::new();
        for item in check_evidence {
            if !item.passed || item.exit_code != 0 {
                return Err(NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    format!(
                        "candidate check `{}` failed (passed={}, exit_code={}); cannot assemble verified candidate",
                        item.check_id, item.passed, item.exit_code
                    ),
                ));
            }
            item.validate_passed()?;
            if !seen.insert(item.check_id.clone()) {
                return Err(NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    format!(
                        "duplicate CandidateCheckEvidence.check_id `{}`",
                        item.check_id
                    ),
                ));
            }
        }
        Ok(())
    }

    /// Verifies that `check_evidence` satisfies every frozen check command in `envelope.check_commands`
    /// where `required_for_candidate_closure == true` and references only declared `check_id`s.
    pub fn verify_frozen_candidate_checks(
        &self,
        envelope: &FrozenControllerExecutionEnvelope,
        check_evidence: &[CandidateCheckEvidence],
    ) -> Result<(), NativeError> {
        self.verify_check_evidence_for_candidate(check_evidence)?;
        for item in check_evidence {
            if !envelope.check_commands.contains_key(&item.check_id) {
                return Err(NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    format!(
                        "CandidateCheckEvidence.check_id `{}` is not declared in FrozenControllerExecutionEnvelope.check_commands",
                        item.check_id
                    ),
                ));
            }
        }
        for (check_id, spec) in &envelope.check_commands {
            if spec.required_for_candidate_closure {
                let Some(found) = check_evidence.iter().find(|c| &c.check_id == check_id) else {
                    return Err(NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        format!(
                            "missing required frozen candidate closure check evidence for `{check_id}`"
                        ),
                    ));
                };
                if !found.passed || found.exit_code != 0 {
                    return Err(NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        format!(
                            "required frozen candidate closure check `{check_id}` did not pass"
                        ),
                    ));
                }
            }
        }
        Ok(())
    }

    /// Legacy manual candidate assembly helper retained only for isolated manual API compatibility.
    ///
    /// Automatic controller closure MUST use [`Self::assemble_and_inspect_operations_candidate`]
    /// and [`FinalCandidateClosureStore`] over an owned `WorktreeKind::RunIntegration` checkout.
    /// `RoleFlowStore` `ApprovedCycleBase` reservations are strictly prohibited when a controller
    /// capability envelope is bound or when `final_tree_sha != base_tree_sha`.
    pub fn assemble_verified_candidate(
        &self,
        authority: &AuthorityStore,
        request: &AssembleVerifiedCandidateRequest,
    ) -> Result<AssembleVerifiedCandidateOutcome, NativeError> {
        validate_bounded_identifier(&request.binding_id, "binding_id", MAX_COWORKER_ID_LEN)?;
        validate_bounded_identifier(&request.job_id, "job_id", MAX_COWORKER_ID_LEN)?;
        validate_bounded_identifier(&request.request_key, "request_key", MAX_COWORKER_ID_LEN)?;
        validate_exact_40_hex_sha(&request.final_tree_sha, "final_tree_sha").map_err(|_| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                "final_tree_sha must be a 40-character lowercase hex SHA-1",
            )
        })?;
        if let Some(ref commit_sha) = request.final_commit_sha {
            validate_exact_40_hex_sha(commit_sha, "final_commit_sha").map_err(|_| {
                NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "final_commit_sha must be a 40-character lowercase hex SHA-1",
                )
            })?;
        }

        self.verify_check_evidence_for_candidate(&request.check_evidence)?;

        let ledger = self.store.read_binding_ledger(&request.binding_id)?;
        ensure_ledger_thread_scope(&ledger.thread_uuid, &request.thread_uuid)?;
        let job = ledger
            .jobs
            .iter()
            .find(|j| j.job_id == request.job_id)
            .ok_or_else(|| {
                NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "requested job_id was not found in binding ledger",
                )
            })?;

        let linkage = job.approval_linkage.as_ref().ok_or_else(|| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                "job has no FrozenApprovalLinkage",
            )
        })?;
        let _ = authority
            .get_for_operator(&linkage.approval_id)
            .map_err(|_| {
                NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "frozen approval_id was not found in AuthorityStore",
                )
            })?;

        let repo_path = Path::new(&linkage.project_base.repository_path);
        if let Some(ref commit_sha) = request.final_commit_sha {
            let resolved_commit =
                resolve_exact_commit_sha(repo_path, commit_sha, DEFAULT_GIT_COMMAND_TIMEOUT_MS)
                    .map_err(|_| {
                        NativeError::new(
                            NativeErrorCode::ValidationFailed,
                            "final_commit_sha could not be resolved in the canonical repository",
                        )
                    })?;
            let resolved_tree =
                resolve_exact_tree_sha(repo_path, &resolved_commit, DEFAULT_GIT_COMMAND_TIMEOUT_MS)
                    .map_err(|_| {
                        NativeError::new(
                            NativeErrorCode::ValidationFailed,
                            "failed to resolve tree SHA for final_commit_sha",
                        )
                    })?;
            if resolved_tree != request.final_tree_sha {
                return Err(NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "final_commit_sha tree does not match final_tree_sha",
                ));
            }
        } else {
            let resolved_tree = resolve_exact_tree_sha(
                repo_path,
                &request.final_tree_sha,
                DEFAULT_GIT_COMMAND_TIMEOUT_MS,
            )
            .map_err(|_| {
                NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "final_tree_sha does not exist in the canonical repository",
                )
            })?;
            if resolved_tree != request.final_tree_sha {
                return Err(NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "final_tree_sha resolution mismatch in the canonical repository",
                ));
            }
        }

        let predecessor_receipts = self.verify_task_candidates_for_job(job)?;

        let mut closure_reservation: Option<PackageClosureReservation> = None;
        if request.reserve_package_closure {
            if linkage.controller_envelope.is_some()
                || request.final_tree_sha != linkage.project_base.base_tree_sha
            {
                return Err(NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "legacy RoleFlowStore ApprovedCycleBase closure reservation is prohibited for controller-governed jobs and modified candidate trees; use assemble_and_inspect_operations_candidate with FinalCandidateClosureStore",
                ));
            }
            let cycle_id = request.role_flow_cycle_id.as_ref().ok_or_else(|| {
                NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "role_flow_cycle_id is required when reserve_package_closure is true",
                )
            })?;
            let snapshot = self.role_flow.get_binding_snapshot(&request.binding_id)?;
            let cycle = snapshot
                .cycles
                .iter()
                .find(|c| &c.cycle_id == cycle_id)
                .ok_or_else(|| {
                    NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "role_flow_cycle_id was not found in RoleFlowStore",
                    )
                })?;
            let res = self
                .role_flow
                .reserve_closure(&ReservePackageClosureRequest {
                    binding_id: request.binding_id.clone(),
                    thread_uuid: request.thread_uuid.clone(),
                    cycle_id: cycle_id.clone(),
                    expected_cycle_revision: cycle.cycle_revision,
                    request_key: format!("cls_{}", request.request_key),
                    package_id: request
                        .closure_summary
                        .clone()
                        .unwrap_or_else(|| request.job_id.clone()),
                    source: ClosureSubjectSource::ApprovedCycleBase {
                        subject_digest: linkage.approval_subject_digest.clone(),
                        expected_candidate_tree_sha: Some(request.final_tree_sha.clone()),
                    },
                    task_id: request.closure_task_id.clone(),
                })?;
            closure_reservation = Some(res);
        }

        let (updated_job, combined_candidate) =
            self.store
                .record_combined_candidate(&RecordCombinedCandidateRequest {
                    binding_id: request.binding_id.clone(),
                    thread_uuid: request.thread_uuid.clone(),
                    job_id: request.job_id.clone(),
                    expected_job_revision: request.expected_job_revision,
                    expected_ledger_revision: request.expected_ledger_revision,
                    request_key: request.request_key.clone(),
                    final_commit_sha: request.final_commit_sha.clone(),
                    final_tree_sha: request.final_tree_sha.clone(),
                    accepted_predecessor_receipts: predecessor_receipts,
                    check_evidence: request.check_evidence.clone(),
                    closure_reservation_id: closure_reservation
                        .as_ref()
                        .map(|r| r.reservation_id.clone()),
                })?;

        Ok(AssembleVerifiedCandidateOutcome {
            job: updated_job,
            combined_candidate,
            closure_reservation,
        })
    }

    /// Inspects the live owned `WorktreeKind::RunIntegration` candidate via
    /// [`inspect_final_candidate_readonly`], verifies all predecessor [`ReviewReceipt`] and
    /// [`IntegrationReceipt`] records on disk, rejects legacy base-tree closure substitution via
    /// [`verify_legacy_base_closure_insufficient`], optionally reserves or reuses a closure attempt
    /// in [`FinalCandidateClosureStore`], and records the [`CombinedCandidateIdentity`] in
    /// [`CoworkerWorkflowStore`].
    pub fn assemble_and_inspect_operations_candidate(
        &self,
        authority: &AuthorityStore,
        policy_store: &ControllerPolicyStore,
        request: &AssembleOperationsFinalCandidateRequest,
    ) -> Result<AssembleOperationsFinalCandidateOutcome, NativeError> {
        validate_bounded_identifier(&request.binding_id, "binding_id", MAX_COWORKER_ID_LEN)?;
        validate_bounded_identifier(&request.job_id, "job_id", MAX_COWORKER_ID_LEN)?;
        validate_bounded_identifier(&request.request_key, "request_key", MAX_COWORKER_ID_LEN)?;
        validate_bounded_identifier(&request.run_id, "run_id", MAX_COWORKER_ID_LEN)?;
        validate_bounded_identifier(
            &request.integration_worktree_id,
            "integration_worktree_id",
            MAX_COWORKER_ID_LEN,
        )?;
        validate_bounded_identifier(
            &request.closure_task_id,
            "closure_task_id",
            MAX_COWORKER_ID_LEN,
        )?;

        self.verify_check_evidence_for_candidate(&request.check_evidence)?;

        let ledger = self.store.read_binding_ledger(&request.binding_id)?;
        ensure_ledger_thread_scope(&ledger.thread_uuid, &request.thread_uuid)?;
        let job = ledger
            .jobs
            .iter()
            .find(|j| j.job_id == request.job_id)
            .ok_or_else(|| {
                NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "requested job_id was not found in binding ledger",
                )
            })?;

        let runtime = policy_store
            .read_job_runtime(&request.binding_id, &request.job_id)?
            .ok_or_else(|| {
                NativeError::new(
                    NativeErrorCode::NotConfigured,
                    "ControllerJobRuntimeState with frozen execution envelope is required before assembling operations candidate",
                )
            })?;
        runtime.envelope.verify_matches_job(job)?;
        let linkage = job.approval_linkage.as_ref().ok_or_else(|| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                "job has no FrozenApprovalLinkage",
            )
        })?;
        let approval = authority
            .get_for_operator(&linkage.approval_id)
            .map_err(|_| {
                NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "frozen approval_id was not found in AuthorityStore",
                )
            })?;
        verify_live_approval_and_registry(&approval, job, &runtime.envelope.policy, authority)?;

        if !runtime.envelope.allow_run_integration_execution {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "FrozenControllerExecutionEnvelope does not authorize RunIntegration candidate assembly",
            ));
        }
        self.verify_frozen_candidate_checks(&runtime.envelope, &request.check_evidence)?;

        let closure_choice = resolve_closure_choice_for_envelope(&runtime.envelope)?;

        let predecessor_receipts = self.verify_task_candidates_for_job(job)?;

        let mut ops_predecessors = Vec::with_capacity(job.tasks.len());
        let mut expected_aggregate_write_paths = BTreeSet::new();
        for task in &job.tasks {
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
                        "accepted_candidate_id not found in task.candidates",
                    )
                })?;
            let review_receipt_id = cand.review_receipt_id.as_ref().ok_or_else(|| {
                NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "accepted candidate is missing review_receipt_id",
                )
            })?;
            let integration_receipt_id = cand.integration_receipt_id.as_ref().ok_or_else(|| {
                NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "accepted candidate is missing integration_receipt_id",
                )
            })?;

            let review_receipt = load_review_receipt(&request.work_area_root, review_receipt_id)
                .map_err(map_operations_error)?;
            let integration_receipt =
                load_integration_receipt(&request.work_area_root, integration_receipt_id)
                    .map_err(map_operations_error)?;

            for p in &task.write_paths {
                expected_aggregate_write_paths.insert(p.clone());
            }

            ops_predecessors.push(FinalCandidateTaskInput {
                task_id: review_receipt.task_id.clone(),
                task_digest: task.task_digest.clone(),
                task_worktree_id: review_receipt.task_worktree_id.clone(),
                review_receipt_id: review_receipt.receipt_id.clone(),
                integration_receipt_id: integration_receipt.receipt_id.clone(),
                expected_execution_base_commit: Some(review_receipt.base_commit.clone()),
                expected_allowed_write_paths: Some(
                    task.write_paths.iter().cloned().collect::<BTreeSet<_>>(),
                ),
                expected_provider: review_receipt.policy.requested_provider.clone(),
                expected_model: review_receipt.policy.requested_model.clone(),
                expected_effort: review_receipt.policy.requested_effort.clone(),
            });
        }

        let ops_check_evidence = build_operations_test_evidence(&request.check_evidence)?;
        let closure_task_digest = compute_closure_task_digest(
            &job.job_id,
            &request.closure_task_id,
            &runtime.envelope.envelope_digest,
        );
        let closure_provider = map_tool_adapter_to_provider(closure_choice.adapter)?;
        let resolved_closure_target = resolve_model_target(
            closure_provider,
            &closure_choice.model_arg,
            closure_choice.effort.as_deref(),
        )
        .map_err(|err| NativeError::new(NativeErrorCode::ValidationFailed, err.to_string()))?;

        let policy_binding = FinalCandidatePolicyBinding {
            policy_id: linkage.policy_id.clone(),
            policy_digest: linkage.policy_digest.clone(),
            scope_revision: linkage.plan_revision,
            registry_revision: linkage.registry_revision,
            approval_id: linkage.approval_id.clone(),
            approval_subject_digest: linkage.approval_subject_digest.clone(),
            closure_task_id: request.closure_task_id.clone(),
            closure_task_digest,
            requested_provider: closure_provider.as_str().to_string(),
            requested_model: resolved_closure_target.base_model,
            requested_effort: resolved_closure_target.effort.clone(),
            require_effort_evidence: resolved_closure_target.effort.is_some(),
            require_passing_checks: true,
        };

        let repo_path = PathBuf::from(&linkage.project_base.repository_path);
        let final_subject = inspect_final_candidate_readonly(&InspectFinalCandidateRequest {
            repository_path: repo_path.clone(),
            work_area_root: request.work_area_root.clone(),
            run_id: request.run_id.clone(),
            integration_worktree_id: request.integration_worktree_id.clone(),
            approved_base_commit: linkage.project_base.base_commit.clone(),
            expected_source_head_commit: linkage.project_base.base_commit.clone(),
            expected_candidate_head_commit: None,
            expected_candidate_tree_sha: None,
            predecessor_tasks: ops_predecessors,
            expected_aggregate_write_paths: Some(expected_aggregate_write_paths),
            policy: policy_binding,
            check_evidence: ops_check_evidence,
        })
        .map_err(map_operations_error)?;

        verify_legacy_base_closure_insufficient(
            &linkage.project_base.base_tree_sha,
            &final_subject,
        )
        .map_err(map_operations_error)?;
        verify_legacy_base_closure_insufficient(&linkage.approval_subject_digest, &final_subject)
            .map_err(map_operations_error)?;

        let mut closure_reservation = None;
        if request.reserve_closure_attempt {
            let closure_store =
                FinalCandidateClosureStore::open(&repo_path, &request.work_area_root)
                    .map_err(map_operations_error)?;
            let current_closure_ledger = closure_store
                .inspect_ledger_readonly()
                .map_err(map_operations_error)?;
            let outcome = closure_store
                .reserve_closure_attempt(&ReserveClosureAttemptRequest {
                    expected_ledger_revision: current_closure_ledger.ledger_revision,
                    command_key: request.request_key.clone(),
                    attempt_id: None,
                    subject: final_subject.clone(),
                    legacy_base_tree_digest: Some(linkage.project_base.base_tree_sha.clone()),
                    reserved_at_ms: request.reserved_at_ms,
                })
                .map_err(map_operations_error)?;
            closure_reservation = Some(outcome);
        }

        let closure_reservation_id = closure_reservation
            .as_ref()
            .map(|o| o.attempt().attempt_id.clone());

        let (updated_job, combined_candidate) =
            self.store
                .record_combined_candidate(&RecordCombinedCandidateRequest {
                    binding_id: request.binding_id.clone(),
                    thread_uuid: request.thread_uuid.clone(),
                    job_id: request.job_id.clone(),
                    expected_job_revision: request.expected_job_revision,
                    expected_ledger_revision: request.expected_ledger_revision,
                    request_key: request.request_key.clone(),
                    final_commit_sha: Some(final_subject.candidate_head_commit.clone()),
                    final_tree_sha: final_subject.candidate_tree_sha.clone(),
                    accepted_predecessor_receipts: predecessor_receipts,
                    check_evidence: request.check_evidence.clone(),
                    closure_reservation_id: closure_reservation_id.clone(),
                })?;

        let () = policy_store.mutate_job_runtime_for_operator(
            &request.binding_id,
            &request.job_id,
            |state| {
                state.candidate_check_evidence = request.check_evidence.clone();
                state.final_candidate_subject = Some(final_subject.clone());
                if let Some(ref cid) = closure_reservation_id {
                    state.closure_attempt_id = Some(cid.clone());
                }
                Ok(())
            },
        )?;

        Ok(AssembleOperationsFinalCandidateOutcome {
            job: updated_job,
            combined_candidate,
            final_subject,
            closure_reservation,
        })
    }

    /// Reserves or reuses a single candidate-bound Closure evaluation in
    /// [`FinalCandidateClosureStore`], invokes the read-only Closure worker via
    /// [`run_advisory_worker_with_structured_result`] if not already cached, records the terminal
    /// [`ClosureVerdict`] (with `grants_merge_or_push_authority: false` and
    /// `substitutes_for_required_checks: false`), and updates the [`CoworkerJobRecord`] phase.
    ///
    /// If an identical verified [`FinalCandidateSubject`] already has an `Accepted` verdict in
    /// [`FinalCandidateClosureStore`], it reuses the cached verdict without dispatching a worker.
    /// If an unresolved attempt (`Reserved`, `Submitted`, or `OutcomeUnknown`) exists under a
    /// different `command_key`, reservation fails closed without bypassing the unresolved attempt.
    pub async fn execute_final_candidate_closure(
        &self,
        authority: &AuthorityStore,
        policy_store: &ControllerPolicyStore,
        request: &ExecuteFinalCandidateClosureRequest,
    ) -> Result<ExecuteFinalCandidateClosureOutcome, NativeError> {
        validate_bounded_identifier(&request.binding_id, "binding_id", MAX_COWORKER_ID_LEN)?;
        validate_bounded_identifier(&request.job_id, "job_id", MAX_COWORKER_ID_LEN)?;
        validate_bounded_identifier(&request.command_key, "command_key", MAX_COWORKER_ID_LEN)?;
        validate_bounded_identifier(
            &request.combined_candidate_id,
            "combined_candidate_id",
            MAX_COWORKER_ID_LEN,
        )?;

        verify_final_candidate_subject_readonly(&request.final_subject)
            .map_err(map_operations_error)?;

        let ledger = self.store.read_binding_ledger(&request.binding_id)?;
        ensure_ledger_thread_scope(&ledger.thread_uuid, &request.thread_uuid)?;
        let job = ledger
            .jobs
            .iter()
            .find(|j| j.job_id == request.job_id)
            .ok_or_else(|| {
                NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "requested job_id was not found in binding ledger",
                )
            })?;

        if job.job_revision != request.expected_job_revision {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "expected_job_revision does not match current job_revision",
            ));
        }
        if job.holds.iter().any(|h| h.active) {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "cannot execute Closure while the job has an active workflow hold",
            ));
        }

        let runtime = policy_store
            .read_job_runtime(&request.binding_id, &request.job_id)?
            .ok_or_else(|| {
                NativeError::new(
                    NativeErrorCode::NotConfigured,
                    "ControllerJobRuntimeState is not configured for this job",
                )
            })?;
        if runtime.stop_requested {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "cannot execute Closure while controller stop_requested is active",
            ));
        }
        runtime.envelope.verify_matches_job(job)?;
        let linkage = job.approval_linkage.as_ref().ok_or_else(|| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                "job has no FrozenApprovalLinkage",
            )
        })?;
        let approval = authority
            .get_for_operator(&linkage.approval_id)
            .map_err(|_| {
                NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "frozen approval_id was not found in AuthorityStore",
                )
            })?;
        verify_live_approval_and_registry(&approval, job, &runtime.envelope.policy, authority)?;

        let combined_cand = job
            .combined_candidates
            .iter()
            .find(|c| c.combined_candidate_id == request.combined_candidate_id)
            .ok_or_else(|| {
                NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "combined_candidate_id was not found on job",
                )
            })?;
        if combined_cand
            .closure_reservation_id
            .as_deref()
            .is_some_and(|id| id.starts_with("cls_"))
        {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "legacy RoleFlowStore ApprovedCycleBase closure reservation cannot satisfy automatic final candidate closure",
            ));
        }
        if combined_cand.final_tree_sha != request.final_subject.candidate_tree_sha
            || combined_cand.final_commit_sha.as_deref()
                != Some(request.final_subject.candidate_head_commit.as_str())
        {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "combined_candidate tree or commit on job does not match FinalCandidateSubject",
            ));
        }
        verify_legacy_base_closure_insufficient(
            &linkage.project_base.base_tree_sha,
            &request.final_subject,
        )
        .map_err(map_operations_error)?;
        verify_legacy_base_closure_insufficient(
            &linkage.approval_subject_digest,
            &request.final_subject,
        )
        .map_err(map_operations_error)?;
        self.verify_frozen_candidate_checks(&runtime.envelope, &combined_cand.check_evidence)?;
        let expected_ops_checks = build_operations_test_evidence(&combined_cand.check_evidence)?;
        if expected_ops_checks != request.final_subject.check_evidence {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "FinalCandidateSubject.check_evidence does not match frozen combined_candidate check evidence",
            ));
        }

        let closure_choice = resolve_closure_choice_for_envelope(&runtime.envelope)?;
        let provider = map_tool_adapter_to_provider(closure_choice.adapter)?;
        let resolved_closure_target = resolve_model_target(
            provider,
            &closure_choice.model_arg,
            closure_choice.effort.as_deref(),
        )
        .map_err(|err| NativeError::new(NativeErrorCode::ValidationFailed, err.to_string()))?;
        verify_subject_policy_matches_envelope(
            &request.final_subject,
            linkage,
            provider,
            &resolved_closure_target.base_model,
            resolved_closure_target.effort.as_deref(),
        )?;
        let closure_budget = runtime.envelope.closure_budget()?.clone();

        let closure_store = FinalCandidateClosureStore::open(
            &request.final_subject.repository_path,
            &request.final_subject.work_area_root,
        )
        .map_err(map_operations_error)?;
        let initial_closure_ledger = closure_store
            .inspect_ledger_readonly()
            .map_err(map_operations_error)?;
        let has_cached_accepted = initial_closure_ledger.attempts.iter().any(|a| {
            a.subject_digest == request.final_subject.subject_digest
                && a.state == threadlane_operations::ClosureAttemptState::Terminal
                && a.verdict
                    .as_ref()
                    .is_some_and(|v| v.kind == ClosureVerdictKind::Accepted)
        });
        let has_idempotent_reserved = initial_closure_ledger.attempts.iter().any(|a| {
            a.subject_digest == request.final_subject.subject_digest
                && a.state == threadlane_operations::ClosureAttemptState::Reserved
                && a.command_key == request.command_key
        });
        if !has_cached_accepted && !has_idempotent_reserved {
            runtime.ensure_role_budget_available(WorkflowRole::Closure)?;
        }

        let reservation_outcome = closure_store
            .reserve_closure_attempt(&ReserveClosureAttemptRequest {
                expected_ledger_revision: initial_closure_ledger.ledger_revision,
                command_key: request.command_key.clone(),
                attempt_id: None,
                subject: request.final_subject.clone(),
                legacy_base_tree_digest: Some(request.final_subject.approved_base_tree_sha.clone()),
                reserved_at_ms: request.current_time_ms,
            })
            .map_err(map_operations_error)?;

        if let ClosureReservationOutcome::CacheHit {
            ledger_revision,
            attempt,
            verdict,
        } = reservation_outcome
        {
            let updated_job = self.record_closure_outcome_on_job(
                &request.binding_id,
                &request.thread_uuid,
                &request.job_id,
                request.expected_job_revision,
                request.expected_ledger_revision,
                &request.combined_candidate_id,
                &attempt.attempt_id,
                Some(&verdict),
                None,
            )?;
            return Ok(ExecuteFinalCandidateClosureOutcome {
                job: updated_job,
                closure_ledger_revision: ledger_revision,
                attempt,
                verdict: Some(*verdict),
                reused_cached_verdict: true,
            });
        }

        let reserved_ledger_rev = reservation_outcome.ledger_revision();
        let reserved_attempt = reservation_outcome.attempt().clone();

        let effect_id = deterministic_prefixed_id(
            "threadlane-native:controller-closure-effect:v1",
            "eff",
            &[
                &request.job_id,
                &reserved_attempt.attempt_id,
                &request.command_key,
            ],
        );
        let external_request_id = deterministic_prefixed_id(
            "threadlane-native:closure-req:v1",
            "crq",
            &[
                &request.job_id,
                &reserved_attempt.attempt_id,
                &request.final_subject.subject_digest,
            ],
        );

        let executable = resolve_tool_executable(authority, &closure_choice.tool_id)?;

        // Record intent-before-effect in ControllerPolicyStore and reserve 1 closure call budget.
        let () = policy_store.mutate_job_runtime_for_operator(
            &request.binding_id,
            &request.job_id,
            |state| {
                if !state.pending_effects.contains_key(&effect_id) {
                    let _ = state.reserve_role_call(WorkflowRole::Closure)?;
                }
                state.closure_attempt_id = Some(reserved_attempt.attempt_id.clone());
                state.pending_effects.insert(
                    effect_id.clone(),
                    PendingControllerEffect {
                        effect_id: effect_id.clone(),
                        request_key: request.command_key.clone(),
                        job_id: request.job_id.clone(),
                        task_id: None,
                        attempt_no: None,
                        kind: PendingControllerEffectKind::ClosureInvocation,
                        status: PendingControllerEffectStatus::ReservedBeforeEffect,
                        subject_digest: request.final_subject.subject_digest.clone(),
                        external_correlation_id: Some(reserved_attempt.attempt_id.clone()),
                        created_at_ms: request.current_time_ms.max(0) as u64,
                        updated_at_ms: request.current_time_ms.max(0) as u64,
                        diagnostic: None,
                    },
                );
                Ok(())
            },
        )?;

        let (submitted_ledger_rev, _submitted_attempt) = closure_store
            .mark_closure_attempt_submitted(&MarkClosureSubmittedRequest {
                expected_ledger_revision: reserved_ledger_rev,
                attempt_id: reserved_attempt.attempt_id.clone(),
                command_key: request.command_key.clone(),
                expected_subject_digest: request.final_subject.subject_digest.clone(),
                external_request_id: external_request_id.clone(),
                submitted_at_ms: request.current_time_ms,
            })
            .map_err(map_operations_error)?;

        let prompt = build_closure_worker_prompt(&request.final_subject);
        let worker_id = deterministic_prefixed_id(
            "threadlane-native:closure-worker:v1",
            "wrk",
            &[&request.job_id, &reserved_attempt.attempt_id],
        );
        let mut allowed_models = BTreeSet::new();
        allowed_models.insert(closure_choice.model_arg.clone());
        let advisory_spec = AdvisoryWorkerSpec {
            provider,
            executable,
            model: closure_choice.model_arg.clone(),
            effort: closure_choice.effort.clone(),
            allowed_models,
            session_id: None,
            standalone: provider == WorkerProvider::OpenCodeV2,
            working_directory: request.final_subject.integration_checkout_path.clone(),
            allowed_write_paths: BTreeSet::new(),
            prompt,
            file_permissions: WorkerFilePermissions::ReadOnly,
            budgets: ExecutionBudgets {
                timeout_ms: closure_budget.max_timeout_ms_per_call,
                max_stdout_bytes: closure_budget.max_stdout_bytes_per_call,
                max_stderr_bytes: closure_budget.max_stderr_bytes_per_call,
                ..ExecutionBudgets::default()
            },
        };
        let response_budget =
            AdvisoryResponseBudget::new(closure_budget.max_structured_response_bytes);
        response_budget
            .validate()
            .map_err(|err| NativeError::new(NativeErrorCode::InvalidConfig, err.to_string()))?;

        let structured_exec =
            match run_advisory_worker_with_structured_result(&advisory_spec, response_budget).await
            {
                Ok(exec) => exec,
                Err(worker_err) => {
                    let reason = format!(
                        "closure worker invocation failed with uncertain outcome: {worker_err}"
                    );
                    return self.record_unknown_closure_outcome(
                        policy_store,
                        &closure_store,
                        request,
                        submitted_ledger_rev,
                        &reserved_attempt.attempt_id,
                        &effect_id,
                        &reason,
                    );
                }
            };

        let duration_ms = structured_exec.execution.duration_ms;
        let actual_observed_model = structured_exec
            .execution
            .effective_observed_model()
            .map(str::to_string);
        let actual_observed_effort = structured_exec
            .execution
            .effective_observed_effort()
            .map(str::to_string);
        let recorded_at_ms = now_epoch_ms().max(request.current_time_ms);

        // Update wall-clock completion counter and persist private execution artifact for recovery.
        let cumulative_runtime_ms = policy_store.mutate_job_runtime_for_operator(
            &request.binding_id,
            &request.job_id,
            |state| {
                state.record_role_completion(WorkflowRole::Closure, duration_ms);
                state.closure_artifacts.insert(
                    reserved_attempt.attempt_id.clone(),
                    ClosureExecutionArtifactRecord {
                        attempt_id: reserved_attempt.attempt_id.clone(),
                        command_key: request.command_key.clone(),
                        subject_digest: request.final_subject.subject_digest.clone(),
                        candidate_tree_sha: request.final_subject.candidate_tree_sha.clone(),
                        execution_report: structured_exec.execution.clone(),
                        advisory_response: structured_exec.response.clone(),
                        observed_model: actual_observed_model.clone(),
                        observed_effort: actual_observed_effort.clone(),
                        fenced_from_promotion: false,
                        diagnostic: structured_exec
                            .response_error
                            .as_ref()
                            .map(|e| e.message.clone()),
                        recorded_at_ms: recorded_at_ms.max(0) as u64,
                    },
                );
                Ok(state
                    .role_usage
                    .get(&WorkflowRole::Closure)
                    .map_or(duration_ms, |u| u.total_runtime_ms))
            },
        )?;

        if structured_exec.execution.cleanup_incomplete
            || structured_exec.execution.status == WorkerRunStatus::IncompleteCleanup
        {
            let reason =
                "closure worker process-group cleanup remained incomplete after exit".to_string();
            return self.record_unknown_closure_outcome(
                policy_store,
                &closure_store,
                request,
                submitted_ledger_rev,
                &reserved_attempt.attempt_id,
                &effect_id,
                &reason,
            );
        }

        if duration_ms > closure_budget.max_timeout_ms_per_call
            || cumulative_runtime_ms > closure_budget.max_total_runtime_ms
        {
            let reason = format!(
                "closure worker exceeded frozen runtime budget (call {} ms / max {} ms, cumulative {} ms / max {} ms)",
                duration_ms,
                closure_budget.max_timeout_ms_per_call,
                cumulative_runtime_ms,
                closure_budget.max_total_runtime_ms
            );
            return self.record_local_terminal_closure_failure(
                policy_store,
                &closure_store,
                request,
                submitted_ledger_rev,
                &reserved_attempt.attempt_id,
                &effect_id,
                "role_runtime_budget_exceeded",
                &reason,
                &structured_exec,
                actual_observed_model,
                actual_observed_effort,
                false,
                recorded_at_ms,
            );
        }

        let verified_response = match (
            structured_exec
                .validate_verified_response(closure_budget.max_structured_response_bytes),
            structured_exec
                .verified_response_within_budget(closure_budget.max_structured_response_bytes),
        ) {
            (Ok(resp), Some(_))
                if structured_exec.execution.status == WorkerRunStatus::Succeeded
                    && structured_exec
                        .execution
                        .has_complete_natural_completion_evidence()
                    && structured_exec.execution.has_satisfied_model_evidence()
                    && structured_exec.execution.has_satisfied_effort_evidence() =>
            {
                resp
            }
            _ => {
                let (failure_code, reason) =
                    classify_local_closure_worker_failure(&structured_exec);
                return self.record_local_terminal_closure_failure(
                    policy_store,
                    &closure_store,
                    request,
                    submitted_ledger_rev,
                    &reserved_attempt.attempt_id,
                    &effect_id,
                    failure_code,
                    &reason,
                    &structured_exec,
                    actual_observed_model,
                    actual_observed_effort,
                    false,
                    recorded_at_ms,
                );
            }
        };

        verified_response
            .validate_integrity(closure_budget.max_structured_response_bytes)
            .map_err(|err| NativeError::new(NativeErrorCode::ValidationFailed, err.to_string()))?;

        let parsed_response = match StructuredClosureModelResponse::parse_and_verify(
            &verified_response.text,
            &request.final_subject,
        ) {
            Ok(resp) => resp,
            Err(parse_err) => {
                let reason = format!(
                    "closure worker output failed structured JSON verification: {}",
                    parse_err.message
                );
                return self.record_local_terminal_closure_failure(
                    policy_store,
                    &closure_store,
                    request,
                    submitted_ledger_rev,
                    &reserved_attempt.attempt_id,
                    &effect_id,
                    "response_validation_failed",
                    &reason,
                    &structured_exec,
                    actual_observed_model,
                    actual_observed_effort,
                    false,
                    recorded_at_ms,
                );
            }
        };

        let Some(ref observed_model) = actual_observed_model else {
            return self.record_local_terminal_closure_failure(
                policy_store,
                &closure_store,
                request,
                submitted_ledger_rev,
                &reserved_attempt.attempt_id,
                &effect_id,
                "model_evidence_failed",
                "closure worker did not emit verified model evidence",
                &structured_exec,
                actual_observed_model,
                actual_observed_effort,
                false,
                recorded_at_ms,
            );
        };
        if observed_model != &request.final_subject.policy.requested_model {
            let reason = format!(
                "closure observed_model `{observed_model}` does not match requested_model `{}`",
                request.final_subject.policy.requested_model
            );
            return self.record_local_terminal_closure_failure(
                policy_store,
                &closure_store,
                request,
                submitted_ledger_rev,
                &reserved_attempt.attempt_id,
                &effect_id,
                "model_evidence_failed",
                &reason,
                &structured_exec,
                actual_observed_model,
                actual_observed_effort,
                false,
                recorded_at_ms,
            );
        }
        if request.final_subject.policy.require_effort_evidence
            && actual_observed_effort.as_deref()
                != request.final_subject.policy.requested_effort.as_deref()
        {
            let reason = format!(
                "closure observed_effort `{:?}` does not match requested_effort `{:?}`",
                actual_observed_effort, request.final_subject.policy.requested_effort
            );
            return self.record_local_terminal_closure_failure(
                policy_store,
                &closure_store,
                request,
                submitted_ledger_rev,
                &reserved_attempt.attempt_id,
                &effect_id,
                "effort_evidence_failed",
                &reason,
                &structured_exec,
                actual_observed_model,
                actual_observed_effort,
                false,
                recorded_at_ms,
            );
        }

        // Re-validate live candidate subject, active holds, stop_requested, and current job_revision
        // AFTER awaited worker completion before promoting acceptance or job completion.
        if let Err(fence_reason) =
            self.check_post_completion_fencing(authority, policy_store, request)
        {
            return self.record_local_terminal_closure_failure(
                policy_store,
                &closure_store,
                request,
                submitted_ledger_rev,
                &reserved_attempt.attempt_id,
                &effect_id,
                "post_completion_fenced",
                &fence_reason,
                &structured_exec,
                actual_observed_model,
                actual_observed_effort,
                true,
                recorded_at_ms,
            );
        }

        let evidence_id = deterministic_prefixed_id(
            "threadlane-native:closure-evidence:v1",
            "cev",
            &[
                &reserved_attempt.attempt_id,
                &verified_response.sha256_digest,
            ],
        );
        let handoff_receipt_id = deterministic_prefixed_id(
            "threadlane-native:closure-handoff:v1",
            "rcp",
            &[&request.job_id, &reserved_attempt.attempt_id, &worker_id],
        );
        let execution_evidence = ClosureExecutionEvidence {
            evidence_id,
            producer_run_id: request.final_subject.run_id.clone(),
            producer_worker_id: Some(worker_id.clone()),
            handoff_receipt_id,
            observed_provider: provider.as_str().to_string(),
            observed_model: observed_model.clone(),
            observed_effort: actual_observed_effort.clone(),
            transcript_or_event_digest: verified_response.sha256_digest.clone(),
            grants_merge_or_push_authority: false,
            substitutes_for_required_checks: false,
        };

        let (terminal_rev, terminal_attempt, verdict) =
            match closure_store.record_closure_verdict(&RecordClosureVerdictRequest {
                expected_ledger_revision: submitted_ledger_rev,
                attempt_id: reserved_attempt.attempt_id.clone(),
                command_key: request.command_key.clone(),
                verdict_id: None,
                subject: request.final_subject.clone(),
                kind: parsed_response.outcome,
                summary: parsed_response.summary.clone(),
                execution_evidence,
                recorded_at_ms,
            }) {
                Ok(triple) => triple,
                Err(ops_err) => {
                    let reason = format!("closure execution evidence validation failed: {ops_err}");
                    return self.record_local_terminal_closure_failure(
                        policy_store,
                        &closure_store,
                        request,
                        submitted_ledger_rev,
                        &reserved_attempt.attempt_id,
                        &effect_id,
                        "execution_evidence_rejected",
                        &reason,
                        &structured_exec,
                        actual_observed_model,
                        actual_observed_effort,
                        false,
                        recorded_at_ms,
                    );
                }
            };

        let () = policy_store.mutate_job_runtime_for_operator(
            &request.binding_id,
            &request.job_id,
            |state| {
                state.closure_verdict_id = Some(verdict.verdict_id.clone());
                if let Some(eff) = state.pending_effects.get_mut(&effect_id) {
                    eff.status = PendingControllerEffectStatus::EffectConfirmed;
                    eff.external_correlation_id = Some(verdict.verdict_id.clone());
                    eff.updated_at_ms = now_epoch_ms().max(request.current_time_ms).max(0) as u64;
                }
                Ok(())
            },
        )?;

        let updated_job = self.record_closure_outcome_on_job(
            &request.binding_id,
            &request.thread_uuid,
            &request.job_id,
            request.expected_job_revision,
            request.expected_ledger_revision,
            &request.combined_candidate_id,
            &terminal_attempt.attempt_id,
            Some(&verdict),
            None,
        )?;

        Ok(ExecuteFinalCandidateClosureOutcome {
            job: updated_job,
            closure_ledger_revision: terminal_rev,
            attempt: terminal_attempt,
            verdict: Some(verdict),
            reused_cached_verdict: false,
        })
    }

    /// Reconciles an unresolved (`Reserved`, `Submitted`, or `OutcomeUnknown`) final-candidate
    /// closure attempt in [`FinalCandidateClosureStore`] using explicit receipt-bound execution
    /// evidence.
    pub fn reconcile_final_candidate_closure(
        &self,
        repository_path: &Path,
        work_area_root: &Path,
        request: &ReconcileClosureAttemptRequest,
    ) -> Result<(u64, ClosureAttemptRecord, ClosureVerdict), NativeError> {
        let closure_store = FinalCandidateClosureStore::open(repository_path, work_area_root)
            .map_err(map_operations_error)?;
        closure_store
            .reconcile_closure_attempt(request)
            .map_err(map_operations_error)
    }

    #[allow(clippy::too_many_arguments)]
    fn record_closure_outcome_on_job(
        &self,
        binding_id: &str,
        thread_uuid: &str,
        job_id: &str,
        expected_job_revision: u64,
        expected_ledger_revision: Option<u64>,
        combined_candidate_id: &str,
        attempt_id: &str,
        verdict: Option<&ClosureVerdict>,
        unknown_reason: Option<&str>,
    ) -> Result<CoworkerJobRecord, NativeError> {
        let (_, updated_job) =
            self.store
                .transact_binding_ledger(binding_id, expected_ledger_revision, |ledger| {
                    ensure_ledger_thread_scope(&ledger.thread_uuid, thread_uuid)?;
                    let job = ledger
                        .jobs
                        .iter_mut()
                        .find(|j| j.job_id == job_id)
                        .ok_or_else(|| {
                            NativeError::new(
                                NativeErrorCode::ValidationFailed,
                                "job_id was not found in binding ledger",
                            )
                        })?;
                    if job.job_revision != expected_job_revision {
                        return Err(NativeError::new(
                            NativeErrorCode::ValidationFailed,
                            "expected_job_revision does not match current job_revision",
                        ));
                    }
                    if verdict.is_some_and(|v| v.kind == ClosureVerdictKind::Accepted)
                        && job.holds.iter().any(|h| h.active)
                    {
                        return Err(NativeError::new(
                            NativeErrorCode::ValidationFailed,
                            "cannot promote closure acceptance while job has an active hold",
                        ));
                    }

                    let cand = job
                        .combined_candidates
                        .iter_mut()
                        .find(|c| c.combined_candidate_id == combined_candidate_id)
                        .ok_or_else(|| {
                            NativeError::new(
                                NativeErrorCode::ValidationFailed,
                                "combined_candidate_id was not found on job",
                            )
                        })?;
                    cand.closure_reservation_id = Some(attempt_id.to_string());

                    match verdict {
                        Some(v) if v.kind == ClosureVerdictKind::Accepted => {
                            cand.status = CombinedCandidateStatus::ClosureAccepted;
                            job.phase = CoworkerJobPhase::Completed;
                        }
                        Some(_) => {
                            cand.status = CombinedCandidateStatus::Rejected;
                            job.phase = CoworkerJobPhase::ClosureReview;
                        }
                        None if unknown_reason.is_some() => {
                            cand.status = CombinedCandidateStatus::ClosureReserved;
                            job.phase = CoworkerJobPhase::ClosureReview;
                        }
                        None => {
                            cand.status = CombinedCandidateStatus::Rejected;
                            job.phase = CoworkerJobPhase::ClosureReview;
                        }
                    }

                    job.job_revision = job.job_revision.saturating_add(1);
                    Ok(CoworkerLedgerMutationOutcome::Mutated(job.clone()))
                })?;
        Ok(updated_job)
    }

    fn check_post_completion_fencing(
        &self,
        authority: &AuthorityStore,
        policy_store: &ControllerPolicyStore,
        request: &ExecuteFinalCandidateClosureRequest,
    ) -> Result<(), String> {
        if let Err(err) = verify_final_candidate_subject_readonly(&request.final_subject) {
            return Err(format!(
                "candidate subject drifted during closure execution: {err}"
            ));
        }
        let ledger = self
            .store
            .read_binding_ledger(&request.binding_id)
            .map_err(|e| e.message)?;
        let Some(job) = ledger.jobs.iter().find(|j| j.job_id == request.job_id) else {
            return Err("job_id no longer present in binding ledger".to_string());
        };
        if job.holds.iter().any(|h| h.active) {
            return Err("workflow hold became active while closure worker was running".to_string());
        }
        if job.job_revision != request.expected_job_revision {
            return Err(format!(
                "job_revision drifted from {} to {} while closure worker was running",
                request.expected_job_revision, job.job_revision
            ));
        }
        let Some(cand) = job
            .combined_candidates
            .iter()
            .find(|c| c.combined_candidate_id == request.combined_candidate_id)
        else {
            return Err("combined_candidate_id no longer present on job".to_string());
        };
        if cand.final_tree_sha != request.final_subject.candidate_tree_sha
            || cand.final_commit_sha.as_deref()
                != Some(request.final_subject.candidate_head_commit.as_str())
        {
            return Err("combined candidate tree or commit drifted on job".to_string());
        }
        let runtime = policy_store
            .read_job_runtime(&request.binding_id, &request.job_id)
            .map_err(|e| e.message)?
            .ok_or_else(|| {
                "ControllerJobRuntimeState missing after worker completion".to_string()
            })?;
        if runtime.stop_requested {
            return Err(
                "controller stop_requested became active during closure execution".to_string(),
            );
        }
        runtime
            .envelope
            .verify_matches_job(job)
            .map_err(|e| e.message)?;
        if let Some(ref linkage) = job.approval_linkage {
            let approval = authority
                .get_for_operator(&linkage.approval_id)
                .map_err(|e| e.to_string())?;
            verify_live_approval_and_registry(&approval, job, &runtime.envelope.policy, authority)
                .map_err(|e| e.message)?;
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn record_unknown_closure_outcome(
        &self,
        policy_store: &ControllerPolicyStore,
        closure_store: &FinalCandidateClosureStore,
        request: &ExecuteFinalCandidateClosureRequest,
        submitted_ledger_rev: u64,
        attempt_id: &str,
        effect_id: &str,
        reason: &str,
    ) -> Result<ExecuteFinalCandidateClosureOutcome, NativeError> {
        let recorded_at_ms = now_epoch_ms().max(request.current_time_ms);
        let (unknown_rev, unknown_attempt) = closure_store
            .mark_closure_attempt_unknown(&MarkClosureUnknownRequest {
                expected_ledger_revision: submitted_ledger_rev,
                attempt_id: attempt_id.to_string(),
                expected_subject_digest: request.final_subject.subject_digest.clone(),
                reason: reason.to_string(),
                recorded_at_ms,
            })
            .map_err(map_operations_error)?;

        let () = policy_store.mutate_job_runtime_for_operator(
            &request.binding_id,
            &request.job_id,
            |state| {
                if let Some(eff) = state.pending_effects.get_mut(effect_id) {
                    eff.status = PendingControllerEffectStatus::EffectUnknown;
                    eff.external_correlation_id = Some(attempt_id.to_string());
                    eff.diagnostic = Some(sanitize_diagnostic_text(reason));
                    eff.updated_at_ms = recorded_at_ms.max(0) as u64;
                }
                Ok(())
            },
        )?;

        let updated_job = self.record_closure_outcome_on_job(
            &request.binding_id,
            &request.thread_uuid,
            &request.job_id,
            request.expected_job_revision,
            request.expected_ledger_revision,
            &request.combined_candidate_id,
            &unknown_attempt.attempt_id,
            None,
            Some(reason),
        )?;

        Ok(ExecuteFinalCandidateClosureOutcome {
            job: updated_job,
            closure_ledger_revision: unknown_rev,
            attempt: unknown_attempt,
            verdict: None,
            reused_cached_verdict: false,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn record_local_terminal_closure_failure(
        &self,
        policy_store: &ControllerPolicyStore,
        closure_store: &FinalCandidateClosureStore,
        request: &ExecuteFinalCandidateClosureRequest,
        submitted_ledger_rev: u64,
        attempt_id: &str,
        effect_id: &str,
        failure_code: &str,
        reason: &str,
        structured_exec: &AdvisoryStructuredExecution,
        observed_model: Option<String>,
        observed_effort: Option<String>,
        fenced_late_output: bool,
        recorded_at_ms: i64,
    ) -> Result<ExecuteFinalCandidateClosureOutcome, NativeError> {
        let response_digest = structured_exec
            .response
            .as_ref()
            .map(|r| r.sha256_digest.clone());
        let (failed_rev, failed_attempt) = closure_store
            .record_closure_failure(&RecordClosureFailureRequest {
                expected_ledger_revision: submitted_ledger_rev,
                attempt_id: attempt_id.to_string(),
                command_key: request.command_key.clone(),
                expected_subject_digest: request.final_subject.subject_digest.clone(),
                failure: ClosureTerminalFailure {
                    failure_code: failure_code.to_string(),
                    reason: reason.to_string(),
                    exit_code: structured_exec.execution.exit_code,
                    observed_provider: Some(
                        structured_exec.execution.provider.as_str().to_string(),
                    ),
                    observed_model,
                    observed_effort,
                    response_sha256_digest: response_digest,
                    fenced_late_output,
                    recorded_at_ms,
                },
            })
            .map_err(map_operations_error)?;

        let () = policy_store.mutate_job_runtime_for_operator(
            &request.binding_id,
            &request.job_id,
            |state| {
                if let Some(eff) = state.pending_effects.get_mut(effect_id) {
                    eff.status = PendingControllerEffectStatus::EffectFailedTerminal;
                    eff.external_correlation_id = Some(attempt_id.to_string());
                    eff.diagnostic = Some(sanitize_diagnostic_text(reason));
                    eff.updated_at_ms = recorded_at_ms.max(0) as u64;
                }
                if let Some(art) = state.closure_artifacts.get_mut(attempt_id) {
                    art.fenced_from_promotion = fenced_late_output;
                    art.diagnostic = Some(sanitize_diagnostic_text(reason));
                }
                Ok(())
            },
        )?;

        let job = if fenced_late_output {
            let ledger = self.store.read_binding_ledger(&request.binding_id)?;
            ledger
                .jobs
                .into_iter()
                .find(|j| j.job_id == request.job_id)
                .ok_or_else(|| {
                    NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "job_id was not found in binding ledger",
                    )
                })?
        } else {
            self.record_closure_outcome_on_job(
                &request.binding_id,
                &request.thread_uuid,
                &request.job_id,
                request.expected_job_revision,
                request.expected_ledger_revision,
                &request.combined_candidate_id,
                &failed_attempt.attempt_id,
                None,
                None,
            )?
        };

        Ok(ExecuteFinalCandidateClosureOutcome {
            job,
            closure_ledger_revision: failed_rev,
            attempt: failed_attempt,
            verdict: None,
            reused_cached_verdict: false,
        })
    }
}

/// Resolves the authorized `Closure` role model tuple from a [`FrozenControllerExecutionEnvelope`].
pub fn resolve_closure_choice_for_envelope(
    envelope: &FrozenControllerExecutionEnvelope,
) -> Result<SelectedRoleChoice, NativeError> {
    validate_non_native_worker_choice(WorkflowRole::Closure, &envelope.closure_choice)?;
    if !envelope.policy.allows(
        WorkflowRole::Closure,
        &envelope.closure_choice.tool_id,
        &envelope.closure_choice.model_id,
        envelope.closure_choice.effort.as_deref(),
    )? {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "frozen ExecutionPolicy does not allow envelope.closure_choice for WorkflowRole::Closure",
        ));
    }
    Ok(envelope.closure_choice.clone())
}

/// Resolves and validates the canonical non-symlink executable path for `tool_id` in `authority.registry()`.
pub fn resolve_tool_executable(
    authority: &AuthorityStore,
    tool_id: &str,
) -> Result<PathBuf, NativeError> {
    let snap = authority.registry().snapshot().map_err(|_| {
        NativeError::new(
            NativeErrorCode::ProtocolError,
            "failed to read workflow registry snapshot",
        )
    })?;
    let tool = snap
        .tools
        .iter()
        .find(|t| t.id == tool_id && t.enabled)
        .ok_or_else(|| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!("enabled tool `{tool_id}` was not found in RegistryStore"),
            )
        })?;
    let exe_path = tool.executable_path.as_ref().ok_or_else(|| {
        NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!("tool `{tool_id}` has no executable_path configured"),
        )
    })?;
    validate_executable_file(exe_path)
        .map_err(|err| NativeError::new(NativeErrorCode::SecurityViolation, err.to_string()))
}

/// Converts [`CandidateCheckEvidence`] records from the coworker workflow contract into an
/// operations [`TestEvidenceSummary`].
pub fn build_operations_test_evidence(
    checks: &[CandidateCheckEvidence],
) -> Result<TestEvidenceSummary, NativeError> {
    if checks.is_empty() {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "CandidateCheckEvidence list must not be empty",
        ));
    }

    let mut formatting_passed = true;
    let mut clippy_passed = true;
    let mut tests_passed = true;
    let mut passed_test_count = 0u32;
    let mut failed_test_count = 0u32;
    let mut command_digests = Vec::with_capacity(checks.len());

    for item in checks {
        item.validate_passed()?;
        let passed = item.passed && item.exit_code == 0;
        let lower_id = item.check_id.to_ascii_lowercase();
        let lower_cmd = item.command_summary.to_ascii_lowercase();
        if lower_id.contains("fmt") || lower_cmd.contains("fmt") {
            if !passed {
                formatting_passed = false;
            }
        } else if lower_id.contains("clippy") || lower_cmd.contains("clippy") {
            if !passed {
                clippy_passed = false;
            }
        } else if passed {
            passed_test_count = passed_test_count.saturating_add(1);
        } else {
            tests_passed = false;
            failed_test_count = failed_test_count.saturating_add(1);
        }
        command_digests.push(format!("{}:{}", item.check_id, item.evidence_digest));
    }

    if tests_passed && passed_test_count == 0 {
        passed_test_count = 1;
    }

    let summary_text = format!(
        "verified {} candidate check(s): {}",
        checks.len(),
        command_digests.join(", ")
    );
    Ok(TestEvidenceSummary {
        formatting_passed,
        clippy_passed,
        tests_passed,
        passed_test_count,
        failed_test_count,
        evidence_summary: sanitize_diagnostic_text(&summary_text),
    })
}

/// Maps a [`ToolAdapter`] to a headless [`WorkerProvider`].
pub fn map_tool_adapter_to_provider(adapter: ToolAdapter) -> Result<WorkerProvider, NativeError> {
    match adapter {
        ToolAdapter::Antigravity => Ok(WorkerProvider::Antigravity),
        ToolAdapter::OpencodeV2 => Ok(WorkerProvider::OpenCodeV2),
        ToolAdapter::CodexCli => Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "CodexCli is the native human conversational interface and cannot be dispatched as a headless worker",
        )),
        ToolAdapter::JevApi
        | ToolAdapter::OpenaiDecisionsApi
        | ToolAdapter::Git
        | ToolAdapter::Tmux
        | ToolAdapter::Codegraph => Err(NativeError::new(
            NativeErrorCode::UnsupportedCapability,
            format!(
                "tool adapter `{}` is not an executable headless worker provider",
                adapter.as_str()
            ),
        )),
    }
}

/// Maps an [`OperationsError`] into a structured [`NativeError`].
pub fn map_operations_error(err: OperationsError) -> NativeError {
    match err {
        OperationsError::SecurityViolation(msg) | OperationsError::WorktreeNotOwned(msg) => {
            NativeError::new(NativeErrorCode::SecurityViolation, msg)
        }
        OperationsError::UncertainWorkerOutcome(msg)
        | OperationsError::ActiveJob { detail: msg, .. } => {
            NativeError::new(NativeErrorCode::AmbiguousWakeupPendingReconciliation, msg)
        }
        OperationsError::UnsupportedPlatform(msg) => {
            NativeError::new(NativeErrorCode::UnsupportedPlatform, msg)
        }
        OperationsError::Io(io_err) => {
            NativeError::new(NativeErrorCode::IoError, io_err.to_string())
        }
        other => NativeError::new(NativeErrorCode::ValidationFailed, other.to_string()),
    }
}

fn ensure_ledger_thread_scope(
    ledger_thread_uuid: &Option<String>,
    expected_thread_uuid: &str,
) -> Result<(), NativeError> {
    match ledger_thread_uuid.as_deref() {
        Some(existing) if constant_time_str_eq(existing, expected_thread_uuid) => Ok(()),
        Some(_) => Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "binding ledger thread_uuid does not match requested thread_uuid",
        )),
        None => Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "coworker workflow ledger was not found for binding_id",
        )),
    }
}

fn compute_closure_task_digest(
    job_id: &str,
    closure_task_id: &str,
    envelope_digest: &str,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"threadlane-native:closure-task-digest:v1\0");
    hasher.update(job_id.as_bytes());
    hasher.update(b"\0");
    hasher.update(closure_task_id.as_bytes());
    hasher.update(b"\0");
    hasher.update(envelope_digest.as_bytes());
    format!("{:x}", hasher.finalize())
}

fn build_closure_worker_prompt(subject: &FinalCandidateSubject) -> String {
    format!(
        "Perform read-only final package closure review for RunIntegration worktree `{}`.\n\
         subject_digest: {}\n\
         approved_base_commit: {}\n\
         candidate_head_commit: {}\n\
         candidate_tree_sha: {}\n\
         candidate_diff_digest: {}\n\
         Return ONLY a single JSON object matching StructuredClosureModelResponse \
         (schema_version: 1, subject_digest, candidate_tree_sha, outcome: \"accepted\"|\"rejected\"|\"blocked\"|\"unclear\", summary, findings).",
        subject.integration_worktree_id,
        subject.subject_digest,
        subject.approved_base_commit,
        subject.candidate_head_commit,
        subject.candidate_tree_sha,
        subject.candidate_diff_digest,
    )
}

fn extract_json_payload(raw_text: &str) -> &str {
    let trimmed = raw_text.trim();
    if let Some(rest) = trimmed.strip_prefix("```json") {
        if let Some(inner) = rest.strip_suffix("```") {
            return inner.trim();
        }
    }
    if let Some(rest) = trimmed.strip_prefix("```") {
        if let Some(inner) = rest.strip_suffix("```") {
            return inner.trim();
        }
    }
    let _ = validate_sha256_hex_digest;
    trimmed
}

fn verify_subject_policy_matches_envelope(
    subject: &FinalCandidateSubject,
    linkage: &crate::coworker_workflow::contracts::FrozenApprovalLinkage,
    expected_provider: WorkerProvider,
    expected_base_model: &str,
    expected_effort: Option<&str>,
) -> Result<(), NativeError> {
    if subject.policy.policy_id != linkage.policy_id
        || !constant_time_str_eq(&subject.policy.policy_digest, &linkage.policy_digest)
        || subject.policy.scope_revision != linkage.plan_revision
        || subject.policy.registry_revision != linkage.registry_revision
        || subject.policy.approval_id != linkage.approval_id
        || !constant_time_str_eq(
            &subject.policy.approval_subject_digest,
            &linkage.approval_subject_digest,
        )
        || subject.policy.requested_provider != expected_provider.as_str()
        || subject.policy.requested_model != expected_base_model
        || subject.policy.requested_effort.as_deref() != expected_effort
        || subject.policy.require_effort_evidence != expected_effort.is_some()
        || !subject.policy.require_passing_checks
    {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "FinalCandidateSubject.policy does not match the job's frozen approval linkage and closure model/effort choice",
        ));
    }
    Ok(())
}

fn classify_local_closure_worker_failure(
    structured_exec: &AdvisoryStructuredExecution,
) -> (&'static str, String) {
    if let Some(ref resp_err) = structured_exec.response_error {
        return (
            resp_err.code.as_str(),
            format!(
                "closure worker structured response failed ({}): {}",
                resp_err.code.as_str(),
                resp_err.message
            ),
        );
    }
    let code = match structured_exec.execution.status {
        WorkerRunStatus::NonZeroExit => "worker_non_zero_exit",
        WorkerRunStatus::TimedOut => "worker_timed_out",
        WorkerRunStatus::OutputOverflow => "worker_output_overflow",
        WorkerRunStatus::MalformedStream => "worker_malformed_stream",
        WorkerRunStatus::ModelMismatch | WorkerRunStatus::MissingModelEvidence => {
            "model_evidence_failed"
        }
        WorkerRunStatus::EffortMismatch | WorkerRunStatus::MissingEffortEvidence => {
            "effort_evidence_failed"
        }
        WorkerRunStatus::SessionMismatch => "session_evidence_failed",
        WorkerRunStatus::ProviderError => "provider_error",
        WorkerRunStatus::Blocked => "worker_blocked",
        _ => "worker_execution_failed",
    };
    (
        code,
        format!(
            "closure worker exited in non-verified state `{}`",
            structured_exec.execution.status.as_str()
        ),
    )
}

fn now_epoch_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}
