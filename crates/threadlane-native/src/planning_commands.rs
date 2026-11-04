//! Durable per-binding planning command ledger (`Architect` and `Orchestrator` requests),
//! live [`CoworkerJobOwnershipGuard`] claim/transition enforcement, and read-only status queries.

use std::collections::BTreeSet;
use std::fmt::Write as _;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use threadlane_authority::validation::validate_canonical_thread_uuid;
use threadlane_workers::{
    is_commit_ancestor, is_source_worktree_clean, resolve_exact_commit_sha, resolve_exact_tree_sha,
    validate_sha256_hex_digest, DEFAULT_GIT_COMMAND_TIMEOUT_MS,
};
use threadlane_workflow::{Difficulty, ToolAdapter, WorkflowRole};

use crate::coworker_workflow::{
    deterministic_prefixed_id, sha256_hex, validate_bounded_text,
    validate_non_native_worker_choice, validate_unique_id_list, verify_recovery_command_context,
    ArchitectAdviceRecord, ArchitectConsultationRequestOutcome, ArchitectDecisionStatus,
    CanonicalProjectBaseRef, CoworkerBindingLedger, CoworkerJobLockState,
    CoworkerJobOwnershipGuard, CoworkerJobOwnershipKey, CoworkerJobOwnershipStore,
    CoworkerOwnershipSummary, CoworkerPlanningService, CoworkerWorkflowStore,
    HumanClarificationRequestOutcome, OrchestrateWorkBriefPlanRequest,
    PersistedPlanningCallArtifact, PlanningAdviceGrant, PlanningArtifactOutcomeKind,
    PlanningGrantStatus, PlanningReservationStatus, PlanningResultRef, PriorArchitectAdviceContext,
    RunArchitectAdviceRequest, StructuredOrchestratorOutput,
    MAX_ARCHITECT_ADVICE_RECORDS_PER_BINDING, MAX_COWORKER_ID_LEN,
    MAX_ORCHESTRATOR_ARCHITECT_ROUNDS, MAX_PLANNING_ARTIFACT_BYTES, MAX_WORK_BRIEF_ITEMS,
};
use crate::error::{NativeError, NativeErrorCode};
use crate::fs_safety::{
    acquire_cross_process_lock, constant_time_str_eq, ensure_platform_supported,
    read_private_file_bounded, validate_absolute_no_symlink_components,
    validate_bounded_identifier, write_private_file_atomic, MAX_PRIVATE_STATE_FILE_BYTES,
};
use crate::profile_flow::{
    preflight, PreflightBlockReason, PreflightRequest, RoleExecutionRoute, RolePreflightRequest,
    SelectedRoleChoice,
};
use crate::role_flow::read_registry_snapshot_readonly;

/// Wire and storage schema version (`1`) for per-binding planning command ledgers.
pub const PLANNING_COMMAND_SCHEMA_VERSION: u32 = 1;

/// Maximum number of [`PlanningCommandRecord`] entries retained per binding ledger (`128`).
pub const MAX_PLANNING_COMMANDS_PER_BINDING: usize = 128;

/// Maximum serialized byte size for a single [`PlanningCommandPayload`] (`64 KiB`).
pub const MAX_PLANNING_COMMAND_PAYLOAD_BYTES: usize = 64 * 1024;

/// Maximum byte size for a per-binding planning command ledger file (`1 MiB`).
pub const MAX_PLANNING_COMMAND_LEDGER_BYTES: usize = MAX_PRIVATE_STATE_FILE_BYTES;

/// Domain separator for deterministic planning command identifiers (`pcm_<32hex>`).
pub const PLANNING_COMMAND_ID_DOMAIN: &str = "threadlane-native:planning-command-id:v1";

/// Domain separator for deterministic internal service request keys (`psr_<32hex>`).
pub const PLANNING_COMMAND_SERVICE_KEY_DOMAIN: &str =
    "threadlane-native:planning-command-service-key:v1";

/// Lifecycle status of a durable [`PlanningCommandRecord`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlanningCommandStatus {
    /// Validated against the active grant and queued for an owned driver to claim.
    Queued,
    /// Claimed by a driver holding a live [`CoworkerJobOwnershipGuard`].
    Running,
    /// Completed with a verified service result reference.
    Completed,
    /// Terminated in a known pre-effect or post-effect failure.
    KnownFailed,
    /// Outcome is uncertain (for example after owner guard release or crash while `Running`);
    /// survives reopen, blocks replay, and cannot be claimed again.
    Unknown,
}

impl PlanningCommandStatus {
    /// Returns `true` when the command is in an in-flight or uncertain state (`Running` or `Unknown`).
    pub const fn is_in_flight_or_unknown(self) -> bool {
        matches!(self, Self::Running | Self::Unknown)
    }
}

/// Typed planning command request payload for either a read-only `Architect` advice call or an
/// `Orchestrator` work-brief planning call.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum PlanningCommandPayload {
    /// Read-only `WorkflowRole::Architect` advice request.
    Architect(RunArchitectAdviceRequest),
    /// Read-only `WorkflowRole::Orchestrator` work-brief planning request.
    Orchestrator(OrchestrateWorkBriefPlanRequest),
}

impl PlanningCommandPayload {
    /// Returns the [`WorkflowRole`] executed by this command payload.
    pub const fn role(&self) -> WorkflowRole {
        match self {
            Self::Architect(_) => WorkflowRole::Architect,
            Self::Orchestrator(_) => WorkflowRole::Orchestrator,
        }
    }

    /// Returns the control-plane `binding_id` inside the payload.
    pub fn binding_id(&self) -> &str {
        match self {
            Self::Architect(req) => &req.binding_id,
            Self::Orchestrator(req) => &req.binding_id,
        }
    }

    /// Returns the native `thread_uuid` inside the payload.
    pub fn thread_uuid(&self) -> &str {
        match self {
            Self::Architect(req) => &req.thread_uuid,
            Self::Orchestrator(req) => &req.thread_uuid,
        }
    }

    /// Returns the standing `grant_id` inside the payload.
    pub fn grant_id(&self) -> &str {
        match self {
            Self::Architect(req) => &req.grant_id,
            Self::Orchestrator(req) => &req.grant_id,
        }
    }

    /// Returns the `request_key` inside the payload.
    pub fn request_key(&self) -> &str {
        match self {
            Self::Architect(req) => &req.request_key,
            Self::Orchestrator(req) => &req.request_key,
        }
    }

    /// Returns the optional difficulty tier requested for the primary planning role.
    pub const fn difficulty(&self) -> Option<Difficulty> {
        match self {
            Self::Architect(req) => req.difficulty,
            Self::Orchestrator(req) => req.difficulty,
        }
    }

    /// Computes the canonical 64-character lowercase hex SHA-256 digest of the caller's semantic
    /// request body (independent of local wall-clock timestamp and frozen internal service key).
    pub fn compute_request_body_digest(&self, caller_key: &str) -> Result<String, NativeError> {
        let bytes = match self {
            Self::Architect(req) => serde_json::to_vec(&(
                "threadlane-native:planning-command-body:architect:v1",
                &req.binding_id,
                &req.thread_uuid,
                &req.grant_id,
                caller_key,
                req.expected_ledger_revision,
                req.difficulty,
                &req.context_dependencies,
                &req.question_summary,
                &req.prior_advice_context,
                req.accept_into_brief,
            )),
            Self::Orchestrator(req) => serde_json::to_vec(&(
                "threadlane-native:planning-command-body:orchestrator:v1",
                &req.binding_id,
                &req.thread_uuid,
                &req.grant_id,
                &req.job_id,
                caller_key,
                req.expected_ledger_revision,
                req.difficulty,
                &req.plan_id,
                req.expected_plan_revision,
                &req.architect_advice_ids,
                req.max_architect_rounds,
                req.architect_difficulty,
            )),
        }
        .map_err(|_| {
            NativeError::new(
                NativeErrorCode::ProtocolError,
                "failed to serialize PlanningCommandPayload for body digest",
            )
        })?;
        if bytes.len() > MAX_PLANNING_COMMAND_PAYLOAD_BYTES {
            return Err(NativeError::new(
                NativeErrorCode::OutputOverflow,
                "PlanningCommandPayload exceeds MAX_PLANNING_COMMAND_PAYLOAD_BYTES",
            ));
        }
        Ok(sha256_hex(&bytes))
    }

    fn freeze_for_record(&self, service_request_key: &str, enqueued_at_ms: u64) -> Self {
        match self {
            Self::Architect(req) => {
                let mut frozen = req.clone();
                frozen.request_key = service_request_key.to_string();
                frozen.current_time_ms = enqueued_at_ms;
                Self::Architect(frozen)
            }
            Self::Orchestrator(req) => {
                let mut frozen = req.clone();
                frozen.request_key = service_request_key.to_string();
                frozen.current_time_ms = enqueued_at_ms;
                Self::Orchestrator(frozen)
            }
        }
    }
}

/// Verified result reference recorded on a completed [`PlanningCommandRecord`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum PlanningCommandResultRef {
    /// Result of a completed `WorkflowRole::Architect` advice command.
    ArchitectAdvice {
        /// Persisted [`ArchitectAdviceRecord::advice_id`] in [`CoworkerBindingLedger`].
        advice_id: String,
        /// Completed [` crate::coworker_workflow::PlanningCallReservation::reservation_id`].
        reservation_id: String,
        /// Persisted report artifact identifier (`pln_<32hex>`).
        report_artifact_id: String,
        /// 64-character lowercase hex SHA-256 `context_digest`.
        context_digest: String,
        /// 64-character lowercase hex SHA-256 `decision_digest`.
        decision_digest: String,
        /// Whether an existing verified [`ArchitectAdviceRecord`] was reused without a new call.
        reused_existing: bool,
        /// `true` only when the persisted report artifact was directly validated on disk.
        artifact_verified: bool,
    },
    /// Result of a completed `WorkflowRole::Orchestrator` command that published a plan.
    OrchestratorPlan {
        /// Target [`crate::coworker_workflow::CoworkerJobRecord::job_id`].
        job_id: String,
        /// Durable [`PlanningResultRef`] recorded on the job in [`CoworkerBindingLedger`].
        planning_result: Box<PlanningResultRef>,
        /// `true` only when the persisted report artifact was directly validated on disk.
        artifact_verified: bool,
    },
    /// Result of a completed `WorkflowRole::Orchestrator` command requiring human clarification.
    OrchestratorRequiresHumanClarification {
        /// Target [`crate::coworker_workflow::CoworkerJobRecord::job_id`].
        job_id: String,
        /// Completed [`crate::coworker_workflow::PlanningCallReservation::reservation_id`].
        reservation_id: String,
        /// Persisted report artifact identifier (`pln_<32hex>`).
        report_artifact_id: String,
        /// 64-character lowercase hex SHA-256 `output_digest`.
        output_digest: String,
        /// Structured clarification questions returned by the Orchestrator.
        clarification: HumanClarificationRequestOutcome,
        /// `true` only when the persisted report artifact was directly validated on disk.
        artifact_verified: bool,
    },
    /// Result of a completed `WorkflowRole::Orchestrator` command requiring Architect advice.
    OrchestratorRequiresArchitectAdvice {
        /// Target [`crate::coworker_workflow::CoworkerJobRecord::job_id`].
        job_id: String,
        /// Completed [`crate::coworker_workflow::PlanningCallReservation::reservation_id`].
        reservation_id: String,
        /// Persisted report artifact identifier (`pln_<32hex>`).
        report_artifact_id: String,
        /// 64-character lowercase hex SHA-256 `output_digest`.
        output_digest: String,
        /// Structured Architect consultation questions returned by the Orchestrator.
        consultation: ArchitectConsultationRequestOutcome,
        /// `true` only when the persisted report artifact was directly validated on disk.
        artifact_verified: bool,
    },
}

impl PlanningCommandResultRef {
    /// Returns `true` when the persisted planning report artifact was directly validated on disk.
    pub const fn is_artifact_verified(&self) -> bool {
        match self {
            Self::ArchitectAdvice {
                artifact_verified, ..
            }
            | Self::OrchestratorPlan {
                artifact_verified, ..
            }
            | Self::OrchestratorRequiresHumanClarification {
                artifact_verified, ..
            }
            | Self::OrchestratorRequiresArchitectAdvice {
                artifact_verified, ..
            } => *artifact_verified,
        }
    }

    /// Validates structural fields and digests on the result reference.
    pub fn validate(&self) -> Result<(), NativeError> {
        match self {
            Self::ArchitectAdvice {
                advice_id,
                reservation_id,
                report_artifact_id,
                context_digest,
                decision_digest,
                ..
            } => {
                validate_bounded_identifier(advice_id, "advice_id", MAX_COWORKER_ID_LEN)?;
                validate_bounded_identifier(reservation_id, "reservation_id", MAX_COWORKER_ID_LEN)?;
                validate_bounded_identifier(
                    report_artifact_id,
                    "report_artifact_id",
                    MAX_COWORKER_ID_LEN,
                )?;
                validate_sha256_hex_digest(context_digest, "context_digest").map_err(|_| {
                    NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "result_ref.context_digest must be a 64-character lowercase hex SHA-256",
                    )
                })?;
                validate_sha256_hex_digest(decision_digest, "decision_digest").map_err(|_| {
                    NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "result_ref.decision_digest must be a 64-character lowercase hex SHA-256",
                    )
                })?;
            }
            Self::OrchestratorPlan {
                job_id,
                planning_result,
                ..
            } => {
                validate_bounded_identifier(job_id, "job_id", MAX_COWORKER_ID_LEN)?;
                planning_result.validate()?;
            }
            Self::OrchestratorRequiresHumanClarification {
                job_id,
                reservation_id,
                report_artifact_id,
                output_digest,
                clarification,
                ..
            } => {
                validate_bounded_identifier(job_id, "job_id", MAX_COWORKER_ID_LEN)?;
                validate_bounded_identifier(reservation_id, "reservation_id", MAX_COWORKER_ID_LEN)?;
                validate_bounded_identifier(
                    report_artifact_id,
                    "report_artifact_id",
                    MAX_COWORKER_ID_LEN,
                )?;
                validate_sha256_hex_digest(output_digest, "output_digest").map_err(|_| {
                    NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "result_ref.output_digest must be a 64-character lowercase hex SHA-256",
                    )
                })?;
                validate_bounded_text(&clarification.summary, "clarification.summary")?;
                if clarification.questions.is_empty() {
                    return Err(NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "clarification.questions must not be empty",
                    ));
                }
            }
            Self::OrchestratorRequiresArchitectAdvice {
                job_id,
                reservation_id,
                report_artifact_id,
                output_digest,
                consultation,
                ..
            } => {
                validate_bounded_identifier(job_id, "job_id", MAX_COWORKER_ID_LEN)?;
                validate_bounded_identifier(reservation_id, "reservation_id", MAX_COWORKER_ID_LEN)?;
                validate_bounded_identifier(
                    report_artifact_id,
                    "report_artifact_id",
                    MAX_COWORKER_ID_LEN,
                )?;
                validate_sha256_hex_digest(output_digest, "output_digest").map_err(|_| {
                    NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "result_ref.output_digest must be a 64-character lowercase hex SHA-256",
                    )
                })?;
                validate_bounded_text(&consultation.summary, "consultation.summary")?;
                if consultation.questions.is_empty() {
                    return Err(NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "consultation.questions must not be empty",
                    ));
                }
            }
        }
        Ok(())
    }
}

/// Immutable command identity, frozen request payload, and durable execution state for a single
/// planning command.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlanningCommandRecord {
    /// Deterministic command identifier (`pcm_<32hex>`).
    pub command_id: String,
    /// Idempotent caller request key.
    pub command_key: String,
    /// Frozen internal service request key (`psr_<32hex>`) embedded in `payload`.
    pub service_request_key: String,
    /// Control-plane chat binding identifier.
    pub binding_id: String,
    /// Canonical lowercase hyphenated non-nil native Codex thread UUID.
    pub thread_uuid: String,
    /// Standing [`PlanningAdviceGrant::grant_id`] authorizing this command.
    pub grant_id: String,
    /// Primary planning role (`Architect` or `Orchestrator`).
    pub role: WorkflowRole,
    /// Resolved headless worker choice verified against the grant profile and policy at enqueue.
    pub selected_choice: SelectedRoleChoice,
    /// Canonical repository and immutable base commit/tree verified at enqueue.
    pub call_project_base: CanonicalProjectBaseRef,
    /// 64-character lowercase hex SHA-256 context digest for the command.
    pub context_digest: String,
    /// 64-character lowercase hex SHA-256 digest of the caller's semantic request body.
    pub request_body_digest: String,
    /// 64-character lowercase hex SHA-256 digest of the immutable command fields and frozen payload.
    pub command_digest: String,
    /// Frozen typed request payload with `request_key == service_request_key` and `current_time_ms == enqueued_at_ms`.
    pub payload: PlanningCommandPayload,
    /// Current command status (`Queued`, `Running`, `Completed`, `KnownFailed`, or `Unknown`).
    pub status: PlanningCommandStatus,
    /// Explicit marker that the command outcome is uncertain and requires artifact/receipt reconciliation.
    pub pending_reconciliation: bool,
    /// Public ownership summary bound when claimed by a live [`CoworkerJobOwnershipGuard`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_summary: Option<CoworkerOwnershipSummary>,
    /// Verified service result reference when `status == Completed`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result_ref: Option<PlanningCommandResultRef>,
    /// Bounded diagnostic message when `KnownFailed` or `Unknown`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub diagnostic: Option<String>,
    /// Local wall-clock timestamp (ms since Unix epoch) when the command was enqueued.
    pub enqueued_at_ms: u64,
    /// Local wall-clock timestamp (ms since Unix epoch) when claimed for `Running`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at_ms: Option<u64>,
    /// Local wall-clock timestamp (ms since Unix epoch) when last updated.
    pub updated_at_ms: u64,
    /// Local wall-clock timestamp (ms since Unix epoch) when `Completed` or `KnownFailed`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed_at_ms: Option<u64>,
}

/// Deterministic publication identity for an exact completed cross-job collision receipt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PlanningRecoveryTarget {
    pub(crate) plan_id: String,
    pub(crate) command_key: String,
}

impl PlanningRecoveryTarget {
    pub(crate) fn derive(
        cmd: &PlanningCommandRecord,
        ledger: &CoworkerBindingLedger,
        artifact_id: &str,
        output_digest: &str,
    ) -> Option<Self> {
        let PlanningCommandPayload::Orchestrator(req) = &cmd.payload else {
            return None;
        };
        let original_plan_id = req.plan_id.as_ref()?;
        if !ledger.jobs.iter().any(|job| {
            job.job_id != req.job_id
                && job
                    .planning_result
                    .as_ref()
                    .is_some_and(|pr| &pr.plan_id == original_plan_id)
        }) {
            return None;
        }
        let parts = [
            cmd.binding_id.as_str(),
            cmd.thread_uuid.as_str(),
            cmd.command_id.as_str(),
            cmd.command_digest.as_str(),
            req.job_id.as_str(),
            artifact_id,
            output_digest,
        ];
        Some(Self {
            plan_id: deterministic_prefixed_id(
                "threadlane-native:planning-recovery-plan:v1",
                "plan",
                &parts,
            ),
            command_key: deterministic_prefixed_id(
                "threadlane-native:planning-recovery-draft:v1",
                "cmd",
                &parts,
            ),
        })
    }
}

impl PlanningCommandRecord {
    /// Returns the [`CoworkerJobOwnershipKey`] (`binding_id`, `thread_uuid`, `command_id`) for this command.
    pub fn ownership_key(&self) -> Result<CoworkerJobOwnershipKey, NativeError> {
        CoworkerJobOwnershipKey::new(&self.binding_id, &self.thread_uuid, &self.command_id)
    }

    /// Computes the canonical 64-character lowercase hex SHA-256 digest of the immutable command fields.
    pub fn compute_command_digest(&self) -> Result<String, NativeError> {
        let seed = (
            "threadlane-native:planning-command-record:v1",
            &self.command_id,
            &self.command_key,
            &self.service_request_key,
            &self.binding_id,
            &self.thread_uuid,
            &self.grant_id,
            self.role,
            &self.selected_choice,
            &self.call_project_base,
            &self.context_digest,
            &self.request_body_digest,
            &self.payload,
            self.enqueued_at_ms,
        );
        let bytes = serde_json::to_vec(&seed).map_err(|_| {
            NativeError::new(
                NativeErrorCode::ProtocolError,
                "failed to serialize PlanningCommandRecord for digest calculation",
            )
        })?;
        Ok(sha256_hex(&bytes))
    }

    /// Validates all identifiers, digests, payload invariants, and state transitions on this record.
    pub fn validate(&self) -> Result<(), NativeError> {
        validate_bounded_identifier(&self.command_id, "command_id", MAX_COWORKER_ID_LEN)?;
        validate_bounded_identifier(&self.command_key, "command_key", MAX_COWORKER_ID_LEN)?;
        validate_bounded_identifier(
            &self.service_request_key,
            "service_request_key",
            MAX_COWORKER_ID_LEN,
        )?;
        validate_bounded_identifier(&self.binding_id, "binding_id", MAX_COWORKER_ID_LEN)?;
        validate_canonical_thread_uuid("thread_uuid", &self.thread_uuid).map_err(|_| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                "thread_uuid must be a canonical lowercase hyphenated non-nil UUID",
            )
        })?;
        validate_bounded_identifier(&self.grant_id, "grant_id", MAX_COWORKER_ID_LEN)?;
        if !matches!(
            self.role,
            WorkflowRole::Architect | WorkflowRole::Orchestrator
        ) {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "PlanningCommandRecord.role must be Architect or Orchestrator",
            ));
        }
        validate_non_native_worker_choice(self.role, &self.selected_choice)?;
        self.call_project_base.validate_syntax()?;
        validate_sha256_hex_digest(&self.context_digest, "context_digest").map_err(|_| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                "context_digest must be a 64-character lowercase hex SHA-256",
            )
        })?;
        validate_sha256_hex_digest(&self.request_body_digest, "request_body_digest").map_err(
            |_| {
                NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "request_body_digest must be a 64-character lowercase hex SHA-256",
                )
            },
        )?;
        validate_sha256_hex_digest(&self.command_digest, "command_digest").map_err(|_| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                "command_digest must be a 64-character lowercase hex SHA-256",
            )
        })?;

        let expected_cmd_id = deterministic_prefixed_id(
            PLANNING_COMMAND_ID_DOMAIN,
            "pcm",
            &[
                &self.binding_id,
                &self.thread_uuid,
                &self.grant_id,
                &self.command_key,
            ],
        );
        let expected_srv_key = deterministic_prefixed_id(
            PLANNING_COMMAND_SERVICE_KEY_DOMAIN,
            "psr",
            &[
                &self.binding_id,
                &self.thread_uuid,
                &self.grant_id,
                &self.command_key,
            ],
        );
        if self.command_id != expected_cmd_id || self.service_request_key != expected_srv_key {
            return Err(NativeError::new(
                NativeErrorCode::ProtocolError,
                "PlanningCommandRecord command_id or service_request_key does not match domain hash",
            ));
        }

        if self.payload.role() != self.role
            || self.payload.binding_id() != self.binding_id
            || self.payload.thread_uuid() != self.thread_uuid
            || self.payload.grant_id() != self.grant_id
            || self.payload.request_key() != self.service_request_key
        {
            return Err(NativeError::new(
                NativeErrorCode::ProtocolError,
                "PlanningCommandRecord payload scope or frozen service_request_key does not match record header",
            ));
        }

        let computed_body = self
            .payload
            .compute_request_body_digest(&self.command_key)?;
        if !constant_time_str_eq(&computed_body, &self.request_body_digest) {
            return Err(NativeError::new(
                NativeErrorCode::ProtocolError,
                "PlanningCommandRecord.request_body_digest does not match payload",
            ));
        }
        let computed_cmd = self.compute_command_digest()?;
        if !constant_time_str_eq(&computed_cmd, &self.command_digest) {
            return Err(NativeError::new(
                NativeErrorCode::ProtocolError,
                "PlanningCommandRecord.command_digest does not match immutable record fields",
            ));
        }

        if let Some(ref summary) = self.owner_summary {
            summary.validate_for_key(&self.ownership_key()?)?;
        }
        if let Some(ref res_ref) = self.result_ref {
            res_ref.validate()?;
        }
        if let Some(ref diag) = self.diagnostic {
            validate_bounded_text(diag, "diagnostic")?;
        }

        match self.status {
            PlanningCommandStatus::Queued => {
                if self.owner_summary.is_some()
                    || self.started_at_ms.is_some()
                    || self.completed_at_ms.is_some()
                    || self.result_ref.is_some()
                    || self.pending_reconciliation
                {
                    return Err(NativeError::new(
                        NativeErrorCode::ProtocolError,
                        "Queued PlanningCommandRecord must not carry owner_summary, timestamps, result_ref, or pending_reconciliation",
                    ));
                }
            }
            PlanningCommandStatus::Running => {
                if self.owner_summary.is_none()
                    || self.started_at_ms.is_none()
                    || self.completed_at_ms.is_some()
                    || self.result_ref.is_some()
                    || self.pending_reconciliation
                {
                    return Err(NativeError::new(
                        NativeErrorCode::ProtocolError,
                        "Running PlanningCommandRecord requires owner_summary and started_at_ms without terminal fields",
                    ));
                }
            }
            PlanningCommandStatus::Completed => {
                if self.owner_summary.is_none()
                    || self.started_at_ms.is_none()
                    || self.completed_at_ms.is_none()
                    || !self
                        .result_ref
                        .as_ref()
                        .is_some_and(PlanningCommandResultRef::is_artifact_verified)
                    || self.pending_reconciliation
                {
                    return Err(NativeError::new(
                        NativeErrorCode::ProtocolError,
                        "Completed PlanningCommandRecord requires owner_summary, started_at_ms, completed_at_ms, and a verified result_ref (artifact_verified = true)",
                    ));
                }
            }
            PlanningCommandStatus::KnownFailed => {
                if self.owner_summary.is_none()
                    || self.started_at_ms.is_none()
                    || self.completed_at_ms.is_none()
                    || self.result_ref.is_some()
                    || self.diagnostic.is_none()
                    || self.pending_reconciliation
                {
                    return Err(NativeError::new(
                        NativeErrorCode::ProtocolError,
                        "KnownFailed PlanningCommandRecord requires owner_summary, started_at_ms, completed_at_ms, and diagnostic",
                    ));
                }
            }
            PlanningCommandStatus::Unknown => {
                if self.owner_summary.is_none()
                    || self.started_at_ms.is_none()
                    || self.result_ref.is_some()
                    || self.diagnostic.is_none()
                    || !self.pending_reconciliation
                {
                    return Err(NativeError::new(
                        NativeErrorCode::ProtocolError,
                        "Unknown PlanningCommandRecord requires owner_summary, started_at_ms, diagnostic, and pending_reconciliation = true",
                    ));
                }
            }
        }
        Ok(())
    }
}

/// Persisted per-binding planning command ledger.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlanningCommandBindingLedger {
    /// Wire and storage schema version (`1`).
    pub schema_version: u32,
    /// Control-plane chat binding identifier.
    pub binding_id: String,
    /// Pinned canonical native Codex thread UUID (`None` when revision is `0`).
    pub thread_uuid: Option<String>,
    /// Monotonically increasing ledger revision (`0` when uninitialized).
    pub revision: u64,
    /// Ordered durable planning commands recorded for this binding.
    #[serde(default)]
    pub commands: Vec<PlanningCommandRecord>,
}

impl PlanningCommandBindingLedger {
    /// Constructs an empty revision-0 [`PlanningCommandBindingLedger`] for `binding_id`.
    pub fn empty(binding_id: impl Into<String>) -> Result<Self, NativeError> {
        let binding_id = binding_id.into();
        validate_bounded_identifier(&binding_id, "binding_id", MAX_COWORKER_ID_LEN)?;
        Ok(Self {
            schema_version: PLANNING_COMMAND_SCHEMA_VERSION,
            binding_id,
            thread_uuid: None,
            revision: 0,
            commands: Vec::new(),
        })
    }

    /// Validates the ledger header, thread pinning, capacity bounds, and every command record.
    pub fn validate(&self, expected_binding_id: &str) -> Result<(), NativeError> {
        if self.schema_version != PLANNING_COMMAND_SCHEMA_VERSION {
            return Err(NativeError::new(
                NativeErrorCode::ProtocolError,
                format!(
                    "unsupported planning command ledger schema_version {} (expected {PLANNING_COMMAND_SCHEMA_VERSION})",
                    self.schema_version
                ),
            ));
        }
        validate_bounded_identifier(&self.binding_id, "binding_id", MAX_COWORKER_ID_LEN)?;
        if self.binding_id != expected_binding_id {
            return Err(NativeError::new(
                NativeErrorCode::ProtocolError,
                "planning command ledger binding_id does not match expected binding file",
            ));
        }
        if self.revision == 0 {
            if self.thread_uuid.is_some() || !self.commands.is_empty() {
                return Err(NativeError::new(
                    NativeErrorCode::ProtocolError,
                    "revision-0 planning command ledger must not contain thread_uuid or commands",
                ));
            }
            return Ok(());
        }
        let pinned_thread = self.thread_uuid.as_deref().ok_or_else(|| {
            NativeError::new(
                NativeErrorCode::ProtocolError,
                "non-empty planning command ledger must pin a canonical thread_uuid",
            )
        })?;
        validate_canonical_thread_uuid("thread_uuid", pinned_thread).map_err(|_| {
            NativeError::new(
                NativeErrorCode::ProtocolError,
                "ledger.thread_uuid is not a canonical lowercase hyphenated non-nil UUID",
            )
        })?;
        if self.commands.len() > MAX_PLANNING_COMMANDS_PER_BINDING {
            return Err(NativeError::new(
                NativeErrorCode::OutputOverflow,
                "planning command ledger exceeds MAX_PLANNING_COMMANDS_PER_BINDING",
            ));
        }
        let mut seen_ids = BTreeSet::new();
        let mut seen_keys = BTreeSet::new();
        for cmd in &self.commands {
            cmd.validate()?;
            if cmd.binding_id != expected_binding_id || cmd.thread_uuid != pinned_thread {
                return Err(NativeError::new(
                    NativeErrorCode::ProtocolError,
                    "PlanningCommandRecord scope does not match binding ledger",
                ));
            }
            if !seen_ids.insert(cmd.command_id.as_str())
                || !seen_keys.insert(cmd.command_key.as_str())
            {
                return Err(NativeError::new(
                    NativeErrorCode::ProtocolError,
                    "duplicate command_id or command_key in planning command ledger",
                ));
            }
        }
        Ok(())
    }
}

/// Request to validate and enqueue a [`PlanningCommandPayload`] in [`PlanningCommandStore`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EnqueuePlanningCommandRequest {
    /// Control-plane chat binding identifier.
    pub binding_id: String,
    /// Canonical lowercase hyphenated non-nil native Codex thread UUID.
    pub thread_uuid: String,
    /// Standing [`PlanningAdviceGrant::grant_id`] authorizing the call.
    pub grant_id: String,
    /// Idempotent caller request key.
    pub command_key: String,
    /// Optional expected current planning command ledger revision (CAS check).
    pub expected_revision: Option<u64>,
    /// Typed Architect or Orchestrator planning request payload.
    pub payload: PlanningCommandPayload,
}

impl EnqueuePlanningCommandRequest {
    /// Constructs an [`EnqueuePlanningCommandRequest`] from a [`PlanningCommandPayload`], copying
    /// `binding_id`, `thread_uuid`, `grant_id`, and `request_key` from the payload.
    pub fn from_payload(payload: PlanningCommandPayload) -> Self {
        Self {
            binding_id: payload.binding_id().to_string(),
            thread_uuid: payload.thread_uuid().to_string(),
            grant_id: payload.grant_id().to_string(),
            command_key: payload.request_key().to_string(),
            expected_revision: None,
            payload,
        }
    }

    /// Constructs an [`EnqueuePlanningCommandRequest`] for [`RunArchitectAdviceRequest`].
    pub fn from_architect(request: RunArchitectAdviceRequest) -> Self {
        Self::from_payload(PlanningCommandPayload::Architect(request))
    }

    /// Constructs an [`EnqueuePlanningCommandRequest`] for [`OrchestrateWorkBriefPlanRequest`].
    pub fn from_orchestrator(request: OrchestrateWorkBriefPlanRequest) -> Self {
        Self::from_payload(PlanningCommandPayload::Orchestrator(request))
    }
}

/// Private durable per-binding store for [`PlanningCommandBindingLedger`].
#[derive(Debug, Clone)]
pub struct PlanningCommandStore {
    commands_dir: PathBuf,
    workflow_store: CoworkerWorkflowStore,
    registry_dir: PathBuf,
    ownership_store: CoworkerJobOwnershipStore,
    artifacts_dir: Option<PathBuf>,
}

enum CommandLedgerTxOutcome<T> {
    Idempotent(T),
    Mutated(T),
    MutatedThenErr(NativeError),
}

const ABANDONED_RUNNING_DIAGNOSTIC: &str =
    "running planning command owner lock was released before recording a terminal outcome; outcome is unknown pending artifact/receipt reconciliation";

impl PlanningCommandStore {
    /// Opens a [`PlanningCommandStore`] over an existing private (`0700`) `commands_dir` without
    /// creating directories, state files, or lockfiles.
    pub fn new(
        commands_dir: impl AsRef<Path>,
        workflow_store: CoworkerWorkflowStore,
        registry_dir: impl AsRef<Path>,
        ownership_store: CoworkerJobOwnershipStore,
    ) -> Result<Self, NativeError> {
        ensure_platform_supported()?;
        let clean_commands_dir =
            validate_existing_private_store_dir(commands_dir.as_ref(), "planning_commands_dir")?;
        let clean_registry_dir =
            validate_absolute_no_symlink_components(registry_dir.as_ref(), "registry_dir")?;
        Ok(Self {
            commands_dir: clean_commands_dir,
            workflow_store,
            registry_dir: clean_registry_dir,
            ownership_store,
            artifacts_dir: None,
        })
    }

    /// Convenience constructor opening [`CoworkerJobOwnershipStore`] on the same existing private
    /// (`0700`) `commands_dir` without creating directories, state files, or lockfiles.
    pub fn open_existing(
        commands_dir: impl AsRef<Path>,
        workflow_store: CoworkerWorkflowStore,
        registry_dir: impl AsRef<Path>,
    ) -> Result<Self, NativeError> {
        let ownership_store = CoworkerJobOwnershipStore::new(commands_dir.as_ref())?;
        Self::new(commands_dir, workflow_store, registry_dir, ownership_store)
    }

    /// Configures an existing private (`0700`) planning `artifacts_dir` used to verify persisted
    /// [`PersistedPlanningCallArtifact`] files when recording `Completed` outcomes.
    pub fn with_artifacts_dir(
        mut self,
        artifacts_dir: impl AsRef<Path>,
    ) -> Result<Self, NativeError> {
        let clean =
            validate_existing_private_store_dir(artifacts_dir.as_ref(), "planning_artifacts_dir")?;
        self.artifacts_dir = Some(clean);
        Ok(self)
    }

    /// Returns the validated private (`0700`) `commands_dir` path.
    pub fn commands_dir(&self) -> &Path {
        &self.commands_dir
    }

    /// Returns the underlying [`CoworkerWorkflowStore`].
    pub fn workflow_store(&self) -> &CoworkerWorkflowStore {
        &self.workflow_store
    }

    /// Returns the canonical [`threadlane_workflow::RegistryStore`] directory path.
    pub fn registry_dir(&self) -> &Path {
        &self.registry_dir
    }

    /// Returns the [`CoworkerJobOwnershipStore`] enforcing live OS kernel lock ownership.
    pub fn ownership_store(&self) -> &CoworkerJobOwnershipStore {
        &self.ownership_store
    }

    /// Returns the configured private (`0700`) planning `artifacts_dir` path, if any.
    pub fn artifacts_dir(&self) -> Option<&Path> {
        self.artifacts_dir.as_deref()
    }

    /// Returns the deterministic per-binding JSON state file path (`planning-commands-<tag>.json`).
    pub fn ledger_path_for_binding(&self, binding_id: &str) -> Result<PathBuf, NativeError> {
        validate_bounded_identifier(binding_id, "binding_id", MAX_COWORKER_ID_LEN)?;
        Ok(self
            .commands_dir
            .join(ledger_filename_for_binding(binding_id)))
    }

    /// Returns the deterministic per-binding lockfile path (`planning-commands-<tag>.lock`).
    pub fn lock_path_for_binding(&self, binding_id: &str) -> Result<PathBuf, NativeError> {
        validate_bounded_identifier(binding_id, "binding_id", MAX_COWORKER_ID_LEN)?;
        Ok(self
            .commands_dir
            .join(lock_filename_for_binding(binding_id)))
    }

    /// Pure read-only snapshot of `binding_id`'s [`PlanningCommandBindingLedger`].
    ///
    /// Never creates directories, state files, or lockfiles. Any persisted `Running` command whose
    /// live [`CoworkerJobOwnershipGuard`] kernel lock is no longer held by its recorded
    /// `owner_summary.instance_id` is surfaced as [`PlanningCommandStatus::Unknown`] with
    /// `pending_reconciliation = true`.
    pub fn read_binding_ledger(
        &self,
        binding_id: &str,
    ) -> Result<PlanningCommandBindingLedger, NativeError> {
        validate_bounded_identifier(binding_id, "binding_id", MAX_COWORKER_ID_LEN)?;
        let _ = validate_existing_private_store_dir(&self.commands_dir, "planning_commands_dir")?;
        let path = self.ledger_path_for_binding(binding_id)?;
        let Some(mut ledger) = self.read_ledger_unlocked(binding_id, &path)? else {
            return PlanningCommandBindingLedger::empty(binding_id);
        };
        for cmd in &mut ledger.commands {
            if cmd.status == PlanningCommandStatus::Running
                && !self.is_running_owner_still_active(cmd)?
            {
                cmd.status = PlanningCommandStatus::Unknown;
                cmd.pending_reconciliation = true;
                if cmd.diagnostic.is_none() {
                    cmd.diagnostic = Some(ABANDONED_RUNNING_DIAGNOSTIC.to_string());
                }
            }
        }
        Ok(ledger)
    }

    /// Pure read-only lookup of a command by `(binding_id, thread_uuid, command_id)`.
    ///
    /// Never creates directories, state files, or lockfiles.
    pub fn get_command(
        &self,
        binding_id: &str,
        thread_uuid: &str,
        command_id: &str,
    ) -> Result<Option<PlanningCommandRecord>, NativeError> {
        validate_bounded_identifier(binding_id, "binding_id", MAX_COWORKER_ID_LEN)?;
        validate_canonical_thread_uuid("thread_uuid", thread_uuid).map_err(|_| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                "thread_uuid must be a canonical lowercase hyphenated non-nil UUID",
            )
        })?;
        validate_bounded_identifier(command_id, "command_id", MAX_COWORKER_ID_LEN)?;
        let ledger = self.read_binding_ledger(binding_id)?;
        if let Some(ref pinned) = ledger.thread_uuid {
            if pinned != thread_uuid {
                return Err(NativeError::new(
                    NativeErrorCode::SecurityViolation,
                    "thread_uuid does not match the thread pinned to this binding's command ledger",
                ));
            }
        }
        Ok(ledger
            .commands
            .into_iter()
            .find(|c| c.command_id == command_id))
    }

    /// Pure read-only lookup of a command by `(binding_id, thread_uuid, command_key)`.
    ///
    /// Never creates directories, state files, or lockfiles.
    pub fn get_command_by_key(
        &self,
        binding_id: &str,
        thread_uuid: &str,
        command_key: &str,
    ) -> Result<Option<PlanningCommandRecord>, NativeError> {
        validate_bounded_identifier(binding_id, "binding_id", MAX_COWORKER_ID_LEN)?;
        validate_canonical_thread_uuid("thread_uuid", thread_uuid).map_err(|_| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                "thread_uuid must be a canonical lowercase hyphenated non-nil UUID",
            )
        })?;
        validate_bounded_identifier(command_key, "command_key", MAX_COWORKER_ID_LEN)?;
        let ledger = self.read_binding_ledger(binding_id)?;
        if let Some(ref pinned) = ledger.thread_uuid {
            if pinned != thread_uuid {
                return Err(NativeError::new(
                    NativeErrorCode::SecurityViolation,
                    "thread_uuid does not match the thread pinned to this binding's command ledger",
                ));
            }
        }
        Ok(ledger
            .commands
            .into_iter()
            .find(|c| c.command_key == command_key))
    }

    /// Pure read-only list of all commands recorded for `(binding_id, thread_uuid)`.
    ///
    /// Never creates directories, state files, or lockfiles.
    pub fn list_commands(
        &self,
        binding_id: &str,
        thread_uuid: &str,
    ) -> Result<Vec<PlanningCommandRecord>, NativeError> {
        validate_bounded_identifier(binding_id, "binding_id", MAX_COWORKER_ID_LEN)?;
        validate_canonical_thread_uuid("thread_uuid", thread_uuid).map_err(|_| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                "thread_uuid must be a canonical lowercase hyphenated non-nil UUID",
            )
        })?;
        let ledger = self.read_binding_ledger(binding_id)?;
        if let Some(ref pinned) = ledger.thread_uuid {
            if pinned != thread_uuid {
                return Err(NativeError::new(
                    NativeErrorCode::SecurityViolation,
                    "thread_uuid does not match the thread pinned to this binding's command ledger",
                ));
            }
        }
        Ok(ledger.commands)
    }

    /// Validates `request` against the live [`PlanningAdviceGrant`], role/profile/policy registry,
    /// and `Orchestrator` job/brief or `Architect` context dependencies without reserving provider
    /// budget or executing a worker, and durably records (or idempotently returns) the
    /// [`PlanningCommandRecord`].
    pub fn enqueue_command(
        &self,
        request: &EnqueuePlanningCommandRequest,
    ) -> Result<PlanningCommandRecord, NativeError> {
        validate_bounded_identifier(&request.binding_id, "binding_id", MAX_COWORKER_ID_LEN)?;
        validate_canonical_thread_uuid("thread_uuid", &request.thread_uuid).map_err(|_| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                "thread_uuid must be a canonical lowercase hyphenated non-nil UUID",
            )
        })?;
        validate_bounded_identifier(&request.grant_id, "grant_id", MAX_COWORKER_ID_LEN)?;
        validate_bounded_identifier(&request.command_key, "command_key", MAX_COWORKER_ID_LEN)?;

        if request.payload.binding_id() != request.binding_id
            || request.payload.thread_uuid() != request.thread_uuid
            || request.payload.grant_id() != request.grant_id
        {
            return Err(NativeError::new(
                NativeErrorCode::SecurityViolation,
                "EnqueuePlanningCommandRequest scope does not match payload binding_id, thread_uuid, or grant_id",
            ));
        }
        if !request.payload.request_key().is_empty()
            && request.payload.request_key() != request.command_key
        {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "payload.request_key must match EnqueuePlanningCommandRequest.command_key",
            ));
        }

        let request_body_digest = request
            .payload
            .compute_request_body_digest(&request.command_key)?;
        let now_ms = current_wall_clock_ms()?;

        let (_, record) =
            self.transact_binding_ledger(&request.binding_id, request.expected_revision, |ledger| {
                let abandoned_mutated = self.promote_abandoned_running_in_ledger(ledger, now_ms)?;
                ensure_ledger_thread(ledger, &request.thread_uuid)?;

                if let Some(existing) = ledger
                    .commands
                    .iter()
                    .find(|c| c.command_key == request.command_key)
                {
                    existing.validate()?;
                    if !constant_time_str_eq(&existing.request_body_digest, &request_body_digest)
                        || existing.grant_id != request.grant_id
                        || existing.role != request.payload.role()
                    {
                        let err = NativeError::new(
                            NativeErrorCode::ValidationFailed,
                            "planning command_key was already used with a different request payload",
                        );
                        return if abandoned_mutated {
                            Ok(CommandLedgerTxOutcome::MutatedThenErr(err))
                        } else {
                            Err(err)
                        };
                    }
                    let existing_cloned = existing.clone();
                    return if abandoned_mutated {
                        Ok(CommandLedgerTxOutcome::Mutated(existing_cloned))
                    } else {
                        Ok(CommandLedgerTxOutcome::Idempotent(existing_cloned))
                    };
                }

                if ledger.commands.len() >= MAX_PLANNING_COMMANDS_PER_BINDING {
                    let err = NativeError::new(
                        NativeErrorCode::OutputOverflow,
                        "planning command ledger reached MAX_PLANNING_COMMANDS_PER_BINDING",
                    );
                    return if abandoned_mutated {
                        Ok(CommandLedgerTxOutcome::MutatedThenErr(err))
                    } else {
                        Err(err)
                    };
                }

                if ledger
                    .commands
                    .iter()
                    .any(|c| c.status.is_in_flight_or_unknown())
                {
                    let err = NativeError::new(
                        NativeErrorCode::AmbiguousWakeupPendingReconciliation,
                        "another planning command on this binding is Running or Unknown pending reconciliation; replay is forbidden",
                    );
                    return if abandoned_mutated {
                        Ok(CommandLedgerTxOutcome::MutatedThenErr(err))
                    } else {
                        Err(err)
                    };
                }

                let validated = match self.validate_new_command_against_stores(request, now_ms) {
                    Ok(v) => v,
                    Err(err) => {
                        return if abandoned_mutated {
                            Ok(CommandLedgerTxOutcome::MutatedThenErr(err))
                        } else {
                            Err(err)
                        };
                    }
                };

                let command_id = deterministic_prefixed_id(
                    PLANNING_COMMAND_ID_DOMAIN,
                    "pcm",
                    &[
                        &request.binding_id,
                        &request.thread_uuid,
                        &request.grant_id,
                        &request.command_key,
                    ],
                );
                let service_request_key = deterministic_prefixed_id(
                    PLANNING_COMMAND_SERVICE_KEY_DOMAIN,
                    "psr",
                    &[
                        &request.binding_id,
                        &request.thread_uuid,
                        &request.grant_id,
                        &request.command_key,
                    ],
                );
                let frozen_payload = request
                    .payload
                    .freeze_for_record(&service_request_key, now_ms);

                let mut record = PlanningCommandRecord {
                    command_id,
                    command_key: request.command_key.clone(),
                    service_request_key,
                    binding_id: request.binding_id.clone(),
                    thread_uuid: request.thread_uuid.clone(),
                    grant_id: request.grant_id.clone(),
                    role: request.payload.role(),
                    selected_choice: validated.selected_choice,
                    call_project_base: validated.call_project_base,
                    context_digest: validated.context_digest,
                    request_body_digest,
                    command_digest: String::new(),
                    payload: frozen_payload,
                    status: PlanningCommandStatus::Queued,
                    pending_reconciliation: false,
                    owner_summary: None,
                    result_ref: None,
                    diagnostic: None,
                    enqueued_at_ms: now_ms,
                    started_at_ms: None,
                    updated_at_ms: now_ms,
                    completed_at_ms: None,
                };
                record.command_digest = record.compute_command_digest()?;
                record.validate()?;

                ledger.commands.push(record.clone());
                Ok(CommandLedgerTxOutcome::Mutated(record))
            })?;

        Ok(record)
    }

    /// Transitions a `Queued` command to `Running` under an active [`CoworkerJobOwnershipGuard`]
    /// whose scope matches `(binding_id, thread_uuid, command_id)`.
    pub fn claim_running(
        &self,
        guard: &CoworkerJobOwnershipGuard,
        binding_id: &str,
        thread_uuid: &str,
        command_id: &str,
        expected_revision: Option<u64>,
    ) -> Result<PlanningCommandRecord, NativeError> {
        let expected_key = CoworkerJobOwnershipKey::new(binding_id, thread_uuid, command_id)?;
        let active_summary = self
            .ownership_store
            .verify_live_guard(guard, &expected_key)?;
        let now_ms = current_wall_clock_ms()?;

        let (_, updated) =
            self.transact_binding_ledger(binding_id, expected_revision, |ledger| {
                let abandoned_mutated = self.promote_abandoned_running_in_ledger(ledger, now_ms)?;
                ensure_ledger_thread(ledger, thread_uuid)?;

                let cmd_idx = ledger
                    .commands
                    .iter()
                    .position(|c| c.command_id == command_id)
                    .ok_or_else(|| {
                        NativeError::new(
                            NativeErrorCode::ValidationFailed,
                            format!("planning command `{command_id}` was not found in binding"),
                        )
                    })?;

                ledger.commands[cmd_idx].validate()?;
                if ledger.commands[cmd_idx].binding_id != binding_id
                    || ledger.commands[cmd_idx].thread_uuid != thread_uuid
                {
                    return Err(NativeError::new(
                        NativeErrorCode::SecurityViolation,
                        "planning command scope does not match claim_running arguments",
                    ));
                }

                match ledger.commands[cmd_idx].status {
                    PlanningCommandStatus::Queued => {}
                    PlanningCommandStatus::Unknown | PlanningCommandStatus::Running => {
                        let err = NativeError::new(
                            NativeErrorCode::AmbiguousWakeupPendingReconciliation,
                            format!(
                                "planning command `{command_id}` is in `{:?}` state and cannot be claimed again",
                                ledger.commands[cmd_idx].status
                            ),
                        );
                        return if abandoned_mutated {
                            Ok(CommandLedgerTxOutcome::MutatedThenErr(err))
                        } else {
                            Err(err)
                        };
                    }
                    status => {
                        let err = NativeError::new(
                            NativeErrorCode::ValidationFailed,
                            format!(
                                "planning command `{command_id}` is in terminal `{status:?}` state and cannot be claimed"
                            ),
                        );
                        return if abandoned_mutated {
                            Ok(CommandLedgerTxOutcome::MutatedThenErr(err))
                        } else {
                            Err(err)
                        };
                    }
                }

                if ledger
                    .commands
                    .iter()
                    .enumerate()
                    .any(|(idx, c)| idx != cmd_idx && c.status.is_in_flight_or_unknown())
                {
                    let err = NativeError::new(
                        NativeErrorCode::AmbiguousWakeupPendingReconciliation,
                        "another planning command on this binding is Running or Unknown pending reconciliation",
                    );
                    return if abandoned_mutated {
                        Ok(CommandLedgerTxOutcome::MutatedThenErr(err))
                    } else {
                        Err(err)
                    };
                }

                // Re-verify active grant, registry, job/dependency state, and frozen context before Running.
                let claim_req = EnqueuePlanningCommandRequest::from_payload(
                    ledger.commands[cmd_idx].payload.clone(),
                );
                let current_ctx =
                    match self.validate_new_command_against_stores(&claim_req, now_ms) {
                        Ok(ctx) => ctx,
                        Err(err) => {
                            return if abandoned_mutated {
                                Ok(CommandLedgerTxOutcome::MutatedThenErr(err))
                            } else {
                                Err(err)
                            };
                        }
                    };
                if current_ctx.selected_choice != ledger.commands[cmd_idx].selected_choice
                    || current_ctx.call_project_base != ledger.commands[cmd_idx].call_project_base
                    || !constant_time_str_eq(
                        &current_ctx.context_digest,
                        &ledger.commands[cmd_idx].context_digest,
                    )
                {
                    let err = NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "planning command context_digest, call_project_base, or selected_choice drifted before claim_running",
                    );
                    return if abandoned_mutated {
                        Ok(CommandLedgerTxOutcome::MutatedThenErr(err))
                    } else {
                        Err(err)
                    };
                }

                let cmd = &mut ledger.commands[cmd_idx];
                cmd.status = PlanningCommandStatus::Running;
                cmd.pending_reconciliation = false;
                cmd.owner_summary = Some(active_summary.clone());
                cmd.started_at_ms = Some(now_ms);
                cmd.updated_at_ms = now_ms;
                cmd.validate()?;

                Ok(CommandLedgerTxOutcome::Mutated(cmd.clone()))
            })?;

        Ok(updated)
    }

    /// Records a verified `Completed` outcome on a `Running` command owned by `guard`.
    pub fn record_completed(
        &self,
        guard: &CoworkerJobOwnershipGuard,
        binding_id: &str,
        thread_uuid: &str,
        command_id: &str,
        result_ref: PlanningCommandResultRef,
        expected_revision: Option<u64>,
    ) -> Result<PlanningCommandRecord, NativeError> {
        let expected_key = CoworkerJobOwnershipKey::new(binding_id, thread_uuid, command_id)?;
        let _ = self
            .ownership_store
            .verify_live_guard(guard, &expected_key)?;
        result_ref.validate()?;
        let now_ms = current_wall_clock_ms()?;

        let (_, updated) =
            self.transact_binding_ledger(binding_id, expected_revision, |ledger| {
                let abandoned_mutated = self.promote_abandoned_running_in_ledger(ledger, now_ms)?;
                ensure_ledger_thread(ledger, thread_uuid)?;

                let cmd_idx = ledger
                    .commands
                    .iter()
                    .position(|c| c.command_id == command_id)
                    .ok_or_else(|| {
                        NativeError::new(
                            NativeErrorCode::ValidationFailed,
                            format!("planning command `{command_id}` was not found in binding"),
                        )
                    })?;

                let cmd = &ledger.commands[cmd_idx];
                cmd.validate()?;
                if cmd.status != PlanningCommandStatus::Running {
                    let err = NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        format!(
                            "planning command `{command_id}` must be Running to record Completed (actual `{:?}`)",
                            cmd.status
                        ),
                    );
                    return if abandoned_mutated {
                        Ok(CommandLedgerTxOutcome::MutatedThenErr(err))
                    } else {
                        Err(err)
                    };
                }
                let recorded_owner = cmd.owner_summary.as_ref().ok_or_else(|| {
                    NativeError::new(
                        NativeErrorCode::ProtocolError,
                        "Running command is missing owner_summary",
                    )
                })?;
                if let Err(err) = guard.validate_summary_before_effect(&expected_key, recorded_owner)
                {
                    return if abandoned_mutated {
                        Ok(CommandLedgerTxOutcome::MutatedThenErr(err))
                    } else {
                        Err(err)
                    };
                }

                let verified_ref = self.verify_completed_result_against_stores(cmd, result_ref)?;

                let cmd_mut = &mut ledger.commands[cmd_idx];
                cmd_mut.status = PlanningCommandStatus::Completed;
                cmd_mut.pending_reconciliation = false;
                cmd_mut.result_ref = Some(verified_ref);
                cmd_mut.diagnostic = None;
                cmd_mut.updated_at_ms = now_ms;
                cmd_mut.completed_at_ms = Some(now_ms);
                cmd_mut.validate()?;

                Ok(CommandLedgerTxOutcome::Mutated(cmd_mut.clone()))
            })?;

        Ok(updated)
    }

    /// Records a `KnownFailed` outcome on a `Running` command owned by `guard`.
    pub fn record_known_failed(
        &self,
        guard: &CoworkerJobOwnershipGuard,
        binding_id: &str,
        thread_uuid: &str,
        command_id: &str,
        diagnostic: &str,
        expected_revision: Option<u64>,
    ) -> Result<PlanningCommandRecord, NativeError> {
        let expected_key = CoworkerJobOwnershipKey::new(binding_id, thread_uuid, command_id)?;
        let _ = self
            .ownership_store
            .verify_live_guard(guard, &expected_key)?;
        validate_bounded_text(diagnostic, "diagnostic")?;
        let now_ms = current_wall_clock_ms()?;

        let (_, updated) =
            self.transact_binding_ledger(binding_id, expected_revision, |ledger| {
                let abandoned_mutated = self.promote_abandoned_running_in_ledger(ledger, now_ms)?;
                ensure_ledger_thread(ledger, thread_uuid)?;

                let cmd_idx = ledger
                    .commands
                    .iter()
                    .position(|c| c.command_id == command_id)
                    .ok_or_else(|| {
                        NativeError::new(
                            NativeErrorCode::ValidationFailed,
                            format!("planning command `{command_id}` was not found in binding"),
                        )
                    })?;

                let cmd = &ledger.commands[cmd_idx];
                cmd.validate()?;
                if cmd.status != PlanningCommandStatus::Running {
                    let err = NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        format!(
                            "planning command `{command_id}` must be Running to record KnownFailed (actual `{:?}`)",
                            cmd.status
                        ),
                    );
                    return if abandoned_mutated {
                        Ok(CommandLedgerTxOutcome::MutatedThenErr(err))
                    } else {
                        Err(err)
                    };
                }
                let recorded_owner = cmd.owner_summary.as_ref().ok_or_else(|| {
                    NativeError::new(
                        NativeErrorCode::ProtocolError,
                        "Running command is missing owner_summary",
                    )
                })?;
                if let Err(err) = guard.validate_summary_before_effect(&expected_key, recorded_owner)
                {
                    return if abandoned_mutated {
                        Ok(CommandLedgerTxOutcome::MutatedThenErr(err))
                    } else {
                        Err(err)
                    };
                }

                let cmd_mut = &mut ledger.commands[cmd_idx];
                cmd_mut.status = PlanningCommandStatus::KnownFailed;
                cmd_mut.pending_reconciliation = false;
                cmd_mut.result_ref = None;
                cmd_mut.diagnostic = Some(diagnostic.to_string());
                cmd_mut.updated_at_ms = now_ms;
                cmd_mut.completed_at_ms = Some(now_ms);
                cmd_mut.validate()?;

                Ok(CommandLedgerTxOutcome::Mutated(cmd_mut.clone()))
            })?;

        Ok(updated)
    }

    /// Records an `Unknown` outcome (`pending_reconciliation = true`) on a `Running` command
    /// owned by `guard`.
    pub fn record_unknown(
        &self,
        guard: &CoworkerJobOwnershipGuard,
        binding_id: &str,
        thread_uuid: &str,
        command_id: &str,
        diagnostic: &str,
        expected_revision: Option<u64>,
    ) -> Result<PlanningCommandRecord, NativeError> {
        let expected_key = CoworkerJobOwnershipKey::new(binding_id, thread_uuid, command_id)?;
        let _ = self
            .ownership_store
            .verify_live_guard(guard, &expected_key)?;
        validate_bounded_text(diagnostic, "diagnostic")?;
        let now_ms = current_wall_clock_ms()?;

        let (_, updated) =
            self.transact_binding_ledger(binding_id, expected_revision, |ledger| {
                let abandoned_mutated = self.promote_abandoned_running_in_ledger(ledger, now_ms)?;
                ensure_ledger_thread(ledger, thread_uuid)?;

                let cmd_idx = ledger
                    .commands
                    .iter()
                    .position(|c| c.command_id == command_id)
                    .ok_or_else(|| {
                        NativeError::new(
                            NativeErrorCode::ValidationFailed,
                            format!("planning command `{command_id}` was not found in binding"),
                        )
                    })?;

                let cmd = &ledger.commands[cmd_idx];
                cmd.validate()?;
                if cmd.status != PlanningCommandStatus::Running {
                    let err = NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        format!(
                            "planning command `{command_id}` must be Running to record Unknown (actual `{:?}`)",
                            cmd.status
                        ),
                    );
                    return if abandoned_mutated {
                        Ok(CommandLedgerTxOutcome::MutatedThenErr(err))
                    } else {
                        Err(err)
                    };
                }
                let recorded_owner = cmd.owner_summary.as_ref().ok_or_else(|| {
                    NativeError::new(
                        NativeErrorCode::ProtocolError,
                        "Running command is missing owner_summary",
                    )
                })?;
                if let Err(err) = guard.validate_summary_before_effect(&expected_key, recorded_owner)
                {
                    return if abandoned_mutated {
                        Ok(CommandLedgerTxOutcome::MutatedThenErr(err))
                    } else {
                        Err(err)
                    };
                }

                let cmd_mut = &mut ledger.commands[cmd_idx];
                cmd_mut.status = PlanningCommandStatus::Unknown;
                cmd_mut.pending_reconciliation = true;
                cmd_mut.result_ref = None;
                cmd_mut.diagnostic = Some(diagnostic.to_string());
                cmd_mut.updated_at_ms = now_ms;
                cmd_mut.validate()?;

                Ok(CommandLedgerTxOutcome::Mutated(cmd_mut.clone()))
            })?;

        Ok(updated)
    }

    /// Re-verifies an already-`Completed` [`PlanningCommandRecord`] against the durable workflow
    /// store and persisted planning report artifact on disk without mutating state.
    pub fn verify_completed_command(
        &self,
        cmd: &PlanningCommandRecord,
    ) -> Result<PlanningCommandResultRef, NativeError> {
        cmd.validate()?;
        if cmd.status != PlanningCommandStatus::Completed {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!(
                    "planning command `{}` is not Completed (actual `{:?}`)",
                    cmd.command_id, cmd.status
                ),
            ));
        }
        let result_ref = cmd.result_ref.clone().ok_or_else(|| {
            NativeError::new(
                NativeErrorCode::ProtocolError,
                "Completed planning command is missing result_ref",
            )
        })?;
        self.verify_completed_result_against_stores(cmd, result_ref)
    }

    /// Verifies a candidate without forging a terminal command before its receipt is checked.
    pub(crate) fn verify_reconciliation_candidate(
        &self,
        cmd: &PlanningCommandRecord,
        result_ref: PlanningCommandResultRef,
    ) -> Result<PlanningCommandResultRef, NativeError> {
        cmd.validate()?;
        if cmd.status != PlanningCommandStatus::Unknown || !cmd.pending_reconciliation {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "reconciliation candidate requires an Unknown command",
            ));
        }
        result_ref.validate()?;
        self.verify_completed_result_against_stores(cmd, result_ref)
    }

    /// Reconciles an `Unknown` (`pending_reconciliation = true`) command to `Completed` under a
    /// live [`CoworkerJobOwnershipGuard`] when a complete, store- and artifact-verified service
    /// outcome produced for this command already exists on disk without executing a new model call.
    pub fn reconcile_unknown_completed(
        &self,
        guard: &CoworkerJobOwnershipGuard,
        binding_id: &str,
        thread_uuid: &str,
        command_id: &str,
        result_ref: PlanningCommandResultRef,
        expected_revision: Option<u64>,
    ) -> Result<PlanningCommandRecord, NativeError> {
        let expected_key = CoworkerJobOwnershipKey::new(binding_id, thread_uuid, command_id)?;
        let active_summary = self
            .ownership_store
            .verify_live_guard(guard, &expected_key)?;
        result_ref.validate()?;
        if matches!(
            result_ref,
            PlanningCommandResultRef::ArchitectAdvice {
                reused_existing: true,
                ..
            }
        ) {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "Unknown planning command reconciliation requires an exact reservation and artifact produced for this command's service_request_key",
            ));
        }
        let now_ms = current_wall_clock_ms()?;

        let (_, updated) =
            self.transact_binding_ledger(binding_id, expected_revision, |ledger| {
                let abandoned_mutated = self.promote_abandoned_running_in_ledger(ledger, now_ms)?;
                ensure_ledger_thread(ledger, thread_uuid)?;

                let cmd_idx = ledger
                    .commands
                    .iter()
                    .position(|c| c.command_id == command_id)
                    .ok_or_else(|| {
                        NativeError::new(
                            NativeErrorCode::ValidationFailed,
                            format!("planning command `{command_id}` was not found in binding"),
                        )
                    })?;

                let cmd = &ledger.commands[cmd_idx];
                cmd.validate()?;
                if cmd.binding_id != binding_id || cmd.thread_uuid != thread_uuid {
                    return Err(NativeError::new(
                        NativeErrorCode::SecurityViolation,
                        "planning command scope does not match reconcile_unknown_completed arguments",
                    ));
                }
                if cmd.status != PlanningCommandStatus::Unknown || !cmd.pending_reconciliation {
                    let err = NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        format!(
                            "planning command `{command_id}` must be Unknown pending reconciliation to reconcile (actual `{:?}`)",
                            cmd.status
                        ),
                    );
                    return if abandoned_mutated {
                        Ok(CommandLedgerTxOutcome::MutatedThenErr(err))
                    } else {
                        Err(err)
                    };
                }
                if let Err(err) = guard.validate_summary_before_effect(&expected_key, active_summary)
                {
                    return if abandoned_mutated {
                        Ok(CommandLedgerTxOutcome::MutatedThenErr(err))
                    } else {
                        Err(err)
                    };
                }

                let verified_ref = match self.verify_completed_result_against_stores(cmd, result_ref)
                {
                    Ok(v) => v,
                    Err(err) => {
                        return if abandoned_mutated {
                            Ok(CommandLedgerTxOutcome::MutatedThenErr(err))
                        } else {
                            Err(err)
                        };
                    }
                };

                let cmd_mut = &mut ledger.commands[cmd_idx];
                cmd_mut.status = PlanningCommandStatus::Completed;
                cmd_mut.pending_reconciliation = false;
                cmd_mut.owner_summary = Some(active_summary.clone());
                cmd_mut.result_ref = Some(verified_ref);
                cmd_mut.diagnostic = None;
                cmd_mut.updated_at_ms = now_ms;
                cmd_mut.completed_at_ms = Some(now_ms);
                cmd_mut.validate()?;

                Ok(CommandLedgerTxOutcome::Mutated(cmd_mut.clone()))
            })?;

        Ok(updated)
    }
}

struct ValidatedEnqueueContext {
    selected_choice: SelectedRoleChoice,
    call_project_base: CanonicalProjectBaseRef,
    context_digest: String,
}

impl PlanningCommandStore {
    fn is_running_owner_still_active(
        &self,
        cmd: &PlanningCommandRecord,
    ) -> Result<bool, NativeError> {
        let Some(ref expected_owner) = cmd.owner_summary else {
            return Ok(false);
        };
        let key = cmd.ownership_key()?;
        match self.ownership_store.inspect_job_lock(&key)? {
            CoworkerJobLockState::HeldByActiveProcess {
                recorded_summary: Some(ref active_summary),
                lock_file_identity,
            } => Ok(lock_file_identity == expected_owner.lock_file_identity
                && constant_time_str_eq(&active_summary.instance_id, &expected_owner.instance_id)
                && constant_time_str_eq(
                    &active_summary.challenge_digest,
                    &expected_owner.challenge_digest,
                )
                && active_summary == expected_owner),
            _ => Ok(false),
        }
    }

    fn promote_abandoned_running_in_ledger(
        &self,
        ledger: &mut PlanningCommandBindingLedger,
        now_ms: u64,
    ) -> Result<bool, NativeError> {
        let mut mutated = false;
        for cmd in &mut ledger.commands {
            if cmd.status == PlanningCommandStatus::Running
                && !self.is_running_owner_still_active(cmd)?
            {
                cmd.status = PlanningCommandStatus::Unknown;
                cmd.pending_reconciliation = true;
                if cmd.diagnostic.is_none() {
                    cmd.diagnostic = Some(ABANDONED_RUNNING_DIAGNOSTIC.to_string());
                }
                cmd.updated_at_ms = now_ms;
                cmd.validate()?;
                mutated = true;
            }
        }
        Ok(mutated)
    }

    fn transact_binding_ledger<T, F>(
        &self,
        binding_id: &str,
        expected_revision: Option<u64>,
        f: F,
    ) -> Result<(PlanningCommandBindingLedger, T), NativeError>
    where
        F: FnOnce(
            &mut PlanningCommandBindingLedger,
        ) -> Result<CommandLedgerTxOutcome<T>, NativeError>,
    {
        validate_bounded_identifier(binding_id, "binding_id", MAX_COWORKER_ID_LEN)?;
        let _ = validate_existing_private_store_dir(&self.commands_dir, "planning_commands_dir")?;
        let lock_filename = lock_filename_for_binding(binding_id);
        let _lock = acquire_cross_process_lock(
            &self.commands_dir,
            &lock_filename,
            "planning_command_ledger_lock",
        )?;
        let path = self.ledger_path_for_binding(binding_id)?;
        let original = match self.read_ledger_unlocked(binding_id, &path)? {
            Some(existing) => existing,
            None => PlanningCommandBindingLedger::empty(binding_id)?,
        };
        let mut working = original.clone();

        match f(&mut working)? {
            CommandLedgerTxOutcome::Idempotent(val) => {
                if working != original {
                    return Err(NativeError::new(
                        NativeErrorCode::ProtocolError,
                        "planning command transaction returned Idempotent after mutating working ledger",
                    ));
                }
                Ok((original, val))
            }
            CommandLedgerTxOutcome::Mutated(val) => {
                if let Some(exp) = expected_revision {
                    if exp != original.revision {
                        return Err(NativeError::new(
                            NativeErrorCode::ValidationFailed,
                            format!(
                                "expected_revision {exp} does not match current planning command ledger revision {}",
                                original.revision
                            ),
                        ));
                    }
                }
                working.revision = original.revision.checked_add(1).ok_or_else(|| {
                    NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "planning command ledger revision overflow",
                    )
                })?;
                self.write_ledger_unlocked(binding_id, &working)?;
                Ok((working, val))
            }
            CommandLedgerTxOutcome::MutatedThenErr(err) => {
                working.revision = original.revision.checked_add(1).ok_or_else(|| {
                    NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "planning command ledger revision overflow",
                    )
                })?;
                self.write_ledger_unlocked(binding_id, &working)?;
                Err(err)
            }
        }
    }

    fn read_ledger_unlocked(
        &self,
        binding_id: &str,
        path: &Path,
    ) -> Result<Option<PlanningCommandBindingLedger>, NativeError> {
        let Some(bytes) = read_private_file_bounded(
            path,
            MAX_PLANNING_COMMAND_LEDGER_BYTES,
            "planning_command_ledger",
        )?
        else {
            return Ok(None);
        };
        let ledger: PlanningCommandBindingLedger =
            serde_json::from_slice(&bytes).map_err(|_| {
                NativeError::new(
                    NativeErrorCode::ProtocolError,
                    "persisted planning command ledger JSON is corrupt or invalid",
                )
            })?;
        ledger.validate(binding_id)?;
        Ok(Some(ledger))
    }

    fn write_ledger_unlocked(
        &self,
        binding_id: &str,
        ledger: &PlanningCommandBindingLedger,
    ) -> Result<(), NativeError> {
        ledger.validate(binding_id)?;
        let mut bytes = serde_json::to_vec_pretty(ledger).map_err(|_| {
            NativeError::new(
                NativeErrorCode::ProtocolError,
                "failed to serialize PlanningCommandBindingLedger",
            )
        })?;
        bytes.push(b'\n');
        if bytes.len() > MAX_PLANNING_COMMAND_LEDGER_BYTES {
            return Err(NativeError::new(
                NativeErrorCode::OutputOverflow,
                "planning command ledger exceeds MAX_PLANNING_COMMAND_LEDGER_BYTES",
            ));
        }
        let filename = ledger_filename_for_binding(binding_id);
        write_private_file_atomic(
            &self.commands_dir,
            &filename,
            &bytes,
            "planning_command_ledger",
        )
        .map(|_| ())
    }

    fn validate_new_command_against_stores(
        &self,
        request: &EnqueuePlanningCommandRequest,
        now_ms: u64,
    ) -> Result<ValidatedEnqueueContext, NativeError> {
        let wf_ledger = self
            .workflow_store
            .read_binding_ledger(&request.binding_id)?;
        let grant = find_active_grant(
            &wf_ledger,
            &request.binding_id,
            &request.thread_uuid,
            &request.grant_id,
            request.payload.role(),
            now_ms,
        )?;

        if wf_ledger
            .planning_grants
            .iter()
            .any(|g| g.reservations.iter().any(|r| r.status.is_ambiguous()))
        {
            return Err(NativeError::new(
                NativeErrorCode::AmbiguousWakeupPendingReconciliation,
                "a planning call reservation on this binding has an uncertain outcome pending reconciliation",
            ));
        }

        let reg_snapshot = read_registry_snapshot_readonly(&self.registry_dir)?;
        if reg_snapshot.revision != grant.registry_revision {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!(
                    "workflow registry revision drifted from standing grant (grant {}, actual {})",
                    grant.registry_revision, reg_snapshot.revision
                ),
            ));
        }
        let selected_choice = resolve_headless_choice(
            &reg_snapshot,
            grant,
            request.payload.role(),
            request.payload.difficulty(),
        )?;

        match &request.payload {
            PlanningCommandPayload::Architect(req) => {
                if let Some(exp_rev) = req.expected_ledger_revision {
                    if wf_ledger.ledger_revision != exp_rev {
                        return Err(NativeError::new(
                            NativeErrorCode::ValidationFailed,
                            format!(
                                "coworker workflow ledger revision drifted (expected {exp_rev}, actual {})",
                                wf_ledger.ledger_revision
                            ),
                        ));
                    }
                }
                validate_bounded_text(&req.question_summary, "question_summary")?;
                if req.prior_advice_context.len() > MAX_WORK_BRIEF_ITEMS {
                    return Err(NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "prior_advice_context exceeds maximum item count",
                    ));
                }
                verify_prior_advice_list(
                    &wf_ledger,
                    grant,
                    &request.binding_id,
                    &request.thread_uuid,
                    &req.prior_advice_context,
                )?;
                let context_digest = req.context_dependencies.context_digest()?;
                if !constant_time_str_eq(
                    &req.context_dependencies.policy_digest,
                    &grant.policy_digest,
                ) {
                    return Err(NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "ArchitectContextDependencies.policy_digest does not match standing grant policy_digest",
                    ));
                }
                verify_call_base_against_grant(
                    &grant.project_base,
                    &req.context_dependencies.project_base,
                )?;
                verify_dependency_blobs_at_base(
                    &req.context_dependencies.project_base,
                    &req.context_dependencies.dependency_file_blobs,
                )?;
                Ok(ValidatedEnqueueContext {
                    selected_choice,
                    call_project_base: req.context_dependencies.project_base.clone(),
                    context_digest,
                })
            }
            PlanningCommandPayload::Orchestrator(req) => {
                if let Some(exp_rev) = req.expected_ledger_revision {
                    if wf_ledger.ledger_revision != exp_rev {
                        return Err(NativeError::new(
                            NativeErrorCode::ValidationFailed,
                            format!(
                                "coworker workflow ledger revision drifted (expected {exp_rev}, actual {})",
                                wf_ledger.ledger_revision
                            ),
                        ));
                    }
                }
                validate_bounded_identifier(&req.job_id, "job_id", MAX_COWORKER_ID_LEN)?;
                if let Some(ref plan_id) = req.plan_id {
                    validate_bounded_identifier(plan_id, "plan_id", MAX_COWORKER_ID_LEN)?;
                }
                validate_unique_id_list(
                    &req.architect_advice_ids,
                    "architect_advice_ids",
                    MAX_ARCHITECT_ADVICE_RECORDS_PER_BINDING,
                )?;
                if req.max_architect_rounds > MAX_ORCHESTRATOR_ARCHITECT_ROUNDS {
                    return Err(NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        format!(
                            "max_architect_rounds cannot exceed {MAX_ORCHESTRATOR_ARCHITECT_ROUNDS}"
                        ),
                    ));
                }
                if req.max_architect_rounds > 0 {
                    if grant.usage.reserved_architect_calls >= grant.budget.max_architect_calls {
                        return Err(NativeError::new(
                            NativeErrorCode::ValidationFailed,
                            "planning grant architect call budget is exhausted for max_architect_rounds > 0",
                        ));
                    }
                    let _ = resolve_headless_choice(
                        &reg_snapshot,
                        grant,
                        WorkflowRole::Architect,
                        req.architect_difficulty.or(req.difficulty),
                    )?;
                }

                let job = wf_ledger
                    .jobs
                    .iter()
                    .find(|j| j.job_id == req.job_id)
                    .ok_or_else(|| {
                        NativeError::new(
                            NativeErrorCode::ValidationFailed,
                            format!("requested job_id `{}` was not found in binding", req.job_id),
                        )
                    })?;
                job.validate()?;
                if job.binding_id != request.binding_id || job.thread_uuid != request.thread_uuid {
                    return Err(NativeError::new(
                        NativeErrorCode::SecurityViolation,
                        "CoworkerJobRecord scope does not match OrchestrateWorkBriefPlanRequest",
                    ));
                }
                if let Some(plan_id) = &req.plan_id {
                    if wf_ledger.jobs.iter().any(|j| {
                        j.job_id != req.job_id
                            && j.planning_result
                                .as_ref()
                                .is_some_and(|pr| &pr.plan_id == plan_id)
                    }) {
                        return Err(NativeError::new(
                            NativeErrorCode::ValidationFailed,
                            "plan_id is owned by another job; omit plan_id for a new or amendment job",
                        ));
                    }
                }
                if wf_ledger
                    .jobs
                    .iter()
                    .any(|j| j.supersedes_job_id.as_deref() == Some(job.job_id.as_str()))
                {
                    return Err(NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "job has been superseded by an amendment job and cannot run Orchestrator planning",
                    ));
                }
                if job.holds.iter().any(|h| h.active) {
                    return Err(NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "job has an active hold and cannot run Orchestrator planning until resolved",
                    ));
                }
                if job.approval_linkage.is_some() {
                    return Err(NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "job already has an immutable FrozenApprovalLinkage",
                    ));
                }
                if !job.work_brief.is_ready_for_execution_planning() {
                    return Err(NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "WorkBrief is not ready for execution planning",
                    ));
                }

                let call_base = job.work_brief.project_base.clone();
                verify_call_base_against_grant(&grant.project_base, &call_base)?;

                let mut attached_advice = Vec::with_capacity(req.architect_advice_ids.len());
                for advice_id in &req.architect_advice_ids {
                    let adv = wf_ledger
                        .architect_advice
                        .iter()
                        .find(|a| &a.advice_id == advice_id)
                        .ok_or_else(|| {
                            NativeError::new(
                                NativeErrorCode::ValidationFailed,
                                format!(
                                    "referenced architect_advice_id `{advice_id}` was not found in binding ledger"
                                ),
                            )
                        })?;
                    adv.validate()?;
                    if adv.context_dependencies.project_base != call_base
                        || adv.context_dependencies.work_brief_digest != job.work_brief_digest
                        || !constant_time_str_eq(
                            &adv.context_dependencies.policy_digest,
                            &grant.policy_digest,
                        )
                    {
                        return Err(NativeError::new(
                            NativeErrorCode::ValidationFailed,
                            format!(
                                "referenced ArchitectAdviceRecord `{advice_id}` is stale relative to job or grant"
                            ),
                        ));
                    }
                    verify_dependency_blobs_at_base(
                        &call_base,
                        &adv.context_dependencies.dependency_file_blobs,
                    )?;
                    attached_advice.push((
                        adv.advice_id.as_str(),
                        adv.context_digest.as_str(),
                        adv.decision_digest.as_str(),
                    ));
                }

                let seed = (
                    "threadlane-native:coworker-orchestrator-context:v1",
                    &request.binding_id,
                    &request.thread_uuid,
                    &grant.grant_id,
                    &grant.profile_id,
                    grant.registry_revision,
                    &grant.policy_digest,
                    &job.job_id,
                    &job.work_brief_digest,
                    &job.work_brief.project_base,
                    req.plan_id.as_deref(),
                    req.expected_plan_revision,
                    &selected_choice,
                    &attached_advice,
                );
                let ctx_bytes = serde_json::to_vec(&seed).map_err(|_| {
                    NativeError::new(
                        NativeErrorCode::ProtocolError,
                        "failed to serialize Orchestrator planning context for digest",
                    )
                })?;
                Ok(ValidatedEnqueueContext {
                    selected_choice,
                    call_project_base: call_base,
                    context_digest: sha256_hex(&ctx_bytes),
                })
            }
        }
    }

    fn verify_completed_result_against_stores(
        &self,
        cmd: &PlanningCommandRecord,
        mut result_ref: PlanningCommandResultRef,
    ) -> Result<PlanningCommandResultRef, NativeError> {
        let wf_ledger = self.workflow_store.read_binding_ledger(&cmd.binding_id)?;
        let grant = wf_ledger
            .planning_grants
            .iter()
            .find(|g| g.grant_id == cmd.grant_id)
            .ok_or_else(|| {
                NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "standing grant referenced by command was not found in binding ledger",
                )
            })?;
        grant.validate()?;

        match (&cmd.payload, &mut result_ref) {
            (
                PlanningCommandPayload::Architect(req),
                PlanningCommandResultRef::ArchitectAdvice {
                    advice_id,
                    reservation_id,
                    report_artifact_id,
                    context_digest,
                    decision_digest,
                    reused_existing,
                    artifact_verified,
                },
            ) => {
                let stored_adv = wf_ledger
                    .architect_advice
                    .iter()
                    .find(|a| &a.advice_id == advice_id)
                    .ok_or_else(|| {
                        NativeError::new(
                            NativeErrorCode::ValidationFailed,
                            "ArchitectAdviceRecord referenced by Completed result was not found in binding ledger",
                        )
                    })?;
                stored_adv.validate()?;
                if stored_adv.binding_id != cmd.binding_id
                    || stored_adv.thread_uuid != cmd.thread_uuid
                    || stored_adv.reservation_id != *reservation_id
                    || stored_adv.report_artifact_id.as_deref() != Some(report_artifact_id.as_str())
                    || stored_adv.selected_choice != cmd.selected_choice
                    || stored_adv.context_dependencies != req.context_dependencies
                    || !constant_time_str_eq(&stored_adv.context_digest, &cmd.context_digest)
                    || !constant_time_str_eq(&stored_adv.context_digest, context_digest)
                    || !constant_time_str_eq(&stored_adv.decision_digest, decision_digest)
                {
                    return Err(NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "Completed ArchitectAdvice result_ref does not match stored ArchitectAdviceRecord or command scope",
                    ));
                }
                let adv_grant = wf_ledger
                    .planning_grants
                    .iter()
                    .find(|g| g.grant_id == stored_adv.grant_id)
                    .ok_or_else(|| {
                        NativeError::new(
                            NativeErrorCode::ValidationFailed,
                            "grant for stored ArchitectAdviceRecord was not found",
                        )
                    })?;
                adv_grant.validate()?;
                if adv_grant.profile_id != grant.profile_id
                    || adv_grant.registry_revision != grant.registry_revision
                    || !constant_time_str_eq(&adv_grant.policy_digest, &grant.policy_digest)
                {
                    return Err(NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "ArchitectAdviceRecord grant provenance does not match command standing grant",
                    ));
                }
                let res = adv_grant
                    .reservations
                    .iter()
                    .find(|r| r.reservation_id == *reservation_id)
                    .ok_or_else(|| {
                        NativeError::new(
                            NativeErrorCode::ValidationFailed,
                            "PlanningCallReservation referenced by ArchitectAdvice result was not found",
                        )
                    })?;
                if res.role != WorkflowRole::Architect
                    || res.status != PlanningReservationStatus::Completed
                    || res.selected_choice != cmd.selected_choice
                    || res.call_project_base.as_ref() != Some(&cmd.call_project_base)
                    || !constant_time_str_eq(&res.context_digest, &cmd.context_digest)
                    || res.report_artifact_id.as_deref() != Some(report_artifact_id.as_str())
                    || res.request_key != stored_adv.request_key
                {
                    return Err(NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "ArchitectAdvice reservation is not Completed or does not match command scope",
                    ));
                }
                let produced_by_this_cmd = stored_adv.grant_id == cmd.grant_id
                    && stored_adv.request_key == cmd.service_request_key
                    && res.request_key == cmd.service_request_key;
                if produced_by_this_cmd {
                    if *reused_existing {
                        return Err(NativeError::new(
                            NativeErrorCode::ValidationFailed,
                            "ArchitectAdviceRecord produced by this command's service_request_key cannot claim reused_existing = true",
                        ));
                    }
                    if req.accept_into_brief
                        && stored_adv.status != ArchitectDecisionStatus::AcceptedIntoBrief
                    {
                        return Err(NativeError::new(
                            NativeErrorCode::ValidationFailed,
                            "ArchitectAdviceRecord status does not match accept_into_brief = true",
                        ));
                    }
                } else {
                    if !*reused_existing {
                        return Err(NativeError::new(
                            NativeErrorCode::ValidationFailed,
                            "non-reused ArchitectAdviceRecord must bind command grant_id and frozen service_request_key",
                        ));
                    }
                    if !stored_adv.can_reuse_for(&req.context_dependencies)? {
                        return Err(NativeError::new(
                            NativeErrorCode::ValidationFailed,
                            "reused ArchitectAdviceRecord is not eligible for reuse under command context_dependencies",
                        ));
                    }
                }
                let artifact = self.read_and_verify_artifact(report_artifact_id, adv_grant)?;
                if artifact.binding_id != cmd.binding_id
                    || artifact.thread_uuid != cmd.thread_uuid
                    || artifact.grant_id != adv_grant.grant_id
                    || artifact.reservation_id != res.reservation_id
                    || res.run_id.as_deref() != Some(artifact.run_id.as_str())
                    || artifact.request_key != res.request_key
                    || artifact.role != WorkflowRole::Architect
                    || artifact.selected_choice != cmd.selected_choice
                    || artifact.call_project_base != cmd.call_project_base
                    || !constant_time_str_eq(&artifact.context_digest, &cmd.context_digest)
                    || artifact.output_digest.is_none()
                    || artifact.output_digest != res.output_digest
                    || artifact.output_byte_count.unwrap_or(usize::MAX)
                        > grant.budget.max_output_bytes_per_call
                {
                    return Err(NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "persisted Architect artifact metadata does not match command or reservation",
                    ));
                }
                let expected_q_digest = compute_architect_question_digest(
                    &req.question_summary,
                    &req.prior_advice_context,
                )?;
                match &artifact.outcome {
                    PlanningArtifactOutcomeKind::ArchitectAdvice {
                        question_summary,
                        question_digest,
                        context_dependencies,
                        prior_advice_context,
                        structured_advice,
                        decision_digest: art_decision_digest,
                    } => {
                        if question_summary != &req.question_summary
                            || !constant_time_str_eq(question_digest, &expected_q_digest)
                            || context_dependencies != &req.context_dependencies
                            || prior_advice_context != &req.prior_advice_context
                            || structured_advice.decision_summary != stored_adv.decision_summary
                            || !constant_time_str_eq(
                                art_decision_digest,
                                &stored_adv.decision_digest,
                            )
                        {
                            return Err(NativeError::new(
                                NativeErrorCode::ValidationFailed,
                                "persisted Architect artifact question_summary, context_dependencies, or decision_digest does not match command payload",
                            ));
                        }
                    }
                    _ => {
                        return Err(NativeError::new(
                            NativeErrorCode::ValidationFailed,
                            "persisted artifact for Architect command does not have ArchitectAdvice outcome",
                        ));
                    }
                }
                *artifact_verified = true;
            }
            (
                PlanningCommandPayload::Orchestrator(req),
                PlanningCommandResultRef::OrchestratorPlan {
                    job_id,
                    planning_result,
                    artifact_verified,
                },
            ) => {
                if job_id != &req.job_id {
                    return Err(NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "OrchestratorPlan result job_id does not match command job_id",
                    ));
                }
                let job = wf_ledger
                    .jobs
                    .iter()
                    .find(|j| j.job_id == *job_id)
                    .ok_or_else(|| {
                        NativeError::new(
                            NativeErrorCode::ValidationFailed,
                            "CoworkerJobRecord referenced by OrchestratorPlan was not found",
                        )
                    })?;
                job.validate()?;
                let recovered_plan = req
                    .plan_id
                    .as_ref()
                    .is_some_and(|id| id != &planning_result.plan_id);
                if recovered_plan {
                    verify_recovery_command_context(cmd, &wf_ledger, grant, job)?;
                }
                let job_pr = job.planning_result.as_ref().ok_or_else(|| {
                    NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "CoworkerJobRecord has no recorded PlanningResultRef",
                    )
                })?;
                if job_pr != planning_result.as_ref()
                    || planning_result.grant_id != cmd.grant_id
                    || !is_valid_orchestrator_round_key(
                        &planning_result.request_key,
                        &cmd.service_request_key,
                        req.max_architect_rounds,
                    )
                    || planning_result.selected_choice != cmd.selected_choice
                    || planning_result.project_base != cmd.call_project_base
                    || planning_result.profile_id != grant.profile_id
                    || planning_result.registry_revision != grant.registry_revision
                    || !constant_time_str_eq(&planning_result.policy_digest, &grant.policy_digest)
                    || !constant_time_str_eq(
                        &planning_result.work_brief_digest,
                        &job.work_brief_digest,
                    )
                    || (req
                        .plan_id
                        .as_ref()
                        .is_some_and(|expected_pid| &planning_result.plan_id != expected_pid)
                        && !PlanningRecoveryTarget::derive(
                            cmd,
                            &wf_ledger,
                            &planning_result.report_artifact_id,
                            &planning_result.output_digest,
                        )
                        .is_some_and(|target| {
                            target.plan_id == planning_result.plan_id
                                && planning_result.plan_revision == 1
                                && planning_result.request_key == cmd.service_request_key
                        }))
                {
                    return Err(NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "OrchestratorPlan PlanningResultRef does not match job.planning_result or command service_request_key/scope",
                    ));
                }
                let res = grant
                    .reservations
                    .iter()
                    .find(|r| r.reservation_id == planning_result.reservation_id)
                    .ok_or_else(|| {
                        NativeError::new(
                            NativeErrorCode::ValidationFailed,
                            "PlanningCallReservation referenced by PlanningResultRef was not found on grant",
                        )
                    })?;
                if res.role != WorkflowRole::Orchestrator
                    || res.status != PlanningReservationStatus::Completed
                    || res.request_key != planning_result.request_key
                    || res.selected_choice != cmd.selected_choice
                    || res.call_project_base.as_ref() != Some(&cmd.call_project_base)
                    || res.run_id.as_deref() != Some(planning_result.run_id.as_str())
                    || res.report_artifact_id.as_deref()
                        != Some(planning_result.report_artifact_id.as_str())
                    || res.output_digest.as_deref() != Some(planning_result.output_digest.as_str())
                    || (res.request_key == cmd.service_request_key
                        && !constant_time_str_eq(&res.context_digest, &cmd.context_digest))
                {
                    return Err(NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "OrchestratorPlan reservation does not match PlanningResultRef or command scope",
                    ));
                }
                let artifact =
                    self.read_and_verify_artifact(&planning_result.report_artifact_id, grant)?;
                if artifact.binding_id != cmd.binding_id
                    || artifact.thread_uuid != cmd.thread_uuid
                    || artifact.grant_id != cmd.grant_id
                    || artifact.reservation_id != planning_result.reservation_id
                    || artifact.run_id != planning_result.run_id
                    || artifact.request_key != planning_result.request_key
                    || artifact.role != WorkflowRole::Orchestrator
                    || artifact.selected_choice != cmd.selected_choice
                    || artifact.call_project_base != cmd.call_project_base
                    || !constant_time_str_eq(&artifact.context_digest, &res.context_digest)
                    || artifact.output_digest.as_deref()
                        != Some(planning_result.output_digest.as_str())
                {
                    return Err(NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "persisted Orchestrator artifact metadata does not match PlanningResultRef or reservation",
                    ));
                }
                match &artifact.outcome {
                    PlanningArtifactOutcomeKind::OrchestratorOutput {
                        job_id: art_job_id,
                        job_revision: art_job_revision,
                        work_brief_digest: art_brief_digest,
                        work_brief: art_brief,
                        architect_advice_ids: art_advice_ids,
                        structured_output:
                            StructuredOrchestratorOutput::Plan {
                                resolved_open_questions,
                                deferred_proposals,
                                ..
                            },
                        ..
                    } => {
                        if (recovered_plan
                            && (art_brief.is_none()
                                || art_job_revision.is_none()
                                || res.difficulty != req.difficulty))
                            || art_job_id != job_id
                            || !constant_time_str_eq(
                                art_brief_digest,
                                &planning_result.work_brief_digest,
                            )
                            || art_brief.as_ref().is_some_and(|b| b != &job.work_brief)
                            || art_advice_ids != &planning_result.architect_advice_ids
                            || (planning_result.request_key == cmd.service_request_key
                                && art_advice_ids != &req.architect_advice_ids)
                            || resolved_open_questions != &planning_result.resolved_open_questions
                            || deferred_proposals != &planning_result.deferred_proposals
                        {
                            return Err(NativeError::new(
                                NativeErrorCode::ValidationFailed,
                                "persisted Orchestrator Plan artifact outcome does not match PlanningResultRef or job brief",
                            ));
                        }
                    }
                    _ => {
                        return Err(NativeError::new(
                            NativeErrorCode::ValidationFailed,
                            "persisted artifact for OrchestratorPlan does not contain a Plan outcome",
                        ));
                    }
                }
                *artifact_verified = true;
            }
            (
                PlanningCommandPayload::Orchestrator(req),
                PlanningCommandResultRef::OrchestratorRequiresHumanClarification {
                    job_id,
                    reservation_id,
                    report_artifact_id,
                    output_digest,
                    clarification,
                    artifact_verified,
                },
            ) => {
                let (job, res, artifact) = self.verify_orchestrator_non_plan_common(
                    &wf_ledger,
                    grant,
                    cmd,
                    req,
                    job_id,
                    reservation_id,
                    report_artifact_id,
                    output_digest,
                )?;
                match &artifact.outcome {
                    PlanningArtifactOutcomeKind::OrchestratorOutput {
                        job_id: art_job_id,
                        work_brief_digest: art_brief_digest,
                        work_brief: art_brief,
                        architect_advice_ids: art_advice_ids,
                        structured_output:
                            StructuredOrchestratorOutput::RequiresHumanClarification {
                                summary,
                                questions,
                                referenced_files,
                                ..
                            },
                        ..
                    } => {
                        if art_job_id != job_id
                            || !constant_time_str_eq(art_brief_digest, &job.work_brief_digest)
                            || art_brief.as_ref().is_some_and(|b| b != &job.work_brief)
                            || (res.request_key == cmd.service_request_key
                                && art_advice_ids != &req.architect_advice_ids)
                            || summary != &clarification.summary
                            || questions != &clarification.questions
                            || referenced_files != &clarification.referenced_files
                        {
                            return Err(NativeError::new(
                                NativeErrorCode::ValidationFailed,
                                "persisted Orchestrator clarification artifact does not match result_ref",
                            ));
                        }
                    }
                    _ => {
                        return Err(NativeError::new(
                            NativeErrorCode::ValidationFailed,
                            "persisted artifact does not contain a RequiresHumanClarification outcome",
                        ));
                    }
                }
                *artifact_verified = true;
            }
            (
                PlanningCommandPayload::Orchestrator(req),
                PlanningCommandResultRef::OrchestratorRequiresArchitectAdvice {
                    job_id,
                    reservation_id,
                    report_artifact_id,
                    output_digest,
                    consultation,
                    artifact_verified,
                },
            ) => {
                let (job, res, artifact) = self.verify_orchestrator_non_plan_common(
                    &wf_ledger,
                    grant,
                    cmd,
                    req,
                    job_id,
                    reservation_id,
                    report_artifact_id,
                    output_digest,
                )?;
                match &artifact.outcome {
                    PlanningArtifactOutcomeKind::OrchestratorOutput {
                        job_id: art_job_id,
                        work_brief_digest: art_brief_digest,
                        work_brief: art_brief,
                        architect_advice_ids: art_advice_ids,
                        structured_output:
                            StructuredOrchestratorOutput::RequiresArchitectAdvice {
                                summary,
                                questions,
                                ..
                            },
                        ..
                    } => {
                        if art_job_id != job_id
                            || !constant_time_str_eq(art_brief_digest, &job.work_brief_digest)
                            || art_brief.as_ref().is_some_and(|b| b != &job.work_brief)
                            || (res.request_key == cmd.service_request_key
                                && art_advice_ids != &req.architect_advice_ids)
                            || summary != &consultation.summary
                            || questions != &consultation.questions
                        {
                            return Err(NativeError::new(
                                NativeErrorCode::ValidationFailed,
                                "persisted Orchestrator consultation artifact does not match result_ref",
                            ));
                        }
                    }
                    _ => {
                        return Err(NativeError::new(
                            NativeErrorCode::ValidationFailed,
                            "persisted artifact does not contain a RequiresArchitectAdvice outcome",
                        ));
                    }
                }
                *artifact_verified = true;
            }
            _ => {
                return Err(NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "PlanningCommandResultRef variant does not match PlanningCommandPayload role",
                ));
            }
        }
        Ok(result_ref)
    }

    #[allow(clippy::too_many_arguments)]
    fn verify_orchestrator_non_plan_common<'a>(
        &self,
        wf_ledger: &'a CoworkerBindingLedger,
        grant: &'a PlanningAdviceGrant,
        cmd: &PlanningCommandRecord,
        req: &OrchestrateWorkBriefPlanRequest,
        job_id: &str,
        reservation_id: &str,
        report_artifact_id: &str,
        output_digest: &str,
    ) -> Result<
        (
            &'a crate::coworker_workflow::CoworkerJobRecord,
            &'a crate::coworker_workflow::PlanningCallReservation,
            PersistedPlanningCallArtifact,
        ),
        NativeError,
    > {
        if job_id != req.job_id {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "Orchestrator result job_id does not match command job_id",
            ));
        }
        let job = wf_ledger
            .jobs
            .iter()
            .find(|j| j.job_id == job_id)
            .ok_or_else(|| {
                NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "CoworkerJobRecord referenced by Orchestrator result was not found",
                )
            })?;
        job.validate()?;
        let res = grant
            .reservations
            .iter()
            .find(|r| r.reservation_id == reservation_id)
            .ok_or_else(|| {
                NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "PlanningCallReservation referenced by Orchestrator result was not found on grant",
                )
            })?;
        if res.role != WorkflowRole::Orchestrator
            || res.status != PlanningReservationStatus::Completed
            || !is_valid_orchestrator_round_key(
                &res.request_key,
                &cmd.service_request_key,
                req.max_architect_rounds,
            )
            || res.selected_choice != cmd.selected_choice
            || res.call_project_base.as_ref() != Some(&cmd.call_project_base)
            || res.report_artifact_id.as_deref() != Some(report_artifact_id)
            || res.output_digest.as_deref() != Some(output_digest)
            || (res.request_key == cmd.service_request_key
                && !constant_time_str_eq(&res.context_digest, &cmd.context_digest))
        {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "Orchestrator reservation metadata does not match Completed result_ref or command service_request_key",
            ));
        }
        let artifact = self.read_and_verify_artifact(report_artifact_id, grant)?;
        if artifact.binding_id != cmd.binding_id
            || artifact.thread_uuid != cmd.thread_uuid
            || artifact.grant_id != cmd.grant_id
            || artifact.reservation_id != reservation_id
            || res.run_id.as_deref() != Some(artifact.run_id.as_str())
            || artifact.request_key != res.request_key
            || artifact.role != WorkflowRole::Orchestrator
            || artifact.selected_choice != cmd.selected_choice
            || artifact.call_project_base != cmd.call_project_base
            || !constant_time_str_eq(&artifact.context_digest, &res.context_digest)
            || artifact.output_digest.as_deref() != Some(output_digest)
        {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "persisted Orchestrator artifact metadata does not match Completed result_ref or reservation",
            ));
        }
        Ok((job, res, artifact))
    }

    fn read_and_verify_artifact(
        &self,
        artifact_id: &str,
        grant: &PlanningAdviceGrant,
    ) -> Result<PersistedPlanningCallArtifact, NativeError> {
        let Some(ref dir) = self.artifacts_dir else {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "planning_artifacts_dir must be configured on PlanningCommandStore to verify and record a Completed planning command",
            ));
        };
        let path = dir.join(format!("{artifact_id}.json"));
        let bytes = read_private_file_bounded(
            &path,
            MAX_PLANNING_ARTIFACT_BYTES,
            "planning_call_artifact",
        )?
        .ok_or_else(|| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!("persisted planning artifact `{artifact_id}` does not exist"),
            )
        })?;
        let artifact: PersistedPlanningCallArtifact =
            serde_json::from_slice(&bytes).map_err(|_| {
                NativeError::new(
                    NativeErrorCode::ProtocolError,
                    "persisted planning artifact JSON is corrupt or invalid",
                )
            })?;
        artifact.validate_against_grant(artifact_id, grant)?;
        if matches!(
            artifact.outcome,
            PlanningArtifactOutcomeKind::FailedAfterEffect { .. }
        ) {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "FailedAfterEffect artifact cannot back a Completed planning command result",
            ));
        }
        Ok(artifact)
    }
}

fn compute_architect_question_digest(
    question_summary: &str,
    prior_advice: &[PriorArchitectAdviceContext],
) -> Result<String, NativeError> {
    if prior_advice.is_empty() {
        return Ok(sha256_hex(question_summary.trim().as_bytes()));
    }
    let seed = (
        "threadlane-native:coworker-architect-question:v1",
        question_summary.trim(),
        prior_advice,
    );
    let bytes = serde_json::to_vec(&seed).map_err(|_| {
        NativeError::new(
            NativeErrorCode::ProtocolError,
            "failed to serialize Architect question and prior_advice_context for digest calculation",
        )
    })?;
    Ok(sha256_hex(&bytes))
}

fn is_valid_orchestrator_round_key(
    candidate_key: &str,
    root_service_key: &str,
    max_architect_rounds: u32,
) -> bool {
    if candidate_key == root_service_key {
        return true;
    }
    let bounded_rounds = max_architect_rounds.min(MAX_ORCHESTRATOR_ARCHITECT_ROUNDS);
    for round in 1..=bounded_rounds {
        let expected = format!("{root_service_key}_orch_r{round}");
        if candidate_key == expected {
            return true;
        }
    }
    false
}

fn ensure_ledger_thread(
    ledger: &mut PlanningCommandBindingLedger,
    thread_uuid: &str,
) -> Result<(), NativeError> {
    if let Some(ref pinned) = ledger.thread_uuid {
        if pinned != thread_uuid {
            return Err(NativeError::new(
                NativeErrorCode::SecurityViolation,
                "thread_uuid does not match the thread pinned to this binding's command ledger",
            ));
        }
    } else {
        ledger.thread_uuid = Some(thread_uuid.to_string());
    }
    Ok(())
}

fn find_active_grant<'a>(
    wf_ledger: &'a CoworkerBindingLedger,
    binding_id: &str,
    thread_uuid: &str,
    grant_id: &str,
    role: WorkflowRole,
    now_ms: u64,
) -> Result<&'a PlanningAdviceGrant, NativeError> {
    if wf_ledger.binding_id != binding_id {
        return Err(NativeError::new(
            NativeErrorCode::SecurityViolation,
            "workflow binding ledger binding_id mismatch",
        ));
    }
    if wf_ledger.thread_uuid.as_deref() != Some(thread_uuid) {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "binding has no active grant for the requested thread_uuid",
        ));
    }
    let grant = wf_ledger
        .planning_grants
        .iter()
        .find(|g| g.grant_id == grant_id)
        .ok_or_else(|| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!("requested grant_id `{grant_id}` was not found in binding `{binding_id}`"),
            )
        })?;
    grant.validate()?;
    if grant.binding_id != binding_id || grant.thread_uuid != thread_uuid {
        return Err(NativeError::new(
            NativeErrorCode::SecurityViolation,
            "PlanningAdviceGrant scope does not match binding_id and thread_uuid",
        ));
    }
    if grant.status == PlanningGrantStatus::Revoked
        || grant.revoked_at_ms.is_some()
        || grant.revoked_reason.is_some()
    {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "planning grant is revoked and cannot authorize planning commands",
        ));
    }
    if grant.status == PlanningGrantStatus::Expired || now_ms >= grant.expires_at_ms {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "planning grant has expired and cannot authorize planning commands",
        ));
    }
    if now_ms < grant.created_at_ms {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "local wall clock precedes planning grant created_at_ms",
        ));
    }
    if grant.status != PlanningGrantStatus::Active
        || grant.usage.reserved_total_calls >= grant.budget.max_total_calls
    {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "planning grant call budget is exhausted",
        ));
    }
    match role {
        WorkflowRole::Architect => {
            if grant.usage.reserved_architect_calls >= grant.budget.max_architect_calls {
                return Err(NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "planning grant architect call budget is exhausted",
                ));
            }
        }
        WorkflowRole::Orchestrator => {
            if grant.usage.reserved_orchestrator_calls >= grant.budget.max_orchestrator_calls {
                return Err(NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "planning grant orchestrator call budget is exhausted",
                ));
            }
        }
        _ => {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "unsupported role for planning grant",
            ));
        }
    }
    Ok(grant)
}

fn resolve_headless_choice(
    reg_snapshot: &threadlane_workflow::RegistrySnapshot,
    grant: &PlanningAdviceGrant,
    role: WorkflowRole,
    difficulty: Option<Difficulty>,
) -> Result<SelectedRoleChoice, NativeError> {
    let report = preflight(
        reg_snapshot,
        &PreflightRequest {
            profile_id: Some(grant.profile_id.clone()),
            roles: vec![RolePreflightRequest::new(role, difficulty)],
        },
        &grant.policy,
    )?;
    let outcome = report.roles.first().ok_or_else(|| {
        NativeError::new(
            NativeErrorCode::ValidationFailed,
            "preflight returned no role outcome",
        )
    })?;
    if !report.ready || !outcome.ready {
        let code = match outcome.block_reason {
            Some(PreflightBlockReason::UnsupportedRoute) => NativeErrorCode::UnsupportedCapability,
            _ => NativeErrorCode::ValidationFailed,
        };
        return Err(NativeError::new(
            code,
            format!(
                "planning role `{role:?}` blocked by preflight: {:?}",
                outcome.block_reason
            ),
        ));
    }
    let selected = outcome.selected.clone().ok_or_else(|| {
        NativeError::new(
            NativeErrorCode::ValidationFailed,
            "preflight outcome marked ready without SelectedRoleChoice",
        )
    })?;
    if selected.route != RoleExecutionRoute::HeadlessWorker
        || !matches!(
            selected.adapter,
            ToolAdapter::Antigravity | ToolAdapter::OpencodeV2
        )
    {
        return Err(NativeError::new(
            NativeErrorCode::UnsupportedCapability,
            "planning role resolved to unsupported non-headless worker route",
        ));
    }
    validate_non_native_worker_choice(role, &selected)?;
    let tool_def = reg_snapshot
        .tools
        .iter()
        .find(|t| t.id == selected.tool_id)
        .ok_or_else(|| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                "resolved tool_id was not found in registry snapshot",
            )
        })?;
    if !tool_def.enabled || tool_def.executable_path.is_none() {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "resolved planning tool is disabled or missing executable_path",
        ));
    }
    Ok(selected)
}

fn verify_prior_advice_list(
    wf_ledger: &CoworkerBindingLedger,
    current_grant: &PlanningAdviceGrant,
    binding_id: &str,
    thread_uuid: &str,
    supplied_prior: &[PriorArchitectAdviceContext],
) -> Result<(), NativeError> {
    let mut seen_ids = BTreeSet::new();
    for prior in supplied_prior {
        prior.validate()?;
        if !seen_ids.insert(prior.advice_id.as_str()) {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "duplicate advice_id in prior_advice_context",
            ));
        }
        let stored: &ArchitectAdviceRecord = wf_ledger
            .architect_advice
            .iter()
            .find(|a| a.advice_id == prior.advice_id)
            .ok_or_else(|| {
                NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    format!(
                        "referenced prior_advice.advice_id `{}` was not found in binding ledger",
                        prior.advice_id
                    ),
                )
            })?;
        stored.validate()?;
        if stored.binding_id != binding_id
            || stored.thread_uuid != thread_uuid
            || stored.status == ArchitectDecisionStatus::Rejected
            || stored.context_dependencies.project_base.repository_path
                != current_grant.project_base.repository_path
            || !constant_time_str_eq(&stored.context_digest, &prior.context_digest)
            || !constant_time_str_eq(&stored.decision_digest, &prior.decision_digest)
            || stored.decision_summary != prior.decision_summary
        {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "supplied prior_advice_context does not match stored verified ArchitectAdviceRecord",
            ));
        }
    }
    Ok(())
}

fn verify_call_base_against_grant(
    grant_base: &CanonicalProjectBaseRef,
    call_base: &CanonicalProjectBaseRef,
) -> Result<(), NativeError> {
    if call_base.repository_path != grant_base.repository_path {
        return Err(NativeError::new(
            NativeErrorCode::SecurityViolation,
            "call project_base.repository_path does not match standing grant canonical repository_path",
        ));
    }
    call_base.verify_live_repository()?;
    let repo_dir = Path::new(&call_base.repository_path);
    let head_commit = resolve_exact_commit_sha(repo_dir, "HEAD", DEFAULT_GIT_COMMAND_TIMEOUT_MS)
        .map_err(|_| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                "failed to resolve live repository HEAD commit",
            )
        })?;
    let head_tree = resolve_exact_tree_sha(repo_dir, "HEAD", DEFAULT_GIT_COMMAND_TIMEOUT_MS)
        .map_err(|_| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                "failed to resolve live repository HEAD tree SHA",
            )
        })?;
    let clean =
        is_source_worktree_clean(repo_dir, DEFAULT_GIT_COMMAND_TIMEOUT_MS).map_err(|_| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                "failed to inspect live repository worktree cleanliness",
            )
        })?;
    if head_commit != call_base.base_commit || head_tree != call_base.base_tree_sha || !clean {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "live repository HEAD or cleanliness drifted from call project_base",
        ));
    }
    let is_descendant = is_commit_ancestor(
        repo_dir,
        &grant_base.base_commit,
        &call_base.base_commit,
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .map_err(|_| {
        NativeError::new(
            NativeErrorCode::ValidationFailed,
            "failed to verify commit ancestry against standing grant base_commit",
        )
    })?;
    if !is_descendant {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "call base_commit is not equal to or descended from standing grant origin base_commit",
        ));
    }
    Ok(())
}

fn verify_dependency_blobs_at_base(
    project_base: &CanonicalProjectBaseRef,
    expected_blobs: &std::collections::BTreeMap<String, String>,
) -> Result<(), NativeError> {
    if expected_blobs.is_empty() {
        return Ok(());
    }
    let paths: Vec<String> = expected_blobs.keys().cloned().collect();
    let actual_blobs =
        CoworkerPlanningService::compute_dependency_file_blobs(project_base, &paths)?;
    for (path, expected_digest) in expected_blobs {
        let Some(actual_digest) = actual_blobs.get(path) else {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!("missing actual blob digest for dependency file `{path}`"),
            ));
        };
        if !constant_time_str_eq(actual_digest, expected_digest) {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!("dependency_file_blobs digest mismatch for `{path}` at base_commit"),
            ));
        }
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
        std::env::current_dir()
            .map_err(|_| {
                NativeError::new(
                    NativeErrorCode::IoError,
                    format!("failed to resolve current directory for {label}"),
                )
            })?
            .join(dir)
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

fn binding_command_tag(binding_id: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"threadlane-native:planning-command-ledger:v1\0");
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
    format!("planning-commands-{}.json", binding_command_tag(binding_id))
}

fn lock_filename_for_binding(binding_id: &str) -> String {
    format!("planning-commands-{}.lock", binding_command_tag(binding_id))
}

/// Returns the current local wall-clock timestamp in milliseconds since the Unix epoch.
pub fn current_wall_clock_ms() -> Result<u64, NativeError> {
    let dur = SystemTime::now().duration_since(UNIX_EPOCH).map_err(|_| {
        NativeError::new(
            NativeErrorCode::InvalidConfig,
            "system clock is set before the Unix epoch",
        )
    })?;
    u64::try_from(dur.as_millis()).map_err(|_| {
        NativeError::new(
            NativeErrorCode::ValidationFailed,
            "wall-clock millisecond timestamp overflow",
        )
    })
}
