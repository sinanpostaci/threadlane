//! Data contracts, validation rules, deterministic digests, state transition graphs, and
//! independence proofs for the Threadlane Coworker workflow.
//!
//! ## Core Invariants
//!
//! - **Native Coworker is not a registry execution model**: Native Codex is the human's
//!   conversational interface ([`COWORKER_IS_CONVERSATIONAL_INTERFACE_ONLY`]). Headless planning,
//!   architectural advice, coding, review, and closure workers must resolve to supported headless
//!   CLI routes ([`RoleExecutionRoute::HeadlessWorker`]), never [`ToolAdapter::CodexCli`] or
//!   [`WorkflowRole::Follower`]. Local records always set
//!   `native_per_call_identity_verified: false`.
//! - **Structured [`WorkBrief`]**: Separates confirmed requirements, proposed additions,
//!   assumptions, open questions, and accepted architect decisions, and explicitly distinguishes
//!   [`WorkBriefKind::NewJob`] from [`WorkBriefKind::Amendment`].
//! - **Separate read-only [`PlanningAdviceGrant`]**: Planning and architectural advice grants are
//!   distinct from code-execution [`ApprovalRecord`]s, require trusted-local operator attestation,
//!   bind exact [`ExecutionPolicy`] rules restricted to [`WorkflowRole::Orchestrator`] and
//!   [`WorkflowRole::Architect`], enforce read-only capabilities, and reserve finite call budgets
//!   durably before effect.
//! - **Dependency-bound [`ArchitectAdviceRecord`] reuse**: Architectural advice may be reused only
//!   when [`ArchitectContextDependencies`] (repository, base commit/tree, brief/plan/policy
//!   digests, dependency file blobs, interface ownership digests, and accepted predecessor trees)
//!   are unchanged, never by approximate text similarity.
//! - **Structured [`StructuredReviewVerdict`]**: Verdicts are typed as
//!   [`ReviewVerdictOutcome::Accept`], [`ReviewVerdictOutcome::CorrectInScope`],
//!   [`ReviewVerdictOutcome::ScopeConflict`], [`ReviewVerdictOutcome::Blocked`], or
//!   [`ReviewVerdictOutcome::Unclear`]. Positive reviewer prose never advances acceptance.
//! - **Reserve-before-effect [`OutboxDispatchRecord`] and explicit holds**: External dispatches
//!   reserve before effect and never auto-replay [`OutboxDispatchStatus::EffectUnknown`].
//!   Conversational or brief changes hold affected tasks; unaffected tasks advance only when
//!   proven independent via [`prove_task_independence`].

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{self, Write as _};
use std::path::Path;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use threadlane_authority::validation::{
    validate_canonical_thread_uuid, validate_repository_and_base_commit,
    validate_write_scope_entry_at_base_commit,
};
use threadlane_authority::ApprovalRecord;
use threadlane_workers::{
    resolve_exact_tree_sha, validate_exact_40_hex_sha, validate_repo_relative_path,
    validate_sha256_hex_digest, DEFAULT_GIT_COMMAND_TIMEOUT_MS, MAX_STDOUT_BYTES, MAX_TIMEOUT_MS,
};
use threadlane_workflow::{Difficulty, PlanStatus, ToolAdapter, WorkflowRole};

use crate::error::{NativeError, NativeErrorCode};
use crate::fs_safety::{
    constant_time_str_eq, validate_absolute_no_symlink_components, validate_bounded_identifier,
    MAX_PRIVATE_STATE_FILE_BYTES,
};
use crate::profile_flow::{ExecutionPolicy, RoleExecutionRoute, SelectedRoleChoice};

/// Current wire and on-disk schema version for per-binding Coworker workflow ledgers.
pub const COWORKER_WORKFLOW_SCHEMA_VERSION: u32 = 1;

/// Invariant marker: native Coworker (Codex) is the conversational interface only, never a
/// headless registry execution model.
pub const COWORKER_IS_CONVERSATIONAL_INTERFACE_ONLY: bool = true;

/// Maximum byte size permitted for a per-binding Coworker workflow ledger file (`1 MiB`).
pub const MAX_COWORKER_LEDGER_BYTES: usize = MAX_PRIVATE_STATE_FILE_BYTES;

/// Maximum character/byte length for bounded identifiers in Coworker workflow records.
pub const MAX_COWORKER_ID_LEN: usize = 128;

/// Maximum byte length for bounded text fields (summaries, reasons, findings, diagnostics).
pub const MAX_COWORKER_TEXT_BYTES: usize = 4_096;

/// Maximum number of entries in any [`WorkBrief`] requirement/question/decision list.
pub const MAX_WORK_BRIEF_ITEMS: usize = 64;

/// Maximum number of [`PlanningAdviceGrant`] records retained per binding ledger.
pub const MAX_PLANNING_GRANTS_PER_BINDING: usize = 64;

/// Maximum number of [`PlanningCallReservation`] records retained per [`PlanningAdviceGrant`].
pub const MAX_GRANT_RESERVATIONS: usize = 128;

/// Maximum total call budget permitted in a single [`PlanningCallBudget`].
pub const MAX_GRANT_CALL_BUDGET: u32 = 32;

/// Maximum number of [`ArchitectAdviceRecord`] entries retained per binding ledger.
pub const MAX_ARCHITECT_ADVICE_RECORDS_PER_BINDING: usize = 128;

/// Maximum number of entries in [`ArchitectContextDependencies`] maps.
pub const MAX_ARCHITECT_CONTEXT_ENTRIES: usize = 128;

/// Maximum number of [`CoworkerJobRecord`] entries retained per binding ledger.
pub const MAX_JOBS_PER_BINDING: usize = 64;

/// Maximum number of [`CoworkerTaskRecord`] entries permitted in a single [`CoworkerJobRecord`].
pub const MAX_TASKS_PER_JOB: usize = 64;

/// Maximum number of [`TaskAttemptRef`] entries permitted per [`CoworkerTaskRecord`].
pub const MAX_ATTEMPTS_PER_TASK: usize = 16;

/// Maximum number of [`TaskCandidateRef`] entries permitted per [`CoworkerTaskRecord`].
pub const MAX_CANDIDATES_PER_TASK: usize = 16;

/// Maximum number of [`WorkflowHoldRecord`] entries permitted per [`CoworkerJobRecord`].
pub const MAX_HOLDS_PER_JOB: usize = 64;

/// Maximum number of [`OutboxDispatchRecord`] entries permitted per [`CoworkerJobRecord`].
pub const MAX_OUTBOX_RECORDS_PER_JOB: usize = 256;

/// Maximum number of [`CombinedCandidateIdentity`] entries permitted per [`CoworkerJobRecord`].
pub const MAX_COMBINED_CANDIDATES_PER_JOB: usize = 32;

/// Maximum number of [`CandidateCheckEvidence`] entries permitted in a [`CombinedCandidateIdentity`].
pub const MAX_CHECK_EVIDENCE_ITEMS: usize = 64;

/// Canonical Git repository path, exact 40-hex base commit SHA, and resolved 40-hex base tree SHA.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CanonicalProjectBaseRef {
    /// Canonical absolute non-symlink repository path.
    pub repository_path: String,
    /// Exact 40-character lowercase hexadecimal Git commit SHA.
    pub base_commit: String,
    /// Exact 40-character lowercase hexadecimal Git tree SHA resolved from `base_commit`.
    pub base_tree_sha: String,
}

impl CanonicalProjectBaseRef {
    /// Resolves and validates `repository_path` and `base_commit` against the live Git repository
    /// via sanitized read-only Git, returning the canonical [`CanonicalProjectBaseRef`].
    pub fn resolve_and_validate(
        repository_path: &Path,
        base_commit: &str,
    ) -> Result<Self, NativeError> {
        let (canonical_repo, normalized_base) =
            validate_repository_and_base_commit(repository_path, base_commit).map_err(|err| {
                NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    format!(
                        "project repository or base_commit validation failed ({})",
                        err.code()
                    ),
                )
            })?;
        let base_tree_sha = resolve_exact_tree_sha(
            &canonical_repo,
            &normalized_base,
            DEFAULT_GIT_COMMAND_TIMEOUT_MS,
        )
        .map_err(|_| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                "failed to resolve exact Git tree SHA for base_commit",
            )
        })?;
        Ok(Self {
            repository_path: canonical_repo.to_string_lossy().into_owned(),
            base_commit: normalized_base,
            base_tree_sha,
        })
    }

    /// Validates the structural syntax of `repository_path`, `base_commit`, and `base_tree_sha`.
    pub fn validate_syntax(&self) -> Result<(), NativeError> {
        let path = Path::new(&self.repository_path);
        if path.as_os_str().is_empty() || !path.is_absolute() {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "repository_path must be a non-empty absolute path",
            ));
        }
        let _ = validate_absolute_no_symlink_components(path, "repository_path")?;
        validate_exact_40_hex_sha(&self.base_commit, "base_commit").map_err(|_| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                "base_commit must be a 40-character lowercase hex SHA-1",
            )
        })?;
        validate_exact_40_hex_sha(&self.base_tree_sha, "base_tree_sha").map_err(|_| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                "base_tree_sha must be a 40-character lowercase hex SHA-1",
            )
        })?;
        Ok(())
    }

    /// Re-verifies that `repository_path`, `base_commit`, and `base_tree_sha` match the live Git repository.
    pub fn verify_live_repository(&self) -> Result<(), NativeError> {
        self.validate_syntax()?;
        let live = Self::resolve_and_validate(Path::new(&self.repository_path), &self.base_commit)?;
        if live != *self {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "project_base drifted from canonical live Git repository or base tree SHA",
            ));
        }
        Ok(())
    }
}

/// Identifies whether a [`WorkBrief`] starts a new job or amends an existing job revision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
pub enum WorkBriefKind {
    /// Starts an independent new Coworker workflow job.
    NewJob,
    /// Amends an existing Coworker workflow job at an exact expected revision.
    Amendment {
        /// Target job identifier being amended.
        target_job_id: String,
        /// Expected current `job_revision` of `target_job_id` (`>= 1`).
        target_job_revision: u64,
        /// Bounded human-readable reason for the amendment.
        amendment_reason: String,
    },
}

/// Structured Coworker work brief separating confirmed requirements, proposed additions,
/// assumptions, open questions, and accepted architectural decisions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkBrief {
    /// Stable brief identifier (`brf_<hex>` or caller-assigned bounded identifier).
    pub brief_id: String,
    /// Control-plane chat binding identifier.
    pub binding_id: String,
    /// Canonical lowercase hyphenated non-nil native Codex thread UUID.
    pub thread_uuid: String,
    /// Canonical repository and base commit/tree reference.
    pub project_base: CanonicalProjectBaseRef,
    /// Whether this brief initiates a new job or amends an existing job.
    pub kind: WorkBriefKind,
    /// Bounded summary of the work brief.
    pub summary: String,
    /// Explicitly human-confirmed requirements (`>= 1` required).
    pub confirmed_requirements: Vec<String>,
    /// Model-proposed scope or feature additions not yet confirmed by the human.
    #[serde(default)]
    pub proposed_additions: Vec<String>,
    /// Explicit assumptions recorded for the brief.
    #[serde(default)]
    pub assumptions: Vec<String>,
    /// Unresolved questions requiring human clarification before execution approval.
    #[serde(default)]
    pub open_questions: Vec<String>,
    /// Accepted architect advice identifiers or decision digests incorporated into the brief.
    #[serde(default)]
    pub accepted_architect_decisions: Vec<String>,
}

impl WorkBrief {
    /// Validates all identifiers, canonical project/base fields, list bounds, and amendment linkage.
    pub fn validate(&self) -> Result<(), NativeError> {
        validate_bounded_identifier(&self.brief_id, "brief_id", MAX_COWORKER_ID_LEN)?;
        validate_bounded_identifier(&self.binding_id, "binding_id", MAX_COWORKER_ID_LEN)?;
        validate_canonical_thread_uuid("thread_uuid", &self.thread_uuid).map_err(|_| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                "thread_uuid must be a canonical lowercase hyphenated non-nil UUID",
            )
        })?;
        self.project_base.validate_syntax()?;
        validate_bounded_text(&self.summary, "work_brief.summary")?;

        match &self.kind {
            WorkBriefKind::NewJob => {}
            WorkBriefKind::Amendment {
                target_job_id,
                target_job_revision,
                amendment_reason,
            } => {
                validate_bounded_identifier(
                    target_job_id,
                    "amendment.target_job_id",
                    MAX_COWORKER_ID_LEN,
                )?;
                if *target_job_revision == 0 {
                    return Err(NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "amendment.target_job_revision must be >= 1",
                    ));
                }
                validate_bounded_text(amendment_reason, "amendment.amendment_reason")?;
            }
        }

        if self.confirmed_requirements.is_empty() {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "work_brief.confirmed_requirements must contain at least one confirmed requirement",
            ));
        }
        validate_unique_text_list(
            &self.confirmed_requirements,
            "work_brief.confirmed_requirements",
            MAX_WORK_BRIEF_ITEMS,
        )?;
        validate_unique_text_list(
            &self.proposed_additions,
            "work_brief.proposed_additions",
            MAX_WORK_BRIEF_ITEMS,
        )?;
        validate_unique_text_list(
            &self.assumptions,
            "work_brief.assumptions",
            MAX_WORK_BRIEF_ITEMS,
        )?;
        validate_unique_text_list(
            &self.open_questions,
            "work_brief.open_questions",
            MAX_WORK_BRIEF_ITEMS,
        )?;
        validate_unique_id_list(
            &self.accepted_architect_decisions,
            "work_brief.accepted_architect_decisions",
            MAX_WORK_BRIEF_ITEMS,
        )?;
        if self.proposed_additions.iter().any(|proposal| {
            self.confirmed_requirements
                .iter()
                .any(|req| req == proposal)
        }) {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "work_brief.proposed_additions must remain distinct from confirmed_requirements; unapproved proposals cannot silently become confirmed scope",
            ));
        }
        Ok(())
    }

    /// Computes a deterministic 64-character lowercase hex SHA-256 digest of this [`WorkBrief`].
    pub fn digest(&self) -> Result<String, NativeError> {
        self.validate()?;
        let seed = (
            "threadlane-native:coworker_work_brief:v1",
            &self.brief_id,
            &self.binding_id,
            &self.thread_uuid,
            &self.project_base,
            &self.kind,
            &self.summary,
            &self.confirmed_requirements,
            &self.proposed_additions,
            &self.assumptions,
            &self.open_questions,
            &self.accepted_architect_decisions,
        );
        let bytes = serde_json::to_vec(&seed).map_err(|_| {
            NativeError::new(
                NativeErrorCode::ProtocolError,
                "failed to serialize WorkBrief for digest calculation",
            )
        })?;
        Ok(sha256_hex(&bytes))
    }

    /// Returns `true` when the brief is structurally valid and has at least one human-confirmed
    /// requirement, allowing it to reach [`CoworkerJobPhase::Planning`].
    ///
    /// Technical `open_questions` and unapproved `proposed_additions` may accompany a valid brief
    /// into [`CoworkerJobPhase::Planning`] as non-authoritative planner context so the Coworker is
    /// not forced to solve technical planning before the Orchestrator. However, `proposed_additions`
    /// remain explicitly unapproved (never executable scope) and both `open_questions` and
    /// `proposed_additions` block execution approval via [`Self::is_ready_for_execution_approval`].
    pub fn is_ready_for_execution_planning(&self) -> bool {
        self.validate().is_ok() && !self.confirmed_requirements.is_empty()
    }

    /// Returns `true` only when the brief is valid, has confirmed requirements, and has no
    /// unresolved `open_questions` or unconfirmed `proposed_additions`.
    ///
    /// Execution approval (`bind_job_approval`) strictly requires either this gate or explicit
    /// resolution/deferment in the job's recorded [`PlanningResultRef`] via
    /// [`Self::is_ready_for_execution_approval_with_planning`] so unapproved proposals or
    /// unresolved questions can never authorize file writes or worker execution.
    pub fn is_ready_for_execution_approval(&self) -> bool {
        self.is_ready_for_execution_planning()
            && self.open_questions.is_empty()
            && self.proposed_additions.is_empty()
    }

    /// Returns `true` when the brief is structurally valid, has confirmed requirements, and every
    /// technical `open_questions` entry and unapproved `proposed_additions` entry is either absent
    /// from the brief or explicitly resolved/deferred in `planning_result`.
    ///
    /// Unapproved `proposed_additions` are never silently promoted into `confirmed_requirements`
    /// or executable scope; any `proposed_additions` present on the brief must be explicitly
    /// listed in `planning_result.deferred_proposals` (confirming they remain excluded from the
    /// planned scope), and any `open_questions` present on the brief must be explicitly listed in
    /// `planning_result.resolved_open_questions`. If any question remains unresolved or any
    /// proposal remains unaddressed, execution approval is blocked.
    pub fn is_ready_for_execution_approval_with_planning(
        &self,
        planning_result: Option<&PlanningResultRef>,
    ) -> bool {
        if !self.is_ready_for_execution_planning() {
            return false;
        }
        if self.open_questions.is_empty() && self.proposed_additions.is_empty() {
            return true;
        }
        let Some(planning) = planning_result else {
            return false;
        };
        self.open_questions
            .iter()
            .all(|q| planning.resolved_open_questions.iter().any(|rq| rq == q))
            && self
                .proposed_additions
                .iter()
                .all(|p| planning.deferred_proposals.iter().any(|dp| dp == p))
    }
}

/// Enforces that a role and resolved choice represent a supported headless worker model, never
/// native Coworker (`CodexCli` / `Follower`).
pub fn validate_non_native_worker_choice(
    role: WorkflowRole,
    choice: &SelectedRoleChoice,
) -> Result<(), NativeError> {
    if role == WorkflowRole::Follower {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "WorkflowRole::Follower is the conversational native interface, not a headless worker role",
        ));
    }
    if choice.adapter == ToolAdapter::CodexCli || choice.route != RoleExecutionRoute::HeadlessWorker
    {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "native Coworker (codex_cli / native_handoff) is not a headless registry execution model",
        ));
    }
    validate_bounded_identifier(
        &choice.tool_id,
        "selected_choice.tool_id",
        MAX_COWORKER_ID_LEN,
    )?;
    validate_bounded_identifier(
        &choice.model_id,
        "selected_choice.model_id",
        MAX_COWORKER_ID_LEN,
    )?;
    validate_bounded_text(&choice.model_arg, "selected_choice.model_arg")?;
    if let Some(ref effort) = choice.effort {
        validate_bounded_identifier(effort, "selected_choice.effort", MAX_COWORKER_ID_LEN)?;
    }
    Ok(())
}

/// Provider-visible read-only capability and tool-policy envelope enforced on every
/// [`PlanningAdviceGrant`].
///
/// This contract describes the provider-visible capabilities and tool policy enforced by the
/// trusted host middleware, not an OS kernel sandbox. The trusted host may prepare an owned
/// immutable snapshot or read-only inspection view; the advisory model may not write files,
/// mutate Git state, dispatch code execution, or invoke arbitrary mutating tools. Post-run
/// repository cleanliness verification is additional post-hoc evidence, not primary prevention.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlanningReadOnlyCapabilities {
    /// Must be `true`: planning and architect calls expose read-only repository inspection only.
    pub read_only_filesystem: bool,
    /// Must be `false`: planning grants never permit task worktree creation or file writes.
    pub write_worktree_allowed: bool,
    /// Must be `false`: planning grants never permit Git commits, branches, merges, or pushes.
    pub git_mutation_allowed: bool,
    /// Must be `false`: planning grants never permit external network mutations.
    pub network_mutation_allowed: bool,
    /// Must be `false`: planning grants are strictly separate from code execution approvals.
    pub code_execution_approval_granted: bool,
}

impl PlanningReadOnlyCapabilities {
    /// Returns the canonical strictly read-only provider capability set for a [`PlanningAdviceGrant`].
    pub const fn strict_read_only() -> Self {
        Self {
            read_only_filesystem: true,
            write_worktree_allowed: false,
            git_mutation_allowed: false,
            network_mutation_allowed: false,
            code_execution_approval_granted: false,
        }
    }

    /// Validates that no write, Git mutation, network mutation, or execution approval capability is enabled.
    pub fn validate(&self) -> Result<(), NativeError> {
        if !self.read_only_filesystem
            || self.write_worktree_allowed
            || self.git_mutation_allowed
            || self.network_mutation_allowed
            || self.code_execution_approval_granted
        {
            return Err(NativeError::new(
                NativeErrorCode::SecurityViolation,
                "PlanningAdviceGrant capabilities must be strictly read-only and separate from code execution approval",
            ));
        }
        Ok(())
    }
}

/// Finite call and per-call execution budget for a [`PlanningAdviceGrant`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlanningCallBudget {
    /// Maximum total calls (`orchestrator + architect`) permitted under this grant (`1..=32`).
    pub max_total_calls: u32,
    /// Maximum `WorkflowRole::Orchestrator` calls permitted under this grant.
    pub max_orchestrator_calls: u32,
    /// Maximum `WorkflowRole::Architect` calls permitted under this grant.
    pub max_architect_calls: u32,
    /// Maximum wall-clock timeout in milliseconds for any single call.
    pub max_timeout_ms_per_call: u64,
    /// Maximum stdout capture bytes for any single call.
    pub max_output_bytes_per_call: usize,
}

impl PlanningCallBudget {
    /// Validates finite positive bounds on the call budget.
    pub fn validate(&self) -> Result<(), NativeError> {
        if self.max_total_calls == 0 || self.max_total_calls > MAX_GRANT_CALL_BUDGET {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!("max_total_calls must be between 1 and {MAX_GRANT_CALL_BUDGET}"),
            ));
        }
        if self.max_orchestrator_calls > self.max_total_calls
            || self.max_architect_calls > self.max_total_calls
            || (self.max_orchestrator_calls == 0 && self.max_architect_calls == 0)
        {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "role call budgets must be within max_total_calls and permit at least one planning/advice call",
            ));
        }
        if self.max_timeout_ms_per_call == 0 || self.max_timeout_ms_per_call > MAX_TIMEOUT_MS {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "max_timeout_ms_per_call must be within supported worker timeout bounds",
            ));
        }
        if self.max_output_bytes_per_call == 0 || self.max_output_bytes_per_call > MAX_STDOUT_BYTES
        {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "max_output_bytes_per_call must be within supported worker stdout bounds",
            ));
        }
        Ok(())
    }
}

/// Durable call usage counters for a [`PlanningAdviceGrant`].
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlanningGrantUsage {
    /// Total calls currently reserved or consumed (`ReservedBeforeEffect + Completed + FailedAfterEffect + EffectUnknown`).
    pub reserved_total_calls: u32,
    /// Total calls proven completed.
    pub completed_calls: u32,
    /// Total calls proven to have terminated in a known provider, timeout, or output validation failure after effect (`FailedAfterEffect`; budget remains consumed).
    #[serde(default)]
    pub failed_calls: u32,
    /// Total calls whose effect is uncertain (`EffectUnknown`; budget remains consumed).
    pub unknown_effect_calls: u32,
    /// Total calls proven refused before any provider effect.
    pub refused_before_effect_calls: u32,
    /// Total `WorkflowRole::Orchestrator` calls reserved or consumed.
    pub reserved_orchestrator_calls: u32,
    /// Total `WorkflowRole::Architect` calls reserved or consumed.
    pub reserved_architect_calls: u32,
}

/// Lifecycle status of a [`PlanningAdviceGrant`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlanningGrantStatus {
    /// Grant is active and has remaining call budget.
    Active,
    /// Grant has reached its `max_total_calls` reservation limit.
    Exhausted,
    /// Grant has passed its `expires_at_ms` deadline.
    Expired,
    /// Grant was explicitly revoked by the operator.
    Revoked,
}

/// Lifecycle status of a single [`PlanningCallReservation`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlanningReservationStatus {
    /// Budget was durably reserved before crossing the provider execution boundary.
    ReservedBeforeEffect,
    /// Call was proven refused before any provider process started; budget was released.
    RefusedBeforeEffect,
    /// Call completed and returned a verified read-only output digest.
    Completed,
    /// Call crossed the provider execution boundary and terminated in a known provider, timeout,
    /// output, or structured-schema validation failure; budget remains consumed as a known paid
    /// failed call (never `EffectUnknown` and never an invented valid plan).
    FailedAfterEffect,
    /// Call outcome is uncertain; budget stays consumed and automatic replay is forbidden.
    EffectUnknown,
}

impl PlanningReservationStatus {
    /// Returns `true` when this reservation is in an uncertain or in-flight state that blocks replay.
    pub const fn is_ambiguous(self) -> bool {
        matches!(self, Self::ReservedBeforeEffect | Self::EffectUnknown)
    }
}

/// Durable pre-effect budget reservation for a single orchestrator or architect call.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlanningCallReservation {
    /// Deterministic reservation identifier (`prs_<32hex>`).
    pub reservation_id: String,
    /// Idempotent caller request key.
    pub request_key: String,
    /// Planning/advisory workflow role (`Orchestrator` or `Architect`).
    pub role: WorkflowRole,
    /// Optional difficulty tier used to resolve `selected_choice`.
    pub difficulty: Option<Difficulty>,
    /// Exact resolved headless worker choice permitted by the grant's [`ExecutionPolicy`].
    pub selected_choice: SelectedRoleChoice,
    /// 64-character lowercase hex SHA-256 digest of the call input/context dependencies.
    pub context_digest: String,
    /// Optional canonical project repository and exact immutable base commit/tree SHA inspected
    /// by this specific call.
    ///
    /// When present, `repository_path` must equal the standing grant's canonical repository
    /// (`grant.project_base.repository_path`), while `base_commit` and `base_tree_sha` record
    /// the exact immutable commit/tree snapshot verified for this call (which may advance across
    /// normal commits in the same canonical repository without requiring a new human grant).
    #[serde(default)]
    pub call_project_base: Option<CanonicalProjectBaseRef>,
    /// Current reservation status.
    pub status: PlanningReservationStatus,
    /// Timestamp (ms since Unix epoch) when the budget was reserved.
    pub reserved_at_ms: u64,
    /// Timestamp (ms since Unix epoch) when the reservation was last updated.
    pub updated_at_ms: u64,
    /// Optional runtime `run_id` if a provider execution receipt was observed.
    pub run_id: Option<String>,
    /// Optional local report artifact identifier.
    pub report_artifact_id: Option<String>,
    /// Optional 64-character lowercase hex SHA-256 digest of the structured output.
    pub output_digest: Option<String>,
    /// Optional sanitized diagnostic message when refused or uncertain.
    pub diagnostic: Option<String>,
}

/// Scoped, expiring, revocable read-only planning and architectural advice grant.
///
/// Separate from code execution [`ApprovalRecord`]. Requires trusted-local operator attestation
/// and reserves call budget durably before effect.
///
/// Standing grant scope is bound to the canonical repository (`project_base.repository_path`)
/// plus frozen explicit models, execution policy, budget, and expiry (`profile_id`,
/// `registry_revision`, `policy`, `policy_digest`, `capabilities`, `budget`, `expires_at_ms`).
/// `project_base` records the origin base commit/tree metadata when the grant was issued and does
/// not force new human approval after every normal commit in the same canonical repository; each
/// individual planning or architect call verifies and records its own actual immutable base
/// commit/tree and context digest while rejecting any cross-repository context.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlanningAdviceGrant {
    /// Deterministic grant identifier (`pgr_<32hex>`).
    pub grant_id: String,
    /// Idempotent request key that created this grant.
    pub create_request_key: String,
    /// Control-plane chat binding identifier.
    pub binding_id: String,
    /// Canonical lowercase hyphenated non-nil native Codex thread UUID.
    pub thread_uuid: String,
    /// Canonical project repository scope and origin base commit/tree metadata.
    pub project_base: CanonicalProjectBaseRef,
    /// Pinned workflow profile identifier.
    pub profile_id: String,
    /// Pinned workflow registry revision.
    pub registry_revision: u64,
    /// Exact deny-by-default execution policy restricted to `Orchestrator` and `Architect`.
    pub policy: ExecutionPolicy,
    /// 64-character lowercase hex SHA-256 digest of `policy`.
    pub policy_digest: String,
    /// Strictly read-only capability envelope.
    pub capabilities: PlanningReadOnlyCapabilities,
    /// Finite call and per-call execution budgets.
    pub budget: PlanningCallBudget,
    /// Current durable call usage counters.
    pub usage: PlanningGrantUsage,
    /// Current grant status (`Active`, `Exhausted`, `Expired`, or `Revoked`).
    pub status: PlanningGrantStatus,
    /// Creation timestamp (ms since Unix epoch).
    pub created_at_ms: u64,
    /// Expiry timestamp (ms since Unix epoch; must be `> created_at_ms`).
    pub expires_at_ms: u64,
    /// Optional revocation timestamp (ms since Unix epoch).
    pub revoked_at_ms: Option<u64>,
    /// Optional bounded revocation reason.
    pub revoked_reason: Option<String>,
    /// Must be `true`: trusted-local human-confirmation attestation was supplied.
    pub operator_attested: bool,
    /// Must be `false`: local grant creation never claims verified per-call native identity.
    pub native_per_call_identity_verified: bool,
    /// Durable call reservations recorded under this grant.
    pub reservations: Vec<PlanningCallReservation>,
}

impl PlanningAdviceGrant {
    /// Validates the grant's scope, policy role restrictions, read-only capabilities, budget, and usage consistency.
    pub fn validate(&self) -> Result<(), NativeError> {
        validate_bounded_identifier(&self.grant_id, "grant_id", MAX_COWORKER_ID_LEN)?;
        validate_bounded_identifier(
            &self.create_request_key,
            "create_request_key",
            MAX_COWORKER_ID_LEN,
        )?;
        validate_bounded_identifier(&self.binding_id, "binding_id", MAX_COWORKER_ID_LEN)?;
        validate_canonical_thread_uuid("thread_uuid", &self.thread_uuid).map_err(|_| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                "thread_uuid must be a canonical lowercase hyphenated non-nil UUID",
            )
        })?;
        self.project_base.validate_syntax()?;
        validate_bounded_identifier(&self.profile_id, "profile_id", MAX_COWORKER_ID_LEN)?;
        let computed_policy_digest = self.policy.digest()?;
        if computed_policy_digest != self.policy_digest {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "PlanningAdviceGrant.policy_digest does not match policy.digest()",
            ));
        }
        if self.policy.allow_rules.is_empty() {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "PlanningAdviceGrant.policy must contain at least one explicit allow rule for Orchestrator or Architect",
            ));
        }
        for rule in &self.policy.allow_rules {
            if rule.roles.is_empty()
                || rule.roles.iter().any(|role| {
                    !matches!(role, WorkflowRole::Orchestrator | WorkflowRole::Architect)
                })
            {
                return Err(NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "PlanningAdviceGrant.policy rules must explicitly restrict roles to Orchestrator and/or Architect only",
                ));
            }
        }
        self.capabilities.validate()?;
        self.budget.validate()?;
        if !self.operator_attested {
            return Err(NativeError::new(
                NativeErrorCode::UnauthorizedOperator,
                "PlanningAdviceGrant requires explicit trusted-local operator_attested = true",
            ));
        }
        if self.native_per_call_identity_verified {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "PlanningAdviceGrant.native_per_call_identity_verified must be false",
            ));
        }
        if self.expires_at_ms <= self.created_at_ms {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "PlanningAdviceGrant.expires_at_ms must be strictly greater than created_at_ms",
            ));
        }
        if let Some(ref reason) = self.revoked_reason {
            validate_bounded_text(reason, "revoked_reason")?;
        }
        if self.reservations.len() > MAX_GRANT_RESERVATIONS {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "PlanningAdviceGrant.reservations exceeds maximum capacity",
            ));
        }

        let mut calc_reserved = 0u32;
        let mut calc_completed = 0u32;
        let mut calc_failed = 0u32;
        let mut calc_unknown = 0u32;
        let mut calc_refused = 0u32;
        let mut calc_orch = 0u32;
        let mut calc_arch = 0u32;
        let mut seen_ids = BTreeSet::new();
        let mut seen_keys = BTreeSet::new();

        for res in &self.reservations {
            validate_bounded_identifier(
                &res.reservation_id,
                "reservation_id",
                MAX_COWORKER_ID_LEN,
            )?;
            validate_bounded_identifier(&res.request_key, "request_key", MAX_COWORKER_ID_LEN)?;
            if !seen_ids.insert(res.reservation_id.as_str())
                || !seen_keys.insert(res.request_key.as_str())
            {
                return Err(NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "duplicate reservation_id or request_key in PlanningAdviceGrant",
                ));
            }
            if !matches!(
                res.role,
                WorkflowRole::Orchestrator | WorkflowRole::Architect
            ) {
                return Err(NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "PlanningCallReservation.role must be Orchestrator or Architect",
                ));
            }
            validate_non_native_worker_choice(res.role, &res.selected_choice)?;
            if !self.policy.allows(
                res.role,
                &res.selected_choice.tool_id,
                &res.selected_choice.model_id,
                res.selected_choice.effort.as_deref(),
            )? {
                return Err(NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "PlanningCallReservation.selected_choice is not permitted by the grant policy",
                ));
            }
            validate_sha256_hex_digest(&res.context_digest, "reservation.context_digest").map_err(
                |_| {
                    NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "reservation.context_digest must be a 64-character lowercase hex SHA-256",
                    )
                },
            )?;
            if let Some(ref call_base) = res.call_project_base {
                call_base.validate_syntax()?;
                if call_base.repository_path != self.project_base.repository_path {
                    return Err(NativeError::new(
                        NativeErrorCode::SecurityViolation,
                        "PlanningCallReservation.call_project_base.repository_path must match the grant's canonical repository_path",
                    ));
                }
            }
            if let Some(ref out_digest) = res.output_digest {
                validate_sha256_hex_digest(out_digest, "reservation.output_digest").map_err(
                    |_| {
                        NativeError::new(
                            NativeErrorCode::ValidationFailed,
                            "reservation.output_digest must be a 64-character lowercase hex SHA-256",
                        )
                    },
                )?;
            }

            match res.status {
                PlanningReservationStatus::RefusedBeforeEffect => {
                    calc_refused += 1;
                }
                PlanningReservationStatus::ReservedBeforeEffect => {
                    calc_reserved += 1;
                    if res.role == WorkflowRole::Orchestrator {
                        calc_orch += 1;
                    } else {
                        calc_arch += 1;
                    }
                }
                PlanningReservationStatus::Completed => {
                    calc_reserved += 1;
                    calc_completed += 1;
                    if res.role == WorkflowRole::Orchestrator {
                        calc_orch += 1;
                    } else {
                        calc_arch += 1;
                    }
                }
                PlanningReservationStatus::FailedAfterEffect => {
                    calc_reserved += 1;
                    calc_failed += 1;
                    if res.role == WorkflowRole::Orchestrator {
                        calc_orch += 1;
                    } else {
                        calc_arch += 1;
                    }
                }
                PlanningReservationStatus::EffectUnknown => {
                    calc_reserved += 1;
                    calc_unknown += 1;
                    if res.role == WorkflowRole::Orchestrator {
                        calc_orch += 1;
                    } else {
                        calc_arch += 1;
                    }
                }
            }
        }

        if self.usage.reserved_total_calls != calc_reserved
            || self.usage.completed_calls != calc_completed
            || self.usage.failed_calls != calc_failed
            || self.usage.unknown_effect_calls != calc_unknown
            || self.usage.refused_before_effect_calls != calc_refused
            || self.usage.reserved_orchestrator_calls != calc_orch
            || self.usage.reserved_architect_calls != calc_arch
        {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "PlanningAdviceGrant.usage counters do not match recorded reservations",
            ));
        }
        if calc_reserved > self.budget.max_total_calls
            || calc_orch > self.budget.max_orchestrator_calls
            || calc_arch > self.budget.max_architect_calls
        {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "PlanningAdviceGrant reservations exceed the grant's call budget",
            ));
        }
        Ok(())
    }
}

/// Real context dependencies bound to an [`ArchitectAdviceRecord`].
///
/// Reuse of architectural advice requires every field in [`ArchitectContextDependencies`] to
/// remain unchanged, never approximate text similarity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArchitectContextDependencies {
    /// Canonical repository and base commit/tree SHA.
    pub project_base: CanonicalProjectBaseRef,
    /// 64-character lowercase hex SHA-256 digest of the [`WorkBrief`].
    pub work_brief_digest: String,
    /// Optional 64-character lowercase hex SHA-256 digest of the target plan revision.
    pub plan_digest: Option<String>,
    /// 64-character lowercase hex SHA-256 digest of the governing [`ExecutionPolicy`].
    pub policy_digest: String,
    /// Map of repository-relative file path -> exact 64-character lowercase hex content SHA-256 digest.
    pub dependency_file_blobs: BTreeMap<String, String>,
    /// Map of logical interface identifier -> exact 64-character lowercase hex interface contract SHA-256 digest.
    pub interface_ownership_digests: BTreeMap<String, String>,
    /// Map of predecessor `task_id` -> exact 40-character lowercase hex accepted Git tree SHA.
    pub accepted_predecessor_trees: BTreeMap<String, String>,
}

impl ArchitectContextDependencies {
    /// Validates all digests, paths, and map bounds in [`ArchitectContextDependencies`].
    pub fn validate(&self) -> Result<(), NativeError> {
        self.project_base.validate_syntax()?;
        validate_sha256_hex_digest(&self.work_brief_digest, "work_brief_digest").map_err(|_| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                "work_brief_digest must be a 64-character lowercase hex SHA-256",
            )
        })?;
        if let Some(ref pd) = self.plan_digest {
            validate_sha256_hex_digest(pd, "plan_digest").map_err(|_| {
                NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "plan_digest must be a 64-character lowercase hex SHA-256",
                )
            })?;
        }
        validate_sha256_hex_digest(&self.policy_digest, "policy_digest").map_err(|_| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                "policy_digest must be a 64-character lowercase hex SHA-256",
            )
        })?;

        if self.dependency_file_blobs.len() > MAX_ARCHITECT_CONTEXT_ENTRIES
            || self.interface_ownership_digests.len() > MAX_ARCHITECT_CONTEXT_ENTRIES
            || self.accepted_predecessor_trees.len() > MAX_ARCHITECT_CONTEXT_ENTRIES
        {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "ArchitectContextDependencies map exceeds maximum entry bound",
            ));
        }
        if self.dependency_file_blobs.is_empty() && self.interface_ownership_digests.is_empty() {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "ArchitectContextDependencies must bind at least one dependency file blob or interface ownership digest",
            ));
        }

        for (path, digest) in &self.dependency_file_blobs {
            validate_repo_relative_path(path, "dependency_file_blobs path").map_err(|_| {
                NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "dependency_file_blobs key must be a valid repository-relative path",
                )
            })?;
            validate_sha256_hex_digest(digest, "dependency_file_blobs digest").map_err(|_| {
                NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "dependency_file_blobs value must be a 64-character lowercase hex SHA-256",
                )
            })?;
        }
        for (iface_id, digest) in &self.interface_ownership_digests {
            validate_bounded_identifier(
                iface_id,
                "interface_ownership_digests key",
                MAX_COWORKER_ID_LEN,
            )?;
            validate_sha256_hex_digest(digest, "interface_ownership_digests digest").map_err(
                |_| {
                    NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "interface_ownership_digests value must be a 64-character lowercase hex SHA-256",
                    )
                },
            )?;
        }
        for (task_id, tree_sha) in &self.accepted_predecessor_trees {
            validate_bounded_identifier(
                task_id,
                "accepted_predecessor_trees key",
                MAX_COWORKER_ID_LEN,
            )?;
            validate_exact_40_hex_sha(tree_sha, "accepted_predecessor_trees tree_sha").map_err(
                |_| {
                    NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "accepted_predecessor_trees value must be a 40-character lowercase hex SHA-1",
                    )
                },
            )?;
        }
        Ok(())
    }

    /// Computes a deterministic 64-character lowercase hex SHA-256 digest of the context dependencies.
    pub fn context_digest(&self) -> Result<String, NativeError> {
        self.validate()?;
        let seed = (
            "threadlane-native:coworker_architect_context:v1",
            &self.project_base,
            &self.work_brief_digest,
            &self.plan_digest,
            &self.policy_digest,
            &self.dependency_file_blobs,
            &self.interface_ownership_digests,
            &self.accepted_predecessor_trees,
        );
        let bytes = serde_json::to_vec(&seed).map_err(|_| {
            NativeError::new(
                NativeErrorCode::ProtocolError,
                "failed to serialize ArchitectContextDependencies for digest calculation",
            )
        })?;
        Ok(sha256_hex(&bytes))
    }
}

/// Lifecycle status of an [`ArchitectAdviceRecord`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ArchitectDecisionStatus {
    /// Advice was produced and verified against a completed grant reservation.
    Proposed,
    /// Advice was accepted into a [`WorkBrief`].
    AcceptedIntoBrief,
    /// Advice was superseded by newer context dependencies or an updated brief.
    Superseded,
    /// Advice was explicitly rejected.
    Rejected,
}

/// Durable architectural advice record bound to exact [`ArchitectContextDependencies`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArchitectAdviceRecord {
    /// Deterministic advice identifier (`adv_<32hex>`).
    pub advice_id: String,
    /// Idempotent caller request key.
    pub request_key: String,
    /// Owning [`PlanningAdviceGrant::grant_id`].
    pub grant_id: String,
    /// Completed [`PlanningCallReservation::reservation_id`] that produced this advice.
    pub reservation_id: String,
    /// Control-plane chat binding identifier.
    pub binding_id: String,
    /// Canonical lowercase hyphenated non-nil native Codex thread UUID.
    pub thread_uuid: String,
    /// Resolved headless architect choice used for the call.
    pub selected_choice: SelectedRoleChoice,
    /// Exact real context dependencies bound when the advice was produced.
    pub context_dependencies: ArchitectContextDependencies,
    /// 64-character lowercase hex SHA-256 digest of `context_dependencies`.
    pub context_digest: String,
    /// Bounded architectural decision summary.
    pub decision_summary: String,
    /// 64-character lowercase hex SHA-256 digest of the architectural decision payload.
    pub decision_digest: String,
    /// Optional report artifact identifier from the headless worker run.
    pub report_artifact_id: Option<String>,
    /// Current status of the architectural decision.
    pub status: ArchitectDecisionStatus,
    /// Creation timestamp (ms since Unix epoch).
    pub created_at_ms: u64,
}

impl ArchitectAdviceRecord {
    /// Validates the advice record and verifies `context_digest == context_dependencies.context_digest()`.
    pub fn validate(&self) -> Result<(), NativeError> {
        validate_bounded_identifier(&self.advice_id, "advice_id", MAX_COWORKER_ID_LEN)?;
        validate_bounded_identifier(&self.request_key, "request_key", MAX_COWORKER_ID_LEN)?;
        validate_bounded_identifier(&self.grant_id, "grant_id", MAX_COWORKER_ID_LEN)?;
        validate_bounded_identifier(&self.reservation_id, "reservation_id", MAX_COWORKER_ID_LEN)?;
        validate_bounded_identifier(&self.binding_id, "binding_id", MAX_COWORKER_ID_LEN)?;
        validate_canonical_thread_uuid("thread_uuid", &self.thread_uuid).map_err(|_| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                "thread_uuid must be a canonical lowercase hyphenated non-nil UUID",
            )
        })?;
        validate_non_native_worker_choice(WorkflowRole::Architect, &self.selected_choice)?;
        let expected_context_digest = self.context_dependencies.context_digest()?;
        if expected_context_digest != self.context_digest {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "ArchitectAdviceRecord.context_digest does not match context_dependencies.context_digest()",
            ));
        }
        validate_bounded_text(&self.decision_summary, "decision_summary")?;
        validate_sha256_hex_digest(&self.decision_digest, "decision_digest").map_err(|_| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                "decision_digest must be a 64-character lowercase hex SHA-256",
            )
        })?;
        Ok(())
    }

    /// Returns `true` if and only if this advice record is active (`Proposed` or `AcceptedIntoBrief`)
    /// and `current_deps` has the exact same real context dependencies and digest.
    ///
    /// Never uses approximate text similarity.
    pub fn can_reuse_for(
        &self,
        current_deps: &ArchitectContextDependencies,
    ) -> Result<bool, NativeError> {
        self.validate()?;
        let current_digest = current_deps.context_digest()?;
        if !matches!(
            self.status,
            ArchitectDecisionStatus::Proposed | ArchitectDecisionStatus::AcceptedIntoBrief
        ) {
            return Ok(false);
        }
        Ok(self.context_dependencies == *current_deps
            && constant_time_str_eq(&self.context_digest, &current_digest))
    }
}

/// Explicit frozen execution capability envelope required by the routine execution controller
/// for non-coder actions (structured review dispatch, package closure dispatch, in-scope
/// correction loops, verification check command execution, and local Git integration).
///
/// [`FrozenApprovalLinkage::policy_digest`] or a saved [`WorkflowProfile`](crate::profile_flow::WorkflowProfile)
/// by itself is not authority to dispatch `Reviewer` or `Closure` workers, run check commands,
/// or mutate local Git integration worktrees. A later controller must verify an explicit
/// [`FrozenControllerCapabilityEnvelope`] covering each exact capability before performing it.
fn is_false(v: &bool) -> bool {
    !*v
}

/// Existing coder-only approvals omit this envelope (`controller_envelope: None`) and are not
/// retroactively widened.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FrozenControllerCapabilityEnvelope {
    /// Explicit headless worker choices permitted for `WorkflowRole::Reviewer` dispatches.
    #[serde(default)]
    pub allowed_reviewer_choices: Vec<SelectedRoleChoice>,
    /// Explicit headless worker choices permitted for `WorkflowRole::Closure` dispatches.
    #[serde(default)]
    pub allowed_closure_choices: Vec<SelectedRoleChoice>,
    /// Maximum automatic `CorrectInScope` coding follow-up attempts per task (`0` disables automatic in-scope correction).
    #[serde(default)]
    pub max_in_scope_correction_attempts: u32,
    /// Explicit allowlist of verification `check_id`s that the controller may execute in the candidate worktree.
    #[serde(default)]
    pub allowed_check_ids: Vec<String>,
    /// Whether the controller is authorized to integrate accepted task candidates into the job's local integration worktree.
    #[serde(default)]
    pub local_git_integration_allowed: bool,
    /// Whether the controller is explicitly authorized to prepare and reserve bounded in-scope
    /// coding follow-ups after clean, known failed frozen task-review checks without a `Reviewer`
    /// verdict. Defaults to `false` so existing approvals are not broadened.
    #[serde(default, skip_serializing_if = "is_false")]
    pub allow_check_failure_corrections: bool,
}

impl FrozenControllerCapabilityEnvelope {
    /// Validates all worker choices, attempt bounds, check identifiers, and (when supplied)
    /// consistency with the governing [`ExecutionPolicy`].
    pub fn validate(&self, policy: Option<&ExecutionPolicy>) -> Result<(), NativeError> {
        if self.allowed_reviewer_choices.len() > MAX_WORK_BRIEF_ITEMS
            || self.allowed_closure_choices.len() > MAX_WORK_BRIEF_ITEMS
        {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "FrozenControllerCapabilityEnvelope role choice allowlists exceed maximum bound",
            ));
        }
        if self.max_in_scope_correction_attempts > MAX_ATTEMPTS_PER_TASK as u32 {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!("max_in_scope_correction_attempts must not exceed {MAX_ATTEMPTS_PER_TASK}"),
            ));
        }
        if self.allow_check_failure_corrections
            && (self.max_in_scope_correction_attempts == 0 || self.allowed_check_ids.is_empty())
        {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "allow_check_failure_corrections requires max_in_scope_correction_attempts >= 1 and non-empty allowed_check_ids",
            ));
        }
        validate_unique_id_list(
            &self.allowed_check_ids,
            "controller_envelope.allowed_check_ids",
            MAX_CHECK_EVIDENCE_ITEMS,
        )?;
        for choice in &self.allowed_reviewer_choices {
            validate_non_native_worker_choice(WorkflowRole::Reviewer, choice)?;
            if let Some(p) = policy {
                if !p.allows(
                    WorkflowRole::Reviewer,
                    &choice.tool_id,
                    &choice.model_id,
                    choice.effort.as_deref(),
                )? {
                    return Err(NativeError::new(
                        NativeErrorCode::SecurityViolation,
                        "FrozenControllerCapabilityEnvelope reviewer choice is not permitted by the frozen ExecutionPolicy",
                    ));
                }
            }
        }
        for choice in &self.allowed_closure_choices {
            validate_non_native_worker_choice(WorkflowRole::Closure, choice)?;
            if let Some(p) = policy {
                if !p.allows(
                    WorkflowRole::Closure,
                    &choice.tool_id,
                    &choice.model_id,
                    choice.effort.as_deref(),
                )? {
                    return Err(NativeError::new(
                        NativeErrorCode::SecurityViolation,
                        "FrozenControllerCapabilityEnvelope closure choice is not permitted by the frozen ExecutionPolicy",
                    ));
                }
            }
        }
        Ok(())
    }
}

/// Immutable linkage from a [`CoworkerJobRecord`] to a frozen operator [`ApprovalRecord`].
///
/// Note on authority: [`Self::policy_digest`] and [`Self::profile_id`] record the frozen policy
/// and profile metadata, but by themselves they are **not** execution authority for `Reviewer`,
/// `Closure`, automatic in-scope corrections, verification check commands, or local Git
/// integration. Those capabilities require an explicit [`FrozenControllerCapabilityEnvelope`] in
/// [`Self::controller_envelope`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FrozenApprovalLinkage {
    /// Frozen operator approval identifier (`apr_<32hex>`).
    pub approval_id: String,
    /// 64-character lowercase hex SHA-256 `subject_digest` from [`ApprovalRecord`].
    pub approval_subject_digest: String,
    /// Frozen published plan identifier.
    pub plan_id: String,
    /// Frozen published plan revision (`>= 1`).
    pub plan_revision: u64,
    /// 64-character lowercase hex SHA-256 digest of the compiled plan revision.
    pub plan_digest: String,
    /// Canonical repository path, base commit, and base tree SHA frozen by the approval.
    pub project_base: CanonicalProjectBaseRef,
    /// Frozen workflow profile identifier.
    pub profile_id: String,
    /// Frozen registry revision (`>= 1`).
    pub registry_revision: u64,
    /// Frozen execution policy identifier.
    pub policy_id: String,
    /// 64-character lowercase hex SHA-256 digest of the frozen [`ExecutionPolicy`].
    pub policy_digest: String,
    /// Approved task identifiers frozen in the [`ApprovalRecord`].
    pub approved_task_ids: Vec<String>,
    /// Operator-approved repository-relative write scopes frozen in the [`ApprovalRecord`].
    pub approved_write_paths: Vec<String>,
    /// Optional explicit frozen controller capability envelope covering Reviewer, Closure,
    /// in-scope corrections, verification check commands, and local Git integration.
    ///
    /// Defaults to `None` so existing coder approvals remain backwards-compatible without
    /// implying controller authority from `policy_digest` or a saved profile alone.
    #[serde(default)]
    pub controller_envelope: Option<FrozenControllerCapabilityEnvelope>,
}

impl FrozenApprovalLinkage {
    /// Constructs and validates an immutable [`FrozenApprovalLinkage`] from a verified
    /// [`ApprovalRecord`], resolved `base_tree_sha`, and [`ExecutionPolicy`].
    pub fn from_approval_and_policy(
        approval: &ApprovalRecord,
        base_tree_sha: String,
        policy: &ExecutionPolicy,
    ) -> Result<Self, NativeError> {
        let policy_digest = policy.digest()?;
        let linkage = Self {
            approval_id: approval.approval_id.clone(),
            approval_subject_digest: approval.subject_digest.clone(),
            plan_id: approval.plan_id.clone(),
            plan_revision: approval.plan_revision,
            plan_digest: approval.plan_digest.clone(),
            project_base: CanonicalProjectBaseRef {
                repository_path: approval.repository_path.to_string_lossy().into_owned(),
                base_commit: approval.base_commit.clone(),
                base_tree_sha,
            },
            profile_id: approval.profile_id.clone(),
            registry_revision: approval.registry_revision,
            policy_id: policy.policy_id.clone(),
            policy_digest,
            approved_task_ids: approval.tasks.iter().map(|t| t.task_id.clone()).collect(),
            approved_write_paths: approval.approved_write_paths.clone(),
            controller_envelope: None,
        };
        linkage.validate()?;
        Ok(linkage)
    }

    /// Attaches and validates an explicit [`FrozenControllerCapabilityEnvelope`] against `policy`.
    pub fn with_controller_envelope(
        mut self,
        envelope: FrozenControllerCapabilityEnvelope,
        policy: &ExecutionPolicy,
    ) -> Result<Self, NativeError> {
        let computed_digest = policy.digest()?;
        if !constant_time_str_eq(&self.policy_digest, &computed_digest) {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "FrozenApprovalLinkage.policy_digest does not match the supplied ExecutionPolicy",
            ));
        }
        envelope.validate(Some(policy))?;
        self.controller_envelope = Some(envelope);
        self.validate()?;
        Ok(self)
    }

    /// Returns `true` only if an explicit [`FrozenControllerCapabilityEnvelope`] authorizes
    /// dispatching `choice` for `WorkflowRole::Reviewer`.
    pub fn authorizes_reviewer_dispatch(&self, choice: &SelectedRoleChoice) -> bool {
        self.controller_envelope
            .as_ref()
            .is_some_and(|env| env.allowed_reviewer_choices.iter().any(|c| c == choice))
    }

    /// Returns `true` only if an explicit [`FrozenControllerCapabilityEnvelope`] authorizes
    /// dispatching `choice` for `WorkflowRole::Closure`.
    pub fn authorizes_closure_dispatch(&self, choice: &SelectedRoleChoice) -> bool {
        self.controller_envelope
            .as_ref()
            .is_some_and(|env| env.allowed_closure_choices.iter().any(|c| c == choice))
    }

    /// Returns `true` only if an explicit [`FrozenControllerCapabilityEnvelope`] authorizes an
    /// automatic `CorrectInScope` follow-up coding attempt with 1-based `next_attempt_no` (`>= 2`).
    pub fn authorizes_in_scope_correction(&self, next_attempt_no: u32) -> bool {
        if next_attempt_no < 2 {
            return false;
        }
        let correction_ordinal = next_attempt_no - 1;
        self.controller_envelope.as_ref().is_some_and(|env| {
            env.max_in_scope_correction_attempts > 0
                && correction_ordinal <= env.max_in_scope_correction_attempts
        })
    }

    /// Returns `true` only if an explicit [`FrozenControllerCapabilityEnvelope`] authorizes an
    /// automatic check-failure follow-up coding attempt with 1-based `next_attempt_no` (`>= 2`).
    pub fn authorizes_check_failure_correction(&self, next_attempt_no: u32) -> bool {
        if next_attempt_no < 2 {
            return false;
        }
        let correction_ordinal = next_attempt_no - 1;
        self.controller_envelope.as_ref().is_some_and(|env| {
            env.allow_check_failure_corrections
                && env.max_in_scope_correction_attempts > 0
                && correction_ordinal <= env.max_in_scope_correction_attempts
        })
    }

    /// Returns `true` only if an explicit [`FrozenControllerCapabilityEnvelope`] authorizes
    /// running the verification check identified by `check_id`.
    pub fn authorizes_check_command(&self, check_id: &str) -> bool {
        self.controller_envelope
            .as_ref()
            .is_some_and(|env| env.allowed_check_ids.iter().any(|id| id == check_id))
    }

    /// Returns `true` only if an explicit [`FrozenControllerCapabilityEnvelope`] authorizes
    /// local Git integration into the job's integration worktree.
    pub fn authorizes_local_git_integration(&self) -> bool {
        self.controller_envelope
            .as_ref()
            .is_some_and(|env| env.local_git_integration_allowed)
    }

    /// Validates all identifiers, digests, and non-empty task/write scopes on the linkage.
    pub fn validate(&self) -> Result<(), NativeError> {
        validate_bounded_identifier(&self.approval_id, "approval_id", MAX_COWORKER_ID_LEN)?;
        validate_sha256_hex_digest(&self.approval_subject_digest, "approval_subject_digest")
            .map_err(|_| {
                NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "approval_subject_digest must be a 64-character lowercase hex SHA-256",
                )
            })?;
        validate_bounded_identifier(&self.plan_id, "plan_id", MAX_COWORKER_ID_LEN)?;
        if self.plan_revision == 0 || self.registry_revision == 0 {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "plan_revision and registry_revision must be >= 1",
            ));
        }
        validate_sha256_hex_digest(&self.plan_digest, "plan_digest").map_err(|_| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                "plan_digest must be a 64-character lowercase hex SHA-256",
            )
        })?;
        self.project_base.validate_syntax()?;
        validate_bounded_identifier(&self.profile_id, "profile_id", MAX_COWORKER_ID_LEN)?;
        validate_bounded_identifier(&self.policy_id, "policy_id", MAX_COWORKER_ID_LEN)?;
        validate_sha256_hex_digest(&self.policy_digest, "policy_digest").map_err(|_| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                "policy_digest must be a 64-character lowercase hex SHA-256",
            )
        })?;
        validate_unique_id_list(
            &self.approved_task_ids,
            "approved_task_ids",
            MAX_TASKS_PER_JOB,
        )?;
        if self.approved_task_ids.is_empty() || self.approved_write_paths.is_empty() {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "FrozenApprovalLinkage requires non-empty approved_task_ids and approved_write_paths",
            ));
        }
        validate_unique_text_list(
            &self.approved_write_paths,
            "approved_write_paths",
            MAX_WORK_BRIEF_ITEMS,
        )?;
        if let Some(ref env) = self.controller_envelope {
            env.validate(None)?;
        }
        Ok(())
    }

    /// Verifies that this linkage matches the exact immutable fields of `approval`.
    pub fn verify_matches_approval(&self, approval: &ApprovalRecord) -> Result<(), NativeError> {
        self.validate()?;
        let task_ids: Vec<String> = approval.tasks.iter().map(|t| t.task_id.clone()).collect();
        if self.approval_id != approval.approval_id
            || !constant_time_str_eq(&self.approval_subject_digest, &approval.subject_digest)
            || self.plan_id != approval.plan_id
            || self.plan_revision != approval.plan_revision
            || !constant_time_str_eq(&self.plan_digest, &approval.plan_digest)
            || self.project_base.repository_path != approval.repository_path.to_string_lossy()
            || self.project_base.base_commit != approval.base_commit
            || self.profile_id != approval.profile_id
            || self.registry_revision != approval.registry_revision
            || self.approved_task_ids != task_ids
            || self.approved_write_paths != approval.approved_write_paths
        {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "FrozenApprovalLinkage does not match the persisted ApprovalRecord",
            ));
        }
        Ok(())
    }
}

/// Legal routine task lifecycle stage within a [`CoworkerJobRecord`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CoworkerTaskStage {
    /// Waiting for upstream `depends_on` tasks to be accepted, integrated, and sealed.
    PendingDependencies,
    /// Ready to reserve and dispatch a coding attempt.
    ReadyToDispatch,
    /// Outbox record reserved before invoking worker dispatch.
    DispatchReserved,
    /// Worker attempt is actively executing.
    Running,
    /// Worker attempt finished and passed the diff gate; waiting for structured review.
    ReadyForReview,
    /// Outbox record reserved before invoking reviewer dispatch.
    ReviewReserved,
    /// Candidate is currently under structured review.
    UnderReview,
    /// Reviewer returned `correct_in_scope` or task verification recorded a clean check failure; requires a follow-up in-scope coding attempt.
    CorrectInScopeRequired,
    /// Candidate was accepted by a structured `accept` verdict and review receipt.
    Accepted,
    /// Outbox record reserved before integrating the accepted candidate into the run integration worktree.
    IntegrationReserved,
    /// Candidate is integrated into the run integration worktree with a sealed integration receipt.
    Integrated,
    /// Task advancement is held by an active [`WorkflowHoldRecord`].
    Held,
    /// Task is blocked by a `blocked` or `unclear` review verdict or missing dependency proof.
    Blocked,
    /// Last external dispatch or mutation had an uncertain outcome; requires explicit reconciliation.
    EffectUnknown,
    /// Worker attempt failed execution, telemetry verification, or diff-gate checks.
    Failed,
    /// Task was explicitly cancelled.
    Cancelled,
}

impl CoworkerTaskStage {
    /// Returns the canonical `snake_case` string representation of this task stage.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PendingDependencies => "pending_dependencies",
            Self::ReadyToDispatch => "ready_to_dispatch",
            Self::DispatchReserved => "dispatch_reserved",
            Self::Running => "running",
            Self::ReadyForReview => "ready_for_review",
            Self::ReviewReserved => "review_reserved",
            Self::UnderReview => "under_review",
            Self::CorrectInScopeRequired => "correct_in_scope_required",
            Self::Accepted => "accepted",
            Self::IntegrationReserved => "integration_reserved",
            Self::Integrated => "integrated",
            Self::Held => "held",
            Self::Blocked => "blocked",
            Self::EffectUnknown => "effect_unknown",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }

    /// Returns `true` if transitioning directly from `self` to `next` is a legal state transition.
    pub const fn can_transition_to(self, next: Self) -> bool {
        match self {
            Self::PendingDependencies => matches!(
                next,
                Self::ReadyToDispatch | Self::Held | Self::Blocked | Self::Cancelled
            ),
            Self::ReadyToDispatch => matches!(
                next,
                Self::DispatchReserved | Self::Held | Self::Blocked | Self::Cancelled
            ),
            Self::DispatchReserved => matches!(
                next,
                Self::Running
                    | Self::ReadyToDispatch
                    | Self::EffectUnknown
                    | Self::Held
                    | Self::Blocked
                    | Self::Cancelled
            ),
            Self::Running => matches!(
                next,
                Self::ReadyForReview
                    | Self::Failed
                    | Self::EffectUnknown
                    | Self::Held
                    | Self::Blocked
                    | Self::Cancelled
            ),
            Self::ReadyForReview => matches!(
                next,
                Self::ReviewReserved
                    | Self::CorrectInScopeRequired
                    | Self::Held
                    | Self::Blocked
                    | Self::Cancelled
            ),
            Self::ReviewReserved => matches!(
                next,
                Self::UnderReview
                    | Self::ReadyForReview
                    | Self::EffectUnknown
                    | Self::Held
                    | Self::Blocked
                    | Self::Cancelled
            ),
            Self::UnderReview => matches!(
                next,
                Self::Accepted
                    | Self::CorrectInScopeRequired
                    | Self::Held
                    | Self::Blocked
                    | Self::EffectUnknown
                    | Self::Failed
                    | Self::Cancelled
            ),
            Self::CorrectInScopeRequired => matches!(
                next,
                Self::DispatchReserved | Self::Held | Self::Blocked | Self::Cancelled
            ),
            Self::Accepted => matches!(
                next,
                Self::IntegrationReserved | Self::Held | Self::Blocked | Self::Cancelled
            ),
            Self::IntegrationReserved => matches!(
                next,
                Self::Integrated
                    | Self::Accepted
                    | Self::EffectUnknown
                    | Self::Held
                    | Self::Blocked
                    | Self::Cancelled
            ),
            Self::Integrated => matches!(next, Self::Held | Self::Cancelled),
            Self::Held => matches!(
                next,
                Self::PendingDependencies
                    | Self::ReadyToDispatch
                    | Self::ReadyForReview
                    | Self::CorrectInScopeRequired
                    | Self::Accepted
                    | Self::Blocked
                    | Self::Cancelled
            ),
            Self::Blocked => matches!(
                next,
                Self::Held
                    | Self::ReadyToDispatch
                    | Self::ReadyForReview
                    | Self::CorrectInScopeRequired
                    | Self::Cancelled
            ),
            Self::EffectUnknown => matches!(
                next,
                Self::ReadyToDispatch
                    | Self::Running
                    | Self::ReadyForReview
                    | Self::UnderReview
                    | Self::Accepted
                    | Self::Integrated
                    | Self::Failed
                    | Self::Blocked
                    | Self::Cancelled
            ),
            Self::Failed => matches!(
                next,
                Self::DispatchReserved | Self::Held | Self::Blocked | Self::Cancelled
            ),
            Self::Cancelled => false,
        }
    }
}

impl fmt::Display for CoworkerTaskStage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Structured review verdict classification.
///
/// Only [`Self::Accept`] may advance a task candidate to [`CoworkerTaskStage::Accepted`].
/// Positive reviewer prose never overrides a non-`Accept` verdict.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReviewVerdictOutcome {
    /// Patch satisfies requirements, scope, policy, and test evidence; candidate may be accepted.
    Accept,
    /// Patch requires fixes that remain strictly within the approved task write scope and brief.
    CorrectInScope,
    /// Patch requires changes outside the task's approved write scope or conflicts with another interface.
    ScopeConflict,
    /// Review is blocked by failing checks, missing evidence, or an external blocker.
    Blocked,
    /// Review output or evidence is ambiguous/unclear; fails closed and blocks acceptance.
    Unclear,
}

impl ReviewVerdictOutcome {
    /// Returns the canonical `snake_case` wire identifier.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Accept => "accept",
            Self::CorrectInScope => "correct_in_scope",
            Self::ScopeConflict => "scope_conflict",
            Self::Blocked => "blocked",
            Self::Unclear => "unclear",
        }
    }
}

/// Structured, digest-bound review verdict for a specific [`TaskCandidateRef`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StructuredReviewVerdict {
    /// Deterministic verdict identifier (`vrd_<32hex>`).
    pub verdict_id: String,
    /// Target task identifier.
    pub task_id: String,
    /// 1-based task attempt number that produced the candidate.
    pub attempt_no: u32,
    /// Target candidate identifier.
    pub candidate_id: String,
    /// 64-character lowercase hex SHA-256 digest of the inspected `ReviewSubject`.
    pub review_subject_digest: String,
    /// Exact 40-character lowercase hex base commit SHA of the reviewed diff.
    pub base_commit: String,
    /// Exact 40-character lowercase hex task head commit SHA of the reviewed candidate.
    pub task_head_commit: String,
    /// Exact 40-character lowercase hex Git tree SHA of the reviewed candidate.
    pub task_tree_sha: String,
    /// 64-character lowercase hex SHA-256 digest of the scoped diff.
    pub diff_digest: String,
    /// 64-character lowercase hex SHA-256 digest of the governing [`ExecutionPolicy`].
    pub policy_digest: String,
    /// Structured verdict outcome (`accept`, `correct_in_scope`, `scope_conflict`, `blocked`, or `unclear`).
    pub outcome: ReviewVerdictOutcome,
    /// Bounded structured findings reported by the reviewer.
    #[serde(default)]
    pub findings: Vec<String>,
    /// Required in-scope fixes (must be non-empty for `CorrectInScope` and empty for `Accept`).
    #[serde(default)]
    pub required_in_scope_fixes: Vec<String>,
    /// Conflicting paths or interface identifiers (must be non-empty for `ScopeConflict` and empty for `Accept`).
    #[serde(default)]
    pub conflicting_paths_or_interfaces: Vec<String>,
    /// Optional bounded reviewer prose summary (informational only; never advances acceptance).
    pub prose_summary: Option<String>,
    /// Must always be `false`: positive reviewer prose never advances acceptance.
    pub prose_may_advance_acceptance: bool,
}

impl StructuredReviewVerdict {
    /// Validates all digests, bounds, and outcome-specific field invariants.
    pub fn validate(&self) -> Result<(), NativeError> {
        validate_bounded_identifier(&self.verdict_id, "verdict_id", MAX_COWORKER_ID_LEN)?;
        validate_bounded_identifier(&self.task_id, "task_id", MAX_COWORKER_ID_LEN)?;
        if self.attempt_no == 0 {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "StructuredReviewVerdict.attempt_no must be >= 1",
            ));
        }
        validate_bounded_identifier(&self.candidate_id, "candidate_id", MAX_COWORKER_ID_LEN)?;
        validate_sha256_hex_digest(&self.review_subject_digest, "review_subject_digest").map_err(
            |_| {
                NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "review_subject_digest must be a 64-character lowercase hex SHA-256",
                )
            },
        )?;
        validate_exact_40_hex_sha(&self.base_commit, "base_commit").map_err(|_| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                "base_commit must be a 40-character lowercase hex SHA-1",
            )
        })?;
        validate_exact_40_hex_sha(&self.task_head_commit, "task_head_commit").map_err(|_| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                "task_head_commit must be a 40-character lowercase hex SHA-1",
            )
        })?;
        validate_exact_40_hex_sha(&self.task_tree_sha, "task_tree_sha").map_err(|_| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                "task_tree_sha must be a 40-character lowercase hex SHA-1",
            )
        })?;
        validate_sha256_hex_digest(&self.diff_digest, "diff_digest").map_err(|_| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                "diff_digest must be a 64-character lowercase hex SHA-256",
            )
        })?;
        validate_sha256_hex_digest(&self.policy_digest, "policy_digest").map_err(|_| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                "policy_digest must be a 64-character lowercase hex SHA-256",
            )
        })?;

        if self.prose_may_advance_acceptance {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "StructuredReviewVerdict.prose_may_advance_acceptance must be false; positive prose never advances acceptance",
            ));
        }
        if let Some(ref prose) = self.prose_summary {
            validate_bounded_text(prose, "prose_summary")?;
        }
        validate_unique_text_list(&self.findings, "findings", MAX_WORK_BRIEF_ITEMS)?;
        validate_unique_text_list(
            &self.required_in_scope_fixes,
            "required_in_scope_fixes",
            MAX_WORK_BRIEF_ITEMS,
        )?;
        validate_unique_text_list(
            &self.conflicting_paths_or_interfaces,
            "conflicting_paths_or_interfaces",
            MAX_WORK_BRIEF_ITEMS,
        )?;

        match self.outcome {
            ReviewVerdictOutcome::Accept => {
                if !self.required_in_scope_fixes.is_empty()
                    || !self.conflicting_paths_or_interfaces.is_empty()
                {
                    return Err(NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "ReviewVerdictOutcome::Accept cannot have required_in_scope_fixes or conflicting_paths_or_interfaces",
                    ));
                }
            }
            ReviewVerdictOutcome::CorrectInScope => {
                if self.required_in_scope_fixes.is_empty()
                    || !self.conflicting_paths_or_interfaces.is_empty()
                {
                    return Err(NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "ReviewVerdictOutcome::CorrectInScope requires non-empty required_in_scope_fixes and no scope conflicts",
                    ));
                }
            }
            ReviewVerdictOutcome::ScopeConflict => {
                if self.conflicting_paths_or_interfaces.is_empty() {
                    return Err(NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "ReviewVerdictOutcome::ScopeConflict requires non-empty conflicting_paths_or_interfaces",
                    ));
                }
            }
            ReviewVerdictOutcome::Blocked | ReviewVerdictOutcome::Unclear => {
                if self.findings.is_empty() {
                    return Err(NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "ReviewVerdictOutcome::Blocked and Unclear require at least one structured finding",
                    ));
                }
            }
        }
        Ok(())
    }

    /// Returns `true` if and only if the verdict is valid and its structured outcome is
    /// [`ReviewVerdictOutcome::Accept`].
    pub fn allows_acceptance(&self) -> bool {
        self.validate().is_ok()
            && self.outcome == ReviewVerdictOutcome::Accept
            && self.required_in_scope_fixes.is_empty()
            && self.conflicting_paths_or_interfaces.is_empty()
            && !self.prose_may_advance_acceptance
    }

    /// Maps the structured review verdict outcome to its required [`CoworkerTaskStage`].
    pub const fn target_task_stage(&self) -> CoworkerTaskStage {
        match self.outcome {
            ReviewVerdictOutcome::Accept => CoworkerTaskStage::Accepted,
            ReviewVerdictOutcome::CorrectInScope => CoworkerTaskStage::CorrectInScopeRequired,
            ReviewVerdictOutcome::ScopeConflict => CoworkerTaskStage::Held,
            ReviewVerdictOutcome::Blocked | ReviewVerdictOutcome::Unclear => {
                CoworkerTaskStage::Blocked
            }
        }
    }
}

/// Durable reference to a single task execution or review attempt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskAttemptRef {
    /// 1-based attempt number within the owning [`CoworkerTaskRecord`].
    pub attempt_no: u32,
    /// Role executed in this attempt (`Coder`, `Reviewer`, or `Closure`).
    pub role: WorkflowRole,
    /// Idempotent request key bound to this attempt.
    pub request_key: String,
    /// Bound [`OutboxDispatchRecord::outbox_id`].
    pub outbox_id: String,
    /// Sealed runtime `run_id` (`run_<uuid>`), if observed.
    pub run_id: Option<String>,
    /// Sealed runtime `worker_id` (`wrk_<uuid>`), if observed.
    pub worker_id: Option<String>,
    /// Owned task worktree identifier, if observed.
    pub task_worktree_id: Option<String>,
    /// Exact 40-character lowercase hex base commit SHA for this attempt.
    pub base_commit: String,
    /// Exact 40-character lowercase hex head commit SHA produced by this attempt, if committed.
    pub head_commit: Option<String>,
    /// Exact 40-character lowercase hex Git tree SHA produced by this attempt, if committed.
    pub tree_sha: Option<String>,
    /// 64-character lowercase hex SHA-256 diff digest, if evaluated.
    pub diff_digest: Option<String>,
    /// Local report artifact identifier, if recorded.
    pub report_artifact_id: Option<String>,
    /// Verified provider model observed in worker stream telemetry.
    pub observed_model: Option<String>,
    /// Verified provider reasoning effort observed in worker stream telemetry.
    pub observed_effort: Option<String>,
}

/// Durable reference to a committed per-task patch candidate and its review/integration receipts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskCandidateRef {
    /// Deterministic candidate identifier (`cnd_<32hex>`).
    pub candidate_id: String,
    /// Owning task identifier.
    pub task_id: String,
    /// 1-based attempt number that produced this candidate.
    pub attempt_no: u32,
    /// Exact 40-character lowercase hex base commit SHA.
    pub base_commit: String,
    /// Exact 40-character lowercase hex committed head SHA in the task worktree.
    pub head_commit: String,
    /// Exact 40-character lowercase hex Git tree SHA of `head_commit`.
    pub tree_sha: String,
    /// 64-character lowercase hex SHA-256 digest of the scoped diff.
    pub diff_digest: String,
    /// Repository-relative write paths touched or governed by this candidate.
    pub write_paths: Vec<String>,
    /// 64-character lowercase hex SHA-256 digests of verification check evidence bound to this candidate.
    #[serde(default)]
    pub check_evidence_digests: Vec<String>,
    /// 64-character lowercase hex SHA-256 digest of the inspected `ReviewSubject`, if reviewed.
    pub review_subject_digest: Option<String>,
    /// Structured review verdict recorded for this candidate, if reviewed.
    pub review_verdict: Option<StructuredReviewVerdict>,
    /// Sealed `ReviewReceipt` identifier (`rev_<hex>`), required when accepted.
    pub review_receipt_id: Option<String>,
    /// Sealed `IntegrationReceipt` identifier (`int_<hex>`), required when integrated.
    pub integration_receipt_id: Option<String>,
}

impl TaskCandidateRef {
    /// Validates candidate commits, digests, write paths, and review verdict linkage.
    pub fn validate(&self) -> Result<(), NativeError> {
        validate_bounded_identifier(&self.candidate_id, "candidate_id", MAX_COWORKER_ID_LEN)?;
        validate_bounded_identifier(&self.task_id, "task_id", MAX_COWORKER_ID_LEN)?;
        if self.attempt_no == 0 {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "TaskCandidateRef.attempt_no must be >= 1",
            ));
        }
        validate_exact_40_hex_sha(&self.base_commit, "candidate.base_commit").map_err(|_| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                "candidate.base_commit must be a 40-character lowercase hex SHA-1",
            )
        })?;
        validate_exact_40_hex_sha(&self.head_commit, "candidate.head_commit").map_err(|_| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                "candidate.head_commit must be a 40-character lowercase hex SHA-1",
            )
        })?;
        validate_exact_40_hex_sha(&self.tree_sha, "candidate.tree_sha").map_err(|_| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                "candidate.tree_sha must be a 40-character lowercase hex SHA-1",
            )
        })?;
        validate_sha256_hex_digest(&self.diff_digest, "candidate.diff_digest").map_err(|_| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                "candidate.diff_digest must be a 64-character lowercase hex SHA-256",
            )
        })?;
        if self.write_paths.is_empty() {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "TaskCandidateRef.write_paths must not be empty",
            ));
        }
        validate_unique_text_list(
            &self.write_paths,
            "candidate.write_paths",
            MAX_WORK_BRIEF_ITEMS,
        )?;
        for digest in &self.check_evidence_digests {
            validate_sha256_hex_digest(digest, "check_evidence_digest").map_err(|_| {
                NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "check_evidence_digest must be a 64-character lowercase hex SHA-256",
                )
            })?;
        }
        if let Some(ref subj) = self.review_subject_digest {
            validate_sha256_hex_digest(subj, "review_subject_digest").map_err(|_| {
                NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "review_subject_digest must be a 64-character lowercase hex SHA-256",
                )
            })?;
        }
        if let Some(ref verdict) = self.review_verdict {
            verdict.validate()?;
            if verdict.task_id != self.task_id
                || verdict.attempt_no != self.attempt_no
                || verdict.candidate_id != self.candidate_id
                || verdict.base_commit != self.base_commit
                || verdict.task_head_commit != self.head_commit
                || verdict.task_tree_sha != self.tree_sha
                || verdict.diff_digest != self.diff_digest
                || self.review_subject_digest.as_deref()
                    != Some(verdict.review_subject_digest.as_str())
            {
                return Err(NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "StructuredReviewVerdict fields do not match the bound TaskCandidateRef",
                ));
            }
        }
        if let Some(ref rev_id) = self.review_receipt_id {
            validate_bounded_identifier(rev_id, "review_receipt_id", MAX_COWORKER_ID_LEN)?;
        }
        if let Some(ref int_id) = self.integration_receipt_id {
            validate_bounded_identifier(int_id, "integration_receipt_id", MAX_COWORKER_ID_LEN)?;
        }
        Ok(())
    }
}

/// Receipt-bound reference to an accepted and integrated predecessor task candidate.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AcceptedPredecessorReceiptRef {
    /// Logical predecessor task identifier.
    pub task_id: String,
    /// Accepted candidate identifier for `task_id`.
    pub candidate_id: String,
    /// Exact 40-character lowercase hex committed head SHA of the predecessor task.
    pub head_commit: String,
    /// Exact 40-character lowercase hex Git tree SHA of `head_commit`.
    pub tree_sha: String,
    /// 64-character lowercase hex SHA-256 digest of the accepted `ReviewSubject`.
    pub review_subject_digest: String,
    /// Sealed `ReviewReceipt` identifier (`rev_<hex>`).
    pub review_receipt_id: String,
    /// Sealed `IntegrationReceipt` identifier (`int_<hex>`).
    pub integration_receipt_id: String,
}

impl AcceptedPredecessorReceiptRef {
    /// Validates all identifiers and commit/tree/subject digests on the predecessor receipt reference.
    pub fn validate(&self) -> Result<(), NativeError> {
        validate_bounded_identifier(&self.task_id, "predecessor.task_id", MAX_COWORKER_ID_LEN)?;
        validate_bounded_identifier(
            &self.candidate_id,
            "predecessor.candidate_id",
            MAX_COWORKER_ID_LEN,
        )?;
        validate_exact_40_hex_sha(&self.head_commit, "predecessor.head_commit").map_err(|_| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                "predecessor.head_commit must be a 40-character lowercase hex SHA-1",
            )
        })?;
        validate_exact_40_hex_sha(&self.tree_sha, "predecessor.tree_sha").map_err(|_| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                "predecessor.tree_sha must be a 40-character lowercase hex SHA-1",
            )
        })?;
        validate_sha256_hex_digest(
            &self.review_subject_digest,
            "predecessor.review_subject_digest",
        )
        .map_err(|_| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                "predecessor.review_subject_digest must be a 64-character lowercase hex SHA-256",
            )
        })?;
        validate_bounded_identifier(
            &self.review_receipt_id,
            "predecessor.review_receipt_id",
            MAX_COWORKER_ID_LEN,
        )?;
        validate_bounded_identifier(
            &self.integration_receipt_id,
            "predecessor.integration_receipt_id",
            MAX_COWORKER_ID_LEN,
        )?;
        Ok(())
    }
}

/// Durable per-task execution, review, dependency, hold, and candidate tracking record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CoworkerTaskRecord {
    /// Approved task identifier.
    pub task_id: String,
    /// 64-character lowercase hex SHA-256 digest of the compiled task specification.
    pub task_digest: String,
    /// Assigned workflow role.
    pub role: WorkflowRole,
    /// Assigned difficulty tier.
    pub difficulty: Difficulty,
    /// Frozen headless worker choice resolved for this task.
    pub selected_choice: SelectedRoleChoice,
    /// Direct upstream prerequisite task identifiers.
    pub depends_on: Vec<String>,
    /// Approved repository-relative write scopes for this task.
    pub write_paths: Vec<String>,
    /// Logical interface identifiers owned/defined by this task (used for independence proofs).
    #[serde(default)]
    pub owned_interfaces: Vec<String>,
    /// Logical interface identifiers consumed by this task (used for independence proofs).
    #[serde(default)]
    pub consumed_interfaces: Vec<String>,
    /// Current legal task stage.
    pub stage: CoworkerTaskStage,
    /// Prior task stage saved when transitioning into [`CoworkerTaskStage::Held`].
    pub stage_before_hold: Option<CoworkerTaskStage>,
    /// Current 1-based attempt counter (`0` before first dispatch reservation).
    pub current_attempt_no: u32,
    /// Currently active candidate identifier (`cnd_<32hex>`), if committed.
    pub active_candidate_id: Option<String>,
    /// Accepted candidate identifier (`cnd_<32hex>`), set only after a structured `Accept` verdict.
    pub accepted_candidate_id: Option<String>,
    /// Prepared dependency input commit SHA (required before dispatching a task with non-empty `depends_on`).
    pub dependency_input_commit: Option<String>,
    /// Prepared dependency input Git tree SHA (required before dispatching a task with non-empty `depends_on`).
    pub dependency_input_tree_sha: Option<String>,
    /// Verified accepted predecessor receipts bound to this task's prepared dependency input.
    #[serde(default)]
    pub accepted_predecessor_receipts: Vec<AcceptedPredecessorReceiptRef>,
    /// Recorded task attempts in creation order.
    #[serde(default)]
    pub attempts: Vec<TaskAttemptRef>,
    /// Recorded task candidates in creation order.
    #[serde(default)]
    pub candidates: Vec<TaskCandidateRef>,
    /// Active [`WorkflowHoldRecord::hold_id`]s currently holding this task.
    #[serde(default)]
    pub active_hold_ids: Vec<String>,
    /// Unresolved [`OutboxDispatchRecord::outbox_id`] in `ReservedBeforeEffect` or `EffectUnknown`.
    pub unresolved_outbox_id: Option<String>,
}

/// Category of external effect tracked by an [`OutboxDispatchRecord`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OutboxDispatchKind {
    /// Headless worker coding attempt dispatch.
    TaskAttemptDispatch,
    /// Headless worker review dispatch.
    TaskReviewDispatch,
    /// Dependency input preparation and sealing.
    DependencyPreparation,
    /// Accepted task integration into the run integration worktree.
    TaskIntegration,
    /// Final package closure review dispatch.
    ClosureReviewDispatch,
}

/// Lifecycle status of a reserve-before-effect [`OutboxDispatchRecord`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OutboxDispatchStatus {
    /// Durable intent reserved before crossing the external host/provider boundary.
    ReservedBeforeEffect,
    /// Proven refused before any worker or Git mutation occurred.
    RefusedBeforeEffect,
    /// External dispatch or mutation succeeded and returned a sealed receipt.
    DispatchedEffectConfirmed,
    /// Outcome is uncertain after durable reservation; blocks automatic replay until reconciled.
    EffectUnknown,
    /// Explicitly reconciled against live host/receipt state and confirmed executed.
    ReconciledConfirmed,
    /// Explicitly reconciled against live host/receipt state and proven never executed.
    ReconciledNotExecuted,
}

impl OutboxDispatchStatus {
    /// Returns `true` when this outbox record is in `ReservedBeforeEffect` or `EffectUnknown` and
    /// therefore forbids automatic replay or new conflicting dispatch.
    pub const fn blocks_replay_and_new_dispatch(self) -> bool {
        matches!(self, Self::ReservedBeforeEffect | Self::EffectUnknown)
    }
}

/// Durable outbox reservation record persisted before any external worker or Git effect.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OutboxDispatchRecord {
    /// Deterministic outbox identifier (`obx_<32hex>`).
    pub outbox_id: String,
    /// Idempotent caller request key.
    pub request_key: String,
    /// Category of external dispatch or mutation.
    pub kind: OutboxDispatchKind,
    /// Target task identifier.
    pub task_id: String,
    /// 1-based attempt number for the target task.
    pub attempt_no: u32,
    /// Workflow role associated with this dispatch.
    pub role: WorkflowRole,
    /// 64-character lowercase hex SHA-256 digest of the dispatch subject.
    pub subject_digest: String,
    /// Current outbox status.
    pub status: OutboxDispatchStatus,
    /// Job revision when this outbox record was reserved before effect.
    pub reserved_at_job_revision: u64,
    /// Job revision when this outbox record was most recently updated.
    pub updated_at_job_revision: u64,
    /// Sealed runtime `run_id`, if observed.
    pub run_id: Option<String>,
    /// Sealed runtime `worker_id`, if observed.
    pub worker_id: Option<String>,
    /// Sealed review, integration, or dependency receipt identifier, if observed.
    pub receipt_id: Option<String>,
    /// Optional sanitized diagnostic text when refused or uncertain.
    pub diagnostic: Option<String>,
}

/// Reason why a [`WorkflowHoldRecord`] holds advancement of affected tasks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowHoldReason {
    /// Human submitted a conversational clarification waiting for a safe checkpoint.
    ConversationClarificationPending,
    /// Human requested a scope, model, profile, or stop change requiring replan and re-approval.
    ConversationReplanRequired,
    /// A [`WorkBrief`] amendment is pending against this job.
    BriefAmendmentPending,
    /// Reviewer reported [`ReviewVerdictOutcome::ScopeConflict`].
    ReviewScopeConflict,
    /// An upstream dependency task is held or failed verification.
    UpstreamDependencyHeld,
    /// A sibling task with overlapping write scope or interface ownership was modified or held.
    SharedWriteScopeOrInterfaceHold,
    /// An outbox dispatch outcome is [`OutboxDispatchStatus::EffectUnknown`] and requires reconciliation.
    UnknownEffectReconciliationRequired,
}

/// Durable hold record pausing advancement of affected tasks until explicitly resolved.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkflowHoldRecord {
    /// Deterministic hold identifier (`hld_<32hex>`).
    pub hold_id: String,
    /// Idempotent caller request key.
    pub request_key: String,
    /// Structured hold reason.
    pub reason: WorkflowHoldReason,
    /// Bounded human-readable hold description.
    pub detail: String,
    /// Task identifiers directly targeted by the change or conflict.
    pub directly_affected_task_ids: Vec<String>,
    /// Repository-relative write paths affected by the change or conflict.
    #[serde(default)]
    pub affected_write_paths: Vec<String>,
    /// Logical interface identifiers affected by the change or conflict.
    #[serde(default)]
    pub affected_interfaces: Vec<String>,
    /// When `true`, holds every task in the job (e.g. model/profile replan or job-wide amendment).
    pub holds_entire_job: bool,
    /// Whether this hold is currently active.
    pub active: bool,
    /// Job revision when this hold was recorded.
    pub created_at_job_revision: u64,
    /// Job revision when this hold was resolved, if resolved.
    pub resolved_at_job_revision: Option<u64>,
    /// Bounded resolution note, if resolved.
    pub resolution_note: Option<String>,
}

/// Evidence report evaluating whether a task is disjoint from all active holds and impacted sibling
/// tasks under **explicitly declared** dependency closure, write-scope, and interface metadata.
///
/// This proof evaluates declared metadata only; it cannot prove undeclared semantic independence.
/// Whenever any hold is active or any sibling task is impacted, [`Self::complete_declared_evidence`]
/// requires explicit completeness of dependency, write-scope, and interface declarations on the
/// candidate task and on every active hold or impacted sibling task, conservatively falling back to
/// `independent: false` when any required declaration is incomplete or unknown.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskIndependenceProof {
    /// Task identifier evaluated.
    pub candidate_task_id: String,
    /// `true` if and only if declared evidence is complete and all four disjointness checks pass.
    pub independent: bool,
    /// `true` when complete dependency, write-scope, and interface impact metadata is declared
    /// on the candidate task and on every active hold or impacted sibling task.
    #[serde(default)]
    pub complete_declared_evidence: bool,
    /// `true` when the task's transitive `depends_on` closure contains no held or uncertain tasks.
    pub disjoint_dependency_closure: bool,
    /// `true` when the task's `write_paths` are complete and disjoint from all active holds and held/uncertain tasks.
    pub disjoint_write_scopes: bool,
    /// `true` when the task's owned/consumed interfaces are complete and disjoint from all active holds and held/uncertain tasks.
    pub disjoint_interface_ownership: bool,
    /// `true` when no job-wide hold is active and the task is not directly targeted by a hold.
    pub no_job_wide_hold: bool,
    /// Concrete reasons why declared independence or completeness failed (empty when `independent == true`).
    pub blocking_reasons: Vec<String>,
}

/// Evaluates whether `candidate_task` is disjoint from all active holds and impacted sibling tasks
/// in `all_tasks` based on explicitly declared dependency closure, write-scope, and interface
/// ownership metadata.
///
/// This function cannot prove undeclared semantic independence. Whenever any hold is active or any
/// sibling task is impacted, it requires complete impact/dependency/interface declarations on the
/// candidate task, on every non-job-wide active hold, and on every impacted sibling task. If any
/// required declaration is incomplete (for example, disjoint file `write_paths` but empty
/// `owned_interfaces`/`consumed_interfaces` or empty `affected_interfaces`), it conservatively
/// falls back to `independent: false` and `complete_declared_evidence: false`.
pub fn prove_task_independence(
    candidate_task: &CoworkerTaskRecord,
    all_tasks: &[CoworkerTaskRecord],
    active_holds: &[WorkflowHoldRecord],
) -> Result<TaskIndependenceProof, NativeError> {
    let mut task_by_id = BTreeMap::new();
    for task in all_tasks {
        if task_by_id.insert(task.task_id.as_str(), task).is_some() {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "duplicate task_id in prove_task_independence",
            ));
        }
    }
    if !task_by_id.contains_key(candidate_task.task_id.as_str()) {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "candidate_task is not present in all_tasks",
        ));
    }

    // Compute transitive upstream dependency closure of `candidate_task`.
    let mut upstream_closure = BTreeSet::new();
    let mut stack: Vec<&str> = candidate_task
        .depends_on
        .iter()
        .map(String::as_str)
        .collect();
    while let Some(dep_id) = stack.pop() {
        if dep_id == candidate_task.task_id {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "dependency cycle detected while proving task independence",
            ));
        }
        let dep_task = task_by_id.get(dep_id).ok_or_else(|| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!("unknown dependency task_id `{dep_id}`"),
            )
        })?;
        if upstream_closure.insert(dep_id) {
            for parent in &dep_task.depends_on {
                stack.push(parent.as_str());
            }
        }
    }

    let has_active_holds = active_holds.iter().any(|h| h.active);
    let has_impacted_siblings = all_tasks.iter().any(|other| {
        other.task_id != candidate_task.task_id
            && (matches!(
                other.stage,
                CoworkerTaskStage::Held
                    | CoworkerTaskStage::Blocked
                    | CoworkerTaskStage::EffectUnknown
            ) || !other.active_hold_ids.is_empty()
                || other.unresolved_outbox_id.is_some())
    });

    let mut complete_declared_evidence = true;
    let mut no_job_wide_hold = true;
    let mut disjoint_dependency_closure = true;
    let mut disjoint_write_scopes = true;
    let mut disjoint_interface_ownership = true;
    let mut blocking_reasons = Vec::new();

    if has_active_holds || has_impacted_siblings {
        if candidate_task.write_paths.is_empty() {
            complete_declared_evidence = false;
            disjoint_write_scopes = false;
            blocking_reasons.push(format!(
                "task `{}` has incomplete write_paths declaration during an active hold or sibling task impact",
                candidate_task.task_id
            ));
        }
        if candidate_task.owned_interfaces.is_empty()
            && candidate_task.consumed_interfaces.is_empty()
        {
            complete_declared_evidence = false;
            disjoint_interface_ownership = false;
            blocking_reasons.push(format!(
                "task `{}` has incomplete interface impact declaration (owned_interfaces and consumed_interfaces are both empty) during an active hold or sibling task impact",
                candidate_task.task_id
            ));
        }
    }

    for hold in active_holds.iter().filter(|h| h.active) {
        if hold.holds_entire_job {
            no_job_wide_hold = false;
            blocking_reasons.push(format!(
                "hold `{}` ({:?}) applies to the entire job",
                hold.hold_id, hold.reason
            ));
        } else {
            if hold.directly_affected_task_ids.is_empty() {
                complete_declared_evidence = false;
                disjoint_dependency_closure = false;
                blocking_reasons.push(format!(
                    "active hold `{}` has incomplete directly_affected_task_ids declaration",
                    hold.hold_id
                ));
            }
            if hold.affected_write_paths.is_empty() {
                complete_declared_evidence = false;
                disjoint_write_scopes = false;
                blocking_reasons.push(format!(
                    "active hold `{}` has incomplete affected_write_paths declaration",
                    hold.hold_id
                ));
            }
            if hold.affected_interfaces.is_empty() {
                complete_declared_evidence = false;
                disjoint_interface_ownership = false;
                blocking_reasons.push(format!(
                    "active hold `{}` has incomplete affected_interfaces declaration",
                    hold.hold_id
                ));
            }
        }
        if hold
            .directly_affected_task_ids
            .iter()
            .any(|id| id == &candidate_task.task_id)
        {
            no_job_wide_hold = false;
            blocking_reasons.push(format!(
                "hold `{}` directly targets task `{}`",
                hold.hold_id, candidate_task.task_id
            ));
        }
        for dep_id in &upstream_closure {
            if hold
                .directly_affected_task_ids
                .iter()
                .any(|id| id == dep_id)
            {
                disjoint_dependency_closure = false;
                blocking_reasons.push(format!(
                    "upstream dependency `{dep_id}` is targeted by active hold `{}`",
                    hold.hold_id
                ));
            }
        }
        if write_scope_sets_overlap(&candidate_task.write_paths, &hold.affected_write_paths) {
            disjoint_write_scopes = false;
            blocking_reasons.push(format!(
                "task `{}` write_paths overlap with active hold `{}` affected_write_paths",
                candidate_task.task_id, hold.hold_id
            ));
        }
        if interface_sets_overlap(
            &candidate_task.owned_interfaces,
            &candidate_task.consumed_interfaces,
            &hold.affected_interfaces,
        ) {
            disjoint_interface_ownership = false;
            blocking_reasons.push(format!(
                "task `{}` interfaces overlap with active hold `{}` affected_interfaces",
                candidate_task.task_id, hold.hold_id
            ));
        }
    }

    for other in all_tasks {
        if other.task_id == candidate_task.task_id {
            continue;
        }
        let other_is_impacted = matches!(
            other.stage,
            CoworkerTaskStage::Held | CoworkerTaskStage::Blocked | CoworkerTaskStage::EffectUnknown
        ) || !other.active_hold_ids.is_empty()
            || other.unresolved_outbox_id.is_some();

        if !other_is_impacted {
            continue;
        }

        if other.write_paths.is_empty() {
            complete_declared_evidence = false;
            disjoint_write_scopes = false;
            blocking_reasons.push(format!(
                "impacted task `{}` has incomplete write_paths declaration",
                other.task_id
            ));
        }
        if other.owned_interfaces.is_empty() && other.consumed_interfaces.is_empty() {
            complete_declared_evidence = false;
            disjoint_interface_ownership = false;
            blocking_reasons.push(format!(
                "impacted task `{}` has incomplete interface impact declaration (owned_interfaces and consumed_interfaces are both empty)",
                other.task_id
            ));
        }

        if upstream_closure.contains(other.task_id.as_str()) {
            disjoint_dependency_closure = false;
            blocking_reasons.push(format!(
                "upstream dependency `{}` is in impacted stage `{}`",
                other.task_id, other.stage
            ));
        }
        if write_scope_sets_overlap(&candidate_task.write_paths, &other.write_paths) {
            disjoint_write_scopes = false;
            blocking_reasons.push(format!(
                "task `{}` write_paths overlap with impacted task `{}`",
                candidate_task.task_id, other.task_id
            ));
        }
        if interface_sets_overlap(
            &candidate_task.owned_interfaces,
            &candidate_task.consumed_interfaces,
            &other.owned_interfaces,
        ) || interface_sets_overlap(
            &other.owned_interfaces,
            &other.consumed_interfaces,
            &candidate_task.owned_interfaces,
        ) {
            disjoint_interface_ownership = false;
            blocking_reasons.push(format!(
                "task `{}` interface ownership overlaps with impacted task `{}`",
                candidate_task.task_id, other.task_id
            ));
        }
    }

    let independent = complete_declared_evidence
        && no_job_wide_hold
        && disjoint_dependency_closure
        && disjoint_write_scopes
        && disjoint_interface_ownership;

    Ok(TaskIndependenceProof {
        candidate_task_id: candidate_task.task_id.clone(),
        independent,
        complete_declared_evidence,
        disjoint_dependency_closure,
        disjoint_write_scopes,
        disjoint_interface_ownership,
        no_job_wide_hold,
        blocking_reasons,
    })
}

/// Explicitly named alias for [`prove_task_independence`] emphasizing that independence is
/// evaluated strictly over declared dependency, write-scope, and interface metadata and requires
/// complete impact declarations during holds.
pub fn evaluate_declared_task_independence(
    candidate_task: &CoworkerTaskRecord,
    all_tasks: &[CoworkerTaskRecord],
    active_holds: &[WorkflowHoldRecord],
) -> Result<TaskIndependenceProof, NativeError> {
    prove_task_independence(candidate_task, all_tasks, active_holds)
}

/// Returns `true` if two repository-relative write-path lists overlap (exact match or directory prefix).
pub fn write_scope_sets_overlap(lhs: &[String], rhs: &[String]) -> bool {
    for a in lhs {
        for b in rhs {
            if single_write_paths_overlap(a, b) {
                return true;
            }
        }
    }
    false
}

fn single_write_paths_overlap(a: &str, b: &str) -> bool {
    let a_clean = a.trim_end_matches('/');
    let b_clean = b.trim_end_matches('/');
    if a_clean == b_clean {
        return true;
    }
    if a.ends_with('/') && b_clean.starts_with(a) {
        return true;
    }
    if b.ends_with('/') && a_clean.starts_with(b) {
        return true;
    }
    false
}

fn interface_sets_overlap(
    owned: &[String],
    consumed: &[String],
    target_interfaces: &[String],
) -> bool {
    for target in target_interfaces {
        if owned.iter().any(|x| x == target) || consumed.iter().any(|x| x == target) {
            return true;
        }
    }
    false
}

/// Verification check evidence entry bound into a [`CombinedCandidateIdentity`].
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CandidateCheckEvidence {
    /// Stable check identifier (e.g. `"cargo_fmt"`, `"cargo_clippy"`, `"cargo_test"`).
    pub check_id: String,
    /// Bounded command or gate summary.
    pub command_summary: String,
    /// Whether the check passed (`must be true` for a verified combined candidate).
    pub passed: bool,
    /// Process exit code observed (`must be 0` for a verified combined candidate).
    pub exit_code: i32,
    /// 64-character lowercase hex SHA-256 digest of the check output/artifact evidence.
    pub evidence_digest: String,
}

impl CandidateCheckEvidence {
    /// Validates the check evidence fields and requires `passed == true` and `exit_code == 0`.
    pub fn validate_passed(&self) -> Result<(), NativeError> {
        validate_bounded_identifier(&self.check_id, "check_id", MAX_COWORKER_ID_LEN)?;
        validate_bounded_text(&self.command_summary, "command_summary")?;
        validate_sha256_hex_digest(&self.evidence_digest, "evidence_digest").map_err(|_| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                "evidence_digest must be a 64-character lowercase hex SHA-256",
            )
        })?;
        if !self.passed || self.exit_code != 0 {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!(
                    "CandidateCheckEvidence `{}` must pass with exit_code 0 before sealing a combined candidate",
                    self.check_id
                ),
            ));
        }
        Ok(())
    }
}

/// Lifecycle status of a [`CombinedCandidateIdentity`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CombinedCandidateStatus {
    /// Combined candidate tree and check evidence were assembled and verified.
    ChecksVerified,
    /// Single-closure reservation (`PackageClosureReservation`) is bound to this candidate tree.
    ClosureReserved,
    /// Package closure review accepted this exact combined candidate.
    ClosureAccepted,
    /// Candidate was superseded by a newer candidate tree.
    Superseded,
    /// Candidate was rejected by closure review.
    Rejected,
}

/// Combined final candidate identity binding the final Git tree, base commit/tree, plan/approval
/// revisions, scope/policy revision, accepted predecessor receipts, and check evidence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CombinedCandidateIdentity {
    /// Deterministic combined candidate identifier (`cmb_<32hex>`).
    pub combined_candidate_id: String,
    /// Idempotent caller request key.
    pub request_key: String,
    /// Owning [`CoworkerJobRecord::job_id`].
    pub job_id: String,
    /// Job revision when this combined candidate was recorded.
    pub job_revision: u64,
    /// Canonical repository path, base commit, and base tree SHA.
    pub project_base: CanonicalProjectBaseRef,
    /// Optional 40-character lowercase hex final integration commit SHA.
    pub final_commit_sha: Option<String>,
    /// Exact 40-character lowercase hex final Git tree SHA of the combined candidate.
    pub final_tree_sha: String,
    /// Frozen plan identifier.
    pub plan_id: String,
    /// Frozen published plan revision (`>= 1`).
    pub plan_revision: u64,
    /// 64-character lowercase hex SHA-256 digest of the compiled plan revision.
    pub plan_digest: String,
    /// Frozen operator approval identifier (`apr_<32hex>`).
    pub approval_id: String,
    /// 64-character lowercase hex SHA-256 `subject_digest` of the frozen approval.
    pub approval_subject_digest: String,
    /// Frozen registry / scope-policy revision (`>= 1`).
    pub scope_policy_revision: u64,
    /// Frozen execution policy identifier.
    pub policy_id: String,
    /// 64-character lowercase hex SHA-256 digest of the frozen [`ExecutionPolicy`].
    pub policy_digest: String,
    /// Non-empty list of accepted and integrated task receipts composing this candidate.
    pub accepted_predecessor_receipts: Vec<AcceptedPredecessorReceiptRef>,
    /// Non-empty list of passing check evidence entries for the combined tree.
    pub check_evidence: Vec<CandidateCheckEvidence>,
    /// Deterministic 64-character lowercase hex SHA-256 digest of the combined candidate identity.
    pub candidate_digest: String,
    /// Optional bound `PackageClosureReservation::reservation_id` (`cls_<32hex>`).
    pub closure_reservation_id: Option<String>,
    /// Current status of the combined candidate.
    pub status: CombinedCandidateStatus,
}

impl CombinedCandidateIdentity {
    /// Validates all constituent fields and computes the canonical 64-character lowercase hex
    /// SHA-256 `candidate_digest`.
    pub fn compute_candidate_digest(&self) -> Result<String, NativeError> {
        validate_bounded_identifier(
            &self.combined_candidate_id,
            "combined_candidate_id",
            MAX_COWORKER_ID_LEN,
        )?;
        validate_bounded_identifier(&self.request_key, "request_key", MAX_COWORKER_ID_LEN)?;
        validate_bounded_identifier(&self.job_id, "job_id", MAX_COWORKER_ID_LEN)?;
        if self.job_revision == 0 || self.plan_revision == 0 || self.scope_policy_revision == 0 {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "job_revision, plan_revision, and scope_policy_revision must be >= 1",
            ));
        }
        self.project_base.validate_syntax()?;
        if let Some(ref commit_sha) = self.final_commit_sha {
            validate_exact_40_hex_sha(commit_sha, "final_commit_sha").map_err(|_| {
                NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "final_commit_sha must be a 40-character lowercase hex SHA-1",
                )
            })?;
        }
        validate_exact_40_hex_sha(&self.final_tree_sha, "final_tree_sha").map_err(|_| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                "final_tree_sha must be a 40-character lowercase hex SHA-1",
            )
        })?;
        validate_bounded_identifier(&self.plan_id, "plan_id", MAX_COWORKER_ID_LEN)?;
        validate_sha256_hex_digest(&self.plan_digest, "plan_digest").map_err(|_| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                "plan_digest must be a 64-character lowercase hex SHA-256",
            )
        })?;
        validate_bounded_identifier(&self.approval_id, "approval_id", MAX_COWORKER_ID_LEN)?;
        validate_sha256_hex_digest(&self.approval_subject_digest, "approval_subject_digest")
            .map_err(|_| {
                NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "approval_subject_digest must be a 64-character lowercase hex SHA-256",
                )
            })?;
        validate_bounded_identifier(&self.policy_id, "policy_id", MAX_COWORKER_ID_LEN)?;
        validate_sha256_hex_digest(&self.policy_digest, "policy_digest").map_err(|_| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                "policy_digest must be a 64-character lowercase hex SHA-256",
            )
        })?;

        if self.accepted_predecessor_receipts.is_empty()
            || self.accepted_predecessor_receipts.len() > MAX_TASKS_PER_JOB
        {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "CombinedCandidateIdentity requires between 1 and 64 accepted_predecessor_receipts",
            ));
        }
        let mut seen_tasks = BTreeSet::new();
        let mut canonical_receipts = self.accepted_predecessor_receipts.clone();
        for receipt in &canonical_receipts {
            receipt.validate()?;
            if !seen_tasks.insert(receipt.task_id.as_str()) {
                return Err(NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "duplicate task_id in accepted_predecessor_receipts",
                ));
            }
        }
        canonical_receipts.sort();

        if self.check_evidence.is_empty() || self.check_evidence.len() > MAX_CHECK_EVIDENCE_ITEMS {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "CombinedCandidateIdentity requires between 1 and 64 passing check_evidence items",
            ));
        }
        let mut seen_checks = BTreeSet::new();
        let mut canonical_checks = self.check_evidence.clone();
        for check in &canonical_checks {
            check.validate_passed()?;
            if !seen_checks.insert(check.check_id.as_str()) {
                return Err(NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "duplicate check_id in check_evidence",
                ));
            }
        }
        canonical_checks.sort();

        if let Some(ref cls_id) = self.closure_reservation_id {
            validate_bounded_identifier(cls_id, "closure_reservation_id", MAX_COWORKER_ID_LEN)?;
        }

        let seed = (
            "threadlane-native:coworker_combined_candidate:v1",
            &self.project_base,
            &self.final_commit_sha,
            &self.final_tree_sha,
            &self.plan_id,
            self.plan_revision,
            &self.plan_digest,
            &self.approval_id,
            &self.approval_subject_digest,
            self.scope_policy_revision,
            &self.policy_id,
            &self.policy_digest,
            &canonical_receipts,
            &canonical_checks,
        );
        let bytes = serde_json::to_vec(&seed).map_err(|_| {
            NativeError::new(
                NativeErrorCode::ProtocolError,
                "failed to serialize CombinedCandidateIdentity for digest calculation",
            )
        })?;
        Ok(sha256_hex(&bytes))
    }

    /// Validates all fields and verifies `self.candidate_digest == self.compute_candidate_digest()?`.
    pub fn validate(&self) -> Result<(), NativeError> {
        let computed = self.compute_candidate_digest()?;
        if !constant_time_str_eq(&computed, &self.candidate_digest) {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "CombinedCandidateIdentity.candidate_digest does not match computed digest",
            ));
        }
        Ok(())
    }
}

/// Interface ownership metadata assigned to a task during planning or when binding an
/// [`ApprovalRecord`] to a job.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskInterfaceOwnershipSpec {
    /// Logical interface identifiers owned/defined by the task.
    #[serde(default)]
    pub owned_interfaces: Vec<String>,
    /// Logical interface identifiers consumed by the task.
    #[serde(default)]
    pub consumed_interfaces: Vec<String>,
}

impl TaskInterfaceOwnershipSpec {
    /// Validates bounded unique interface identifiers and requires `owned_interfaces` and
    /// `consumed_interfaces` on the same task to be disjoint.
    pub fn validate(&self) -> Result<(), NativeError> {
        validate_unique_id_list(
            &self.owned_interfaces,
            "owned_interfaces",
            MAX_WORK_BRIEF_ITEMS,
        )?;
        validate_unique_id_list(
            &self.consumed_interfaces,
            "consumed_interfaces",
            MAX_WORK_BRIEF_ITEMS,
        )?;
        for owned in &self.owned_interfaces {
            if self.consumed_interfaces.iter().any(|c| c == owned) {
                return Err(NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "a task cannot both own and consume the same interface identifier",
                ));
            }
        }
        Ok(())
    }
}

/// Durable linkage on a [`CoworkerJobRecord`] binding a published plan revision to the exact
/// verified Orchestrator planning reservation, report/output artifacts, [`WorkBrief`] digest,
/// immutable call base, and profile/registry/policy provenance that produced it.
///
/// Recording a [`PlanningResultRef`] transitions a job to [`CoworkerJobPhase::AwaitingApproval`]
/// and never grants execution approval; subsequent [`crate::coworker_workflow::CoworkerWorkflowStore::bind_job_approval`]
/// enforces that the human-approved plan revision, digest, base, and task interface declarations
/// match this exact planned result rather than an unrelated plan sharing the same base.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlanningResultRef {
    /// Standing [`PlanningAdviceGrant::grant_id`] under which the plan was produced.
    pub grant_id: String,
    /// Completed Orchestrator [`PlanningCallReservation::reservation_id`].
    pub reservation_id: String,
    /// Idempotent Orchestrator planning `request_key`.
    pub request_key: String,
    /// Supervised read-only worker `run_id` that produced the plan.
    pub run_id: String,
    /// Private bounded report artifact identifier persisted for this planning call.
    pub report_artifact_id: String,
    /// 64-character lowercase hex SHA-256 digest of the verified structured Orchestrator output.
    pub output_digest: String,
    /// 64-character lowercase hex SHA-256 digest of the [`WorkBrief`] planned by this call.
    pub work_brief_digest: String,
    /// Canonical repository and exact immutable base commit/tree SHA inspected during planning.
    pub project_base: CanonicalProjectBaseRef,
    /// Pinned workflow profile identifier from the standing grant.
    pub profile_id: String,
    /// Pinned workflow registry revision (`>= 1`) from the standing grant.
    pub registry_revision: u64,
    /// 64-character lowercase hex SHA-256 digest of the standing grant's [`ExecutionPolicy`].
    pub policy_digest: String,
    /// Exact resolved Orchestrator worker choice that produced the plan.
    pub selected_choice: SelectedRoleChoice,
    /// Published [`threadlane_workflow::PlanSummary::plan_id`] in [`threadlane_workflow::PlanStore`].
    pub plan_id: String,
    /// Published [`threadlane_workflow::PlanSummary::revision`] (`>= 1`) in [`threadlane_workflow::PlanStore`].
    pub plan_revision: u64,
    /// 64-character lowercase hex SHA-256 `plan_digest` of the published plan revision.
    pub plan_digest: String,
    /// Published plan status (`PlanStatus::Published`).
    pub plan_status: PlanStatus,
    /// Per-task interface ownership declarations produced by the Orchestrator (`task_id -> TaskInterfaceOwnershipSpec`).
    #[serde(default)]
    pub task_interfaces: BTreeMap<String, TaskInterfaceOwnershipSpec>,
    /// Optional [`ArchitectAdviceRecord::advice_id`] records consulted while producing this plan.
    #[serde(default)]
    pub architect_advice_ids: Vec<String>,
    /// Technical `WorkBrief.open_questions` entries explicitly resolved by this published plan.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub resolved_open_questions: Vec<String>,
    /// Unapproved `WorkBrief.proposed_additions` entries explicitly deferred and excluded from
    /// executable scope by this published plan (never promoted into confirmed requirements).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub deferred_proposals: Vec<String>,
    /// Timestamp (ms since Unix epoch) when the planning result was recorded.
    pub recorded_at_ms: u64,
}

impl PlanningResultRef {
    /// Validates all identifiers, SHA-256 digests, choice route, published plan status, and
    /// per-task interface metadata.
    pub fn validate(&self) -> Result<(), NativeError> {
        validate_bounded_identifier(&self.grant_id, "grant_id", MAX_COWORKER_ID_LEN)?;
        validate_bounded_identifier(&self.reservation_id, "reservation_id", MAX_COWORKER_ID_LEN)?;
        validate_bounded_identifier(&self.request_key, "request_key", MAX_COWORKER_ID_LEN)?;
        validate_bounded_identifier(&self.run_id, "run_id", MAX_COWORKER_ID_LEN)?;
        validate_bounded_identifier(
            &self.report_artifact_id,
            "report_artifact_id",
            MAX_COWORKER_ID_LEN,
        )?;
        validate_sha256_hex_digest(&self.output_digest, "planning_result.output_digest").map_err(
            |_| {
                NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "planning_result.output_digest must be a 64-character lowercase hex SHA-256",
                )
            },
        )?;
        validate_sha256_hex_digest(&self.work_brief_digest, "planning_result.work_brief_digest")
            .map_err(|_| {
                NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "planning_result.work_brief_digest must be a 64-character lowercase hex SHA-256",
                )
            })?;
        self.project_base.validate_syntax()?;
        validate_bounded_identifier(&self.profile_id, "profile_id", MAX_COWORKER_ID_LEN)?;
        if self.registry_revision == 0 || self.plan_revision == 0 {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "planning_result.registry_revision and plan_revision must be >= 1",
            ));
        }
        validate_sha256_hex_digest(&self.policy_digest, "planning_result.policy_digest").map_err(
            |_| {
                NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "planning_result.policy_digest must be a 64-character lowercase hex SHA-256",
                )
            },
        )?;
        validate_non_native_worker_choice(WorkflowRole::Orchestrator, &self.selected_choice)?;
        validate_bounded_identifier(&self.plan_id, "plan_id", MAX_COWORKER_ID_LEN)?;
        validate_sha256_hex_digest(&self.plan_digest, "planning_result.plan_digest").map_err(
            |_| {
                NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "planning_result.plan_digest must be a 64-character lowercase hex SHA-256",
                )
            },
        )?;
        if !self.plan_status.is_active_published() {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "PlanningResultRef.plan_status must be an active published revision",
            ));
        }
        if self.task_interfaces.len() > MAX_TASKS_PER_JOB {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "PlanningResultRef.task_interfaces exceeds maximum task capacity",
            ));
        }
        for (task_id, iface_spec) in &self.task_interfaces {
            validate_bounded_identifier(task_id, "task_id", MAX_COWORKER_ID_LEN)?;
            iface_spec.validate()?;
        }
        validate_unique_id_list(
            &self.architect_advice_ids,
            "architect_advice_ids",
            MAX_ARCHITECT_ADVICE_RECORDS_PER_BINDING,
        )?;
        validate_unique_text_list(
            &self.resolved_open_questions,
            "planning_result.resolved_open_questions",
            MAX_WORK_BRIEF_ITEMS,
        )?;
        validate_unique_text_list(
            &self.deferred_proposals,
            "planning_result.deferred_proposals",
            MAX_WORK_BRIEF_ITEMS,
        )?;
        Ok(())
    }
}

/// High-level lifecycle phase of a [`CoworkerJobRecord`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CoworkerJobPhase {
    /// Structuring or refining the [`WorkBrief`].
    Briefing,
    /// Running read-only orchestrator/architect planning under a [`PlanningAdviceGrant`].
    Planning,
    /// Plan published; waiting for explicit trusted-local human scope approval (`ApprovalRecord`).
    AwaitingApproval,
    /// Executing approved tasks, reviews, fixes, and integrations.
    Executing,
    /// Assembling and verifying the [`CombinedCandidateIdentity`].
    AssemblingCandidate,
    /// Reserved for or undergoing single-closure package review.
    ClosureReview,
    /// Job advancement is paused by a job-wide [`WorkflowHoldRecord`].
    Held,
    /// Job completed closure acceptance.
    Completed,
    /// Job was explicitly cancelled or superseded.
    Cancelled,
}

/// Monotonically versioned per-binding durable Coworker workflow job record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CoworkerJobRecord {
    /// Deterministic job identifier (`job_<32hex>`).
    pub job_id: String,
    /// 1-based monotonically increasing revision of this job.
    pub job_revision: u64,
    /// Idempotent caller request key that created this job.
    pub create_request_key: String,
    /// Control-plane chat binding identifier.
    pub binding_id: String,
    /// Canonical lowercase hyphenated non-nil native Codex thread UUID.
    pub thread_uuid: String,
    /// Current high-level job phase.
    pub phase: CoworkerJobPhase,
    /// Structured work brief for this job.
    pub work_brief: WorkBrief,
    /// 64-character lowercase hex SHA-256 digest of `work_brief`.
    pub work_brief_digest: String,
    /// Optional prior `job_id` amended/superseded by this job.
    pub supersedes_job_id: Option<String>,
    /// Optional durable linkage to the verified Orchestrator planning run and published plan revision.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub planning_result: Option<PlanningResultRef>,
    /// Immutable linkage to the operator [`ApprovalRecord`], once bound.
    pub approval_linkage: Option<FrozenApprovalLinkage>,
    /// Per-task state, attempts, candidates, dependencies, and holds.
    #[serde(default)]
    pub tasks: Vec<CoworkerTaskRecord>,
    /// Recorded workflow holds on this job.
    #[serde(default)]
    pub holds: Vec<WorkflowHoldRecord>,
    /// Reserve-before-effect outbox dispatch records for this job.
    #[serde(default)]
    pub outbox: Vec<OutboxDispatchRecord>,
    /// Combined final candidate identities recorded for this job.
    #[serde(default)]
    pub combined_candidates: Vec<CombinedCandidateIdentity>,
}

impl CoworkerJobRecord {
    /// Validates the job record, brief digest, planning result linkage, immutable approval linkage,
    /// tasks, holds, outbox, and combined candidates.
    pub fn validate(&self) -> Result<(), NativeError> {
        validate_bounded_identifier(&self.job_id, "job_id", MAX_COWORKER_ID_LEN)?;
        if self.job_revision == 0 {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "CoworkerJobRecord.job_revision must be >= 1",
            ));
        }
        validate_bounded_identifier(
            &self.create_request_key,
            "create_request_key",
            MAX_COWORKER_ID_LEN,
        )?;
        validate_bounded_identifier(&self.binding_id, "binding_id", MAX_COWORKER_ID_LEN)?;
        validate_canonical_thread_uuid("thread_uuid", &self.thread_uuid).map_err(|_| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                "thread_uuid must be a canonical lowercase hyphenated non-nil UUID",
            )
        })?;
        let computed_brief_digest = self.work_brief.digest()?;
        if computed_brief_digest != self.work_brief_digest {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "CoworkerJobRecord.work_brief_digest does not match work_brief.digest()",
            ));
        }
        if self.work_brief.binding_id != self.binding_id
            || self.work_brief.thread_uuid != self.thread_uuid
        {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "CoworkerJobRecord.work_brief scope does not match job binding_id and thread_uuid",
            ));
        }
        match (&self.work_brief.kind, &self.supersedes_job_id) {
            (WorkBriefKind::NewJob, None) => {}
            (WorkBriefKind::Amendment { target_job_id, .. }, Some(sup_id))
                if target_job_id == sup_id => {}
            _ => {
                return Err(NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "CoworkerJobRecord.supersedes_job_id must match WorkBriefKind::Amendment target_job_id",
                ));
            }
        }

        if let Some(ref planning_result) = self.planning_result {
            planning_result.validate()?;
            if planning_result.work_brief_digest != self.work_brief_digest {
                return Err(NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "PlanningResultRef.work_brief_digest does not match CoworkerJobRecord.work_brief_digest",
                ));
            }
            if planning_result.project_base != self.work_brief.project_base {
                return Err(NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "PlanningResultRef.project_base does not match WorkBrief.project_base",
                ));
            }
            if planning_result
                .resolved_open_questions
                .iter()
                .any(|rq| !self.work_brief.open_questions.iter().any(|q| q == rq))
            {
                return Err(NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "PlanningResultRef.resolved_open_questions contains an entry not present in WorkBrief.open_questions",
                ));
            }
            if planning_result
                .deferred_proposals
                .iter()
                .any(|dp| !self.work_brief.proposed_additions.iter().any(|p| p == dp))
            {
                return Err(NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "PlanningResultRef.deferred_proposals contains an entry not present in WorkBrief.proposed_additions",
                ));
            }
        }

        if let Some(ref linkage) = self.approval_linkage {
            linkage.validate()?;
            if linkage.project_base != self.work_brief.project_base {
                return Err(NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "FrozenApprovalLinkage.project_base does not match WorkBrief.project_base",
                ));
            }
            if let Some(ref planning_result) = self.planning_result {
                if linkage.plan_id != planning_result.plan_id
                    || linkage.plan_revision != planning_result.plan_revision
                    || linkage.plan_digest != planning_result.plan_digest
                    || linkage.profile_id != planning_result.profile_id
                    || linkage.registry_revision != planning_result.registry_revision
                    || linkage.project_base != planning_result.project_base
                {
                    return Err(NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "FrozenApprovalLinkage does not match the job's recorded PlanningResultRef plan id, revision, digest, profile, or base",
                    ));
                }
            }
        } else if !self.tasks.is_empty()
            || !self.outbox.is_empty()
            || !self.combined_candidates.is_empty()
        {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "tasks, outbox records, and combined candidates require a bound FrozenApprovalLinkage",
            ));
        }

        if self.tasks.len() > MAX_TASKS_PER_JOB
            || self.holds.len() > MAX_HOLDS_PER_JOB
            || self.outbox.len() > MAX_OUTBOX_RECORDS_PER_JOB
            || self.combined_candidates.len() > MAX_COMBINED_CANDIDATES_PER_JOB
        {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "CoworkerJobRecord collection exceeds maximum capacity bounds",
            ));
        }

        let mut seen_tasks = BTreeSet::new();
        for task in &self.tasks {
            validate_bounded_identifier(&task.task_id, "task_id", MAX_COWORKER_ID_LEN)?;
            if !seen_tasks.insert(task.task_id.as_str()) {
                return Err(NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "duplicate task_id in CoworkerJobRecord.tasks",
                ));
            }
            validate_sha256_hex_digest(&task.task_digest, "task.task_digest").map_err(|_| {
                NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "task.task_digest must be a 64-character lowercase hex SHA-256",
                )
            })?;
            validate_non_native_worker_choice(task.role, &task.selected_choice)?;
            if task.attempts.len() > MAX_ATTEMPTS_PER_TASK
                || task.candidates.len() > MAX_CANDIDATES_PER_TASK
            {
                return Err(NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "task attempts or candidates exceed maximum per-task bounds",
                ));
            }
            for cand in &task.candidates {
                cand.validate()?;
                if cand.task_id != task.task_id {
                    return Err(NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "TaskCandidateRef.task_id does not match owning task_id",
                    ));
                }
            }
            if let Some(ref accepted_id) = task.accepted_candidate_id {
                let accepted_cand = task
                    .candidates
                    .iter()
                    .find(|c| &c.candidate_id == accepted_id)
                    .ok_or_else(|| {
                        NativeError::new(
                            NativeErrorCode::ValidationFailed,
                            "accepted_candidate_id was not found in task.candidates",
                        )
                    })?;
                let verdict = accepted_cand.review_verdict.as_ref().ok_or_else(|| {
                    NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "accepted candidate requires a StructuredReviewVerdict",
                    )
                })?;
                if !verdict.allows_acceptance() || accepted_cand.review_receipt_id.is_none() {
                    return Err(NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "accepted candidate requires ReviewVerdictOutcome::Accept and a sealed review_receipt_id",
                    ));
                }
            }
        }

        let mut seen_outbox_ids = BTreeSet::new();
        let mut seen_outbox_keys = BTreeSet::new();
        for rec in &self.outbox {
            validate_bounded_identifier(&rec.outbox_id, "outbox_id", MAX_COWORKER_ID_LEN)?;
            validate_bounded_identifier(&rec.request_key, "request_key", MAX_COWORKER_ID_LEN)?;
            if !seen_outbox_ids.insert(rec.outbox_id.as_str())
                || !seen_outbox_keys.insert(rec.request_key.as_str())
            {
                return Err(NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "duplicate outbox_id or request_key in CoworkerJobRecord.outbox",
                ));
            }
            validate_sha256_hex_digest(&rec.subject_digest, "outbox.subject_digest").map_err(
                |_| {
                    NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "outbox.subject_digest must be a 64-character lowercase hex SHA-256",
                    )
                },
            )?;
        }

        for comb in &self.combined_candidates {
            comb.validate()?;
            if comb.job_id != self.job_id {
                return Err(NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "CombinedCandidateIdentity.job_id does not match owning job_id",
                ));
            }
        }

        Ok(())
    }
}

pub(crate) fn validate_bounded_text(raw: &str, field: &'static str) -> Result<(), NativeError> {
    let trimmed = raw.trim();
    if trimmed.is_empty()
        || raw.len() > MAX_COWORKER_TEXT_BYTES
        || raw
            .chars()
            .any(|c| c.is_control() && c != '\n' && c != '\t')
    {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!("{field} must be non-empty, <= {MAX_COWORKER_TEXT_BYTES} bytes, and free of control characters"),
        ));
    }
    Ok(())
}

pub(crate) fn validate_unique_text_list(
    items: &[String],
    field: &'static str,
    max_items: usize,
) -> Result<(), NativeError> {
    if items.len() > max_items {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!("{field} exceeds maximum of {max_items} entries"),
        ));
    }
    let mut seen = BTreeSet::new();
    for item in items {
        validate_bounded_text(item, field)?;
        if !seen.insert(item.as_str()) {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!("{field} contains duplicate entries"),
            ));
        }
    }
    Ok(())
}

pub(crate) fn validate_unique_id_list(
    items: &[String],
    field: &'static str,
    max_items: usize,
) -> Result<(), NativeError> {
    if items.len() > max_items {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!("{field} exceeds maximum of {max_items} entries"),
        ));
    }
    let mut seen = BTreeSet::new();
    for item in items {
        validate_bounded_identifier(item, field, MAX_COWORKER_ID_LEN)?;
        if !seen.insert(item.as_str()) {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!("{field} contains duplicate identifiers"),
            ));
        }
    }
    Ok(())
}

/// Computes the lowercase 64-character hexadecimal SHA-256 digest of `bytes`.
pub fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut out = String::with_capacity(64);
    for b in digest {
        let _ = write!(&mut out, "{b:02x}");
    }
    out
}

/// Computes a deterministic `<prefix>_<32hex>` identifier from a domain separator and ordered string parts.
pub fn deterministic_prefixed_id(domain: &str, prefix: &str, parts: &[&str]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(domain.as_bytes());
    hasher.update(b"\0");
    hasher.update((parts.len() as u64).to_le_bytes());
    for part in parts {
        hasher.update((part.len() as u64).to_le_bytes());
        hasher.update(part.as_bytes());
    }
    let digest = hasher.finalize();
    let mut hex = String::with_capacity(32);
    for b in &digest[..16] {
        let _ = write!(&mut hex, "{b:02x}");
    }
    format!("{prefix}_{hex}")
}

/// Validates a repository-relative write scope against the canonical repository and base commit.
pub fn validate_write_scope_at_project_base(
    project_base: &CanonicalProjectBaseRef,
    entry: &str,
    label: &str,
) -> Result<String, NativeError> {
    validate_write_scope_entry_at_base_commit(
        entry,
        Path::new(&project_base.repository_path),
        &project_base.base_commit,
        label,
    )
    .map_err(|err| {
        NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!("invalid write scope `{entry}` ({})", err.code()),
        )
    })
}
