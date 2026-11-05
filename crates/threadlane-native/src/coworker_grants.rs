//! Trusted-local operator helpers for preparing, previewing, recording, inspecting, and revoking
//! standing read-only [`PlanningAdviceGrant`] records.
//!
//! ## Core Invariants
//!
//! - **Pure preview (`prepare_planning_grant_preview`)**: Validates binding/thread scope, canonical
//!   live Git repository and base commit/tree (via bounded read-only Git verification), explicit
//!   workflow profile and registry revision, supported headless worker routes (`Antigravity` or
//!   `OpencodeV2`) for [`WorkflowRole::Orchestrator`] and [`WorkflowRole::Architect`], strict
//!   read-only capabilities, finite call and per-call budgets, and meaningful expiry with no
//!   provider, host, vault, network, or state-mutation side effects (never creating directories,
//!   files, lockfiles, or call reservations).
//! - **Honest candidate labeling**: A prepared [`PlanningGrantPreview`] is explicitly labeled
//!   [`PlanningGrantPreviewStatus::ProposedCandidateNotGranted`] with `grant_active: false` and
//!   `capabilities.code_execution_approval_granted: false`.
//! - **Digest-bound recording (`record_planning_grant`)**: Recomputes the canonical SHA-256
//!   `preview_digest`, verifies constant-time equality with `confirmed_preview_digest`, re-validates
//!   the live [`RegistrySnapshot`] and live Git repository, requires explicit trusted-local
//!   [`LocalOperatorGrantAttestation`] (`operator_attested = true`, never model/MCP prose, and
//!   `native_per_call_identity_verified = false`), and persists through
//!   [`CoworkerWorkflowStore::create_planning_grant`].
//! - **Scoped revocation (`revoke_planning_grant_scoped`)**: Enforces exact pinned
//!   [`TrustedGrantScope`] (`binding_id` and `thread_uuid`), rejects cross-binding or cross-thread
//!   caller metadata, requires an explicit matching `expected_ledger_revision`, and validates a
//!   bounded reason before calling [`CoworkerWorkflowStore::revoke_planning_grant`].

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use threadlane_authority::validation::validate_canonical_thread_uuid;
use threadlane_workers::{resolve_model_target, validate_sha256_hex_digest, WorkerProvider};
use threadlane_workflow::{Difficulty, RegistrySnapshot, ToolAdapter, WorkflowRole};

use crate::coworker_workflow::contracts::{
    sha256_hex, validate_bounded_text, validate_non_native_worker_choice, CanonicalProjectBaseRef,
    PlanningAdviceGrant, PlanningCallBudget, PlanningGrantStatus, PlanningGrantUsage,
    PlanningReadOnlyCapabilities, MAX_COWORKER_ID_LEN, MAX_PLANNING_GRANTS_PER_BINDING,
};
use crate::coworker_workflow::store::{
    CoworkerWorkflowStore, CreatePlanningGrantRequest, RevokePlanningGrantRequest,
};
use crate::error::{NativeError, NativeErrorCode};
use crate::fs_safety::{constant_time_str_eq, validate_bounded_identifier};
use crate::profile_flow::{
    normalize_read_only_advisory_choice, preflight, ExecutionPolicy, PreflightRequest,
    RoleExecutionRoute, RolePreflightRequest, SelectedRoleChoice,
};
use crate::role_flow::read_registry_snapshot_readonly;

/// Canonical domain separator for [`PlanningGrantPreview::preview_digest`].
pub const PLANNING_GRANT_PREVIEW_DOMAIN: &str =
    "threadlane-native:coworker_planning_grant_preview:v1";

/// Pinned trusted-local binding and native thread scope for planning grant operations.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TrustedGrantScope {
    /// Control-plane chat binding identifier.
    pub binding_id: String,
    /// Canonical lowercase hyphenated non-nil native Codex thread UUID.
    pub thread_uuid: String,
}

impl TrustedGrantScope {
    /// Constructs and validates a [`TrustedGrantScope`].
    pub fn new(
        binding_id: impl Into<String>,
        thread_uuid: impl Into<String>,
    ) -> Result<Self, NativeError> {
        let scope = Self {
            binding_id: binding_id.into(),
            thread_uuid: thread_uuid.into(),
        };
        scope.validate()?;
        Ok(scope)
    }

    /// Validates `binding_id` and canonical `thread_uuid`.
    pub fn validate(&self) -> Result<(), NativeError> {
        validate_bounded_identifier(&self.binding_id, "binding_id", MAX_COWORKER_ID_LEN)?;
        validate_canonical_thread_uuid("thread_uuid", &self.thread_uuid).map_err(|_| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                "thread_uuid must be a canonical lowercase hyphenated non-nil UUID",
            )
        })?;
        Ok(())
    }
}

/// Honest authority state of a prepared [`PlanningGrantPreview`] prior to recording.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlanningGrantPreviewStatus {
    /// Validated candidate preview only; no standing grant or execution permission has been granted yet.
    ProposedCandidateNotGranted,
}

/// Concrete frozen headless worker model and effort resolved for a planning or architectural role.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FrozenPlanningRoleChoice {
    /// Planning or architectural advice role (`Orchestrator` or `Architect`).
    pub role: WorkflowRole,
    /// Difficulty tier requested for role resolution (`None` when resolved via explicit role default).
    pub requested_difficulty: Option<Difficulty>,
    /// Exact resolved headless worker choice (`Antigravity` or `OpencodeV2`).
    pub selected_choice: SelectedRoleChoice,
}

type FrozenRoleChoiceSortKey<'a> = (
    WorkflowRole,
    Option<Difficulty>,
    &'a str,
    u8,
    RoleExecutionRoute,
    &'a str,
    &'a str,
    Option<&'a str>,
    Option<Difficulty>,
);

impl FrozenPlanningRoleChoice {
    /// Validates that this frozen role choice is a supported headless worker for `Orchestrator` or `Architect`.
    pub fn validate(&self) -> Result<(), NativeError> {
        if !matches!(
            self.role,
            WorkflowRole::Orchestrator | WorkflowRole::Architect
        ) {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "planning grant role must be WorkflowRole::Orchestrator or WorkflowRole::Architect",
            ));
        }
        validate_non_native_worker_choice(self.role, &self.selected_choice)
    }

    fn sort_key(&self) -> FrozenRoleChoiceSortKey<'_> {
        (
            self.role,
            self.requested_difficulty,
            self.selected_choice.tool_id.as_str(),
            adapter_sort_rank(self.selected_choice.adapter),
            self.selected_choice.route,
            self.selected_choice.model_id.as_str(),
            self.selected_choice.model_arg.as_str(),
            self.selected_choice.effort.as_deref(),
            self.selected_choice.matched_difficulty,
        )
    }
}

/// Pure request to prepare and preview a standing read-only [`PlanningAdviceGrant`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreparePlanningGrantRequest {
    /// Idempotent caller request key bound to this grant preview and creation.
    pub request_key: String,
    /// Control-plane chat binding identifier.
    pub binding_id: String,
    /// Canonical lowercase hyphenated non-nil native Codex thread UUID.
    pub thread_uuid: String,
    /// Absolute path to the target Git repository.
    pub repository_path: PathBuf,
    /// Exact 40-character lowercase hex base commit SHA in `repository_path`.
    pub base_commit: String,
    /// Optional expected 40-character lowercase hex Git tree SHA for `base_commit`.
    #[serde(default)]
    pub expected_base_tree_sha: Option<String>,
    /// Explicit workflow profile identifier (never inferred from a hidden default).
    pub profile_id: String,
    /// Optional expected current registry snapshot revision (`>= 1`).
    #[serde(default)]
    pub expected_registry_revision: Option<u64>,
    /// Explicit non-empty list of `(role, difficulty)` combinations for `Orchestrator` and/or `Architect`.
    pub roles: Vec<RolePreflightRequest>,
    /// Explicit deny-by-default execution policy restricted to `Orchestrator` and/or `Architect`.
    pub policy: ExecutionPolicy,
    /// Strictly read-only capability envelope.
    pub capabilities: PlanningReadOnlyCapabilities,
    /// Finite call and per-call execution budget.
    pub budget: PlanningCallBudget,
    /// Creation timestamp (ms since Unix epoch; must be `> 0`).
    pub created_at_ms: u64,
    /// Expiry timestamp (ms since Unix epoch; must be `> created_at_ms`).
    pub expires_at_ms: u64,
}

impl PreparePlanningGrantRequest {
    /// Returns the [`TrustedGrantScope`] for this request after validating identifiers.
    pub fn scope(&self) -> Result<TrustedGrantScope, NativeError> {
        TrustedGrantScope::new(self.binding_id.clone(), self.thread_uuid.clone())
    }
}

/// Validated, side-effect-free preview of a proposed standing read-only [`PlanningAdviceGrant`].
///
/// Labeled [`PlanningGrantPreviewStatus::ProposedCandidateNotGranted`] with `grant_active: false`
/// so a preview is never mistaken for an already recorded permission or code execution approval.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlanningGrantPreview {
    /// Honest preview status (`ProposedCandidateNotGranted`).
    pub status: PlanningGrantPreviewStatus,
    /// Always `false`: previewing a candidate never activates or records a grant.
    pub grant_active: bool,
    /// Idempotent caller request key bound into the preview digest and grant creation.
    pub request_key: String,
    /// Control-plane chat binding identifier.
    pub binding_id: String,
    /// Canonical lowercase hyphenated non-nil native Codex thread UUID.
    pub thread_uuid: String,
    /// Canonical Git repository path, base commit SHA, and resolved base tree SHA.
    pub project_base: CanonicalProjectBaseRef,
    /// Explicit pinned workflow profile identifier.
    pub profile_id: String,
    /// Pinned workflow registry revision (`>= 1`).
    pub registry_revision: u64,
    /// Validated deny-by-default execution policy restricted to `Orchestrator` and/or `Architect`.
    pub policy: ExecutionPolicy,
    /// 64-character lowercase hex SHA-256 digest of `policy`.
    pub policy_digest: String,
    /// Concrete frozen headless worker choices resolved for each requested planning/advisory role.
    pub frozen_role_choices: Vec<FrozenPlanningRoleChoice>,
    /// Strictly read-only capability envelope.
    pub capabilities: PlanningReadOnlyCapabilities,
    /// Finite call and per-call execution budget.
    pub budget: PlanningCallBudget,
    /// Creation timestamp (ms since Unix epoch).
    pub created_at_ms: u64,
    /// Expiry timestamp (ms since Unix epoch; strictly greater than `created_at_ms`).
    pub expires_at_ms: u64,
    /// Canonical 64-character lowercase hex SHA-256 digest binding all authority-relevant fields.
    pub preview_digest: String,
}

impl PlanningGrantPreview {
    /// Validates all fields of the preview and computes the canonical 64-character lowercase hex
    /// SHA-256 `preview_digest`.
    pub fn compute_digest(&self) -> Result<String, NativeError> {
        if self.status != PlanningGrantPreviewStatus::ProposedCandidateNotGranted
            || self.grant_active
        {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "PlanningGrantPreview must remain ProposedCandidateNotGranted with grant_active = false",
            ));
        }
        let scope = TrustedGrantScope::new(&self.binding_id, &self.thread_uuid)?;
        validate_bounded_identifier(&self.request_key, "request_key", MAX_COWORKER_ID_LEN)?;
        self.project_base.validate_syntax()?;
        validate_bounded_identifier(&self.profile_id, "profile_id", MAX_COWORKER_ID_LEN)?;
        if self.registry_revision == 0 {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "PlanningGrantPreview.registry_revision must be >= 1",
            ));
        }
        let computed_policy_digest = self.policy.digest()?;
        if !constant_time_str_eq(&computed_policy_digest, &self.policy_digest) {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "PlanningGrantPreview.policy_digest does not match policy.digest()",
            ));
        }
        validate_planning_policy_roles_and_budget(&self.policy, &self.budget)?;
        self.capabilities.validate()?;
        self.budget.validate()?;
        if self.created_at_ms == 0 || self.expires_at_ms <= self.created_at_ms {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "PlanningGrantPreview requires created_at_ms > 0 and expires_at_ms > created_at_ms",
            ));
        }
        if self.frozen_role_choices.is_empty() {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "PlanningGrantPreview.frozen_role_choices must not be empty",
            ));
        }

        let mut seen_roles = BTreeSet::new();
        let mut canonical_choices = self.frozen_role_choices.clone();
        for item in &canonical_choices {
            item.validate()?;
            if !seen_roles.insert((item.role, item.requested_difficulty)) {
                return Err(NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "PlanningGrantPreview.frozen_role_choices contains duplicate (role, difficulty) entries",
                ));
            }
            if !self.policy.allows(
                item.role,
                &item.selected_choice.tool_id,
                &item.selected_choice.model_id,
                item.selected_choice.effort.as_deref(),
            )? {
                return Err(NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "PlanningGrantPreview frozen role choice is not permitted by policy",
                ));
            }
        }
        sort_frozen_role_choices(&mut canonical_choices);
        validate_role_presence_matches_budget(&canonical_choices, &self.budget)?;
        validate_policy_bound_to_frozen_choices(&self.policy, &canonical_choices)?;

        let seed = (
            PLANNING_GRANT_PREVIEW_DOMAIN,
            &self.request_key,
            &scope.binding_id,
            &scope.thread_uuid,
            &self.project_base,
            &self.profile_id,
            self.registry_revision,
            &self.policy_digest,
            &self.policy,
            &canonical_choices,
            &self.capabilities,
            &self.budget,
            self.created_at_ms,
            self.expires_at_ms,
        );
        let bytes = serde_json::to_vec(&seed).map_err(|_| {
            NativeError::new(
                NativeErrorCode::ProtocolError,
                "failed to serialize PlanningGrantPreview for digest calculation",
            )
        })?;
        Ok(sha256_hex(&bytes))
    }

    /// Validates all preview fields and verifies that `self.preview_digest` matches [`Self::compute_digest`].
    pub fn validate(&self) -> Result<(), NativeError> {
        validate_sha256_hex_digest(&self.preview_digest, "preview_digest").map_err(|_| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                "preview_digest must be a 64-character lowercase hex SHA-256",
            )
        })?;
        let computed = self.compute_digest()?;
        if !constant_time_str_eq(&computed, &self.preview_digest) {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "PlanningGrantPreview.preview_digest does not match recomputed digest",
            ));
        }
        Ok(())
    }
}

/// Honest trusted-local operator attestation confirming explicit human approval of a prepared
/// [`PlanningGrantPreview`].
///
/// Represents trusted-local operator confirmation (`operator_attested = true`), never cryptographic
/// human provenance, never per-call native identity verification, and never model/MCP self-approval.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LocalOperatorGrantAttestation {
    /// Must be `true`: confirms explicit local operator/human approval of the exact preview.
    pub operator_attested: bool,
    /// Must be `false`: machine/provider prose or MCP tool output is never authorization proof.
    #[serde(default)]
    pub derived_from_model_or_mcp_prose: bool,
    /// Must be `false`: local grant recording never claims per-call verified native identity.
    #[serde(default)]
    pub native_per_call_identity_verified: bool,
    /// Optional bounded operator confirmation note.
    #[serde(default)]
    pub note: Option<String>,
}

impl LocalOperatorGrantAttestation {
    /// Constructs a valid trusted-local operator attestation (`operator_attested = true`).
    pub const fn attested() -> Self {
        Self {
            operator_attested: true,
            derived_from_model_or_mcp_prose: false,
            native_per_call_identity_verified: false,
            note: None,
        }
    }

    /// Constructs an unattested record (`operator_attested = false`).
    pub const fn unattested() -> Self {
        Self {
            operator_attested: false,
            derived_from_model_or_mcp_prose: false,
            native_per_call_identity_verified: false,
            note: None,
        }
    }

    /// Validates that explicit local operator attestation is present and honest.
    pub fn validate(&self) -> Result<(), NativeError> {
        if !self.operator_attested {
            return Err(NativeError::new(
                NativeErrorCode::UnauthorizedOperator,
                "planning grant recording requires explicit trusted-local operator_attested = true",
            ));
        }
        if self.derived_from_model_or_mcp_prose {
            return Err(NativeError::new(
                NativeErrorCode::UnauthorizedOperator,
                "model/provider prose or MCP calls cannot self-approve a planning grant",
            ));
        }
        if self.native_per_call_identity_verified {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "trusted-local planning grant attestation must keep native_per_call_identity_verified = false",
            ));
        }
        if let Some(ref note) = self.note {
            validate_bounded_text(note, "attestation.note")?;
        }
        Ok(())
    }
}

/// Request to record a prepared [`PlanningGrantPreview`] as a standing [`PlanningAdviceGrant`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecordPlanningGrantRequest {
    /// Idempotent caller request key for grant creation.
    pub request_key: String,
    /// Optional expected current ledger revision (CAS check).
    #[serde(default)]
    pub expected_ledger_revision: Option<u64>,
    /// Prepared grant preview candidate to record.
    pub preview: PlanningGrantPreview,
    /// Exact 64-character lowercase hex SHA-256 `preview_digest` confirmed by the operator.
    pub confirmed_preview_digest: String,
    /// Explicit trusted-local operator attestation.
    pub attestation: LocalOperatorGrantAttestation,
}

/// Redacted, metadata-only summary of a persisted [`PlanningAdviceGrant`].
///
/// Never embeds secrets, raw chat transcripts, or provider prompt payloads.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlanningGrantSummary {
    /// Deterministic grant identifier (`pgr_<32hex>`).
    pub grant_id: String,
    /// Idempotent request key that created this grant.
    pub create_request_key: String,
    /// Control-plane chat binding identifier.
    pub binding_id: String,
    /// Canonical lowercase hyphenated non-nil native Codex thread UUID.
    pub thread_uuid: String,
    /// Monotonic per-binding coworker workflow ledger revision (`>= 1`).
    pub ledger_revision: u64,
    /// Canonical repository path and origin base commit/tree metadata.
    pub project_base: CanonicalProjectBaseRef,
    /// Pinned workflow profile identifier.
    pub profile_id: String,
    /// Pinned workflow registry revision.
    pub registry_revision: u64,
    /// Pinned execution policy identifier.
    pub policy_id: String,
    /// 64-character lowercase hex SHA-256 digest of the pinned [`ExecutionPolicy`].
    pub policy_digest: String,
    /// Strictly read-only capability envelope.
    pub capabilities: PlanningReadOnlyCapabilities,
    /// Finite call and per-call execution budget.
    pub budget: PlanningCallBudget,
    /// Current durable call usage counters.
    pub usage: PlanningGrantUsage,
    /// Current lifecycle status (`Active`, `Exhausted`, `Expired`, or `Revoked`).
    pub status: PlanningGrantStatus,
    /// Creation timestamp (ms since Unix epoch).
    pub created_at_ms: u64,
    /// Expiry timestamp (ms since Unix epoch).
    pub expires_at_ms: u64,
    /// Optional revocation timestamp (ms since Unix epoch).
    pub revoked_at_ms: Option<u64>,
    /// Optional bounded revocation reason.
    pub revoked_reason: Option<String>,
    /// Always `true` on persisted grants: trusted-local operator attestation was supplied.
    pub operator_attested: bool,
    /// Always `false`: local grant records never claim verified per-call native identity.
    pub native_per_call_identity_verified: bool,
    /// Number of durable call reservations recorded under this grant.
    pub reservation_count: usize,
}

impl PlanningGrantSummary {
    /// Builds a metadata-only [`PlanningGrantSummary`] from a [`PlanningAdviceGrant`] and its
    /// owning `ledger_revision`.
    pub fn from_grant(grant: &PlanningAdviceGrant, ledger_revision: u64) -> Self {
        Self {
            grant_id: grant.grant_id.clone(),
            create_request_key: grant.create_request_key.clone(),
            binding_id: grant.binding_id.clone(),
            thread_uuid: grant.thread_uuid.clone(),
            ledger_revision,
            project_base: grant.project_base.clone(),
            profile_id: grant.profile_id.clone(),
            registry_revision: grant.registry_revision,
            policy_id: grant.policy.policy_id.clone(),
            policy_digest: grant.policy_digest.clone(),
            capabilities: grant.capabilities.clone(),
            budget: grant.budget.clone(),
            usage: grant.usage.clone(),
            status: grant.status,
            created_at_ms: grant.created_at_ms,
            expires_at_ms: grant.expires_at_ms,
            revoked_at_ms: grant.revoked_at_ms,
            revoked_reason: grant.revoked_reason.clone(),
            operator_attested: grant.operator_attested,
            native_per_call_identity_verified: grant.native_per_call_identity_verified,
            reservation_count: grant.reservations.len(),
        }
    }
}

/// Pure, side-effect-free preparation and validation of a [`PlanningGrantPreview`] against a
/// point-in-time [`RegistrySnapshot`] and live Git repository.
///
/// Bounded read-only Git verification of the canonical repository and base commit/tree is
/// permitted; never creates directories, files, or lockfiles, never produces provider, host,
/// vault, network, or state-mutation side effects, and never reserves or consumes a provider call.
pub fn prepare_planning_grant_preview(
    snapshot: &RegistrySnapshot,
    request: &PreparePlanningGrantRequest,
) -> Result<PlanningGrantPreview, NativeError> {
    let scope = request.scope()?;
    validate_bounded_identifier(&request.request_key, "request_key", MAX_COWORKER_ID_LEN)?;
    validate_bounded_identifier(&request.profile_id, "profile_id", MAX_COWORKER_ID_LEN)?;

    if snapshot.revision == 0 {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "registry snapshot revision must be >= 1 for a planning grant",
        ));
    }
    if let Some(expected_rev) = request.expected_registry_revision {
        if snapshot.revision != expected_rev {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!(
                    "expected_registry_revision {expected_rev} does not match current registry revision {}",
                    snapshot.revision
                ),
            ));
        }
    }

    let project_base = CanonicalProjectBaseRef::resolve_and_validate(
        &request.repository_path,
        &request.base_commit,
    )?;
    if let Some(ref expected_tree) = request.expected_base_tree_sha {
        if &project_base.base_tree_sha != expected_tree {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "expected_base_tree_sha does not match resolved live Git tree SHA for base_commit",
            ));
        }
    }

    request.capabilities.validate()?;
    request.budget.validate()?;
    if request.created_at_ms == 0 || request.expires_at_ms <= request.created_at_ms {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "planning grant requires created_at_ms > 0 and expires_at_ms > created_at_ms",
        ));
    }

    if request.roles.is_empty() {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "PreparePlanningGrantRequest.roles must contain at least one planning role",
        ));
    }
    for entry in &request.roles {
        if !matches!(
            entry.role,
            WorkflowRole::Orchestrator | WorkflowRole::Architect
        ) {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!(
                    "role `{}` is not permitted in a PlanningAdviceGrant (only Orchestrator and Architect are allowed)",
                    entry.role
                ),
            ));
        }
    }

    let policy_digest = request.policy.digest()?;
    validate_planning_policy_roles_and_budget(&request.policy, &request.budget)?;
    validate_policy_rules_against_snapshot(snapshot, &request.policy)?;

    let preflight_req = PreflightRequest {
        profile_id: Some(request.profile_id.clone()),
        roles: request.roles.clone(),
    };
    let report = preflight(snapshot, &preflight_req, &request.policy)?;
    if report.used_default_profile
        || report.profile_id.as_deref() != Some(request.profile_id.as_str())
    {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "planning grant preparation requires an explicit workflow profile without default substitution",
        ));
    }

    let mut frozen_role_choices = Vec::with_capacity(report.roles.len());
    for outcome in report.roles {
        if !outcome.ready {
            let reason = outcome
                .block_reason
                .map(|r| r.as_str())
                .unwrap_or("blocked");
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!(
                    "planning grant role `{}` failed preflight ({reason}): {}",
                    outcome.role, outcome.detail
                ),
            ));
        }
        let raw_choice = outcome.selected.ok_or_else(|| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                "ready preflight outcome is missing selected role choice",
            )
        })?;
        let selected_choice = normalize_read_only_advisory_choice(outcome.role, &raw_choice)?;
        validate_non_native_worker_choice(outcome.role, &selected_choice)?;
        frozen_role_choices.push(FrozenPlanningRoleChoice {
            role: outcome.role,
            requested_difficulty: outcome.requested_difficulty,
            selected_choice,
        });
    }
    sort_frozen_role_choices(&mut frozen_role_choices);
    validate_role_presence_matches_budget(&frozen_role_choices, &request.budget)?;
    validate_policy_bound_to_frozen_choices(&request.policy, &frozen_role_choices)?;

    let mut preview = PlanningGrantPreview {
        status: PlanningGrantPreviewStatus::ProposedCandidateNotGranted,
        grant_active: false,
        request_key: request.request_key.clone(),
        binding_id: scope.binding_id,
        thread_uuid: scope.thread_uuid,
        project_base,
        profile_id: request.profile_id.clone(),
        registry_revision: snapshot.revision,
        policy: request.policy.clone(),
        policy_digest,
        frozen_role_choices,
        capabilities: request.capabilities.clone(),
        budget: request.budget.clone(),
        created_at_ms: request.created_at_ms,
        expires_at_ms: request.expires_at_ms,
        preview_digest: String::new(),
    };
    preview.preview_digest = preview.compute_digest()?;
    Ok(preview)
}

/// Pure, side-effect-free preparation of a [`PlanningGrantPreview`] that also verifies read-only
/// thread continuity and grant capacity against `store` without creating any file or lockfile.
pub fn prepare_planning_grant_preview_with_store(
    store: &CoworkerWorkflowStore,
    snapshot: &RegistrySnapshot,
    request: &PreparePlanningGrantRequest,
) -> Result<PlanningGrantPreview, NativeError> {
    let preview = prepare_planning_grant_preview(snapshot, request)?;
    let ledger = store.read_binding_ledger(&preview.binding_id)?;
    if let Some(ref pinned_thread) = ledger.thread_uuid {
        if pinned_thread != &preview.thread_uuid {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "thread_uuid does not match the thread pinned to this binding ledger",
            ));
        }
    }
    if ledger.planning_grants.len() >= MAX_PLANNING_GRANTS_PER_BINDING {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "maximum planning grants reached for this binding",
        ));
    }
    Ok(preview)
}

/// Pure, side-effect-free preparation of a [`PlanningGrantPreview`] reading the registry snapshot
/// from an existing private (`0700`) `registry_dir` without creating `registry.lock`.
pub fn prepare_planning_grant_preview_from_dir_readonly(
    registry_dir: impl AsRef<Path>,
    request: &PreparePlanningGrantRequest,
) -> Result<PlanningGrantPreview, NativeError> {
    let snapshot = read_registry_snapshot_readonly(registry_dir)?;
    prepare_planning_grant_preview(&snapshot, request)
}

/// Verifies `confirmed_preview_digest`, explicit local operator attestation, and current live
/// registry/repository state before recording a [`PlanningAdviceGrant`] in `store`.
pub fn record_planning_grant(
    store: &CoworkerWorkflowStore,
    snapshot: &RegistrySnapshot,
    request: &RecordPlanningGrantRequest,
) -> Result<PlanningGrantSummary, NativeError> {
    validate_bounded_identifier(&request.request_key, "request_key", MAX_COWORKER_ID_LEN)?;
    request.attestation.validate()?;
    validate_sha256_hex_digest(
        &request.confirmed_preview_digest,
        "confirmed_preview_digest",
    )
    .map_err(|_| {
        NativeError::new(
            NativeErrorCode::ValidationFailed,
            "confirmed_preview_digest must be a 64-character lowercase hex SHA-256",
        )
    })?;
    if request.request_key != request.preview.request_key {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "RecordPlanningGrantRequest.request_key does not match confirmed PlanningGrantPreview.request_key",
        ));
    }

    let recomputed_from_candidate = request.preview.compute_digest()?;
    if !constant_time_str_eq(
        &request.preview.preview_digest,
        &request.confirmed_preview_digest,
    ) || !constant_time_str_eq(
        &recomputed_from_candidate,
        &request.confirmed_preview_digest,
    ) {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "confirmed_preview_digest does not match recomputed PlanningGrantPreview digest",
        ));
    }

    let roles: Vec<RolePreflightRequest> = request
        .preview
        .frozen_role_choices
        .iter()
        .map(|choice| RolePreflightRequest {
            role: choice.role,
            difficulty: choice.requested_difficulty,
        })
        .collect();

    let live_prepare_req = PreparePlanningGrantRequest {
        request_key: request.preview.request_key.clone(),
        binding_id: request.preview.binding_id.clone(),
        thread_uuid: request.preview.thread_uuid.clone(),
        repository_path: PathBuf::from(&request.preview.project_base.repository_path),
        base_commit: request.preview.project_base.base_commit.clone(),
        expected_base_tree_sha: Some(request.preview.project_base.base_tree_sha.clone()),
        profile_id: request.preview.profile_id.clone(),
        expected_registry_revision: Some(request.preview.registry_revision),
        roles,
        policy: request.preview.policy.clone(),
        capabilities: request.preview.capabilities.clone(),
        budget: request.preview.budget.clone(),
        created_at_ms: request.preview.created_at_ms,
        expires_at_ms: request.preview.expires_at_ms,
    };

    let live_preview = prepare_planning_grant_preview(snapshot, &live_prepare_req)?;
    if live_preview != request.preview
        || !constant_time_str_eq(
            &live_preview.preview_digest,
            &request.confirmed_preview_digest,
        )
    {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "live registry or repository state drifted from the confirmed PlanningGrantPreview",
        ));
    }

    let create_req = CreatePlanningGrantRequest {
        binding_id: live_preview.binding_id,
        thread_uuid: live_preview.thread_uuid,
        request_key: live_preview.request_key,
        expected_ledger_revision: request.expected_ledger_revision,
        project_base: live_preview.project_base,
        profile_id: live_preview.profile_id,
        registry_revision: live_preview.registry_revision,
        policy: live_preview.policy,
        capabilities: live_preview.capabilities,
        budget: live_preview.budget,
        created_at_ms: live_preview.created_at_ms,
        expires_at_ms: live_preview.expires_at_ms,
        operator_attested: true,
    };

    let grant = store.create_planning_grant(&create_req)?;
    let ledger = store.read_binding_ledger(&grant.binding_id)?;
    Ok(PlanningGrantSummary::from_grant(
        &grant,
        ledger.ledger_revision,
    ))
}

/// Records a [`PlanningGrantPreview`] in `store` after reading the live [`RegistrySnapshot`] from
/// an existing private (`0700`) `registry_dir` without creating `registry.lock`.
pub fn record_planning_grant_from_registry_dir(
    store: &CoworkerWorkflowStore,
    registry_dir: impl AsRef<Path>,
    request: &RecordPlanningGrantRequest,
) -> Result<PlanningGrantSummary, NativeError> {
    let snapshot = read_registry_snapshot_readonly(registry_dir)?;
    record_planning_grant(store, &snapshot, request)
}

/// Revokes a standing [`PlanningAdviceGrant`] after enforcing exact pinned [`TrustedGrantScope`],
/// expected ledger revision, grant ownership, and bounded reason.
///
/// Rejects any attempt to revoke another binding's grant via caller-supplied metadata without
/// touching the other binding's ledger.
pub fn revoke_planning_grant_scoped(
    store: &CoworkerWorkflowStore,
    scope: &TrustedGrantScope,
    request: &RevokePlanningGrantRequest,
) -> Result<PlanningGrantSummary, NativeError> {
    scope.validate()?;
    validate_bounded_identifier(&request.binding_id, "binding_id", MAX_COWORKER_ID_LEN)?;
    validate_canonical_thread_uuid("thread_uuid", &request.thread_uuid).map_err(|_| {
        NativeError::new(
            NativeErrorCode::ValidationFailed,
            "thread_uuid must be a canonical lowercase hyphenated non-nil UUID",
        )
    })?;
    if request.binding_id != scope.binding_id || request.thread_uuid != scope.thread_uuid {
        return Err(NativeError::new(
            NativeErrorCode::RebindRejected,
            "RevokePlanningGrantRequest binding_id or thread_uuid does not match pinned TrustedGrantScope",
        ));
    }

    validate_bounded_identifier(&request.grant_id, "grant_id", MAX_COWORKER_ID_LEN)?;
    validate_bounded_text(&request.reason, "reason")?;
    if request.revoked_at_ms == 0 {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "RevokePlanningGrantRequest.revoked_at_ms must be > 0",
        ));
    }
    let expected_rev = request.expected_ledger_revision.ok_or_else(|| {
        NativeError::new(
            NativeErrorCode::ValidationFailed,
            "revoke_planning_grant_scoped requires an explicit expected_ledger_revision",
        )
    })?;
    if expected_rev == 0 {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "expected_ledger_revision for grant revocation must be >= 1",
        ));
    }

    let ledger = store.read_binding_ledger(&scope.binding_id)?;
    if ledger.thread_uuid.as_deref() != Some(scope.thread_uuid.as_str()) {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "pinned binding ledger does not match TrustedGrantScope.thread_uuid",
        ));
    }
    let existing_grant = ledger
        .planning_grants
        .iter()
        .find(|g| g.grant_id == request.grant_id)
        .ok_or_else(|| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                "requested grant_id was not found in the pinned binding ledger",
            )
        })?;
    if existing_grant.binding_id != scope.binding_id
        || existing_grant.thread_uuid != scope.thread_uuid
    {
        return Err(NativeError::new(
            NativeErrorCode::RebindRejected,
            "target grant scope does not match pinned TrustedGrantScope",
        ));
    }
    if request.revoked_at_ms < existing_grant.created_at_ms {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "revoked_at_ms must not precede grant created_at_ms",
        ));
    }
    let is_idempotent_replay = existing_grant.status == PlanningGrantStatus::Revoked
        && existing_grant.revoked_reason.as_deref() == Some(request.reason.as_str());
    if !is_idempotent_replay && ledger.ledger_revision != expected_rev {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!(
                "expected_ledger_revision {expected_rev} does not match current coworker workflow ledger_revision {}",
                ledger.ledger_revision
            ),
        ));
    }

    let revoked = store.revoke_planning_grant(request)?;
    let updated_ledger = store.read_binding_ledger(&scope.binding_id)?;
    Ok(PlanningGrantSummary::from_grant(
        &revoked,
        updated_ledger.ledger_revision,
    ))
}

/// Pure read-only list of metadata-only [`PlanningGrantSummary`] records for `scope`.
///
/// Reuses [`CoworkerWorkflowStore::read_binding_ledger`] without creating any directory, ledger
/// file, or lockfile.
pub fn list_planning_grant_summaries(
    store: &CoworkerWorkflowStore,
    scope: &TrustedGrantScope,
) -> Result<Vec<PlanningGrantSummary>, NativeError> {
    scope.validate()?;
    let ledger = store.read_binding_ledger(&scope.binding_id)?;
    if let Some(ref pinned_thread) = ledger.thread_uuid {
        if pinned_thread != &scope.thread_uuid {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "thread_uuid does not match the thread pinned to this binding ledger",
            ));
        }
    }
    Ok(ledger
        .planning_grants
        .iter()
        .map(|grant| PlanningGrantSummary::from_grant(grant, ledger.ledger_revision))
        .collect())
}

/// Pure read-only lookup of a single [`PlanningGrantSummary`] by `(scope, grant_id)`.
///
/// Reuses [`CoworkerWorkflowStore::read_binding_ledger`] without creating any directory, ledger
/// file, or lockfile.
pub fn get_planning_grant_summary(
    store: &CoworkerWorkflowStore,
    scope: &TrustedGrantScope,
    grant_id: &str,
) -> Result<Option<PlanningGrantSummary>, NativeError> {
    validate_bounded_identifier(grant_id, "grant_id", MAX_COWORKER_ID_LEN)?;
    let summaries = list_planning_grant_summaries(store, scope)?;
    Ok(summaries.into_iter().find(|s| s.grant_id == grant_id))
}

fn sort_frozen_role_choices(choices: &mut [FrozenPlanningRoleChoice]) {
    choices.sort_by(|lhs, rhs| lhs.sort_key().cmp(&rhs.sort_key()));
}

const fn adapter_sort_rank(adapter: ToolAdapter) -> u8 {
    match adapter {
        ToolAdapter::CodexCli => 0,
        ToolAdapter::Antigravity => 1,
        ToolAdapter::OpencodeV2 => 2,
        ToolAdapter::JevApi => 3,
        ToolAdapter::OpenaiDecisionsApi => 4,
        ToolAdapter::Git => 5,
        ToolAdapter::Tmux => 6,
        ToolAdapter::Codegraph => 7,
    }
}

fn validate_planning_policy_roles_and_budget(
    policy: &ExecutionPolicy,
    budget: &PlanningCallBudget,
) -> Result<(), NativeError> {
    if policy.allow_rules.is_empty() {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "PlanningAdviceGrant policy must contain at least one explicit allow rule for Orchestrator or Architect",
        ));
    }
    for rule in &policy.allow_rules {
        if rule.roles.is_empty() {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "PlanningAdviceGrant policy allow rules must explicitly list Orchestrator and/or Architect roles",
            ));
        }
        for &role in &rule.roles {
            match role {
                WorkflowRole::Orchestrator => {
                    if budget.max_orchestrator_calls == 0 {
                        return Err(NativeError::new(
                            NativeErrorCode::ValidationFailed,
                            "policy permits WorkflowRole::Orchestrator when max_orchestrator_calls is 0",
                        ));
                    }
                }
                WorkflowRole::Architect => {
                    if budget.max_architect_calls == 0 {
                        return Err(NativeError::new(
                            NativeErrorCode::ValidationFailed,
                            "policy permits WorkflowRole::Architect when max_architect_calls is 0",
                        ));
                    }
                }
                _ => {
                    return Err(NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "PlanningAdviceGrant policy rules must restrict roles to Orchestrator and/or Architect only",
                    ));
                }
            }
        }
    }
    Ok(())
}

fn validate_role_presence_matches_budget(
    choices: &[FrozenPlanningRoleChoice],
    budget: &PlanningCallBudget,
) -> Result<(), NativeError> {
    let has_orch = choices.iter().any(|c| c.role == WorkflowRole::Orchestrator);
    let has_arch = choices.iter().any(|c| c.role == WorkflowRole::Architect);
    if has_orch != (budget.max_orchestrator_calls > 0) {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "Orchestrator role presence must match positive max_orchestrator_calls budget",
        ));
    }
    if has_arch != (budget.max_architect_calls > 0) {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "Architect role presence must match positive max_architect_calls budget",
        ));
    }
    Ok(())
}

fn validate_policy_bound_to_frozen_choices(
    policy: &ExecutionPolicy,
    choices: &[FrozenPlanningRoleChoice],
) -> Result<(), NativeError> {
    for rule in &policy.allow_rules {
        for &role in &rule.roles {
            if rule.allow_no_effort {
                let matched = choices.iter().any(|c| {
                    c.role == role
                        && c.selected_choice.tool_id == rule.tool_id
                        && c.selected_choice.model_id == rule.model_id
                        && c.selected_choice.effort.is_none()
                });
                if !matched {
                    return Err(NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "execution policy permits an unpreviewed no-effort role/model choice",
                    ));
                }
            }
            for effort in &rule.efforts {
                let matched = choices.iter().any(|c| {
                    c.role == role
                        && c.selected_choice.tool_id == rule.tool_id
                        && c.selected_choice.model_id == rule.model_id
                        && c.selected_choice.effort.as_deref() == Some(effort.as_str())
                });
                if !matched {
                    return Err(NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "execution policy permits an unpreviewed role/model/effort tuple",
                    ));
                }
            }
        }
    }
    Ok(())
}

fn validate_policy_rules_against_snapshot(
    snapshot: &RegistrySnapshot,
    policy: &ExecutionPolicy,
) -> Result<(), NativeError> {
    for rule in &policy.allow_rules {
        let tool = snapshot
            .tools
            .iter()
            .find(|t| t.id == rule.tool_id)
            .ok_or_else(|| {
                NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    format!("policy references missing registry tool `{}`", rule.tool_id),
                )
            })?;
        if !tool.enabled {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!("policy references disabled registry tool `{}`", tool.id),
            ));
        }
        let route = RoleExecutionRoute::from_adapter(tool.adapter);
        let provider = match tool.adapter {
            ToolAdapter::Antigravity if route == RoleExecutionRoute::HeadlessWorker => {
                WorkerProvider::Antigravity
            }
            ToolAdapter::OpencodeV2 if route == RoleExecutionRoute::HeadlessWorker => {
                WorkerProvider::OpenCodeV2
            }
            ToolAdapter::CodexCli => {
                if rule.roles.is_empty()
                    || !rule.roles.iter().all(|role| {
                        matches!(role, WorkflowRole::Orchestrator | WorkflowRole::Architect)
                    })
                {
                    return Err(NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        format!(
                            "CodexCli policy rule on tool `{}` may only authorize read-only planning roles (Orchestrator or Architect)",
                            tool.id
                        ),
                    ));
                }
                if rule.allow_no_effort || rule.efforts.is_empty() {
                    return Err(NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        format!(
                            "CodexCli policy rule on tool `{}` requires explicit reasoning efforts (allow_no_effort is not supported)",
                            tool.id
                        ),
                    ));
                }
                WorkerProvider::CodexCli
            }
            _ => {
                return Err(NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    format!(
                        "policy tool `{}` uses adapter `{}` ({route}); planning grants require supported headless or read-only advisory workers (Antigravity, OpencodeV2, or CodexCli)",
                        tool.id, tool.adapter
                    ),
                ));
            }
        };
        let model = snapshot
            .models
            .iter()
            .find(|m| m.id == rule.model_id && m.tool_id == tool.id)
            .ok_or_else(|| {
                NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    format!(
                        "policy references missing registry model `{}` on tool `{}`",
                        rule.model_id, tool.id
                    ),
                )
            })?;
        if rule.allow_no_effort {
            if !model.allow_no_effort {
                return Err(NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    format!("model `{}` does not allow empty effort", model.id),
                ));
            }
            resolve_model_target(provider, &model.model_arg, None).map_err(|err| {
                NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    format!(
                        "invalid headless worker target for model `{}`: {err}",
                        model.id
                    ),
                )
            })?;
        }
        for effort in &rule.efforts {
            if !model.efforts.iter().any(|e| e == effort) {
                return Err(NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    format!(
                        "model `{}` does not support effort `{effort}` in registry",
                        model.id
                    ),
                ));
            }
            resolve_model_target(provider, &model.model_arg, Some(effort.as_str()))
                .map_err(|err| {
                    NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        format!(
                            "invalid headless worker target for model `{}` with effort `{effort}`: {err}",
                            model.id
                        ),
                    )
                })?;
        }
    }
    Ok(())
}
