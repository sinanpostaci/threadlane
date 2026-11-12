//! Operator-attested external resolution receipts (`external_resolution_receipts_v1`) stored in
//! [`crate::coworker_workflow::CoworkerBindingLedger`] over [`crate::coworker_workflow::CoworkerWorkflowStore`]
//! and [`crate::fs_safety`].
//!
//! ## Core Security & Persistence Invariants
//!
//! - **No parallel job or authority store**: Receipts are appended atomically to
//!   `CoworkerBindingLedger::external_resolutions` inside the existing per-binding private
//!   (`0700`/`0600`) workflow ledger.
//! - **Pure read-only `preview` and `inspect`**: Neither `preview` nor `inspect` creates
//!   directories, lockfiles, journals, temporary files, or `fsync` calls, and neither spawns
//!   `git`, provider CLIs, or `threadlane-host`.
//! - **Proven target linkage only**: Targets must be either an exact [`CoworkerJobRecord`] in
//!   [`CoworkerWorkflowStore`] or a legacy published [`PlanRevision`] in [`PlanStore`] that is
//!   uniquely linked to a single verified [`ApprovalRecord`] in [`AuthorityStore`]. Raw commit
//!   SHAs, provider prose, and caller JSON self-attestations never prove target or authority scope.
//! - **Separate operator confirmation & deadlock-free lock order**: `record` requires an explicit
//!   [`ExternalResolutionConfirmation`] (`trusted_local_attested == true`, matching canonical
//!   `thread_uuid`, and constant-time matching domain-separated `preview_digest`) and holds:
//!   1. Physical [`CoworkerJobOwnershipGuard`] (`try_acquire_job`),
//!   2. Per-binding conversation advancement lock (`changes-<tag>.lock`),
//!   3. Non-blocking [`HostLaunchIntentLock`] and [`HostSessionLock`] (when an approval is bound),
//!   4. Per-binding [`PlanningCommandStore`] lock,
//!   5. Per-binding [`ControllerPolicyStore`] lock (with the job runtime snapshot for job targets),
//!   6. Per-binding [`CoworkerWorkflowStore`] transaction lock.
//! - **Append-only single mutation**: Recording appends only the immutable
//!   [`ExternalResolutionReceipt`]; it never mutates `job.phase`, `job.job_revision`, worker task
//!   results, holds, approvals, controller envelopes, review/integration/Closure receipts, or host
//!   event/ACK state, and never cancels, retries, or reconciles external effects.
//! - **Explicit `PersistenceUncertain` handling**: `PersistenceUncertain` is never converted into
//!   `Ok`. `inspect` surfaces unwitnessed receipts as [`ExternalResolutionDurability::PersistenceUncertain`],
//!   and [`ExternalResolutionService::reconcile`] establishes file-and-directory `fsync` durability
//!   strictly on existing unchanged ledger bytes bound to the original `request_key` and
//!   `preview_digest`.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use fs2::FileExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use threadlane_authority::validation::validate_canonical_thread_uuid;
use threadlane_authority::{ApprovalRecord, AuthorityStore};
use threadlane_host::{
    read_host_launch_intent_optional, read_host_manifest_optional,
    read_session_task_ledger_recorded_history_readonly, verify_existing_private_dir,
    verify_existing_session_dir, HostError, HostLaunchIntentLock, HostLaunchIntentStatus,
    HostNotificationWakeupStatus, HostPhase, HostSessionLock, HOST_LOCK_FILENAME,
    LAUNCH_INTENTS_SUBDIR,
};
use threadlane_workers::{validate_exact_40_hex_sha, validate_sha256_hex_digest};
use threadlane_workflow::{compile_plan_revision, PlanRevision, PlanStore, RegistryStore};

use crate::conversation_change::{
    inspect_unresolved_blocking_changes, ConversationChangeStore, CONVERSATION_CHANGE_NAMESPACE_DIR,
};
use crate::coworker_driver::coworker_driver_dir;
use crate::coworker_workflow::{
    deterministic_prefixed_id, sha256_hex, CombinedCandidateStatus, ControllerJobRuntimeState,
    ControllerPolicyStore, CoworkerBindingLedger, CoworkerJobLockState, CoworkerJobOwnershipGuard,
    CoworkerJobOwnershipKey, CoworkerJobOwnershipStore, CoworkerJobPhase, CoworkerJobRecord,
    CoworkerLedgerMutationOutcome, CoworkerOwnershipAcquireOutcome, CoworkerTaskStage,
    CoworkerWorkflowStore, MAX_COWORKER_ID_LEN, MAX_TASKS_PER_JOB,
};
use crate::error::{NativeError, NativeErrorCode};
use crate::fs_safety::{
    acquire_cross_process_lock, constant_time_str_eq, ensure_platform_supported,
    prepare_private_dir, sanitize_diagnostic_text, validate_absolute_no_symlink_components,
    validate_bounded_identifier, CrossProcessLockGuard, MAX_PRIVATE_STATE_FILE_BYTES,
};
use crate::planning_commands::{PlanningCommandStatus, PlanningCommandStore};
use crate::planning_driver::planning_commands_dir;

/// Stable wire/contract identifier for external resolution receipts v1.
pub const EXTERNAL_RESOLUTION_INTERFACE_VERSION: &str = "external_resolution_receipts_v1";

/// Schema version for persisted [`ExternalResolutionReceipt`] entries (`1`).
pub const EXTERNAL_RESOLUTION_SCHEMA_VERSION: u32 = 1;

/// Maximum number of immutable [`ExternalResolutionReceipt`] entries retained per binding ledger.
pub const MAX_EXTERNAL_RESOLUTIONS_PER_BINDING: usize = 64;

/// Maximum number of bounded evidence references attached to a single external resolution receipt.
pub const MAX_EXTERNAL_RESOLUTION_EVIDENCE_REFS: usize = 16;

/// Maximum UTF-8 byte length for an operator explanation string.
pub const MAX_EXTERNAL_RESOLUTION_EXPLANATION_BYTES: usize = 2048;

/// Maximum UTF-8 byte length for an evidence artifact reference string.
pub const MAX_EXTERNAL_RESOLUTION_REFERENCE_BYTES: usize = 256;

/// Domain separator for normalized [`RecordExternalResolutionRequest`] SHA-256 digests.
pub const EXTERNAL_RESOLUTION_REQUEST_DOMAIN: &str =
    "threadlane-native:external-resolution-request:v1";

/// Domain separator for [`ExternalResolutionPreview`] confirmation SHA-256 digests.
pub const EXTERNAL_RESOLUTION_PREVIEW_DOMAIN: &str =
    "threadlane-native:external-resolution-preview:v1";

/// Domain separator for persisted [`ExternalResolutionReceipt`] SHA-256 digests.
pub const EXTERNAL_RESOLUTION_RECEIPT_DOMAIN: &str =
    "threadlane-native:external-resolution-receipt:v1";

/// Domain separator for deterministic `exr_<32hex>` receipt identifiers.
pub const EXTERNAL_RESOLUTION_ID_DOMAIN: &str = "threadlane-native:external-resolution-id:v1";

const MAX_WITNESS_ENTRIES: usize = 1024;

type WitnessMap = BTreeMap<(PathBuf, String), String>;

/// Canonical project base identity proven from workflow or authority stores without spawning `git`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExternalResolutionProjectRef {
    /// Canonical non-symlink absolute Git repository root path.
    pub canonical_repo_root: PathBuf,
    /// Exact 40-character lowercase hex base commit SHA proven from workflow/authority stores.
    pub base_commit: String,
    /// Exact 40-character lowercase hex base tree SHA when bound on a [`CoworkerJobRecord`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_tree_sha: Option<String>,
}

impl ExternalResolutionProjectRef {
    /// Validates structural canonical path syntax and 40-hex commit/tree digests without touching Git.
    pub fn validate_syntax(&self) -> Result<(), NativeError> {
        let _ = validate_absolute_no_symlink_components(
            &self.canonical_repo_root,
            "project.canonical_repo_root",
        )?;
        validate_exact_40_hex_sha(&self.base_commit, "project.base_commit")
            .map_err(|_| invalid("project.base_commit must be a 40-character lowercase hex SHA"))?;
        if let Some(ref tree_sha) = self.base_tree_sha {
            validate_exact_40_hex_sha(tree_sha, "project.base_tree_sha").map_err(|_| {
                invalid("project.base_tree_sha must be a 40-character lowercase hex SHA")
            })?;
        }
        Ok(())
    }
}

/// Target workflow unit resolved externally by the operator.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ExternalResolutionTarget {
    /// Exact [`CoworkerJobRecord`] in [`CoworkerWorkflowStore`].
    Job {
        /// Target Coworker job identifier (`job_<32hex>`).
        job_id: String,
    },
    /// Legacy published [`PlanRevision`] in [`PlanStore`] uniquely linked via [`AuthorityStore`].
    LegacyPlan {
        /// Target workflow plan identifier.
        plan_id: String,
        /// Exact published workflow plan revision (`>= 1`).
        plan_revision: u64,
    },
    /// Raw commit SHA without a bound job or uniquely provable legacy plan (always refused).
    CommitOnly {
        /// 40-character lowercase hex Git commit SHA.
        commit_sha: String,
    },
}

impl ExternalResolutionTarget {
    /// Validates structural identifier and revision syntax on the target descriptor.
    pub fn validate_syntax(&self) -> Result<(), NativeError> {
        match self {
            Self::Job { job_id } => {
                validate_bounded_identifier(job_id, "target.job_id", MAX_COWORKER_ID_LEN)?;
            }
            Self::LegacyPlan {
                plan_id,
                plan_revision,
            } => {
                validate_bounded_identifier(plan_id, "target.plan_id", MAX_COWORKER_ID_LEN)?;
                if *plan_revision == 0 {
                    return Err(invalid("target.plan_revision must be >= 1"));
                }
            }
            Self::CommitOnly { commit_sha } => {
                validate_exact_40_hex_sha(commit_sha, "target.commit_sha").map_err(|_| {
                    invalid("target.commit_sha must be a 40-character lowercase hex SHA")
                })?;
            }
        }
        Ok(())
    }
}

/// Explicit resolution coverage within the bound job or legacy plan revision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ExternalResolutionScope {
    /// Covers the entire target job or legacy approved plan revision.
    FullJob,
    /// Covers an explicit non-empty unique subset of task identifiers in the target.
    Tasks {
        /// Non-empty unique task identifiers belonging to the target job or legacy plan.
        task_ids: Vec<String>,
    },
}

impl ExternalResolutionScope {
    /// Validates non-emptiness, identifier syntax, uniqueness, and bounds on the scope.
    pub fn validate(&self) -> Result<(), NativeError> {
        match self {
            Self::FullJob => Ok(()),
            Self::Tasks { task_ids } => {
                if task_ids.is_empty() || task_ids.len() > MAX_TASKS_PER_JOB {
                    return Err(invalid(
                        "scope.task_ids must contain between 1 and MAX_TASKS_PER_JOB unique task IDs",
                    ));
                }
                let mut seen = BTreeSet::new();
                for tid in task_ids {
                    validate_bounded_identifier(tid, "scope.task_id", MAX_COWORKER_ID_LEN)?;
                    if !seen.insert(tid.as_str()) {
                        return Err(invalid(
                            "scope.task_ids must not contain duplicate task IDs",
                        ));
                    }
                }
                Ok(())
            }
        }
    }

    fn normalized(&self) -> Self {
        match self {
            Self::FullJob => Self::FullJob,
            Self::Tasks { task_ids } => {
                let mut sorted = task_ids.clone();
                sorted.sort();
                Self::Tasks { task_ids: sorted }
            }
        }
    }
}

/// Bounded informational evidence annotation supplied by the operator.
///
/// Evidence annotations never substitute for store-proven `(binding_id, thread_uuid, project,
/// target)` linkage or separate trusted-local operator confirmation.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ExternalResolutionEvidenceRef {
    /// Operator-referenced 40-character lowercase hex Git commit SHA annotation.
    CommitSha {
        /// Exact 40-character lowercase hex Git commit SHA.
        commit_sha: String,
    },
    /// Operator-referenced bounded artifact, pull request, or issue label.
    ArtifactRef {
        /// Bounded single-line non-empty reference string free of control characters.
        reference: String,
    },
    /// Untrusted provider/worker prose claim (always rejected as non-evidence).
    ProviderText {
        /// Raw provider text claim.
        text: String,
    },
    /// Untrusted caller JSON self-attestation claim (always rejected as non-evidence).
    JsonSelfAttestation {
        /// Raw self-attestation claim.
        claim: String,
    },
}

impl ExternalResolutionEvidenceRef {
    /// Validates a stored evidence reference on an already-accepted receipt.
    pub fn validate_accepted(&self) -> Result<(), NativeError> {
        match self {
            Self::CommitSha { commit_sha } => {
                validate_exact_40_hex_sha(commit_sha, "evidence_ref.commit_sha").map_err(|_| {
                    invalid("evidence_ref.commit_sha must be a 40-character lowercase hex SHA")
                })
            }
            Self::ArtifactRef { reference } => validate_clean_bounded_text(
                reference,
                "evidence_ref.reference",
                MAX_EXTERNAL_RESOLUTION_REFERENCE_BYTES,
            ),
            Self::ProviderText { .. } | Self::JsonSelfAttestation { .. } => Err(NativeError::new(
                NativeErrorCode::UnsupportedCapability,
                "provider prose and JSON self-attestation are not valid external resolution evidence",
            )),
        }
    }
}

/// Request to preview or record an external resolution receipt under `external_resolution_receipts_v1`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecordExternalResolutionRequest {
    /// Control-plane chat binding identifier.
    pub binding_id: String,
    /// Canonical lowercase hyphenated non-nil native Codex thread UUID.
    pub thread_uuid: String,
    /// Canonical absolute Git repository root expected by the operator.
    pub canonical_repo_root: PathBuf,
    /// Target [`CoworkerJobRecord`] or uniquely linkable legacy [`PlanRevision`].
    pub target: ExternalResolutionTarget,
    /// Explicit full-job or task-subset resolution scope.
    pub scope: ExternalResolutionScope,
    /// Optional expected current [`CoworkerBindingLedger::ledger_revision`] CAS guard.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_ledger_revision: Option<u64>,
    /// Optional expected current [`CoworkerJobRecord::job_revision`] CAS guard.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_job_revision: Option<u64>,
    /// Optional expected current [`ControllerJobRuntimeState::runtime_revision`] CAS guard.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_runtime_revision: Option<u64>,
    /// Immutable caller-supplied idempotency key.
    pub request_key: String,
    /// Immutable caller-supplied creation timestamp in milliseconds since Unix epoch (`> 0`).
    pub created_at_ms: u64,
    /// Bounded human-readable explanation free of control characters.
    pub explanation: String,
    /// Bounded informational operator evidence references (`<= 16` unique entries).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub evidence_refs: Vec<ExternalResolutionEvidenceRef>,
}

impl RecordExternalResolutionRequest {
    /// Validates structural bounds, rejects untrusted non-proof targets/evidence, and checks syntax.
    pub fn validate(&self) -> Result<(), NativeError> {
        let _ = serialize_bounded(self, "external resolution request")?;
        validate_bounded_identifier(&self.binding_id, "binding_id", MAX_COWORKER_ID_LEN)?;
        validate_canonical_thread_uuid("thread_uuid", &self.thread_uuid)
            .map_err(|_| invalid("thread_uuid must be a canonical lowercase non-nil UUID"))?;
        let _ = validate_absolute_no_symlink_components(
            &self.canonical_repo_root,
            "canonical_repo_root",
        )?;
        self.target.validate_syntax()?;
        if matches!(self.target, ExternalResolutionTarget::CommitOnly { .. }) {
            return Err(NativeError::new(
                NativeErrorCode::UnsupportedCapability,
                "raw commit SHA alone does not prove binding, thread, or workflow target scope",
            ));
        }
        if matches!(self.target, ExternalResolutionTarget::LegacyPlan { .. })
            && (self.expected_job_revision.is_some() || self.expected_runtime_revision.is_some())
        {
            return Err(invalid(
                "legacy_plan targets cannot specify expected_job_revision or expected_runtime_revision",
            ));
        }
        if let Some(expected_job_rev) = self.expected_job_revision {
            if expected_job_rev == 0 {
                return Err(invalid("expected_job_revision must be >= 1 when specified"));
            }
        }
        if let Some(expected_rt_rev) = self.expected_runtime_revision {
            if expected_rt_rev == 0 {
                return Err(invalid(
                    "expected_runtime_revision must be >= 1 when specified",
                ));
            }
        }
        self.scope.validate()?;
        validate_bounded_identifier(&self.request_key, "request_key", MAX_COWORKER_ID_LEN)?;
        if self.created_at_ms == 0 {
            return Err(invalid("created_at_ms must be > 0"));
        }
        validate_clean_bounded_text(
            &self.explanation,
            "explanation",
            MAX_EXTERNAL_RESOLUTION_EXPLANATION_BYTES,
        )?;
        if self.evidence_refs.len() > MAX_EXTERNAL_RESOLUTION_EVIDENCE_REFS {
            return Err(invalid(
                "evidence_refs exceeds MAX_EXTERNAL_RESOLUTION_EVIDENCE_REFS",
            ));
        }
        let mut unique_refs = BTreeSet::new();
        for ev in &self.evidence_refs {
            ev.validate_accepted()?;
            if !unique_refs.insert(ev) {
                return Err(invalid("evidence_refs must not contain duplicate entries"));
            }
        }
        Ok(())
    }

    fn normalized(&self) -> Self {
        let mut copy = self.clone();
        copy.scope = self.scope.normalized();
        copy
    }
}

/// Separate trusted-local operator confirmation required by [`ExternalResolutionService::record`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExternalResolutionConfirmation {
    /// Explicit trusted-local operator attestation (`true` required).
    pub trusted_local_attested: bool,
    /// Canonical lowercase hyphenated non-nil native Codex thread UUID confirmed by the operator.
    pub thread_uuid: String,
    /// Exact 64-character lowercase hex `preview_digest` produced by [`ExternalResolutionService::preview`].
    pub preview_digest: String,
}

impl ExternalResolutionConfirmation {
    fn validate_for_request_and_digest(
        &self,
        expected_thread_uuid: &str,
        expected_preview_digest: &str,
    ) -> Result<(), NativeError> {
        if !self.trusted_local_attested {
            return Err(NativeError::new(
                NativeErrorCode::UnauthorizedOperator,
                "recording an external resolution receipt requires trusted_local_attested = true",
            ));
        }
        validate_canonical_thread_uuid("confirmation.thread_uuid", &self.thread_uuid).map_err(
            |_| {
                NativeError::new(
                    NativeErrorCode::UnauthorizedOperator,
                    "confirmation.thread_uuid must be a canonical lowercase non-nil UUID",
                )
            },
        )?;
        validate_sha256_hex_digest(&self.preview_digest, "confirmation.preview_digest").map_err(
            |_| {
                NativeError::new(
                    NativeErrorCode::UnauthorizedOperator,
                    "confirmation.preview_digest must be a 64-character lowercase hex SHA-256 digest",
                )
            },
        )?;
        if self.thread_uuid.as_str() != expected_thread_uuid {
            return Err(NativeError::new(
                NativeErrorCode::UnauthorizedOperator,
                "confirmation.thread_uuid does not match the scoped request thread_uuid",
            ));
        }
        if !constant_time_str_eq(&self.preview_digest, expected_preview_digest) {
            return Err(NativeError::new(
                NativeErrorCode::UnauthorizedOperator,
                "confirmation.preview_digest does not match the exact recomputed preview_digest",
            ));
        }
        Ok(())
    }
}

/// Pure read-only preview of an external resolution receipt (`external_resolution_receipts_v1`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExternalResolutionPreview {
    /// Interface contract version (`"external_resolution_receipts_v1"`).
    pub interface_version: String,
    /// Schema version (`1`).
    pub schema_version: u32,
    /// Scoped control-plane chat binding identifier.
    pub binding_id: String,
    /// Scoped canonical lowercase hyphenated native Codex thread UUID.
    pub thread_uuid: String,
    /// Store-proven canonical project repository and base commit/tree reference.
    pub project: ExternalResolutionProjectRef,
    /// Exact bound [`CoworkerJobRecord`] or uniquely linked legacy [`PlanRevision`].
    pub target: ExternalResolutionTarget,
    /// Normalized explicit full-job or task-subset resolution scope.
    pub scope: ExternalResolutionScope,
    /// Sorted concrete task identifiers covered by `scope` (when tasks exist on the target).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub resolved_task_ids: Vec<String>,
    /// Original [`CoworkerBindingLedger::ledger_revision`] prior to appending the receipt.
    pub source_ledger_revision: u64,
    /// Resulting [`CoworkerBindingLedger::ledger_revision`] (`source_ledger_revision + 1`).
    pub resulting_ledger_revision: u64,
    /// Original [`CoworkerJobRecord::job_revision`] when targeting a Coworker job.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_job_revision: Option<u64>,
    /// Original [`ControllerPolicyStore`] binding ledger revision (`0` when absent).
    pub source_controller_ledger_revision: u64,
    /// Original [`ControllerJobRuntimeState::runtime_revision`] when controller runtime exists.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_runtime_revision: Option<u64>,
    /// Frozen 64-hex [`crate::coworker_workflow::WorkBrief`] digest when targeting a Coworker job.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub work_brief_digest: Option<String>,
    /// Bound workflow `plan_id` when planned or approved.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plan_id: Option<String>,
    /// Bound workflow `plan_revision` when planned or approved.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plan_revision: Option<u64>,
    /// Compiled/frozen 64-hex `plan_digest` when planned or approved.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plan_digest: Option<String>,
    /// Bound [`ApprovalRecord::approval_id`] when approved.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approval_id: Option<String>,
    /// Frozen 64-hex [`ApprovalRecord::subject_digest`] when approved.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approval_subject_digest: Option<String>,
    /// Frozen 64-hex controller execution `envelope_digest` when controller runtime exists.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub envelope_digest: Option<String>,
    /// Immutable caller-supplied idempotency key.
    pub request_key: String,
    /// Immutable caller-supplied creation timestamp (ms since Unix epoch).
    pub created_at_ms: u64,
    /// Bounded operator explanation.
    pub explanation: String,
    /// Bounded operator evidence references.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub evidence_refs: Vec<ExternalResolutionEvidenceRef>,
    /// Domain-separated 64-hex SHA-256 digest of the normalized [`RecordExternalResolutionRequest`].
    pub request_digest: String,
    /// Domain-separated 64-hex SHA-256 confirmation digest over all preview fields above.
    pub preview_digest: String,
}

impl ExternalResolutionPreview {
    fn compute_preview_digest(&self) -> Result<String, NativeError> {
        let mut canonical = self.clone();
        canonical.preview_digest.clear();
        let bytes = serialize_bounded(
            &(EXTERNAL_RESOLUTION_PREVIEW_DOMAIN, &canonical),
            "external resolution preview",
        )?;
        Ok(sha256_hex(&bytes))
    }
}

/// Immutable external resolution receipt appended to `CoworkerBindingLedger::external_resolutions`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExternalResolutionReceipt {
    /// Storage schema version (`1`).
    pub schema_version: u32,
    /// Interface contract version (`"external_resolution_receipts_v1"`).
    pub interface_version: String,
    /// Deterministic receipt identifier (`exr_<32hex>`).
    pub receipt_id: String,
    /// Control-plane chat binding identifier.
    pub binding_id: String,
    /// Pinned canonical lowercase hyphenated native Codex thread UUID.
    pub thread_uuid: String,
    /// Immutable caller-supplied idempotency key.
    pub request_key: String,
    /// Immutable creation timestamp (ms since Unix epoch).
    pub created_at_ms: u64,
    /// Store-proven canonical project repository and base commit/tree reference.
    pub project: ExternalResolutionProjectRef,
    /// Exact bound [`CoworkerJobRecord`] or uniquely linked legacy [`PlanRevision`].
    pub target: ExternalResolutionTarget,
    /// Normalized explicit full-job or task-subset resolution scope.
    pub scope: ExternalResolutionScope,
    /// Sorted concrete task identifiers covered by `scope` (when tasks exist on the target).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub resolved_task_ids: Vec<String>,
    /// [`CoworkerBindingLedger::ledger_revision`] immediately prior to recording this receipt.
    pub source_ledger_revision: u64,
    /// [`CoworkerBindingLedger::ledger_revision`] produced by appending this receipt (`source_ledger_revision + 1`).
    pub recorded_ledger_revision: u64,
    /// [`CoworkerJobRecord::job_revision`] at record time when targeting a Coworker job.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_job_revision: Option<u64>,
    /// [`ControllerPolicyStore`] binding ledger revision at record time.
    pub source_controller_ledger_revision: u64,
    /// [`ControllerJobRuntimeState::runtime_revision`] at record time when controller runtime existed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_runtime_revision: Option<u64>,
    /// Frozen 64-hex [`crate::coworker_workflow::WorkBrief`] digest when targeting a Coworker job.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub work_brief_digest: Option<String>,
    /// Bound workflow `plan_id` when planned or approved.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plan_id: Option<String>,
    /// Bound workflow `plan_revision` when planned or approved.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plan_revision: Option<u64>,
    /// Compiled/frozen 64-hex `plan_digest` when planned or approved.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plan_digest: Option<String>,
    /// Bound [`ApprovalRecord::approval_id`] when approved.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approval_id: Option<String>,
    /// Frozen 64-hex [`ApprovalRecord::subject_digest`] when approved.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approval_subject_digest: Option<String>,
    /// Frozen 64-hex controller execution `envelope_digest` when controller runtime existed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub envelope_digest: Option<String>,
    /// Bounded operator explanation.
    pub explanation: String,
    /// Bounded operator evidence references.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub evidence_refs: Vec<ExternalResolutionEvidenceRef>,
    /// Domain-separated 64-hex SHA-256 digest of the normalized [`RecordExternalResolutionRequest`].
    pub request_digest: String,
    /// Domain-separated 64-hex SHA-256 `preview_digest` confirmed by the operator.
    pub preview_digest: String,
    /// Domain-separated 64-hex SHA-256 integrity digest of this receipt.
    pub receipt_digest: String,
}

impl ExternalResolutionReceipt {
    fn from_preview(preview: &ExternalResolutionPreview) -> Result<Self, NativeError> {
        let receipt_id = deterministic_prefixed_id(
            EXTERNAL_RESOLUTION_ID_DOMAIN,
            "exr",
            &[
                &preview.binding_id,
                &preview.thread_uuid,
                &preview.request_key,
            ],
        );
        let mut receipt = Self {
            schema_version: EXTERNAL_RESOLUTION_SCHEMA_VERSION,
            interface_version: EXTERNAL_RESOLUTION_INTERFACE_VERSION.to_string(),
            receipt_id,
            binding_id: preview.binding_id.clone(),
            thread_uuid: preview.thread_uuid.clone(),
            request_key: preview.request_key.clone(),
            created_at_ms: preview.created_at_ms,
            project: preview.project.clone(),
            target: preview.target.clone(),
            scope: preview.scope.clone(),
            resolved_task_ids: preview.resolved_task_ids.clone(),
            source_ledger_revision: preview.source_ledger_revision,
            recorded_ledger_revision: preview.resulting_ledger_revision,
            source_job_revision: preview.source_job_revision,
            source_controller_ledger_revision: preview.source_controller_ledger_revision,
            source_runtime_revision: preview.source_runtime_revision,
            work_brief_digest: preview.work_brief_digest.clone(),
            plan_id: preview.plan_id.clone(),
            plan_revision: preview.plan_revision,
            plan_digest: preview.plan_digest.clone(),
            approval_id: preview.approval_id.clone(),
            approval_subject_digest: preview.approval_subject_digest.clone(),
            envelope_digest: preview.envelope_digest.clone(),
            explanation: preview.explanation.clone(),
            evidence_refs: preview.evidence_refs.clone(),
            request_digest: preview.request_digest.clone(),
            preview_digest: preview.preview_digest.clone(),
            receipt_digest: String::new(),
        };
        receipt.receipt_digest = receipt.compute_receipt_digest()?;
        receipt.validate_integrity()?;
        Ok(receipt)
    }

    fn to_preview(&self) -> ExternalResolutionPreview {
        ExternalResolutionPreview {
            interface_version: self.interface_version.clone(),
            schema_version: self.schema_version,
            binding_id: self.binding_id.clone(),
            thread_uuid: self.thread_uuid.clone(),
            project: self.project.clone(),
            target: self.target.clone(),
            scope: self.scope.clone(),
            resolved_task_ids: self.resolved_task_ids.clone(),
            source_ledger_revision: self.source_ledger_revision,
            resulting_ledger_revision: self.recorded_ledger_revision,
            source_job_revision: self.source_job_revision,
            source_controller_ledger_revision: self.source_controller_ledger_revision,
            source_runtime_revision: self.source_runtime_revision,
            work_brief_digest: self.work_brief_digest.clone(),
            plan_id: self.plan_id.clone(),
            plan_revision: self.plan_revision,
            plan_digest: self.plan_digest.clone(),
            approval_id: self.approval_id.clone(),
            approval_subject_digest: self.approval_subject_digest.clone(),
            envelope_digest: self.envelope_digest.clone(),
            request_key: self.request_key.clone(),
            created_at_ms: self.created_at_ms,
            explanation: self.explanation.clone(),
            evidence_refs: self.evidence_refs.clone(),
            request_digest: self.request_digest.clone(),
            preview_digest: self.preview_digest.clone(),
        }
    }

    fn compute_receipt_digest(&self) -> Result<String, NativeError> {
        let mut canonical = self.clone();
        canonical.receipt_digest.clear();
        let bytes = serialize_bounded(
            &(EXTERNAL_RESOLUTION_RECEIPT_DOMAIN, &canonical),
            "external resolution receipt",
        )?;
        Ok(sha256_hex(&bytes))
    }

    /// Validates all structural fields, invariants, deterministic `receipt_id`, `preview_digest`,
    /// and `receipt_digest` of this receipt.
    pub fn validate_integrity(&self) -> Result<(), NativeError> {
        if self.schema_version != EXTERNAL_RESOLUTION_SCHEMA_VERSION {
            return Err(NativeError::new(
                NativeErrorCode::ProtocolError,
                format!(
                    "unsupported external resolution schema_version {} (expected {EXTERNAL_RESOLUTION_SCHEMA_VERSION})",
                    self.schema_version
                ),
            ));
        }
        if self.interface_version != EXTERNAL_RESOLUTION_INTERFACE_VERSION {
            return Err(NativeError::new(
                NativeErrorCode::ProtocolError,
                "unsupported external resolution interface_version",
            ));
        }
        validate_bounded_identifier(&self.binding_id, "receipt.binding_id", MAX_COWORKER_ID_LEN)?;
        validate_canonical_thread_uuid("receipt.thread_uuid", &self.thread_uuid)
            .map_err(|_| invalid("receipt.thread_uuid is not a canonical UUID"))?;
        validate_bounded_identifier(
            &self.request_key,
            "receipt.request_key",
            MAX_COWORKER_ID_LEN,
        )?;
        let expected_id = deterministic_prefixed_id(
            EXTERNAL_RESOLUTION_ID_DOMAIN,
            "exr",
            &[&self.binding_id, &self.thread_uuid, &self.request_key],
        );
        if self.receipt_id != expected_id {
            return Err(NativeError::new(
                NativeErrorCode::ProtocolError,
                "external resolution receipt_id does not match deterministic binding/thread/request_key derivation",
            ));
        }
        if self.created_at_ms == 0 {
            return Err(invalid("receipt.created_at_ms must be > 0"));
        }
        let expected_recorded = self
            .source_ledger_revision
            .checked_add(1)
            .ok_or_else(|| invalid("receipt.source_ledger_revision overflow"))?;
        if self.recorded_ledger_revision != expected_recorded {
            return Err(NativeError::new(
                NativeErrorCode::ProtocolError,
                "receipt.recorded_ledger_revision must equal source_ledger_revision + 1",
            ));
        }
        self.project.validate_syntax()?;
        self.target.validate_syntax()?;
        match &self.target {
            ExternalResolutionTarget::Job { .. } => {
                if self.source_job_revision.unwrap_or(0) == 0
                    || self.work_brief_digest.is_none()
                    || self.project.base_tree_sha.is_none()
                {
                    return Err(NativeError::new(
                        NativeErrorCode::ProtocolError,
                        "job external resolution receipt requires source_job_revision >= 1, work_brief_digest, and base_tree_sha",
                    ));
                }
            }
            ExternalResolutionTarget::LegacyPlan {
                plan_id,
                plan_revision,
            } => {
                if self.source_job_revision.is_some()
                    || self.source_runtime_revision.is_some()
                    || self.work_brief_digest.is_some()
                    || self.envelope_digest.is_some()
                    || self.plan_id.as_deref() != Some(plan_id.as_str())
                    || self.plan_revision != Some(*plan_revision)
                    || self.plan_digest.is_none()
                    || self.approval_id.is_none()
                    || self.approval_subject_digest.is_none()
                {
                    return Err(NativeError::new(
                        NativeErrorCode::ProtocolError,
                        "legacy_plan external resolution receipt has inconsistent plan/approval/job fields",
                    ));
                }
            }
            ExternalResolutionTarget::CommitOnly { .. } => {
                return Err(NativeError::new(
                    NativeErrorCode::ProtocolError,
                    "commit_only target can never appear in a persisted ExternalResolutionReceipt",
                ));
            }
        }
        self.scope.validate()?;
        if self.scope != self.scope.normalized() {
            return Err(NativeError::new(
                NativeErrorCode::ProtocolError,
                "receipt.scope task_ids must be normalized in canonical sorted order",
            ));
        }
        if let ExternalResolutionScope::Tasks { ref task_ids } = self.scope {
            if &self.resolved_task_ids != task_ids {
                return Err(NativeError::new(
                    NativeErrorCode::ProtocolError,
                    "receipt.resolved_task_ids must match scope.task_ids for task-scoped receipts",
                ));
            }
        } else if !self.resolved_task_ids.is_empty() {
            let mut seen = BTreeSet::new();
            for tid in &self.resolved_task_ids {
                validate_bounded_identifier(tid, "receipt.resolved_task_id", MAX_COWORKER_ID_LEN)?;
                if !seen.insert(tid.as_str()) {
                    return Err(NativeError::new(
                        NativeErrorCode::ProtocolError,
                        "receipt.resolved_task_ids contains duplicate entries",
                    ));
                }
            }
            let sorted: Vec<&str> = seen.into_iter().collect();
            let actual: Vec<&str> = self.resolved_task_ids.iter().map(String::as_str).collect();
            if sorted != actual {
                return Err(NativeError::new(
                    NativeErrorCode::ProtocolError,
                    "receipt.resolved_task_ids must be canonically sorted",
                ));
            }
        }
        for (opt_digest, field) in [
            (
                self.work_brief_digest.as_deref(),
                "receipt.work_brief_digest",
            ),
            (self.plan_digest.as_deref(), "receipt.plan_digest"),
            (
                self.approval_subject_digest.as_deref(),
                "receipt.approval_subject_digest",
            ),
            (self.envelope_digest.as_deref(), "receipt.envelope_digest"),
        ] {
            if let Some(d) = opt_digest {
                validate_sha256_hex_digest(d, field)
                    .map_err(|_| invalid("receipt optional digest must be 64-hex SHA-256"))?;
            }
        }
        if let Some(ref pid) = self.plan_id {
            validate_bounded_identifier(pid, "receipt.plan_id", MAX_COWORKER_ID_LEN)?;
        }
        if let Some(prev) = self.plan_revision {
            if prev == 0 {
                return Err(invalid("receipt.plan_revision must be >= 1"));
            }
        }
        if let Some(ref aid) = self.approval_id {
            validate_bounded_identifier(aid, "receipt.approval_id", MAX_COWORKER_ID_LEN)?;
        }
        validate_clean_bounded_text(
            &self.explanation,
            "receipt.explanation",
            MAX_EXTERNAL_RESOLUTION_EXPLANATION_BYTES,
        )?;
        if self.evidence_refs.len() > MAX_EXTERNAL_RESOLUTION_EVIDENCE_REFS {
            return Err(invalid(
                "receipt.evidence_refs exceeds MAX_EXTERNAL_RESOLUTION_EVIDENCE_REFS",
            ));
        }
        let mut unique_refs = BTreeSet::new();
        for ev in &self.evidence_refs {
            ev.validate_accepted()?;
            if !unique_refs.insert(ev) {
                return Err(invalid("receipt.evidence_refs contains duplicate entries"));
            }
        }
        validate_sha256_hex_digest(&self.request_digest, "receipt.request_digest")
            .map_err(|_| invalid("receipt.request_digest must be 64-hex SHA-256"))?;
        validate_sha256_hex_digest(&self.preview_digest, "receipt.preview_digest")
            .map_err(|_| invalid("receipt.preview_digest must be 64-hex SHA-256"))?;
        validate_sha256_hex_digest(&self.receipt_digest, "receipt.receipt_digest")
            .map_err(|_| invalid("receipt.receipt_digest must be 64-hex SHA-256"))?;

        let recomputed_preview_digest = self.to_preview().compute_preview_digest()?;
        if !constant_time_str_eq(&self.preview_digest, &recomputed_preview_digest) {
            return Err(NativeError::new(
                NativeErrorCode::ProtocolError,
                "external resolution receipt preview_digest failed integrity verification",
            ));
        }
        let recomputed_receipt_digest = self.compute_receipt_digest()?;
        if !constant_time_str_eq(&self.receipt_digest, &recomputed_receipt_digest) {
            return Err(NativeError::new(
                NativeErrorCode::ProtocolError,
                "external resolution receipt_digest failed integrity verification",
            ));
        }
        Ok(())
    }
}

/// Durability confirmation state surfaced by [`ExternalResolutionService::inspect`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExternalResolutionDurability {
    /// File-and-directory `fsync` durability is witnessed in the current service instance.
    VerifiedDurable,
    /// Receipt bytes are visible in the binding ledger, but directory/file durability has not been
    /// witnessed in this service instance; explicit [`ExternalResolutionService::reconcile`] is required.
    PersistenceUncertain,
}

/// Machine-readable reason why a preserved historical [`ExternalResolutionReceipt`] is now stale.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExternalResolutionStaleReason {
    /// Target `CoworkerJobRecord.job_revision` advanced after the receipt was recorded.
    JobRevisionAdvanced,
    /// Target `CoworkerJobRecord` was superseded by a newer job or moved to the `Cancelled` phase.
    JobSuperseded,
    /// Target `CoworkerJobRecord` brief, plan, or approval linkage drifted from the receipt.
    JobTargetDigestDrifted,
    /// Target job's `ControllerJobRuntimeState` was initialized or advanced after the receipt.
    JobRuntimeAdvanced,
    /// Bound workflow `PlanRevision` is missing, no longer active-published, or superseded.
    BoundPlanChanged,
    /// Legacy plan is now claimed by a `CoworkerJobRecord` in `CoworkerWorkflowStore`.
    LegacyPlanClaimedByJob,
    /// Bound `ApprovalRecord` is missing, invalid, or no longer uniquely links the legacy plan.
    BoundApprovalChanged,
    /// Conversational change superseded or blocked the bound approval/target.
    ConversationChangeInvalidated,
}

/// Read-only applicability classification of a stored [`ExternalResolutionReceipt`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
pub enum ExternalResolutionApplicability {
    /// Receipt remains applicable to the current immutable target state (its own `ledger_revision`
    /// increment and unrelated binding/controller appends do not invalidate it).
    Applicable,
    /// Referenced target advanced, was superseded, or changed; receipt history is preserved intact
    /// on disk and reported as stale.
    Stale {
        /// Ordered, deduplicated reasons why the receipt is no longer applicable to live state.
        reasons: Vec<ExternalResolutionStaleReason>,
    },
}

impl ExternalResolutionApplicability {
    /// Returns `true` when the receipt is [`Self::Applicable`].
    #[must_use]
    pub const fn is_applicable(&self) -> bool {
        matches!(self, Self::Applicable)
    }
}

/// Read-only inspection view for a single stored [`ExternalResolutionReceipt`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExternalResolutionReceiptInspection {
    /// Preserved immutable receipt stored in `CoworkerBindingLedger::external_resolutions`.
    pub receipt: ExternalResolutionReceipt,
    /// Current durability confirmation state (`verified_durable` vs `persistence_uncertain`).
    pub durability: ExternalResolutionDurability,
    /// Current target applicability (`applicable` vs `stale`).
    pub applicability: ExternalResolutionApplicability,
}

/// Pure read-only inspection summary for all external resolution receipts on `(binding_id, thread_uuid)`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExternalResolutionInspection {
    /// Interface contract version (`"external_resolution_receipts_v1"`).
    pub interface_version: String,
    /// Scoped control-plane chat binding identifier.
    pub binding_id: String,
    /// Scoped canonical lowercase hyphenated native Codex thread UUID.
    pub thread_uuid: String,
    /// Current [`CoworkerBindingLedger::ledger_revision`].
    pub current_ledger_revision: u64,
    /// Whether any stored receipt currently reports [`ExternalResolutionDurability::PersistenceUncertain`].
    pub has_uncertain_durability: bool,
    /// Preserved receipts in append order with their read-only durability and applicability status.
    pub receipts: Vec<ExternalResolutionReceiptInspection>,
}

/// Request to explicitly reconcile existing-byte durability for an uncertain or restarted receipt.
///
/// Never rewrites ledger bytes, never advances `ledger_revision`, and never retries a missing or
/// mismatched write.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReconcileExternalResolutionRequest {
    /// Explicit trusted-local operator attestation (`true` required).
    pub trusted_local_attested: bool,
    /// Scoped control-plane chat binding identifier.
    pub binding_id: String,
    /// Scoped canonical lowercase hyphenated native Codex thread UUID.
    pub thread_uuid: String,
    /// Original `request_key` of the receipt to reconcile.
    pub request_key: String,
    /// Original 64-character lowercase hex `preview_digest` bound to the receipt.
    pub expected_preview_digest: String,
    /// Optional 64-character lowercase hex `receipt_digest` for additional pinning.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_receipt_digest: Option<String>,
}

impl ReconcileExternalResolutionRequest {
    fn validate(&self) -> Result<(), NativeError> {
        if !self.trusted_local_attested {
            return Err(NativeError::new(
                NativeErrorCode::UnauthorizedOperator,
                "reconciling external resolution durability requires trusted_local_attested = true",
            ));
        }
        validate_bounded_identifier(&self.binding_id, "binding_id", MAX_COWORKER_ID_LEN)?;
        validate_canonical_thread_uuid("thread_uuid", &self.thread_uuid)
            .map_err(|_| invalid("thread_uuid must be a canonical lowercase non-nil UUID"))?;
        validate_bounded_identifier(&self.request_key, "request_key", MAX_COWORKER_ID_LEN)?;
        validate_sha256_hex_digest(&self.expected_preview_digest, "expected_preview_digest")
            .map_err(|_| {
                invalid("expected_preview_digest must be a 64-character lowercase hex SHA-256")
            })?;
        if let Some(ref rd) = self.expected_receipt_digest {
            validate_sha256_hex_digest(rd, "expected_receipt_digest").map_err(|_| {
                invalid("expected_receipt_digest must be a 64-character lowercase hex SHA-256")
            })?;
        }
        Ok(())
    }
}

/// Service owning `external_resolution_receipts_v1` `preview`, `record`, `inspect`, and `reconcile`
/// over [`CoworkerWorkflowStore`].
#[derive(Clone)]
pub struct ExternalResolutionService {
    workflow: CoworkerWorkflowStore,
    controller: ControllerPolicyStore,
    authority_dir: PathBuf,
    registry_dir: PathBuf,
    sessions_dir: Option<PathBuf>,
    ownership_dir: PathBuf,
    commands_dir: PathBuf,
    witnesses: Arc<Mutex<WitnessMap>>,
    post_persist_probe: Option<fn(&Path) -> Result<(), NativeError>>,
}

impl std::fmt::Debug for ExternalResolutionService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExternalResolutionService")
            .field("plans_dir", &self.workflow.plans_dir())
            .field("authority_dir", &self.authority_dir)
            .field("registry_dir", &self.registry_dir)
            .field("sessions_dir", &self.sessions_dir)
            .field("ownership_dir", &self.ownership_dir)
            .field("commands_dir", &self.commands_dir)
            .finish_non_exhaustive()
    }
}

impl ExternalResolutionService {
    /// Opens an [`ExternalResolutionService`] over existing private (`0700`) stores without
    /// creating directories, lockfiles, journals, or `fsync` calls.
    pub fn new(
        plans_dir: &Path,
        authority_dir: &Path,
        registry_dir: &Path,
        sessions_dir: Option<&Path>,
    ) -> Result<Self, NativeError> {
        ensure_platform_supported()?;
        let clean_plans = validate_existing_private_dir_readonly(plans_dir, "plans_dir")?;
        let clean_authority =
            validate_existing_private_dir_readonly(authority_dir, "authority_dir")?;
        let clean_registry = validate_existing_private_dir_readonly(registry_dir, "registry_dir")?;
        let clean_sessions = match sessions_dir {
            Some(dir) => Some(validate_existing_private_dir_readonly(dir, "sessions_dir")?),
            None => None,
        };

        if paths_overlap(&clean_authority, &clean_plans)
            || paths_overlap(&clean_authority, &clean_registry)
            || clean_sessions
                .as_ref()
                .is_some_and(|s| paths_overlap(&clean_authority, s))
        {
            return Err(invalid(
                "authority_dir must remain isolated from plans_dir, registry_dir, and sessions_dir",
            ));
        }

        let workflow = CoworkerWorkflowStore::new(&clean_plans)?;
        let controller = ControllerPolicyStore::new(&clean_plans)?;
        AuthorityStore::read_snapshot_readonly_at(&clean_authority)
            .map_err(map_authority_read_error)?;
        RegistryStore::read_snapshot_readonly_at(&clean_registry)
            .map_err(|_| invalid("cannot read registry snapshot"))?;
        PlanStore::read_snapshot_readonly_at(&clean_plans)
            .map_err(|_| invalid("cannot read plan snapshot"))?;

        let ownership_dir = coworker_driver_dir(&clean_plans);
        let commands_dir = planning_commands_dir(&clean_plans);

        Ok(Self {
            workflow,
            controller,
            authority_dir: clean_authority,
            registry_dir: clean_registry,
            sessions_dir: clean_sessions,
            ownership_dir,
            commands_dir,
            witnesses: Arc::new(Mutex::new(BTreeMap::new())),
            post_persist_probe: None,
        })
    }

    /// Overrides the physical job-ownership lock directory (defaults to `coworker_driver_dir(plans_dir)`).
    ///
    /// Validates path syntax without creating the directory.
    pub fn with_ownership_dir(mut self, ownership_dir: &Path) -> Result<Self, NativeError> {
        let clean = validate_absolute_no_symlink_components(ownership_dir, "ownership_dir")?;
        self.ownership_dir = clean;
        Ok(self)
    }

    /// Overrides the planning command ledger directory (defaults to `planning_commands_dir(plans_dir)`).
    ///
    /// Validates path syntax without creating the directory.
    pub fn with_commands_dir(mut self, commands_dir: &Path) -> Result<Self, NativeError> {
        let clean = validate_absolute_no_symlink_components(commands_dir, "commands_dir")?;
        self.commands_dir = clean;
        Ok(self)
    }

    /// Installs a deterministic post-persist durability probe for fault-injection tests.
    #[doc(hidden)]
    #[must_use]
    pub fn with_post_persist_probe(mut self, probe: fn(&Path) -> Result<(), NativeError>) -> Self {
        self.post_persist_probe = Some(probe);
        self
    }

    /// Returns the underlying [`CoworkerWorkflowStore`].
    #[must_use]
    pub fn workflow_store(&self) -> &CoworkerWorkflowStore {
        &self.workflow
    }

    /// Computes a pure read-only [`ExternalResolutionPreview`] without creating any directory,
    /// lockfile, journal, temporary file, `fsync`, or subprocess.
    pub fn preview(
        &self,
        request: &RecordExternalResolutionRequest,
    ) -> Result<ExternalResolutionPreview, NativeError> {
        request.validate()?;
        let normalized = request.normalized();
        let canonical_repo =
            verify_canonical_git_repo_dir_readonly(&normalized.canonical_repo_root)?;

        let ledger = self.workflow.read_binding_ledger(&normalized.binding_id)?;
        if let Some(ref pinned_thread) = ledger.thread_uuid {
            if pinned_thread != &normalized.thread_uuid {
                return Err(NativeError::new(
                    NativeErrorCode::SecurityViolation,
                    "request.thread_uuid does not match the thread pinned to this binding ledger",
                ));
            }
        }
        if ledger.external_resolutions.len() >= MAX_EXTERNAL_RESOLUTIONS_PER_BINDING {
            return Err(invalid(
                "binding ledger reached MAX_EXTERNAL_RESOLUTIONS_PER_BINDING capacity",
            ));
        }
        if ledger
            .external_resolutions
            .iter()
            .any(|r| r.request_key == normalized.request_key)
        {
            return Err(invalid(
                "request_key is already recorded in external_resolutions; inspect or replay record with original confirmation",
            ));
        }

        // Reject any ambiguous read-only planning reservation in the binding ledger.
        verify_no_ambiguous_planning_reservations(&ledger)?;

        // Pure read-only check of planning commands if commands_dir exists on disk.
        self.verify_planning_commands_quiescent_readonly(
            &normalized.binding_id,
            &normalized.thread_uuid,
        )?;

        let approvals = AuthorityStore::read_snapshot_readonly_at(&self.authority_dir)
            .map_err(map_authority_read_error)?;
        let plans = PlanStore::read_snapshot_readonly_at(self.workflow.plans_dir())
            .map_err(|_| invalid("cannot read plan store snapshot"))?;
        let registry = RegistryStore::read_snapshot_readonly_at(&self.registry_dir)
            .map_err(|_| invalid("cannot read registry store snapshot"))?;

        let resolved = self.resolve_and_verify_target_snapshot(
            &normalized,
            &canonical_repo,
            &ledger,
            &approvals,
            &plans,
            &registry,
            None,
        )?;

        // In pure read-only preview, probe physical ownership and host state without creating locks/dirs.
        self.verify_ownership_free_readonly(
            &normalized.binding_id,
            &normalized.thread_uuid,
            &resolved.owner_target_id,
        )?;
        self.verify_conversation_changes_unblocked_readonly(
            &normalized.binding_id,
            resolved
                .bound_approval
                .as_ref()
                .map(|a| a.approval_id.as_str()),
            &resolved.resolved_task_ids,
        )?;
        if let Some(ref approval) = resolved.bound_approval {
            self.verify_host_quiescent_readonly(approval)?;
        }

        build_preview_from_resolved(&normalized, &ledger, resolved)
    }

    /// Atomically appends an immutable [`ExternalResolutionReceipt`] to
    /// `CoworkerBindingLedger::external_resolutions` after verifying separate trusted-local
    /// operator confirmation, holding live ownership/advancement/host/store guards in caller lock
    /// order, and re-verifying all CAS revisions and runtime/host quiescence under the guards.
    ///
    /// Repeating an exact `(request, confirmation)` whose durability witness is already established
    /// returns the existing receipt immediately without acquiring locks, journals, syncs, or writes.
    pub fn record(
        &self,
        request: &RecordExternalResolutionRequest,
        confirmation: &ExternalResolutionConfirmation,
    ) -> Result<ExternalResolutionReceipt, NativeError> {
        request.validate()?;
        let normalized = request.normalized();
        let request_digest = compute_request_digest(&normalized)?;
        let ledger_path = self
            .workflow
            .ledger_path_for_binding(&normalized.binding_id)?;

        // Fast pure read-only duplicate / key-conflict / uncertain-durability check before any lock or dir creation.
        let initial_ledger = self.workflow.read_binding_ledger(&normalized.binding_id)?;
        if let Some(ref pinned_thread) = initial_ledger.thread_uuid {
            if pinned_thread != &normalized.thread_uuid {
                return Err(NativeError::new(
                    NativeErrorCode::SecurityViolation,
                    "request.thread_uuid does not match the thread pinned to this binding ledger",
                ));
            }
        }
        if let Some(existing) = initial_ledger
            .external_resolutions
            .iter()
            .find(|r| r.request_key == normalized.request_key)
        {
            return self.handle_existing_receipt_on_record(
                existing,
                &normalized,
                &request_digest,
                confirmation,
                &ledger_path,
            );
        }

        // Validate full preview and operator confirmation before creating or acquiring any guard.
        let pre_guard_preview = self.preview(&normalized)?;
        confirmation.validate_for_request_and_digest(
            &normalized.thread_uuid,
            &pre_guard_preview.preview_digest,
        )?;

        let owner_target_id = owner_target_id_for_preview(&pre_guard_preview)?;
        let now_ms = current_unix_epoch_ms()?;

        // 1. Physical CoworkerJobOwnershipGuard (outermost lock, non-blocking).
        let _owner_guard = self.acquire_physical_ownership_guard(
            &normalized.binding_id,
            &normalized.thread_uuid,
            &owner_target_id,
            now_ms,
        )?;

        // 2. Per-binding conversation advancement lock (acquired before host locks, matching dispatch_guard).
        let _conversation_guard = acquire_conversation_advancement_lock_sync(
            self.workflow.plans_dir(),
            &normalized.binding_id,
        )?;

        // 3. Host launch-intent & session locks (when an approval is bound, either pre-known or re-verified).
        let pre_approval = match pre_guard_preview.approval_id {
            Some(ref aid) => Some(
                AuthorityStore::get_for_operator_readonly_at(&self.authority_dir, aid)
                    .map_err(map_authority_read_error)?,
            ),
            None => None,
        };
        let _host_guards = match pre_approval {
            Some(ref approval) => Some(self.acquire_and_verify_host_guards(approval)?),
            None => None,
        };

        // 4. Per-binding PlanningCommandStore lock + quiescence check.
        let (_planning_cmd_guard, cmd_ledger) =
            self.acquire_planning_command_guard(&normalized.binding_id)?;
        verify_planning_command_ledger_quiescent(&cmd_ledger, &normalized.thread_uuid)?;

        // 5. Per-binding ControllerPolicyStore lock + runtime snapshot (runtime for Job targets).
        let controller_locked = match &normalized.target {
            ExternalResolutionTarget::Job { job_id } => Some(
                self.controller
                    .acquire_binding_lock_and_read_job_runtime(&normalized.binding_id, job_id)?,
            ),
            ExternalResolutionTarget::LegacyPlan { .. } => {
                let (guard, controller_ledger_revision) = self
                    .controller
                    .acquire_binding_lock_and_read_ledger_revision(&normalized.binding_id)?;
                Some((guard, controller_ledger_revision, None))
            }
            ExternalResolutionTarget::CommitOnly { .. } => unreachable!(),
        };
        let locked_controller_snapshot = controller_locked
            .as_ref()
            .map(|(_, ctrl_rev, rt)| (*ctrl_rev, rt.clone()));

        // 6. Per-binding CoworkerWorkflowStore atomic CAS transaction (innermost lock).
        let probe = self.post_persist_probe;
        let (_, receipt) = self.workflow.transact_binding_ledger_with_post_persist_probe(
            &normalized.binding_id,
            Some(pre_guard_preview.source_ledger_revision),
            |working_ledger| {
                if let Some(existing) = working_ledger
                    .external_resolutions
                    .iter()
                    .find(|r| r.request_key == normalized.request_key)
                    .cloned()
                {
                    let confirmed = self.handle_existing_receipt_on_record(
                        &existing,
                        &normalized,
                        &request_digest,
                        confirmation,
                        &ledger_path,
                    )?;
                    return Ok(CoworkerLedgerMutationOutcome::IdempotentExisting(confirmed));
                }

                if working_ledger.external_resolutions.len()
                    >= MAX_EXTERNAL_RESOLUTIONS_PER_BINDING
                {
                    return Err(invalid(
                        "binding ledger reached MAX_EXTERNAL_RESOLUTIONS_PER_BINDING capacity",
                    ));
                }
                if working_ledger.ledger_revision != pre_guard_preview.source_ledger_revision {
                    return Err(invalid(
                        "source_ledger_revision changed concurrently before external resolution append",
                    ));
                }

                verify_no_ambiguous_planning_reservations(working_ledger)?;

                let canonical_repo =
                    verify_canonical_git_repo_dir_readonly(&normalized.canonical_repo_root)?;
                let approvals = AuthorityStore::read_snapshot_readonly_at(&self.authority_dir)
                    .map_err(map_authority_read_error)?;
                let plans = PlanStore::read_snapshot_readonly_at(self.workflow.plans_dir())
                    .map_err(|_| invalid("cannot read plan store snapshot"))?;
                let registry = RegistryStore::read_snapshot_readonly_at(&self.registry_dir)
                    .map_err(|_| invalid("cannot read registry store snapshot"))?;

                let resolved = self.resolve_and_verify_target_snapshot(
                    &normalized,
                    &canonical_repo,
                    working_ledger,
                    &approvals,
                    &plans,
                    &registry,
                    locked_controller_snapshot.as_ref(),
                )?;

                // Ensure approval identity did not appear/change after host guard acquisition.
                if resolved.bound_approval.as_ref().map(|a| &a.approval_id)
                    != pre_guard_preview.approval_id.as_ref()
                {
                    return Err(invalid(
                        "bound approval_id changed concurrently before external resolution append",
                    ));
                }
                self.verify_conversation_changes_unblocked_readonly(
                    &normalized.binding_id,
                    resolved
                        .bound_approval
                        .as_ref()
                        .map(|a| a.approval_id.as_str()),
                    &resolved.resolved_task_ids,
                )?;
                if let Some(ref approval) = resolved.bound_approval {
                    self.verify_host_quiescent_under_held_guards(approval)?;
                }

                let locked_preview =
                    build_preview_from_resolved(&normalized, working_ledger, resolved)?;
                if locked_preview != pre_guard_preview
                    || !constant_time_str_eq(
                        &locked_preview.preview_digest,
                        &pre_guard_preview.preview_digest,
                    )
                {
                    return Err(invalid(
                        "target, revisions, or digests drifted from confirmed preview before persistence",
                    ));
                }
                confirmation.validate_for_request_and_digest(
                    &normalized.thread_uuid,
                    &locked_preview.preview_digest,
                )?;

                working_ledger.ensure_thread_scope(&normalized.thread_uuid)?;
                let receipt = ExternalResolutionReceipt::from_preview(&locked_preview)?;
                working_ledger.external_resolutions.push(receipt.clone());
                Ok(CoworkerLedgerMutationOutcome::Mutated(receipt))
            },
            |persisted_path| match probe {
                Some(hook) => hook(persisted_path),
                None => Ok(()),
            },
        )?;

        self.remember_durability_witness(ledger_path, &receipt)?;
        Ok(receipt)
    }

    /// Pure read-only inspection of all external resolution receipts and their current durability
    /// and target applicability for `(binding_id, thread_uuid)`.
    ///
    /// Never creates directories, lockfiles, journals, `fsync` calls, or writes.
    pub fn inspect(
        &self,
        binding_id: &str,
        thread_uuid: &str,
    ) -> Result<ExternalResolutionInspection, NativeError> {
        validate_bounded_identifier(binding_id, "binding_id", MAX_COWORKER_ID_LEN)?;
        validate_canonical_thread_uuid("thread_uuid", thread_uuid)
            .map_err(|_| invalid("thread_uuid must be a canonical lowercase non-nil UUID"))?;

        let ledger = self.workflow.read_binding_ledger(binding_id)?;
        if let Some(ref pinned) = ledger.thread_uuid {
            if pinned.as_str() != thread_uuid {
                return Err(NativeError::new(
                    NativeErrorCode::SecurityViolation,
                    "thread_uuid does not match the thread pinned to this binding ledger",
                ));
            }
        }
        let ledger_path = self.workflow.ledger_path_for_binding(binding_id)?;
        let approvals = AuthorityStore::read_snapshot_readonly_at(&self.authority_dir)
            .map_err(map_authority_read_error)?;
        let plans = PlanStore::read_snapshot_readonly_at(self.workflow.plans_dir())
            .map_err(|_| invalid("cannot read plan store snapshot"))?;

        let mut receipts = Vec::with_capacity(ledger.external_resolutions.len());
        let mut has_uncertain_durability = false;
        for receipt in &ledger.external_resolutions {
            let durability = if self.has_durability_witness(&ledger_path, receipt)? {
                ExternalResolutionDurability::VerifiedDurable
            } else {
                has_uncertain_durability = true;
                ExternalResolutionDurability::PersistenceUncertain
            };
            let runtime_for_job = match &receipt.target {
                ExternalResolutionTarget::Job { job_id } => self
                    .controller
                    .get_job_runtime_readonly(binding_id, job_id)?,
                ExternalResolutionTarget::LegacyPlan { .. }
                | ExternalResolutionTarget::CommitOnly { .. } => None,
            };
            let applicability = evaluate_receipt_applicability(
                receipt,
                &ledger,
                &approvals,
                &plans,
                runtime_for_job.as_ref(),
                self.workflow.plans_dir(),
            )?;
            receipts.push(ExternalResolutionReceiptInspection {
                receipt: receipt.clone(),
                durability,
                applicability,
            });
        }

        Ok(ExternalResolutionInspection {
            interface_version: EXTERNAL_RESOLUTION_INTERFACE_VERSION.to_string(),
            binding_id: binding_id.to_string(),
            thread_uuid: thread_uuid.to_string(),
            current_ledger_revision: ledger.ledger_revision,
            has_uncertain_durability,
            receipts,
        })
    }

    /// Pure read-only lookup and applicability/durability inspection of a single stored receipt
    /// by `request_key`.
    pub fn inspect_by_request_key(
        &self,
        binding_id: &str,
        thread_uuid: &str,
        request_key: &str,
    ) -> Result<Option<ExternalResolutionReceiptInspection>, NativeError> {
        validate_bounded_identifier(request_key, "request_key", MAX_COWORKER_ID_LEN)?;
        let all = self.inspect(binding_id, thread_uuid)?;
        Ok(all
            .receipts
            .into_iter()
            .find(|entry| entry.receipt.request_key == request_key))
    }

    /// Establishes file-and-directory `fsync` durability strictly for an existing, unchanged
    /// [`ExternalResolutionReceipt`] already persisted in `CoworkerBindingLedger`, bound to its
    /// original `request_key` and `expected_preview_digest`.
    ///
    /// Never writes or rewrites ledger bytes, never increments `ledger_revision`, and never
    /// retries a missing write.
    pub fn reconcile(
        &self,
        request: &ReconcileExternalResolutionRequest,
    ) -> Result<ExternalResolutionReceipt, NativeError> {
        request.validate()?;
        let matched: Mutex<Option<ExternalResolutionReceipt>> = Mutex::new(None);
        let (ledger_path, _) = self.workflow.sync_existing_binding_ledger_durability(
            &request.binding_id,
            |ledger| {
                if ledger.thread_uuid.as_deref() != Some(request.thread_uuid.as_str()) {
                    return Err(NativeError::new(
                        NativeErrorCode::SecurityViolation,
                        "reconcile thread_uuid does not match the thread pinned to this binding ledger",
                    ));
                }
                let found = ledger
                    .external_resolutions
                    .iter()
                    .find(|r| r.request_key == request.request_key)
                    .ok_or_else(|| {
                        invalid(
                            "cannot reconcile external resolution: no receipt with request_key exists on disk",
                        )
                    })?;
                found.validate_integrity()?;
                if !constant_time_str_eq(
                    &found.preview_digest,
                    &request.expected_preview_digest,
                ) {
                    return Err(NativeError::new(
                        NativeErrorCode::UnauthorizedOperator,
                        "expected_preview_digest does not match the stored receipt's preview_digest",
                    ));
                }
                if let Some(ref expected_rd) = request.expected_receipt_digest {
                    if !constant_time_str_eq(&found.receipt_digest, expected_rd) {
                        return Err(NativeError::new(
                            NativeErrorCode::UnauthorizedOperator,
                            "expected_receipt_digest does not match the stored receipt's receipt_digest",
                        ));
                    }
                }
                let mut guard = matched
                    .lock()
                    .map_err(|_| invalid("reconcile state lock poisoned"))?;
                *guard = Some(found.clone());
                Ok(())
            },
        )?;

        let receipt = matched
            .into_inner()
            .map_err(|_| invalid("reconcile state lock poisoned"))?
            .ok_or_else(|| invalid("missing matched receipt after durability sync"))?;
        self.remember_durability_witness(ledger_path, &receipt)?;
        Ok(receipt)
    }

    fn handle_existing_receipt_on_record(
        &self,
        existing: &ExternalResolutionReceipt,
        normalized_request: &RecordExternalResolutionRequest,
        request_digest: &str,
        confirmation: &ExternalResolutionConfirmation,
        ledger_path: &Path,
    ) -> Result<ExternalResolutionReceipt, NativeError> {
        existing.validate_integrity()?;
        if existing.binding_id != normalized_request.binding_id
            || existing.thread_uuid != normalized_request.thread_uuid
            || !constant_time_str_eq(&existing.request_digest, request_digest)
        {
            return Err(invalid(
                "conflicting reuse of external resolution request_key with different request content",
            ));
        }
        confirmation.validate_for_request_and_digest(
            &normalized_request.thread_uuid,
            &existing.preview_digest,
        )?;
        if !self.has_durability_witness(ledger_path, existing)? {
            return Err(NativeError::persistence_uncertain(
                "external resolution receipt is visible on disk with unconfirmed durability; inspect and run explicit existing-byte reconcile",
            ));
        }
        Ok(existing.clone())
    }

    #[allow(clippy::too_many_arguments)]
    fn resolve_and_verify_target_snapshot(
        &self,
        request: &RecordExternalResolutionRequest,
        canonical_repo: &Path,
        ledger: &CoworkerBindingLedger,
        approvals: &[ApprovalRecord],
        plans: &[PlanRevision],
        registry: &threadlane_workflow::RegistrySnapshot,
        locked_controller_snapshot: Option<&(u64, Option<ControllerJobRuntimeState>)>,
    ) -> Result<ResolvedTargetSnapshot, NativeError> {
        if let Some(expected_ledger_rev) = request.expected_ledger_revision {
            if ledger.ledger_revision != expected_ledger_rev {
                return Err(invalid(
                    "expected_ledger_revision does not match current CoworkerBindingLedger.ledger_revision",
                ));
            }
        }

        match &request.target {
            ExternalResolutionTarget::Job { job_id } => {
                let job = ledger
                    .jobs
                    .iter()
                    .find(|j| &j.job_id == job_id)
                    .ok_or_else(|| {
                        invalid("target job_id does not exist in CoworkerBindingLedger")
                    })?;
                if job.binding_id != request.binding_id || job.thread_uuid != request.thread_uuid {
                    return Err(NativeError::new(
                        NativeErrorCode::SecurityViolation,
                        "target job does not belong to the requested binding_id and thread_uuid",
                    ));
                }
                if job.phase == CoworkerJobPhase::Cancelled
                    || ledger.jobs.iter().any(|other| {
                        other.supersedes_job_id.as_deref() == Some(job.job_id.as_str())
                    })
                {
                    return Err(invalid(
                        "superseded CoworkerJobRecord cannot record a new external resolution",
                    ));
                }
                if job.phase == CoworkerJobPhase::Completed {
                    return Err(invalid(
                        "completed CoworkerJobRecord already finished workflow closure",
                    ));
                }
                if let Some(expected_job_rev) = request.expected_job_revision {
                    if job.job_revision != expected_job_rev {
                        return Err(invalid(
                            "expected_job_revision does not match current CoworkerJobRecord.job_revision",
                        ));
                    }
                }

                verify_job_quiescent(job)?;

                let job_repo = PathBuf::from(job.work_brief.project_base.repository_path.as_str());
                if job_repo != canonical_repo {
                    return Err(invalid(
                        "request.canonical_repo_root does not match job.work_brief.project_base.repository_path",
                    ));
                }
                let project = ExternalResolutionProjectRef {
                    canonical_repo_root: job_repo,
                    base_commit: job.work_brief.project_base.base_commit.clone(),
                    base_tree_sha: Some(job.work_brief.project_base.base_tree_sha.clone()),
                };
                project.validate_syntax()?;

                let resolved_task_ids = match &request.scope {
                    ExternalResolutionScope::FullJob => {
                        let mut ids: Vec<String> =
                            job.tasks.iter().map(|t| t.task_id.clone()).collect();
                        ids.sort();
                        ids
                    }
                    ExternalResolutionScope::Tasks { task_ids } => {
                        if job.tasks.is_empty() {
                            return Err(invalid(
                                "task-scoped external resolution requires bound tasks on the target job",
                            ));
                        }
                        for tid in task_ids {
                            if !job.tasks.iter().any(|t| &t.task_id == tid) {
                                return Err(invalid(
                                    "scope.task_ids references a task_id absent from the target job",
                                ));
                            }
                        }
                        task_ids.clone()
                    }
                };

                let (ctrl_ledger_rev, runtime_opt) = match locked_controller_snapshot {
                    Some((rev, rt)) => (*rev, rt.clone()),
                    None => self
                        .controller
                        .get_binding_runtime_and_job_readonly(&request.binding_id, &job.job_id)?,
                };
                if let Some(expected_rt_rev) = request.expected_runtime_revision {
                    let actual_rt_rev = runtime_opt.as_ref().map(|r| r.runtime_revision);
                    if actual_rt_rev != Some(expected_rt_rev) {
                        return Err(invalid(
                            "expected_runtime_revision does not match current ControllerJobRuntimeState.runtime_revision",
                        ));
                    }
                }
                let mut envelope_digest = None;
                if let Some(ref runtime_state) = runtime_opt {
                    runtime_state.envelope.verify_matches_job(job)?;
                    if let Some(ref lease) = runtime_state.lease {
                        if lease.active {
                            return Err(invalid(
                                "cannot record external resolution while controller job lease is active",
                            ));
                        }
                    }
                    if !runtime_state.unresolved_effects().is_empty() {
                        let has_unknown = runtime_state.unresolved_effects().iter().any(|eff| {
                            eff.status
                                == crate::coworker_workflow::PendingControllerEffectStatus::EffectUnknown
                        });
                        let code = if has_unknown {
                            NativeErrorCode::AmbiguousWakeupPendingReconciliation
                        } else {
                            NativeErrorCode::ValidationFailed
                        };
                        return Err(NativeError::new(
                            code,
                            "cannot record external resolution while Pending or Unknown controller effects exist",
                        ));
                    }
                    envelope_digest = Some(runtime_state.envelope.envelope_digest.clone());
                }

                let mut plan_id = None;
                let mut plan_revision = None;
                let mut plan_digest = None;
                if let Some(ref pr) = job.planning_result {
                    let plan_rev = plans
                        .iter()
                        .find(|p| p.plan_id == pr.plan_id && p.revision == pr.plan_revision)
                        .ok_or_else(|| {
                            invalid("job.planning_result plan revision is missing from PlanStore")
                        })?;
                    if plan_rev.binding_id != request.binding_id
                        || !plan_rev.status.is_active_published()
                    {
                        return Err(invalid(
                            "job.planning_result plan revision is no longer active-published in PlanStore",
                        ));
                    }
                    plan_id = Some(pr.plan_id.clone());
                    plan_revision = Some(pr.plan_revision);
                    plan_digest = Some(pr.plan_digest.clone());
                }

                let mut bound_approval = None;
                let mut approval_id = None;
                let mut approval_subject_digest = None;
                if let Some(ref linkage) = job.approval_linkage {
                    if linkage.project_base != job.work_brief.project_base {
                        return Err(invalid(
                            "job.approval_linkage.project_base drifted from job.work_brief.project_base",
                        ));
                    }
                    let approval = approvals
                        .iter()
                        .find(|a| a.approval_id == linkage.approval_id)
                        .ok_or_else(|| {
                            invalid("job.approval_linkage references an ApprovalRecord missing from AuthorityStore")
                        })?
                        .clone();
                    if approval.binding_id != request.binding_id
                        || approval.thread_uuid != request.thread_uuid
                        || approval.subject_digest != linkage.approval_subject_digest
                        || approval.repository_path != canonical_repo
                        || approval.base_commit != job.work_brief.project_base.base_commit
                    {
                        return Err(invalid(
                            "bound ApprovalRecord does not match job scope, subject_digest, repository, or base_commit",
                        ));
                    }
                    let plan_rev = plans
                        .iter()
                        .find(|p| {
                            p.plan_id == approval.plan_id && p.revision == approval.plan_revision
                        })
                        .ok_or_else(|| {
                            invalid("bound ApprovalRecord plan revision is missing from PlanStore")
                        })?;
                    if plan_rev.binding_id != request.binding_id
                        || !plan_rev.status.is_active_published()
                    {
                        return Err(invalid(
                            "bound ApprovalRecord plan revision is not active-published in PlanStore",
                        ));
                    }
                    plan_id = Some(approval.plan_id.clone());
                    plan_revision = Some(approval.plan_revision);
                    plan_digest = Some(approval.plan_digest.clone());
                    approval_id = Some(approval.approval_id.clone());
                    approval_subject_digest = Some(approval.subject_digest.clone());
                    bound_approval = Some(approval);
                }

                Ok(ResolvedTargetSnapshot {
                    owner_target_id: job.job_id.clone(),
                    project,
                    resolved_task_ids,
                    source_job_revision: Some(job.job_revision),
                    source_controller_ledger_revision: ctrl_ledger_rev,
                    source_runtime_revision: runtime_opt.map(|r| r.runtime_revision),
                    work_brief_digest: Some(job.work_brief_digest.clone()),
                    plan_id,
                    plan_revision,
                    plan_digest,
                    approval_id,
                    approval_subject_digest,
                    envelope_digest,
                    bound_approval,
                })
            }
            ExternalResolutionTarget::LegacyPlan {
                plan_id,
                plan_revision,
            } => {
                if ledger.jobs.iter().any(|j| {
                    j.planning_result
                        .as_ref()
                        .is_some_and(|pr| &pr.plan_id == plan_id)
                        || j.approval_linkage
                            .as_ref()
                            .is_some_and(|al| &al.plan_id == plan_id)
                }) {
                    return Err(NativeError::new(
                        NativeErrorCode::UnsupportedCapability,
                        "plan_id is already owned by a CoworkerJobRecord; target the job instead of legacy_plan",
                    ));
                }

                let plan = plans
                    .iter()
                    .find(|p| &p.plan_id == plan_id && p.revision == *plan_revision)
                    .ok_or_else(|| {
                        NativeError::new(
                            NativeErrorCode::UnsupportedCapability,
                            "legacy plan revision does not exist in PlanStore",
                        )
                    })?;
                if plan.binding_id != request.binding_id {
                    return Err(NativeError::new(
                        NativeErrorCode::SecurityViolation,
                        "legacy plan revision belongs to a different binding_id",
                    ));
                }
                if !plan.status.is_active_published() {
                    return Err(NativeError::new(
                        NativeErrorCode::UnsupportedCapability,
                        "legacy plan revision is not an active published revision",
                    ));
                }
                if plans
                    .iter()
                    .filter(|p| &p.plan_id == plan_id && p.status.is_active_published())
                    .any(|p| p.revision > *plan_revision)
                {
                    return Err(NativeError::new(
                        NativeErrorCode::UnsupportedCapability,
                        "legacy plan revision has been superseded by a newer published revision",
                    ));
                }

                // Legacy PlanRevision alone does not bind thread_uuid or canonical project base;
                // require a unique verified ApprovalRecord in AuthorityStore for (plan_id, plan_revision).
                let matching_approvals: Vec<&ApprovalRecord> = approvals
                    .iter()
                    .filter(|a| &a.plan_id == plan_id && a.plan_revision == *plan_revision)
                    .collect();
                if matching_approvals.len() != 1 {
                    return Err(NativeError::new(
                        NativeErrorCode::UnsupportedCapability,
                        "legacy plan revision cannot be uniquely linked to a single verified ApprovalRecord in AuthorityStore",
                    ));
                }
                let approval = matching_approvals[0].clone();
                if approval.binding_id != request.binding_id
                    || approval.thread_uuid != request.thread_uuid
                {
                    return Err(NativeError::new(
                        NativeErrorCode::SecurityViolation,
                        "uniquely linked ApprovalRecord does not match requested binding_id and thread_uuid",
                    ));
                }
                if approval.repository_path != canonical_repo {
                    return Err(invalid(
                        "request.canonical_repo_root does not match the uniquely linked ApprovalRecord.repository_path",
                    ));
                }

                // When registry revision matches, cross-verify compiled plan_digest as well.
                if registry.revision == approval.registry_revision {
                    let compiled = compile_plan_revision(plan, registry)
                        .map_err(|_| invalid("cannot compile legacy published plan revision"))?;
                    if compiled.plan_digest != approval.plan_digest {
                        return Err(invalid(
                            "compiled legacy plan_digest drifted from ApprovalRecord.plan_digest",
                        ));
                    }
                }

                let approved_set: BTreeSet<&str> = approval
                    .approved_task_ids
                    .iter()
                    .map(String::as_str)
                    .collect();
                let plan_task_set: BTreeSet<&str> =
                    plan.tasks.iter().map(|t| t.id.as_str()).collect();
                let resolved_task_ids = match &request.scope {
                    ExternalResolutionScope::FullJob => {
                        let mut ids = approval.approved_task_ids.clone();
                        ids.sort();
                        ids
                    }
                    ExternalResolutionScope::Tasks { task_ids } => {
                        for tid in task_ids {
                            if !approved_set.contains(tid.as_str())
                                || !plan_task_set.contains(tid.as_str())
                            {
                                return Err(invalid(
                                    "scope.task_ids references a task_id absent from the legacy plan's approved tasks",
                                ));
                            }
                        }
                        task_ids.clone()
                    }
                };

                let ctrl_ledger_rev = match locked_controller_snapshot {
                    Some((revision, _)) => *revision,
                    None => self
                        .controller
                        .get_binding_ledger_revision_readonly(&request.binding_id)?,
                };
                let project = ExternalResolutionProjectRef {
                    canonical_repo_root: approval.repository_path.clone(),
                    base_commit: approval.base_commit.clone(),
                    base_tree_sha: None,
                };
                project.validate_syntax()?;

                Ok(ResolvedTargetSnapshot {
                    owner_target_id: approval.approval_id.clone(),
                    project,
                    resolved_task_ids,
                    source_job_revision: None,
                    source_controller_ledger_revision: ctrl_ledger_rev,
                    source_runtime_revision: None,
                    work_brief_digest: None,
                    plan_id: Some(plan.plan_id.clone()),
                    plan_revision: Some(plan.revision),
                    plan_digest: Some(approval.plan_digest.clone()),
                    approval_id: Some(approval.approval_id.clone()),
                    approval_subject_digest: Some(approval.subject_digest.clone()),
                    envelope_digest: None,
                    bound_approval: Some(approval),
                })
            }
            ExternalResolutionTarget::CommitOnly { .. } => Err(NativeError::new(
                NativeErrorCode::UnsupportedCapability,
                "raw commit SHA alone does not prove binding, thread, or workflow target scope",
            )),
        }
    }

    fn verify_planning_commands_quiescent_readonly(
        &self,
        binding_id: &str,
        thread_uuid: &str,
    ) -> Result<(), NativeError> {
        let clean = validate_absolute_no_symlink_components(&self.commands_dir, "commands_dir")?;
        match fs::symlink_metadata(&clean) {
            Ok(meta) => {
                if meta.file_type().is_symlink() || !meta.file_type().is_dir() {
                    return Err(NativeError::new(
                        NativeErrorCode::SecurityViolation,
                        "commands_dir must be a non-symlink private directory",
                    ));
                }
                let store = PlanningCommandStore::open_existing(
                    &clean,
                    self.workflow.clone(),
                    &self.registry_dir,
                )?;
                let cmd_ledger = store.read_binding_ledger(binding_id)?;
                verify_planning_command_ledger_quiescent(&cmd_ledger, thread_uuid)
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(NativeError::new(
                NativeErrorCode::IoError,
                format!(
                    "cannot inspect commands_dir metadata: {}",
                    sanitize_diagnostic_text(&err.to_string())
                ),
            )),
        }
    }

    fn acquire_planning_command_guard(
        &self,
        binding_id: &str,
    ) -> Result<
        (
            CrossProcessLockGuard,
            crate::planning_commands::PlanningCommandBindingLedger,
        ),
        NativeError,
    > {
        let clean = prepare_private_dir(&self.commands_dir, "planning_commands_dir")?;
        let store =
            PlanningCommandStore::open_existing(&clean, self.workflow.clone(), &self.registry_dir)?;
        store.acquire_binding_lock_and_read_ledger(binding_id)
    }

    fn verify_ownership_free_readonly(
        &self,
        binding_id: &str,
        thread_uuid: &str,
        owner_target_id: &str,
    ) -> Result<(), NativeError> {
        let clean = validate_absolute_no_symlink_components(&self.ownership_dir, "ownership_dir")?;
        match fs::symlink_metadata(&clean) {
            Ok(meta) => {
                if meta.file_type().is_symlink() || !meta.file_type().is_dir() {
                    return Err(NativeError::new(
                        NativeErrorCode::SecurityViolation,
                        "ownership_dir must be a non-symlink private directory",
                    ));
                }
                let store = CoworkerJobOwnershipStore::new(&clean)?;
                let key = CoworkerJobOwnershipKey::new(binding_id, thread_uuid, owner_target_id)?;
                match store.inspect_job_lock(&key)? {
                    CoworkerJobLockState::HeldByActiveProcess { .. } => Err(invalid(
                        "target job/approval is currently owned by a live controller/driver process",
                    )),
                    CoworkerJobLockState::Absent
                    | CoworkerJobLockState::UnlockedStaleFile { .. } => Ok(()),
                }
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(NativeError::new(
                NativeErrorCode::IoError,
                format!(
                    "cannot inspect ownership_dir metadata: {}",
                    sanitize_diagnostic_text(&err.to_string())
                ),
            )),
        }
    }

    fn acquire_physical_ownership_guard(
        &self,
        binding_id: &str,
        thread_uuid: &str,
        owner_target_id: &str,
        now_ms: u64,
    ) -> Result<CoworkerJobOwnershipGuard, NativeError> {
        let clean = prepare_private_dir(&self.ownership_dir, "ownership_dir")?;
        let store = CoworkerJobOwnershipStore::new(&clean)?;
        let key = CoworkerJobOwnershipKey::new(binding_id, thread_uuid, owner_target_id)?;
        match store.try_acquire_job(&key, now_ms)? {
            CoworkerOwnershipAcquireOutcome::Acquired(guard) => Ok(guard),
            CoworkerOwnershipAcquireOutcome::Busy { .. } => Err(invalid(
                "target job/approval is currently owned by a live controller/driver process",
            )),
        }
    }

    fn verify_conversation_changes_unblocked_readonly(
        &self,
        binding_id: &str,
        approval_id: Option<&str>,
        resolved_task_ids: &[String],
    ) -> Result<(), NativeError> {
        let plans_dir = self.workflow.plans_dir();
        let top_summary =
            inspect_unresolved_blocking_changes(plans_dir, binding_id, approval_id, None)?;
        if top_summary.blocked || top_summary.approval_superseded {
            return Err(invalid(
                "unresolved blocking conversational change or superseded approval blocks external resolution",
            ));
        }
        if let Some(aid) = approval_id {
            for tid in resolved_task_ids {
                let task_summary = inspect_unresolved_blocking_changes(
                    plans_dir,
                    binding_id,
                    Some(aid),
                    Some(tid.as_str()),
                )?;
                if task_summary.blocked || task_summary.approval_superseded {
                    return Err(invalid(
                        "unresolved task-level conversational change blocks external resolution",
                    ));
                }
            }
        }
        Ok(())
    }

    fn require_verified_sessions_dir(&self) -> Result<PathBuf, NativeError> {
        let Some(ref sessions_dir) = self.sessions_dir else {
            return Err(NativeError::new(
                NativeErrorCode::UnsupportedCapability,
                "sessions_dir is required to prove host runtime quiescence for an approved target",
            ));
        };
        verify_existing_private_dir(sessions_dir).map_err(map_host_error)
    }

    fn verify_host_quiescent_readonly(&self, approval: &ApprovalRecord) -> Result<(), NativeError> {
        let sessions_dir = self.require_verified_sessions_dir()?;
        let launch_lock_path = sessions_dir
            .join(LAUNCH_INTENTS_SUBDIR)
            .join(format!("{}.lock", approval.approval_id));
        if probe_existing_lockfile_busy_readonly(&launch_lock_path)? {
            return Err(invalid(
                "host launch-intent lock is currently held by an active startup process",
            ));
        }
        verify_host_launch_intent_state(&sessions_dir, approval)?;

        let session_dir_candidate = sessions_dir.join(&approval.approval_id);
        let _ = validate_absolute_no_symlink_components(&session_dir_candidate, "session_dir")?;
        match fs::symlink_metadata(&session_dir_candidate) {
            Ok(_) => {
                let session_dir = verify_existing_session_dir(&sessions_dir, &approval.approval_id)
                    .map_err(map_host_error)?;
                if probe_existing_lockfile_busy_readonly(&session_dir.join(HOST_LOCK_FILENAME))? {
                    return Err(invalid(
                        "live ApprovedHost session lock is currently held for this approval",
                    ));
                }
                verify_stopped_host_session_contents(&session_dir, approval)
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(NativeError::new(
                NativeErrorCode::IoError,
                format!(
                    "cannot inspect host session directory metadata: {}",
                    sanitize_diagnostic_text(&err.to_string())
                ),
            )),
        }
    }

    fn acquire_and_verify_host_guards(
        &self,
        approval: &ApprovalRecord,
    ) -> Result<HeldHostGuards, NativeError> {
        let sessions_dir = self.require_verified_sessions_dir()?;
        let launch_guard = HostLaunchIntentLock::try_acquire(&sessions_dir, &approval.approval_id)
            .map_err(map_host_error)?
            .ok_or_else(|| {
                invalid("host launch-intent lock is currently held by an active startup process")
            })?;
        verify_host_launch_intent_state(&sessions_dir, approval)?;

        let session_dir_candidate = sessions_dir.join(&approval.approval_id);
        let _ = validate_absolute_no_symlink_components(&session_dir_candidate, "session_dir")?;
        let session_guard = match fs::symlink_metadata(&session_dir_candidate) {
            Ok(_) => {
                let session_dir = verify_existing_session_dir(&sessions_dir, &approval.approval_id)
                    .map_err(map_host_error)?;
                let lock = HostSessionLock::acquire(&session_dir, &approval.approval_id).map_err(
                    |e| match e {
                        HostError::LockBusy { .. } => invalid(
                            "live ApprovedHost session lock is currently held for this approval",
                        ),
                        other => map_host_error(other),
                    },
                )?;
                verify_stopped_host_session_contents(&session_dir, approval)?;
                Some(lock)
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => None,
            Err(err) => {
                return Err(NativeError::new(
                    NativeErrorCode::IoError,
                    format!(
                        "cannot inspect host session directory metadata: {}",
                        sanitize_diagnostic_text(&err.to_string())
                    ),
                ))
            }
        };

        Ok(HeldHostGuards {
            _launch_guard: launch_guard,
            _session_guard: session_guard,
        })
    }

    fn verify_host_quiescent_under_held_guards(
        &self,
        approval: &ApprovalRecord,
    ) -> Result<(), NativeError> {
        let sessions_dir = self.require_verified_sessions_dir()?;
        verify_host_launch_intent_state(&sessions_dir, approval)?;
        let session_dir_candidate = sessions_dir.join(&approval.approval_id);
        match fs::symlink_metadata(&session_dir_candidate) {
            Ok(_) => {
                let session_dir = verify_existing_session_dir(&sessions_dir, &approval.approval_id)
                    .map_err(map_host_error)?;
                verify_stopped_host_session_contents(&session_dir, approval)
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(NativeError::new(
                NativeErrorCode::IoError,
                format!(
                    "cannot inspect host session directory metadata: {}",
                    sanitize_diagnostic_text(&err.to_string())
                ),
            )),
        }
    }

    fn remember_durability_witness(
        &self,
        ledger_path: PathBuf,
        receipt: &ExternalResolutionReceipt,
    ) -> Result<(), NativeError> {
        let mut map = self
            .witnesses
            .lock()
            .map_err(|_| invalid("external resolution witness lock poisoned"))?;
        if map.len() >= MAX_WITNESS_ENTRIES {
            map.clear();
        }
        map.insert(
            (ledger_path, receipt.request_key.clone()),
            receipt.receipt_digest.clone(),
        );
        Ok(())
    }

    fn has_durability_witness(
        &self,
        ledger_path: &Path,
        receipt: &ExternalResolutionReceipt,
    ) -> Result<bool, NativeError> {
        let map = self
            .witnesses
            .lock()
            .map_err(|_| invalid("external resolution witness lock poisoned"))?;
        Ok(map
            .get(&(ledger_path.to_path_buf(), receipt.request_key.clone()))
            .is_some_and(|digest| constant_time_str_eq(digest, &receipt.receipt_digest)))
    }
}

struct HeldHostGuards {
    _launch_guard: HostLaunchIntentLock,
    _session_guard: Option<HostSessionLock>,
}

struct ResolvedTargetSnapshot {
    owner_target_id: String,
    project: ExternalResolutionProjectRef,
    resolved_task_ids: Vec<String>,
    source_job_revision: Option<u64>,
    source_controller_ledger_revision: u64,
    source_runtime_revision: Option<u64>,
    work_brief_digest: Option<String>,
    plan_id: Option<String>,
    plan_revision: Option<u64>,
    plan_digest: Option<String>,
    approval_id: Option<String>,
    approval_subject_digest: Option<String>,
    envelope_digest: Option<String>,
    bound_approval: Option<ApprovalRecord>,
}

/// Pure read-only applicability evaluator for a persisted [`ExternalResolutionReceipt`] against
/// current workflow, authority, plan, controller-runtime, and conversation-change state.
///
/// The receipt's own `ledger_revision` bump (`source_ledger_revision + 1`) and unrelated ledger
/// appends never invalidate applicability; changes to the referenced target mark it [`ExternalResolutionApplicability::Stale`].
pub fn evaluate_receipt_applicability(
    receipt: &ExternalResolutionReceipt,
    ledger: &CoworkerBindingLedger,
    approvals: &[ApprovalRecord],
    plans: &[PlanRevision],
    current_job_runtime: Option<&ControllerJobRuntimeState>,
    plans_dir: &Path,
) -> Result<ExternalResolutionApplicability, NativeError> {
    receipt.validate_integrity()?;
    let mut reasons = BTreeSet::new();

    match &receipt.target {
        ExternalResolutionTarget::Job { job_id } => {
            match ledger.jobs.iter().find(|j| &j.job_id == job_id) {
                None => {
                    reasons.insert(ExternalResolutionStaleReason::JobTargetDigestDrifted);
                }
                Some(job) => {
                    if Some(job.job_revision) != receipt.source_job_revision {
                        reasons.insert(ExternalResolutionStaleReason::JobRevisionAdvanced);
                    }
                    if job.phase == CoworkerJobPhase::Cancelled
                        || ledger
                            .jobs
                            .iter()
                            .any(|o| o.supersedes_job_id.as_deref() == Some(job.job_id.as_str()))
                    {
                        reasons.insert(ExternalResolutionStaleReason::JobSuperseded);
                    }
                    if Some(job.work_brief_digest.as_str()) != receipt.work_brief_digest.as_deref()
                        || job.work_brief.project_base.base_commit != receipt.project.base_commit
                        || Some(job.work_brief.project_base.base_tree_sha.as_str())
                            != receipt.project.base_tree_sha.as_deref()
                        || Path::new(&job.work_brief.project_base.repository_path)
                            != receipt.project.canonical_repo_root.as_path()
                    {
                        reasons.insert(ExternalResolutionStaleReason::JobTargetDigestDrifted);
                    }
                    let current_approval_id = job
                        .approval_linkage
                        .as_ref()
                        .map(|l| l.approval_id.as_str());
                    let current_approval_digest = job
                        .approval_linkage
                        .as_ref()
                        .map(|l| l.approval_subject_digest.as_str());
                    if current_approval_id != receipt.approval_id.as_deref()
                        || current_approval_digest != receipt.approval_subject_digest.as_deref()
                    {
                        reasons.insert(ExternalResolutionStaleReason::JobTargetDigestDrifted);
                    }
                }
            }
            let actual_rt_rev = current_job_runtime.map(|r| r.runtime_revision);
            let actual_env_digest =
                current_job_runtime.map(|r| r.envelope.envelope_digest.as_str());
            if actual_rt_rev != receipt.source_runtime_revision
                || actual_env_digest != receipt.envelope_digest.as_deref()
            {
                reasons.insert(ExternalResolutionStaleReason::JobRuntimeAdvanced);
            }
        }
        ExternalResolutionTarget::LegacyPlan {
            plan_id,
            plan_revision,
        } => {
            if ledger.jobs.iter().any(|j| {
                j.planning_result
                    .as_ref()
                    .is_some_and(|pr| &pr.plan_id == plan_id)
                    || j.approval_linkage
                        .as_ref()
                        .is_some_and(|al| &al.plan_id == plan_id)
            }) {
                reasons.insert(ExternalResolutionStaleReason::LegacyPlanClaimedByJob);
            }
            let matching_approvals: Vec<&ApprovalRecord> = approvals
                .iter()
                .filter(|a| &a.plan_id == plan_id && a.plan_revision == *plan_revision)
                .collect();
            if matching_approvals.len() != 1
                || Some(matching_approvals[0].approval_id.as_str())
                    != receipt.approval_id.as_deref()
                || Some(matching_approvals[0].subject_digest.as_str())
                    != receipt.approval_subject_digest.as_deref()
            {
                reasons.insert(ExternalResolutionStaleReason::BoundApprovalChanged);
            }
        }
        ExternalResolutionTarget::CommitOnly { .. } => {
            return Err(NativeError::new(
                NativeErrorCode::ProtocolError,
                "commit_only target cannot exist in a stored receipt",
            ));
        }
    }

    if let (Some(ref pid), Some(prev)) = (&receipt.plan_id, receipt.plan_revision) {
        let plan_ok = plans
            .iter()
            .find(|p| &p.plan_id == pid && p.revision == prev)
            .is_some_and(|p| p.binding_id == receipt.binding_id && p.status.is_active_published());
        let newer_published = plans
            .iter()
            .any(|p| &p.plan_id == pid && p.status.is_active_published() && p.revision > prev);
        if !plan_ok || newer_published {
            reasons.insert(ExternalResolutionStaleReason::BoundPlanChanged);
        }
    }

    if let Some(ref aid) = receipt.approval_id {
        let approval_ok = approvals.iter().any(|a| {
            &a.approval_id == aid
                && a.binding_id == receipt.binding_id
                && a.thread_uuid == receipt.thread_uuid
                && Some(a.subject_digest.as_str()) == receipt.approval_subject_digest.as_deref()
        });
        if !approval_ok {
            reasons.insert(ExternalResolutionStaleReason::BoundApprovalChanged);
        }
        match inspect_unresolved_blocking_changes(
            plans_dir,
            &receipt.binding_id,
            Some(aid.as_str()),
            None,
        ) {
            Ok(summary) if summary.blocked || summary.approval_superseded => {
                reasons.insert(ExternalResolutionStaleReason::ConversationChangeInvalidated);
            }
            Ok(_) => {
                for tid in &receipt.resolved_task_ids {
                    match inspect_unresolved_blocking_changes(
                        plans_dir,
                        &receipt.binding_id,
                        Some(aid.as_str()),
                        Some(tid.as_str()),
                    ) {
                        Ok(ts) if ts.blocked || ts.approval_superseded => {
                            reasons.insert(
                                ExternalResolutionStaleReason::ConversationChangeInvalidated,
                            );
                        }
                        Ok(_) => {}
                        Err(_) => {
                            reasons.insert(
                                ExternalResolutionStaleReason::ConversationChangeInvalidated,
                            );
                        }
                    }
                }
            }
            Err(_) => {
                reasons.insert(ExternalResolutionStaleReason::ConversationChangeInvalidated);
            }
        }
    } else if let Ok(summary) =
        inspect_unresolved_blocking_changes(plans_dir, &receipt.binding_id, None, None)
    {
        if summary.blocked {
            reasons.insert(ExternalResolutionStaleReason::ConversationChangeInvalidated);
        }
    } else {
        reasons.insert(ExternalResolutionStaleReason::ConversationChangeInvalidated);
    }

    if reasons.is_empty() {
        Ok(ExternalResolutionApplicability::Applicable)
    } else {
        Ok(ExternalResolutionApplicability::Stale {
            reasons: reasons.into_iter().collect(),
        })
    }
}

pub(crate) fn validate_ledger_external_resolutions(
    receipts: &[ExternalResolutionReceipt],
    expected_binding_id: &str,
    pinned_thread_uuid: &str,
    ledger_revision: u64,
    jobs: &[CoworkerJobRecord],
) -> Result<(), NativeError> {
    if receipts.len() > MAX_EXTERNAL_RESOLUTIONS_PER_BINDING {
        return Err(NativeError::new(
            NativeErrorCode::ProtocolError,
            "external_resolutions exceeds MAX_EXTERNAL_RESOLUTIONS_PER_BINDING",
        ));
    }
    let mut receipt_ids = BTreeSet::new();
    let mut request_keys = BTreeSet::new();
    let mut prev_recorded_rev = 0u64;

    for receipt in receipts {
        receipt.validate_integrity().map_err(|err| {
            NativeError::new(
                NativeErrorCode::ProtocolError,
                format!(
                    "invalid ExternalResolutionReceipt in ledger: {}",
                    err.message()
                ),
            )
        })?;
        if receipt.binding_id.as_str() != expected_binding_id
            || receipt.thread_uuid.as_str() != pinned_thread_uuid
        {
            return Err(NativeError::new(
                NativeErrorCode::ProtocolError,
                "ExternalResolutionReceipt binding_id or thread_uuid does not match ledger scope",
            ));
        }
        if !receipt_ids.insert(receipt.receipt_id.as_str())
            || !request_keys.insert(receipt.request_key.as_str())
        {
            return Err(NativeError::new(
                NativeErrorCode::ProtocolError,
                "duplicate receipt_id or request_key in external_resolutions",
            ));
        }
        if receipt.recorded_ledger_revision == 0
            || receipt.recorded_ledger_revision > ledger_revision
            || receipt.recorded_ledger_revision <= prev_recorded_rev
        {
            return Err(NativeError::new(
                NativeErrorCode::ProtocolError,
                "ExternalResolutionReceipt recorded_ledger_revision violates monotonic ledger bounds",
            ));
        }
        prev_recorded_rev = receipt.recorded_ledger_revision;

        if let ExternalResolutionTarget::Job { ref job_id } = receipt.target {
            let job = jobs.iter().find(|j| &j.job_id == job_id).ok_or_else(|| {
                NativeError::new(
                    NativeErrorCode::ProtocolError,
                    "ExternalResolutionReceipt references a job_id missing from ledger.jobs",
                )
            })?;
            let min_job_rev = receipt.source_job_revision.unwrap_or(1);
            if job.job_revision < min_job_rev
                || Path::new(&job.work_brief.project_base.repository_path)
                    != receipt.project.canonical_repo_root.as_path()
                || job.work_brief.project_base.base_commit != receipt.project.base_commit
                || Some(job.work_brief.project_base.base_tree_sha.as_str())
                    != receipt.project.base_tree_sha.as_deref()
            {
                return Err(NativeError::new(
                    NativeErrorCode::ProtocolError,
                    "ExternalResolutionReceipt job revision or immutable project_base conflicts with ledger.jobs",
                ));
            }
            if receipt
                .resolved_task_ids
                .iter()
                .any(|task_id| !job.tasks.iter().any(|task| &task.task_id == task_id))
            {
                return Err(NativeError::new(
                    NativeErrorCode::ProtocolError,
                    "ExternalResolutionReceipt resolved_task_ids references a task missing from ledger.jobs",
                ));
            }
        }
    }
    Ok(())
}

fn build_preview_from_resolved(
    normalized: &RecordExternalResolutionRequest,
    ledger: &CoworkerBindingLedger,
    resolved: ResolvedTargetSnapshot,
) -> Result<ExternalResolutionPreview, NativeError> {
    let resulting_ledger_revision = ledger
        .ledger_revision
        .checked_add(1)
        .ok_or_else(|| invalid("coworker workflow ledger_revision overflow"))?;
    let request_digest = compute_request_digest(normalized)?;
    let mut preview = ExternalResolutionPreview {
        interface_version: EXTERNAL_RESOLUTION_INTERFACE_VERSION.to_string(),
        schema_version: EXTERNAL_RESOLUTION_SCHEMA_VERSION,
        binding_id: normalized.binding_id.clone(),
        thread_uuid: normalized.thread_uuid.clone(),
        project: resolved.project,
        target: normalized.target.clone(),
        scope: normalized.scope.clone(),
        resolved_task_ids: resolved.resolved_task_ids,
        source_ledger_revision: ledger.ledger_revision,
        resulting_ledger_revision,
        source_job_revision: resolved.source_job_revision,
        source_controller_ledger_revision: resolved.source_controller_ledger_revision,
        source_runtime_revision: resolved.source_runtime_revision,
        work_brief_digest: resolved.work_brief_digest,
        plan_id: resolved.plan_id,
        plan_revision: resolved.plan_revision,
        plan_digest: resolved.plan_digest,
        approval_id: resolved.approval_id,
        approval_subject_digest: resolved.approval_subject_digest,
        envelope_digest: resolved.envelope_digest,
        request_key: normalized.request_key.clone(),
        created_at_ms: normalized.created_at_ms,
        explanation: normalized.explanation.clone(),
        evidence_refs: normalized.evidence_refs.clone(),
        request_digest,
        preview_digest: String::new(),
    };
    preview.preview_digest = preview.compute_preview_digest()?;
    Ok(preview)
}

fn owner_target_id_for_preview(preview: &ExternalResolutionPreview) -> Result<String, NativeError> {
    match &preview.target {
        ExternalResolutionTarget::Job { job_id } => Ok(job_id.clone()),
        ExternalResolutionTarget::LegacyPlan { .. } => preview
            .approval_id
            .clone()
            .ok_or_else(|| invalid("legacy_plan preview is missing bound approval_id")),
        ExternalResolutionTarget::CommitOnly { .. } => Err(NativeError::new(
            NativeErrorCode::UnsupportedCapability,
            "commit_only target has no ownership target",
        )),
    }
}

fn verify_no_ambiguous_planning_reservations(
    ledger: &CoworkerBindingLedger,
) -> Result<(), NativeError> {
    for grant in &ledger.planning_grants {
        if grant
            .reservations
            .iter()
            .any(|reservation| reservation.status.is_ambiguous())
        {
            return Err(NativeError::new(
                NativeErrorCode::AmbiguousWakeupPendingReconciliation,
                "cannot record external resolution while a PlanningCallReservation is ReservedBeforeEffect or EffectUnknown",
            ));
        }
    }
    Ok(())
}

fn verify_planning_command_ledger_quiescent(
    cmd_ledger: &crate::planning_commands::PlanningCommandBindingLedger,
    expected_thread_uuid: &str,
) -> Result<(), NativeError> {
    if let Some(ref pinned) = cmd_ledger.thread_uuid {
        if pinned != expected_thread_uuid {
            return Err(NativeError::new(
                NativeErrorCode::SecurityViolation,
                "planning command ledger thread_uuid does not match expected thread_uuid",
            ));
        }
    }
    for cmd in &cmd_ledger.commands {
        if cmd.status == PlanningCommandStatus::Unknown || cmd.pending_reconciliation {
            return Err(NativeError::new(
                NativeErrorCode::AmbiguousWakeupPendingReconciliation,
                "cannot record external resolution while a planning command is Unknown or pending reconciliation",
            ));
        }
        if matches!(
            cmd.status,
            PlanningCommandStatus::Queued | PlanningCommandStatus::Running
        ) {
            return Err(invalid(
                "cannot record external resolution while a planning command is Queued or Running",
            ));
        }
    }
    Ok(())
}

fn verify_job_quiescent(job: &CoworkerJobRecord) -> Result<(), NativeError> {
    for outbox in &job.outbox {
        if outbox.status.blocks_replay_and_new_dispatch() {
            let code =
                if outbox.status == crate::coworker_workflow::OutboxDispatchStatus::EffectUnknown {
                    NativeErrorCode::AmbiguousWakeupPendingReconciliation
                } else {
                    NativeErrorCode::ValidationFailed
                };
            return Err(NativeError::new(
                code,
                "cannot record external resolution while an unresolved outbox record exists on the job",
            ));
        }
    }
    for task in &job.tasks {
        if task.unresolved_outbox_id.is_some() || task.stage == CoworkerTaskStage::EffectUnknown {
            return Err(NativeError::new(
                NativeErrorCode::AmbiguousWakeupPendingReconciliation,
                "cannot record external resolution while a task is in EffectUnknown or has an unresolved outbox record",
            ));
        }
        if matches!(
            task.stage,
            CoworkerTaskStage::DispatchReserved
                | CoworkerTaskStage::Running
                | CoworkerTaskStage::ReviewReserved
                | CoworkerTaskStage::UnderReview
                | CoworkerTaskStage::IntegrationReserved
        ) {
            return Err(invalid(
                "cannot record external resolution while a task attempt, review, or integration stage is in flight",
            ));
        }
    }
    if job
        .combined_candidates
        .iter()
        .any(|c| c.status == CombinedCandidateStatus::ClosureReserved)
    {
        return Err(invalid(
            "cannot record external resolution while a combined candidate has a ClosureReserved attempt",
        ));
    }
    Ok(())
}

fn verify_host_launch_intent_state(
    sessions_dir: &Path,
    approval: &ApprovalRecord,
) -> Result<(), NativeError> {
    if let Some(intent) =
        read_host_launch_intent_optional(sessions_dir, approval).map_err(map_host_error)?
    {
        match intent.status {
            HostLaunchIntentStatus::Pending => {
                return Err(invalid(
                    "cannot record external resolution while HostLaunchIntent is Pending",
                ));
            }
            HostLaunchIntentStatus::Unknown => {
                return Err(NativeError::new(
                    NativeErrorCode::AmbiguousWakeupPendingReconciliation,
                    "cannot record external resolution while HostLaunchIntent is Unknown",
                ));
            }
            HostLaunchIntentStatus::ReconciledReady => {}
        }
    }
    Ok(())
}

fn verify_stopped_host_session_contents(
    session_dir: &Path,
    approval: &ApprovalRecord,
) -> Result<(), NativeError> {
    let manifest = read_host_manifest_optional(session_dir, approval)
        .map_err(map_host_error)?
        .ok_or_else(|| {
            NativeError::new(
                NativeErrorCode::AmbiguousWakeupPendingReconciliation,
                "existing host session directory lacks a verified host-manifest.json",
            )
        })?;
    if !matches!(manifest.phase, HostPhase::Stopped | HostPhase::Failed) {
        return Err(NativeError::new(
            NativeErrorCode::AmbiguousWakeupPendingReconciliation,
            "existing host session manifest is not in Stopped or Failed phase",
        ));
    }
    if matches!(
        manifest.notifications.wakeup_status,
        HostNotificationWakeupStatus::InFlight | HostNotificationWakeupStatus::Unknown
    ) {
        return Err(NativeError::new(
            NativeErrorCode::AmbiguousWakeupPendingReconciliation,
            "host notification wakeup status is InFlight or Unknown",
        ));
    }
    if let Some(ref sb) = manifest.session_binding_id {
        if let Some((pending_intents, receipts)) =
            read_session_task_ledger_recorded_history_readonly(session_dir, approval, sb)
                .map_err(map_host_error)?
        {
            for intent in &pending_intents {
                let sealed = receipts
                    .iter()
                    .any(|r| r.task_id == intent.task_id && r.command_key == intent.command_key);
                if !sealed {
                    return Err(NativeError::new(
                        NativeErrorCode::AmbiguousWakeupPendingReconciliation,
                        "host task ledger contains an unsealed TaskDispatchIntent without a TaskRunReceipt",
                    ));
                }
            }
        }
    }
    Ok(())
}

fn probe_existing_lockfile_busy_readonly(lock_path: &Path) -> Result<bool, NativeError> {
    let clean = validate_absolute_no_symlink_components(lock_path, "host_lock_probe")?;
    let meta = match fs::symlink_metadata(&clean) {
        Ok(m) => m,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(err) => {
            return Err(NativeError::new(
                NativeErrorCode::IoError,
                format!(
                    "cannot stat lockfile metadata: {}",
                    sanitize_diagnostic_text(&err.to_string())
                ),
            ))
        }
    };
    if meta.file_type().is_symlink() || !meta.file_type().is_file() {
        return Err(NativeError::new(
            NativeErrorCode::SecurityViolation,
            "probed lockfile must be a non-symlink regular file",
        ));
    }
    let file = File::open(&clean).map_err(|err| {
        NativeError::new(
            NativeErrorCode::IoError,
            format!(
                "cannot open lockfile for read-only probe: {}",
                sanitize_diagnostic_text(&err.to_string())
            ),
        )
    })?;
    match file.try_lock_exclusive() {
        Ok(()) => {
            let _ = file.unlock();
            Ok(false)
        }
        Err(_) => Ok(true),
    }
}

fn acquire_conversation_advancement_lock_sync(
    plans_dir: &Path,
    binding_id: &str,
) -> Result<CrossProcessLockGuard, NativeError> {
    let _ = ConversationChangeStore::open(plans_dir)?;
    let namespace_dir = plans_dir.join(CONVERSATION_CHANGE_NAMESPACE_DIR);
    let tag = conversation_change_binding_tag(binding_id);
    let lock_filename = format!("changes-{tag}.lock");
    acquire_cross_process_lock(
        &namespace_dir,
        &lock_filename,
        "conversation_advancement_lock",
    )
}

fn conversation_change_binding_tag(binding_id: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"threadlane-native:conversation-changes:v1\0");
    hasher.update(binding_id.as_bytes());
    let digest = hasher.finalize();
    let mut hex = String::with_capacity(32);
    for byte in &digest[..16] {
        let _ = write!(&mut hex, "{byte:02x}");
    }
    hex
}

fn verify_canonical_git_repo_dir_readonly(repo_path: &Path) -> Result<PathBuf, NativeError> {
    let clean = validate_absolute_no_symlink_components(repo_path, "canonical_repo_root")?;
    let meta = fs::symlink_metadata(&clean).map_err(|err| {
        if err.kind() == std::io::ErrorKind::NotFound {
            NativeError::new(
                NativeErrorCode::NotConfigured,
                "canonical_repo_root does not exist on disk",
            )
        } else {
            NativeError::new(
                NativeErrorCode::IoError,
                format!(
                    "cannot inspect canonical_repo_root metadata: {}",
                    sanitize_diagnostic_text(&err.to_string())
                ),
            )
        }
    })?;
    if meta.file_type().is_symlink() || !meta.file_type().is_dir() {
        return Err(NativeError::new(
            NativeErrorCode::SecurityViolation,
            "canonical_repo_root must be a non-symlink directory",
        ));
    }
    let canonical = fs::canonicalize(&clean).map_err(|err| {
        NativeError::new(
            NativeErrorCode::IoError,
            format!(
                "cannot canonicalize canonical_repo_root: {}",
                sanitize_diagnostic_text(&err.to_string())
            ),
        )
    })?;
    if canonical != clean {
        return Err(NativeError::new(
            NativeErrorCode::SecurityViolation,
            "canonical_repo_root drifted during canonicalization",
        ));
    }
    let git_entry = clean.join(".git");
    let git_meta = fs::symlink_metadata(&git_entry).map_err(|_| {
        invalid("canonical_repo_root does not contain a valid non-symlink .git entry")
    })?;
    if git_meta.file_type().is_symlink() {
        return Err(NativeError::new(
            NativeErrorCode::SecurityViolation,
            "canonical_repo_root/.git must not be a symlink",
        ));
    }
    Ok(clean)
}

fn validate_existing_private_dir_readonly(dir: &Path, label: &str) -> Result<PathBuf, NativeError> {
    let clean = validate_absolute_no_symlink_components(dir, label)?;
    let meta = fs::symlink_metadata(&clean).map_err(|err| {
        if err.kind() == std::io::ErrorKind::NotFound {
            NativeError::new(
                NativeErrorCode::NotConfigured,
                format!("required {label} directory does not exist"),
            )
        } else {
            NativeError::new(
                NativeErrorCode::IoError,
                format!(
                    "failed to inspect {label} directory metadata: {}",
                    sanitize_diagnostic_text(&err.to_string())
                ),
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

fn validate_clean_bounded_text(
    raw: &str,
    field: &'static str,
    max_bytes: usize,
) -> Result<(), NativeError> {
    if raw.is_empty()
        || raw.len() > max_bytes
        || raw.trim().is_empty()
        || raw.trim() != raw
        || raw.chars().any(char::is_control)
    {
        return Err(invalid(&format!(
            "{field} must be non-empty, trimmed, <= {max_bytes} bytes, and free of control characters"
        )));
    }
    Ok(())
}

fn compute_request_digest(
    normalized: &RecordExternalResolutionRequest,
) -> Result<String, NativeError> {
    let bytes = serialize_bounded(
        &(EXTERNAL_RESOLUTION_REQUEST_DOMAIN, normalized),
        "external resolution request digest",
    )?;
    Ok(sha256_hex(&bytes))
}

fn serialize_bounded<T: Serialize>(value: &T, label: &str) -> Result<Vec<u8>, NativeError> {
    let bytes = serde_json::to_vec(value)
        .map_err(|_| invalid(&format!("failed to serialize {label} JSON")))?;
    if bytes.len() > MAX_PRIVATE_STATE_FILE_BYTES {
        return Err(NativeError::new(
            NativeErrorCode::OutputOverflow,
            format!("{label} exceeds maximum private state byte bound"),
        ));
    }
    Ok(bytes)
}

fn paths_overlap(a: &Path, b: &Path) -> bool {
    a.starts_with(b) || b.starts_with(a)
}

fn current_unix_epoch_ms() -> Result<u64, NativeError> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| invalid("system clock is before Unix epoch"))?
        .as_millis();
    Ok(u64::try_from(millis).unwrap_or(u64::MAX).max(1))
}

fn invalid(message: &str) -> NativeError {
    NativeError::new(NativeErrorCode::ValidationFailed, message)
}

fn map_authority_read_error(err: threadlane_authority::AuthorityError) -> NativeError {
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
            "AuthorityStore reported an I/O error",
        ),
        code => invalid(&format!("AuthorityStore validation failed ({code})")),
    }
}

fn map_host_error(err: HostError) -> NativeError {
    match err {
        HostError::SecurityViolation { .. } => NativeError::new(
            NativeErrorCode::SecurityViolation,
            "host session directory reported a security violation",
        ),
        HostError::CorruptManifest { .. }
        | HostError::CorruptReceipts { .. }
        | HostError::CorruptSessionState { .. } => NativeError::new(
            NativeErrorCode::ProtocolError,
            "host session state failed integrity verification",
        ),
        HostError::LockBusy { .. } => invalid("host session lock is currently held"),
        other => invalid(&format!(
            "host session verification failed: {}",
            sanitize_diagnostic_text(&other.to_string())
        )),
    }
}
