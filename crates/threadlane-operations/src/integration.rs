use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

use fs2::FileExt;
use serde::{Deserialize, Serialize};
use threadlane_workers::{
    begin_worktree_job, cleanup_integrated_worktree, ensure_platform_supported, ensure_private_dir,
    evaluate_diff_gate, is_commit_ancestor, is_control_config_path, is_path_allowed_by_scopes,
    is_worktree_clean, load_and_verify_manifest, open_private_lock_file, read_dependency_input,
    read_private_json, resolve_allowed_write_scopes_at_base, resolve_exact_commit_sha,
    resolve_exact_tree_sha, run_sanitized_git, run_sanitized_git_allow_nonzero,
    run_sanitized_git_bytes, sanitize_and_redact, validate_absolute_no_symlink_components,
    validate_commit_ref, validate_exact_40_hex_sha, validate_existing_canonical_dir,
    validate_repo_relative_path, validate_sha256_hex_digest, validate_simple_identifier,
    write_private_json_atomic, DependencyInputPolicy, DependencyInputSeal, IntegratedCleanupReport,
    IntegratedCleanupRequest, WorkAreaOwnershipMarker, WorktreeKind, WorktreeManifest,
    CHECKOUTS_SUBDIR, DEFAULT_GIT_COMMAND_TIMEOUT_MS, DEFAULT_MAX_GIT_OUTPUT_BYTES,
    MANIFESTS_SUBDIR, MAX_DEPENDENCY_INPUT_BYTES, WORK_AREA_MARKER_FILENAME,
    WORK_AREA_MARKER_VERSION,
};
use uuid::Uuid;

use crate::dependency::{
    dependency_input_intent_path, dependency_input_snapshot_ref,
    inspect_dependency_input_seal_readonly, DependencyPreparationIntent,
    MAX_DEPENDENCY_CLOSURE_TASKS,
};
use crate::error::OperationsError;
use crate::review::{
    ensure_worktree_inactive, ensure_worktree_inactive_readonly, inspect_review_subject,
    load_and_verify_task_manifest, load_review_receipt, sha256_hex, verify_review_receipt,
    verify_review_receipt_readonly, verify_worktree_branch, InspectReviewSubjectRequest,
    ReviewPolicyBinding, ReviewReceipt, ReviewSubject, TestEvidenceSummary, MAX_RECEIPT_BYTES,
    MAX_REVIEW_NOTE_BYTES, RECEIPTS_SUBDIR,
};

/// Schema version for [`IntegrationReceipt`].
pub const INTEGRATION_RECEIPT_SCHEMA_VERSION: u32 = 1;

/// Schema version for [`JobIntegrationIntent`].
pub const JOB_INTEGRATION_INTENT_SCHEMA_VERSION: u32 = 1;

/// Subdirectory inside `<work_area_root>/manifests` storing private (`0600`) job integration intents and locks.
pub const JOB_INTEGRATIONS_SUBDIR: &str = "job-integrations";

/// Schema version for [`PushReceipt`].
pub const PUSH_RECEIPT_SCHEMA_VERSION: u32 = 1;

/// Schema version for [`RemoteDeleteReceipt`].
pub const REMOTE_DELETE_RECEIPT_SCHEMA_VERSION: u32 = 1;

/// Strategy for integrating an approved task commit into an owned integration worktree.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IntegrationStrategy {
    /// Fast-forward only (`git merge --ff-only -- <task_head_commit>`).
    FastForwardOnly,
    /// Explicit merge commit (`git merge --no-ff --no-edit -m <msg> -- <task_head_commit>`).
    MergeNoFf,
}

/// Request to compute a read-only [`IntegrationProposal`] without mutating any worktree or ref.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IntegrationProposalRequest {
    /// Trusted private work-area root directory.
    pub work_area_root: PathBuf,
    /// Owned integration worktree identifier (`WorktreeKind::RunIntegration`).
    pub integration_worktree_id: String,
    /// Owned task worktree identifier (`WorktreeKind::Task`).
    pub task_worktree_id: String,
    /// Approved review subject.
    pub subject: ReviewSubject,
    /// Frozen review receipt matching `subject`.
    pub review_receipt: ReviewReceipt,
    /// Requested integration strategy.
    pub strategy: IntegrationStrategy,
}

/// Read-only integration plan describing current heads, mergeability, and exact Git argv.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IntegrationProposal {
    /// Run identifier.
    pub run_id: String,
    /// Task identifier.
    pub task_id: String,
    /// Owned integration worktree identifier.
    pub integration_worktree_id: String,
    /// Canonical integration worktree checkout path.
    pub integration_checkout_path: PathBuf,
    /// Integration worktree branch name (`threadlane/run/...`).
    pub integration_branch_name: String,
    /// Owned task worktree identifier.
    pub task_worktree_id: String,
    /// Canonical task worktree checkout path.
    pub task_checkout_path: PathBuf,
    /// Task worktree branch name (`threadlane/task/...`).
    pub task_branch_name: String,
    /// Canonical primary source repository path.
    pub repository_path: PathBuf,
    /// Current 40-hex `HEAD` commit SHA of the primary source repository.
    pub source_head_commit: String,
    /// Base 40-hex commit SHA from which the worktrees were created.
    pub base_commit: String,
    /// Current 40-hex `HEAD` commit SHA of the integration worktree.
    pub integration_head_commit: String,
    /// Current 40-hex `HEAD` commit SHA of the task worktree.
    pub task_head_commit: String,
    /// Current 40-hex `HEAD^{tree}` SHA of the task worktree.
    pub task_tree_sha: String,
    /// Bound 64-hex `ReviewSubject::subject_digest`.
    pub review_subject_digest: String,
    /// Bound `ReviewReceipt::receipt_id`.
    pub review_receipt_id: String,
    /// Bound 64-hex `ReviewReceipt::receipt_digest`.
    pub review_receipt_digest: String,
    /// Requested integration strategy.
    pub strategy: IntegrationStrategy,
    /// True if `integration_head_commit` is an ancestor of `task_head_commit`.
    pub can_fast_forward: bool,
    /// True if `task_head_commit` is already an ancestor of `integration_head_commit`.
    pub already_integrated: bool,
    /// True if read-only merge simulation predicts a conflict-free integration under `strategy`.
    pub clean_merge_predicted: bool,
    /// Exact Git argument vector (excluding `git -C <integration_checkout>`) planned for execution.
    pub planned_git_argv: Vec<String>,
    /// Always `true`: proposal generation never mutates checkouts or refs.
    pub read_only: bool,
    /// Canonical 64-hex SHA-256 digest of this proposal.
    pub proposal_digest: String,
}

#[derive(Serialize)]
struct IntegrationProposalDigestPayload<'a> {
    run_id: &'a str,
    task_id: &'a str,
    integration_worktree_id: &'a str,
    integration_checkout_path: &'a Path,
    integration_branch_name: &'a str,
    task_worktree_id: &'a str,
    task_checkout_path: &'a Path,
    task_branch_name: &'a str,
    repository_path: &'a Path,
    source_head_commit: &'a str,
    base_commit: &'a str,
    integration_head_commit: &'a str,
    task_head_commit: &'a str,
    task_tree_sha: &'a str,
    review_subject_digest: &'a str,
    review_receipt_id: &'a str,
    review_receipt_digest: &'a str,
    strategy: IntegrationStrategy,
    can_fast_forward: bool,
    already_integrated: bool,
    clean_merge_predicted: bool,
    planned_git_argv: &'a [String],
    read_only: bool,
}

impl IntegrationProposal {
    /// Computes the canonical 64-character lowercase hexadecimal SHA-256 digest of this proposal.
    pub fn compute_digest(&self) -> Result<String, OperationsError> {
        let payload = IntegrationProposalDigestPayload {
            run_id: &self.run_id,
            task_id: &self.task_id,
            integration_worktree_id: &self.integration_worktree_id,
            integration_checkout_path: &self.integration_checkout_path,
            integration_branch_name: &self.integration_branch_name,
            task_worktree_id: &self.task_worktree_id,
            task_checkout_path: &self.task_checkout_path,
            task_branch_name: &self.task_branch_name,
            repository_path: &self.repository_path,
            source_head_commit: &self.source_head_commit,
            base_commit: &self.base_commit,
            integration_head_commit: &self.integration_head_commit,
            task_head_commit: &self.task_head_commit,
            task_tree_sha: &self.task_tree_sha,
            review_subject_digest: &self.review_subject_digest,
            review_receipt_id: &self.review_receipt_id,
            review_receipt_digest: &self.review_receipt_digest,
            strategy: self.strategy,
            can_fast_forward: self.can_fast_forward,
            already_integrated: self.already_integrated,
            clean_merge_predicted: self.clean_merge_predicted,
            planned_git_argv: &self.planned_git_argv,
            read_only: self.read_only,
        };
        let encoded = serde_json::to_vec(&payload)?;
        Ok(sha256_hex(&encoded))
    }
}

/// Explicit operator request to integrate an approved [`ReviewSubject`] into an owned
/// `WorktreeKind::RunIntegration` worktree.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecuteIntegrationRequest {
    /// Trusted private work-area root directory.
    pub work_area_root: PathBuf,
    /// Owned integration worktree identifier (`WorktreeKind::RunIntegration`).
    pub integration_worktree_id: String,
    /// Owned task worktree identifier (`WorktreeKind::Task`).
    pub task_worktree_id: String,
    /// Approved review subject.
    pub subject: ReviewSubject,
    /// Frozen review receipt matching `subject`.
    pub review_receipt: ReviewReceipt,
    /// Expected current 40-hex `HEAD` commit SHA of the integration worktree.
    pub expected_integration_head_commit: String,
    /// Expected current 40-hex `HEAD` commit SHA of the task worktree (must match `subject.task_head_commit`).
    pub expected_task_head_commit: String,
    /// Expected current 40-hex `HEAD` commit SHA of the primary source repository (preserved untouched).
    pub expected_source_head_commit: String,
    /// Integration strategy (`fast_forward_only` or `merge_no_ff`).
    pub strategy: IntegrationStrategy,
    /// Optional explicit integration receipt ID; generated automatically if `None`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub receipt_id: Option<String>,
    /// Operator identifier performing the integration.
    pub operator_id: String,
    /// Wall-clock timestamp (milliseconds since Unix epoch) recorded on the receipt.
    pub integrated_at_ms: i64,
}

/// Frozen integration receipt proving that an approved [`ReviewSubject`] was integrated into
/// an owned integration worktree at `integrated_target_commit`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IntegrationReceipt {
    /// Schema version (`1`).
    pub schema_version: u32,
    /// Unique integration receipt identifier.
    pub receipt_id: String,
    /// Run identifier.
    pub run_id: String,
    /// Task identifier.
    pub task_id: String,
    /// Owned integration worktree identifier (`WorktreeKind::RunIntegration`).
    pub integration_worktree_id: String,
    /// Canonical integration worktree checkout path.
    pub integration_checkout_path: PathBuf,
    /// Integration worktree branch name (`threadlane/run/...`).
    pub integration_branch_name: String,
    /// Owned task worktree identifier (`WorktreeKind::Task`).
    pub task_worktree_id: String,
    /// Task worktree branch name (`threadlane/task/...`).
    pub task_branch_name: String,
    /// Canonical primary source repository path.
    pub repository_path: PathBuf,
    /// Canonical private work-area root path.
    pub work_area_root: PathBuf,
    /// Base 40-hex commit SHA.
    pub base_commit: String,
    /// Integration worktree 40-hex `HEAD` commit SHA prior to integration.
    pub previous_integration_head_commit: String,
    /// Accepted 40-hex task `HEAD` commit SHA that was integrated.
    pub accepted_task_head_commit: String,
    /// Resulting 40-hex target commit SHA on the integration worktree branch after integration.
    pub integrated_target_commit: String,
    /// Resulting 40-hex `HEAD^{tree}` SHA on the integration worktree branch after integration.
    pub integrated_tree_sha: String,
    /// Primary source checkout 40-hex `HEAD` commit SHA verified unchanged before and after integration.
    pub source_head_commit: String,
    /// Bound 64-hex `ReviewSubject::subject_digest`.
    pub review_subject_digest: String,
    /// Bound `ReviewReceipt::receipt_id`.
    pub review_receipt_id: String,
    /// Bound 64-hex `ReviewReceipt::receipt_digest`.
    pub review_receipt_digest: String,
    /// Integration strategy executed.
    pub strategy: IntegrationStrategy,
    /// Operator identifier.
    pub operator_id: String,
    /// Integration timestamp in milliseconds since Unix epoch.
    pub integrated_at_ms: i64,
    /// Optional job-scoped multi-worker aggregation provenance when this task was integrated
    /// from an independent worker run into a job-scoped `WorktreeKind::RunIntegration` target.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub job_aggregation: Option<JobAggregationReceiptProvenance>,
    /// Canonical 64-hex SHA-256 digest of this receipt.
    pub receipt_digest: String,
}

/// Explicit job and approval scope provenance for aggregating accepted task worktrees from
/// distinct worker runs into a single owned `WorktreeKind::RunIntegration` target worktree.
///
/// ## Trust Boundary
/// The native controller or trusted local caller must separately verify that an immutable
/// human-approved job and authority envelope permits local task commit and integration.
/// Passing [`JobIntegrationScopeProvenance`] to this operations API binds provenance into
/// durable intents and receipts, but does **not** cryptographically prove human user authority,
/// verify per-call native chat identity, or grant main-branch merge/push authority.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JobIntegrationScopeProvenance {
    /// Job identifier (`job_...`) owning the aggregated tasks.
    pub job_id: String,
    /// Monotonic job revision (`>= 1`).
    pub job_revision: u64,
    /// Frozen approval identifier (`app_...`) bound to the job.
    pub approval_id: String,
    /// 64-character lowercase hex SHA-256 digest of the approved job/plan subject.
    pub approval_subject_digest: String,
    /// Run identifier (`run_...`) owning the target `WorktreeKind::RunIntegration` worktree.
    pub target_integration_run_id: String,
    /// Repository-relative write scope ceiling approved for this job.
    pub aggregate_allowed_write_paths: BTreeSet<String>,
}

impl JobIntegrationScopeProvenance {
    /// Validates all identifiers, digests, revision bounds, and repository-relative write paths.
    pub fn validate(&self) -> Result<(), OperationsError> {
        validate_simple_identifier(&self.job_id, "job_scope.job_id")?;
        if self.job_revision == 0 {
            return Err(OperationsError::InvalidInput(
                "job_scope.job_revision must be non-zero".to_string(),
            ));
        }
        validate_simple_identifier(&self.approval_id, "job_scope.approval_id")?;
        let _ = validate_sha256_hex_digest(
            &self.approval_subject_digest,
            "job_scope.approval_subject_digest",
        )?;
        validate_simple_identifier(
            &self.target_integration_run_id,
            "job_scope.target_integration_run_id",
        )?;
        if self.aggregate_allowed_write_paths.is_empty() {
            return Err(OperationsError::InvalidInput(
                "job_scope.aggregate_allowed_write_paths must not be empty".to_string(),
            ));
        }
        for entry in &self.aggregate_allowed_write_paths {
            validate_repo_relative_path(entry, "job_scope.aggregate_allowed_write_paths entry")?;
            if is_control_config_path(entry) {
                return Err(OperationsError::SecurityViolation(format!(
                    "job_scope.aggregate_allowed_write_paths entry `{entry}` targets protected control configuration"
                )));
            }
        }
        Ok(())
    }
}

/// Typed provenance bound into an [`IntegrationReceipt`] when an accepted task from an
/// independent worker run (`original_task_run_id`) is integrated into a job-scoped
/// `WorktreeKind::RunIntegration` target worktree (`target_integration_run_id`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JobAggregationReceiptProvenance {
    /// Job identifier (`job_...`).
    pub job_id: String,
    /// Monotonic job revision (`>= 1`).
    pub job_revision: u64,
    /// Frozen approval identifier (`app_...`).
    pub approval_id: String,
    /// 64-character lowercase hex SHA-256 digest of the approved job/plan subject.
    pub approval_subject_digest: String,
    /// Original worker run identifier (`run_...`) that produced the task worktree and review receipt.
    pub original_task_run_id: String,
    /// Target aggregation run identifier (`run_...`) owning the `WorktreeKind::RunIntegration` worktree.
    pub target_integration_run_id: String,
    /// Root approved 40-hex base commit SHA of the job and target `RunIntegration` worktree.
    pub approved_base_commit: String,
    /// Logical plan task identifier (`task_id` in the approved plan and dependency seals).
    pub logical_task_id: String,
    /// 64-character lowercase hex SHA-256 digest of the frozen task specification.
    pub task_digest: String,
    /// Optional 64-hex [`DependencyInputSeal::seal_digest`] when this task executed from a
    /// non-root sealed dependency input base commit.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dependency_seal_digest: Option<String>,
    /// Optional private Git ref (`refs/threadlane/dep-inputs/<approval_id>/<task_id>`) when this
    /// task executed from a non-root sealed dependency input base commit.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dependency_snapshot_ref: Option<String>,
    /// Zero-based index of this task within the job integration batch.
    pub batch_step_index: usize,
    /// Total number of tasks in the job integration batch (`>= 1`).
    pub batch_total_tasks: usize,
    /// Accepted 40-hex `HEAD^{tree}` SHA of the task worktree from [`ReviewSubject::task_tree_sha`].
    pub accepted_task_tree_sha: String,
    /// Accepted 64-hex diff digest of the task worktree from [`ReviewSubject::diff_digest`].
    pub accepted_task_diff_digest: String,
    /// Verified repository-relative `allowed_write_paths` for this task.
    pub task_allowed_write_paths: BTreeSet<String>,
    /// Verified repository-relative aggregate `allowed_write_paths` ceiling for the job.
    pub job_aggregate_allowed_write_paths: BTreeSet<String>,
    /// Bound [`ReviewPolicyBinding`] from the task's [`ReviewSubject`].
    pub task_policy: ReviewPolicyBinding,
    /// Bound [`TestEvidenceSummary`] from the task's [`ReviewSubject`].
    pub task_test_evidence: TestEvidenceSummary,
    /// Canonical 64-hex SHA-256 digest of the read-only integration proposal verified prior to effect.
    pub proposal_digest: String,
    /// Durable job integration intent identifier (`jint_...`) recorded before mutating the integration worktree.
    pub intent_id: String,
    /// Idempotency command key bound to the job integration intent.
    pub command_key: String,
    /// Always `false`: caller-supplied `job_id`/`approval_subject_digest` in operations requests
    /// does not cryptographically prove human user authority.
    pub cryptographically_proves_user_authority: bool,
    /// Always `false`: local operations receipts do not verify native chat identity per call.
    pub native_chat_identity_verified: bool,
    /// Always `false`: integrating into an owned `RunIntegration` worktree never grants main-branch merge or push authority.
    pub grants_main_merge_or_push_authority: bool,
}

impl JobAggregationReceiptProvenance {
    /// Validates all structural fields, digests, write scopes, policy/test evidence, and trust boundary flags.
    pub fn validate(&self) -> Result<(), OperationsError> {
        validate_simple_identifier(&self.job_id, "job_aggregation.job_id")?;
        if self.job_revision == 0 {
            return Err(OperationsError::ReceiptInvalidated(
                "job_aggregation.job_revision must be non-zero".to_string(),
            ));
        }
        validate_simple_identifier(&self.approval_id, "job_aggregation.approval_id")?;
        let _ = validate_sha256_hex_digest(
            &self.approval_subject_digest,
            "job_aggregation.approval_subject_digest",
        )?;
        validate_simple_identifier(
            &self.original_task_run_id,
            "job_aggregation.original_task_run_id",
        )?;
        validate_simple_identifier(
            &self.target_integration_run_id,
            "job_aggregation.target_integration_run_id",
        )?;
        let _ = validate_exact_40_hex_sha(
            &self.approved_base_commit,
            "job_aggregation.approved_base_commit",
        )?;
        validate_simple_identifier(&self.logical_task_id, "job_aggregation.logical_task_id")?;
        let _ = validate_sha256_hex_digest(&self.task_digest, "job_aggregation.task_digest")?;
        match (&self.dependency_seal_digest, &self.dependency_snapshot_ref) {
            (Some(seal_digest), Some(snap_ref)) => {
                let _ = validate_sha256_hex_digest(
                    seal_digest,
                    "job_aggregation.dependency_seal_digest",
                )?;
                let expected_ref =
                    dependency_input_snapshot_ref(&self.approval_id, &self.logical_task_id)?;
                if snap_ref != &expected_ref {
                    return Err(OperationsError::ReceiptInvalidated(format!(
                        "job_aggregation.dependency_snapshot_ref `{snap_ref}` does not match `{expected_ref}`"
                    )));
                }
            }
            (None, None) => {}
            _ => {
                return Err(OperationsError::ReceiptInvalidated(
                    "job_aggregation.dependency_seal_digest and dependency_snapshot_ref must both be present or both be absent"
                        .to_string(),
                ));
            }
        }
        if self.batch_total_tasks == 0
            || self.batch_total_tasks > MAX_DEPENDENCY_CLOSURE_TASKS
            || self.batch_step_index >= self.batch_total_tasks
        {
            return Err(OperationsError::ReceiptInvalidated(format!(
                "invalid job_aggregation batch bounds: step {} of {}",
                self.batch_step_index, self.batch_total_tasks
            )));
        }
        let _ = validate_exact_40_hex_sha(
            &self.accepted_task_tree_sha,
            "job_aggregation.accepted_task_tree_sha",
        )?;
        let _ = validate_sha256_hex_digest(
            &self.accepted_task_diff_digest,
            "job_aggregation.accepted_task_diff_digest",
        )?;
        if self.task_allowed_write_paths.is_empty() {
            return Err(OperationsError::ReceiptInvalidated(
                "job_aggregation.task_allowed_write_paths must not be empty".to_string(),
            ));
        }
        for path in &self.task_allowed_write_paths {
            validate_repo_relative_path(path, "job_aggregation.task_allowed_write_paths entry")?;
            if is_control_config_path(path) {
                return Err(OperationsError::SecurityViolation(format!(
                    "job_aggregation.task_allowed_write_paths entry `{path}` targets protected control configuration"
                )));
            }
        }
        if self.job_aggregate_allowed_write_paths.is_empty() {
            return Err(OperationsError::ReceiptInvalidated(
                "job_aggregation.job_aggregate_allowed_write_paths must not be empty".to_string(),
            ));
        }
        for path in &self.job_aggregate_allowed_write_paths {
            validate_repo_relative_path(
                path,
                "job_aggregation.job_aggregate_allowed_write_paths entry",
            )?;
            if is_control_config_path(path) {
                return Err(OperationsError::SecurityViolation(format!(
                    "job_aggregation.job_aggregate_allowed_write_paths entry `{path}` targets protected control configuration"
                )));
            }
        }
        self.task_policy.validate(true)?;
        if !self.task_policy.require_passing_tests || !self.task_test_evidence.all_checks_passed() {
            return Err(OperationsError::ReceiptInvalidated(
                "job_aggregation requires passing formatting, Clippy, and behavioral test evidence"
                    .to_string(),
            ));
        }
        self.task_test_evidence.validate(true)?;
        let _ =
            validate_sha256_hex_digest(&self.proposal_digest, "job_aggregation.proposal_digest")?;
        validate_simple_identifier(&self.intent_id, "job_aggregation.intent_id")?;
        validate_simple_identifier(&self.command_key, "job_aggregation.command_key")?;

        if self.cryptographically_proves_user_authority {
            return Err(OperationsError::SecurityViolation(
                "job_aggregation must never claim cryptographically_proves_user_authority = true"
                    .to_string(),
            ));
        }
        if self.native_chat_identity_verified {
            return Err(OperationsError::SecurityViolation(
                "job_aggregation must never claim native_chat_identity_verified = true".to_string(),
            ));
        }
        if self.grants_main_merge_or_push_authority {
            return Err(OperationsError::SecurityViolation(
                "job_aggregation must never claim grants_main_merge_or_push_authority = true"
                    .to_string(),
            ));
        }
        Ok(())
    }
}

#[derive(Serialize)]
struct IntegrationReceiptDigestPayload<'a> {
    schema_version: u32,
    receipt_id: &'a str,
    run_id: &'a str,
    task_id: &'a str,
    integration_worktree_id: &'a str,
    integration_checkout_path: &'a Path,
    integration_branch_name: &'a str,
    task_worktree_id: &'a str,
    task_branch_name: &'a str,
    repository_path: &'a Path,
    work_area_root: &'a Path,
    base_commit: &'a str,
    previous_integration_head_commit: &'a str,
    accepted_task_head_commit: &'a str,
    integrated_target_commit: &'a str,
    integrated_tree_sha: &'a str,
    source_head_commit: &'a str,
    review_subject_digest: &'a str,
    review_receipt_id: &'a str,
    review_receipt_digest: &'a str,
    strategy: IntegrationStrategy,
    operator_id: &'a str,
    integrated_at_ms: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    job_aggregation: Option<&'a JobAggregationReceiptProvenance>,
}

impl IntegrationReceipt {
    /// Returns the target `RunIntegration` worktree's owning `run_id` (`job_aggregation.target_integration_run_id`
    /// for job-aggregated receipts, or `self.run_id` for legacy same-run receipts).
    pub fn target_integration_run_id(&self) -> &str {
        match self.job_aggregation {
            Some(ref agg) => agg.target_integration_run_id.as_str(),
            None => self.run_id.as_str(),
        }
    }

    /// Computes the canonical 64-character lowercase hexadecimal SHA-256 digest of this receipt.
    pub fn compute_digest(&self) -> Result<String, OperationsError> {
        let payload = IntegrationReceiptDigestPayload {
            schema_version: self.schema_version,
            receipt_id: &self.receipt_id,
            run_id: &self.run_id,
            task_id: &self.task_id,
            integration_worktree_id: &self.integration_worktree_id,
            integration_checkout_path: &self.integration_checkout_path,
            integration_branch_name: &self.integration_branch_name,
            task_worktree_id: &self.task_worktree_id,
            task_branch_name: &self.task_branch_name,
            repository_path: &self.repository_path,
            work_area_root: &self.work_area_root,
            base_commit: &self.base_commit,
            previous_integration_head_commit: &self.previous_integration_head_commit,
            accepted_task_head_commit: &self.accepted_task_head_commit,
            integrated_target_commit: &self.integrated_target_commit,
            integrated_tree_sha: &self.integrated_tree_sha,
            source_head_commit: &self.source_head_commit,
            review_subject_digest: &self.review_subject_digest,
            review_receipt_id: &self.review_receipt_id,
            review_receipt_digest: &self.review_receipt_digest,
            strategy: self.strategy,
            operator_id: &self.operator_id,
            integrated_at_ms: self.integrated_at_ms,
            job_aggregation: self.job_aggregation.as_ref(),
        };
        let encoded = serde_json::to_vec(&payload)?;
        Ok(sha256_hex(&encoded))
    }

    /// Validates all fields and verifies that `self.receipt_digest` matches [`Self::compute_digest`].
    pub fn verify_integrity(&self) -> Result<(), OperationsError> {
        if self.schema_version != INTEGRATION_RECEIPT_SCHEMA_VERSION {
            return Err(OperationsError::ReceiptInvalidated(format!(
                "unsupported integration receipt schema_version {} (expected {INTEGRATION_RECEIPT_SCHEMA_VERSION})",
                self.schema_version
            )));
        }
        validate_simple_identifier(&self.receipt_id, "receipt_id")?;
        validate_simple_identifier(&self.run_id, "run_id")?;
        validate_simple_identifier(&self.task_id, "task_id")?;
        validate_simple_identifier(&self.integration_worktree_id, "integration_worktree_id")?;
        validate_simple_identifier(&self.task_worktree_id, "task_worktree_id")?;
        validate_simple_identifier(&self.review_receipt_id, "review_receipt_id")?;
        validate_simple_identifier(&self.operator_id, "operator_id")?;
        let _ = validate_exact_40_hex_sha(&self.base_commit, "base_commit")?;
        let _ = validate_exact_40_hex_sha(
            &self.previous_integration_head_commit,
            "previous_integration_head_commit",
        )?;
        let _ = validate_exact_40_hex_sha(
            &self.accepted_task_head_commit,
            "accepted_task_head_commit",
        )?;
        let _ =
            validate_exact_40_hex_sha(&self.integrated_target_commit, "integrated_target_commit")?;
        let _ = validate_exact_40_hex_sha(&self.integrated_tree_sha, "integrated_tree_sha")?;
        let _ = validate_exact_40_hex_sha(&self.source_head_commit, "source_head_commit")?;
        let _ = validate_sha256_hex_digest(&self.review_subject_digest, "review_subject_digest")?;
        let _ = validate_sha256_hex_digest(&self.review_receipt_digest, "review_receipt_digest")?;
        let _ = validate_sha256_hex_digest(&self.receipt_digest, "receipt_digest")?;

        if !self.integration_branch_name.starts_with("threadlane/run/") {
            return Err(OperationsError::ReceiptInvalidated(format!(
                "integration_branch_name `{}` must carry `threadlane/run/` prefix",
                self.integration_branch_name
            )));
        }
        if !self.task_branch_name.starts_with("threadlane/task/") {
            return Err(OperationsError::ReceiptInvalidated(format!(
                "task_branch_name `{}` must carry `threadlane/task/` prefix",
                self.task_branch_name
            )));
        }

        if let Some(ref agg) = self.job_aggregation {
            agg.validate()?;
            if agg.original_task_run_id != self.run_id {
                return Err(OperationsError::ReceiptInvalidated(format!(
                    "job_aggregation.original_task_run_id `{}` does not match receipt.run_id `{}`",
                    agg.original_task_run_id, self.run_id
                )));
            }
            if self.base_commit != agg.approved_base_commit && agg.dependency_seal_digest.is_none()
            {
                return Err(OperationsError::ReceiptInvalidated(format!(
                    "non-root job_aggregation receipt `{}` base_commit `{}` differs from approved_base_commit `{}` without dependency_seal_digest provenance",
                    self.receipt_id, self.base_commit, agg.approved_base_commit
                )));
            }
            if self.base_commit == agg.approved_base_commit && agg.dependency_seal_digest.is_some()
            {
                return Err(OperationsError::ReceiptInvalidated(format!(
                    "root job_aggregation receipt `{}` base_commit equals approved_base_commit `{}` but carries unexpected dependency_seal_digest",
                    self.receipt_id, agg.approved_base_commit
                )));
            }
            let expected_int_prefix = format!("threadlane/run/{}/", agg.target_integration_run_id);
            if !self
                .integration_branch_name
                .starts_with(&expected_int_prefix)
            {
                return Err(OperationsError::ReceiptInvalidated(format!(
                    "integration_branch_name `{}` does not match target_integration_run_id prefix `{expected_int_prefix}`",
                    self.integration_branch_name
                )));
            }
            let expected_task_prefix = format!("threadlane/task/{}/{}/", self.run_id, self.task_id);
            if !self.task_branch_name.starts_with(&expected_task_prefix) {
                return Err(OperationsError::ReceiptInvalidated(format!(
                    "task_branch_name `{}` does not match task prefix `{expected_task_prefix}`",
                    self.task_branch_name
                )));
            }
        }

        let computed = self.compute_digest()?;
        if computed != self.receipt_digest {
            return Err(OperationsError::ReceiptInvalidated(format!(
                "integration receipt digest mismatch: expected `{}`, computed `{computed}`",
                self.receipt_digest
            )));
        }
        Ok(())
    }

    /// Verifies that this integration receipt matches `subject` and `review_receipt`.
    pub fn verify_against_review(
        &self,
        subject: &ReviewSubject,
        review_receipt: &ReviewReceipt,
    ) -> Result<(), OperationsError> {
        review_receipt.verify_against_subject(subject)?;
        self.verify_integrity()?;

        if self.run_id != subject.run_id
            || self.task_id != subject.task_id
            || self.task_worktree_id != subject.task_worktree_id
            || self.task_branch_name != subject.task_branch_name
            || self.repository_path != subject.repository_path
            || self.work_area_root != subject.work_area_root
            || self.base_commit != subject.base_commit
            || self.accepted_task_head_commit != subject.task_head_commit
            || self.review_subject_digest != subject.subject_digest
            || self.review_receipt_id != review_receipt.receipt_id
            || self.review_receipt_digest != review_receipt.receipt_digest
        {
            return Err(OperationsError::ReceiptInvalidated(
                "integration receipt does not match approved review subject and review receipt"
                    .to_string(),
            ));
        }

        if let Some(ref agg) = self.job_aggregation {
            if agg.original_task_run_id != subject.run_id
                || agg.accepted_task_tree_sha != subject.task_tree_sha
                || agg.accepted_task_diff_digest != subject.diff_digest
                || agg.task_allowed_write_paths != subject.allowed_write_paths
                || agg.task_policy != subject.policy
                || agg.task_test_evidence != subject.test_evidence
            {
                return Err(OperationsError::ReceiptInvalidated(
                    "integration receipt job_aggregation provenance does not match approved review subject"
                        .to_string(),
                ));
            }
        }
        Ok(())
    }

    /// Verifies that this integration receipt carries [`JobAggregationReceiptProvenance`] matching
    /// `subject`, `review_receipt`, `expected_job_scope`, `expected_logical_task_id`, and `expected_task_digest`.
    pub fn verify_against_job_scope(
        &self,
        subject: &ReviewSubject,
        review_receipt: &ReviewReceipt,
        expected_job_scope: &JobIntegrationScopeProvenance,
        expected_logical_task_id: &str,
        expected_task_digest: &str,
    ) -> Result<(), OperationsError> {
        expected_job_scope.validate()?;
        validate_simple_identifier(expected_logical_task_id, "expected_logical_task_id")?;
        let normalized_task_digest =
            validate_sha256_hex_digest(expected_task_digest, "expected_task_digest")?;
        self.verify_against_review(subject, review_receipt)?;
        let Some(ref agg) = self.job_aggregation else {
            return Err(OperationsError::ReceiptInvalidated(
                "integration receipt is missing job_aggregation provenance".to_string(),
            ));
        };
        if agg.job_id != expected_job_scope.job_id
            || agg.job_revision != expected_job_scope.job_revision
            || agg.approval_id != expected_job_scope.approval_id
            || agg.approval_subject_digest != expected_job_scope.approval_subject_digest
            || agg.target_integration_run_id != expected_job_scope.target_integration_run_id
            || agg.job_aggregate_allowed_write_paths
                != expected_job_scope.aggregate_allowed_write_paths
            || agg.logical_task_id != expected_logical_task_id
            || agg.task_digest != normalized_task_digest
        {
            return Err(OperationsError::ReceiptInvalidated(
                "integration receipt job_aggregation provenance does not match expected job scope"
                    .to_string(),
            ));
        }
        Ok(())
    }
}

/// Request to perform proof-verified cleanup of an integrated task worktree using its
/// [`ReviewSubject`], [`ReviewReceipt`], and [`IntegrationReceipt`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VerifiedIntegratedCleanupRequest {
    /// Trusted private work-area root directory.
    pub work_area_root: PathBuf,
    /// Owned task worktree identifier (`WorktreeKind::Task`).
    pub task_worktree_id: String,
    /// Approved review subject.
    pub subject: ReviewSubject,
    /// Frozen review receipt matching `subject`.
    pub review_receipt: ReviewReceipt,
    /// Frozen integration receipt proving `subject` was integrated into `integrated_target_commit`.
    pub integration_receipt: IntegrationReceipt,
    /// Expected current 40-hex `HEAD` commit SHA of the primary source repository.
    pub expected_source_head_commit: String,
}

/// Request to build a read-only [`PushProposal`] for an integrated commit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PushProposalRequest {
    /// Approved review subject.
    pub subject: ReviewSubject,
    /// Frozen review receipt.
    pub review_receipt: ReviewReceipt,
    /// Frozen integration receipt.
    pub integration_receipt: IntegrationReceipt,
    /// Remote name (e.g. `"origin"`).
    pub remote_name: String,
    /// Target branch name on the remote (without `refs/heads/` prefix, e.g. `"main"` or `"threadlane/run/..."`).
    pub remote_target_branch: String,
    /// Optional expected current 40-hex `HEAD` SHA of `remote_target_branch` on the remote for `--force-with-lease`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_remote_target_head: Option<String>,
}

/// Read-only push plan describing the verified commit, remote target, and exact Git argv.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PushProposal {
    /// Canonical repository path.
    pub repository_path: PathBuf,
    /// Remote name.
    pub remote_name: String,
    /// Source commit SHA to push (`integration_receipt.integrated_target_commit`).
    pub source_commit: String,
    /// Accepted task head commit SHA contained in `source_commit`.
    pub accepted_task_head_commit: String,
    /// Remote target branch name.
    pub remote_target_branch: String,
    /// Optional expected remote target head SHA for lease verification.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_remote_target_head: Option<String>,
    /// Bound integration receipt ID.
    pub integration_receipt_id: String,
    /// Exact Git argument vector (excluding `git -C <repo>`) planned for execution.
    pub planned_git_argv: Vec<String>,
    /// Always `true`: proposal generation never executes a push.
    pub read_only: bool,
    /// Canonical 64-hex SHA-256 digest of this proposal.
    pub proposal_digest: String,
}

/// Explicit operator request to push an integrated commit to a remote branch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutePushRequest {
    /// Approved review subject.
    pub subject: ReviewSubject,
    /// Frozen review receipt.
    pub review_receipt: ReviewReceipt,
    /// Frozen integration receipt.
    pub integration_receipt: IntegrationReceipt,
    /// Remote name (e.g. `"origin"`).
    pub remote_name: String,
    /// Target branch name on the remote (without `refs/heads/` prefix).
    pub remote_target_branch: String,
    /// Optional expected current 40-hex `HEAD` SHA of `remote_target_branch` on the remote.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_remote_target_head: Option<String>,
    /// Expected current 40-hex `HEAD` SHA of the primary source repository.
    pub expected_source_head_commit: String,
    /// Operator identifier performing the push.
    pub operator_id: String,
    /// Wall-clock timestamp (milliseconds since Unix epoch).
    pub pushed_at_ms: i64,
}

/// Frozen receipt recording a completed explicit operator push operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PushReceipt {
    /// Schema version (`1`).
    pub schema_version: u32,
    /// Unique push receipt identifier.
    pub receipt_id: String,
    /// Remote name.
    pub remote_name: String,
    /// Remote target branch name.
    pub remote_target_branch: String,
    /// Pushed commit SHA (`integrated_target_commit`).
    pub pushed_commit: String,
    /// Accepted task head commit SHA verified in the pushed commit's ancestry.
    pub accepted_task_head_commit: String,
    /// Verified remote target branch head SHA after push.
    pub verified_remote_target_head: String,
    /// Bound integration receipt ID.
    pub integration_receipt_id: String,
    /// Bound review subject digest.
    pub review_subject_digest: String,
    /// Operator identifier.
    pub operator_id: String,
    /// Timestamp in milliseconds since Unix epoch.
    pub pushed_at_ms: i64,
    /// Canonical 64-hex SHA-256 digest of this receipt.
    pub receipt_digest: String,
}

/// Request to build a read-only [`RemoteDeleteProposal`] for a remote task branch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RemoteDeleteProposalRequest {
    /// Approved review subject.
    pub subject: ReviewSubject,
    /// Frozen review receipt.
    pub review_receipt: ReviewReceipt,
    /// Frozen integration receipt.
    pub integration_receipt: IntegrationReceipt,
    /// Remote name (e.g. `"origin"`).
    pub remote_name: String,
    /// Remote target branch name that must already contain `accepted_task_head_commit` on the remote.
    pub remote_target_branch: String,
    /// Remote task branch name to delete (must start with `threadlane/task/`).
    pub remote_source_branch: String,
    /// Expected 40-hex commit SHA of `remote_source_branch` on the remote (must equal `accepted_task_head_commit`).
    pub expected_remote_source_head: String,
}

/// Read-only remote branch deletion plan.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RemoteDeleteProposal {
    /// Canonical repository path.
    pub repository_path: PathBuf,
    /// Remote name.
    pub remote_name: String,
    /// Remote target branch that must contain `accepted_task_head_commit`.
    pub remote_target_branch: String,
    /// Remote task branch to delete (`threadlane/task/...`).
    pub remote_source_branch: String,
    /// Expected 40-hex commit SHA leased on `remote_source_branch`.
    pub expected_remote_source_head: String,
    /// Bound integration receipt ID.
    pub integration_receipt_id: String,
    /// Exact Git argument vector (excluding `git -C <repo>`) planned for execution.
    pub planned_git_argv: Vec<String>,
    /// Always `true`: proposal generation never mutates the remote.
    pub read_only: bool,
    /// Canonical 64-hex SHA-256 digest of this proposal.
    pub proposal_digest: String,
}

/// Explicit operator request to delete an integrated `threadlane/task/...` branch on a remote.
///
/// Requires that the actual remote target branch (`remote_target_branch`) already contains
/// `accepted_task_head_commit` in its ancestry AND that the remote source branch head matches
/// `expected_remote_source_head` via both `git ls-remote` and `--force-with-lease`. Never
/// blindly deletes a remote branch based on a local merge alone.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecuteRemoteDeleteRequest {
    /// Approved review subject.
    pub subject: ReviewSubject,
    /// Frozen review receipt.
    pub review_receipt: ReviewReceipt,
    /// Frozen integration receipt.
    pub integration_receipt: IntegrationReceipt,
    /// Remote name (e.g. `"origin"`).
    pub remote_name: String,
    /// Remote target branch name that must contain `accepted_task_head_commit` on the remote.
    pub remote_target_branch: String,
    /// Remote task branch name to delete (must start with `threadlane/task/`).
    pub remote_source_branch: String,
    /// Expected 40-hex commit SHA of `remote_source_branch` on the remote (must equal `accepted_task_head_commit`).
    pub expected_remote_source_head: String,
    /// Expected current 40-hex `HEAD` SHA of the primary source repository.
    pub expected_source_head_commit: String,
    /// Operator identifier.
    pub operator_id: String,
    /// Wall-clock timestamp (milliseconds since Unix epoch).
    pub deleted_at_ms: i64,
}

/// Frozen receipt recording a completed lease- and ancestry-verified remote branch deletion.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RemoteDeleteReceipt {
    /// Schema version (`1`).
    pub schema_version: u32,
    /// Unique receipt identifier.
    pub receipt_id: String,
    /// Remote name.
    pub remote_name: String,
    /// Remote target branch verified to contain `accepted_task_head_commit`.
    pub remote_target_branch: String,
    /// Verified 40-hex commit SHA of `remote_target_branch` on the remote.
    pub verified_remote_target_head: String,
    /// Deleted remote task branch name (`threadlane/task/...`).
    pub deleted_remote_source_branch: String,
    /// Leased 40-hex commit SHA of the deleted remote branch.
    pub leased_remote_source_head: String,
    /// Bound integration receipt ID.
    pub integration_receipt_id: String,
    /// Bound review subject digest.
    pub review_subject_digest: String,
    /// Operator identifier.
    pub operator_id: String,
    /// Timestamp in milliseconds since Unix epoch.
    pub deleted_at_ms: i64,
    /// Canonical 64-hex SHA-256 digest of this receipt.
    pub receipt_digest: String,
}

/// Computes a read-only [`IntegrationProposal`] for integrating an approved task worktree into
/// an owned `WorktreeKind::RunIntegration` worktree.
///
/// Performs no checkout or branch mutations.
pub fn propose_integration(
    request: &IntegrationProposalRequest,
) -> Result<IntegrationProposal, OperationsError> {
    verify_review_receipt(&request.subject, &request.review_receipt)?;
    if request.task_worktree_id != request.subject.task_worktree_id {
        return Err(OperationsError::SubjectMismatch(format!(
            "request.task_worktree_id `{}` does not match subject.task_worktree_id `{}`",
            request.task_worktree_id, request.subject.task_worktree_id
        )));
    }

    let integration_manifest = load_and_verify_integration_manifest(
        &request.work_area_root,
        &request.integration_worktree_id,
    )?;
    let task_manifest =
        load_and_verify_task_manifest(&request.work_area_root, &request.task_worktree_id)?;

    verify_matching_worktree_owners(&integration_manifest, &task_manifest, &request.subject)?;
    ensure_worktree_inactive(&integration_manifest)?;
    ensure_worktree_inactive(&task_manifest)?;

    let integration_checkout = validate_existing_canonical_dir(
        &integration_manifest.checkout_path,
        "integration checkout_path",
    )?;
    let task_checkout =
        validate_existing_canonical_dir(&task_manifest.checkout_path, "task checkout_path")?;

    verify_worktree_branch(
        &integration_checkout,
        &integration_manifest.branch_name,
        &integration_manifest.worktree_id,
    )?;
    verify_worktree_branch(
        &task_checkout,
        &task_manifest.branch_name,
        &task_manifest.worktree_id,
    )?;

    if !is_worktree_clean(&integration_checkout, DEFAULT_GIT_COMMAND_TIMEOUT_MS)? {
        return Err(OperationsError::DirtyWorktree {
            worktree_id: integration_manifest.worktree_id,
            detail: "integration worktree has uncommitted, untracked, or ignored files".to_string(),
        });
    }
    if !is_worktree_clean(&task_checkout, DEFAULT_GIT_COMMAND_TIMEOUT_MS)? {
        return Err(OperationsError::DirtyWorktree {
            worktree_id: task_manifest.worktree_id,
            detail: "task worktree has uncommitted, untracked, or ignored files".to_string(),
        });
    }

    let source_head_commit = resolve_exact_commit_sha(
        &integration_manifest.repository_path,
        "HEAD",
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )?;
    let integration_head_commit = resolve_exact_commit_sha(
        &integration_checkout,
        "HEAD",
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )?;
    let task_head_commit =
        resolve_exact_commit_sha(&task_checkout, "HEAD", DEFAULT_GIT_COMMAND_TIMEOUT_MS)?;
    let task_tree_sha =
        resolve_exact_tree_sha(&task_checkout, "HEAD", DEFAULT_GIT_COMMAND_TIMEOUT_MS)?;

    if task_head_commit != request.subject.task_head_commit
        || task_tree_sha != request.subject.task_tree_sha
    {
        return Err(OperationsError::ReceiptInvalidated(
            "task worktree HEAD or tree SHA changed since review receipt was issued".to_string(),
        ));
    }

    let can_fast_forward = is_commit_ancestor(
        &integration_checkout,
        &integration_head_commit,
        &task_head_commit,
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )?;
    let already_integrated = is_commit_ancestor(
        &integration_checkout,
        &task_head_commit,
        &integration_head_commit,
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )?;
    let merge_tree_clean = check_merge_tree_conflict_free(
        &integration_checkout,
        &integration_head_commit,
        &task_head_commit,
    )?;

    let clean_merge_predicted = match request.strategy {
        IntegrationStrategy::FastForwardOnly => can_fast_forward,
        IntegrationStrategy::MergeNoFf => merge_tree_clean,
    };

    let planned_git_argv = build_integration_git_argv(
        request.strategy,
        &request.subject.run_id,
        &request.subject.task_id,
        &task_head_commit,
        &request.review_receipt.receipt_id,
    );

    let mut proposal = IntegrationProposal {
        run_id: request.subject.run_id.clone(),
        task_id: request.subject.task_id.clone(),
        integration_worktree_id: integration_manifest.worktree_id,
        integration_checkout_path: integration_checkout,
        integration_branch_name: integration_manifest.branch_name,
        task_worktree_id: task_manifest.worktree_id,
        task_checkout_path: task_checkout,
        task_branch_name: task_manifest.branch_name,
        repository_path: integration_manifest.repository_path,
        source_head_commit,
        base_commit: request.subject.base_commit.clone(),
        integration_head_commit,
        task_head_commit,
        task_tree_sha,
        review_subject_digest: request.subject.subject_digest.clone(),
        review_receipt_id: request.review_receipt.receipt_id.clone(),
        review_receipt_digest: request.review_receipt.receipt_digest.clone(),
        strategy: request.strategy,
        can_fast_forward,
        already_integrated,
        clean_merge_predicted,
        planned_git_argv,
        read_only: true,
        proposal_digest: String::new(),
    };
    proposal.proposal_digest = proposal.compute_digest()?;
    Ok(proposal)
}

/// Explicit operator action to integrate an approved [`ReviewSubject`] into an owned
/// `WorktreeKind::RunIntegration` checkout and freeze an [`IntegrationReceipt`].
///
/// Operates exclusively inside the owned integration worktree checkout, preserves the primary
/// source checkout and any dirty artifacts, verifies current heads and mergeability before
/// mutating refs, and persists the receipt atomically (`0600`).
pub fn execute_integration(
    request: &ExecuteIntegrationRequest,
) -> Result<IntegrationReceipt, OperationsError> {
    validate_simple_identifier(&request.operator_id, "operator_id")?;
    let expected_int_head = validate_exact_40_hex_sha(
        &request.expected_integration_head_commit,
        "expected_integration_head_commit",
    )?;
    let expected_task_head = validate_exact_40_hex_sha(
        &request.expected_task_head_commit,
        "expected_task_head_commit",
    )?;
    let expected_source_head = validate_exact_40_hex_sha(
        &request.expected_source_head_commit,
        "expected_source_head_commit",
    )?;

    if request.task_worktree_id != request.subject.task_worktree_id {
        return Err(OperationsError::SubjectMismatch(format!(
            "request.task_worktree_id `{}` does not match subject.task_worktree_id `{}`",
            request.task_worktree_id, request.subject.task_worktree_id
        )));
    }
    if expected_task_head != request.subject.task_head_commit {
        return Err(OperationsError::HeadMoved {
            target: format!("task worktree `{}`", request.task_worktree_id),
            expected: request.subject.task_head_commit.clone(),
            actual: expected_task_head,
        });
    }

    // 1. Verify review subject and receipt (including live task worktree state).
    verify_review_receipt(&request.subject, &request.review_receipt)?;

    let integration_manifest = load_and_verify_integration_manifest(
        &request.work_area_root,
        &request.integration_worktree_id,
    )?;
    let task_manifest =
        load_and_verify_task_manifest(&request.work_area_root, &request.task_worktree_id)?;
    verify_matching_worktree_owners(&integration_manifest, &task_manifest, &request.subject)?;

    // 2. Acquire exclusive active-job locks on both the integration worktree and task worktree.
    let int_guard = begin_worktree_job(
        &integration_manifest.work_area_root,
        &integration_manifest.worktree_id,
    )?;
    let task_guard =
        match begin_worktree_job(&task_manifest.work_area_root, &task_manifest.worktree_id) {
            Ok(guard) => guard,
            Err(err) => {
                let _ = int_guard.finish();
                return Err(err.into());
            }
        };

    let result = (|| -> Result<IntegrationReceipt, OperationsError> {
        let integration_checkout = validate_existing_canonical_dir(
            &integration_manifest.checkout_path,
            "integration checkout_path",
        )?;
        let task_checkout =
            validate_existing_canonical_dir(&task_manifest.checkout_path, "task checkout_path")?;

        verify_worktree_branch(
            &integration_checkout,
            &integration_manifest.branch_name,
            &integration_manifest.worktree_id,
        )?;
        verify_worktree_branch(
            &task_checkout,
            &task_manifest.branch_name,
            &task_manifest.worktree_id,
        )?;

        if !is_worktree_clean(&integration_checkout, DEFAULT_GIT_COMMAND_TIMEOUT_MS)? {
            return Err(OperationsError::DirtyWorktree {
                worktree_id: integration_manifest.worktree_id.clone(),
                detail: "integration worktree has uncommitted, untracked, or ignored files"
                    .to_string(),
            });
        }
        if !is_worktree_clean(&task_checkout, DEFAULT_GIT_COMMAND_TIMEOUT_MS)? {
            return Err(OperationsError::DirtyWorktree {
                worktree_id: task_manifest.worktree_id.clone(),
                detail: "task worktree has uncommitted, untracked, or ignored files".to_string(),
            });
        }

        // Verify current HEADs of primary source repo, integration worktree, and task worktree.
        let actual_source_head = resolve_exact_commit_sha(
            &integration_manifest.repository_path,
            "HEAD",
            DEFAULT_GIT_COMMAND_TIMEOUT_MS,
        )?;
        if actual_source_head != expected_source_head {
            return Err(OperationsError::HeadMoved {
                target: "primary source repository".to_string(),
                expected: expected_source_head.clone(),
                actual: actual_source_head,
            });
        }

        let actual_int_head = resolve_exact_commit_sha(
            &integration_checkout,
            "HEAD",
            DEFAULT_GIT_COMMAND_TIMEOUT_MS,
        )?;
        if actual_int_head != expected_int_head {
            return Err(OperationsError::HeadMoved {
                target: format!(
                    "integration worktree `{}`",
                    integration_manifest.worktree_id
                ),
                expected: expected_int_head.clone(),
                actual: actual_int_head,
            });
        }

        let actual_task_head =
            resolve_exact_commit_sha(&task_checkout, "HEAD", DEFAULT_GIT_COMMAND_TIMEOUT_MS)?;
        if actual_task_head != expected_task_head {
            return Err(OperationsError::HeadMoved {
                target: format!("task worktree `{}`", task_manifest.worktree_id),
                expected: expected_task_head.clone(),
                actual: actual_task_head,
            });
        }

        let diff_gate =
            evaluate_diff_gate(&task_manifest.work_area_root, &task_manifest.worktree_id)?;
        if !diff_gate.passed || !diff_gate.evidence_complete {
            return Err(OperationsError::DiffGateRejected {
                worktree_id: task_manifest.worktree_id.clone(),
                detail: "task worktree failed diff gate re-verification before integration"
                    .to_string(),
            });
        }

        // Pre-verify mergeability without mutating the integration worktree so conflicts are
        // refused cleanly before touching the working tree.
        match request.strategy {
            IntegrationStrategy::FastForwardOnly => {
                let can_ff = is_commit_ancestor(
                    &integration_checkout,
                    &actual_int_head,
                    &actual_task_head,
                    DEFAULT_GIT_COMMAND_TIMEOUT_MS,
                )?;
                if !can_ff {
                    return Err(OperationsError::AncestryMismatch(format!(
                        "integration HEAD `{actual_int_head}` cannot fast-forward to task HEAD `{actual_task_head}`"
                    )));
                }
            }
            IntegrationStrategy::MergeNoFf => {
                let conflict_free = check_merge_tree_conflict_free(
                    &integration_checkout,
                    &actual_int_head,
                    &actual_task_head,
                )?;
                if !conflict_free {
                    return Err(OperationsError::MergeConflict(format!(
                        "merging task HEAD `{actual_task_head}` into integration HEAD `{actual_int_head}` has conflicts; integration worktree preserved untouched"
                    )));
                }
            }
        }

        let git_argv = build_integration_git_argv(
            request.strategy,
            &request.subject.run_id,
            &request.subject.task_id,
            &actual_task_head,
            &request.review_receipt.receipt_id,
        );
        let argv_refs: Vec<&str> = git_argv.iter().map(String::as_str).collect();
        run_sanitized_git(
            &integration_checkout,
            &argv_refs,
            DEFAULT_GIT_COMMAND_TIMEOUT_MS,
        )?;

        if !is_worktree_clean(&integration_checkout, DEFAULT_GIT_COMMAND_TIMEOUT_MS)? {
            return Err(OperationsError::DirtyWorktree {
                worktree_id: integration_manifest.worktree_id.clone(),
                detail: "integration worktree is not clean after merge execution".to_string(),
            });
        }

        let integrated_target_commit = resolve_exact_commit_sha(
            &integration_checkout,
            "HEAD",
            DEFAULT_GIT_COMMAND_TIMEOUT_MS,
        )?;
        let integrated_tree_sha = resolve_exact_tree_sha(
            &integration_checkout,
            "HEAD",
            DEFAULT_GIT_COMMAND_TIMEOUT_MS,
        )?;

        let task_in_target = is_commit_ancestor(
            &integration_checkout,
            &actual_task_head,
            &integrated_target_commit,
            DEFAULT_GIT_COMMAND_TIMEOUT_MS,
        )?;
        if !task_in_target {
            return Err(OperationsError::AncestryMismatch(format!(
                "accepted task commit `{actual_task_head}` is not an ancestor of integrated target commit `{integrated_target_commit}`"
            )));
        }

        let post_source_head = resolve_exact_commit_sha(
            &integration_manifest.repository_path,
            "HEAD",
            DEFAULT_GIT_COMMAND_TIMEOUT_MS,
        )?;
        if post_source_head != expected_source_head {
            return Err(OperationsError::SecurityViolation(format!(
                "primary source repository HEAD unexpectedly changed from `{expected_source_head}` to `{post_source_head}`"
            )));
        }

        let receipt_id = match request.receipt_id {
            Some(ref id) => {
                validate_simple_identifier(id, "receipt_id")?;
                id.clone()
            }
            None => format!("int_{}", &Uuid::new_v4().simple().to_string()[..16]),
        };

        let mut receipt = IntegrationReceipt {
            schema_version: INTEGRATION_RECEIPT_SCHEMA_VERSION,
            receipt_id: receipt_id.clone(),
            run_id: request.subject.run_id.clone(),
            task_id: request.subject.task_id.clone(),
            integration_worktree_id: integration_manifest.worktree_id.clone(),
            integration_checkout_path: integration_checkout,
            integration_branch_name: integration_manifest.branch_name.clone(),
            task_worktree_id: task_manifest.worktree_id.clone(),
            task_branch_name: task_manifest.branch_name.clone(),
            repository_path: integration_manifest.repository_path.clone(),
            work_area_root: integration_manifest.work_area_root.clone(),
            base_commit: request.subject.base_commit.clone(),
            previous_integration_head_commit: actual_int_head,
            accepted_task_head_commit: actual_task_head,
            integrated_target_commit,
            integrated_tree_sha,
            source_head_commit: post_source_head,
            review_subject_digest: request.subject.subject_digest.clone(),
            review_receipt_id: request.review_receipt.receipt_id.clone(),
            review_receipt_digest: request.review_receipt.receipt_digest.clone(),
            strategy: request.strategy,
            operator_id: request.operator_id.clone(),
            integrated_at_ms: request.integrated_at_ms,
            job_aggregation: None,
            receipt_digest: String::new(),
        };
        receipt.receipt_digest = receipt.compute_digest()?;
        receipt.verify_against_review(&request.subject, &request.review_receipt)?;

        let receipts_dir = integration_manifest
            .work_area_root
            .join(MANIFESTS_SUBDIR)
            .join(RECEIPTS_SUBDIR);
        let filename = format!("integration-{receipt_id}.json");
        write_private_json_atomic(&receipts_dir, &filename, &receipt, "integration receipt")?;

        Ok(receipt)
    })();

    let _ = task_guard.finish()?;
    let _ = int_guard.finish()?;
    result
}

/// Loads and verifies a persisted [`IntegrationReceipt`] from `<work_area_root>/manifests/receipts/integration-<receipt_id>.json`.
pub fn load_integration_receipt(
    work_area_root: &Path,
    receipt_id: &str,
) -> Result<IntegrationReceipt, OperationsError> {
    validate_simple_identifier(receipt_id, "receipt_id")?;
    let canonical_work_area = validate_existing_canonical_dir(work_area_root, "work_area_root")?;
    let receipt_path = canonical_work_area
        .join(MANIFESTS_SUBDIR)
        .join(RECEIPTS_SUBDIR)
        .join(format!("integration-{receipt_id}.json"));
    let receipt: IntegrationReceipt =
        read_private_json(&receipt_path, MAX_RECEIPT_BYTES, "integration receipt")?;
    if receipt.receipt_id != receipt_id || receipt.work_area_root != canonical_work_area {
        return Err(OperationsError::ReceiptInvalidated(
            "loaded integration receipt identity or work_area_root mismatch".to_string(),
        ));
    }
    receipt.verify_integrity()?;
    Ok(receipt)
}

/// Performs proof-verified cleanup of an integrated task worktree after verifying its
/// [`ReviewSubject`], [`ReviewReceipt`], and [`IntegrationReceipt`].
pub fn cleanup_integrated_task_worktree(
    request: &VerifiedIntegratedCleanupRequest,
) -> Result<IntegratedCleanupReport, OperationsError> {
    request
        .integration_receipt
        .verify_against_review(&request.subject, &request.review_receipt)?;

    if request.task_worktree_id != request.subject.task_worktree_id
        || request.task_worktree_id != request.integration_receipt.task_worktree_id
    {
        return Err(OperationsError::ReceiptInvalidated(format!(
            "cleanup task_worktree_id `{}` does not match receipt task_worktree_id `{}`",
            request.task_worktree_id, request.integration_receipt.task_worktree_id
        )));
    }

    let canonical_work_area =
        validate_existing_canonical_dir(&request.work_area_root, "work_area_root")?;
    if canonical_work_area != request.subject.work_area_root {
        return Err(OperationsError::ReceiptInvalidated(
            "cleanup work_area_root does not match approved review subject".to_string(),
        ));
    }

    // Also verify that the persisted receipts on disk match the supplied receipts.
    let disk_review =
        load_review_receipt(&canonical_work_area, &request.review_receipt.receipt_id)?;
    if disk_review != request.review_receipt {
        return Err(OperationsError::ReceiptInvalidated(
            "supplied review_receipt does not match persisted review receipt in work_area_root"
                .to_string(),
        ));
    }
    let disk_integration = load_integration_receipt(
        &canonical_work_area,
        &request.integration_receipt.receipt_id,
    )?;
    if disk_integration != request.integration_receipt {
        return Err(OperationsError::ReceiptInvalidated(
            "supplied integration_receipt does not match persisted integration receipt in work_area_root"
                .to_string(),
        ));
    }

    // If the integration worktree still exists, verify its current HEAD still contains
    // `integrated_target_commit` as an ancestor.
    if let Ok(int_manifest) = load_and_verify_integration_manifest(
        &canonical_work_area,
        &request.integration_receipt.integration_worktree_id,
    ) {
        if fs::symlink_metadata(&int_manifest.checkout_path).is_ok() {
            let current_int_head = resolve_exact_commit_sha(
                &int_manifest.checkout_path,
                "HEAD",
                DEFAULT_GIT_COMMAND_TIMEOUT_MS,
            )?;
            let target_still_in_int = is_commit_ancestor(
                &int_manifest.checkout_path,
                &request.integration_receipt.integrated_target_commit,
                &current_int_head,
                DEFAULT_GIT_COMMAND_TIMEOUT_MS,
            )?;
            if !target_still_in_int {
                return Err(OperationsError::AncestryMismatch(format!(
                    "integration worktree HEAD `{current_int_head}` no longer contains integrated_target_commit `{}`",
                    request.integration_receipt.integrated_target_commit
                )));
            }
        }
    }

    let report = cleanup_integrated_worktree(&IntegratedCleanupRequest {
        work_area_root: canonical_work_area,
        worktree_id: request.task_worktree_id.clone(),
        expected_base_commit: request.subject.base_commit.clone(),
        accepted_head_commit: request
            .integration_receipt
            .accepted_task_head_commit
            .clone(),
        expected_target_commit: request.integration_receipt.integrated_target_commit.clone(),
        expected_source_head_commit: request.expected_source_head_commit.clone(),
        review_subject_digest: request.subject.subject_digest.clone(),
        review_receipt_id: request.review_receipt.receipt_id.clone(),
        integration_receipt_id: request.integration_receipt.receipt_id.clone(),
    })?;

    Ok(report)
}

/// Builds a read-only [`PushProposal`] for pushing an integrated commit to a remote branch.
pub fn propose_push(request: &PushProposalRequest) -> Result<PushProposal, OperationsError> {
    request
        .integration_receipt
        .verify_against_review(&request.subject, &request.review_receipt)?;
    validate_remote_name(&request.remote_name)?;
    validate_branch_ref_name(&request.remote_target_branch, "remote_target_branch")?;
    let expected_remote_head = match request.expected_remote_target_head {
        Some(ref sha) => Some(validate_exact_40_hex_sha(
            sha,
            "expected_remote_target_head",
        )?),
        None => None,
    };

    let planned_git_argv = build_push_git_argv(
        &request.remote_name,
        &request.integration_receipt.integrated_target_commit,
        &request.remote_target_branch,
        expected_remote_head.as_deref(),
    );

    let mut proposal = PushProposal {
        repository_path: request.integration_receipt.repository_path.clone(),
        remote_name: request.remote_name.clone(),
        source_commit: request.integration_receipt.integrated_target_commit.clone(),
        accepted_task_head_commit: request
            .integration_receipt
            .accepted_task_head_commit
            .clone(),
        remote_target_branch: request.remote_target_branch.clone(),
        expected_remote_target_head: expected_remote_head,
        integration_receipt_id: request.integration_receipt.receipt_id.clone(),
        planned_git_argv,
        read_only: true,
        proposal_digest: String::new(),
    };
    let encoded = serde_json::to_vec(&proposal)?;
    proposal.proposal_digest = sha256_hex(&encoded);
    Ok(proposal)
}

/// Explicit operator action to push a verified [`IntegrationReceipt`]'s target commit to a remote branch.
///
/// Push is strictly separate from integration and cleanup. Verifies receipt integrity, primary
/// source checkout `HEAD`, and optional remote lease (`--force-with-lease`) before pushing.
pub fn execute_push(request: &ExecutePushRequest) -> Result<PushReceipt, OperationsError> {
    validate_simple_identifier(&request.operator_id, "operator_id")?;
    let expected_source_head = validate_exact_40_hex_sha(
        &request.expected_source_head_commit,
        "expected_source_head_commit",
    )?;
    let proposal = propose_push(&PushProposalRequest {
        subject: request.subject.clone(),
        review_receipt: request.review_receipt.clone(),
        integration_receipt: request.integration_receipt.clone(),
        remote_name: request.remote_name.clone(),
        remote_target_branch: request.remote_target_branch.clone(),
        expected_remote_target_head: request.expected_remote_target_head.clone(),
    })?;

    let repo_path = validate_existing_canonical_dir(&proposal.repository_path, "repository_path")?;
    let actual_source_head =
        resolve_exact_commit_sha(&repo_path, "HEAD", DEFAULT_GIT_COMMAND_TIMEOUT_MS)?;
    if actual_source_head != expected_source_head {
        return Err(OperationsError::HeadMoved {
            target: "primary source repository".to_string(),
            expected: expected_source_head,
            actual: actual_source_head,
        });
    }

    let accepted_in_source_commit = is_commit_ancestor(
        &repo_path,
        &proposal.accepted_task_head_commit,
        &proposal.source_commit,
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )?;
    if !accepted_in_source_commit {
        return Err(OperationsError::AncestryMismatch(format!(
            "accepted_task_head_commit `{}` is not an ancestor of source_commit `{}`",
            proposal.accepted_task_head_commit, proposal.source_commit
        )));
    }

    if let Some(ref expected_remote_sha) = proposal.expected_remote_target_head {
        let remote_heads = list_remote_heads(&repo_path, &proposal.remote_name)?;
        let actual_remote_sha = remote_heads.get(&proposal.remote_target_branch).cloned();
        if actual_remote_sha.as_deref() != Some(expected_remote_sha.as_str()) {
            return Err(OperationsError::RemoteLeaseMismatch {
                remote: proposal.remote_name,
                branch: proposal.remote_target_branch,
                expected: expected_remote_sha.clone(),
                actual: actual_remote_sha,
            });
        }
    }

    let argv_refs: Vec<&str> = proposal
        .planned_git_argv
        .iter()
        .map(String::as_str)
        .collect();
    run_sanitized_git(&repo_path, &argv_refs, DEFAULT_GIT_COMMAND_TIMEOUT_MS)?;

    let post_remote_heads = list_remote_heads(&repo_path, &proposal.remote_name)?;
    let verified_remote_target_head = post_remote_heads
        .get(&proposal.remote_target_branch)
        .cloned()
        .ok_or_else(|| OperationsError::RemoteAncestryMissing {
            remote: proposal.remote_name.clone(),
            target_branch: proposal.remote_target_branch.clone(),
            accepted_head: proposal.accepted_task_head_commit.clone(),
            detail: "remote target branch was not found on remote after push".to_string(),
        })?;

    if verified_remote_target_head != proposal.source_commit {
        return Err(OperationsError::RemoteLeaseMismatch {
            remote: proposal.remote_name,
            branch: proposal.remote_target_branch,
            expected: proposal.source_commit,
            actual: Some(verified_remote_target_head),
        });
    }

    let receipt_id = format!("psh_{}", &Uuid::new_v4().simple().to_string()[..16]);
    let mut receipt = PushReceipt {
        schema_version: PUSH_RECEIPT_SCHEMA_VERSION,
        receipt_id: receipt_id.clone(),
        remote_name: proposal.remote_name,
        remote_target_branch: proposal.remote_target_branch,
        pushed_commit: proposal.source_commit,
        accepted_task_head_commit: proposal.accepted_task_head_commit,
        verified_remote_target_head,
        integration_receipt_id: request.integration_receipt.receipt_id.clone(),
        review_subject_digest: request.subject.subject_digest.clone(),
        operator_id: request.operator_id.clone(),
        pushed_at_ms: request.pushed_at_ms,
        receipt_digest: String::new(),
    };
    let encoded = serde_json::to_vec(&receipt)?;
    receipt.receipt_digest = sha256_hex(&encoded);

    let receipts_dir = request
        .integration_receipt
        .work_area_root
        .join(MANIFESTS_SUBDIR)
        .join(RECEIPTS_SUBDIR);
    let filename = format!("push-{receipt_id}.json");
    write_private_json_atomic(&receipts_dir, &filename, &receipt, "push receipt")?;

    Ok(receipt)
}

/// Builds a read-only [`RemoteDeleteProposal`] for deleting an integrated `threadlane/task/...`
/// branch on a remote.
pub fn propose_remote_branch_delete(
    request: &RemoteDeleteProposalRequest,
) -> Result<RemoteDeleteProposal, OperationsError> {
    request
        .integration_receipt
        .verify_against_review(&request.subject, &request.review_receipt)?;
    validate_remote_name(&request.remote_name)?;
    validate_branch_ref_name(&request.remote_target_branch, "remote_target_branch")?;
    validate_branch_ref_name(&request.remote_source_branch, "remote_source_branch")?;

    if !request.remote_source_branch.starts_with("threadlane/task/") {
        return Err(OperationsError::InvalidInput(format!(
            "remote_source_branch `{}` must carry the software `threadlane/task/` prefix",
            request.remote_source_branch
        )));
    }
    if request.remote_source_branch == request.remote_target_branch {
        return Err(OperationsError::InvalidInput(
            "remote_source_branch and remote_target_branch must be distinct".to_string(),
        ));
    }

    let expected_source_sha = validate_exact_40_hex_sha(
        &request.expected_remote_source_head,
        "expected_remote_source_head",
    )?;
    if expected_source_sha != request.integration_receipt.accepted_task_head_commit {
        return Err(OperationsError::ReceiptInvalidated(format!(
            "expected_remote_source_head `{expected_source_sha}` does not match accepted_task_head_commit `{}`",
            request.integration_receipt.accepted_task_head_commit
        )));
    }

    let planned_git_argv = build_remote_delete_git_argv(
        &request.remote_name,
        &request.remote_source_branch,
        &expected_source_sha,
    );

    let mut proposal = RemoteDeleteProposal {
        repository_path: request.integration_receipt.repository_path.clone(),
        remote_name: request.remote_name.clone(),
        remote_target_branch: request.remote_target_branch.clone(),
        remote_source_branch: request.remote_source_branch.clone(),
        expected_remote_source_head: expected_source_sha,
        integration_receipt_id: request.integration_receipt.receipt_id.clone(),
        planned_git_argv,
        read_only: true,
        proposal_digest: String::new(),
    };
    let encoded = serde_json::to_vec(&proposal)?;
    proposal.proposal_digest = sha256_hex(&encoded);
    Ok(proposal)
}

/// Explicit operator action to delete an integrated `threadlane/task/...` branch on a remote.
///
/// Refuses deletion unless:
/// 1. `ReviewSubject`, `ReviewReceipt`, and `IntegrationReceipt` are valid and match;
/// 2. The primary source repository `HEAD` matches `expected_source_head_commit`;
/// 3. `refs/heads/<remote_source_branch>` on the actual remote equals `expected_remote_source_head`
///    (and `--force-with-lease` enforces this atomically during `git push --delete`);
/// 4. `refs/heads/<remote_target_branch>` on the actual remote exists AND contains
///    `accepted_task_head_commit` in its ancestry (so a local-only merge can never blindly
///    delete an unpushed remote task branch).
pub fn execute_remote_branch_delete(
    request: &ExecuteRemoteDeleteRequest,
) -> Result<RemoteDeleteReceipt, OperationsError> {
    validate_simple_identifier(&request.operator_id, "operator_id")?;
    let expected_source_head = validate_exact_40_hex_sha(
        &request.expected_source_head_commit,
        "expected_source_head_commit",
    )?;
    let proposal = propose_remote_branch_delete(&RemoteDeleteProposalRequest {
        subject: request.subject.clone(),
        review_receipt: request.review_receipt.clone(),
        integration_receipt: request.integration_receipt.clone(),
        remote_name: request.remote_name.clone(),
        remote_target_branch: request.remote_target_branch.clone(),
        remote_source_branch: request.remote_source_branch.clone(),
        expected_remote_source_head: request.expected_remote_source_head.clone(),
    })?;

    let repo_path = validate_existing_canonical_dir(&proposal.repository_path, "repository_path")?;
    let actual_source_head =
        resolve_exact_commit_sha(&repo_path, "HEAD", DEFAULT_GIT_COMMAND_TIMEOUT_MS)?;
    if actual_source_head != expected_source_head {
        return Err(OperationsError::HeadMoved {
            target: "primary source repository".to_string(),
            expected: expected_source_head,
            actual: actual_source_head,
        });
    }

    // Query actual remote branch heads via `git ls-remote --heads`.
    let remote_heads = list_remote_heads(&repo_path, &proposal.remote_name)?;

    let actual_remote_source_head = remote_heads.get(&proposal.remote_source_branch).cloned();
    if actual_remote_source_head.as_deref() != Some(proposal.expected_remote_source_head.as_str()) {
        return Err(OperationsError::RemoteLeaseMismatch {
            remote: proposal.remote_name,
            branch: proposal.remote_source_branch,
            expected: proposal.expected_remote_source_head,
            actual: actual_remote_source_head,
        });
    }

    let actual_remote_target_head = remote_heads
        .get(&proposal.remote_target_branch)
        .cloned()
        .ok_or_else(|| OperationsError::RemoteAncestryMissing {
            remote: proposal.remote_name.clone(),
            target_branch: proposal.remote_target_branch.clone(),
            accepted_head: request
                .integration_receipt
                .accepted_task_head_commit
                .clone(),
            detail: format!(
                "remote target branch `refs/heads/{}` does not exist on remote `{}`",
                proposal.remote_target_branch, proposal.remote_name
            ),
        })?;

    // Verify `actual_remote_target_head` is a known commit in our repository and contains
    // `accepted_task_head_commit` in its ancestry.
    let remote_target_known = resolve_exact_commit_sha(
        &repo_path,
        &actual_remote_target_head,
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .is_ok();
    if !remote_target_known {
        return Err(OperationsError::RemoteAncestryMissing {
            remote: proposal.remote_name,
            target_branch: proposal.remote_target_branch,
            accepted_head: request
                .integration_receipt
                .accepted_task_head_commit
                .clone(),
            detail: format!(
                "remote target head `{actual_remote_target_head}` is not present in local object store"
            ),
        });
    }

    let accepted_in_remote_target = is_commit_ancestor(
        &repo_path,
        &request.integration_receipt.accepted_task_head_commit,
        &actual_remote_target_head,
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )?;
    if !accepted_in_remote_target {
        return Err(OperationsError::RemoteAncestryMissing {
            remote: proposal.remote_name,
            target_branch: proposal.remote_target_branch,
            accepted_head: request
                .integration_receipt
                .accepted_task_head_commit
                .clone(),
            detail: format!(
                "remote target head `{actual_remote_target_head}` does not contain accepted_task_head_commit `{}`; refusing to delete remote branch on local merge only",
                request.integration_receipt.accepted_task_head_commit
            ),
        });
    }

    let argv_refs: Vec<&str> = proposal
        .planned_git_argv
        .iter()
        .map(String::as_str)
        .collect();
    run_sanitized_git(&repo_path, &argv_refs, DEFAULT_GIT_COMMAND_TIMEOUT_MS)?;

    let receipt_id = format!("rdel_{}", &Uuid::new_v4().simple().to_string()[..16]);
    let mut receipt = RemoteDeleteReceipt {
        schema_version: REMOTE_DELETE_RECEIPT_SCHEMA_VERSION,
        receipt_id: receipt_id.clone(),
        remote_name: proposal.remote_name,
        remote_target_branch: proposal.remote_target_branch,
        verified_remote_target_head: actual_remote_target_head,
        deleted_remote_source_branch: proposal.remote_source_branch,
        leased_remote_source_head: proposal.expected_remote_source_head,
        integration_receipt_id: request.integration_receipt.receipt_id.clone(),
        review_subject_digest: request.subject.subject_digest.clone(),
        operator_id: request.operator_id.clone(),
        deleted_at_ms: request.deleted_at_ms,
        receipt_digest: String::new(),
    };
    let encoded = serde_json::to_vec(&receipt)?;
    receipt.receipt_digest = sha256_hex(&encoded);

    let receipts_dir = request
        .integration_receipt
        .work_area_root
        .join(MANIFESTS_SUBDIR)
        .join(RECEIPTS_SUBDIR);
    let filename = format!("remote-delete-{receipt_id}.json");
    write_private_json_atomic(&receipts_dir, &filename, &receipt, "remote delete receipt")?;

    Ok(receipt)
}

fn load_and_verify_integration_manifest(
    work_area_root: &Path,
    integration_worktree_id: &str,
) -> Result<WorktreeManifest, OperationsError> {
    let manifest = load_and_verify_manifest(work_area_root, integration_worktree_id)?;
    if manifest.kind != WorktreeKind::RunIntegration {
        return Err(OperationsError::WorktreeNotOwned(format!(
            "worktree `{integration_worktree_id}` has kind `{:?}`, expected `RunIntegration`",
            manifest.kind
        )));
    }
    Ok(manifest)
}

fn verify_matching_worktree_owners(
    integration_manifest: &WorktreeManifest,
    task_manifest: &WorktreeManifest,
    subject: &ReviewSubject,
) -> Result<(), OperationsError> {
    if integration_manifest.repository_path != task_manifest.repository_path
        || integration_manifest.repository_path != subject.repository_path
    {
        return Err(OperationsError::WorktreeNotOwned(
            "integration worktree, task worktree, and review subject do not belong to the same repository_path".to_string(),
        ));
    }
    if integration_manifest.work_area_root != task_manifest.work_area_root
        || integration_manifest.work_area_root != subject.work_area_root
    {
        return Err(OperationsError::WorktreeNotOwned(
            "integration worktree, task worktree, and review subject do not belong to the same work_area_root".to_string(),
        ));
    }
    if integration_manifest.repository_common_git_dir != task_manifest.repository_common_git_dir {
        return Err(OperationsError::WorktreeNotOwned(
            "integration worktree and task worktree have mismatched repository_common_git_dir"
                .to_string(),
        ));
    }
    if integration_manifest.run_id != task_manifest.run_id
        || integration_manifest.run_id != subject.run_id
    {
        return Err(OperationsError::WorktreeNotOwned(format!(
            "integration worktree run_id `{}` does not match task run_id `{}`",
            integration_manifest.run_id, subject.run_id
        )));
    }
    if integration_manifest.base_commit != task_manifest.base_commit
        || integration_manifest.base_commit != subject.base_commit
    {
        return Err(OperationsError::SubjectMismatch(format!(
            "integration worktree base_commit `{}` does not match task base_commit `{}`",
            integration_manifest.base_commit, subject.base_commit
        )));
    }
    Ok(())
}

fn check_merge_tree_conflict_free(
    checkout_path: &Path,
    integration_head: &str,
    task_head: &str,
) -> Result<bool, OperationsError> {
    let out = run_sanitized_git_allow_nonzero(
        checkout_path,
        &[
            "merge-tree",
            "--write-tree",
            "--",
            integration_head,
            task_head,
        ],
        DEFAULT_MAX_GIT_OUTPUT_BYTES,
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )?;
    match out.exit_code {
        Some(0) => Ok(true),
        Some(1) => Ok(false),
        other => Err(OperationsError::GitCommandFailed {
            command: format!("git merge-tree --write-tree -- {integration_head} {task_head}"),
            status: other,
            stderr: out.stderr,
        }),
    }
}

fn build_integration_git_argv(
    strategy: IntegrationStrategy,
    run_id: &str,
    task_id: &str,
    task_head_commit: &str,
    review_receipt_id: &str,
) -> Vec<String> {
    match strategy {
        IntegrationStrategy::FastForwardOnly => vec![
            "merge".to_string(),
            "--ff-only".to_string(),
            "--".to_string(),
            task_head_commit.to_string(),
        ],
        IntegrationStrategy::MergeNoFf => {
            let msg = format!(
                "Integrate Threadlane task {run_id}/{task_id} ({task_head_commit}, receipt {review_receipt_id})"
            );
            vec![
                "-c".to_string(),
                "user.name=Threadlane Operator".to_string(),
                "-c".to_string(),
                "user.email=operator@threadlane.local".to_string(),
                "-c".to_string(),
                "commit.gpgsign=false".to_string(),
                "merge".to_string(),
                "--no-ff".to_string(),
                "--no-edit".to_string(),
                "-m".to_string(),
                msg,
                "--".to_string(),
                task_head_commit.to_string(),
            ]
        }
    }
}

fn build_push_git_argv(
    remote_name: &str,
    source_commit: &str,
    remote_target_branch: &str,
    expected_remote_target_head: Option<&str>,
) -> Vec<String> {
    let mut argv = vec!["push".to_string(), "--porcelain".to_string()];
    if let Some(expected_sha) = expected_remote_target_head {
        argv.push(format!(
            "--force-with-lease=refs/heads/{remote_target_branch}:{expected_sha}"
        ));
    }
    argv.push("--".to_string());
    argv.push(remote_name.to_string());
    argv.push(format!("{source_commit}:refs/heads/{remote_target_branch}"));
    argv
}

fn build_remote_delete_git_argv(
    remote_name: &str,
    remote_source_branch: &str,
    expected_remote_source_head: &str,
) -> Vec<String> {
    vec![
        "push".to_string(),
        "--porcelain".to_string(),
        format!(
            "--force-with-lease=refs/heads/{remote_source_branch}:{expected_remote_source_head}"
        ),
        "--delete".to_string(),
        "--".to_string(),
        remote_name.to_string(),
        format!("refs/heads/{remote_source_branch}"),
    ]
}

fn validate_remote_name(remote_name: &str) -> Result<(), OperationsError> {
    validate_simple_identifier(remote_name, "remote_name")?;
    Ok(())
}

fn validate_branch_ref_name(branch_name: &str, label: &str) -> Result<(), OperationsError> {
    if branch_name.starts_with("refs/") {
        return Err(OperationsError::InvalidInput(format!(
            "{label} `{branch_name}` must be a short branch name without `refs/heads/` prefix"
        )));
    }
    validate_commit_ref(branch_name)?;
    validate_repo_relative_path(branch_name, label)?;
    Ok(())
}

fn list_remote_heads(
    repo_path: &Path,
    remote_name: &str,
) -> Result<BTreeMap<String, String>, OperationsError> {
    let raw = run_sanitized_git(
        repo_path,
        &["ls-remote", "--heads", "--", remote_name],
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )?;
    let mut map = BTreeMap::new();
    for line in raw.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let Some((sha_part, ref_part)) = trimmed.split_once('\t') else {
            continue;
        };
        if let Some(branch_name) = ref_part.strip_prefix("refs/heads/") {
            if sha_part.len() == 40 && sha_part.chars().all(|c| c.is_ascii_hexdigit()) {
                map.insert(branch_name.to_string(), sha_part.to_ascii_lowercase());
            }
        }
    }
    Ok(map)
}

/// Proof binding a dependent task's verified [`DependencyInputPolicy`] and frozen instruction text
/// for job-scoped integration when the task executed from a sealed
/// [`DependencyInputSeal::execution_base_commit`] rather than the root `approved_base_commit`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JobTaskDependencyExecutionProof {
    /// Frozen dependency-input policy for this dependent task.
    pub policy: DependencyInputPolicy,
    /// Exact frozen instruction text whose SHA-256 digest matches `policy.instruction_digest` and the persisted seal.
    pub instruction: String,
}

/// Input binding an accepted task from an independently owned worker run for job-scoped
/// integration into a target `WorktreeKind::RunIntegration` worktree.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JobIntegrationTaskInput {
    /// Original worker run identifier (`run_...`) that owns `task_worktree_id`.
    pub original_run_id: String,
    /// Logical task identifier (`task_id`).
    pub task_id: String,
    /// 64-character lowercase hex SHA-256 digest of the frozen task specification.
    pub task_digest: String,
    /// Owned task worktree identifier (`WorktreeKind::Task`).
    pub task_worktree_id: String,
    /// Approved [`ReviewSubject`] for this task.
    pub subject: ReviewSubject,
    /// Persisted [`ReviewReceipt`] approving `subject`.
    pub review_receipt: ReviewReceipt,
    /// Expected current 40-hex `HEAD` commit SHA of the task worktree (must match `subject.task_head_commit`).
    pub expected_task_head_commit: String,
    /// Integration strategy for this task (`FastForwardOnly` or `MergeNoFf`).
    pub strategy: IntegrationStrategy,
    /// Optional explicit integration receipt ID for this task; generated deterministically if `None`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub receipt_id: Option<String>,
    /// Optional dependency-input execution proof when this task executed from a sealed
    /// [`DependencyInputSeal::execution_base_commit`] rather than the root `approved_base_commit`.
    /// Must be `None` for root tasks.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dependency_input: Option<JobTaskDependencyExecutionProof>,
}

/// Request to compute a read-only [`JobIntegrationProposal`] for integrating accepted tasks from
/// distinct worker runs into a single owned `WorktreeKind::RunIntegration` worktree.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProposeJobIntegrationRequest {
    /// Canonical path to the primary source Git repository.
    pub repository_path: PathBuf,
    /// Canonical path to the trusted private work-area root directory.
    pub work_area_root: PathBuf,
    /// Owned target integration worktree identifier (`WorktreeKind::RunIntegration`).
    pub integration_worktree_id: String,
    /// Approved 40-hex base commit SHA shared by the job, integration worktree, and tasks.
    pub approved_base_commit: String,
    /// Expected current 40-hex `HEAD` commit SHA of the primary source repository.
    pub expected_source_head_commit: String,
    /// Expected 40-hex `HEAD` commit SHA of the target `RunIntegration` worktree prior to integration.
    pub expected_integration_head_commit: String,
    /// Job and approval scope provenance binding.
    pub job_scope: JobIntegrationScopeProvenance,
    /// Non-empty ordered list of accepted tasks from worker runs to integrate.
    pub tasks: Vec<JobIntegrationTaskInput>,
}

/// Per-task read-only integration plan within a [`JobIntegrationProposal`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JobTaskIntegrationProposal {
    /// Zero-based step index within the job integration batch.
    pub step_index: usize,
    /// Total number of tasks in the job integration batch.
    pub total_tasks: usize,
    /// Job identifier (`job_...`).
    pub job_id: String,
    /// Monotonic job revision (`>= 1`).
    pub job_revision: u64,
    /// Approval identifier (`app_...`).
    pub approval_id: String,
    /// 64-hex SHA-256 approval subject digest.
    pub approval_subject_digest: String,
    /// Original worker run identifier (`run_...`) owning the task worktree.
    pub original_task_run_id: String,
    /// Target aggregation run identifier (`run_...`) owning the `RunIntegration` worktree.
    pub target_integration_run_id: String,
    /// Logical task identifier.
    pub task_id: String,
    /// 64-hex SHA-256 task specification digest.
    pub task_digest: String,
    /// Owned target integration worktree identifier (`WorktreeKind::RunIntegration`).
    pub integration_worktree_id: String,
    /// Canonical integration worktree checkout path.
    pub integration_checkout_path: PathBuf,
    /// Integration worktree branch name (`threadlane/run/...`).
    pub integration_branch_name: String,
    /// Owned task worktree identifier (`WorktreeKind::Task`).
    pub task_worktree_id: String,
    /// Canonical task worktree checkout path.
    pub task_checkout_path: PathBuf,
    /// Task worktree branch name (`threadlane/task/...`).
    pub task_branch_name: String,
    /// Canonical primary source repository path.
    pub repository_path: PathBuf,
    /// Current 40-hex `HEAD` commit SHA of the primary source repository.
    pub source_head_commit: String,
    /// Root approved 40-hex base commit SHA of the job and target `RunIntegration` worktree.
    pub approved_base_commit: String,
    /// Actual 40-hex execution base commit SHA of the task worktree (`approved_base_commit` for
    /// root tasks, or `DependencyInputSeal::execution_base_commit` for dependent tasks).
    pub base_commit: String,
    /// Optional 64-hex [`DependencyInputSeal::seal_digest`] when this task executed from a sealed
    /// non-root dependency input base commit.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dependency_seal_digest: Option<String>,
    /// Current 40-hex `HEAD` commit SHA of the integration worktree before the batch.
    pub initial_integration_head_commit: String,
    /// Current 40-hex `HEAD` commit SHA of the task worktree.
    pub task_head_commit: String,
    /// Current 40-hex `HEAD^{tree}` SHA of the task worktree.
    pub task_tree_sha: String,
    /// Bound 64-hex `ReviewSubject::diff_digest`.
    pub task_diff_digest: String,
    /// Bound 64-hex `ReviewSubject::subject_digest`.
    pub review_subject_digest: String,
    /// Bound `ReviewReceipt::receipt_id`.
    pub review_receipt_id: String,
    /// Bound 64-hex `ReviewReceipt::receipt_digest`.
    pub review_receipt_digest: String,
    /// Requested integration strategy.
    pub strategy: IntegrationStrategy,
    /// True if the integration branch can fast-forward to `task_head_commit` at this step.
    pub can_fast_forward: bool,
    /// True if `task_head_commit` is already an ancestor of `initial_integration_head_commit`.
    pub already_integrated: bool,
    /// True if read-only merge simulation predicts a conflict-free integration at this step.
    pub clean_merge_predicted: bool,
    /// Exact Git argument vector (excluding `git -C <integration_checkout>`) planned for this step.
    pub planned_git_argv: Vec<String>,
    /// Always `true`: proposal generation never mutates checkouts or refs.
    pub read_only: bool,
    /// Canonical 64-hex SHA-256 digest of this step proposal.
    pub proposal_digest: String,
}

#[derive(Serialize)]
struct JobTaskIntegrationProposalDigestPayload<'a> {
    step_index: usize,
    total_tasks: usize,
    job_id: &'a str,
    job_revision: u64,
    approval_id: &'a str,
    approval_subject_digest: &'a str,
    original_task_run_id: &'a str,
    target_integration_run_id: &'a str,
    task_id: &'a str,
    task_digest: &'a str,
    integration_worktree_id: &'a str,
    integration_checkout_path: &'a Path,
    integration_branch_name: &'a str,
    task_worktree_id: &'a str,
    task_checkout_path: &'a Path,
    task_branch_name: &'a str,
    repository_path: &'a Path,
    source_head_commit: &'a str,
    approved_base_commit: &'a str,
    base_commit: &'a str,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    dependency_seal_digest: Option<&'a str>,
    initial_integration_head_commit: &'a str,
    task_head_commit: &'a str,
    task_tree_sha: &'a str,
    task_diff_digest: &'a str,
    review_subject_digest: &'a str,
    review_receipt_id: &'a str,
    review_receipt_digest: &'a str,
    strategy: IntegrationStrategy,
    can_fast_forward: bool,
    already_integrated: bool,
    clean_merge_predicted: bool,
    planned_git_argv: &'a [String],
    read_only: bool,
}

impl JobTaskIntegrationProposal {
    /// Computes the canonical 64-character lowercase hexadecimal SHA-256 digest of this step proposal.
    pub fn compute_digest(&self) -> Result<String, OperationsError> {
        let payload = JobTaskIntegrationProposalDigestPayload {
            step_index: self.step_index,
            total_tasks: self.total_tasks,
            job_id: &self.job_id,
            job_revision: self.job_revision,
            approval_id: &self.approval_id,
            approval_subject_digest: &self.approval_subject_digest,
            original_task_run_id: &self.original_task_run_id,
            target_integration_run_id: &self.target_integration_run_id,
            task_id: &self.task_id,
            task_digest: &self.task_digest,
            integration_worktree_id: &self.integration_worktree_id,
            integration_checkout_path: &self.integration_checkout_path,
            integration_branch_name: &self.integration_branch_name,
            task_worktree_id: &self.task_worktree_id,
            task_checkout_path: &self.task_checkout_path,
            task_branch_name: &self.task_branch_name,
            repository_path: &self.repository_path,
            source_head_commit: &self.source_head_commit,
            approved_base_commit: &self.approved_base_commit,
            base_commit: &self.base_commit,
            dependency_seal_digest: self.dependency_seal_digest.as_deref(),
            initial_integration_head_commit: &self.initial_integration_head_commit,
            task_head_commit: &self.task_head_commit,
            task_tree_sha: &self.task_tree_sha,
            task_diff_digest: &self.task_diff_digest,
            review_subject_digest: &self.review_subject_digest,
            review_receipt_id: &self.review_receipt_id,
            review_receipt_digest: &self.review_receipt_digest,
            strategy: self.strategy,
            can_fast_forward: self.can_fast_forward,
            already_integrated: self.already_integrated,
            clean_merge_predicted: self.clean_merge_predicted,
            planned_git_argv: &self.planned_git_argv,
            read_only: self.read_only,
        };
        let encoded = serde_json::to_vec(&payload)?;
        Ok(sha256_hex(&encoded))
    }
}

/// Read-only multi-worker job integration plan binding job/approval provenance and all per-task proposals.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JobIntegrationProposal {
    /// Job and approval scope provenance.
    pub job_scope: JobIntegrationScopeProvenance,
    /// Owned target integration worktree identifier (`WorktreeKind::RunIntegration`).
    pub integration_worktree_id: String,
    /// Canonical integration worktree checkout path.
    pub integration_checkout_path: PathBuf,
    /// Integration worktree branch name (`threadlane/run/...`).
    pub integration_branch_name: String,
    /// Canonical primary source repository path.
    pub repository_path: PathBuf,
    /// Canonical private work-area root path.
    pub work_area_root: PathBuf,
    /// Approved 40-hex base commit SHA.
    pub approved_base_commit: String,
    /// Current 40-hex `HEAD` commit SHA of the primary source repository.
    pub source_head_commit: String,
    /// Current 40-hex `HEAD` commit SHA of the integration worktree before integration.
    pub initial_integration_head_commit: String,
    /// Ordered per-task integration proposals.
    pub task_proposals: Vec<JobTaskIntegrationProposal>,
    /// True if all steps in `task_proposals` predict a clean conflict-free integration.
    pub all_clean_merges_predicted: bool,
    /// Always `true`: proposal generation never mutates checkouts or refs.
    pub read_only: bool,
    /// Canonical 64-hex SHA-256 digest of this job integration proposal.
    pub proposal_digest: String,
}

#[derive(Serialize)]
struct JobIntegrationProposalDigestPayload<'a> {
    job_scope: &'a JobIntegrationScopeProvenance,
    integration_worktree_id: &'a str,
    integration_checkout_path: &'a Path,
    integration_branch_name: &'a str,
    repository_path: &'a Path,
    work_area_root: &'a Path,
    approved_base_commit: &'a str,
    source_head_commit: &'a str,
    initial_integration_head_commit: &'a str,
    task_proposals: &'a [JobTaskIntegrationProposal],
    all_clean_merges_predicted: bool,
    read_only: bool,
}

impl JobIntegrationProposal {
    /// Computes the canonical 64-character lowercase hexadecimal SHA-256 digest of this proposal.
    pub fn compute_digest(&self) -> Result<String, OperationsError> {
        let payload = JobIntegrationProposalDigestPayload {
            job_scope: &self.job_scope,
            integration_worktree_id: &self.integration_worktree_id,
            integration_checkout_path: &self.integration_checkout_path,
            integration_branch_name: &self.integration_branch_name,
            repository_path: &self.repository_path,
            work_area_root: &self.work_area_root,
            approved_base_commit: &self.approved_base_commit,
            source_head_commit: &self.source_head_commit,
            initial_integration_head_commit: &self.initial_integration_head_commit,
            task_proposals: &self.task_proposals,
            all_clean_merges_predicted: self.all_clean_merges_predicted,
            read_only: self.read_only,
        };
        let encoded = serde_json::to_vec(&payload)?;
        Ok(sha256_hex(&encoded))
    }
}

/// Lifecycle state of a persisted [`JobIntegrationIntent`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobIntegrationIntentState {
    /// Durable intent persisted before mutating the target `RunIntegration` worktree.
    InProgress,
    /// All planned task integrations completed and their [`IntegrationReceipt`]s were persisted.
    Completed,
    /// Execution was interrupted or left in an uncertain state; blocks blind replay across all command keys.
    OutcomeUnknown,
}

/// Planned task entry recorded inside a durable [`JobIntegrationIntent`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JobIntegrationPlannedTask {
    /// Zero-based step index within the batch.
    pub step_index: usize,
    /// Original worker run identifier (`run_...`).
    pub original_run_id: String,
    /// Logical task identifier (`task_id`).
    pub task_id: String,
    /// 64-hex SHA-256 digest of the frozen task specification.
    pub task_digest: String,
    /// Owned task worktree identifier (`WorktreeKind::Task`).
    pub task_worktree_id: String,
    /// Actual 40-hex execution base commit SHA of this task.
    pub task_base_commit: String,
    /// Optional 64-hex [`DependencyInputSeal::seal_digest`] when this task executed from a sealed
    /// non-root dependency input base commit.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dependency_seal_digest: Option<String>,
    /// Accepted 40-hex task `HEAD` commit SHA.
    pub accepted_task_head_commit: String,
    /// Accepted 40-hex task `HEAD^{tree}` SHA.
    pub accepted_task_tree_sha: String,
    /// Accepted 64-hex task diff digest.
    pub accepted_task_diff_digest: String,
    /// Bound 64-hex `ReviewSubject::subject_digest`.
    pub review_subject_digest: String,
    /// Bound `ReviewReceipt::receipt_id`.
    pub review_receipt_id: String,
    /// Bound 64-hex `ReviewReceipt::receipt_digest`.
    pub review_receipt_digest: String,
    /// Integration strategy for this step.
    pub strategy: IntegrationStrategy,
    /// Planned [`IntegrationReceipt::receipt_id`] for this step.
    pub planned_receipt_id: String,
    /// Bound 64-hex [`JobTaskIntegrationProposal::proposal_digest`] for this step.
    pub proposal_digest: String,
}

/// Durable intent record persisted at
/// `<work_area_root>/manifests/job-integrations/job-integration-intent-<integration_worktree_id>.json`
/// before mutating an owned `WorktreeKind::RunIntegration` worktree.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JobIntegrationIntent {
    /// Schema version (`JOB_INTEGRATION_INTENT_SCHEMA_VERSION`).
    pub schema_version: u32,
    /// Unique intent identifier (`jint_...`).
    pub intent_id: String,
    /// Idempotency command key bound to this intent.
    pub command_key: String,
    /// Job and approval scope provenance.
    pub job_scope: JobIntegrationScopeProvenance,
    /// Canonical primary source repository path.
    pub repository_path: PathBuf,
    /// Canonical private work-area root path.
    pub work_area_root: PathBuf,
    /// Owned target integration worktree identifier (`WorktreeKind::RunIntegration`).
    pub integration_worktree_id: String,
    /// Approved 40-hex base commit SHA.
    pub approved_base_commit: String,
    /// Primary source checkout 40-hex `HEAD` commit SHA verified prior to integration.
    pub expected_source_head_commit: String,
    /// Integration worktree 40-hex `HEAD` commit SHA prior to executing the batch.
    pub initial_integration_head_commit: String,
    /// Ordered list of planned task integrations.
    pub tasks: Vec<JobIntegrationPlannedTask>,
    /// Current lifecycle state (`in_progress`, `completed`, `outcome_unknown`).
    pub state: JobIntegrationIntentState,
    /// Final 40-hex `HEAD` commit SHA on the integration worktree when `state == Completed`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub final_integrated_target_commit: Option<String>,
    /// Final 40-hex `HEAD^{tree}` SHA on the integration worktree when `state == Completed`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub final_integrated_tree_sha: Option<String>,
    /// Sanitized diagnostic recorded when `state == OutcomeUnknown`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uncertainty_reason: Option<String>,
    /// Operator identifier.
    pub operator_id: String,
    /// Timestamp (ms since Unix epoch) when the intent was first recorded.
    pub recorded_at_ms: i64,
    /// Timestamp (ms since Unix epoch) when the intent was last updated.
    pub updated_at_ms: i64,
    /// Canonical 64-hex SHA-256 digest of this intent record.
    pub intent_digest: String,
}

#[derive(Serialize)]
struct JobIntegrationIntentDigestPayload<'a> {
    schema_version: u32,
    intent_id: &'a str,
    command_key: &'a str,
    job_scope: &'a JobIntegrationScopeProvenance,
    repository_path: &'a Path,
    work_area_root: &'a Path,
    integration_worktree_id: &'a str,
    approved_base_commit: &'a str,
    expected_source_head_commit: &'a str,
    initial_integration_head_commit: &'a str,
    tasks: &'a [JobIntegrationPlannedTask],
    state: JobIntegrationIntentState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    final_integrated_target_commit: Option<&'a str>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    final_integrated_tree_sha: Option<&'a str>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    uncertainty_reason: Option<&'a str>,
    operator_id: &'a str,
    recorded_at_ms: i64,
    updated_at_ms: i64,
}

impl JobIntegrationIntent {
    /// Computes the canonical 64-character lowercase hexadecimal SHA-256 digest of this intent.
    pub fn compute_digest(&self) -> Result<String, OperationsError> {
        let payload = JobIntegrationIntentDigestPayload {
            schema_version: self.schema_version,
            intent_id: &self.intent_id,
            command_key: &self.command_key,
            job_scope: &self.job_scope,
            repository_path: &self.repository_path,
            work_area_root: &self.work_area_root,
            integration_worktree_id: &self.integration_worktree_id,
            approved_base_commit: &self.approved_base_commit,
            expected_source_head_commit: &self.expected_source_head_commit,
            initial_integration_head_commit: &self.initial_integration_head_commit,
            tasks: &self.tasks,
            state: self.state,
            final_integrated_target_commit: self.final_integrated_target_commit.as_deref(),
            final_integrated_tree_sha: self.final_integrated_tree_sha.as_deref(),
            uncertainty_reason: self.uncertainty_reason.as_deref(),
            operator_id: &self.operator_id,
            recorded_at_ms: self.recorded_at_ms,
            updated_at_ms: self.updated_at_ms,
        };
        let encoded = serde_json::to_vec(&payload)?;
        Ok(sha256_hex(&encoded))
    }

    /// Validates all structural fields and verifies that `self.intent_digest` matches [`Self::compute_digest`].
    pub fn verify_integrity(&self) -> Result<(), OperationsError> {
        if self.schema_version != JOB_INTEGRATION_INTENT_SCHEMA_VERSION {
            return Err(OperationsError::ReceiptInvalidated(format!(
                "unsupported job integration intent schema_version {} (expected {JOB_INTEGRATION_INTENT_SCHEMA_VERSION})",
                self.schema_version
            )));
        }
        validate_simple_identifier(&self.intent_id, "intent.intent_id")?;
        validate_simple_identifier(&self.command_key, "intent.command_key")?;
        self.job_scope.validate()?;
        validate_simple_identifier(
            &self.integration_worktree_id,
            "intent.integration_worktree_id",
        )?;
        let _ = validate_exact_40_hex_sha(&self.approved_base_commit, "approved_base_commit")?;
        let _ = validate_exact_40_hex_sha(
            &self.expected_source_head_commit,
            "expected_source_head_commit",
        )?;
        let _ = validate_exact_40_hex_sha(
            &self.initial_integration_head_commit,
            "initial_integration_head_commit",
        )?;
        validate_simple_identifier(&self.operator_id, "intent.operator_id")?;
        let _ = validate_sha256_hex_digest(&self.intent_digest, "intent.intent_digest")?;

        if self.tasks.is_empty() || self.tasks.len() > MAX_DEPENDENCY_CLOSURE_TASKS {
            return Err(OperationsError::ReceiptInvalidated(format!(
                "intent tasks count ({}) must be between 1 and {MAX_DEPENDENCY_CLOSURE_TASKS}",
                self.tasks.len()
            )));
        }
        for (idx, t) in self.tasks.iter().enumerate() {
            if t.step_index != idx {
                return Err(OperationsError::ReceiptInvalidated(
                    "intent task step_index is not sequential".to_string(),
                ));
            }
            validate_simple_identifier(&t.original_run_id, "intent.task.original_run_id")?;
            validate_simple_identifier(&t.task_id, "intent.task.task_id")?;
            let _ = validate_sha256_hex_digest(&t.task_digest, "intent.task.task_digest")?;
            validate_simple_identifier(&t.task_worktree_id, "intent.task.task_worktree_id")?;
            let _ = validate_exact_40_hex_sha(&t.task_base_commit, "intent.task.task_base_commit")?;
            if let Some(ref seal_digest) = t.dependency_seal_digest {
                let _ =
                    validate_sha256_hex_digest(seal_digest, "intent.task.dependency_seal_digest")?;
                if t.task_base_commit == self.approved_base_commit {
                    return Err(OperationsError::ReceiptInvalidated(format!(
                        "intent task `{}` has root task_base_commit but carries dependency_seal_digest",
                        t.task_id
                    )));
                }
            } else if t.task_base_commit != self.approved_base_commit {
                return Err(OperationsError::ReceiptInvalidated(format!(
                    "intent task `{}` has non-root task_base_commit `{}` without dependency_seal_digest",
                    t.task_id, t.task_base_commit
                )));
            }
            let _ = validate_exact_40_hex_sha(
                &t.accepted_task_head_commit,
                "intent.task.accepted_task_head_commit",
            )?;
            let _ = validate_exact_40_hex_sha(
                &t.accepted_task_tree_sha,
                "intent.task.accepted_task_tree_sha",
            )?;
            let _ = validate_sha256_hex_digest(
                &t.accepted_task_diff_digest,
                "intent.task.accepted_task_diff_digest",
            )?;
            let _ = validate_sha256_hex_digest(
                &t.review_subject_digest,
                "intent.task.review_subject_digest",
            )?;
            validate_simple_identifier(&t.review_receipt_id, "intent.task.review_receipt_id")?;
            let _ = validate_sha256_hex_digest(
                &t.review_receipt_digest,
                "intent.task.review_receipt_digest",
            )?;
            validate_simple_identifier(&t.planned_receipt_id, "intent.task.planned_receipt_id")?;
            let _ = validate_sha256_hex_digest(&t.proposal_digest, "intent.task.proposal_digest")?;
        }

        match self.state {
            JobIntegrationIntentState::Completed => {
                let Some(ref target_commit) = self.final_integrated_target_commit else {
                    return Err(OperationsError::ReceiptInvalidated(
                        "completed job integration intent is missing final_integrated_target_commit"
                            .to_string(),
                    ));
                };
                let Some(ref target_tree) = self.final_integrated_tree_sha else {
                    return Err(OperationsError::ReceiptInvalidated(
                        "completed job integration intent is missing final_integrated_tree_sha"
                            .to_string(),
                    ));
                };
                let _ = validate_exact_40_hex_sha(target_commit, "final_integrated_target_commit")?;
                let _ = validate_exact_40_hex_sha(target_tree, "final_integrated_tree_sha")?;
            }
            JobIntegrationIntentState::InProgress | JobIntegrationIntentState::OutcomeUnknown => {
                if let Some(ref target_commit) = self.final_integrated_target_commit {
                    let _ =
                        validate_exact_40_hex_sha(target_commit, "final_integrated_target_commit")?;
                }
                if let Some(ref target_tree) = self.final_integrated_tree_sha {
                    let _ = validate_exact_40_hex_sha(target_tree, "final_integrated_tree_sha")?;
                }
            }
        }

        let computed = self.compute_digest()?;
        if computed != self.intent_digest {
            return Err(OperationsError::ReceiptInvalidated(format!(
                "job integration intent digest mismatch: expected `{}`, computed `{computed}`",
                self.intent_digest
            )));
        }
        Ok(())
    }
}

/// Explicit operator/controller request to aggregate and integrate an ordered list of accepted
/// task worktrees from distinct worker runs into a single owned `WorktreeKind::RunIntegration`
/// target worktree.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecuteJobIntegrationRequest {
    /// Canonical path to the primary source Git repository.
    pub repository_path: PathBuf,
    /// Canonical path to the trusted private work-area root directory.
    pub work_area_root: PathBuf,
    /// Owned target integration worktree identifier (`WorktreeKind::RunIntegration`).
    pub integration_worktree_id: String,
    /// Approved 40-hex base commit SHA shared by the job, integration worktree, and tasks.
    pub approved_base_commit: String,
    /// Expected current 40-hex `HEAD` commit SHA of the primary source repository (preserved untouched).
    pub expected_source_head_commit: String,
    /// Expected 40-hex `HEAD` commit SHA of the target `RunIntegration` worktree prior to this job integration batch.
    pub expected_integration_head_commit: String,
    /// Job and approval scope provenance binding.
    pub job_scope: JobIntegrationScopeProvenance,
    /// Non-empty ordered list of accepted tasks from worker runs to integrate.
    pub tasks: Vec<JobIntegrationTaskInput>,
    /// Idempotency command key for this job integration operation.
    pub command_key: String,
    /// Optional explicit intent identifier (`jint_...`); derived deterministically if `None`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub intent_id: Option<String>,
    /// Operator or controller identifier performing the integration.
    pub operator_id: String,
    /// Wall-clock timestamp (milliseconds since Unix epoch) recorded on the receipts.
    pub integrated_at_ms: i64,
}

/// Result of executing or recovering a job-scoped multi-worker integration into an owned
/// `WorktreeKind::RunIntegration` target worktree.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JobIntegrationReport {
    /// Job and approval scope provenance.
    pub job_scope: JobIntegrationScopeProvenance,
    /// Owned target integration worktree identifier (`WorktreeKind::RunIntegration`).
    pub integration_worktree_id: String,
    /// Canonical integration worktree checkout path.
    pub integration_checkout_path: PathBuf,
    /// Integration worktree branch name (`threadlane/run/...`).
    pub integration_branch_name: String,
    /// Canonical primary source repository path.
    pub repository_path: PathBuf,
    /// Canonical private work-area root path.
    pub work_area_root: PathBuf,
    /// Approved 40-hex base commit SHA.
    pub approved_base_commit: String,
    /// Integration worktree 40-hex `HEAD` commit SHA prior to this job integration batch.
    pub initial_integration_head_commit: String,
    /// Resulting 40-hex `HEAD` commit SHA on the integration worktree after all tasks were integrated.
    pub final_integrated_target_commit: String,
    /// Resulting 40-hex `HEAD^{tree}` SHA on the integration worktree after all tasks were integrated.
    pub final_integrated_tree_sha: String,
    /// Primary source checkout 40-hex `HEAD` commit SHA verified unchanged before and after integration.
    pub source_head_commit: String,
    /// Completed durable [`JobIntegrationIntent`] record.
    pub intent: JobIntegrationIntent,
    /// Persisted [`IntegrationReceipt`] records (one per integrated task, in batch order).
    pub receipts: Vec<IntegrationReceipt>,
    /// `true` if existing verified receipts were reused/recovered without creating new Git commits.
    pub reused_existing_receipts: bool,
}

struct VerifiedJobTaskContext {
    step_index: usize,
    original_run_id: String,
    task_id: String,
    task_digest: String,
    task_manifest: WorktreeManifest,
    task_checkout: PathBuf,
    subject: ReviewSubject,
    review_receipt: ReviewReceipt,
    expected_task_head_commit: String,
    dependency_seal_digest: Option<String>,
    dependency_snapshot_ref: Option<String>,
    strategy: IntegrationStrategy,
    planned_receipt_id: String,
    step_proposal: JobTaskIntegrationProposal,
}

struct VerifiedJobIntegrationPreFlight {
    canonical_repo: PathBuf,
    canonical_work_area: PathBuf,
    approved_base_commit: String,
    expected_source_head: String,
    expected_int_head: String,
    pre_source_status: Vec<u8>,
    integration_manifest: WorktreeManifest,
    integration_checkout: PathBuf,
    verified_tasks: Vec<VerifiedJobTaskContext>,
    proposal: JobIntegrationProposal,
}

/// Computes a strictly read-only [`JobIntegrationProposal`] for integrating an ordered list of
/// accepted tasks from independent worker runs into a single owned `WorktreeKind::RunIntegration`
/// target worktree.
///
/// Performs no checkout, branch, lock-file, or receipt mutations.
pub fn propose_job_integration(
    request: &ProposeJobIntegrationRequest,
) -> Result<JobIntegrationProposal, OperationsError> {
    let preflight = verify_job_integration_preflight(
        &request.repository_path,
        &request.work_area_root,
        &request.integration_worktree_id,
        &request.approved_base_commit,
        &request.expected_source_head_commit,
        &request.expected_integration_head_commit,
        &request.job_scope,
        &request.tasks,
        "readonly_proposer",
        true,
    )?;

    let post_source_head = resolve_exact_commit_sha(
        &preflight.canonical_repo,
        "HEAD",
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )?;
    let post_source_status = capture_repo_porcelain_status(&preflight.canonical_repo)?;
    if post_source_head != preflight.expected_source_head
        || post_source_status != preflight.pre_source_status
    {
        return Err(OperationsError::SecurityViolation(
            "primary source repository HEAD or working tree changed during read-only job integration proposal"
                .to_string(),
        ));
    }

    Ok(preflight.proposal)
}

/// Inspects any persisted [`JobIntegrationIntent`] for `integration_worktree_id` in read-only mode
/// without creating directories or lock files.
pub fn inspect_job_integration_intent_readonly(
    work_area_root: &Path,
    integration_worktree_id: &str,
) -> Result<Option<JobIntegrationIntent>, OperationsError> {
    ensure_platform_supported()?;
    validate_simple_identifier(integration_worktree_id, "integration_worktree_id")?;
    if !work_area_root.is_absolute() {
        return Err(OperationsError::InvalidInput(
            "work_area_root must be an absolute path".to_string(),
        ));
    }
    let canonical_work_area = validate_existing_canonical_dir(work_area_root, "work_area_root")?;
    if canonical_work_area != work_area_root {
        return Err(OperationsError::SecurityViolation(
            "work_area_root canonicalization mismatch".to_string(),
        ));
    }
    load_job_integration_intent_unlocked(&canonical_work_area, integration_worktree_id)
}

/// Validates all inputs and records a durable [`JobIntegrationIntent`] in
/// [`JobIntegrationIntentState::InProgress`] before any Git merge effect occurs.
///
/// If an unresolved (`InProgress` or `OutcomeUnknown`) intent already exists on disk without
/// complete verified receipts, refuses with [`OperationsError::UncertainWorkerOutcome`].
pub fn record_job_integration_intent_before_effect(
    request: &ExecuteJobIntegrationRequest,
) -> Result<JobIntegrationIntent, OperationsError> {
    ensure_platform_supported()?;
    validate_simple_identifier(&request.command_key, "command_key")?;
    validate_simple_identifier(&request.operator_id, "operator_id")?;
    if let Some(ref explicit_intent_id) = request.intent_id {
        validate_simple_identifier(explicit_intent_id, "intent_id")?;
    }

    let preflight = verify_job_integration_preflight(
        &request.repository_path,
        &request.work_area_root,
        &request.integration_worktree_id,
        &request.approved_base_commit,
        &request.expected_source_head_commit,
        &request.expected_integration_head_commit,
        &request.job_scope,
        &request.tasks,
        &request.command_key,
        true,
    )?;

    let job_int_dir = job_integrations_dir(&preflight.canonical_work_area);
    ensure_private_dir(&job_int_dir, "manifests/job-integrations")?;
    let lock_path = job_integration_lock_path(
        &preflight.canonical_work_area,
        &request.integration_worktree_id,
    );
    let lock_file = open_private_lock_file(&lock_path, "job integration lock")?;
    lock_file.lock_exclusive().map_err(OperationsError::Io)?;

    let result = (|| -> Result<JobIntegrationIntent, OperationsError> {
        if let Some(existing) = load_job_integration_intent_unlocked(
            &preflight.canonical_work_area,
            &request.integration_worktree_id,
        )? {
            match existing.state {
                JobIntegrationIntentState::OutcomeUnknown => {
                    return Err(OperationsError::UncertainWorkerOutcome(format!(
                        "job integration intent `{}` for worktree `{}` is in OutcomeUnknown state (`{}`); refusing blind replay",
                        existing.intent_id,
                        existing.integration_worktree_id,
                        existing
                            .uncertainty_reason
                            .as_deref()
                            .unwrap_or("unspecified uncertainty")
                    )));
                }
                JobIntegrationIntentState::InProgress => {
                    if existing.command_key == request.command_key
                        && intent_matches_request_payload(
                            &existing,
                            request,
                            &preflight.canonical_repo,
                            &preflight.canonical_work_area,
                        )
                    {
                        return Ok(existing);
                    }
                    return Err(OperationsError::UncertainWorkerOutcome(format!(
                        "job integration intent `{}` for worktree `{}` is already InProgress under command_key `{}`; idempotency keys cannot bypass unresolved intent",
                        existing.intent_id, existing.integration_worktree_id, existing.command_key
                    )));
                }
                JobIntegrationIntentState::Completed => {
                    let same_payload = intent_matches_request_payload(
                        &existing,
                        request,
                        &preflight.canonical_repo,
                        &preflight.canonical_work_area,
                    );
                    if !same_payload {
                        if existing.command_key == request.command_key {
                            return Err(OperationsError::ReceiptInvalidated(format!(
                                "command_key `{}` was already used for a different job integration payload on worktree `{}`",
                                request.command_key, request.integration_worktree_id
                            )));
                        }
                        return Err(OperationsError::ReceiptInvalidated(format!(
                            "job integration intent `{}` for worktree `{}` is already Completed under command_key `{}`; single-final-batch job integration forbids replacing a completed batch with a different payload",
                            existing.intent_id, existing.integration_worktree_id, existing.command_key
                        )));
                    }
                    return Ok(existing);
                }
            }
        }

        let actual_int_head = resolve_exact_commit_sha(
            &preflight.integration_checkout,
            "HEAD",
            DEFAULT_GIT_COMMAND_TIMEOUT_MS,
        )?;
        if actual_int_head != preflight.expected_int_head {
            return Err(OperationsError::HeadMoved {
                target: format!(
                    "integration worktree `{}`",
                    preflight.integration_manifest.worktree_id
                ),
                expected: preflight.expected_int_head.clone(),
                actual: actual_int_head,
            });
        }

        verify_preflight_merge_chain(&preflight, &actual_int_head)?;

        let intent = build_in_progress_intent(request, &preflight, &actual_int_head)?;
        write_job_integration_intent_unlocked(&preflight.canonical_work_area, &intent)?;
        Ok(intent)
    })();

    let _ = lock_file.unlock();
    result
}

/// Transitions an existing [`JobIntegrationIntent`] for `integration_worktree_id` to
/// [`JobIntegrationIntentState::OutcomeUnknown`], blocking subsequent automatic replay across all
/// command keys.
pub fn mark_job_integration_intent_unknown(
    work_area_root: &Path,
    integration_worktree_id: &str,
    command_key: &str,
    reason: &str,
    updated_at_ms: i64,
) -> Result<JobIntegrationIntent, OperationsError> {
    ensure_platform_supported()?;
    validate_simple_identifier(integration_worktree_id, "integration_worktree_id")?;
    validate_simple_identifier(command_key, "command_key")?;
    let clean_reason = sanitize_and_redact(reason, MAX_REVIEW_NOTE_BYTES);
    if clean_reason.trim().is_empty() {
        return Err(OperationsError::InvalidInput(
            "uncertainty reason must not be empty".to_string(),
        ));
    }
    let canonical_work_area = validate_existing_canonical_dir(work_area_root, "work_area_root")?;
    let job_int_dir = job_integrations_dir(&canonical_work_area);
    ensure_private_dir(&job_int_dir, "manifests/job-integrations")?;
    let lock_path = job_integration_lock_path(&canonical_work_area, integration_worktree_id);
    let lock_file = open_private_lock_file(&lock_path, "job integration lock")?;
    lock_file.lock_exclusive().map_err(OperationsError::Io)?;

    let result = (|| -> Result<JobIntegrationIntent, OperationsError> {
        let Some(mut intent) =
            load_job_integration_intent_unlocked(&canonical_work_area, integration_worktree_id)?
        else {
            return Err(OperationsError::ReceiptInvalidated(format!(
                "no job integration intent found for worktree `{integration_worktree_id}`"
            )));
        };
        if intent.command_key != command_key {
            return Err(OperationsError::ReceiptInvalidated(format!(
                "job integration intent command_key `{}` does not match `{command_key}`",
                intent.command_key
            )));
        }
        if intent.state == JobIntegrationIntentState::Completed {
            return Err(OperationsError::ReceiptInvalidated(format!(
                "completed job integration intent `{}` cannot be marked OutcomeUnknown",
                intent.intent_id
            )));
        }
        intent.state = JobIntegrationIntentState::OutcomeUnknown;
        intent.uncertainty_reason = Some(clean_reason);
        intent.updated_at_ms = updated_at_ms;
        intent.intent_digest = intent.compute_digest()?;
        intent.verify_integrity()?;
        write_job_integration_intent_unlocked(&canonical_work_area, &intent)?;
        Ok(intent)
    })();

    let _ = lock_file.unlock();
    result
}

/// Explicit operator/controller action to aggregate and integrate an ordered list of accepted
/// task worktrees from distinct worker runs into a single owned `WorktreeKind::RunIntegration`
/// target worktree.
///
/// ## Guarantees
/// - Verifies every input, worktree ownership manifest, live Git branch/cleanliness/HEAD, persisted
///   [`ReviewReceipt`], and job write-scope ceiling before any effect.
/// - Records a durable [`JobIntegrationIntent`] before mutating the `RunIntegration` worktree.
/// - Reuses/recovers existing verified [`IntegrationReceipt`]s without creating new commits when
///   re-invoked for an already completed batch, and refuses blind replay on [`JobIntegrationIntentState::OutcomeUnknown`]
///   or incomplete [`JobIntegrationIntentState::InProgress`] intents across all command keys.
/// - Mutates only the owned `WorktreeKind::RunIntegration` checkout; never merges into `main`,
///   pushes, deletes branches/worktrees, or modifies the primary source checkout.
pub fn execute_job_integration(
    request: &ExecuteJobIntegrationRequest,
) -> Result<JobIntegrationReport, OperationsError> {
    ensure_platform_supported()?;
    validate_simple_identifier(&request.command_key, "command_key")?;
    validate_simple_identifier(&request.operator_id, "operator_id")?;
    if let Some(ref explicit_intent_id) = request.intent_id {
        validate_simple_identifier(explicit_intent_id, "intent_id")?;
    }

    let preflight = verify_job_integration_preflight(
        &request.repository_path,
        &request.work_area_root,
        &request.integration_worktree_id,
        &request.approved_base_commit,
        &request.expected_source_head_commit,
        &request.expected_integration_head_commit,
        &request.job_scope,
        &request.tasks,
        &request.command_key,
        false,
    )?;

    let job_int_dir = job_integrations_dir(&preflight.canonical_work_area);
    ensure_private_dir(&job_int_dir, "manifests/job-integrations")?;
    let lock_path = job_integration_lock_path(
        &preflight.canonical_work_area,
        &request.integration_worktree_id,
    );
    let lock_file = open_private_lock_file(&lock_path, "job integration lock")?;
    lock_file.lock_exclusive().map_err(OperationsError::Io)?;

    let result = (|| -> Result<JobIntegrationReport, OperationsError> {
        // 1. Check existing durable intent for recovery, idempotent reuse, or uncertainty fencing.
        if let Some(mut existing_intent) = load_job_integration_intent_unlocked(
            &preflight.canonical_work_area,
            &request.integration_worktree_id,
        )? {
            match existing_intent.state {
                JobIntegrationIntentState::OutcomeUnknown => {
                    return Err(OperationsError::UncertainWorkerOutcome(format!(
                        "job integration intent `{}` for worktree `{}` is in OutcomeUnknown state (`{}`); refusing blind replay",
                        existing_intent.intent_id,
                        existing_intent.integration_worktree_id,
                        existing_intent
                            .uncertainty_reason
                            .as_deref()
                            .unwrap_or("unspecified uncertainty")
                    )));
                }
                JobIntegrationIntentState::InProgress => {
                    // An InProgress intent exists on disk. Never blindly re-execute merges!
                    // Only recover if all planned IntegrationReceipts were already persisted on
                    // disk and match the live RunIntegration HEAD and this request.
                    if intent_matches_request_payload(
                        &existing_intent,
                        request,
                        &preflight.canonical_repo,
                        &preflight.canonical_work_area,
                    ) {
                        if let Ok(Some(recovered_report)) =
                            try_recover_completed_receipts_for_intent(
                                &mut existing_intent,
                                &preflight,
                                request.integrated_at_ms,
                            )
                        {
                            return Ok(recovered_report);
                        }
                    }

                    let reason = format!(
                        "interrupted InProgress job integration intent `{}` (command_key `{}`) lacks complete verified receipts on disk; refusing blind replay",
                        existing_intent.intent_id, existing_intent.command_key
                    );
                    existing_intent.state = JobIntegrationIntentState::OutcomeUnknown;
                    existing_intent.uncertainty_reason = Some(reason.clone());
                    existing_intent.updated_at_ms = request.integrated_at_ms;
                    existing_intent.intent_digest = existing_intent.compute_digest()?;
                    let _ = write_job_integration_intent_unlocked(
                        &preflight.canonical_work_area,
                        &existing_intent,
                    );
                    return Err(OperationsError::UncertainWorkerOutcome(reason));
                }
                JobIntegrationIntentState::Completed => {
                    let same_payload = intent_matches_request_payload(
                        &existing_intent,
                        request,
                        &preflight.canonical_repo,
                        &preflight.canonical_work_area,
                    );
                    if !same_payload {
                        if existing_intent.command_key == request.command_key {
                            return Err(OperationsError::ReceiptInvalidated(format!(
                                "command_key `{}` was already used for a different completed job integration payload on worktree `{}`",
                                request.command_key, request.integration_worktree_id
                            )));
                        }
                        return Err(OperationsError::ReceiptInvalidated(format!(
                            "job integration intent `{}` for worktree `{}` is already Completed under command_key `{}`; single-final-batch job integration forbids replacing a completed batch with a different payload",
                            existing_intent.intent_id,
                            existing_intent.integration_worktree_id,
                            existing_intent.command_key
                        )));
                    }
                    return reuse_completed_intent_receipts(&existing_intent, &preflight);
                }
            }
        }

        // 2. Acquire exclusive active-job locks on the target RunIntegration worktree and all task worktrees.
        let int_guard = begin_worktree_job(
            &preflight.integration_manifest.work_area_root,
            &preflight.integration_manifest.worktree_id,
        )?;
        let mut sorted_task_worktree_ids: Vec<String> = preflight
            .verified_tasks
            .iter()
            .map(|t| t.task_manifest.worktree_id.clone())
            .collect();
        sorted_task_worktree_ids.sort();
        let mut task_guards = Vec::with_capacity(sorted_task_worktree_ids.len());
        for wt_id in &sorted_task_worktree_ids {
            match begin_worktree_job(&preflight.canonical_work_area, wt_id) {
                Ok(g) => task_guards.push(g),
                Err(err) => {
                    for g in task_guards {
                        let _ = g.finish();
                    }
                    let _ = int_guard.finish();
                    return Err(err.into());
                }
            }
        }

        let inner_res = (|| -> Result<JobIntegrationReport, OperationsError> {
            // Re-verify live HEADs and clean checkouts under the exclusive worktree locks.
            let actual_source_head = resolve_exact_commit_sha(
                &preflight.canonical_repo,
                "HEAD",
                DEFAULT_GIT_COMMAND_TIMEOUT_MS,
            )?;
            if actual_source_head != preflight.expected_source_head {
                return Err(OperationsError::HeadMoved {
                    target: "primary source repository".to_string(),
                    expected: preflight.expected_source_head.clone(),
                    actual: actual_source_head,
                });
            }

            if !is_worktree_clean(
                &preflight.integration_checkout,
                DEFAULT_GIT_COMMAND_TIMEOUT_MS,
            )? {
                return Err(OperationsError::DirtyWorktree {
                    worktree_id: preflight.integration_manifest.worktree_id.clone(),
                    detail: "integration worktree has uncommitted, untracked, or ignored files"
                        .to_string(),
                });
            }
            let actual_int_head = resolve_exact_commit_sha(
                &preflight.integration_checkout,
                "HEAD",
                DEFAULT_GIT_COMMAND_TIMEOUT_MS,
            )?;
            if actual_int_head != preflight.expected_int_head {
                return Err(OperationsError::HeadMoved {
                    target: format!(
                        "integration worktree `{}`",
                        preflight.integration_manifest.worktree_id
                    ),
                    expected: preflight.expected_int_head.clone(),
                    actual: actual_int_head,
                });
            }

            for vt in &preflight.verified_tasks {
                if !is_worktree_clean(&vt.task_checkout, DEFAULT_GIT_COMMAND_TIMEOUT_MS)? {
                    return Err(OperationsError::DirtyWorktree {
                        worktree_id: vt.task_manifest.worktree_id.clone(),
                        detail: "task worktree has uncommitted, untracked, or ignored files"
                            .to_string(),
                    });
                }
                let actual_task_head = resolve_exact_commit_sha(
                    &vt.task_checkout,
                    "HEAD",
                    DEFAULT_GIT_COMMAND_TIMEOUT_MS,
                )?;
                if actual_task_head != vt.expected_task_head_commit {
                    return Err(OperationsError::HeadMoved {
                        target: format!("task worktree `{}`", vt.task_manifest.worktree_id),
                        expected: vt.expected_task_head_commit.clone(),
                        actual: actual_task_head,
                    });
                }
                let diff_gate = evaluate_diff_gate(
                    &vt.task_manifest.work_area_root,
                    &vt.task_manifest.worktree_id,
                )?;
                if !diff_gate.passed || !diff_gate.evidence_complete {
                    return Err(OperationsError::DiffGateRejected {
                        worktree_id: vt.task_manifest.worktree_id.clone(),
                        detail:
                            "task worktree failed diff gate re-verification before job integration"
                                .to_string(),
                    });
                }
            }

            // 3. Pre-verify conflict-free mergeability across all tasks before writing intent or mutating refs.
            verify_preflight_merge_chain(&preflight, &actual_int_head)?;

            // 4. Persist durable InProgress intent before mutating the RunIntegration checkout.
            let mut intent = build_in_progress_intent(request, &preflight, &actual_int_head)?;
            write_job_integration_intent_unlocked(&preflight.canonical_work_area, &intent)?;

            // 5. Execute each task integration sequentially inside the owned RunIntegration checkout.
            let exec_outcome =
                (|| -> Result<(Vec<IntegrationReceipt>, String, String), OperationsError> {
                    let mut current_int_head = actual_int_head.clone();
                    let mut current_int_tree = resolve_exact_tree_sha(
                        &preflight.integration_checkout,
                        "HEAD",
                        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
                    )?;
                    let total_tasks = preflight.verified_tasks.len();
                    let mut receipts = Vec::with_capacity(total_tasks);
                    let receipts_dir = preflight
                        .canonical_work_area
                        .join(MANIFESTS_SUBDIR)
                        .join(RECEIPTS_SUBDIR);

                    for vt in &preflight.verified_tasks {
                        match vt.strategy {
                            IntegrationStrategy::FastForwardOnly => {
                                let can_ff = is_commit_ancestor(
                                    &preflight.integration_checkout,
                                    &current_int_head,
                                    &vt.expected_task_head_commit,
                                    DEFAULT_GIT_COMMAND_TIMEOUT_MS,
                                )?;
                                if !can_ff {
                                    return Err(OperationsError::AncestryMismatch(format!(
                                    "integration HEAD `{current_int_head}` cannot fast-forward to task `{}` HEAD `{}`",
                                    vt.task_id, vt.expected_task_head_commit
                                )));
                                }
                            }
                            IntegrationStrategy::MergeNoFf => {
                                let conflict_free = check_merge_tree_conflict_free(
                                    &preflight.integration_checkout,
                                    &current_int_head,
                                    &vt.expected_task_head_commit,
                                )?;
                                if !conflict_free {
                                    return Err(OperationsError::MergeConflict(format!(
                                    "merging task `{}` HEAD `{}` into integration HEAD `{current_int_head}` has conflicts",
                                    vt.task_id, vt.expected_task_head_commit
                                )));
                                }
                            }
                        }

                        let argv_refs: Vec<&str> = vt
                            .step_proposal
                            .planned_git_argv
                            .iter()
                            .map(String::as_str)
                            .collect();
                        run_sanitized_git(
                            &preflight.integration_checkout,
                            &argv_refs,
                            DEFAULT_GIT_COMMAND_TIMEOUT_MS,
                        )?;

                        if !is_worktree_clean(
                            &preflight.integration_checkout,
                            DEFAULT_GIT_COMMAND_TIMEOUT_MS,
                        )? {
                            return Err(OperationsError::DirtyWorktree {
                            worktree_id: preflight.integration_manifest.worktree_id.clone(),
                            detail:
                                "integration worktree is not clean after job task merge execution"
                                    .to_string(),
                        });
                        }

                        let new_int_head = resolve_exact_commit_sha(
                            &preflight.integration_checkout,
                            "HEAD",
                            DEFAULT_GIT_COMMAND_TIMEOUT_MS,
                        )?;
                        let new_int_tree = resolve_exact_tree_sha(
                            &preflight.integration_checkout,
                            "HEAD",
                            DEFAULT_GIT_COMMAND_TIMEOUT_MS,
                        )?;

                        if !is_commit_ancestor(
                            &preflight.integration_checkout,
                            &vt.expected_task_head_commit,
                            &new_int_head,
                            DEFAULT_GIT_COMMAND_TIMEOUT_MS,
                        )? {
                            return Err(OperationsError::AncestryMismatch(format!(
                            "accepted task `{}` commit `{}` is not an ancestor of integrated target commit `{new_int_head}`",
                            vt.task_id, vt.expected_task_head_commit
                        )));
                        }
                        if !is_commit_ancestor(
                            &preflight.integration_checkout,
                            &current_int_head,
                            &new_int_head,
                            DEFAULT_GIT_COMMAND_TIMEOUT_MS,
                        )? {
                            return Err(OperationsError::AncestryMismatch(format!(
                            "previous integration head `{current_int_head}` is not an ancestor of integrated target commit `{new_int_head}`"
                        )));
                        }

                        let post_step_source_head = resolve_exact_commit_sha(
                            &preflight.canonical_repo,
                            "HEAD",
                            DEFAULT_GIT_COMMAND_TIMEOUT_MS,
                        )?;
                        if post_step_source_head != preflight.expected_source_head {
                            return Err(OperationsError::SecurityViolation(format!(
                            "primary source repository HEAD unexpectedly changed from `{}` to `{post_step_source_head}`",
                            preflight.expected_source_head
                        )));
                        }

                        let job_agg = JobAggregationReceiptProvenance {
                            job_id: request.job_scope.job_id.clone(),
                            job_revision: request.job_scope.job_revision,
                            approval_id: request.job_scope.approval_id.clone(),
                            approval_subject_digest: request
                                .job_scope
                                .approval_subject_digest
                                .clone(),
                            original_task_run_id: vt.original_run_id.clone(),
                            target_integration_run_id: request
                                .job_scope
                                .target_integration_run_id
                                .clone(),
                            approved_base_commit: preflight.approved_base_commit.clone(),
                            logical_task_id: vt.task_id.clone(),
                            task_digest: vt.task_digest.clone(),
                            batch_step_index: vt.step_index,
                            batch_total_tasks: total_tasks,
                            dependency_seal_digest: vt.dependency_seal_digest.clone(),
                            dependency_snapshot_ref: vt.dependency_snapshot_ref.clone(),
                            accepted_task_tree_sha: vt.subject.task_tree_sha.clone(),
                            accepted_task_diff_digest: vt.subject.diff_digest.clone(),
                            task_allowed_write_paths: vt.subject.allowed_write_paths.clone(),
                            job_aggregate_allowed_write_paths: request
                                .job_scope
                                .aggregate_allowed_write_paths
                                .clone(),
                            task_policy: vt.subject.policy.clone(),
                            task_test_evidence: vt.subject.test_evidence.clone(),
                            proposal_digest: vt.step_proposal.proposal_digest.clone(),
                            intent_id: intent.intent_id.clone(),
                            command_key: request.command_key.clone(),
                            cryptographically_proves_user_authority: false,
                            native_chat_identity_verified: false,
                            grants_main_merge_or_push_authority: false,
                        };

                        let mut receipt = IntegrationReceipt {
                            schema_version: INTEGRATION_RECEIPT_SCHEMA_VERSION,
                            receipt_id: vt.planned_receipt_id.clone(),
                            run_id: vt.original_run_id.clone(),
                            task_id: vt.subject.task_id.clone(),
                            integration_worktree_id: preflight
                                .integration_manifest
                                .worktree_id
                                .clone(),
                            integration_checkout_path: preflight.integration_checkout.clone(),
                            integration_branch_name: preflight
                                .integration_manifest
                                .branch_name
                                .clone(),
                            task_worktree_id: vt.task_manifest.worktree_id.clone(),
                            task_branch_name: vt.task_manifest.branch_name.clone(),
                            repository_path: preflight.canonical_repo.clone(),
                            work_area_root: preflight.canonical_work_area.clone(),
                            base_commit: vt.subject.base_commit.clone(),
                            previous_integration_head_commit: current_int_head,
                            accepted_task_head_commit: vt.expected_task_head_commit.clone(),
                            integrated_target_commit: new_int_head.clone(),
                            integrated_tree_sha: new_int_tree.clone(),
                            source_head_commit: post_step_source_head,
                            review_subject_digest: vt.subject.subject_digest.clone(),
                            review_receipt_id: vt.review_receipt.receipt_id.clone(),
                            review_receipt_digest: vt.review_receipt.receipt_digest.clone(),
                            strategy: vt.strategy,
                            operator_id: request.operator_id.clone(),
                            integrated_at_ms: request.integrated_at_ms,
                            job_aggregation: Some(job_agg),
                            receipt_digest: String::new(),
                        };
                        receipt.receipt_digest = receipt.compute_digest()?;
                        receipt.verify_against_job_scope(
                            &vt.subject,
                            &vt.review_receipt,
                            &request.job_scope,
                            &vt.task_id,
                            &vt.task_digest,
                        )?;

                        let filename = format!("integration-{}.json", receipt.receipt_id);
                        write_private_json_atomic(
                            &receipts_dir,
                            &filename,
                            &receipt,
                            "integration receipt",
                        )?;

                        current_int_head = new_int_head;
                        current_int_tree = new_int_tree;
                        receipts.push(receipt);
                    }

                    Ok((receipts, current_int_head, current_int_tree))
                })();

            let (receipts, final_commit, final_tree) = match exec_outcome {
                Ok(done) => done,
                Err(err) => {
                    intent.state = JobIntegrationIntentState::OutcomeUnknown;
                    intent.uncertainty_reason = Some(sanitize_and_redact(
                        &format!(
                            "job integration execution failed after intent was recorded: {err}"
                        ),
                        MAX_REVIEW_NOTE_BYTES,
                    ));
                    intent.updated_at_ms = request.integrated_at_ms;
                    if let Ok(dig) = intent.compute_digest() {
                        intent.intent_digest = dig;
                        let _ = write_job_integration_intent_unlocked(
                            &preflight.canonical_work_area,
                            &intent,
                        );
                    }
                    return Err(err);
                }
            };

            let post_source_head = resolve_exact_commit_sha(
                &preflight.canonical_repo,
                "HEAD",
                DEFAULT_GIT_COMMAND_TIMEOUT_MS,
            )?;
            let post_source_status = capture_repo_porcelain_status(&preflight.canonical_repo)?;
            if post_source_head != preflight.expected_source_head
                || post_source_status != preflight.pre_source_status
            {
                return Err(OperationsError::SecurityViolation(
                    "primary source repository HEAD or working tree changed during job integration"
                        .to_string(),
                ));
            }

            intent.state = JobIntegrationIntentState::Completed;
            intent.final_integrated_target_commit = Some(final_commit.clone());
            intent.final_integrated_tree_sha = Some(final_tree.clone());
            intent.uncertainty_reason = None;
            intent.updated_at_ms = request.integrated_at_ms;
            intent.intent_digest = intent.compute_digest()?;
            intent.verify_integrity()?;
            write_job_integration_intent_unlocked(&preflight.canonical_work_area, &intent)?;

            Ok(JobIntegrationReport {
                job_scope: request.job_scope.clone(),
                integration_worktree_id: preflight.integration_manifest.worktree_id.clone(),
                integration_checkout_path: preflight.integration_checkout.clone(),
                integration_branch_name: preflight.integration_manifest.branch_name.clone(),
                repository_path: preflight.canonical_repo.clone(),
                work_area_root: preflight.canonical_work_area.clone(),
                approved_base_commit: preflight.approved_base_commit.clone(),
                initial_integration_head_commit: actual_int_head,
                final_integrated_target_commit: final_commit,
                final_integrated_tree_sha: final_tree,
                source_head_commit: post_source_head,
                intent,
                receipts,
                reused_existing_receipts: false,
            })
        })();

        for g in task_guards {
            let _ = g.finish()?;
        }
        let _ = int_guard.finish()?;
        inner_res
    })();

    let _ = lock_file.unlock();
    result
}

#[allow(clippy::too_many_arguments)]
fn verify_job_integration_preflight(
    repository_path: &Path,
    work_area_root: &Path,
    integration_worktree_id: &str,
    approved_base_commit_raw: &str,
    expected_source_head_raw: &str,
    expected_int_head_raw: &str,
    job_scope: &JobIntegrationScopeProvenance,
    tasks: &[JobIntegrationTaskInput],
    command_key_seed: &str,
    enforce_expected_int_head_now: bool,
) -> Result<VerifiedJobIntegrationPreFlight, OperationsError> {
    ensure_platform_supported()?;
    validate_simple_identifier(integration_worktree_id, "integration_worktree_id")?;
    let approved_base_commit =
        validate_exact_40_hex_sha(approved_base_commit_raw, "approved_base_commit")?;
    let expected_source_head =
        validate_exact_40_hex_sha(expected_source_head_raw, "expected_source_head_commit")?;
    let expected_int_head =
        validate_exact_40_hex_sha(expected_int_head_raw, "expected_integration_head_commit")?;
    job_scope.validate()?;

    if tasks.is_empty() || tasks.len() > MAX_DEPENDENCY_CLOSURE_TASKS {
        return Err(OperationsError::InvalidInput(format!(
            "tasks count ({}) must be between 1 and {MAX_DEPENDENCY_CLOSURE_TASKS}",
            tasks.len()
        )));
    }
    if !repository_path.is_absolute() || !work_area_root.is_absolute() {
        return Err(OperationsError::InvalidInput(
            "repository_path and work_area_root must be absolute paths".to_string(),
        ));
    }

    let canonical_repo = validate_existing_canonical_dir(repository_path, "repository_path")?;
    if canonical_repo != repository_path {
        return Err(OperationsError::SecurityViolation(
            "repository_path canonicalization mismatch".to_string(),
        ));
    }
    let canonical_work_area = validate_existing_canonical_dir(work_area_root, "work_area_root")?;
    if canonical_work_area != work_area_root {
        return Err(OperationsError::SecurityViolation(
            "work_area_root canonicalization mismatch".to_string(),
        ));
    }
    if canonical_repo.starts_with(&canonical_work_area)
        || canonical_work_area.starts_with(&canonical_repo)
    {
        return Err(OperationsError::SecurityViolation(
            "repository_path and work_area_root must not overlap".to_string(),
        ));
    }

    let pre_source_head =
        resolve_exact_commit_sha(&canonical_repo, "HEAD", DEFAULT_GIT_COMMAND_TIMEOUT_MS)?;
    if pre_source_head != expected_source_head {
        return Err(OperationsError::HeadMoved {
            target: "primary source repository".to_string(),
            expected: expected_source_head,
            actual: pre_source_head,
        });
    }
    let pre_source_status = capture_repo_porcelain_status(&canonical_repo)?;

    let repo_common_git_dir = resolve_repo_common_git_dir(&canonical_repo)?;
    verify_work_area_marker(&canonical_work_area, &canonical_repo, &repo_common_git_dir)?;

    let resolved_base = resolve_exact_commit_sha(
        &canonical_repo,
        &approved_base_commit,
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )?;
    if resolved_base != approved_base_commit {
        return Err(OperationsError::SubjectMismatch(format!(
            "approved_base_commit `{approved_base_commit}` resolved to `{resolved_base}`"
        )));
    }

    let integration_manifest =
        load_and_verify_integration_manifest(&canonical_work_area, integration_worktree_id)?;
    if integration_manifest.repository_path != canonical_repo
        || integration_manifest.work_area_root != canonical_work_area
        || integration_manifest.repository_common_git_dir.as_ref() != Some(&repo_common_git_dir)
    {
        return Err(OperationsError::WorktreeNotOwned(format!(
            "RunIntegration worktree `{integration_worktree_id}` does not belong to canonical repository and work_area_root"
        )));
    }
    if integration_manifest.run_id != job_scope.target_integration_run_id {
        return Err(OperationsError::WorktreeNotOwned(format!(
            "RunIntegration worktree `{integration_worktree_id}` run_id `{}` does not match job_scope.target_integration_run_id `{}`",
            integration_manifest.run_id, job_scope.target_integration_run_id
        )));
    }
    if integration_manifest.task_id.is_some() {
        return Err(OperationsError::WorktreeNotOwned(format!(
            "RunIntegration worktree `{integration_worktree_id}` must not carry a task_id"
        )));
    }
    if integration_manifest.base_commit != approved_base_commit {
        return Err(OperationsError::SubjectMismatch(format!(
            "RunIntegration worktree base_commit `{}` does not match approved_base_commit `{approved_base_commit}`",
            integration_manifest.base_commit
        )));
    }
    let expected_runs_dir = canonical_work_area.join(CHECKOUTS_SUBDIR).join("runs");
    if !integration_manifest
        .checkout_path
        .starts_with(&expected_runs_dir)
    {
        return Err(OperationsError::WorktreeNotOwned(format!(
            "RunIntegration checkout `{}` is outside `{}`",
            integration_manifest.checkout_path.display(),
            expected_runs_dir.display()
        )));
    }
    let expected_int_branch_prefix =
        format!("threadlane/run/{}/", job_scope.target_integration_run_id);
    if !integration_manifest
        .branch_name
        .starts_with(&expected_int_branch_prefix)
    {
        return Err(OperationsError::WorktreeNotOwned(format!(
            "RunIntegration branch `{}` does not start with `{expected_int_branch_prefix}`",
            integration_manifest.branch_name
        )));
    }

    ensure_worktree_inactive_readonly(&integration_manifest)?;

    let integration_checkout = validate_existing_canonical_dir(
        &integration_manifest.checkout_path,
        "integration checkout_path",
    )?;
    if integration_checkout != integration_manifest.checkout_path {
        return Err(OperationsError::SecurityViolation(
            "integration checkout_path canonicalization mismatch".to_string(),
        ));
    }
    let int_common_git_dir = resolve_repo_common_git_dir(&integration_checkout)?;
    if int_common_git_dir != repo_common_git_dir {
        return Err(OperationsError::WorktreeNotOwned(format!(
            "RunIntegration checkout `{}` git-common-dir `{}` does not match repository `{}`",
            integration_manifest.worktree_id,
            int_common_git_dir.display(),
            repo_common_git_dir.display()
        )));
    }
    verify_worktree_branch(
        &integration_checkout,
        &integration_manifest.branch_name,
        &integration_manifest.worktree_id,
    )?;
    if !is_worktree_clean(&integration_checkout, DEFAULT_GIT_COMMAND_TIMEOUT_MS)? {
        return Err(OperationsError::DirtyWorktree {
            worktree_id: integration_manifest.worktree_id,
            detail: "integration worktree has uncommitted, untracked, or ignored files".to_string(),
        });
    }

    let live_int_head = resolve_exact_commit_sha(
        &integration_checkout,
        "HEAD",
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )?;
    if enforce_expected_int_head_now && live_int_head != expected_int_head {
        return Err(OperationsError::HeadMoved {
            target: format!(
                "integration worktree `{}`",
                integration_manifest.worktree_id
            ),
            expected: expected_int_head,
            actual: live_int_head,
        });
    }

    let resolved_job_scopes = resolve_allowed_write_scopes_at_base(
        &canonical_repo,
        &approved_base_commit,
        &job_scope.aggregate_allowed_write_paths,
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )?;

    let mut seen_task_ids = BTreeSet::new();
    let mut seen_task_worktrees = BTreeSet::new();
    let mut seen_review_receipts = BTreeSet::new();
    let mut seen_receipt_ids = BTreeSet::new();

    let total_tasks = tasks.len();
    let mut verified_tasks = Vec::with_capacity(total_tasks);
    let mut task_proposals = Vec::with_capacity(total_tasks);

    for (step_index, task_input) in tasks.iter().enumerate() {
        validate_simple_identifier(&task_input.original_run_id, "task.original_run_id")?;
        validate_simple_identifier(&task_input.task_id, "task.task_id")?;
        let task_digest = validate_sha256_hex_digest(&task_input.task_digest, "task.task_digest")?;
        validate_simple_identifier(&task_input.task_worktree_id, "task.task_worktree_id")?;
        let expected_task_head = validate_exact_40_hex_sha(
            &task_input.expected_task_head_commit,
            "task.expected_task_head_commit",
        )?;

        if !seen_task_ids.insert(task_input.task_id.clone()) {
            return Err(OperationsError::InvalidInput(format!(
                "duplicate task_id `{}` in job integration request",
                task_input.task_id
            )));
        }
        if !seen_task_worktrees.insert(task_input.task_worktree_id.clone()) {
            return Err(OperationsError::InvalidInput(format!(
                "duplicate task_worktree_id `{}` in job integration request",
                task_input.task_worktree_id
            )));
        }
        if !seen_review_receipts.insert(task_input.review_receipt.receipt_id.clone()) {
            return Err(OperationsError::InvalidInput(format!(
                "duplicate review_receipt_id `{}` in job integration request",
                task_input.review_receipt.receipt_id
            )));
        }

        if task_input.original_run_id != task_input.subject.run_id
            || task_input.original_run_id != task_input.review_receipt.run_id
        {
            return Err(OperationsError::SubjectMismatch(format!(
                "task `{}` original_run_id `{}` does not match subject.run_id `{}` and review_receipt.run_id `{}`",
                task_input.task_id,
                task_input.original_run_id,
                task_input.subject.run_id,
                task_input.review_receipt.run_id
            )));
        }
        if task_input.subject.task_id != task_input.review_receipt.task_id {
            return Err(OperationsError::SubjectMismatch(format!(
                "task `{}` subject.task_id `{}` does not match review_receipt.task_id `{}`",
                task_input.task_id, task_input.subject.task_id, task_input.review_receipt.task_id
            )));
        }
        if task_input.task_worktree_id != task_input.subject.task_worktree_id
            || task_input.task_worktree_id != task_input.review_receipt.task_worktree_id
        {
            return Err(OperationsError::SubjectMismatch(format!(
                "task_worktree_id `{}` does not match subject.task_worktree_id `{}`",
                task_input.task_worktree_id, task_input.subject.task_worktree_id
            )));
        }
        if expected_task_head != task_input.subject.task_head_commit {
            return Err(OperationsError::HeadMoved {
                target: format!("task worktree `{}`", task_input.task_worktree_id),
                expected: task_input.subject.task_head_commit.clone(),
                actual: expected_task_head,
            });
        }

        let task_manifest =
            load_and_verify_task_manifest(&canonical_work_area, &task_input.task_worktree_id)?;
        ensure_worktree_inactive_readonly(&task_manifest)?;

        if task_manifest.repository_path != canonical_repo
            || task_manifest.work_area_root != canonical_work_area
            || task_manifest.repository_common_git_dir.as_ref() != Some(&repo_common_git_dir)
        {
            return Err(OperationsError::WorktreeNotOwned(format!(
                "task `{}` worktree `{}` does not belong to canonical repository and work_area_root",
                task_input.task_id, task_input.task_worktree_id
            )));
        }
        if task_manifest.run_id != task_input.original_run_id {
            return Err(OperationsError::WorktreeNotOwned(format!(
                "task worktree `{}` manifest run_id `{}` does not match original_run_id `{}`",
                task_input.task_worktree_id, task_manifest.run_id, task_input.original_run_id
            )));
        }
        if task_manifest.task_id.as_deref() != Some(task_input.subject.task_id.as_str()) {
            return Err(OperationsError::WorktreeNotOwned(format!(
                "task worktree `{}` manifest task_id `{:?}` does not match subject.task_id `{}`",
                task_input.task_worktree_id, task_manifest.task_id, task_input.subject.task_id
            )));
        }
        let (task_execution_base_commit, dependency_seal_digest, dependency_snapshot_ref) =
            match task_input.dependency_input {
                None => {
                    if task_manifest.base_commit != approved_base_commit
                        || task_input.subject.base_commit != approved_base_commit
                        || task_input.review_receipt.base_commit != approved_base_commit
                    {
                        return Err(OperationsError::SubjectMismatch(format!(
                            "task `{}` base_commit `{}` does not match approved_base_commit `{approved_base_commit}` (and no dependency_input proof was supplied)",
                            task_input.task_id, task_manifest.base_commit
                        )));
                    }
                    (approved_base_commit.clone(), None, None)
                }
                Some(ref dep_proof) => {
                    let seal = verify_dependent_task_execution_proof(
                        dep_proof,
                        &canonical_repo,
                        &canonical_work_area,
                        &approved_base_commit,
                        job_scope,
                        task_input,
                        &task_digest,
                        &task_manifest,
                        &verified_tasks,
                    )?;
                    (
                        seal.execution_base_commit.clone(),
                        Some(seal.seal_digest.clone()),
                        Some(seal.snapshot_ref.clone()),
                    )
                }
            };

        // Verify persisted review receipt on disk matches supplied review_receipt and is not superseded.
        let disk_review =
            load_review_receipt(&canonical_work_area, &task_input.review_receipt.receipt_id)
                .map_err(|err| {
                    OperationsError::ReceiptInvalidated(format!(
                        "task `{}` review receipt `{}` is not persisted or failed verification on disk: {err}",
                        task_input.task_id, task_input.review_receipt.receipt_id
                    ))
                })?;
        if disk_review != task_input.review_receipt {
            return Err(OperationsError::ReceiptInvalidated(format!(
                "task `{}` supplied review_receipt `{}` does not match persisted review receipt on disk",
                task_input.task_id, task_input.review_receipt.receipt_id
            )));
        }
        ensure_review_receipt_not_superseded(&canonical_work_area, &disk_review)?;

        // Verify live task worktree against subject and review_receipt in read-only mode.
        verify_review_receipt_readonly(&task_input.subject, &task_input.review_receipt)?;
        task_input.subject.policy.validate(true)?;
        if !task_input.subject.policy.require_passing_tests
            || !task_input.subject.test_evidence.all_checks_passed()
        {
            return Err(OperationsError::ReceiptInvalidated(format!(
                "task `{}` review subject is missing required passing formatting, Clippy, and behavioral test evidence",
                task_input.task_id
            )));
        }
        task_input.subject.test_evidence.validate(true)?;

        // Verify task write scope is within job_scope.aggregate_allowed_write_paths at both root and execution base.
        let resolved_job_scopes_at_task_base = if task_execution_base_commit == approved_base_commit
        {
            resolved_job_scopes.clone()
        } else {
            resolve_allowed_write_scopes_at_base(
                &canonical_repo,
                &task_execution_base_commit,
                &job_scope.aggregate_allowed_write_paths,
                DEFAULT_GIT_COMMAND_TIMEOUT_MS,
            )?
        };
        for scope_entry in &task_input.subject.allowed_write_paths {
            let clean_scope = scope_entry
                .strip_suffix('/')
                .unwrap_or(scope_entry.as_str());
            if !job_scope
                .aggregate_allowed_write_paths
                .contains(scope_entry)
                && (!is_path_allowed_by_scopes(clean_scope, &resolved_job_scopes)
                    || !is_path_allowed_by_scopes(clean_scope, &resolved_job_scopes_at_task_base))
            {
                return Err(OperationsError::DiffGateRejected {
                    worktree_id: task_manifest.worktree_id.clone(),
                    detail: format!(
                        "task `{}` allowed_write_paths entry `{scope_entry}` is outside job_scope.aggregate_allowed_write_paths {:?}",
                        task_input.task_id, job_scope.aggregate_allowed_write_paths
                    ),
                });
            }
        }
        for changed in &task_input.subject.changed_paths {
            if !is_path_allowed_by_scopes(&changed.path, &resolved_job_scopes)
                || !is_path_allowed_by_scopes(&changed.path, &resolved_job_scopes_at_task_base)
            {
                return Err(OperationsError::DiffGateRejected {
                    worktree_id: task_manifest.worktree_id.clone(),
                    detail: format!(
                        "task `{}` changed path `{}` is outside job_scope.aggregate_allowed_write_paths {:?}",
                        task_input.task_id, changed.path, job_scope.aggregate_allowed_write_paths
                    ),
                });
            }
            if let Some(ref orig) = changed.orig_path {
                if !is_path_allowed_by_scopes(orig, &resolved_job_scopes)
                    || !is_path_allowed_by_scopes(orig, &resolved_job_scopes_at_task_base)
                {
                    return Err(OperationsError::DiffGateRejected {
                        worktree_id: task_manifest.worktree_id.clone(),
                        detail: format!(
                            "task `{}` rename origin `{orig}` is outside job_scope.aggregate_allowed_write_paths {:?}",
                            task_input.task_id, job_scope.aggregate_allowed_write_paths
                        ),
                    });
                }
            }
        }

        let task_checkout =
            validate_existing_canonical_dir(&task_manifest.checkout_path, "task checkout_path")?;
        verify_worktree_branch(
            &task_checkout,
            &task_manifest.branch_name,
            &task_manifest.worktree_id,
        )?;
        if !is_worktree_clean(&task_checkout, DEFAULT_GIT_COMMAND_TIMEOUT_MS)? {
            return Err(OperationsError::DirtyWorktree {
                worktree_id: task_manifest.worktree_id,
                detail: "task worktree has uncommitted, untracked, or ignored files".to_string(),
            });
        }
        let actual_task_head =
            resolve_exact_commit_sha(&task_checkout, "HEAD", DEFAULT_GIT_COMMAND_TIMEOUT_MS)?;
        let actual_task_tree =
            resolve_exact_tree_sha(&task_checkout, "HEAD", DEFAULT_GIT_COMMAND_TIMEOUT_MS)?;
        if actual_task_head != expected_task_head
            || actual_task_tree != task_input.subject.task_tree_sha
        {
            return Err(OperationsError::ReceiptInvalidated(format!(
                "task `{}` worktree HEAD or tree SHA changed since review receipt was issued",
                task_input.task_id
            )));
        }

        let planned_receipt_id = match task_input.receipt_id {
            Some(ref id) => {
                validate_simple_identifier(id, "task.receipt_id")?;
                id.clone()
            }
            None => {
                let seed = format!(
                    "job_int_rcp:{}:{}:{}:{}:{}:{}",
                    job_scope.job_id,
                    job_scope.job_revision,
                    integration_manifest.worktree_id,
                    command_key_seed,
                    task_input.task_id,
                    task_input.review_receipt.receipt_digest
                );
                format!("int_{}", &sha256_hex(seed.as_bytes())[..16])
            }
        };
        if !seen_receipt_ids.insert(planned_receipt_id.clone()) {
            return Err(OperationsError::InvalidInput(format!(
                "duplicate integration receipt_id `{planned_receipt_id}` in job integration request"
            )));
        }

        let already_integrated = is_commit_ancestor(
            &integration_checkout,
            &expected_task_head,
            &expected_int_head,
            DEFAULT_GIT_COMMAND_TIMEOUT_MS,
        )?;
        let can_ff = if step_index == 0 {
            is_commit_ancestor(
                &integration_checkout,
                &expected_int_head,
                &expected_task_head,
                DEFAULT_GIT_COMMAND_TIMEOUT_MS,
            )?
        } else {
            false
        };
        let mut clean_merge_predicted = match task_input.strategy {
            IntegrationStrategy::FastForwardOnly => can_ff,
            IntegrationStrategy::MergeNoFf => check_merge_tree_conflict_free(
                &integration_checkout,
                &expected_int_head,
                &expected_task_head,
            )?,
        };
        if task_input.strategy == IntegrationStrategy::MergeNoFf && clean_merge_predicted {
            for prev in &verified_tasks {
                let prev_ctx: &VerifiedJobTaskContext = prev;
                if !check_merge_tree_conflict_free(
                    &integration_checkout,
                    &prev_ctx.expected_task_head_commit,
                    &expected_task_head,
                )? {
                    clean_merge_predicted = false;
                    break;
                }
            }
        }

        let planned_git_argv = build_job_integration_git_argv(
            task_input.strategy,
            &job_scope.job_id,
            &job_scope.target_integration_run_id,
            &task_input.original_run_id,
            &task_input.task_id,
            &expected_task_head,
            &task_input.review_receipt.receipt_id,
        );

        let mut step_proposal = JobTaskIntegrationProposal {
            step_index,
            total_tasks,
            job_id: job_scope.job_id.clone(),
            job_revision: job_scope.job_revision,
            approval_id: job_scope.approval_id.clone(),
            approval_subject_digest: job_scope.approval_subject_digest.clone(),
            original_task_run_id: task_input.original_run_id.clone(),
            target_integration_run_id: job_scope.target_integration_run_id.clone(),
            task_id: task_input.task_id.clone(),
            task_digest: task_digest.clone(),
            integration_worktree_id: integration_manifest.worktree_id.clone(),
            integration_checkout_path: integration_checkout.clone(),
            integration_branch_name: integration_manifest.branch_name.clone(),
            task_worktree_id: task_manifest.worktree_id.clone(),
            task_checkout_path: task_checkout.clone(),
            task_branch_name: task_manifest.branch_name.clone(),
            repository_path: canonical_repo.clone(),
            source_head_commit: pre_source_head.clone(),
            approved_base_commit: approved_base_commit.clone(),
            base_commit: task_execution_base_commit,
            dependency_seal_digest: dependency_seal_digest.clone(),
            initial_integration_head_commit: expected_int_head.clone(),
            task_head_commit: expected_task_head.clone(),
            task_tree_sha: actual_task_tree,
            task_diff_digest: task_input.subject.diff_digest.clone(),
            review_subject_digest: task_input.subject.subject_digest.clone(),
            review_receipt_id: task_input.review_receipt.receipt_id.clone(),
            review_receipt_digest: task_input.review_receipt.receipt_digest.clone(),
            strategy: task_input.strategy,
            can_fast_forward: can_ff,
            already_integrated,
            clean_merge_predicted,
            planned_git_argv,
            read_only: true,
            proposal_digest: String::new(),
        };
        step_proposal.proposal_digest = step_proposal.compute_digest()?;
        task_proposals.push(step_proposal.clone());

        verified_tasks.push(VerifiedJobTaskContext {
            step_index,
            original_run_id: task_input.original_run_id.clone(),
            task_id: task_input.task_id.clone(),
            task_digest,
            task_manifest,
            task_checkout,
            subject: task_input.subject.clone(),
            review_receipt: task_input.review_receipt.clone(),
            expected_task_head_commit: expected_task_head,
            dependency_seal_digest,
            dependency_snapshot_ref,
            strategy: task_input.strategy,
            planned_receipt_id,
            step_proposal,
        });
    }

    let all_clean_merges_predicted = task_proposals.iter().all(|p| p.clean_merge_predicted);
    let mut proposal = JobIntegrationProposal {
        job_scope: job_scope.clone(),
        integration_worktree_id: integration_manifest.worktree_id.clone(),
        integration_checkout_path: integration_checkout.clone(),
        integration_branch_name: integration_manifest.branch_name.clone(),
        repository_path: canonical_repo.clone(),
        work_area_root: canonical_work_area.clone(),
        approved_base_commit: approved_base_commit.clone(),
        source_head_commit: pre_source_head,
        initial_integration_head_commit: expected_int_head.clone(),
        task_proposals,
        all_clean_merges_predicted,
        read_only: true,
        proposal_digest: String::new(),
    };
    proposal.proposal_digest = proposal.compute_digest()?;

    Ok(VerifiedJobIntegrationPreFlight {
        canonical_repo,
        canonical_work_area,
        approved_base_commit,
        expected_source_head,
        expected_int_head,
        pre_source_status,
        integration_manifest,
        integration_checkout,
        verified_tasks,
        proposal,
    })
}

#[allow(clippy::too_many_arguments)]
fn verify_dependent_task_execution_proof(
    dep_proof: &JobTaskDependencyExecutionProof,
    canonical_repo: &Path,
    canonical_work_area: &Path,
    approved_base_commit: &str,
    job_scope: &JobIntegrationScopeProvenance,
    task_input: &JobIntegrationTaskInput,
    task_digest: &str,
    task_manifest: &WorktreeManifest,
    verified_tasks: &[VerifiedJobTaskContext],
) -> Result<DependencyInputSeal, OperationsError> {
    if dep_proof.policy.approval_id != job_scope.approval_id
        || dep_proof.policy.approval_subject_digest != job_scope.approval_subject_digest
    {
        return Err(OperationsError::SubjectMismatch(format!(
            "dependent task `{}` dependency_input policy approval binding (`{}` / `{}`) does not match job_scope (`{}` / `{}`)",
            task_input.task_id,
            dep_proof.policy.approval_id,
            dep_proof.policy.approval_subject_digest,
            job_scope.approval_id,
            job_scope.approval_subject_digest
        )));
    }
    if dep_proof.policy.task_id != task_input.task_id || dep_proof.policy.task_digest != task_digest
    {
        return Err(OperationsError::SubjectMismatch(format!(
            "dependent task `{}` dependency_input policy task binding (`{}` / `{}`) does not match task input (`{}` / `{task_digest}`)",
            task_input.task_id,
            dep_proof.policy.task_id,
            dep_proof.policy.task_digest,
            task_input.task_id
        )));
    }
    if task_manifest.allowed_write_paths != task_input.subject.allowed_write_paths
        || task_manifest.allowed_write_paths != task_input.review_receipt.allowed_write_paths
    {
        return Err(OperationsError::SubjectMismatch(format!(
            "dependent task `{}` manifest allowed_write_paths {:?} does not match subject/review_receipt {:?}",
            task_input.task_id,
            task_manifest.allowed_write_paths,
            task_input.subject.allowed_write_paths
        )));
    }

    let seal = match inspect_dependency_input_seal_readonly(
        &dep_proof.policy,
        canonical_repo,
        canonical_work_area,
        approved_base_commit,
        &dep_proof.instruction,
        &dep_proof.policy.command_key,
    ) {
        Ok(Some(seal)) => seal,
        Ok(None) => {
            return Err(OperationsError::ReceiptInvalidated(format!(
                "dependent task `{}` is missing persisted DependencyInputSeal at `{}`",
                task_input.task_id,
                dep_proof.policy.seal_path.display()
            )));
        }
        Err(OperationsError::ReceiptInvalidated(msg)) if msg.contains("has been superseded by") => {
            verify_dependency_seal_dual_receipt_aware(
                &dep_proof.policy,
                canonical_repo,
                canonical_work_area,
                approved_base_commit,
                &dep_proof.instruction,
            )?
        }
        Err(err) => return Err(err),
    };

    if seal.approval_id != job_scope.approval_id
        || seal.approval_subject_digest != job_scope.approval_subject_digest
        || seal.task_id != task_input.task_id
        || seal.task_digest != task_digest
        || seal.repository_path != canonical_repo
        || seal.work_area_root != canonical_work_area
        || seal.approved_base_commit != approved_base_commit
    {
        return Err(OperationsError::SubjectMismatch(format!(
            "dependent task `{}` DependencyInputSeal provenance does not match job_scope, repository, work_area_root, or approved_base_commit",
            task_input.task_id
        )));
    }

    if seal.execution_base_commit == approved_base_commit {
        return Err(OperationsError::SubjectMismatch(format!(
            "dependent task `{}` DependencyInputSeal execution_base_commit `{}` must differ from root approved_base_commit `{approved_base_commit}`",
            task_input.task_id, seal.execution_base_commit
        )));
    }

    if task_manifest.base_commit != seal.execution_base_commit
        || task_input.subject.base_commit != seal.execution_base_commit
        || task_input.review_receipt.base_commit != seal.execution_base_commit
    {
        return Err(OperationsError::SubjectMismatch(format!(
            "dependent task `{}` base_commit (manifest `{}`, subject `{}`, review_receipt `{}`) does not match sealed execution_base_commit `{}`",
            task_input.task_id,
            task_manifest.base_commit,
            task_input.subject.base_commit,
            task_input.review_receipt.base_commit,
            seal.execution_base_commit
        )));
    }

    let resolved_exec_base = resolve_exact_commit_sha(
        canonical_repo,
        &seal.execution_base_commit,
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )?;
    if resolved_exec_base != seal.execution_base_commit {
        return Err(OperationsError::ReceiptInvalidated(format!(
            "dependent task `{}` sealed execution_base_commit `{}` resolved to `{resolved_exec_base}`",
            task_input.task_id, seal.execution_base_commit
        )));
    }
    let resolved_exec_tree = resolve_exact_tree_sha(
        canonical_repo,
        &seal.execution_base_commit,
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )?;
    if resolved_exec_tree != seal.execution_tree_sha {
        return Err(OperationsError::ReceiptInvalidated(format!(
            "dependent task `{}` sealed execution_base_commit `{}` tree `{resolved_exec_tree}` does not match sealed execution_tree_sha `{}`",
            task_input.task_id, seal.execution_base_commit, seal.execution_tree_sha
        )));
    }
    if !is_commit_ancestor(
        canonical_repo,
        approved_base_commit,
        &seal.execution_base_commit,
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )? {
        return Err(OperationsError::AncestryMismatch(format!(
            "approved_base_commit `{approved_base_commit}` is not an ancestor of dependent task `{}` sealed execution_base_commit `{}`",
            task_input.task_id, seal.execution_base_commit
        )));
    }
    if !is_commit_ancestor(
        canonical_repo,
        &seal.execution_base_commit,
        &task_input.expected_task_head_commit,
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )? {
        return Err(OperationsError::AncestryMismatch(format!(
            "dependent task `{}` sealed execution_base_commit `{}` is not an ancestor of task head `{}`",
            task_input.task_id, seal.execution_base_commit, task_input.expected_task_head_commit
        )));
    }

    for pred in &seal.predecessors {
        let Some(prev_task) = verified_tasks.iter().find(|vt| vt.task_id == pred.task_id) else {
            return Err(OperationsError::ReceiptInvalidated(format!(
                "dependent task `{}` sealed predecessor `{}` must appear earlier in the job integration batch",
                task_input.task_id, pred.task_id
            )));
        };
        if prev_task.original_run_id != prev_task.task_manifest.run_id
            || prev_task.task_digest != pred.task_digest
            || prev_task.task_manifest.worktree_id != pred.task_worktree_id
            || prev_task.subject.subject_digest != pred.review_subject_digest
            || prev_task.review_receipt.receipt_id != pred.review_receipt_id
            || prev_task.review_receipt.receipt_digest != pred.review_receipt_digest
            || prev_task.expected_task_head_commit != pred.accepted_task_head_commit
        {
            return Err(OperationsError::ReceiptInvalidated(format!(
                "dependent task `{}` sealed predecessor `{}` proof does not match predecessor task in job integration batch",
                task_input.task_id, pred.task_id
            )));
        }
        let orig_int_receipt =
            load_integration_receipt(canonical_work_area, &pred.integration_receipt_id)?;
        if orig_int_receipt.job_aggregation.is_some()
            || orig_int_receipt.receipt_digest != pred.integration_receipt_digest
            || orig_int_receipt.run_id != prev_task.task_manifest.run_id
            || Some(orig_int_receipt.task_id.as_str()) != prev_task.task_manifest.task_id.as_deref()
            || orig_int_receipt.task_worktree_id != pred.task_worktree_id
            || orig_int_receipt.review_receipt_id != pred.review_receipt_id
            || orig_int_receipt.review_receipt_digest != pred.review_receipt_digest
            || orig_int_receipt.review_subject_digest != pred.review_subject_digest
            || orig_int_receipt.accepted_task_head_commit != pred.accepted_task_head_commit
        {
            return Err(OperationsError::ReceiptInvalidated(format!(
                "dependent task `{}` sealed predecessor `{}` original same-run integration receipt `{}` failed verification",
                task_input.task_id, pred.task_id, pred.integration_receipt_id
            )));
        }
    }

    Ok(seal)
}

fn verify_dependency_seal_dual_receipt_aware(
    policy: &DependencyInputPolicy,
    canonical_repo: &Path,
    canonical_work_area: &Path,
    approved_base_commit: &str,
    instruction: &str,
) -> Result<DependencyInputSeal, OperationsError> {
    let snapshot_ref = dependency_input_snapshot_ref(&policy.approval_id, &policy.task_id)?;
    let symbolic_check = run_sanitized_git_allow_nonzero(
        canonical_repo,
        &["symbolic-ref", "-q", &snapshot_ref],
        DEFAULT_MAX_GIT_OUTPUT_BYTES,
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )?;
    if symbolic_check.exit_code == Some(0) {
        return Err(OperationsError::SecurityViolation(format!(
            "dependency snapshot ref `{snapshot_ref}` must not be a symbolic ref"
        )));
    }

    let verified = read_dependency_input(
        policy,
        canonical_repo,
        canonical_work_area,
        approved_base_commit,
        instruction,
        &policy.command_key,
    )?;
    let intent_path = dependency_input_intent_path(policy);
    if intent_path.exists() {
        let existing_intent: DependencyPreparationIntent = read_private_json(
            &intent_path,
            MAX_DEPENDENCY_INPUT_BYTES as u64,
            "dependency preparation intent",
        )?;
        existing_intent.seal.verify_integrity()?;
        if existing_intent.seal != verified.seal {
            return Err(OperationsError::ReceiptInvalidated(format!(
                "dependency preparation intent for task `{}` conflicts with sealed dependency input",
                policy.task_id
            )));
        }
    }

    let seal = verified.seal.clone();
    seal.verify_integrity()?;

    for pred in &seal.predecessors {
        let pred_task_manifest =
            load_and_verify_task_manifest(canonical_work_area, &pred.task_worktree_id)?;
        let pred_review_receipt =
            load_review_receipt(canonical_work_area, &pred.review_receipt_id)?;
        let pred_int_receipt =
            load_integration_receipt(canonical_work_area, &pred.integration_receipt_id)?;

        if pred_int_receipt.job_aggregation.is_some() {
            return Err(OperationsError::ReceiptInvalidated(format!(
                "predecessor `{}` dependency seal must reference original same-run integration receipt, not job_aggregation receipt `{}`",
                pred.task_id, pred_int_receipt.receipt_id
            )));
        }
        if pred_review_receipt.receipt_id != pred.review_receipt_id
            || pred_review_receipt.receipt_digest != pred.review_receipt_digest
            || pred_review_receipt.subject_digest != pred.review_subject_digest
            || pred_review_receipt.task_worktree_id != pred.task_worktree_id
            || pred_review_receipt.run_id != pred_task_manifest.run_id
            || Some(pred_review_receipt.task_id.as_str()) != pred_task_manifest.task_id.as_deref()
            || pred_review_receipt.repository_path != canonical_repo
            || pred_review_receipt.work_area_root != canonical_work_area
            || pred_review_receipt.base_commit != pred_task_manifest.base_commit
            || pred_review_receipt.task_head_commit != pred.accepted_task_head_commit
            || pred_review_receipt.allowed_write_paths != pred_task_manifest.allowed_write_paths
        {
            return Err(OperationsError::ReceiptInvalidated(format!(
                "review receipt `{}` does not match sealed predecessor `{}`",
                pred_review_receipt.receipt_id, pred.task_id
            )));
        }
        if pred_int_receipt.receipt_id != pred.integration_receipt_id
            || pred_int_receipt.receipt_digest != pred.integration_receipt_digest
            || pred_int_receipt.review_receipt_id != pred_review_receipt.receipt_id
            || pred_int_receipt.review_receipt_digest != pred_review_receipt.receipt_digest
            || pred_int_receipt.review_subject_digest != pred_review_receipt.subject_digest
            || pred_int_receipt.task_worktree_id != pred.task_worktree_id
            || pred_int_receipt.run_id != pred_task_manifest.run_id
            || Some(pred_int_receipt.task_id.as_str()) != pred_task_manifest.task_id.as_deref()
            || pred_int_receipt.repository_path != canonical_repo
            || pred_int_receipt.work_area_root != canonical_work_area
            || pred_int_receipt.base_commit != pred_task_manifest.base_commit
            || pred_int_receipt.source_head_commit != seal.approved_base_commit
            || pred_int_receipt.accepted_task_head_commit != pred.accepted_task_head_commit
        {
            return Err(OperationsError::ReceiptInvalidated(format!(
                "integration receipt `{}` does not match sealed predecessor `{}`",
                pred_int_receipt.receipt_id, pred.task_id
            )));
        }

        let pred_int_manifest = load_and_verify_integration_manifest(
            canonical_work_area,
            &pred_int_receipt.integration_worktree_id,
        )?;
        if pred_task_manifest.run_id != pred_int_manifest.run_id
            || pred_task_manifest.base_commit != pred_int_manifest.base_commit
        {
            return Err(OperationsError::WorktreeNotOwned(format!(
                "predecessor `{}` same-run integration manifest mismatch",
                pred.task_id
            )));
        }

        let reconstructed_subject = inspect_review_subject(&InspectReviewSubjectRequest {
            work_area_root: canonical_work_area.to_path_buf(),
            task_worktree_id: pred.task_worktree_id.clone(),
            policy: pred_review_receipt.policy.clone(),
            test_evidence: pred_review_receipt.test_evidence.clone(),
            worker_status: None,
        })?;
        verify_review_receipt(&reconstructed_subject, &pred_review_receipt)?;
        pred_int_receipt.verify_against_review(&reconstructed_subject, &pred_review_receipt)?;
        ensure_review_receipt_not_superseded(canonical_work_area, &pred_review_receipt)?;
        ensure_integration_receipt_not_superseded_on_worktree(
            canonical_work_area,
            &pred_int_receipt,
        )?;

        ensure_worktree_inactive(&pred_int_manifest)?;
        let pred_int_checkout = validate_existing_canonical_dir(
            &pred_int_manifest.checkout_path,
            "predecessor integration checkout_path",
        )?;
        verify_worktree_branch(
            &pred_int_checkout,
            &pred_int_manifest.branch_name,
            &pred_int_manifest.worktree_id,
        )?;
        if !is_worktree_clean(&pred_int_checkout, DEFAULT_GIT_COMMAND_TIMEOUT_MS)? {
            return Err(OperationsError::DirtyWorktree {
                worktree_id: pred_int_manifest.worktree_id,
                detail: "predecessor integration worktree is dirty".to_string(),
            });
        }
        let live_pred_int_head =
            resolve_exact_commit_sha(&pred_int_checkout, "HEAD", DEFAULT_GIT_COMMAND_TIMEOUT_MS)?;
        if live_pred_int_head != pred_int_receipt.integrated_target_commit {
            return Err(OperationsError::HeadMoved {
                target: format!("integration_worktree:{}", pred_int_manifest.worktree_id),
                expected: pred_int_receipt.integrated_target_commit,
                actual: live_pred_int_head,
            });
        }
    }

    Ok(seal)
}

fn ensure_integration_receipt_not_superseded_on_worktree(
    canonical_work_area: &Path,
    integration_receipt: &IntegrationReceipt,
) -> Result<(), OperationsError> {
    let receipts_dir = canonical_work_area
        .join(MANIFESTS_SUBDIR)
        .join(RECEIPTS_SUBDIR);
    let entries = fs::read_dir(&receipts_dir).map_err(OperationsError::Io)?;
    for entry in entries {
        let entry = entry.map_err(OperationsError::Io)?;
        let file_name = entry.file_name();
        let Some(name) = file_name.to_str() else {
            return Err(OperationsError::SecurityViolation(
                "non-UTF-8 entry in manifests/receipts".to_string(),
            ));
        };
        if name.starts_with('.') {
            continue;
        }
        if let Some(other_int_id) = name
            .strip_prefix("integration-")
            .and_then(|rest| rest.strip_suffix(".json"))
        {
            let other = load_integration_receipt(canonical_work_area, other_int_id)?;
            if other.task_worktree_id == integration_receipt.task_worktree_id
                && other.integration_worktree_id == integration_receipt.integration_worktree_id
                && other.receipt_id != integration_receipt.receipt_id
                && other.integrated_at_ms >= integration_receipt.integrated_at_ms
            {
                return Err(OperationsError::ReceiptInvalidated(format!(
                    "integration receipt `{}` for worktree `{}` on integration worktree `{}` has been superseded by `{}`",
                    integration_receipt.receipt_id,
                    integration_receipt.task_worktree_id,
                    integration_receipt.integration_worktree_id,
                    other.receipt_id
                )));
            }
        }
    }
    Ok(())
}

fn verify_preflight_merge_chain(
    preflight: &VerifiedJobIntegrationPreFlight,
    actual_int_head: &str,
) -> Result<(), OperationsError> {
    let mut current_known_head: Option<String> = Some(actual_int_head.to_string());
    for (idx, vt) in preflight.verified_tasks.iter().enumerate() {
        match vt.strategy {
            IntegrationStrategy::FastForwardOnly => {
                let can_ff = match current_known_head {
                    Some(ref cur_head) => is_commit_ancestor(
                        &preflight.integration_checkout,
                        cur_head,
                        &vt.expected_task_head_commit,
                        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
                    )?,
                    None => false,
                };
                if !can_ff {
                    return Err(OperationsError::AncestryMismatch(format!(
                        "integration worktree cannot fast-forward to task `{}` HEAD `{}` at step {idx}",
                        vt.task_id, vt.expected_task_head_commit
                    )));
                }
                current_known_head = Some(vt.expected_task_head_commit.clone());
            }
            IntegrationStrategy::MergeNoFf => {
                if !check_merge_tree_conflict_free(
                    &preflight.integration_checkout,
                    actual_int_head,
                    &vt.expected_task_head_commit,
                )? {
                    return Err(OperationsError::MergeConflict(format!(
                        "merging task `{}` HEAD `{}` into integration HEAD `{actual_int_head}` has conflicts; integration worktree preserved untouched",
                        vt.task_id, vt.expected_task_head_commit
                    )));
                }
                for prev in &preflight.verified_tasks[..idx] {
                    if !check_merge_tree_conflict_free(
                        &preflight.integration_checkout,
                        &prev.expected_task_head_commit,
                        &vt.expected_task_head_commit,
                    )? {
                        return Err(OperationsError::MergeConflict(format!(
                            "merging task `{}` HEAD `{}` conflicts with predecessor task `{}` HEAD `{}`; integration worktree preserved untouched",
                            vt.task_id,
                            vt.expected_task_head_commit,
                            prev.task_id,
                            prev.expected_task_head_commit
                        )));
                    }
                }
                current_known_head = None;
            }
        }
    }
    Ok(())
}

fn build_in_progress_intent(
    request: &ExecuteJobIntegrationRequest,
    preflight: &VerifiedJobIntegrationPreFlight,
    actual_int_head: &str,
) -> Result<JobIntegrationIntent, OperationsError> {
    let intent_id = match request.intent_id {
        Some(ref id) => {
            validate_simple_identifier(id, "intent_id")?;
            id.clone()
        }
        None => {
            let seed = format!(
                "job_int_intent:{}:{}:{}:{}:{}",
                request.job_scope.job_id,
                request.job_scope.job_revision,
                preflight.integration_manifest.worktree_id,
                request.command_key,
                preflight.proposal.proposal_digest
            );
            format!("jint_{}", &sha256_hex(seed.as_bytes())[..16])
        }
    };

    let planned_tasks: Vec<JobIntegrationPlannedTask> = preflight
        .verified_tasks
        .iter()
        .map(|vt| JobIntegrationPlannedTask {
            step_index: vt.step_index,
            original_run_id: vt.original_run_id.clone(),
            task_id: vt.task_id.clone(),
            task_digest: vt.task_digest.clone(),
            task_worktree_id: vt.task_manifest.worktree_id.clone(),
            task_base_commit: vt.subject.base_commit.clone(),
            dependency_seal_digest: vt.dependency_seal_digest.clone(),
            accepted_task_head_commit: vt.expected_task_head_commit.clone(),
            accepted_task_tree_sha: vt.subject.task_tree_sha.clone(),
            accepted_task_diff_digest: vt.subject.diff_digest.clone(),
            review_subject_digest: vt.subject.subject_digest.clone(),
            review_receipt_id: vt.review_receipt.receipt_id.clone(),
            review_receipt_digest: vt.review_receipt.receipt_digest.clone(),
            strategy: vt.strategy,
            planned_receipt_id: vt.planned_receipt_id.clone(),
            proposal_digest: vt.step_proposal.proposal_digest.clone(),
        })
        .collect();

    let mut intent = JobIntegrationIntent {
        schema_version: JOB_INTEGRATION_INTENT_SCHEMA_VERSION,
        intent_id,
        command_key: request.command_key.clone(),
        job_scope: request.job_scope.clone(),
        repository_path: preflight.canonical_repo.clone(),
        work_area_root: preflight.canonical_work_area.clone(),
        integration_worktree_id: preflight.integration_manifest.worktree_id.clone(),
        approved_base_commit: preflight.approved_base_commit.clone(),
        expected_source_head_commit: preflight.expected_source_head.clone(),
        initial_integration_head_commit: actual_int_head.to_string(),
        tasks: planned_tasks,
        state: JobIntegrationIntentState::InProgress,
        final_integrated_target_commit: None,
        final_integrated_tree_sha: None,
        uncertainty_reason: None,
        operator_id: request.operator_id.clone(),
        recorded_at_ms: request.integrated_at_ms,
        updated_at_ms: request.integrated_at_ms,
        intent_digest: String::new(),
    };
    intent.intent_digest = intent.compute_digest()?;
    intent.verify_integrity()?;
    Ok(intent)
}

fn intent_matches_request_payload(
    intent: &JobIntegrationIntent,
    request: &ExecuteJobIntegrationRequest,
    canonical_repo: &Path,
    canonical_work_area: &Path,
) -> bool {
    if intent.job_scope != request.job_scope
        || intent.repository_path != canonical_repo
        || intent.work_area_root != canonical_work_area
        || intent.integration_worktree_id != request.integration_worktree_id
        || intent.approved_base_commit != request.approved_base_commit
        || intent.expected_source_head_commit != request.expected_source_head_commit
        || intent.initial_integration_head_commit != request.expected_integration_head_commit
        || intent.tasks.len() != request.tasks.len()
    {
        return false;
    }

    for (planned, req_task) in intent.tasks.iter().zip(request.tasks.iter()) {
        if planned.original_run_id != req_task.original_run_id
            || planned.task_id != req_task.task_id
            || planned.task_digest != req_task.task_digest
            || planned.task_worktree_id != req_task.task_worktree_id
            || planned.task_base_commit != req_task.subject.base_commit
            || planned.dependency_seal_digest.is_some() != req_task.dependency_input.is_some()
            || planned.accepted_task_head_commit != req_task.expected_task_head_commit
            || planned.accepted_task_tree_sha != req_task.subject.task_tree_sha
            || planned.accepted_task_diff_digest != req_task.subject.diff_digest
            || planned.review_subject_digest != req_task.subject.subject_digest
            || planned.review_receipt_id != req_task.review_receipt.receipt_id
            || planned.review_receipt_digest != req_task.review_receipt.receipt_digest
            || planned.strategy != req_task.strategy
        {
            return false;
        }
        if let Some(ref exp_rcp_id) = req_task.receipt_id {
            if &planned.planned_receipt_id != exp_rcp_id {
                return false;
            }
        }
    }
    true
}

fn reuse_completed_intent_receipts(
    intent: &JobIntegrationIntent,
    preflight: &VerifiedJobIntegrationPreFlight,
) -> Result<JobIntegrationReport, OperationsError> {
    let Some(ref expected_final_commit) = intent.final_integrated_target_commit else {
        return Err(OperationsError::ReceiptInvalidated(
            "completed job integration intent is missing final_integrated_target_commit"
                .to_string(),
        ));
    };
    let Some(ref expected_final_tree) = intent.final_integrated_tree_sha else {
        return Err(OperationsError::ReceiptInvalidated(
            "completed job integration intent is missing final_integrated_tree_sha".to_string(),
        ));
    };

    let live_int_head = resolve_exact_commit_sha(
        &preflight.integration_checkout,
        "HEAD",
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )?;
    if &live_int_head != expected_final_commit {
        return Err(OperationsError::HeadMoved {
            target: format!(
                "integration worktree `{}`",
                preflight.integration_manifest.worktree_id
            ),
            expected: expected_final_commit.clone(),
            actual: live_int_head,
        });
    }
    let live_int_tree = resolve_exact_tree_sha(
        &preflight.integration_checkout,
        "HEAD",
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )?;
    if &live_int_tree != expected_final_tree {
        return Err(OperationsError::ReceiptInvalidated(format!(
            "integration worktree `{}` tree `{live_int_tree}` does not match completed intent tree `{expected_final_tree}`",
            preflight.integration_manifest.worktree_id
        )));
    }

    let receipts =
        load_and_verify_intent_receipt_chain(intent, preflight, &live_int_head, &live_int_tree)?;
    let post_source_head = resolve_exact_commit_sha(
        &preflight.canonical_repo,
        "HEAD",
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )?;
    let post_source_status = capture_repo_porcelain_status(&preflight.canonical_repo)?;
    if post_source_head != preflight.expected_source_head
        || post_source_status != preflight.pre_source_status
    {
        return Err(OperationsError::SecurityViolation(
            "primary source repository HEAD or working tree changed during duplicate job integration verification"
                .to_string(),
        ));
    }

    Ok(JobIntegrationReport {
        job_scope: intent.job_scope.clone(),
        integration_worktree_id: preflight.integration_manifest.worktree_id.clone(),
        integration_checkout_path: preflight.integration_checkout.clone(),
        integration_branch_name: preflight.integration_manifest.branch_name.clone(),
        repository_path: preflight.canonical_repo.clone(),
        work_area_root: preflight.canonical_work_area.clone(),
        approved_base_commit: preflight.approved_base_commit.clone(),
        initial_integration_head_commit: intent.initial_integration_head_commit.clone(),
        final_integrated_target_commit: live_int_head,
        final_integrated_tree_sha: live_int_tree,
        source_head_commit: post_source_head,
        intent: intent.clone(),
        receipts,
        reused_existing_receipts: true,
    })
}

fn try_recover_completed_receipts_for_intent(
    intent: &mut JobIntegrationIntent,
    preflight: &VerifiedJobIntegrationPreFlight,
    recovered_at_ms: i64,
) -> Result<Option<JobIntegrationReport>, OperationsError> {
    let live_int_head = resolve_exact_commit_sha(
        &preflight.integration_checkout,
        "HEAD",
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )?;
    if live_int_head == intent.initial_integration_head_commit {
        return Ok(None);
    }
    let live_int_tree = resolve_exact_tree_sha(
        &preflight.integration_checkout,
        "HEAD",
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )?;

    let Ok(receipts) =
        load_and_verify_intent_receipt_chain(intent, preflight, &live_int_head, &live_int_tree)
    else {
        return Ok(None);
    };

    intent.state = JobIntegrationIntentState::Completed;
    intent.final_integrated_target_commit = Some(live_int_head.clone());
    intent.final_integrated_tree_sha = Some(live_int_tree.clone());
    intent.uncertainty_reason = None;
    intent.updated_at_ms = recovered_at_ms;
    intent.intent_digest = intent.compute_digest()?;
    intent.verify_integrity()?;
    write_job_integration_intent_unlocked(&preflight.canonical_work_area, intent)?;

    Ok(Some(JobIntegrationReport {
        job_scope: intent.job_scope.clone(),
        integration_worktree_id: preflight.integration_manifest.worktree_id.clone(),
        integration_checkout_path: preflight.integration_checkout.clone(),
        integration_branch_name: preflight.integration_manifest.branch_name.clone(),
        repository_path: preflight.canonical_repo.clone(),
        work_area_root: preflight.canonical_work_area.clone(),
        approved_base_commit: preflight.approved_base_commit.clone(),
        initial_integration_head_commit: intent.initial_integration_head_commit.clone(),
        final_integrated_target_commit: live_int_head,
        final_integrated_tree_sha: live_int_tree,
        source_head_commit: preflight.expected_source_head.clone(),
        intent: intent.clone(),
        receipts,
        reused_existing_receipts: true,
    }))
}

fn load_and_verify_intent_receipt_chain(
    intent: &JobIntegrationIntent,
    preflight: &VerifiedJobIntegrationPreFlight,
    expected_final_commit: &str,
    expected_final_tree: &str,
) -> Result<Vec<IntegrationReceipt>, OperationsError> {
    let mut expected_prev_head = intent.initial_integration_head_commit.clone();
    let mut receipts = Vec::with_capacity(intent.tasks.len());

    for (planned, vt) in intent.tasks.iter().zip(preflight.verified_tasks.iter()) {
        let receipt =
            load_integration_receipt(&preflight.canonical_work_area, &planned.planned_receipt_id)
                .map_err(|err| {
                OperationsError::ReceiptInvalidated(format!(
                    "failed to load persisted integration receipt `{}` for task `{}`: {err}",
                    planned.planned_receipt_id, planned.task_id
                ))
            })?;
        receipt.verify_against_job_scope(
            &vt.subject,
            &vt.review_receipt,
            &intent.job_scope,
            &vt.task_id,
            &vt.task_digest,
        )?;
        let Some(ref agg) = receipt.job_aggregation else {
            return Err(OperationsError::ReceiptInvalidated(format!(
                "persisted integration receipt `{}` is missing job_aggregation provenance",
                receipt.receipt_id
            )));
        };
        if agg.approved_base_commit != preflight.approved_base_commit
            || agg.dependency_seal_digest != vt.dependency_seal_digest
            || agg.dependency_snapshot_ref != vt.dependency_snapshot_ref
        {
            return Err(OperationsError::ReceiptInvalidated(format!(
                "persisted integration receipt `{}` job_aggregation base or dependency seal provenance does not match planned task `{}`",
                receipt.receipt_id, vt.task_id
            )));
        }

        if receipt.integration_worktree_id != preflight.integration_manifest.worktree_id
            || receipt.integration_checkout_path != preflight.integration_checkout
            || receipt.integration_branch_name != preflight.integration_manifest.branch_name
            || receipt.previous_integration_head_commit != expected_prev_head
            || receipt.source_head_commit != preflight.expected_source_head
            || receipt.strategy != vt.strategy
        {
            return Err(OperationsError::ReceiptInvalidated(format!(
                "persisted integration receipt `{}` does not match expected integration chain for task `{}`",
                receipt.receipt_id, vt.task_id
            )));
        }

        if !is_commit_ancestor(
            &preflight.integration_checkout,
            &vt.expected_task_head_commit,
            &receipt.integrated_target_commit,
            DEFAULT_GIT_COMMAND_TIMEOUT_MS,
        )? {
            return Err(OperationsError::AncestryMismatch(format!(
                "task `{}` commit `{}` is not an ancestor of receipt target commit `{}`",
                vt.task_id, vt.expected_task_head_commit, receipt.integrated_target_commit
            )));
        }
        if !is_commit_ancestor(
            &preflight.integration_checkout,
            &receipt.integrated_target_commit,
            expected_final_commit,
            DEFAULT_GIT_COMMAND_TIMEOUT_MS,
        )? {
            return Err(OperationsError::AncestryMismatch(format!(
                "receipt target commit `{}` is not an ancestor of live integration HEAD `{expected_final_commit}`",
                receipt.integrated_target_commit
            )));
        }

        expected_prev_head = receipt.integrated_target_commit.clone();
        receipts.push(receipt);
    }

    let Some(last_receipt) = receipts.last() else {
        return Err(OperationsError::ReceiptInvalidated(
            "job integration intent has no receipts".to_string(),
        ));
    };
    if last_receipt.integrated_target_commit != expected_final_commit
        || last_receipt.integrated_tree_sha != expected_final_tree
    {
        return Err(OperationsError::ReceiptInvalidated(format!(
            "final integration receipt `{}` (`{}`, tree `{}`) does not match live integration HEAD (`{expected_final_commit}`, tree `{expected_final_tree}`)",
            last_receipt.receipt_id,
            last_receipt.integrated_target_commit,
            last_receipt.integrated_tree_sha
        )));
    }

    Ok(receipts)
}

fn build_job_integration_git_argv(
    strategy: IntegrationStrategy,
    job_id: &str,
    target_integration_run_id: &str,
    original_task_run_id: &str,
    task_id: &str,
    task_head_commit: &str,
    review_receipt_id: &str,
) -> Vec<String> {
    match strategy {
        IntegrationStrategy::FastForwardOnly => vec![
            "merge".to_string(),
            "--ff-only".to_string(),
            "--".to_string(),
            task_head_commit.to_string(),
        ],
        IntegrationStrategy::MergeNoFf => {
            let msg = format!(
                "Integrate Threadlane job {job_id} target {target_integration_run_id} task {original_task_run_id}/{task_id} ({task_head_commit}, receipt {review_receipt_id})"
            );
            vec![
                "-c".to_string(),
                "user.name=Threadlane Operator".to_string(),
                "-c".to_string(),
                "user.email=operator@threadlane.local".to_string(),
                "-c".to_string(),
                "commit.gpgsign=false".to_string(),
                "merge".to_string(),
                "--no-ff".to_string(),
                "--no-edit".to_string(),
                "-m".to_string(),
                msg,
                "--".to_string(),
                task_head_commit.to_string(),
            ]
        }
    }
}

fn job_integrations_dir(canonical_work_area: &Path) -> PathBuf {
    canonical_work_area
        .join(MANIFESTS_SUBDIR)
        .join(JOB_INTEGRATIONS_SUBDIR)
}

fn job_integration_intent_filename(integration_worktree_id: &str) -> String {
    format!("job-integration-intent-{integration_worktree_id}.json")
}

fn job_integration_lock_path(canonical_work_area: &Path, integration_worktree_id: &str) -> PathBuf {
    job_integrations_dir(canonical_work_area)
        .join(format!("job-integration-{integration_worktree_id}.lock"))
}

fn load_job_integration_intent_unlocked(
    canonical_work_area: &Path,
    integration_worktree_id: &str,
) -> Result<Option<JobIntegrationIntent>, OperationsError> {
    let dir = job_integrations_dir(canonical_work_area);
    let path = dir.join(job_integration_intent_filename(integration_worktree_id));
    let _ = validate_absolute_no_symlink_components(&path, "job integration intent")?;
    match fs::symlink_metadata(&path) {
        Ok(meta) => {
            if meta.file_type().is_symlink() {
                return Err(OperationsError::SecurityViolation(format!(
                    "job integration intent `{}` must not be a symlink",
                    path.display()
                )));
            }
            if !meta.file_type().is_file() {
                return Err(OperationsError::SecurityViolation(format!(
                    "job integration intent `{}` must be a regular file",
                    path.display()
                )));
            }
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(OperationsError::Io(err)),
    }

    let intent: JobIntegrationIntent =
        read_private_json(&path, MAX_RECEIPT_BYTES, "job integration intent")?;
    if intent.integration_worktree_id != integration_worktree_id
        || intent.work_area_root != canonical_work_area
    {
        return Err(OperationsError::ReceiptInvalidated(
            "loaded job integration intent worktree_id or work_area_root mismatch".to_string(),
        ));
    }
    intent.verify_integrity()?;
    Ok(Some(intent))
}

fn write_job_integration_intent_unlocked(
    canonical_work_area: &Path,
    intent: &JobIntegrationIntent,
) -> Result<(), OperationsError> {
    let dir = job_integrations_dir(canonical_work_area);
    let filename = job_integration_intent_filename(&intent.integration_worktree_id);
    let _ = write_private_json_atomic(&dir, &filename, intent, "job integration intent")?;
    Ok(())
}

fn ensure_review_receipt_not_superseded(
    canonical_work_area: &Path,
    review_receipt: &ReviewReceipt,
) -> Result<(), OperationsError> {
    let receipts_dir = canonical_work_area
        .join(MANIFESTS_SUBDIR)
        .join(RECEIPTS_SUBDIR);
    let entries = match fs::read_dir(&receipts_dir) {
        Ok(entries) => entries,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(err) => return Err(OperationsError::Io(err)),
    };
    for entry in entries {
        let entry = entry.map_err(OperationsError::Io)?;
        let file_name = entry.file_name();
        let Some(name) = file_name.to_str() else {
            return Err(OperationsError::SecurityViolation(
                "non-UTF-8 entry in manifests/receipts".to_string(),
            ));
        };
        if name.starts_with('.') {
            continue;
        }
        if let Some(other_rev_id) = name
            .strip_prefix("review-")
            .and_then(|rest| rest.strip_suffix(".json"))
        {
            let other = load_review_receipt(canonical_work_area, other_rev_id)?;
            if other.task_worktree_id == review_receipt.task_worktree_id
                && other.receipt_id != review_receipt.receipt_id
                && other.approved_at_ms >= review_receipt.approved_at_ms
            {
                return Err(OperationsError::ReceiptInvalidated(format!(
                    "review receipt `{}` for worktree `{}` has been superseded by `{}`",
                    review_receipt.receipt_id, review_receipt.task_worktree_id, other.receipt_id
                )));
            }
        }
    }
    Ok(())
}

fn verify_work_area_marker(
    canonical_work_area: &Path,
    canonical_repo: &Path,
    repo_common_git_dir: &Path,
) -> Result<(), OperationsError> {
    if canonical_work_area.starts_with(canonical_repo)
        || canonical_repo.starts_with(canonical_work_area)
        || canonical_work_area.starts_with(repo_common_git_dir)
        || repo_common_git_dir.starts_with(canonical_work_area)
    {
        return Err(OperationsError::WorktreeNotOwned(
            "work_area_root and repository_path / repository_common_git_dir must not overlap"
                .to_string(),
        ));
    }
    let marker_path = canonical_work_area.join(WORK_AREA_MARKER_FILENAME);
    let marker: WorkAreaOwnershipMarker =
        read_private_json(&marker_path, 16_384, "work area ownership marker").map_err(|err| {
            OperationsError::WorktreeNotOwned(format!(
                "failed to read work area ownership marker: {err}"
            ))
        })?;
    if marker.marker_version != WORK_AREA_MARKER_VERSION {
        return Err(OperationsError::WorktreeNotOwned(format!(
            "unsupported work area marker version {} (expected {WORK_AREA_MARKER_VERSION})",
            marker.marker_version
        )));
    }
    if marker.work_area_root != canonical_work_area
        || marker.repository_common_git_dir != repo_common_git_dir
    {
        return Err(OperationsError::WorktreeNotOwned(
            "work_area_root ownership marker does not match work_area_root and repository_common_git_dir"
                .to_string(),
        ));
    }
    Ok(())
}

fn resolve_repo_common_git_dir(checkout_or_repo: &Path) -> Result<PathBuf, OperationsError> {
    let raw = run_sanitized_git(
        checkout_or_repo,
        &["rev-parse", "--git-common-dir"],
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )?;
    let trimmed = raw.trim();
    let candidate = Path::new(trimmed);
    let full = if candidate.is_absolute() {
        candidate.to_path_buf()
    } else {
        checkout_or_repo.join(candidate)
    };
    Ok(validate_existing_canonical_dir(
        &full,
        "repository git-common-dir",
    )?)
}

fn capture_repo_porcelain_status(repo_or_checkout: &Path) -> Result<Vec<u8>, OperationsError> {
    let out = run_sanitized_git_bytes(
        repo_or_checkout,
        &[
            "status",
            "--porcelain=v1",
            "-z",
            "--untracked-files=all",
            "--ignored=matching",
        ],
        DEFAULT_MAX_GIT_OUTPUT_BYTES,
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )?;
    Ok(out.stdout_bytes)
}
