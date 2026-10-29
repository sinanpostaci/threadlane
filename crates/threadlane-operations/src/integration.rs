use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use threadlane_workers::{
    begin_worktree_job, cleanup_integrated_worktree, evaluate_diff_gate, is_commit_ancestor,
    is_worktree_clean, load_and_verify_manifest, read_private_json, resolve_exact_commit_sha,
    resolve_exact_tree_sha, run_sanitized_git, run_sanitized_git_allow_nonzero,
    validate_commit_ref, validate_exact_40_hex_sha, validate_existing_canonical_dir,
    validate_repo_relative_path, validate_sha256_hex_digest, validate_simple_identifier,
    write_private_json_atomic, IntegratedCleanupReport, IntegratedCleanupRequest, WorktreeKind,
    WorktreeManifest, DEFAULT_GIT_COMMAND_TIMEOUT_MS, DEFAULT_MAX_GIT_OUTPUT_BYTES,
    MANIFESTS_SUBDIR,
};
use uuid::Uuid;

use crate::error::OperationsError;
use crate::review::{
    ensure_worktree_inactive, load_and_verify_task_manifest, load_review_receipt, sha256_hex,
    verify_review_receipt, verify_worktree_branch, ReviewReceipt, ReviewSubject, MAX_RECEIPT_BYTES,
    RECEIPTS_SUBDIR,
};

/// Schema version for [`IntegrationReceipt`].
pub const INTEGRATION_RECEIPT_SCHEMA_VERSION: u32 = 1;

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
    /// Canonical 64-hex SHA-256 digest of this receipt.
    pub receipt_digest: String,
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
}

impl IntegrationReceipt {
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
