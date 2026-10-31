use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use threadlane_workers::{
    begin_worktree_job, ensure_worktree_job_inactive_readonly, evaluate_diff_gate,
    is_commit_ancestor, is_process_alive, is_worktree_clean, load_and_verify_manifest,
    open_private_lock_file, read_private_json, resolve_exact_commit_sha, resolve_exact_tree_sha,
    run_sanitized_git, run_sanitized_git_bytes, sanitize_and_redact, validate_exact_40_hex_sha,
    validate_existing_canonical_dir, validate_repo_relative_path, validate_sha256_hex_digest,
    validate_simple_identifier, write_private_json_atomic, ChangedPathEntry, CleanupRefusalReason,
    WorkerError, WorkerFilePermissions, WorkerRunStatus, WorktreeKind, WorktreeManifest,
    DEFAULT_GIT_COMMAND_TIMEOUT_MS, DEFAULT_MAX_GIT_OUTPUT_BYTES, MANIFESTS_SUBDIR,
};
use uuid::Uuid;

use crate::error::OperationsError;

/// Schema version for [`ReviewSubject`].
pub const REVIEW_SUBJECT_SCHEMA_VERSION: u32 = 1;

/// Schema version for [`ReviewReceipt`].
pub const REVIEW_RECEIPT_SCHEMA_VERSION: u32 = 1;

/// Subdirectory inside `<work_area_root>/manifests` storing private (`0600`) operation receipts.
pub const RECEIPTS_SUBDIR: &str = "receipts";

/// Maximum byte size when reading a persisted receipt file (64 KiB).
pub const MAX_RECEIPT_BYTES: u64 = 65_536;

/// Maximum byte length for review notes and evidence summaries.
pub const MAX_REVIEW_NOTE_BYTES: usize = 2_048;

/// Policy and model/effort telemetry binding included in a [`ReviewSubject`].
///
/// Any modification to this policy structure changes [`ReviewSubject::subject_digest`]
/// and invalidates any previously issued [`ReviewReceipt`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReviewPolicyBinding {
    /// Policy or profile identifier governing this task.
    pub policy_id: String,
    /// Requested provider identifier (e.g. `"antigravity"` or `"opencode_v2"`), if applicable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requested_provider: Option<String>,
    /// Requested model identifier, if applicable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requested_model: Option<String>,
    /// Requested reasoning effort or variant identifier, if applicable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requested_effort: Option<String>,
    /// Observed model identifier verified from worker stream telemetry, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_model: Option<String>,
    /// Observed effort or variant verified from worker stream telemetry, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_effort: Option<String>,
    /// Provider file permissions mode used for the task.
    #[serde(default)]
    pub file_permissions: WorkerFilePermissions,
    /// Whether verified effort evidence matching `requested_effort` is mandatory for acceptance.
    pub require_effort_evidence: bool,
    /// Whether passing formatting, Clippy, and behavioral test evidence is mandatory for acceptance.
    pub require_passing_tests: bool,
}

impl ReviewPolicyBinding {
    /// Validates all identifiers in the policy binding and enforces policy compliance
    /// when `enforce_satisfaction` is `true`.
    pub fn validate(&self, enforce_satisfaction: bool) -> Result<(), OperationsError> {
        validate_simple_identifier(&self.policy_id, "policy_id")?;
        if let Some(ref prov) = self.requested_provider {
            validate_simple_identifier(prov, "requested_provider")?;
        }
        if let Some(ref req_model) = self.requested_model {
            validate_model_like_identifier(req_model, "requested_model")?;
        }
        if let Some(ref req_effort) = self.requested_effort {
            validate_simple_identifier(req_effort, "requested_effort")?;
        }
        if let Some(ref obs_model) = self.observed_model {
            validate_model_like_identifier(obs_model, "observed_model")?;
        }
        if let Some(ref obs_effort) = self.observed_effort {
            validate_simple_identifier(obs_effort, "observed_effort")?;
        }

        if enforce_satisfaction {
            if let Some(ref req_model) = self.requested_model {
                match self.observed_model.as_deref() {
                    Some(obs) if obs == req_model => {}
                    Some(obs) => {
                        return Err(OperationsError::UncertainWorkerOutcome(format!(
                            "observed_model `{obs}` does not match requested_model `{req_model}`"
                        )));
                    }
                    None => {
                        return Err(OperationsError::UncertainWorkerOutcome(format!(
                            "missing observed_model evidence for requested_model `{req_model}`"
                        )));
                    }
                }
            }
            if self.require_effort_evidence {
                if let Some(ref req_effort) = self.requested_effort {
                    match self.observed_effort.as_deref() {
                        Some(obs) if obs == req_effort => {}
                        Some(obs) => {
                            return Err(OperationsError::UncertainWorkerOutcome(format!(
                                "observed_effort `{obs}` does not match requested_effort `{req_effort}`"
                            )));
                        }
                        None => {
                            return Err(OperationsError::UncertainWorkerOutcome(format!(
                                "missing observed_effort evidence for requested_effort `{req_effort}` while require_effort_evidence is true"
                            )));
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

/// Structured summary of formatting, Clippy, and behavioral test verification bound to a [`ReviewSubject`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TestEvidenceSummary {
    /// Whether `cargo fmt --check` (or equivalent formatting check) passed.
    pub formatting_passed: bool,
    /// Whether `cargo clippy -- -D warnings` (or equivalent linter check) passed.
    pub clippy_passed: bool,
    /// Whether the behavioral regression test suite passed.
    pub tests_passed: bool,
    /// Number of passing tests recorded.
    pub passed_test_count: u32,
    /// Number of failing tests recorded.
    pub failed_test_count: u32,
    /// Sanitized, bounded summary of the verification evidence.
    pub evidence_summary: String,
}

impl TestEvidenceSummary {
    /// Returns `true` if formatting, Clippy, and tests all passed with zero failures.
    pub const fn all_checks_passed(&self) -> bool {
        self.formatting_passed
            && self.clippy_passed
            && self.tests_passed
            && self.failed_test_count == 0
    }

    /// Validates the summary fields and enforces passing status when `require_passing` is `true`.
    pub fn validate(&self, require_passing: bool) -> Result<(), OperationsError> {
        if self.evidence_summary.trim().is_empty()
            || self.evidence_summary.len() > MAX_REVIEW_NOTE_BYTES
        {
            return Err(OperationsError::InvalidInput(format!(
                "test_evidence.evidence_summary must be non-empty and at most {MAX_REVIEW_NOTE_BYTES} bytes"
            )));
        }
        if self.failed_test_count > 0 && self.tests_passed {
            return Err(OperationsError::InvalidInput(
                "test_evidence cannot report `tests_passed = true` when `failed_test_count > 0`"
                    .to_string(),
            ));
        }
        if require_passing && !self.all_checks_passed() {
            return Err(OperationsError::UncertainWorkerOutcome(
                "policy requires passing formatting, Clippy, and test evidence with zero failed tests"
                    .to_string(),
            ));
        }
        Ok(())
    }
}

/// Explicit operator request to stage and commit validated in-scope changes inside an owned task worktree.
///
/// Never triggered automatically from worker process exit or model prose.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommitTaskChangesRequest {
    /// Trusted private work-area root directory.
    pub work_area_root: PathBuf,
    /// Owned task worktree identifier (`WorktreeKind::Task`).
    pub task_worktree_id: String,
    /// Commit message for the task branch commit.
    pub commit_message: String,
    /// Optional worker execution status to verify before committing (rejects uncertain/failed statuses).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub worker_status: Option<WorkerRunStatus>,
    /// Whether `MissingEffortEvidence` is permitted when `worker_status` is provided.
    #[serde(default)]
    pub allow_missing_effort_evidence: bool,
}

/// Report returned when [`commit_task_worktree_changes`] creates a commit on an owned task branch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskCommitReport {
    /// Owned task worktree identifier.
    pub task_worktree_id: String,
    /// Task branch name.
    pub branch_name: String,
    /// Base commit SHA from which the task worktree was created.
    pub base_commit: String,
    /// New `HEAD` 40-hex commit SHA on the task branch.
    pub task_head_commit: String,
    /// New `HEAD^{tree}` 40-hex tree SHA on the task branch.
    pub task_tree_sha: String,
    /// Number of validated changed paths included in the commit.
    pub changed_paths_count: usize,
}

/// Request to inspect an owned task worktree and construct a digest-bound [`ReviewSubject`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InspectReviewSubjectRequest {
    /// Trusted private work-area root directory.
    pub work_area_root: PathBuf,
    /// Owned task worktree identifier (`WorktreeKind::Task`).
    pub task_worktree_id: String,
    /// Policy and telemetry binding for this review subject.
    pub policy: ReviewPolicyBinding,
    /// Formatting, Clippy, and behavioral test evidence for this review subject.
    pub test_evidence: TestEvidenceSummary,
    /// Optional worker terminal status to gate inspection (rejects failed/timed-out/cancelled/unknown runs).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub worker_status: Option<WorkerRunStatus>,
}

/// Immutable review subject binding repository, base commit, task head commit, tree and diff
/// digests, allowed write scope, policy/telemetry, and test evidence into a single SHA-256
/// `subject_digest`.
///
/// Any change to `base_commit`, `task_head_commit`, `task_tree_sha`, `diff_digest`,
/// `allowed_write_paths`, `policy`, or `test_evidence` produces a different `subject_digest`
/// and invalidates any previously issued [`ReviewReceipt`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReviewSubject {
    /// Schema version (`1`).
    pub schema_version: u32,
    /// Run identifier from the owned worktree manifest.
    pub run_id: String,
    /// Task identifier from the owned worktree manifest.
    pub task_id: String,
    /// Owned task worktree identifier.
    pub task_worktree_id: String,
    /// Canonical primary source repository path.
    pub repository_path: PathBuf,
    /// Canonical common Git directory of the repository.
    pub repository_common_git_dir: PathBuf,
    /// Canonical private work-area root path.
    pub work_area_root: PathBuf,
    /// Canonical task checkout directory path inside `<work_area_root>/checkouts/tasks/`.
    pub task_checkout_path: PathBuf,
    /// Software branch name checked out in `task_checkout_path` (`threadlane/task/...`).
    pub task_branch_name: String,
    /// Exact 40-character hexadecimal base commit SHA.
    pub base_commit: String,
    /// Exact 40-character hexadecimal task `HEAD` commit SHA.
    pub task_head_commit: String,
    /// Exact 40-character hexadecimal `HEAD^{tree}` SHA of the task commit.
    pub task_tree_sha: String,
    /// Canonical 64-character SHA-256 hex digest of the changed paths and binary Git diff (`base_commit..task_head_commit`).
    pub diff_digest: String,
    /// Allowed repository-relative write scope bound to this task.
    pub allowed_write_paths: BTreeSet<String>,
    /// Bounded changed path entries verified by the diff gate.
    pub changed_paths: Vec<ChangedPathEntry>,
    /// Policy and requested/observed model/effort telemetry binding.
    pub policy: ReviewPolicyBinding,
    /// Formatting, Clippy, and behavioral test verification evidence.
    pub test_evidence: TestEvidenceSummary,
    /// Canonical 64-character SHA-256 hex digest over all bound fields of this review subject.
    pub subject_digest: String,
}

#[derive(Serialize)]
struct ReviewSubjectDigestPayload<'a> {
    schema_version: u32,
    run_id: &'a str,
    task_id: &'a str,
    task_worktree_id: &'a str,
    repository_path: &'a Path,
    repository_common_git_dir: &'a Path,
    work_area_root: &'a Path,
    task_checkout_path: &'a Path,
    task_branch_name: &'a str,
    base_commit: &'a str,
    task_head_commit: &'a str,
    task_tree_sha: &'a str,
    diff_digest: &'a str,
    allowed_write_paths: &'a BTreeSet<String>,
    changed_paths: &'a [ChangedPathEntry],
    policy: &'a ReviewPolicyBinding,
    test_evidence: &'a TestEvidenceSummary,
}

impl ReviewSubject {
    /// Computes the canonical 64-character lowercase hexadecimal SHA-256 digest of this subject's fields.
    pub fn compute_digest(&self) -> Result<String, OperationsError> {
        let payload = ReviewSubjectDigestPayload {
            schema_version: self.schema_version,
            run_id: &self.run_id,
            task_id: &self.task_id,
            task_worktree_id: &self.task_worktree_id,
            repository_path: &self.repository_path,
            repository_common_git_dir: &self.repository_common_git_dir,
            work_area_root: &self.work_area_root,
            task_checkout_path: &self.task_checkout_path,
            task_branch_name: &self.task_branch_name,
            base_commit: &self.base_commit,
            task_head_commit: &self.task_head_commit,
            task_tree_sha: &self.task_tree_sha,
            diff_digest: &self.diff_digest,
            allowed_write_paths: &self.allowed_write_paths,
            changed_paths: &self.changed_paths,
            policy: &self.policy,
            test_evidence: &self.test_evidence,
        };
        let encoded = serde_json::to_vec(&payload)?;
        Ok(sha256_hex(&encoded))
    }

    /// Validates all fields and verifies that `self.subject_digest` matches [`Self::compute_digest`].
    pub fn verify_integrity(&self) -> Result<(), OperationsError> {
        if self.schema_version != REVIEW_SUBJECT_SCHEMA_VERSION {
            return Err(OperationsError::SubjectMismatch(format!(
                "unsupported review subject schema_version {} (expected {REVIEW_SUBJECT_SCHEMA_VERSION})",
                self.schema_version
            )));
        }
        validate_simple_identifier(&self.run_id, "run_id")?;
        validate_simple_identifier(&self.task_id, "task_id")?;
        validate_simple_identifier(&self.task_worktree_id, "task_worktree_id")?;
        let base = validate_exact_40_hex_sha(&self.base_commit, "base_commit")?;
        let head = validate_exact_40_hex_sha(&self.task_head_commit, "task_head_commit")?;
        let tree = validate_exact_tree_or_commit_format(&self.task_tree_sha, "task_tree_sha")?;
        let diff_dig = validate_sha256_hex_digest(&self.diff_digest, "diff_digest")?;
        let subj_dig = validate_sha256_hex_digest(&self.subject_digest, "subject_digest")?;

        if base == head {
            return Err(OperationsError::SubjectMismatch(
                "review subject task_head_commit must advance beyond base_commit".to_string(),
            ));
        }
        if self.base_commit != base
            || self.task_head_commit != head
            || self.task_tree_sha != tree
            || self.diff_digest != diff_dig
            || self.subject_digest != subj_dig
        {
            return Err(OperationsError::SubjectMismatch(
                "review subject SHAs and digests must be normalized lowercase hex".to_string(),
            ));
        }
        if !self.task_branch_name.starts_with("threadlane/task/") {
            return Err(OperationsError::SubjectMismatch(format!(
                "task_branch_name `{}` must carry `threadlane/task/` prefix",
                self.task_branch_name
            )));
        }
        if self.allowed_write_paths.is_empty() {
            return Err(OperationsError::SubjectMismatch(
                "allowed_write_paths must not be empty".to_string(),
            ));
        }
        for path in &self.allowed_write_paths {
            validate_repo_relative_path(path, "allowed_write_paths entry")?;
        }
        if self.changed_paths.is_empty() {
            return Err(OperationsError::SubjectMismatch(
                "changed_paths must not be empty for a reviewable subject".to_string(),
            ));
        }
        self.policy.validate(false)?;
        self.test_evidence.validate(false)?;

        let computed = self.compute_digest()?;
        if computed != self.subject_digest {
            return Err(OperationsError::SubjectMismatch(format!(
                "review subject digest mismatch: expected `{}`, computed `{computed}`",
                self.subject_digest
            )));
        }
        Ok(())
    }
}

/// Explicit operator request to accept a validated [`ReviewSubject`] and issue a frozen [`ReviewReceipt`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReviewAcceptanceRequest {
    /// Review subject to approve.
    pub subject: ReviewSubject,
    /// Expected 64-character hex `subject_digest` confirmed by the reviewer.
    pub expected_subject_digest: String,
    /// Optional explicit receipt ID (`[A-Za-z0-9._-]`); generated automatically if `None`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub receipt_id: Option<String>,
    /// Reviewer identifier (e.g. `"primary-codex"` or `"gpt-6-astra"`).
    pub reviewer_id: String,
    /// Bounded human-readable review disposition note.
    pub review_note: String,
    /// Wall-clock timestamp (milliseconds since Unix epoch) recorded on the receipt.
    pub approved_at_ms: i64,
}

/// Frozen operator approval receipt bound to a specific [`ReviewSubject`].
///
/// Any change to the repository, `base_commit`, `task_head_commit`, `task_tree_sha`,
/// `diff_digest`, `allowed_write_paths`, `policy`, or `test_evidence` invalidates this receipt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReviewReceipt {
    /// Schema version (`1`).
    pub schema_version: u32,
    /// Unique receipt identifier.
    pub receipt_id: String,
    /// Run identifier.
    pub run_id: String,
    /// Task identifier.
    pub task_id: String,
    /// Owned task worktree identifier.
    pub task_worktree_id: String,
    /// Canonical primary source repository path.
    pub repository_path: PathBuf,
    /// Canonical private work-area root path.
    pub work_area_root: PathBuf,
    /// Approved 40-hex base commit SHA.
    pub base_commit: String,
    /// Approved 40-hex task `HEAD` commit SHA.
    pub task_head_commit: String,
    /// Approved 40-hex `HEAD^{tree}` SHA.
    pub task_tree_sha: String,
    /// Approved 64-hex diff digest.
    pub diff_digest: String,
    /// Approved repository-relative write scope.
    pub allowed_write_paths: BTreeSet<String>,
    /// Approved policy and telemetry binding.
    pub policy: ReviewPolicyBinding,
    /// Approved test evidence summary.
    pub test_evidence: TestEvidenceSummary,
    /// Bound 64-hex `ReviewSubject::subject_digest`.
    pub subject_digest: String,
    /// Reviewer identifier.
    pub reviewer_id: String,
    /// Sanitized review note.
    pub review_note: String,
    /// Approval timestamp in milliseconds since Unix epoch.
    pub approved_at_ms: i64,
    /// Canonical 64-hex SHA-256 digest of this receipt.
    pub receipt_digest: String,
}

#[derive(Serialize)]
struct ReviewReceiptDigestPayload<'a> {
    schema_version: u32,
    receipt_id: &'a str,
    run_id: &'a str,
    task_id: &'a str,
    task_worktree_id: &'a str,
    repository_path: &'a Path,
    work_area_root: &'a Path,
    base_commit: &'a str,
    task_head_commit: &'a str,
    task_tree_sha: &'a str,
    diff_digest: &'a str,
    allowed_write_paths: &'a BTreeSet<String>,
    policy: &'a ReviewPolicyBinding,
    test_evidence: &'a TestEvidenceSummary,
    subject_digest: &'a str,
    reviewer_id: &'a str,
    review_note: &'a str,
    approved_at_ms: i64,
}

impl ReviewReceipt {
    /// Computes the canonical 64-character lowercase hexadecimal SHA-256 digest of this receipt.
    pub fn compute_digest(&self) -> Result<String, OperationsError> {
        let payload = ReviewReceiptDigestPayload {
            schema_version: self.schema_version,
            receipt_id: &self.receipt_id,
            run_id: &self.run_id,
            task_id: &self.task_id,
            task_worktree_id: &self.task_worktree_id,
            repository_path: &self.repository_path,
            work_area_root: &self.work_area_root,
            base_commit: &self.base_commit,
            task_head_commit: &self.task_head_commit,
            task_tree_sha: &self.task_tree_sha,
            diff_digest: &self.diff_digest,
            allowed_write_paths: &self.allowed_write_paths,
            policy: &self.policy,
            test_evidence: &self.test_evidence,
            subject_digest: &self.subject_digest,
            reviewer_id: &self.reviewer_id,
            review_note: &self.review_note,
            approved_at_ms: self.approved_at_ms,
        };
        let encoded = serde_json::to_vec(&payload)?;
        Ok(sha256_hex(&encoded))
    }

    /// Validates all receipt fields and verifies that `self.receipt_digest` matches [`Self::compute_digest`].
    pub fn verify_integrity(&self) -> Result<(), OperationsError> {
        if self.schema_version != REVIEW_RECEIPT_SCHEMA_VERSION {
            return Err(OperationsError::ReceiptInvalidated(format!(
                "unsupported review receipt schema_version {} (expected {REVIEW_RECEIPT_SCHEMA_VERSION})",
                self.schema_version
            )));
        }
        validate_simple_identifier(&self.receipt_id, "receipt_id")?;
        validate_simple_identifier(&self.run_id, "run_id")?;
        validate_simple_identifier(&self.task_id, "task_id")?;
        validate_simple_identifier(&self.task_worktree_id, "task_worktree_id")?;
        validate_simple_identifier(&self.reviewer_id, "reviewer_id")?;
        let _ = validate_exact_40_hex_sha(&self.base_commit, "base_commit")?;
        let _ = validate_exact_40_hex_sha(&self.task_head_commit, "task_head_commit")?;
        let _ = validate_exact_tree_or_commit_format(&self.task_tree_sha, "task_tree_sha")?;
        let _ = validate_sha256_hex_digest(&self.diff_digest, "diff_digest")?;
        let _ = validate_sha256_hex_digest(&self.subject_digest, "subject_digest")?;
        let _ = validate_sha256_hex_digest(&self.receipt_digest, "receipt_digest")?;

        if self.review_note.trim().is_empty() || self.review_note.len() > MAX_REVIEW_NOTE_BYTES {
            return Err(OperationsError::ReceiptInvalidated(
                "review_note must be non-empty and within byte bounds".to_string(),
            ));
        }

        self.policy.validate(true).map_err(|err| {
            OperationsError::ReceiptInvalidated(format!("receipt policy unsatisfied: {err}"))
        })?;
        self.test_evidence
            .validate(self.policy.require_passing_tests)
            .map_err(|err| {
                OperationsError::ReceiptInvalidated(format!(
                    "receipt test_evidence unsatisfied: {err}"
                ))
            })?;

        let computed = self.compute_digest()?;
        if computed != self.receipt_digest {
            return Err(OperationsError::ReceiptInvalidated(format!(
                "review receipt digest mismatch: expected `{}`, computed `{computed}`",
                self.receipt_digest
            )));
        }
        Ok(())
    }

    /// Verifies that this receipt cryptographically and structurally matches `subject`
    /// without querying live disk state.
    ///
    /// Any change to `repository_path`, `base_commit`, `task_head_commit`, `task_tree_sha`,
    /// `diff_digest`, `allowed_write_paths`, `policy`, or `test_evidence` returns
    /// [`OperationsError::ReceiptInvalidated`].
    pub fn verify_against_subject(&self, subject: &ReviewSubject) -> Result<(), OperationsError> {
        subject.verify_integrity().map_err(|err| {
            OperationsError::ReceiptInvalidated(format!("review subject integrity failed: {err}"))
        })?;
        self.verify_integrity()?;

        if self.run_id != subject.run_id
            || self.task_id != subject.task_id
            || self.task_worktree_id != subject.task_worktree_id
        {
            return Err(OperationsError::ReceiptInvalidated(
                "review receipt run/task/worktree identity does not match subject".to_string(),
            ));
        }
        if self.repository_path != subject.repository_path
            || self.work_area_root != subject.work_area_root
        {
            return Err(OperationsError::ReceiptInvalidated(
                "review receipt repository_path or work_area_root does not match subject"
                    .to_string(),
            ));
        }
        if self.base_commit != subject.base_commit {
            return Err(OperationsError::ReceiptInvalidated(format!(
                "base_commit changed from `{}` to `{}`",
                self.base_commit, subject.base_commit
            )));
        }
        if self.task_head_commit != subject.task_head_commit {
            return Err(OperationsError::ReceiptInvalidated(format!(
                "task_head_commit changed from `{}` to `{}`",
                self.task_head_commit, subject.task_head_commit
            )));
        }
        if self.task_tree_sha != subject.task_tree_sha {
            return Err(OperationsError::ReceiptInvalidated(format!(
                "task_tree_sha changed from `{}` to `{}`",
                self.task_tree_sha, subject.task_tree_sha
            )));
        }
        if self.diff_digest != subject.diff_digest {
            return Err(OperationsError::ReceiptInvalidated(format!(
                "diff_digest changed from `{}` to `{}`",
                self.diff_digest, subject.diff_digest
            )));
        }
        if self.allowed_write_paths != subject.allowed_write_paths {
            return Err(OperationsError::ReceiptInvalidated(
                "allowed_write_paths scope changed since review receipt was issued".to_string(),
            ));
        }
        if self.policy != subject.policy {
            return Err(OperationsError::ReceiptInvalidated(
                "review policy binding changed since review receipt was issued".to_string(),
            ));
        }
        if self.test_evidence != subject.test_evidence {
            return Err(OperationsError::ReceiptInvalidated(
                "test_evidence changed since review receipt was issued".to_string(),
            ));
        }
        if self.subject_digest != subject.subject_digest {
            return Err(OperationsError::ReceiptInvalidated(format!(
                "receipt subject_digest `{}` does not match subject `{}`",
                self.subject_digest, subject.subject_digest
            )));
        }
        Ok(())
    }
}

/// Validates that an optional [`WorkerRunStatus`] is eligible for review preparation.
///
/// Rejects failed, timed-out, cancelled, blocked, overflowed, or incomplete-cleanup outcomes.
pub fn validate_worker_status_for_review(
    status: WorkerRunStatus,
    allow_missing_effort_evidence: bool,
) -> Result<(), OperationsError> {
    match status {
        WorkerRunStatus::Succeeded => Ok(()),
        WorkerRunStatus::MissingEffortEvidence if allow_missing_effort_evidence => Ok(()),
        other => Err(OperationsError::UncertainWorkerOutcome(format!(
            "worker run status `{}` is not eligible for review acceptance or integration",
            other.as_str()
        ))),
    }
}

/// Explicit operator action to stage and commit validated in-scope changes inside an owned
/// `WorktreeKind::Task` checkout.
///
/// Evaluates the post-run diff gate before and after staging, refuses dirty/violating paths or
/// active jobs, and uses a sanitized `GIT_*` environment with fixed author/gpg arguments.
pub fn commit_task_worktree_changes(
    request: &CommitTaskChangesRequest,
) -> Result<TaskCommitReport, OperationsError> {
    if let Some(status) = request.worker_status {
        validate_worker_status_for_review(status, request.allow_missing_effort_evidence)?;
    }
    let clean_msg = sanitize_and_redact(&request.commit_message, 512);
    if clean_msg.trim().is_empty() {
        return Err(OperationsError::InvalidInput(
            "commit_message must not be empty".to_string(),
        ));
    }

    let manifest =
        load_and_verify_task_manifest(&request.work_area_root, &request.task_worktree_id)?;
    let job_guard = begin_worktree_job(&manifest.work_area_root, &manifest.worktree_id)?;

    let result = (|| -> Result<TaskCommitReport, OperationsError> {
        let canonical_checkout =
            validate_existing_canonical_dir(&manifest.checkout_path, "task checkout_path")?;
        if canonical_checkout != manifest.checkout_path {
            return Err(OperationsError::SecurityViolation(
                "task checkout_path canonicalization mismatch".to_string(),
            ));
        }
        verify_worktree_branch(
            &canonical_checkout,
            &manifest.branch_name,
            &manifest.worktree_id,
        )?;

        // 1. Evaluate diff gate before staging to refuse out-of-scope or symlink/control violations
        // while preserving all dirty artifacts untouched on disk.
        let pre_diff = evaluate_diff_gate(&manifest.work_area_root, &manifest.worktree_id)?;
        if !pre_diff.passed || !pre_diff.evidence_complete {
            return Err(OperationsError::DiffGateRejected {
                worktree_id: manifest.worktree_id.clone(),
                detail: format!(
                    "diff gate failed with {} violation(s) (evidence_complete={})",
                    pre_diff.violations.len(),
                    pre_diff.evidence_complete
                ),
            });
        }
        if pre_diff.changed_paths.is_empty() {
            return Err(OperationsError::InvalidInput(format!(
                "task worktree `{}` has no changes relative to base_commit `{}`",
                manifest.worktree_id, manifest.base_commit
            )));
        }

        if !is_worktree_clean(&canonical_checkout, DEFAULT_GIT_COMMAND_TIMEOUT_MS)? {
            run_sanitized_git(
                &canonical_checkout,
                &["add", "-A", "--", "."],
                DEFAULT_GIT_COMMAND_TIMEOUT_MS,
            )?;

            let post_stage_diff =
                evaluate_diff_gate(&manifest.work_area_root, &manifest.worktree_id)?;
            if !post_stage_diff.passed || !post_stage_diff.evidence_complete {
                return Err(OperationsError::DiffGateRejected {
                    worktree_id: manifest.worktree_id.clone(),
                    detail: "post-staging diff gate validation failed".to_string(),
                });
            }

            run_sanitized_git(
                &canonical_checkout,
                &[
                    "-c",
                    "user.name=Threadlane Operator",
                    "-c",
                    "user.email=operator@threadlane.local",
                    "-c",
                    "commit.gpgsign=false",
                    "commit",
                    "-q",
                    "-m",
                    clean_msg.trim(),
                ],
                DEFAULT_GIT_COMMAND_TIMEOUT_MS,
            )?;
        }

        if !is_worktree_clean(&canonical_checkout, DEFAULT_GIT_COMMAND_TIMEOUT_MS)? {
            return Err(OperationsError::DirtyWorktree {
                worktree_id: manifest.worktree_id.clone(),
                detail: "task worktree still has uncommitted or ignored files after commit"
                    .to_string(),
            });
        }

        let task_head_commit =
            resolve_exact_commit_sha(&canonical_checkout, "HEAD", DEFAULT_GIT_COMMAND_TIMEOUT_MS)?;
        let task_tree_sha =
            resolve_exact_tree_sha(&canonical_checkout, "HEAD", DEFAULT_GIT_COMMAND_TIMEOUT_MS)?;

        Ok(TaskCommitReport {
            task_worktree_id: manifest.worktree_id.clone(),
            branch_name: manifest.branch_name.clone(),
            base_commit: manifest.base_commit.clone(),
            task_head_commit,
            task_tree_sha,
            changed_paths_count: pre_diff.changed_paths.len(),
        })
    })();

    let _ = job_guard.finish()?;
    result
}

/// Inspection of an owned `WorktreeKind::Task` checkout to produce a digest-bound
/// [`ReviewSubject`].
///
/// Requires the task checkout to have no active job, a clean working tree (all task changes
/// committed on `HEAD`), `HEAD != base_commit`, `base_commit` an ancestor of `HEAD`, and a
/// passing post-run diff gate.
pub fn inspect_review_subject(
    request: &InspectReviewSubjectRequest,
) -> Result<ReviewSubject, OperationsError> {
    inspect_review_subject_with_inactive_check(request, ensure_worktree_inactive)
}

/// Strictly read-only inspection of an owned `WorktreeKind::Task` checkout to produce a
/// digest-bound [`ReviewSubject`] without creating a missing worktree `.lock` file or
/// rewriting optional Git index locks.
///
/// Preserves all subject, digest, write-scope, ancestry, active-job, and clean-checkout
/// validations performed by [`inspect_review_subject`], while using
/// [`ensure_worktree_job_inactive_readonly`] so a missing inactive `.lock` file remains absent.
pub fn inspect_review_subject_readonly(
    request: &InspectReviewSubjectRequest,
) -> Result<ReviewSubject, OperationsError> {
    inspect_review_subject_with_inactive_check(request, ensure_worktree_inactive_readonly)
}

fn inspect_review_subject_with_inactive_check<F>(
    request: &InspectReviewSubjectRequest,
    inactive_check: F,
) -> Result<ReviewSubject, OperationsError>
where
    F: FnOnce(&WorktreeManifest) -> Result<(), OperationsError>,
{
    if let Some(status) = request.worker_status {
        validate_worker_status_for_review(status, !request.policy.require_effort_evidence)?;
    }
    request.policy.validate(false)?;
    request.test_evidence.validate(false)?;

    let manifest =
        load_and_verify_task_manifest(&request.work_area_root, &request.task_worktree_id)?;
    inactive_check(&manifest)?;

    let canonical_checkout =
        validate_existing_canonical_dir(&manifest.checkout_path, "task checkout_path")?;
    if canonical_checkout != manifest.checkout_path {
        return Err(OperationsError::SecurityViolation(
            "task checkout_path canonicalization mismatch".to_string(),
        ));
    }
    verify_worktree_branch(
        &canonical_checkout,
        &manifest.branch_name,
        &manifest.worktree_id,
    )?;

    if !is_worktree_clean(&canonical_checkout, DEFAULT_GIT_COMMAND_TIMEOUT_MS)? {
        return Err(OperationsError::DirtyWorktree {
            worktree_id: manifest.worktree_id,
            detail: "task worktree has uncommitted, untracked, or ignored changes; commit or clean before freezing a ReviewSubject".to_string(),
        });
    }

    let diff_gate = evaluate_diff_gate(&manifest.work_area_root, &manifest.worktree_id)?;
    if !diff_gate.passed || !diff_gate.evidence_complete {
        return Err(OperationsError::DiffGateRejected {
            worktree_id: manifest.worktree_id,
            detail: format!(
                "diff gate rejected worktree with {} violation(s) (evidence_complete={})",
                diff_gate.violations.len(),
                diff_gate.evidence_complete
            ),
        });
    }
    if diff_gate.changed_paths.is_empty() {
        return Err(OperationsError::SubjectMismatch(format!(
            "task worktree `{}` has no changed paths relative to base_commit `{}`",
            manifest.worktree_id, manifest.base_commit
        )));
    }

    let task_head_commit =
        resolve_exact_commit_sha(&canonical_checkout, "HEAD", DEFAULT_GIT_COMMAND_TIMEOUT_MS)?;
    if task_head_commit == manifest.base_commit {
        return Err(OperationsError::SubjectMismatch(format!(
            "task worktree `{}` HEAD equals base_commit `{task_head_commit}`",
            manifest.worktree_id
        )));
    }

    let base_is_ancestor = is_commit_ancestor(
        &manifest.repository_path,
        &manifest.base_commit,
        &task_head_commit,
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )?;
    if !base_is_ancestor {
        return Err(OperationsError::AncestryMismatch(format!(
            "base_commit `{}` is not an ancestor of task_head_commit `{task_head_commit}`",
            manifest.base_commit
        )));
    }

    let task_tree_sha =
        resolve_exact_tree_sha(&canonical_checkout, "HEAD", DEFAULT_GIT_COMMAND_TIMEOUT_MS)?;
    let diff_digest = compute_worktree_diff_digest(
        &canonical_checkout,
        &manifest.base_commit,
        &task_head_commit,
        &diff_gate.changed_paths,
    )?;

    let task_id = manifest.task_id.clone().ok_or_else(|| {
        OperationsError::WorktreeNotOwned(format!(
            "task worktree `{}` manifest is missing task_id",
            manifest.worktree_id
        ))
    })?;
    let common_git_dir = manifest
        .repository_common_git_dir
        .clone()
        .unwrap_or_else(|| manifest.repository_path.join(".git"));

    let mut clean_evidence = request.test_evidence.clone();
    clean_evidence.evidence_summary =
        sanitize_and_redact(&clean_evidence.evidence_summary, MAX_REVIEW_NOTE_BYTES);

    let mut subject = ReviewSubject {
        schema_version: REVIEW_SUBJECT_SCHEMA_VERSION,
        run_id: manifest.run_id,
        task_id,
        task_worktree_id: manifest.worktree_id,
        repository_path: manifest.repository_path,
        repository_common_git_dir: common_git_dir,
        work_area_root: manifest.work_area_root,
        task_checkout_path: canonical_checkout,
        task_branch_name: manifest.branch_name,
        base_commit: manifest.base_commit,
        task_head_commit,
        task_tree_sha,
        diff_digest,
        allowed_write_paths: manifest.allowed_write_paths,
        changed_paths: diff_gate.changed_paths,
        policy: request.policy.clone(),
        test_evidence: clean_evidence,
        subject_digest: String::new(),
    };
    subject.subject_digest = subject.compute_digest()?;
    subject.verify_integrity()?;
    Ok(subject)
}

/// Explicit operator entry point to approve a [`ReviewSubject`] and persist a frozen [`ReviewReceipt`].
///
/// Re-verifies the live task worktree on disk (manifest, scope, clean checkout, `HEAD`, `tree`,
/// diff gate, and diff digest) and enforces policy/test requirements before issuing the receipt.
pub fn accept_review_subject(
    request: &ReviewAcceptanceRequest,
) -> Result<ReviewReceipt, OperationsError> {
    request.subject.verify_integrity()?;
    let expected_digest =
        validate_sha256_hex_digest(&request.expected_subject_digest, "expected_subject_digest")?;
    if expected_digest != request.subject.subject_digest {
        return Err(OperationsError::SubjectMismatch(format!(
            "expected_subject_digest `{expected_digest}` does not match subject.subject_digest `{}`",
            request.subject.subject_digest
        )));
    }

    validate_simple_identifier(&request.reviewer_id, "reviewer_id")?;
    let clean_note = sanitize_and_redact(&request.review_note, MAX_REVIEW_NOTE_BYTES);
    if clean_note.trim().is_empty() {
        return Err(OperationsError::InvalidInput(
            "review_note must not be empty".to_string(),
        ));
    }

    // Enforce policy and test evidence requirements at acceptance time.
    request.subject.policy.validate(true)?;
    request
        .subject
        .test_evidence
        .validate(request.subject.policy.require_passing_tests)?;

    // Re-inspect the live task worktree on disk to guarantee nothing changed since subject creation.
    let live_subject = inspect_review_subject(&InspectReviewSubjectRequest {
        work_area_root: request.subject.work_area_root.clone(),
        task_worktree_id: request.subject.task_worktree_id.clone(),
        policy: request.subject.policy.clone(),
        test_evidence: request.subject.test_evidence.clone(),
        worker_status: None,
    })?;
    if live_subject != request.subject {
        return Err(OperationsError::SubjectMismatch(format!(
            "live worktree state (digest `{}`) no longer matches review subject (digest `{}`)",
            live_subject.subject_digest, request.subject.subject_digest
        )));
    }

    let receipt_id = match request.receipt_id {
        Some(ref id) => {
            validate_simple_identifier(id, "receipt_id")?;
            id.clone()
        }
        None => format!("rev_{}", &Uuid::new_v4().simple().to_string()[..16]),
    };

    let mut receipt = ReviewReceipt {
        schema_version: REVIEW_RECEIPT_SCHEMA_VERSION,
        receipt_id: receipt_id.clone(),
        run_id: request.subject.run_id.clone(),
        task_id: request.subject.task_id.clone(),
        task_worktree_id: request.subject.task_worktree_id.clone(),
        repository_path: request.subject.repository_path.clone(),
        work_area_root: request.subject.work_area_root.clone(),
        base_commit: request.subject.base_commit.clone(),
        task_head_commit: request.subject.task_head_commit.clone(),
        task_tree_sha: request.subject.task_tree_sha.clone(),
        diff_digest: request.subject.diff_digest.clone(),
        allowed_write_paths: request.subject.allowed_write_paths.clone(),
        policy: request.subject.policy.clone(),
        test_evidence: request.subject.test_evidence.clone(),
        subject_digest: request.subject.subject_digest.clone(),
        reviewer_id: request.reviewer_id.clone(),
        review_note: clean_note,
        approved_at_ms: request.approved_at_ms,
        receipt_digest: String::new(),
    };
    receipt.receipt_digest = receipt.compute_digest()?;
    receipt.verify_against_subject(&request.subject)?;

    let receipts_dir = request
        .subject
        .work_area_root
        .join(MANIFESTS_SUBDIR)
        .join(RECEIPTS_SUBDIR);
    let filename = format!("review-{receipt_id}.json");
    write_private_json_atomic(&receipts_dir, &filename, &receipt, "review receipt")?;

    Ok(receipt)
}

/// Loads and verifies a persisted [`ReviewReceipt`] from `<work_area_root>/manifests/receipts/review-<receipt_id>.json`.
pub fn load_review_receipt(
    work_area_root: &Path,
    receipt_id: &str,
) -> Result<ReviewReceipt, OperationsError> {
    validate_simple_identifier(receipt_id, "receipt_id")?;
    let canonical_work_area = validate_existing_canonical_dir(work_area_root, "work_area_root")?;
    let receipt_path = canonical_work_area
        .join(MANIFESTS_SUBDIR)
        .join(RECEIPTS_SUBDIR)
        .join(format!("review-{receipt_id}.json"));
    let receipt: ReviewReceipt =
        read_private_json(&receipt_path, MAX_RECEIPT_BYTES, "review receipt")?;
    if receipt.receipt_id != receipt_id || receipt.work_area_root != canonical_work_area {
        return Err(OperationsError::ReceiptInvalidated(
            "loaded review receipt identity or work_area_root mismatch".to_string(),
        ));
    }
    receipt.verify_integrity()?;
    Ok(receipt)
}

/// Verifies that `receipt` matches `subject` AND that the live task worktree on disk still
/// matches the exact `base_commit`, `task_head_commit`, `task_tree_sha`, `diff_digest`, and
/// `allowed_write_paths` bound in the receipt.
pub fn verify_review_receipt(
    subject: &ReviewSubject,
    receipt: &ReviewReceipt,
) -> Result<(), OperationsError> {
    verify_review_receipt_with_inspector(subject, receipt, inspect_review_subject)
}

/// Verifies the receipt and exact live subject without creating job locks or
/// refreshing optional Git index state. Preserves all normal receipt checks.
pub fn verify_review_receipt_readonly(
    subject: &ReviewSubject,
    receipt: &ReviewReceipt,
) -> Result<(), OperationsError> {
    verify_review_receipt_with_inspector(subject, receipt, inspect_review_subject_readonly)
}

fn verify_review_receipt_with_inspector(
    subject: &ReviewSubject,
    receipt: &ReviewReceipt,
    inspector: fn(&InspectReviewSubjectRequest) -> Result<ReviewSubject, OperationsError>,
) -> Result<(), OperationsError> {
    receipt.verify_against_subject(subject)?;

    let live_subject = inspector(&InspectReviewSubjectRequest {
        work_area_root: subject.work_area_root.clone(),
        task_worktree_id: subject.task_worktree_id.clone(),
        policy: subject.policy.clone(),
        test_evidence: subject.test_evidence.clone(),
        worker_status: None,
    })
    .map_err(|err| {
        OperationsError::ReceiptInvalidated(format!(
            "live task worktree no longer satisfies review subject: {err}"
        ))
    })?;

    if live_subject != *subject {
        return Err(OperationsError::ReceiptInvalidated(format!(
            "live task worktree digest `{}` diverged from approved subject digest `{}`",
            live_subject.subject_digest, subject.subject_digest
        )));
    }
    Ok(())
}

pub(crate) fn load_and_verify_task_manifest(
    work_area_root: &Path,
    task_worktree_id: &str,
) -> Result<WorktreeManifest, OperationsError> {
    let manifest = load_and_verify_manifest(work_area_root, task_worktree_id)?;
    if manifest.kind != WorktreeKind::Task {
        return Err(OperationsError::WorktreeNotOwned(format!(
            "worktree `{task_worktree_id}` has kind `{:?}`, expected `Task`",
            manifest.kind
        )));
    }
    Ok(manifest)
}

pub(crate) fn ensure_worktree_inactive(manifest: &WorktreeManifest) -> Result<(), OperationsError> {
    if manifest.job_active {
        return Err(OperationsError::ActiveJob {
            worktree_id: manifest.worktree_id.clone(),
            detail: "manifest indicates `job_active == true`".to_string(),
        });
    }
    if let Some(pid) = manifest.active_job_pid.filter(|&pid| is_process_alive(pid)) {
        return Err(OperationsError::ActiveJob {
            worktree_id: manifest.worktree_id.clone(),
            detail: format!("active_job_pid {pid} is still running"),
        });
    }
    let manifests_dir = manifest.work_area_root.join(MANIFESTS_SUBDIR);
    let lock_path = manifests_dir.join(format!("{}.lock", manifest.worktree_id));
    let lock_file = open_private_lock_file(&lock_path, "worktree job lock")?;
    fs2::FileExt::try_lock_exclusive(&lock_file).map_err(|_| OperationsError::ActiveJob {
        worktree_id: manifest.worktree_id.clone(),
        detail: "worktree job lock is held by an active process".to_string(),
    })?;
    let _ = fs2::FileExt::unlock(&lock_file);
    Ok(())
}

pub(crate) fn ensure_worktree_inactive_readonly(
    manifest: &WorktreeManifest,
) -> Result<(), OperationsError> {
    ensure_worktree_job_inactive_readonly(manifest).map_err(|err| match err {
        WorkerError::CleanupRefused {
            worktree_id,
            reason: CleanupRefusalReason::ActiveJob,
            detail,
        } => OperationsError::ActiveJob {
            worktree_id,
            detail,
        },
        other => OperationsError::from(other),
    })
}

pub(crate) fn verify_worktree_branch(
    checkout_path: &Path,
    expected_branch: &str,
    worktree_id: &str,
) -> Result<(), OperationsError> {
    let branch_out = run_sanitized_git(
        checkout_path,
        &["symbolic-ref", "--quiet", "--short", "HEAD"],
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .map_err(|_| {
        OperationsError::WorktreeNotOwned(format!(
            "worktree `{worktree_id}` HEAD is detached or not on expected branch `{expected_branch}`"
        ))
    })?;
    if branch_out.trim() != expected_branch {
        return Err(OperationsError::WorktreeNotOwned(format!(
            "worktree `{worktree_id}` is on branch `{}`, expected `{expected_branch}`",
            branch_out.trim()
        )));
    }
    Ok(())
}

fn compute_worktree_diff_digest(
    checkout_path: &Path,
    base_commit: &str,
    task_head_commit: &str,
    changed_paths: &[ChangedPathEntry],
) -> Result<String, OperationsError> {
    let raw_patch = run_sanitized_git_bytes(
        checkout_path,
        &[
            "diff",
            "--binary",
            "--find-renames",
            "--full-index",
            base_commit,
            task_head_commit,
        ],
        DEFAULT_MAX_GIT_OUTPUT_BYTES,
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )?;
    let paths_json = serde_json::to_vec(changed_paths)?;
    let mut hasher = Sha256::new();
    hasher.update(b"threadlane-diff-v1\0");
    hasher.update(base_commit.as_bytes());
    hasher.update(b"\0");
    hasher.update(task_head_commit.as_bytes());
    hasher.update(b"\0");
    hasher.update((paths_json.len() as u64).to_le_bytes());
    hasher.update(&paths_json);
    hasher.update((raw_patch.stdout_bytes.len() as u64).to_le_bytes());
    hasher.update(&raw_patch.stdout_bytes);
    Ok(format!("{:x}", hasher.finalize()))
}

fn validate_model_like_identifier(value: &str, label: &str) -> Result<(), OperationsError> {
    if value.is_empty() || value.len() > 128 || value.trim() != value {
        return Err(OperationsError::InvalidInput(format!(
            "{label} must be a non-empty identifier of at most 128 bytes"
        )));
    }
    if value.starts_with('-') || value.starts_with('.') {
        return Err(OperationsError::InvalidInput(format!(
            "{label} `{value}` must not start with `-` or `.`"
        )));
    }
    if !value
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-' | '/' | '#'))
    {
        return Err(OperationsError::InvalidInput(format!(
            "{label} `{value}` contains disallowed characters"
        )));
    }
    Ok(())
}

fn validate_exact_tree_or_commit_format(
    value: &str,
    label: &str,
) -> Result<String, OperationsError> {
    Ok(validate_exact_40_hex_sha(value, label)?)
}

pub(crate) fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("{:x}", hasher.finalize())
}
