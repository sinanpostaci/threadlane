use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

use fs2::FileExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use threadlane_workers::{
    ensure_platform_supported, ensure_private_dir, is_commit_ancestor, is_control_config_path,
    is_path_allowed_by_scopes, is_worktree_clean, load_and_verify_manifest,
    open_existing_private_lock_file, open_private_lock_file, read_private_json,
    resolve_allowed_write_scopes_at_base, resolve_exact_commit_sha, resolve_exact_tree_sha,
    run_sanitized_git, run_sanitized_git_bytes, sanitize_and_redact,
    validate_absolute_no_symlink_components, validate_exact_40_hex_sha,
    validate_existing_canonical_dir, validate_repo_relative_path, validate_sha256_hex_digest,
    validate_simple_identifier, write_private_json_atomic, ChangedPathEntry, DependencyInputSeal,
    DiffChangeKind, WorkAreaOwnershipMarker, WorktreeKind, WorktreeManifest, WriteScopeKind,
    CHECKOUTS_SUBDIR, DEFAULT_GIT_COMMAND_TIMEOUT_MS, DEFAULT_MAX_DIFF_CHANGED_PATHS,
    DEFAULT_MAX_GIT_OUTPUT_BYTES, DEFAULT_MAX_SCAN_DEPTH, DEFAULT_MAX_SCAN_ENTRIES,
    MANIFESTS_SUBDIR, MAX_DEPENDENCY_INPUT_BYTES, WORK_AREA_MARKER_FILENAME,
    WORK_AREA_MARKER_VERSION,
};
use uuid::Uuid;

use crate::dependency::{
    dependency_input_snapshot_ref, DEPENDENCIES_SUBDIR, MAX_DEPENDENCY_CLOSURE_TASKS,
};
use crate::error::OperationsError;
use crate::integration::{
    inspect_job_integration_intent_readonly, load_integration_receipt, IntegrationReceipt,
    IntegrationStrategy, JobIntegrationIntentState,
};
use crate::review::{
    ensure_worktree_inactive_readonly, inspect_review_subject_readonly,
    load_and_verify_task_manifest, load_review_receipt, sha256_hex, verify_review_receipt_readonly,
    verify_worktree_branch, InspectReviewSubjectRequest, ReviewReceipt, TestEvidenceSummary,
    MAX_REVIEW_NOTE_BYTES, RECEIPTS_SUBDIR,
};

/// Schema version for [`FinalCandidateSubject`].
pub const FINAL_CANDIDATE_SUBJECT_SCHEMA_VERSION: u32 = 1;

/// Schema version for [`FinalCandidateClosureLedger`] and [`ClosureVerdict`].
pub const FINAL_CANDIDATE_CLOSURE_SCHEMA_VERSION: u32 = 1;

/// Subdirectory inside `<work_area_root>/manifests` storing private (`0600`) final candidate closure ledgers.
pub const CLOSURES_SUBDIR: &str = "closures";

/// Filename for the bounded private final-candidate closure ledger JSON inside [`CLOSURES_SUBDIR`].
pub const CLOSURE_LEDGER_FILENAME: &str = "final-candidate-closures.json";

/// Filename for the cross-process lock file protecting [`CLOSURE_LEDGER_FILENAME`].
pub const CLOSURE_LEDGER_LOCK_FILENAME: &str = "final-candidate-closures.lock";

/// Maximum byte size when reading a persisted [`FinalCandidateClosureLedger`] (256 KiB).
pub const MAX_CLOSURE_LEDGER_BYTES: u64 = 262_144;

/// Maximum number of closure attempt records retained in a single [`FinalCandidateClosureLedger`].
pub const MAX_CLOSURE_ATTEMPTS: usize = 128;

/// Policy, scope/registry revision, approval, and model/effort requirements bound into a [`FinalCandidateSubject`].
///
/// Any modification to this structure changes [`FinalCandidateSubject::subject_digest`] and
/// invalidates any previously recorded closure reservation or verdict for an older candidate state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FinalCandidatePolicyBinding {
    /// Policy or profile identifier governing the final closure step.
    pub policy_id: String,
    /// 64-character lowercase hex SHA-256 digest of the active execution policy.
    pub policy_digest: String,
    /// Monotonic scope/plan revision bound to the candidate.
    pub scope_revision: u64,
    /// Monotonic profile/registry revision bound to the candidate.
    pub registry_revision: u64,
    /// Frozen approval identifier (`app_...` or validated simple identifier).
    pub approval_id: String,
    /// 64-character lowercase hex SHA-256 digest of the approved plan/cycle subject.
    pub approval_subject_digest: String,
    /// Logical task identifier for the final closure step.
    pub closure_task_id: String,
    /// 64-character lowercase hex SHA-256 digest of the frozen closure task definition.
    pub closure_task_digest: String,
    /// Requested provider identifier for closure evaluation (e.g. `"antigravity"`, `"opencode_v2"`, or `"codex"`).
    pub requested_provider: String,
    /// Requested model identifier for closure evaluation (e.g. `"gpt-6-astra"`).
    pub requested_model: String,
    /// Requested reasoning effort or variant (e.g. `"max"`), if required by policy.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requested_effort: Option<String>,
    /// Whether verified observed effort evidence matching `requested_effort` is mandatory.
    pub require_effort_evidence: bool,
    /// Whether passing formatting, Clippy, and behavioral test evidence is mandatory.
    pub require_passing_checks: bool,
}

impl FinalCandidatePolicyBinding {
    /// Validates all identifiers, digests, and revision bounds in this policy binding.
    pub fn validate(&self) -> Result<(), OperationsError> {
        validate_simple_identifier(&self.policy_id, "policy.policy_id")?;
        let _ = validate_sha256_hex_digest(&self.policy_digest, "policy.policy_digest")?;
        if self.scope_revision == 0 {
            return Err(OperationsError::InvalidInput(
                "policy.scope_revision must be non-zero".to_string(),
            ));
        }
        if self.registry_revision == 0 {
            return Err(OperationsError::InvalidInput(
                "policy.registry_revision must be non-zero".to_string(),
            ));
        }
        validate_simple_identifier(&self.approval_id, "policy.approval_id")?;
        let _ = validate_sha256_hex_digest(
            &self.approval_subject_digest,
            "policy.approval_subject_digest",
        )?;
        validate_simple_identifier(&self.closure_task_id, "policy.closure_task_id")?;
        let _ =
            validate_sha256_hex_digest(&self.closure_task_digest, "policy.closure_task_digest")?;
        validate_simple_identifier(&self.requested_provider, "policy.requested_provider")?;
        validate_closure_model_identifier(&self.requested_model, "policy.requested_model")?;
        if let Some(ref effort) = self.requested_effort {
            validate_simple_identifier(effort, "policy.requested_effort")?;
        }
        if self.require_effort_evidence && self.requested_effort.is_none() {
            return Err(OperationsError::InvalidInput(
                "policy.requested_effort is required when require_effort_evidence is true"
                    .to_string(),
            ));
        }
        Ok(())
    }
}

/// Caller-supplied reference to an accepted and integrated predecessor task that contributed to
/// the final combined candidate in the owned `WorktreeKind::RunIntegration` checkout.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FinalCandidateTaskInput {
    /// Logical task identifier (`task_id`).
    pub task_id: String,
    /// 64-character lowercase hex SHA-256 digest of the frozen task specification.
    pub task_digest: String,
    /// Owned task worktree identifier (`WorktreeKind::Task`).
    pub task_worktree_id: String,
    /// Persisted [`ReviewReceipt`] identifier (`rev_...`).
    pub review_receipt_id: String,
    /// Persisted [`IntegrationReceipt`] identifier (`int_...`).
    pub integration_receipt_id: String,
    /// Optional expected execution base commit SHA for this task.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_execution_base_commit: Option<String>,
    /// Optional expected repository-relative `allowed_write_paths` for this task.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_allowed_write_paths: Option<BTreeSet<String>>,
    /// Optional expected requested provider identifier for this task.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_provider: Option<String>,
    /// Optional expected requested model identifier for this task.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_model: Option<String>,
    /// Optional expected requested effort identifier for this task.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_effort: Option<String>,
}

/// Verified, canonicalized predecessor task proof bound into a [`FinalCandidateSubject`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VerifiedCandidateTaskEvidence {
    /// Logical task identifier (`task_id`), used as the deterministic sort key.
    pub task_id: String,
    /// 64-character lowercase hex SHA-256 digest of the frozen task specification.
    pub task_digest: String,
    /// Run identifier of the task and integration receipts.
    pub run_id: String,
    /// Owned task worktree identifier (`WorktreeKind::Task`).
    pub task_worktree_id: String,
    /// Owned integration worktree identifier (`WorktreeKind::RunIntegration`) recorded in the integration receipt.
    pub integration_worktree_id: String,
    /// Task base commit SHA (`40`-hex).
    pub task_base_commit: String,
    /// Accepted task `HEAD` commit SHA (`40`-hex).
    pub accepted_task_head_commit: String,
    /// Accepted task `HEAD^{tree}` SHA (`40`-hex).
    pub accepted_task_tree_sha: String,
    /// Integrated target commit SHA (`40`-hex) recorded in the [`IntegrationReceipt`].
    pub integrated_target_commit: String,
    /// Integrated target `HEAD^{tree}` SHA (`40`-hex) recorded in the [`IntegrationReceipt`].
    pub integrated_tree_sha: String,
    /// 64-character lowercase hex SHA-256 digest of the task's [` crate::ReviewSubject`].
    pub review_subject_digest: String,
    /// Verified [`ReviewReceipt::receipt_id`].
    pub review_receipt_id: String,
    /// 64-character lowercase hex SHA-256 [`ReviewReceipt::receipt_digest`].
    pub review_receipt_digest: String,
    /// Verified [`IntegrationReceipt::receipt_id`].
    pub integration_receipt_id: String,
    /// 64-character lowercase hex SHA-256 [`IntegrationReceipt::receipt_digest`].
    pub integration_receipt_digest: String,
    /// Integration strategy used when integrating this task.
    pub integration_strategy: IntegrationStrategy,
    /// Verified repository-relative `allowed_write_paths` for this task.
    pub allowed_write_paths: BTreeSet<String>,
}

/// Request to inspect and freeze a read-only [`FinalCandidateSubject`] from the actual owned
/// `WorktreeKind::RunIntegration` checkout and its accepted/integrated task receipts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InspectFinalCandidateRequest {
    /// Canonical path to the primary source Git repository.
    pub repository_path: PathBuf,
    /// Canonical path to the private work-area root directory.
    pub work_area_root: PathBuf,
    /// Expected run identifier owning the `WorktreeKind::RunIntegration` checkout.
    pub run_id: String,
    /// Owned integration worktree identifier (`WorktreeKind::RunIntegration`).
    pub integration_worktree_id: String,
    /// Approved 40-character lowercase hex base commit SHA of the workflow cycle.
    pub approved_base_commit: String,
    /// Expected current 40-character lowercase hex `HEAD` commit SHA of the primary source repository.
    pub expected_source_head_commit: String,
    /// Optional expected 40-character lowercase hex `HEAD` commit SHA of the `RunIntegration` checkout.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_candidate_head_commit: Option<String>,
    /// Optional expected 40-character lowercase hex `HEAD^{tree}` SHA of the `RunIntegration` checkout.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_candidate_tree_sha: Option<String>,
    /// Non-empty set of selected accepted and integrated predecessor tasks.
    pub predecessor_tasks: Vec<FinalCandidateTaskInput>,
    /// Optional expected aggregate union of `allowed_write_paths` across all predecessor tasks.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_aggregate_write_paths: Option<BTreeSet<String>>,
    /// Policy, scope/registry revision, and closure role/model binding.
    pub policy: FinalCandidatePolicyBinding,
    /// Candidate-level formatting, Clippy, and behavioral test verification evidence.
    pub check_evidence: TestEvidenceSummary,
}

/// Frozen, digest-bound final combined candidate subject inspected from an owned
/// `WorktreeKind::RunIntegration` checkout.
///
/// Unlike legacy base-tree closure reservations (`ApprovedCycleBase`), [`FinalCandidateSubject`]
/// binds the actual combined `candidate_head_commit`, `candidate_tree_sha`, `candidate_diff_digest`,
/// sorted [`VerifiedCandidateTaskEvidence`] proofs, `aggregate_allowed_write_paths`,
/// [`FinalCandidatePolicyBinding`], and [`TestEvidenceSummary`] into a deterministic SHA-256
/// `subject_digest`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FinalCandidateSubject {
    /// Schema version (`FINAL_CANDIDATE_SUBJECT_SCHEMA_VERSION`).
    pub schema_version: u32,
    /// Run identifier owning the `RunIntegration` worktree.
    pub run_id: String,
    /// Owned integration worktree identifier (`WorktreeKind::RunIntegration`).
    pub integration_worktree_id: String,
    /// Canonical primary repository path.
    pub repository_path: PathBuf,
    /// Canonical `.git` common directory of the primary repository.
    pub repository_common_git_dir: PathBuf,
    /// Canonical private work-area root directory.
    pub work_area_root: PathBuf,
    /// Canonical `RunIntegration` checkout directory path.
    pub integration_checkout_path: PathBuf,
    /// Checked-out `threadlane/run/...` branch name of the `RunIntegration` worktree.
    pub integration_branch_name: String,
    /// Current 40-hex `HEAD` commit SHA of the primary source repository (verified untouched).
    pub source_head_commit: String,
    /// Approved 40-hex base commit SHA of the cycle.
    pub approved_base_commit: String,
    /// Exact 40-hex `approved_base_commit^{tree}` SHA.
    pub approved_base_tree_sha: String,
    /// Base 40-hex commit SHA recorded in the `RunIntegration` worktree manifest.
    pub integration_base_commit: String,
    /// Final combined candidate 40-hex `HEAD` commit SHA in the `RunIntegration` worktree.
    pub candidate_head_commit: String,
    /// Final combined candidate 40-hex `HEAD^{tree}` SHA in the `RunIntegration` worktree.
    pub candidate_tree_sha: String,
    /// 64-hex SHA-256 digest of the combined diff between `approved_base_commit` and `candidate_head_commit`.
    pub candidate_diff_digest: String,
    /// Deterministic union of verified predecessor `allowed_write_paths`.
    pub aggregate_allowed_write_paths: BTreeSet<String>,
    /// Sorted list of changed paths between `approved_base_commit` and `candidate_head_commit`.
    pub changed_paths: Vec<ChangedPathEntry>,
    /// Deterministically sorted (`by task_id`) verified predecessor task proofs.
    pub predecessors: Vec<VerifiedCandidateTaskEvidence>,
    /// Policy, scope/registry revision, approval, and closure model/effort binding.
    pub policy: FinalCandidatePolicyBinding,
    /// Candidate-level formatting, Clippy, and test verification summary.
    pub check_evidence: TestEvidenceSummary,
    /// Canonical 64-character lowercase hex SHA-256 digest of all fields above.
    pub subject_digest: String,
}

#[derive(Serialize)]
struct FinalCandidateSubjectDigestPayload<'a> {
    schema_version: u32,
    run_id: &'a str,
    integration_worktree_id: &'a str,
    repository_path: &'a Path,
    repository_common_git_dir: &'a Path,
    work_area_root: &'a Path,
    integration_checkout_path: &'a Path,
    integration_branch_name: &'a str,
    source_head_commit: &'a str,
    approved_base_commit: &'a str,
    approved_base_tree_sha: &'a str,
    integration_base_commit: &'a str,
    candidate_head_commit: &'a str,
    candidate_tree_sha: &'a str,
    candidate_diff_digest: &'a str,
    aggregate_allowed_write_paths: &'a BTreeSet<String>,
    changed_paths: &'a [ChangedPathEntry],
    predecessors: &'a [VerifiedCandidateTaskEvidence],
    policy: &'a FinalCandidatePolicyBinding,
    check_evidence: &'a TestEvidenceSummary,
}

impl FinalCandidateSubject {
    /// Computes the canonical 64-character lowercase hexadecimal SHA-256 digest of this subject.
    pub fn compute_digest(&self) -> Result<String, OperationsError> {
        let payload = FinalCandidateSubjectDigestPayload {
            schema_version: self.schema_version,
            run_id: &self.run_id,
            integration_worktree_id: &self.integration_worktree_id,
            repository_path: &self.repository_path,
            repository_common_git_dir: &self.repository_common_git_dir,
            work_area_root: &self.work_area_root,
            integration_checkout_path: &self.integration_checkout_path,
            integration_branch_name: &self.integration_branch_name,
            source_head_commit: &self.source_head_commit,
            approved_base_commit: &self.approved_base_commit,
            approved_base_tree_sha: &self.approved_base_tree_sha,
            integration_base_commit: &self.integration_base_commit,
            candidate_head_commit: &self.candidate_head_commit,
            candidate_tree_sha: &self.candidate_tree_sha,
            candidate_diff_digest: &self.candidate_diff_digest,
            aggregate_allowed_write_paths: &self.aggregate_allowed_write_paths,
            changed_paths: &self.changed_paths,
            predecessors: &self.predecessors,
            policy: &self.policy,
            check_evidence: &self.check_evidence,
        };
        let encoded = serde_json::to_vec(&payload)?;
        let mut hasher = Sha256::new();
        hasher.update(b"threadlane-final-candidate-subject-v1\0");
        hasher.update(&encoded);
        Ok(format!("{:x}", hasher.finalize()))
    }

    /// Validates all structural invariants and verifies that `self.subject_digest` matches [`Self::compute_digest`].
    pub fn verify_integrity(&self) -> Result<(), OperationsError> {
        if self.schema_version != FINAL_CANDIDATE_SUBJECT_SCHEMA_VERSION {
            return Err(OperationsError::InvalidInput(format!(
                "unsupported final candidate subject schema_version {} (expected {FINAL_CANDIDATE_SUBJECT_SCHEMA_VERSION})",
                self.schema_version
            )));
        }
        validate_simple_identifier(&self.run_id, "run_id")?;
        validate_simple_identifier(&self.integration_worktree_id, "integration_worktree_id")?;
        if !self.integration_branch_name.starts_with("threadlane/run/") {
            return Err(OperationsError::WorktreeNotOwned(format!(
                "integration_branch_name `{}` must carry the `threadlane/run/` prefix",
                self.integration_branch_name
            )));
        }
        let _ = validate_exact_40_hex_sha(&self.source_head_commit, "source_head_commit")?;
        let _ = validate_exact_40_hex_sha(&self.approved_base_commit, "approved_base_commit")?;
        let _ = validate_exact_40_hex_sha(&self.approved_base_tree_sha, "approved_base_tree_sha")?;
        let _ =
            validate_exact_40_hex_sha(&self.integration_base_commit, "integration_base_commit")?;
        let _ = validate_exact_40_hex_sha(&self.candidate_head_commit, "candidate_head_commit")?;
        let _ = validate_exact_40_hex_sha(&self.candidate_tree_sha, "candidate_tree_sha")?;
        let _ = validate_sha256_hex_digest(&self.candidate_diff_digest, "candidate_diff_digest")?;
        let _ = validate_sha256_hex_digest(&self.subject_digest, "subject_digest")?;

        if self.candidate_head_commit == self.approved_base_commit
            || self.candidate_head_commit == self.integration_base_commit
        {
            return Err(OperationsError::SubjectMismatch(
                "final candidate_head_commit must advance beyond approved_base_commit and integration_base_commit"
                    .to_string(),
            ));
        }
        if self.candidate_tree_sha == self.approved_base_tree_sha {
            return Err(OperationsError::SubjectMismatch(
                "final candidate_tree_sha equals approved_base_tree_sha; base-tree-only state cannot be frozen as a final combined candidate"
                    .to_string(),
            ));
        }
        if self.aggregate_allowed_write_paths.is_empty() {
            return Err(OperationsError::InvalidInput(
                "aggregate_allowed_write_paths must not be empty".to_string(),
            ));
        }
        for path in &self.aggregate_allowed_write_paths {
            validate_repo_relative_path(path, "aggregate_allowed_write_paths entry")?;
            if is_control_config_path(path) {
                return Err(OperationsError::SecurityViolation(format!(
                    "aggregate_allowed_write_paths entry `{path}` targets protected control configuration"
                )));
            }
        }
        if self.changed_paths.is_empty() {
            return Err(OperationsError::SubjectMismatch(
                "final candidate changed_paths must not be empty".to_string(),
            ));
        }
        for window in self.changed_paths.windows(2) {
            if window[0].path >= window[1].path {
                return Err(OperationsError::InvalidInput(
                    "final candidate changed_paths must be strictly sorted by path".to_string(),
                ));
            }
        }
        if self.predecessors.is_empty() || self.predecessors.len() > MAX_DEPENDENCY_CLOSURE_TASKS {
            return Err(OperationsError::InvalidInput(format!(
                "predecessors count ({}) must be between 1 and {MAX_DEPENDENCY_CLOSURE_TASKS}",
                self.predecessors.len()
            )));
        }
        for window in self.predecessors.windows(2) {
            if window[0].task_id >= window[1].task_id {
                return Err(OperationsError::InvalidInput(
                    "predecessors must be strictly sorted and unique by task_id".to_string(),
                ));
            }
        }

        self.policy.validate()?;
        self.check_evidence
            .validate(self.policy.require_passing_checks)?;

        let computed = self.compute_digest()?;
        if computed != self.subject_digest {
            return Err(OperationsError::SubjectMismatch(format!(
                "final candidate subject_digest mismatch: expected `{}`, computed `{computed}`",
                self.subject_digest
            )));
        }
        Ok(())
    }
}

/// Inspects and freezes a read-only [`FinalCandidateSubject`] from the actual owned
/// `WorktreeKind::RunIntegration` checkout and its accepted/integrated task receipts.
///
/// ## Guarantees
/// - Strictly read-only: never merges, pushes, creates missing worktree `.lock` files, or mutates
///   the primary checkout or any worktree.
/// - Verifies that the primary source repository `HEAD` equals `expected_source_head_commit` and
///   that primary checkout status before and after inspection is completely unchanged.
/// - Verifies the `WorktreeKind::RunIntegration` ownership manifest, directory confinement under
///   `<work_area_root>/checkouts/runs/`, `threadlane/run/...` branch binding, inactive writer state,
///   clean working tree, and ancestry from `approved_base_commit`.
/// - Reconstructs every selected predecessor task's [`crate::ReviewSubject`] from live disk via
///   [`inspect_review_subject_readonly`], verifies its persisted [`ReviewReceipt`] and
///   [`IntegrationReceipt`], ensures neither receipt is superseded, and verifies commit ancestry
///   into `candidate_head_commit`.
/// - Verifies that the latest [`IntegrationReceipt`] targeting `integration_worktree_id` is in the
///   selected predecessor set and its `integrated_target_commit` and `integrated_tree_sha` match
///   the live `RunIntegration` `HEAD` and `HEAD^{tree}` (rejecting unreviewed extra commits on the
///   integration branch).
/// - Resolves the union `aggregate_allowed_write_paths` against `approved_base_commit` and validates
///   all combined diff paths in `approved_base_commit..candidate_head_commit` (rejecting symlinks,
///   control-config edits, path escapes, out-of-scope edits, and paths not attributable to verified
///   predecessor task diffs).
pub fn inspect_final_candidate_readonly(
    request: &InspectFinalCandidateRequest,
) -> Result<FinalCandidateSubject, OperationsError> {
    ensure_platform_supported()?;
    validate_simple_identifier(&request.run_id, "run_id")?;
    validate_simple_identifier(&request.integration_worktree_id, "integration_worktree_id")?;
    let approved_base_commit =
        validate_exact_40_hex_sha(&request.approved_base_commit, "approved_base_commit")?;
    let expected_source_head = validate_exact_40_hex_sha(
        &request.expected_source_head_commit,
        "expected_source_head_commit",
    )?;
    let expected_candidate_head = match request.expected_candidate_head_commit {
        Some(ref sha) => Some(validate_exact_40_hex_sha(
            sha,
            "expected_candidate_head_commit",
        )?),
        None => None,
    };
    let expected_candidate_tree = match request.expected_candidate_tree_sha {
        Some(ref sha) => Some(validate_exact_40_hex_sha(
            sha,
            "expected_candidate_tree_sha",
        )?),
        None => None,
    };

    if request.predecessor_tasks.is_empty()
        || request.predecessor_tasks.len() > MAX_DEPENDENCY_CLOSURE_TASKS
    {
        return Err(OperationsError::InvalidInput(format!(
            "predecessor_tasks count ({}) must be between 1 and {MAX_DEPENDENCY_CLOSURE_TASKS}",
            request.predecessor_tasks.len()
        )));
    }

    request.policy.validate()?;
    let mut clean_check_evidence = request.check_evidence.clone();
    clean_check_evidence.evidence_summary = sanitize_and_redact(
        &clean_check_evidence.evidence_summary,
        MAX_REVIEW_NOTE_BYTES,
    );
    clean_check_evidence.validate(request.policy.require_passing_checks)?;

    if !request.repository_path.is_absolute() || !request.work_area_root.is_absolute() {
        return Err(OperationsError::InvalidInput(
            "repository_path and work_area_root must be absolute paths".to_string(),
        ));
    }

    let canonical_repo =
        validate_existing_canonical_dir(&request.repository_path, "repository_path")?;
    if canonical_repo != request.repository_path {
        return Err(OperationsError::SecurityViolation(
            "repository_path canonicalization mismatch".to_string(),
        ));
    }
    let canonical_work_area =
        validate_existing_canonical_dir(&request.work_area_root, "work_area_root")?;
    if canonical_work_area != request.work_area_root {
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

    // Snapshot primary repository HEAD and status before inspection to prove zero mutation.
    let pre_source_head =
        resolve_exact_commit_sha(&canonical_repo, "HEAD", DEFAULT_GIT_COMMAND_TIMEOUT_MS)?;
    if pre_source_head != expected_source_head {
        return Err(OperationsError::HeadMoved {
            target: "primary_repository".to_string(),
            expected: expected_source_head,
            actual: pre_source_head,
        });
    }
    let pre_source_status = capture_porcelain_status(&canonical_repo)?;

    // Verify work_area_root ownership marker against the primary repository's common git dir.
    let repo_common_git_dir = resolve_common_git_dir(&canonical_repo)?;
    verify_work_area_ownership_marker(&canonical_work_area, &canonical_repo, &repo_common_git_dir)?;

    // Load and verify the owned RunIntegration manifest.
    let integration_manifest = load_and_verify_run_integration_manifest(
        &canonical_work_area,
        &request.integration_worktree_id,
    )?;
    if integration_manifest.repository_path != canonical_repo
        || integration_manifest.work_area_root != canonical_work_area
        || integration_manifest.repository_common_git_dir.as_ref() != Some(&repo_common_git_dir)
    {
        return Err(OperationsError::WorktreeNotOwned(format!(
            "RunIntegration worktree `{}` does not belong to the requested repository and work_area_root",
            request.integration_worktree_id
        )));
    }
    if integration_manifest.run_id != request.run_id {
        return Err(OperationsError::WorktreeNotOwned(format!(
            "RunIntegration worktree `{}` run_id `{}` does not match expected run_id `{}`",
            integration_manifest.worktree_id, integration_manifest.run_id, request.run_id
        )));
    }
    if integration_manifest.task_id.is_some() {
        return Err(OperationsError::WorktreeNotOwned(format!(
            "RunIntegration worktree `{}` must not carry a task_id",
            integration_manifest.worktree_id
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
    let expected_branch_prefix = format!("threadlane/run/{}/", request.run_id);
    if !integration_manifest
        .branch_name
        .starts_with(&expected_branch_prefix)
    {
        return Err(OperationsError::WorktreeNotOwned(format!(
            "RunIntegration branch `{}` does not start with `{expected_branch_prefix}`",
            integration_manifest.branch_name
        )));
    }

    // Verify no active job/writer on the RunIntegration worktree without creating a lock file.
    ensure_worktree_inactive_readonly(&integration_manifest)?;
    if let Some(job_intent) = inspect_job_integration_intent_readonly(
        &canonical_work_area,
        &integration_manifest.worktree_id,
    )? {
        if job_intent.state != JobIntegrationIntentState::Completed {
            return Err(OperationsError::UncertainWorkerOutcome(format!(
                "RunIntegration worktree `{}` has unresolved job integration intent `{}` in state `{:?}`; cannot freeze final candidate until resolved",
                integration_manifest.worktree_id, job_intent.intent_id, job_intent.state
            )));
        }
    }

    let integration_checkout = validate_existing_canonical_dir(
        &integration_manifest.checkout_path,
        "integration checkout_path",
    )?;
    if integration_checkout != integration_manifest.checkout_path {
        return Err(OperationsError::SecurityViolation(
            "integration checkout_path canonicalization mismatch".to_string(),
        ));
    }
    let checkout_common_git_dir = resolve_common_git_dir(&integration_checkout)?;
    if checkout_common_git_dir != repo_common_git_dir {
        return Err(OperationsError::WorktreeNotOwned(format!(
            "RunIntegration checkout `{}` git-common-dir `{}` does not match repository `{}`",
            integration_manifest.worktree_id,
            checkout_common_git_dir.display(),
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
            detail:
                "RunIntegration worktree has uncommitted, staged, untracked, or ignored changes"
                    .to_string(),
        });
    }

    // Resolve base and candidate commits/trees and verify ancestry.
    let resolved_approved_base = resolve_exact_commit_sha(
        &canonical_repo,
        &approved_base_commit,
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )?;
    if resolved_approved_base != approved_base_commit {
        return Err(OperationsError::SubjectMismatch(format!(
            "approved_base_commit `{approved_base_commit}` resolved to `{resolved_approved_base}`"
        )));
    }
    let approved_base_tree_sha = resolve_exact_tree_sha(
        &canonical_repo,
        &approved_base_commit,
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )?;
    let integration_base_commit = validate_exact_40_hex_sha(
        &integration_manifest.base_commit,
        "integration_manifest.base_commit",
    )?;
    let candidate_head_commit = resolve_exact_commit_sha(
        &integration_checkout,
        "HEAD",
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )?;
    let candidate_tree_sha = resolve_exact_tree_sha(
        &integration_checkout,
        "HEAD",
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )?;

    if let Some(ref exp_head) = expected_candidate_head {
        if &candidate_head_commit != exp_head {
            return Err(OperationsError::HeadMoved {
                target: format!("integration_worktree:{}", integration_manifest.worktree_id),
                expected: exp_head.clone(),
                actual: candidate_head_commit,
            });
        }
    }
    if let Some(ref exp_tree) = expected_candidate_tree {
        if &candidate_tree_sha != exp_tree {
            return Err(OperationsError::SubjectMismatch(format!(
                "RunIntegration candidate_tree_sha `{candidate_tree_sha}` does not match expected `{exp_tree}`"
            )));
        }
    }

    if candidate_head_commit == approved_base_commit
        || candidate_head_commit == integration_base_commit
    {
        return Err(OperationsError::SubjectMismatch(format!(
            "RunIntegration worktree `{}` HEAD `{candidate_head_commit}` has not advanced beyond base commit",
            integration_manifest.worktree_id
        )));
    }
    if candidate_tree_sha == approved_base_tree_sha {
        return Err(OperationsError::SubjectMismatch(format!(
            "RunIntegration worktree `{}` tree `{candidate_tree_sha}` is identical to approved_base_tree_sha; base tree alone does not constitute a final candidate",
            integration_manifest.worktree_id
        )));
    }

    if !is_commit_ancestor(
        &canonical_repo,
        &approved_base_commit,
        &integration_base_commit,
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )? {
        return Err(OperationsError::AncestryMismatch(format!(
            "approved_base_commit `{approved_base_commit}` is not an ancestor of integration_base_commit `{integration_base_commit}`"
        )));
    }
    if !is_commit_ancestor(
        &canonical_repo,
        &integration_base_commit,
        &candidate_head_commit,
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )? {
        return Err(OperationsError::AncestryMismatch(format!(
            "integration_base_commit `{integration_base_commit}` is not an ancestor of candidate_head_commit `{candidate_head_commit}`"
        )));
    }

    // Sort and deduplicate predecessor task inputs by `task_id`.
    let mut sorted_inputs = request.predecessor_tasks.clone();
    sorted_inputs.sort_by(|a, b| a.task_id.cmp(&b.task_id));

    let mut seen_task_ids = BTreeSet::new();
    let mut seen_task_worktrees = BTreeSet::new();
    let mut seen_review_receipts = BTreeSet::new();
    let mut seen_integration_receipts = BTreeSet::new();

    let mut verified_predecessors = Vec::with_capacity(sorted_inputs.len());
    let mut aggregate_allowed_write_paths = BTreeSet::new();
    let mut union_task_changed_paths = BTreeSet::new();
    let mut latest_run_integration_receipt: Option<IntegrationReceipt> = None;
    let mut observed_job_aggregation_scope: Option<(
        String,
        u64,
        String,
        String,
        String,
        BTreeSet<String>,
    )> = None;
    let mut saw_legacy_same_run_receipt = false;

    for task_input in &sorted_inputs {
        validate_simple_identifier(&task_input.task_id, "predecessor.task_id")?;
        let task_digest =
            validate_sha256_hex_digest(&task_input.task_digest, "predecessor.task_digest")?;
        validate_simple_identifier(&task_input.task_worktree_id, "predecessor.task_worktree_id")?;
        validate_simple_identifier(
            &task_input.review_receipt_id,
            "predecessor.review_receipt_id",
        )?;
        validate_simple_identifier(
            &task_input.integration_receipt_id,
            "predecessor.integration_receipt_id",
        )?;

        if !seen_task_ids.insert(task_input.task_id.clone()) {
            return Err(OperationsError::InvalidInput(format!(
                "duplicate predecessor task_id `{}` in final candidate request",
                task_input.task_id
            )));
        }
        if !seen_task_worktrees.insert(task_input.task_worktree_id.clone()) {
            return Err(OperationsError::InvalidInput(format!(
                "duplicate predecessor task_worktree_id `{}` in final candidate request",
                task_input.task_worktree_id
            )));
        }
        if !seen_review_receipts.insert(task_input.review_receipt_id.clone()) {
            return Err(OperationsError::InvalidInput(format!(
                "duplicate predecessor review_receipt_id `{}` in final candidate request",
                task_input.review_receipt_id
            )));
        }
        if !seen_integration_receipts.insert(task_input.integration_receipt_id.clone()) {
            return Err(OperationsError::InvalidInput(format!(
                "duplicate predecessor integration_receipt_id `{}` in final candidate request",
                task_input.integration_receipt_id
            )));
        }

        let task_manifest =
            load_and_verify_task_manifest(&canonical_work_area, &task_input.task_worktree_id)?;
        ensure_worktree_inactive_readonly(&task_manifest)?;

        if task_manifest.repository_path != canonical_repo
            || task_manifest.work_area_root != canonical_work_area
            || task_manifest.repository_common_git_dir.as_ref() != Some(&repo_common_git_dir)
        {
            return Err(OperationsError::WorktreeNotOwned(format!(
                "predecessor task `{}` worktree `{}` does not belong to canonical repository and work_area_root",
                task_input.task_id, task_input.task_worktree_id
            )));
        }
        let review_receipt =
            load_review_receipt(&canonical_work_area, &task_input.review_receipt_id)?;
        let integration_receipt =
            load_integration_receipt(&canonical_work_area, &task_input.integration_receipt_id)?;

        match integration_receipt.job_aggregation {
            Some(ref agg) => {
                if agg.logical_task_id != task_input.task_id {
                    return Err(OperationsError::ReceiptInvalidated(format!(
                        "predecessor task `{}` integration receipt `{}` job_aggregation.logical_task_id `{}` mismatch",
                        task_input.task_id, integration_receipt.receipt_id, agg.logical_task_id
                    )));
                }
                if task_manifest.task_id.as_deref() != Some(review_receipt.task_id.as_str())
                    || task_manifest.task_id.as_deref()
                        != Some(integration_receipt.task_id.as_str())
                {
                    return Err(OperationsError::WorktreeNotOwned(format!(
                        "predecessor task `{}` worktree `{}` manifest task_id `{:?}` does not match review/integration receipt task_id `{}`",
                        task_input.task_id,
                        task_input.task_worktree_id,
                        task_manifest.task_id,
                        review_receipt.task_id
                    )));
                }
            }
            None => {
                if task_manifest.task_id.as_deref() != Some(task_input.task_id.as_str()) {
                    return Err(OperationsError::WorktreeNotOwned(format!(
                        "predecessor task worktree `{}` manifest task_id `{:?}` does not match `{}`",
                        task_input.task_worktree_id, task_manifest.task_id, task_input.task_id
                    )));
                }
                if review_receipt.task_id != task_input.task_id {
                    return Err(OperationsError::ReceiptInvalidated(format!(
                        "review receipt `{}` identity mismatch for predecessor task `{}`",
                        review_receipt.receipt_id, task_input.task_id
                    )));
                }
            }
        }

        if review_receipt.task_worktree_id != task_input.task_worktree_id
            || review_receipt.run_id != task_manifest.run_id
            || review_receipt.repository_path != canonical_repo
            || review_receipt.work_area_root != canonical_work_area
        {
            return Err(OperationsError::ReceiptInvalidated(format!(
                "review receipt `{}` identity mismatch for predecessor task `{}`",
                review_receipt.receipt_id, task_input.task_id
            )));
        }

        if let Some(ref exp_base) = task_input.expected_execution_base_commit {
            let exp_base_sha =
                validate_exact_40_hex_sha(exp_base, "predecessor.expected_execution_base_commit")?;
            if task_manifest.base_commit != exp_base_sha
                || review_receipt.base_commit != exp_base_sha
                || integration_receipt.base_commit != exp_base_sha
            {
                return Err(OperationsError::SubjectMismatch(format!(
                    "predecessor task `{}` base_commit `{}` does not match expected_execution_base_commit `{exp_base_sha}`",
                    task_input.task_id, review_receipt.base_commit
                )));
            }
        }
        if let Some(ref exp_scope) = task_input.expected_allowed_write_paths {
            if review_receipt.allowed_write_paths != *exp_scope
                || task_manifest.allowed_write_paths != *exp_scope
            {
                return Err(OperationsError::SubjectMismatch(format!(
                    "predecessor task `{}` allowed_write_paths do not match expected_allowed_write_paths",
                    task_input.task_id
                )));
            }
        }
        if let Some(ref exp_provider) = task_input.expected_provider {
            if review_receipt.policy.requested_provider.as_deref() != Some(exp_provider.as_str()) {
                return Err(OperationsError::ReceiptInvalidated(format!(
                    "predecessor task `{}` review receipt requested_provider `{:?}` does not match expected `{exp_provider}`",
                    task_input.task_id, review_receipt.policy.requested_provider
                )));
            }
        }
        if let Some(ref exp_model) = task_input.expected_model {
            if review_receipt.policy.requested_model.as_deref() != Some(exp_model.as_str()) {
                return Err(OperationsError::ReceiptInvalidated(format!(
                    "predecessor task `{}` review receipt requested_model `{:?}` does not match expected `{exp_model}`",
                    task_input.task_id, review_receipt.policy.requested_model
                )));
            }
        }
        if let Some(ref exp_effort) = task_input.expected_effort {
            if review_receipt.policy.requested_effort.as_deref() != Some(exp_effort.as_str())
                || !review_receipt.policy.require_effort_evidence
                || review_receipt.policy.observed_effort.as_deref() != Some(exp_effort.as_str())
            {
                return Err(OperationsError::UncertainWorkerOutcome(format!(
                    "predecessor task `{}` review receipt is missing verified effort `{exp_effort}`",
                    task_input.task_id
                )));
            }
        }
        if !review_receipt.policy.require_passing_tests
            || !review_receipt.test_evidence.all_checks_passed()
        {
            return Err(OperationsError::ReceiptInvalidated(format!(
                "predecessor task `{}` review receipt does not carry required passing test evidence",
                task_input.task_id
            )));
        }

        // Reconstruct live ReviewSubject from disk in read-only mode (never trusting caller DTOs).
        let live_task_subject = inspect_review_subject_readonly(&InspectReviewSubjectRequest {
            work_area_root: canonical_work_area.clone(),
            task_worktree_id: task_input.task_worktree_id.clone(),
            policy: review_receipt.policy.clone(),
            test_evidence: review_receipt.test_evidence.clone(),
            worker_status: None,
        })
        .map_err(|err| {
            OperationsError::ReceiptInvalidated(format!(
                "predecessor task `{}` live worktree failed review subject verification: {err}",
                task_input.task_id
            ))
        })?;

        verify_review_receipt_readonly(&live_task_subject, &review_receipt)?;
        integration_receipt.verify_against_review(&live_task_subject, &review_receipt)?;

        if integration_receipt.source_head_commit != expected_source_head {
            return Err(OperationsError::ReceiptInvalidated(format!(
                "predecessor task `{}` integration_receipt.source_head_commit `{}` does not match expected_source_head_commit `{expected_source_head}`",
                task_input.task_id, integration_receipt.source_head_commit
            )));
        }

        // Verify neither receipt has been superseded by a newer receipt for the same task worktree.
        ensure_task_receipts_not_superseded(
            &canonical_work_area,
            &review_receipt,
            &integration_receipt,
        )?;

        // Verify commit ancestry chain:
        // approved_base_commit <= task_base_commit <= accepted_task_head_commit <= integrated_target_commit <= candidate_head_commit
        if !is_commit_ancestor(
            &canonical_repo,
            &approved_base_commit,
            &review_receipt.base_commit,
            DEFAULT_GIT_COMMAND_TIMEOUT_MS,
        )? {
            return Err(OperationsError::AncestryMismatch(format!(
                "approved_base_commit `{approved_base_commit}` is not an ancestor of predecessor `{}` base_commit `{}`",
                task_input.task_id, review_receipt.base_commit
            )));
        }
        if !is_commit_ancestor(
            &canonical_repo,
            &review_receipt.base_commit,
            &integration_receipt.accepted_task_head_commit,
            DEFAULT_GIT_COMMAND_TIMEOUT_MS,
        )? {
            return Err(OperationsError::AncestryMismatch(format!(
                "predecessor `{}` base_commit `{}` is not an ancestor of accepted_task_head_commit `{}`",
                task_input.task_id,
                review_receipt.base_commit,
                integration_receipt.accepted_task_head_commit
            )));
        }
        if !is_commit_ancestor(
            &canonical_repo,
            &integration_receipt.accepted_task_head_commit,
            &integration_receipt.integrated_target_commit,
            DEFAULT_GIT_COMMAND_TIMEOUT_MS,
        )? {
            return Err(OperationsError::AncestryMismatch(format!(
                "predecessor `{}` accepted_task_head_commit `{}` is not an ancestor of integrated_target_commit `{}`",
                task_input.task_id,
                integration_receipt.accepted_task_head_commit,
                integration_receipt.integrated_target_commit
            )));
        }
        if !is_commit_ancestor(
            &canonical_repo,
            &integration_receipt.integrated_target_commit,
            &candidate_head_commit,
            DEFAULT_GIT_COMMAND_TIMEOUT_MS,
        )? {
            return Err(OperationsError::AncestryMismatch(format!(
                "predecessor `{}` integrated_target_commit `{}` is not an ancestor of RunIntegration candidate_head_commit `{candidate_head_commit}`",
                task_input.task_id, integration_receipt.integrated_target_commit
            )));
        }

        if task_manifest.run_id != integration_manifest.run_id
            && (integration_receipt.job_aggregation.is_none()
                || integration_receipt.integration_worktree_id != integration_manifest.worktree_id)
        {
            return Err(OperationsError::ReceiptInvalidated(format!(
                "predecessor `{}` from run `{}` cannot satisfy RunIntegration run `{}` without a verified job_aggregation receipt targeting worktree `{}`",
                task_input.task_id,
                task_manifest.run_id,
                integration_manifest.run_id,
                integration_manifest.worktree_id
            )));
        }

        if integration_receipt.integration_worktree_id == integration_manifest.worktree_id {
            if integration_receipt.integration_checkout_path != integration_checkout
                || integration_receipt.integration_branch_name != integration_manifest.branch_name
            {
                return Err(OperationsError::ReceiptInvalidated(format!(
                    "predecessor `{}` integration receipt `{}` has mismatched RunIntegration metadata",
                    task_input.task_id, integration_receipt.receipt_id
                )));
            }
            match integration_receipt.job_aggregation {
                Some(ref agg) => {
                    if saw_legacy_same_run_receipt {
                        return Err(OperationsError::ReceiptInvalidated(
                            "cannot mix legacy same-run and job-aggregated integration receipts on the same RunIntegration worktree"
                                .to_string(),
                        ));
                    }
                    if agg.original_task_run_id != task_manifest.run_id
                        || agg.original_task_run_id != review_receipt.run_id
                        || agg.original_task_run_id != integration_receipt.run_id
                        || agg.target_integration_run_id != integration_manifest.run_id
                        || agg.approved_base_commit != approved_base_commit
                        || agg.logical_task_id != task_input.task_id
                        || agg.task_digest != task_digest
                        || agg.approval_id != request.policy.approval_id
                        || agg.approval_subject_digest != request.policy.approval_subject_digest
                        || agg.accepted_task_tree_sha != live_task_subject.task_tree_sha
                        || agg.accepted_task_diff_digest != live_task_subject.diff_digest
                        || agg.task_allowed_write_paths != live_task_subject.allowed_write_paths
                        || agg.task_policy != live_task_subject.policy
                        || agg.task_test_evidence != live_task_subject.test_evidence
                    {
                        return Err(OperationsError::ReceiptInvalidated(format!(
                            "predecessor `{}` integration receipt `{}` job_aggregation provenance does not match target RunIntegration run, task digest, or approval policy",
                            task_input.task_id, integration_receipt.receipt_id
                        )));
                    }
                    if review_receipt.base_commit == approved_base_commit {
                        if agg.dependency_seal_digest.is_some()
                            || agg.dependency_snapshot_ref.is_some()
                        {
                            return Err(OperationsError::ReceiptInvalidated(format!(
                                "root predecessor `{}` integration receipt `{}` must not carry dependency seal provenance",
                                task_input.task_id, integration_receipt.receipt_id
                            )));
                        }
                    } else {
                        let (Some(ref expected_seal_digest), Some(ref expected_snapshot_ref)) =
                            (&agg.dependency_seal_digest, &agg.dependency_snapshot_ref)
                        else {
                            return Err(OperationsError::ReceiptInvalidated(format!(
                                "non-root predecessor `{}` integration receipt `{}` is missing dependency_seal_digest or dependency_snapshot_ref",
                                task_input.task_id, integration_receipt.receipt_id
                            )));
                        };
                        let canonical_snapshot_ref = dependency_input_snapshot_ref(
                            &request.policy.approval_id,
                            &task_input.task_id,
                        )?;
                        if expected_snapshot_ref != &canonical_snapshot_ref {
                            return Err(OperationsError::ReceiptInvalidated(format!(
                                "non-root predecessor `{}` integration receipt `{}` dependency_snapshot_ref `{expected_snapshot_ref}` does not match `{canonical_snapshot_ref}`",
                                task_input.task_id, integration_receipt.receipt_id
                            )));
                        }
                        let seal_path = canonical_work_area
                            .join(MANIFESTS_SUBDIR)
                            .join(DEPENDENCIES_SUBDIR)
                            .join(format!("{}.json", task_input.task_id));
                        let seal: DependencyInputSeal = read_private_json(
                            &seal_path,
                            MAX_DEPENDENCY_INPUT_BYTES as u64,
                            "dependency input seal",
                        )
                        .map_err(|err| {
                            OperationsError::ReceiptInvalidated(format!(
                                "non-root predecessor `{}` dependency input seal at `{}` could not be read: {err}",
                                task_input.task_id,
                                seal_path.display()
                            ))
                        })?;
                        seal.verify_integrity()?;
                        if &seal.seal_digest != expected_seal_digest
                            || &seal.snapshot_ref != expected_snapshot_ref
                            || seal.approval_id != request.policy.approval_id
                            || seal.approval_subject_digest
                                != request.policy.approval_subject_digest
                            || seal.task_id != task_input.task_id
                            || seal.task_digest != task_digest
                            || seal.repository_path != canonical_repo
                            || seal.work_area_root != canonical_work_area
                            || seal.approved_base_commit != approved_base_commit
                            || seal.execution_base_commit != review_receipt.base_commit
                        {
                            return Err(OperationsError::ReceiptInvalidated(format!(
                                "non-root predecessor `{}` persisted DependencyInputSeal does not match integration receipt `{}` and candidate policy",
                                task_input.task_id, integration_receipt.receipt_id
                            )));
                        }
                        let resolved_snapshot_sha = resolve_exact_commit_sha(
                            &canonical_repo,
                            expected_snapshot_ref,
                            DEFAULT_GIT_COMMAND_TIMEOUT_MS,
                        )?;
                        let resolved_snapshot_tree = resolve_exact_tree_sha(
                            &canonical_repo,
                            &resolved_snapshot_sha,
                            DEFAULT_GIT_COMMAND_TIMEOUT_MS,
                        )?;
                        if resolved_snapshot_sha != seal.execution_base_commit
                            || resolved_snapshot_tree != seal.execution_tree_sha
                        {
                            return Err(OperationsError::ReceiptInvalidated(format!(
                                "non-root predecessor `{}` dependency snapshot ref `{expected_snapshot_ref}` does not match sealed execution base/tree",
                                task_input.task_id
                            )));
                        }
                        for dep_pred in &seal.predecessors {
                            let Some(matching_input) = request
                                .predecessor_tasks
                                .iter()
                                .find(|cand_pred| cand_pred.task_id == dep_pred.task_id)
                            else {
                                return Err(OperationsError::ReceiptInvalidated(format!(
                                    "non-root predecessor `{}` depends on sealed predecessor `{}` which is missing from candidate predecessor_tasks",
                                    task_input.task_id, dep_pred.task_id
                                )));
                            };
                            if matching_input.task_digest != dep_pred.task_digest
                                || matching_input.task_worktree_id != dep_pred.task_worktree_id
                                || matching_input.review_receipt_id != dep_pred.review_receipt_id
                                || matching_input.integration_receipt_id
                                    == dep_pred.integration_receipt_id
                            {
                                return Err(OperationsError::ReceiptInvalidated(format!(
                                    "non-root predecessor `{}` sealed predecessor `{}` proof does not match candidate predecessor_tasks entry",
                                    task_input.task_id, dep_pred.task_id
                                )));
                            }
                            let pred_manifest = load_and_verify_task_manifest(
                                &canonical_work_area,
                                &dep_pred.task_worktree_id,
                            )?;
                            let pred_review = load_review_receipt(
                                &canonical_work_area,
                                &dep_pred.review_receipt_id,
                            )?;
                            let pred_orig_int = load_integration_receipt(
                                &canonical_work_area,
                                &dep_pred.integration_receipt_id,
                            )?;
                            if pred_manifest.task_id.as_deref()
                                != Some(pred_review.task_id.as_str())
                                || pred_manifest.task_id.as_deref()
                                    != Some(pred_orig_int.task_id.as_str())
                                || pred_review.run_id != pred_manifest.run_id
                                || pred_review.task_worktree_id != dep_pred.task_worktree_id
                                || pred_review.subject_digest != dep_pred.review_subject_digest
                                || pred_review.receipt_digest != dep_pred.review_receipt_digest
                                || pred_review.task_head_commit
                                    != dep_pred.accepted_task_head_commit
                                || pred_orig_int.job_aggregation.is_some()
                                || pred_orig_int.run_id != pred_manifest.run_id
                                || pred_orig_int.task_worktree_id != dep_pred.task_worktree_id
                                || pred_orig_int.receipt_digest
                                    != dep_pred.integration_receipt_digest
                                || pred_orig_int.review_receipt_id != dep_pred.review_receipt_id
                                || pred_orig_int.review_receipt_digest
                                    != dep_pred.review_receipt_digest
                                || pred_orig_int.review_subject_digest
                                    != dep_pred.review_subject_digest
                                || pred_orig_int.accepted_task_head_commit
                                    != dep_pred.accepted_task_head_commit
                            {
                                return Err(OperationsError::ReceiptInvalidated(format!(
                                    "non-root predecessor `{}` sealed predecessor `{}` persisted manifest or receipts failed verification",
                                    task_input.task_id, dep_pred.task_id
                                )));
                            }
                        }
                    }
                    let current_scope = (
                        agg.job_id.clone(),
                        agg.job_revision,
                        agg.approval_id.clone(),
                        agg.approval_subject_digest.clone(),
                        agg.target_integration_run_id.clone(),
                        agg.job_aggregate_allowed_write_paths.clone(),
                    );
                    if let Some(ref existing_scope) = observed_job_aggregation_scope {
                        if existing_scope != &current_scope {
                            return Err(OperationsError::ReceiptInvalidated(format!(
                                "predecessor `{}` integration receipt `{}` job_aggregation scope diverges from other predecessors in the candidate",
                                task_input.task_id, integration_receipt.receipt_id
                            )));
                        }
                    } else {
                        observed_job_aggregation_scope = Some(current_scope);
                    }
                }
                None => {
                    if observed_job_aggregation_scope.is_some() {
                        return Err(OperationsError::ReceiptInvalidated(
                            "cannot mix legacy same-run and job-aggregated integration receipts on the same RunIntegration worktree"
                                .to_string(),
                        ));
                    }
                    saw_legacy_same_run_receipt = true;
                    if integration_receipt.run_id != integration_manifest.run_id {
                        return Err(OperationsError::ReceiptInvalidated(format!(
                            "predecessor `{}` integration receipt `{}` has mismatched RunIntegration metadata",
                            task_input.task_id, integration_receipt.receipt_id
                        )));
                    }
                }
            }
            let replace_latest = match latest_run_integration_receipt {
                None => true,
                Some(ref current_latest) => {
                    if integration_receipt.integrated_at_ms > current_latest.integrated_at_ms {
                        true
                    } else if integration_receipt.integrated_at_ms
                        == current_latest.integrated_at_ms
                    {
                        is_commit_ancestor(
                            &canonical_repo,
                            &current_latest.integrated_target_commit,
                            &integration_receipt.integrated_target_commit,
                            DEFAULT_GIT_COMMAND_TIMEOUT_MS,
                        )?
                    } else {
                        false
                    }
                }
            };
            if replace_latest {
                latest_run_integration_receipt = Some(integration_receipt.clone());
            }
        }

        for scope_entry in &review_receipt.allowed_write_paths {
            aggregate_allowed_write_paths.insert(scope_entry.clone());
        }
        for changed in &live_task_subject.changed_paths {
            union_task_changed_paths.insert(changed.path.clone());
            if let Some(ref orig) = changed.orig_path {
                union_task_changed_paths.insert(orig.clone());
            }
        }

        verified_predecessors.push(VerifiedCandidateTaskEvidence {
            task_id: task_input.task_id.clone(),
            task_digest,
            run_id: review_receipt.run_id.clone(),
            task_worktree_id: task_input.task_worktree_id.clone(),
            integration_worktree_id: integration_receipt.integration_worktree_id.clone(),
            task_base_commit: review_receipt.base_commit.clone(),
            accepted_task_head_commit: integration_receipt.accepted_task_head_commit.clone(),
            accepted_task_tree_sha: review_receipt.task_tree_sha.clone(),
            integrated_target_commit: integration_receipt.integrated_target_commit.clone(),
            integrated_tree_sha: integration_receipt.integrated_tree_sha.clone(),
            review_subject_digest: review_receipt.subject_digest.clone(),
            review_receipt_id: review_receipt.receipt_id.clone(),
            review_receipt_digest: review_receipt.receipt_digest.clone(),
            integration_receipt_id: integration_receipt.receipt_id.clone(),
            integration_receipt_digest: integration_receipt.receipt_digest.clone(),
            integration_strategy: integration_receipt.strategy,
            allowed_write_paths: review_receipt.allowed_write_paths.clone(),
        });
    }

    // Ensure at least one verified integration receipt targeted this RunIntegration worktree and
    // that the RunIntegration HEAD matches the latest verified integration receipt on this worktree
    // (preventing unreviewed manual commits on the integration branch or omitted integration receipts).
    let Some(latest_int_receipt) = latest_run_integration_receipt else {
        return Err(OperationsError::ReceiptInvalidated(format!(
            "no supplied integration receipt targets RunIntegration worktree `{}`",
            integration_manifest.worktree_id
        )));
    };
    if latest_int_receipt.integrated_target_commit != candidate_head_commit
        || latest_int_receipt.integrated_tree_sha != candidate_tree_sha
    {
        return Err(OperationsError::ReceiptInvalidated(format!(
            "RunIntegration worktree `{}` HEAD (`{candidate_head_commit}`, tree `{candidate_tree_sha}`) diverges from latest verified integration receipt `{}` (`{}`, tree `{}`)",
            integration_manifest.worktree_id,
            latest_int_receipt.receipt_id,
            latest_int_receipt.integrated_target_commit,
            latest_int_receipt.integrated_tree_sha
        )));
    }
    ensure_no_unlisted_integration_receipt_for_run_worktree(
        &canonical_work_area,
        &integration_manifest.worktree_id,
        &seen_integration_receipts,
    )?;

    let effective_aggregate_write_paths = match request.expected_aggregate_write_paths {
        Some(ref explicit_scope) => {
            if explicit_scope.is_empty() {
                return Err(OperationsError::InvalidInput(
                    "expected_aggregate_write_paths must not be empty when supplied".to_string(),
                ));
            }
            for entry in explicit_scope {
                validate_repo_relative_path(entry, "expected_aggregate_write_paths entry")?;
                if is_control_config_path(entry) {
                    return Err(OperationsError::SecurityViolation(format!(
                        "expected_aggregate_write_paths entry `{entry}` targets protected control configuration"
                    )));
                }
            }
            let explicit_resolved = resolve_allowed_write_scopes_at_base(
                &canonical_repo,
                &approved_base_commit,
                explicit_scope,
                DEFAULT_GIT_COMMAND_TIMEOUT_MS,
            )?;
            for pred_scope in &aggregate_allowed_write_paths {
                let clean_pred = pred_scope.strip_suffix('/').unwrap_or(pred_scope.as_str());
                if !explicit_scope.contains(pred_scope)
                    && !is_path_allowed_by_scopes(clean_pred, &explicit_resolved)
                {
                    return Err(OperationsError::DiffGateRejected {
                        worktree_id: integration_manifest.worktree_id.clone(),
                        detail: format!(
                            "predecessor allowed_write_paths entry `{pred_scope}` is outside expected_aggregate_write_paths {:?}",
                            explicit_scope
                        ),
                    });
                }
            }
            explicit_scope.clone()
        }
        None => aggregate_allowed_write_paths,
    };

    // Resolve effective_aggregate_write_paths at approved_base_commit and validate the combined diff
    // in the RunIntegration checkout between approved_base_commit and candidate_head_commit.
    let resolved_scopes = resolve_allowed_write_scopes_at_base(
        &canonical_repo,
        &approved_base_commit,
        &effective_aggregate_write_paths,
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )?;

    let changed_paths = collect_and_validate_candidate_diff(
        &integration_checkout,
        &integration_manifest.worktree_id,
        &approved_base_commit,
        &candidate_head_commit,
        &effective_aggregate_write_paths,
        &resolved_scopes,
        &union_task_changed_paths,
    )?;

    let candidate_diff_digest = compute_candidate_diff_digest(
        &integration_checkout,
        &approved_base_commit,
        &candidate_head_commit,
        &changed_paths,
    )?;

    // Re-verify primary repository HEAD and status after inspection to guarantee zero primary mutation.
    let post_source_head =
        resolve_exact_commit_sha(&canonical_repo, "HEAD", DEFAULT_GIT_COMMAND_TIMEOUT_MS)?;
    let post_source_status = capture_porcelain_status(&canonical_repo)?;
    if post_source_head != pre_source_head || post_source_status != pre_source_status {
        return Err(OperationsError::SecurityViolation(
            "primary source repository HEAD or working tree changed during read-only candidate inspection"
                .to_string(),
        ));
    }

    let mut subject = FinalCandidateSubject {
        schema_version: FINAL_CANDIDATE_SUBJECT_SCHEMA_VERSION,
        run_id: integration_manifest.run_id,
        integration_worktree_id: integration_manifest.worktree_id,
        repository_path: canonical_repo,
        repository_common_git_dir: repo_common_git_dir,
        work_area_root: canonical_work_area,
        integration_checkout_path: integration_checkout,
        integration_branch_name: integration_manifest.branch_name,
        source_head_commit: post_source_head,
        approved_base_commit,
        approved_base_tree_sha,
        integration_base_commit,
        candidate_head_commit,
        candidate_tree_sha,
        candidate_diff_digest,
        aggregate_allowed_write_paths: effective_aggregate_write_paths,
        changed_paths,
        predecessors: verified_predecessors,
        policy: request.policy.clone(),
        check_evidence: clean_check_evidence,
        subject_digest: String::new(),
    };
    subject.subject_digest = subject.compute_digest()?;
    subject.verify_integrity()?;
    Ok(subject)
}

/// Re-verifies in read-only mode that a previously frozen [`FinalCandidateSubject`] is structurally
/// valid AND still matches the live `WorktreeKind::RunIntegration` checkout, primary repository
/// `HEAD`, predecessor task worktrees, and persisted review/integration receipts on disk.
pub fn verify_final_candidate_subject_readonly(
    subject: &FinalCandidateSubject,
) -> Result<(), OperationsError> {
    subject.verify_integrity()?;

    let predecessor_inputs: Vec<FinalCandidateTaskInput> = subject
        .predecessors
        .iter()
        .map(|pred| FinalCandidateTaskInput {
            task_id: pred.task_id.clone(),
            task_digest: pred.task_digest.clone(),
            task_worktree_id: pred.task_worktree_id.clone(),
            review_receipt_id: pred.review_receipt_id.clone(),
            integration_receipt_id: pred.integration_receipt_id.clone(),
            expected_execution_base_commit: Some(pred.task_base_commit.clone()),
            expected_allowed_write_paths: Some(pred.allowed_write_paths.clone()),
            expected_provider: None,
            expected_model: None,
            expected_effort: None,
        })
        .collect();

    let live_subject = inspect_final_candidate_readonly(&InspectFinalCandidateRequest {
        repository_path: subject.repository_path.clone(),
        work_area_root: subject.work_area_root.clone(),
        run_id: subject.run_id.clone(),
        integration_worktree_id: subject.integration_worktree_id.clone(),
        approved_base_commit: subject.approved_base_commit.clone(),
        expected_source_head_commit: subject.source_head_commit.clone(),
        expected_candidate_head_commit: Some(subject.candidate_head_commit.clone()),
        expected_candidate_tree_sha: Some(subject.candidate_tree_sha.clone()),
        predecessor_tasks: predecessor_inputs,
        expected_aggregate_write_paths: Some(subject.aggregate_allowed_write_paths.clone()),
        policy: subject.policy.clone(),
        check_evidence: subject.check_evidence.clone(),
    })?;

    if live_subject != *subject {
        return Err(OperationsError::SubjectMismatch(format!(
            "live RunIntegration candidate state (digest `{}`) no longer matches frozen FinalCandidateSubject (digest `{}`)",
            live_subject.subject_digest, subject.subject_digest
        )));
    }
    Ok(())
}

/// Lifecycle state of a final-candidate closure attempt in [`FinalCandidateClosureLedger`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ClosureAttemptState {
    /// Durable reservation persisted before any external closure evaluation call is dispatched.
    Reserved,
    /// External closure evaluation call has been marked in-flight (`Submitted`).
    Submitted,
    /// Attempt reached a terminal structured [`ClosureVerdict`].
    Terminal,
    /// External execution or result persistence failed in an uncertain state; blocks replay across
    /// all command keys until explicit receipt-bound reconciliation.
    OutcomeUnknown,
}

impl ClosureAttemptState {
    /// Returns the canonical snake_case wire label for this state.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Reserved => "reserved",
            Self::Submitted => "submitted",
            Self::Terminal => "terminal",
            Self::OutcomeUnknown => "outcome_unknown",
        }
    }
}

/// Compact structured verdict classification for a final combined candidate closure attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ClosureVerdictKind {
    /// Final combined candidate satisfied closure review and all required checks.
    Accepted,
    /// Final combined candidate was rejected by closure review.
    Rejected,
    /// Closure evaluation was blocked by an environmental, policy, or precondition constraint.
    Blocked,
    /// Closure evaluation completed without a clear accept/reject determination.
    Unclear,
}

impl ClosureVerdictKind {
    /// Returns the canonical snake_case wire label for this verdict kind.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Accepted => "accepted",
            Self::Rejected => "rejected",
            Self::Blocked => "blocked",
            Self::Unclear => "unclear",
        }
    }
}

/// Actual execution evidence bound to a recorded [`ClosureVerdict`].
///
/// Verified pinned read-only CLI launch configuration bound into [`ClosureExecutionEvidence`] for
/// `codex_cli` closure runs whose structured JSONL output omits runtime model/effort telemetry.
///
/// This record stores only requested launch configuration and the SHA-256 digest of the constructed
/// argv vector; it never claims runtime-observed model/effort telemetry and never persists raw argv
/// or prompt strings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PinnedClosureLaunchConfiguration {
    /// Exact requested model identifier passed on the pinned CLI command line.
    pub requested_model: String,
    /// Explicit reasoning effort passed on the pinned CLI command line.
    pub requested_effort: String,
    /// 64-character lowercase hex SHA-256 digest of the JSON-serialized constructed argv vector.
    pub constructed_argv_sha256: String,
}

/// Recording a positive verdict (`ClosureVerdictKind::Accepted`) requires verified execution
/// telemetry matching [`FinalCandidatePolicyBinding`] and explicitly sets both
/// `grants_merge_or_push_authority` and `substitutes_for_required_checks` to `false`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClosureExecutionEvidence {
    /// Unique execution evidence identifier (`cev_...` or validated simple identifier).
    pub evidence_id: String,
    /// Runtime run identifier (`run_...`) or controller session identifier that produced this evidence.
    pub producer_run_id: String,
    /// Optional worker identifier (`wrk_...`) if produced by a supervised worker process.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub producer_worker_id: Option<String>,
    /// Durable handoff or turn receipt identifier (`hnd_...` / `rcp_...`).
    pub handoff_receipt_id: String,
    /// Observed provider identifier verified from structured execution telemetry.
    pub observed_provider: String,
    /// Observed model identifier verified from structured execution telemetry, if emitted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_model: Option<String>,
    /// Observed reasoning effort or variant verified from structured execution telemetry, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_effort: Option<String>,
    /// 64-character lowercase hex SHA-256 digest of the structured execution event/transcript payload.
    pub transcript_or_event_digest: String,
    /// Always `false`: a closure verdict never grants merge or push authority.
    pub grants_merge_or_push_authority: bool,
    /// Always `false`: a closure verdict never substitutes for required formatting, Clippy, or test checks.
    pub substitutes_for_required_checks: bool,
    /// Verified pinned read-only CLI launch configuration for `codex_cli` closure runs whose
    /// structured JSONL output omits runtime model/effort telemetry.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pinned_launch_configuration: Option<PinnedClosureLaunchConfiguration>,
}

impl ClosureExecutionEvidence {
    /// Validates structural fields, boundary flags, and compliance with `subject.policy`.
    pub fn validate_against_subject(
        &self,
        subject: &FinalCandidateSubject,
    ) -> Result<(), OperationsError> {
        validate_simple_identifier(&self.evidence_id, "execution_evidence.evidence_id")?;
        validate_simple_identifier(&self.producer_run_id, "execution_evidence.producer_run_id")?;
        if let Some(ref worker_id) = self.producer_worker_id {
            validate_simple_identifier(worker_id, "execution_evidence.producer_worker_id")?;
        }
        validate_simple_identifier(
            &self.handoff_receipt_id,
            "execution_evidence.handoff_receipt_id",
        )?;
        validate_simple_identifier(
            &self.observed_provider,
            "execution_evidence.observed_provider",
        )?;
        if let Some(ref model) = self.observed_model {
            validate_closure_model_identifier(model, "execution_evidence.observed_model")?;
        }
        if let Some(ref effort) = self.observed_effort {
            validate_simple_identifier(effort, "execution_evidence.observed_effort")?;
        }
        if let Some(ref pin) = self.pinned_launch_configuration {
            validate_closure_model_identifier(
                &pin.requested_model,
                "execution_evidence.pinned_launch_configuration.requested_model",
            )?;
            validate_simple_identifier(
                &pin.requested_effort,
                "execution_evidence.pinned_launch_configuration.requested_effort",
            )?;
            let _ = validate_sha256_hex_digest(
                &pin.constructed_argv_sha256,
                "execution_evidence.pinned_launch_configuration.constructed_argv_sha256",
            )?;
        }
        let _ = validate_sha256_hex_digest(
            &self.transcript_or_event_digest,
            "execution_evidence.transcript_or_event_digest",
        )?;

        if self.grants_merge_or_push_authority {
            return Err(OperationsError::SecurityViolation(
                "closure execution evidence must never set grants_merge_or_push_authority = true"
                    .to_string(),
            ));
        }
        if self.substitutes_for_required_checks {
            return Err(OperationsError::SecurityViolation(
                "closure execution evidence must never set substitutes_for_required_checks = true"
                    .to_string(),
            ));
        }

        if self.observed_provider != subject.policy.requested_provider {
            return Err(OperationsError::UncertainWorkerOutcome(format!(
                "closure observed_provider `{}` does not match requested_provider `{}`",
                self.observed_provider, subject.policy.requested_provider
            )));
        }
        if let Some(ref pin) = self.pinned_launch_configuration {
            if self.observed_provider != "codex_cli" {
                return Err(OperationsError::UncertainWorkerOutcome(format!(
                    "closure pinned_launch_configuration is only permitted for `codex_cli`, found `{}`",
                    self.observed_provider
                )));
            }
            if pin.requested_model != subject.policy.requested_model {
                return Err(OperationsError::UncertainWorkerOutcome(format!(
                    "closure pinned_launch_configuration.requested_model `{}` does not match requested_model `{}`",
                    pin.requested_model, subject.policy.requested_model
                )));
            }
            if Some(pin.requested_effort.as_str()) != subject.policy.requested_effort.as_deref() {
                return Err(OperationsError::UncertainWorkerOutcome(format!(
                    "closure pinned_launch_configuration.requested_effort `{}` does not match requested_effort `{:?}`",
                    pin.requested_effort, subject.policy.requested_effort
                )));
            }
        }
        match self.observed_model.as_deref() {
            Some(obs_model) if obs_model == subject.policy.requested_model => {}
            Some(obs_model) => {
                return Err(OperationsError::UncertainWorkerOutcome(format!(
                    "closure observed_model `{obs_model}` does not match requested_model `{}`",
                    subject.policy.requested_model
                )));
            }
            None if self.observed_provider == "codex_cli"
                && self.pinned_launch_configuration.is_some() => {}
            None => {
                return Err(OperationsError::UncertainWorkerOutcome(format!(
                    "missing observed_model for requested_model `{}` on provider `{}`",
                    subject.policy.requested_model, self.observed_provider
                )));
            }
        }
        if let Some(ref obs_effort) = self.observed_effort {
            if Some(obs_effort.as_str()) != subject.policy.requested_effort.as_deref() {
                return Err(OperationsError::UncertainWorkerOutcome(format!(
                    "closure observed_effort `{obs_effort}` does not match requested_effort `{:?}`",
                    subject.policy.requested_effort
                )));
            }
        }
        if subject.policy.require_effort_evidence {
            let expected_effort =
                subject.policy.requested_effort.as_deref().ok_or_else(|| {
                    OperationsError::InvalidInput(
                        "subject.policy.requested_effort is missing while require_effort_evidence is true"
                            .to_string(),
                    )
                })?;
            match self.observed_effort.as_deref() {
                Some(obs) if obs == expected_effort => {}
                Some(obs) => {
                    return Err(OperationsError::UncertainWorkerOutcome(format!(
                        "closure observed_effort `{obs}` does not match requested_effort `{expected_effort}`"
                    )));
                }
                None if self.observed_provider == "codex_cli"
                    && self
                        .pinned_launch_configuration
                        .as_ref()
                        .is_some_and(|pin| pin.requested_effort == expected_effort) => {}
                None => {
                    return Err(OperationsError::UncertainWorkerOutcome(format!(
                        "missing observed_effort for requested_effort `{expected_effort}` while require_effort_evidence is true"
                    )));
                }
            }
        }
        Ok(())
    }
}

/// Compact structured closure verdict bound to an exact [`FinalCandidateSubject`] and [`ClosureExecutionEvidence`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClosureVerdict {
    /// Schema version (`FINAL_CANDIDATE_CLOSURE_SCHEMA_VERSION`).
    pub schema_version: u32,
    /// Unique verdict identifier (`cvd_...`).
    pub verdict_id: String,
    /// Attempt identifier (`cat_...`) that produced this verdict.
    pub attempt_id: String,
    /// Bound 64-hex [`FinalCandidateSubject::subject_digest`].
    pub subject_digest: String,
    /// Bound 40-hex [`FinalCandidateSubject::approved_base_commit`].
    pub approved_base_commit: String,
    /// Bound 40-hex [`FinalCandidateSubject::candidate_head_commit`].
    pub candidate_head_commit: String,
    /// Bound 40-hex [`FinalCandidateSubject::candidate_tree_sha`].
    pub candidate_tree_sha: String,
    /// Bound 64-hex [`FinalCandidateSubject::candidate_diff_digest`].
    pub candidate_diff_digest: String,
    /// Bound policy digest from [`FinalCandidatePolicyBinding::policy_digest`].
    pub policy_digest: String,
    /// Bound scope revision from [`FinalCandidatePolicyBinding::scope_revision`].
    pub scope_revision: u64,
    /// Bound registry revision from [`FinalCandidatePolicyBinding::registry_revision`].
    pub registry_revision: u64,
    /// Structured verdict classification (`accepted`, `rejected`, `blocked`, `unclear`).
    pub kind: ClosureVerdictKind,
    /// Sanitized, bounded summary note describing the closure decision.
    pub summary: String,
    /// Verified execution evidence bound to this verdict.
    pub execution_evidence: ClosureExecutionEvidence,
    /// Wall-clock timestamp (milliseconds since Unix epoch) when the verdict was recorded.
    pub recorded_at_ms: i64,
    /// Canonical 64-character lowercase hex SHA-256 digest of this verdict.
    pub verdict_digest: String,
}

#[derive(Serialize)]
struct ClosureVerdictDigestPayload<'a> {
    schema_version: u32,
    verdict_id: &'a str,
    attempt_id: &'a str,
    subject_digest: &'a str,
    approved_base_commit: &'a str,
    candidate_head_commit: &'a str,
    candidate_tree_sha: &'a str,
    candidate_diff_digest: &'a str,
    policy_digest: &'a str,
    scope_revision: u64,
    registry_revision: u64,
    kind: ClosureVerdictKind,
    summary: &'a str,
    execution_evidence: &'a ClosureExecutionEvidence,
    recorded_at_ms: i64,
}

impl ClosureVerdict {
    /// Computes the canonical 64-character lowercase hex SHA-256 digest of this verdict.
    pub fn compute_digest(&self) -> Result<String, OperationsError> {
        let payload = ClosureVerdictDigestPayload {
            schema_version: self.schema_version,
            verdict_id: &self.verdict_id,
            attempt_id: &self.attempt_id,
            subject_digest: &self.subject_digest,
            approved_base_commit: &self.approved_base_commit,
            candidate_head_commit: &self.candidate_head_commit,
            candidate_tree_sha: &self.candidate_tree_sha,
            candidate_diff_digest: &self.candidate_diff_digest,
            policy_digest: &self.policy_digest,
            scope_revision: self.scope_revision,
            registry_revision: self.registry_revision,
            kind: self.kind,
            summary: &self.summary,
            execution_evidence: &self.execution_evidence,
            recorded_at_ms: self.recorded_at_ms,
        };
        let encoded = serde_json::to_vec(&payload)?;
        Ok(sha256_hex(&encoded))
    }

    /// Verifies that this verdict is structurally valid and cryptographically bound to `subject`.
    pub fn verify_against_subject(
        &self,
        subject: &FinalCandidateSubject,
    ) -> Result<(), OperationsError> {
        subject.verify_integrity()?;
        if self.schema_version != FINAL_CANDIDATE_CLOSURE_SCHEMA_VERSION {
            return Err(OperationsError::ReceiptInvalidated(format!(
                "unsupported closure verdict schema_version {} (expected {FINAL_CANDIDATE_CLOSURE_SCHEMA_VERSION})",
                self.schema_version
            )));
        }
        validate_simple_identifier(&self.verdict_id, "verdict.verdict_id")?;
        validate_simple_identifier(&self.attempt_id, "verdict.attempt_id")?;
        let _ = validate_sha256_hex_digest(&self.verdict_digest, "verdict.verdict_digest")?;
        if self.summary.trim().is_empty() || self.summary.len() > MAX_REVIEW_NOTE_BYTES {
            return Err(OperationsError::ReceiptInvalidated(
                "closure verdict summary must be non-empty and within byte bounds".to_string(),
            ));
        }

        if self.subject_digest != subject.subject_digest
            || self.approved_base_commit != subject.approved_base_commit
            || self.candidate_head_commit != subject.candidate_head_commit
            || self.candidate_tree_sha != subject.candidate_tree_sha
            || self.candidate_diff_digest != subject.candidate_diff_digest
            || self.policy_digest != subject.policy.policy_digest
            || self.scope_revision != subject.policy.scope_revision
            || self.registry_revision != subject.policy.registry_revision
        {
            return Err(OperationsError::ReceiptInvalidated(format!(
                "closure verdict `{}` does not match FinalCandidateSubject `{}`",
                self.verdict_id, subject.subject_digest
            )));
        }

        self.execution_evidence.validate_against_subject(subject)?;
        if self.kind == ClosureVerdictKind::Accepted {
            if !subject.policy.require_passing_checks || !subject.check_evidence.all_checks_passed()
            {
                return Err(OperationsError::ReceiptInvalidated(
                    "positive closure verdict (`accepted`) requires require_passing_checks == true and all candidate checks to pass"
                        .to_string(),
                ));
            }
            subject.check_evidence.validate(true)?;
        }

        let computed = self.compute_digest()?;
        if computed != self.verdict_digest {
            return Err(OperationsError::ReceiptInvalidated(format!(
                "closure verdict_digest mismatch: expected `{}`, computed `{computed}`",
                self.verdict_digest
            )));
        }
        Ok(())
    }
}

/// Persisted closure attempt record inside [`FinalCandidateClosureLedger`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClosureAttemptRecord {
    /// Unique attempt identifier (`cat_...`).
    pub attempt_id: String,
    /// Idempotency command key bound when reserving the attempt.
    pub command_key: String,
    /// Run identifier owning the `RunIntegration` worktree.
    pub run_id: String,
    /// Owned `RunIntegration` worktree identifier.
    pub integration_worktree_id: String,
    /// Canonical repository path.
    pub repository_path: PathBuf,
    /// Bound 64-hex [`FinalCandidateSubject::subject_digest`].
    pub subject_digest: String,
    /// Bound 40-hex [`FinalCandidateSubject::approved_base_commit`].
    pub approved_base_commit: String,
    /// Bound 40-hex [`FinalCandidateSubject::approved_base_tree_sha`].
    pub approved_base_tree_sha: String,
    /// Bound 40-hex [`FinalCandidateSubject::candidate_head_commit`].
    pub candidate_head_commit: String,
    /// Bound 40-hex [`FinalCandidateSubject::candidate_tree_sha`].
    pub candidate_tree_sha: String,
    /// Bound 64-hex [`FinalCandidateSubject::candidate_diff_digest`].
    pub candidate_diff_digest: String,
    /// Bound policy digest.
    pub policy_digest: String,
    /// Bound scope revision.
    pub scope_revision: u64,
    /// Bound registry revision.
    pub registry_revision: u64,
    /// Requested provider identifier.
    pub requested_provider: String,
    /// Requested model identifier.
    pub requested_model: String,
    /// Requested reasoning effort or variant, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requested_effort: Option<String>,
    /// Current lifecycle state (`reserved`, `submitted`, `terminal`, `outcome_unknown`).
    pub state: ClosureAttemptState,
    /// Optional external request/correlation identifier recorded when transitioning to `Submitted`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub external_request_id: Option<String>,
    /// Optional sanitized diagnostic recorded when transitioning to `OutcomeUnknown`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uncertainty_reason: Option<String>,
    /// Optional receipt identifier recorded when an uncertain attempt is explicitly reconciled.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reconciliation_receipt_id: Option<String>,
    /// Structured terminal verdict when `state == ClosureAttemptState::Terminal`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verdict: Option<ClosureVerdict>,
    /// Bounded local terminal failure evidence when `state == ClosureAttemptState::Terminal` and `verdict.is_none()`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub terminal_failure: Option<ClosureTerminalFailure>,
    /// Wall-clock timestamp (ms since Unix epoch) when the attempt was reserved.
    pub reserved_at_ms: i64,
    /// Wall-clock timestamp (ms since Unix epoch) when the attempt was last updated.
    pub updated_at_ms: i64,
}

/// Bounded terminal failure evidence recorded when a local closure worker or response validation
/// fails terminally without producing a verified structured [`ClosureVerdict`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClosureTerminalFailure {
    /// Machine-readable terminal failure classification (`validate_simple_identifier`).
    pub failure_code: String,
    /// Sanitized diagnostic explaining the local terminal failure.
    pub reason: String,
    /// Observed subprocess exit code when the worker exited locally.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
    /// Observed provider identifier if reported by the local supervisor (never fabricated).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_provider: Option<String>,
    /// Observed model identifier if reported by the local supervisor (never fabricated).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_model: Option<String>,
    /// Observed effort identifier if reported by the local supervisor (never fabricated).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_effort: Option<String>,
    /// 64-character lowercase hex SHA-256 digest of the raw response payload if captured.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response_sha256_digest: Option<String>,
    /// Whether valid late worker output was fenced from promotion due to active hold, revision drift, or subject drift.
    #[serde(default)]
    pub fenced_late_output: bool,
    /// Wall-clock timestamp (ms since Unix epoch) when the terminal failure was recorded.
    pub recorded_at_ms: i64,
}

/// Request to transition an active (`Reserved` or `Submitted`) closure attempt to
/// [`ClosureAttemptState::Terminal`] with bounded [`ClosureTerminalFailure`] evidence and `verdict: None`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecordClosureFailureRequest {
    /// Expected current `ledger_revision` for compare-and-swap.
    pub expected_ledger_revision: u64,
    /// Target `attempt_id`.
    pub attempt_id: String,
    /// Bound `command_key` that reserved the attempt.
    pub command_key: String,
    /// Expected 64-hex `subject_digest`.
    pub expected_subject_digest: String,
    /// Bounded terminal failure evidence.
    pub failure: ClosureTerminalFailure,
}

/// Bounded private closure-attempt ledger persisted at `<work_area_root>/manifests/closures/final-candidate-closures.json`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FinalCandidateClosureLedger {
    /// Schema version (`FINAL_CANDIDATE_CLOSURE_SCHEMA_VERSION`).
    pub schema_version: u32,
    /// Canonical repository path bound to this work area.
    pub repository_path: PathBuf,
    /// Canonical private work-area root directory.
    pub work_area_root: PathBuf,
    /// Monotonically increasing compare-and-swap revision of this ledger (`0` when empty).
    pub ledger_revision: u64,
    /// Bounded list of closure attempt records (`<= MAX_CLOSURE_ATTEMPTS`).
    pub attempts: Vec<ClosureAttemptRecord>,
}

/// Request to reserve a final-candidate closure attempt or reuse a verified terminal verdict for the
/// identical [`FinalCandidateSubject`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReserveClosureAttemptRequest {
    /// Expected current `ledger_revision` for compare-and-swap (`0` if no ledger exists yet).
    pub expected_ledger_revision: u64,
    /// Client/controller idempotency command key (`validate_simple_identifier`).
    pub command_key: String,
    /// Optional explicit `attempt_id` (`cat_...`); generated if `None`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attempt_id: Option<String>,
    /// Frozen [`FinalCandidateSubject`] to verify against live disk state and bind to the attempt.
    pub subject: FinalCandidateSubject,
    /// Optional legacy `ApprovedCycleBase` tree SHA or subject digest to cross-check so an old
    /// base-tree reservation can never be mistaken for final combined candidate closure.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub legacy_base_tree_digest: Option<String>,
    /// Wall-clock timestamp (ms since Unix epoch).
    pub reserved_at_ms: i64,
}

/// Explicit outcome returned by [`FinalCandidateClosureStore::reserve_closure_attempt`].
///
/// Cache reuse is returned as an explicit [`ClosureReservationOutcome::CacheHit`] decision rather
/// than an implicit new dispatch, so identical subjects never pay for another closure evaluation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "decision", rename_all = "snake_case")]
pub enum ClosureReservationOutcome {
    /// A new closure attempt was reserved in [`ClosureAttemptState::Reserved`].
    Reserved {
        /// New ledger revision after persisting the reservation.
        ledger_revision: u64,
        /// Newly created [`ClosureAttemptRecord`].
        attempt: ClosureAttemptRecord,
    },
    /// An identical retry with the same `command_key` and `subject_digest` matched an existing
    /// [`ClosureAttemptState::Reserved`] record without mutating the ledger.
    IdempotentReserved {
        /// Current ledger revision.
        ledger_revision: u64,
        /// Existing [`ClosureAttemptRecord`].
        attempt: ClosureAttemptRecord,
    },
    /// An existing verified [`ClosureVerdictKind::Accepted`] terminal verdict for the exact same
    /// [`FinalCandidateSubject`] was reused without reserving or dispatching a new closure call.
    CacheHit {
        /// Current ledger revision (unchanged).
        ledger_revision: u64,
        /// Existing terminal [`ClosureAttemptRecord`].
        attempt: ClosureAttemptRecord,
        /// Verified [`ClosureVerdict`] bound to `subject`.
        verdict: Box<ClosureVerdict>,
    },
}

impl ClosureReservationOutcome {
    /// Returns `true` when an existing verified terminal verdict was reused (`CacheHit`).
    pub const fn is_cache_hit(&self) -> bool {
        matches!(self, Self::CacheHit { .. })
    }

    /// Returns the ledger revision associated with this decision.
    pub const fn ledger_revision(&self) -> u64 {
        match self {
            Self::Reserved {
                ledger_revision, ..
            }
            | Self::IdempotentReserved {
                ledger_revision, ..
            }
            | Self::CacheHit {
                ledger_revision, ..
            } => *ledger_revision,
        }
    }

    /// Returns the [`ClosureAttemptRecord`] associated with this decision.
    pub const fn attempt(&self) -> &ClosureAttemptRecord {
        match self {
            Self::Reserved { attempt, .. }
            | Self::IdempotentReserved { attempt, .. }
            | Self::CacheHit { attempt, .. } => attempt,
        }
    }

    /// Returns the reused [`ClosureVerdict`] when this decision is [`Self::CacheHit`].
    pub fn cached_verdict(&self) -> Option<&ClosureVerdict> {
        match self {
            Self::CacheHit { verdict, .. } => Some(verdict.as_ref()),
            Self::Reserved { .. } | Self::IdempotentReserved { .. } => None,
        }
    }
}

/// Request to transition a [`ClosureAttemptState::Reserved`] attempt to [`ClosureAttemptState::Submitted`]
/// before invoking an external closure evaluation provider.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MarkClosureSubmittedRequest {
    /// Expected current `ledger_revision` for compare-and-swap.
    pub expected_ledger_revision: u64,
    /// Target `attempt_id`.
    pub attempt_id: String,
    /// Bound `command_key` that reserved the attempt.
    pub command_key: String,
    /// Expected 64-hex `subject_digest`.
    pub expected_subject_digest: String,
    /// External dispatch/request correlation identifier (`validate_simple_identifier`).
    pub external_request_id: String,
    /// Wall-clock timestamp (ms since Unix epoch).
    pub submitted_at_ms: i64,
}

/// Request to transition an in-flight closure attempt (`Reserved` or `Submitted`) to
/// [`ClosureAttemptState::OutcomeUnknown`] when transport, provider execution, or result
/// persistence fails with an uncertain outcome.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MarkClosureUnknownRequest {
    /// Expected current `ledger_revision` for compare-and-swap.
    pub expected_ledger_revision: u64,
    /// Target `attempt_id`.
    pub attempt_id: String,
    /// Expected 64-hex `subject_digest`.
    pub expected_subject_digest: String,
    /// Sanitized diagnostic describing why the outcome is unknown.
    pub reason: String,
    /// Wall-clock timestamp (ms since Unix epoch).
    pub recorded_at_ms: i64,
}

/// Request to record a structured [`ClosureVerdict`] for an active (`Reserved` or `Submitted`)
/// closure attempt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecordClosureVerdictRequest {
    /// Expected current `ledger_revision` for compare-and-swap.
    pub expected_ledger_revision: u64,
    /// Target `attempt_id`.
    pub attempt_id: String,
    /// Bound `command_key` that reserved the attempt.
    pub command_key: String,
    /// Optional explicit `verdict_id` (`cvd_...`); generated if `None`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verdict_id: Option<String>,
    /// Frozen [`FinalCandidateSubject`] (re-verified against live disk state before recording).
    pub subject: FinalCandidateSubject,
    /// Structured verdict classification (`accepted`, `rejected`, `blocked`, `unclear`).
    pub kind: ClosureVerdictKind,
    /// Sanitized summary note.
    pub summary: String,
    /// Actual supplied execution evidence (must not grant merge/push authority or substitute for checks).
    pub execution_evidence: ClosureExecutionEvidence,
    /// Wall-clock timestamp (ms since Unix epoch).
    pub recorded_at_ms: i64,
}

/// Explicit receipt-bound reconciliation proof for an unresolved (`Reserved`, `Submitted`, or
/// `OutcomeUnknown`) closure attempt.
///
/// Reconciliation never infers execution outcome from raw model prose; it requires a structured
/// receipt identifier, a 64-hex SHA-256 receipt digest, and verified [`ClosureExecutionEvidence`]
/// bound to the exact [`FinalCandidateSubject`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReconcileClosureAttemptRequest {
    /// Expected current `ledger_revision` for compare-and-swap.
    pub expected_ledger_revision: u64,
    /// Target `attempt_id` to reconcile.
    pub attempt_id: String,
    /// Durable reconciliation receipt identifier (`rcp_...` / `hnd_...`).
    pub reconciliation_receipt_id: String,
    /// 64-character lowercase hex SHA-256 digest of the durable reconciliation receipt.
    pub reconciliation_receipt_digest: String,
    /// Frozen [`FinalCandidateSubject`] (re-verified against live disk state).
    pub subject: FinalCandidateSubject,
    /// Reconciled terminal verdict kind (`accepted`, `rejected`, `blocked`, `unclear`).
    pub resolved_kind: ClosureVerdictKind,
    /// Structured summary note from the verified receipt.
    pub summary: String,
    /// Verified execution evidence from the receipt.
    pub execution_evidence: ClosureExecutionEvidence,
    /// Wall-clock timestamp (ms since Unix epoch).
    pub reconciled_at_ms: i64,
}

/// Cross-process locked, compare-and-swap closure-attempt ledger store for an owned `work_area_root`.
#[derive(Debug, Clone)]
pub struct FinalCandidateClosureStore {
    repository_path: PathBuf,
    work_area_root: PathBuf,
}

impl FinalCandidateClosureStore {
    /// Opens a [`FinalCandidateClosureStore`] bound to `repository_path` and `work_area_root`.
    pub fn open(repository_path: &Path, work_area_root: &Path) -> Result<Self, OperationsError> {
        ensure_platform_supported()?;
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
        let canonical_work_area =
            validate_existing_canonical_dir(work_area_root, "work_area_root")?;
        if canonical_work_area != work_area_root {
            return Err(OperationsError::SecurityViolation(
                "work_area_root canonicalization mismatch".to_string(),
            ));
        }
        let repo_common_git_dir = resolve_common_git_dir(&canonical_repo)?;
        verify_work_area_ownership_marker(
            &canonical_work_area,
            &canonical_repo,
            &repo_common_git_dir,
        )?;

        Ok(Self {
            repository_path: canonical_repo,
            work_area_root: canonical_work_area,
        })
    }

    /// Returns the canonical repository path.
    pub fn repository_path(&self) -> &Path {
        &self.repository_path
    }

    /// Returns the canonical private work-area root directory.
    pub fn work_area_root(&self) -> &Path {
        &self.work_area_root
    }

    /// Returns `<work_area_root>/manifests/closures`.
    pub fn closures_dir(&self) -> PathBuf {
        self.work_area_root
            .join(MANIFESTS_SUBDIR)
            .join(CLOSURES_SUBDIR)
    }

    /// Returns `<work_area_root>/manifests/closures/final-candidate-closures.json`.
    pub fn ledger_path(&self) -> PathBuf {
        self.closures_dir().join(CLOSURE_LEDGER_FILENAME)
    }

    /// Returns `<work_area_root>/manifests/closures/final-candidate-closures.lock`.
    pub fn lock_path(&self) -> PathBuf {
        self.closures_dir().join(CLOSURE_LEDGER_LOCK_FILENAME)
    }

    /// Reads the current [`FinalCandidateClosureLedger`] in strictly read-only mode without
    /// creating the closures directory or lock file if absent.
    pub fn inspect_ledger_readonly(&self) -> Result<FinalCandidateClosureLedger, OperationsError> {
        if !self.verify_closures_dir_readonly()? {
            return Ok(self.empty_ledger());
        }
        let lock_path = self.lock_path();
        let maybe_lock = open_existing_private_lock_file(&lock_path, "closure ledger lock")?;
        if let Some(ref lock_file) = maybe_lock {
            lock_file
                .try_lock_shared()
                .map_err(|_| OperationsError::ActiveJob {
                    worktree_id: "final-candidate-closures".to_string(),
                    detail: "closure ledger lock is currently held exclusively".to_string(),
                })?;
        }
        let result = self.read_or_default_ledger_unlocked();
        if let Some(ref lock_file) = maybe_lock {
            let _ = lock_file.unlock();
        }
        result
    }

    /// Reserves a final-candidate closure attempt or returns an explicit [`ClosureReservationOutcome::CacheHit`]
    /// when a verified [`ClosureVerdictKind::Accepted`] verdict already exists for the identical
    /// live [`FinalCandidateSubject`].
    ///
    /// ## Key Invariants
    /// - Re-verifies `request.subject` against live disk state via [`verify_final_candidate_subject_readonly`].
    /// - Rejects any attempt where `legacy_base_tree_digest` equals `candidate_tree_sha` or
    ///   `subject_digest`, ensuring legacy `ApprovedCycleBase` base-tree reservations never satisfy
    ///   final combined candidate closure.
    /// - Enforces `expected_ledger_revision` compare-and-swap.
    /// - Refuses any new or conflicting `command_key` when an unresolved (`Reserved`, `Submitted`,
    ///   or `OutcomeUnknown`) attempt already exists for the same `subject_digest`.
    /// - Returns [`ClosureReservationOutcome::CacheHit`] for an existing verified `Accepted`
    ///   terminal attempt on the identical subject without creating a new attempt.
    pub fn reserve_closure_attempt(
        &self,
        request: &ReserveClosureAttemptRequest,
    ) -> Result<ClosureReservationOutcome, OperationsError> {
        validate_simple_identifier(&request.command_key, "command_key")?;
        if let Some(ref explicit_id) = request.attempt_id {
            validate_simple_identifier(explicit_id, "attempt_id")?;
        }
        if request.subject.repository_path != self.repository_path
            || request.subject.work_area_root != self.work_area_root
        {
            return Err(OperationsError::WorktreeNotOwned(
                "subject repository_path or work_area_root does not match FinalCandidateClosureStore"
                    .to_string(),
            ));
        }

        if let Some(ref legacy_digest) = request.legacy_base_tree_digest {
            verify_legacy_base_closure_insufficient(legacy_digest, &request.subject)?;
        }

        // Re-verify the live RunIntegration candidate and all predecessor proofs on disk.
        verify_final_candidate_subject_readonly(&request.subject)?;

        let lock_file = self.open_exclusive_closure_lock()?;

        let result = (|| -> Result<ClosureReservationOutcome, OperationsError> {
            let mut ledger = self.read_or_default_ledger_unlocked()?;

            // First check if an attempt for this exact subject_digest already exists.
            if let Some(existing) = ledger
                .attempts
                .iter()
                .find(|a| a.subject_digest == request.subject.subject_digest)
                .cloned()
            {
                match existing.state {
                    ClosureAttemptState::Reserved => {
                        if existing.command_key == request.command_key
                            && existing.requested_provider
                                == request.subject.policy.requested_provider
                            && existing.requested_model == request.subject.policy.requested_model
                            && existing.requested_effort == request.subject.policy.requested_effort
                        {
                            if request.expected_ledger_revision != ledger.ledger_revision
                                && request.expected_ledger_revision.saturating_add(1)
                                    != ledger.ledger_revision
                            {
                                return Err(OperationsError::ReceiptInvalidated(format!(
                                    "stale expected_ledger_revision {} (current ledger_revision is {})",
                                    request.expected_ledger_revision, ledger.ledger_revision
                                )));
                            }
                            return Ok(ClosureReservationOutcome::IdempotentReserved {
                                ledger_revision: ledger.ledger_revision,
                                attempt: existing,
                            });
                        }
                        return Err(OperationsError::UncertainWorkerOutcome(format!(
                            "unresolved closure attempt `{}` in state `reserved` already exists for subject `{}` (bound to command_key `{}`); command_key `{}` cannot bypass an unresolved attempt",
                            existing.attempt_id,
                            existing.subject_digest,
                            existing.command_key,
                            request.command_key
                        )));
                    }
                    ClosureAttemptState::Submitted | ClosureAttemptState::OutcomeUnknown => {
                        return Err(OperationsError::UncertainWorkerOutcome(format!(
                            "unresolved closure attempt `{}` in state `{}` exists for subject `{}` (command_key `{}`); reconcile with receipt-bound evidence before retrying",
                            existing.attempt_id,
                            existing.state.as_str(),
                            existing.subject_digest,
                            existing.command_key
                        )));
                    }
                    ClosureAttemptState::Terminal => {
                        if let Some(verdict) = existing.verdict.clone() {
                            verdict.verify_against_subject(&request.subject)?;
                            if verdict.kind == ClosureVerdictKind::Accepted {
                                if request.expected_ledger_revision != ledger.ledger_revision {
                                    return Err(OperationsError::ReceiptInvalidated(format!(
                                        "stale expected_ledger_revision {} on cache reuse check (current ledger_revision is {})",
                                        request.expected_ledger_revision, ledger.ledger_revision
                                    )));
                                }
                                return Ok(ClosureReservationOutcome::CacheHit {
                                    ledger_revision: ledger.ledger_revision,
                                    attempt: existing,
                                    verdict: Box::new(verdict),
                                });
                            }
                            return Err(OperationsError::UncertainWorkerOutcome(format!(
                                "subject `{}` already has a terminal non-accepted closure verdict `{}` (`{}`); candidate tree, scope, check evidence, or predecessor receipts must change before a new closure evaluation",
                                existing.subject_digest,
                                verdict.verdict_id,
                                verdict.kind.as_str()
                            )));
                        }
                        if let Some(ref failure) = existing.terminal_failure {
                            return Err(OperationsError::UncertainWorkerOutcome(format!(
                                "subject `{}` already has a terminal failed closure attempt `{}` (`{}`: {}); reconcile with receipt-bound evidence or produce a new candidate subject before retrying",
                                existing.subject_digest,
                                existing.attempt_id,
                                failure.failure_code,
                                failure.reason
                            )));
                        }
                        return Err(OperationsError::ReceiptInvalidated(format!(
                            "terminal closure attempt `{}` is missing its structured verdict",
                            existing.attempt_id
                        )));
                    }
                }
            }

            // Also reject if the caller reuses a `command_key` that is already bound to a different subject.
            if let Some(conflict) = ledger
                .attempts
                .iter()
                .find(|a| a.command_key == request.command_key)
            {
                return Err(OperationsError::SubjectMismatch(format!(
                    "command_key `{}` is already bound to closure attempt `{}` for subject `{}` (cannot rebind to `{}`)",
                    request.command_key,
                    conflict.attempt_id,
                    conflict.subject_digest,
                    request.subject.subject_digest
                )));
            }

            // Also reject if any other attempt for the same integration_worktree_id is currently
            // unresolved (`Reserved`, `Submitted`, or `OutcomeUnknown`), even if the subject changed!
            if let Some(unresolved) = ledger.attempts.iter().find(|a| {
                a.integration_worktree_id == request.subject.integration_worktree_id
                    && matches!(
                        a.state,
                        ClosureAttemptState::Reserved
                            | ClosureAttemptState::Submitted
                            | ClosureAttemptState::OutcomeUnknown
                    )
            }) {
                return Err(OperationsError::UncertainWorkerOutcome(format!(
                    "RunIntegration worktree `{}` has an unresolved closure attempt `{}` in state `{}` for subject `{}`; reconcile it before reserving a new attempt",
                    request.subject.integration_worktree_id,
                    unresolved.attempt_id,
                    unresolved.state.as_str(),
                    unresolved.subject_digest
                )));
            }

            if request.expected_ledger_revision != ledger.ledger_revision {
                return Err(OperationsError::ReceiptInvalidated(format!(
                    "closure ledger CAS mismatch: expected_ledger_revision {}, actual {}",
                    request.expected_ledger_revision, ledger.ledger_revision
                )));
            }

            if ledger.attempts.len() >= MAX_CLOSURE_ATTEMPTS {
                return Err(OperationsError::InvalidInput(format!(
                    "closure ledger reached maximum capacity ({MAX_CLOSURE_ATTEMPTS} attempts)"
                )));
            }

            let attempt_id = match request.attempt_id {
                Some(ref id) => id.clone(),
                None => format!("cat_{}", &Uuid::new_v4().simple().to_string()[..16]),
            };
            if ledger.attempts.iter().any(|a| a.attempt_id == attempt_id) {
                return Err(OperationsError::InvalidInput(format!(
                    "closure attempt_id `{attempt_id}` already exists in ledger"
                )));
            }

            let attempt = ClosureAttemptRecord {
                attempt_id,
                command_key: request.command_key.clone(),
                run_id: request.subject.run_id.clone(),
                integration_worktree_id: request.subject.integration_worktree_id.clone(),
                repository_path: request.subject.repository_path.clone(),
                subject_digest: request.subject.subject_digest.clone(),
                approved_base_commit: request.subject.approved_base_commit.clone(),
                approved_base_tree_sha: request.subject.approved_base_tree_sha.clone(),
                candidate_head_commit: request.subject.candidate_head_commit.clone(),
                candidate_tree_sha: request.subject.candidate_tree_sha.clone(),
                candidate_diff_digest: request.subject.candidate_diff_digest.clone(),
                policy_digest: request.subject.policy.policy_digest.clone(),
                scope_revision: request.subject.policy.scope_revision,
                registry_revision: request.subject.policy.registry_revision,
                requested_provider: request.subject.policy.requested_provider.clone(),
                requested_model: request.subject.policy.requested_model.clone(),
                requested_effort: request.subject.policy.requested_effort.clone(),
                state: ClosureAttemptState::Reserved,
                external_request_id: None,
                uncertainty_reason: None,
                reconciliation_receipt_id: None,
                verdict: None,
                terminal_failure: None,
                reserved_at_ms: request.reserved_at_ms,
                updated_at_ms: request.reserved_at_ms,
            };

            ledger.ledger_revision = ledger.ledger_revision.saturating_add(1);
            ledger.attempts.push(attempt.clone());
            self.write_ledger_unlocked(&ledger)?;

            Ok(ClosureReservationOutcome::Reserved {
                ledger_revision: ledger.ledger_revision,
                attempt,
            })
        })();

        let _ = lock_file.unlock();
        result
    }

    /// Transitions a [`ClosureAttemptState::Reserved`] attempt to [`ClosureAttemptState::Submitted`]
    /// with compare-and-swap before an external closure call is dispatched.
    ///
    /// If persisting the `Submitted` state fails, the caller must not invoke external inference.
    pub fn mark_closure_attempt_submitted(
        &self,
        request: &MarkClosureSubmittedRequest,
    ) -> Result<(u64, ClosureAttemptRecord), OperationsError> {
        validate_simple_identifier(&request.attempt_id, "attempt_id")?;
        validate_simple_identifier(&request.command_key, "command_key")?;
        let expected_subject_digest = validate_sha256_hex_digest(
            &request.expected_subject_digest,
            "expected_subject_digest",
        )?;
        validate_simple_identifier(&request.external_request_id, "external_request_id")?;

        let lock_file = self.open_exclusive_closure_lock()?;

        let result = (|| -> Result<(u64, ClosureAttemptRecord), OperationsError> {
            let mut ledger = self.read_or_default_ledger_unlocked()?;
            let idx = ledger
                .attempts
                .iter()
                .position(|a| a.attempt_id == request.attempt_id)
                .ok_or_else(|| {
                    OperationsError::InvalidInput(format!(
                        "closure attempt `{}` not found in ledger",
                        request.attempt_id
                    ))
                })?;

            let existing = &ledger.attempts[idx];
            if existing.command_key != request.command_key {
                return Err(OperationsError::UncertainWorkerOutcome(format!(
                    "closure attempt `{}` is bound to command_key `{}`, not `{}`",
                    existing.attempt_id, existing.command_key, request.command_key
                )));
            }
            if existing.subject_digest != expected_subject_digest {
                return Err(OperationsError::SubjectMismatch(format!(
                    "closure attempt `{}` subject_digest `{}` does not match expected `{expected_subject_digest}`",
                    existing.attempt_id, existing.subject_digest
                )));
            }

            if existing.state == ClosureAttemptState::Submitted
                && existing.external_request_id.as_deref()
                    == Some(request.external_request_id.as_str())
            {
                if request.expected_ledger_revision != ledger.ledger_revision
                    && request.expected_ledger_revision.saturating_add(1) != ledger.ledger_revision
                {
                    return Err(OperationsError::ReceiptInvalidated(format!(
                        "closure ledger CAS mismatch on idempotent submit: expected {}, actual {}",
                        request.expected_ledger_revision, ledger.ledger_revision
                    )));
                }
                return Ok((ledger.ledger_revision, existing.clone()));
            }

            if existing.state != ClosureAttemptState::Reserved {
                return Err(OperationsError::UncertainWorkerOutcome(format!(
                    "closure attempt `{}` cannot transition to `submitted` from state `{}`",
                    existing.attempt_id,
                    existing.state.as_str()
                )));
            }
            if request.expected_ledger_revision != ledger.ledger_revision {
                return Err(OperationsError::ReceiptInvalidated(format!(
                    "closure ledger CAS mismatch: expected_ledger_revision {}, actual {}",
                    request.expected_ledger_revision, ledger.ledger_revision
                )));
            }

            let updated = {
                let record = &mut ledger.attempts[idx];
                record.state = ClosureAttemptState::Submitted;
                record.external_request_id = Some(request.external_request_id.clone());
                record.updated_at_ms = request.submitted_at_ms;
                record.clone()
            };
            ledger.ledger_revision = ledger.ledger_revision.saturating_add(1);
            self.write_ledger_unlocked(&ledger)?;
            Ok((ledger.ledger_revision, updated))
        })();

        let _ = lock_file.unlock();
        result
    }

    /// Transitions an active (`Reserved` or `Submitted`) closure attempt to
    /// [`ClosureAttemptState::OutcomeUnknown`] when external execution or result persistence fails
    /// with an uncertain outcome.
    ///
    /// Once in `OutcomeUnknown`, no retry (even with a different `command_key`) can proceed for the
    /// same subject or `RunIntegration` worktree until [`Self::reconcile_closure_attempt`] succeeds.
    pub fn mark_closure_attempt_unknown(
        &self,
        request: &MarkClosureUnknownRequest,
    ) -> Result<(u64, ClosureAttemptRecord), OperationsError> {
        validate_simple_identifier(&request.attempt_id, "attempt_id")?;
        let expected_subject_digest = validate_sha256_hex_digest(
            &request.expected_subject_digest,
            "expected_subject_digest",
        )?;
        let clean_reason = sanitize_and_redact(&request.reason, MAX_REVIEW_NOTE_BYTES);
        if clean_reason.trim().is_empty() {
            return Err(OperationsError::InvalidInput(
                "uncertainty reason must not be empty".to_string(),
            ));
        }

        let lock_file = self.open_exclusive_closure_lock()?;

        let result = (|| -> Result<(u64, ClosureAttemptRecord), OperationsError> {
            let mut ledger = self.read_or_default_ledger_unlocked()?;
            let idx = ledger
                .attempts
                .iter()
                .position(|a| a.attempt_id == request.attempt_id)
                .ok_or_else(|| {
                    OperationsError::InvalidInput(format!(
                        "closure attempt `{}` not found in ledger",
                        request.attempt_id
                    ))
                })?;

            let existing = &ledger.attempts[idx];
            if existing.subject_digest != expected_subject_digest {
                return Err(OperationsError::SubjectMismatch(format!(
                    "closure attempt `{}` subject_digest `{}` does not match expected `{expected_subject_digest}`",
                    existing.attempt_id, existing.subject_digest
                )));
            }
            if existing.state == ClosureAttemptState::Terminal {
                return Err(OperationsError::ReceiptInvalidated(format!(
                    "terminal closure attempt `{}` cannot be marked `outcome_unknown`",
                    existing.attempt_id
                )));
            }
            if request.expected_ledger_revision != ledger.ledger_revision {
                return Err(OperationsError::ReceiptInvalidated(format!(
                    "closure ledger CAS mismatch: expected_ledger_revision {}, actual {}",
                    request.expected_ledger_revision, ledger.ledger_revision
                )));
            }

            let updated = {
                let record = &mut ledger.attempts[idx];
                record.state = ClosureAttemptState::OutcomeUnknown;
                record.uncertainty_reason = Some(clean_reason);
                record.updated_at_ms = request.recorded_at_ms;
                record.clone()
            };
            ledger.ledger_revision = ledger.ledger_revision.saturating_add(1);
            self.write_ledger_unlocked(&ledger)?;
            Ok((ledger.ledger_revision, updated))
        })();

        let _ = lock_file.unlock();
        result
    }

    /// Records a terminal structured [`ClosureVerdict`] for an active (`Reserved` or `Submitted`)
    /// closure attempt after re-verifying the live [`FinalCandidateSubject`] and [`ClosureExecutionEvidence`].
    ///
    /// Attempts in [`ClosureAttemptState::OutcomeUnknown`] cannot be finalized through this method;
    /// they require explicit receipt-bound reconciliation via [`Self::reconcile_closure_attempt`].
    pub fn record_closure_verdict(
        &self,
        request: &RecordClosureVerdictRequest,
    ) -> Result<(u64, ClosureAttemptRecord, ClosureVerdict), OperationsError> {
        validate_simple_identifier(&request.attempt_id, "attempt_id")?;
        validate_simple_identifier(&request.command_key, "command_key")?;
        if let Some(ref explicit_verdict_id) = request.verdict_id {
            validate_simple_identifier(explicit_verdict_id, "verdict_id")?;
        }
        let clean_summary = sanitize_and_redact(&request.summary, MAX_REVIEW_NOTE_BYTES);
        if clean_summary.trim().is_empty() {
            return Err(OperationsError::InvalidInput(
                "closure verdict summary must not be empty".to_string(),
            ));
        }

        // Re-verify the live FinalCandidateSubject and execution evidence before locking/writing.
        verify_final_candidate_subject_readonly(&request.subject)?;
        request
            .execution_evidence
            .validate_against_subject(&request.subject)?;
        if request.kind == ClosureVerdictKind::Accepted {
            if !request.subject.policy.require_passing_checks
                || !request.subject.check_evidence.all_checks_passed()
            {
                return Err(OperationsError::ReceiptInvalidated(
                    "cannot record `accepted` closure verdict without require_passing_checks == true and passing candidate check evidence"
                        .to_string(),
                ));
            }
            request.subject.check_evidence.validate(true)?;
        }

        let lock_file = self.open_exclusive_closure_lock()?;

        let result =
            (|| -> Result<(u64, ClosureAttemptRecord, ClosureVerdict), OperationsError> {
                let mut ledger = self.read_or_default_ledger_unlocked()?;
                let idx = ledger
                    .attempts
                    .iter()
                    .position(|a| a.attempt_id == request.attempt_id)
                    .ok_or_else(|| {
                        OperationsError::InvalidInput(format!(
                            "closure attempt `{}` not found in ledger",
                            request.attempt_id
                        ))
                    })?;

                let existing = &ledger.attempts[idx];
                if existing.command_key != request.command_key {
                    return Err(OperationsError::UncertainWorkerOutcome(format!(
                        "closure attempt `{}` is bound to command_key `{}`, not `{}`",
                        existing.attempt_id, existing.command_key, request.command_key
                    )));
                }
                if existing.subject_digest != request.subject.subject_digest {
                    return Err(OperationsError::SubjectMismatch(format!(
                        "closure attempt `{}` subject_digest `{}` does not match supplied subject `{}`",
                        existing.attempt_id, existing.subject_digest, request.subject.subject_digest
                    )));
                }

                if existing.state == ClosureAttemptState::OutcomeUnknown {
                    return Err(OperationsError::UncertainWorkerOutcome(format!(
                        "closure attempt `{}` is in `outcome_unknown` state; use `reconcile_closure_attempt` with a durable reconciliation receipt",
                        existing.attempt_id
                    )));
                }
                if existing.state == ClosureAttemptState::Terminal {
                    if let Some(ref existing_verdict) = existing.verdict {
                        if existing_verdict.kind == request.kind
                            && existing_verdict.execution_evidence == request.execution_evidence
                            && existing_verdict.summary == clean_summary
                        {
                            return Ok((
                                ledger.ledger_revision,
                                existing.clone(),
                                existing_verdict.clone(),
                            ));
                        }
                    }
                    return Err(OperationsError::ReceiptInvalidated(format!(
                        "closure attempt `{}` is already terminal and cannot be overwritten",
                        existing.attempt_id
                    )));
                }

                if request.expected_ledger_revision != ledger.ledger_revision {
                    return Err(OperationsError::ReceiptInvalidated(format!(
                        "closure ledger CAS mismatch: expected_ledger_revision {}, actual {}",
                        request.expected_ledger_revision, ledger.ledger_revision
                    )));
                }

                let verdict_id = match request.verdict_id {
                    Some(ref id) => id.clone(),
                    None => format!("cvd_{}", &Uuid::new_v4().simple().to_string()[..16]),
                };
                let mut verdict = ClosureVerdict {
                    schema_version: FINAL_CANDIDATE_CLOSURE_SCHEMA_VERSION,
                    verdict_id,
                    attempt_id: existing.attempt_id.clone(),
                    subject_digest: request.subject.subject_digest.clone(),
                    approved_base_commit: request.subject.approved_base_commit.clone(),
                    candidate_head_commit: request.subject.candidate_head_commit.clone(),
                    candidate_tree_sha: request.subject.candidate_tree_sha.clone(),
                    candidate_diff_digest: request.subject.candidate_diff_digest.clone(),
                    policy_digest: request.subject.policy.policy_digest.clone(),
                    scope_revision: request.subject.policy.scope_revision,
                    registry_revision: request.subject.policy.registry_revision,
                    kind: request.kind,
                    summary: clean_summary,
                    execution_evidence: request.execution_evidence.clone(),
                    recorded_at_ms: request.recorded_at_ms,
                    verdict_digest: String::new(),
                };
                verdict.verdict_digest = verdict.compute_digest()?;
                verdict.verify_against_subject(&request.subject)?;

                let updated = {
                    let record = &mut ledger.attempts[idx];
                    record.state = ClosureAttemptState::Terminal;
                    record.uncertainty_reason = None;
                    record.terminal_failure = None;
                    record.verdict = Some(verdict.clone());
                    record.updated_at_ms = request.recorded_at_ms;
                    record.clone()
                };
                ledger.ledger_revision = ledger.ledger_revision.saturating_add(1);
                self.write_ledger_unlocked(&ledger)?;
                Ok((ledger.ledger_revision, updated, verdict))
            })();

        let _ = lock_file.unlock();
        result
    }

    /// Transitions an active (`Reserved` or `Submitted`) closure attempt to
    /// [`ClosureAttemptState::Terminal`] with bounded [`ClosureTerminalFailure`] evidence and `verdict: None`.
    ///
    /// Used when a local closure worker exits with a known terminal error or fails structured
    /// response/subject validation, without fabricating a [`ClosureVerdict`] or marking the
    /// external outcome as permanently unknown.
    pub fn record_closure_failure(
        &self,
        request: &RecordClosureFailureRequest,
    ) -> Result<(u64, ClosureAttemptRecord), OperationsError> {
        validate_simple_identifier(&request.attempt_id, "attempt_id")?;
        validate_simple_identifier(&request.command_key, "command_key")?;
        let expected_subject_digest = validate_sha256_hex_digest(
            &request.expected_subject_digest,
            "expected_subject_digest",
        )?;
        validate_simple_identifier(&request.failure.failure_code, "failure_code")?;
        let clean_reason = sanitize_and_redact(&request.failure.reason, MAX_REVIEW_NOTE_BYTES);
        if clean_reason.trim().is_empty() {
            return Err(OperationsError::InvalidInput(
                "closure terminal failure reason must not be empty".to_string(),
            ));
        }
        if let Some(ref p) = request.failure.observed_provider {
            validate_simple_identifier(p, "observed_provider")?;
        }
        if let Some(ref m) = request.failure.observed_model {
            validate_closure_model_identifier(m, "observed_model")?;
        }
        if let Some(ref e) = request.failure.observed_effort {
            validate_simple_identifier(e, "observed_effort")?;
        }
        let clean_digest = match request.failure.response_sha256_digest {
            Some(ref d) => Some(validate_sha256_hex_digest(d, "response_sha256_digest")?),
            None => None,
        };
        let clean_failure = ClosureTerminalFailure {
            failure_code: request.failure.failure_code.clone(),
            reason: clean_reason,
            exit_code: request.failure.exit_code,
            observed_provider: request.failure.observed_provider.clone(),
            observed_model: request.failure.observed_model.clone(),
            observed_effort: request.failure.observed_effort.clone(),
            response_sha256_digest: clean_digest,
            fenced_late_output: request.failure.fenced_late_output,
            recorded_at_ms: request.failure.recorded_at_ms,
        };

        let lock_file = self.open_exclusive_closure_lock()?;
        let result = (|| -> Result<(u64, ClosureAttemptRecord), OperationsError> {
            let mut ledger = self.read_or_default_ledger_unlocked()?;
            let idx = ledger
                .attempts
                .iter()
                .position(|a| a.attempt_id == request.attempt_id)
                .ok_or_else(|| {
                    OperationsError::InvalidInput(format!(
                        "closure attempt `{}` not found in ledger",
                        request.attempt_id
                    ))
                })?;
            let existing = &ledger.attempts[idx];
            if existing.command_key != request.command_key {
                return Err(OperationsError::UncertainWorkerOutcome(format!(
                    "closure attempt `{}` is bound to command_key `{}`, not `{}`",
                    existing.attempt_id, existing.command_key, request.command_key
                )));
            }
            if existing.subject_digest != expected_subject_digest {
                return Err(OperationsError::SubjectMismatch(format!(
                    "closure attempt `{}` subject_digest `{}` does not match expected `{expected_subject_digest}`",
                    existing.attempt_id, existing.subject_digest
                )));
            }
            if existing.state == ClosureAttemptState::OutcomeUnknown {
                return Err(OperationsError::UncertainWorkerOutcome(format!(
                    "closure attempt `{}` is in `outcome_unknown` state; use `reconcile_closure_attempt`",
                    existing.attempt_id
                )));
            }
            if existing.state == ClosureAttemptState::Terminal {
                if existing.verdict.is_none()
                    && existing.terminal_failure.as_ref() == Some(&clean_failure)
                {
                    return Ok((ledger.ledger_revision, existing.clone()));
                }
                return Err(OperationsError::ReceiptInvalidated(format!(
                    "closure attempt `{}` is already terminal and cannot be overwritten",
                    existing.attempt_id
                )));
            }
            if request.expected_ledger_revision != ledger.ledger_revision {
                return Err(OperationsError::ReceiptInvalidated(format!(
                    "closure ledger CAS mismatch: expected_ledger_revision {}, actual {}",
                    request.expected_ledger_revision, ledger.ledger_revision
                )));
            }
            let updated = {
                let record = &mut ledger.attempts[idx];
                record.state = ClosureAttemptState::Terminal;
                record.uncertainty_reason = None;
                record.verdict = None;
                record.terminal_failure = Some(clean_failure.clone());
                record.updated_at_ms = clean_failure.recorded_at_ms;
                record.clone()
            };
            ledger.ledger_revision = ledger.ledger_revision.saturating_add(1);
            self.write_ledger_unlocked(&ledger)?;
            Ok((ledger.ledger_revision, updated))
        })();
        let _ = lock_file.unlock();
        result
    }

    /// Reconciles an unresolved (`Reserved`, `Submitted`, or `OutcomeUnknown`) closure attempt using
    /// explicit receipt-bound execution evidence (`reconciliation_receipt_id`,
    /// `reconciliation_receipt_digest`, and [`ClosureExecutionEvidence`]).
    ///
    /// Never infers completion from raw model prose.
    pub fn reconcile_closure_attempt(
        &self,
        request: &ReconcileClosureAttemptRequest,
    ) -> Result<(u64, ClosureAttemptRecord, ClosureVerdict), OperationsError> {
        validate_simple_identifier(&request.attempt_id, "attempt_id")?;
        validate_simple_identifier(
            &request.reconciliation_receipt_id,
            "reconciliation_receipt_id",
        )?;
        let _ = validate_sha256_hex_digest(
            &request.reconciliation_receipt_digest,
            "reconciliation_receipt_digest",
        )?;
        if request.execution_evidence.handoff_receipt_id != request.reconciliation_receipt_id {
            return Err(OperationsError::ReceiptInvalidated(format!(
                "reconciliation_receipt_id `{}` must match execution_evidence.handoff_receipt_id `{}`",
                request.reconciliation_receipt_id, request.execution_evidence.handoff_receipt_id
            )));
        }
        let clean_summary = sanitize_and_redact(&request.summary, MAX_REVIEW_NOTE_BYTES);
        if clean_summary.trim().is_empty() {
            return Err(OperationsError::InvalidInput(
                "reconciliation summary must not be empty".to_string(),
            ));
        }

        verify_final_candidate_subject_readonly(&request.subject)?;
        request
            .execution_evidence
            .validate_against_subject(&request.subject)?;
        if request.resolved_kind == ClosureVerdictKind::Accepted {
            if !request.subject.policy.require_passing_checks
                || !request.subject.check_evidence.all_checks_passed()
            {
                return Err(OperationsError::ReceiptInvalidated(
                    "cannot reconcile to `accepted` closure verdict without require_passing_checks == true and passing candidate check evidence"
                        .to_string(),
                ));
            }
            request.subject.check_evidence.validate(true)?;
        }

        let lock_file = self.open_exclusive_closure_lock()?;

        let result =
            (|| -> Result<(u64, ClosureAttemptRecord, ClosureVerdict), OperationsError> {
                let mut ledger = self.read_or_default_ledger_unlocked()?;
                let idx = ledger
                    .attempts
                    .iter()
                    .position(|a| a.attempt_id == request.attempt_id)
                    .ok_or_else(|| {
                        OperationsError::InvalidInput(format!(
                            "closure attempt `{}` not found in ledger",
                            request.attempt_id
                        ))
                    })?;

                let existing = &ledger.attempts[idx];
                if existing.subject_digest != request.subject.subject_digest {
                    return Err(OperationsError::SubjectMismatch(format!(
                        "closure attempt `{}` subject_digest `{}` does not match reconciliation subject `{}`",
                        existing.attempt_id, existing.subject_digest, request.subject.subject_digest
                    )));
                }
                if existing.state == ClosureAttemptState::Terminal && existing.verdict.is_some() {
                    return Err(OperationsError::ReceiptInvalidated(format!(
                        "closure attempt `{}` is already terminal and cannot be reconciled again",
                        existing.attempt_id
                    )));
                }
                if request.expected_ledger_revision != ledger.ledger_revision {
                    return Err(OperationsError::ReceiptInvalidated(format!(
                        "closure ledger CAS mismatch: expected_ledger_revision {}, actual {}",
                        request.expected_ledger_revision, ledger.ledger_revision
                    )));
                }

                let verdict_id = format!("cvd_{}", &Uuid::new_v4().simple().to_string()[..16]);
                let mut verdict = ClosureVerdict {
                    schema_version: FINAL_CANDIDATE_CLOSURE_SCHEMA_VERSION,
                    verdict_id,
                    attempt_id: existing.attempt_id.clone(),
                    subject_digest: request.subject.subject_digest.clone(),
                    approved_base_commit: request.subject.approved_base_commit.clone(),
                    candidate_head_commit: request.subject.candidate_head_commit.clone(),
                    candidate_tree_sha: request.subject.candidate_tree_sha.clone(),
                    candidate_diff_digest: request.subject.candidate_diff_digest.clone(),
                    policy_digest: request.subject.policy.policy_digest.clone(),
                    scope_revision: request.subject.policy.scope_revision,
                    registry_revision: request.subject.policy.registry_revision,
                    kind: request.resolved_kind,
                    summary: clean_summary,
                    execution_evidence: request.execution_evidence.clone(),
                    recorded_at_ms: request.reconciled_at_ms,
                    verdict_digest: String::new(),
                };
                verdict.verdict_digest = verdict.compute_digest()?;
                verdict.verify_against_subject(&request.subject)?;

                let updated = {
                    let record = &mut ledger.attempts[idx];
                    record.state = ClosureAttemptState::Terminal;
                    record.uncertainty_reason = None;
                    record.reconciliation_receipt_id =
                        Some(request.reconciliation_receipt_id.clone());
                    record.verdict = Some(verdict.clone());
                    record.updated_at_ms = request.reconciled_at_ms;
                    record.clone()
                };
                ledger.ledger_revision = ledger.ledger_revision.saturating_add(1);
                self.write_ledger_unlocked(&ledger)?;
                Ok((ledger.ledger_revision, updated, verdict))
            })();

        let _ = lock_file.unlock();
        result
    }

    /// Verifies in read-only mode that `verdict` is recorded as a terminal `Accepted` verdict in
    /// the persisted ledger AND still matches the live `WorktreeKind::RunIntegration` candidate on disk.
    pub fn verify_accepted_closure_verdict_readonly(
        &self,
        subject: &FinalCandidateSubject,
        verdict: &ClosureVerdict,
    ) -> Result<ClosureAttemptRecord, OperationsError> {
        verify_final_candidate_subject_readonly(subject)?;
        verdict.verify_against_subject(subject)?;
        if verdict.kind != ClosureVerdictKind::Accepted {
            return Err(OperationsError::ReceiptInvalidated(format!(
                "closure verdict `{}` has kind `{}`, expected `accepted`",
                verdict.verdict_id,
                verdict.kind.as_str()
            )));
        }

        let ledger = self.inspect_ledger_readonly()?;
        let attempt = ledger
            .attempts
            .into_iter()
            .find(|a| a.attempt_id == verdict.attempt_id)
            .ok_or_else(|| {
                OperationsError::ReceiptInvalidated(format!(
                    "closure attempt `{}` for verdict `{}` is not present in persisted closure ledger",
                    verdict.attempt_id, verdict.verdict_id
                ))
            })?;

        if attempt.state != ClosureAttemptState::Terminal
            || attempt.subject_digest != subject.subject_digest
            || attempt.verdict.as_ref() != Some(verdict)
        {
            return Err(OperationsError::ReceiptInvalidated(format!(
                "persisted closure attempt `{}` does not match supplied verdict `{}`",
                attempt.attempt_id, verdict.verdict_id
            )));
        }
        Ok(attempt)
    }

    fn empty_ledger(&self) -> FinalCandidateClosureLedger {
        FinalCandidateClosureLedger {
            schema_version: FINAL_CANDIDATE_CLOSURE_SCHEMA_VERSION,
            repository_path: self.repository_path.clone(),
            work_area_root: self.work_area_root.clone(),
            ledger_revision: 0,
            attempts: Vec::new(),
        }
    }

    fn verify_closures_dir_readonly(&self) -> Result<bool, OperationsError> {
        let closures_dir = self.closures_dir();
        let _ = validate_absolute_no_symlink_components(&closures_dir, "closures dir")?;

        let manifests_dir = self.work_area_root.join(MANIFESTS_SUBDIR);
        let manifests_meta = fs::symlink_metadata(&manifests_dir).map_err(|err| {
            if err.kind() == std::io::ErrorKind::NotFound {
                OperationsError::WorktreeNotOwned(format!(
                    "manifests directory `{}` is missing",
                    manifests_dir.display()
                ))
            } else {
                OperationsError::Io(err)
            }
        })?;
        if manifests_meta.file_type().is_symlink() || !manifests_meta.file_type().is_dir() {
            return Err(OperationsError::SecurityViolation(format!(
                "manifests directory `{}` must be a non-symlink directory",
                manifests_dir.display()
            )));
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = manifests_meta.permissions().mode() & 0o777;
            if mode & 0o077 != 0 {
                return Err(OperationsError::SecurityViolation(format!(
                    "manifests directory `{}` has non-private permissions {:04o} (expected 0700)",
                    manifests_dir.display(),
                    mode
                )));
            }
        }

        match fs::symlink_metadata(&closures_dir) {
            Ok(meta) => {
                if meta.file_type().is_symlink() || !meta.file_type().is_dir() {
                    return Err(OperationsError::SecurityViolation(format!(
                        "closures directory `{}` must be a non-symlink directory",
                        closures_dir.display()
                    )));
                }
                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;
                    let mode = meta.permissions().mode() & 0o777;
                    if mode & 0o077 != 0 {
                        return Err(OperationsError::SecurityViolation(format!(
                            "closures directory `{}` has non-private permissions {:04o} (expected 0700)",
                            closures_dir.display(),
                            mode
                        )));
                    }
                }
                Ok(true)
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(err) => Err(OperationsError::Io(err)),
        }
    }

    fn open_exclusive_closure_lock(&self) -> Result<fs::File, OperationsError> {
        let _ = ensure_private_dir(&self.closures_dir(), "closures dir")?;
        let lock_file = open_private_lock_file(&self.lock_path(), "closure ledger lock")?;
        lock_file.lock_exclusive().map_err(OperationsError::Io)?;
        Ok(lock_file)
    }

    fn read_or_default_ledger_unlocked(
        &self,
    ) -> Result<FinalCandidateClosureLedger, OperationsError> {
        let path = self.ledger_path();
        let _ = validate_absolute_no_symlink_components(&path, "final candidate closure ledger")?;
        match fs::symlink_metadata(&path) {
            Ok(meta) => {
                if meta.file_type().is_symlink() {
                    return Err(OperationsError::SecurityViolation(format!(
                        "final candidate closure ledger `{}` must not be a symlink",
                        path.display()
                    )));
                }
                if !meta.file_type().is_file() {
                    return Err(OperationsError::SecurityViolation(format!(
                        "final candidate closure ledger `{}` must be a regular file",
                        path.display()
                    )));
                }
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                return Ok(self.empty_ledger());
            }
            Err(err) => return Err(OperationsError::Io(err)),
        }

        let ledger: FinalCandidateClosureLedger = read_private_json(
            &path,
            MAX_CLOSURE_LEDGER_BYTES,
            "final candidate closure ledger",
        )?;
        if ledger.schema_version != FINAL_CANDIDATE_CLOSURE_SCHEMA_VERSION {
            return Err(OperationsError::ReceiptInvalidated(format!(
                "unsupported closure ledger schema_version {} (expected {FINAL_CANDIDATE_CLOSURE_SCHEMA_VERSION})",
                ledger.schema_version
            )));
        }
        if ledger.repository_path != self.repository_path
            || ledger.work_area_root != self.work_area_root
        {
            return Err(OperationsError::ReceiptInvalidated(
                "closure ledger repository_path or work_area_root mismatch".to_string(),
            ));
        }
        if ledger.attempts.len() > MAX_CLOSURE_ATTEMPTS {
            return Err(OperationsError::ReceiptInvalidated(format!(
                "closure ledger contains {} attempts, exceeding MAX_CLOSURE_ATTEMPTS ({MAX_CLOSURE_ATTEMPTS})",
                ledger.attempts.len()
            )));
        }
        Ok(ledger)
    }

    fn write_ledger_unlocked(
        &self,
        ledger: &FinalCandidateClosureLedger,
    ) -> Result<(), OperationsError> {
        let dir = self.closures_dir();
        let _ = write_private_json_atomic(
            &dir,
            CLOSURE_LEDGER_FILENAME,
            ledger,
            "final candidate closure ledger",
        )?;
        Ok(())
    }
}

/// Verifies that a legacy `ApprovedCycleBase` base-tree digest (40-hex tree SHA or 64-hex approval
/// subject digest) is distinct from and cannot satisfy final combined candidate closure for `subject`.
///
/// Returns `Ok(())` when `legacy_base_digest` is distinct from `subject.candidate_tree_sha` and
/// `subject.subject_digest`, or [`OperationsError::SubjectMismatch`] if a caller attempts to equate
/// a base-tree reservation with a final candidate subject.
pub fn verify_legacy_base_closure_insufficient(
    legacy_base_digest: &str,
    subject: &FinalCandidateSubject,
) -> Result<(), OperationsError> {
    subject.verify_integrity()?;
    let trimmed = legacy_base_digest.trim();
    if trimmed.is_empty() {
        return Err(OperationsError::InvalidInput(
            "legacy_base_digest must not be empty".to_string(),
        ));
    }
    if trimmed.eq_ignore_ascii_case(&subject.candidate_tree_sha)
        || trimmed.eq_ignore_ascii_case(&subject.subject_digest)
        || trimmed.eq_ignore_ascii_case(&subject.candidate_head_commit)
    {
        return Err(OperationsError::SubjectMismatch(format!(
            "legacy ApprovedCycleBase digest `{trimmed}` only binds the approved base tree and cannot satisfy final combined candidate closure for subject `{}`",
            subject.subject_digest
        )));
    }
    Ok(())
}

fn load_and_verify_run_integration_manifest(
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

fn verify_work_area_ownership_marker(
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

fn resolve_common_git_dir(checkout_or_repo: &Path) -> Result<PathBuf, OperationsError> {
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

fn capture_porcelain_status(repo_or_checkout: &Path) -> Result<Vec<u8>, OperationsError> {
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

fn ensure_task_receipts_not_superseded(
    canonical_work_area: &Path,
    review_receipt: &ReviewReceipt,
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
        } else if let Some(other_int_id) = name
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
                    "integration receipt `{}` for worktree `{}` has been superseded by `{}`",
                    integration_receipt.receipt_id,
                    integration_receipt.task_worktree_id,
                    other.receipt_id
                )));
            }
        }
    }
    Ok(())
}

fn ensure_no_unlisted_integration_receipt_for_run_worktree(
    canonical_work_area: &Path,
    integration_worktree_id: &str,
    listed_integration_receipt_ids: &BTreeSet<String>,
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
        if let Some(int_id) = name
            .strip_prefix("integration-")
            .and_then(|rest| rest.strip_suffix(".json"))
        {
            let receipt = load_integration_receipt(canonical_work_area, int_id)?;
            if receipt.integration_worktree_id == integration_worktree_id
                && !listed_integration_receipt_ids.contains(&receipt.receipt_id)
            {
                return Err(OperationsError::ReceiptInvalidated(format!(
                    "RunIntegration worktree `{integration_worktree_id}` has persisted integration receipt `{}` (for task `{}`) that was omitted from predecessor_tasks",
                    receipt.receipt_id, receipt.task_id
                )));
            }
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn collect_and_validate_candidate_diff(
    integration_checkout: &Path,
    integration_worktree_id: &str,
    approved_base_commit: &str,
    candidate_head_commit: &str,
    aggregate_allowed_write_paths: &BTreeSet<String>,
    resolved_scopes: &BTreeMap<String, WriteScopeKind>,
    union_task_changed_paths: &BTreeSet<String>,
) -> Result<Vec<ChangedPathEntry>, OperationsError> {
    // 1. Collect baseline tracked symlinks in `approved_base_commit`.
    let ls_tree_out = run_sanitized_git_bytes(
        integration_checkout,
        &["ls-tree", "-r", "-z", approved_base_commit],
        DEFAULT_MAX_GIT_OUTPUT_BYTES,
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )?;
    let ls_tree_text = std::str::from_utf8(&ls_tree_out.stdout_bytes).map_err(|_| {
        OperationsError::DiffGateRejected {
            worktree_id: integration_worktree_id.to_string(),
            detail: "git ls-tree output contained non-UTF-8 path bytes".to_string(),
        }
    })?;
    let mut baseline_symlinks = BTreeSet::new();
    for record in ls_tree_text.split('\0') {
        if record.is_empty() {
            continue;
        }
        if let Some((meta, path)) = record.split_once('\t') {
            if meta.starts_with("120000 ") {
                baseline_symlinks.insert(path.to_string());
            }
        }
    }

    // 2. Collect binary markers from `git diff --numstat --find-renames -z <approved_base_commit> <candidate_head_commit>`.
    let numstat_out = run_sanitized_git_bytes(
        integration_checkout,
        &[
            "diff",
            "--numstat",
            "--find-renames",
            "-z",
            approved_base_commit,
            candidate_head_commit,
        ],
        DEFAULT_MAX_GIT_OUTPUT_BYTES,
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )?;
    let numstat_text = std::str::from_utf8(&numstat_out.stdout_bytes).map_err(|_| {
        OperationsError::DiffGateRejected {
            worktree_id: integration_worktree_id.to_string(),
            detail: "git diff --numstat output contained non-UTF-8 path bytes".to_string(),
        }
    })?;
    let mut binary_paths = BTreeSet::new();
    parse_candidate_numstat_z(numstat_text, &mut binary_paths);

    // 3. Collect changed path entries from `git diff --name-status --find-renames -z <approved_base_commit> <candidate_head_commit>`.
    let name_status_out = run_sanitized_git_bytes(
        integration_checkout,
        &[
            "diff",
            "--name-status",
            "--find-renames",
            "-z",
            approved_base_commit,
            candidate_head_commit,
        ],
        DEFAULT_MAX_GIT_OUTPUT_BYTES,
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )?;
    let name_status_text = std::str::from_utf8(&name_status_out.stdout_bytes).map_err(|_| {
        OperationsError::DiffGateRejected {
            worktree_id: integration_worktree_id.to_string(),
            detail: "git diff --name-status output contained non-UTF-8 path bytes".to_string(),
        }
    })?;
    let mut entries_map: BTreeMap<String, ChangedPathEntry> = BTreeMap::new();
    parse_candidate_name_status_z(
        name_status_text,
        integration_checkout,
        integration_worktree_id,
        &binary_paths,
        &mut entries_map,
    )?;

    // 4. Scan checkout directory for symlinks or non-UTF-8 entries.
    let mut visited_entries = 0usize;
    scan_checkout_for_symlinks(
        integration_checkout,
        integration_checkout,
        0,
        &mut visited_entries,
        &baseline_symlinks,
        integration_worktree_id,
    )?;

    if entries_map.is_empty() {
        return Err(OperationsError::SubjectMismatch(format!(
            "RunIntegration worktree `{integration_worktree_id}` has no changed paths between `{approved_base_commit}` and `{candidate_head_commit}`"
        )));
    }
    if entries_map.len() > DEFAULT_MAX_DIFF_CHANGED_PATHS {
        return Err(OperationsError::DiffGateRejected {
            worktree_id: integration_worktree_id.to_string(),
            detail: format!(
                "changed_paths count ({}) exceeded budget ({DEFAULT_MAX_DIFF_CHANGED_PATHS})",
                entries_map.len()
            ),
        });
    }

    let changed_paths: Vec<ChangedPathEntry> = entries_map.into_values().collect();
    for entry in &changed_paths {
        validate_repo_relative_path(&entry.path, "candidate changed path").map_err(|err| {
            OperationsError::DiffGateRejected {
                worktree_id: integration_worktree_id.to_string(),
                detail: err.to_string(),
            }
        })?;
        if let Some(ref orig) = entry.orig_path {
            validate_repo_relative_path(orig, "candidate rename orig_path").map_err(|err| {
                OperationsError::DiffGateRejected {
                    worktree_id: integration_worktree_id.to_string(),
                    detail: err.to_string(),
                }
            })?;
        }

        if entry.is_symlink || has_symlink_component(integration_checkout, &entry.path) {
            return Err(OperationsError::DiffGateRejected {
                worktree_id: integration_worktree_id.to_string(),
                detail: format!(
                    "candidate changed path `{}` creates or traverses a symbolic link",
                    entry.path
                ),
            });
        }
        if is_control_config_path(&entry.path)
            || entry
                .orig_path
                .as_deref()
                .is_some_and(is_control_config_path)
        {
            return Err(OperationsError::DiffGateRejected {
                worktree_id: integration_worktree_id.to_string(),
                detail: format!(
                    "candidate changed path `{}` modifies protected control configuration",
                    entry.path
                ),
            });
        }

        let path_is_dir = fs::symlink_metadata(integration_checkout.join(&entry.path))
            .is_ok_and(|meta| meta.file_type().is_dir());
        let exact_file_replaced_by_dir = path_is_dir
            && resolved_scopes.get(&entry.path) == Some(&WriteScopeKind::ExactFile)
            && !resolved_scopes.iter().any(|(scope_path, kind)| {
                *kind == WriteScopeKind::DirectorySubtree
                    && Path::new(&entry.path).starts_with(Path::new(scope_path))
            });

        if !is_path_allowed_by_scopes(&entry.path, resolved_scopes) || exact_file_replaced_by_dir {
            return Err(OperationsError::DiffGateRejected {
                worktree_id: integration_worktree_id.to_string(),
                detail: format!(
                    "candidate changed path `{}` is outside aggregate_allowed_write_paths {:?}",
                    entry.path, aggregate_allowed_write_paths
                ),
            });
        }
        if let Some(orig) = entry
            .orig_path
            .as_deref()
            .filter(|orig| !is_path_allowed_by_scopes(orig, resolved_scopes))
        {
            return Err(OperationsError::DiffGateRejected {
                worktree_id: integration_worktree_id.to_string(),
                detail: format!(
                    "candidate rename origin `{orig}` is outside aggregate_allowed_write_paths {:?}",
                    aggregate_allowed_write_paths
                ),
            });
        }

        if !union_task_changed_paths.contains(&entry.path) {
            return Err(OperationsError::SubjectMismatch(format!(
                "candidate changed path `{}` is not present in any verified predecessor task review receipt",
                entry.path
            )));
        }
    }

    Ok(changed_paths)
}

fn parse_candidate_numstat_z(text: &str, binary_paths: &mut BTreeSet<String>) {
    let mut tokens = text.split('\0').peekable();
    while let Some(tok) = tokens.next() {
        if tok.is_empty() {
            continue;
        }
        let mut parts = tok.splitn(3, '\t');
        let Some(added) = parts.next() else {
            continue;
        };
        let Some(deleted) = parts.next() else {
            continue;
        };
        let Some(path_part) = parts.next() else {
            continue;
        };
        let is_bin = added == "-" && deleted == "-";
        if path_part.is_empty() {
            let orig = tokens.next().unwrap_or_default();
            let new_path = tokens.next().unwrap_or_default();
            if is_bin {
                if !orig.is_empty() {
                    binary_paths.insert(orig.to_string());
                }
                if !new_path.is_empty() {
                    binary_paths.insert(new_path.to_string());
                }
            }
        } else if is_bin {
            binary_paths.insert(path_part.to_string());
        }
    }
}

fn parse_candidate_name_status_z(
    text: &str,
    checkout_root: &Path,
    integration_worktree_id: &str,
    binary_paths: &BTreeSet<String>,
    entries_map: &mut BTreeMap<String, ChangedPathEntry>,
) -> Result<(), OperationsError> {
    let mut tokens = text.split('\0').peekable();
    while let Some(status_tok) = tokens.next() {
        if status_tok.is_empty() {
            continue;
        }
        let Some(status_char) = status_tok.chars().next() else {
            continue;
        };
        if status_char == 'R' || status_char == 'C' {
            let orig_path = tokens.next().unwrap_or_default().to_string();
            let new_path = tokens.next().unwrap_or_default().to_string();
            if orig_path.is_empty() || new_path.is_empty() {
                return Err(OperationsError::DiffGateRejected {
                    worktree_id: integration_worktree_id.to_string(),
                    detail: format!(
                        "malformed rename/copy git diff --name-status token `{status_tok}`"
                    ),
                });
            }
            let (fs_bin, is_symlink) = inspect_entry_in_checkout(
                checkout_root,
                &new_path,
                binary_paths.contains(&new_path) || binary_paths.contains(&orig_path),
            );
            entries_map.insert(
                new_path.clone(),
                ChangedPathEntry {
                    path: new_path,
                    orig_path: Some(orig_path),
                    change_kind: if status_char == 'R' {
                        DiffChangeKind::Renamed
                    } else {
                        DiffChangeKind::Copied
                    },
                    is_binary: fs_bin,
                    is_symlink,
                },
            );
        } else {
            let path = tokens.next().unwrap_or_default().to_string();
            if path.is_empty() {
                return Err(OperationsError::DiffGateRejected {
                    worktree_id: integration_worktree_id.to_string(),
                    detail: format!("malformed git diff --name-status token `{status_tok}`"),
                });
            }
            let change_kind = match status_char {
                'A' => DiffChangeKind::Added,
                'M' => DiffChangeKind::Modified,
                'D' => DiffChangeKind::Deleted,
                'T' => DiffChangeKind::TypeChanged,
                other => {
                    return Err(OperationsError::DiffGateRejected {
                        worktree_id: integration_worktree_id.to_string(),
                        detail: format!(
                            "unsupported or unmerged git diff status `{other}` (`{status_tok}`) for path `{path}`"
                        ),
                    });
                }
            };
            let (fs_bin, is_symlink) =
                inspect_entry_in_checkout(checkout_root, &path, binary_paths.contains(&path));
            entries_map.insert(
                path.clone(),
                ChangedPathEntry {
                    path,
                    orig_path: None,
                    change_kind,
                    is_binary: fs_bin,
                    is_symlink: is_symlink || status_char == 'T',
                },
            );
        }
    }
    Ok(())
}

fn inspect_entry_in_checkout(
    checkout_root: &Path,
    rel_path: &str,
    numstat_marked_binary: bool,
) -> (bool, bool) {
    let full = checkout_root.join(rel_path);
    match fs::symlink_metadata(&full) {
        Ok(meta) => {
            let is_symlink = meta.file_type().is_symlink();
            (numstat_marked_binary, is_symlink)
        }
        Err(_) => (numstat_marked_binary, false),
    }
}

fn has_symlink_component(checkout_root: &Path, rel_path: &str) -> bool {
    let mut cur = checkout_root.to_path_buf();
    for comp in Path::new(rel_path).components() {
        cur.push(comp.as_os_str());
        if let Ok(meta) = fs::symlink_metadata(&cur) {
            if meta.file_type().is_symlink() {
                return true;
            }
        }
    }
    false
}

fn scan_checkout_for_symlinks(
    checkout_root: &Path,
    current_dir: &Path,
    depth: usize,
    visited_entries: &mut usize,
    baseline_symlinks: &BTreeSet<String>,
    worktree_id: &str,
) -> Result<(), OperationsError> {
    if depth > DEFAULT_MAX_SCAN_DEPTH {
        return Err(OperationsError::DiffGateRejected {
            worktree_id: worktree_id.to_string(),
            detail: format!("worktree directory depth exceeded {DEFAULT_MAX_SCAN_DEPTH}"),
        });
    }
    let entries = fs::read_dir(current_dir).map_err(OperationsError::Io)?;
    for entry in entries {
        let entry = entry.map_err(OperationsError::Io)?;
        let file_name = entry.file_name();
        if file_name == ".git" {
            continue;
        }
        *visited_entries = visited_entries.saturating_add(1);
        if *visited_entries > DEFAULT_MAX_SCAN_ENTRIES {
            return Err(OperationsError::DiffGateRejected {
                worktree_id: worktree_id.to_string(),
                detail: format!("worktree scan exceeded {DEFAULT_MAX_SCAN_ENTRIES} entries"),
            });
        }
        let path = entry.path();
        let rel = path.strip_prefix(checkout_root).map_err(|_| {
            OperationsError::SecurityViolation("failed to strip checkout_root prefix".to_string())
        })?;
        let Some(rel_str) = rel.to_str() else {
            return Err(OperationsError::DiffGateRejected {
                worktree_id: worktree_id.to_string(),
                detail: "worktree contains non-UTF-8 path".to_string(),
            });
        };
        let meta = fs::symlink_metadata(&path).map_err(OperationsError::Io)?;
        if meta.file_type().is_symlink() {
            if !baseline_symlinks.contains(rel_str) {
                return Err(OperationsError::DiffGateRejected {
                    worktree_id: worktree_id.to_string(),
                    detail: format!("worktree contains symlink `{rel_str}`"),
                });
            }
        } else if meta.file_type().is_dir() {
            scan_checkout_for_symlinks(
                checkout_root,
                &path,
                depth + 1,
                visited_entries,
                baseline_symlinks,
                worktree_id,
            )?;
        }
    }
    Ok(())
}

fn compute_candidate_diff_digest(
    checkout_path: &Path,
    approved_base_commit: &str,
    candidate_head_commit: &str,
    changed_paths: &[ChangedPathEntry],
) -> Result<String, OperationsError> {
    let raw_patch = run_sanitized_git_bytes(
        checkout_path,
        &[
            "diff",
            "--binary",
            "--find-renames",
            "--full-index",
            approved_base_commit,
            candidate_head_commit,
        ],
        DEFAULT_MAX_GIT_OUTPUT_BYTES,
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )?;
    let paths_json = serde_json::to_vec(changed_paths)?;
    let mut hasher = Sha256::new();
    hasher.update(b"threadlane-final-candidate-diff-v1\0");
    hasher.update(approved_base_commit.as_bytes());
    hasher.update(b"\0");
    hasher.update(candidate_head_commit.as_bytes());
    hasher.update(b"\0");
    hasher.update((paths_json.len() as u64).to_le_bytes());
    hasher.update(&paths_json);
    hasher.update((raw_patch.stdout_bytes.len() as u64).to_le_bytes());
    hasher.update(&raw_patch.stdout_bytes);
    Ok(format!("{:x}", hasher.finalize()))
}

fn validate_closure_model_identifier(value: &str, label: &str) -> Result<(), OperationsError> {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_candidate_name_status_z_rejects_unmerged_and_unknown_statuses_fail_closed() {
        let binary_paths = BTreeSet::new();
        let mut entries_map = BTreeMap::new();
        let unmerged_err = parse_candidate_name_status_z(
            "U\0src/alpha.rs\0",
            Path::new("/nonexistent"),
            "wt-run-test-int01",
            &binary_paths,
            &mut entries_map,
        )
        .expect_err("unmerged status U must be rejected fail-closed");
        assert!(matches!(
            unmerged_err,
            OperationsError::DiffGateRejected { .. }
        ));

        let unknown_err = parse_candidate_name_status_z(
            "X\0src/alpha.rs\0",
            Path::new("/nonexistent"),
            "wt-run-test-int01",
            &binary_paths,
            &mut entries_map,
        )
        .expect_err("unknown status X must be rejected fail-closed");
        assert!(matches!(
            unknown_err,
            OperationsError::DiffGateRejected { .. }
        ));
    }
}
