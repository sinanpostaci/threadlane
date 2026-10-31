use std::collections::BTreeSet;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use fs2::FileExt;
use serde::{Deserialize, Serialize};
use threadlane_workers::{
    dependency_input_lock_path, ensure_private_dir, is_commit_ancestor, is_worktree_clean,
    load_and_verify_manifest, open_private_lock_file, read_dependency_input,
    resolve_exact_commit_sha, resolve_exact_tree_sha, run_sanitized_git,
    run_sanitized_git_allow_nonzero, validate_exact_40_hex_sha, validate_existing_canonical_dir,
    validate_repo_relative_path, validate_sha256_hex_digest, validate_simple_identifier,
    DependencyInputPolicy, DependencyInputSeal, DependencyPredecessorInput, WorktreeKind,
    WorktreeManifest, DEFAULT_GIT_COMMAND_TIMEOUT_MS, DEFAULT_MAX_GIT_OUTPUT_BYTES,
    DEPENDENCY_INPUT_SCHEMA_VERSION, MANIFESTS_SUBDIR, MAX_DEPENDENCY_INPUT_BYTES,
};
use uuid::Uuid;

use crate::error::OperationsError;
use crate::integration::{load_integration_receipt, IntegrationReceipt};
use crate::review::{
    ensure_worktree_inactive, inspect_review_subject, load_and_verify_task_manifest,
    load_review_receipt, sha256_hex, verify_review_receipt, verify_worktree_branch,
    InspectReviewSubjectRequest, ReviewPolicyBinding, ReviewReceipt, RECEIPTS_SUBDIR,
};

/// Maximum number of direct predecessors permitted for a single dependent task.
pub const MAX_DEPENDENCY_PREDECESSORS: usize = 32;

/// Maximum number of unique ancestor tasks permitted in a transitive dependency closure.
pub const MAX_DEPENDENCY_CLOSURE_TASKS: usize = 128;

/// Subdirectory inside `<work_area_root>/manifests` storing dependency input seals, locks, and journals.
pub const DEPENDENCIES_SUBDIR: &str = "dependencies";

/// Git ref namespace prefix for sealed dependency input commits.
pub const DEPENDENCY_INPUT_REF_PREFIX: &str = "refs/threadlane/dependency-inputs";

const ZERO_COMMIT_SHA: &str = "0000000000000000000000000000000000000000";
const DETERMINISTIC_GIT_IDENTITY: &str =
    "Threadlane Dependency Host <dependency@threadlane.local> 0 +0000";

/// Expected predecessor identity, worktree bindings, policy, and receipt IDs for dependency proof verification.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DependencyPredecessorCandidate {
    /// Approved logical task identifier of the predecessor.
    pub logical_task_id: String,
    /// 64-character lowercase hex SHA-256 digest of the predecessor's frozen task record.
    pub logical_task_digest: String,
    /// Runtime supervisor run identifier (`run_<uuid>`) from the sealed `TaskRunReceipt`.
    pub runtime_run_id: String,
    /// Runtime worker identifier (`wrk_...`) from the sealed `TaskRunReceipt`.
    pub worker_id: String,
    /// Owned integration worktree identifier (`w_...`) from the predecessor's live `WorkerSnapshot`.
    pub integration_worktree_id: String,
    /// Owned task worktree identifier (`w_...`) from the predecessor's live `WorkerSnapshot`.
    pub task_worktree_id: String,
    /// Canonical path to the approved primary Git repository.
    pub repository_path: PathBuf,
    /// Canonical path to the private work-area root directory.
    pub work_area_root: PathBuf,
    /// Approved 40-character lowercase hex base commit SHA of the plan approval.
    pub approved_base_commit: String,
    /// Expected 40-character lowercase hex execution base commit SHA for this predecessor
    /// (`approved_base_commit` for root tasks, or the verified `execution_base_commit` of the
    /// predecessor's own dependency seal for dependent tasks).
    pub expected_execution_base_commit: String,
    /// Frozen repository-relative `allowed_write_paths` for the predecessor task.
    pub allowed_write_paths: BTreeSet<String>,
    /// Expected requested provider identifier (`"antigravity"` or `"opencode_v2"`).
    pub expected_provider: String,
    /// Expected requested base model identifier.
    pub expected_model: String,
    /// Expected requested reasoning effort or variant, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_effort: Option<String>,
    /// Persisted review receipt identifier (`rev_...` or custom receipt ID).
    pub review_receipt_id: String,
    /// Persisted integration receipt identifier (`int_...` or custom receipt ID).
    pub integration_receipt_id: String,
}

/// Durable preparation intent journal written before applying the Git snapshot ref effect.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DependencyPreparationIntent {
    /// Schema version (`DEPENDENCY_INPUT_SCHEMA_VERSION`).
    pub schema_version: u32,
    /// Complete deterministic [`DependencyInputSeal`] to be bound to `snapshot_ref` and `seal_path`.
    pub seal: DependencyInputSeal,
    /// Timestamp (milliseconds since Unix epoch) when the intent was recorded.
    pub recorded_at_ms: i64,
}

/// Returns the durable preparation intent journal path (`<seal_path with .intent.json extension>`)
/// corresponding to `policy`.
pub fn dependency_input_intent_path(policy: &DependencyInputPolicy) -> PathBuf {
    policy.seal_path.with_extension("intent.json")
}

/// Returns the deterministic Git ref name (`refs/threadlane/dependency-inputs/<approval_id>/<task_id>`)
/// for a prepared dependent task.
pub fn dependency_input_snapshot_ref(
    approval_id: &str,
    task_id: &str,
) -> Result<String, OperationsError> {
    validate_simple_identifier(approval_id, "approval_id")?;
    validate_simple_identifier(task_id, "task_id")?;
    Ok(format!(
        "{DEPENDENCY_INPUT_REF_PREFIX}/{approval_id}/{task_id}"
    ))
}

/// Verifies a predecessor task's persisted [`ReviewReceipt`], [`IntegrationReceipt`], live task
/// and integration worktrees, reconstructed [`crate::ReviewSubject`], model/effort/scope policy,
/// and Git commit ancestry, returning a verified [`DependencyPredecessorInput`].
///
/// Never trusts caller-supplied [`crate::ReviewSubject`] DTOs; always reconstructs the live
/// subject from disk using [`inspect_review_subject`] and verifies that neither the review receipt
/// nor the integration receipt has been superseded by a newer receipt on the same task worktree.
pub fn verify_dependency_predecessor_proof(
    candidate: &DependencyPredecessorCandidate,
) -> Result<DependencyPredecessorInput, OperationsError> {
    validate_simple_identifier(&candidate.logical_task_id, "logical_task_id")?;
    let logical_task_digest =
        validate_sha256_hex_digest(&candidate.logical_task_digest, "logical_task_digest")?;
    validate_simple_identifier(&candidate.runtime_run_id, "runtime_run_id")?;
    if !candidate.runtime_run_id.starts_with("run_") {
        return Err(OperationsError::InvalidInput(format!(
            "runtime_run_id `{}` must start with `run_`",
            candidate.runtime_run_id
        )));
    }
    validate_simple_identifier(&candidate.worker_id, "worker_id")?;
    if !candidate.worker_id.starts_with("wrk_") {
        return Err(OperationsError::InvalidInput(format!(
            "worker_id `{}` must start with `wrk_`",
            candidate.worker_id
        )));
    }
    validate_simple_identifier(
        &candidate.integration_worktree_id,
        "integration_worktree_id",
    )?;
    validate_simple_identifier(&candidate.task_worktree_id, "task_worktree_id")?;
    validate_simple_identifier(&candidate.expected_provider, "expected_provider")?;
    validate_simple_identifier(&candidate.review_receipt_id, "review_receipt_id")?;
    validate_simple_identifier(&candidate.integration_receipt_id, "integration_receipt_id")?;

    let approved_base_commit =
        validate_exact_40_hex_sha(&candidate.approved_base_commit, "approved_base_commit")?;
    let expected_execution_base_commit = validate_exact_40_hex_sha(
        &candidate.expected_execution_base_commit,
        "expected_execution_base_commit",
    )?;

    if candidate.allowed_write_paths.is_empty() {
        return Err(OperationsError::InvalidInput(
            "predecessor allowed_write_paths must not be empty".to_string(),
        ));
    }
    for path in &candidate.allowed_write_paths {
        validate_repo_relative_path(path, "predecessor allowed_write_paths entry")?;
    }

    let canonical_repo =
        validate_existing_canonical_dir(&candidate.repository_path, "repository_path")?;
    if canonical_repo != candidate.repository_path {
        return Err(OperationsError::SecurityViolation(
            "predecessor repository_path canonicalization mismatch".to_string(),
        ));
    }
    let canonical_work_area =
        validate_existing_canonical_dir(&candidate.work_area_root, "work_area_root")?;
    if canonical_work_area != candidate.work_area_root {
        return Err(OperationsError::SecurityViolation(
            "predecessor work_area_root canonicalization mismatch".to_string(),
        ));
    }

    // Verify primary repository checkout HEAD has not moved away from approved_base_commit.
    let current_source_head =
        resolve_exact_commit_sha(&canonical_repo, "HEAD", DEFAULT_GIT_COMMAND_TIMEOUT_MS)?;
    if current_source_head != approved_base_commit {
        return Err(OperationsError::HeadMoved {
            target: "primary_repository".to_string(),
            expected: approved_base_commit,
            actual: current_source_head,
        });
    }

    // Load and verify both worktree manifests on disk.
    let task_manifest =
        load_and_verify_task_manifest(&canonical_work_area, &candidate.task_worktree_id)?;
    let integration_manifest = load_and_verify_integration_manifest(
        &canonical_work_area,
        &candidate.integration_worktree_id,
    )?;

    if task_manifest.repository_path != canonical_repo
        || integration_manifest.repository_path != canonical_repo
        || task_manifest.work_area_root != canonical_work_area
        || integration_manifest.work_area_root != canonical_work_area
        || task_manifest.repository_common_git_dir != integration_manifest.repository_common_git_dir
    {
        return Err(OperationsError::WorktreeNotOwned(format!(
            "predecessor `{}` worktree manifests do not match repository or work_area_root",
            candidate.logical_task_id
        )));
    }
    if task_manifest.run_id != integration_manifest.run_id {
        return Err(OperationsError::WorktreeNotOwned(format!(
            "predecessor `{}` task worktree run_id `{}` does not match integration worktree run_id `{}`",
            candidate.logical_task_id, task_manifest.run_id, integration_manifest.run_id
        )));
    }
    if task_manifest.base_commit != expected_execution_base_commit
        || integration_manifest.base_commit != expected_execution_base_commit
    {
        return Err(OperationsError::SubjectMismatch(format!(
            "predecessor `{}` worktree base_commit `{}` does not match expected execution base `{expected_execution_base_commit}`",
            candidate.logical_task_id, task_manifest.base_commit
        )));
    }
    if task_manifest.allowed_write_paths != candidate.allowed_write_paths {
        return Err(OperationsError::SubjectMismatch(format!(
            "predecessor `{}` task manifest allowed_write_paths do not match frozen approval task scope",
            candidate.logical_task_id
        )));
    }

    // Load persisted ReviewReceipt and IntegrationReceipt from disk.
    let review_receipt = load_review_receipt(&canonical_work_area, &candidate.review_receipt_id)?;
    let integration_receipt =
        load_integration_receipt(&canonical_work_area, &candidate.integration_receipt_id)?;

    // Verify receipt correlation with the candidate's owned worktrees, base commit, and scope.
    if review_receipt.task_worktree_id != candidate.task_worktree_id
        || review_receipt.run_id != task_manifest.run_id
        || Some(review_receipt.task_id.as_str()) != task_manifest.task_id.as_deref()
        || review_receipt.repository_path != canonical_repo
        || review_receipt.work_area_root != canonical_work_area
        || review_receipt.base_commit != expected_execution_base_commit
        || review_receipt.allowed_write_paths != candidate.allowed_write_paths
    {
        return Err(OperationsError::ReceiptInvalidated(format!(
            "review receipt `{}` does not match predecessor task `{}` worktree, base_commit, or write scope",
            review_receipt.receipt_id, candidate.logical_task_id
        )));
    }

    verify_receipt_policy_matches_frozen_task(
        &review_receipt.policy,
        &candidate.expected_provider,
        &candidate.expected_model,
        candidate.expected_effort.as_deref(),
        &candidate.logical_task_id,
    )?;

    if integration_receipt.task_worktree_id != candidate.task_worktree_id
        || integration_receipt.integration_worktree_id != candidate.integration_worktree_id
        || integration_receipt.run_id != task_manifest.run_id
        || Some(integration_receipt.task_id.as_str()) != task_manifest.task_id.as_deref()
        || integration_receipt.repository_path != canonical_repo
        || integration_receipt.work_area_root != canonical_work_area
        || integration_receipt.base_commit != expected_execution_base_commit
        || integration_receipt.review_receipt_id != review_receipt.receipt_id
        || integration_receipt.review_receipt_digest != review_receipt.receipt_digest
        || integration_receipt.review_subject_digest != review_receipt.subject_digest
        || integration_receipt.source_head_commit != approved_base_commit
    {
        return Err(OperationsError::ReceiptInvalidated(format!(
            "integration receipt `{}` does not match predecessor task `{}` and review receipt `{}`",
            integration_receipt.receipt_id, candidate.logical_task_id, review_receipt.receipt_id
        )));
    }

    // Reconstruct the real ReviewSubject from live worktree state on disk (never trusting caller DTOs).
    let reconstructed_subject = inspect_review_subject(&InspectReviewSubjectRequest {
        work_area_root: canonical_work_area.clone(),
        task_worktree_id: candidate.task_worktree_id.clone(),
        policy: review_receipt.policy.clone(),
        test_evidence: review_receipt.test_evidence.clone(),
        worker_status: None,
    })
    .map_err(|err| {
        OperationsError::ReceiptInvalidated(format!(
            "predecessor `{}` live task worktree failed review subject reconstruction: {err}",
            candidate.logical_task_id
        ))
    })?;

    verify_review_receipt(&reconstructed_subject, &review_receipt)?;
    integration_receipt.verify_against_review(&reconstructed_subject, &review_receipt)?;

    // Ensure neither receipt has been replaced/superseded by another receipt for the same worktree.
    ensure_receipts_not_superseded(&canonical_work_area, &review_receipt, &integration_receipt)?;

    // Verify live integration worktree state on disk.
    ensure_worktree_inactive(&integration_manifest)?;
    let canonical_integration_checkout = validate_existing_canonical_dir(
        &integration_manifest.checkout_path,
        "integration checkout_path",
    )?;
    if canonical_integration_checkout != integration_manifest.checkout_path
        || canonical_integration_checkout != integration_receipt.integration_checkout_path
    {
        return Err(OperationsError::WorktreeNotOwned(format!(
            "predecessor `{}` integration checkout_path mismatch",
            candidate.logical_task_id
        )));
    }
    verify_worktree_branch(
        &canonical_integration_checkout,
        &integration_manifest.branch_name,
        &integration_manifest.worktree_id,
    )?;
    if !is_worktree_clean(
        &canonical_integration_checkout,
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )? {
        return Err(OperationsError::DirtyWorktree {
            worktree_id: integration_manifest.worktree_id,
            detail: "integration worktree has uncommitted, untracked, or ignored files".to_string(),
        });
    }

    let live_integration_head = resolve_exact_commit_sha(
        &canonical_integration_checkout,
        "HEAD",
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )?;
    if live_integration_head != integration_receipt.integrated_target_commit {
        return Err(OperationsError::HeadMoved {
            target: format!("integration_worktree:{}", integration_manifest.worktree_id),
            expected: integration_receipt.integrated_target_commit,
            actual: live_integration_head,
        });
    }
    let live_integration_tree = resolve_exact_tree_sha(
        &canonical_integration_checkout,
        "HEAD",
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )?;
    if live_integration_tree != integration_receipt.integrated_tree_sha {
        return Err(OperationsError::ReceiptInvalidated(format!(
            "predecessor `{}` live integration tree `{live_integration_tree}` does not match receipt `{}`",
            candidate.logical_task_id, integration_receipt.integrated_tree_sha
        )));
    }

    // Verify Git commit and tree resolution and ancestry in the canonical repository.
    let resolved_accepted_head = resolve_exact_commit_sha(
        &canonical_repo,
        &integration_receipt.accepted_task_head_commit,
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )?;
    if resolved_accepted_head != integration_receipt.accepted_task_head_commit
        || resolved_accepted_head != review_receipt.task_head_commit
    {
        return Err(OperationsError::ReceiptInvalidated(format!(
            "predecessor `{}` accepted_task_head_commit mismatch",
            candidate.logical_task_id
        )));
    }
    let resolved_accepted_tree = resolve_exact_tree_sha(
        &canonical_repo,
        &resolved_accepted_head,
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )?;
    if resolved_accepted_tree != review_receipt.task_tree_sha {
        return Err(OperationsError::ReceiptInvalidated(format!(
            "predecessor `{}` accepted_task_head_commit tree `{resolved_accepted_tree}` does not match review receipt `{}`",
            candidate.logical_task_id, review_receipt.task_tree_sha
        )));
    }

    if !is_commit_ancestor(
        &canonical_repo,
        &approved_base_commit,
        &expected_execution_base_commit,
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )? {
        return Err(OperationsError::AncestryMismatch(format!(
            "approved_base_commit `{approved_base_commit}` is not an ancestor of predecessor `{}` execution base `{expected_execution_base_commit}`",
            candidate.logical_task_id
        )));
    }
    if !is_commit_ancestor(
        &canonical_repo,
        &approved_base_commit,
        &resolved_accepted_head,
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )? {
        return Err(OperationsError::AncestryMismatch(format!(
            "approved_base_commit `{approved_base_commit}` is not an ancestor of predecessor `{}` accepted_task_head_commit `{resolved_accepted_head}`",
            candidate.logical_task_id
        )));
    }
    if !is_commit_ancestor(
        &canonical_repo,
        &expected_execution_base_commit,
        &resolved_accepted_head,
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )? {
        return Err(OperationsError::AncestryMismatch(format!(
            "predecessor `{}` execution base `{expected_execution_base_commit}` is not an ancestor of accepted_task_head_commit `{resolved_accepted_head}`",
            candidate.logical_task_id
        )));
    }
    if !is_commit_ancestor(
        &canonical_repo,
        &resolved_accepted_head,
        &integration_receipt.integrated_target_commit,
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )? {
        return Err(OperationsError::AncestryMismatch(format!(
            "predecessor `{}` accepted_task_head_commit `{resolved_accepted_head}` is not an ancestor of integrated_target_commit `{}`",
            candidate.logical_task_id, integration_receipt.integrated_target_commit
        )));
    }

    Ok(DependencyPredecessorInput {
        task_id: candidate.logical_task_id.clone(),
        task_digest: logical_task_digest,
        runtime_run_id: candidate.runtime_run_id.clone(),
        worker_id: candidate.worker_id.clone(),
        task_worktree_id: candidate.task_worktree_id.clone(),
        review_subject_digest: review_receipt.subject_digest,
        review_receipt_id: review_receipt.receipt_id,
        review_receipt_digest: review_receipt.receipt_digest,
        integration_receipt_id: integration_receipt.receipt_id,
        integration_receipt_digest: integration_receipt.receipt_digest,
        accepted_task_head_commit: resolved_accepted_head,
    })
}

/// Composes verified predecessor `accepted_task_head_commit` inputs (never `integrated_target_commit`)
/// into a deterministic Git commit object, journals a [`DependencyPreparationIntent`], updates
/// `refs/threadlane/dependency-inputs/<approval_id>/<task_id>` with compare-and-swap, and writes
/// the immutable [`DependencyInputSeal`] write-once under the shared lock protocol.
///
/// Retrying the exact same request with unchanged predecessor proofs returns the existing
/// [`DependencyInputSeal`] idempotently; any conflicting or stale retry fails closed without
/// overwriting the ref, intent, or seal.
pub fn prepare_and_seal_dependency_input(
    policy: &DependencyInputPolicy,
    repository_path: &Path,
    work_area_root: &Path,
    approved_base_commit: &str,
    instruction: &str,
    mut predecessors: Vec<DependencyPredecessorInput>,
) -> Result<DependencyInputSeal, OperationsError> {
    validate_dependency_policy(
        policy,
        repository_path,
        work_area_root,
        approved_base_commit,
        instruction,
    )?;

    if predecessors.is_empty() || predecessors.len() > MAX_DEPENDENCY_PREDECESSORS {
        return Err(OperationsError::InvalidInput(format!(
            "dependency predecessors count must be in 1..={MAX_DEPENDENCY_PREDECESSORS} (got {})",
            predecessors.len()
        )));
    }

    predecessors.sort_by(|left, right| left.task_id.cmp(&right.task_id));
    let mut seen_pred_ids = BTreeSet::new();
    for pred in &predecessors {
        validate_predecessor_input_fields(pred)?;
        if !seen_pred_ids.insert(pred.task_id.clone()) {
            return Err(OperationsError::InvalidInput(format!(
                "duplicate predecessor task_id `{}`",
                pred.task_id
            )));
        }
    }

    let canonical_repo = validate_existing_canonical_dir(repository_path, "repository_path")?;
    let canonical_work_area = validate_existing_canonical_dir(work_area_root, "work_area_root")?;
    let approved_base_sha =
        validate_exact_40_hex_sha(approved_base_commit, "approved_base_commit")?;

    let manifests_dir =
        ensure_private_dir(&canonical_work_area.join(MANIFESTS_SUBDIR), "manifests_dir")?;
    let dependencies_dir =
        ensure_private_dir(&manifests_dir.join(DEPENDENCIES_SUBDIR), "dependencies_dir")?;
    let expected_seal_path = dependencies_dir.join(format!("{}.json", policy.task_id));
    if policy.seal_path != expected_seal_path {
        return Err(OperationsError::SecurityViolation(format!(
            "dependency policy seal_path `{}` does not match expected `{}`",
            policy.seal_path.display(),
            expected_seal_path.display()
        )));
    }

    let lock_path = dependency_input_lock_path(policy);
    let intent_path = dependency_input_intent_path(policy);
    let snapshot_ref = dependency_input_snapshot_ref(&policy.approval_id, &policy.task_id)?;

    let lock_file = open_private_lock_file(&lock_path, "dependency input lock")?;
    lock_file.lock_exclusive().map_err(OperationsError::Io)?;

    let result = (|| -> Result<DependencyInputSeal, OperationsError> {
        let primary_head_before =
            resolve_exact_commit_sha(&canonical_repo, "HEAD", DEFAULT_GIT_COMMAND_TIMEOUT_MS)?;
        if primary_head_before != approved_base_sha {
            return Err(OperationsError::HeadMoved {
                target: "primary_repository".to_string(),
                expected: approved_base_sha.clone(),
                actual: primary_head_before,
            });
        }
        let primary_status_before = run_sanitized_git(
            &canonical_repo,
            &["status", "--porcelain=v1", "-uall"],
            DEFAULT_GIT_COMMAND_TIMEOUT_MS,
        )?;

        ensure_snapshot_ref_not_symbolic(&canonical_repo, &snapshot_ref)?;

        // Compute the deterministic composed tree and commit object from accepted_task_head_commit inputs.
        let (execution_tree_sha, execution_base_commit) = compose_accepted_predecessor_heads(
            &canonical_repo,
            &dependencies_dir,
            policy,
            &approved_base_sha,
            &predecessors,
        )?;

        let mut expected_seal = DependencyInputSeal {
            schema_version: DEPENDENCY_INPUT_SCHEMA_VERSION,
            approval_id: policy.approval_id.clone(),
            approval_subject_digest: policy.approval_subject_digest.clone(),
            task_id: policy.task_id.clone(),
            task_digest: policy.task_digest.clone(),
            instruction_digest: policy.instruction_digest.clone(),
            repository_path: canonical_repo.clone(),
            work_area_root: canonical_work_area.clone(),
            approved_base_commit: approved_base_sha.clone(),
            execution_base_commit: execution_base_commit.clone(),
            execution_tree_sha: execution_tree_sha.clone(),
            snapshot_ref: snapshot_ref.clone(),
            predecessors: predecessors.clone(),
            seal_digest: String::new(),
        };
        expected_seal.seal_digest = expected_seal.compute_digest()?;
        expected_seal.verify_integrity()?;

        let seal_exists = path_exists_no_symlink(&policy.seal_path, "dependency input seal")?;
        let intent_exists = path_exists_no_symlink(&intent_path, "dependency preparation intent")?;

        if seal_exists {
            let existing_seal: DependencyInputSeal = read_dependency_json(
                &policy.seal_path,
                MAX_DEPENDENCY_INPUT_BYTES as u64,
                "dependency input seal",
            )?;
            existing_seal.verify_integrity()?;
            if existing_seal != expected_seal {
                return Err(OperationsError::ReceiptInvalidated(format!(
                    "dependency input seal for task `{}` is already sealed with different parameters or predecessor proofs",
                    policy.task_id
                )));
            }
            if intent_exists {
                let existing_intent: DependencyPreparationIntent = read_dependency_json(
                    &intent_path,
                    MAX_DEPENDENCY_INPUT_BYTES as u64,
                    "dependency preparation intent",
                )?;
                verify_intent_structure(&existing_intent)?;
                if existing_intent.seal != existing_seal {
                    return Err(OperationsError::ReceiptInvalidated(format!(
                        "dependency preparation intent for task `{}` conflicts with sealed dependency input",
                        policy.task_id
                    )));
                }
            }
            verify_snapshot_ref_and_ancestry(&canonical_repo, &existing_seal)?;
            verify_primary_repo_unchanged(
                &canonical_repo,
                &primary_head_before,
                &primary_status_before,
            )?;
            return Ok(existing_seal);
        }

        if intent_exists {
            // Reconcile an interrupted explicit prepare call using the durable intent journal.
            let existing_intent: DependencyPreparationIntent = read_dependency_json(
                &intent_path,
                MAX_DEPENDENCY_INPUT_BYTES as u64,
                "dependency preparation intent",
            )?;
            verify_intent_structure(&existing_intent)?;
            if existing_intent.seal != expected_seal {
                return Err(OperationsError::ReceiptInvalidated(format!(
                    "interrupted dependency preparation intent for task `{}` does not match retry recipe",
                    policy.task_id
                )));
            }
            apply_or_reconcile_snapshot_ref(
                &canonical_repo,
                &snapshot_ref,
                &expected_seal.execution_base_commit,
                true,
            )?;
        } else {
            if let Some(current_sha) = probe_direct_snapshot_ref(&canonical_repo, &snapshot_ref)? {
                return Err(OperationsError::ReceiptInvalidated(format!(
                    "snapshot ref `{snapshot_ref}` already exists at `{current_sha}` (expected `{ZERO_COMMIT_SHA}`)"
                )));
            }

            // Write durable preparation intent BEFORE mutating the Git ref.
            let intent = DependencyPreparationIntent {
                schema_version: DEPENDENCY_INPUT_SCHEMA_VERSION,
                seal: expected_seal.clone(),
                recorded_at_ms: now_epoch_ms(),
            };
            write_once_private_json(&intent_path, &intent, "dependency preparation intent")?;

            // Apply compare-and-swap ref creation (require ref did not previously exist).
            apply_or_reconcile_snapshot_ref(
                &canonical_repo,
                &snapshot_ref,
                &expected_seal.execution_base_commit,
                false,
            )?;
        }

        verify_snapshot_ref_and_ancestry(&canonical_repo, &expected_seal)?;
        write_once_private_json(&policy.seal_path, &expected_seal, "dependency input seal")?;
        verify_primary_repo_unchanged(
            &canonical_repo,
            &primary_head_before,
            &primary_status_before,
        )?;

        Ok(expected_seal)
    })();

    let _ = lock_file.unlock();
    let seal = result?;

    // Verify through the shared-lock reader in threadlane-workers after releasing the exclusive lock.
    let verified = read_dependency_input(
        policy,
        &canonical_repo,
        &canonical_work_area,
        &approved_base_sha,
        instruction,
        &policy.command_key,
    )?;
    if verified.seal != seal {
        return Err(OperationsError::ReceiptInvalidated(
            "verified dependency input seal did not match written seal".to_string(),
        ));
    }

    Ok(seal)
}

/// Strictly read-only inspection of a dependent task's prepared [`DependencyInputSeal`].
///
/// Never creates directories, lock files, journals, refs, or manifests.
/// - Returns `Ok(None)` if no seal file exists yet (while still validating any existing `.intent.json`
///   journal for corruption without reconciling it).
/// - Returns `Ok(Some(DependencyInputSeal))` if a valid seal exists.
/// - Fails closed if the seal, journal, or Git ref is corrupt, tampered, or inconsistent.
pub fn inspect_dependency_input_seal_readonly(
    policy: &DependencyInputPolicy,
    repository_path: &Path,
    work_area_root: &Path,
    approved_base_commit: &str,
    instruction: &str,
    command_key: &str,
) -> Result<Option<DependencyInputSeal>, OperationsError> {
    validate_dependency_policy(
        policy,
        repository_path,
        work_area_root,
        approved_base_commit,
        instruction,
    )?;
    if policy.command_key != command_key {
        return Err(OperationsError::InvalidInput(
            "command_key does not match dependency input policy".to_string(),
        ));
    }

    let canonical_repo = validate_existing_canonical_dir(repository_path, "repository_path")?;
    let canonical_work_area = validate_existing_canonical_dir(work_area_root, "work_area_root")?;
    let approved_base_sha =
        validate_exact_40_hex_sha(approved_base_commit, "approved_base_commit")?;

    let snapshot_ref = dependency_input_snapshot_ref(&policy.approval_id, &policy.task_id)?;
    ensure_snapshot_ref_not_symbolic(&canonical_repo, &snapshot_ref)?;

    let intent_path = dependency_input_intent_path(policy);
    let seal_exists = path_exists_no_symlink(&policy.seal_path, "dependency input seal")?;
    let intent_exists = path_exists_no_symlink(&intent_path, "dependency preparation intent")?;

    if !seal_exists {
        if intent_exists {
            let existing_intent: DependencyPreparationIntent = read_dependency_json(
                &intent_path,
                MAX_DEPENDENCY_INPUT_BYTES as u64,
                "dependency preparation intent",
            )?;
            verify_intent_structure(&existing_intent)?;
        }
        return Ok(None);
    }

    // Prepared execution requires the original shared-protocol lock. Never repair a missing
    // lock during observation or report a seal as ready without the runtime's verification.
    let verified = read_dependency_input(
        policy,
        &canonical_repo,
        &canonical_work_area,
        &approved_base_sha,
        instruction,
        command_key,
    )?;
    if intent_exists {
        let existing_intent: DependencyPreparationIntent = read_dependency_json(
            &intent_path,
            MAX_DEPENDENCY_INPUT_BYTES as u64,
            "dependency preparation intent",
        )?;
        verify_intent_structure(&existing_intent)?;
        if existing_intent.seal != verified.seal {
            return Err(OperationsError::ReceiptInvalidated(format!(
                "dependency preparation intent for task `{}` conflicts with sealed dependency input",
                policy.task_id
            )));
        }
    }
    verify_dependency_predecessor_receipts(&verified.seal)?;
    Ok(Some(verified.seal.clone()))
}

/// Reloads persisted predecessor [`ReviewReceipt`] and [`IntegrationReceipt`] proofs for every
/// predecessor recorded in `seal`, reconstructs each predecessor's live [`crate::ReviewSubject`]
/// from its owned task worktree on disk, and validates receipt digests, worktree ownership,
/// branch/cleanliness invariants, non-supersession, and Git commit ancestry.
///
/// Safe to invoke while holding a shared lock lease on the dependency seal `.lock` file:
/// performs no file creation, no lock reacquisition on the dependency seal lock, and no mutation.
pub fn verify_dependency_predecessor_receipts(
    seal: &DependencyInputSeal,
) -> Result<(), OperationsError> {
    seal.verify_integrity()?;

    let canonical_repo =
        validate_existing_canonical_dir(&seal.repository_path, "seal.repository_path")?;
    if canonical_repo != seal.repository_path {
        return Err(OperationsError::SecurityViolation(
            "dependency seal repository_path canonicalization mismatch".to_string(),
        ));
    }
    let canonical_work_area =
        validate_existing_canonical_dir(&seal.work_area_root, "seal.work_area_root")?;
    if canonical_work_area != seal.work_area_root {
        return Err(OperationsError::SecurityViolation(
            "dependency seal work_area_root canonicalization mismatch".to_string(),
        ));
    }

    let current_source_head =
        resolve_exact_commit_sha(&canonical_repo, "HEAD", DEFAULT_GIT_COMMAND_TIMEOUT_MS)?;
    if current_source_head != seal.approved_base_commit {
        return Err(OperationsError::HeadMoved {
            target: "primary_repository".to_string(),
            expected: seal.approved_base_commit.clone(),
            actual: current_source_head,
        });
    }

    verify_snapshot_ref_and_ancestry(&canonical_repo, seal)?;

    for pred in &seal.predecessors {
        validate_predecessor_input_fields(pred)?;

        let task_manifest =
            load_and_verify_task_manifest(&canonical_work_area, &pred.task_worktree_id)?;
        let review_receipt = load_review_receipt(&canonical_work_area, &pred.review_receipt_id)?;
        let integration_receipt =
            load_integration_receipt(&canonical_work_area, &pred.integration_receipt_id)?;

        if review_receipt.receipt_id != pred.review_receipt_id
            || review_receipt.receipt_digest != pred.review_receipt_digest
            || review_receipt.subject_digest != pred.review_subject_digest
            || review_receipt.task_worktree_id != pred.task_worktree_id
            || review_receipt.run_id != task_manifest.run_id
            || Some(review_receipt.task_id.as_str()) != task_manifest.task_id.as_deref()
            || review_receipt.repository_path != canonical_repo
            || review_receipt.work_area_root != canonical_work_area
            || review_receipt.base_commit != task_manifest.base_commit
            || review_receipt.task_head_commit != pred.accepted_task_head_commit
            || review_receipt.allowed_write_paths != task_manifest.allowed_write_paths
        {
            return Err(OperationsError::ReceiptInvalidated(format!(
                "review receipt `{}` does not match sealed predecessor `{}`",
                review_receipt.receipt_id, pred.task_id
            )));
        }

        if integration_receipt.receipt_id != pred.integration_receipt_id
            || integration_receipt.receipt_digest != pred.integration_receipt_digest
            || integration_receipt.review_receipt_id != review_receipt.receipt_id
            || integration_receipt.review_receipt_digest != review_receipt.receipt_digest
            || integration_receipt.review_subject_digest != review_receipt.subject_digest
            || integration_receipt.task_worktree_id != pred.task_worktree_id
            || integration_receipt.run_id != task_manifest.run_id
            || Some(integration_receipt.task_id.as_str()) != task_manifest.task_id.as_deref()
            || integration_receipt.repository_path != canonical_repo
            || integration_receipt.work_area_root != canonical_work_area
            || integration_receipt.base_commit != task_manifest.base_commit
            || integration_receipt.source_head_commit != seal.approved_base_commit
            || integration_receipt.accepted_task_head_commit != pred.accepted_task_head_commit
        {
            return Err(OperationsError::ReceiptInvalidated(format!(
                "integration receipt `{}` does not match sealed predecessor `{}` and review receipt `{}`",
                integration_receipt.receipt_id, pred.task_id, review_receipt.receipt_id
            )));
        }

        let integration_manifest = load_and_verify_integration_manifest(
            &canonical_work_area,
            &integration_receipt.integration_worktree_id,
        )?;

        if task_manifest.repository_path != canonical_repo
            || integration_manifest.repository_path != canonical_repo
            || task_manifest.work_area_root != canonical_work_area
            || integration_manifest.work_area_root != canonical_work_area
            || task_manifest.repository_common_git_dir
                != integration_manifest.repository_common_git_dir
            || task_manifest.run_id != integration_manifest.run_id
            || task_manifest.base_commit != integration_manifest.base_commit
        {
            return Err(OperationsError::WorktreeNotOwned(format!(
                "predecessor `{}` worktree manifests do not match repository, work_area_root, or run_id",
                pred.task_id
            )));
        }

        let reconstructed_subject = inspect_review_subject(&InspectReviewSubjectRequest {
            work_area_root: canonical_work_area.clone(),
            task_worktree_id: pred.task_worktree_id.clone(),
            policy: review_receipt.policy.clone(),
            test_evidence: review_receipt.test_evidence.clone(),
            worker_status: None,
        })
        .map_err(|err| {
            OperationsError::ReceiptInvalidated(format!(
                "predecessor `{}` live task worktree failed review subject reconstruction: {err}",
                pred.task_id
            ))
        })?;

        verify_review_receipt(&reconstructed_subject, &review_receipt)?;
        integration_receipt.verify_against_review(&reconstructed_subject, &review_receipt)?;
        ensure_receipts_not_superseded(
            &canonical_work_area,
            &review_receipt,
            &integration_receipt,
        )?;

        ensure_worktree_inactive(&integration_manifest)?;
        let canonical_integration_checkout = validate_existing_canonical_dir(
            &integration_manifest.checkout_path,
            "integration checkout_path",
        )?;
        if canonical_integration_checkout != integration_manifest.checkout_path
            || canonical_integration_checkout != integration_receipt.integration_checkout_path
        {
            return Err(OperationsError::WorktreeNotOwned(format!(
                "predecessor `{}` integration checkout_path mismatch",
                pred.task_id
            )));
        }
        verify_worktree_branch(
            &canonical_integration_checkout,
            &integration_manifest.branch_name,
            &integration_manifest.worktree_id,
        )?;
        if !is_worktree_clean(
            &canonical_integration_checkout,
            DEFAULT_GIT_COMMAND_TIMEOUT_MS,
        )? {
            return Err(OperationsError::DirtyWorktree {
                worktree_id: integration_manifest.worktree_id,
                detail: "integration worktree has uncommitted, untracked, or ignored files"
                    .to_string(),
            });
        }

        let live_integration_head = resolve_exact_commit_sha(
            &canonical_integration_checkout,
            "HEAD",
            DEFAULT_GIT_COMMAND_TIMEOUT_MS,
        )?;
        if live_integration_head != integration_receipt.integrated_target_commit {
            return Err(OperationsError::HeadMoved {
                target: format!("integration_worktree:{}", integration_manifest.worktree_id),
                expected: integration_receipt.integrated_target_commit,
                actual: live_integration_head,
            });
        }
        let live_integration_tree = resolve_exact_tree_sha(
            &canonical_integration_checkout,
            "HEAD",
            DEFAULT_GIT_COMMAND_TIMEOUT_MS,
        )?;
        if live_integration_tree != integration_receipt.integrated_tree_sha {
            return Err(OperationsError::ReceiptInvalidated(format!(
                "predecessor `{}` live integration tree `{live_integration_tree}` does not match receipt `{}`",
                pred.task_id, integration_receipt.integrated_tree_sha
            )));
        }

        let resolved_accepted_head = resolve_exact_commit_sha(
            &canonical_repo,
            &pred.accepted_task_head_commit,
            DEFAULT_GIT_COMMAND_TIMEOUT_MS,
        )?;
        if resolved_accepted_head != pred.accepted_task_head_commit {
            return Err(OperationsError::ReceiptInvalidated(format!(
                "predecessor `{}` accepted_task_head_commit `{}` mismatch",
                pred.task_id, pred.accepted_task_head_commit
            )));
        }
        let resolved_accepted_tree = resolve_exact_tree_sha(
            &canonical_repo,
            &resolved_accepted_head,
            DEFAULT_GIT_COMMAND_TIMEOUT_MS,
        )?;
        if resolved_accepted_tree != review_receipt.task_tree_sha {
            return Err(OperationsError::ReceiptInvalidated(format!(
                "predecessor `{}` accepted_task_head_commit tree `{resolved_accepted_tree}` does not match review receipt `{}`",
                pred.task_id, review_receipt.task_tree_sha
            )));
        }

        if !is_commit_ancestor(
            &canonical_repo,
            &seal.approved_base_commit,
            &review_receipt.base_commit,
            DEFAULT_GIT_COMMAND_TIMEOUT_MS,
        )? {
            return Err(OperationsError::AncestryMismatch(format!(
                "approved_base_commit `{}` is not an ancestor of predecessor `{}` base_commit `{}`",
                seal.approved_base_commit, pred.task_id, review_receipt.base_commit
            )));
        }
        if !is_commit_ancestor(
            &canonical_repo,
            &review_receipt.base_commit,
            &pred.accepted_task_head_commit,
            DEFAULT_GIT_COMMAND_TIMEOUT_MS,
        )? {
            return Err(OperationsError::AncestryMismatch(format!(
                "predecessor `{}` base_commit `{}` is not an ancestor of accepted_task_head_commit `{}`",
                pred.task_id, review_receipt.base_commit, pred.accepted_task_head_commit
            )));
        }
        if !is_commit_ancestor(
            &canonical_repo,
            &pred.accepted_task_head_commit,
            &integration_receipt.integrated_target_commit,
            DEFAULT_GIT_COMMAND_TIMEOUT_MS,
        )? {
            return Err(OperationsError::AncestryMismatch(format!(
                "predecessor `{}` accepted_task_head_commit `{}` is not an ancestor of integrated_target_commit `{}`",
                pred.task_id, pred.accepted_task_head_commit, integration_receipt.integrated_target_commit
            )));
        }
    }

    Ok(())
}

fn validate_dependency_policy(
    policy: &DependencyInputPolicy,
    repository_path: &Path,
    work_area_root: &Path,
    approved_base_commit: &str,
    instruction: &str,
) -> Result<(), OperationsError> {
    validate_simple_identifier(&policy.approval_id, "policy.approval_id")?;
    let _ = validate_sha256_hex_digest(
        &policy.approval_subject_digest,
        "policy.approval_subject_digest",
    )?;
    validate_simple_identifier(&policy.task_id, "policy.task_id")?;
    let _ = validate_sha256_hex_digest(&policy.task_digest, "policy.task_digest")?;
    let expected_instr_digest =
        validate_sha256_hex_digest(&policy.instruction_digest, "policy.instruction_digest")?;
    validate_simple_identifier(&policy.command_key, "policy.command_key")?;
    let _ = validate_exact_40_hex_sha(approved_base_commit, "approved_base_commit")?;

    let actual_instr_digest = sha256_hex(instruction.as_bytes());
    if actual_instr_digest != expected_instr_digest {
        return Err(OperationsError::SubjectMismatch(format!(
            "instruction digest `{actual_instr_digest}` does not match policy `{expected_instr_digest}`"
        )));
    }
    if !repository_path.is_absolute()
        || !work_area_root.is_absolute()
        || !policy.seal_path.is_absolute()
    {
        return Err(OperationsError::InvalidInput(
            "repository_path, work_area_root, and policy.seal_path must be absolute paths"
                .to_string(),
        ));
    }
    Ok(())
}

fn validate_predecessor_input_fields(
    pred: &DependencyPredecessorInput,
) -> Result<(), OperationsError> {
    validate_simple_identifier(&pred.task_id, "predecessor.task_id")?;
    let _ = validate_sha256_hex_digest(&pred.task_digest, "predecessor.task_digest")?;
    validate_simple_identifier(&pred.runtime_run_id, "predecessor.runtime_run_id")?;
    if !pred.runtime_run_id.starts_with("run_") {
        return Err(OperationsError::InvalidInput(format!(
            "predecessor runtime_run_id `{}` must start with `run_`",
            pred.runtime_run_id
        )));
    }
    validate_simple_identifier(&pred.worker_id, "predecessor.worker_id")?;
    if !pred.worker_id.starts_with("wrk_") {
        return Err(OperationsError::InvalidInput(format!(
            "predecessor worker_id `{}` must start with `wrk_`",
            pred.worker_id
        )));
    }
    validate_simple_identifier(&pred.task_worktree_id, "predecessor.task_worktree_id")?;
    let _ = validate_sha256_hex_digest(
        &pred.review_subject_digest,
        "predecessor.review_subject_digest",
    )?;
    validate_simple_identifier(&pred.review_receipt_id, "predecessor.review_receipt_id")?;
    let _ = validate_sha256_hex_digest(
        &pred.review_receipt_digest,
        "predecessor.review_receipt_digest",
    )?;
    validate_simple_identifier(
        &pred.integration_receipt_id,
        "predecessor.integration_receipt_id",
    )?;
    let _ = validate_sha256_hex_digest(
        &pred.integration_receipt_digest,
        "predecessor.integration_receipt_digest",
    )?;
    let _ = validate_exact_40_hex_sha(
        &pred.accepted_task_head_commit,
        "predecessor.accepted_task_head_commit",
    )?;
    Ok(())
}

fn verify_receipt_policy_matches_frozen_task(
    policy: &ReviewPolicyBinding,
    expected_provider: &str,
    expected_model: &str,
    expected_effort: Option<&str>,
    logical_task_id: &str,
) -> Result<(), OperationsError> {
    policy.validate(true)?;
    if policy.requested_provider.as_deref() != Some(expected_provider) {
        return Err(OperationsError::ReceiptInvalidated(format!(
            "predecessor `{logical_task_id}` review receipt requested_provider `{:?}` does not match frozen `{expected_provider}`",
            policy.requested_provider
        )));
    }
    if policy.requested_model.as_deref() != Some(expected_model) {
        return Err(OperationsError::ReceiptInvalidated(format!(
            "predecessor `{logical_task_id}` review receipt requested_model `{:?}` does not match frozen `{expected_model}`",
            policy.requested_model
        )));
    }
    if let Some(req_effort) = expected_effort {
        if policy.requested_effort.as_deref() != Some(req_effort) {
            return Err(OperationsError::ReceiptInvalidated(format!(
                "predecessor `{logical_task_id}` review receipt requested_effort `{:?}` does not match frozen `{req_effort}`",
                policy.requested_effort
            )));
        }
        if !policy.require_effort_evidence || policy.observed_effort.as_deref() != Some(req_effort)
        {
            return Err(OperationsError::UncertainWorkerOutcome(format!(
                "predecessor `{logical_task_id}` review receipt is missing verified observed_effort `{req_effort}`"
            )));
        }
    }
    if !policy.require_passing_tests {
        return Err(OperationsError::ReceiptInvalidated(format!(
            "predecessor `{logical_task_id}` review receipt must require passing test evidence"
        )));
    }
    Ok(())
}

fn ensure_receipts_not_superseded(
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

fn compose_accepted_predecessor_heads(
    canonical_repo: &Path,
    dependencies_dir: &Path,
    policy: &DependencyInputPolicy,
    approved_base_commit: &str,
    predecessors: &[DependencyPredecessorInput],
) -> Result<(String, String), OperationsError> {
    for pred in predecessors {
        let resolved_head = resolve_exact_commit_sha(
            canonical_repo,
            &pred.accepted_task_head_commit,
            DEFAULT_GIT_COMMAND_TIMEOUT_MS,
        )?;
        if resolved_head != pred.accepted_task_head_commit {
            return Err(OperationsError::ReceiptInvalidated(format!(
                "predecessor `{}` accepted_task_head_commit `{}` does not resolve in repository",
                pred.task_id, pred.accepted_task_head_commit
            )));
        }
        if !is_commit_ancestor(
            canonical_repo,
            approved_base_commit,
            &pred.accepted_task_head_commit,
            DEFAULT_GIT_COMMAND_TIMEOUT_MS,
        )? {
            return Err(OperationsError::AncestryMismatch(format!(
                "approved_base_commit `{approved_base_commit}` is not an ancestor of predecessor `{}` accepted_task_head_commit `{}`",
                pred.task_id, pred.accepted_task_head_commit
            )));
        }
    }

    let mut acc_commit = predecessors[0].accepted_task_head_commit.clone();
    let mut acc_tree =
        resolve_exact_tree_sha(canonical_repo, &acc_commit, DEFAULT_GIT_COMMAND_TIMEOUT_MS)?;

    for (idx, pred) in predecessors.iter().enumerate().skip(1) {
        if pred.accepted_task_head_commit == acc_commit
            || is_commit_ancestor(
                canonical_repo,
                &pred.accepted_task_head_commit,
                &acc_commit,
                DEFAULT_GIT_COMMAND_TIMEOUT_MS,
            )?
        {
            continue;
        }
        if is_commit_ancestor(
            canonical_repo,
            &acc_commit,
            &pred.accepted_task_head_commit,
            DEFAULT_GIT_COMMAND_TIMEOUT_MS,
        )? {
            acc_commit = pred.accepted_task_head_commit.clone();
            acc_tree = resolve_exact_tree_sha(
                canonical_repo,
                &acc_commit,
                DEFAULT_GIT_COMMAND_TIMEOUT_MS,
            )?;
            continue;
        }

        let merge_out = run_sanitized_git_allow_nonzero(
            canonical_repo,
            &[
                "merge-tree",
                "--write-tree",
                "--",
                &acc_commit,
                &pred.accepted_task_head_commit,
            ],
            DEFAULT_MAX_GIT_OUTPUT_BYTES,
            DEFAULT_GIT_COMMAND_TIMEOUT_MS,
        )?;
        match merge_out.exit_code {
            Some(0) => {
                let merge_stdout = String::from_utf8_lossy(&merge_out.stdout_bytes);
                let first_line = merge_stdout
                    .lines()
                    .next()
                    .unwrap_or("")
                    .trim()
                    .to_ascii_lowercase();
                let merged_tree = validate_exact_40_hex_sha(&first_line, "merged_tree_sha")?;
                let verified_tree = resolve_exact_tree_sha(
                    canonical_repo,
                    &merged_tree,
                    DEFAULT_GIT_COMMAND_TIMEOUT_MS,
                )?;
                acc_tree = verified_tree;

                if idx + 1 < predecessors.len() {
                    let fold_msg = format!(
                        "threadlane-dependency-fold v1\napproval_id: {}\ntask_id: {}\nstep: {}\n",
                        policy.approval_id, policy.task_id, idx
                    );
                    let fold_parents = vec![acc_commit, pred.accepted_task_head_commit.clone()];
                    acc_commit = write_deterministic_commit_object(
                        canonical_repo,
                        dependencies_dir,
                        &policy.task_id,
                        &acc_tree,
                        &fold_parents,
                        &fold_msg,
                    )?;
                }
            }
            Some(1) => {
                return Err(OperationsError::MergeConflict(format!(
                    "fan-in merge conflict while composing predecessor `{}` ({}) for dependent task `{}`",
                    pred.task_id, pred.accepted_task_head_commit, policy.task_id
                )));
            }
            other => {
                return Err(OperationsError::GitCommandFailed {
                    command: format!(
                        "git merge-tree --write-tree -- {acc_commit} {}",
                        pred.accepted_task_head_commit
                    ),
                    status: other,
                    stderr: merge_out.stderr,
                });
            }
        }
    }

    let mut ordered_parents = Vec::with_capacity(predecessors.len());
    for pred in predecessors {
        if !ordered_parents.contains(&pred.accepted_task_head_commit) {
            ordered_parents.push(pred.accepted_task_head_commit.clone());
        }
    }

    let mut commit_message = format!(
        "threadlane-dependency-input v1\napproval_id: {}\napproval_subject_digest: {}\ntask_id: {}\ntask_digest: {}\napproved_base_commit: {}\n",
        policy.approval_id,
        policy.approval_subject_digest,
        policy.task_id,
        policy.task_digest,
        approved_base_commit,
    );
    for pred in predecessors {
        commit_message.push_str(&format!(
            "predecessor: {} {} {} {}\n",
            pred.task_id,
            pred.accepted_task_head_commit,
            pred.review_receipt_id,
            pred.integration_receipt_id,
        ));
    }

    let execution_base_commit = write_deterministic_commit_object(
        canonical_repo,
        dependencies_dir,
        &policy.task_id,
        &acc_tree,
        &ordered_parents,
        &commit_message,
    )?;

    Ok((acc_tree, execution_base_commit))
}

fn write_deterministic_commit_object(
    canonical_repo: &Path,
    dependencies_dir: &Path,
    task_id: &str,
    tree_sha: &str,
    parents: &[String],
    message: &str,
) -> Result<String, OperationsError> {
    let mut raw = String::new();
    raw.push_str(&format!("tree {tree_sha}\n"));
    for parent in parents {
        raw.push_str(&format!("parent {parent}\n"));
    }
    raw.push_str(&format!("author {DETERMINISTIC_GIT_IDENTITY}\n"));
    raw.push_str(&format!("committer {DETERMINISTIC_GIT_IDENTITY}\n\n"));
    raw.push_str(message);

    let temp_name = format!(".commit-{task_id}-{}.tmp", Uuid::new_v4().simple());
    let temp_path = dependencies_dir.join(temp_name);

    let write_res = (|| -> Result<String, OperationsError> {
        let mut opts = OpenOptions::new();
        opts.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            opts.mode(0o600);
        }
        let mut file = opts.open(&temp_path).map_err(OperationsError::Io)?;
        file.write_all(raw.as_bytes())
            .map_err(OperationsError::Io)?;
        file.sync_all().map_err(OperationsError::Io)?;
        drop(file);

        let temp_path_str = temp_path.to_str().ok_or_else(|| {
            OperationsError::InvalidInput("commit tempfile path is not valid UTF-8".to_string())
        })?;
        let out = run_sanitized_git(
            canonical_repo,
            &["hash-object", "-t", "commit", "-w", "--", temp_path_str],
            DEFAULT_GIT_COMMAND_TIMEOUT_MS,
        )?;
        let commit_sha = validate_exact_40_hex_sha(out.trim(), "execution_base_commit")?;
        let resolved_commit =
            resolve_exact_commit_sha(canonical_repo, &commit_sha, DEFAULT_GIT_COMMAND_TIMEOUT_MS)?;
        let resolved_tree = resolve_exact_tree_sha(
            canonical_repo,
            &resolved_commit,
            DEFAULT_GIT_COMMAND_TIMEOUT_MS,
        )?;
        if resolved_tree != tree_sha {
            return Err(OperationsError::ReceiptInvalidated(format!(
                "written commit `{resolved_commit}` tree `{resolved_tree}` did not match expected `{tree_sha}`"
            )));
        }
        Ok(resolved_commit)
    })();

    let _ = fs::remove_file(&temp_path);
    write_res
}

fn ensure_snapshot_ref_not_symbolic(
    canonical_repo: &Path,
    snapshot_ref: &str,
) -> Result<(), OperationsError> {
    let sym_check = run_sanitized_git_allow_nonzero(
        canonical_repo,
        &["symbolic-ref", "--quiet", snapshot_ref],
        4096,
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )?;
    match sym_check.exit_code {
        Some(0) => Err(OperationsError::SecurityViolation(format!(
            "dependency snapshot_ref `{snapshot_ref}` must not be a symbolic ref"
        ))),
        Some(1) => Ok(()),
        other => Err(OperationsError::GitCommandFailed {
            command: format!("git symbolic-ref --quiet {snapshot_ref}"),
            status: other,
            stderr: sym_check.stderr,
        }),
    }
}

fn probe_direct_snapshot_ref(
    canonical_repo: &Path,
    snapshot_ref: &str,
) -> Result<Option<String>, OperationsError> {
    ensure_snapshot_ref_not_symbolic(canonical_repo, snapshot_ref)?;

    // With --quiet, a missing ref is exit 1; other failures must not be treated as absence.
    let exists = run_sanitized_git_allow_nonzero(
        canonical_repo,
        &["show-ref", "--verify", "--quiet", "--", snapshot_ref],
        DEFAULT_MAX_GIT_OUTPUT_BYTES,
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )?;
    match exists.exit_code {
        Some(0) => {}
        Some(1) => return Ok(None),
        other => {
            return Err(OperationsError::GitCommandFailed {
                command: "git show-ref --verify --quiet".to_string(),
                status: other,
                stderr: exists.stderr,
            })
        }
    }

    let show_ref = run_sanitized_git_allow_nonzero(
        canonical_repo,
        &["show-ref", "--verify", "--hash", "--", snapshot_ref],
        DEFAULT_MAX_GIT_OUTPUT_BYTES,
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )?;
    if show_ref.exit_code == Some(0) {
        let stdout = String::from_utf8_lossy(&show_ref.stdout_bytes);
        let raw_sha = validate_exact_40_hex_sha(stdout.trim(), "existing_snapshot_ref")?;
        let commit_sha =
            resolve_exact_commit_sha(canonical_repo, &raw_sha, DEFAULT_GIT_COMMAND_TIMEOUT_MS)
                .map_err(|err| {
                    OperationsError::ReceiptInvalidated(format!(
                "snapshot ref `{snapshot_ref}` target `{raw_sha}` is not a valid commit: {err}"
            ))
                })?;
        if commit_sha != raw_sha {
            return Err(OperationsError::ReceiptInvalidated(format!(
                "snapshot ref `{snapshot_ref}` target `{raw_sha}` did not resolve to exact commit (`{commit_sha}`)"
            )));
        }
        return Ok(Some(commit_sha));
    }

    Err(OperationsError::GitCommandFailed {
        command: "git show-ref --verify --hash".to_string(),
        status: show_ref.exit_code,
        stderr: show_ref.stderr,
    })
}

fn apply_or_reconcile_snapshot_ref(
    canonical_repo: &Path,
    snapshot_ref: &str,
    expected_commit_sha: &str,
    allow_existing_match: bool,
) -> Result<(), OperationsError> {
    if let Some(current_sha) = probe_direct_snapshot_ref(canonical_repo, snapshot_ref)? {
        if !allow_existing_match || current_sha != expected_commit_sha {
            return Err(OperationsError::ReceiptInvalidated(format!(
                "snapshot ref `{snapshot_ref}` already exists at `{current_sha}` (expected `{}`)",
                if allow_existing_match {
                    expected_commit_sha
                } else {
                    ZERO_COMMIT_SHA
                }
            )));
        }
        run_sanitized_git(
            canonical_repo,
            &[
                "update-ref",
                "--no-deref",
                "--",
                snapshot_ref,
                expected_commit_sha,
                &current_sha,
            ],
            DEFAULT_GIT_COMMAND_TIMEOUT_MS,
        )
        .map_err(|err| {
            OperationsError::ReceiptInvalidated(format!(
                "compare-and-swap update-ref failed for `{snapshot_ref}`: {err}"
            ))
        })?;
        ensure_snapshot_ref_not_symbolic(canonical_repo, snapshot_ref)?;
        return Ok(());
    }

    run_sanitized_git(
        canonical_repo,
        &[
            "update-ref",
            "--no-deref",
            "--",
            snapshot_ref,
            expected_commit_sha,
            ZERO_COMMIT_SHA,
        ],
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .map_err(|err| {
        OperationsError::ReceiptInvalidated(format!(
            "compare-and-swap update-ref failed for `{snapshot_ref}`: {err}"
        ))
    })?;
    ensure_snapshot_ref_not_symbolic(canonical_repo, snapshot_ref)?;
    Ok(())
}

fn verify_snapshot_ref_and_ancestry(
    canonical_repo: &Path,
    seal: &DependencyInputSeal,
) -> Result<(), OperationsError> {
    let expected_snapshot_ref = dependency_input_snapshot_ref(&seal.approval_id, &seal.task_id)?;
    if seal.snapshot_ref != expected_snapshot_ref {
        return Err(OperationsError::SecurityViolation(format!(
            "dependency seal snapshot_ref `{}` must equal `{expected_snapshot_ref}`",
            seal.snapshot_ref
        )));
    }
    let Some(ref_commit) = probe_direct_snapshot_ref(canonical_repo, &seal.snapshot_ref)? else {
        return Err(OperationsError::ReceiptInvalidated(format!(
            "snapshot_ref `{}` does not exist",
            seal.snapshot_ref
        )));
    };
    if ref_commit != seal.execution_base_commit {
        return Err(OperationsError::ReceiptInvalidated(format!(
            "snapshot_ref `{}` resolves to `{ref_commit}`, expected `{}`",
            seal.snapshot_ref, seal.execution_base_commit
        )));
    }
    let ref_tree = resolve_exact_tree_sha(
        canonical_repo,
        &seal.execution_base_commit,
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )?;
    if ref_tree != seal.execution_tree_sha {
        return Err(OperationsError::ReceiptInvalidated(format!(
            "execution_base_commit `{}` has tree `{ref_tree}`, expected `{}`",
            seal.execution_base_commit, seal.execution_tree_sha
        )));
    }
    if !is_commit_ancestor(
        canonical_repo,
        &seal.approved_base_commit,
        &seal.execution_base_commit,
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )? {
        return Err(OperationsError::AncestryMismatch(format!(
            "approved_base_commit `{}` is not an ancestor of execution_base_commit `{}`",
            seal.approved_base_commit, seal.execution_base_commit
        )));
    }
    for pred in &seal.predecessors {
        if !is_commit_ancestor(
            canonical_repo,
            &pred.accepted_task_head_commit,
            &seal.execution_base_commit,
            DEFAULT_GIT_COMMAND_TIMEOUT_MS,
        )? {
            return Err(OperationsError::AncestryMismatch(format!(
                "predecessor `{}` accepted_task_head_commit `{}` is not an ancestor of execution_base_commit `{}`",
                pred.task_id, pred.accepted_task_head_commit, seal.execution_base_commit
            )));
        }
    }
    Ok(())
}

fn verify_primary_repo_unchanged(
    canonical_repo: &Path,
    expected_head: &str,
    expected_status: &str,
) -> Result<(), OperationsError> {
    let head_after =
        resolve_exact_commit_sha(canonical_repo, "HEAD", DEFAULT_GIT_COMMAND_TIMEOUT_MS)?;
    if head_after != expected_head {
        return Err(OperationsError::HeadMoved {
            target: "primary_repository".to_string(),
            expected: expected_head.to_string(),
            actual: head_after,
        });
    }
    let status_after = run_sanitized_git(
        canonical_repo,
        &["status", "--porcelain=v1", "-uall"],
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )?;
    if status_after != expected_status {
        return Err(OperationsError::DirtyWorktree {
            worktree_id: "primary_repository".to_string(),
            detail: "primary repository working tree status changed during dependency preparation"
                .to_string(),
        });
    }
    Ok(())
}

fn verify_intent_structure(intent: &DependencyPreparationIntent) -> Result<(), OperationsError> {
    if intent.schema_version != DEPENDENCY_INPUT_SCHEMA_VERSION {
        return Err(OperationsError::ReceiptInvalidated(format!(
            "unsupported dependency preparation intent schema_version {} (expected {DEPENDENCY_INPUT_SCHEMA_VERSION})",
            intent.schema_version
        )));
    }
    if intent.recorded_at_ms <= 0 {
        return Err(OperationsError::ReceiptInvalidated(
            "dependency preparation intent recorded_at_ms must be positive".to_string(),
        ));
    }
    intent.seal.verify_integrity()?;
    Ok(())
}

fn path_exists_no_symlink(path: &Path, label: &str) -> Result<bool, OperationsError> {
    match fs::symlink_metadata(path) {
        Ok(meta) => {
            if meta.file_type().is_symlink() {
                return Err(OperationsError::SecurityViolation(format!(
                    "{label} `{}` must not be a symlink",
                    path.display()
                )));
            }
            if !meta.file_type().is_file() {
                return Err(OperationsError::SecurityViolation(format!(
                    "{label} `{}` must be a regular file",
                    path.display()
                )));
            }
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                let mode = meta.permissions().mode() & 0o777;
                if mode != 0o600 {
                    return Err(OperationsError::SecurityViolation(format!(
                        "{label} `{}` has permissions {:04o}; expected private 0600",
                        path.display(),
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

fn write_once_private_json<T: Serialize>(
    target_path: &Path,
    value: &T,
    label: &str,
) -> Result<(), OperationsError> {
    let parent = target_path.parent().ok_or_else(|| {
        OperationsError::InvalidInput(format!(
            "{label} `{}` must have a parent directory",
            target_path.display()
        ))
    })?;
    let mut bytes = serde_json::to_vec_pretty(value)?;
    bytes.push(b'\n');
    if bytes.len() > MAX_DEPENDENCY_INPUT_BYTES {
        return Err(OperationsError::InvalidInput(format!(
            "{label} serialized size ({} bytes) exceeds maximum ({MAX_DEPENDENCY_INPUT_BYTES} bytes)",
            bytes.len()
        )));
    }

    let file_name = target_path
        .file_name()
        .and_then(|s| s.to_str())
        .ok_or_else(|| {
            OperationsError::InvalidInput(format!(
                "{label} `{}` has invalid file name",
                target_path.display()
            ))
        })?;
    let temp_path = parent.join(format!(".{file_name}.tmp.{}", Uuid::new_v4().simple()));

    let write_res = (|| -> Result<(), OperationsError> {
        let mut opts = OpenOptions::new();
        opts.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            opts.mode(0o600);
        }
        let mut file = opts.open(&temp_path).map_err(OperationsError::Io)?;
        file.write_all(&bytes).map_err(OperationsError::Io)?;
        file.sync_all().map_err(OperationsError::Io)?;
        drop(file);

        // Enforce write-once semantics using hard_link so an existing target is never overwritten.
        fs::hard_link(&temp_path, target_path).map_err(|err| {
            OperationsError::ReceiptInvalidated(format!(
                "write-once {label} `{}` already exists or could not be linked: {err}",
                target_path.display()
            ))
        })?;
        let _ = fs::remove_file(&temp_path);

        let dir_file = File::open(parent).map_err(OperationsError::Io)?;
        dir_file.sync_all().map_err(OperationsError::Io)?;
        Ok(())
    })();

    if write_res.is_err() {
        let _ = fs::remove_file(&temp_path);
    }
    write_res
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

fn now_epoch_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|d| i64::try_from(d.as_millis()).ok())
        .unwrap_or(1)
        .max(1)
}

fn read_dependency_json<T: serde::de::DeserializeOwned>(
    path: &Path,
    max_bytes: u64,
    label: &str,
) -> Result<T, OperationsError> {
    let limit = usize::try_from(max_bytes).map_err(|_| {
        OperationsError::InvalidInput("dependency artifact limit overflow".to_string())
    })?;
    let (bytes, _) = threadlane_workers::dependency::read_bounded_dependency_bytes(path, limit)?;
    serde_json::from_slice(&bytes)
        .map_err(|_| OperationsError::ReceiptInvalidated(format!("invalid {label} JSON")))
}
