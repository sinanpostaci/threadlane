use std::fs::{self, File, OpenOptions};
use std::io::Read;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::error::WorkerError;
use crate::fs_safety::{
    ensure_platform_supported, read_private_json, sanitize_and_redact,
    validate_absolute_no_symlink_components, validate_existing_canonical_dir,
};
use crate::git::{
    is_commit_ancestor, resolve_exact_commit_sha, resolve_exact_tree_sha, run_sanitized_git,
    run_sanitized_git_allow_nonzero, WorkAreaOwnershipMarker, DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    MANIFESTS_SUBDIR, MAX_MANIFEST_BYTES, WORK_AREA_MARKER_FILENAME, WORK_AREA_MARKER_VERSION,
};
use crate::spec::{validate_simple_identifier, MAX_PROMPT_BYTES};

/// Schema version for [`DependencyInputSeal`] manifests.
pub const DEPENDENCY_INPUT_SCHEMA_VERSION: u32 = 1;

/// Maximum permitted byte length for a dependency input seal file (`256 KiB`).
pub const MAX_DEPENDENCY_INPUT_BYTES: usize = 262_144;

/// Maximum number of direct predecessor inputs allowed in a single [`DependencyInputSeal`].
pub const MAX_DEPENDENCY_PREDECESSORS: usize = 32;

/// Subdirectory inside `<work_area_root>/manifests` holding private dependency input seals and locks.
pub const DEPENDENCIES_MANIFEST_SUBDIR: &str = "dependencies";

/// Maximum permitted length for a dependency input `command_key`.
const MAX_COMMAND_KEY_LEN: usize = 128;

const DEPENDENCY_INPUT_SEAL_DIGEST_DOMAIN: &str = "threadlane.dependency_input_seal.v1";

/// Static policy binding a dependent worker profile to an expected sealed dependency input manifest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DependencyInputPolicy {
    /// Absolute path to `<work_area_root>/manifests/dependencies/<task_id>.json`.
    pub seal_path: PathBuf,
    /// Identifier of the governing plan approval receipt.
    pub approval_id: String,
    /// Lowercase 64-character SHA-256 hex digest of the approved plan subject.
    pub approval_subject_digest: String,
    /// Dependent task identifier in the approved plan.
    pub task_id: String,
    /// Lowercase 64-character SHA-256 hex digest of the dependent task definition.
    pub task_digest: String,
    /// Lowercase 64-character SHA-256 hex digest of the exact worker instruction.
    pub instruction_digest: String,
    /// Expected idempotent `StartWorker` command key bound to this dependent task.
    pub command_key: String,
}

/// Verified predecessor task provenance recorded inside a [`DependencyInputSeal`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DependencyPredecessorInput {
    /// Predecessor task identifier.
    pub task_id: String,
    /// Lowercase 64-character SHA-256 hex digest of the predecessor task definition.
    pub task_digest: String,
    /// Runtime run identifier that executed the predecessor task.
    pub runtime_run_id: String,
    /// Runtime worker identifier that executed the predecessor task.
    pub worker_id: String,
    /// Owned task worktree identifier of the predecessor execution.
    pub task_worktree_id: String,
    /// Lowercase 64-character SHA-256 hex digest of the reviewed predecessor subject.
    pub review_subject_digest: String,
    /// Identifier of the predecessor review acceptance receipt.
    pub review_receipt_id: String,
    /// Lowercase 64-character SHA-256 hex digest of the predecessor review receipt.
    pub review_receipt_digest: String,
    /// Identifier of the predecessor integration receipt.
    pub integration_receipt_id: String,
    /// Lowercase 64-character SHA-256 hex digest of the predecessor integration receipt.
    pub integration_receipt_digest: String,
    /// Exact lowercase 40-character hexadecimal Git commit SHA of the accepted predecessor head.
    pub accepted_task_head_commit: String,
}

/// Private (`0600`) dependency input seal persisted at
/// `<work_area_root>/manifests/dependencies/<task_id>.json`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DependencyInputSeal {
    /// Seal schema version (`1`).
    pub schema_version: u32,
    /// Governing plan approval identifier.
    pub approval_id: String,
    /// Lowercase 64-character SHA-256 hex digest of the approved plan subject.
    pub approval_subject_digest: String,
    /// Dependent task identifier.
    pub task_id: String,
    /// Lowercase 64-character SHA-256 hex digest of the dependent task definition.
    pub task_digest: String,
    /// Lowercase 64-character SHA-256 hex digest of the exact worker instruction.
    pub instruction_digest: String,
    /// Canonical path to the source Git repository.
    pub repository_path: PathBuf,
    /// Canonical path to the private work-area root directory.
    pub work_area_root: PathBuf,
    /// Approved baseline 40-character lowercase hex commit SHA of the plan.
    pub approved_base_commit: String,
    /// Synthesized execution base 40-character lowercase hex commit SHA for the dependent task.
    pub execution_base_commit: String,
    /// Exact 40-character lowercase hex Git tree SHA of `execution_base_commit`.
    pub execution_tree_sha: String,
    /// Exact Git ref (`refs/threadlane/dependency-inputs/<approval_id>/<task_id>`) pinning `execution_base_commit`.
    pub snapshot_ref: String,
    /// Strictly sorted, non-empty list of verified direct predecessor inputs (`1..=32`).
    pub predecessors: Vec<DependencyPredecessorInput>,
    /// Canonical lowercase 64-character SHA-256 hex digest covering all fields of this seal.
    pub seal_digest: String,
}

#[derive(Serialize)]
struct CanonicalDependencyInputSealDigestPayload<'a> {
    domain: &'static str,
    schema_version: u32,
    approval_id: &'a str,
    approval_subject_digest: &'a str,
    task_id: &'a str,
    task_digest: &'a str,
    instruction_digest: &'a str,
    repository_path: &'a Path,
    work_area_root: &'a Path,
    approved_base_commit: &'a str,
    execution_base_commit: &'a str,
    execution_tree_sha: &'a str,
    snapshot_ref: &'a str,
    predecessors: &'a [DependencyPredecessorInput],
}

impl DependencyInputSeal {
    /// Validates all structural invariants of this seal and computes its canonical 64-character
    /// lowercase SHA-256 hex digest.
    pub fn compute_digest(&self) -> Result<String, WorkerError> {
        if self.schema_version != DEPENDENCY_INPUT_SCHEMA_VERSION {
            return Err(WorkerError::InvalidSpec(format!(
                "unsupported dependency input seal schema_version {} (expected {DEPENDENCY_INPUT_SCHEMA_VERSION})",
                self.schema_version
            )));
        }

        validate_simple_identifier(&self.approval_id, "approval_id")?;
        validate_lowercase_sha256_hex(&self.approval_subject_digest, "approval_subject_digest")?;
        validate_simple_identifier(&self.task_id, "task_id")?;
        validate_lowercase_sha256_hex(&self.task_digest, "task_digest")?;
        validate_lowercase_sha256_hex(&self.instruction_digest, "instruction_digest")?;

        let checked_repo =
            validate_absolute_no_symlink_components(&self.repository_path, "repository_path")?;
        let checked_work_area =
            validate_absolute_no_symlink_components(&self.work_area_root, "work_area_root")?;
        if checked_work_area.starts_with(&checked_repo)
            || checked_repo.starts_with(&checked_work_area)
        {
            return Err(WorkerError::InvalidSpec(format!(
                "dependency seal work_area_root `{}` and repository_path `{}` must not overlap",
                checked_work_area.display(),
                checked_repo.display()
            )));
        }

        validate_lowercase_40_hex(&self.approved_base_commit, "approved_base_commit")?;
        validate_lowercase_40_hex(&self.execution_base_commit, "execution_base_commit")?;
        validate_lowercase_40_hex(&self.execution_tree_sha, "execution_tree_sha")?;

        let expected_snapshot_ref = format!(
            "refs/threadlane/dependency-inputs/{}/{}",
            self.approval_id, self.task_id
        );
        if self.snapshot_ref != expected_snapshot_ref {
            return Err(WorkerError::InvalidSpec(format!(
                "dependency seal snapshot_ref `{}` must equal `{expected_snapshot_ref}`",
                self.snapshot_ref
            )));
        }

        if self.predecessors.is_empty() || self.predecessors.len() > MAX_DEPENDENCY_PREDECESSORS {
            return Err(WorkerError::InvalidSpec(format!(
                "dependency seal predecessors count {} must be within 1..={MAX_DEPENDENCY_PREDECESSORS}",
                self.predecessors.len()
            )));
        }

        for pred in &self.predecessors {
            validate_simple_identifier(&pred.task_id, "predecessor.task_id")?;
            if pred.task_id == self.task_id {
                return Err(WorkerError::InvalidSpec(format!(
                    "dependency seal task `{}` must not list itself as a predecessor",
                    self.task_id
                )));
            }
            validate_lowercase_sha256_hex(&pred.task_digest, "predecessor.task_digest")?;
            validate_simple_identifier(&pred.runtime_run_id, "predecessor.runtime_run_id")?;
            validate_simple_identifier(&pred.worker_id, "predecessor.worker_id")?;
            validate_simple_identifier(&pred.task_worktree_id, "predecessor.task_worktree_id")?;
            validate_lowercase_sha256_hex(
                &pred.review_subject_digest,
                "predecessor.review_subject_digest",
            )?;
            validate_simple_identifier(&pred.review_receipt_id, "predecessor.review_receipt_id")?;
            validate_lowercase_sha256_hex(
                &pred.review_receipt_digest,
                "predecessor.review_receipt_digest",
            )?;
            validate_simple_identifier(
                &pred.integration_receipt_id,
                "predecessor.integration_receipt_id",
            )?;
            validate_lowercase_sha256_hex(
                &pred.integration_receipt_digest,
                "predecessor.integration_receipt_digest",
            )?;
            validate_lowercase_40_hex(
                &pred.accepted_task_head_commit,
                "predecessor.accepted_task_head_commit",
            )?;
        }

        for pair in self.predecessors.windows(2) {
            if pair[0].task_id >= pair[1].task_id {
                return Err(WorkerError::InvalidSpec(format!(
                    "dependency seal predecessors must be strictly sorted and unique by task_id (`{}` >= `{}`)",
                    pair[0].task_id, pair[1].task_id
                )));
            }
        }

        let payload = CanonicalDependencyInputSealDigestPayload {
            domain: DEPENDENCY_INPUT_SEAL_DIGEST_DOMAIN,
            schema_version: self.schema_version,
            approval_id: &self.approval_id,
            approval_subject_digest: &self.approval_subject_digest,
            task_id: &self.task_id,
            task_digest: &self.task_digest,
            instruction_digest: &self.instruction_digest,
            repository_path: &self.repository_path,
            work_area_root: &self.work_area_root,
            approved_base_commit: &self.approved_base_commit,
            execution_base_commit: &self.execution_base_commit,
            execution_tree_sha: &self.execution_tree_sha,
            snapshot_ref: &self.snapshot_ref,
            predecessors: &self.predecessors,
        };
        let encoded = serde_json::to_vec(&payload)?;
        Ok(sha256_hex_bytes(&encoded))
    }

    /// Verifies that all fields of this seal are valid and that `self.seal_digest` matches
    /// [`Self::compute_digest`].
    pub fn verify_integrity(&self) -> Result<(), WorkerError> {
        validate_lowercase_sha256_hex(&self.seal_digest, "seal_digest")?;
        let expected = self.compute_digest()?;
        if self.seal_digest != expected {
            return Err(WorkerError::SecurityViolation(format!(
                "dependency seal_digest `{}` does not match computed digest `{expected}`",
                self.seal_digest
            )));
        }
        Ok(())
    }
}

/// Verified dependency input seal together with its raw file digest and active shared lock lease.
///
/// Holds the shared `fs2` lock on `<work_area_root>/manifests/dependencies/<task_id>.lock` until dropped.
#[derive(Debug)]
pub struct VerifiedDependencyInput {
    /// Parsed and integrity-verified dependency input seal.
    pub seal: DependencyInputSeal,
    /// Lowercase 64-character SHA-256 hex digest of the exact on-disk seal JSON bytes.
    pub seal_file_digest: String,
    lock_file: File,
}

impl Drop for VerifiedDependencyInput {
    fn drop(&mut self) {
        let _ = self.lock_file.unlock();
    }
}

/// Returns the lock file path (`<work_area_root>/manifests/dependencies/<task_id>.lock`) corresponding
/// to `policy.seal_path`.
pub fn dependency_input_lock_path(policy: &DependencyInputPolicy) -> PathBuf {
    policy.seal_path.with_extension("lock")
}

/// Validates the static shape of a [`DependencyInputPolicy`] against `work_area_root` without
/// requiring the seal file to exist or performing any filesystem writes.
pub fn validate_dependency_input_policy(
    policy: &DependencyInputPolicy,
    work_area_root: &Path,
) -> Result<(), WorkerError> {
    let checked_work_area =
        validate_absolute_no_symlink_components(work_area_root, "work_area_root")?;
    let checked_seal_path =
        validate_absolute_no_symlink_components(&policy.seal_path, "dependency_input.seal_path")?;

    validate_simple_identifier(&policy.approval_id, "dependency_input.approval_id")?;
    validate_lowercase_sha256_hex(
        &policy.approval_subject_digest,
        "dependency_input.approval_subject_digest",
    )?;
    validate_simple_identifier(&policy.task_id, "dependency_input.task_id")?;
    validate_lowercase_sha256_hex(&policy.task_digest, "dependency_input.task_digest")?;
    validate_lowercase_sha256_hex(
        &policy.instruction_digest,
        "dependency_input.instruction_digest",
    )?;
    validate_command_key(&policy.command_key, "dependency_input.command_key")?;

    let expected_seal_path = checked_work_area
        .join(MANIFESTS_SUBDIR)
        .join(DEPENDENCIES_MANIFEST_SUBDIR)
        .join(format!("{}.json", policy.task_id));
    if checked_seal_path != expected_seal_path {
        return Err(WorkerError::InvalidSpec(format!(
            "dependency_input.seal_path `{}` must equal `{}`",
            checked_seal_path.display(),
            expected_seal_path.display()
        )));
    }

    let lock_path = dependency_input_lock_path(policy);
    let _ = validate_absolute_no_symlink_components(&lock_path, "dependency_input.lock_path")?;
    let expected_lock_path = checked_work_area
        .join(MANIFESTS_SUBDIR)
        .join(DEPENDENCIES_MANIFEST_SUBDIR)
        .join(format!("{}.lock", policy.task_id));
    if lock_path != expected_lock_path {
        return Err(WorkerError::InvalidSpec(format!(
            "dependency_input lock path `{}` must equal `{}`",
            lock_path.display(),
            expected_lock_path.display()
        )));
    }

    Ok(())
}

/// Reads, locks, and verifies a sealed dependency input manifest and its Git object/ancestry
/// invariants without creating or modifying any files or directories.
pub fn read_dependency_input(
    policy: &DependencyInputPolicy,
    repository: &Path,
    work_area: &Path,
    approved_base: &str,
    instruction: &str,
    command_key: &str,
) -> Result<VerifiedDependencyInput, WorkerError> {
    ensure_platform_supported()?;

    validate_lowercase_40_hex(approved_base, "approved_base_commit")?;
    validate_instruction_text(instruction)?;
    validate_command_key(command_key, "command_key")?;

    let canonical_repo = validate_existing_canonical_dir(repository, "repository_path")?;
    let checked_work_area = validate_existing_private_dir_readonly(work_area, "work_area_root")?;

    if checked_work_area.starts_with(&canonical_repo)
        || canonical_repo.starts_with(&checked_work_area)
    {
        return Err(WorkerError::InvalidSpec(format!(
            "work_area_root `{}` and repository_path `{}` must not overlap",
            checked_work_area.display(),
            canonical_repo.display()
        )));
    }

    validate_dependency_input_policy(policy, &checked_work_area)?;

    if command_key != policy.command_key {
        return Err(WorkerError::SecurityViolation(format!(
            "command_key `{command_key}` does not match dependency_input.command_key `{}`",
            policy.command_key
        )));
    }

    let actual_instruction_digest = sha256_hex_bytes(instruction.as_bytes());
    if actual_instruction_digest != policy.instruction_digest {
        return Err(WorkerError::SecurityViolation(format!(
            "instruction digest `{actual_instruction_digest}` does not match dependency_input.instruction_digest `{}`",
            policy.instruction_digest
        )));
    }

    let common_git_dir = resolve_repository_common_git_dir_readonly(&canonical_repo)?;
    if checked_work_area.starts_with(&common_git_dir)
        || common_git_dir.starts_with(&checked_work_area)
    {
        return Err(WorkerError::InvalidSpec(format!(
            "work_area_root `{}` and repository common Git dir `{}` must not overlap",
            checked_work_area.display(),
            common_git_dir.display()
        )));
    }

    verify_work_area_ownership_marker_readonly(&checked_work_area, &common_git_dir)?;

    let manifests_dir = checked_work_area.join(MANIFESTS_SUBDIR);
    let _ = validate_existing_private_dir_readonly(&manifests_dir, "manifests dir")?;
    let deps_dir = manifests_dir.join(DEPENDENCIES_MANIFEST_SUBDIR);
    let _ = validate_existing_private_dir_readonly(&deps_dir, "dependencies manifests dir")?;

    let lock_path = dependency_input_lock_path(policy);
    let lock_file = open_existing_private_lock_shared(&lock_path)?;

    let (raw_bytes, seal_file_digest) =
        read_bounded_dependency_bytes(&policy.seal_path, MAX_DEPENDENCY_INPUT_BYTES)?;

    let seal: DependencyInputSeal = serde_json::from_slice(&raw_bytes).map_err(|err| {
        WorkerError::SecurityViolation(format!(
            "failed to parse dependency input seal `{}`: {}",
            policy.seal_path.display(),
            sanitize_and_redact(&err.to_string(), 256)
        ))
    })?;

    seal.verify_integrity()?;

    if seal.approval_id != policy.approval_id {
        return Err(WorkerError::SecurityViolation(format!(
            "dependency seal approval_id `{}` does not match policy `{}`",
            seal.approval_id, policy.approval_id
        )));
    }
    if seal.approval_subject_digest != policy.approval_subject_digest {
        return Err(WorkerError::SecurityViolation(format!(
            "dependency seal approval_subject_digest `{}` does not match policy `{}`",
            seal.approval_subject_digest, policy.approval_subject_digest
        )));
    }
    if seal.task_id != policy.task_id {
        return Err(WorkerError::SecurityViolation(format!(
            "dependency seal task_id `{}` does not match policy `{}`",
            seal.task_id, policy.task_id
        )));
    }
    if seal.task_digest != policy.task_digest {
        return Err(WorkerError::SecurityViolation(format!(
            "dependency seal task_digest `{}` does not match policy `{}`",
            seal.task_digest, policy.task_digest
        )));
    }
    if seal.instruction_digest != policy.instruction_digest {
        return Err(WorkerError::SecurityViolation(format!(
            "dependency seal instruction_digest `{}` does not match policy `{}`",
            seal.instruction_digest, policy.instruction_digest
        )));
    }
    if seal.repository_path != canonical_repo {
        return Err(WorkerError::SecurityViolation(format!(
            "dependency seal repository_path `{}` does not match canonical repository `{}`",
            seal.repository_path.display(),
            canonical_repo.display()
        )));
    }
    if seal.work_area_root != checked_work_area {
        return Err(WorkerError::SecurityViolation(format!(
            "dependency seal work_area_root `{}` does not match canonical work_area_root `{}`",
            seal.work_area_root.display(),
            checked_work_area.display()
        )));
    }
    if seal.approved_base_commit != approved_base {
        return Err(WorkerError::SecurityViolation(format!(
            "dependency seal approved_base_commit `{}` does not match profile base_commit `{approved_base}`",
            seal.approved_base_commit
        )));
    }

    verify_seal_git_invariants(&canonical_repo, &seal)?;

    Ok(VerifiedDependencyInput {
        seal,
        seal_file_digest,
        lock_file,
    })
}

fn verify_seal_git_invariants(
    canonical_repo: &Path,
    seal: &DependencyInputSeal,
) -> Result<(), WorkerError> {
    // Reject symbolic snapshot refs so snapshot_ref must be a direct ref pinning execution_base_commit.
    let sym_check = run_sanitized_git_allow_nonzero(
        canonical_repo,
        &["symbolic-ref", "--quiet", &seal.snapshot_ref],
        4096,
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )?;
    if sym_check.exit_code == Some(0) {
        return Err(WorkerError::SecurityViolation(format!(
            "dependency snapshot_ref `{}` must not be a symbolic ref",
            seal.snapshot_ref
        )));
    }

    let show_ref_out = run_sanitized_git(
        canonical_repo,
        &["show-ref", "--verify", "--hash", "--", &seal.snapshot_ref],
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .map_err(|err| {
        WorkerError::SecurityViolation(format!(
            "dependency snapshot_ref `{}` does not exist or failed verification: {err}",
            seal.snapshot_ref
        ))
    })?;
    let snapshot_target = show_ref_out.trim();
    if snapshot_target != seal.execution_base_commit {
        return Err(WorkerError::SecurityViolation(format!(
            "dependency snapshot_ref `{}` resolved to `{snapshot_target}`, expected execution_base_commit `{}`",
            seal.snapshot_ref, seal.execution_base_commit
        )));
    }

    let resolved_exec_commit = resolve_exact_commit_sha(
        canonical_repo,
        &seal.execution_base_commit,
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .map_err(|err| {
        WorkerError::SecurityViolation(format!(
            "dependency execution_base_commit `{}` is not a valid commit in repository: {err}",
            seal.execution_base_commit
        ))
    })?;
    if resolved_exec_commit != seal.execution_base_commit {
        return Err(WorkerError::SecurityViolation(format!(
            "dependency execution_base_commit resolved to `{resolved_exec_commit}`, expected `{}`",
            seal.execution_base_commit
        )));
    }

    let resolved_approved_base = resolve_exact_commit_sha(
        canonical_repo,
        &seal.approved_base_commit,
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .map_err(|err| {
        WorkerError::SecurityViolation(format!(
            "dependency approved_base_commit `{}` is not a valid commit in repository: {err}",
            seal.approved_base_commit
        ))
    })?;
    if resolved_approved_base != seal.approved_base_commit {
        return Err(WorkerError::SecurityViolation(format!(
            "dependency approved_base_commit resolved to `{resolved_approved_base}`, expected `{}`",
            seal.approved_base_commit
        )));
    }

    let actual_tree_sha = resolve_exact_tree_sha(
        canonical_repo,
        &seal.execution_base_commit,
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .map_err(|err| {
        WorkerError::SecurityViolation(format!(
            "failed to resolve tree SHA for execution_base_commit `{}`: {err}",
            seal.execution_base_commit
        ))
    })?;
    if actual_tree_sha != seal.execution_tree_sha {
        return Err(WorkerError::SecurityViolation(format!(
            "execution_base_commit `{}` has tree `{actual_tree_sha}`, expected seal execution_tree_sha `{}`",
            seal.execution_base_commit, seal.execution_tree_sha
        )));
    }

    if !is_commit_ancestor(
        canonical_repo,
        &seal.approved_base_commit,
        &seal.execution_base_commit,
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )? {
        return Err(WorkerError::SecurityViolation(format!(
            "approved_base_commit `{}` is not an ancestor of execution_base_commit `{}`",
            seal.approved_base_commit, seal.execution_base_commit
        )));
    }

    for pred in &seal.predecessors {
        let resolved_pred_head = resolve_exact_commit_sha(
            canonical_repo,
            &pred.accepted_task_head_commit,
            DEFAULT_GIT_COMMAND_TIMEOUT_MS,
        )
        .map_err(|err| {
            WorkerError::SecurityViolation(format!(
                "predecessor `{}` accepted_task_head_commit `{}` is not a valid commit: {err}",
                pred.task_id, pred.accepted_task_head_commit
            ))
        })?;
        if resolved_pred_head != pred.accepted_task_head_commit {
            return Err(WorkerError::SecurityViolation(format!(
                "predecessor `{}` accepted_task_head_commit resolved to `{resolved_pred_head}`, expected `{}`",
                pred.task_id, pred.accepted_task_head_commit
            )));
        }

        if !is_commit_ancestor(
            canonical_repo,
            &seal.approved_base_commit,
            &pred.accepted_task_head_commit,
            DEFAULT_GIT_COMMAND_TIMEOUT_MS,
        )? {
            return Err(WorkerError::SecurityViolation(format!(
                "approved_base_commit `{}` is not an ancestor of predecessor `{}` accepted_task_head_commit `{}`",
                seal.approved_base_commit, pred.task_id, pred.accepted_task_head_commit
            )));
        }

        if !is_commit_ancestor(
            canonical_repo,
            &pred.accepted_task_head_commit,
            &seal.execution_base_commit,
            DEFAULT_GIT_COMMAND_TIMEOUT_MS,
        )? {
            return Err(WorkerError::SecurityViolation(format!(
                "predecessor `{}` accepted_task_head_commit `{}` is not an ancestor of execution_base_commit `{}`",
                pred.task_id, pred.accepted_task_head_commit, seal.execution_base_commit
            )));
        }
    }

    Ok(())
}

fn validate_existing_private_dir_readonly(dir: &Path, label: &str) -> Result<PathBuf, WorkerError> {
    let canonical = validate_existing_canonical_dir(dir, label)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let meta = fs::symlink_metadata(&canonical)?;
        if meta.file_type().is_symlink() || !meta.file_type().is_dir() {
            return Err(WorkerError::SecurityViolation(format!(
                "{label} `{}` must be a non-symlink directory",
                canonical.display()
            )));
        }
        let mode = meta.permissions().mode() & 0o777;
        if mode != 0o700 {
            return Err(WorkerError::SecurityViolation(format!(
                "{label} `{}` has permissions {:04o} (expected 0700)",
                canonical.display(),
                mode
            )));
        }
    }
    Ok(canonical)
}

fn resolve_repository_common_git_dir_readonly(
    canonical_repo: &Path,
) -> Result<PathBuf, WorkerError> {
    let raw = run_sanitized_git(
        canonical_repo,
        &["rev-parse", "--git-common-dir"],
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )?;
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(WorkerError::GitCommandFailed {
            command: "git rev-parse --git-common-dir".to_string(),
            status: Some(0),
            stderr: "empty output from git rev-parse --git-common-dir".to_string(),
        });
    }
    let path = if Path::new(trimmed).is_absolute() {
        PathBuf::from(trimmed)
    } else {
        canonical_repo.join(trimmed)
    };
    let canonical = fs::canonicalize(&path)?;
    let meta = fs::symlink_metadata(&canonical)?;
    if !meta.file_type().is_dir() {
        return Err(WorkerError::InvalidSpec(format!(
            "resolved git-common-dir `{}` is not a directory",
            canonical.display()
        )));
    }
    Ok(canonical)
}

fn verify_work_area_ownership_marker_readonly(
    canonical_work_area: &Path,
    expected_common_git_dir: &Path,
) -> Result<(), WorkerError> {
    let marker_path = canonical_work_area.join(WORK_AREA_MARKER_FILENAME);
    if fs::symlink_metadata(&marker_path).is_err() {
        return Err(WorkerError::SecurityViolation(format!(
            "work_area_root ownership marker `{}` is missing",
            marker_path.display()
        )));
    }
    let marker: WorkAreaOwnershipMarker = read_private_json(
        &marker_path,
        MAX_MANIFEST_BYTES,
        "work area ownership marker",
    )?;
    if marker.marker_version != WORK_AREA_MARKER_VERSION {
        return Err(WorkerError::SecurityViolation(format!(
            "unsupported work area marker_version {} (expected {WORK_AREA_MARKER_VERSION})",
            marker.marker_version
        )));
    }
    if marker.work_area_root != canonical_work_area {
        return Err(WorkerError::SecurityViolation(format!(
            "work area marker root `{}` does not match canonical `{}`",
            marker.work_area_root.display(),
            canonical_work_area.display()
        )));
    }
    if marker.repository_common_git_dir != expected_common_git_dir {
        return Err(WorkerError::SecurityViolation(format!(
            "work_area_root `{}` is owned by repository `{}` (expected `{}`)",
            canonical_work_area.display(),
            marker.repository_common_git_dir.display(),
            expected_common_git_dir.display()
        )));
    }
    Ok(())
}

fn open_existing_private_lock_shared(lock_path: &Path) -> Result<File, WorkerError> {
    let _ = validate_absolute_no_symlink_components(lock_path, "dependency input lock")?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};

        let pre_meta = fs::symlink_metadata(lock_path).map_err(|err| {
            if err.kind() == std::io::ErrorKind::NotFound {
                WorkerError::SecurityViolation(format!(
                    "dependency input lock `{}` does not exist",
                    lock_path.display()
                ))
            } else {
                WorkerError::Io(err)
            }
        })?;
        if pre_meta.file_type().is_symlink() || !pre_meta.file_type().is_file() {
            return Err(WorkerError::SecurityViolation(format!(
                "dependency input lock `{}` must be a regular non-symlink file",
                lock_path.display()
            )));
        }
        let pre_mode = pre_meta.permissions().mode() & 0o777;
        if pre_mode != 0o600 {
            return Err(WorkerError::SecurityViolation(format!(
                "dependency input lock `{}` has insecure permissions {:04o} (expected 0600)",
                lock_path.display(),
                pre_mode
            )));
        }

        let file = OpenOptions::new()
            .read(true)
            .write(false)
            .create(false)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
            .open(lock_path)
            .map_err(|err| {
                if err.raw_os_error() == Some(libc::ELOOP) {
                    WorkerError::SecurityViolation(format!(
                        "dependency input lock `{}` must not be a symlink",
                        lock_path.display()
                    ))
                } else {
                    WorkerError::Io(err)
                }
            })?;

        file.lock_shared().map_err(WorkerError::Io)?;

        let post_sym = fs::symlink_metadata(lock_path)?;
        let open_meta = file.metadata()?;
        if post_sym.file_type().is_symlink()
            || !post_sym.file_type().is_file()
            || !open_meta.file_type().is_file()
        {
            let _ = file.unlock();
            return Err(WorkerError::SecurityViolation(format!(
                "dependency input lock `{}` must be a regular non-symlink file",
                lock_path.display()
            )));
        }
        if open_meta.dev() != pre_meta.dev()
            || open_meta.ino() != pre_meta.ino()
            || post_sym.dev() != open_meta.dev()
            || post_sym.ino() != open_meta.ino()
        {
            let _ = file.unlock();
            return Err(WorkerError::SecurityViolation(format!(
                "dependency input lock `{}` was replaced during open",
                lock_path.display()
            )));
        }
        let open_mode = open_meta.permissions().mode() & 0o777;
        if open_mode != 0o600 {
            let _ = file.unlock();
            return Err(WorkerError::SecurityViolation(format!(
                "dependency input lock `{}` has insecure permissions {:04o} (expected 0600)",
                lock_path.display(),
                open_mode
            )));
        }

        Ok(file)
    }

    #[cfg(not(unix))]
    {
        let _ = lock_path;
        ensure_platform_supported()?;
        unreachable!()
    }
}

/// Reads a private regular dependency artifact with no-follow/nonblocking opens and a hard byte limit.
/// Returns the acquired bytes and their exact SHA-256 digest without creating files.
pub fn read_bounded_dependency_bytes(
    seal_path: &Path,
    max_bytes: usize,
) -> Result<(Vec<u8>, String), WorkerError> {
    let _ = validate_absolute_no_symlink_components(seal_path, "dependency input seal")?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};

        let pre_meta = fs::symlink_metadata(seal_path).map_err(|err| {
            if err.kind() == std::io::ErrorKind::NotFound {
                WorkerError::SecurityViolation(format!(
                    "dependency input seal `{}` does not exist",
                    seal_path.display()
                ))
            } else {
                WorkerError::Io(err)
            }
        })?;
        if pre_meta.file_type().is_symlink() || !pre_meta.file_type().is_file() {
            return Err(WorkerError::SecurityViolation(format!(
                "dependency input seal `{}` must be a regular non-symlink file",
                seal_path.display()
            )));
        }
        let pre_mode = pre_meta.permissions().mode() & 0o777;
        if pre_mode != 0o600 {
            return Err(WorkerError::SecurityViolation(format!(
                "dependency input seal `{}` has insecure permissions {:04o} (expected 0600)",
                seal_path.display(),
                pre_mode
            )));
        }
        let max_u64 = max_bytes as u64;
        if pre_meta.len() > max_u64 {
            return Err(WorkerError::SecurityViolation(format!(
                "dependency input seal `{}` size {} exceeds maximum of {max_bytes} bytes",
                seal_path.display(),
                pre_meta.len()
            )));
        }

        let file = OpenOptions::new()
            .read(true)
            .write(false)
            .create(false)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
            .open(seal_path)
            .map_err(|err| {
                if err.raw_os_error() == Some(libc::ELOOP) {
                    WorkerError::SecurityViolation(format!(
                        "dependency input seal `{}` must not be a symlink",
                        seal_path.display()
                    ))
                } else {
                    WorkerError::Io(err)
                }
            })?;

        let open_meta = file.metadata()?;
        if !open_meta.file_type().is_file() {
            return Err(WorkerError::SecurityViolation(format!(
                "dependency input seal `{}` must be a regular file",
                seal_path.display()
            )));
        }
        if open_meta.dev() != pre_meta.dev() || open_meta.ino() != pre_meta.ino() {
            return Err(WorkerError::SecurityViolation(format!(
                "dependency input seal `{}` inode changed during open",
                seal_path.display()
            )));
        }
        let open_mode = open_meta.permissions().mode() & 0o777;
        if open_mode != 0o600 {
            return Err(WorkerError::SecurityViolation(format!(
                "dependency input seal `{}` has insecure permissions {:04o} (expected 0600)",
                seal_path.display(),
                open_mode
            )));
        }
        if open_meta.len() > max_u64 {
            return Err(WorkerError::SecurityViolation(format!(
                "dependency input seal `{}` size {} exceeds maximum of {max_bytes} bytes",
                seal_path.display(),
                open_meta.len()
            )));
        }

        let mut bytes = Vec::with_capacity(usize::try_from(open_meta.len()).unwrap_or(0));
        (&file)
            .take(max_u64.saturating_add(1))
            .read_to_end(&mut bytes)?;
        if bytes.len() > max_bytes {
            return Err(WorkerError::SecurityViolation(format!(
                "dependency input seal `{}` exceeded maximum size of {max_bytes} bytes during read",
                seal_path.display()
            )));
        }

        let post_meta = file.metadata()?;
        let post_sym = fs::symlink_metadata(seal_path)?;
        if post_meta.dev() != open_meta.dev()
            || post_meta.ino() != open_meta.ino()
            || post_sym.dev() != open_meta.dev()
            || post_sym.ino() != open_meta.ino()
        {
            return Err(WorkerError::SecurityViolation(format!(
                "dependency input seal `{}` was replaced during read",
                seal_path.display()
            )));
        }
        if post_meta.len() != open_meta.len() || (bytes.len() as u64) != open_meta.len() {
            return Err(WorkerError::SecurityViolation(format!(
                "dependency input seal `{}` size changed during read (expected {} bytes, read {} bytes, post-read {} bytes)",
                seal_path.display(),
                open_meta.len(),
                bytes.len(),
                post_meta.len()
            )));
        }

        let digest = sha256_hex_bytes(&bytes);
        Ok((bytes, digest))
    }

    #[cfg(not(unix))]
    {
        let _ = (seal_path, max_bytes);
        ensure_platform_supported()?;
        unreachable!()
    }
}

fn validate_lowercase_40_hex(value: &str, label: &str) -> Result<(), WorkerError> {
    if value.len() != 40
        || !value
            .bytes()
            .all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
    {
        return Err(WorkerError::InvalidSpec(format!(
            "{label} must be an exact 40-character lowercase hexadecimal SHA (got `{value}`)"
        )));
    }
    Ok(())
}

fn validate_lowercase_sha256_hex(value: &str, label: &str) -> Result<(), WorkerError> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
    {
        return Err(WorkerError::InvalidSpec(format!(
            "{label} must be an exact 64-character lowercase hexadecimal SHA-256 digest (got `{value}`)"
        )));
    }
    Ok(())
}

fn validate_command_key(value: &str, label: &str) -> Result<(), WorkerError> {
    let trimmed = value.trim();
    if trimmed.is_empty() || trimmed != value {
        return Err(WorkerError::InvalidSpec(format!(
            "{label} must be non-empty without leading or trailing whitespace"
        )));
    }
    if value.len() > MAX_COMMAND_KEY_LEN {
        return Err(WorkerError::InvalidSpec(format!(
            "{label} exceeds maximum length of {MAX_COMMAND_KEY_LEN} bytes"
        )));
    }
    if !value
        .bytes()
        .all(|b| b.is_ascii_graphic() && !b.is_ascii_control())
    {
        return Err(WorkerError::InvalidSpec(format!(
            "{label} must contain only printable non-whitespace ASCII characters"
        )));
    }
    Ok(())
}

fn validate_instruction_text(instruction: &str) -> Result<(), WorkerError> {
    if instruction.trim().is_empty() {
        return Err(WorkerError::InvalidSpec(
            "instruction must not be empty".to_string(),
        ));
    }
    if instruction.len() > MAX_PROMPT_BYTES {
        return Err(WorkerError::InvalidSpec(format!(
            "instruction length {} exceeds maximum of {MAX_PROMPT_BYTES} bytes",
            instruction.len()
        )));
    }
    if instruction.contains('\0') {
        return Err(WorkerError::InvalidSpec(
            "instruction must not contain NUL bytes".to_string(),
        ));
    }
    Ok(())
}

fn sha256_hex_bytes(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    let digest = hasher.finalize();
    let mut out = String::with_capacity(64);
    for byte in digest {
        use std::fmt::Write as _;
        let _ = write!(&mut out, "{byte:02x}");
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs_safety::{ensure_private_dir, open_private_lock_file, write_private_json_atomic};
    use crate::git::WorktreeManager;
    use fs2::FileExt;
    use std::process::Command;

    fn sample_predecessor(task_id: &str, head_commit: &str) -> DependencyPredecessorInput {
        DependencyPredecessorInput {
            task_id: task_id.to_string(),
            task_digest: "11".repeat(32),
            runtime_run_id: "run-pred-1".to_string(),
            worker_id: "worker-pred-1".to_string(),
            task_worktree_id: "wt-pred-1".to_string(),
            review_subject_digest: "22".repeat(32),
            review_receipt_id: "rev-rec-1".to_string(),
            review_receipt_digest: "33".repeat(32),
            integration_receipt_id: "int-rec-1".to_string(),
            integration_receipt_digest: "44".repeat(32),
            accepted_task_head_commit: head_commit.to_string(),
        }
    }

    fn sample_seal(repo: PathBuf, work_area: PathBuf) -> DependencyInputSeal {
        let mut seal = DependencyInputSeal {
            schema_version: DEPENDENCY_INPUT_SCHEMA_VERSION,
            approval_id: "approval-1".to_string(),
            approval_subject_digest: "aa".repeat(32),
            task_id: "task-b".to_string(),
            task_digest: "bb".repeat(32),
            instruction_digest: sha256_hex_bytes(b"Implement task B"),
            repository_path: repo,
            work_area_root: work_area,
            approved_base_commit: "1111111111111111111111111111111111111111".to_string(),
            execution_base_commit: "2222222222222222222222222222222222222222".to_string(),
            execution_tree_sha: "3333333333333333333333333333333333333333".to_string(),
            snapshot_ref: "refs/threadlane/dependency-inputs/approval-1/task-b".to_string(),
            predecessors: vec![sample_predecessor(
                "task-a",
                "2222222222222222222222222222222222222222",
            )],
            seal_digest: String::new(),
        };
        seal.seal_digest = seal.compute_digest().expect("compute digest");
        seal
    }

    fn git_out(repo: &Path, args: &[&str]) -> String {
        let out = Command::new("git")
            .arg("-C")
            .arg(repo)
            .args(args)
            .output()
            .expect("run git");
        assert!(
            out.status.success(),
            "git {:?} failed: {}",
            args,
            String::from_utf8_lossy(&out.stderr)
        );
        String::from_utf8_lossy(&out.stdout).trim().to_string()
    }

    #[test]
    fn seal_digest_and_integrity_roundtrip_and_tamper_detection() {
        let seal = sample_seal(
            PathBuf::from("/tmp/threadlane-test-repo"),
            PathBuf::from("/tmp/threadlane-test-work-area"),
        );
        seal.verify_integrity().expect("valid seal integrity");

        let mut tampered = seal.clone();
        tampered.execution_base_commit = "4444444444444444444444444444444444444444".to_string();
        assert!(tampered.verify_integrity().is_err());

        let mut unsorted = seal.clone();
        unsorted.predecessors = vec![
            sample_predecessor("task-c", "2222222222222222222222222222222222222222"),
            sample_predecessor("task-a", "2222222222222222222222222222222222222222"),
        ];
        assert!(unsorted.compute_digest().is_err());

        let mut self_dep = seal;
        self_dep.predecessors = vec![sample_predecessor(
            "task-b",
            "2222222222222222222222222222222222222222",
        )];
        assert!(self_dep.compute_digest().is_err());
    }

    #[test]
    fn read_dependency_input_verifies_git_ancestry_snapshot_ref_and_holds_shared_lock() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let repo = tmp.path().join("repo");
        let work_area = tmp.path().join("work-area");
        fs::create_dir_all(&repo).expect("create repo");

        git_out(&repo, &["init"]);
        git_out(&repo, &["config", "user.name", "Threadlane Test"]);
        git_out(&repo, &["config", "user.email", "test@threadlane.local"]);
        fs::write(repo.join("src.txt"), "base\n").expect("write base");
        git_out(&repo, &["add", "."]);
        git_out(&repo, &["commit", "-m", "base"]);
        let approved_base = git_out(&repo, &["rev-parse", "HEAD"]);

        fs::write(repo.join("src.txt"), "pred-a\n").expect("write pred-a");
        git_out(&repo, &["add", "."]);
        git_out(&repo, &["commit", "-m", "pred-a"]);
        let pred_head = git_out(&repo, &["rev-parse", "HEAD"]);
        let exec_tree = git_out(&repo, &["rev-parse", "HEAD^{tree}"]);

        let _mgr = WorktreeManager::new(&repo, &work_area, &approved_base).expect("init work area");
        let canonical_repo = fs::canonicalize(&repo).expect("canonical repo");
        let canonical_work_area = fs::canonicalize(&work_area).expect("canonical work area");

        let deps_dir = ensure_private_dir(
            &canonical_work_area
                .join(MANIFESTS_SUBDIR)
                .join(DEPENDENCIES_MANIFEST_SUBDIR),
            "dependencies dir",
        )
        .expect("create dependencies dir");

        let snapshot_ref = "refs/threadlane/dependency-inputs/approval-1/task-b";
        git_out(&canonical_repo, &["update-ref", snapshot_ref, &pred_head]);

        let instruction = "Implement task B";
        let instruction_digest = sha256_hex_bytes(instruction.as_bytes());
        let mut seal = DependencyInputSeal {
            schema_version: DEPENDENCY_INPUT_SCHEMA_VERSION,
            approval_id: "approval-1".to_string(),
            approval_subject_digest: "aa".repeat(32),
            task_id: "task-b".to_string(),
            task_digest: "bb".repeat(32),
            instruction_digest: instruction_digest.clone(),
            repository_path: canonical_repo.clone(),
            work_area_root: canonical_work_area.clone(),
            approved_base_commit: approved_base.clone(),
            execution_base_commit: pred_head.clone(),
            execution_tree_sha: exec_tree,
            snapshot_ref: snapshot_ref.to_string(),
            predecessors: vec![sample_predecessor("task-a", &pred_head)],
            seal_digest: String::new(),
        };
        seal.seal_digest = seal.compute_digest().expect("compute digest");

        let seal_path =
            write_private_json_atomic(&deps_dir, "task-b.json", &seal, "seal").expect("write seal");
        let policy = DependencyInputPolicy {
            seal_path,
            approval_id: "approval-1".to_string(),
            approval_subject_digest: "aa".repeat(32),
            task_id: "task-b".to_string(),
            task_digest: "bb".repeat(32),
            instruction_digest,
            command_key: "cmd-task-b".to_string(),
        };
        let lock_path = dependency_input_lock_path(&policy);
        let _ = open_private_lock_file(&lock_path, "dependency lock").expect("create lock");

        let verified = read_dependency_input(
            &policy,
            &canonical_repo,
            &canonical_work_area,
            &approved_base,
            instruction,
            "cmd-task-b",
        )
        .expect("read dependency input");
        assert_eq!(verified.seal, seal);

        // Shared lock lease must prevent exclusive lock acquisition until dropped.
        let writer_lock = File::open(&lock_path).expect("open lock");
        assert!(writer_lock.try_lock_exclusive().is_err());
        drop(verified);
        assert!(writer_lock.try_lock_exclusive().is_ok());
        let _ = writer_lock.unlock();

        // Moving snapshot_ref away from execution_base_commit must fail closed.
        git_out(
            &canonical_repo,
            &["update-ref", snapshot_ref, &approved_base],
        );
        assert!(read_dependency_input(
            &policy,
            &canonical_repo,
            &canonical_work_area,
            &approved_base,
            instruction,
            "cmd-task-b",
        )
        .is_err());
    }
}
