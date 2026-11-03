//! Bounded frozen verification check execution over software-owned `Task` and `RunIntegration`
//! Git worktrees, post-run subject revalidation, and verified [`CandidateCheckEvidence`] promotion.
//!
//! ## Internal Trusted Service Boundary & Authority Model
//!
//! - **Internal primitive, not standalone execution authority**: This module is an internal
//!   execution primitive invoked by the Coworker controller after verifying frozen operator
//!   approval and [`super::controller_policy::FrozenControllerExecutionEnvelope`] authority
//!   ([`CHECK_RUNNER_IS_INTERNAL_TRUSTED_PRIMITIVE`]).
//! - **Ownership guard proves single-driver liveness, not permission**: Holding a live
//!   [`CoworkerJobOwnershipGuard`] is mandatory for the entire check execution `.await` so two
//!   drivers cannot concurrently execute effects for the same `(binding_id, thread_uuid, job_id)`,
//!   but holding the guard alone grants no approval, model, or check execution authority
//!   ([`HOLDING_OWNERSHIP_GUARD_ALONE_GRANTS_NO_CHECK_EXECUTION_AUTHORITY`]).
//! - **No duplicate ledger**: This module does not maintain a separate on-disk state store. The
//!   calling controller persists [`FrozenCheckExecutionReport`] records and promoted
//!   [`CandidateCheckEvidence`] entries under its existing transactional runtime authority.
//! - **Observed execution proof only**: [`CandidateCheckEvidence`] can only be produced from a
//!   complete, integrity-verified [`FrozenCheckExecutionReport`] whose status is
//!   [`FrozenCheckExecutionStatus::Passed`] and whose subject and [`FrozenCheckCommandSpec`]
//!   digests match the expected context. Model prose or caller-supplied check IDs/digests can
//!   never be promoted into observed check proof.
//! - **Non-destructive drift refusal**: The target owned worktree must be clean and match the
//!   expected branch, base commit, `HEAD` commit, and `HEAD^{tree}` before spawning. If the check
//!   command mutates the worktree or the subject/ownership context drifts during execution, all
//!   worktree modifications and captured command outputs are preserved for inspection and passing
//!   evidence is refused ([`FrozenCheckExecutionStatus::SubjectMutatedOrDrifted`] or
//!   [`FrozenCheckExecutionStatus::OwnershipOrContextDrifted`]).

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Component, Path, PathBuf};

use serde::{Deserialize, Serialize};
use tokio::sync::watch;

use threadlane_workers::{
    begin_worktree_job, ensure_worktree_job_inactive_readonly, is_control_config_path,
    is_worktree_clean, load_and_verify_manifest, resolve_exact_commit_sha, resolve_exact_tree_sha,
    run_bounded_command_with_cancel, run_sanitized_git, validate_exact_40_hex_sha,
    validate_repo_relative_path, validate_sha256_hex_digest, BoundedCommandBudgets,
    BoundedCommandOverflow, BoundedCommandSpec, BoundedCommandStatus, CleanupRefusalReason,
    WorkerError, WorktreeKind, WorktreeManifest, DEFAULT_GIT_COMMAND_TIMEOUT_MS,
};

use crate::coworker_observation::{
    begin_coworker_call_observation, resolve_coworker_journal_dir, CoworkerCallFinishSpec,
    CoworkerCallObservationSpec, CoworkerObservationExecutorSpec, COWORKER_OBS_OP_CHECKS_CANDIDATE,
    COWORKER_OBS_OP_CHECKS_TASK,
};
use crate::error::{NativeError, NativeErrorCode};
use crate::fs_safety::{
    constant_time_str_eq, ensure_platform_supported, sanitize_diagnostic_text,
    validate_absolute_no_symlink_components, validate_bounded_identifier,
};
use threadlane_core::observation::ObservationCallState;

use super::contracts::{
    sha256_hex, CandidateCheckEvidence, MAX_COWORKER_ID_LEN, MAX_COWORKER_TEXT_BYTES,
};
use super::controller_policy::{FrozenCheckCommandSpec, FrozenCheckKind};
use super::ownership::{
    CoworkerDriverOwnershipClaim, CoworkerJobOwnershipGuard, CoworkerJobOwnershipKey,
    CoworkerLockFileIdentity, CoworkerOwnershipSummary,
};

/// Current wire and storage schema version for [`FrozenCheckExecutionReport`].
pub const FROZEN_CHECK_EXECUTION_SCHEMA_VERSION: u32 = 1;

/// Invariant marker: the frozen check runner is an internal primitive for the Coworker controller.
pub const CHECK_RUNNER_IS_INTERNAL_TRUSTED_PRIMITIVE: bool = true;

/// Invariant marker: holding [`CoworkerJobOwnershipGuard`] alone grants no check execution authority.
pub const HOLDING_OWNERSHIP_GUARD_ALONE_GRANTS_NO_CHECK_EXECUTION_AUTHORITY: bool = true;

/// Target owned worktree role for a frozen check command invocation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FrozenCheckTargetKind {
    /// Per-task execution worktree (`WorktreeKind::Task`, `required_for_task_review == true`).
    TaskWorktree,
    /// Per-run integration worktree (`WorktreeKind::RunIntegration`, `required_for_candidate_closure == true`).
    RunIntegrationWorktree,
}

impl FrozenCheckTargetKind {
    /// Returns the canonical `snake_case` wire label.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::TaskWorktree => "task_worktree",
            Self::RunIntegrationWorktree => "run_integration_worktree",
        }
    }

    /// Returns the required [`WorktreeKind`] in the private worktree manifest.
    pub const fn expected_worktree_kind(self) -> WorktreeKind {
        match self {
            Self::TaskWorktree => WorktreeKind::Task,
            Self::RunIntegrationWorktree => WorktreeKind::RunIntegration,
        }
    }

    /// Returns `true` when `spec` is configured to run on this target worktree kind.
    pub const fn is_spec_applicable(self, spec: &FrozenCheckCommandSpec) -> bool {
        match self {
            Self::TaskWorktree => spec.required_for_task_review,
            Self::RunIntegrationWorktree => spec.required_for_candidate_closure,
        }
    }
}

/// Exact scoped job, worktree, and Git commit/tree subject bound to a frozen check execution.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FrozenCheckSubject {
    /// Control-plane chat binding identifier.
    pub binding_id: String,
    /// Canonical lowercase hyphenated non-nil native Codex thread UUID.
    pub thread_uuid: String,
    /// Scoped Coworker workflow job identifier.
    pub job_id: String,
    /// Target worktree role (`TaskWorktree` or `RunIntegrationWorktree`).
    pub target_kind: FrozenCheckTargetKind,
    /// Canonical path to the primary source Git repository (never the worktree checkout itself).
    pub repository_path: PathBuf,
    /// Canonical path to the private Threadlane work-area root directory.
    pub work_area_root: PathBuf,
    /// Software-owned worktree identifier (`<work_area_root>/manifests/<worktree_id>.json`).
    pub worktree_id: String,
    /// Expected `run_id` recorded in the worktree manifest.
    pub expected_run_id: String,
    /// Expected `task_id` (`Some(task_id)` for `TaskWorktree`, `None` for `RunIntegrationWorktree`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_task_id: Option<String>,
    /// Expected 40-character lowercase hex `base_commit` SHA recorded in the worktree manifest.
    pub expected_base_commit: String,
    /// Expected 40-character lowercase hex `HEAD` commit SHA of the frozen worktree.
    pub expected_head_commit: String,
    /// Expected 40-character lowercase hex `HEAD^{tree}` Git tree SHA of the frozen worktree.
    pub expected_tree_sha: String,
}

impl FrozenCheckSubject {
    /// Returns the scoped [`CoworkerJobOwnershipKey`] for this subject.
    pub fn ownership_key(&self) -> Result<CoworkerJobOwnershipKey, NativeError> {
        CoworkerJobOwnershipKey::new(&self.binding_id, &self.thread_uuid, &self.job_id)
    }

    /// Validates all structural identifier, path, target-kind, and 40-hex SHA invariants.
    pub fn validate(&self) -> Result<(), NativeError> {
        let _ = self.ownership_key()?;
        validate_bounded_identifier(
            &self.worktree_id,
            "subject.worktree_id",
            MAX_COWORKER_ID_LEN,
        )?;
        validate_bounded_identifier(
            &self.expected_run_id,
            "subject.expected_run_id",
            MAX_COWORKER_ID_LEN,
        )?;

        match (self.target_kind, self.expected_task_id.as_deref()) {
            (FrozenCheckTargetKind::TaskWorktree, Some(task_id)) => {
                validate_bounded_identifier(
                    task_id,
                    "subject.expected_task_id",
                    MAX_COWORKER_ID_LEN,
                )?;
            }
            (FrozenCheckTargetKind::TaskWorktree, None) => {
                return Err(NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "FrozenCheckSubject for TaskWorktree requires expected_task_id.",
                ));
            }
            (FrozenCheckTargetKind::RunIntegrationWorktree, None) => {}
            (FrozenCheckTargetKind::RunIntegrationWorktree, Some(_)) => {
                return Err(NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "FrozenCheckSubject for RunIntegrationWorktree must not set expected_task_id.",
                ));
            }
        }

        validate_canonical_lowercase_40_hex(
            &self.expected_base_commit,
            "subject.expected_base_commit",
        )?;
        validate_canonical_lowercase_40_hex(
            &self.expected_head_commit,
            "subject.expected_head_commit",
        )?;
        validate_canonical_lowercase_40_hex(&self.expected_tree_sha, "subject.expected_tree_sha")?;

        validate_structural_absolute_path(&self.repository_path, "subject.repository_path")?;
        validate_structural_absolute_path(&self.work_area_root, "subject.work_area_root")?;

        if self.repository_path.starts_with(&self.work_area_root)
            || self.work_area_root.starts_with(&self.repository_path)
        {
            return Err(NativeError::new(
                NativeErrorCode::SecurityViolation,
                "FrozenCheckSubject.repository_path and work_area_root must not overlap.",
            ));
        }

        Ok(())
    }

    /// Computes a deterministic 64-character lowercase hex SHA-256 digest of this subject.
    pub fn subject_digest(&self) -> Result<String, NativeError> {
        self.validate()?;
        let repo_str = self.repository_path.to_string_lossy();
        let area_str = self.work_area_root.to_string_lossy();
        let seed = (
            "threadlane-native:frozen_check_subject:v1",
            self.binding_id.as_str(),
            self.thread_uuid.as_str(),
            self.job_id.as_str(),
            self.target_kind.as_str(),
            repo_str.as_ref(),
            area_str.as_ref(),
            self.worktree_id.as_str(),
            self.expected_run_id.as_str(),
            self.expected_task_id.as_deref(),
            self.expected_base_commit.as_str(),
            self.expected_head_commit.as_str(),
            self.expected_tree_sha.as_str(),
        );
        let bytes = serde_json::to_vec(&seed).map_err(|_| {
            NativeError::new(
                NativeErrorCode::ProtocolError,
                "Failed to serialize FrozenCheckSubject for digest calculation.",
            )
        })?;
        Ok(sha256_hex(&bytes))
    }
}

/// Terminal status of a supervised [`FrozenCheckCommandSpec`] execution over an owned worktree.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FrozenCheckExecutionStatus {
    /// Exited `0` cleanly within all budgets and post-run revalidation confirmed the owned
    /// worktree remained clean at the exact expected commit/tree with live ownership intact.
    Passed,
    /// Check process exited with a non-zero exit code or was terminated by a signal.
    NonZeroExit,
    /// Stdout or stderr exceeded the frozen byte cap (`max_stdout_bytes` or `max_stderr_bytes`).
    OutputOverflow,
    /// Execution exceeded `spec.timeout_ms` and the owned process group was terminated.
    TimedOut,
    /// Execution was explicitly cancelled and the owned process group was terminated.
    Cancelled,
    /// Direct child exited `0`, but surviving descendant processes in the owned process group
    /// had to be forcibly terminated after exit, so natural check completion was not proven.
    DescendantsTerminatedAfterExit,
    /// Post-exit pipe drainage timed out or owned process-group termination remained incomplete.
    IncompleteCleanup,
    /// The check command mutated the worktree (dirty/untracked/ignored files, commit/tree/branch
    /// drift, or working-directory symlink drift). Outputs and worktree changes are preserved;
    /// passing evidence is refused.
    SubjectMutatedOrDrifted,
    /// The held [`CoworkerJobOwnershipGuard`] failed post-run revalidation or drifted during
    /// execution.
    OwnershipOrContextDrifted,
}

impl FrozenCheckExecutionStatus {
    /// Returns the canonical `snake_case` wire label.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Passed => "passed",
            Self::NonZeroExit => "non_zero_exit",
            Self::OutputOverflow => "output_overflow",
            Self::TimedOut => "timed_out",
            Self::Cancelled => "cancelled",
            Self::DescendantsTerminatedAfterExit => "descendants_terminated_after_exit",
            Self::IncompleteCleanup => "incomplete_cleanup",
            Self::SubjectMutatedOrDrifted => "subject_mutated_or_drifted",
            Self::OwnershipOrContextDrifted => "ownership_or_context_drifted",
        }
    }

    /// Returns `true` only when the status is [`FrozenCheckExecutionStatus::Passed`].
    pub const fn is_passed(self) -> bool {
        matches!(self, Self::Passed)
    }
}

/// Complete, serializable execution report for a single [`FrozenCheckCommandSpec`] run over a
/// software-owned worktree.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FrozenCheckExecutionReport {
    /// Wire and storage schema version (`1`).
    pub schema_version: u32,
    /// Bound job, worktree, and Git commit/tree subject.
    pub subject: FrozenCheckSubject,
    /// CSPRNG `instance_id` (`cwo_<64hex>`) of the live [`CoworkerJobOwnershipGuard`] held across execution.
    pub ownership_instance_id: String,
    /// 64-character lowercase hex SHA-256 `challenge_digest` of the held ownership guard.
    pub ownership_challenge_digest: String,
    /// Locked file `(dev, ino)` identity of the held ownership guard.
    pub ownership_lock_file_identity: CoworkerLockFileIdentity,
    /// True if post-run revalidation of the held [`CoworkerJobOwnershipGuard`] succeeded without drift.
    pub ownership_verified_post_run: bool,
    /// Executed check identifier (`spec.check_id`).
    pub check_id: String,
    /// Executed check kind (`spec.kind`).
    pub kind: FrozenCheckKind,
    /// Canonical 64-character lowercase hex SHA-256 digest of the executed [`FrozenCheckCommandSpec`].
    pub spec_digest: String,
    /// Canonical executable path invoked.
    pub executable: PathBuf,
    /// Exact argument vector passed to `executable` (excluding `argv[0]`).
    pub argv: Vec<String>,
    /// Repository-relative working directory inside the worktree checkout.
    pub repo_relative_cwd: String,
    /// Canonical resolved working directory path where the process executed.
    pub resolved_working_directory: PathBuf,
    /// Enforced wall-clock timeout in milliseconds.
    pub timeout_ms: u64,
    /// Enforced stdout byte cap.
    pub max_stdout_bytes: usize,
    /// Enforced stderr byte cap.
    pub max_stderr_bytes: usize,
    /// Canonical worktree checkout path from the verified manifest.
    pub checkout_path: PathBuf,
    /// Dedicated software branch name checked out in `checkout_path`.
    pub branch_name: String,
    /// Terminal check execution status.
    pub status: FrozenCheckExecutionStatus,
    /// Observed process exit code, if exited normally.
    pub exit_code: Option<i32>,
    /// Observed termination signal, if terminated by a Unix signal.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signal: Option<i32>,
    /// Owned Unix process group ID (`pgid`) created for this check run.
    pub process_group_id: Option<i32>,
    /// Wall-clock duration in milliseconds.
    pub duration_ms: u64,
    /// Bounded raw stdout bytes captured (`<= max_stdout_bytes`).
    pub stdout_bytes: Vec<u8>,
    /// Bounded raw stderr bytes captured (`<= max_stderr_bytes`).
    pub stderr_bytes: Vec<u8>,
    /// Number of stdout bytes captured in `stdout_bytes`.
    pub stdout_bytes_captured: usize,
    /// Number of stderr bytes captured in `stderr_bytes`.
    pub stderr_bytes_captured: usize,
    /// 64-character lowercase hex SHA-256 digest of `stdout_bytes`.
    pub stdout_sha256: String,
    /// 64-character lowercase hex SHA-256 digest of `stderr_bytes`.
    pub stderr_sha256: String,
    /// True if stdout exceeded `max_stdout_bytes`.
    pub stdout_overflowed: bool,
    /// True if stderr exceeded `max_stderr_bytes`.
    pub stderr_overflowed: bool,
    /// Primary overflow classification when `stdout_overflowed` or `stderr_overflowed` is true.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub overflow: Option<BoundedCommandOverflow>,
    /// True if lingering descendant processes in the owned process group had to be terminated after direct child exit.
    pub descendants_terminated_after_exit: bool,
    /// True if post-exit pipe drainage or process-group termination remained incomplete.
    pub cleanup_incomplete: bool,
    /// Sanitized, secret-redacted UTF-8 summary of captured stdout (if non-empty).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub redacted_stdout_summary: Option<String>,
    /// Sanitized, secret-redacted UTF-8 summary of captured stderr (if non-empty).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub redacted_stderr_summary: Option<String>,
    /// True if the target worktree remained completely clean (no modified, staged, untracked, or ignored files) after execution.
    pub post_worktree_clean: bool,
    /// Observed 40-hex `HEAD` commit SHA in the worktree after execution, if resolvable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub post_head_commit: Option<String>,
    /// Observed 40-hex `HEAD^{tree}` SHA in the worktree after execution, if resolvable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub post_tree_sha: Option<String>,
    /// True if the worktree or its checkout/branch/commit/tree/cwd state drifted during execution.
    pub subject_drifted: bool,
    /// Sanitized diagnostic explanation when `subject_drifted` or `!ownership_verified_post_run` is true.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub drift_reason: Option<String>,
    /// Bounded, deterministic command summary for [`CandidateCheckEvidence::command_summary`].
    pub command_summary: String,
    /// Canonical 64-character lowercase hex SHA-256 digest committing to the full subject, spec,
    /// ownership identity, process outcome, output digests, and post-run revalidation state.
    pub evidence_digest: String,
}

impl FrozenCheckExecutionReport {
    /// Returns `true` if and only if this report passes full cryptographic/structural integrity
    /// validation and represents a clean, drift-free [`FrozenCheckExecutionStatus::Passed`] run.
    pub fn passed(&self) -> bool {
        self.status == FrozenCheckExecutionStatus::Passed
            && self.exit_code == Some(0)
            && self.signal.is_none()
            && !self.stdout_overflowed
            && !self.stderr_overflowed
            && self.overflow.is_none()
            && !self.descendants_terminated_after_exit
            && !self.cleanup_incomplete
            && self.ownership_verified_post_run
            && self.post_worktree_clean
            && !self.subject_drifted
            && self.drift_reason.is_none()
            && self.validate_integrity().is_ok()
    }

    /// Computes the canonical 64-character lowercase hex SHA-256 `evidence_digest` over all
    /// subject, spec, ownership, output-digest, and post-run state fields.
    pub fn compute_evidence_digest(&self) -> Result<String, NativeError> {
        let subject_digest = self.subject.subject_digest()?;
        let exe_str = self.executable.to_string_lossy();
        let workdir_str = self.resolved_working_directory.to_string_lossy();
        let checkout_str = self.checkout_path.to_string_lossy();
        let seed = (
            (
                "threadlane-native:frozen_check_execution_report:v1",
                self.schema_version,
                subject_digest.as_str(),
                self.ownership_instance_id.as_str(),
                self.ownership_challenge_digest.as_str(),
                self.ownership_lock_file_identity,
                self.ownership_verified_post_run,
                self.check_id.as_str(),
                self.kind.as_str(),
                self.spec_digest.as_str(),
            ),
            (
                exe_str.as_ref(),
                &self.argv,
                self.repo_relative_cwd.as_str(),
                workdir_str.as_ref(),
                self.timeout_ms,
                self.max_stdout_bytes,
                self.max_stderr_bytes,
                checkout_str.as_ref(),
                self.branch_name.as_str(),
            ),
            (
                self.status.as_str(),
                self.exit_code,
                self.signal,
                self.stdout_bytes_captured,
                self.stderr_bytes_captured,
                self.stdout_sha256.as_str(),
                self.stderr_sha256.as_str(),
                self.stdout_overflowed,
                self.stderr_overflowed,
                self.overflow,
                self.descendants_terminated_after_exit,
                self.cleanup_incomplete,
            ),
            (
                self.post_worktree_clean,
                self.post_head_commit.as_deref(),
                self.post_tree_sha.as_deref(),
                self.subject_drifted,
                self.drift_reason.as_deref(),
                self.command_summary.as_str(),
            ),
        );
        let bytes = serde_json::to_vec(&seed).map_err(|_| {
            NativeError::new(
                NativeErrorCode::ProtocolError,
                "Failed to serialize FrozenCheckExecutionReport for digest calculation.",
            )
        })?;
        Ok(sha256_hex(&bytes))
    }

    /// Validates structural invariants, output byte counts, SHA-256 byte digests, status
    /// consistency, and the top-level `evidence_digest` (suitable for post-serde recovery checks).
    pub fn validate_integrity(&self) -> Result<(), NativeError> {
        if self.schema_version != FROZEN_CHECK_EXECUTION_SCHEMA_VERSION {
            return Err(NativeError::new(
                NativeErrorCode::ProtocolError,
                format!(
                    "Unsupported FrozenCheckExecutionReport.schema_version {} (expected {FROZEN_CHECK_EXECUTION_SCHEMA_VERSION}).",
                    self.schema_version
                ),
            ));
        }
        self.subject.validate()?;
        validate_bounded_identifier(&self.check_id, "report.check_id", MAX_COWORKER_ID_LEN)?;
        validate_csprng_instance_id(&self.ownership_instance_id)?;
        validate_canonical_lowercase_64_hex(
            &self.ownership_challenge_digest,
            "report.ownership_challenge_digest",
        )?;
        validate_canonical_lowercase_64_hex(&self.spec_digest, "report.spec_digest")?;
        validate_canonical_lowercase_64_hex(&self.stdout_sha256, "report.stdout_sha256")?;
        validate_canonical_lowercase_64_hex(&self.stderr_sha256, "report.stderr_sha256")?;
        validate_canonical_lowercase_64_hex(&self.evidence_digest, "report.evidence_digest")?;

        if self.command_summary.trim().is_empty()
            || self.command_summary.len() > MAX_COWORKER_TEXT_BYTES
            || self.command_summary.chars().any(char::is_control)
        {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "FrozenCheckExecutionReport.command_summary must be non-empty, bounded, and free of control characters.",
            ));
        }

        if self.timeout_ms == 0 || self.max_stdout_bytes == 0 || self.max_stderr_bytes == 0 {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "FrozenCheckExecutionReport budgets must be non-zero.",
            ));
        }

        if self.stdout_bytes.len() != self.stdout_bytes_captured
            || self.stdout_bytes_captured > self.max_stdout_bytes
        {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "FrozenCheckExecutionReport stdout_bytes length does not match stdout_bytes_captured or exceeds max_stdout_bytes.",
            ));
        }
        if self.stderr_bytes.len() != self.stderr_bytes_captured
            || self.stderr_bytes_captured > self.max_stderr_bytes
        {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "FrozenCheckExecutionReport stderr_bytes length does not match stderr_bytes_captured or exceeds max_stderr_bytes.",
            ));
        }

        let actual_stdout_sha = sha256_hex(&self.stdout_bytes);
        if !constant_time_str_eq(&actual_stdout_sha, &self.stdout_sha256) {
            return Err(NativeError::new(
                NativeErrorCode::SecurityViolation,
                "FrozenCheckExecutionReport.stdout_sha256 does not match stdout_bytes.",
            ));
        }
        let actual_stderr_sha = sha256_hex(&self.stderr_bytes);
        if !constant_time_str_eq(&actual_stderr_sha, &self.stderr_sha256) {
            return Err(NativeError::new(
                NativeErrorCode::SecurityViolation,
                "FrozenCheckExecutionReport.stderr_sha256 does not match stderr_bytes.",
            ));
        }

        if let Some(ref head) = self.post_head_commit {
            validate_canonical_lowercase_40_hex(head, "report.post_head_commit")?;
        }
        if let Some(ref tree) = self.post_tree_sha {
            validate_canonical_lowercase_40_hex(tree, "report.post_tree_sha")?;
        }

        if self.status == FrozenCheckExecutionStatus::Passed
            && (self.exit_code != Some(0)
                || self.signal.is_some()
                || self.stdout_overflowed
                || self.stderr_overflowed
                || self.overflow.is_some()
                || self.descendants_terminated_after_exit
                || self.cleanup_incomplete
                || !self.ownership_verified_post_run
                || !self.post_worktree_clean
                || self.subject_drifted
                || self.drift_reason.is_some()
                || self.post_head_commit.as_deref()
                    != Some(self.subject.expected_head_commit.as_str())
                || self.post_tree_sha.as_deref() != Some(self.subject.expected_tree_sha.as_str()))
        {
            return Err(NativeError::new(
                NativeErrorCode::SecurityViolation,
                "FrozenCheckExecutionReport has status `passed` but violates required exit, natural descendant completion, budget, ownership, or clean-subject invariants.",
            ));
        }
        if self.status == FrozenCheckExecutionStatus::DescendantsTerminatedAfterExit
            && !self.descendants_terminated_after_exit
        {
            return Err(NativeError::new(
                NativeErrorCode::SecurityViolation,
                "FrozenCheckExecutionReport has status `descendants_terminated_after_exit` while descendants_terminated_after_exit is false.",
            ));
        }

        let computed_digest = self.compute_evidence_digest()?;
        if !constant_time_str_eq(&computed_digest, &self.evidence_digest) {
            return Err(NativeError::new(
                NativeErrorCode::SecurityViolation,
                "FrozenCheckExecutionReport.evidence_digest verification failed (report bytes were modified).",
            ));
        }

        Ok(())
    }

    /// Verifies [`Self::validate_integrity`] and confirms that this report was produced for the
    /// exact `expected_subject` and `expected_spec` (including `spec_digest`, executable, argv,
    /// cwd, and budgets).
    pub fn verify_against_context(
        &self,
        expected_subject: &FrozenCheckSubject,
        expected_spec: &FrozenCheckCommandSpec,
    ) -> Result<(), NativeError> {
        self.validate_integrity()?;
        expected_subject.validate()?;
        expected_spec.validate()?;

        if !expected_subject
            .target_kind
            .is_spec_applicable(expected_spec)
        {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!(
                    "FrozenCheckCommandSpec `{}` is not applicable to target_kind `{}`.",
                    expected_spec.check_id,
                    expected_subject.target_kind.as_str()
                ),
            ));
        }

        if &self.subject != expected_subject {
            return Err(NativeError::new(
                NativeErrorCode::SecurityViolation,
                "FrozenCheckExecutionReport.subject does not match expected check subject context.",
            ));
        }

        let expected_spec_digest = expected_spec.spec_digest()?;
        if self.check_id != expected_spec.check_id
            || self.kind != expected_spec.kind
            || !constant_time_str_eq(&self.spec_digest, &expected_spec_digest)
            || self.executable != expected_spec.executable
            || self.argv != expected_spec.argv
            || self.repo_relative_cwd != expected_spec.repo_relative_cwd
            || self.timeout_ms != expected_spec.timeout_ms
            || self.max_stdout_bytes != expected_spec.max_stdout_bytes
            || self.max_stderr_bytes != expected_spec.max_stderr_bytes
        {
            return Err(NativeError::new(
                NativeErrorCode::SecurityViolation,
                "FrozenCheckExecutionReport does not match expected FrozenCheckCommandSpec or spec_digest.",
            ));
        }

        let expected_summary = build_check_command_summary(expected_spec);
        if self.command_summary != expected_summary {
            return Err(NativeError::new(
                NativeErrorCode::SecurityViolation,
                "FrozenCheckExecutionReport.command_summary does not match expected FrozenCheckCommandSpec summary.",
            ));
        }

        Ok(())
    }

    /// Promotes this execution report into a verified [`CandidateCheckEvidence`] record if and
    /// only if [`Self::verify_against_context`] succeeds and [`Self::passed`] is `true`.
    pub fn to_candidate_check_evidence(
        &self,
        expected_subject: &FrozenCheckSubject,
        expected_spec: &FrozenCheckCommandSpec,
    ) -> Result<CandidateCheckEvidence, NativeError> {
        self.verify_against_context(expected_subject, expected_spec)?;
        if !self.passed() {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!(
                    "Cannot promote check `{}` with status `{}` to passing CandidateCheckEvidence.",
                    self.check_id,
                    self.status.as_str()
                ),
            ));
        }

        let evidence = CandidateCheckEvidence {
            check_id: self.check_id.clone(),
            command_summary: self.command_summary.clone(),
            passed: true,
            exit_code: 0,
            evidence_digest: self.evidence_digest.clone(),
        };
        evidence.validate_passed()?;
        Ok(evidence)
    }
}

/// Verifies that a [`CandidateCheckEvidence`] entry matches an authentic, passing
/// [`FrozenCheckExecutionReport`] for `expected_subject` and `expected_spec`.
///
/// Rejects fabricated caller/model check IDs, summaries, or digests that are not backed by a
/// verified execution report.
pub fn verify_candidate_check_evidence(
    evidence: &CandidateCheckEvidence,
    report: &FrozenCheckExecutionReport,
    expected_subject: &FrozenCheckSubject,
    expected_spec: &FrozenCheckCommandSpec,
) -> Result<(), NativeError> {
    evidence.validate_passed()?;
    let expected_evidence = report.to_candidate_check_evidence(expected_subject, expected_spec)?;
    if evidence.check_id != expected_evidence.check_id
        || evidence.command_summary != expected_evidence.command_summary
        || evidence.passed != expected_evidence.passed
        || evidence.exit_code != expected_evidence.exit_code
        || !constant_time_str_eq(
            &evidence.evidence_digest,
            &expected_evidence.evidence_digest,
        )
    {
        return Err(NativeError::new(
            NativeErrorCode::SecurityViolation,
            format!(
                "CandidateCheckEvidence `{}` does not match verified FrozenCheckExecutionReport proof.",
                evidence.check_id
            ),
        ));
    }
    Ok(())
}

/// Verifies a slice of [`FrozenCheckExecutionReport`] records against all required specs in
/// `check_commands` for `subject.target_kind` and returns the deterministically ordered
/// [`CandidateCheckEvidence`] list.
///
/// Fails closed if no checks are required for `subject.target_kind`, if any required check is
/// missing or failed, or if duplicate/unexpected check reports are supplied.
pub fn verify_and_promote_check_reports(
    subject: &FrozenCheckSubject,
    check_commands: &BTreeMap<String, FrozenCheckCommandSpec>,
    reports: &[FrozenCheckExecutionReport],
) -> Result<Vec<CandidateCheckEvidence>, NativeError> {
    subject.validate()?;
    let mut required_specs = BTreeMap::new();
    for (check_id, spec) in check_commands {
        spec.validate()?;
        if check_id != &spec.check_id {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "check_commands map key does not match FrozenCheckCommandSpec.check_id.",
            ));
        }
        if subject.target_kind.is_spec_applicable(spec) {
            required_specs.insert(check_id.clone(), spec);
        }
    }

    if required_specs.is_empty() {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!(
                "No frozen check commands are configured for target_kind `{}`.",
                subject.target_kind.as_str()
            ),
        ));
    }

    let mut seen_check_ids = BTreeSet::new();
    let mut reports_by_id = BTreeMap::new();
    for report in reports {
        if !seen_check_ids.insert(report.check_id.clone()) {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!(
                    "Duplicate FrozenCheckExecutionReport supplied for check_id `{}`.",
                    report.check_id
                ),
            ));
        }
        if !required_specs.contains_key(&report.check_id) {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!(
                    "Unexpected FrozenCheckExecutionReport supplied for check_id `{}`.",
                    report.check_id
                ),
            ));
        }
        reports_by_id.insert(report.check_id.clone(), report);
    }

    let mut promoted = Vec::with_capacity(required_specs.len());
    for (check_id, spec) in &required_specs {
        let Some(report) = reports_by_id.get(check_id) else {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!("Missing FrozenCheckExecutionReport for required check `{check_id}`."),
            ));
        };
        promoted.push(report.to_candidate_check_evidence(subject, spec)?);
    }

    Ok(promoted)
}

/// Executes a [`FrozenCheckCommandSpec`] over the software-owned worktree identified by `subject`
/// while holding `ownership_guard` across the entire `.await`, using a no-op cancellation channel.
pub async fn run_frozen_check(
    ownership_guard: &CoworkerJobOwnershipGuard,
    subject: &FrozenCheckSubject,
    spec: &FrozenCheckCommandSpec,
) -> Result<FrozenCheckExecutionReport, NativeError> {
    let (_cancel_tx, mut cancel_rx) = watch::channel(false);
    run_frozen_check_with_cancel(ownership_guard, subject, spec, &mut cancel_rx).await
}

/// Executes a [`FrozenCheckCommandSpec`] over the software-owned worktree identified by `subject`
/// while holding `ownership_guard` across the entire `.await` and supporting cancellation via
/// `cancel_rx`.
pub async fn run_frozen_check_with_cancel(
    ownership_guard: &CoworkerJobOwnershipGuard,
    subject: &FrozenCheckSubject,
    spec: &FrozenCheckCommandSpec,
    cancel_rx: &mut watch::Receiver<bool>,
) -> Result<FrozenCheckExecutionReport, NativeError> {
    run_frozen_check_with_claim(ownership_guard, None, None, subject, spec, cancel_rx).await
}

/// Executes a [`FrozenCheckCommandSpec`] over the software-owned worktree identified by `subject`
/// while holding `ownership_guard` (and optionally verifying an in-memory
/// [`CoworkerDriverOwnershipClaim`] and/or [`CoworkerOwnershipSummary`]) across the entire
/// `.await`.
pub async fn run_frozen_check_with_claim(
    ownership_guard: &CoworkerJobOwnershipGuard,
    expected_claim: Option<&CoworkerDriverOwnershipClaim>,
    expected_summary: Option<&CoworkerOwnershipSummary>,
    subject: &FrozenCheckSubject,
    spec: &FrozenCheckCommandSpec,
    cancel_rx: &mut watch::Receiver<bool>,
) -> Result<FrozenCheckExecutionReport, NativeError> {
    ensure_platform_supported()?;
    subject.validate()?;

    let ownership_key = subject.ownership_key()?;
    let pre_summary = ownership_guard
        .validate_before_effect(&ownership_key)?
        .clone();
    if let Some(claim) = expected_claim {
        let _ = ownership_guard.validate_driver_claim(&ownership_key, claim)?;
    }
    if let Some(summary) = expected_summary {
        let _ = ownership_guard.validate_summary_before_effect(&ownership_key, summary)?;
    }

    spec.validate()?;
    if !subject.target_kind.is_spec_applicable(spec) {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!(
                "FrozenCheckCommandSpec `{}` is not enabled for target_kind `{}`.",
                spec.check_id,
                subject.target_kind.as_str()
            ),
        ));
    }
    let spec_digest = spec.spec_digest()?;

    let canonical_repo = validate_existing_canonical_dir_native(
        &subject.repository_path,
        "subject.repository_path",
    )?;
    let canonical_work_area =
        validate_existing_canonical_dir_native(&subject.work_area_root, "subject.work_area_root")?;

    let manifest = load_and_verify_manifest(&canonical_work_area, &subject.worktree_id)
        .map_err(map_worker_error_to_native)?;
    validate_manifest_against_subject(&manifest, subject, &canonical_repo, &canonical_work_area)?;

    // Reject if a worker or check job is already active on this worktree (without creating a .lock file).
    ensure_worktree_job_inactive_readonly(&manifest).map_err(map_worker_error_to_native)?;

    let canonical_checkout =
        validate_existing_canonical_dir_native(&manifest.checkout_path, "manifest.checkout_path")?;
    if canonical_checkout != manifest.checkout_path {
        return Err(NativeError::new(
            NativeErrorCode::SecurityViolation,
            "Worktree checkout_path does not match its canonical path.",
        ));
    }

    verify_pre_run_checkout_state(&canonical_checkout, &manifest, subject)?;
    let resolved_working_directory =
        resolve_and_validate_check_cwd(&canonical_checkout, &spec.repo_relative_cwd)?;

    // Acquire exclusive worktree job lock for the duration of the check subprocess.
    let worktree_job_guard = begin_worktree_job(&canonical_work_area, &manifest.worktree_id)
        .map_err(map_worker_error_to_native)?;

    let bounded_spec = BoundedCommandSpec {
        executable: spec.executable.clone(),
        argv: spec.argv.clone(),
        working_directory: resolved_working_directory.clone(),
        env_overrides: Vec::new(),
        strip_git_env: true,
        budgets: BoundedCommandBudgets {
            timeout_ms: spec.timeout_ms,
            max_stdout_bytes: spec.max_stdout_bytes,
            max_stderr_bytes: spec.max_stderr_bytes,
        },
    };

    let obs_handle = resolve_coworker_journal_dir(
        None,
        Some(ownership_guard.lock_root_dir()),
        Some(&canonical_work_area),
        &subject.binding_id,
    )
    .map(|journal_dir| {
        let operation = match subject.target_kind {
            FrozenCheckTargetKind::TaskWorktree => COWORKER_OBS_OP_CHECKS_TASK,
            FrozenCheckTargetKind::RunIntegrationWorktree => COWORKER_OBS_OP_CHECKS_CANDIDATE,
        };
        begin_coworker_call_observation(CoworkerCallObservationSpec {
            journal_dir,
            operation: operation.to_string(),
            title: format!("Verification check `{}`", spec.check_id),
            call_key: format!(
                "chk:{}:{}:{}:{}:{}:{}",
                subject.job_id,
                subject.target_kind.as_str(),
                subject.worktree_id,
                subject.expected_head_commit,
                spec.check_id,
                spec_digest
            ),
            binding_id: subject.binding_id.clone(),
            thread_uuid: subject.thread_uuid.clone(),
            repository_path: canonical_repo.clone(),
            job_id: Some(subject.job_id.clone()),
            plan_id: None,
            task_id: subject.expected_task_id.clone(),
            approval_id: None,
            attempt_no: None,
            run_id: Some(subject.expected_run_id.clone()),
            worker_id: None,
            candidate_id: None,
            executor: CoworkerObservationExecutorSpec::local_verification_check(&spec.check_id),
            verified_context: None,
            entry_summary: Some(format!("check_id={}", spec.check_id)),
        })
    });

    let cmd_res = run_bounded_command_with_cancel(&bounded_spec, cancel_rx).await;
    let finish_res = worktree_job_guard.finish();

    let cmd_report = match cmd_res {
        Ok(rep) => rep,
        Err(err) => {
            if let Some(obs) = obs_handle {
                let _ = obs.finish(CoworkerCallFinishSpec {
                    state: Some(ObservationCallState::Failed),
                    diagnostic: Some(err.to_string()),
                    ..CoworkerCallFinishSpec::default()
                });
            }
            return Err(map_worker_error_to_native(err));
        }
    };
    if let Err(err) = finish_res {
        if let Some(obs) = obs_handle {
            let _ = obs.finish(CoworkerCallFinishSpec {
                state: Some(ObservationCallState::Failed),
                diagnostic: Some(err.to_string()),
                ..CoworkerCallFinishSpec::default()
            });
        }
        return Err(map_worker_error_to_native(err));
    }

    // Post-run revalidation of live ownership guard and worktree state.
    let mut drift_reasons = Vec::new();
    let ownership_verified_post_run = match ownership_guard.validate_before_effect(&ownership_key) {
        Ok(post_summary) => {
            if !constant_time_str_eq(&post_summary.instance_id, &pre_summary.instance_id)
                || !constant_time_str_eq(
                    &post_summary.challenge_digest,
                    &pre_summary.challenge_digest,
                )
                || post_summary.lock_file_identity != pre_summary.lock_file_identity
            {
                drift_reasons
                    .push("ownership guard identity drifted during check execution".to_string());
                false
            } else {
                true
            }
        }
        Err(err) => {
            drift_reasons.push(format!(
                "post-run ownership guard validation failed: {}",
                err.message
            ));
            false
        }
    };

    let post_state = inspect_post_run_subject_state(
        &canonical_work_area,
        &canonical_checkout,
        &resolved_working_directory,
        &manifest,
        subject,
        &spec.repo_relative_cwd,
    );
    if let Some(reason) = post_state.drift_reason {
        drift_reasons.push(reason);
    }

    let subject_drifted = post_state.subject_drifted;
    let post_worktree_clean = post_state.worktree_clean;
    let drift_reason = if drift_reasons.is_empty() {
        None
    } else {
        Some(sanitize_diagnostic_text(&drift_reasons.join("; ")))
    };

    let status = classify_check_status(
        cmd_report.status,
        cmd_report.passed(),
        cmd_report.descendants_terminated_after_exit,
        ownership_verified_post_run,
        post_worktree_clean,
        subject_drifted,
    );

    let stdout_sha256 = sha256_hex(&cmd_report.stdout_bytes);
    let stderr_sha256 = sha256_hex(&cmd_report.stderr_bytes);
    let command_summary = build_check_command_summary(spec);

    let mut report = FrozenCheckExecutionReport {
        schema_version: FROZEN_CHECK_EXECUTION_SCHEMA_VERSION,
        subject: subject.clone(),
        ownership_instance_id: pre_summary.instance_id,
        ownership_challenge_digest: pre_summary.challenge_digest,
        ownership_lock_file_identity: pre_summary.lock_file_identity,
        ownership_verified_post_run,
        check_id: spec.check_id.clone(),
        kind: spec.kind,
        spec_digest,
        executable: spec.executable.clone(),
        argv: spec.argv.clone(),
        repo_relative_cwd: spec.repo_relative_cwd.clone(),
        resolved_working_directory,
        timeout_ms: spec.timeout_ms,
        max_stdout_bytes: spec.max_stdout_bytes,
        max_stderr_bytes: spec.max_stderr_bytes,
        checkout_path: canonical_checkout,
        branch_name: manifest.branch_name,
        status,
        exit_code: cmd_report.exit_code,
        signal: cmd_report.signal,
        process_group_id: cmd_report.process_group_id,
        duration_ms: cmd_report.duration_ms,
        stdout_bytes: cmd_report.stdout_bytes,
        stderr_bytes: cmd_report.stderr_bytes,
        stdout_bytes_captured: cmd_report.stdout_bytes_captured,
        stderr_bytes_captured: cmd_report.stderr_bytes_captured,
        stdout_sha256,
        stderr_sha256,
        stdout_overflowed: cmd_report.stdout_overflowed,
        stderr_overflowed: cmd_report.stderr_overflowed,
        overflow: cmd_report.overflow,
        descendants_terminated_after_exit: cmd_report.descendants_terminated_after_exit,
        cleanup_incomplete: cmd_report.cleanup_incomplete,
        redacted_stdout_summary: cmd_report.redacted_stdout_summary,
        redacted_stderr_summary: cmd_report.redacted_stderr_summary,
        post_worktree_clean,
        post_head_commit: post_state.head_commit,
        post_tree_sha: post_state.tree_sha,
        subject_drifted,
        drift_reason,
        command_summary,
        evidence_digest: String::new(),
    };
    report.evidence_digest = report.compute_evidence_digest()?;
    report.validate_integrity()?;

    if let Some(obs) = obs_handle {
        let obs_state = match report.status {
            FrozenCheckExecutionStatus::Passed => ObservationCallState::Completed,
            FrozenCheckExecutionStatus::Cancelled => ObservationCallState::Cancelled,
            FrozenCheckExecutionStatus::IncompleteCleanup
            | FrozenCheckExecutionStatus::OwnershipOrContextDrifted => {
                ObservationCallState::Unknown
            }
            FrozenCheckExecutionStatus::NonZeroExit
            | FrozenCheckExecutionStatus::OutputOverflow
            | FrozenCheckExecutionStatus::TimedOut
            | FrozenCheckExecutionStatus::DescendantsTerminatedAfterExit
            | FrozenCheckExecutionStatus::SubjectMutatedOrDrifted => ObservationCallState::Failed,
        };
        let _ = obs.finish(CoworkerCallFinishSpec {
            state: Some(obs_state),
            artifact_id: Some(format!("fcr_{}", &report.evidence_digest[..16])),
            diagnostic: report
                .drift_reason
                .clone()
                .or_else(|| Some(format!("status={}", report.status.as_str()))),
            ..CoworkerCallFinishSpec::default()
        });
    }

    Ok(report)
}

struct PostRunSubjectState {
    worktree_clean: bool,
    head_commit: Option<String>,
    tree_sha: Option<String>,
    subject_drifted: bool,
    drift_reason: Option<String>,
}

fn validate_manifest_against_subject(
    manifest: &WorktreeManifest,
    subject: &FrozenCheckSubject,
    canonical_repo: &Path,
    canonical_work_area: &Path,
) -> Result<(), NativeError> {
    if manifest.repository_path != canonical_repo {
        return Err(NativeError::new(
            NativeErrorCode::SecurityViolation,
            "Worktree manifest repository_path does not match subject.repository_path.",
        ));
    }
    if manifest.work_area_root != canonical_work_area {
        return Err(NativeError::new(
            NativeErrorCode::SecurityViolation,
            "Worktree manifest work_area_root does not match subject.work_area_root.",
        ));
    }
    if manifest.checkout_path == canonical_repo
        || manifest.checkout_path.starts_with(canonical_repo)
        || canonical_repo.starts_with(&manifest.checkout_path)
    {
        return Err(NativeError::new(
            NativeErrorCode::SecurityViolation,
            "Refusing to run frozen check in primary repository checkout or overlapping path.",
        ));
    }
    if manifest.kind != subject.target_kind.expected_worktree_kind() {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!(
                "Worktree `{}` has kind `{:?}`, expected `{:?}`.",
                manifest.worktree_id,
                manifest.kind,
                subject.target_kind.expected_worktree_kind()
            ),
        ));
    }
    if manifest.run_id != subject.expected_run_id {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!(
                "Worktree `{}` run_id `{}` does not match expected_run_id `{}`.",
                manifest.worktree_id, manifest.run_id, subject.expected_run_id
            ),
        ));
    }
    if manifest.task_id != subject.expected_task_id {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!(
                "Worktree `{}` task_id `{:?}` does not match expected_task_id `{:?}`.",
                manifest.worktree_id, manifest.task_id, subject.expected_task_id
            ),
        ));
    }
    if manifest.base_commit != subject.expected_base_commit {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!(
                "Worktree `{}` base_commit `{}` does not match expected_base_commit `{}`.",
                manifest.worktree_id, manifest.base_commit, subject.expected_base_commit
            ),
        ));
    }
    if manifest.partial_cleanup_accepted_head.is_some() {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "Refusing to run frozen check on a partially cleaned worktree.",
        ));
    }
    Ok(())
}

fn verify_pre_run_checkout_state(
    canonical_checkout: &Path,
    manifest: &WorktreeManifest,
    subject: &FrozenCheckSubject,
) -> Result<(), NativeError> {
    let toplevel = run_sanitized_git(
        canonical_checkout,
        &["rev-parse", "--show-toplevel"],
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .map_err(map_worker_error_to_native)?;
    let canonical_toplevel =
        validate_existing_canonical_dir_native(Path::new(toplevel.trim()), "worktree toplevel")?;
    if canonical_toplevel != canonical_checkout {
        return Err(NativeError::new(
            NativeErrorCode::SecurityViolation,
            "Worktree git show-toplevel does not match manifest checkout_path.",
        ));
    }

    let current_branch = run_sanitized_git(
        canonical_checkout,
        &["symbolic-ref", "--quiet", "--short", "HEAD"],
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .map_err(|_| {
        NativeError::new(
            NativeErrorCode::ValidationFailed,
            "Worktree HEAD is detached or not on expected software branch.",
        )
    })?;
    if current_branch.trim() != manifest.branch_name {
        return Err(NativeError::new(
            NativeErrorCode::SecurityViolation,
            format!(
                "Worktree is on branch `{}`, expected `{}`.",
                current_branch.trim(),
                manifest.branch_name
            ),
        ));
    }

    let clean = is_worktree_clean(canonical_checkout, DEFAULT_GIT_COMMAND_TIMEOUT_MS)
        .map_err(map_worker_error_to_native)?;
    if !clean {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "Refusing to run frozen check on a dirty worktree (uncommitted, untracked, or ignored files present).",
        ));
    }

    let head_commit =
        resolve_exact_commit_sha(canonical_checkout, "HEAD", DEFAULT_GIT_COMMAND_TIMEOUT_MS)
            .map_err(map_worker_error_to_native)?;
    if head_commit != subject.expected_head_commit {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!(
                "Worktree HEAD commit `{head_commit}` does not match expected_head_commit `{}`.",
                subject.expected_head_commit
            ),
        ));
    }

    let tree_sha =
        resolve_exact_tree_sha(canonical_checkout, "HEAD", DEFAULT_GIT_COMMAND_TIMEOUT_MS)
            .map_err(map_worker_error_to_native)?;
    if tree_sha != subject.expected_tree_sha {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!(
                "Worktree HEAD tree `{tree_sha}` does not match expected_tree_sha `{}`.",
                subject.expected_tree_sha
            ),
        ));
    }

    Ok(())
}

fn resolve_and_validate_check_cwd(
    canonical_checkout: &Path,
    repo_relative_cwd: &str,
) -> Result<PathBuf, NativeError> {
    if repo_relative_cwd == "." {
        return Ok(canonical_checkout.to_path_buf());
    }
    validate_repo_relative_path(repo_relative_cwd, "repo_relative_cwd").map_err(|_| {
        NativeError::new(
            NativeErrorCode::ValidationFailed,
            "repo_relative_cwd must be `.` or a valid repository-relative path.",
        )
    })?;
    if is_control_config_path(repo_relative_cwd) {
        return Err(NativeError::new(
            NativeErrorCode::SecurityViolation,
            "repo_relative_cwd cannot target protected control configuration.",
        ));
    }

    let mut current_abs = canonical_checkout.to_path_buf();
    let mut rel_accum = PathBuf::new();
    for comp in Path::new(repo_relative_cwd).components() {
        match comp {
            Component::Normal(part) => {
                rel_accum.push(part);
                if let Some(rel_str) = rel_accum.to_str() {
                    if is_control_config_path(rel_str) {
                        return Err(NativeError::new(
                            NativeErrorCode::SecurityViolation,
                            "repo_relative_cwd cannot traverse protected control configuration.",
                        ));
                    }
                }
                current_abs.push(part);
                let meta = fs::symlink_metadata(&current_abs).map_err(|_| {
                    NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        format!(
                            "repo_relative_cwd component `{}` does not exist inside worktree.",
                            rel_accum.display()
                        ),
                    )
                })?;
                if meta.file_type().is_symlink() {
                    return Err(NativeError::new(
                        NativeErrorCode::SecurityViolation,
                        format!(
                            "repo_relative_cwd traverses symbolic link `{}` inside worktree.",
                            rel_accum.display()
                        ),
                    ));
                }
                if !meta.file_type().is_dir() {
                    return Err(NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        format!(
                            "repo_relative_cwd component `{}` is not a directory.",
                            rel_accum.display()
                        ),
                    ));
                }
            }
            _ => {
                return Err(NativeError::new(
                    NativeErrorCode::SecurityViolation,
                    "repo_relative_cwd contains disallowed path traversal components.",
                ));
            }
        }
    }

    let canonical_cwd = fs::canonicalize(&current_abs).map_err(|_| {
        NativeError::new(
            NativeErrorCode::ValidationFailed,
            "Failed to canonicalize resolved repo_relative_cwd.",
        )
    })?;
    if canonical_cwd != current_abs || !canonical_cwd.starts_with(canonical_checkout) {
        return Err(NativeError::new(
            NativeErrorCode::SecurityViolation,
            "Resolved repo_relative_cwd escaped worktree checkout containment.",
        ));
    }

    Ok(canonical_cwd)
}

fn inspect_post_run_subject_state(
    canonical_work_area: &Path,
    canonical_checkout: &Path,
    expected_working_directory: &Path,
    pre_manifest: &WorktreeManifest,
    subject: &FrozenCheckSubject,
    repo_relative_cwd: &str,
) -> PostRunSubjectState {
    let mut reasons = Vec::new();

    match load_and_verify_manifest(canonical_work_area, &subject.worktree_id) {
        Ok(post_manifest) => {
            if post_manifest.checkout_path != pre_manifest.checkout_path
                || post_manifest.branch_name != pre_manifest.branch_name
                || post_manifest.base_commit != pre_manifest.base_commit
                || post_manifest.kind != pre_manifest.kind
                || post_manifest.run_id != pre_manifest.run_id
                || post_manifest.task_id != pre_manifest.task_id
            {
                reasons
                    .push("worktree manifest metadata drifted during check execution".to_string());
            }
        }
        Err(err) => {
            reasons.push(format!(
                "post-run worktree manifest verification failed: {err}"
            ));
        }
    }

    match resolve_and_validate_check_cwd(canonical_checkout, repo_relative_cwd) {
        Ok(post_cwd) => {
            if post_cwd != expected_working_directory {
                reasons
                    .push("resolved check working directory drifted during execution".to_string());
            }
        }
        Err(err) => {
            reasons.push(format!(
                "post-run check working directory validation failed: {}",
                err.message
            ));
        }
    }

    match run_sanitized_git(
        canonical_checkout,
        &["symbolic-ref", "--quiet", "--short", "HEAD"],
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    ) {
        Ok(branch) => {
            if branch.trim() != pre_manifest.branch_name {
                reasons.push(format!(
                    "worktree branch drifted to `{}` (expected `{}`)",
                    branch.trim(),
                    pre_manifest.branch_name
                ));
            }
        }
        Err(_) => {
            reasons.push("worktree HEAD became detached or unreadable during check".to_string());
        }
    }

    let head_commit =
        resolve_exact_commit_sha(canonical_checkout, "HEAD", DEFAULT_GIT_COMMAND_TIMEOUT_MS).ok();
    if head_commit.as_deref() != Some(subject.expected_head_commit.as_str()) {
        reasons.push(format!(
            "worktree HEAD commit drifted to `{:?}` (expected `{}`)",
            head_commit, subject.expected_head_commit
        ));
    }

    let tree_sha =
        resolve_exact_tree_sha(canonical_checkout, "HEAD", DEFAULT_GIT_COMMAND_TIMEOUT_MS).ok();
    if tree_sha.as_deref() != Some(subject.expected_tree_sha.as_str()) {
        reasons.push(format!(
            "worktree HEAD tree drifted to `{:?}` (expected `{}`)",
            tree_sha, subject.expected_tree_sha
        ));
    }

    let worktree_clean =
        is_worktree_clean(canonical_checkout, DEFAULT_GIT_COMMAND_TIMEOUT_MS).unwrap_or(false);
    if !worktree_clean {
        reasons.push(
            "check command mutated worktree (dirty, staged, untracked, or ignored files detected; changes preserved)"
                .to_string(),
        );
    }

    let subject_drifted = !reasons.is_empty();
    let drift_reason = if reasons.is_empty() {
        None
    } else {
        Some(reasons.join("; "))
    };

    PostRunSubjectState {
        worktree_clean,
        head_commit,
        tree_sha,
        subject_drifted,
        drift_reason,
    }
}

fn classify_check_status(
    cmd_status: BoundedCommandStatus,
    cmd_passed: bool,
    descendants_terminated_after_exit: bool,
    ownership_verified_post_run: bool,
    post_worktree_clean: bool,
    subject_drifted: bool,
) -> FrozenCheckExecutionStatus {
    match cmd_status {
        BoundedCommandStatus::Cancelled => FrozenCheckExecutionStatus::Cancelled,
        BoundedCommandStatus::TimedOut => FrozenCheckExecutionStatus::TimedOut,
        BoundedCommandStatus::OutputOverflow => FrozenCheckExecutionStatus::OutputOverflow,
        BoundedCommandStatus::IncompleteCleanup => FrozenCheckExecutionStatus::IncompleteCleanup,
        BoundedCommandStatus::NonZeroExit => FrozenCheckExecutionStatus::NonZeroExit,
        BoundedCommandStatus::DescendantsTerminatedAfterExit => {
            FrozenCheckExecutionStatus::DescendantsTerminatedAfterExit
        }
        BoundedCommandStatus::Succeeded => {
            if descendants_terminated_after_exit {
                FrozenCheckExecutionStatus::DescendantsTerminatedAfterExit
            } else if !cmd_passed {
                FrozenCheckExecutionStatus::NonZeroExit
            } else if !ownership_verified_post_run {
                FrozenCheckExecutionStatus::OwnershipOrContextDrifted
            } else if !post_worktree_clean || subject_drifted {
                FrozenCheckExecutionStatus::SubjectMutatedOrDrifted
            } else {
                FrozenCheckExecutionStatus::Passed
            }
        }
    }
}

fn build_check_command_summary(spec: &FrozenCheckCommandSpec) -> String {
    let exe_name = spec
        .executable
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("check");
    let joined_args = if spec.argv.is_empty() {
        String::new()
    } else {
        format!(" {}", spec.argv.join(" "))
    };
    let raw = format!(
        "{} ({}): {}{} [cwd={}]",
        spec.check_id,
        spec.kind.as_str(),
        exe_name,
        joined_args,
        spec.repo_relative_cwd
    );
    sanitize_diagnostic_text(&raw)
}

fn validate_canonical_lowercase_40_hex(value: &str, label: &str) -> Result<(), NativeError> {
    let normalized = validate_exact_40_hex_sha(value, label).map_err(|_| {
        NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!("{label} must be an exact 40-character hexadecimal SHA."),
        )
    })?;
    if normalized != value {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!("{label} must be lowercase hexadecimal."),
        ));
    }
    Ok(())
}

fn validate_canonical_lowercase_64_hex(value: &str, label: &str) -> Result<(), NativeError> {
    let normalized = validate_sha256_hex_digest(value, label).map_err(|_| {
        NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!("{label} must be a 64-character hexadecimal SHA-256 digest."),
        )
    })?;
    if normalized != value {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!("{label} must be lowercase hexadecimal."),
        ));
    }
    Ok(())
}

fn validate_csprng_instance_id(instance_id: &str) -> Result<(), NativeError> {
    let Some(hex_part) = instance_id.strip_prefix("cwo_") else {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "ownership_instance_id must start with `cwo_`.",
        ));
    };
    validate_canonical_lowercase_64_hex(hex_part, "ownership_instance_id")
}

fn validate_structural_absolute_path(path: &Path, label: &str) -> Result<(), NativeError> {
    if !path.is_absolute() {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!("{label} must be an absolute path."),
        ));
    }
    let mut saw_normal = false;
    for comp in path.components() {
        match comp {
            Component::RootDir | Component::Prefix(_) => {}
            Component::Normal(part) => {
                if part.is_empty() {
                    return Err(NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        format!("{label} contains an empty path component."),
                    ));
                }
                saw_normal = true;
            }
            Component::CurDir | Component::ParentDir => {
                return Err(NativeError::new(
                    NativeErrorCode::SecurityViolation,
                    format!("{label} must not contain `.` or `..` traversal components."),
                ));
            }
        }
    }
    if !saw_normal {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!("{label} must not be the filesystem root directory."),
        ));
    }
    Ok(())
}

fn validate_existing_canonical_dir_native(
    path: &Path,
    label: &str,
) -> Result<PathBuf, NativeError> {
    let checked = validate_absolute_no_symlink_components(path, label)?;
    let meta = fs::symlink_metadata(&checked).map_err(|_| {
        NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!("{label} `{}` does not exist.", checked.display()),
        )
    })?;
    if meta.file_type().is_symlink() || !meta.file_type().is_dir() {
        return Err(NativeError::new(
            NativeErrorCode::SecurityViolation,
            format!(
                "{label} `{}` must be a non-symlink directory.",
                checked.display()
            ),
        ));
    }
    let canonical = fs::canonicalize(&checked).map_err(|_| {
        NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!("Failed to canonicalize {label} `{}`.", checked.display()),
        )
    })?;
    if canonical != checked {
        return Err(NativeError::new(
            NativeErrorCode::SecurityViolation,
            format!(
                "{label} `{}` does not match its canonical path.",
                checked.display()
            ),
        ));
    }
    Ok(canonical)
}

fn map_worker_error_to_native(err: WorkerError) -> NativeError {
    match err {
        WorkerError::SecurityViolation(msg) => {
            NativeError::new(NativeErrorCode::SecurityViolation, msg)
        }
        WorkerError::CleanupRefused {
            worktree_id,
            reason,
            detail,
        } => {
            let code = match reason {
                CleanupRefusalReason::UnexpectedBranchOrOwner => NativeErrorCode::SecurityViolation,
                CleanupRefusalReason::MissingOrInvalidManifest
                | CleanupRefusalReason::ActiveJob
                | CleanupRefusalReason::DirtyWorktree => NativeErrorCode::ValidationFailed,
            };
            NativeError::new(
                code,
                format!("worktree `{worktree_id}` check refused ({reason:?}): {detail}"),
            )
        }
        other => NativeError::new(NativeErrorCode::ValidationFailed, other.to_string()),
    }
}
