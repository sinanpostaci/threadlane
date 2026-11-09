use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::io::Read;
use std::path::{Path, PathBuf};
#[cfg(unix)]
use std::process::{Command, Stdio};
#[cfg(unix)]
use std::time::{Duration, Instant};

use fs2::FileExt;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, watch};
use uuid::Uuid;

use crate::error::{CleanupRefusalReason, WorkerError};
use crate::fs_safety::{
    ensure_platform_supported, ensure_private_dir, open_private_lock_file, read_private_json,
    sanitize_and_redact, validate_absolute_no_symlink_components, validate_existing_canonical_dir,
    write_private_json_atomic,
};
use crate::spec::{
    is_control_config_path, is_path_allowed_by_scopes, validate_commit_ref,
    validate_repo_relative_path, validate_simple_identifier, WorkerSpec, WriteScopeKind,
};
use crate::stream::NormalizedWorkerEvent;
use crate::supervisor::{is_process_alive, run_worker_with_observer, WorkerExecutionReport};
#[cfg(unix)]
use crate::supervisor::{unix_pgroup, ProcessGroupGuard, POST_EXIT_PIPE_DRAIN_TIMEOUT_MS};

/// Schema version for private worktree ownership manifests.
pub const MANIFEST_SCHEMA_VERSION: u32 = 1;

/// Schema version for the private work-area root ownership marker.
pub const WORK_AREA_MARKER_VERSION: u32 = 1;

/// Filename of the private (`0600`) ownership marker inside `work_area_root`.
pub const WORK_AREA_MARKER_FILENAME: &str = ".threadlane-work-area.json";

/// Filename of the private (`0600`) cross-process initialization lock inside `work_area_root`.
pub const WORK_AREA_LOCK_FILENAME: &str = ".threadlane-work-area.lock";

/// Maximum permitted byte size when reading a worktree ownership manifest or work-area marker.
pub const MAX_MANIFEST_BYTES: u64 = 65_536;

/// Default maximum stdout bytes captured from a single Git diff/status/ls-files/ls-tree command (4 MiB).
pub const DEFAULT_MAX_GIT_OUTPUT_BYTES: usize = 4_194_304;

/// Default maximum number of changed paths retained in a [`DiffGateReport`] before failing the budget.
pub const DEFAULT_MAX_DIFF_CHANGED_PATHS: usize = 4_096;

/// Default maximum number of filesystem entries (files, directories, symlinks) visited during a worktree scan.
pub const DEFAULT_MAX_SCAN_ENTRIES: usize = 32_768;

/// Default maximum directory recursion depth during a worktree scan.
pub const DEFAULT_MAX_SCAN_DEPTH: usize = 64;

/// Default finite wall-clock timeout for a single supervised Git subprocess (10 seconds).
pub const DEFAULT_GIT_COMMAND_TIMEOUT_MS: u64 = 10_000;

/// Maximum stderr bytes captured from a Git subprocess (64 KiB).
pub const MAX_GIT_STDERR_BYTES: usize = 65_536;

/// Subdirectory inside `work_area_root` holding private (`0600`) ownership manifests.
pub const MANIFESTS_SUBDIR: &str = "manifests";

/// Subdirectory inside `work_area_root` holding Git worktree checkouts.
pub const CHECKOUTS_SUBDIR: &str = "checkouts";

/// Private ownership marker (`0600`) persisted at `<work_area_root>/.threadlane-work-area.json`
/// to prove `work_area_root` was initialized by Threadlane for a specific repository.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkAreaOwnershipMarker {
    /// Marker format version (`1`).
    pub marker_version: u32,
    /// Canonical path to this work-area root directory.
    pub work_area_root: PathBuf,
    /// Canonical common Git directory (`git rev-parse --git-common-dir`) of the owning repository.
    pub repository_common_git_dir: PathBuf,
}

/// Role of a software-managed Git worktree.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorktreeKind {
    /// Per-run integration worktree (`threadlane/run/<run_id>/<uid>`).
    RunIntegration,
    /// Per-task execution worktree (`threadlane/task/<run_id>/<task_id>/<uid>`).
    Task,
}

/// Explicit label describing the isolation boundary of the Git diff gate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IsolationLevel {
    /// Post-run Git diff inspection only; not a preventive OS-level sandbox.
    PostRunDiffEvidence,
}

/// Separate lifecycle state distinguishing worker process completion from diff validation and review acceptance.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkAcceptanceState {
    /// Worktree created; worker has not yet executed.
    PendingExecution,
    /// Worker process exited 0 with valid stream evidence, pending post-run diff gate evaluation.
    WorkerSucceededPendingDiff,
    /// Worker succeeded with full stream evidence and post-run diff gate passed; awaiting primary review.
    /// Never automatically promoted to accepted work without review.
    DiffGatePassedPendingReview,
    /// Worker exited 0 with verified model and terminal result event and diff gate passed,
    /// but the provider stream omitted effort evidence (`MissingEffortEvidence`); requires review
    /// and is not accepted work.
    FinishedNeedsEvidenceAndReview,
    /// Rejected because the worker process failed, timed out, blocked, overflowed, or lacked required evidence.
    RejectedByWorkerOutcome,
    /// Rejected because the post-run diff gate detected disallowed paths, symlinks, control config edits, or budget overflow.
    RejectedByDiffGate,
}

/// Private ownership and provenance manifest persisted outside the worktree checkout
/// (`<work_area_root>/manifests/<worktree_id>.json`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorktreeManifest {
    /// Manifest format version (`1`).
    pub manifest_version: u32,
    /// Unique software-generated worktree identifier.
    pub worktree_id: String,
    /// Worktree role (`run_integration` or `task`).
    pub kind: WorktreeKind,
    /// Associated run identifier.
    pub run_id: String,
    /// Associated task identifier (present when `kind == Task`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub task_id: Option<String>,
    /// Canonical path to the trusted source Git repository (or source worktree).
    pub repository_path: PathBuf,
    /// Canonical common Git directory (`git rev-parse --git-common-dir`) of the source repository.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub repository_common_git_dir: Option<PathBuf>,
    /// Canonical path to the private work area root.
    pub work_area_root: PathBuf,
    /// Canonical path to the owned worktree checkout directory inside `<work_area_root>/checkouts/`.
    pub checkout_path: PathBuf,
    /// Canonical path to this private manifest file inside `<work_area_root>/manifests/`.
    pub manifest_path: PathBuf,
    /// Dedicated software-generated Git branch name checked out in `checkout_path`.
    pub branch_name: String,
    /// Exact 40-character hexadecimal base commit hash from which the worktree was created.
    pub base_commit: String,
    /// Repository-relative paths permitted to be modified in this worktree.
    pub allowed_write_paths: BTreeSet<String>,
    /// True while a worker job is executing (or if a previous execution crashed before reconciliation).
    pub job_active: bool,
    /// PID of the supervisor/runner currently executing a job in this worktree, if any.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub active_job_pid: Option<u32>,
    /// Set when `cleanup_integrated_worktree` removed the clean checkout at this verified
    /// `accepted_head_commit`, but safe `git branch -d` refused because the primary source
    /// checkout `HEAD` did not yet contain `accepted_head_commit`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub partial_cleanup_accepted_head: Option<String>,
}

/// Classification of a file change detected in a worktree relative to `base_commit`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DiffChangeKind {
    /// Tracked file added.
    Added,
    /// Tracked file modified.
    Modified,
    /// Tracked file deleted.
    Deleted,
    /// Tracked file renamed.
    Renamed,
    /// Tracked file copied.
    Copied,
    /// File mode/type changed (e.g. regular file replaced with symlink).
    TypeChanged,
    /// Untracked file created in the working tree.
    Untracked,
}

/// Single changed path reported by the post-run Git diff gate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChangedPathEntry {
    /// Repository-relative path (new path for renames/copies).
    pub path: String,
    /// Original repository-relative path prior to rename or copy.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub orig_path: Option<String>,
    /// Type of modification.
    pub change_kind: DiffChangeKind,
    /// Whether the file content or diff is binary.
    pub is_binary: bool,
    /// Whether the entry on disk is a symbolic link.
    pub is_symlink: bool,
}

/// Specific policy violation kind detected by the post-run diff gate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DiffViolationKind {
    /// Changed path (or rename origin) is outside `allowed_write_paths`.
    OutsideAllowedPaths,
    /// A symbolic link was created, modified, or traversed inside the worktree checkout.
    SymlinkDetected,
    /// Path contained `..`, leading `/`, or escaped the checkout root.
    PathEscape,
    /// Protected repository metadata or agent/toolchain control configuration was modified.
    ControlConfigModified,
    /// Git diff/status output or changed-path count exceeded the bounded inspection budget.
    DiffBudgetExceeded,
}

/// Single policy violation recorded by the post-run diff gate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiffViolation {
    /// Repository-relative path that triggered the violation.
    pub path: String,
    /// Violation category.
    pub kind: DiffViolationKind,
    /// Human-readable explanation.
    pub detail: String,
}

/// Structured post-run Git diff validation report.
///
/// Explicitly labeled as post-run evidence (`preventive_sandbox: false`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiffGateReport {
    /// Owned worktree identifier.
    pub worktree_id: String,
    /// Canonical checkout directory inspected.
    pub checkout_path: PathBuf,
    /// Base 40-hex commit SHA compared against.
    pub base_commit: String,
    /// Current HEAD 40-hex commit SHA in the worktree.
    pub head_commit: String,
    /// Isolation classification (`post_run_diff_evidence`).
    pub isolation_level: IsolationLevel,
    /// Always `false` in M2: workers are not confined by an OS-level filesystem sandbox.
    pub preventive_sandbox: bool,
    /// Explicit human-readable note explaining the post-run evidence boundary.
    pub isolation_note: String,
    /// True if all Git diff/ls-files/ls-tree queries and `changed_paths` fit within bounded budgets.
    #[serde(default = "default_true")]
    pub evidence_complete: bool,
    /// True if and only if `evidence_complete` is true and `violations` is empty.
    pub passed: bool,
    /// Resulting acceptance state (`DiffGatePassedPendingReview` or `RejectedByDiffGate`).
    pub acceptance_state: WorkAcceptanceState,
    /// All changed, renamed, deleted, binary, and untracked paths relative to `base_commit` (bounded).
    pub changed_paths: Vec<ChangedPathEntry>,
    /// Policy violations detected across `changed_paths` (including budget overflow).
    pub violations: Vec<DiffViolation>,
}

const fn default_true() -> bool {
    true
}

/// Trusted ownership parameters required to perform explicit worktree cleanup.
///
/// Cleanup never deletes a directory based solely on a caller-supplied checkout path;
/// it always loads and verifies `<work_area_root>/manifests/<worktree_id>.json`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CleanupRequest {
    /// Trusted private work-area root directory.
    pub work_area_root: PathBuf,
    /// Software-generated worktree identifier whose manifest resides in `<work_area_root>/manifests/`.
    pub worktree_id: String,
}

/// Structured confirmation returned when explicit worktree cleanup succeeds.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CleanupReport {
    /// Cleaned worktree identifier.
    pub worktree_id: String,
    /// Worktree kind.
    pub kind: WorktreeKind,
    /// Removed checkout directory path.
    pub removed_checkout_path: PathBuf,
    /// Deleted Git branch name.
    pub removed_branch_name: String,
    /// Removed private ownership manifest path.
    pub removed_manifest_path: PathBuf,
}

/// Explicit ownership, ancestry, and receipt proof required to clean up a productive
/// worktree whose `HEAD` has advanced beyond `base_commit` and been integrated into a
/// target commit.
///
/// Unlike [`cleanup_worktree`], which strictly refuses any worktree where `HEAD != base_commit`,
/// [`cleanup_integrated_worktree`] verifies ownership, clean checkout state, inactive job locks,
/// exact primary source `HEAD` match, `accepted_head_commit` ancestry inside `expected_target_commit`,
/// and non-empty review/integration receipt proof without rewriting `base_commit` or using
/// `--force` / `-D`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IntegratedCleanupRequest {
    /// Trusted private work-area root directory.
    pub work_area_root: PathBuf,
    /// Software-generated worktree identifier whose manifest resides in `<work_area_root>/manifests/`.
    pub worktree_id: String,
    /// Expected 40-character hex base commit SHA recorded in the worktree manifest.
    pub expected_base_commit: String,
    /// Reviewed and accepted 40-character hex commit SHA that must match the worktree's current `HEAD`.
    pub accepted_head_commit: String,
    /// Target 40-character hex commit SHA (e.g. integration branch head) that must contain `accepted_head_commit` in its ancestry.
    pub expected_target_commit: String,
    /// Expected 40-character hex commit SHA of the primary source repository's current `HEAD`.
    pub expected_source_head_commit: String,
    /// 64-character hex SHA-256 digest of the approved review subject.
    pub review_subject_digest: String,
    /// Identifier of the operator review acceptance receipt.
    pub review_receipt_id: String,
    /// Identifier of the operator integration receipt.
    pub integration_receipt_id: String,
}

/// Structured confirmation returned when [`cleanup_integrated_worktree`] completes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IntegratedCleanupReport {
    /// Cleaned worktree identifier.
    pub worktree_id: String,
    /// Worktree kind.
    pub kind: WorktreeKind,
    /// Canonical checkout directory path that was removed.
    pub removed_checkout_path: PathBuf,
    /// Software branch name associated with the worktree.
    pub branch_name: String,
    /// Path to the private ownership manifest.
    pub manifest_path: PathBuf,
    /// Verified accepted task `HEAD` commit SHA.
    pub accepted_head_commit: String,
    /// Verified target commit SHA that contains `accepted_head_commit` in its ancestry.
    pub verified_target_commit: String,
    /// Verified primary source checkout `HEAD` commit SHA.
    pub verified_source_head_commit: String,
    /// Approved review subject SHA-256 digest bound to this cleanup.
    pub review_subject_digest: String,
    /// Approved review receipt identifier bound to this cleanup.
    pub review_receipt_id: String,
    /// Integration receipt identifier bound to this cleanup.
    pub integration_receipt_id: String,
    /// True when the worktree checkout directory was removed via `git worktree remove`.
    pub checkout_removed: bool,
    /// True when the software branch was deleted via safe `git branch -d`.
    pub branch_removed: bool,
    /// True when the private ownership manifest was removed (only when `branch_removed` is also true).
    pub manifest_removed: bool,
    /// True when the checkout directory was removed, but safe `git branch -d` refused to delete
    /// the branch (preserving the branch ref and manifest).
    pub partial_cleanup: bool,
    /// Diagnostic explanation when `partial_cleanup` is `true` and the branch was preserved.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub branch_preserved_reason: Option<String>,
}

/// Bounded output from a supervised, environment-sanitized Git subprocess.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SupervisedGitOutput {
    /// Raw stdout bytes captured within the configured byte budget.
    pub stdout_bytes: Vec<u8>,
    /// Sanitized and secret-redacted stderr diagnostic summary.
    pub stderr: String,
    /// Process exit code, if exited normally.
    pub exit_code: Option<i32>,
}

/// Controlled Git environment overrides applied after stripping all inherited `GIT_*` variables.
pub const SANITIZED_GIT_ENV_OVERRIDES: &[(&str, &str)] = &[
    ("GIT_TERMINAL_PROMPT", "0"),
    ("GIT_CONFIG_NOSYSTEM", "1"),
    ("GIT_CONFIG_SYSTEM", "/dev/null"),
    ("GIT_CONFIG_GLOBAL", "/dev/null"),
    ("GIT_OPTIONAL_LOCKS", "0"),
];

/// Combined report of running a worker inside an owned task worktree and evaluating the post-run diff gate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskExecutionReport {
    /// Updated private ownership manifest for the task worktree.
    pub manifest: WorktreeManifest,
    /// Process-group supervision and NDJSON stream evidence report.
    pub worker_report: WorkerExecutionReport,
    /// Post-run Git diff gate report.
    pub diff_gate: DiffGateReport,
    /// Overall work disposition (never automatically `accepted`; at most `diff_gate_passed_pending_review`).
    pub overall_acceptance_state: WorkAcceptanceState,
}

/// Exclusive process-level guard marking a worktree as having an active worker job.
#[derive(Debug)]
pub struct WorktreeJobGuard {
    lock_file: File,
    work_area_root: PathBuf,
    worktree_id: String,
}

impl WorktreeJobGuard {
    /// Marks the job as finished in the private manifest and releases the exclusive lock.
    pub fn finish(self) -> Result<WorktreeManifest, WorkerError> {
        let mut manifest = load_and_verify_manifest(&self.work_area_root, &self.worktree_id)?;
        manifest.job_active = false;
        manifest.active_job_pid = None;
        let filename = format!("{}.json", self.worktree_id);
        let manifests_dir = self.work_area_root.join(MANIFESTS_SUBDIR);
        write_private_json_atomic(&manifests_dir, &filename, &manifest, "worktree manifest")?;
        let _ = self.lock_file.unlock();
        Ok(manifest)
    }
}

impl Drop for WorktreeJobGuard {
    fn drop(&mut self) {
        let _ = self.lock_file.unlock();
    }
}

struct CreateWorktreeParams {
    worktree_id: String,
    kind: WorktreeKind,
    run_id: String,
    task_id: Option<String>,
    checkout_path: PathBuf,
    branch_name: String,
    allowed_write_paths: BTreeSet<String>,
}

/// Manager for creating, inspecting, gating, and cleaning up isolated Git worktrees.
#[derive(Debug, Clone)]
pub struct WorktreeManager {
    repository_path: PathBuf,
    repository_common_git_dir: PathBuf,
    work_area_root: PathBuf,
    resolved_base_commit: String,
    git_timeout_ms: u64,
}

impl WorktreeManager {
    /// Validates `repository_path`, `work_area_root`, and resolves `base_commit` to an exact
    /// 40-character hexadecimal commit SHA without switching or mutating the repository's active checkout.
    ///
    /// Resolves and verifies the repository and `base_commit` before performing any filesystem
    /// operations on `work_area_root`, and refuses any pre-existing unrelated or non-private
    /// `work_area_root` before changing permissions or creating subdirectories.
    pub fn new(
        repository_path: &Path,
        work_area_root: &Path,
        base_commit: &str,
    ) -> Result<Self, WorkerError> {
        ensure_platform_supported()?;
        validate_commit_ref(base_commit)?;

        let canonical_repo = validate_existing_canonical_dir(repository_path, "repository_path")?;
        let checked_work_area =
            validate_absolute_no_symlink_components(work_area_root, "work_area_root")?;

        if checked_work_area.starts_with(&canonical_repo)
            || canonical_repo.starts_with(&checked_work_area)
        {
            return Err(WorkerError::InvalidSpec(format!(
                "work_area_root `{}` and repository_path `{}` must not overlap",
                checked_work_area.display(),
                canonical_repo.display()
            )));
        }

        // Resolve common Git dir and base_commit SHA BEFORE any filesystem side effects on work_area_root.
        let repository_common_git_dir = resolve_repository_common_git_dir(&canonical_repo)?;
        if checked_work_area.starts_with(&repository_common_git_dir)
            || repository_common_git_dir.starts_with(&checked_work_area)
        {
            return Err(WorkerError::InvalidSpec(format!(
                "work_area_root `{}` and repository common Git dir `{}` must not overlap",
                checked_work_area.display(),
                repository_common_git_dir.display()
            )));
        }
        let resolved_base_commit = resolve_commit_sha(&canonical_repo, base_commit)?;

        let canonical_work_area =
            prepare_owned_work_area(&checked_work_area, &repository_common_git_dir)?;

        Ok(Self {
            repository_path: canonical_repo,
            repository_common_git_dir,
            work_area_root: canonical_work_area,
            resolved_base_commit,
            git_timeout_ms: DEFAULT_GIT_COMMAND_TIMEOUT_MS,
        })
    }

    /// Configures a custom finite wall-clock timeout (in milliseconds) for Git commands
    /// invoked by this [`WorktreeManager`].
    pub fn with_git_timeout_ms(mut self, timeout_ms: u64) -> Result<Self, WorkerError> {
        if timeout_ms == 0 {
            return Err(WorkerError::InvalidSpec(
                "git_timeout_ms must be greater than zero".to_string(),
            ));
        }
        self.git_timeout_ms = timeout_ms;
        Ok(self)
    }

    /// Returns the canonical repository path.
    pub fn repository_path(&self) -> &Path {
        &self.repository_path
    }

    /// Returns the canonical common Git directory of the repository (`git rev-parse --git-common-dir`).
    pub fn repository_common_git_dir(&self) -> &Path {
        &self.repository_common_git_dir
    }

    /// Returns the canonical private work area root path.
    pub fn work_area_root(&self) -> &Path {
        &self.work_area_root
    }

    /// Returns the resolved 40-character hexadecimal base commit SHA.
    pub fn resolved_base_commit(&self) -> &str {
        &self.resolved_base_commit
    }

    /// Creates a new per-run integration worktree with a unique software-generated ID and branch.
    pub fn create_run_worktree(&self, run_id: &str) -> Result<WorktreeManifest, WorkerError> {
        let token = generate_short_token();
        self.create_run_worktree_with_token(run_id, &token)
    }

    /// Creates a per-run integration worktree using a specific validated software token.
    pub fn create_run_worktree_with_token(
        &self,
        run_id: &str,
        token: &str,
    ) -> Result<WorktreeManifest, WorkerError> {
        validate_simple_identifier(run_id, "run_id")?;
        validate_simple_identifier(token, "worktree token")?;

        let worktree_id = format!("wt-run-{run_id}-{token}");
        let branch_name = format!("threadlane/run/{run_id}/{token}");
        let checkout_path = self
            .work_area_root
            .join(CHECKOUTS_SUBDIR)
            .join("runs")
            .join(&worktree_id);

        self.create_worktree_internal(CreateWorktreeParams {
            worktree_id,
            kind: WorktreeKind::RunIntegration,
            run_id: run_id.to_string(),
            task_id: None,
            checkout_path,
            branch_name,
            allowed_write_paths: BTreeSet::new(),
        })
    }

    /// Creates a new per-task execution worktree with a unique software-generated ID and branch.
    pub fn create_task_worktree(
        &self,
        run_id: &str,
        task_id: &str,
        allowed_write_paths: &BTreeSet<String>,
    ) -> Result<WorktreeManifest, WorkerError> {
        let token = generate_short_token();
        self.create_task_worktree_with_token(run_id, task_id, &token, allowed_write_paths)
    }

    /// Creates a per-task execution worktree using a specific validated software token.
    pub fn create_task_worktree_with_token(
        &self,
        run_id: &str,
        task_id: &str,
        token: &str,
        allowed_write_paths: &BTreeSet<String>,
    ) -> Result<WorktreeManifest, WorkerError> {
        validate_simple_identifier(run_id, "run_id")?;
        validate_simple_identifier(task_id, "task_id")?;
        validate_simple_identifier(token, "worktree token")?;

        if allowed_write_paths.is_empty() {
            return Err(WorkerError::InvalidSpec(
                "task worktree requires non-empty allowed_write_paths".to_string(),
            ));
        }
        for entry in allowed_write_paths {
            validate_repo_relative_path(entry, "allowed_write_paths entry")?;
            if is_control_config_path(entry) {
                return Err(WorkerError::InvalidSpec(format!(
                    "allowed_write_paths entry `{entry}` targets protected control configuration"
                )));
            }
        }

        let worktree_id = format!("wt-task-{run_id}-{task_id}-{token}");
        let branch_name = format!("threadlane/task/{run_id}/{task_id}/{token}");
        let checkout_path = self
            .work_area_root
            .join(CHECKOUTS_SUBDIR)
            .join("tasks")
            .join(&worktree_id);

        self.create_worktree_internal(CreateWorktreeParams {
            worktree_id,
            kind: WorktreeKind::Task,
            run_id: run_id.to_string(),
            task_id: Some(task_id.to_string()),
            checkout_path,
            branch_name,
            allowed_write_paths: allowed_write_paths.clone(),
        })
    }

    fn create_worktree_internal(
        &self,
        params: CreateWorktreeParams,
    ) -> Result<WorktreeManifest, WorkerError> {
        let CreateWorktreeParams {
            worktree_id,
            kind,
            run_id,
            task_id,
            checkout_path,
            branch_name,
            allowed_write_paths,
        } = params;

        let _ = validate_absolute_no_symlink_components(&checkout_path, "checkout_path")?;
        let manifests_dir = self.work_area_root.join(MANIFESTS_SUBDIR);
        let manifest_filename = format!("{worktree_id}.json");
        let manifest_path = manifests_dir.join(&manifest_filename);
        let _ = validate_absolute_no_symlink_components(&manifest_path, "manifest_path")?;

        let area_lock_path = self.work_area_root.join(WORK_AREA_LOCK_FILENAME);
        let area_lock_file = open_work_area_lock_file(&area_lock_path)?;
        area_lock_file.lock_exclusive().map_err(WorkerError::Io)?;

        // Refuse if target checkout path or manifest already exists; never reset or overwrite!
        if fs::symlink_metadata(&checkout_path).is_ok() {
            let _ = area_lock_file.unlock();
            return Err(WorkerError::TargetAlreadyExists(checkout_path));
        }
        if fs::symlink_metadata(&manifest_path).is_ok() {
            let _ = area_lock_file.unlock();
            return Err(WorkerError::TargetAlreadyExists(manifest_path));
        }
        if branch_exists(&self.repository_path, &branch_name, self.git_timeout_ms)? {
            let _ = area_lock_file.unlock();
            return Err(WorkerError::TargetAlreadyExists(PathBuf::from(format!(
                "refs/heads/{branch_name}"
            ))));
        }

        let checkout_str = checkout_path.to_str().ok_or_else(|| {
            WorkerError::InvalidSpec("checkout_path must be valid UTF-8".to_string())
        })?;

        run_git_with_timeout(
            &self.repository_path,
            &[
                "worktree",
                "add",
                "-b",
                &branch_name,
                "--",
                checkout_str,
                &self.resolved_base_commit,
            ],
            self.git_timeout_ms,
        )?;

        let canonical_checkout = validate_existing_canonical_dir(&checkout_path, "checkout_path")?;

        let manifest = WorktreeManifest {
            manifest_version: MANIFEST_SCHEMA_VERSION,
            worktree_id,
            kind,
            run_id,
            task_id,
            repository_path: self.repository_path.clone(),
            repository_common_git_dir: Some(self.repository_common_git_dir.clone()),
            work_area_root: self.work_area_root.clone(),
            checkout_path: canonical_checkout,
            manifest_path: manifest_path.clone(),
            branch_name,
            base_commit: self.resolved_base_commit.clone(),
            allowed_write_paths,
            job_active: false,
            active_job_pid: None,
            partial_cleanup_accepted_head: None,
        };

        write_private_json_atomic(
            &manifests_dir,
            &manifest_filename,
            &manifest,
            "worktree manifest",
        )?;
        let _ = area_lock_file.unlock();

        Ok(manifest)
    }

    /// Acquires an exclusive active-job lock and sets `job_active = true` in the worktree's
    /// private manifest.
    pub fn begin_job(&self, worktree_id: &str) -> Result<WorktreeJobGuard, WorkerError> {
        begin_worktree_job(&self.work_area_root, worktree_id)
    }
}

/// Acquires an exclusive active-job lock and sets `job_active = true` on the private manifest
/// for `worktree_id` inside `work_area_root`.
pub fn begin_worktree_job(
    work_area_root: &Path,
    worktree_id: &str,
) -> Result<WorktreeJobGuard, WorkerError> {
    let mut manifest = load_and_verify_manifest(work_area_root, worktree_id)?;
    let manifests_dir = manifest.work_area_root.join(MANIFESTS_SUBDIR);
    let lock_path = manifests_dir.join(format!("{worktree_id}.lock"));
    let lock_file = open_private_lock_file(&lock_path, "worktree job lock")?;

    lock_file
        .try_lock_exclusive()
        .map_err(|_| WorkerError::CleanupRefused {
            worktree_id: worktree_id.to_string(),
            reason: CleanupRefusalReason::ActiveJob,
            detail: "worktree job lock is already held by another active process".to_string(),
        })?;

    if manifest.job_active {
        return Err(WorkerError::CleanupRefused {
            worktree_id: worktree_id.to_string(),
            reason: CleanupRefusalReason::ActiveJob,
            detail: concat!(
                "worktree manifest is already marked `job_active = true` ",
                "from an unfinished or crashed execution"
            )
            .to_string(),
        });
    }

    manifest.job_active = true;
    manifest.active_job_pid = Some(std::process::id());
    let filename = format!("{worktree_id}.json");
    write_private_json_atomic(&manifests_dir, &filename, &manifest, "worktree manifest")?;

    Ok(WorktreeJobGuard {
        lock_file,
        work_area_root: manifest.work_area_root,
        worktree_id: worktree_id.to_string(),
    })
}

/// Opens an existing private (`0600`) regular lock file without creating or mutating it.
///
/// - Returns `Ok(None)` if `path` does not exist (`ErrorKind::NotFound`), leaving the missing file
///   absent on disk.
/// - Refuses symlinks (in `path` or any ancestor component), non-regular files (directories, FIFOs,
///   sockets, device nodes), non-`0600` permissions, foreign UID ownership, or inode/metadata
///   changes between `lstat` and `open` (`O_RDONLY | O_NOFOLLOW | O_NONBLOCK`).
pub fn open_existing_private_lock_file(
    path: &Path,
    label: &str,
) -> Result<Option<File>, WorkerError> {
    ensure_platform_supported()?;
    #[cfg(unix)]
    {
        open_existing_private_lock_file_with_uid(path, label, current_euid())
    }
    #[cfg(not(unix))]
    {
        let _ = (path, label);
        unreachable!()
    }
}

/// Verifies in read-only mode that a managed worktree has no active job or held lock without
/// creating a missing `.lock` file.
///
/// - Refuses with [`CleanupRefusalReason::ActiveJob`] if `manifest.job_active` is `true` or
///   `manifest.active_job_pid` refers to a live process.
/// - If `<work_area_root>/manifests/<worktree_id>.lock` does not exist, returns `Ok(())` without
///   creating the lock file.
/// - If the `.lock` file exists, opens it via [`open_existing_private_lock_file`], probes a
///   nonblocking exclusive lock via [`fs2::FileExt::try_lock_exclusive`], verifies that the locked
///   file's device/inode, regular-file type, `0600` permissions, and owner UID still match the
///   path on disk, and unlocks via [`fs2::FileExt::unlock`] without killing any process.
pub fn ensure_worktree_job_inactive_readonly(
    manifest: &WorktreeManifest,
) -> Result<(), WorkerError> {
    ensure_platform_supported()?;
    #[cfg(unix)]
    {
        ensure_worktree_job_inactive_readonly_with_uid(manifest, current_euid())
    }
    #[cfg(not(unix))]
    {
        let _ = manifest;
        unreachable!()
    }
}

#[cfg(unix)]
#[allow(unsafe_code)]
fn current_euid() -> u32 {
    // SAFETY: `geteuid` is a thread-safe POSIX syscall taking no arguments.
    unsafe { libc::geteuid() }
}

#[cfg(unix)]
fn open_existing_private_lock_file_with_uid(
    path: &Path,
    label: &str,
    expected_uid: u32,
) -> Result<Option<File>, WorkerError> {
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};

    let _ = validate_absolute_no_symlink_components(path, label)?;
    let parent = path.parent().ok_or_else(|| {
        WorkerError::InvalidSpec(format!(
            "{label} path `{}` must have a parent directory",
            path.display()
        ))
    })?;
    let parent_meta = fs::symlink_metadata(parent)?;
    if parent_meta.file_type().is_symlink() || !parent_meta.file_type().is_dir() {
        return Err(WorkerError::SecurityViolation(format!(
            "{label} parent `{}` must be a non-symlink directory",
            parent.display()
        )));
    }

    let pre_meta = match fs::symlink_metadata(path) {
        Ok(meta) => meta,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(WorkerError::Io(err)),
    };

    if pre_meta.file_type().is_symlink() {
        return Err(WorkerError::SecurityViolation(format!(
            "{label} `{}` must not be a symlink",
            path.display()
        )));
    }
    if !pre_meta.file_type().is_file() {
        return Err(WorkerError::SecurityViolation(format!(
            "{label} `{}` must be a regular file",
            path.display()
        )));
    }
    let pre_mode = pre_meta.permissions().mode() & 0o7777;
    if pre_mode != 0o600 {
        return Err(WorkerError::SecurityViolation(format!(
            "{label} `{}` has insecure permissions {:04o} (expected 0600)",
            path.display(),
            pre_mode
        )));
    }
    if pre_meta.uid() != expected_uid {
        return Err(WorkerError::SecurityViolation(format!(
            "{label} `{}` has foreign owner uid {} (expected {expected_uid})",
            path.display(),
            pre_meta.uid()
        )));
    }

    let file = match fs::OpenOptions::new()
        .read(true)
        .write(false)
        .create(false)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)
    {
        Ok(file) => file,
        Err(err) if err.raw_os_error() == Some(libc::ELOOP) => {
            return Err(WorkerError::SecurityViolation(format!(
                "{label} `{}` must not be a symlink",
                path.display()
            )));
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            return Err(WorkerError::SecurityViolation(format!(
                "{label} `{}` was removed or replaced during open",
                path.display()
            )));
        }
        Err(err) => return Err(WorkerError::Io(err)),
    };

    verify_locked_private_file_unchanged(&file, path, &pre_meta, label, expected_uid)?;
    Ok(Some(file))
}

#[cfg(unix)]
fn verify_locked_private_file_unchanged(
    file: &File,
    path: &Path,
    expected_meta: &fs::Metadata,
    label: &str,
    expected_uid: u32,
) -> Result<(), WorkerError> {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    let open_meta = file.metadata()?;
    let post_sym = fs::symlink_metadata(path).map_err(|err| {
        if err.kind() == std::io::ErrorKind::NotFound {
            WorkerError::SecurityViolation(format!(
                "{label} `{}` was removed or replaced during inspection",
                path.display()
            ))
        } else {
            WorkerError::Io(err)
        }
    })?;

    if post_sym.file_type().is_symlink()
        || !post_sym.file_type().is_file()
        || !open_meta.file_type().is_file()
    {
        return Err(WorkerError::SecurityViolation(format!(
            "{label} `{}` must be a regular non-symlink file",
            path.display()
        )));
    }
    if open_meta.dev() != expected_meta.dev()
        || open_meta.ino() != expected_meta.ino()
        || post_sym.dev() != open_meta.dev()
        || post_sym.ino() != open_meta.ino()
    {
        return Err(WorkerError::SecurityViolation(format!(
            "{label} `{}` inode changed during inspection",
            path.display()
        )));
    }
    let open_mode = open_meta.permissions().mode() & 0o7777;
    let post_mode = post_sym.permissions().mode() & 0o7777;
    if open_mode != 0o600 || post_mode != 0o600 {
        return Err(WorkerError::SecurityViolation(format!(
            "{label} `{}` has insecure permissions {:04o} (expected 0600)",
            path.display(),
            if open_mode != 0o600 {
                open_mode
            } else {
                post_mode
            }
        )));
    }
    if open_meta.uid() != expected_uid || post_sym.uid() != expected_uid {
        return Err(WorkerError::SecurityViolation(format!(
            "{label} `{}` has foreign owner uid {} (expected {expected_uid})",
            path.display(),
            if open_meta.uid() != expected_uid {
                open_meta.uid()
            } else {
                post_sym.uid()
            }
        )));
    }
    Ok(())
}

#[cfg(unix)]
fn ensure_worktree_job_inactive_readonly_with_uid(
    manifest: &WorktreeManifest,
    expected_uid: u32,
) -> Result<(), WorkerError> {
    if manifest.job_active {
        return Err(WorkerError::CleanupRefused {
            worktree_id: manifest.worktree_id.clone(),
            reason: CleanupRefusalReason::ActiveJob,
            detail: "manifest indicates `job_active == true`".to_string(),
        });
    }
    if let Some(pid) = manifest.active_job_pid.filter(|&pid| is_process_alive(pid)) {
        return Err(WorkerError::CleanupRefused {
            worktree_id: manifest.worktree_id.clone(),
            reason: CleanupRefusalReason::ActiveJob,
            detail: format!("active_job_pid {pid} is still running"),
        });
    }

    let manifests_dir = manifest.work_area_root.join(MANIFESTS_SUBDIR);
    let lock_path = manifests_dir.join(format!("{}.lock", manifest.worktree_id));
    if let Some(lock_file) =
        open_existing_private_lock_file_with_uid(&lock_path, "worktree job lock", expected_uid)?
    {
        let open_meta = lock_file.metadata()?;
        fs2::FileExt::try_lock_exclusive(&lock_file).map_err(|_| WorkerError::CleanupRefused {
            worktree_id: manifest.worktree_id.clone(),
            reason: CleanupRefusalReason::ActiveJob,
            detail: "worktree job lock is held by an active process".to_string(),
        })?;
        let verify_res = verify_locked_private_file_unchanged(
            &lock_file,
            &lock_path,
            &open_meta,
            "worktree job lock",
            expected_uid,
        );
        let _ = fs2::FileExt::unlock(&lock_file);
        verify_res?;
    }
    Ok(())
}

/// Loads and validates a private [`WorktreeManifest`] from `<work_area_root>/manifests/<worktree_id>.json`.
pub fn load_and_verify_manifest(
    work_area_root: &Path,
    worktree_id: &str,
) -> Result<WorktreeManifest, WorkerError> {
    ensure_platform_supported()?;
    validate_simple_identifier(worktree_id, "worktree_id")?;

    let canonical_work_area = validate_existing_canonical_dir(work_area_root, "work_area_root")
        .map_err(|err| WorkerError::CleanupRefused {
            worktree_id: worktree_id.to_string(),
            reason: CleanupRefusalReason::MissingOrInvalidManifest,
            detail: format!("invalid work_area_root: {err}"),
        })?;

    let area_marker = verify_existing_work_area_marker(&canonical_work_area).map_err(|err| {
        WorkerError::CleanupRefused {
            worktree_id: worktree_id.to_string(),
            reason: CleanupRefusalReason::MissingOrInvalidManifest,
            detail: format!("work_area_root ownership marker verification failed: {err}"),
        }
    })?;

    let manifest_path = canonical_work_area
        .join(MANIFESTS_SUBDIR)
        .join(format!("{worktree_id}.json"));

    if fs::symlink_metadata(&manifest_path).is_err() {
        return Err(WorkerError::CleanupRefused {
            worktree_id: worktree_id.to_string(),
            reason: CleanupRefusalReason::MissingOrInvalidManifest,
            detail: format!(
                "private ownership manifest `{}` does not exist",
                manifest_path.display()
            ),
        });
    }

    let manifest: WorktreeManifest =
        read_private_json(&manifest_path, MAX_MANIFEST_BYTES, "worktree manifest").map_err(
            |err| WorkerError::CleanupRefused {
                worktree_id: worktree_id.to_string(),
                reason: CleanupRefusalReason::MissingOrInvalidManifest,
                detail: format!("failed to read private manifest: {err}"),
            },
        )?;

    if manifest.manifest_version != MANIFEST_SCHEMA_VERSION {
        return Err(WorkerError::CleanupRefused {
            worktree_id: worktree_id.to_string(),
            reason: CleanupRefusalReason::MissingOrInvalidManifest,
            detail: format!(
                "unsupported manifest_version {} (expected {MANIFEST_SCHEMA_VERSION})",
                manifest.manifest_version
            ),
        });
    }

    if manifest.worktree_id != worktree_id {
        return Err(WorkerError::CleanupRefused {
            worktree_id: worktree_id.to_string(),
            reason: CleanupRefusalReason::MissingOrInvalidManifest,
            detail: format!(
                "manifest worktree_id `{}` does not match requested `{worktree_id}`",
                manifest.worktree_id
            ),
        });
    }

    if manifest.work_area_root != canonical_work_area || manifest.manifest_path != manifest_path {
        return Err(WorkerError::CleanupRefused {
            worktree_id: worktree_id.to_string(),
            reason: CleanupRefusalReason::UnexpectedBranchOrOwner,
            detail: "manifest work_area_root or manifest_path does not match canonical location"
                .to_string(),
        });
    }

    if let Some(manifest_common_git) = manifest
        .repository_common_git_dir
        .as_ref()
        .filter(|&dir| dir != &area_marker.repository_common_git_dir)
    {
        return Err(WorkerError::CleanupRefused {
            worktree_id: worktree_id.to_string(),
            reason: CleanupRefusalReason::UnexpectedBranchOrOwner,
            detail: format!(
                "manifest repository_common_git_dir `{}` does not match work-area marker `{}`",
                manifest_common_git.display(),
                area_marker.repository_common_git_dir.display()
            ),
        });
    }

    let expected_checkouts_prefix = canonical_work_area.join(CHECKOUTS_SUBDIR);
    if !manifest
        .checkout_path
        .starts_with(&expected_checkouts_prefix)
    {
        return Err(WorkerError::CleanupRefused {
            worktree_id: worktree_id.to_string(),
            reason: CleanupRefusalReason::UnexpectedBranchOrOwner,
            detail: format!(
                "manifest checkout_path `{}` is not confined inside `{}`",
                manifest.checkout_path.display(),
                expected_checkouts_prefix.display()
            ),
        });
    }

    if !manifest.branch_name.starts_with("threadlane/run/")
        && !manifest.branch_name.starts_with("threadlane/task/")
    {
        return Err(WorkerError::CleanupRefused {
            worktree_id: worktree_id.to_string(),
            reason: CleanupRefusalReason::UnexpectedBranchOrOwner,
            detail: format!(
                "manifest branch_name `{}` does not carry a `threadlane/` software prefix",
                manifest.branch_name
            ),
        });
    }

    Ok(manifest)
}

/// Evaluates the post-run Git diff gate on a verified software-owned worktree using default
/// bounded output and changed-path limits.
///
/// Reports all added, modified, deleted, renamed, copied, type-changed, binary, and untracked
/// files (including ignored files and changed/new symlinks), allows unchanged tracked baseline
/// symlinks, and rejects any path outside `allowed_write_paths`, any changed/new/traversed
/// symlink inside the worktree, any path traversal, any control config modification, or any
/// diff budget overflow.
///
/// Never resets or deletes any files in the worktree so review artifacts remain intact.
pub fn evaluate_diff_gate(
    work_area_root: &Path,
    worktree_id: &str,
) -> Result<DiffGateReport, WorkerError> {
    evaluate_diff_gate_with_limits(
        work_area_root,
        worktree_id,
        DEFAULT_MAX_GIT_OUTPUT_BYTES,
        DEFAULT_MAX_DIFF_CHANGED_PATHS,
    )
}

/// Evaluates the post-run Git diff gate with explicit byte and path-count budgets.
///
/// If a Git query exceeds `max_git_output_bytes`, times out, or has incomplete process-group/pipe
/// cleanup, or if the number of changed paths exceeds `max_changed_paths`, returns a [`DiffGateReport`]
/// with `evidence_complete: false`, `passed: false`, `acceptance_state: WorkAcceptanceState::RejectedByDiffGate`,
/// and a [`DiffViolationKind::DiffBudgetExceeded`] violation rather than silently truncating and passing.
pub fn evaluate_diff_gate_with_limits(
    work_area_root: &Path,
    worktree_id: &str,
    max_git_output_bytes: usize,
    max_changed_paths: usize,
) -> Result<DiffGateReport, WorkerError> {
    evaluate_diff_gate_with_timeout(
        work_area_root,
        worktree_id,
        max_git_output_bytes,
        max_changed_paths,
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
}

/// Evaluates the post-run Git diff gate with explicit byte, path-count, and per-command wall-clock
/// timeout budgets.
pub fn evaluate_diff_gate_with_timeout(
    work_area_root: &Path,
    worktree_id: &str,
    max_git_output_bytes: usize,
    max_changed_paths: usize,
    git_timeout_ms: u64,
) -> Result<DiffGateReport, WorkerError> {
    evaluate_diff_gate_internal(
        work_area_root,
        worktree_id,
        DiffGateBudgets {
            max_git_output_bytes,
            max_changed_paths,
            git_timeout_ms,
            max_scan_entries: DEFAULT_MAX_SCAN_ENTRIES,
            max_scan_depth: DEFAULT_MAX_SCAN_DEPTH,
        },
    )
}

/// Evaluates the post-run Git diff gate with explicit byte, changed-path, visited-entry, and
/// directory-depth budgets.
pub fn evaluate_diff_gate_with_scan_limits(
    work_area_root: &Path,
    worktree_id: &str,
    max_git_output_bytes: usize,
    max_changed_paths: usize,
    max_scan_entries: usize,
    max_scan_depth: usize,
) -> Result<DiffGateReport, WorkerError> {
    evaluate_diff_gate_internal(
        work_area_root,
        worktree_id,
        DiffGateBudgets {
            max_git_output_bytes,
            max_changed_paths,
            git_timeout_ms: DEFAULT_GIT_COMMAND_TIMEOUT_MS,
            max_scan_entries,
            max_scan_depth,
        },
    )
}

struct DiffGateBudgets {
    max_git_output_bytes: usize,
    max_changed_paths: usize,
    git_timeout_ms: u64,
    max_scan_entries: usize,
    max_scan_depth: usize,
}

fn evaluate_diff_gate_internal(
    work_area_root: &Path,
    worktree_id: &str,
    budgets: DiffGateBudgets,
) -> Result<DiffGateReport, WorkerError> {
    let DiffGateBudgets {
        max_git_output_bytes,
        max_changed_paths,
        git_timeout_ms,
        max_scan_entries,
        max_scan_depth,
    } = budgets;

    if max_git_output_bytes == 0
        || max_changed_paths == 0
        || git_timeout_ms == 0
        || max_scan_entries == 0
        || max_scan_depth == 0
    {
        return Err(WorkerError::InvalidSpec(
            "diff gate budgets (max_git_output_bytes, max_changed_paths, git_timeout_ms, max_scan_entries, and max_scan_depth) must be non-zero"
                .to_string(),
        ));
    }

    let manifest = load_and_verify_manifest(work_area_root, worktree_id)?;
    let canonical_checkout =
        validate_existing_canonical_dir(&manifest.checkout_path, "worktree checkout_path")?;
    if canonical_checkout != manifest.checkout_path {
        return Err(WorkerError::SecurityViolation(format!(
            "worktree checkout `{}` canonicalized to unexpected path `{}`",
            manifest.checkout_path.display(),
            canonical_checkout.display()
        )));
    }

    let head_commit = resolve_commit_sha_with_timeout(&canonical_checkout, "HEAD", git_timeout_ms)?;
    let mut violations = Vec::new();
    let mut evidence_complete = true;

    // 0. Collect baseline tracked symlinks (`mode == 120000`) and baseline path kinds in
    // `base_commit` so unchanged baseline symlinks are not misclassified as newly created
    // untracked symlinks and `allowed_write_paths` scope kinds are resolved against the
    // immutable `base_commit` tree rather than mutable worktree filesystem state.
    let mut baseline_symlinks = BTreeSet::new();
    let mut baseline_path_kinds = BTreeMap::new();
    let ls_tree_out = run_git_bytes_bounded_with_timeout(
        &canonical_checkout,
        &["ls-tree", "-r", "-z", &manifest.base_commit],
        max_git_output_bytes,
        git_timeout_ms,
    )?;
    if let Some(reason) = ls_tree_out.failure_reason(max_git_output_bytes, git_timeout_ms) {
        evidence_complete = false;
        violations.push(DiffViolation {
            path: "<git-ls-tree-budget>".to_string(),
            kind: ls_tree_out.violation_kind(),
            detail: format!("git ls-tree failed supervision/budget check: {reason}"),
        });
    } else if let Ok(text) = std::str::from_utf8(&ls_tree_out.bytes) {
        if let Err(reason) =
            parse_ls_tree_baseline_z(text, &mut baseline_symlinks, &mut baseline_path_kinds)
        {
            evidence_complete = false;
            violations.push(DiffViolation {
                path: "<git-ls-tree-parse>".to_string(),
                kind: DiffViolationKind::DiffBudgetExceeded,
                detail: format!("git ls-tree output parse failed: {reason}"),
            });
        }
    }

    let resolved_write_scopes = if evidence_complete {
        resolve_diff_gate_write_scopes(
            &canonical_checkout,
            &manifest.base_commit,
            &manifest.allowed_write_paths,
            &baseline_path_kinds,
            max_git_output_bytes,
            git_timeout_ms,
            &mut evidence_complete,
            &mut violations,
        )?
    } else {
        None
    };

    // 1. Collect binary path markers from `git diff --numstat --find-renames -z <base_commit>`
    let mut binary_paths = BTreeSet::new();
    if evidence_complete {
        let numstat_out = run_git_bytes_bounded_with_timeout(
            &canonical_checkout,
            &[
                "diff",
                "--numstat",
                "--find-renames",
                "-z",
                &manifest.base_commit,
            ],
            max_git_output_bytes,
            git_timeout_ms,
        )?;
        if let Some(reason) = numstat_out.failure_reason(max_git_output_bytes, git_timeout_ms) {
            evidence_complete = false;
            violations.push(DiffViolation {
                path: "<git-diff-numstat-budget>".to_string(),
                kind: numstat_out.violation_kind(),
                detail: format!("git diff --numstat failed supervision/budget check: {reason}"),
            });
        } else if let Ok(text) = std::str::from_utf8(&numstat_out.bytes) {
            parse_numstat_z(text, &mut binary_paths);
        }
    }

    // 2. Collect tracked changes between `base_commit` and working tree
    let mut entries_map: BTreeMap<String, ChangedPathEntry> = BTreeMap::new();
    if evidence_complete {
        let name_status_out = run_git_bytes_bounded_with_timeout(
            &canonical_checkout,
            &[
                "diff",
                "--name-status",
                "--find-renames",
                "-z",
                &manifest.base_commit,
            ],
            max_git_output_bytes,
            git_timeout_ms,
        )?;
        if let Some(reason) = name_status_out.failure_reason(max_git_output_bytes, git_timeout_ms) {
            evidence_complete = false;
            violations.push(DiffViolation {
                path: "<git-diff-name-status-budget>".to_string(),
                kind: name_status_out.violation_kind(),
                detail: format!("git diff --name-status failed supervision/budget check: {reason}"),
            });
        } else if let Ok(text) = std::str::from_utf8(&name_status_out.bytes) {
            parse_name_status_z(text, &canonical_checkout, &binary_paths, &mut entries_map);
        }
    }

    // 3. If HEAD moved from `base_commit`, also union changes between `base_commit` and `HEAD`
    if evidence_complete && head_commit != manifest.base_commit {
        let head_numstat = run_git_bytes_bounded_with_timeout(
            &canonical_checkout,
            &[
                "diff",
                "--numstat",
                "--find-renames",
                "-z",
                &manifest.base_commit,
                &head_commit,
            ],
            max_git_output_bytes,
            git_timeout_ms,
        )?;
        if let Some(reason) = head_numstat.failure_reason(max_git_output_bytes, git_timeout_ms) {
            evidence_complete = false;
            violations.push(DiffViolation {
                path: "<git-diff-head-numstat-budget>".to_string(),
                kind: head_numstat.violation_kind(),
                detail: format!(
                    "git diff HEAD --numstat failed supervision/budget check: {reason}"
                ),
            });
        } else if let Ok(text) = std::str::from_utf8(&head_numstat.bytes) {
            parse_numstat_z(text, &mut binary_paths);
        }

        if evidence_complete {
            let head_diff = run_git_bytes_bounded_with_timeout(
                &canonical_checkout,
                &[
                    "diff",
                    "--name-status",
                    "--find-renames",
                    "-z",
                    &manifest.base_commit,
                    &head_commit,
                ],
                max_git_output_bytes,
                git_timeout_ms,
            )?;
            if let Some(reason) = head_diff.failure_reason(max_git_output_bytes, git_timeout_ms) {
                evidence_complete = false;
                violations.push(DiffViolation {
                    path: "<git-diff-head-name-status-budget>".to_string(),
                    kind: head_diff.violation_kind(),
                    detail: format!(
                        "git diff HEAD --name-status failed supervision/budget check: {reason}"
                    ),
                });
            } else if let Ok(text) = std::str::from_utf8(&head_diff.bytes) {
                parse_name_status_z(text, &canonical_checkout, &binary_paths, &mut entries_map);
            }
        }
    }

    // 4. Collect all untracked files (without `--exclude-standard`, so `.gitignore` cannot hide files)
    if evidence_complete {
        let untracked_out = run_git_bytes_bounded_with_timeout(
            &canonical_checkout,
            &["ls-files", "--others", "-z"],
            max_git_output_bytes,
            git_timeout_ms,
        )?;
        if let Some(reason) = untracked_out.failure_reason(max_git_output_bytes, git_timeout_ms) {
            evidence_complete = false;
            violations.push(DiffViolation {
                path: "<git-ls-files-others-budget>".to_string(),
                kind: untracked_out.violation_kind(),
                detail: format!("git ls-files --others failed supervision/budget check: {reason}"),
            });
        } else if let Ok(text) = std::str::from_utf8(&untracked_out.bytes) {
            for rel_path in text.split('\0') {
                if rel_path.is_empty() {
                    continue;
                }
                if entries_map.contains_key(rel_path) {
                    continue;
                }
                let (is_binary, is_symlink) =
                    inspect_worktree_entry(&canonical_checkout, rel_path, false);
                entries_map.insert(
                    rel_path.to_string(),
                    ChangedPathEntry {
                        path: rel_path.to_string(),
                        orig_path: None,
                        change_kind: DiffChangeKind::Untracked,
                        is_binary,
                        is_symlink,
                    },
                );
            }
        }
    }

    // 5. Walk the checkout directory with bounded visited-entry and depth budgets without
    // following symlinks to catch any newly created or modified symlink or non-UTF-8 path.
    let symlink_scan_cap = max_changed_paths
        .saturating_add(baseline_symlinks.len())
        .saturating_add(1);
    let mut scan_state = CheckoutScanState {
        max_entries: max_scan_entries,
        max_depth: max_scan_depth,
        max_symlinks: symlink_scan_cap,
        visited_entries: 0,
        symlinks: Vec::new(),
        entries_overflowed: false,
        depth_overflowed: false,
        invalid_utf8_path: false,
    };
    scan_symlinks_in_checkout(&canonical_checkout, &canonical_checkout, 0, &mut scan_state)?;
    if scan_state.invalid_utf8_path {
        evidence_complete = false;
        violations.push(DiffViolation {
            path: "<non-utf8-worktree-path>".to_string(),
            kind: DiffViolationKind::PathEscape,
            detail: "worktree contains a path with invalid UTF-8 bytes (non-UTF-8 paths are unsupported)"
                .to_string(),
        });
    }
    if scan_state.entries_overflowed {
        evidence_complete = false;
        violations.push(DiffViolation {
            path: "<symlink-scan-budget>".to_string(),
            kind: DiffViolationKind::DiffBudgetExceeded,
            detail: format!(
                "worktree scan exceeded entry budget (max_scan_entries={max_scan_entries}, max_changed_paths={max_changed_paths})"
            ),
        });
    }
    if scan_state.depth_overflowed {
        evidence_complete = false;
        violations.push(DiffViolation {
            path: "<symlink-scan-depth-budget>".to_string(),
            kind: DiffViolationKind::DiffBudgetExceeded,
            detail: format!(
                "worktree directory depth exceeded max_scan_depth budget ({max_scan_depth})"
            ),
        });
    }
    for sym_rel in scan_state.symlinks {
        if let Some(existing) = entries_map.get_mut(&sym_rel) {
            existing.is_symlink = true;
        } else if !baseline_symlinks.contains(&sym_rel) {
            entries_map.insert(
                sym_rel.clone(),
                ChangedPathEntry {
                    path: sym_rel,
                    orig_path: None,
                    change_kind: DiffChangeKind::Untracked,
                    is_binary: false,
                    is_symlink: true,
                },
            );
        }
    }

    let mut changed_paths: Vec<ChangedPathEntry> = entries_map.into_values().collect();
    if changed_paths.len() > max_changed_paths {
        let total_found = changed_paths.len();
        changed_paths.truncate(max_changed_paths);
        evidence_complete = false;
        violations.push(DiffViolation {
            path: "<changed-paths-budget>".to_string(),
            kind: DiffViolationKind::DiffBudgetExceeded,
            detail: format!(
                "changed_paths count ({total_found}) exceeded max_changed_paths budget ({max_changed_paths})"
            ),
        });
    }

    for entry in &changed_paths {
        evaluate_entry_violations(
            &canonical_checkout,
            entry,
            &manifest.allowed_write_paths,
            resolved_write_scopes.as_ref(),
            &mut violations,
        );
    }

    let passed = evidence_complete && resolved_write_scopes.is_some() && violations.is_empty();
    let acceptance_state = if passed {
        WorkAcceptanceState::DiffGatePassedPendingReview
    } else {
        WorkAcceptanceState::RejectedByDiffGate
    };

    Ok(DiffGateReport {
        worktree_id: manifest.worktree_id,
        checkout_path: canonical_checkout,
        base_commit: manifest.base_commit,
        head_commit,
        isolation_level: IsolationLevel::PostRunDiffEvidence,
        preventive_sandbox: false,
        isolation_note: concat!(
            "Post-run Git diff validation evidence only; ",
            "worker processes are not OS-sandboxed during execution."
        )
        .to_string(),
        evidence_complete,
        passed,
        acceptance_state,
        changed_paths,
        violations,
    })
}

#[allow(clippy::too_many_arguments)]
fn resolve_diff_gate_write_scopes(
    canonical_checkout: &Path,
    base_commit: &str,
    allowed_write_paths: &BTreeSet<String>,
    baseline_path_kinds: &BTreeMap<String, BaseCommitPathKind>,
    max_git_output_bytes: usize,
    git_timeout_ms: u64,
    evidence_complete: &mut bool,
    violations: &mut Vec<DiffViolation>,
) -> Result<Option<BTreeMap<String, WriteScopeKind>>, WorkerError> {
    let mut resolved = BTreeMap::new();

    for entry in allowed_write_paths {
        if let Err(err) = validate_repo_relative_path(entry, "allowed_write_paths entry") {
            violations.push(DiffViolation {
                path: entry.clone(),
                kind: DiffViolationKind::PathEscape,
                detail: err.to_string(),
            });
            continue;
        }
        let has_trailing_slash = entry.ends_with('/');
        let clean = entry.strip_suffix('/').unwrap_or(entry.as_str());
        if is_control_config_path(clean) {
            violations.push(DiffViolation {
                path: entry.clone(),
                kind: DiffViolationKind::ControlConfigModified,
                detail: format!(
                    "allowed_write_paths entry `{entry}` targets protected control configuration"
                ),
            });
            continue;
        }

        // Check whether any ancestor component of `clean` is a symlink or gitlink in `base_commit`.
        let mut ancestor_violation = false;
        let mut ancestor_regular_file = false;
        let mut anc = Path::new(clean).parent();
        while let Some(parent) = anc {
            let parent_str = parent.to_string_lossy();
            if parent_str.is_empty() {
                break;
            }
            if let Some(&anc_kind) = baseline_path_kinds.get(parent_str.as_ref()) {
                match anc_kind {
                    BaseCommitPathKind::Symlink => {
                        violations.push(DiffViolation {
                            path: entry.clone(),
                            kind: DiffViolationKind::SymlinkDetected,
                            detail: format!(
                                "allowed_write_paths entry `{entry}` traverses baseline symlink `{parent_str}` at base_commit `{base_commit}`"
                            ),
                        });
                        ancestor_violation = true;
                        break;
                    }
                    BaseCommitPathKind::Gitlink => {
                        violations.push(DiffViolation {
                            path: entry.clone(),
                            kind: DiffViolationKind::OutsideAllowedPaths,
                            detail: format!(
                                "allowed_write_paths entry `{entry}` traverses baseline submodule gitlink `{parent_str}` at base_commit `{base_commit}`"
                            ),
                        });
                        ancestor_violation = true;
                        break;
                    }
                    BaseCommitPathKind::RegularFile => {
                        ancestor_regular_file = true;
                    }
                    BaseCommitPathKind::Directory | BaseCommitPathKind::Missing => {}
                }
            }
            anc = parent.parent();
        }
        if ancestor_violation {
            continue;
        }

        let base_kind = if let Some(&known_kind) = baseline_path_kinds.get(clean) {
            known_kind
        } else if ancestor_regular_file {
            BaseCommitPathKind::Missing
        } else {
            let tree_out = run_git_bytes_bounded_with_timeout(
                canonical_checkout,
                &["ls-tree", "-d", "-z", base_commit, "--", clean],
                max_git_output_bytes,
                git_timeout_ms,
            )?;
            if let Some(reason) = tree_out.failure_reason(max_git_output_bytes, git_timeout_ms) {
                *evidence_complete = false;
                violations.push(DiffViolation {
                    path: "<git-ls-tree-scope-budget>".to_string(),
                    kind: tree_out.violation_kind(),
                    detail: format!(
                        "git ls-tree -d for allowed_write_paths entry `{entry}` failed supervision/budget check: {reason}"
                    ),
                });
                return Ok(None);
            }
            let text = std::str::from_utf8(&tree_out.bytes).map_err(|_| {
                WorkerError::GitCommandFailed {
                    command: format!("git ls-tree -d -z {base_commit} -- {clean}"),
                    status: tree_out.exit_code,
                    stderr: "git ls-tree stdout contained invalid UTF-8 bytes".to_string(),
                }
            })?;
            match parse_exact_ls_tree_entry_z(text, clean) {
                Ok(Some(kind)) => kind,
                Ok(None) => BaseCommitPathKind::Missing,
                Err(detail) => {
                    *evidence_complete = false;
                    violations.push(DiffViolation {
                        path: "<git-ls-tree-scope-parse>".to_string(),
                        kind: DiffViolationKind::DiffBudgetExceeded,
                        detail,
                    });
                    return Ok(None);
                }
            }
        };

        let scope_kind = match (has_trailing_slash, base_kind) {
            (_, BaseCommitPathKind::Symlink) => {
                violations.push(DiffViolation {
                    path: entry.clone(),
                    kind: DiffViolationKind::SymlinkDetected,
                    detail: format!(
                        "allowed_write_paths entry `{entry}` targets a symlink at base_commit `{base_commit}`"
                    ),
                });
                continue;
            }
            (_, BaseCommitPathKind::Gitlink) => {
                violations.push(DiffViolation {
                    path: entry.clone(),
                    kind: DiffViolationKind::OutsideAllowedPaths,
                    detail: format!(
                        "allowed_write_paths entry `{entry}` targets a submodule gitlink at base_commit `{base_commit}`"
                    ),
                });
                continue;
            }
            (true, BaseCommitPathKind::RegularFile) => {
                violations.push(DiffViolation {
                    path: entry.clone(),
                    kind: DiffViolationKind::OutsideAllowedPaths,
                    detail: format!(
                        "directory allowed_write_paths entry `{entry}` (with trailing `/`) targets a non-directory file at base_commit `{base_commit}`"
                    ),
                });
                continue;
            }
            (true, BaseCommitPathKind::Directory | BaseCommitPathKind::Missing) => {
                WriteScopeKind::DirectorySubtree
            }
            (false, BaseCommitPathKind::Directory) => WriteScopeKind::DirectorySubtree,
            (false, BaseCommitPathKind::RegularFile | BaseCommitPathKind::Missing) => {
                WriteScopeKind::ExactFile
            }
        };

        resolved
            .entry(clean.to_string())
            .and_modify(|existing| {
                if scope_kind == WriteScopeKind::DirectorySubtree {
                    *existing = WriteScopeKind::DirectorySubtree;
                }
            })
            .or_insert(scope_kind);
    }

    Ok(Some(resolved))
}

fn evaluate_entry_violations(
    checkout_root: &Path,
    entry: &ChangedPathEntry,
    allowed_write_paths: &BTreeSet<String>,
    resolved_write_scopes: Option<&BTreeMap<String, WriteScopeKind>>,
    violations: &mut Vec<DiffViolation>,
) {
    // 1. Validate path syntax & confinement for `entry.path` and `entry.orig_path`
    if let Err(err) = validate_repo_relative_path(&entry.path, "changed path") {
        violations.push(DiffViolation {
            path: entry.path.clone(),
            kind: DiffViolationKind::PathEscape,
            detail: err.to_string(),
        });
        return;
    }
    if let Some(ref orig) = entry.orig_path {
        if let Err(err) = validate_repo_relative_path(orig, "rename original path") {
            violations.push(DiffViolation {
                path: orig.clone(),
                kind: DiffViolationKind::PathEscape,
                detail: err.to_string(),
            });
            return;
        }
    }

    // 2. Check for symlinks on the entry itself or any ancestor component inside the checkout
    if entry.is_symlink || has_symlink_component_in_checkout(checkout_root, &entry.path) {
        violations.push(DiffViolation {
            path: entry.path.clone(),
            kind: DiffViolationKind::SymlinkDetected,
            detail: format!(
                "changed path `{}` creates or traverses a symbolic link inside the worktree",
                entry.path
            ),
        });
    }

    // 3. Check for modifications to protected repository or agent control configuration
    let mut flagged_control = false;
    if is_control_config_path(&entry.path) {
        flagged_control = true;
        violations.push(DiffViolation {
            path: entry.path.clone(),
            kind: DiffViolationKind::ControlConfigModified,
            detail: format!(
                "changed path `{}` modifies protected repository or agent control configuration",
                entry.path
            ),
        });
    }
    if let Some(orig) = entry
        .orig_path
        .as_deref()
        .filter(|orig| is_control_config_path(orig))
    {
        flagged_control = true;
        violations.push(DiffViolation {
            path: orig.to_string(),
            kind: DiffViolationKind::ControlConfigModified,
            detail: format!(
                "renamed source path `{orig}` modifies protected control configuration"
            ),
        });
    }

    // 4. Check base_commit-resolved write scopes for both `path` and `orig_path`
    if !flagged_control {
        let Some(scopes) = resolved_write_scopes else {
            violations.push(DiffViolation {
                path: entry.path.clone(),
                kind: DiffViolationKind::OutsideAllowedPaths,
                detail: format!(
                    "cannot verify changed path `{}` because baseline write-scope kinds at base_commit could not be proven",
                    entry.path
                ),
            });
            if let Some(ref orig) = entry.orig_path {
                violations.push(DiffViolation {
                    path: orig.clone(),
                    kind: DiffViolationKind::OutsideAllowedPaths,
                    detail: format!(
                        "cannot verify renamed origin path `{orig}` because baseline write-scope kinds at base_commit could not be proven"
                    ),
                });
            }
            return;
        };

        let path_is_dir_in_checkout = fs::symlink_metadata(checkout_root.join(&entry.path))
            .is_ok_and(|meta| meta.file_type().is_dir());
        let exact_file_replaced_by_dir = path_is_dir_in_checkout
            && scopes.get(&entry.path) == Some(&WriteScopeKind::ExactFile)
            && !scopes.iter().any(|(scope_path, kind)| {
                *kind == WriteScopeKind::DirectorySubtree
                    && Path::new(&entry.path).starts_with(Path::new(scope_path))
            });

        if !is_path_allowed_by_scopes(&entry.path, scopes) || exact_file_replaced_by_dir {
            violations.push(DiffViolation {
                path: entry.path.clone(),
                kind: DiffViolationKind::OutsideAllowedPaths,
                detail: format!(
                    "changed path `{}` is outside allowed_write_paths {:?}",
                    entry.path, allowed_write_paths
                ),
            });
        }
        if let Some(orig) = entry
            .orig_path
            .as_deref()
            .filter(|orig| !is_path_allowed_by_scopes(orig, scopes))
        {
            violations.push(DiffViolation {
                path: orig.to_string(),
                kind: DiffViolationKind::OutsideAllowedPaths,
                detail: format!(
                    "renamed origin path `{orig}` is outside allowed_write_paths {:?}",
                    allowed_write_paths
                ),
            });
        }
    }
}

/// Executes a configured [`WorkerSpec`] inside an owned task worktree (either an existing one
/// identified by `existing_worktree_id` or a newly created one for `(run_id, task_id)`),
/// then evaluates the post-run Git diff gate and persists updated manifest metadata,
/// with no live event observer (`progress_tx = None`).
///
/// Never performs auto-merge, auto-push, or automatic cleanup.
pub async fn execute_worker_task(
    spec: &WorkerSpec,
    run_id: &str,
    task_id: &str,
    existing_worktree_id: Option<&str>,
    cancel_rx: &mut watch::Receiver<bool>,
) -> Result<TaskExecutionReport, WorkerError> {
    execute_worker_task_with_observer(spec, run_id, task_id, existing_worktree_id, cancel_rx, None)
        .await
}

/// Executes a configured [`WorkerSpec`] inside an owned task worktree (either an existing one
/// identified by `existing_worktree_id` or a newly created one for `(run_id, task_id)`),
/// forwarding live normalized stdout events to `progress_tx` when provided, then evaluates the
/// post-run Git diff gate and persists updated manifest metadata.
///
/// When `progress_tx` is `Some`, each sanitized, bounded [`NormalizedWorkerEvent`] parsed from
/// stdout is emitted incrementally before process exit using [`mpsc::Sender::try_send`]. A full
/// or closed observer channel never blocks pipe drainage, cancellation, timeout handling, or
/// final evidence verification; undelivered live events are counted in
/// [`WorkerExecutionReport::dropped_live_events`].
///
/// Never performs auto-merge, auto-push, or automatic cleanup.
pub async fn execute_worker_task_with_observer(
    spec: &WorkerSpec,
    run_id: &str,
    task_id: &str,
    existing_worktree_id: Option<&str>,
    cancel_rx: &mut watch::Receiver<bool>,
    progress_tx: Option<mpsc::Sender<NormalizedWorkerEvent>>,
) -> Result<TaskExecutionReport, WorkerError> {
    let _validation = spec.validate()?;
    let manager = WorktreeManager::new(
        &spec.repository_path,
        &spec.work_area_root,
        &spec.base_commit,
    )?;

    let manifest = match existing_worktree_id {
        Some(wt_id) => {
            let loaded = load_and_verify_manifest(manager.work_area_root(), wt_id)?;
            if loaded.kind != WorktreeKind::Task {
                return Err(WorkerError::InvalidSpec(format!(
                    "worktree `{wt_id}` has kind `{:?}`; workers may only run in `Task` worktrees",
                    loaded.kind
                )));
            }
            if loaded.base_commit != manager.resolved_base_commit() {
                return Err(WorkerError::InvalidSpec(format!(
                    "worktree `{wt_id}` base_commit `{}` != spec resolved base_commit `{}`",
                    loaded.base_commit,
                    manager.resolved_base_commit()
                )));
            }
            if loaded.allowed_write_paths != spec.allowed_write_paths {
                return Err(WorkerError::InvalidSpec(format!(
                    "worktree `{wt_id}` allowed_write_paths {:?} != spec {:?}",
                    loaded.allowed_write_paths, spec.allowed_write_paths
                )));
            }
            loaded
        }
        None => manager.create_task_worktree(run_id, task_id, &spec.allowed_write_paths)?,
    };

    let guard = manager.begin_job(&manifest.worktree_id)?;
    let mut launch_spec = spec.clone();
    launch_spec.base_commit = manifest.base_commit.clone();
    let worker_report = run_worker_with_observer(
        &launch_spec,
        &manifest.checkout_path,
        cancel_rx,
        progress_tx,
    )
    .await?;
    let diff_gate = evaluate_diff_gate(manager.work_area_root(), &manifest.worktree_id)?;
    let updated_manifest = guard.finish()?;

    let worker_succeeded = worker_report.has_complete_natural_completion_evidence();
    let completed_pending_effort = worker_report.is_completed_pending_effort_evidence();
    let overall_acceptance_state =
        if !diff_gate.passed && (worker_succeeded || completed_pending_effort) {
            WorkAcceptanceState::RejectedByDiffGate
        } else if worker_succeeded {
            WorkAcceptanceState::DiffGatePassedPendingReview
        } else if completed_pending_effort {
            WorkAcceptanceState::FinishedNeedsEvidenceAndReview
        } else {
            WorkAcceptanceState::RejectedByWorkerOutcome
        };

    Ok(TaskExecutionReport {
        manifest: updated_manifest,
        worker_report,
        diff_gate,
        overall_acceptance_state,
    })
}

/// Performs explicit, ownership-verified cleanup of a managed worktree.
///
/// Refuses cleanup if:
/// - The private ownership manifest in `<work_area_root>/manifests/<worktree_id>.json` is missing or invalid;
/// - A worker job is currently active on the worktree (via flock or `job_active` flag);
/// - The worktree checkout branch or common gitdir does not match the manifest;
/// - The worktree contains modified, staged, untracked, or committed changes relative to `base_commit`.
///
/// Never uses `git worktree remove --force` or `git branch -D`.
pub fn cleanup_worktree(request: &CleanupRequest) -> Result<CleanupReport, WorkerError> {
    let manifest = load_and_verify_manifest(&request.work_area_root, &request.worktree_id)?;

    // 1. Refuse if a job is active (manifest flag, live PID, or held flock)
    if manifest.job_active {
        return Err(WorkerError::CleanupRefused {
            worktree_id: manifest.worktree_id,
            reason: CleanupRefusalReason::ActiveJob,
            detail: "manifest indicates `job_active == true`".to_string(),
        });
    }
    if let Some(pid) = manifest.active_job_pid.filter(|&pid| is_process_alive(pid)) {
        return Err(WorkerError::CleanupRefused {
            worktree_id: manifest.worktree_id,
            reason: CleanupRefusalReason::ActiveJob,
            detail: format!("active_job_pid {pid} is still running"),
        });
    }

    let manifests_dir = manifest.work_area_root.join(MANIFESTS_SUBDIR);
    let lock_path = manifests_dir.join(format!("{}.lock", manifest.worktree_id));
    let lock_file = open_private_lock_file(&lock_path, "worktree job lock")?;
    lock_file
        .try_lock_exclusive()
        .map_err(|_| WorkerError::CleanupRefused {
            worktree_id: manifest.worktree_id.clone(),
            reason: CleanupRefusalReason::ActiveJob,
            detail: "worktree job lock is held by an active process".to_string(),
        })?;

    // 2. Verify checkout directory still exists and is a non-symlink canonical directory
    let canonical_checkout =
        validate_existing_canonical_dir(&manifest.checkout_path, "worktree checkout_path")
            .map_err(|err| WorkerError::CleanupRefused {
                worktree_id: manifest.worktree_id.clone(),
                reason: CleanupRefusalReason::UnexpectedBranchOrOwner,
                detail: format!("checkout_path validation failed: {err}"),
            })?;
    if canonical_checkout != manifest.checkout_path {
        return Err(WorkerError::CleanupRefused {
            worktree_id: manifest.worktree_id,
            reason: CleanupRefusalReason::UnexpectedBranchOrOwner,
            detail: "checkout_path canonicalization mismatch".to_string(),
        });
    }

    // 3. Verify repository ownership (git-common-dir) and checked-out branch name
    let canonical_common_dir =
        resolve_repository_common_git_dir(&canonical_checkout).map_err(|err| {
            WorkerError::CleanupRefused {
                worktree_id: manifest.worktree_id.clone(),
                reason: CleanupRefusalReason::UnexpectedBranchOrOwner,
                detail: format!("failed to resolve worktree git-common-dir: {err}"),
            }
        })?;
    let expected_git_dir =
        resolve_repository_common_git_dir(&manifest.repository_path).map_err(|err| {
            WorkerError::CleanupRefused {
                worktree_id: manifest.worktree_id.clone(),
                reason: CleanupRefusalReason::UnexpectedBranchOrOwner,
                detail: format!("failed to resolve expected repository git-common-dir: {err}"),
            }
        })?;
    if canonical_common_dir != expected_git_dir {
        return Err(WorkerError::CleanupRefused {
            worktree_id: manifest.worktree_id,
            reason: CleanupRefusalReason::UnexpectedBranchOrOwner,
            detail: format!(
                "worktree git-common-dir `{}` does not belong to repository `{}`",
                canonical_common_dir.display(),
                expected_git_dir.display()
            ),
        });
    }
    if let Some(manifest_common_dir) = manifest
        .repository_common_git_dir
        .as_ref()
        .filter(|&dir| dir != &canonical_common_dir)
    {
        return Err(WorkerError::CleanupRefused {
            worktree_id: manifest.worktree_id,
            reason: CleanupRefusalReason::UnexpectedBranchOrOwner,
            detail: format!(
                "worktree git-common-dir `{}` does not match manifest `{}`",
                canonical_common_dir.display(),
                manifest_common_dir.display()
            ),
        });
    }

    let current_branch = run_git(
        &canonical_checkout,
        &["symbolic-ref", "--quiet", "--short", "HEAD"],
    )
    .map_err(|_| WorkerError::CleanupRefused {
        worktree_id: manifest.worktree_id.clone(),
        reason: CleanupRefusalReason::UnexpectedBranchOrOwner,
        detail: "worktree HEAD is detached or not on expected software branch".to_string(),
    })?;
    if current_branch.trim() != manifest.branch_name {
        return Err(WorkerError::CleanupRefused {
            worktree_id: manifest.worktree_id,
            reason: CleanupRefusalReason::UnexpectedBranchOrOwner,
            detail: format!(
                "worktree is on branch `{}`, expected `{}`",
                current_branch.trim(),
                manifest.branch_name
            ),
        });
    }

    // 4. Refuse if worktree has any dirty, staged, untracked, or ignored files, or HEAD != base_commit
    let status_out = run_git_bytes_bounded_with_timeout(
        &canonical_checkout,
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
    if status_out
        .failure_reason(DEFAULT_MAX_GIT_OUTPUT_BYTES, DEFAULT_GIT_COMMAND_TIMEOUT_MS)
        .is_some()
        || !status_out.bytes.is_empty()
    {
        return Err(WorkerError::CleanupRefused {
            worktree_id: manifest.worktree_id,
            reason: CleanupRefusalReason::DirtyWorktree,
            detail: concat!(
                "worktree has uncommitted, untracked, or ignored files; ",
                "preserving artifacts for review"
            )
            .to_string(),
        });
    }

    let head_sha = resolve_commit_sha(&canonical_checkout, "HEAD")?;
    if head_sha != manifest.base_commit {
        return Err(WorkerError::CleanupRefused {
            worktree_id: manifest.worktree_id,
            reason: CleanupRefusalReason::DirtyWorktree,
            detail: format!(
                "worktree HEAD `{head_sha}` diverges from base_commit `{}`; preserving for review",
                manifest.base_commit
            ),
        });
    }

    // 5. Remove worktree and branch cleanly without `--force` or `-D`
    let checkout_str = canonical_checkout
        .to_str()
        .ok_or_else(|| WorkerError::InvalidSpec("checkout_path must be valid UTF-8".to_string()))?;
    run_git(
        &manifest.repository_path,
        &["worktree", "remove", "--", checkout_str],
    )?;
    run_git(
        &manifest.repository_path,
        &["branch", "-d", "--", &manifest.branch_name],
    )?;

    let _ = fs::remove_file(&manifest.manifest_path);
    let _ = lock_file.unlock();
    let _ = fs::remove_file(&lock_path);

    Ok(CleanupReport {
        worktree_id: manifest.worktree_id,
        kind: manifest.kind,
        removed_checkout_path: canonical_checkout,
        removed_branch_name: manifest.branch_name,
        removed_manifest_path: manifest.manifest_path,
    })
}

/// Performs explicit, proof-verified cleanup of an owned worktree whose reviewed changes
/// (`accepted_head_commit`) have been integrated into `expected_target_commit`.
///
/// Unlike [`cleanup_worktree`], which refuses any worktree where `HEAD != base_commit`,
/// this function preserves `manifest.base_commit` without modification and verifies:
/// - The private ownership manifest in `<work_area_root>/manifests/<worktree_id>.json` is valid
///   and its `base_commit` matches `request.expected_base_commit`;
/// - No worker job is active (`job_active == false`, no live `active_job_pid`, and `.lock` is free);
/// - The primary source repository's `git-common-dir` matches the manifest and its current `HEAD`
///   equals `request.expected_source_head_commit`;
/// - `manifest.base_commit` is an ancestor of `request.accepted_head_commit`, and
///   `request.accepted_head_commit` is an ancestor of `request.expected_target_commit`;
/// - Non-empty validated `review_subject_digest` (64-hex SHA-256), `review_receipt_id`, and
///   `integration_receipt_id` are supplied;
/// - The worktree checkout is on `manifest.branch_name`, has no uncommitted, staged, untracked,
///   or ignored files, and its `HEAD` equals `request.accepted_head_commit` (or was already
///   removed by a prior verified partial cleanup for the exact same `accepted_head_commit`).
///
/// Never uses `git worktree remove --force` or `git branch -D`. If safe `git branch -d`
/// refuses to delete the branch (for example, because `expected_target_commit` is on an
/// integration branch not yet merged into the primary source repository's `HEAD`), records
/// `partial_cleanup: true` and preserves both the Git branch ref and the ownership manifest.
pub fn cleanup_integrated_worktree(
    request: &IntegratedCleanupRequest,
) -> Result<IntegratedCleanupReport, WorkerError> {
    let mut manifest = load_and_verify_manifest(&request.work_area_root, &request.worktree_id)?;

    // 1. Refuse if a job is active (manifest flag, live PID, or held flock).
    if manifest.job_active {
        return Err(WorkerError::CleanupRefused {
            worktree_id: manifest.worktree_id,
            reason: CleanupRefusalReason::ActiveJob,
            detail: "manifest indicates `job_active == true`".to_string(),
        });
    }
    if let Some(pid) = manifest.active_job_pid.filter(|&pid| is_process_alive(pid)) {
        return Err(WorkerError::CleanupRefused {
            worktree_id: manifest.worktree_id,
            reason: CleanupRefusalReason::ActiveJob,
            detail: format!("active_job_pid {pid} is still running"),
        });
    }

    let manifests_dir = manifest.work_area_root.join(MANIFESTS_SUBDIR);
    let lock_path = manifests_dir.join(format!("{}.lock", manifest.worktree_id));
    let lock_file = open_private_lock_file(&lock_path, "worktree job lock")?;
    lock_file
        .try_lock_exclusive()
        .map_err(|_| WorkerError::CleanupRefused {
            worktree_id: manifest.worktree_id.clone(),
            reason: CleanupRefusalReason::ActiveJob,
            detail: "worktree job lock is held by an active process".to_string(),
        })?;

    // 2. Validate proof fields (40-hex commit SHAs, 64-hex review subject digest, receipt IDs).
    let expected_base =
        validate_exact_40_hex_sha(&request.expected_base_commit, "expected_base_commit").map_err(
            |err| WorkerError::CleanupRefused {
                worktree_id: manifest.worktree_id.clone(),
                reason: CleanupRefusalReason::MissingOrInvalidManifest,
                detail: err.to_string(),
            },
        )?;
    let accepted_head =
        validate_exact_40_hex_sha(&request.accepted_head_commit, "accepted_head_commit").map_err(
            |err| WorkerError::CleanupRefused {
                worktree_id: manifest.worktree_id.clone(),
                reason: CleanupRefusalReason::MissingOrInvalidManifest,
                detail: err.to_string(),
            },
        )?;
    let expected_target =
        validate_exact_40_hex_sha(&request.expected_target_commit, "expected_target_commit")
            .map_err(|err| WorkerError::CleanupRefused {
                worktree_id: manifest.worktree_id.clone(),
                reason: CleanupRefusalReason::MissingOrInvalidManifest,
                detail: err.to_string(),
            })?;
    let expected_source_head = validate_exact_40_hex_sha(
        &request.expected_source_head_commit,
        "expected_source_head_commit",
    )
    .map_err(|err| WorkerError::CleanupRefused {
        worktree_id: manifest.worktree_id.clone(),
        reason: CleanupRefusalReason::MissingOrInvalidManifest,
        detail: err.to_string(),
    })?;
    let review_subject_digest =
        validate_sha256_hex_digest(&request.review_subject_digest, "review_subject_digest")
            .map_err(|err| WorkerError::CleanupRefused {
                worktree_id: manifest.worktree_id.clone(),
                reason: CleanupRefusalReason::MissingOrInvalidManifest,
                detail: err.to_string(),
            })?;
    validate_simple_identifier(&request.review_receipt_id, "review_receipt_id").map_err(|err| {
        WorkerError::CleanupRefused {
            worktree_id: manifest.worktree_id.clone(),
            reason: CleanupRefusalReason::MissingOrInvalidManifest,
            detail: err.to_string(),
        }
    })?;
    validate_simple_identifier(&request.integration_receipt_id, "integration_receipt_id").map_err(
        |err| WorkerError::CleanupRefused {
            worktree_id: manifest.worktree_id.clone(),
            reason: CleanupRefusalReason::MissingOrInvalidManifest,
            detail: err.to_string(),
        },
    )?;

    if manifest.base_commit != expected_base {
        return Err(WorkerError::CleanupRefused {
            worktree_id: manifest.worktree_id,
            reason: CleanupRefusalReason::UnexpectedBranchOrOwner,
            detail: format!(
                "manifest base_commit `{}` does not match expected_base_commit `{expected_base}`",
                manifest.base_commit
            ),
        });
    }

    // 3. Verify primary source repository ownership and current source HEAD.
    let expected_git_dir =
        resolve_repository_common_git_dir(&manifest.repository_path).map_err(|err| {
            WorkerError::CleanupRefused {
                worktree_id: manifest.worktree_id.clone(),
                reason: CleanupRefusalReason::UnexpectedBranchOrOwner,
                detail: format!("failed to resolve expected repository git-common-dir: {err}"),
            }
        })?;
    if let Some(manifest_common_dir) = manifest
        .repository_common_git_dir
        .as_ref()
        .filter(|&dir| dir != &expected_git_dir)
    {
        return Err(WorkerError::CleanupRefused {
            worktree_id: manifest.worktree_id,
            reason: CleanupRefusalReason::UnexpectedBranchOrOwner,
            detail: format!(
                "repository git-common-dir `{}` does not match manifest `{}`",
                expected_git_dir.display(),
                manifest_common_dir.display()
            ),
        });
    }

    let current_source_head =
        resolve_commit_sha(&manifest.repository_path, "HEAD").map_err(|err| {
            WorkerError::CleanupRefused {
                worktree_id: manifest.worktree_id.clone(),
                reason: CleanupRefusalReason::UnexpectedBranchOrOwner,
                detail: format!("failed to resolve primary source repository HEAD: {err}"),
            }
        })?;
    if current_source_head != expected_source_head {
        return Err(WorkerError::CleanupRefused {
            worktree_id: manifest.worktree_id,
            reason: CleanupRefusalReason::UnexpectedBranchOrOwner,
            detail: format!(
                "primary source repository HEAD `{current_source_head}` does not match expected_source_head_commit `{expected_source_head}`"
            ),
        });
    }

    // 4. Verify commit existence and ancestry (`base_commit <= accepted_head <= expected_target`).
    let resolved_target =
        resolve_commit_sha(&manifest.repository_path, &expected_target).map_err(|err| {
            WorkerError::CleanupRefused {
                worktree_id: manifest.worktree_id.clone(),
                reason: CleanupRefusalReason::DirtyWorktree,
                detail: format!(
                "expected_target_commit `{expected_target}` does not exist in repository: {err}"
            ),
            }
        })?;
    if resolved_target != expected_target {
        return Err(WorkerError::CleanupRefused {
            worktree_id: manifest.worktree_id,
            reason: CleanupRefusalReason::DirtyWorktree,
            detail: format!(
                "expected_target_commit resolved to `{resolved_target}`, expected `{expected_target}`"
            ),
        });
    }

    let base_is_ancestor = is_commit_ancestor(
        &manifest.repository_path,
        &manifest.base_commit,
        &accepted_head,
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .map_err(|err| WorkerError::CleanupRefused {
        worktree_id: manifest.worktree_id.clone(),
        reason: CleanupRefusalReason::DirtyWorktree,
        detail: format!("failed to verify base_commit ancestry of accepted_head_commit: {err}"),
    })?;
    if !base_is_ancestor {
        return Err(WorkerError::CleanupRefused {
            worktree_id: manifest.worktree_id,
            reason: CleanupRefusalReason::DirtyWorktree,
            detail: format!(
                "manifest base_commit `{}` is not an ancestor of accepted_head_commit `{accepted_head}`",
                manifest.base_commit
            ),
        });
    }

    let accepted_in_target = is_commit_ancestor(
        &manifest.repository_path,
        &accepted_head,
        &expected_target,
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .map_err(|err| WorkerError::CleanupRefused {
        worktree_id: manifest.worktree_id.clone(),
        reason: CleanupRefusalReason::DirtyWorktree,
        detail: format!(
            "failed to verify accepted_head_commit ancestry in expected_target_commit: {err}"
        ),
    })?;
    if !accepted_in_target {
        return Err(WorkerError::CleanupRefused {
            worktree_id: manifest.worktree_id,
            reason: CleanupRefusalReason::DirtyWorktree,
            detail: format!(
                "accepted_head_commit `{accepted_head}` is not an ancestor of expected_target_commit `{expected_target}`; refusing to delete unmerged worktree"
            ),
        });
    }

    // 5. Verify and remove the worktree checkout (or verify a prior partial cleanup where the
    // checkout was already cleanly removed at this exact `accepted_head`).
    let resuming_partial_cleanup = manifest.partial_cleanup_accepted_head.as_deref()
        == Some(accepted_head.as_str())
        && fs::symlink_metadata(&manifest.checkout_path).is_err();

    let removed_checkout_path = if resuming_partial_cleanup {
        let branch_ref = format!("refs/heads/{}", manifest.branch_name);
        let branch_tip = resolve_commit_sha(&manifest.repository_path, &branch_ref).map_err(
            |err| WorkerError::CleanupRefused {
                worktree_id: manifest.worktree_id.clone(),
                reason: CleanupRefusalReason::UnexpectedBranchOrOwner,
                detail: format!(
                    "failed to resolve preserved branch `{branch_ref}` during partial cleanup completion: {err}"
                ),
            },
        )?;
        if branch_tip != accepted_head {
            return Err(WorkerError::CleanupRefused {
                worktree_id: manifest.worktree_id,
                reason: CleanupRefusalReason::DirtyWorktree,
                detail: format!(
                    "preserved branch `{}` tip `{branch_tip}` diverges from accepted_head_commit `{accepted_head}`",
                    manifest.branch_name
                ),
            });
        }
        manifest.checkout_path.clone()
    } else {
        let canonical_checkout =
            validate_existing_canonical_dir(&manifest.checkout_path, "worktree checkout_path")
                .map_err(|err| WorkerError::CleanupRefused {
                    worktree_id: manifest.worktree_id.clone(),
                    reason: CleanupRefusalReason::UnexpectedBranchOrOwner,
                    detail: format!("checkout_path validation failed: {err}"),
                })?;
        if canonical_checkout != manifest.checkout_path {
            return Err(WorkerError::CleanupRefused {
                worktree_id: manifest.worktree_id,
                reason: CleanupRefusalReason::UnexpectedBranchOrOwner,
                detail: "checkout_path canonicalization mismatch".to_string(),
            });
        }

        let canonical_common_dir =
            resolve_repository_common_git_dir(&canonical_checkout).map_err(|err| {
                WorkerError::CleanupRefused {
                    worktree_id: manifest.worktree_id.clone(),
                    reason: CleanupRefusalReason::UnexpectedBranchOrOwner,
                    detail: format!("failed to resolve worktree git-common-dir: {err}"),
                }
            })?;
        if canonical_common_dir != expected_git_dir {
            return Err(WorkerError::CleanupRefused {
                worktree_id: manifest.worktree_id,
                reason: CleanupRefusalReason::UnexpectedBranchOrOwner,
                detail: format!(
                    "worktree git-common-dir `{}` does not belong to repository `{}`",
                    canonical_common_dir.display(),
                    expected_git_dir.display()
                ),
            });
        }

        let current_branch = run_git(
            &canonical_checkout,
            &["symbolic-ref", "--quiet", "--short", "HEAD"],
        )
        .map_err(|_| WorkerError::CleanupRefused {
            worktree_id: manifest.worktree_id.clone(),
            reason: CleanupRefusalReason::UnexpectedBranchOrOwner,
            detail: "worktree HEAD is detached or not on expected software branch".to_string(),
        })?;
        if current_branch.trim() != manifest.branch_name {
            return Err(WorkerError::CleanupRefused {
                worktree_id: manifest.worktree_id,
                reason: CleanupRefusalReason::UnexpectedBranchOrOwner,
                detail: format!(
                    "worktree is on branch `{}`, expected `{}`",
                    current_branch.trim(),
                    manifest.branch_name
                ),
            });
        }

        if !is_worktree_clean(&canonical_checkout, DEFAULT_GIT_COMMAND_TIMEOUT_MS)? {
            return Err(WorkerError::CleanupRefused {
                worktree_id: manifest.worktree_id,
                reason: CleanupRefusalReason::DirtyWorktree,
                detail: concat!(
                    "worktree has uncommitted, untracked, or ignored files; ",
                    "preserving artifacts for review"
                )
                .to_string(),
            });
        }

        let head_sha = resolve_commit_sha(&canonical_checkout, "HEAD")?;
        if head_sha != accepted_head {
            return Err(WorkerError::CleanupRefused {
                worktree_id: manifest.worktree_id,
                reason: CleanupRefusalReason::DirtyWorktree,
                detail: format!(
                    "worktree HEAD `{head_sha}` does not match accepted_head_commit `{accepted_head}`; preserving worktree"
                ),
            });
        }

        let checkout_str = canonical_checkout.to_str().ok_or_else(|| {
            WorkerError::InvalidSpec("checkout_path must be valid UTF-8".to_string())
        })?;
        run_git(
            &manifest.repository_path,
            &["worktree", "remove", "--", checkout_str],
        )?;
        canonical_checkout
    };

    // 6. Attempt safe branch deletion (`git branch -d`, never `-D`).
    let branch_out = run_git_bytes_bounded_raw(
        &manifest.repository_path,
        &["branch", "-d", "--", &manifest.branch_name],
        MAX_GIT_STDERR_BYTES,
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )?;
    if let Some(reason) =
        branch_out.failure_reason(MAX_GIT_STDERR_BYTES, DEFAULT_GIT_COMMAND_TIMEOUT_MS)
    {
        return Err(WorkerError::GitCommandFailed {
            command: format!("git branch -d -- {}", manifest.branch_name),
            status: branch_out.exit_code,
            stderr: reason,
        });
    }

    if branch_out.exit_code == Some(0) {
        let _ = fs::remove_file(&manifest.manifest_path);
        let _ = lock_file.unlock();
        let _ = fs::remove_file(&lock_path);

        Ok(IntegratedCleanupReport {
            worktree_id: manifest.worktree_id,
            kind: manifest.kind,
            removed_checkout_path,
            branch_name: manifest.branch_name,
            manifest_path: manifest.manifest_path,
            accepted_head_commit: accepted_head,
            verified_target_commit: expected_target,
            verified_source_head_commit: expected_source_head,
            review_subject_digest,
            review_receipt_id: request.review_receipt_id.clone(),
            integration_receipt_id: request.integration_receipt_id.clone(),
            checkout_removed: true,
            branch_removed: true,
            manifest_removed: true,
            partial_cleanup: false,
            branch_preserved_reason: None,
        })
    } else {
        manifest.partial_cleanup_accepted_head = Some(accepted_head.clone());
        let manifest_filename = format!("{}.json", manifest.worktree_id);
        write_private_json_atomic(
            &manifests_dir,
            &manifest_filename,
            &manifest,
            "worktree manifest",
        )?;
        let _ = lock_file.unlock();

        let reason = if branch_out.stderr.trim().is_empty() {
            format!(
                "safe `git branch -d` refused to delete branch `{}` (exit {:?}); branch and manifest preserved",
                manifest.branch_name, branch_out.exit_code
            )
        } else {
            branch_out.stderr
        };

        Ok(IntegratedCleanupReport {
            worktree_id: manifest.worktree_id,
            kind: manifest.kind,
            removed_checkout_path,
            branch_name: manifest.branch_name,
            manifest_path: manifest.manifest_path,
            accepted_head_commit: accepted_head,
            verified_target_commit: expected_target,
            verified_source_head_commit: expected_source_head,
            review_subject_digest,
            review_receipt_id: request.review_receipt_id.clone(),
            integration_receipt_id: request.integration_receipt_id.clone(),
            checkout_removed: true,
            branch_removed: false,
            manifest_removed: false,
            partial_cleanup: true,
            branch_preserved_reason: Some(reason),
        })
    }
}

/// Object kind of a repository-relative path in an immutable pinned Git `base_commit` tree.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BaseCommitPathKind {
    /// Path does not exist in `base_commit` (and does not traverse a tracked symlink or gitlink).
    Missing,
    /// Path is a regular or executable file (`100644` or `100755` blob) in `base_commit`.
    RegularFile,
    /// Path is a directory (`040000` tree) in `base_commit`.
    Directory,
    /// Path is a symbolic link (`120000` blob) or traverses an ancestor symbolic link in `base_commit`.
    Symlink,
    /// Path is a submodule gitlink (`160000` commit) or traverses an ancestor gitlink in `base_commit`.
    Gitlink,
}

/// Inspects the object kind of `rel_path` in the immutable `base_commit` tree of `repo_or_worktree`
/// using bounded, environment-sanitized, process-group-supervised `git ls-tree` invocations.
pub fn inspect_base_commit_path_kind(
    repo_or_worktree: &Path,
    base_commit: &str,
    rel_path: &str,
    timeout_ms: u64,
) -> Result<BaseCommitPathKind, WorkerError> {
    let canonical_dir = validate_existing_canonical_dir(repo_or_worktree, "repository_path")?;
    validate_commit_ref(base_commit)?;
    validate_repo_relative_path(rel_path, "allowed_write_paths entry")?;
    let clean = rel_path.strip_suffix('/').unwrap_or(rel_path);

    let mut ancestors = Vec::new();
    let mut anc = Path::new(clean).parent();
    while let Some(parent) = anc {
        let s = parent.to_string_lossy();
        if s.is_empty() {
            break;
        }
        ancestors.push(s.to_string());
        anc = parent.parent();
    }
    ancestors.reverse();

    for ancestor_path in &ancestors {
        match inspect_single_clean_path_at_base(
            &canonical_dir,
            base_commit,
            ancestor_path,
            timeout_ms,
        )? {
            BaseCommitPathKind::Directory => {}
            BaseCommitPathKind::Symlink => return Ok(BaseCommitPathKind::Symlink),
            BaseCommitPathKind::Gitlink => return Ok(BaseCommitPathKind::Gitlink),
            BaseCommitPathKind::RegularFile | BaseCommitPathKind::Missing => {
                return Ok(BaseCommitPathKind::Missing);
            }
        }
    }

    inspect_single_clean_path_at_base(&canonical_dir, base_commit, clean, timeout_ms)
}

/// Resolves a set of `allowed_write_paths` entries against the immutable `base_commit` tree in
/// `repo_or_worktree`:
/// - Explicit trailing-`/` entries (`"foo/"`) resolve to [`WriteScopeKind::DirectorySubtree`]
///   (and fail closed if `"foo"` at `base_commit` is a regular file, symlink, or gitlink).
/// - Bare entries (`"foo"`, without trailing `/`) resolve to [`WriteScopeKind::DirectorySubtree`]
///   **only** if `"foo"` exists as a directory (`040000` tree) in `base_commit` (preserving legacy
///   startup configuration semantics), and resolve to [`WriteScopeKind::ExactFile`] when `"foo"`
///   is a regular file or absent (`Missing`) in `base_commit`.
/// - Entries targeting or traversing a symlink or submodule gitlink in `base_commit` are rejected.
pub fn resolve_allowed_write_scopes_at_base(
    repo_or_worktree: &Path,
    base_commit: &str,
    allowed_write_paths: &BTreeSet<String>,
    timeout_ms: u64,
) -> Result<BTreeMap<String, WriteScopeKind>, WorkerError> {
    let canonical_dir = validate_existing_canonical_dir(repo_or_worktree, "repository_path")?;
    validate_commit_ref(base_commit)?;
    let mut resolved = BTreeMap::new();

    for entry in allowed_write_paths {
        validate_repo_relative_path(entry, "allowed_write_paths entry")?;
        let has_trailing_slash = entry.ends_with('/');
        let clean = entry.strip_suffix('/').unwrap_or(entry.as_str());
        if is_control_config_path(clean) {
            return Err(WorkerError::SecurityViolation(format!(
                "allowed_write_paths entry `{entry}` targets protected control configuration"
            )));
        }

        let base_kind =
            inspect_base_commit_path_kind(&canonical_dir, base_commit, entry, timeout_ms)?;
        let scope_kind = match (has_trailing_slash, base_kind) {
            (_, BaseCommitPathKind::Symlink) => {
                return Err(WorkerError::SecurityViolation(format!(
                    "allowed_write_paths entry `{entry}` targets or traverses a symlink at base_commit `{base_commit}`"
                )));
            }
            (_, BaseCommitPathKind::Gitlink) => {
                return Err(WorkerError::SecurityViolation(format!(
                    "allowed_write_paths entry `{entry}` targets or traverses a submodule gitlink at base_commit `{base_commit}`"
                )));
            }
            (true, BaseCommitPathKind::RegularFile) => {
                return Err(WorkerError::SecurityViolation(format!(
                    "directory allowed_write_paths entry `{entry}` (with trailing `/`) targets a non-directory file at base_commit `{base_commit}`"
                )));
            }
            (true, BaseCommitPathKind::Directory | BaseCommitPathKind::Missing) => {
                WriteScopeKind::DirectorySubtree
            }
            (false, BaseCommitPathKind::Directory) => WriteScopeKind::DirectorySubtree,
            (false, BaseCommitPathKind::RegularFile | BaseCommitPathKind::Missing) => {
                WriteScopeKind::ExactFile
            }
        };

        resolved
            .entry(clean.to_string())
            .and_modify(|existing| {
                if scope_kind == WriteScopeKind::DirectorySubtree {
                    *existing = WriteScopeKind::DirectorySubtree;
                }
            })
            .or_insert(scope_kind);
    }

    Ok(resolved)
}

fn inspect_single_clean_path_at_base(
    canonical_dir: &Path,
    base_commit: &str,
    clean: &str,
    timeout_ms: u64,
) -> Result<BaseCommitPathKind, WorkerError> {
    let dir_out = run_git_bytes_bounded_with_timeout(
        canonical_dir,
        &["ls-tree", "-d", "-z", base_commit, "--", clean],
        MAX_GIT_STDERR_BYTES,
        timeout_ms,
    )?;
    if let Some(reason) = dir_out.failure_reason(MAX_GIT_STDERR_BYTES, timeout_ms) {
        return Err(WorkerError::GitCommandFailed {
            command: format!("git ls-tree -d -z {base_commit} -- {clean}"),
            status: dir_out.exit_code,
            stderr: reason,
        });
    }
    let dir_text =
        std::str::from_utf8(&dir_out.bytes).map_err(|_| WorkerError::GitCommandFailed {
            command: format!("git ls-tree -d -z {base_commit} -- {clean}"),
            status: dir_out.exit_code,
            stderr: "git ls-tree stdout contained invalid UTF-8 bytes".to_string(),
        })?;
    if let Some(kind) = parse_exact_ls_tree_entry_z(dir_text, clean).map_err(|detail| {
        WorkerError::GitCommandFailed {
            command: format!("git ls-tree -d -z {base_commit} -- {clean}"),
            status: dir_out.exit_code,
            stderr: detail,
        }
    })? {
        return Ok(kind);
    }

    let entry_out = run_git_bytes_bounded_with_timeout(
        canonical_dir,
        &["ls-tree", "-z", base_commit, "--", clean],
        MAX_GIT_STDERR_BYTES,
        timeout_ms,
    )?;
    if let Some(reason) = entry_out.failure_reason(MAX_GIT_STDERR_BYTES, timeout_ms) {
        return Err(WorkerError::GitCommandFailed {
            command: format!("git ls-tree -z {base_commit} -- {clean}"),
            status: entry_out.exit_code,
            stderr: reason,
        });
    }
    let entry_text =
        std::str::from_utf8(&entry_out.bytes).map_err(|_| WorkerError::GitCommandFailed {
            command: format!("git ls-tree -z {base_commit} -- {clean}"),
            status: entry_out.exit_code,
            stderr: "git ls-tree stdout contained invalid UTF-8 bytes".to_string(),
        })?;
    let maybe_kind = parse_exact_ls_tree_entry_z(entry_text, clean).map_err(|detail| {
        WorkerError::GitCommandFailed {
            command: format!("git ls-tree -z {base_commit} -- {clean}"),
            status: entry_out.exit_code,
            stderr: detail,
        }
    })?;
    Ok(maybe_kind.unwrap_or(BaseCommitPathKind::Missing))
}

/// Validates an exact 40-character hexadecimal Git object SHA and normalizes it to lowercase.
pub fn validate_exact_40_hex_sha(value: &str, label: &str) -> Result<String, WorkerError> {
    if value.len() != 40 || !value.chars().all(|c| c.is_ascii_hexdigit()) {
        return Err(WorkerError::InvalidSpec(format!(
            "{label} must be an exact 40-character hexadecimal SHA (got `{value}`)"
        )));
    }
    Ok(value.to_ascii_lowercase())
}

/// Validates an exact 64-character hexadecimal SHA-256 digest and normalizes it to lowercase.
pub fn validate_sha256_hex_digest(value: &str, label: &str) -> Result<String, WorkerError> {
    if value.len() != 64 || !value.chars().all(|c| c.is_ascii_hexdigit()) {
        return Err(WorkerError::InvalidSpec(format!(
            "{label} must be an exact 64-character hexadecimal SHA-256 digest (got `{value}`)"
        )));
    }
    Ok(value.to_ascii_lowercase())
}

/// Resolves `rev` in `repo_or_worktree` to an exact 40-character lowercase hexadecimal commit SHA.
pub fn resolve_exact_commit_sha(
    repo_or_worktree: &Path,
    rev: &str,
    timeout_ms: u64,
) -> Result<String, WorkerError> {
    validate_commit_ref(rev)?;
    resolve_commit_sha_with_timeout(repo_or_worktree, rev, timeout_ms)
}

/// Resolves `rev^{tree}` in `repo_or_worktree` to an exact 40-character lowercase hexadecimal tree SHA.
pub fn resolve_exact_tree_sha(
    repo_or_worktree: &Path,
    rev: &str,
    timeout_ms: u64,
) -> Result<String, WorkerError> {
    validate_commit_ref(rev)?;
    let rev_arg = format!("{rev}^{{tree}}");
    let out = run_git_with_timeout(
        repo_or_worktree,
        &["rev-parse", "--verify", &rev_arg],
        timeout_ms,
    )?;
    let sha = out.trim();
    if sha.len() != 40 || !sha.chars().all(|c| c.is_ascii_hexdigit()) {
        return Err(WorkerError::GitCommandFailed {
            command: format!("rev-parse --verify {rev_arg}"),
            status: Some(0),
            stderr: format!("expected 40-hex tree SHA, got `{sha}`"),
        });
    }
    Ok(sha.to_ascii_lowercase())
}

/// Returns `true` if `ancestor_rev` is an ancestor of (or equal to) `descendant_rev` in `repo_or_worktree`.
pub fn is_commit_ancestor(
    repo_or_worktree: &Path,
    ancestor_rev: &str,
    descendant_rev: &str,
    timeout_ms: u64,
) -> Result<bool, WorkerError> {
    validate_commit_ref(ancestor_rev)?;
    validate_commit_ref(descendant_rev)?;
    let out = run_git_bytes_bounded_raw(
        repo_or_worktree,
        &[
            "merge-base",
            "--is-ancestor",
            "--",
            ancestor_rev,
            descendant_rev,
        ],
        MAX_GIT_STDERR_BYTES,
        timeout_ms,
    )?;
    if let Some(reason) = out.failure_reason(MAX_GIT_STDERR_BYTES, timeout_ms) {
        return Err(WorkerError::GitCommandFailed {
            command: format!("git merge-base --is-ancestor -- {ancestor_rev} {descendant_rev}"),
            status: out.exit_code,
            stderr: reason,
        });
    }
    match out.exit_code {
        Some(0) => Ok(true),
        Some(1) => Ok(false),
        other => Err(WorkerError::GitCommandFailed {
            command: format!("git merge-base --is-ancestor -- {ancestor_rev} {descendant_rev}"),
            status: other,
            stderr: out.stderr,
        }),
    }
}

/// Returns `true` if `checkout_path` has no uncommitted, staged, untracked, or ignored files.
pub fn is_worktree_clean(checkout_path: &Path, timeout_ms: u64) -> Result<bool, WorkerError> {
    let status_out = run_git_bytes_bounded_with_timeout(
        checkout_path,
        &[
            "status",
            "--porcelain=v1",
            "-z",
            "--untracked-files=all",
            "--ignored=matching",
        ],
        DEFAULT_MAX_GIT_OUTPUT_BYTES,
        timeout_ms,
    )?;
    if status_out
        .failure_reason(DEFAULT_MAX_GIT_OUTPUT_BYTES, timeout_ms)
        .is_some()
    {
        return Ok(false);
    }
    Ok(status_out.bytes.is_empty())
}

/// Returns `true` if `checkout_path` has no uncommitted, staged, or non-ignored untracked files,
/// while allowing ignored source caches (such as `target/`, `node_modules/`, or `.local/`).
pub fn is_source_worktree_clean(
    checkout_path: &Path,
    timeout_ms: u64,
) -> Result<bool, WorkerError> {
    let status_out = run_git_bytes_bounded_with_timeout(
        checkout_path,
        &[
            "status",
            "--porcelain=v1",
            "-z",
            "--untracked-files=all",
            "--ignored=no",
        ],
        DEFAULT_MAX_GIT_OUTPUT_BYTES,
        timeout_ms,
    )?;
    if status_out
        .failure_reason(DEFAULT_MAX_GIT_OUTPUT_BYTES, timeout_ms)
        .is_some()
    {
        return Ok(false);
    }
    Ok(status_out.bytes.is_empty())
}

/// Removes all inherited `GIT_*` environment variables (case-insensitive) from `cmd`
/// and installs controlled `GIT_TERMINAL_PROMPT=0`, `GIT_CONFIG_NOSYSTEM=1`,
/// `GIT_CONFIG_SYSTEM=/dev/null`, `GIT_CONFIG_GLOBAL=/dev/null`, and
/// `GIT_OPTIONAL_LOCKS=0` overrides.
pub fn sanitize_git_command_env(cmd: &mut std::process::Command) {
    sanitize_git_command_env_from(cmd, std::env::vars_os());
}

/// Removes all `GIT_*` environment variables (case-insensitive) present in `vars` from `cmd`
/// and installs controlled `SANITIZED_GIT_ENV_OVERRIDES`.
pub fn sanitize_git_command_env_from<I, K, V>(cmd: &mut std::process::Command, vars: I)
where
    I: IntoIterator<Item = (K, V)>,
    K: AsRef<std::ffi::OsStr>,
{
    for (key, _) in vars {
        let key_os = key.as_ref();
        if key_os
            .to_string_lossy()
            .to_ascii_uppercase()
            .starts_with("GIT_")
        {
            cmd.env_remove(key_os);
        }
    }
    for &(k, v) in SANITIZED_GIT_ENV_OVERRIDES {
        cmd.env(k, v);
    }
}

/// Runs a supervised, environment-sanitized `git -C <dir> <args...>` command with bounded output
/// and returns its UTF-8 stdout on exit code 0.
pub fn run_sanitized_git(
    dir: &Path,
    args: &[&str],
    timeout_ms: u64,
) -> Result<String, WorkerError> {
    run_git_with_timeout(dir, args, timeout_ms)
}

/// Runs a supervised, environment-sanitized `git -C <dir> <args...>` command with an explicit
/// stdout byte cap and returns its raw stdout bytes on exit code 0.
pub fn run_sanitized_git_bytes(
    dir: &Path,
    args: &[&str],
    max_stdout_bytes: usize,
    timeout_ms: u64,
) -> Result<SupervisedGitOutput, WorkerError> {
    let out = run_git_bytes_bounded_raw(dir, args, max_stdout_bytes, timeout_ms)?;
    if let Some(reason) = out.failure_overflow_or_timeout_reason(max_stdout_bytes, timeout_ms) {
        return Err(WorkerError::GitCommandFailed {
            command: format!("git {}", args.join(" ")),
            status: out.exit_code,
            stderr: reason,
        });
    }
    if out.exit_code != Some(0) {
        return Err(WorkerError::GitCommandFailed {
            command: format!("git {}", args.join(" ")),
            status: out.exit_code,
            stderr: out.stderr,
        });
    }
    Ok(SupervisedGitOutput {
        stdout_bytes: out.bytes,
        stderr: out.stderr,
        exit_code: out.exit_code,
    })
}

/// Runs a supervised, environment-sanitized `git -C <dir> <args...>` command and returns its
/// bounded output even when the exit code is non-zero (still failing if stdout/stderr overflows,
/// times out, or has incomplete process-group cleanup).
pub fn run_sanitized_git_allow_nonzero(
    dir: &Path,
    args: &[&str],
    max_stdout_bytes: usize,
    timeout_ms: u64,
) -> Result<SupervisedGitOutput, WorkerError> {
    let out = run_git_bytes_bounded_raw(dir, args, max_stdout_bytes, timeout_ms)?;
    if let Some(reason) = out.failure_overflow_or_timeout_reason(max_stdout_bytes, timeout_ms) {
        return Err(WorkerError::GitCommandFailed {
            command: format!("git {}", args.join(" ")),
            status: out.exit_code,
            stderr: reason,
        });
    }
    Ok(SupervisedGitOutput {
        stdout_bytes: out.bytes,
        stderr: out.stderr,
        exit_code: out.exit_code,
    })
}

fn prepare_owned_work_area(
    checked_work_area: &Path,
    repository_common_git_dir: &Path,
) -> Result<PathBuf, WorkerError> {
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;

    match fs::symlink_metadata(checked_work_area) {
        Ok(meta) => {
            if meta.file_type().is_symlink() {
                return Err(WorkerError::SecurityViolation(format!(
                    "work_area_root `{}` must not be a symlink",
                    checked_work_area.display()
                )));
            }
            if !meta.file_type().is_dir() {
                return Err(WorkerError::InvalidSpec(format!(
                    "work_area_root `{}` must be a directory",
                    checked_work_area.display()
                )));
            }

            #[cfg(unix)]
            {
                let mode = meta.permissions().mode() & 0o777;
                if mode & 0o077 != 0 {
                    let mut entry_names = Vec::new();
                    for entry_res in fs::read_dir(checked_work_area)? {
                        let entry = entry_res?;
                        let name = entry.file_name().to_string_lossy().to_string();
                        if name != WORK_AREA_LOCK_FILENAME {
                            entry_names.push(name);
                        }
                    }
                    entry_names.sort();
                    if !entry_names.is_empty() {
                        let has_marker = entry_names.iter().any(|n| n == WORK_AREA_MARKER_FILENAME);
                        let unexpected = entry_names.iter().find(|n| {
                            !matches!(
                                n.as_str(),
                                WORK_AREA_MARKER_FILENAME | MANIFESTS_SUBDIR | CHECKOUTS_SUBDIR
                            )
                        });
                        if !has_marker || unexpected.is_some() {
                            return Err(WorkerError::SecurityViolation(format!(
                                "refusing existing non-empty work_area_root `{}` without valid Threadlane ownership marker (found entries: {:?})",
                                checked_work_area.display(),
                                entry_names
                            )));
                        }
                    }
                    return Err(WorkerError::SecurityViolation(format!(
                        "existing work_area_root `{}` has non-private permissions {:04o} (expected 0700); refusing to chmod existing directory",
                        checked_work_area.display(),
                        mode
                    )));
                }
            }
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            let _ = ensure_work_area_root_dir(checked_work_area)?;
        }
        Err(err) => return Err(WorkerError::Io(err)),
    }

    let canonical_work_area = fs::canonicalize(checked_work_area)?;
    if canonical_work_area != checked_work_area {
        return Err(WorkerError::SecurityViolation(format!(
            "work_area_root `{}` resolved to unexpected canonical path `{}`",
            checked_work_area.display(),
            canonical_work_area.display()
        )));
    }

    // Serialize initial work_area_root ownership initialization and verification across processes
    // BEFORE inspecting directory entries or writing the atomic ownership marker.
    let lock_path = canonical_work_area.join(WORK_AREA_LOCK_FILENAME);
    let lock_existed_before = fs::symlink_metadata(&lock_path).is_ok();
    let lock_file = open_work_area_lock_file(&lock_path)?;
    lock_file.lock_exclusive().map_err(WorkerError::Io)?;

    let mut entry_names = Vec::new();
    for entry_res in fs::read_dir(&canonical_work_area)? {
        let entry = entry_res?;
        let name = entry.file_name().to_string_lossy().to_string();
        if name == WORK_AREA_LOCK_FILENAME {
            continue;
        }
        entry_names.push(name);
    }
    entry_names.sort();

    // Refuse unrelated non-empty roots (including arbitrary tempfiles) BEFORE any subdirectory creation.
    if !entry_names.is_empty() {
        let has_marker = entry_names.iter().any(|n| n == WORK_AREA_MARKER_FILENAME);
        let unexpected = entry_names.iter().find(|n| {
            !matches!(
                n.as_str(),
                WORK_AREA_MARKER_FILENAME | MANIFESTS_SUBDIR | CHECKOUTS_SUBDIR
            )
        });
        if !has_marker || unexpected.is_some() {
            if !lock_existed_before {
                let _ = fs::remove_file(&lock_path);
            }
            let _ = lock_file.unlock();
            return Err(WorkerError::SecurityViolation(format!(
                "refusing existing non-empty work_area_root `{}` without valid Threadlane ownership marker (found entries: {:?})",
                checked_work_area.display(),
                entry_names
            )));
        }

        let marker = verify_existing_work_area_marker(&canonical_work_area)?;
        if marker.repository_common_git_dir != repository_common_git_dir {
            let _ = lock_file.unlock();
            return Err(WorkerError::SecurityViolation(format!(
                "work_area_root `{}` is owned by repository `{}` (cannot reuse for `{}`)",
                canonical_work_area.display(),
                marker.repository_common_git_dir.display(),
                repository_common_git_dir.display()
            )));
        }
    } else {
        let marker = WorkAreaOwnershipMarker {
            marker_version: WORK_AREA_MARKER_VERSION,
            work_area_root: canonical_work_area.clone(),
            repository_common_git_dir: repository_common_git_dir.to_path_buf(),
        };
        write_private_json_atomic(
            &canonical_work_area,
            WORK_AREA_MARKER_FILENAME,
            &marker,
            "work area ownership marker",
        )?;
    }

    let _ = ensure_private_dir(&canonical_work_area.join(MANIFESTS_SUBDIR), "manifests dir")?;
    let checkouts_dir =
        ensure_private_dir(&canonical_work_area.join(CHECKOUTS_SUBDIR), "checkouts dir")?;
    let _ = ensure_private_dir(&checkouts_dir.join("runs"), "run checkouts dir")?;
    let _ = ensure_private_dir(&checkouts_dir.join("tasks"), "task checkouts dir")?;
    let _ = lock_file.unlock();
    Ok(canonical_work_area)
}

fn ensure_work_area_root_dir(checked_work_area: &Path) -> Result<PathBuf, WorkerError> {
    let checked = validate_absolute_no_symlink_components(checked_work_area, "work_area_root")?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::{DirBuilderExt, PermissionsExt};
        use std::path::Component;

        let mut current = PathBuf::new();
        for comp in checked.components() {
            match comp {
                Component::Prefix(prefix) => current.push(prefix.as_os_str()),
                Component::RootDir => current.push(comp.as_os_str()),
                Component::CurDir | Component::ParentDir => {
                    return Err(WorkerError::SecurityViolation(format!(
                        "work_area_root path `{}` must not contain relative traversal components",
                        checked_work_area.display()
                    )));
                }
                Component::Normal(part) => {
                    current.push(part);
                    match fs::symlink_metadata(&current) {
                        Ok(meta) => {
                            if meta.file_type().is_symlink() {
                                return Err(WorkerError::SecurityViolation(format!(
                                    "work_area_root path `{}` traverses symlink `{}`",
                                    checked_work_area.display(),
                                    current.display()
                                )));
                            }
                            if !meta.file_type().is_dir() {
                                return Err(WorkerError::SecurityViolation(format!(
                                    "work_area_root component `{}` is not a directory",
                                    current.display()
                                )));
                            }
                        }
                        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                            let mut builder = fs::DirBuilder::new();
                            builder.mode(0o700);
                            match builder.create(&current) {
                                Ok(()) => {
                                    fs::set_permissions(
                                        &current,
                                        fs::Permissions::from_mode(0o700),
                                    )?;
                                }
                                Err(create_err)
                                    if create_err.kind() == std::io::ErrorKind::AlreadyExists =>
                                {
                                    let meta = fs::symlink_metadata(&current)?;
                                    if meta.file_type().is_symlink() {
                                        return Err(WorkerError::SecurityViolation(format!(
                                            "work_area_root path `{}` traverses symlink `{}`",
                                            checked_work_area.display(),
                                            current.display()
                                        )));
                                    }
                                    if !meta.file_type().is_dir() {
                                        return Err(WorkerError::SecurityViolation(format!(
                                            "work_area_root component `{}` is not a directory",
                                            current.display()
                                        )));
                                    }
                                }
                                Err(create_err) => return Err(WorkerError::Io(create_err)),
                            }
                        }
                        Err(err) => return Err(WorkerError::Io(err)),
                    }
                }
            }
        }

        let meta = fs::symlink_metadata(&checked)?;
        if meta.file_type().is_symlink() || !meta.file_type().is_dir() {
            return Err(WorkerError::SecurityViolation(format!(
                "work_area_root `{}` must be a non-symlink directory",
                checked.display()
            )));
        }
        let mode = meta.permissions().mode() & 0o777;
        if mode & 0o077 != 0 {
            return Err(WorkerError::SecurityViolation(format!(
                "existing work_area_root `{}` has non-private permissions {:04o} (expected 0700); refusing to chmod existing directory",
                checked.display(),
                mode
            )));
        }
    }

    Ok(checked)
}

fn open_work_area_lock_file(lock_path: &Path) -> Result<File, WorkerError> {
    let _ = validate_absolute_no_symlink_components(lock_path, "work area ownership lock")?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};

        match fs::symlink_metadata(lock_path) {
            Ok(meta) => {
                if meta.file_type().is_symlink() {
                    return Err(WorkerError::SecurityViolation(format!(
                        "work area ownership lock `{}` must not be a symlink",
                        lock_path.display()
                    )));
                }
                if !meta.file_type().is_file() {
                    return Err(WorkerError::SecurityViolation(format!(
                        "work area ownership lock `{}` must be a regular file",
                        lock_path.display()
                    )));
                }
                let mode = meta.permissions().mode() & 0o777;
                if mode & 0o077 != 0 {
                    return Err(WorkerError::SecurityViolation(format!(
                        "work area ownership lock `{}` has insecure permissions {:04o} (expected 0600)",
                        lock_path.display(),
                        mode
                    )));
                }
                if meta.len() != 0 {
                    return Err(WorkerError::SecurityViolation(format!(
                        "work area ownership lock `{}` must be an empty 0-byte lock file (found {} bytes)",
                        lock_path.display(),
                        meta.len()
                    )));
                }
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => return Err(WorkerError::Io(err)),
        }

        let file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW)
            .open(lock_path)
            .map_err(|err| {
                if err.raw_os_error() == Some(libc::ELOOP) {
                    WorkerError::SecurityViolation(format!(
                        "work area ownership lock `{}` must not be a symlink",
                        lock_path.display()
                    ))
                } else {
                    WorkerError::Io(err)
                }
            })?;

        let sym_meta = fs::symlink_metadata(lock_path)?;
        let file_meta = file.metadata()?;
        if sym_meta.file_type().is_symlink()
            || !sym_meta.file_type().is_file()
            || !file_meta.file_type().is_file()
        {
            return Err(WorkerError::SecurityViolation(format!(
                "work area ownership lock `{}` must be a regular non-symlink file",
                lock_path.display()
            )));
        }
        let mode = file_meta.permissions().mode() & 0o777;
        if mode & 0o077 != 0 {
            return Err(WorkerError::SecurityViolation(format!(
                "work area ownership lock `{}` has insecure permissions {:04o} (expected 0600)",
                lock_path.display(),
                mode
            )));
        }
        if file_meta.len() != 0 {
            return Err(WorkerError::SecurityViolation(format!(
                "work area ownership lock `{}` must be an empty 0-byte lock file (found {} bytes)",
                lock_path.display(),
                file_meta.len()
            )));
        }
        file.set_permissions(fs::Permissions::from_mode(0o600))?;
        Ok(file)
    }

    #[cfg(not(unix))]
    {
        let _ = lock_path;
        ensure_platform_supported()?;
        unreachable!()
    }
}

fn verify_existing_work_area_marker(
    canonical_work_area: &Path,
) -> Result<WorkAreaOwnershipMarker, WorkerError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let meta = fs::symlink_metadata(canonical_work_area)?;
        let mode = meta.permissions().mode() & 0o777;
        if mode & 0o077 != 0 {
            return Err(WorkerError::SecurityViolation(format!(
                "work_area_root `{}` has non-private permissions {:04o} (expected 0700)",
                canonical_work_area.display(),
                mode
            )));
        }
    }

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
    Ok(marker)
}

fn resolve_repository_common_git_dir(repo_or_worktree: &Path) -> Result<PathBuf, WorkerError> {
    let raw = run_git(repo_or_worktree, &["rev-parse", "--git-common-dir"])?;
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
        repo_or_worktree.join(trimmed)
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

fn generate_short_token() -> String {
    let simple = Uuid::new_v4().simple().to_string();
    simple[..12].to_string()
}

fn resolve_commit_sha(repo_or_worktree: &Path, rev: &str) -> Result<String, WorkerError> {
    resolve_commit_sha_with_timeout(repo_or_worktree, rev, DEFAULT_GIT_COMMAND_TIMEOUT_MS)
}

fn resolve_commit_sha_with_timeout(
    repo_or_worktree: &Path,
    rev: &str,
    timeout_ms: u64,
) -> Result<String, WorkerError> {
    let rev_arg = format!("{rev}^{{commit}}");
    let out = run_git_with_timeout(
        repo_or_worktree,
        &["rev-parse", "--verify", &rev_arg],
        timeout_ms,
    )?;
    let sha = out.trim();
    if sha.len() != 40 || !sha.chars().all(|c| c.is_ascii_hexdigit()) {
        return Err(WorkerError::GitCommandFailed {
            command: format!("rev-parse --verify {rev_arg}"),
            status: Some(0),
            stderr: format!("expected 40-hex commit SHA, got `{sha}`"),
        });
    }
    Ok(sha.to_ascii_lowercase())
}

fn branch_exists(
    repo_path: &Path,
    branch_name: &str,
    timeout_ms: u64,
) -> Result<bool, WorkerError> {
    let ref_name = format!("refs/heads/{branch_name}");
    let out = run_git_bytes_bounded_raw(
        repo_path,
        &["show-ref", "--verify", "--quiet", &ref_name],
        MAX_GIT_STDERR_BYTES,
        timeout_ms,
    )?;
    if let Some(reason) = out.failure_reason(MAX_GIT_STDERR_BYTES, timeout_ms) {
        return Err(WorkerError::GitCommandFailed {
            command: format!("git show-ref --verify --quiet {ref_name}"),
            status: out.exit_code,
            stderr: reason,
        });
    }
    Ok(out.exit_code == Some(0))
}

#[derive(Debug)]
struct GitBoundedOutput {
    bytes: Vec<u8>,
    stderr: String,
    exit_code: Option<i32>,
    stdout_overflowed: bool,
    stderr_overflowed: bool,
    timed_out: bool,
    incomplete_cleanup: bool,
    stdout_invalid_utf8: bool,
}

impl GitBoundedOutput {
    fn failure_overflow_or_timeout_reason(
        &self,
        max_stdout_bytes: usize,
        timeout_ms: u64,
    ) -> Option<String> {
        if self.stdout_overflowed {
            Some(format!(
                "git command stdout exceeded bounded limit ({max_stdout_bytes} bytes)"
            ))
        } else if self.stderr_overflowed {
            Some(format!(
                "git command stderr exceeded bounded limit ({MAX_GIT_STDERR_BYTES} bytes)"
            ))
        } else if self.timed_out {
            Some(format!(
                "git command timed out after {timeout_ms} ms; owned git process group terminated"
            ))
        } else if self.incomplete_cleanup {
            Some(
                "git command had incomplete cleanup (a descendant or hook held stdout/stderr pipes open after exit or escaped the process group)"
                    .to_string(),
            )
        } else {
            None
        }
    }

    fn failure_reason(&self, max_stdout_bytes: usize, timeout_ms: u64) -> Option<String> {
        if let Some(reason) = self.failure_overflow_or_timeout_reason(max_stdout_bytes, timeout_ms)
        {
            Some(reason)
        } else if self.stdout_invalid_utf8 {
            Some(
                "git command stdout contained invalid UTF-8 path bytes (non-UTF-8 repository paths are unsupported)"
                    .to_string(),
            )
        } else {
            None
        }
    }

    fn violation_kind(&self) -> DiffViolationKind {
        if self.stdout_invalid_utf8 {
            DiffViolationKind::PathEscape
        } else {
            DiffViolationKind::DiffBudgetExceeded
        }
    }
}

fn run_git(dir: &Path, args: &[&str]) -> Result<String, WorkerError> {
    run_git_with_timeout(dir, args, DEFAULT_GIT_COMMAND_TIMEOUT_MS)
}

fn run_git_with_timeout(dir: &Path, args: &[&str], timeout_ms: u64) -> Result<String, WorkerError> {
    let out = run_git_bytes_bounded_with_timeout(dir, args, MAX_GIT_STDERR_BYTES, timeout_ms)?;
    if let Some(reason) = out.failure_reason(MAX_GIT_STDERR_BYTES, timeout_ms) {
        return Err(WorkerError::GitCommandFailed {
            command: format!("git {}", args.join(" ")),
            status: out.exit_code,
            stderr: reason,
        });
    }
    String::from_utf8(out.bytes).map_err(|_| WorkerError::GitCommandFailed {
        command: format!("git {}", args.join(" ")),
        status: out.exit_code,
        stderr: "git command stdout contained invalid UTF-8 bytes".to_string(),
    })
}

fn run_git_bytes_bounded_with_timeout(
    dir: &Path,
    args: &[&str],
    max_stdout_bytes: usize,
    timeout_ms: u64,
) -> Result<GitBoundedOutput, WorkerError> {
    let out = run_git_bytes_bounded_raw(dir, args, max_stdout_bytes, timeout_ms)?;
    if out.failure_reason(max_stdout_bytes, timeout_ms).is_none() && out.exit_code != Some(0) {
        return Err(WorkerError::GitCommandFailed {
            command: format!("git {}", args.join(" ")),
            status: out.exit_code,
            stderr: out.stderr,
        });
    }
    Ok(out)
}

fn run_git_bytes_bounded_raw(
    dir: &Path,
    args: &[&str],
    max_stdout_bytes: usize,
    timeout_ms: u64,
) -> Result<GitBoundedOutput, WorkerError> {
    ensure_platform_supported()?;

    #[cfg(unix)]
    {
        use std::os::fd::AsRawFd;
        use std::os::unix::process::CommandExt;

        let mut cmd = Command::new("git");
        sanitize_git_command_env(&mut cmd);
        cmd.arg("-C")
            .arg(dir)
            .args(args)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .process_group(0);

        let mut child = cmd.spawn()?;
        let child_pid = child.id();
        let pgid = i32::try_from(child_pid).map_err(|_| {
            WorkerError::Io(std::io::Error::other(format!(
                "git PID {child_pid} exceeds i32 range"
            )))
        })?;
        if pgid <= 1 || pgid == unix_pgroup::caller_pgid() {
            let _ = child.kill();
            let _ = child.wait();
            return Err(WorkerError::SecurityViolation(format!(
                "spawned git child did not enter an isolated process group (pgid={pgid})"
            )));
        }

        let mut pgroup_guard = ProcessGroupGuard::new(pgid);
        let start_instant = Instant::now();
        let deadline = start_instant + Duration::from_millis(timeout_ms.max(1));
        let post_exit_drain_window = Duration::from_millis(POST_EXIT_PIPE_DRAIN_TIMEOUT_MS);

        let mut stdout_bytes = Vec::new();
        let mut stderr_bytes = Vec::new();
        let mut stdout_open = true;
        let mut stderr_open = true;
        let mut stdout_overflowed = false;
        let mut stderr_overflowed = false;
        let mut timed_out = false;
        let mut incomplete_cleanup = false;
        let mut exit_status: Option<std::process::ExitStatus> = None;
        let mut child_exited_at: Option<Instant> = None;

        if let (Some(mut stdout_pipe), Some(mut stderr_pipe)) =
            (child.stdout.take(), child.stderr.take())
        {
            let stdout_fd = stdout_pipe.as_raw_fd();
            let stderr_fd = stderr_pipe.as_raw_fd();
            unix_pgroup::set_fd_nonblocking(stdout_fd)?;
            unix_pgroup::set_fd_nonblocking(stderr_fd)?;

            loop {
                drain_nonblocking_pipe(
                    &mut stdout_pipe,
                    &mut stdout_bytes,
                    max_stdout_bytes,
                    &mut stdout_open,
                    &mut stdout_overflowed,
                )?;
                drain_nonblocking_pipe(
                    &mut stderr_pipe,
                    &mut stderr_bytes,
                    MAX_GIT_STDERR_BYTES,
                    &mut stderr_open,
                    &mut stderr_overflowed,
                )?;

                if stdout_overflowed || stderr_overflowed {
                    break;
                }

                if exit_status.is_none() {
                    if let Some(status) = child.try_wait()? {
                        exit_status = Some(status);
                        child_exited_at = Some(Instant::now());
                        drain_nonblocking_pipe(
                            &mut stdout_pipe,
                            &mut stdout_bytes,
                            max_stdout_bytes,
                            &mut stdout_open,
                            &mut stdout_overflowed,
                        )?;
                        drain_nonblocking_pipe(
                            &mut stderr_pipe,
                            &mut stderr_bytes,
                            MAX_GIT_STDERR_BYTES,
                            &mut stderr_open,
                            &mut stderr_overflowed,
                        )?;
                        if stdout_overflowed || stderr_overflowed {
                            break;
                        }
                    }
                }

                if !stdout_open && !stderr_open && exit_status.is_some() {
                    break;
                }

                let now = Instant::now();
                if now >= deadline {
                    timed_out = true;
                    break;
                }

                if let Some(exited_at) = child_exited_at {
                    if now.saturating_duration_since(exited_at) >= post_exit_drain_window {
                        incomplete_cleanup = true;
                        break;
                    }
                }

                let mut wait_dur = deadline.saturating_duration_since(now);
                if let Some(exited_at) = child_exited_at {
                    let rem_drain = post_exit_drain_window
                        .saturating_sub(now.saturating_duration_since(exited_at));
                    wait_dur = wait_dur.min(rem_drain);
                }
                // Both pipes can reach EOF before the child's exit is observable: the final
                // read may return 0 while `try_wait` still reports the child as running for
                // a scheduling instant. With no live descriptors `poll([-1, -1], n)` is just
                // a sleep, so the 20 ms active-pipe cap would add up to 20 ms to every short
                // Git command after EOF. Recheck the no-pipe child at a short bounded interval
                // instead; this remains a sleep (not a busy spin), and the overall deadline and
                // post-exit drain window above still bound the loop. Live pipes keep the
                // event-driven 20 ms poll.
                let poll_cap = if stdout_open || stderr_open {
                    Duration::from_millis(20)
                } else {
                    Duration::from_millis(1)
                };
                wait_dur = wait_dur.min(poll_cap);
                let poll_ms = i32::try_from(wait_dur.as_millis()).unwrap_or(20).max(1);

                unix_pgroup::poll_pipe_fds(
                    stdout_open.then_some(stdout_fd),
                    stderr_open.then_some(stderr_fd),
                    poll_ms,
                )?;
            }
            // Drop stdout_pipe and stderr_pipe here before group termination/reaping so we
            // never block waiting for pipe EOF if a descendant or hook holds the write end open.
        } else {
            return Err(WorkerError::Io(std::io::Error::other(
                "failed to capture git stdout/stderr pipes",
            )));
        }

        let (final_status, reap_incomplete) = terminate_and_reap_git_group(
            pgid,
            &mut child,
            exit_status,
            stdout_overflowed || stderr_overflowed || timed_out || incomplete_cleanup,
        );
        if reap_incomplete {
            incomplete_cleanup = true;
        } else {
            pgroup_guard.disarm();
        }

        let stdout_invalid_utf8 = !stdout_overflowed && std::str::from_utf8(&stdout_bytes).is_err();
        let stderr_str = String::from_utf8_lossy(&stderr_bytes);
        Ok(GitBoundedOutput {
            bytes: stdout_bytes,
            stderr: sanitize_and_redact(&stderr_str, 512),
            exit_code: final_status.and_then(|s| s.code()),
            stdout_overflowed,
            stderr_overflowed,
            timed_out,
            incomplete_cleanup,
            stdout_invalid_utf8,
        })
    }

    #[cfg(not(unix))]
    {
        let _ = (dir, args, max_stdout_bytes, timeout_ms);
        Err(WorkerError::UnsupportedPlatform(
            std::env::consts::OS.to_string(),
        ))
    }
}

#[cfg(unix)]
fn drain_nonblocking_pipe<R: Read>(
    pipe: &mut R,
    dest: &mut Vec<u8>,
    max_bytes: usize,
    pipe_open: &mut bool,
    overflowed: &mut bool,
) -> std::io::Result<()> {
    let mut chunk = [0u8; 8192];
    while *pipe_open && !*overflowed {
        match pipe.read(&mut chunk) {
            Ok(0) => {
                *pipe_open = false;
                break;
            }
            Ok(n) => {
                let remaining = max_bytes.saturating_sub(dest.len());
                if n > remaining {
                    dest.extend_from_slice(&chunk[..remaining]);
                    *overflowed = true;
                    break;
                }
                dest.extend_from_slice(&chunk[..n]);
            }
            Err(err) if err.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => break,
            Err(err) => return Err(err),
        }
    }
    Ok(())
}

#[cfg(unix)]
fn terminate_and_reap_git_group(
    pgid: i32,
    child: &mut std::process::Child,
    mut exit_status: Option<std::process::ExitStatus>,
    force_terminate: bool,
) -> (Option<std::process::ExitStatus>, bool) {
    if force_terminate || exit_status.is_none() || unix_pgroup::is_process_group_alive(pgid) {
        let _ = unix_pgroup::signal_process_group(pgid, libc::SIGTERM);
        let term_deadline = Instant::now() + Duration::from_millis(100);
        while Instant::now() < term_deadline {
            if exit_status.is_none() {
                if let Ok(Some(status)) = child.try_wait() {
                    exit_status = Some(status);
                }
            }
            if exit_status.is_some() && !unix_pgroup::is_process_group_alive(pgid) {
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }

        if exit_status.is_none() || unix_pgroup::is_process_group_alive(pgid) {
            let _ = unix_pgroup::signal_process_group(pgid, libc::SIGKILL);
            let _ = child.kill();
            let kill_deadline = Instant::now() + Duration::from_millis(150);
            while Instant::now() < kill_deadline {
                if exit_status.is_none() {
                    if let Ok(Some(status)) = child.try_wait() {
                        exit_status = Some(status);
                    }
                }
                if exit_status.is_some() && !unix_pgroup::is_process_group_alive(pgid) {
                    break;
                }
                std::thread::sleep(Duration::from_millis(10));
            }
        }
    }

    if exit_status.is_none() {
        if let Ok(Some(status)) = child.try_wait() {
            exit_status = Some(status);
        }
    }

    let group_still_alive = unix_pgroup::is_process_group_alive(pgid);
    let reap_incomplete = exit_status.is_none() || group_still_alive;
    (exit_status, reap_incomplete)
}

fn parse_ls_tree_baseline_z(
    text: &str,
    baseline_symlinks: &mut BTreeSet<String>,
    baseline_path_kinds: &mut BTreeMap<String, BaseCommitPathKind>,
) -> Result<(), String> {
    for record in text.split('\0') {
        if record.is_empty() {
            continue;
        }
        let Some((meta_part, path_part)) = record.split_once('\t') else {
            return Err(format!("malformed git ls-tree record `{record}`"));
        };
        if path_part.is_empty() {
            return Err("git ls-tree record has empty path".to_string());
        }

        let mut ancestor = Path::new(path_part).parent();
        while let Some(parent) = ancestor {
            let parent_str = parent.to_string_lossy();
            if parent_str.is_empty() {
                break;
            }
            baseline_path_kinds
                .entry(parent_str.to_string())
                .or_insert(BaseCommitPathKind::Directory);
            ancestor = parent.parent();
        }

        let kind = if meta_part.starts_with("120000 ") {
            baseline_symlinks.insert(path_part.to_string());
            BaseCommitPathKind::Symlink
        } else if meta_part.starts_with("160000 ") {
            BaseCommitPathKind::Gitlink
        } else if meta_part.starts_with("040000 tree ") {
            BaseCommitPathKind::Directory
        } else if meta_part.starts_with("100644 ") || meta_part.starts_with("100755 ") {
            BaseCommitPathKind::RegularFile
        } else {
            return Err(format!(
                "unrecognized git ls-tree mode/type `{meta_part}` for `{path_part}`"
            ));
        };
        baseline_path_kinds.insert(path_part.to_string(), kind);
    }
    Ok(())
}

fn parse_exact_ls_tree_entry_z(
    text: &str,
    expected_path: &str,
) -> Result<Option<BaseCommitPathKind>, String> {
    for record in text.split('\0') {
        if record.is_empty() {
            continue;
        }
        let Some((meta_part, path_part)) = record.split_once('\t') else {
            return Err(format!("malformed git ls-tree record `{record}`"));
        };
        if path_part != expected_path {
            continue;
        }
        if meta_part.starts_with("040000 tree ") {
            return Ok(Some(BaseCommitPathKind::Directory));
        } else if meta_part.starts_with("120000 ") {
            return Ok(Some(BaseCommitPathKind::Symlink));
        } else if meta_part.starts_with("160000 ") {
            return Ok(Some(BaseCommitPathKind::Gitlink));
        } else if meta_part.starts_with("100644 ") || meta_part.starts_with("100755 ") {
            return Ok(Some(BaseCommitPathKind::RegularFile));
        } else {
            return Err(format!(
                "unrecognized git ls-tree mode/type `{meta_part}` for `{expected_path}`"
            ));
        }
    }
    Ok(None)
}

fn parse_numstat_z(text: &str, binary_paths: &mut BTreeSet<String>) {
    let mut tokens = text.split('\0').peekable();
    while let Some(token) = tokens.next() {
        if token.is_empty() {
            continue;
        }
        let mut parts = token.splitn(3, '\t');
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
            // Rename or copy: next two NUL-delimited tokens are orig_path and new_path
            let orig = tokens.next().map(str::to_string);
            let new_p = tokens.next().map(str::to_string);
            if is_bin {
                if let Some(o) = orig {
                    binary_paths.insert(o);
                }
                if let Some(n) = new_p {
                    binary_paths.insert(n);
                }
            }
        } else if is_bin {
            binary_paths.insert(path_part.to_string());
        }
    }
}

fn parse_name_status_z(
    text: &str,
    checkout_root: &Path,
    binary_paths: &BTreeSet<String>,
    entries_map: &mut BTreeMap<String, ChangedPathEntry>,
) {
    let mut tokens = text.split('\0');
    while let Some(status_tok) = tokens.next() {
        if status_tok.is_empty() {
            continue;
        }
        let status_char = status_tok.chars().next().unwrap_or('M');

        if status_char == 'R' || status_char == 'C' {
            let Some(orig_str) = tokens.next() else {
                break;
            };
            let Some(new_str) = tokens.next() else {
                break;
            };
            let orig_path = orig_str.to_string();
            let new_path = new_str.to_string();
            let change_kind = if status_char == 'R' {
                DiffChangeKind::Renamed
            } else {
                DiffChangeKind::Copied
            };
            let marked_binary =
                binary_paths.contains(&new_path) || binary_paths.contains(&orig_path);
            let (is_binary, is_symlink) =
                inspect_worktree_entry(checkout_root, &new_path, marked_binary);
            entries_map.insert(
                new_path.clone(),
                ChangedPathEntry {
                    path: new_path,
                    orig_path: Some(orig_path),
                    change_kind,
                    is_binary,
                    is_symlink,
                },
            );
        } else {
            let Some(path_str) = tokens.next() else {
                break;
            };
            let path = path_str.to_string();
            let change_kind = match status_char {
                'A' => DiffChangeKind::Added,
                'D' => DiffChangeKind::Deleted,
                'T' => DiffChangeKind::TypeChanged,
                _ => DiffChangeKind::Modified,
            };
            let marked_binary = binary_paths.contains(&path);
            let (is_binary, is_symlink) =
                inspect_worktree_entry(checkout_root, &path, marked_binary);
            entries_map.insert(
                path.clone(),
                ChangedPathEntry {
                    path,
                    orig_path: None,
                    change_kind,
                    is_binary,
                    is_symlink,
                },
            );
        }
    }
}

fn inspect_worktree_entry(
    checkout_root: &Path,
    rel_path: &str,
    git_marked_binary: bool,
) -> (bool, bool) {
    let full_path = checkout_root.join(rel_path);
    let Ok(meta) = fs::symlink_metadata(&full_path) else {
        return (git_marked_binary, false);
    };
    if meta.file_type().is_symlink() {
        return (git_marked_binary, true);
    }
    if git_marked_binary {
        return (true, false);
    }
    if !meta.file_type().is_file() {
        return (false, false);
    }
    let Ok(mut file) = File::open(&full_path) else {
        return (false, false);
    };
    let mut probe = [0u8; 8192];
    let Ok(n) = file.read(&mut probe) else {
        return (false, false);
    };
    (probe[..n].contains(&0), false)
}

fn has_symlink_component_in_checkout(checkout_root: &Path, rel_path: &str) -> bool {
    let mut current = checkout_root.to_path_buf();
    for comp in Path::new(rel_path).components() {
        current.push(comp.as_os_str());
        if fs::symlink_metadata(&current).is_ok_and(|meta| meta.file_type().is_symlink()) {
            return true;
        }
    }
    false
}

struct CheckoutScanState {
    max_entries: usize,
    max_depth: usize,
    max_symlinks: usize,
    visited_entries: usize,
    symlinks: Vec<String>,
    entries_overflowed: bool,
    depth_overflowed: bool,
    invalid_utf8_path: bool,
}

fn scan_symlinks_in_checkout(
    checkout_root: &Path,
    current_dir: &Path,
    current_depth: usize,
    state: &mut CheckoutScanState,
) -> Result<(), WorkerError> {
    if current_depth > state.max_depth {
        state.depth_overflowed = true;
        return Ok(());
    }

    let entries = fs::read_dir(current_dir)?;

    for entry_res in entries {
        let entry = entry_res?;
        if current_dir == checkout_root && entry.file_name() == ".git" {
            continue;
        }

        state.visited_entries = state.visited_entries.saturating_add(1);
        if state.visited_entries > state.max_entries {
            state.entries_overflowed = true;
            return Ok(());
        }

        if entry.file_name().to_str().is_none() {
            state.invalid_utf8_path = true;
            return Ok(());
        }

        let path = entry.path();
        let meta = fs::symlink_metadata(&path)?;
        if meta.file_type().is_symlink() {
            let Ok(rel) = path.strip_prefix(checkout_root) else {
                continue;
            };
            let Some(rel_str) = rel.to_str() else {
                state.invalid_utf8_path = true;
                return Ok(());
            };
            state.symlinks.push(rel_str.to_string());
            if state.symlinks.len() >= state.max_symlinks {
                state.entries_overflowed = true;
                return Ok(());
            }
        } else if meta.file_type().is_dir() {
            if current_depth + 1 > state.max_depth {
                state.depth_overflowed = true;
                return Ok(());
            }
            scan_symlinks_in_checkout(checkout_root, &path, current_depth + 1, state)?;
            if state.entries_overflowed || state.depth_overflowed || state.invalid_utf8_path {
                return Ok(());
            }
        }
    }
    Ok(())
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::ffi::OsStr;
    use std::os::unix::ffi::OsStrExt;
    use std::os::unix::fs::{symlink, MetadataExt, PermissionsExt};
    use tempfile::TempDir;

    fn init_repo_and_manager() -> (TempDir, PathBuf, WorktreeManager) {
        let tmp = TempDir::new().expect("tempdir");
        let root = fs::canonicalize(tmp.path()).expect("canonicalize tempdir");
        let repo = root.join("repo");
        let work_area = root.join("work-area");
        fs::create_dir_all(repo.join("src")).expect("create repo src");

        run_sanitized_git(&repo, &["init", "--quiet"], DEFAULT_GIT_COMMAND_TIMEOUT_MS)
            .expect("git init");
        run_sanitized_git(
            &repo,
            &["config", "user.name", "Threadlane Test"],
            DEFAULT_GIT_COMMAND_TIMEOUT_MS,
        )
        .expect("git config user.name");
        run_sanitized_git(
            &repo,
            &["config", "user.email", "test@threadlane.local"],
            DEFAULT_GIT_COMMAND_TIMEOUT_MS,
        )
        .expect("git config user.email");

        fs::write(repo.join("src/lib.rs"), "pub fn v() -> u32 { 1 }\n").expect("write base file");
        run_sanitized_git(&repo, &["add", "."], DEFAULT_GIT_COMMAND_TIMEOUT_MS).expect("git add");
        run_sanitized_git(
            &repo,
            &["commit", "-m", "initial", "--quiet"],
            DEFAULT_GIT_COMMAND_TIMEOUT_MS,
        )
        .expect("git commit");

        let base_sha = resolve_exact_commit_sha(&repo, "HEAD", DEFAULT_GIT_COMMAND_TIMEOUT_MS)
            .expect("resolve HEAD");
        let manager =
            WorktreeManager::new(&repo, &work_area, &base_sha).expect("create WorktreeManager");
        (tmp, repo, manager)
    }

    fn resolve_worktree_index_path(checkout_path: &Path) -> PathBuf {
        let raw = run_sanitized_git(
            checkout_path,
            &["rev-parse", "--git-path", "index"],
            DEFAULT_GIT_COMMAND_TIMEOUT_MS,
        )
        .expect("resolve git index path");
        let trimmed = raw.trim();
        let path = Path::new(trimmed);
        if path.is_absolute() {
            path.to_path_buf()
        } else {
            checkout_path.join(path)
        }
    }

    #[allow(unsafe_code)]
    fn create_fifo(path: &Path) {
        let mut bytes = path.as_os_str().as_bytes().to_vec();
        bytes.push(0);
        // SAFETY: `bytes` is a NUL-terminated path buffer in an owned temporary directory.
        let rc = unsafe { libc::mkfifo(bytes.as_ptr().cast::<libc::c_char>(), 0o600) };
        assert_eq!(rc, 0, "mkfifo failed: {}", std::io::Error::last_os_error());
    }

    #[test]
    fn sanitize_git_command_env_sets_optional_locks_zero_and_preserves_index_across_status() {
        assert!(
            SANITIZED_GIT_ENV_OVERRIDES.contains(&("GIT_OPTIONAL_LOCKS", "0")),
            "SANITIZED_GIT_ENV_OVERRIDES must include GIT_OPTIONAL_LOCKS=0"
        );

        let mut cmd = Command::new("git");
        cmd.env("GIT_DIR", "/tmp/poison-git-dir");
        cmd.env("git_index_file", "/tmp/poison-index");
        cmd.env("GIT_OPTIONAL_LOCKS", "1");
        cmd.env("KEEP_NON_GIT", "ok");
        sanitize_git_command_env_from(
            &mut cmd,
            vec![
                (OsStr::new("GIT_DIR"), OsStr::new("/tmp/poison-git-dir")),
                (
                    OsStr::new("git_index_file"),
                    OsStr::new("/tmp/poison-index"),
                ),
                (OsStr::new("GIT_OPTIONAL_LOCKS"), OsStr::new("1")),
                (OsStr::new("KEEP_NON_GIT"), OsStr::new("ok")),
            ],
        );

        let envs: BTreeMap<String, Option<String>> = cmd
            .get_envs()
            .map(|(k, v)| {
                (
                    k.to_string_lossy().to_string(),
                    v.map(|val| val.to_string_lossy().to_string()),
                )
            })
            .collect();
        assert_eq!(envs.get("GIT_DIR"), Some(&None));
        assert_eq!(envs.get("git_index_file"), Some(&None));
        assert_eq!(envs.get("GIT_OPTIONAL_LOCKS"), Some(&Some("0".to_string())));
        assert_eq!(envs.get("KEEP_NON_GIT"), Some(&Some("ok".to_string())));

        let (_tmp, _repo, manager) = init_repo_and_manager();
        let allowed = BTreeSet::from(["src/lib.rs".to_string()]);
        let task = manager
            .create_task_worktree_with_token("run-1", "task-1", "tok01", &allowed)
            .expect("create task worktree");

        // Mandatory write operations (`git add`, `git commit`) must continue to work normally.
        let lib_file = task.checkout_path.join("src/lib.rs");
        let updated_content = "pub fn v() -> u32 { 42 }\n";
        fs::write(&lib_file, updated_content).expect("write updated file");
        run_sanitized_git(
            &task.checkout_path,
            &["add", "-A", "--", "."],
            DEFAULT_GIT_COMMAND_TIMEOUT_MS,
        )
        .expect("git add");
        run_sanitized_git(
            &task.checkout_path,
            &[
                "-c",
                "user.name=Threadlane Test",
                "-c",
                "user.email=test@threadlane.local",
                "-c",
                "commit.gpgsign=false",
                "commit",
                "-q",
                "-m",
                "task commit",
            ],
            DEFAULT_GIT_COMMAND_TIMEOUT_MS,
        )
        .expect("git commit");

        // Rewrite the exact same committed bytes to dirty the working tree stat cache relative
        // to the index without changing file content.
        std::thread::sleep(Duration::from_millis(20));
        fs::write(&lib_file, updated_content).expect("rewrite identical content");

        let index_path = resolve_worktree_index_path(&task.checkout_path);
        let index_bytes_before = fs::read(&index_path).expect("read index before");
        let index_meta_before = fs::symlink_metadata(&index_path).expect("stat index before");

        assert!(
            is_worktree_clean(&task.checkout_path, DEFAULT_GIT_COMMAND_TIMEOUT_MS)
                .expect("is_worktree_clean")
        );
        let diff_report =
            evaluate_diff_gate(manager.work_area_root(), &task.worktree_id).expect("diff gate");
        assert!(diff_report.passed);

        let index_bytes_after = fs::read(&index_path).expect("read index after");
        let index_meta_after = fs::symlink_metadata(&index_path).expect("stat index after");
        assert_eq!(index_bytes_after, index_bytes_before);
        assert_eq!(
            index_meta_after.modified().expect("mtime after"),
            index_meta_before.modified().expect("mtime before")
        );
        assert_eq!(index_meta_after.ino(), index_meta_before.ino());
    }

    #[test]
    fn open_existing_private_lock_file_and_readonly_inactive_check_enforce_security_and_keep_missing_lock_absent(
    ) {
        let (_tmp, _repo, manager) = init_repo_and_manager();
        let allowed = BTreeSet::from(["src/lib.rs".to_string()]);
        let task = manager
            .create_task_worktree_with_token("run-1", "task-1", "tok02", &allowed)
            .expect("create task worktree");
        let manifests_dir = manager.work_area_root().join(MANIFESTS_SUBDIR);
        let lock_path = manifests_dir.join(format!("{}.lock", task.worktree_id));

        // 1. Missing lock stays missing and passes readonly inactive check when manifest guards pass.
        assert!(fs::symlink_metadata(&lock_path).is_err());
        assert!(
            open_existing_private_lock_file(&lock_path, "worktree job lock")
                .expect("open missing lock")
                .is_none()
        );
        assert!(fs::symlink_metadata(&lock_path).is_err());
        ensure_worktree_job_inactive_readonly(&task).expect("inactive when lock missing");
        assert!(fs::symlink_metadata(&lock_path).is_err());

        // 2. Missing lock still refuses when manifest job_active == true or active_job_pid is alive.
        let mut active_flag_manifest = task.clone();
        active_flag_manifest.job_active = true;
        let err = ensure_worktree_job_inactive_readonly(&active_flag_manifest)
            .expect_err("must refuse job_active == true");
        assert!(matches!(
            err,
            WorkerError::CleanupRefused {
                reason: CleanupRefusalReason::ActiveJob,
                ..
            }
        ));
        assert!(fs::symlink_metadata(&lock_path).is_err());

        let mut live_pid_manifest = task.clone();
        live_pid_manifest.active_job_pid = Some(std::process::id());
        let err = ensure_worktree_job_inactive_readonly(&live_pid_manifest)
            .expect_err("must refuse live active_job_pid");
        assert!(matches!(
            err,
            WorkerError::CleanupRefused {
                reason: CleanupRefusalReason::ActiveJob,
                ..
            }
        ));
        assert!(fs::symlink_metadata(&lock_path).is_err());

        // 3. Held job lock via begin_worktree_job or external fs2 lock refuses without killing processes.
        let guard =
            begin_worktree_job(manager.work_area_root(), &task.worktree_id).expect("begin job");
        let err =
            ensure_worktree_job_inactive_readonly(&task).expect_err("must refuse held job lock");
        assert!(matches!(
            err,
            WorkerError::CleanupRefused {
                reason: CleanupRefusalReason::ActiveJob,
                ..
            }
        ));
        let finished_manifest = guard.finish().expect("finish job");
        ensure_worktree_job_inactive_readonly(&finished_manifest)
            .expect("inactive after guard finish");

        let external_lock = open_private_lock_file(&lock_path, "worktree job lock")
            .expect("open existing lock for test");
        fs2::FileExt::try_lock_exclusive(&external_lock).expect("lock exclusive");
        let err = ensure_worktree_job_inactive_readonly(&finished_manifest)
            .expect_err("must refuse externally held lock");
        assert!(matches!(
            err,
            WorkerError::CleanupRefused {
                reason: CleanupRefusalReason::ActiveJob,
                ..
            }
        ));
        fs2::FileExt::unlock(&external_lock).expect("unlock external");
        drop(external_lock);

        // 4. Deleting inactive lock leaves it absent across readonly checks.
        fs::remove_file(&lock_path).expect("remove lock");
        ensure_worktree_job_inactive_readonly(&finished_manifest)
            .expect("inactive after deleting lock");
        assert!(fs::symlink_metadata(&lock_path).is_err());

        // 5. Insecure lock permissions (0644 and 0700) are refused without chmodding.
        let _ = open_private_lock_file(&lock_path, "worktree job lock").expect("create lock");
        fs::set_permissions(&lock_path, fs::Permissions::from_mode(0o644)).expect("chmod 0644");
        assert!(matches!(
            open_existing_private_lock_file(&lock_path, "worktree job lock"),
            Err(WorkerError::SecurityViolation(_))
        ));
        assert!(matches!(
            ensure_worktree_job_inactive_readonly(&finished_manifest),
            Err(WorkerError::SecurityViolation(_))
        ));
        let mode_after = fs::symlink_metadata(&lock_path)
            .expect("stat 0644 lock")
            .permissions()
            .mode()
            & 0o7777;
        assert_eq!(mode_after, 0o644);

        fs::set_permissions(&lock_path, fs::Permissions::from_mode(0o700)).expect("chmod 0700");
        assert!(matches!(
            ensure_worktree_job_inactive_readonly(&finished_manifest),
            Err(WorkerError::SecurityViolation(_))
        ));
        fs::remove_file(&lock_path).expect("remove insecure lock");

        // 6. Symlink lock is refused.
        let symlink_target = manifests_dir.join("target.lock");
        let _ = open_private_lock_file(&symlink_target, "target lock").expect("create target");
        symlink(&symlink_target, &lock_path).expect("create symlink lock");
        assert!(matches!(
            open_existing_private_lock_file(&lock_path, "worktree job lock"),
            Err(WorkerError::SecurityViolation(_))
        ));
        assert!(matches!(
            ensure_worktree_job_inactive_readonly(&finished_manifest),
            Err(WorkerError::SecurityViolation(_))
        ));
        fs::remove_file(&lock_path).expect("remove symlink lock");
        fs::remove_file(&symlink_target).expect("remove target lock");

        // 7. Non-regular files (directory and FIFO) are refused without blocking.
        fs::create_dir(&lock_path).expect("create dir at lock path");
        assert!(matches!(
            open_existing_private_lock_file(&lock_path, "worktree job lock"),
            Err(WorkerError::SecurityViolation(_))
        ));
        fs::remove_dir(&lock_path).expect("remove dir at lock path");

        create_fifo(&lock_path);
        assert!(matches!(
            open_existing_private_lock_file(&lock_path, "worktree job lock"),
            Err(WorkerError::SecurityViolation(_))
        ));
        assert!(matches!(
            ensure_worktree_job_inactive_readonly(&finished_manifest),
            Err(WorkerError::SecurityViolation(_))
        ));
        fs::remove_file(&lock_path).expect("remove fifo at lock path");

        // 8. Foreign owner UID and inode replacement during inspection are refused.
        let opened = open_private_lock_file(&lock_path, "worktree job lock").expect("create lock");
        let open_meta = opened.metadata().expect("opened metadata");
        let foreign_uid = current_euid().wrapping_add(1);
        assert!(matches!(
            open_existing_private_lock_file_with_uid(&lock_path, "worktree job lock", foreign_uid),
            Err(WorkerError::SecurityViolation(_))
        ));
        assert!(matches!(
            ensure_worktree_job_inactive_readonly_with_uid(&finished_manifest, foreign_uid),
            Err(WorkerError::SecurityViolation(_))
        ));

        fs::remove_file(&lock_path).expect("unlink original lock inode");
        let _replacement =
            open_private_lock_file(&lock_path, "worktree job lock").expect("create new inode");
        assert!(matches!(
            verify_locked_private_file_unchanged(
                &opened,
                &lock_path,
                &open_meta,
                "worktree job lock",
                current_euid(),
            ),
            Err(WorkerError::SecurityViolation(_))
        ));
    }
}
