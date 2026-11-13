//! Read-only physical delivery inspection for an already verified completed worker task.
//!
//! [`inspect_delivery_for_verified_task`] accepts trusted-local approval, task snapshot, and
//! private work-area inputs resolved by a parent host facade. It cross-checks those inputs,
//! reloads the owned task worktree manifest, and derives a bounded content digest for the actual
//! checkout, including ordinary uncommitted, untracked, and deleted changes. It never commits,
//! accepts, merges, cleans up, writes session state, or invokes a provider.
//!
//! This module does **not** read the authority ledger, sealed run receipt, runtime worker store,
//! or frozen registry profile. The caller must already have resolved and verified the approval,
//! session binding, sealed receipt, and frozen worker profile before calling it. It also does not
//! prove semantic acceptance, expected delivery category, test results, or that a prose summary
//! was satisfied; those remain explicit upstream Coworker attestation and backlog decisions.
//! A successful record or worker exit code is evidence input, not a `Done` decision.
//!
//! Inspection is a stable read of an inactive, owned checkout. It is not an atomic freeze and is
//! not protection against a malicious same-UID process that changes files during inspection.
//! Callers must use [`recheck_delivery_for_verified_task`] immediately before recording `Done`
//! and must treat a mismatch as a changed delivery rather than silently choosing an old result.

use std::collections::BTreeSet;
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use threadlane::{WorkerAcceptanceState, WorkerOutcomeSummary, WorkerSnapshot, WorkerStatus};
use threadlane_authority::{ApprovalRecord, ApprovedTaskRecord};
use threadlane_workers::{
    ensure_worktree_job_inactive_readonly, evaluate_diff_gate, is_commit_ancestor,
    load_and_verify_manifest, resolve_exact_commit_sha, run_sanitized_git,
    validate_exact_40_hex_sha, validate_repo_relative_path, validate_simple_identifier,
    ChangedPathEntry, CleanupRefusalReason, DiffChangeKind, DiffGateReport, WorkAcceptanceState,
    WorkerError, WorkerProvider, WorktreeKind, CHECKOUTS_SUBDIR, DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    DEFAULT_MAX_DIFF_CHANGED_PATHS, MANIFESTS_SUBDIR,
};

use crate::client::compact_worktree_tag;
use crate::error::HostError;
use crate::storage::{
    ensure_no_symlink_components, validate_approval_id, validate_canonical_thread_uuid,
    validate_hex_digest, validate_identifier, verify_existing_private_dir,
};
use crate::types::{TaskExecutionStatus, TaskStateSnapshot};

/// Maximum changed paths accepted by physical delivery inspection.
pub const MAX_DELIVERY_CHANGED_PATHS: usize = DEFAULT_MAX_DIFF_CHANGED_PATHS;

/// Maximum total bytes read across all present changed regular files (16 MiB).
pub const MAX_DELIVERY_CONTENT_BYTES: u64 = 16 * 1024 * 1024;

const DELIVERY_DIGEST_SCHEMA: &str = "threadlane-delivery-inspection-v1";
const RESULT_DIGEST_SCHEMA: &str = "threadlane-worker-terminal-result-v1";
const EXECUTION_BINDING_SCHEMA_VERSION: u32 = 1;

/// Finite physical delivery category. `NoChanges` is an explicit read-only result, never a
/// placeholder patch or an automatic acceptance of arbitrary prose work.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryKind {
    /// The owned checkout has no changed paths relative to its frozen base commit.
    NoChanges,
    /// The owned checkout has one or more bounded changed paths.
    Patch,
}

/// Read-only, path-free receipt for the actual physical state of one verified task checkout.
///
/// The DTO contains only software identities, commit SHAs, aggregate counts, and SHA-256 digests.
/// It never contains raw file contents, task instructions, worker prose, or absolute private
/// paths. `delivery_digest` binds the physical checkout state; `result_digest` binds stable
/// worker terminal evidence. Neither digest is an acceptance decision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeliveryInspection {
    /// Approval identifier resolved by the trusted caller.
    pub approval_id: String,
    /// Frozen approval subject digest.
    pub subject_digest: String,
    /// Authoritative control-plane binding identifier.
    pub binding_id: String,
    /// Frozen native conversation thread UUID.
    pub thread_uuid: String,
    /// Runtime session binding identifier.
    pub session_binding_id: String,
    /// Approved task identifier.
    pub task_id: String,
    /// Frozen compiled task specification digest.
    pub task_digest: String,
    /// Deterministic worker profile identifier.
    pub profile_id: String,
    /// Immutable runtime worker profile digest.
    pub profile_digest: String,
    /// Deterministic task command key.
    pub command_key: String,
    /// Runtime run identifier.
    pub run_id: String,
    /// Runtime worker identifier.
    pub worker_id: String,
    /// Owned per-task worktree identifier.
    pub worktree_id: String,
    /// Exact frozen task execution base commit.
    pub base_commit: String,
    /// Exact current `HEAD` of the owned task checkout.
    pub head_commit: String,
    /// Finite physical delivery category.
    pub delivery_kind: DeliveryKind,
    /// SHA-256 digest over the exact bounded physical delivery encoding.
    pub delivery_digest: String,
    /// SHA-256 digest over bounded stable worker terminal evidence.
    pub result_digest: String,
    /// Number of changed paths included in the delivery digest.
    pub changed_paths_count: u32,
    /// SHA-256 digest over sorted changed path identities (paths and change kinds only).
    pub changed_path_set_digest: String,
    /// Total bytes read from present changed regular files.
    pub content_bytes: u64,
}

/// Recomputes the physical delivery and confirms it still equals a prior inspection receipt.
///
/// Call this immediately before upstream backlog completion. A mismatch is a typed refusal; the
/// helper never mutates state, chooses the first or last observed checkout, or records `Done`.
pub fn recheck_delivery_for_verified_task(
    record: &ApprovalRecord,
    snapshot: &TaskStateSnapshot,
    work_area_root: &Path,
    expected: &DeliveryInspection,
) -> Result<(), HostError> {
    let live = inspect_delivery_for_verified_task(record, snapshot, work_area_root)?;
    if &live != expected {
        return Err(HostError::SecurityViolation {
            path: "task-delivery".to_string(),
            reason: "task checkout changed after the delivery inspection receipt".to_string(),
        });
    }
    Ok(())
}

/// Inspects the actual owned checkout of a task whose authority, receipt, and runtime snapshot
/// were already verified by the trusted parent facade.
///
/// Preconditions (not re-derived here):
///
/// - `record` came from the authoritative approval store and its frozen task/profile identity was
///   already verified;
/// - `snapshot` came from the live or stopped verified host route, including its sealed task-run
///   receipt and execution binding;
/// - `work_area_root` is the private work area owned by that exact approval/session.
///
/// The function still validates all supplied correlations, the exact owned manifest/path/branch
/// and base commit, inactive job state, `ReadyForReview`/`Completed` terminal gates, complete diff
/// gate success, and bounded file evidence. It validates dependent execution-binding shape and
/// base consistency, but does not recompute the sealed receipt, frozen profile digest, or authority
/// ledger. It reads the checkout twice and refuses if the two stable reads disagree. It does not
/// prove semantic acceptance or expected delivery content.
pub fn inspect_delivery_for_verified_task(
    record: &ApprovalRecord,
    snapshot: &TaskStateSnapshot,
    work_area_root: &Path,
) -> Result<DeliveryInspection, HostError> {
    let first = inspect_once(record, snapshot, work_area_root)?;
    let second = inspect_once(record, snapshot, work_area_root)?;
    if first != second {
        return Err(HostError::SecurityViolation {
            path: "task-delivery".to_string(),
            reason: "task checkout changed during read-only delivery inspection".to_string(),
        });
    }
    Ok(first)
}

struct VerifiedDeliveryContext<'a> {
    record: &'a ApprovalRecord,
    snapshot: &'a TaskStateSnapshot,
    task: &'a ApprovedTaskRecord,
    worker: &'a WorkerSnapshot,
    worktree_id: String,
    canonical_checkout: PathBuf,
    expected_run_tag: String,
    expected_task_tag: String,
    expected_base_commit: String,
    head_commit: String,
    diff_gate: DiffGateReport,
}

fn inspect_once(
    record: &ApprovalRecord,
    snapshot: &TaskStateSnapshot,
    work_area_root: &Path,
) -> Result<DeliveryInspection, HostError> {
    let context = verify_delivery_context(record, snapshot, work_area_root)?;
    capture_delivery(&context)
}

fn verify_delivery_context<'a>(
    record: &'a ApprovalRecord,
    snapshot: &'a TaskStateSnapshot,
    work_area_root: &Path,
) -> Result<VerifiedDeliveryContext<'a>, HostError> {
    validate_approval_id(&record.approval_id)?;
    validate_hex_digest("subject_digest", &record.subject_digest)?;
    validate_identifier("binding_id", &record.binding_id)?;
    validate_canonical_thread_uuid("thread_uuid", &record.thread_uuid)?;
    let record_base_commit =
        validate_exact_40_hex_sha(&record.base_commit, "record.base_commit")
            .map_err(|_| invalid_delivery("record base commit is not an exact 40-hex SHA"))?;

    if record.approval_id != snapshot.approval_id
        || record.subject_digest != snapshot.subject_digest
        || record.binding_id != snapshot.binding_id
        || record.thread_uuid != snapshot.thread_uuid
    {
        return Err(HostError::ScopeMismatch {
            field: "approval_subject".to_string(),
            expected: record.approval_id.clone(),
            actual: snapshot.approval_id.clone(),
            reason: "approval/task snapshot authority identity mismatch".to_string(),
        });
    }

    validate_identifier("task_id", &snapshot.task_id)?;
    validate_hex_digest("task_digest", &snapshot.task_digest)?;
    if snapshot.accepted {
        return Err(invalid_delivery(
            "task snapshot must remain unaccepted at the physical delivery boundary",
        ));
    }
    if snapshot.status != TaskExecutionStatus::ReadyForReview {
        return Err(invalid_delivery(
            "task snapshot is not at ReadyForReview for physical delivery inspection",
        ));
    }

    let mut matching_tasks = record
        .tasks
        .iter()
        .filter(|task| task.task_id == snapshot.task_id);
    let task = matching_tasks
        .next()
        .ok_or_else(|| HostError::UnknownTask {
            approval_id: record.approval_id.clone(),
            task_id: snapshot.task_id.clone(),
        })?;
    if matching_tasks.next().is_some() {
        return Err(invalid_delivery(
            "approval record contains duplicate task identifiers",
        ));
    }
    if !record
        .approved_task_ids
        .iter()
        .any(|task_id| task_id == &task.task_id)
    {
        return Err(HostError::UnknownTask {
            approval_id: record.approval_id.clone(),
            task_id: task.task_id.clone(),
        });
    }

    if task.task_digest != snapshot.task_digest
        || task.adapter != snapshot.adapter
        || task.provider != snapshot.provider
        || task.depends_on != snapshot.depends_on
        || task.profile_id != snapshot.profile_id
        || task.command_key != snapshot.command_key
    {
        return Err(invalid_delivery(
            "task snapshot does not match the frozen approved task identity",
        ));
    }
    if !task.is_frozen_headless_worker() {
        return Err(invalid_delivery(
            "task is not a complete frozen headless worker contract",
        ));
    }
    if task.instruction_digest != sha256_hex(task.instruction.as_bytes()) {
        return Err(invalid_delivery(
            "frozen task instruction digest does not match its instruction",
        ));
    }
    let task_profile_id = task
        .profile_id
        .as_deref()
        .ok_or_else(|| invalid_delivery("frozen worker task is missing its profile identifier"))?;
    if task_profile_id.is_empty() || task.command_key.as_deref().is_none_or(str::is_empty) {
        return Err(invalid_delivery(
            "frozen worker task is missing its profile identifier or command key",
        ));
    }

    let worker = snapshot
        .worker
        .as_ref()
        .ok_or_else(|| invalid_delivery("task snapshot is missing the runtime worker snapshot"))?;
    if snapshot.session_binding_id != worker.binding_id {
        return Err(HostError::ScopeMismatch {
            field: "session_binding_id".to_string(),
            expected: snapshot.session_binding_id.clone(),
            actual: worker.binding_id.clone(),
            reason: "worker snapshot binding does not match the task snapshot session binding"
                .to_string(),
        });
    }
    if snapshot.run_id.as_deref() != Some(worker.run_id.as_str())
        || snapshot.worker_id.as_deref() != Some(worker.worker_id.as_str())
    {
        return Err(HostError::ScopeMismatch {
            field: "run_worker_identity".to_string(),
            expected: worker.run_id.clone(),
            actual: snapshot.run_id.clone().unwrap_or_default(),
            reason: "task snapshot run/worker identity does not match the worker snapshot"
                .to_string(),
        });
    }
    if worker.status != WorkerStatus::Completed {
        return Err(invalid_delivery(
            "worker snapshot is not in the Completed terminal state",
        ));
    }
    validate_hex_digest("worker.profile_digest", &worker.profile_digest)?;
    if worker.profile_id != task_profile_id
        || worker.provider != task.provider.map(WorkerProvider::as_str).unwrap_or("")
        || worker.requested_model != task.base_model
        || worker.requested_effort != task.effort.as_deref().unwrap_or("default")
        || worker.instruction != task.instruction
    {
        return Err(invalid_delivery(
            "worker snapshot does not match the frozen task profile, model, effort, or instruction",
        ));
    }
    if worker.observed_model.as_deref() != Some(task.base_model.as_str()) {
        return Err(invalid_delivery(
            "worker snapshot is missing matching observed model evidence",
        ));
    }
    if let Some(expected_effort) = task.effort.as_deref() {
        if worker.observed_effort.as_deref() != Some(expected_effort) {
            return Err(invalid_delivery(
                "worker snapshot is missing matching observed effort evidence",
            ));
        }
    }

    let outcome = worker
        .outcome
        .as_ref()
        .ok_or_else(|| invalid_delivery("Completed worker snapshot is missing terminal outcome"))?;
    if !successful_outcome(outcome) {
        return Err(invalid_delivery(
            "worker terminal outcome does not satisfy the complete successful-result gates",
        ));
    }

    let expected_base_commit = if task.depends_on.is_empty() {
        if worker.execution_binding.is_some() {
            return Err(invalid_delivery(
                "root worker task unexpectedly carries a dependency execution binding",
            ));
        }
        record_base_commit
    } else {
        let binding = worker.execution_binding.as_ref().ok_or_else(|| {
            invalid_delivery("dependent worker task is missing its frozen execution binding")
        })?;
        if binding.schema_version != EXECUTION_BINDING_SCHEMA_VERSION {
            return Err(invalid_delivery(
                "worker execution binding has an unsupported schema version",
            ));
        }
        validate_hex_digest("execution_binding.input_digest", &binding.input_digest)?;
        validate_hex_digest(
            "execution_binding.seal_file_digest",
            &binding.seal_file_digest,
        )?;
        validate_hex_digest(
            "execution_binding.execution_digest",
            &binding.execution_digest,
        )?;
        validate_exact_40_hex_sha(
            &binding.execution_base_commit,
            "execution_binding.execution_base_commit",
        )
        .map_err(|_| invalid_delivery("execution binding base commit is not an exact 40-hex SHA"))?
    };
    if worker.worktree.base_commit != expected_base_commit {
        return Err(invalid_delivery(
            "worker worktree base commit does not match the expected task execution base",
        ));
    }

    if !work_area_root.is_absolute() {
        return Err(invalid_delivery(
            "trusted work-area root must be an absolute canonical path",
        ));
    }
    let canonical_work_area = verify_existing_private_dir(work_area_root)?;
    let canonical_repo = validate_canonical_repository(&record.repository_path)?;

    let worktree_id = worker
        .worktree
        .task_worktree_id
        .as_deref()
        .ok_or_else(|| invalid_delivery("worker snapshot is missing its task worktree id"))?;
    validate_simple_identifier(worktree_id, "task_worktree_id")
        .map_err(|_| invalid_delivery("worker task worktree identifier is invalid"))?;
    let task_worktree_path = worker
        .worktree
        .task_worktree_path
        .as_deref()
        .ok_or_else(|| invalid_delivery("worker snapshot is missing its task worktree path"))?;
    let task_branch_name = worker
        .worktree
        .task_branch_name
        .as_deref()
        .ok_or_else(|| invalid_delivery("worker snapshot is missing its task branch name"))?;

    let expected_run_tag = compact_worktree_tag(&worker.run_id, "r");
    let expected_task_tag = compact_worktree_tag(&worker.worker_id, "w");
    let expected_run_worktree_prefix = format!("wt-run-{expected_run_tag}-");
    if let Some(run_worktree_id) = worker.worktree.run_worktree_id.as_deref() {
        if !run_worktree_id.starts_with(&expected_run_worktree_prefix)
            || run_worktree_id.len() == expected_run_worktree_prefix.len()
        {
            return Err(invalid_delivery(
                "worker run worktree identifier is not derived from the exact run identity",
            ));
        }
    }

    let manifest = load_and_verify_manifest(&canonical_work_area, worktree_id)
        .map_err(|err| map_worker_error("task worktree manifest verification", err))?;
    let expected_checkout = canonical_work_area
        .join(CHECKOUTS_SUBDIR)
        .join("tasks")
        .join(worktree_id);
    let expected_manifest_path = canonical_work_area
        .join(MANIFESTS_SUBDIR)
        .join(format!("{worktree_id}.json"));
    let expected_allowed_write_paths: BTreeSet<String> =
        task.allowed_write_paths.iter().cloned().collect();
    let expected_branch_prefix = format!("threadlane/task/{expected_run_tag}/{expected_task_tag}/");

    if manifest.job_active || manifest.active_job_pid.is_some() {
        return Err(active_job_error(&worker.run_id));
    }
    if manifest.kind != WorktreeKind::Task
        || manifest.worktree_id != worktree_id
        || manifest.run_id != expected_run_tag
        || manifest.task_id.as_deref() != Some(expected_task_tag.as_str())
        || manifest.repository_path != canonical_repo
        || manifest.work_area_root != canonical_work_area
        || manifest.manifest_path != expected_manifest_path
        || manifest.checkout_path != expected_checkout
        || manifest.base_commit != expected_base_commit
        || manifest.allowed_write_paths != expected_allowed_write_paths
        || manifest.partial_cleanup_accepted_head.is_some()
    {
        return Err(invalid_delivery(
            "task worktree manifest does not match the exact frozen task identity, scope, or inactive ownership",
        ));
    }
    if !manifest.branch_name.starts_with(&expected_branch_prefix) {
        return Err(invalid_delivery(
            "task worktree branch is not derived from the exact run and worker identity",
        ));
    }
    let branch_token = &manifest.branch_name[expected_branch_prefix.len()..];
    if branch_token.is_empty() {
        return Err(invalid_delivery(
            "task worktree branch is missing its software token",
        ));
    }
    validate_simple_identifier(branch_token, "task branch token")
        .map_err(|_| invalid_delivery("task worktree branch token is invalid"))?;
    let expected_worktree_id =
        format!("wt-task-{expected_run_tag}-{expected_task_tag}-{branch_token}");
    if worktree_id != expected_worktree_id {
        return Err(invalid_delivery(
            "task worktree identifier and branch token do not share the exact run and worker identity",
        ));
    }
    if task_worktree_path != manifest.checkout_path.to_string_lossy().as_ref()
        || task_branch_name != manifest.branch_name
    {
        return Err(invalid_delivery(
            "worker snapshot task worktree path or branch does not match the owned manifest",
        ));
    }
    validate_task_scope_within_approval(task, record)?;

    ensure_worktree_job_inactive_readonly(&manifest)
        .map_err(|err| map_worker_error("task worktree inactive-job verification", err))?;

    let canonical_checkout = threadlane_workers::validate_existing_canonical_dir(
        &manifest.checkout_path,
        "task checkout",
    )
    .map_err(|err| map_worker_error("task checkout canonicalization", err))?;
    if canonical_checkout != manifest.checkout_path {
        return Err(invalid_delivery(
            "task checkout path does not canonicalize to the owned manifest path",
        ));
    }

    let current_branch = run_sanitized_git(
        &canonical_checkout,
        &["symbolic-ref", "--quiet", "--short", "HEAD"],
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .map_err(|err| map_worker_error("task checkout branch verification", err))?;
    if current_branch.trim() != manifest.branch_name {
        return Err(invalid_delivery(
            "task checkout is not on its exact owned software branch",
        ));
    }

    let head_commit =
        resolve_exact_commit_sha(&canonical_checkout, "HEAD", DEFAULT_GIT_COMMAND_TIMEOUT_MS)
            .map_err(|err| map_worker_error("task checkout HEAD resolution", err))?;
    if worker.worktree.head_commit.as_deref() != Some(head_commit.as_str()) {
        return Err(invalid_delivery(
            "worker terminal head commit does not match the actual task checkout HEAD",
        ));
    }
    if head_commit != expected_base_commit {
        let base_is_ancestor = is_commit_ancestor(
            &canonical_repo,
            &expected_base_commit,
            &head_commit,
            DEFAULT_GIT_COMMAND_TIMEOUT_MS,
        )
        .map_err(|err| map_worker_error("task checkout ancestry verification", err))?;
        if !base_is_ancestor {
            return Err(invalid_delivery(
                "frozen task base commit is not an ancestor of the task checkout HEAD",
            ));
        }
    }

    let diff_gate = evaluate_diff_gate(&canonical_work_area, worktree_id)
        .map_err(|err| map_worker_error("task delivery diff gate", err))?;
    if diff_gate.worktree_id != worktree_id
        || diff_gate.checkout_path != canonical_checkout
        || diff_gate.base_commit != expected_base_commit
        || diff_gate.head_commit != head_commit
        || !diff_gate.passed
        || !diff_gate.evidence_complete
        || !diff_gate.violations.is_empty()
        || diff_gate.acceptance_state != WorkAcceptanceState::DiffGatePassedPendingReview
        || diff_gate.changed_paths.len() > MAX_DELIVERY_CHANGED_PATHS
    {
        return Err(invalid_delivery(
            "task checkout did not pass the complete bounded diff gate",
        ));
    }

    Ok(VerifiedDeliveryContext {
        record,
        snapshot,
        task,
        worker,
        worktree_id: worktree_id.to_string(),
        canonical_checkout,
        expected_run_tag,
        expected_task_tag,
        expected_base_commit,
        head_commit,
        diff_gate,
    })
}

fn successful_outcome(outcome: &WorkerOutcomeSummary) -> bool {
    outcome.acceptance_state == WorkerAcceptanceState::ReadyForReview
        && !outcome.missing_model_evidence
        && !outcome.missing_effort_evidence
        && !outcome.provider_error
        && !outcome.incomplete_cleanup
        && !outcome.rejected_by_diff_gate
        && outcome.violation_count == 0
        && outcome.exit_code == Some(0)
        && outcome.signal.is_none()
        && !outcome.timed_out
        && !outcome.cancelled
        && !outcome.stdout_truncated
        && !outcome.stderr_truncated
}

#[derive(Serialize)]
struct DeliveryPathEvidence {
    path: String,
    orig_path: Option<String>,
    change_kind: DiffChangeKind,
    presence: &'static str,
    executable: Option<bool>,
    size_bytes: u64,
    content_sha256: Option<String>,
}

#[derive(Serialize)]
struct DeliveryPathIdentity<'a> {
    path: &'a str,
    orig_path: Option<&'a str>,
    change_kind: DiffChangeKind,
    presence: &'a str,
}

#[derive(Serialize)]
struct DeliveryDigestEncoding<'a> {
    schema: &'static str,
    approval_id: &'a str,
    subject_digest: &'a str,
    binding_id: &'a str,
    thread_uuid: &'a str,
    session_binding_id: &'a str,
    task_id: &'a str,
    task_digest: &'a str,
    profile_id: &'a str,
    profile_digest: &'a str,
    command_key: &'a str,
    run_id: &'a str,
    worker_id: &'a str,
    run_worktree_tag: &'a str,
    task_worktree_tag: &'a str,
    worktree_id: &'a str,
    base_commit: &'a str,
    head_commit: &'a str,
    delivery_kind: DeliveryKind,
    changed_paths: &'a [DeliveryPathEvidence],
}

#[derive(Serialize)]
struct DeliveryPathSetEncoding<'a> {
    schema: &'static str,
    delivery_kind: DeliveryKind,
    paths: Vec<DeliveryPathIdentity<'a>>,
}

#[derive(Serialize)]
struct TerminalOutcomeEncoding {
    acceptance_state: WorkerAcceptanceState,
    exit_code: Option<i32>,
    signal: Option<i32>,
    timed_out: bool,
    cancelled: bool,
    cancel_requested_before_completion: bool,
    missing_model_evidence: bool,
    missing_effort_evidence: bool,
    provider_error: bool,
    incomplete_cleanup: bool,
    rejected_by_diff_gate: bool,
    stdout_truncated: bool,
    stderr_truncated: bool,
    changed_paths_count: u32,
    violation_count: u32,
}

#[derive(Serialize)]
struct ResultDigestEncoding<'a> {
    schema: &'static str,
    approval_id: &'a str,
    subject_digest: &'a str,
    binding_id: &'a str,
    thread_uuid: &'a str,
    session_binding_id: &'a str,
    task_id: &'a str,
    task_digest: &'a str,
    profile_id: &'a str,
    profile_digest: &'a str,
    command_key: &'a str,
    run_id: &'a str,
    worker_id: &'a str,
    worker_revision: u64,
    attempt_count: u32,
    provider: &'a str,
    requested_model: &'a str,
    requested_effort: &'a str,
    observed_model: Option<&'a str>,
    observed_effort: Option<&'a str>,
    base_commit: &'a str,
    head_commit: &'a str,
    outcome: TerminalOutcomeEncoding,
}

fn capture_delivery(
    context: &VerifiedDeliveryContext<'_>,
) -> Result<DeliveryInspection, HostError> {
    let delivery_kind = if context.diff_gate.changed_paths.is_empty() {
        DeliveryKind::NoChanges
    } else {
        DeliveryKind::Patch
    };

    let mut total_content_bytes = 0u64;
    let mut paths = Vec::with_capacity(context.diff_gate.changed_paths.len());
    for entry in &context.diff_gate.changed_paths {
        paths.push(capture_changed_path(
            &context.canonical_checkout,
            entry,
            &mut total_content_bytes,
        )?);
    }
    paths.sort_by(|left, right| {
        left.path
            .cmp(&right.path)
            .then_with(|| left.orig_path.cmp(&right.orig_path))
            .then_with(|| left.change_kind.cmp(&right.change_kind))
    });

    let delivery_encoding = DeliveryDigestEncoding {
        schema: DELIVERY_DIGEST_SCHEMA,
        approval_id: &context.record.approval_id,
        subject_digest: &context.record.subject_digest,
        binding_id: &context.record.binding_id,
        thread_uuid: &context.record.thread_uuid,
        session_binding_id: &context.snapshot.session_binding_id,
        task_id: &context.task.task_id,
        task_digest: &context.task.task_digest,
        profile_id: &context.worker.profile_id,
        profile_digest: &context.worker.profile_digest,
        command_key: context.task.command_key.as_deref().unwrap_or(""),
        run_id: &context.worker.run_id,
        worker_id: &context.worker.worker_id,
        run_worktree_tag: &context.expected_run_tag,
        task_worktree_tag: &context.expected_task_tag,
        worktree_id: &context.worktree_id,
        base_commit: &context.expected_base_commit,
        head_commit: &context.head_commit,
        delivery_kind,
        changed_paths: &paths,
    };
    let delivery_digest = canonical_json_digest(&delivery_encoding)?;

    let path_identities: Vec<DeliveryPathIdentity<'_>> = paths
        .iter()
        .map(|path| DeliveryPathIdentity {
            path: &path.path,
            orig_path: path.orig_path.as_deref(),
            change_kind: path.change_kind,
            presence: path.presence,
        })
        .collect();
    let path_set_encoding = DeliveryPathSetEncoding {
        schema: DELIVERY_DIGEST_SCHEMA,
        delivery_kind,
        paths: path_identities,
    };
    let changed_path_set_digest = canonical_json_digest(&path_set_encoding)?;

    let outcome =
        context.worker.outcome.as_ref().ok_or_else(|| {
            invalid_delivery("Completed worker snapshot is missing terminal outcome")
        })?;
    let result_encoding = ResultDigestEncoding {
        schema: RESULT_DIGEST_SCHEMA,
        approval_id: &context.record.approval_id,
        subject_digest: &context.record.subject_digest,
        binding_id: &context.record.binding_id,
        thread_uuid: &context.record.thread_uuid,
        session_binding_id: &context.snapshot.session_binding_id,
        task_id: &context.task.task_id,
        task_digest: &context.task.task_digest,
        profile_id: &context.worker.profile_id,
        profile_digest: &context.worker.profile_digest,
        command_key: context.task.command_key.as_deref().unwrap_or(""),
        run_id: &context.worker.run_id,
        worker_id: &context.worker.worker_id,
        worker_revision: context.worker.revision,
        attempt_count: context.worker.attempt_count,
        provider: &context.worker.provider,
        requested_model: &context.worker.requested_model,
        requested_effort: &context.worker.requested_effort,
        observed_model: context.worker.observed_model.as_deref(),
        observed_effort: context.worker.observed_effort.as_deref(),
        base_commit: &context.expected_base_commit,
        head_commit: &context.head_commit,
        outcome: TerminalOutcomeEncoding {
            acceptance_state: outcome.acceptance_state,
            exit_code: outcome.exit_code,
            signal: outcome.signal,
            timed_out: outcome.timed_out,
            cancelled: outcome.cancelled,
            cancel_requested_before_completion: outcome.cancel_requested_before_completion,
            missing_model_evidence: outcome.missing_model_evidence,
            missing_effort_evidence: outcome.missing_effort_evidence,
            provider_error: outcome.provider_error,
            incomplete_cleanup: outcome.incomplete_cleanup,
            rejected_by_diff_gate: outcome.rejected_by_diff_gate,
            stdout_truncated: outcome.stdout_truncated,
            stderr_truncated: outcome.stderr_truncated,
            changed_paths_count: outcome.changed_paths_count,
            violation_count: outcome.violation_count,
        },
    };
    let result_digest = canonical_json_digest(&result_encoding)?;

    let changed_paths_count = u32::try_from(paths.len())
        .map_err(|_| invalid_delivery("delivery changed-path count overflowed its bound"))?;
    Ok(DeliveryInspection {
        approval_id: context.record.approval_id.clone(),
        subject_digest: context.record.subject_digest.clone(),
        binding_id: context.record.binding_id.clone(),
        thread_uuid: context.record.thread_uuid.clone(),
        session_binding_id: context.snapshot.session_binding_id.clone(),
        task_id: context.task.task_id.clone(),
        task_digest: context.task.task_digest.clone(),
        profile_id: context.worker.profile_id.clone(),
        profile_digest: context.worker.profile_digest.clone(),
        command_key: context.task.command_key.clone().unwrap_or_default(),
        run_id: context.worker.run_id.clone(),
        worker_id: context.worker.worker_id.clone(),
        worktree_id: context.worktree_id.clone(),
        base_commit: context.expected_base_commit.clone(),
        head_commit: context.head_commit.clone(),
        delivery_kind,
        delivery_digest,
        result_digest,
        changed_paths_count,
        changed_path_set_digest,
        content_bytes: total_content_bytes,
    })
}

fn capture_changed_path(
    checkout_root: &Path,
    entry: &ChangedPathEntry,
    total_content_bytes: &mut u64,
) -> Result<DeliveryPathEvidence, HostError> {
    validate_repo_relative_path(&entry.path, "changed path")
        .map_err(|_| invalid_delivery("delivery contains an invalid relative changed path"))?;
    if let Some(orig_path) = entry.orig_path.as_deref() {
        validate_repo_relative_path(orig_path, "changed original path")
            .map_err(|_| invalid_delivery("delivery contains an invalid relative original path"))?;
    }
    let full_path = checkout_root.join(&entry.path);
    if !full_path.starts_with(checkout_root) {
        return Err(invalid_delivery(
            "delivery path escaped the owned task checkout",
        ));
    }
    ensure_no_symlink_components(&full_path)
        .map_err(|_| invalid_delivery("delivery path traverses a symlink component"))?;

    if entry.change_kind == DiffChangeKind::Deleted {
        match fs::symlink_metadata(&full_path) {
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            _ => {
                return Err(invalid_delivery(
                    "deleted delivery path still exists or could not be inspected",
                ))
            }
        }
        return Ok(DeliveryPathEvidence {
            path: entry.path.clone(),
            orig_path: entry.orig_path.clone(),
            change_kind: entry.change_kind,
            presence: "deleted",
            executable: None,
            size_bytes: 0,
            content_sha256: None,
        });
    }

    let metadata = fs::symlink_metadata(&full_path)
        .map_err(|_| invalid_delivery("delivery path metadata is missing or unreadable"))?;
    if metadata.file_type().is_symlink() || !metadata.file_type().is_file() {
        return Err(invalid_delivery(
            "delivery contains an unsupported symlink, submodule, or special file entry",
        ));
    }

    let remaining = MAX_DELIVERY_CONTENT_BYTES
        .checked_sub(*total_content_bytes)
        .ok_or_else(|| invalid_delivery("delivery content byte budget was exhausted"))?;
    let size_bytes = metadata.len();
    if size_bytes > remaining {
        return Err(invalid_delivery(
            "delivery content exceeded the bounded total byte budget",
        ));
    }

    let file = fs::File::open(&full_path)
        .map_err(|_| invalid_delivery("delivery file could not be opened read-only"))?;
    let opened_metadata = file
        .metadata()
        .map_err(|_| invalid_delivery("delivery file metadata could not be read"))?;
    if opened_metadata.file_type().is_symlink() || !opened_metadata.file_type().is_file() {
        return Err(invalid_delivery(
            "delivery file is not a regular non-symlink file",
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if opened_metadata.dev() != metadata.dev() || opened_metadata.ino() != metadata.ino() {
            return Err(invalid_delivery(
                "delivery file identity changed during read-only inspection",
            ));
        }
    }

    let mut bytes = Vec::new();
    file.take(size_bytes.saturating_add(1))
        .read_to_end(&mut bytes)
        .map_err(|_| invalid_delivery("delivery file could not be read within its byte budget"))?;
    if bytes.len() as u64 != size_bytes {
        return Err(invalid_delivery(
            "delivery file length changed during read-only inspection",
        ));
    }

    let after = fs::symlink_metadata(&full_path)
        .map_err(|_| invalid_delivery("delivery file disappeared during read-only inspection"))?;
    if after.file_type().is_symlink() || !after.file_type().is_file() || after.len() != size_bytes {
        return Err(invalid_delivery(
            "delivery file identity changed during read-only inspection",
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if after.dev() != metadata.dev() || after.ino() != metadata.ino() {
            return Err(invalid_delivery(
                "delivery file identity changed during read-only inspection",
            ));
        }
    }

    *total_content_bytes = total_content_bytes
        .checked_add(size_bytes)
        .ok_or_else(|| invalid_delivery("delivery content byte budget overflowed"))?;
    #[cfg(unix)]
    let executable = {
        use std::os::unix::fs::PermissionsExt;
        Some(opened_metadata.permissions().mode() & 0o111 != 0)
    };
    #[cfg(not(unix))]
    let executable = None;

    Ok(DeliveryPathEvidence {
        path: entry.path.clone(),
        orig_path: entry.orig_path.clone(),
        change_kind: entry.change_kind,
        presence: "regular_file",
        executable,
        size_bytes,
        content_sha256: Some(sha256_hex(&bytes)),
    })
}

fn validate_task_scope_within_approval(
    task: &ApprovedTaskRecord,
    record: &ApprovalRecord,
) -> Result<(), HostError> {
    if task.allowed_write_paths.is_empty() {
        return Err(invalid_delivery(
            "frozen worker task has an empty write scope",
        ));
    }
    for scope in &task.allowed_write_paths {
        validate_repo_relative_path(scope, "task allowed_write_paths entry")
            .map_err(|_| invalid_delivery("frozen task write scope is not a relative path"))?;
        if !approved_scope_covers(scope, &record.approved_write_paths) {
            return Err(invalid_delivery(
                "frozen task write scope is outside the operator-approved write scope",
            ));
        }
    }
    Ok(())
}

fn approved_scope_covers(candidate: &str, approved_scopes: &[String]) -> bool {
    let candidate_is_directory = candidate.ends_with('/');
    let candidate_path = Path::new(candidate.trim_end_matches('/'));
    approved_scopes.iter().any(|scope| {
        let trimmed = scope.trim_end_matches('/');
        if trimmed.is_empty() {
            return false;
        }
        let scope_path = Path::new(trimmed);
        if scope.ends_with('/') {
            candidate_path == scope_path || candidate_path.starts_with(scope_path)
        } else if candidate_is_directory {
            false
        } else {
            candidate_path == scope_path
        }
    })
}

fn validate_canonical_repository(path: &Path) -> Result<PathBuf, HostError> {
    if !path.is_absolute() {
        return Err(invalid_delivery(
            "trusted approval repository path must be absolute",
        ));
    }
    let checked = ensure_no_symlink_components(path)?;
    let metadata = fs::symlink_metadata(&checked)
        .map_err(|_| invalid_delivery("trusted approval repository path is unavailable"))?;
    if metadata.file_type().is_symlink() || !metadata.file_type().is_dir() {
        return Err(invalid_delivery(
            "trusted approval repository path is not a real directory",
        ));
    }
    fs::canonicalize(&checked)
        .map_err(|_| invalid_delivery("trusted approval repository path cannot be canonicalized"))
}

fn canonical_json_digest<T: Serialize>(value: &T) -> Result<String, HostError> {
    let bytes = serde_json::to_vec(value)
        .map_err(|_| invalid_delivery("delivery digest encoding failed"))?;
    Ok(sha256_hex(&bytes))
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    let mut hex = String::with_capacity(64);
    for byte in hasher.finalize() {
        use std::fmt::Write as _;
        let _ = write!(&mut hex, "{byte:02x}");
    }
    hex
}

fn invalid_delivery(reason: impl Into<String>) -> HostError {
    HostError::Validation {
        field: "delivery".to_string(),
        reason: reason.into(),
    }
}

fn active_job_error(run_id: &str) -> HostError {
    HostError::InvalidRunState {
        run_id: run_id.to_string(),
        action: "inspect_delivery_for_verified_task".to_string(),
        reason: "task worktree has an active job or held job lock".to_string(),
    }
}

fn map_worker_error(operation: &str, error: WorkerError) -> HostError {
    match error {
        WorkerError::CleanupRefused {
            reason: CleanupRefusalReason::ActiveJob,
            worktree_id,
            ..
        } => active_job_error(&worktree_id),
        WorkerError::SecurityViolation(_) | WorkerError::CleanupRefused { .. } => {
            HostError::SecurityViolation {
                path: "task-delivery".to_string(),
                reason: format!("{operation} refused insecure or unowned delivery evidence"),
            }
        }
        _ => HostError::Io {
            operation: operation.to_string(),
            reason: "bounded read-only delivery inspection failed".to_string(),
        },
    }
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::{symlink, PermissionsExt};

    use fs2::FileExt as _;
    use tempfile::TempDir;
    use threadlane::{
        WorkerExecutionBinding, WorkerOutcomeSummary, WorkerProgressSummary, WorkerSnapshot,
        WorkerStatus, WorkerWorktreeSummary,
    };
    use threadlane_authority::{ApprovalRecord, ApprovedTaskRecord, AUTHORITY_POLICY_VERSION};
    use threadlane_plans::{Difficulty, WorkflowRole};
    use threadlane_registry::ToolAdapter;
    use threadlane_workers::{
        begin_worktree_job, evaluate_diff_gate, open_private_lock_file, resolve_exact_commit_sha,
        run_sanitized_git, write_private_json_atomic, ExecutionBudgets, WorkerFilePermissions,
        WorkerProvider, WorktreeManager, WorktreeManifest, DEFAULT_GIT_COMMAND_TIMEOUT_MS,
        MANIFESTS_SUBDIR,
    };

    use super::*;
    use crate::client::compact_worktree_tag;
    use crate::types::TaskStateSnapshot;

    const RUN_ID: &str = "run_delivery_fixture";
    const WORKER_ID: &str = "wrk_delivery_fixture";
    const SESSION_BINDING_ID: &str = "bnd_session_delivery";
    const TASK_ID: &str = "task-delivery";
    const INSTRUCTION: &str = "Implement the bounded delivery fixture";
    const MODEL: &str = "opencode-go/deepseek-v4.1-flash";
    const EFFORT: &str = "max";

    struct DeliveryFixture {
        _tmp: TempDir,
        record: ApprovalRecord,
        snapshot: TaskStateSnapshot,
        work_area_root: PathBuf,
        worktree_id: String,
        manifest_path: PathBuf,
        checkout: PathBuf,
        base_commit: String,
    }

    impl DeliveryFixture {
        fn new() -> Self {
            let tmp = TempDir::new().expect("tempdir");
            let root = fs::canonicalize(tmp.path()).expect("canonicalize temp root");
            let repo = root.join("repo");
            init_git_repo(&repo);
            let base_commit =
                resolve_exact_commit_sha(&repo, "HEAD", DEFAULT_GIT_COMMAND_TIMEOUT_MS)
                    .expect("resolve base commit");

            let work_area_root = root.join("work-area");
            let manager = WorktreeManager::new(&repo, &work_area_root, &base_commit)
                .expect("create worktree manager");
            let run_tag = compact_worktree_tag(RUN_ID, "r");
            let task_tag = compact_worktree_tag(WORKER_ID, "w");
            let run_manifest = manager
                .create_run_worktree(&run_tag)
                .expect("create run worktree");
            let allowed: BTreeSet<String> = ["src/".to_string()].into_iter().collect();
            let task_manifest = manager
                .create_task_worktree(&run_tag, &task_tag, &allowed)
                .expect("create task worktree");

            let task = build_task();
            let record = build_record(&repo, &base_commit, task.clone());
            let worker = build_worker(&task, &run_manifest, &task_manifest, &base_commit, 0);
            let snapshot = build_snapshot(&record, &task, worker);

            DeliveryFixture {
                manifest_path: task_manifest.manifest_path.clone(),
                checkout: task_manifest.checkout_path.clone(),
                worktree_id: task_manifest.worktree_id.clone(),
                work_area_root: manager.work_area_root().to_path_buf(),
                _tmp: tmp,
                record,
                snapshot,
                base_commit,
            }
        }

        fn apply_patch(&self) {
            fs::write(
                self.checkout.join("src/lib.rs"),
                "pub fn alpha() -> u32 { 2 }\n",
            )
            .expect("write modified file");
            fs::remove_file(self.checkout.join("src/extra.rs")).expect("delete tracked file");
            fs::write(
                self.checkout.join("src/new.rs"),
                "pub fn new_helper() -> bool { true }\n",
            )
            .expect("write untracked file");
        }

        fn restore_deleted_file(&self) {
            fs::write(self.checkout.join("src/extra.rs"), "pub fn extra() {}\n")
                .expect("restore deleted file");
        }

        fn commit_patch(&self) {
            run_sanitized_git(
                &self.checkout,
                &["add", "--", "src"],
                DEFAULT_GIT_COMMAND_TIMEOUT_MS,
            )
            .expect("git add patch");
            run_sanitized_git(
                &self.checkout,
                &["commit", "--quiet", "-m", "delivery fixture"],
                DEFAULT_GIT_COMMAND_TIMEOUT_MS,
            )
            .expect("git commit patch");
        }

        fn refresh_terminal_snapshot(&mut self) {
            let head =
                resolve_exact_commit_sha(&self.checkout, "HEAD", DEFAULT_GIT_COMMAND_TIMEOUT_MS)
                    .expect("resolve refreshed HEAD");
            let gate = evaluate_diff_gate(&self.work_area_root, &self.worktree_id)
                .expect("evaluate refreshed diff gate");
            let worker = self
                .snapshot
                .worker
                .as_mut()
                .expect("fixture worker snapshot");
            worker.worktree.head_commit = Some(head);
            worker
                .outcome
                .as_mut()
                .expect("fixture terminal outcome")
                .changed_paths_count = gate.changed_paths.len() as u32;
        }

        fn inspect(&self) -> Result<DeliveryInspection, HostError> {
            inspect_delivery_for_verified_task(&self.record, &self.snapshot, &self.work_area_root)
        }

        fn recheck(&self, expected: &DeliveryInspection) -> Result<(), HostError> {
            recheck_delivery_for_verified_task(
                &self.record,
                &self.snapshot,
                &self.work_area_root,
                expected,
            )
        }
    }

    fn init_git_repo(repo: &Path) {
        fs::create_dir_all(repo.join("src")).expect("create repo src");
        fs::write(repo.join("src/lib.rs"), "pub fn alpha() -> u32 { 1 }\n").expect("write lib");
        fs::write(repo.join("src/extra.rs"), "pub fn extra() {}\n").expect("write extra");
        run_sanitized_git(repo, &["init", "--quiet"], DEFAULT_GIT_COMMAND_TIMEOUT_MS)
            .expect("git init");
        run_sanitized_git(
            repo,
            &["config", "user.email", "test@threadlane.local"],
            DEFAULT_GIT_COMMAND_TIMEOUT_MS,
        )
        .expect("git config email");
        run_sanitized_git(
            repo,
            &["config", "user.name", "Threadlane Test"],
            DEFAULT_GIT_COMMAND_TIMEOUT_MS,
        )
        .expect("git config name");
        run_sanitized_git(repo, &["add", "."], DEFAULT_GIT_COMMAND_TIMEOUT_MS)
            .expect("git add initial");
        run_sanitized_git(
            repo,
            &["commit", "-m", "initial", "--quiet"],
            DEFAULT_GIT_COMMAND_TIMEOUT_MS,
        )
        .expect("git commit initial");
    }

    fn build_task() -> ApprovedTaskRecord {
        ApprovedTaskRecord {
            task_id: TASK_ID.to_string(),
            title: "Delivery fixture".to_string(),
            instruction: INSTRUCTION.to_string(),
            instruction_digest: sha256_hex(INSTRUCTION.as_bytes()),
            role: WorkflowRole::Coder,
            difficulty: Difficulty::Easy,
            tool_id: "tool-oc".to_string(),
            adapter: ToolAdapter::OpencodeV2,
            provider: Some(WorkerProvider::OpenCodeV2),
            canonical_executable: Some(PathBuf::from("/tmp/threadlane-fake-opencode")),
            model_id: "model-deepseek".to_string(),
            model_arg: MODEL.to_string(),
            base_model: MODEL.to_string(),
            effort: Some(EFFORT.to_string()),
            cli_model_arg: Some(format!("{MODEL}#{EFFORT}")),
            standalone: Some(true),
            file_permissions: Some(WorkerFilePermissions::TaskFiles),
            depends_on: Vec::new(),
            allowed_write_paths: vec!["src/".to_string()],
            task_digest: sha256_hex(b"compiled-delivery-task"),
            profile_id: Some("profile-worker-delivery".to_string()),
            command_key: Some("cmd-delivery-fixture".to_string()),
        }
    }

    fn build_record(repo: &Path, base_commit: &str, task: ApprovedTaskRecord) -> ApprovalRecord {
        let subject_digest = sha256_hex(b"delivery-approval-subject");
        ApprovalRecord {
            approval_id: format!("apr_{}", &sha256_hex(b"delivery-approval-id")[..32]),
            subject_digest,
            policy_version: AUTHORITY_POLICY_VERSION,
            binding_id: "bnd_control_delivery".to_string(),
            thread_uuid: "550e8400-e29b-41d4-a716-446655440000".to_string(),
            plan_id: "plan-delivery".to_string(),
            plan_revision: 1,
            profile_id: "profile-main".to_string(),
            registry_revision: 1,
            plan_digest: sha256_hex(b"delivery-plan"),
            repository_path: repo.to_path_buf(),
            base_commit: base_commit.to_string(),
            approved_task_ids: vec![task.task_id.clone()],
            approved_write_paths: vec!["src/".to_string()],
            max_parallel: 1,
            budgets: ExecutionBudgets::default(),
            tasks: vec![task],
            created_at_ms: 1_700_000_000_000,
        }
    }

    fn build_worker(
        task: &ApprovedTaskRecord,
        run_manifest: &WorktreeManifest,
        task_manifest: &WorktreeManifest,
        base_commit: &str,
        changed_paths_count: u32,
    ) -> WorkerSnapshot {
        WorkerSnapshot {
            untrusted_data: true,
            worker_id: WORKER_ID.to_string(),
            run_id: RUN_ID.to_string(),
            binding_id: SESSION_BINDING_ID.to_string(),
            revision: 3,
            status: WorkerStatus::Completed,
            profile_id: task.profile_id.clone().expect("task profile"),
            profile_digest: sha256_hex(b"frozen-worker-profile"),
            provider: WorkerProvider::OpenCodeV2.as_str().to_string(),
            requested_model: MODEL.to_string(),
            requested_effort: EFFORT.to_string(),
            observed_model: Some(MODEL.to_string()),
            observed_effort: Some(EFFORT.to_string()),
            instruction: task.instruction.clone(),
            claim_id: Some("wclm_delivery_fixture".to_string()),
            attempt_count: 1,
            execution_binding: None,
            worktree: WorkerWorktreeSummary {
                run_worktree_id: Some(run_manifest.worktree_id.clone()),
                run_worktree_path: Some(run_manifest.checkout_path.to_string_lossy().to_string()),
                run_branch_name: Some(run_manifest.branch_name.clone()),
                task_worktree_id: Some(task_manifest.worktree_id.clone()),
                task_worktree_path: Some(task_manifest.checkout_path.to_string_lossy().to_string()),
                task_branch_name: Some(task_manifest.branch_name.clone()),
                base_commit: base_commit.to_string(),
                head_commit: Some(base_commit.to_string()),
            },
            progress: WorkerProgressSummary::default(),
            outcome: Some(WorkerOutcomeSummary {
                provider_retry_high_water: None,
                provider_health: None,
                exit_code: Some(0),
                signal: None,
                duration_ms: Some(1),
                timed_out: false,
                cancelled: false,
                cancel_requested_before_completion: false,
                missing_model_evidence: false,
                missing_effort_evidence: false,
                provider_error: false,
                incomplete_cleanup: false,
                rejected_by_diff_gate: false,
                stdout_truncated: false,
                stderr_truncated: false,
                acceptance_state: WorkerAcceptanceState::ReadyForReview,
                changed_paths_count,
                changed_paths_preview: Vec::new(),
                violation_count: 0,
                violation_summaries: Vec::new(),
                final_summary: Some("worker prose is not delivery proof".to_string()),
                failure_reason: None,
                report_artifact_id: None,
            }),
            created_at_ms: 1_700_000_000_000,
            updated_at_ms: 1_700_000_001_000,
            started_at_ms: Some(1_700_000_000_100),
            completed_at_ms: Some(1_700_000_001_000),
            cancel_requested_at_ms: None,
            cancel_reason: None,
        }
    }

    fn build_snapshot(
        record: &ApprovalRecord,
        task: &ApprovedTaskRecord,
        worker: WorkerSnapshot,
    ) -> TaskStateSnapshot {
        TaskStateSnapshot {
            approval_id: record.approval_id.clone(),
            subject_digest: record.subject_digest.clone(),
            binding_id: record.binding_id.clone(),
            thread_uuid: record.thread_uuid.clone(),
            session_binding_id: SESSION_BINDING_ID.to_string(),
            task_id: task.task_id.clone(),
            task_digest: task.task_digest.clone(),
            adapter: task.adapter,
            provider: task.provider,
            depends_on: task.depends_on.clone(),
            dependency_input: None,
            profile_id: task.profile_id.clone(),
            command_key: task.command_key.clone(),
            status: TaskExecutionStatus::ReadyForReview,
            run_id: Some(worker.run_id.clone()),
            worker_id: Some(worker.worker_id.clone()),
            worker: Some(worker),
            accepted: false,
            lifecycle: None,
        }
    }

    fn assert_rejected_with_snapshot(
        fixture: &DeliveryFixture,
        mutate: impl FnOnce(&mut TaskStateSnapshot),
    ) {
        let mut snapshot = fixture.snapshot.clone();
        mutate(&mut snapshot);
        assert!(
            inspect_delivery_for_verified_task(&fixture.record, &snapshot, &fixture.work_area_root)
                .is_err(),
            "mutated task snapshot must be rejected"
        );
    }

    fn rewrite_manifest(fixture: &DeliveryFixture, mutate: impl FnOnce(&mut WorktreeManifest)) {
        let mut manifest: WorktreeManifest =
            serde_json::from_slice(&fs::read(&fixture.manifest_path).expect("read manifest"))
                .expect("parse manifest");
        mutate(&mut manifest);
        write_private_json_atomic(
            &fixture.work_area_root.join(MANIFESTS_SUBDIR),
            &format!("{}.json", fixture.worktree_id),
            &manifest,
            "worktree manifest",
        )
        .expect("rewrite manifest");
    }

    #[test]
    fn exact_patch_including_untracked_and_deleted_content_is_stable_and_read_only() {
        let mut fixture = DeliveryFixture::new();
        fixture.apply_patch();
        fixture.refresh_terminal_snapshot();

        let manifest_before = fs::read(&fixture.manifest_path).expect("manifest before");
        let lock_path = fixture
            .work_area_root
            .join(MANIFESTS_SUBDIR)
            .join(format!("{}.lock", fixture.worktree_id));
        assert!(!lock_path.exists());

        let first = fixture.inspect().expect("inspect exact patch");
        assert_eq!(first.delivery_kind, DeliveryKind::Patch);
        assert_eq!(first.base_commit, fixture.base_commit);
        assert_eq!(first.head_commit, fixture.base_commit);
        assert_eq!(first.changed_paths_count, 3);
        assert!(first.content_bytes > 0);
        assert_eq!(first.run_id, RUN_ID);
        assert_eq!(first.worker_id, WORKER_ID);
        assert_eq!(first.session_binding_id, SESSION_BINDING_ID);
        let serialized = serde_json::to_string(&first).expect("serialize inspection");
        assert!(!serialized.contains(INSTRUCTION));
        assert!(!serialized.contains("pub fn alpha"));
        assert!(!serialized.contains(fixture.checkout.to_string_lossy().as_ref()));

        let second = fixture.inspect().expect("repeat inspect exact patch");
        assert_eq!(first, second);
        assert_eq!(
            fs::read(&fixture.manifest_path).expect("manifest after"),
            manifest_before
        );
        assert!(!lock_path.exists(), "inspection must not create a job lock");
        fixture.recheck(&first).expect("recheck stable patch");

        // Restoring the deleted file changes the deletion identity even though the tracked
        // baseline is otherwise unchanged.
        fixture.restore_deleted_file();
        let restored = fixture.inspect().expect("inspect restored file");
        assert_eq!(restored.delivery_kind, DeliveryKind::Patch);
        assert_eq!(restored.changed_paths_count, 2);
        assert_ne!(restored.delivery_digest, first.delivery_digest);
        assert!(fixture.recheck(&first).is_err());
    }

    #[test]
    fn same_length_content_change_changes_digest_and_recheck_rejects() {
        let mut fixture = DeliveryFixture::new();
        fixture.apply_patch();
        fixture.refresh_terminal_snapshot();
        let before = fixture.inspect().expect("inspect before change");

        let lib_path = fixture.checkout.join("src/lib.rs");
        fs::write(&lib_path, "pub fn alpha() -> u32 { 3 }\n").expect("rewrite same length");
        let after = fixture.inspect().expect("inspect after same-length change");
        assert_eq!(after.changed_paths_count, before.changed_paths_count);
        assert_eq!(
            after.changed_path_set_digest, before.changed_path_set_digest,
            "the same path identity must retain the same path-set digest"
        );
        assert_ne!(after.delivery_digest, before.delivery_digest);
        assert_eq!(after.result_digest, before.result_digest);
        assert!(fixture.recheck(&before).is_err());
    }

    #[test]
    fn executable_mode_change_changes_delivery_digest() {
        let mut fixture = DeliveryFixture::new();
        fixture.apply_patch();
        fixture.refresh_terminal_snapshot();
        let before = fixture.inspect().expect("inspect before mode change");

        let lib_path = fixture.checkout.join("src/lib.rs");
        let mut permissions = fs::metadata(&lib_path).expect("lib metadata").permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&lib_path, permissions).expect("set executable mode");

        let after = fixture.inspect().expect("inspect after mode change");
        assert_eq!(
            after.changed_path_set_digest, before.changed_path_set_digest,
            "a mode-only change must retain the same path identity"
        );
        assert_ne!(after.delivery_digest, before.delivery_digest);
        assert!(fixture.recheck(&before).is_err());
    }

    #[test]
    fn no_change_delivery_is_explicit_and_not_a_fake_patch() {
        let fixture = DeliveryFixture::new();
        let inspection = fixture.inspect().expect("inspect no-change delivery");
        assert_eq!(inspection.delivery_kind, DeliveryKind::NoChanges);
        assert_eq!(inspection.changed_paths_count, 0);
        assert_eq!(inspection.content_bytes, 0);
        assert_eq!(inspection.head_commit, fixture.base_commit);
        fixture.recheck(&inspection).expect("recheck no-change");

        let mut prose_claim = fixture.snapshot.clone();
        prose_claim
            .worker
            .as_mut()
            .expect("worker")
            .outcome
            .as_mut()
            .expect("outcome")
            .final_summary = Some("I created a complete patch".to_string());
        let prose_inspection = inspect_delivery_for_verified_task(
            &fixture.record,
            &prose_claim,
            &fixture.work_area_root,
        )
        .expect("inspect prose-only claim");
        assert_eq!(prose_inspection.delivery_kind, DeliveryKind::NoChanges);
        assert_eq!(prose_inspection.changed_paths_count, 0);
    }

    #[test]
    fn committed_and_uncommitted_deliveries_are_supported() {
        let mut uncommitted = DeliveryFixture::new();
        uncommitted.apply_patch();
        uncommitted.refresh_terminal_snapshot();
        let uncommitted_inspection = uncommitted.inspect().expect("inspect uncommitted patch");
        assert_eq!(uncommitted_inspection.head_commit, uncommitted.base_commit);

        let mut committed = DeliveryFixture::new();
        committed.apply_patch();
        committed.commit_patch();
        committed.refresh_terminal_snapshot();
        let committed_inspection = committed.inspect().expect("inspect committed patch");
        assert_eq!(committed_inspection.delivery_kind, DeliveryKind::Patch);
        assert_eq!(committed_inspection.changed_paths_count, 3);
        assert_ne!(committed_inspection.head_commit, committed.base_commit);
        assert_ne!(
            committed_inspection.delivery_digest,
            uncommitted_inspection.delivery_digest
        );
        committed
            .recheck(&committed_inspection)
            .expect("recheck committed patch");
    }

    #[test]
    fn terminal_prose_and_timing_do_not_enter_the_result_digest() {
        let fixture = DeliveryFixture::new();
        let baseline = fixture.inspect().expect("inspect baseline");
        let mut altered = fixture.snapshot.clone();
        let worker = altered.worker.as_mut().expect("worker");
        worker.completed_at_ms = Some(9_999);
        worker.updated_at_ms = 9_999;
        let outcome = worker.outcome.as_mut().expect("outcome");
        outcome.final_summary = Some("different provider prose".to_string());
        outcome.failure_reason = Some("not proof".to_string());
        outcome.duration_ms = Some(42);
        let altered_inspection =
            inspect_delivery_for_verified_task(&fixture.record, &altered, &fixture.work_area_root)
                .expect("inspect altered prose");
        assert_eq!(baseline, altered_inspection);
    }

    #[test]
    fn cross_record_task_worker_worktree_branch_base_and_scope_mismatches_are_rejected() {
        let fixture = DeliveryFixture::new();
        let mut wrong_record = fixture.record.clone();
        wrong_record.approval_id = format!("apr_{}", &sha256_hex(b"other-approval")[..32]);
        assert!(inspect_delivery_for_verified_task(
            &wrong_record,
            &fixture.snapshot,
            &fixture.work_area_root
        )
        .is_err());

        assert_rejected_with_snapshot(&fixture, |snapshot| {
            snapshot.task_id = "task-unknown".to_string();
        });
        assert_rejected_with_snapshot(&fixture, |snapshot| {
            snapshot.worker_id = Some("wrk_other".to_string());
        });
        assert_rejected_with_snapshot(&fixture, |snapshot| {
            snapshot.session_binding_id = "bnd_other_session".to_string();
        });
        assert_rejected_with_snapshot(&fixture, |snapshot| {
            snapshot
                .worker
                .as_mut()
                .expect("worker")
                .worktree
                .task_worktree_id = Some("wt-task-other".to_string());
        });

        let branch_fixture = DeliveryFixture::new();
        let expected_run_tag = compact_worktree_tag(RUN_ID, "r");
        let expected_task_tag = compact_worktree_tag(WORKER_ID, "w");
        rewrite_manifest(&branch_fixture, |manifest| {
            manifest.branch_name =
                format!("threadlane/task/{expected_run_tag}/{expected_task_tag}/different");
        });
        assert!(branch_fixture.inspect().is_err());

        let base_fixture = DeliveryFixture::new();
        rewrite_manifest(&base_fixture, |manifest| {
            manifest.base_commit = "1111111111111111111111111111111111111111".to_string();
        });
        assert!(base_fixture.inspect().is_err());

        let scope_fixture = DeliveryFixture::new();
        rewrite_manifest(&scope_fixture, |manifest| {
            manifest.allowed_write_paths.insert("README.md".to_string());
        });
        assert!(scope_fixture.inspect().is_err());

        let mut narrowed_record = fixture.record.clone();
        narrowed_record.approved_write_paths = vec!["docs/".to_string()];
        assert!(inspect_delivery_for_verified_task(
            &narrowed_record,
            &fixture.snapshot,
            &fixture.work_area_root
        )
        .is_err());
    }

    #[test]
    fn active_manifest_job_and_held_job_lock_are_rejected() {
        let fixture = DeliveryFixture::new();
        let guard = begin_worktree_job(&fixture.work_area_root, &fixture.worktree_id)
            .expect("begin active job");
        let active_err = fixture.inspect().expect_err("active job must be rejected");
        assert_eq!(active_err.code(), "invalid_run_state");
        drop(guard);

        let clean_fixture = DeliveryFixture::new();
        let lock_path = clean_fixture
            .work_area_root
            .join(MANIFESTS_SUBDIR)
            .join(format!("{}.lock", clean_fixture.worktree_id));
        let lock_file =
            open_private_lock_file(&lock_path, "test job lock").expect("open private lock");
        lock_file
            .try_lock_exclusive()
            .expect("hold exclusive job lock");
        let held_err = clean_fixture
            .inspect()
            .expect_err("held lock must be rejected");
        assert_eq!(held_err.code(), "invalid_run_state");
        fs2::FileExt::unlock(&lock_file).expect("unlock test job lock");
    }

    #[test]
    fn failed_unknown_missing_and_dishonest_terminal_evidence_is_rejected() {
        let fixture = DeliveryFixture::new();

        assert_rejected_with_snapshot(&fixture, |snapshot| snapshot.worker = None);
        assert_rejected_with_snapshot(&fixture, |snapshot| {
            snapshot.status = TaskExecutionStatus::Unknown;
        });
        assert_rejected_with_snapshot(&fixture, |snapshot| {
            snapshot.accepted = true;
        });
        assert_rejected_with_snapshot(&fixture, |snapshot| {
            snapshot.run_id = None;
        });
        assert_rejected_with_snapshot(&fixture, |snapshot| {
            snapshot.worker_id = None;
        });
        assert_rejected_with_snapshot(&fixture, |snapshot| {
            snapshot.task_digest = sha256_hex(b"different-task-digest");
        });
        assert_rejected_with_snapshot(&fixture, |snapshot| {
            snapshot.worker.as_mut().expect("worker").status = WorkerStatus::Failed;
        });
        assert_rejected_with_snapshot(&fixture, |snapshot| {
            snapshot.worker.as_mut().expect("worker").outcome = None;
        });
        assert_rejected_with_snapshot(&fixture, |snapshot| {
            snapshot
                .worker
                .as_mut()
                .expect("worker")
                .outcome
                .as_mut()
                .expect("outcome")
                .acceptance_state = WorkerAcceptanceState::ExecutionFailed;
        });
        assert_rejected_with_snapshot(&fixture, |snapshot| {
            snapshot
                .worker
                .as_mut()
                .expect("worker")
                .outcome
                .as_mut()
                .expect("outcome")
                .exit_code = None;
        });
        assert_rejected_with_snapshot(&fixture, |snapshot| {
            snapshot
                .worker
                .as_mut()
                .expect("worker")
                .outcome
                .as_mut()
                .expect("outcome")
                .missing_model_evidence = true;
        });
        assert_rejected_with_snapshot(&fixture, |snapshot| {
            snapshot
                .worker
                .as_mut()
                .expect("worker")
                .outcome
                .as_mut()
                .expect("outcome")
                .missing_effort_evidence = true;
        });
        assert_rejected_with_snapshot(&fixture, |snapshot| {
            snapshot
                .worker
                .as_mut()
                .expect("worker")
                .outcome
                .as_mut()
                .expect("outcome")
                .provider_error = true;
        });
        assert_rejected_with_snapshot(&fixture, |snapshot| {
            snapshot
                .worker
                .as_mut()
                .expect("worker")
                .outcome
                .as_mut()
                .expect("outcome")
                .incomplete_cleanup = true;
        });
        assert_rejected_with_snapshot(&fixture, |snapshot| {
            snapshot
                .worker
                .as_mut()
                .expect("worker")
                .outcome
                .as_mut()
                .expect("outcome")
                .rejected_by_diff_gate = true;
        });
        assert_rejected_with_snapshot(&fixture, |snapshot| {
            snapshot
                .worker
                .as_mut()
                .expect("worker")
                .outcome
                .as_mut()
                .expect("outcome")
                .violation_count = 1;
        });
        assert_rejected_with_snapshot(&fixture, |snapshot| {
            snapshot
                .worker
                .as_mut()
                .expect("worker")
                .outcome
                .as_mut()
                .expect("outcome")
                .timed_out = true;
        });
        assert_rejected_with_snapshot(&fixture, |snapshot| {
            snapshot
                .worker
                .as_mut()
                .expect("worker")
                .outcome
                .as_mut()
                .expect("outcome")
                .cancelled = true;
        });
        assert_rejected_with_snapshot(&fixture, |snapshot| {
            snapshot
                .worker
                .as_mut()
                .expect("worker")
                .outcome
                .as_mut()
                .expect("outcome")
                .stdout_truncated = true;
        });
        assert_rejected_with_snapshot(&fixture, |snapshot| {
            snapshot
                .worker
                .as_mut()
                .expect("worker")
                .outcome
                .as_mut()
                .expect("outcome")
                .stderr_truncated = true;
        });
        assert_rejected_with_snapshot(&fixture, |snapshot| {
            snapshot.worker.as_mut().expect("worker").observed_model = None;
        });
        assert_rejected_with_snapshot(&fixture, |snapshot| {
            snapshot.worker.as_mut().expect("worker").observed_effort = None;
        });

        // A cancellation request that lost the race to complete natural success keeps the
        // existing successful-result policy and remains eligible for inspection.
        let mut cancel_raced = fixture.snapshot.clone();
        cancel_raced
            .worker
            .as_mut()
            .expect("worker")
            .outcome
            .as_mut()
            .expect("outcome")
            .cancel_requested_before_completion = true;
        inspect_delivery_for_verified_task(&fixture.record, &cancel_raced, &fixture.work_area_root)
            .expect("natural success with retained cancellation request flag");
    }

    #[test]
    fn out_of_scope_symlink_and_oversized_deliveries_are_typed_refusals() {
        let out_of_scope = DeliveryFixture::new();
        fs::write(out_of_scope.checkout.join("README.md"), "outside scope\n")
            .expect("write out-of-scope file");
        let scope_err = out_of_scope
            .inspect()
            .expect_err("out-of-scope file must be rejected");
        assert_eq!(scope_err.code(), "validation");

        let symlink_fixture = DeliveryFixture::new();
        symlink("lib.rs", symlink_fixture.checkout.join("src/link.rs")).expect("create symlink");
        let symlink_err = symlink_fixture
            .inspect()
            .expect_err("symlink must be rejected");
        assert_eq!(symlink_err.code(), "validation");

        let oversized = DeliveryFixture::new();
        let oversized_path = oversized.checkout.join("src/huge.bin");
        let oversized_file = fs::File::create(&oversized_path).expect("create oversized file");
        oversized_file
            .set_len(MAX_DELIVERY_CONTENT_BYTES + 1)
            .expect("set oversized length");
        let oversized_err = oversized
            .inspect()
            .expect_err("oversized delivery must be rejected");
        assert_eq!(oversized_err.code(), "validation");
    }

    #[test]
    fn dependency_binding_must_match_the_exact_execution_base() {
        let mut fixture = DeliveryFixture::new();
        let task = fixture.record.tasks[0].clone();
        let mut dependent_task = task.clone();
        dependent_task.depends_on = vec!["task-parent".to_string()];
        fixture.record.tasks[0] = dependent_task.clone();
        fixture.snapshot.task_id = dependent_task.task_id.clone();
        fixture.snapshot.depends_on = dependent_task.depends_on.clone();
        fixture.snapshot.task_digest = dependent_task.task_digest.clone();
        fixture.snapshot.profile_id = dependent_task.profile_id.clone();
        fixture.snapshot.command_key = dependent_task.command_key.clone();

        let binding = WorkerExecutionBinding {
            schema_version: EXECUTION_BINDING_SCHEMA_VERSION,
            input_digest: sha256_hex(b"input"),
            seal_file_digest: sha256_hex(b"seal"),
            execution_base_commit: fixture.base_commit.clone(),
            execution_digest: sha256_hex(b"execution"),
        };
        let worker = fixture.snapshot.worker.as_mut().expect("worker");
        worker.execution_binding = Some(binding);
        fixture
            .inspect()
            .expect("dependent task binding matching checkout base");

        fixture
            .snapshot
            .worker
            .as_mut()
            .expect("worker")
            .execution_binding
            .as_mut()
            .expect("binding")
            .execution_base_commit = "2222222222222222222222222222222222222222".to_string();
        assert!(fixture.inspect().is_err());
    }
}
